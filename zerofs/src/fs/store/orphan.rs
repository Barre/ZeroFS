use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::{KeyCodec, KeyPrefix, ParsedKey};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

/// Durable set of inodes that were unlinked while a 9P fid still held them
/// open (POSIX open-unlink). Membership means: the namespace entry is gone and
/// nlink==0, but the inode record + extents are intentionally kept alive so the
/// open fid can keep reading/writing. The entry is removed in the same
/// transaction that finally reclaims the inode (last clunk, or startup drain).
#[derive(Clone)]
pub struct OrphanStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
}

impl OrphanStore {
    pub fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>) -> Self {
        Self { db, key_codec }
    }

    /// Mark `inode_id` as an orphan in the surrounding namespace transaction.
    pub fn add(&self, txn: &mut Transaction, inode_id: InodeId) {
        let key = self.key_codec.orphan_key(inode_id);
        txn.put_bytes(&key, bytes::Bytes::new());
    }

    pub fn remove(&self, txn: &mut Transaction, inode_id: InodeId) {
        let key = self.key_codec.orphan_key(inode_id);
        txn.delete_bytes(&key);
    }

    /// Whether `inode_id` currently has an orphan-set entry. A point read used
    /// to avoid committing a no-op delete txn on the hot clunk path.
    pub async fn contains(&self, inode_id: InodeId) -> Result<bool, FsError> {
        let key = self.key_codec.orphan_key(inode_id);
        Ok(self
            .db
            .get_bytes(&key)
            .await
            .map_err(|_| FsError::IoError)?
            .is_some())
    }

    pub async fn list(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<InodeId, FsError>> + Send + '_>>, FsError> {
        let (start, end) = self.key_codec.prefix_range(KeyPrefix::Orphan);
        let codec = self.key_codec.clone();

        let iter = self
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(
            (iter, codec),
            |(mut iter, codec)| async move {
                match futures::StreamExt::next(&mut iter).await {
                    Some(Ok((key, _value))) => match codec.parse_key(&key) {
                        ParsedKey::Orphan { inode_id } => Some((Ok(inode_id), (iter, codec))),
                        _ => Some((Err(FsError::InvalidData), (iter, codec))),
                    },
                    Some(Err(_)) => Some((Err(FsError::IoError), (iter, codec))),
                    None => None,
                }
            },
        )))
    }
}
