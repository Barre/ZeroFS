use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::{KeyCodec, KeyPrefix, ParsedKey};
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct TombstoneEntry {
    pub key: Bytes,
    pub inode_id: InodeId,
    pub remaining_size: u64,
}

#[derive(Clone)]
pub struct TombstoneStore {
    db: Arc<EncryptedDb>,
}

impl TombstoneStore {
    pub fn new(db: Arc<EncryptedDb>) -> Self {
        Self { db }
    }

    pub fn add(&self, txn: &mut EncryptedTransaction, inode_id: InodeId, size: u64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let key = KeyCodec::tombstone_key(timestamp, inode_id);
        txn.put_bytes(&key, KeyCodec::encode_tombstone_size(size));
    }

    pub fn update(&self, txn: &mut EncryptedTransaction, key: &Bytes, new_size: u64) {
        txn.put_bytes(key, KeyCodec::encode_tombstone_size(new_size));
    }

    pub fn remove(&self, txn: &mut EncryptedTransaction, key: &Bytes) {
        txn.delete_bytes(key);
    }

    pub async fn list(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<TombstoneEntry, FsError>> + Send + '_>>, FsError>
    {
        let (start, end) = KeyCodec::prefix_range(KeyPrefix::Tombstone);

        let iter = self
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(iter, |mut iter| async {
            match futures::StreamExt::next(&mut iter).await {
                Some(Ok((key, value))) => match KeyCodec::parse_key(&key) {
                    ParsedKey::Tombstone { inode_id } => {
                        match KeyCodec::decode_tombstone_size(&value) {
                            Ok(remaining_size) => Some((
                                Ok(TombstoneEntry {
                                    key,
                                    inode_id,
                                    remaining_size,
                                }),
                                iter,
                            )),
                            Err(e) => Some((Err(e), iter)),
                        }
                    }
                    _ => Some((Err(FsError::InvalidData), iter)),
                },
                Some(Err(_)) => Some((Err(FsError::IoError), iter)),
                None => None,
            }
        })))
    }
}
