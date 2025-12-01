use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::KeyCodec;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_HARDLINKS_PER_INODE: u32 = u32::MAX;

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<EncryptedDb>,
    next_id: Arc<AtomicU64>,
}

impl InodeStore {
    pub fn new(db: Arc<EncryptedDb>, initial_next_id: u64) -> Self {
        Self {
            db,
            next_id: Arc::new(AtomicU64::new(initial_next_id)),
        }
    }

    pub fn allocate(&self) -> InodeId {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub async fn get(&self, id: InodeId) -> Result<Inode, FsError> {
        let key = KeyCodec::inode_key(id);

        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                tracing::error!(
                    "InodeStore::get({}): database get_bytes failed: {:?}",
                    id,
                    e
                );
                FsError::IoError
            })?
            .ok_or_else(|| {
                tracing::warn!(
                    "InodeStore::get({}): inode key not found in database (key={:?}).",
                    id,
                    key
                );
                FsError::NotFound
            })?;

        bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "InodeStore::get({}): failed to deserialize inode data (len={}): {:?}.",
                id,
                data.len(),
                e
            );
            FsError::InvalidData
        })
    }

    pub fn save(
        &self,
        txn: &mut EncryptedTransaction,
        id: InodeId,
        inode: &Inode,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let key = KeyCodec::inode_key(id);
        let data = bincode::serialize(inode)?;
        txn.put_bytes(&key, Bytes::from(data));
        Ok(())
    }

    pub fn delete(&self, txn: &mut EncryptedTransaction, id: InodeId) {
        let key = KeyCodec::inode_key(id);
        txn.delete_bytes(&key);
    }

    pub fn save_counter(&self, txn: &mut EncryptedTransaction) {
        let key = KeyCodec::system_counter_key();
        let next_id = self.next_id.load(Ordering::SeqCst);
        txn.put_bytes(&key, KeyCodec::encode_counter(next_id));
    }
}
