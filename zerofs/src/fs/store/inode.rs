use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::KeyCodec;
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<EncryptedDb>,
}

impl InodeStore {
    pub fn new(db: Arc<EncryptedDb>) -> Self {
        Self { db }
    }

    pub async fn load(&self, id: InodeId) -> Result<Inode, FsError> {
        let key = KeyCodec::inode_key(id);

        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                tracing::error!(
                    "InodeStore::load({}): database get_bytes failed: {:?}",
                    id,
                    e
                );
                FsError::IoError
            })?
            .ok_or_else(|| {
                tracing::warn!(
                    "InodeStore::load({}): inode key not found in database (key={:?}).",
                    id,
                    key
                );
                FsError::NotFound
            })?;

        bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "InodeStore::load({}): failed to deserialize inode data (len={}): {:?}.",
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

    pub fn save_counter(&self, txn: &mut EncryptedTransaction, next_id: u64) {
        let key = KeyCodec::system_counter_key();
        txn.put_bytes(&key, KeyCodec::encode_counter(next_id));
    }
}
