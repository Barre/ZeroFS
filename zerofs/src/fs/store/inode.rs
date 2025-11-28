use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::cache::{CacheKey, CacheValue, UnifiedCache};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::KeyCodec;
use crate::fs::metrics::FileSystemStats;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<EncryptedDb>,
    cache: Arc<UnifiedCache>,
    stats: Arc<FileSystemStats>,
}

impl InodeStore {
    pub fn new(
        db: Arc<EncryptedDb>,
        cache: Arc<UnifiedCache>,
        stats: Arc<FileSystemStats>,
    ) -> Self {
        Self { db, cache, stats }
    }

    pub async fn get(&self, id: InodeId) -> Result<Inode, FsError> {
        let cache_key = CacheKey::Metadata(id);
        if let Some(CacheValue::Metadata(cached_inode)) = self.cache.get(cache_key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((*cached_inode).clone());
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

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

        let inode: Inode = bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "InodeStore::load({}): failed to deserialize inode data (len={}): {:?}.",
                id,
                data.len(),
                e
            );
            FsError::InvalidData
        })?;

        let cache_key = CacheKey::Metadata(id);
        let cache_value = CacheValue::Metadata(Arc::new(inode.clone()));
        self.cache.insert(cache_key, cache_value);

        Ok(inode)
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
        self.cache.remove(CacheKey::Metadata(id));
        Ok(())
    }

    pub fn delete(&self, txn: &mut EncryptedTransaction, id: InodeId) {
        let key = KeyCodec::inode_key(id);
        txn.delete_bytes(&key);
        self.cache.remove(CacheKey::Metadata(id));
    }

    pub fn save_counter(&self, txn: &mut EncryptedTransaction, next_id: u64) {
        let key = KeyCodec::system_counter_key();
        txn.put_bytes(&key, KeyCodec::encode_counter(next_id));
    }
}
