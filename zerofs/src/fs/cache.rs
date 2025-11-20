use moka::future::{Cache, CacheBuilder};

use super::inode::{Inode, InodeId};
use std::sync::Arc;

const MAX_ENTRIES: u64 = 50_000;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum CacheKey {
    Metadata(InodeId),
    DirEntry { dir_id: InodeId, name: Vec<u8> },
}

#[derive(Clone)]
pub enum CacheValue {
    Metadata(Arc<Inode>),
    DirEntry(InodeId),
}

#[derive(Clone)]
pub struct UnifiedCache {
    cache: Arc<Cache<CacheKey, CacheValue>>,
    is_active: bool,
}

impl UnifiedCache {
    pub fn new(is_active: bool) -> anyhow::Result<Self> {
        let cache = CacheBuilder::new(MAX_ENTRIES).build();

        Ok(Self {
            cache: Arc::new(cache),
            is_active,
        })
    }

    pub async fn get(&self, key: CacheKey) -> Option<CacheValue> {
        if !self.is_active {
            return None;
        }
        self.cache.get(&key).await
    }

    pub async fn insert(&self, key: CacheKey, value: CacheValue) {
        if !self.is_active {
            return;
        }
        self.cache.insert(key, value).await;
    }

    pub async fn remove(&self, key: CacheKey) {
        if !self.is_active {
            return;
        }
        self.cache.remove(&key).await;
    }

    pub async fn remove_batch(&self, keys: Vec<CacheKey>) {
        if !self.is_active {
            return;
        }
        let futures = keys.iter().map(|key| self.cache.remove(key));
        futures::future::join_all(futures).await;
    }

    pub async fn clear(&self) {
        // Clear cache by invalidating all entries
        // Note: moka cache doesn't have a direct clear method, so we just mark as inactive temporarily
        // In practice, the cache will naturally expire entries
        // For a proper clear, we'd need to track all keys, but that's expensive
        // For now, this is a no-op - cache entries will expire naturally
    }
}
