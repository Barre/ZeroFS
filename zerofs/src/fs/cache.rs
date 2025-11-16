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
}

impl UnifiedCache {
    pub fn new() -> anyhow::Result<Self> {
        let cache = CacheBuilder::new(MAX_ENTRIES).build();

        Ok(Self {
            cache: Arc::new(cache),
        })
    }

    pub async fn get(&self, key: CacheKey) -> Option<CacheValue> {
        self.cache.get(&key).await
    }

    pub async fn insert(&self, key: CacheKey, value: CacheValue) {
        self.cache.insert(key, value).await;
    }

    pub async fn remove(&self, key: CacheKey) {
        self.cache.remove(&key).await;
    }

    pub async fn remove_batch(&self, keys: Vec<CacheKey>) {
        let futures = keys.iter().map(|key| self.cache.remove(key));
        futures::future::join_all(futures).await;
    }
}
