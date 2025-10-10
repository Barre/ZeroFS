use super::inode::{Inode, InodeId};
use foyer::{Cache, CacheBuilder};
use std::sync::Arc;

const MAX_ENTRIES: usize = 10_000;

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
        let cache = CacheBuilder::new(MAX_ENTRIES).with_shards(64).build();

        Ok(Self {
            cache: Arc::new(cache),
        })
    }

    pub fn get(&self, key: CacheKey) -> Option<CacheValue> {
        self.cache.get(&key).map(|entry| entry.value().clone())
    }

    pub fn insert(&self, key: CacheKey, value: CacheValue) {
        self.cache.insert(key, value);
    }

    pub fn remove(&self, key: CacheKey) {
        self.cache.remove(&key);
    }

    pub fn remove_batch(&self, keys: Vec<CacheKey>) {
        for key in keys {
            self.cache.remove(&key);
        }
    }
}
