use foyer_memory::{Cache, CacheBuilder};

use super::inode::{Inode, InodeId};
use std::sync::Arc;

const MAX_ENTRIES: usize = 50_000;

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
        let cache = CacheBuilder::new(MAX_ENTRIES).with_shards(64).build();

        Ok(Self {
            cache: Arc::new(cache),
            is_active,
        })
    }

    pub fn get(&self, key: CacheKey) -> Option<CacheValue> {
        if !self.is_active {
            return None;
        }
        self.cache.get(&key).map(|entry| entry.value().clone())
    }

    pub fn insert(&self, key: CacheKey, value: CacheValue) {
        if !self.is_active {
            return;
        }
        self.cache.insert(key, value);
    }

    pub fn remove(&self, key: CacheKey) {
        if !self.is_active {
            return;
        }
        self.cache.remove(&key);
    }

    pub fn remove_batch(&self, keys: Vec<CacheKey>) {
        if !self.is_active {
            return;
        }
        for key in keys {
            self.cache.remove(&key);
        }
    }
}
