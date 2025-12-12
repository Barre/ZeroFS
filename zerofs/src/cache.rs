use async_trait::async_trait;
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};

pub struct FoyerCache {
    inner: foyer_memory::Cache<CachedKey, CachedEntry>,
}

impl FoyerCache {
    pub fn new(capacity: usize) -> Self {
        let builder = foyer_memory::CacheBuilder::new(capacity)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .with_shards(128);

        let cache = builder.build();

        Self { inner: cache }
    }
}

#[async_trait]
impl DbCache for FoyerCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inner.insert(key, value);
    }

    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key);
    }

    fn entry_count(&self) -> u64 {
        // foyer cache doesn't support an entry count estimate
        0
    }
}
