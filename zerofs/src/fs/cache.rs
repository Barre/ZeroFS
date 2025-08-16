use super::inode::{Inode, InodeId};
use foyer::{Cache, CacheBuilder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum CacheKey {
    Metadata(InodeId),
    DirEntry { dir_id: InodeId, name: String },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum CacheValue {
    Metadata(#[serde(with = "serde_arc")] Arc<Inode>),
    DirEntry(InodeId),
}

mod serde_arc {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

    pub fn serialize<S, T>(arc: &Arc<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        (**arc).serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Arc<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Arc::new(T::deserialize(deserializer)?))
    }
}

#[derive(Clone)]
pub struct UnifiedCache {
    cache: Arc<Cache<Vec<u8>, Vec<u8>>>,
}

impl UnifiedCache {
    pub async fn new(
        _cache_dir: &str,
        _disk_capacity_gb: f64,
        memory_capacity_gb: Option<f64>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let memory_capacity_bytes =
            (memory_capacity_gb.unwrap_or(1.0) * 1024.0 * 1024.0 * 1024.0) as usize;

        let cache = CacheBuilder::new(memory_capacity_bytes)
            .with_shards(64)
            .with_weighter(|_key: &Vec<u8>, value: &Vec<u8>| -> usize { _key.len() + value.len() })
            .build();

        Ok(Self {
            cache: Arc::new(cache),
        })
    }

    pub async fn get(&self, key: CacheKey) -> Option<CacheValue> {
        let serialized_key = bincode::serialize(&key).ok()?;
        let value = self
            .cache
            .get(&serialized_key)
            .map(|entry| entry.value().clone())?;
        bincode::deserialize(&value).ok()
    }

    pub async fn insert(&self, key: CacheKey, value: CacheValue, _prefer_on_disk: bool) {
        if let (Ok(serialized_key), Ok(serialized_value)) =
            (bincode::serialize(&key), bincode::serialize(&value))
        {
            self.cache.insert(serialized_key, serialized_value);
        }
    }

    pub async fn remove(&self, key: CacheKey) {
        if let Ok(serialized_key) = bincode::serialize(&key) {
            self.cache.remove(&serialized_key);
        }
    }

    pub async fn remove_batch(&self, keys: Vec<CacheKey>) {
        for key in keys {
            if let Ok(serialized_key) = bincode::serialize(&key) {
                self.cache.remove(&serialized_key);
            }
        }
    }
}
