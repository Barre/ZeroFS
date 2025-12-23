use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// A generic lock manager that provides per-key mutex locks.
#[derive(Clone)]
pub struct KeyedLockManager<K: Eq + Hash + Clone + Send + Sync + 'static> {
    locks: Arc<DashMap<K, Arc<Mutex<()>>>>,
}

pub struct KeyedLockGuard<K: Eq + Hash + Clone + Send + Sync + 'static> {
    _guard: OwnedMutexGuard<()>,
    key: K,
    locks: Arc<DashMap<K, Arc<Mutex<()>>>>,
}

struct ShardLockGuard<K: Eq + Hash + Clone + Send + Sync + 'static> {
    _guard: KeyedLockGuard<K>,
}

pub struct MultiLockGuard<K: Eq + Hash + Clone + Send + Sync + 'static> {
    _guards: Vec<ShardLockGuard<K>>,
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> Default for KeyedLockManager<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> KeyedLockManager<K> {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
        }
    }

    /// Get or create the lock for a given key
    fn get_or_create_lock(&self, key: &K) -> Arc<Mutex<()>> {
        self.locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Acquire a lock for the given key
    pub async fn acquire(&self, key: K) -> KeyedLockGuard<K> {
        let lock = self.get_or_create_lock(&key);
        let guard = lock.lock_owned().await;
        KeyedLockGuard {
            _guard: guard,
            key,
            locks: self.locks.clone(),
        }
    }
}

impl<K: Eq + Hash + Clone + Ord + Send + Sync + 'static> KeyedLockManager<K> {
    /// Acquire multiple locks with automatic ordering to prevent deadlocks.
    pub async fn acquire_multi(&self, mut keys: Vec<K>) -> MultiLockGuard<K> {
        // Sort by key to ensure consistent ordering
        keys.sort();
        keys.dedup();

        let mut guards = Vec::with_capacity(keys.len());

        for key in keys {
            let lock = self.get_or_create_lock(&key);
            let guard = lock.lock_owned().await;
            let lock_guard = KeyedLockGuard {
                _guard: guard,
                key,
                locks: self.locks.clone(),
            };

            guards.push(ShardLockGuard { _guard: lock_guard });
        }

        MultiLockGuard { _guards: guards }
    }
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> Drop for KeyedLockGuard<K> {
    fn drop(&mut self) {
        // Try to remove the lock if it's no longer in use
        self.locks.remove_if(&self.key, |_, lock| {
            // The guard holds one reference via OwnedMutexGuard
            // DashMap holds another
            Arc::strong_count(lock) <= 2
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_single_lock_acquisition() {
        let manager: KeyedLockManager<u64> = KeyedLockManager::new();

        let _guard1 = manager.acquire(1).await;
        drop(_guard1);

        let _guard2 = manager.acquire(1).await;
    }

    #[tokio::test]
    async fn test_multiple_lock_ordering() {
        let manager: KeyedLockManager<u64> = KeyedLockManager::new();

        let guard1 = manager.acquire_multi(vec![3, 1, 2]).await;
        drop(guard1);

        let _guard2 = manager.acquire_multi(vec![2, 3, 1]).await;
    }

    #[tokio::test]
    async fn test_no_collision_different_keys() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let manager: Arc<KeyedLockManager<u64>> = Arc::new(KeyedLockManager::new());

        let _guard1 = manager.acquire(0).await;

        let manager2 = manager.clone();
        let acquired = Arc::new(AtomicBool::new(false));
        let acquired2 = acquired.clone();

        let handle = tokio::spawn(async move {
            let _guard = manager2.acquire(1).await;
            acquired2.store(true, Ordering::SeqCst);
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(
            acquired.load(Ordering::SeqCst),
            "Should NOT be blocked - different keys have different locks"
        );

        drop(_guard1);

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_keys_no_deadlock() {
        let manager: KeyedLockManager<u64> = KeyedLockManager::new();

        let _guard = manager.acquire_multi(vec![0, 4, 8]).await;
    }

    #[tokio::test]
    async fn test_generic_with_bytes() {
        use bytes::Bytes;

        let manager: KeyedLockManager<Bytes> = KeyedLockManager::new();

        let key1 = Bytes::from_static(b"key1");
        let key2 = Bytes::from_static(b"key2");

        let _guard1 = manager.acquire(key1.clone()).await;
        let _guard2 = manager.acquire(key2).await;

        drop(_guard1);
        let _guard3 = manager.acquire(key1).await;
        drop(_guard3);
    }
}
