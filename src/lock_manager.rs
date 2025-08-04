use crate::inode::InodeId;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockKey {
    Inode(InodeId),
    Chunk(InodeId, u64), // (inode_id, chunk_index)
}

#[derive(Clone)]
pub struct LockManager {
    locks: Arc<DashMap<LockKey, Arc<RwLock<()>>>>,
}

pub enum LockGuard {
    Read {
        _guard: OwnedRwLockReadGuard<()>,
        lock_key: LockKey,
        locks: Arc<DashMap<LockKey, Arc<RwLock<()>>>>,
    },
    Write {
        _guard: OwnedRwLockWriteGuard<()>,
        lock_key: LockKey,
        locks: Arc<DashMap<LockKey, Arc<RwLock<()>>>>,
    },
}

struct ShardLockGuard {
    _guard: LockGuard,
}

pub struct MultiLockGuard {
    _guards: Vec<ShardLockGuard>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
        }
    }

    /// Get or create the lock for a given key
    fn get_or_create_lock(&self, key: LockKey) -> Arc<RwLock<()>> {
        self.locks
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Acquire a single lock for reading (inode-level)
    pub async fn acquire_read(&self, inode_id: InodeId) -> LockGuard {
        let lock_key = LockKey::Inode(inode_id);
        let lock = self.get_or_create_lock(lock_key);
        let guard = lock.read_owned().await;
        LockGuard::Read {
            _guard: guard,
            lock_key,
            locks: self.locks.clone(),
        }
    }

    /// Acquire a single lock for writing (inode-level)
    pub async fn acquire_write(&self, inode_id: InodeId) -> LockGuard {
        let lock_key = LockKey::Inode(inode_id);
        let lock = self.get_or_create_lock(lock_key);
        let guard = lock.write_owned().await;
        LockGuard::Write {
            _guard: guard,
            lock_key,
            locks: self.locks.clone(),
        }
    }

    /// Acquire chunk locks for writing
    pub async fn acquire_chunk_locks(
        &self,
        inode_id: InodeId,
        chunk_indices: Vec<u64>,
    ) -> MultiLockGuard {
        let mut lock_keys: Vec<LockKey> = chunk_indices
            .into_iter()
            .map(|idx| LockKey::Chunk(inode_id, idx))
            .collect();

        // Sort to ensure consistent ordering and prevent deadlocks
        lock_keys.sort();
        lock_keys.dedup();

        let mut guards = Vec::with_capacity(lock_keys.len());

        for key in lock_keys {
            let lock = self.get_or_create_lock(key);
            let guard = lock.write_owned().await;
            let lock_guard = LockGuard::Write {
                _guard: guard,
                lock_key: key,
                locks: self.locks.clone(),
            };
            guards.push(ShardLockGuard { _guard: lock_guard });
        }

        MultiLockGuard { _guards: guards }
    }

    /// Acquire multiple write locks with automatic ordering to prevent deadlocks.
    pub async fn acquire_multiple_write(&self, inode_ids: Vec<InodeId>) -> MultiLockGuard {
        // Convert to LockKey and sort to ensure consistent ordering
        let mut lock_keys: Vec<LockKey> = inode_ids.into_iter().map(LockKey::Inode).collect();
        lock_keys.sort();
        lock_keys.dedup();

        let mut guards = Vec::with_capacity(lock_keys.len());

        for key in lock_keys {
            let lock = self.get_or_create_lock(key);
            let guard = lock.write_owned().await;
            let lock_guard = LockGuard::Write {
                _guard: guard,
                lock_key: key,
                locks: self.locks.clone(),
            };

            guards.push(ShardLockGuard { _guard: lock_guard });
        }

        MultiLockGuard { _guards: guards }
    }
}

/// Implement drop to clean up unused locks
impl Drop for LockGuard {
    fn drop(&mut self) {
        let (lock_key, locks) = match self {
            LockGuard::Read {
                lock_key, locks, ..
            } => (lock_key, locks),
            LockGuard::Write {
                lock_key, locks, ..
            } => (lock_key, locks),
        };

        // Try to remove the lock if it's no longer in use
        locks.remove_if(lock_key, |_, lock| {
            // The guard holds one reference via OwnedRwLockGuard
            // DashMap holds another
            // If strong_count is 2 or less, we can safely remove
            Arc::strong_count(lock) <= 2
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_single_lock_acquisition() {
        let manager = LockManager::new();

        let _guard1 = manager.acquire_write(1).await;
        drop(_guard1);

        let _guard2 = manager.acquire_write(1).await;
    }

    #[tokio::test]
    async fn test_multiple_lock_ordering() {
        let manager = LockManager::new();

        let guard1 = manager.acquire_multiple_write(vec![3, 1, 2]).await;
        drop(guard1);

        let _guard2 = manager.acquire_multiple_write(vec![2, 3, 1]).await;
    }

    #[tokio::test]
    async fn test_no_collision_different_inodes() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let manager = Arc::new(LockManager::new());

        let _guard1 = manager.acquire_write(0).await;

        let manager2 = manager.clone();
        let acquired = Arc::new(AtomicBool::new(false));
        let acquired2 = acquired.clone();

        let handle = tokio::spawn(async move {
            let _guard = manager2.acquire_write(1).await;
            acquired2.store(true, Ordering::SeqCst);
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(
            acquired.load(Ordering::SeqCst),
            "Should NOT be blocked - different inodes have different locks now"
        );

        drop(_guard1);

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_inodes_no_deadlock() {
        let manager = LockManager::new();

        let _guard = manager.acquire_multiple_write(vec![0, 4, 8]).await;
        let _guard2 = manager.acquire_multiple_write(vec![1, 5, 9]).await;
    }

    #[tokio::test]
    async fn test_lock_cleanup() {
        let manager = LockManager::new();

        // Acquire and release a lock
        {
            let _guard = manager.acquire_write(42).await;
            assert_eq!(manager.locks.len(), 1);
        }

        // Lock should be cleaned up after drop
        assert_eq!(manager.locks.len(), 0);

        // Acquire multiple locks
        {
            let _guard1 = manager.acquire_write(1).await;
            let _guard2 = manager.acquire_write(2).await;
            assert_eq!(manager.locks.len(), 2);
        }

        // All locks should be cleaned up
        assert_eq!(manager.locks.len(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_lock_creation() {
        use std::sync::Arc;

        let manager = Arc::new(LockManager::new());
        let inode_id = 42;

        // Spawn multiple tasks trying to create the same lock
        let mut handles = vec![];
        for _ in 0..10 {
            let manager_clone = manager.clone();
            let handle =
                tokio::spawn(
                    async move { manager_clone.get_or_create_lock(LockKey::Inode(inode_id)) },
                );
            handles.push(handle);
        }

        // Collect all the Arc<RwLock<()>> results
        let locks: Vec<Arc<RwLock<()>>> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should be the same lock instance
        let first_lock = &locks[0];
        for lock in &locks[1..] {
            assert!(
                Arc::ptr_eq(first_lock, lock),
                "All locks should be the same instance"
            );
        }

        // Should only have created one entry in the map
        assert_eq!(manager.locks.len(), 1);
    }
}
