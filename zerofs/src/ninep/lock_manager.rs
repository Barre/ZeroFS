use super::protocol::LockType;
use crate::fs::inode::InodeId;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct LockId(u64);

// Represents a POSIX file lock
#[derive(Debug, Clone)]
pub struct FileLock {
    pub lock_type: LockType,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: Vec<u8>,
    pub fid: u32,
    pub inode_id: InodeId,
}

impl FileLock {
    /// Returns the exclusive end of this lock's byte range.
    /// A length of 0 means "to end of file" per POSIX, represented as u64::MAX.
    pub fn end(&self) -> u64 {
        if self.length == 0 {
            u64::MAX
        } else {
            self.start.saturating_add(self.length)
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileLockManager {
    // Locks indexed by inode for conflict checking
    locks_by_inode: Arc<DashMap<InodeId, Vec<LockId>>>,
    // Lock IDs indexed by session for cleanup
    locks_by_session: Arc<DashMap<u64, Vec<LockId>>>,
    // Lock details
    locks: Arc<DashMap<LockId, FileLock>>,
    // Counter for generating unique lock IDs
    next_lock_id: Arc<AtomicU64>,
    // Mutex for atomic lock operations
    lock_mutex: Arc<tokio::sync::Mutex<()>>,
}

impl FileLockManager {
    pub fn new() -> Self {
        Self {
            locks_by_inode: Arc::new(DashMap::new()),
            locks_by_session: Arc::new(DashMap::new()),
            locks: Arc::new(DashMap::new()),
            next_lock_id: Arc::new(AtomicU64::new(1)),
            lock_mutex: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Inserts a lock into all tracking structures and returns the new lock ID.
    /// Must be called while holding the lock_mutex.
    fn insert_lock(&self, session_id: u64, lock: FileLock) -> LockId {
        let lock_id = LockId(self.next_lock_id.fetch_add(1, AtomicOrdering::SeqCst));
        let inode_id = lock.inode_id;
        self.locks.insert(lock_id, lock);
        self.locks_by_session
            .entry(session_id)
            .or_default()
            .push(lock_id);
        self.locks_by_inode
            .entry(inode_id)
            .or_default()
            .push(lock_id);
        lock_id
    }

    /// Removes a lock from all tracking structures.
    /// Must be called while holding the lock_mutex.
    fn remove_lock(&self, session_id: u64, lock_id: LockId) {
        if let Some((_, lock)) = self.locks.remove(&lock_id) {
            if let Some(mut session_locks) = self.locks_by_session.get_mut(&session_id) {
                session_locks.retain(|id| id != &lock_id);
            }
            if let Some(mut inode_locks) = self.locks_by_inode.get_mut(&lock.inode_id) {
                inode_locks.retain(|id| id != &lock_id);
            }
        }
    }

    /// Attempts to add a lock, returning the lock ID on success or `None` on conflict.
    pub async fn try_add_lock(&self, session_id: u64, lock: FileLock) -> Option<LockId> {
        let _guard = self.lock_mutex.lock().await;

        // First, remove any existing locks from this session that overlap
        // This implements POSIX lock replacement behavior
        let mut to_remove = Vec::new();
        if let Some(lock_ids) = self.locks_by_session.get(&session_id) {
            for lock_id in lock_ids.iter() {
                if let Some(existing_lock) = self.locks.get(lock_id)
                    && existing_lock.inode_id == lock.inode_id
                    && existing_lock.fid == lock.fid
                    && lock.start < existing_lock.end()
                    && lock.end() > existing_lock.start
                {
                    // Overlapping lock from same session - mark for removal
                    to_remove.push(*lock_id);
                }
            }
        }

        for lock_id in to_remove {
            self.remove_lock(session_id, lock_id);
        }

        if self.check_lock_conflict(lock.inode_id, &lock, session_id) {
            return None;
        }

        Some(self.insert_lock(session_id, lock))
    }

    pub async fn unlock_range(
        &self,
        inode_id: InodeId,
        fid: u32,
        start: u64,
        length: u64,
        session_id: u64,
    ) -> bool {
        let _guard = self.lock_mutex.lock().await;

        let unlock_end = if length == 0 {
            u64::MAX
        } else {
            start.saturating_add(length)
        };

        let mut locks_to_process = Vec::new();

        // Find locks that overlap with unlock range
        if let Some(lock_ids) = self.locks_by_session.get(&session_id) {
            for lock_id in lock_ids.iter() {
                if let Some(lock) = self.locks.get(lock_id)
                    && lock.inode_id == inode_id
                    && lock.fid == fid
                {
                    // Check if lock overlaps with unlock range
                    if lock.start < unlock_end && lock.end() > start {
                        locks_to_process.push((*lock_id, lock.clone()));
                    }
                }
            }
        }

        if locks_to_process.is_empty() {
            return false; // No locks to unlock
        }

        // Process each overlapping lock
        for (lock_id, existing_lock) in locks_to_process {
            let lock_end = existing_lock.end();

            // Remove the original lock
            self.remove_lock(session_id, lock_id);

            // Handle lock splitting if necessary
            if start > existing_lock.start && unlock_end < lock_end {
                // Create first part (before unlock range)
                let first_part = FileLock {
                    lock_type: existing_lock.lock_type,
                    start: existing_lock.start,
                    length: start - existing_lock.start,
                    proc_id: existing_lock.proc_id,
                    client_id: existing_lock.client_id.clone(),
                    fid: existing_lock.fid,
                    inode_id: existing_lock.inode_id,
                };
                self.insert_lock(session_id, first_part);

                // Create second part (after unlock range)
                let second_length = if existing_lock.length == 0 {
                    0 // Keep infinite length
                } else {
                    lock_end - unlock_end
                };

                let second_part = FileLock {
                    lock_type: existing_lock.lock_type,
                    start: unlock_end,
                    length: second_length,
                    proc_id: existing_lock.proc_id,
                    client_id: existing_lock.client_id.clone(),
                    fid: existing_lock.fid,
                    inode_id: existing_lock.inode_id,
                };
                self.insert_lock(session_id, second_part);
            } else if start <= existing_lock.start && unlock_end < lock_end {
                // Keep only the part after unlock range
                let new_length = if existing_lock.length == 0 {
                    0 // Keep infinite length
                } else {
                    lock_end - unlock_end
                };

                let new_lock = FileLock {
                    lock_type: existing_lock.lock_type,
                    start: unlock_end,
                    length: new_length,
                    proc_id: existing_lock.proc_id,
                    client_id: existing_lock.client_id,
                    fid: existing_lock.fid,
                    inode_id: existing_lock.inode_id,
                };
                self.insert_lock(session_id, new_lock);
            } else if start > existing_lock.start && unlock_end >= lock_end {
                // Keep only the part before unlock range
                let new_lock = FileLock {
                    lock_type: existing_lock.lock_type,
                    start: existing_lock.start,
                    length: start - existing_lock.start,
                    proc_id: existing_lock.proc_id,
                    client_id: existing_lock.client_id,
                    fid: existing_lock.fid,
                    inode_id: existing_lock.inode_id,
                };
                self.insert_lock(session_id, new_lock);
            }
        }

        true
    }

    fn check_lock_conflict(&self, inode_id: InodeId, new_lock: &FileLock, session_id: u64) -> bool {
        if let Some(lock_ids) = self.locks_by_inode.get(&inode_id) {
            for lock_id in lock_ids.iter() {
                if let Some(existing_lock) = self.locks.get(lock_id) {
                    // Skip locks from the same session - they will be replaced
                    if let Some(session_locks) = self.locks_by_session.get(&session_id)
                        && session_locks.contains(lock_id)
                    {
                        continue;
                    }

                    // Check if ranges overlap
                    if new_lock.start < existing_lock.end() && new_lock.end() > existing_lock.start
                    {
                        // Ranges overlap, check compatibility
                        match (new_lock.lock_type, existing_lock.lock_type) {
                            (LockType::ReadLock, LockType::ReadLock) => {
                                // Read locks are compatible
                                continue;
                            }
                            _ => {
                                // Write locks conflict with everything
                                return true;
                            }
                        }
                    }
                }
            }
        }
        false
    }

    pub async fn check_would_block(
        &self,
        inode_id: InodeId,
        test_lock: &FileLock,
        session_id: u64,
    ) -> Option<FileLock> {
        let _guard = self.lock_mutex.lock().await;
        if let Some(lock_ids) = self.locks_by_inode.get(&inode_id) {
            for lock_id in lock_ids.iter() {
                if let Some(existing_lock) = self.locks.get(lock_id) {
                    // Skip locks from the same session
                    if let Some(session_locks) = self.locks_by_session.get(&session_id)
                        && session_locks.contains(lock_id)
                    {
                        continue;
                    }

                    // Check if ranges overlap
                    if test_lock.start < existing_lock.end()
                        && test_lock.end() > existing_lock.start
                    {
                        // Ranges overlap, check compatibility
                        match (test_lock.lock_type, existing_lock.lock_type) {
                            (LockType::ReadLock, LockType::ReadLock) => {
                                // Read locks are compatible
                                continue;
                            }
                            _ => {
                                // Write locks conflict with everything
                                return Some(existing_lock.value().clone());
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub async fn release_session_locks(&self, session_id: u64) {
        let _guard = self.lock_mutex.lock().await;

        if let Some((_, lock_ids)) = self.locks_by_session.remove(&session_id) {
            for lock_id in lock_ids {
                if let Some((_, lock)) = self.locks.remove(&lock_id) {
                    // Remove from inode index
                    if let Some(mut inode_locks) = self.locks_by_inode.get_mut(&lock.inode_id) {
                        inode_locks.retain(|id| id != &lock_id);
                    }
                }
            }
        }
    }
}
