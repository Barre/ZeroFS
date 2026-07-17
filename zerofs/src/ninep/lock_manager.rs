use crate::fs::inode::InodeId;
use ninep_proto::LockType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

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

fn locks_conflict(requested: &FileLock, held: &FileLock) -> bool {
    requested.start < ninep_proto::lock_range_end(held.start, held.length)
        && ninep_proto::lock_range_end(requested.start, requested.length) > held.start
        && !matches!(
            (requested.lock_type, held.lock_type),
            (LockType::ReadLock, LockType::ReadLock)
        )
}

#[derive(Debug)]
struct HeldLock {
    session_id: u64,
    lock: FileLock,
}

fn subtract_matching_range(
    locks: &mut Vec<HeldLock>,
    start: u64,
    length: u64,
    mut matches: impl FnMut(&HeldLock) -> bool,
) -> bool {
    let end = ninep_proto::lock_range_end(start, length);
    let mut changed = false;
    let mut updated = Vec::with_capacity(locks.len());
    for held in locks.drain(..) {
        if matches(&held)
            && held.lock.start < end
            && ninep_proto::lock_range_end(held.lock.start, held.lock.length) > start
        {
            changed = true;
            let session_id = held.session_id;
            updated.extend(
                ninep_proto::subtract_lock_range(held.lock.start, held.lock.length, start, length)
                    .into_iter()
                    .map(|(start, length)| HeldLock {
                        session_id,
                        lock: FileLock {
                            start,
                            length,
                            ..held.lock.clone()
                        },
                    }),
            );
        } else {
            updated.push(held);
        }
    }
    *locks = updated;
    changed
}

#[derive(Debug, Clone, Default)]
pub struct FileLockManager {
    state: Arc<Mutex<HashMap<InodeId, Vec<HeldLock>>>>,
}

impl FileLockManager {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_state(&self) -> MutexGuard<'_, HashMap<InodeId, Vec<HeldLock>>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn session_has_locks(&self, session_id: u64) -> bool {
        self.lock_state()
            .values()
            .flatten()
            .any(|held| held.session_id == session_id)
    }

    /// Attempts to add a lock, returning whether it was granted.
    pub fn try_add_lock(&self, session_id: u64, lock: FileLock) -> bool {
        let mut state = self.lock_state();

        if state.get(&lock.inode_id).is_some_and(|locks| {
            locks
                .iter()
                .any(|held| held.session_id != session_id && locks_conflict(&lock, &held.lock))
        }) {
            return false;
        }

        let locks = state.entry(lock.inode_id).or_default();
        subtract_matching_range(locks, lock.start, lock.length, |held| {
            held.session_id == session_id && held.lock.fid == lock.fid
        });
        locks.push(HeldLock { session_id, lock });

        true
    }

    pub fn unlock_range(
        &self,
        inode_id: InodeId,
        fid: u32,
        start: u64,
        length: u64,
        session_id: u64,
    ) -> bool {
        let mut state = self.lock_state();
        let Some(locks) = state.get_mut(&inode_id) else {
            return false;
        };
        let changed = subtract_matching_range(locks, start, length, |held| {
            held.session_id == session_id && held.lock.fid == fid
        });
        if locks.is_empty() {
            state.remove(&inode_id);
        }
        changed
    }

    pub fn check_would_block(
        &self,
        inode_id: InodeId,
        test_lock: &FileLock,
        session_id: u64,
    ) -> Option<FileLock> {
        self.lock_state()
            .get(&inode_id)?
            .iter()
            .find(|held| held.session_id != session_id && locks_conflict(test_lock, &held.lock))
            .map(|held| held.lock.clone())
    }

    pub fn release_session_locks(&self, session_id: u64) {
        self.lock_state().retain(|_, locks| {
            locks.retain(|held| held.session_id != session_id);
            !locks.is_empty()
        });
    }

    /// Releases all locks held by a session fid.
    pub fn release_fid_locks(&self, session_id: u64, fid: u32) -> bool {
        let mut removed = false;
        self.lock_state().retain(|_, locks| {
            locks.retain(|held| {
                let matches = held.session_id == session_id && held.lock.fid == fid;
                removed |= matches;
                !matches
            });
            !locks.is_empty()
        });
        removed
    }
}

/// Releases a session fid's byte-range locks on drop.
#[derive(Debug)]
pub struct LockGuard {
    lock_manager: Arc<FileLockManager>,
    session_id: u64,
    fid: u32,
}

impl LockGuard {
    pub fn new(lock_manager: Arc<FileLockManager>, session_id: u64, fid: u32) -> Self {
        Self {
            lock_manager,
            session_id,
            fid,
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.lock_manager
            .release_fid_locks(self.session_id, self.fid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INODE: InodeId = 1;

    fn lock(lock_type: LockType, start: u64, length: u64) -> FileLock {
        lock_fid(lock_type, start, length, 0)
    }

    #[test]
    fn failed_upgrade_preserves_existing_lock() {
        let m = FileLockManager::new();

        assert!(m.try_add_lock(1, lock(LockType::ReadLock, 0, 10)));
        assert!(m.try_add_lock(2, lock(LockType::ReadLock, 0, 10)));

        assert!(
            !m.try_add_lock(1, lock(LockType::WriteLock, 0, 10)),
            "conflicting upgrade must be refused"
        );

        m.unlock_range(INODE, 0, 0, 10, 2);

        let conflict = m.check_would_block(INODE, &lock(LockType::WriteLock, 0, 10), 3);
        assert!(
            conflict.is_some(),
            "owner 1's read lock must survive the failed upgrade"
        );
    }

    fn lock_fid(lock_type: LockType, start: u64, length: u64, fid: u32) -> FileLock {
        FileLock {
            lock_type,
            start,
            length,
            proc_id: 0,
            client_id: Vec::new(),
            fid,
            inode_id: INODE,
        }
    }

    fn occupied(m: &FileLockManager, start: u64, length: u64) -> bool {
        m.check_would_block(INODE, &lock(LockType::WriteLock, start, length), 99)
            .is_some()
    }

    #[test]
    fn read_locks_share_but_writes_conflict() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::ReadLock, 0, 10)));
        assert!(
            m.try_add_lock(2, lock(LockType::ReadLock, 0, 10)),
            "read locks are compatible"
        );
        assert!(
            !m.try_add_lock(3, lock(LockType::WriteLock, 5, 2)),
            "a write conflicts with held reads"
        );
    }

    #[test]
    fn same_session_relock_replaces_in_place() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::ReadLock, 0, 10)));
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 10)));
        assert!(
            !m.try_add_lock(2, lock(LockType::ReadLock, 0, 10)),
            "the same-session read was replaced by a write"
        );
    }

    #[test]
    fn same_session_subrange_relock_preserves_both_old_tails() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 100)));
        assert!(m.try_add_lock(1, lock(LockType::ReadLock, 20, 30)));

        let mut shape: Vec<_> = m
            .lock_state()
            .get(&INODE)
            .unwrap()
            .iter()
            .filter(|held| held.session_id == 1)
            .map(|held| {
                (
                    held.lock.start,
                    held.lock.length,
                    match held.lock.lock_type {
                        LockType::ReadLock => "read",
                        LockType::WriteLock => "write",
                        LockType::Unlock => "unlock",
                    },
                )
            })
            .collect();
        shape.sort();
        assert_eq!(
            shape,
            vec![(0, 20, "write"), (20, 30, "read"), (50, 50, "write")]
        );

        assert!(
            m.check_would_block(INODE, &lock(LockType::ReadLock, 5, 1), 2)
                .is_some()
        );
        assert!(
            m.check_would_block(INODE, &lock(LockType::ReadLock, 25, 1), 2)
                .is_none()
        );
        assert!(
            m.check_would_block(INODE, &lock(LockType::WriteLock, 25, 1), 2)
                .is_some()
        );
        assert!(
            m.check_would_block(INODE, &lock(LockType::ReadLock, 75, 1), 2)
                .is_some()
        );
    }

    #[test]
    fn unlock_range_splits_a_lock_in_the_middle() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 30)));
        assert!(m.unlock_range(INODE, 0, 10, 10, 1));
        assert!(occupied(&m, 5, 1), "[0,10) stays locked");
        assert!(!occupied(&m, 12, 1), "[10,20) is freed");
        assert!(occupied(&m, 22, 1), "[20,30) stays locked");
    }

    #[test]
    fn unlock_range_trims_the_front() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 30)));
        m.unlock_range(INODE, 0, 0, 10, 1);
        assert!(!occupied(&m, 5, 1), "[0,10) freed");
        assert!(occupied(&m, 15, 1), "[10,30) kept");
    }

    #[test]
    fn unlock_range_trims_the_back() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 30)));
        m.unlock_range(INODE, 0, 20, 10, 1);
        assert!(occupied(&m, 5, 1), "[0,20) kept");
        assert!(!occupied(&m, 25, 1), "[20,30) freed");
    }

    #[test]
    fn unlock_range_removes_a_whole_lock_then_reports_nothing_left() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 10)));
        assert!(m.unlock_range(INODE, 0, 0, 10, 1));
        assert!(!occupied(&m, 0, 10), "fully unlocked");
        assert!(
            !m.unlock_range(INODE, 0, 0, 10, 1),
            "nothing left to unlock"
        );
    }

    #[test]
    fn check_would_block_returns_the_conflicting_lock() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::ReadLock, 0, 10)));
        let blocker = m
            .check_would_block(INODE, &lock(LockType::WriteLock, 5, 2), 2)
            .expect("a write over a held read must report the blocker");
        assert_eq!((blocker.start, blocker.length), (0, 10));
        assert!(matches!(blocker.lock_type, LockType::ReadLock));
        assert!(
            m.check_would_block(INODE, &lock(LockType::WriteLock, 5, 2), 1)
                .is_none(),
            "the holder sees no conflict with its own lock"
        );
    }

    #[test]
    fn release_session_locks_clears_all_of_a_sessions_locks() {
        let m = FileLockManager::new();
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 0, 10)));
        assert!(m.try_add_lock(1, lock(LockType::WriteLock, 100, 10)));
        assert!(m.session_has_locks(1));

        m.release_session_locks(1);
        assert!(!m.session_has_locks(1));
        assert!(!occupied(&m, 0, 10) && !occupied(&m, 100, 10));
    }

    #[test]
    fn release_fid_locks_targets_one_fid_and_backs_the_lock_guard() {
        let m = Arc::new(FileLockManager::new());
        assert!(m.try_add_lock(1, lock_fid(LockType::WriteLock, 0, 10, 1)));
        assert!(m.try_add_lock(1, lock_fid(LockType::WriteLock, 100, 10, 2)));

        assert!(m.release_fid_locks(1, 1), "fid 1's lock released");
        assert!(!occupied(&m, 0, 10), "fid 1's range freed");
        assert!(occupied(&m, 100, 10), "fid 2's lock untouched");
        assert!(!m.release_fid_locks(1, 1), "nothing left for fid 1");

        // LockGuard frees the remaining fid on drop.
        drop(LockGuard::new(Arc::clone(&m), 1, 2));
        assert!(!occupied(&m, 100, 10), "drop released fid 2's lock");
        assert!(!m.session_has_locks(1));
    }
}
