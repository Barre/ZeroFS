//! Failpoint-placed crashes: the timer killer can only land at instants with
//! virtual width, so awaitless windows are unreachable by it. An armed
//! failpoint callback isolates the store and fires the kill signal there.

use std::sync::Mutex;

pub(crate) const POINTS: &[&str] = &[
    zerofs::failpoints::FLUSH_AFTER_SEAL_BEFORE_MANIFEST,
    zerofs::failpoints::COMPACT_AFTER_SEAL_BEFORE_REPOINT,
    zerofs::failpoints::COMPACT_BETWEEN_REPOINTS,
    zerofs::failpoints::RECLAIM_AFTER_BARRIER_BEFORE_SCAN,
    zerofs::failpoints::RECLAIM_AFTER_SEGMENT_DELETE,
    // Foreground mutation commit sites. Each op is a single atomic
    // transaction (one write_coordinator.commit), so a crash placed at any
    // of these leaves either the whole op or none of it; the *_AFTER_INODE /
    // *_AFTER_DIR_ENTRY sites are pre-commit (op discarded), *_AFTER_COMMIT
    // is post-commit-pre-flush (op acked but only durable once fsynced).
    // The data and namespace prefix oracles cover every resulting state.
    zerofs::failpoints::WRITE_AFTER_EXTENT,
    zerofs::failpoints::WRITE_AFTER_INODE,
    zerofs::failpoints::WRITE_AFTER_COMMIT,
    zerofs::failpoints::TRUNCATE_AFTER_EXTENTS,
    zerofs::failpoints::TRUNCATE_AFTER_INODE,
    zerofs::failpoints::TRUNCATE_AFTER_COMMIT,
    zerofs::failpoints::CREATE_AFTER_INODE,
    zerofs::failpoints::CREATE_AFTER_DIR_ENTRY,
    zerofs::failpoints::CREATE_AFTER_COMMIT,
    zerofs::failpoints::MKDIR_AFTER_INODE,
    zerofs::failpoints::MKDIR_AFTER_DIR_ENTRY,
    zerofs::failpoints::MKDIR_AFTER_COMMIT,
    zerofs::failpoints::MKNOD_AFTER_INODE,
    zerofs::failpoints::MKNOD_AFTER_DIR_ENTRY,
    zerofs::failpoints::MKNOD_AFTER_COMMIT,
    zerofs::failpoints::SYMLINK_AFTER_INODE,
    zerofs::failpoints::SYMLINK_AFTER_DIR_ENTRY,
    zerofs::failpoints::SYMLINK_AFTER_COMMIT,
    zerofs::failpoints::LINK_AFTER_DIR_ENTRY,
    zerofs::failpoints::LINK_AFTER_INODE,
    zerofs::failpoints::LINK_AFTER_COMMIT,
    zerofs::failpoints::REMOVE_AFTER_INODE_DELETE,
    zerofs::failpoints::REMOVE_AFTER_TOMBSTONE,
    zerofs::failpoints::REMOVE_AFTER_DIR_UNLINK,
    zerofs::failpoints::REMOVE_AFTER_COMMIT,
    zerofs::failpoints::RMDIR_AFTER_INODE_DELETE,
    zerofs::failpoints::RMDIR_AFTER_DIR_CLEANUP,
    zerofs::failpoints::RENAME_AFTER_TARGET_DELETE,
    zerofs::failpoints::RENAME_AFTER_SOURCE_UNLINK,
    zerofs::failpoints::RENAME_AFTER_NEW_ENTRY,
    zerofs::failpoints::RENAME_AFTER_COMMIT,
    // Open-unlink lifecycle: `remove` defers (orphan add), then the close
    // reclaims. A crash between the two must leave the orphan for the
    // startup drain; a crash inside the reclaim must leave a re-reclaimable
    // state. Only the namespace open-unlink op reaches these.
    zerofs::failpoints::REMOVE_AFTER_ORPHAN_ADD,
    zerofs::failpoints::RECLAIM_BEFORE_LOCK,
    zerofs::failpoints::RECLAIM_HOLDING_LOCK_BEFORE_DELETE,
    zerofs::failpoints::CLUNK_AFTER_RECLAIM_INODE_DELETE,
];

/// Sites that await a configured delay, stretching a read-decide-act gap
/// so concurrent commits can land inside it (see zerofs::failpoints::widen).
pub(crate) const WIDEN_POINTS: &[&str] = &[
    zerofs::failpoints::RECLAIM_AFTER_BARRIER_BEFORE_SCAN,
    zerofs::failpoints::RECLAIM_AFTER_VERIFY_BEFORE_DELETE,
    zerofs::failpoints::COMPACT_BETWEEN_REPOINTS,
    zerofs::failpoints::READ_AFTER_RESOLVE_BEFORE_FETCH,
];

pub(crate) struct Armed {
    pub(crate) point: &'static str,
    /// Worlds are single-threaded, so the thread id scopes the hook to
    /// the armed world; parallel tests in this binary cannot cross-fire.
    pub(crate) thread: std::thread::ThreadId,
    pub(crate) hits_left: u32,
    pub(crate) fire: Box<dyn Fn() + Send>,
}

pub(crate) static ARMED: Mutex<Option<Armed>> = Mutex::new(None);

/// Register the pass-through callbacks once per process; arming happens
/// per round via `ARMED`.
pub(crate) fn register_callbacks() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        for point in POINTS {
            fail::cfg_callback(*point, move || trip(point)).expect("cfg_callback");
        }
    });
}

fn trip(point: &'static str) {
    let mut slot = ARMED.lock().unwrap();
    let Some(armed) = slot.as_mut() else {
        return;
    };
    if armed.point != point || armed.thread != std::thread::current().id() {
        return;
    }
    armed.hits_left -= 1;
    if armed.hits_left == 0 {
        (armed.fire)();
        *slot = None;
    }
}
