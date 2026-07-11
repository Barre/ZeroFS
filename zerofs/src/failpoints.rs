pub use fail::fail_point;

pub const WRITE_AFTER_EXTENT: &str = "write_after_extent";
pub const WRITE_AFTER_INODE: &str = "write_after_inode";
pub const WRITE_AFTER_COMMIT: &str = "write_after_commit";

pub const FALLOCATE_AFTER_EXTENTS: &str = "fallocate_after_extents";
pub const FALLOCATE_AFTER_INODE: &str = "fallocate_after_inode";
pub const FALLOCATE_AFTER_COMMIT: &str = "fallocate_after_commit";

pub const CREATE_AFTER_INODE: &str = "create_after_inode";
pub const CREATE_AFTER_DIR_ENTRY: &str = "create_after_dir_entry";
pub const CREATE_AFTER_COMMIT: &str = "create_after_commit";

pub const REMOVE_AFTER_INODE_DELETE: &str = "remove_after_inode_delete";
pub const REMOVE_AFTER_TOMBSTONE: &str = "remove_after_tombstone";
pub const REMOVE_AFTER_DIR_UNLINK: &str = "remove_after_dir_unlink";
pub const REMOVE_AFTER_COMMIT: &str = "remove_after_commit";
pub const REMOVE_AFTER_ORPHAN_ADD: &str = "remove_after_orphan_add";

pub const CLUNK_AFTER_RECLAIM_INODE_DELETE: &str = "clunk_after_reclaim_inode_delete";

/// `lock()`, between registering the lock and installing its guard. Pausing
/// here drives the Tlock-vs-Tversion/clunk race in tests.
pub const LOCK_AFTER_REGISTER_BEFORE_GUARD: &str = "lock_after_register_before_guard";

/// `reclaim_if_unreferenced`, before taking the inode lock. Lets a reopen win
/// the lock first, so reclaim's recheck must abort.
pub const RECLAIM_BEFORE_LOCK: &str = "reclaim_before_lock";

/// `reclaim_if_unreferenced`, after the recheck and holding the inode lock,
/// before the delete. A reopen racing in here blocks, then sees the inode gone.
pub const RECLAIM_HOLDING_LOCK_BEFORE_DELETE: &str = "reclaim_holding_lock_before_delete";

pub const RENAME_AFTER_TARGET_DELETE: &str = "rename_after_target_delete";
pub const RENAME_AFTER_SOURCE_UNLINK: &str = "rename_after_source_unlink";
pub const RENAME_AFTER_NEW_ENTRY: &str = "rename_after_new_entry";
pub const RENAME_AFTER_COMMIT: &str = "rename_after_commit";

pub const GC_AFTER_EXTENT_DELETE: &str = "gc_after_extent_delete";
pub const GC_AFTER_TOMBSTONE_UPDATE: &str = "gc_after_tombstone_update";

pub const LINK_AFTER_DIR_ENTRY: &str = "link_after_dir_entry";
pub const LINK_AFTER_INODE: &str = "link_after_inode";
pub const LINK_AFTER_COMMIT: &str = "link_after_commit";

pub const SYMLINK_AFTER_INODE: &str = "symlink_after_inode";
pub const SYMLINK_AFTER_DIR_ENTRY: &str = "symlink_after_dir_entry";
pub const SYMLINK_AFTER_COMMIT: &str = "symlink_after_commit";

pub const MKDIR_AFTER_INODE: &str = "mkdir_after_inode";
pub const MKDIR_AFTER_DIR_ENTRY: &str = "mkdir_after_dir_entry";
pub const MKDIR_AFTER_COMMIT: &str = "mkdir_after_commit";

pub const TRUNCATE_AFTER_EXTENTS: &str = "truncate_after_extents";
pub const TRUNCATE_AFTER_INODE: &str = "truncate_after_inode";
pub const TRUNCATE_AFTER_COMMIT: &str = "truncate_after_commit";

pub const MKNOD_AFTER_INODE: &str = "mknod_after_inode";
pub const MKNOD_AFTER_DIR_ENTRY: &str = "mknod_after_dir_entry";
pub const MKNOD_AFTER_COMMIT: &str = "mknod_after_commit";

pub const RMDIR_AFTER_INODE_DELETE: &str = "rmdir_after_inode_delete";
pub const RMDIR_AFTER_DIR_CLEANUP: &str = "rmdir_after_dir_cleanup";

pub const FLUSH_AFTER_COMPLETE: &str = "flush_after_complete";

// Data plane (extent-over-segments).

/// Flush path, after the open segment is sealed + PUT but before the metadata
/// manifest is made durable. A crash here must leave the recovered state
/// consistent: the just-PUT segment is orphaned (no durable FrameLoc references it,
/// so it is reclaimable), and never a durable FrameLoc pointing at a missing one.
pub const FLUSH_AFTER_SEAL_BEFORE_MANIFEST: &str = "flush_after_seal_before_manifest";

/// Compaction, after the packed segment is sealed + PUT but before any extent is
/// repointed to it. A crash here must keep every source frame readable (the repoint
/// never committed), so no relocated data is lost; the packed segment is orphaned.
pub const COMPACT_AFTER_SEAL_BEFORE_REPOINT: &str = "compact_after_seal_before_repoint";

/// Segment reclaim, after a dead segment's object is deleted but before its
/// `segcount` counter key is dropped. A crash here must not dangle (the segment was
/// directory-verified dead before deletion); the stale counter is a benign leak.
/// `reclaim_segments_gated`, after the durable barrier and gate, before the
/// segcount scan. A crash here lands between "everything committed so far is
/// durable" and "the pass acted on it".
pub const RECLAIM_AFTER_BARRIER_BEFORE_SCAN: &str = "reclaim_after_barrier_before_scan";

/// DST window widening: `(point, arming thread, virtual millis)`. The named
/// site awaits the delay via [`widen`], turning an otherwise awaitless window
/// into one concurrent commits can land in (the fail crate's own sleep blocks
/// the thread, which stalls a single-threaded virtual-time world instead of
/// widening anything). The thread id scopes the hook to the arming world.
#[allow(clippy::type_complexity)]
pub static WIDEN: std::sync::Mutex<Option<(&'static str, std::thread::ThreadId, u64)>> =
    std::sync::Mutex::new(None);

/// Await the configured widening delay at `point`, if armed for this thread.
pub async fn widen(point: &'static str) {
    let millis = {
        let slot = WIDEN.lock().unwrap();
        match &*slot {
            Some((p, thread, millis)) if *p == point && *thread == std::thread::current().id() => {
                Some(*millis)
            }
            _ => None,
        }
    };
    if let Some(millis) = millis {
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    }
}

/// `seal_and_repoint`, between one pack's per-inode repoint commits. A crash
/// here leaves the pack partially repointed: some inodes point at it, the
/// rest still point at their source segments; widening it maximizes the
/// overwrites the repoint CAS must reject.
pub const COMPACT_BETWEEN_REPOINTS: &str = "compact_between_repoints";

/// `reclaim_segments_gated`, after the directory verify and before the
/// irreversible object delete. Widening it tests the "no frame points here
/// is a permanent verdict" claim the delete rests on.
pub const RECLAIM_AFTER_VERIFY_BEFORE_DELETE: &str = "reclaim_after_verify_before_delete";

/// The coalesced run read, after the FrameLocs are resolved and before the
/// segment GET. Widening it lets a repoint+delete 404 the segment mid-read,
/// forcing the per-extent re-resolve fallback.
pub const READ_AFTER_RESOLVE_BEFORE_FETCH: &str = "read_after_resolve_before_fetch";

pub const RECLAIM_AFTER_SEGMENT_DELETE: &str = "reclaim_after_segment_delete";

/// Synchronous open-segment seal (the flush/fsync path), forcing the directory
/// seal to return an error (a non-crash failure, e.g. an AEAD seal error). The
/// open buffer and its already-committed FrameLocs must survive intact so a retry
/// can seal them, rather than being dropped into a dangling pointer. Return-style:
/// `fail::cfg(SEAL_OPEN_FAIL, "return")`.
pub const SEAL_OPEN_FAIL: &str = "seal_open_fail";
