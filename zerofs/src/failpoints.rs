pub use fail::fail_point;

pub const WRITE_AFTER_CHUNK: &str = "write_after_chunk";
pub const WRITE_AFTER_INODE: &str = "write_after_inode";
pub const WRITE_AFTER_COMMIT: &str = "write_after_commit";

pub const CREATE_AFTER_INODE: &str = "create_after_inode";
pub const CREATE_AFTER_DIR_ENTRY: &str = "create_after_dir_entry";
pub const CREATE_AFTER_COMMIT: &str = "create_after_commit";

pub const REMOVE_AFTER_INODE_DELETE: &str = "remove_after_inode_delete";
pub const REMOVE_AFTER_TOMBSTONE: &str = "remove_after_tombstone";
pub const REMOVE_AFTER_DIR_UNLINK: &str = "remove_after_dir_unlink";
pub const REMOVE_AFTER_COMMIT: &str = "remove_after_commit";

pub const RENAME_AFTER_TARGET_DELETE: &str = "rename_after_target_delete";
pub const RENAME_AFTER_SOURCE_UNLINK: &str = "rename_after_source_unlink";
pub const RENAME_AFTER_NEW_ENTRY: &str = "rename_after_new_entry";
pub const RENAME_AFTER_COMMIT: &str = "rename_after_commit";

pub const GC_AFTER_CHUNK_DELETE: &str = "gc_after_chunk_delete";
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

pub const TRUNCATE_AFTER_CHUNKS: &str = "truncate_after_chunks";
pub const TRUNCATE_AFTER_INODE: &str = "truncate_after_inode";
pub const TRUNCATE_AFTER_COMMIT: &str = "truncate_after_commit";

pub const MKNOD_AFTER_INODE: &str = "mknod_after_inode";
pub const MKNOD_AFTER_DIR_ENTRY: &str = "mknod_after_dir_entry";
pub const MKNOD_AFTER_COMMIT: &str = "mknod_after_commit";

pub const RMDIR_AFTER_INODE_DELETE: &str = "rmdir_after_inode_delete";
pub const RMDIR_AFTER_DIR_CLEANUP: &str = "rmdir_after_dir_cleanup";

pub const FLUSH_AFTER_COMPLETE: &str = "flush_after_complete";
