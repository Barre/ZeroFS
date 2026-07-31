/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Build-time layout constants for VFS types that the canonical Rust bindings
 * intentionally leave opaque. This header contributes no code to the module.
 */

#include <linux/filelock.h>
#include <linux/fs.h>
#include <linux/statfs.h>

enum {
	ZEROFS_I_NEW = I_NEW,
	ZEROFS_INODE_I_STATE_SIZE =
		sizeof(((struct inode *)0)->i_state),
	ZEROFS_INODE_I_STATE_ALIGN =
		__alignof__(((struct inode *)0)->i_state),

	ZEROFS_FILE_LOCK_SIZE = sizeof(struct file_lock),
	ZEROFS_FILE_LOCK_ALIGN = __alignof__(struct file_lock),
	ZEROFS_FILE_LOCK_CORE_SIZE = sizeof(struct file_lock_core),
	ZEROFS_FILE_LOCK_CORE_ALIGN = __alignof__(struct file_lock_core),
	ZEROFS_FILE_LOCK_CORE_OFFSET =
		__builtin_offsetof(struct file_lock, c),
	ZEROFS_FILE_LOCK_FLC_FLAGS_OFFSET =
		__builtin_offsetof(struct file_lock, c.flc_flags),
	ZEROFS_FILE_LOCK_FLC_TYPE_OFFSET =
		__builtin_offsetof(struct file_lock, c.flc_type),
	ZEROFS_FILE_LOCK_FLC_PID_OFFSET =
		__builtin_offsetof(struct file_lock, c.flc_pid),
	ZEROFS_FILE_LOCK_FL_START_OFFSET =
		__builtin_offsetof(struct file_lock, fl_start),
	ZEROFS_FILE_LOCK_FL_END_OFFSET =
		__builtin_offsetof(struct file_lock, fl_end),
	ZEROFS_FILE_LOCK_FLC_FLAGS_SIZE =
		sizeof(((struct file_lock *)0)->c.flc_flags),
	ZEROFS_FILE_LOCK_FLC_TYPE_SIZE =
		sizeof(((struct file_lock *)0)->c.flc_type),
	ZEROFS_FILE_LOCK_FLC_PID_SIZE =
		sizeof(((struct file_lock *)0)->c.flc_pid),
	ZEROFS_FILE_LOCK_FL_START_SIZE =
		sizeof(((struct file_lock *)0)->fl_start),
	ZEROFS_FILE_LOCK_FL_END_SIZE =
		sizeof(((struct file_lock *)0)->fl_end),
	ZEROFS_FL_FLOCK = FL_FLOCK,
	ZEROFS_FL_CLOSE = FL_CLOSE,
	ZEROFS_POSIX_TEST_LOCK_SIGNATURE =
		__builtin_types_compatible_p(
			__typeof__(&posix_test_lock),
			void (*)(struct file *, struct file_lock *)),
	ZEROFS_LOCKS_LOCK_INODE_WAIT_SIGNATURE =
		__builtin_types_compatible_p(
			__typeof__(&locks_lock_inode_wait),
			int (*)(struct inode *, struct file_lock *)),

	ZEROFS_KSTATFS_SIZE = sizeof(struct kstatfs),
	ZEROFS_KSTATFS_ALIGN = __alignof__(struct kstatfs),
	ZEROFS_KSTATFS_F_TYPE_OFFSET =
		__builtin_offsetof(struct kstatfs, f_type),
	ZEROFS_KSTATFS_F_BSIZE_OFFSET =
		__builtin_offsetof(struct kstatfs, f_bsize),
	ZEROFS_KSTATFS_F_BLOCKS_OFFSET =
		__builtin_offsetof(struct kstatfs, f_blocks),
	ZEROFS_KSTATFS_F_BFREE_OFFSET =
		__builtin_offsetof(struct kstatfs, f_bfree),
	ZEROFS_KSTATFS_F_BAVAIL_OFFSET =
		__builtin_offsetof(struct kstatfs, f_bavail),
	ZEROFS_KSTATFS_F_FILES_OFFSET =
		__builtin_offsetof(struct kstatfs, f_files),
	ZEROFS_KSTATFS_F_FFREE_OFFSET =
		__builtin_offsetof(struct kstatfs, f_ffree),
	ZEROFS_KSTATFS_F_FSID_OFFSET =
		__builtin_offsetof(struct kstatfs, f_fsid),
	ZEROFS_KSTATFS_F_NAMELEN_OFFSET =
		__builtin_offsetof(struct kstatfs, f_namelen),
	ZEROFS_KSTATFS_F_FRSIZE_OFFSET =
		__builtin_offsetof(struct kstatfs, f_frsize),
	ZEROFS_KSTATFS_F_FLAGS_OFFSET =
		__builtin_offsetof(struct kstatfs, f_flags),
	ZEROFS_KSTATFS_F_SPARE_OFFSET =
		__builtin_offsetof(struct kstatfs, f_spare),
	ZEROFS_KSTATFS_F_TYPE_SIZE =
		sizeof(((struct kstatfs *)0)->f_type),
	ZEROFS_KSTATFS_F_BSIZE_SIZE =
		sizeof(((struct kstatfs *)0)->f_bsize),
	ZEROFS_KSTATFS_F_BLOCKS_SIZE =
		sizeof(((struct kstatfs *)0)->f_blocks),
	ZEROFS_KSTATFS_F_BFREE_SIZE =
		sizeof(((struct kstatfs *)0)->f_bfree),
	ZEROFS_KSTATFS_F_BAVAIL_SIZE =
		sizeof(((struct kstatfs *)0)->f_bavail),
	ZEROFS_KSTATFS_F_FILES_SIZE =
		sizeof(((struct kstatfs *)0)->f_files),
	ZEROFS_KSTATFS_F_FFREE_SIZE =
		sizeof(((struct kstatfs *)0)->f_ffree),
	ZEROFS_KSTATFS_F_FSID_SIZE =
		sizeof(((struct kstatfs *)0)->f_fsid),
	ZEROFS_KSTATFS_F_NAMELEN_SIZE =
		sizeof(((struct kstatfs *)0)->f_namelen),
	ZEROFS_KSTATFS_F_FRSIZE_SIZE =
		sizeof(((struct kstatfs *)0)->f_frsize),
	ZEROFS_KSTATFS_F_FLAGS_SIZE =
		sizeof(((struct kstatfs *)0)->f_flags),
	ZEROFS_KSTATFS_F_SPARE_SIZE =
		sizeof(((struct kstatfs *)0)->f_spare),
};
