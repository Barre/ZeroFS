//! Stable Rust entry points for target-kernel VFS helpers and
//! layout-sensitive operations.

use kernel::bindings;

#[allow(improper_ctypes)]
unsafe extern "C" {
    fn zerofs_vfs_file_accessed(file: *mut bindings::file);
    fn zerofs_vfs_inode_slab_flags() -> bindings::slab_flags_t;
    fn zerofs_vfs_make_kuid(
        namespace: *mut bindings::user_namespace,
        uid: bindings::uid_t,
    ) -> bindings::kuid_t;
    fn zerofs_vfs_make_kgid(
        namespace: *mut bindings::user_namespace,
        gid: bindings::gid_t,
    ) -> bindings::kgid_t;
    fn zerofs_vfs_from_kuid(
        namespace: *mut bindings::user_namespace,
        uid: bindings::kuid_t,
    ) -> bindings::uid_t;
    fn zerofs_vfs_from_kgid(
        namespace: *mut bindings::user_namespace,
        gid: bindings::kgid_t,
    ) -> bindings::gid_t;
    fn zerofs_vfs_zero_exposed_eof_tail(
        inode: *mut bindings::inode,
        from: bindings::loff_t,
        to: bindings::loff_t,
    );
    fn zerofs_vfs_filemap_fault_after_revalidation(
        fault: *mut bindings::vm_fault,
    ) -> bindings::vm_fault_t;
    fn zerofs_vfs_pin_fault_file_and_unlock(fault: *mut bindings::vm_fault) -> *mut bindings::file;
    fn zerofs_vfs_iov_iter_count(iter: *const bindings::iov_iter) -> usize;
    fn zerofs_vfs_iov_iter_truncate(iter: *mut bindings::iov_iter, count: usize);
    fn zerofs_vfs_release_pinned_iov_iter(iter: *mut bindings::iov_iter, dirty_bytes: usize);
}

/// Return the target configuration's flags for the inode cache.
pub(crate) fn inode_slab_flags() -> bindings::slab_flags_t {
    // SAFETY: The helper only evaluates target-kernel slab flag macros.
    unsafe { zerofs_vfs_inode_slab_flags() }
}

/// Map a userspace UID through a live user namespace.
///
/// # Safety
///
/// `namespace` must remain live for the call.
pub(crate) unsafe fn make_kuid(
    namespace: *mut bindings::user_namespace,
    uid: bindings::uid_t,
) -> bindings::kuid_t {
    unsafe { zerofs_vfs_make_kuid(namespace, uid) }
}

/// Map a userspace GID through a live user namespace.
///
/// # Safety
///
/// `namespace` must remain live for the call.
pub(crate) unsafe fn make_kgid(
    namespace: *mut bindings::user_namespace,
    gid: bindings::gid_t,
) -> bindings::kgid_t {
    unsafe { zerofs_vfs_make_kgid(namespace, gid) }
}

/// Map an internal UID into a live user namespace.
///
/// # Safety
///
/// `namespace` must remain live for the call.
pub(crate) unsafe fn from_kuid(
    namespace: *mut bindings::user_namespace,
    uid: bindings::kuid_t,
) -> bindings::uid_t {
    unsafe { zerofs_vfs_from_kuid(namespace, uid) }
}

/// Map an internal GID into a live user namespace.
///
/// # Safety
///
/// `namespace` must remain live for the call.
pub(crate) unsafe fn from_kgid(
    namespace: *mut bindings::user_namespace,
    gid: bindings::kgid_t,
) -> bindings::gid_t {
    unsafe { zerofs_vfs_from_kgid(namespace, gid) }
}

/// Apply the normal read-atime update to a live file.
///
/// # Safety
///
/// `file` must remain live for the call.
pub(crate) unsafe fn file_accessed(file: *mut bindings::file) {
    unsafe { zerofs_vfs_file_accessed(file) };
}

/// Clear dirty mmap data that a write starting beyond the old EOF would expose.
///
/// # Safety
///
/// `inode` must remain live, its `i_size` must already cover `to`, and the
/// caller must hold `i_rwsem` exclusively from before the size extension.
pub(crate) unsafe fn zero_exposed_eof_tail(
    inode: *mut bindings::inode,
    from: bindings::loff_t,
    to: bindings::loff_t,
) {
    unsafe { zerofs_vfs_zero_exposed_eof_tail(inode, from, to) };
}

/// Run generic filemap fault handling with a fresh lock-dropping retry budget.
///
/// # Safety
///
/// `fault` must be the live state for a fault whose ZeroFS revalidation gate
/// already caused the MM retry represented by `FAULT_FLAG_TRIED`.
pub(crate) unsafe fn filemap_fault_after_revalidation(
    fault: *mut bindings::vm_fault,
) -> bindings::vm_fault_t {
    unsafe { zerofs_vfs_filemap_fault_after_revalidation(fault) }
}

/// Pin the mapped file, release the mmap or per-VMA fault lock, and transfer
/// the file reference to the caller.
///
/// # Safety
///
/// `fault` must be a live filesystem fault callback whose flags include
/// `FAULT_FLAG_ALLOW_RETRY` and exclude `FAULT_FLAG_RETRY_NOWAIT`. The caller
/// must consume the returned reference with `fput`.
pub(crate) unsafe fn pin_fault_file_and_unlock(
    fault: *mut bindings::vm_fault,
) -> *mut bindings::file {
    // SAFETY: Guaranteed by the caller.
    unsafe { zerofs_vfs_pin_fault_file_and_unlock(fault) }
}

/// Return the remaining byte count of a live iterator.
///
/// # Safety
///
/// `iter` must point to a live `iov_iter`.
pub(crate) unsafe fn iov_iter_count(iter: *const bindings::iov_iter) -> usize {
    unsafe { zerofs_vfs_iov_iter_count(iter) }
}

/// Limit a live iterator to at most `count` bytes.
///
/// # Safety
///
/// `iter` must point to a live, exclusively borrowed `iov_iter`.
pub(crate) unsafe fn iov_iter_truncate(iter: *mut bindings::iov_iter, count: usize) {
    unsafe { zerofs_vfs_iov_iter_truncate(iter, count) };
}

/// Release the bvec allocation and page pins produced by
/// `netfs_extract_user_iter`.
///
/// # Safety
///
/// `iter` must be the initialized output of one successful extraction and
/// must no longer be reachable by netfslib.
pub(crate) unsafe fn release_pinned_iov_iter(iter: *mut bindings::iov_iter, dirty_bytes: usize) {
    unsafe { zerofs_vfs_release_pinned_iov_iter(iter, dirty_bytes) };
}
