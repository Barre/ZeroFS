//! Stable Rust entry points for target-kernel netfslib helpers and
//! layout-sensitive operations.
//!
//! Linux 6.18/6.19 expose the netfslib size fields directly. Newer kernels
//! make them private and publish them with acquire/release helpers. The small
//! C bridge is compiled against the exact target headers, delegates inode
//! initialization to the target helper, and publishes remote size and
//! zero-point changes with acquire/release semantics.

use kernel::bindings;

use super::abi;

#[allow(improper_ctypes)]
unsafe extern "C" {
    fn zerofs_netfs_initialize_inode(
        inode: *mut bindings::inode,
        ops: *const abi::netfs_request_ops,
    );
    fn zerofs_netfs_retain_writeback_group(
        request: *mut abi::netfs_io_request,
    ) -> *mut abi::netfs_group;
    fn zerofs_netfs_read_remote_size(inode: *const bindings::inode) -> bindings::loff_t;
    fn zerofs_netfs_write_remote_size(inode: *mut bindings::inode, size: bindings::loff_t);
    fn zerofs_netfs_extend_remote_size(inode: *mut bindings::inode, end: bindings::loff_t);
    fn zerofs_netfs_write_local_and_remote_size(
        inode: *mut bindings::inode,
        size: bindings::loff_t,
    );
}

/// Initialize the netfslib tail of a new, unpublished ZeroFS inode.
///
/// # Safety
///
/// `inode` must be a new, exclusively owned ZeroFS inode whose VFS inode and
/// mapping are already initialized. `ops` must remain live for the inode's
/// lifetime.
pub(crate) unsafe fn initialize_inode(
    inode: *mut bindings::inode,
    ops: *const abi::netfs_request_ops,
) {
    // SAFETY: Guaranteed by the caller.
    unsafe {
        zerofs_netfs_initialize_inode(inode, ops);
    }
}

/// Retain the group attached to the dirty folio currently selected by
/// netfslib for writeback.
///
/// # Safety
///
/// `request` must be a live writeback request passed to `begin_writeback`.
/// The returned pointer transfers one netfs group reference to the caller.
pub(crate) unsafe fn retain_writeback_group(
    request: *mut abi::netfs_io_request,
) -> *mut abi::netfs_group {
    // SAFETY: Guaranteed by the caller.
    unsafe { zerofs_netfs_retain_writeback_group(request) }
}

/// Read the server-published EOF with the target netfslib's semantics.
///
/// # Safety
///
/// `inode` must point to a live ZeroFS regular-file inode.
pub(crate) unsafe fn read_remote_size(inode: *mut bindings::inode) -> bindings::loff_t {
    // SAFETY: Guaranteed by the caller.
    unsafe { zerofs_netfs_read_remote_size(inode) }
}

/// Publish a server EOF with the target netfslib's synchronization semantics.
///
/// # Safety
///
/// `inode` must point to a live ZeroFS inode and `size` must be nonnegative.
pub(crate) unsafe fn write_remote_size(inode: *mut bindings::inode, size: bindings::loff_t) {
    // SAFETY: Guaranteed by the caller.
    unsafe {
        zerofs_netfs_write_remote_size(inode, size);
    }
}

/// Extend the server EOF monotonically with netfslib-compatible synchronization.
///
/// # Safety
///
/// `inode` must point to a live ZeroFS inode and `end` must be nonnegative.
pub(crate) unsafe fn extend_remote_size(inode: *mut bindings::inode, end: bindings::loff_t) {
    // SAFETY: Guaranteed by the caller.
    unsafe {
        zerofs_netfs_extend_remote_size(inode, end);
    }
}

/// Publish matching local and server EOFs after cache invalidation.
///
/// # Safety
///
/// `inode` must point to a live ZeroFS inode, `size` must be nonnegative, and
/// the caller must hold the inode/mapping exclusion required for an i_size
/// mutation.
pub(crate) unsafe fn write_local_and_remote_size(
    inode: *mut bindings::inode,
    size: bindings::loff_t,
) {
    // SAFETY: Guaranteed by the caller.
    unsafe {
        zerofs_netfs_write_local_and_remote_size(inode, size);
    }
}
