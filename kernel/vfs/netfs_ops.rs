//! Netfs request callbacks and writeback-group coordination.

use core::{
    cmp,
    ffi::c_void,
    marker::PhantomData,
    ptr::{self, NonNull},
    sync::atomic::Ordering,
};

use kernel::{
    alloc::KBox,
    bindings,
    error::{
        Error, Result,
        code::{EBADF, EFAULT, EINVAL, EIO},
        from_result,
    },
    ffi, pr_err,
    sync::aref::ARef,
    types::{ForeignOwnable, ScopeGuard},
};

use crate::{
    netfs::{
        abi as netfs, compat as netfs_compat,
        io::{
            Origin as NetfsOrigin, PreparedRead, ReadSubrequest, RequestMut, RequestRef,
            WriteSubrequest,
        },
    },
    protocol,
};

use super::attributes::{
    expire_cached_data, invalidate_inode_attributes, monotonic_now_ns, protocol_error,
};
use super::{
    FileState, READ_REPLY_OVERHEAD, REQUEST_FILE_STATE, block_direct_io_excluded,
    io::{InodeRef, OpenFileRef},
};

pub(super) fn select_writeback_group(
    inode: &InodeRef<'_>,
    file_state: &FileState,
    coalesce_same_credentials: bool,
) -> Result<ARef<FileState>> {
    if !coalesce_same_credentials {
        return Ok(ARef::from(file_state));
    }
    Ok(inode.state()?.writeback_groups.lock().select(file_state))
}

/// Inode ordering retained while netfslib consumes an access=user group.
pub(super) struct GroupedBufferedWrite<'a> {
    inode: *mut bindings::inode,
    selected: ARef<FileState>,
    exclusive: bool,
    old_size: bindings::loff_t,
    _inode: PhantomData<&'a ()>,
}

impl GroupedBufferedWrite<'_> {
    /// Acquire inode ordering and select the access=user writeback group.
    ///
    /// Ordinary in-range writes downgrade to the shared lock expected by
    /// netfs. Append and writes starting beyond EOF retain the exclusive lock
    /// for atomic offset selection and EOF-tail cleanup.
    pub(super) fn acquire<'a>(
        inode: &'a InodeRef<'_>,
        file_state: &FileState,
        coalesce_same_credentials: bool,
        append: bool,
        position: bindings::loff_t,
    ) -> Result<GroupedBufferedWrite<'a>> {
        let inode_ptr = inode.as_ptr();
        // SAFETY: The typed inode borrow pins the embedded semaphore until the
        // returned guard is dropped.
        let semaphore = unsafe { ptr::addr_of_mut!((*inode_ptr).i_rwsem) };
        // SAFETY: The semaphore is embedded in the pinned inode above and is
        // not held by this task yet.
        let status = unsafe { bindings::down_write_killable(semaphore) };
        if status < 0 {
            return Err(Error::from_errno(status));
        }
        // Every fallible step below returns through this guard, so adding one
        // more cannot leak the exclusive lock. Nothing fallible runs after the
        // downgrade, so the guard never releases a lock it no longer holds
        // exclusively.
        let held = ScopeGuard::new_with_data(semaphore, |semaphore| {
            // SAFETY: down_write_killable above locked exactly this semaphore,
            // and the inode borrow keeps it live for this scope.
            unsafe { bindings::up_write(semaphore) }
        });

        // SAFETY: down_write_killable acquired this exact inode's i_rwsem and
        // the scope guard retains it across the transition.
        unsafe { block_direct_io_excluded(inode_ptr)? };
        let selected = select_writeback_group(inode, file_state, coalesce_same_credentials)?;
        let old_size = inode.size();
        let exclusive = append || position > old_size;
        if !exclusive {
            // SAFETY: This task holds the semaphore exclusively.
            unsafe {
                bindings::downgrade_write(semaphore);
            }
        }
        held.dismiss();
        Ok(GroupedBufferedWrite {
            inode: inode_ptr,
            selected,
            exclusive,
            old_size,
            _inode: PhantomData,
        })
    }

    pub(super) fn group_ptr(&self) -> *mut netfs::netfs_group {
        self.selected.group_ptr()
    }

    pub(super) fn zero_exposed_eof_tail(
        &self,
        inode: &InodeRef<'_>,
        write_start: bindings::loff_t,
    ) {
        if write_start <= self.old_size {
            return;
        }
        debug_assert!(self.exclusive);
        // SAFETY: An extending write retains the exclusive i_rwsem acquired
        // before old_size was sampled, and netfslib has already published the
        // new i_size before returning a positive byte count.
        unsafe {
            inode.zero_exposed_eof_tail(self.old_size, write_start);
        }
    }
}

impl Drop for GroupedBufferedWrite<'_> {
    fn drop(&mut self) {
        // SAFETY: acquire either retained the exclusive lock or downgraded it
        // to netfs's shared-write form, and this guard is the sole releaser.
        unsafe {
            if self.exclusive {
                bindings::up_write(ptr::addr_of_mut!((*self.inode).i_rwsem));
            } else {
                netfs::netfs_end_io_write(self.inode);
            }
        }
    }
}

pub(super) unsafe extern "C" fn zerofs_netfs_free_group(group: *mut netfs::netfs_group) {
    if group.is_null() {
        return;
    }
    // FileState.group is the first field.
    let file_state = group.cast::<FileState>();
    // SAFETY: The final group reference still owns the enclosing allocation.
    let file_state_ref = unsafe { &*file_state };
    if !file_state_ref.mount().teardown_started() {
        let _ = file_state_ref.mount().client.clunk(file_state_ref.fid());
    }
    // SAFETY: zerofs_open transferred exactly one KBox allocation to this
    // refcount. This callback runs exactly once at the zero transition.
    unsafe {
        drop(<KBox<FileState> as ForeignOwnable>::from_foreign(
            file_state.cast::<c_void>(),
        ));
    }
}

fn file_state_read_size(file_state: &FileState) -> u32 {
    let mount = file_state.mount();
    let protocol_limit = mount
        .client
        .negotiated_msize()
        .saturating_sub(READ_REPLY_OVERHEAD);
    if file_state.iounit() == 0 {
        protocol_limit
    } else {
        cmp::min(protocol_limit, file_state.iounit())
    }
}

fn file_state_write_size(file_state: &FileState) -> u32 {
    let mount = file_state.mount();
    let protocol_limit = protocol::max_write_payload(mount.client.negotiated_msize());
    if file_state.iounit() == 0 {
        protocol_limit
    } else {
        cmp::min(protocol_limit, file_state.iounit())
    }
}

pub(super) unsafe extern "C" fn zerofs_netfs_init_request(
    request: *mut netfs::netfs_io_request,
    file: *mut bindings::file,
) -> ffi::c_int {
    from_result(|| {
        let mut request = unsafe { RequestMut::from_raw(request) }.ok_or(EINVAL)?;
        // Writeback discovers the retained access=user group from the first
        // dirty folio in begin_writeback().
        if request.origin().is_writeback() {
            return Ok(0);
        }
        if file.is_null() {
            return Err(EBADF);
        }
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        let file_state_ref = file.state();
        let retained = ARef::from(file_state_ref);
        let read_size = file_state_read_size(file_state_ref);
        let write_size = file_state_write_size(file_state_ref);
        request.set_io_sizes(read_size, write_size);
        if matches!(
            request.origin(),
            NetfsOrigin::Writethrough | NetfsOrigin::UnbufferedWrite | NetfsOrigin::DioWrite
        ) {
            request.set_group(file_state_ref.group_ptr());
        }
        if read_size == 0 || write_size == 0 {
            return Err(errno!(EMSGSIZE));
        }
        if REQUEST_FILE_STATE.install(&mut request, retained).is_err() {
            return Err(EIO);
        }
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_netfs_free_request(request: *mut netfs::netfs_io_request) {
    let Some(mut request) = (unsafe { RequestMut::from_raw(request) }) else {
        return;
    };
    drop(REQUEST_FILE_STATE.take(&mut request));
}

pub(super) unsafe extern "C" fn zerofs_netfs_prepare_read(
    subrequest: *mut netfs::netfs_io_subrequest,
) -> ffi::c_int {
    from_result(|| {
        let mut subrequest = unsafe { PreparedRead::from_raw(subrequest) }.ok_or(EINVAL)?;
        let read_size = subrequest.request().ok_or(EIO)?.read_size();
        if read_size == 0 {
            return Err(EIO);
        }
        subrequest.cap_len(read_size as usize);
        Ok(0)
    })
}

/// Whether netfslib can have sibling subrequests of this request in flight.
fn read_subrequests_overlap(request: &RequestRef<'_>) -> bool {
    let origin = request.origin();
    origin == NetfsOrigin::Readahead
        || (origin.is_direct_read()
            && (request.has_iocb() || request.len() > u64::from(request.read_size())))
}

pub(crate) fn run_netfs_read_subrequest(subrequest: ReadSubrequest<'static>) {
    zerofs_netfs_issue_read_sync(subrequest);
}

pub(crate) fn run_netfs_write_subrequest(subrequest: WriteSubrequest<'static>) {
    zerofs_netfs_issue_write_sync(subrequest);
}

pub(super) unsafe extern "C" fn zerofs_netfs_issue_read(
    subrequest: *mut netfs::netfs_io_subrequest,
) {
    let Some(subrequest) = (unsafe { ReadSubrequest::from_raw(subrequest) }) else {
        return;
    };
    let overlapping = subrequest
        .request()
        .is_some_and(|request| read_subrequests_overlap(&request));
    if !overlapping {
        zerofs_netfs_issue_read_sync(subrequest);
        return;
    }

    // Transfer the issued subrequest to its embedded worker-safe work item.
    let subrequest_work = match subrequest.try_into_work() {
        Ok(work) => work,
        Err(subrequest) => {
            zerofs_netfs_issue_read_sync(subrequest);
            return;
        }
    };
    if !subrequest_work.queue() {
        // The work item was already pending, which cannot happen for a
        // subrequest netfslib issues once. The pending embedded worker owns and
        // terminates the subrequest, so do not terminate it here.
        pr_err!("read subrequest work item was already queued\n");
    }
}

fn zerofs_netfs_issue_read_sync(mut subrequest: ReadSubrequest<'_>) {
    let Some(request) = subrequest.request() else {
        subrequest.set_result(Err(EBADF));
        subrequest.terminate();
        return;
    };
    let origin = request.origin();
    let inode_size = request.inode_size();
    let remote_inode_size = request.remote_inode_size();
    let accessed =
        subrequest.with_request_private(&REQUEST_FILE_STATE, |file_state, subrequest| {
            let remaining = subrequest.remaining();
            let count = cmp::min(remaining, u32::MAX as usize) as u32;
            let position = subrequest.position();
            let result = if count == 0 {
                Ok(0usize)
            } else {
                let state = file_state.mount();
                let fid = file_state.fid();
                match subrequest.reply_destination() {
                    Some(mut destination) => {
                        state
                            .client
                            .read_into(fid, position, count, &mut destination)
                    }
                    None => match state.client.read(fid, position, count) {
                        Ok(payload) => {
                            let bytes = payload.as_slice();
                            let copied = subrequest.copy_to_iter(bytes);
                            if copied != bytes.len() {
                                Err(EFAULT)
                            } else {
                                Ok(copied)
                            }
                        }
                        Err(error) => Err(error),
                    },
                }
            };

            match result {
                Ok(transferred) => {
                    if transferred != 0 {
                        subrequest.add_transferred(transferred);
                        subrequest.mark_progress();
                    }
                    // A successful zero-length Tread is the protocol's authoritative
                    // EOF indication even if relaxed-consistency i_size has not
                    // observed a peer's recent truncate yet.
                    let end = position.saturating_add(transferred as u64);
                    if transferred == 0 || inode_size.is_some_and(|size| end >= size) {
                        subrequest.mark_eof();
                    }
                    if !origin.is_direct_read()
                        && remote_inode_size.is_some_and(|remote_size| end >= remote_size)
                    {
                        subrequest.mark_clear_tail();
                    }
                    subrequest.set_result(Ok(()));
                }
                Err(error) => subrequest.set_result(Err(error)),
            }
        });
    if accessed.is_none() {
        subrequest.set_result(Err(EBADF));
    }
    subrequest.terminate();
}

pub(super) unsafe extern "C" fn zerofs_netfs_begin_writeback(
    request: *mut netfs::netfs_io_request,
) {
    let raw_request = request;
    let Some(mut request) = (unsafe { RequestMut::from_raw(request) }) else {
        return;
    };
    // SAFETY: netfslib invokes this callback with the current writeback folio
    // locked. The helper transfers one reference from that folio's group.
    let group = unsafe { netfs_compat::retain_writeback_group(raw_request) };
    let Some(file_state) = NonNull::new(group.cast::<FileState>()) else {
        request.set_error(EBADF);
        return;
    };
    // FileState.group is the first field, so both pointers have the same
    // address and share the same refcount.
    let retained = unsafe { ARef::from_raw(file_state) };
    let file_state_ref: &FileState = &retained;
    let write_size = file_state_write_size(file_state_ref);
    let read_size = request.as_ref().read_size();
    let group = file_state_ref.group_ptr();
    if REQUEST_FILE_STATE.install(&mut request, retained).is_err() {
        request.set_error(EIO);
        return;
    }
    request.set_io_sizes(read_size, write_size);
    request.set_group(group);
    request.set_write_stream_available(write_size != 0);
}

pub(super) unsafe extern "C" fn zerofs_netfs_issue_write(
    subrequest: *mut netfs::netfs_io_subrequest,
) {
    let Some(subrequest) = (unsafe { WriteSubrequest::from_raw(subrequest) }) else {
        return;
    };
    let unbuffered = subrequest
        .request()
        .is_some_and(|request| request.origin().is_direct_write());
    if unbuffered {
        // Netfslib deliberately serializes direct-write chunks and waits for
        // each issue callback. Running inline avoids a workqueue handoff that
        // cannot add parallelism to this request.
        zerofs_netfs_issue_write_sync(subrequest);
        return;
    }

    // Returning from issue_write lets netfslib submit the next wsize-bounded
    // range while this tagged RPC waits for its reply.
    let subrequest_work = match subrequest.try_into_work() {
        Ok(work) => work,
        Err(subrequest) => {
            zerofs_netfs_issue_write_sync(subrequest);
            return;
        }
    };
    if !subrequest_work.queue() {
        // The work item was already pending, which cannot happen for a
        // subrequest netfslib issues once. The pending embedded worker owns and
        // terminates the subrequest, so do not terminate it here.
        pr_err!("write subrequest work item was already queued\n");
    }
}

fn zerofs_netfs_issue_write_sync(mut subrequest: WriteSubrequest<'_>) {
    let Some(request) = subrequest.request() else {
        subrequest.terminate(Err(EBADF));
        return;
    };
    let origin = request.origin();
    let inode = request.inode_ptr();
    // SAFETY: netfslib retains the request inode until this subrequest ends.
    let inode = unsafe { InodeRef::from_raw(inode) }.ok();

    let transferred = subrequest
        .with_request_private(&REQUEST_FILE_STATE, |file_state, subrequest| {
            let remaining = subrequest.remaining();
            if remaining == 0 {
                return Ok(0);
            }
            // The same iounit/msize clamp netfslib already applied through
            // wsize, kept here because the payload length is what the Twrite
            // header declares.
            let write_size = file_state_write_size(file_state);
            let limit = write_size as usize;
            if limit == 0 {
                return Err(errno!(EMSGSIZE));
            }
            let Some(payload) = subrequest.payload(cmp::min(remaining, limit)) else {
                return Err(EFAULT);
            };
            let declared = payload.len();

            let position = subrequest.position();
            let fid = file_state.fid();
            let result = file_state
                .mount()
                .client
                .write(fid, position, payload)
                .and_then(|acknowledged| {
                    if acknowledged <= declared {
                        Ok(acknowledged)
                    } else {
                        Err(protocol_error())
                    }
                });
            match result {
                // A non-empty Twrite that makes no progress cannot be retried
                // indefinitely: netfslib treats a zero short write as
                // NEED_RETRY, and the direct-write loop has no retry bound.
                Ok(0) => Err(EIO),
                Ok(acknowledged) => {
                    subrequest.mark_progress();
                    if let Some(inode) = inode.as_ref() {
                        inode.extend_remote_size(position.saturating_add(acknowledged as u64));
                        if origin.is_direct_write() {
                            // Unlike buffered writeback, netfslib does not
                            // invoke request_ops.post_modify for direct I/O.
                            netfs_post_modify(&inode);
                        }
                    }
                    Ok(acknowledged)
                }
                Err(error) => {
                    // A dispatched Twrite may have reached durable database
                    // apply even when interruption, disconnect, or a malformed
                    // reply prevents an acknowledgement. Do not leave the
                    // pre-write size/attributes or mapping baseline locally
                    // fresh in that ambiguous state.
                    if let Some(inode) = inode.as_ref() {
                        if let (Ok(mount), Ok(remote_inode)) = (inode.mount(), inode.remote_id()) {
                            mount.invalidate_object_hints(&[remote_inode]);
                        }
                        expire_cached_data(&inode);
                    }
                    Err(error)
                }
            }
        })
        .unwrap_or(Err(EBADF));
    subrequest.terminate(transferred);
}

pub(super) unsafe extern "C" fn zerofs_netfs_post_modify(inode: *mut bindings::inode) {
    let inode = match unsafe { InodeRef::from_raw(inode) } {
        Ok(inode) => inode,
        Err(_) => return,
    };
    netfs_post_modify(&inode);
}

fn netfs_post_modify(inode: &InodeRef<'_>) {
    // Netfslib calls this per write syscall, not per RPC. A data write changes
    // this file's attributes and nothing else, so every other directory entry's
    // snapshot stays usable and the mount-wide fence is deliberately skipped.
    if let (Ok(mount), Ok(remote_inode)) = (inode.mount(), inode.remote_id()) {
        mount.invalidate_object_hints(&[remote_inode]);
    }
    invalidate_inode_attributes(inode);
    if let Ok(state) = inode.state() {
        state
            .last_data_revalidate_ns
            .store(monotonic_now_ns(), Ordering::Release);
    }
}
