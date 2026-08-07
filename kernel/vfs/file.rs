//! Regular-file operations, mmap integration, and data-cache coherency.

use core::{ffi::c_void, ptr, sync::atomic::Ordering};

use kernel::{
    alloc::flags::GFP_KERNEL,
    bindings,
    error::{
        Result,
        code::{EACCES, EAGAIN, EBUSY, EFBIG, EINVAL, EIO, EISDIR, ENOMEM},
        from_result, to_result,
    },
    ffi,
    sync::Arc,
    types::{ForeignOwnable, ScopeGuard},
};

use crate::{
    client::RebindCredentials,
    protocol::{self, QID_TYPE_DIR, QID_TYPE_FILE, Stat},
};

use super::{
    AttributeRefresh, CacheObservation, DrainedCoherentMappingGuard, FILE_VM_OPERATIONS,
    FMODE_WRITE, FileState, IOCB_APPEND, IOCB_DSYNC, IOCB_NOWAIT, IOCB_SYNC, InodeState,
    IoExcludedInodeGuard, MMAP_INVALIDATION_RETRIES, MmapRefresh, MmapRevalidation, MountState,
    OPEN_PREFETCH_WINDOW, OpenPrefetchSeed, RELAXED_CACHE_REVALIDATE_NS, RevalidateState,
    RevalidateStatus,
    attributes::{
        cache_data_observation, cache_metadata_observation, expire_cached_attributes_at,
        expire_cached_data_at, from_fault_result, monotonic_now_ns, protocol_error,
        publish_coherent_data_stat_at, publish_mutation_stat, stat_matches_inode,
        store_data_cache_baseline, validate_stat, validate_stat_for_inode,
    },
    io::{
        FileOpenRef, FileReleaseRef, InodeRef, MappedFileFault, MmapAreaRef, OpenFileRef, ReadCall,
        WriteCall,
    },
    netfs_ops::{GroupedBufferedWrite, select_writeback_group},
    new_file_state,
    remote::open_inode,
    remote_fsync_locked,
};

pub(super) unsafe extern "C" fn zerofs_open(
    inode: *mut bindings::inode,
    file: *mut bindings::file,
) -> ffi::c_int {
    from_result(|| {
        if inode.is_null() || file.is_null() {
            return Err(EINVAL);
        }
        // SAFETY: VFS retains the inode/file pair throughout open.
        let inode_ref = unsafe { InodeRef::from_raw(inode) }?;
        // SAFETY: VFS supplies this live inode/file pair. The generic check
        // applies the non-large-file size rule before the remote open is
        // published.
        to_result(unsafe { bindings::generic_file_open(inode, file) })?;

        // SAFETY: VFS supplies exclusive access to the still-unpublished file.
        let file = unsafe { FileOpenRef::from_raw(file) }?;
        let state = inode_ref.mount()?;
        let mut open_flags = file.flags() & bindings::O_ACCMODE;
        let direct = file.flags() & bindings::O_DIRECT != 0;
        let expected_qid_type = match inode_ref.file_type() {
            bindings::S_IFDIR => QID_TYPE_DIR,
            bindings::S_IFREG => QID_TYPE_FILE,
            _ => return Err(errno!(EOPNOTSUPP)),
        };
        // Start the TTL before eligibility and the RPC. Content validity uses
        // the generation captured under the attribute-cache lock below.
        let prefetch_observed_ns = monotonic_now_ns();
        let prefetch_content_generation =
            if expected_qid_type == QID_TYPE_FILE && state.is_relaxed() {
                let inode_state = inode_ref.state()?;
                let cached = inode_state.cached_attributes.lock();
                (mapped_data_is_fresh(inode_state) && cached.data_valid)
                    .then_some(cached.content_generation)
            } else {
                None
            };
        let prefetch_count = if prefetch_content_generation.is_some()
            && open_flags == bindings::O_RDONLY
            && !direct
            && file.flags() & bindings::O_TRUNC == 0
        {
            OPEN_PREFETCH_WINDOW.min(protocol::max_lopenatread_payload(
                state.client.negotiated_msize(),
            ))
        } else {
            0
        };
        let mut force_unbuffered = expected_qid_type == QID_TYPE_FILE
            && (!state.is_relaxed() || (direct && open_flags == bindings::O_WRONLY));
        // Buffered partial-folio writes may need to fetch the untouched bytes.
        // This is the same writeback-cache upgrade used by the FUSE client and
        // v9fs: userspace still has a write-only fd, while the retained
        // backing capability is read/write for kernel read-modify-write.
        let upgraded_for_writeback = expected_qid_type == QID_TYPE_FILE
            && !force_unbuffered
            && open_flags == bindings::O_WRONLY;
        if upgraded_for_writeback {
            open_flags = bindings::O_RDWR;
        }
        // struct file owns a reference to this immutable credential for its
        // entire lifetime; copy it before the blocking remote open.
        let credentials = RebindCredentials::from_credential(file.credential())?;
        let opened = open_inode(
            &state.client,
            &inode_ref,
            &credentials,
            expected_qid_type,
            open_flags,
            prefetch_count,
            prefetch_observed_ns,
            prefetch_content_generation.unwrap_or(0),
        );
        let opened = match opened {
            // The upgrade asks the server for read access userspace never
            // requested, which a write-only file (mode 0200) denies. Fall back
            // to the capability that was actually asked for and keep the
            // descriptor unbuffered, exactly as an O_WRONLY|O_DIRECT open is.
            Err(error) if upgraded_for_writeback && error == EACCES => {
                force_unbuffered = true;
                open_inode(
                    &state.client,
                    &inode_ref,
                    &credentials,
                    expected_qid_type,
                    bindings::O_WRONLY,
                    0,
                    prefetch_observed_ns,
                    0,
                )?
            }
            other => other?,
        };
        // Clunk the fid unless the file publishes it.
        let fid = ScopeGuard::new_with_data(opened.fid, |fid| {
            let _ = state.client.clunk(fid);
        });
        let prefetch =
            opened
                .prefetch
                .as_ref()
                .and_then(|(observed_ns, content_generation, payload)| {
                    let inode_state = inode_ref.state().ok()?;
                    let cached = inode_state.cached_attributes.lock();
                    (*content_generation == cached.content_generation).then_some(OpenPrefetchSeed {
                        observed_ns: *observed_ns,
                        content_generation: *content_generation,
                        data: payload.as_slice(),
                    })
                });
        let file_state = new_file_state(
            state,
            *fid,
            opened.iounit,
            force_unbuffered,
            credentials,
            prefetch,
        )?;

        // This is the successful open path's sole publication. release() drops
        // the file's reference; dirty folios and in-flight netfs requests may
        // retain the access=user capability beyond struct file lifetime.
        fid.dismiss();
        file.publish(file_state, false);
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_release(
    _inode: *mut bindings::inode,
    file: *mut bindings::file,
) -> ffi::c_int {
    // SAFETY: VFS invokes release once with exclusive final access.
    let Some(file) = (unsafe { FileReleaseRef::from_raw(file) }) else {
        return 0;
    };
    let Some(file_state) = file.take_state() else {
        return 0;
    };

    // The final netfs group reference clunks and frees the state. In
    // particular, dirty folios keep their write capability alive after close.
    drop(file_state);
    0
}

pub(super) unsafe extern "C" fn zerofs_flush(
    file: *mut bindings::file,
    _owner: bindings::fl_owner_t,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains this open file for the flush callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        // Like NFS and FUSE writeback-cache close, make data written through
        // this file visible to the server before close succeeds. This is only
        // an upload barrier; fsync remains the explicit ZeroFS durability
        // barrier.
        if file.mode() & FMODE_WRITE == 0 {
            return Ok(0);
        }
        let status = file.write_and_wait_range(0, bindings::loff_t::MAX);
        let writeback_error = file.take_writeback_error();
        to_result(status)?;
        to_result(writeback_error)?;
        Ok(0)
    })
}

fn mapped_data_is_fresh(inode_state: &InodeState) -> bool {
    monotonic_now_ns().wrapping_sub(inode_state.last_data_revalidate_ns.load(Ordering::Acquire))
        < RELAXED_CACHE_REVALIDATE_NS
}

/// Clone the reference stored in one live VMA's private-data slot.
///
/// # Safety
///
/// `private` must be null or a live `Arc<MmapRevalidation>` foreign pointer
/// retained by a VMA whose lifetime is protected by the caller.
unsafe fn clone_mmap_revalidation(private: *mut c_void) -> Option<Arc<MmapRevalidation>> {
    if private.is_null() {
        return None;
    }
    let borrowed = unsafe { <Arc<MmapRevalidation> as ForeignOwnable>::borrow(private) };
    Some(Arc::from(borrowed))
}

fn synchronize_mmap_revalidation(
    call: &MappedFileFault<'_>,
    gate: &MmapRevalidation,
) -> Result<RevalidateState> {
    let inode_state = call.file().inode().state()?;
    loop {
        let state = gate.load();
        // Sample freshness after the exact state under consideration. An old
        // fresh sample must not complete a newer pending generation after this
        // task was preempted.
        let fresh = mapped_data_is_fresh(inode_state);
        match (state.status().is_ready(), fresh) {
            (true, false) => {
                let pending = state.next_generation(RevalidateStatus::Pending);
                if gate.publish(state, pending) {
                    return Ok(pending);
                }
            }
            (false, true) => gate.complete(state),
            _ => return Ok(state),
        }
    }
}

pub(super) unsafe extern "C" fn zerofs_vma_open(area: *mut bindings::vm_area_struct) {
    if area.is_null() {
        return;
    }
    let private = unsafe { ptr::addr_of!((*area).vm_private_data).read() };
    let Some(gate) = (unsafe { clone_mmap_revalidation(private) }) else {
        return;
    };
    // Transfer the cloned reference to the copied/split VMA. Its private-data
    // pointer is already a byte-for-byte copy of the same Arc representation.
    let cloned = gate.into_foreign();
    debug_assert_eq!(cloned, private);
}

pub(super) unsafe extern "C" fn zerofs_vma_close(area: *mut bindings::vm_area_struct) {
    if area.is_null() {
        return;
    }
    let slot = unsafe { ptr::addr_of_mut!((*area).vm_private_data) };
    let private = unsafe { slot.read() };
    if private.is_null() {
        return;
    }
    unsafe {
        slot.write(ptr::null_mut());
        drop(<Arc<MmapRevalidation> as ForeignOwnable>::from_foreign(
            private,
        ));
    }
}

pub(super) unsafe extern "C" fn zerofs_mmap(
    file: *mut bindings::file,
    area: *mut bindings::vm_area_struct,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the mapped file for this callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        // SAFETY: VFS lends mmap exclusive VMA initialization access.
        let mut area = unsafe { MmapAreaRef::from_raw(area) }?;
        if !file.state().mount().is_relaxed() || file.state().force_unbuffered() {
            // Strict mappings would reuse the generic page cache without a
            // per-fault authority check. Unbuffered O_WRONLY descriptors also
            // lack the read capability needed for partial-folio
            // read-modify-write.
            return Err(errno!(EOPNOTSUPP));
        }
        let inode_state = file.inode().state()?;
        let status = if mapped_data_is_fresh(inode_state) {
            RevalidateStatus::Ready
        } else {
            RevalidateStatus::Pending
        };
        let gate = Arc::new(MmapRevalidation::new(status), GFP_KERNEL).map_err(|_| ENOMEM)?;
        // Each newly allocated VMA owns a coherency gate. Faults re-arm it
        // after the relaxed-consistency window expires, including ranges later
        // added by VMA merging or mremap.
        Ok(area.initialize_filemap(&file, FILE_VM_OPERATIONS.as_ptr(), gate))
    })
}

pub(super) unsafe extern "C" fn zerofs_filemap_fault(
    fault: *mut bindings::vm_fault,
) -> bindings::vm_fault_t {
    from_fault_result(|| {
        // SAFETY: VFS retains the fault, VMA, and mapped file for this
        // callback.
        let call = unsafe { MappedFileFault::from_raw(fault) }?;
        // SAFETY: The retained VMA owns the gate reference in its private slot.
        let gate = unsafe { clone_mmap_revalidation(call.private_data()) }.ok_or(EIO)?;
        let pending = loop {
            let state = synchronize_mmap_revalidation(&call, &gate)?;
            match state.status() {
                RevalidateStatus::Ready => return Ok(call.run_filemap_fault(false)),
                RevalidateStatus::Revalidated => return Ok(call.run_filemap_fault(true)),
                RevalidateStatus::Pending => break state,
                // Report the recorded failure once, opening a new generation so
                // a later independent fault retries instead of inheriting it.
                failed @ RevalidateStatus::Failed(_) => {
                    if gate.publish(state, state.next_generation(RevalidateStatus::Pending)) {
                        return Ok(failed.fault());
                    }
                }
            }
        };

        let flags = call.flags();
        if flags & bindings::fault_flag_FAULT_FLAG_RETRY_NOWAIT != 0 {
            // MM expects RETRY_NOWAIT handlers to retain the fault lock.
            return Ok(bindings::vm_fault_reason_VM_FAULT_RETRY);
        }
        if flags & bindings::fault_flag_FAULT_FLAG_ALLOW_RETRY == 0 {
            // Blocking here would invert mmap_lock against inode.i_rwsem.
            return Err(EIO);
        }
        // `gate` and the returned file reference are the only callback-derived
        // objects used after the compatibility helper drops the MM fault lock.
        let pinned = call.pin_file_and_unlock().ok_or(EIO)?;
        // Past this point the fault lock is gone, so every outcome is recorded
        // on the gate and reported as RETRY rather than as a fault reason.
        match pinned.open().and_then(|file| {
            let state = file.inode().mount()?;
            revalidate_mapped_file(file.inode(), file.state(), state)
        }) {
            Ok(MmapRefresh::Complete) => gate.complete(pending),
            Ok(MmapRefresh::Retry) => {}
            Err(error) => gate.fail(pending, error),
        }
        Ok(bindings::vm_fault_reason_VM_FAULT_RETRY)
    })
}

pub(super) unsafe extern "C" fn zerofs_filemap_map_pages(
    fault: *mut bindings::vm_fault,
    start: ffi::c_ulong,
    end: ffi::c_ulong,
) -> bindings::vm_fault_t {
    // SAFETY: VFS retains the fault, VMA, and mapped file for this callback.
    let Ok(mut call) = (unsafe { MappedFileFault::from_raw(fault) }) else {
        return 0;
    };
    // SAFETY: The retained VMA owns the gate reference in its private slot.
    let Some(gate) = (unsafe { clone_mmap_revalidation(call.private_data()) }) else {
        return 0;
    };
    // Speculative mapping only: anything short of a ready gate simply declines,
    // leaving the real fault path to revalidate.
    match synchronize_mmap_revalidation(&call, &gate) {
        Ok(state) if state.status().is_ready() => call.run_filemap_map_pages(start, end),
        _ => 0,
    }
}

pub(super) unsafe extern "C" fn zerofs_page_mkwrite(
    fault: *mut bindings::vm_fault,
) -> bindings::vm_fault_t {
    from_fault_result(|| {
        // SAFETY: VFS retains the fault, VMA, and mapped file for this callback.
        let call = unsafe { MappedFileFault::from_raw(fault) }?;
        // SAFETY: The retained VMA owns the gate reference in its private slot.
        let gate = unsafe { clone_mmap_revalidation(call.private_data()) }.ok_or(EIO)?;
        loop {
            let state = synchronize_mmap_revalidation(&call, &gate)?;
            match state.status() {
                RevalidateStatus::Ready | RevalidateStatus::Revalidated => break,
                RevalidateStatus::Pending => {
                    // Revoke the write-protected PTE and retry as a missing-page
                    // fault. That path can release the MM fault lock before
                    // blocking on inode revalidation, avoiding lock-order
                    // inversions.
                    call.unmap_file_page();
                    return Ok(bindings::vm_fault_reason_VM_FAULT_NOPAGE);
                }
                failed @ RevalidateStatus::Failed(_) => {
                    if gate.publish(state, state.next_generation(RevalidateStatus::Pending)) {
                        return Ok(failed.fault());
                    }
                }
            }
        }
        let selected = select_writeback_group(call.file().inode(), call.file().state(), true)?;
        let result = call.run_netfs_page_mkwrite(selected.group_ptr());
        Ok(if result & bindings::vm_fault_reason_VM_FAULT_RETRY != 0 {
            // do_page_mkwrite() does not treat RETRY as a lock-dropping result.
            bindings::vm_fault_reason_VM_FAULT_NOPAGE
        } else {
            result
        })
    })
}

pub(super) unsafe extern "C" fn zerofs_read_iter(
    iocb: *mut bindings::kiocb,
    destination: *mut bindings::iov_iter,
) -> isize {
    from_result(|| {
        // SAFETY: VFS retains the kiocb and its iterator for this callback.
        let mut call = unsafe { ReadCall::from_raw(iocb, destination) }?;
        zerofs_read_iter_inner(&mut call)
    })
}

pub(super) unsafe extern "C" fn zerofs_llseek(
    file: *mut bindings::file,
    offset: bindings::loff_t,
    whence: ffi::c_int,
) -> bindings::loff_t {
    // SEEK_END, SEEK_DATA and SEEK_HOLE all depend on the current remote EOF.
    // SEEK_SET and SEEK_CUR only manipulate the local file position.
    from_result(|| {
        if whence == bindings::SEEK_SET as ffi::c_int || whence == bindings::SEEK_CUR as ffi::c_int
        {
            // SAFETY: VFS retains the file for this callback.
            return Ok(unsafe { bindings::generic_file_llseek(file, offset, whence) });
        }
        // SAFETY: As above.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        let state = file.inode().mount()?;
        revalidate_cached_file(file.inode(), file.state(), state, false)?;
        // SAFETY: The revalidated file is the one VFS supplied.
        Ok(unsafe { bindings::generic_file_llseek(file.as_ptr(), offset, whence) })
    })
}

fn zerofs_read_iter_inner(call: &mut ReadCall<'_>) -> Result<isize> {
    let count = call.count();
    let ki_flags = call.flags();
    if call.position() < 0 {
        return Err(EINVAL);
    }
    call.state().clear_expired_prefetch(monotonic_now_ns());
    if count == 0 {
        return Ok(0);
    }
    if call.inode().file_type() != bindings::S_IFREG {
        return Err(EISDIR);
    }
    let direct = ki_flags & bindings::IOCB_DIRECT as ffi::c_int != 0;
    let unbuffered = direct || call.state().force_unbuffered();
    if unbuffered && ki_flags & bindings::IOCB_NOIO as ffi::c_int != 0 {
        // An unbuffered read necessarily issues server I/O.
        return Err(EAGAIN);
    }
    if unbuffered && ki_flags & (IOCB_NOWAIT | bindings::IOCB_WAITQ as ffi::c_int) != 0 {
        // Netfslib can avoid waiting on dirty cache state, but the ZeroFS
        // socket RPC itself is blocking and cannot honor RWF_NOWAIT. WAITQ is
        // the buffered-page wait mechanism rather than DIO completion.
        return Err(errno!(EOPNOTSUPP));
    }

    let state = call.inode().mount()?;
    revalidate_cached_file(
        call.inode(),
        call.state(),
        state,
        ki_flags
            & (IOCB_NOWAIT
                | bindings::IOCB_NOIO as ffi::c_int
                | bindings::IOCB_WAITQ as ffi::c_int)
            != 0,
    )?;
    let atime_before = call.inode().access_time();

    let size = call.inode().size();
    let readable = if size <= call.position() {
        0
    } else {
        count.min((size - call.position()) as usize)
    };
    if readable == 0 {
        // A nonempty read at EOF still performs the ordinary atime update,
        // without touching or validating the unused destination buffer.
        call.accessed();
        persist_read_access_time(call, state, atime_before);
        return Ok(0);
    }
    let pinned = call.pin_destination(readable)?;

    // Strict opens use netfslib's unbuffered path; relaxed opens retain the
    // existing page-cache and writeback behavior.
    let result = call.run_netfs(unbuffered, pinned);
    persist_read_access_time(call, state, atime_before);
    Ok(result)
}

fn persist_read_access_time(call: &ReadCall<'_>, state: &MountState, before: bindings::timespec64) {
    let after = call.inode().access_time();
    if before.tv_sec == after.tv_sec && before.tv_nsec == after.tv_nsec {
        return;
    }

    let result = state.client.record_access_time(call.state().fid());
    let observation = state.begin_cache_observation();
    state.fence_readdir_replies();
    if let Ok(remote_inode) = call.inode().remote_id() {
        state.invalidate_object_hints(&[remote_inode]);
    }

    let Ok(attributes) = result else {
        expire_cached_attributes_at(call.inode(), observation.generation);
        return;
    };
    if validate_stat_for_inode(call.inode(), &attributes).is_err() {
        expire_cached_attributes_at(call.inode(), observation.generation);
        return;
    }
    let Ok(inode_state) = call.inode().state() else {
        return;
    };
    let mut cached = inode_state.cached_attributes.lock();
    if observation.generation <= cached.data_generation {
        return;
    }
    call.inode().refresh_access_time_from_stat(&attributes);
    cached.stat.atime_sec = attributes.atime_sec;
    cached.stat.atime_nsec = attributes.atime_nsec;
    cached.data_generation = observation.generation;
}

/// Whether an authoritative Stat matches the relaxed-consistency baseline for
/// this file's cached data.
fn retains_cached_pages(inode: &InodeRef<'_>, inode_state: &InodeState, remote: &Stat) -> bool {
    inode_state
        .cached_attributes
        .lock()
        .data_baseline
        .matches(inode, remote)
}

/// Publish a mmap-safe data refresh under inode and mapping exclusion.
///
/// The caller has already written back and invalidated the mapping as needed.
/// Keeping the attribute-cache mutex across metadata, size and baseline
/// publication makes a racing local write either precede this snapshot or
/// invalidate it afterward.
fn publish_mapped_data_at(
    coherent: &DrainedCoherentMappingGuard<'_>,
    inode_state: &InodeState,
    attributes: &Stat,
    observation: CacheObservation,
    refresh_size: bool,
) -> Result<bool> {
    let inode = coherent.inode();
    if !stat_matches_inode(inode, attributes) {
        return Ok(false);
    }
    let mut cached = inode_state.cached_attributes.lock();
    if observation.generation <= cached.data_generation {
        return Ok(false);
    }

    let publish_metadata = observation.generation > cached.metadata_generation;
    // Preserve VFS-maintained relative link-count updates while refreshing
    // data; namespace operations can still have a newer metadata observation.
    inode.refresh_attributes_from_stat(
        attributes,
        AttributeRefresh {
            metadata: publish_metadata,
            data: true,
            link_count: false,
        },
    );
    if refresh_size {
        coherent.refresh_size_after_invalidation(attributes.size)?;
    }
    if refresh_size {
        cached.content_generation = observation.generation;
    }
    cache_data_observation(&mut cached, attributes, observation);
    if publish_metadata {
        cache_metadata_observation(inode_state, &mut cached, attributes, observation, false);
    }
    store_data_cache_baseline(
        inode_state,
        &mut cached,
        attributes,
        observation.observed_ns,
    );
    cached.mapping_generation = observation.generation;
    Ok(true)
}

/// Whether a reply still outranks every
/// observation and invalidation recorded for this inode.
///
/// This is an early check for the generation publication rechecks while holding
/// the attribute-cache mutex.
fn attributes_reply_is_current(inode_state: &InodeState, observation: CacheObservation) -> bool {
    let cached = inode_state.cached_attributes.lock();
    observation.generation > cached.data_generation
}

/// Refresh metadata without discarding a single cached folio.
///
/// Returns false when size, mtime or ctime no longer matches the cache baseline
/// and the caller must run the barriered refresh instead.
fn try_retain_cached_file(
    inode: &InodeRef<'_>,
    inode_state: &InodeState,
    file_state: &FileState,
    state: &MountState,
) -> Result<bool> {
    let inode_guard = IoExcludedInodeGuard::acquire(inode)?;
    let mapping = inode.mapping()?;
    let coherent = DrainedCoherentMappingGuard::drain_from_inode_guard(
        inode.reborrow(),
        mapping,
        inode_guard,
    )?;

    // Take the authoritative snapshot after writeback: a dirty upload may
    // itself change remote size or timestamps.
    let observation = state.begin_cache_observation();
    let reply = state.client.getattr(file_state.fid())?;
    validate_stat_for_inode(inode, &reply.stat)?;
    if !retains_cached_pages(inode, inode_state, &reply.stat)
        || !attributes_reply_is_current(inode_state, observation)
    {
        return Ok(false);
    }
    if !publish_coherent_data_stat_at(&coherent, &reply.stat, observation)? {
        return Ok(false);
    }
    Ok(true)
}

/// Drop stale clean folios before a new mapping can fault them in.
///
/// The fault callback pins the mapped file and releases its MM lock before
/// entering this function, so normal revalidation lock ordering is safe.
/// Following NFS, this syncs and then invalidates rather than truncating:
/// `invalidate_inode_pages2_range` preserves a concurrently dirtied folio
/// instead of discarding newer local bytes.
fn revalidate_mapped_file(
    inode: &InodeRef<'_>,
    file_state: &FileState,
    state: &MountState,
) -> Result<MmapRefresh> {
    let inode_state = inode.state()?;
    if state.is_relaxed() && mapped_data_is_fresh(inode_state) {
        return Ok(MmapRefresh::Complete);
    }

    let _refresh_guard = inode_state.data_revalidate.lock();
    if state.is_relaxed() && mapped_data_is_fresh(inode_state) {
        return Ok(MmapRefresh::Complete);
    }

    let mut invalidation_retries = 0;
    'retry_invalidation: loop {
        let inode_guard = IoExcludedInodeGuard::acquire(inode)?;
        let mapping = inode.mapping()?;

        // The combined guard uploads before the authoritative getattr.
        // Fetching first would compare against a server state that writeback
        // supersedes.
        let coherent = DrainedCoherentMappingGuard::drain_from_inode_guard(
            inode.reborrow(),
            mapping,
            inode_guard,
        )?;

        loop {
            let observation = state.begin_cache_observation();
            let reply = state.client.getattr(file_state.fid())?;
            validate_stat_for_inode(inode, &reply.stat)?;

            let changed = !retains_cached_pages(inode, inode_state, &reply.stat);
            if changed {
                if let Err(error) = coherent.invalidate_all() {
                    if error == EBUSY {
                        invalidation_retries += 1;
                        if invalidation_retries >= MMAP_INVALIDATION_RETRIES {
                            // Dirty folios from another credential group are
                            // drained in a later fault attempt. Keep the gate
                            // pending instead of exposing a transient backlog
                            // to userspace as SIGBUS.
                            return Ok(MmapRefresh::Retry);
                        }
                        // A failed invalidation normally unmapped a racing
                        // folio. Drop both exclusions, let its owner run, then
                        // drain and retry without monopolizing revalidation if
                        // the folio is persistently unreleasable.
                        drop(coherent);
                        unsafe {
                            bindings::schedule_timeout_uninterruptible(1);
                        }
                        continue 'retry_invalidation;
                    }
                    return Err(error);
                }
            }
            if publish_mapped_data_at(&coherent, inode_state, &reply.stat, observation, changed)? {
                return Ok(MmapRefresh::Complete);
            }
            // A newer stat observation or mutation outranked this reply.
            // i_rwsem excludes local writers here, so retry until this mapping
            // is reconciled with a newer snapshot.
        }
    }
}

fn revalidate_cached_file(
    inode: &InodeRef<'_>,
    file_state: &FileState,
    state: &MountState,
    cache_only: bool,
) -> Result<()> {
    let inode_state = inode.state()?;

    let now = monotonic_now_ns();
    if state.is_relaxed()
        && now.wrapping_sub(inode_state.last_data_revalidate_ns.load(Ordering::Acquire))
            < RELAXED_CACHE_REVALIDATE_NS
    {
        return Ok(());
    }
    if cache_only {
        return Err(EAGAIN);
    }

    // Serialize one authoritative refresh for this inode. Lock ordering is
    // data_revalidate -> inode.i_rwsem -> mapping.invalidate_lock; read_folio
    // must never reverse the last step because generic filemap already holds
    // the invalidate_lock shared.
    let _refresh_guard = inode_state.data_revalidate.lock();
    let now = monotonic_now_ns();
    if state.is_relaxed()
        && now.wrapping_sub(inode_state.last_data_revalidate_ns.load(Ordering::Acquire))
            < RELAXED_CACHE_REVALIDATE_NS
    {
        return Ok(());
    }

    if state.is_relaxed() && try_retain_cached_file(inode, inode_state, file_state, state)? {
        return Ok(());
    }

    let inode_guard = IoExcludedInodeGuard::acquire(inode)?;
    let mapping = inode.mapping()?;
    // The combined guard uploads dirty folios before granting destructive
    // cache/size operations.
    let coherent = DrainedCoherentMappingGuard::drain_from_inode_guard(
        inode.reborrow(),
        mapping,
        inode_guard,
    )?;

    loop {
        let observation = state.begin_cache_observation();
        let reply = state.client.getattr(file_state.fid())?;
        validate_stat_for_inode(inode, &reply.stat)?;

        // A size, mtime or ctime change conservatively invalidates the mapping.
        // This matches the userspace mount's relaxed-consistency contract
        // without imposing a persistent generation on the storage format.
        if !state.is_relaxed() || !retains_cached_pages(inode, inode_state, &reply.stat) {
            coherent.truncate_all();
        }
        if publish_coherent_data_stat_at(&coherent, &reply.stat, observation)? {
            return Ok(());
        }
        // A newer full-stat observation or mutation won the coherent-data
        // generation. Retry while exclusion is held rather than treating an
        // older, unpublishable mapping snapshot as success.
    }
}

pub(super) unsafe extern "C" fn zerofs_write_iter(
    iocb: *mut bindings::kiocb,
    source: *mut bindings::iov_iter,
) -> isize {
    from_result(|| {
        // SAFETY: VFS retains the kiocb and its iterator for this callback.
        let mut call = unsafe { WriteCall::from_raw(iocb, source) }?;
        zerofs_write_iter_inner(&mut call)
    })
}

fn zerofs_write_iter_inner(call: &mut WriteCall<'_>) -> Result<isize> {
    let count = call.count();
    let ki_flags = call.flags();
    if call.inode().file_type() != bindings::S_IFREG {
        return Err(EISDIR);
    }
    if count == 0 {
        return Ok(0);
    }
    if ki_flags & IOCB_NOWAIT != 0 {
        // NOWAIT cannot be honored by the blocking transport.
        return Err(errno!(EOPNOTSUPP));
    }
    let direct = ki_flags & bindings::IOCB_DIRECT as ffi::c_int != 0;
    let force_unbuffered = call.state().force_unbuffered();
    let unbuffered = direct || force_unbuffered;
    let asynchronous = call.is_asynchronous();
    // The durability barrier below must run before an async completion can
    // escape. Submit DSYNC through a synchronous kiocb copy and return the
    // completed result directly, which async-capable callers support.
    let force_synchronous = asynchronous && ki_flags & IOCB_DSYNC != 0;
    let append = ki_flags & IOCB_APPEND != 0;

    let state = call.inode().mount()?;
    // A partial page-cache write may need the remote bytes around the modified
    // span. Revalidate before netfslib performs that read-modify-write.
    revalidate_cached_file(call.inode(), call.state(), state, false)?;

    if call.position() < 0 {
        return Err(EINVAL);
    }
    // Establish normal EFBIG/RLIMIT error precedence and determine the legal
    // prefix before faulting user pages. The serialized path repeats this
    // check after acquiring its inode exclusion, in particular to select
    // append EOF.
    let preliminary = call.generic_write_check_count();
    if preliminary <= 0 {
        return Ok(preliminary);
    }
    let mut pinned = call.pin_source(preliminary as usize)?;
    let (written, start) = if unbuffered {
        let original_position = call.position();
        let written = call.run_netfs_unbuffered(pinned.take(), force_synchronous);
        // generic_write_checks() inside netfslib selects EOF for append and
        // successful synchronous I/O advances ki_pos. Recover the actual byte
        // range so a following synchronous-completion barrier covers it.
        let start = if append && written > 0 {
            call.position().saturating_sub(written as bindings::loff_t)
        } else {
            original_position
        };
        (written, start)
    } else {
        // Select this opener's group under exclusive inode ordering, then
        // retain that lock exclusively for local append offset selection, or
        // downgrade it for an ordinary positioned write.
        let inode = call.inode().reborrow();
        let grouped = GroupedBufferedWrite::acquire(
            &inode,
            call.state(),
            ki_flags & IOCB_DSYNC == 0,
            append,
            call.position(),
        )?;
        let checked = call.generic_write_checks(pinned.as_mut());
        let start = call.position();
        let written = if checked > 0 {
            call.run_netfs_buffered(pinned.as_mut(), grouped.group_ptr(), force_synchronous)
        } else {
            checked
        };
        if written > 0 {
            grouped.zero_exposed_eof_tail(&inode, start);
        }
        (written, start)
    };
    drop(pinned);

    // Match generic_write_sync, but route through this filesystem's fsync so
    // the dirty folios are uploaded before the ZeroFS durability barrier.
    if written > 0 && ki_flags & IOCB_DSYNC != 0 {
        let end = start.saturating_add(written as bindings::loff_t - 1);
        let datasync = ffi::c_int::from(ki_flags & IOCB_SYNC == 0);
        let sync_status = call.fsync_range(start, end, datasync);
        to_result(sync_status)?;
    }
    Ok(written)
}

pub(super) unsafe extern "C" fn zerofs_fsync(
    file: *mut bindings::file,
    start: bindings::loff_t,
    end: bindings::loff_t,
    datasync: ffi::c_int,
) -> ffi::c_int {
    from_result(|| {
        if start < 0 || end < start || (datasync != 0 && datasync != 1) {
            return Err(EINVAL);
        }

        // SAFETY: VFS retains this open file for the fsync callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        let fid = file.state().fid();
        let state = file.inode().mount()?;

        // Netfslib turns dirty folios into ZeroFS writes and records
        // asynchronous failures in mapping->wb_err. Only after upload
        // completes may the protocol durability barrier snapshot the mutation
        // generation.
        let upload_status = file.write_and_wait_range(start, end);
        let remote_status = if upload_status == 0 {
            remote_fsync_locked(state, file.inode(), fid, datasync != 0)
        } else {
            Ok(())
        };
        let writeback_error = file.take_writeback_error();
        to_result(upload_status)?;
        remote_status?;
        to_result(writeback_error)?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_fallocate(
    file: *mut bindings::file,
    mode: ffi::c_int,
    offset: bindings::loff_t,
    length: bindings::loff_t,
) -> ffi::c_long {
    from_result(|| {
        if offset < 0 || length <= 0 {
            return Err(EINVAL);
        }
        let mode = mode as u32;
        let supported = mode == 0
            || mode == protocol::FALLOC_FL_ZERO_RANGE
            || mode == protocol::FALLOC_FL_ZERO_RANGE | protocol::FALLOC_FL_KEEP_SIZE
            || mode == protocol::FALLOC_FL_PUNCH_HOLE | protocol::FALLOC_FL_KEEP_SIZE;
        if !supported {
            return Err(errno!(EOPNOTSUPP));
        }
        let end = match (offset as u64).checked_add(length as u64) {
            Some(end) if end <= bindings::loff_t::MAX as u64 => end,
            _ => return Err(EFBIG),
        };

        // SAFETY: VFS retains this open file for the fallocate callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        if file.inode().file_type() != bindings::S_IFREG {
            return Err(EINVAL);
        }
        let inode = file.inode().as_ptr();
        let fid = file.state().fid();
        let flags = file.flags();
        let state = file.inode().mount()?;
        let inode_guard = IoExcludedInodeGuard::acquire(file.inode())?;
        let extends_size = mode & protocol::FALLOC_FL_KEEP_SIZE == 0;
        if extends_size {
            // SAFETY: The guard above keeps this inode exclusively ours.
            to_result(unsafe { bindings::inode_newsize_ok(inode, end as bindings::loff_t) })?;
        }
        let mapping = file.inode().mapping()?;
        // The combined guard orders dirty bytes before exposing destructive cache
        // operations, so delayed writeback cannot resurrect punched/zeroed data.
        let coherent = DrainedCoherentMappingGuard::drain_from_inode_guard(
            file.inode().reborrow(),
            mapping,
            inode_guard,
        )?;
        let cached_size = file.inode().size();
        if cached_size < 0 {
            return Err(protocol_error());
        }
        if mode & (protocol::FALLOC_FL_ZERO_RANGE | protocol::FALLOC_FL_PUNCH_HOLE) != 0 {
            coherent.invalidate_range(offset as u64, length as usize)?;
        }
        if extends_size && end > cached_size as u64 {
            // Extending i_size can expose the zero tail of the old EOF folio. Drop
            // that folio in case the remote file had concurrently grown there.
            coherent.invalidate_range(cached_size as u64, 1)?;
        }
        let remote_inode = file.inode().remote_id().ok();
        let result = state
            .client
            .fallocate(fid, offset as u64, length as u64, mode);
        let mutation = state.begin_cache_observation();
        // A range mutation can commit before an interrupted/lost reply. Invalidate
        // its attributes and full-file data baseline for either outcome.
        state.fence_readdir_replies();
        if let Some(identifier) = remote_inode {
            state.invalidate_object_hints(&[identifier]);
        }
        expire_cached_data_at(file.inode(), mutation.generation);
        result?;

        // ZeroFS fallocate also updates blocks, mtime/ctime, and may clear set-id
        // bits. Fetch those fields while the open fid remains valid. The mutation
        // is already committed, so a failed refresh must not turn it into an error;
        // retain the minimum size bookkeeping needed by the local VFS instead.
        let refresh = state.begin_cache_observation();
        let attributes_refreshed = match (remote_inode, state.client.getattr(fid)) {
            (Some(identifier), Ok(reply))
                if validate_stat(&reply.stat).is_ok()
                    && reply.stat.qid.path == identifier
                    && reply.stat.qid.type_ == QID_TYPE_FILE
                    && reply.stat.mode & bindings::S_IFMT == bindings::S_IFREG =>
            {
                // Import metadata but not a size concurrently changed by another
                // client. This fallocate's own deterministic size extension is
                // applied below.
                publish_mutation_stat(file.inode(), &reply.stat, false, refresh);
                true
            }
            _ => false,
        };
        if !attributes_refreshed {
            expire_cached_attributes_at(file.inode(), refresh.generation);
        }
        if extends_size {
            coherent.extend_size_to(end)?;
        }
        // The durability registry is inode-scoped. Release only the mapping lock;
        // retain i_rwsem so no later mutation can pass this operation's barrier.
        let _inode_guard = coherent.into_inode_guard();
        if flags & bindings::O_DSYNC != 0 {
            let datasync = flags & bindings::O_SYNC != bindings::O_SYNC;
            remote_fsync_locked(state, file.inode(), fid, datasync)?;
        }
        Ok(0)
    })
}
