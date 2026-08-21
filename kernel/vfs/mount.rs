//! Mount, superblock, and dentry callbacks.

use core::{
    cmp, ptr,
    sync::atomic::{AtomicBool, AtomicU64},
};

use kernel::{
    alloc::{KBox, flags::GFP_KERNEL},
    bindings,
    error::{
        Result,
        code::{ECHILD, ENOENT, ENOMEM},
        from_result,
    },
    ffi,
    init::InPlaceInit,
    new_mutex, pr_err,
    seq_file::SeqFile,
    seq_print, try_pin_init,
};

use crate::{
    client::{Client, Endpoint, RebindCredentials},
    protocol::{GETATTR_ALL, QID_TYPE_DIR},
};

use super::{
    Consistency, DentryState, LOOKUP_RCU, LOOKUP_REVAL, MAX_READAHEAD_DEPTH,
    MAX_READAHEAD_WINDOW_BYTES, MIN_READAHEAD_WINDOW_BYTES, MountState, READ_REPLY_OVERHEAD,
    ReaddirHintCache, SB_RDONLY, ST_NODEV, ST_NOSUID, ST_RDONLY, ST_VALID, ZEROFS_MAGIC,
    attributes::{
        cache_inode_attributes_at, dentry_cache_is_fresh, expected_qid_type, mark_dentry_observed,
        monotonic_now_ns, protocol_error, validate_stat,
    },
    inode::get_inode,
    io::{
        DentryIdentityRef, DentryInitRef, DentryRef, DentryReleaseRef, EvictionInode, InodeRef,
        KstatfsOut, KstatfsValues, LookupNameRef, SuperBlockInitRef, SuperBlockRef,
        SuperBlockReleaseRef,
    },
    remote::{getattr_attributes, lookup_attributes, retain_bound_fid},
};

/// Connect a client and populate a nodev superblock.
///
/// `s_root` is published only after the remote root has been fetched and every
/// local object is initialized. The caller must arrange for `kill_anon_super`
/// to tear down a successfully mounted superblock.
///
/// # Safety
///
/// `super_block` must be the live, exclusively initialized superblock supplied
/// to a `get_tree_nodev()` fill callback.
pub(crate) unsafe fn fill_super_with_endpoint(
    super_block: *mut bindings::super_block,
    endpoint: Endpoint,
    credentials: RebindCredentials,
    consistency: Consistency,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: get_tree_nodev lends an exclusively initialized superblock.
        let super_block = unsafe { SuperBlockInitRef::from_raw(super_block) }?;
        try_fill_super(super_block, endpoint, credentials, consistency).inspect_err(|error| {
            pr_err!("mount initialization failed: errno={}\n", error.to_errno());
        })?;
        Ok(0)
    })
}

fn try_fill_super(
    mut super_block: SuperBlockInitRef<'_>,
    endpoint: Endpoint,
    credentials: RebindCredentials,
    consistency: Consistency,
) -> Result<()> {
    let client = Client::connect(endpoint, &credentials)?;
    let root_qid = client.root_qid();
    if root_qid.path != 0 || root_qid.type_ != QID_TYPE_DIR {
        return Err(protocol_error());
    }
    let state = KBox::pin_init(
        try_pin_init!(MountState {
            client,
            consistency,
            readdir_hints <- new_mutex!(ReaddirHintCache::new()?),
            hint_generation: AtomicU64::new(0),
            cache_generation: AtomicU64::new(0),
            teardown_started: AtomicBool::new(false),
        }),
        GFP_KERNEL,
    )?;
    let root_observation = state.begin_cache_observation();
    let root_reply = state.client.getattr(state.client.root_fid())?;
    if root_reply.valid & GETATTR_ALL != GETATTR_ALL {
        return Err(protocol_error());
    }
    let root_attributes = root_reply.stat;
    validate_stat(&root_attributes)?;
    if root_attributes.qid.path != root_qid.path || root_attributes.qid.type_ != root_qid.type_ {
        return Err(protocol_error());
    }

    // Nodev superblocks start on noop_backing_dev_info, whose zero ra_pages
    // disables generic file readahead even when an address-space callback is
    // installed. Keep several rsize-bounded subrequests in the BDI window so
    // the asynchronous issue callback can overlap their tagged RPCs.
    let payload_bytes = state
        .client
        .negotiated_msize()
        .saturating_sub(READ_REPLY_OVERHEAD) as usize;
    let parallelism = cmp::max(
        1,
        cmp::min(state.client.pending_tag_capacity(), MAX_READAHEAD_DEPTH),
    );
    let window_bytes = payload_bytes
        .saturating_mul(parallelism)
        .clamp(MIN_READAHEAD_WINDOW_BYTES, MAX_READAHEAD_WINDOW_BYTES);
    let readahead_pages = cmp::max(1, window_bytes.div_ceil(bindings::PAGE_SIZE as usize));
    super_block.configure(state, readahead_pages)?;

    // The configured superblock has valid operation tables and mount state.
    // NewInode owns every failure transition before d_make_root consumes it.
    let root_inode = get_inode(&super_block.as_ref(), &root_attributes, root_observation)?;

    // SAFETY: d_make_root() consumes the inode reference on both success and
    // failure. A null return denotes allocation failure.
    let root = unsafe { bindings::d_make_root(root_inode.into_raw()) };
    let root = ptr::NonNull::new(root).ok_or_else(|| ENOMEM)?;

    // Everything below is infallible. Publish the root last, then transfer the
    // allocation to put_super().
    super_block.publish(root)
}

pub(super) unsafe extern "C" fn zerofs_put_super(super_block: *mut bindings::super_block) {
    // SAFETY: VFS invokes put_super once with exclusive teardown access.
    let Some(super_block) = (unsafe { SuperBlockReleaseRef::from_raw(super_block) }) else {
        return;
    };
    drop(super_block.take_state());
}

/// Mark ordinary unmount before generic shutdown starts evicting dentries.
///
/// The client deliberately remains connected through kill_anon_super: its
/// sync_filesystem phase still needs every open/writeback fid. put_super drops
/// the client afterward, closing the connection and releasing all remaining
/// server-side fids together.
pub(crate) unsafe fn begin_shutdown(super_block: *mut bindings::super_block) {
    let Ok(super_block) = (unsafe { SuperBlockRef::from_raw(super_block) }) else {
        return;
    };
    if let Ok(state) = super_block.mount() {
        state.begin_teardown();
    }
}

pub(super) unsafe extern "C" fn zerofs_umount_begin(super_block: *mut bindings::super_block) {
    let Ok(super_block) = (unsafe { SuperBlockRef::from_raw(super_block) }) else {
        return;
    };
    let Ok(state) = super_block.mount() else {
        return;
    };
    state.begin_teardown();
    state.client.terminate_for_unmount();
}

pub(super) unsafe extern "C" fn zerofs_evict_inode(inode: *mut bindings::inode) {
    // SAFETY: VFS gives this callback exclusive final ownership of the inode.
    let Some(inode) = (unsafe { EvictionInode::from_raw(inode) }) else {
        return;
    };
    let inode = match inode {
        // iget_failed marks an incompletely initialized inode bad before
        // dropping its final reference. Only the embedded VFS inode is valid.
        EvictionInode::Bad(inode) => {
            inode.clear();
            return;
        }
        EvictionInode::Initialized(inode) => inode,
    };

    inode.drain_io();
    {
        let inode_ref = inode.as_ref();
        if let Some(inode_state) = inode.state() {
            let mut retained_fids = {
                let mut cache = inode_state.bound_fids.lock();
                cache.take_all()
            };
            if let Ok(mount) = inode_ref.mount() {
                if !mount.teardown_started() {
                    while let Some(entry) = retained_fids.pop() {
                        let _ = mount.client.clunk(entry.fid);
                    }
                }
            }
        }
    }

    drop(inode.finish());
}

pub(super) unsafe extern "C" fn zerofs_sync_fs(
    super_block: *mut bindings::super_block,
    wait: ffi::c_int,
) -> ffi::c_int {
    // VFS invokes the non-waiting pass before starting inode/page writeback,
    // then invokes the waiting pass after netfslib has uploaded dirty folios.
    // Only that second pass may snapshot the protocol mutation generation.
    from_result(|| {
        if wait == 0 {
            return Ok(0);
        }
        let super_block = unsafe { SuperBlockRef::from_raw(super_block) }?;
        let state = super_block.mount()?;
        // A mount-wide barrier must answer for every fid's outstanding
        // lineage, not just the root's.
        state.client.fsync_all(state.client.root_fid())?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_statfs(
    dentry: *mut bindings::dentry,
    output: *mut bindings::kstatfs,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS lends this callback exclusive output storage.
        let output = unsafe { KstatfsOut::from_raw(output) }?;
        // SAFETY: VFS supplies a live dentry for the duration of statfs.
        let dentry = unsafe { DentryRef::from_raw(dentry) }?;
        let super_block = dentry.super_block()?;
        let state = super_block.mount()?;
        let remote = state.client.statfs(state.client.root_fid())?;
        if remote.r#type != ZEROFS_MAGIC as u32
            || remote.bsize == 0
            || remote.namelen == 0
            || remote.bfree > remote.blocks
            || remote.bavail > remote.bfree
            || remote.ffree > remote.files
        {
            return Err(protocol_error());
        }

        output.write(KstatfsValues {
            filesystem_type: remote.r#type as ffi::c_long,
            block_size: remote.bsize as ffi::c_long,
            blocks: remote.blocks,
            blocks_free: remote.bfree,
            blocks_available: remote.bavail,
            files: remote.files,
            files_free: remote.ffree,
            filesystem_id: remote.fsid,
            name_length: cmp::min(remote.namelen, bindings::NAME_MAX) as ffi::c_long,
            fragment_size: remote.bsize as ffi::c_long,
            flags: (if super_block.flags() & SB_RDONLY != 0 {
                ST_RDONLY
            } else {
                0
            }) | ST_NOSUID
                | ST_NODEV
                | ST_VALID,
        });
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_show_options(
    output: *mut bindings::seq_file,
    dentry: *mut bindings::dentry,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS supplies exclusive access to this seq_file for the
        // duration of show_options.
        let output = unsafe { SeqFile::from_raw(output) };
        // SAFETY: VFS supplies a live dentry for the duration of show_options.
        let dentry = unsafe { DentryRef::from_raw(dentry) }?;
        let state = dentry.super_block()?.mount()?;
        let consistency = match state.consistency {
            Consistency::Relaxed => "relaxed",
            Consistency::Strict => "strict",
        };
        seq_print!(
            output,
            ",consistency={},msize={}",
            consistency,
            state.client.negotiated_msize()
        );
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_d_init(dentry: *mut bindings::dentry) -> ffi::c_int {
    from_result(|| {
        // SAFETY: d_init receives exclusive access to an unpublished dentry.
        let dentry = unsafe { DentryInitRef::from_raw(dentry) }?;
        let state = KBox::pin_init(DentryState::new(), GFP_KERNEL).map_err(|_| ENOMEM)?;
        dentry.publish(state);
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_d_release(dentry: *mut bindings::dentry) {
    // SAFETY: VFS invokes d_release once with exclusive final access.
    let Some(dentry) = (unsafe { DentryReleaseRef::from_raw(dentry) }) else {
        return;
    };
    drop(dentry.take_state());
}

pub(super) unsafe extern "C" fn zerofs_d_revalidate(
    parent: *mut bindings::inode,
    name: *const bindings::qstr,
    dentry: *mut bindings::dentry,
    flags: ffi::c_uint,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: d_revalidate runs with either a dentry reference or RCU
        // protection, both of which keep the private dentry state allocated.
        let Some(identity) = (unsafe { DentryIdentityRef::from_raw(dentry) }) else {
            return Ok(0);
        };

        if identity.is_mount_root() {
            return Ok(1);
        }
        // DentryState is released by d_release before the dentry object's
        // final RCU grace period. A ref-walk owns a dentry reference and can
        // safely read it; an RCU walk cannot. Fall back before touching
        // d_fsdata rather than requiring module callbacks to outlive an
        // RCU-deferred state free.
        if flags & LOOKUP_RCU != 0 {
            return Err(ECHILD);
        }
        // SAFETY: Ref-walk retains the dentry for this callback.
        let dentry = unsafe { DentryRef::from_raw(dentry) }?;
        // SAFETY: The VFS supplies a separate stable lookup component because
        // the dentry itself is not protected from rename by this callback.
        let name = unsafe { LookupNameRef::from_raw(name) }?;
        let name = name.name()?;
        let credentials = RebindCredentials::current()?;
        // SAFETY: Ref-walk retains the parent for this callback.
        let parent = unsafe { InodeRef::from_raw(parent) }?;
        let state = parent.mount()?;
        let now = monotonic_now_ns();
        if dentry_cache_is_fresh(state, &dentry, &credentials, now, flags & LOOKUP_REVAL != 0) {
            return Ok(1);
        }

        // An expired positive dentry is not evidence that its name
        // disappeared. Returning zero here makes namei call d_invalidate(),
        // which temporarily unhashes a directory held as another task's cwd
        // while the subsequent lookup reconnects the same alias. A getcwd
        // racing that remote lookup then observes ENOENT. Revalidate in place,
        // as 9p, NFS and FUSE do, and ask the VFS to invalidate only when the
        // name really changed.
        let valid = revalidate_dentry(&parent, &dentry, name, &credentials)?;
        Ok(valid.into())
    })
}

/// Refresh one expired name without disconnecting an unchanged positive dentry.
fn revalidate_dentry(
    parent: &InodeRef<'_>,
    dentry: &DentryRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
) -> Result<bool> {
    let inode = dentry.inode()?;
    let mount = parent.mount()?;
    let observation = mount.begin_cache_observation();
    let client = &mount.client;
    let lookup = match lookup_attributes(client, parent, name, credentials) {
        Ok(lookup) => lookup,
        Err(error) if error == ENOENT => {
            if inode.is_none() {
                mark_dentry_observed(dentry, credentials, observation);
                return Ok(true);
            }
            return Ok(false);
        }
        Err(error) => return Err(error),
    };

    let Some(inode) = inode else {
        // A formerly negative name now exists. The ordinary lookup path must
        // instantiate it, and this speculative walk owns no retained fid.
        let _ = client.clunk(lookup.child_fid);
        return Ok(false);
    };
    let expected_type = expected_qid_type(inode.file_type())?;
    let same_inode = lookup.stat.qid.path == inode.remote_id()?
        && lookup.stat.qid.type_ == expected_type
        && lookup.stat.mode & bindings::S_IFMT == inode.file_type();
    if !same_inode {
        let _ = client.clunk(lookup.child_fid);
        return Ok(false);
    }

    // Publish permission-relevant shared fields under i_lock. Regular-file
    // size and data-derived timestamps still require the data-cache barrier.
    cache_inode_attributes_at(&inode, &lookup.stat, observation);
    retain_bound_fid(client, &inode, credentials, lookup.child_fid);
    mark_dentry_observed(dentry, credentials, observation);
    Ok(true)
}

/// Revalidate the inode behind a path reached without a parent/name lookup.
///
/// VFS uses this weaker operation for `.`, `..`, mountpoint traversal and
/// procfs-style links. Its question is whether the inode remains valid, not
/// whether this dentry is still its current name.
pub(super) unsafe extern "C" fn zerofs_d_weak_revalidate(
    dentry: *mut bindings::dentry,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: weak revalidation runs only after leaving RCU walk and
        // retains a live dentry reference throughout the callback.
        let Some(identity) = (unsafe { DentryIdentityRef::from_raw(dentry) }) else {
            return Ok(0);
        };
        if identity.is_mount_root() {
            return Ok(1);
        }
        // SAFETY: As above.
        let dentry = unsafe { DentryRef::from_raw(dentry) }?;
        let state = dentry.super_block()?.mount()?;
        let Some(inode) = dentry.inode()? else {
            return Ok(state.is_relaxed().into());
        };
        let credentials = RebindCredentials::current()?;
        let now = monotonic_now_ns();
        if dentry_cache_is_fresh(state, &dentry, &credentials, now, false) {
            return Ok(1);
        }

        let expected_type = expected_qid_type(inode.file_type())?;
        let observation = state.begin_cache_observation();
        let attributes =
            match getattr_attributes(&state.client, &inode, &credentials, expected_type) {
                Ok(attributes) => attributes,
                Err(error) if error == ENOENT => return Ok(0),
                Err(error) => return Err(error),
            };
        validate_stat(&attributes)?;
        if attributes.qid.path != inode.remote_id()?
            || attributes.qid.type_ != expected_type
            || attributes.mode & bindings::S_IFMT != inode.file_type()
        {
            return Ok(0);
        }
        cache_inode_attributes_at(&inode, &attributes, observation);
        mark_dentry_observed(&dentry, &credentials, observation);
        Ok(1)
    })
}
