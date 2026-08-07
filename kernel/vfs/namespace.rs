//! Namespace and inode-metadata VFS callbacks.

use core::{ptr, sync::atomic::Ordering};

use kernel::{
    bindings,
    error::{
        Result,
        code::{
            EACCES, ECHILD, EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOMEM, ENOTDIR, EOVERFLOW, EPERM,
        },
        from_result, to_result,
    },
    ffi,
    types::ScopeGuard,
};

use crate::{
    client::{DeviceNumber, RebindCredentials, SetAttributes, TimeChange, WireTime},
    protocol::{QID_TYPE_FILE, Stat},
};

use super::{
    BoundFidUse, CacheObservation, CallbackDrainedMappingGuard, CallbackInodeWriteGuard,
    LOOKUP_REVAL, MODE_PERMISSIONS, MountState, OpenedCreation, RELAXED_CACHE_REVALIDATE_NS,
    attributes::{
        apply_killpriv_mode_at, cache_getattr_attributes_at, cache_inode_attributes_at,
        cache_inode_mutation_attributes_at, cached_inode_attributes, creation_gid,
        expected_qid_type, expire_cached_attributes_at, expire_cached_data_at, fill_kstat,
        from_const_ptr_result, from_ptr_result, mark_dentry_invalidated, mark_dentry_observed,
        mark_spliced_dentry_observed, monotonic_now_ns, protocol_error, publish_mutation_stat,
        validate_stat, validate_stat_for_inode,
    },
    compat,
    inode::get_inode,
    io::{
        DelayedCallRef, DentryRef, FileOpenRef, InodeRef, KstatOut, NameDentryRef, OpenFileRef,
        PathRef, SetattrRequest, SymlinkTargetRef,
    },
    new_file_state,
    remote::{
        acquire_bound_fid, create_directory, create_hard_link, create_opened_regular_file,
        create_regular_file, create_special_node, create_symlink, getattr_attributes,
        lookup_attributes, read_symlink, remove_entry, rename_entry, retain_bound_fid,
    },
};

pub(super) unsafe extern "C" fn zerofs_lookup(
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
    flags: ffi::c_uint,
) -> *mut bindings::dentry {
    from_ptr_result(|| {
        // SAFETY: VFS retains the parent and stabilizes the child name
        // throughout this lookup callback.
        let parent = unsafe { InodeRef::from_raw(parent) }?;
        let dentry = unsafe { NameDentryRef::from_namei_raw(dentry) }?;
        lookup_entry(&parent, &dentry, flags)
    })
}

fn lookup_entry(
    parent: &InodeRef<'_>,
    dentry: &NameDentryRef<'_>,
    flags: ffi::c_uint,
) -> Result<*mut bindings::dentry> {
    let name = dentry.name()?;
    let super_block = parent.super_block()?;
    let remote_parent = parent.remote_id()?;
    let credentials = RebindCredentials::current()?;
    let state = parent.mount()?;
    let now = monotonic_now_ns();
    let hint = if flags & LOOKUP_REVAL == 0 {
        state.take_readdir_hint(remote_parent, name, &credentials, now)
    } else {
        None
    };
    let (attributes, observation, walked_fid) = if let Some((attributes, observation)) = hint {
        (attributes, observation, None)
    } else {
        let observation = state.begin_cache_observation();
        let lookup = match lookup_attributes(&state.client, parent, name, &credentials) {
            Ok(lookup) => lookup,
            Err(error) if error == ENOENT => {
                // SAFETY: lookup owns this unhashed dentry. A null inode
                // records a negative result for the current path walk.
                unsafe {
                    mark_dentry_observed(dentry.as_dentry(), &credentials, observation);
                    bindings::d_add(dentry.as_ptr(), ptr::null_mut());
                }
                return Ok(ptr::null_mut());
            }
            Err(error) => return Err(error),
        };
        (lookup.stat, observation, Some(lookup.child_fid))
    };
    // Clunk the walked fid unless it is transferred to the inode cache.
    let walked_fid = ScopeGuard::new_with_data(walked_fid, |fid: Option<u32>| {
        if let Some(fid) = fid {
            let _ = state.client.clunk(fid);
        }
    });
    validate_stat(&attributes)?;
    if attributes.qid.path == remote_parent {
        return Err(protocol_error());
    };

    let inode = get_inode(&super_block, &attributes, observation)?;
    if let Some(fid) = walked_fid.dismiss() {
        retain_bound_fid(&state.client, &inode.as_ref(), &credentials, fid);
    }

    // SAFETY: d_splice_alias() consumes the inode reference and returns either
    // null, an alias dentry, or an ERR_PTR, exactly as lookup requires.
    let result = unsafe { bindings::d_splice_alias(inode.into_raw(), dentry.as_ptr()) };
    mark_spliced_dentry_observed(dentry.as_dentry(), result, &credentials, observation);
    Ok(result)
}

pub(super) unsafe extern "C" fn zerofs_getattr(
    idmap: *mut bindings::mnt_idmap,
    path: *const bindings::path,
    output: *mut bindings::kstat,
    request_mask: u32,
    query_flags: ffi::c_uint,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS lends this callback exclusive output storage.
        let mut output = unsafe { KstatOut::from_raw(output) }?;
        // SAFETY: VFS supplies a live path for the duration of getattr.
        let path = unsafe { PathRef::from_raw(path) }?;
        let inode = path.dentry()?.inode()?.ok_or(ENOENT)?;

        // Trebind intentionally refuses namespace-detached inodes. An open
        // file or directory still has a valid local inode after unlink/rmdir,
        // so fstat must use the cache maintained by writes and ATTR_FILE
        // setattr operations.
        if inode.link_count() == 0 {
            // SAFETY: VFS lends this callback exclusive output storage, and
            // the inode is live for its duration.
            unsafe {
                bindings::generic_fillattr(idmap, request_mask, inode.as_ptr(), output.as_ptr());
            }
            return Ok(0);
        }

        let state = inode.mount()?;
        let sync_type = query_flags & bindings::AT_STATX_SYNC_TYPE;
        let force_sync = sync_type == bindings::AT_STATX_FORCE_SYNC;
        let dont_sync = sync_type == bindings::AT_STATX_DONT_SYNC;
        let now = monotonic_now_ns();
        // DONT_SYNC explicitly requests a local answer even on a strict mount.
        // Ordinary strict getattr never treats the one-second relaxed window
        // as authoritative.
        if !force_sync && (state.is_relaxed() || dont_sync) {
            if let Some(attributes) = cached_inode_attributes(&inode, now, dont_sync) {
                fill_kstat(idmap, request_mask, &inode, &attributes, &mut output)?;
                return Ok(0);
            }
        }

        let expected_qid_type = expected_qid_type(inode.file_type())?;
        let credentials = RebindCredentials::current()?;
        let writeback_mapping = if expected_qid_type == QID_TYPE_FILE {
            Some(inode.mapping()?)
        } else {
            None
        };

        let attributes = loop {
            // The observation starts before the writeback barrier. Any writer
            // that completes after this point advances the cache generation and
            // makes the resulting Stat unpublishable; retrying repeats this
            // barrier.
            let observation = state.begin_cache_observation();
            if let Some(mapping) = writeback_mapping.as_ref() {
                // A remote Stat cannot authoritatively describe a locally
                // extended or timestamp-modified dirty file. Cached getattr
                // remains RPC-free and AT_STATX_DONT_SYNC was handled above.
                to_result(mapping.write_and_wait_all())?;
            }
            let attributes =
                match getattr_attributes(&state.client, &inode, &credentials, expected_qid_type) {
                    Ok(attributes) => attributes,
                    // Refreshing rebinds the inode, which re-authorizes its
                    // whole path with the caller's live credentials. fstat
                    // must not depend on those: a descriptor survives a
                    // privilege drop, an SCM_RIGHTS hand-off, and a chmod of a
                    // parent directory. Whatever reached this inode was
                    // authorized when it did, so answer from the last observed
                    // attributes rather than failing the syscall. Path-based
                    // callers are still stopped earlier, by ->permission on
                    // each ancestor.
                    Err(error) if error == EACCES || error == EPERM => {
                        match cached_inode_attributes(&inode, now, true) {
                            Some(attributes) => {
                                fill_kstat(idmap, request_mask, &inode, &attributes, &mut output)?;
                            }
                            None => {
                                // SAFETY: VFS lends this callback exclusive
                                // output storage, and the inode is live for
                                // its duration.
                                unsafe {
                                    bindings::generic_fillattr(
                                        idmap,
                                        request_mask,
                                        inode.as_ptr(),
                                        output.as_ptr(),
                                    );
                                }
                            }
                        }
                        return Ok(0);
                    }
                    Err(error) => return Err(error),
                };
            validate_stat_for_inode(&inode, &attributes)?;
            if cache_getattr_attributes_at(&inode, &attributes, observation) {
                // Both independently ordered subsets came from this response.
                // Use it for the current stat even if a very slow RPC consumed
                // the entire cache-validity window.
                break attributes;
            }
            // A newer mutation/observation won while this RPC was in flight.
            // If it left a complete current snapshot, use that; otherwise
            // retry rather than returning the stale reply that lost
            // publication.
            if state.is_relaxed() {
                if let Some(current) = cached_inode_attributes(&inode, monotonic_now_ns(), false) {
                    break current;
                }
            }
        };

        // Permission-relevant shared fields were published under i_lock above.
        // fill_kstat returns every authoritative field directly from this
        // Stat.
        fill_kstat(idmap, request_mask, &inode, &attributes, &mut output)?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_permission(
    idmap: *mut bindings::mnt_idmap,
    inode: *mut bindings::inode,
    mask: ffi::c_int,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains this inode for the duration of the check.
        let inode = unsafe { InodeRef::from_raw(inode) }?;
        let inode_state = inode.state()?;
        let mount = inode.mount()?;
        loop {
            let now = monotonic_now_ns();
            let metadata_fresh_ns = inode_state.metadata_fresh_ns.load(Ordering::Acquire);
            let metadata_is_fresh = mount.is_relaxed()
                && metadata_fresh_ns != 0
                && now.wrapping_sub(metadata_fresh_ns) < RELAXED_CACHE_REVALIDATE_NS;
            if metadata_is_fresh || inode.link_count() == 0 {
                break;
            }

            // RCU walk cannot take the cache mutex or issue a blocking RPC.
            // Shared inode fields were published before metadata_fresh_ns's
            // release store, so a fresh lockless observation may proceed
            // directly.
            if mask & bindings::MAY_NOT_BLOCK as ffi::c_int != 0 {
                return Err(ECHILD);
            }

            let credentials = RebindCredentials::current()?;
            let expected_type = expected_qid_type(inode.file_type())?;
            let observation = mount.begin_cache_observation();
            let attributes =
                getattr_attributes(&mount.client, &inode, &credentials, expected_type)?;
            validate_stat_for_inode(&inode, &attributes)?;
            if cache_inode_attributes_at(&inode, &attributes, observation) {
                break;
            }
            // A local mutation or newer metadata request outranked this reply.
            // Repeat only if it did not publish a fresh replacement itself;
            // this avoids authorizing against fields whose invalidation won
            // the race.
        }

        // SAFETY: The inode is live and its permission-relevant fields were
        // published above.
        to_result(unsafe { bindings::generic_permission(idmap, inode.as_ptr(), mask) })?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_setattr(
    idmap: *mut bindings::mnt_idmap,
    dentry: *mut bindings::dentry,
    attributes: *mut bindings::iattr,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS supplies a positive dentry and holds the target inode's
        // i_rwsem across notify_change() and this callback. Most callers hold
        // it exclusively, while netfslib's write-killpriv paths hold it
        // shared.
        let dentry_ref = unsafe { DentryRef::from_raw(dentry) }?;
        let inode_ref = dentry_ref.inode()?.ok_or(ENOENT)?;
        // SAFETY: This callback exclusively owns the normalized iattr.
        let attributes = unsafe { SetattrRequest::from_raw(attributes) }?;
        // SAFETY: The dentry and iattr are the ones this callback was given.
        let status =
            unsafe { bindings::setattr_prepare(idmap, dentry_ref.as_ptr(), attributes.as_ptr()) };
        to_result(status)?;

        // Snapshot the VFS request after setattr_prepare() has normalized
        // kill-bit changes and checked ownership, file-size, and
        // immutable/append rules.
        let requested = attributes.valid();
        let mut mutation = SetAttributes::default();

        if requested & bindings::ATTR_MODE != 0 {
            mutation.mode = Some(attributes.mode() & MODE_PERMISSIONS);
        }
        if requested & bindings::ATTR_UID != 0 {
            // iattr stores an idmapped vfsuid. Translate it back into this
            // filesystem's user namespace before placing the numeric ID on
            // wire.
            let filesystem_namespace = inode_ref.super_block()?.user_namespace_ptr();
            let vfsuid = attributes.vfsuid();
            // SAFETY: The namespace belongs to the live superblock and the
            // vfsuid comes from the callback's own iattr.
            let uid = unsafe {
                let kuid = bindings::from_vfsuid(idmap, filesystem_namespace, vfsuid);
                compat::from_kuid(filesystem_namespace, kuid)
            };
            if uid == u32::MAX {
                return Err(EOVERFLOW);
            }
            mutation.uid = Some(uid);
        }
        if requested & bindings::ATTR_GID != 0 {
            let filesystem_namespace = inode_ref.super_block()?.user_namespace_ptr();
            let vfsgid = attributes.vfsgid();
            // SAFETY: As for the uid above.
            let gid = unsafe {
                let kgid = bindings::from_vfsgid(idmap, filesystem_namespace, vfsgid);
                compat::from_kgid(filesystem_namespace, kgid)
            };
            if gid == u32::MAX {
                return Err(EOVERFLOW);
            }
            mutation.gid = Some(gid);
        }
        if requested & bindings::ATTR_SIZE != 0 {
            let requested_size = attributes.size();
            if requested_size < 0 {
                return Err(EINVAL);
            }
            mutation.size = Some(requested_size as u64);
        }
        mutation.atime = time_change(
            requested,
            bindings::ATTR_ATIME,
            bindings::ATTR_ATIME_SET,
            || attributes.atime(),
        )?;
        mutation.mtime = time_change(
            requested,
            bindings::ATTR_MTIME,
            bindings::ATTR_MTIME_SET,
            || attributes.mtime(),
        )?;

        // The remaining ATTR_* values are VFS control/hint bits or request a
        // server-maintained ctime. If no remotely mutable field remains, there
        // is no wire operation to issue.
        if mutation.is_empty() {
            return Ok(0);
        }

        let expected_qid_type = expected_qid_type(inode_ref.file_type())?;
        let remote_inode = inode_ref.remote_id()?;
        let state = inode_ref.mount()?;
        let writeback_mapping =
            if expected_qid_type == QID_TYPE_FILE || requested & bindings::ATTR_SIZE != 0 {
                Some(inode_ref.mapping()?)
            } else {
                None
            };
        let size_mapping = if requested & bindings::ATTR_SIZE != 0 {
            writeback_mapping.as_ref()
        } else {
            None
        };

        // ATTR_FILE identifies an already-open capability. This is essential
        // for ftruncate of an unlinked file, which cannot be rebound by inode
        // ID. Anything acquired here instead is this callback's to release.
        let mut transient_fid = ScopeGuard::new_with_data(None, |bound: Option<BoundFidUse>| {
            if let Some(bound) = bound {
                let _ = bound.cleanup(&state.client);
            }
        });
        let fid = if requested & bindings::ATTR_FILE != 0 {
            // SAFETY: VFS retains the ATTR_FILE descriptor for this callback.
            let file = unsafe { OpenFileRef::from_setattr_raw(attributes.file_ptr(), &inode_ref) }?;
            file.state().fid()
        } else {
            let credentials = RebindCredentials::current()?;
            let bound =
                acquire_bound_fid(&state.client, &inode_ref, &credentials, expected_qid_type)?;
            let fid = bound.fid;
            *transient_fid = Some(bound);
            fid
        };

        let write_killpriv = requested
            & (bindings::ATTR_KILL_SUID | bindings::ATTR_KILL_SGID | bindings::ATTR_KILL_PRIV)
            != 0
            && requested & bindings::ATTR_SIZE == 0;
        let mut callback_guard = if !write_killpriv {
            // SAFETY: The ordinary setattr callback is listed as exclusive in
            // the VFS locking contract. The internal killpriv path, which may
            // retain only shared ownership, is excluded above.
            Some(unsafe { CallbackInodeWriteGuard::from_setattr(&inode_ref) })
        } else {
            None
        };
        let mut callback_io_guard = None;
        if expected_qid_type == QID_TYPE_FILE {
            // Ordinary setattr and truncate callers hold i_rwsem exclusively,
            // so they may switch the inode out of direct-I/O mode and wait.
            // The one exception is file_remove_privs() inside a netfslib
            // write, which holds i_rwsem shared; changing direct-I/O mode
            // there would let buffered I/O overlap an active direct request.
            if let Some(guard) = callback_guard.take() {
                callback_io_guard = Some(guard.exclude_direct_io()?);
            }
        }

        if size_mapping.is_none() {
            if let Some(mapping) = writeback_mapping.as_ref() {
                // Preserve all writes ordered before a regular-file metadata
                // mutation. Besides truncate, tar commonly follows buffered
                // writes with fchmod/fchown/futimens before close. Sending
                // that setattr first would cache a server Stat whose size is
                // still zero and could let delayed writeback overwrite the
                // requested mtime.
                to_result(mapping.write_and_wait_all())?;
            }
        }
        let mut callback_mapping_guard = None;
        if let Some(mapping) = size_mapping {
            let inode_guard = callback_io_guard.take().ok_or(EIO)?;
            let coherent = CallbackDrainedMappingGuard::drain_from_inode_guard(
                inode_ref.reborrow(),
                mapping,
                inode_guard,
            )?;
            // A truncate may already have committed when a reply is
            // interrupted or fails validation. Purge the clean cache before
            // the RPC so no old bytes survive either an acknowledged or
            // ambiguous size mutation.
            coherent.truncate_all();
            callback_mapping_guard = Some(coherent);
        }
        let result = state.client.setattrattr(fid, &mutation);
        let observation = state.begin_cache_observation();
        // Rsetattrattr is the mutation's commit point. A subsequent Tclunk
        // failure kills the session and releases its server-side fids, but
        // must not turn the already-committed setattr into an error or
        // suppress the corresponding local inode update.
        if let Some(bound) = transient_fid.dismiss() {
            let _ = bound.cleanup(&state.client);
        }
        // A dispatched setattr may have committed even when its reply is
        // interrupted or lost. Expire both metadata and the data baseline
        // before interpreting the result so truncate/ctime cannot remain
        // locally fresh at their pre-operation values.
        state.fence_readdir_replies();
        state.invalidate_object_hints(&[remote_inode]);
        let content_changed = requested & bindings::ATTR_SIZE != 0;
        if content_changed {
            expire_cached_data_at(&inode_ref, observation.generation);
        } else {
            expire_cached_attributes_at(&inode_ref, observation.generation);
        }
        let remote = result?;
        validate_stat_for_inode(&inode_ref, &remote)?;
        if write_killpriv {
            // This callback may hold i_rwsem only shared. The generation check
            // prevents an older set-id clear from replacing newer metadata.
            apply_killpriv_mode_at(&inode_ref, &remote, observation.generation);
            return Ok(0);
        }

        // setattr_copy applies VFS bookkeeping that is independent of i_size.
        // The returned ZeroFS Stat then makes the cached inode reflect
        // server-side policy (for example cleared set-id bits and the
        // authoritative ctime).
        if requested & bindings::ATTR_SIZE != 0 {
            callback_mapping_guard
                .as_ref()
                .ok_or(EIO)?
                .publish_truncated_size(remote.size)?;
        }
        // The write-killpriv path returned above because setattr_copy requires
        // exclusive i_rwsem ownership.
        if let Some(guard) = callback_mapping_guard.as_ref() {
            // SAFETY: The callback owns idmap and attributes for this guard.
            unsafe {
                guard.copy_attributes(idmap, &attributes);
            }
        } else if let Some(guard) = callback_io_guard.as_ref() {
            // SAFETY: The callback owns idmap and attributes for this guard.
            unsafe {
                guard.copy_attributes(idmap, &attributes);
            }
        } else if let Some(guard) = callback_guard.as_ref() {
            // SAFETY: The callback owns idmap and attributes for this guard.
            unsafe {
                guard.copy_attributes(idmap, &attributes);
            }
        }
        // A metadata-only setattr must not import an unrelated concurrent
        // remote size without cache invalidation. ATTR_SIZE was handled above
        // while holding mapping.invalidate_lock.
        publish_mutation_stat(&inode_ref, &remote, content_changed, observation);
        Ok(0)
    })
}

/// Translate and validate one VFS timestamp request.
fn time_change(
    requested: u32,
    change_bit: u32,
    set_bit: u32,
    timestamp: impl FnOnce() -> bindings::timespec64,
) -> Result<TimeChange> {
    if requested & set_bit != 0 && requested & change_bit == 0 {
        return Err(EINVAL);
    }
    if requested & change_bit == 0 {
        return Ok(TimeChange::Retain);
    }
    if requested & set_bit == 0 {
        return Ok(TimeChange::Now);
    }
    let timestamp = timestamp();
    if timestamp.tv_sec < 0 || timestamp.tv_nsec < 0 {
        return Err(EOVERFLOW);
    }
    let time =
        WireTime::new(timestamp.tv_sec as u64, timestamp.tv_nsec as u64).map_err(|_| EOVERFLOW)?;
    Ok(TimeChange::Set(time))
}

/// State shared by the VFS name-creation callbacks.
struct NewEntryContext<'a> {
    parent: InodeRef<'a>,
    dentry: NameDentryRef<'a>,
    mount: &'a MountState,
    credentials: RebindCredentials,
    parent_id: u64,
    gid: u32,
}

impl<'a> NewEntryContext<'a> {
    /// # Safety
    ///
    /// `parent` and `dentry` must satisfy the VFS create-family callback
    /// contract for `'a`; in particular the child name and negative dentry
    /// attachment must remain stable until the operation returns.
    unsafe fn from_raw(
        parent: *mut bindings::inode,
        dentry: *mut bindings::dentry,
    ) -> Result<Self> {
        // SAFETY: The caller supplies the stronger create-family contract.
        let parent = unsafe { InodeRef::from_raw(parent)? };
        let dentry = unsafe { NameDentryRef::from_namei_raw(dentry)? };
        // Validate the name while the callback's namei lock pins it.
        dentry.name()?;
        let gid = creation_gid(&parent)?;
        let credentials = RebindCredentials::current()?;
        let mount = parent.mount()?;
        let parent_id = parent.remote_id()?;
        Ok(Self {
            parent,
            dentry,
            mount,
            credentials,
            parent_id,
            gid,
        })
    }

    fn name(&self) -> Result<&[u8]> {
        self.dentry.name()
    }

    /// Conservatively expire every observation an attempted create may have
    /// made stale.
    ///
    /// A transport error after dispatch is ambiguous: the server may have
    /// committed even though this syscall has no successful reply. Doing this
    /// for a proven pre-dispatch error costs only a later revalidation.
    fn invalidate_after_attempt(&self, generation: u64) {
        invalidate_new_entry_attempt(
            self.mount,
            &self.parent,
            self.parent_id,
            self.dentry.as_dentry(),
            self.name().ok(),
            generation,
        );
    }

    fn drop_failed_negative(&self) {
        // SAFETY: Create-family callbacks own this namei-stabilized dentry.
        // Dropping a failed negative result prevents an ambiguously committed
        // create from remaining hidden behind the old hash entry.
        unsafe {
            bindings::d_drop(self.dentry.as_ptr());
        }
    }

    fn validate_created(&self, remote: &Stat, expected_type: u32) -> Result<()> {
        validate_stat(remote)?;
        if remote.qid.path == self.parent_id || remote.mode & bindings::S_IFMT != expected_type {
            return Err(protocol_error());
        }
        Ok(())
    }

    /// Instantiate an ordinary non-directory child after its RPC committed.
    fn instantiate(
        self,
        remote: &Stat,
        expected_type: u32,
        observation: CacheObservation,
    ) -> Result<()> {
        self.validate_created(remote, expected_type)?;
        let inode = get_inode(&self.parent.super_block()?, remote, observation)?;
        // SAFETY: The constructor captured the create callback's exclusive
        // negative dentry. d_instantiate consumes the inode reference.
        unsafe {
            mark_dentry_observed(self.dentry.as_dentry(), &self.credentials, observation);
            bindings::d_instantiate(self.dentry.as_ptr(), inode.into_raw());
        }
        Ok(())
    }

    fn finish_non_directory(self, result: Result<Stat>, expected_type: u32) -> Result<()> {
        let observation = self.mount.begin_cache_observation();
        self.invalidate_after_attempt(observation.generation);
        match result {
            Ok(remote) => self.instantiate(&remote, expected_type, observation),
            Err(error) => {
                self.drop_failed_negative();
                Err(error)
            }
        }
    }
}

fn invalidate_new_entry_attempt(
    mount: &MountState,
    parent: &InodeRef<'_>,
    parent_id: u64,
    dentry: &DentryRef<'_>,
    name: Option<&[u8]>,
    generation: u64,
) {
    mark_dentry_invalidated(dentry, generation);
    mount.fence_readdir_replies();
    if let Some(name) = name {
        // Another client may have unlinked this name after our readdir cached
        // it, in which case a hint recorded under a different identity still
        // describes the dead object while the name now resolves to ours.
        mount.invalidate_entry_hint(parent_id, name);
    }
    mount.invalidate_object_hints(&[parent_id]);
    expire_cached_attributes_at(parent, generation);
}

pub(super) unsafe extern "C" fn zerofs_create(
    _idmap: *mut bindings::mnt_idmap,
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
    mode: bindings::umode_t,
    _exclusive: bindings::bool_,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the locked parent and negative child dentry.
        let entry = unsafe { NewEntryContext::from_raw(parent, dentry) }?;
        create_regular_entry(entry, mode)?;
        Ok(0)
    })
}

fn create_regular_entry(entry: NewEntryContext<'_>, mode: bindings::umode_t) -> Result<()> {
    let wire_mode = (mode as u32 & MODE_PERMISSIONS) | bindings::S_IFREG;
    let result = create_regular_file(
        &entry.mount.client,
        &entry.parent,
        entry.name()?,
        &entry.credentials,
        entry.gid,
        wire_mode,
    );
    entry.finish_non_directory(result, bindings::S_IFREG)
}

pub(super) unsafe extern "C" fn zerofs_atomic_open(
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
    file: *mut bindings::file,
    flags: ffi::c_uint,
    mode: bindings::umode_t,
) -> ffi::c_int {
    from_result(|| {
        if parent.is_null() || dentry.is_null() || file.is_null() {
            return Err(EINVAL);
        }
        // SAFETY: VFS retains the parent throughout atomic_open.
        let parent_ref = unsafe { InodeRef::from_raw(parent) }?;
        // SAFETY: atomic_open receives exclusive access to this unpublished
        // file.
        let file = unsafe { FileOpenRef::from_raw(file) }?;
        let dentry_ref = unsafe { NameDentryRef::from_namei_raw(dentry) }?;

        // Complete the normal lookup first. A positive result is opened
        // through zerofs_open after finish_no_open(); only a negative O_CREAT
        // reaches the one-RPC create+open fast path below.
        if dentry_ref.as_dentry().is_parallel_lookup() {
            let result = lookup_entry(&parent_ref, &dentry_ref, 0)?;
            if !result.is_null() || dentry_ref.as_dentry().has_inode() {
                file.finish_no_open(result)?;
                return Ok(0);
            }
        } else if dentry_ref.as_dentry().has_inode() {
            file.finish_no_open(ptr::null_mut())?;
            return Ok(0);
        }
        if flags & bindings::O_CREAT == 0 {
            file.finish_no_open(ptr::null_mut())?;
            return Ok(0);
        }

        let name = dentry_ref.name()?;
        let gid = creation_gid(&parent_ref)?;
        let state = parent_ref.mount()?;
        let mut open_flags = flags & bindings::O_ACCMODE;
        let direct = file.flags() & bindings::O_DIRECT != 0;
        let force_unbuffered = !state.is_relaxed() || (direct && open_flags == bindings::O_WRONLY);
        // Netfslib may need to read untouched bytes for a partial buffered
        // write. Strict and direct-only descriptors do not, and retaining
        // O_WRONLY avoids requiring read permission userspace did not request.
        if !force_unbuffered && open_flags == bindings::O_WRONLY {
            open_flags = bindings::O_RDWR;
        }
        let credentials = RebindCredentials::from_credential(file.credential())?;
        // Resolved before the RPC so its failure cannot leave a committed
        // create uninvalidated.
        let parent_id = parent_ref.remote_id()?;
        let wire_mode = (mode as u32 & MODE_PERMISSIONS) | bindings::S_IFREG;
        let mut retried_create = false;
        let (created, observation) = loop {
            let result = create_opened_regular_file(
                &state.client,
                &parent_ref,
                name,
                &credentials,
                gid,
                wire_mode,
                open_flags,
            );
            let observation = state.begin_cache_observation();
            invalidate_new_entry_attempt(
                state,
                &parent_ref,
                parent_id,
                dentry_ref.as_dentry(),
                Some(name),
                observation.generation,
            );
            match result {
                Ok(created) => break (created, observation),
                Err(error) if error == EEXIST && flags & bindings::O_EXCL == 0 => {
                    if retried_create {
                        // The outer open path retries ESTALE with
                        // LOOKUP_REVAL.
                        unsafe {
                            bindings::d_drop(dentry_ref.as_ptr());
                        }
                        return Err(errno!(ESTALE));
                    }
                    retried_create = true;
                    // A concurrent creator won after our negative lookup.
                    // Resolve it and let ordinary open use the retained walked
                    // fid. If it vanished again, retry this atomic create
                    // once.
                    //
                    // The initial negative lookup hashed this dentry. Drop it
                    // before invoking lookup again; d_add on an already-hashed
                    // negative would corrupt the dcache hash chain.
                    unsafe {
                        bindings::d_drop(dentry_ref.as_ptr());
                    }
                    let result = lookup_entry(&parent_ref, &dentry_ref, LOOKUP_REVAL)?;
                    if !result.is_null() || dentry_ref.as_dentry().has_inode() {
                        file.finish_no_open(result)?;
                        return Ok(0);
                    }
                }
                Err(error) => {
                    unsafe {
                        bindings::d_drop(dentry_ref.as_ptr());
                    }
                    return Err(error);
                }
            }
        };
        let OpenedCreation {
            stat: remote,
            fid,
            iounit,
            parent_cleanup,
        } = created;

        // Clunk the created fid unless it reaches the published FileState.
        let fid = ScopeGuard::new_with_data(fid, |fid| {
            let _ = state.client.clunk(fid);
        });

        // The namespace mutation is committed even if a later local
        // allocation or finish_open step fails. A failed transient-parent
        // clunk killed this session, including the newly opened child
        // capability.
        parent_cleanup?;
        validate_stat(&remote)?;
        if remote.qid.path == parent_id
            || remote.mode & bindings::S_IFMT != bindings::S_IFREG
            || remote.qid.type_ != QID_TYPE_FILE
        {
            return Err(protocol_error());
        }

        let inode = get_inode(&parent_ref.super_block()?, &remote, observation)?;
        let file_state = new_file_state(state, *fid, iounit, force_unbuffered, credentials, None)?;

        // The newly created inode cannot have a pre-existing alias. Publish it
        // before finish_open so do_dentry_open observes the final
        // mapping/fops.
        unsafe {
            mark_dentry_observed(
                dentry_ref.as_dentry(),
                file_state.credentials(),
                observation,
            );
            bindings::d_instantiate(dentry_ref.as_ptr(), inode.into_raw());
        }
        file.finish_open(dentry_ref.as_dentry())?;

        // finish_open succeeded and the file is still unpublished to
        // userspace. Transfer the sole FileState reference and mark creation
        // for namei.
        fid.dismiss();
        file.publish(file_state, true);
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_mkdir(
    _idmap: *mut bindings::mnt_idmap,
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
    mode: bindings::umode_t,
) -> *mut bindings::dentry {
    from_ptr_result(|| {
        // SAFETY: VFS retains the locked parent and negative child dentry.
        let entry = unsafe { NewEntryContext::from_raw(parent, dentry) }?;
        create_directory_entry(entry, mode)
    })
}

fn create_directory_entry(
    entry: NewEntryContext<'_>,
    mode: bindings::umode_t,
) -> Result<*mut bindings::dentry> {
    let mut wire_mode = (mode as u32 & MODE_PERMISSIONS) | bindings::S_IFDIR;
    if entry.parent.mode() & bindings::S_ISGID != 0 {
        wire_mode |= bindings::S_ISGID;
    }
    let result = create_directory(
        &entry.mount.client,
        &entry.parent,
        entry.name()?,
        &entry.credentials,
        entry.gid,
        wire_mode,
    );
    let observation = entry.mount.begin_cache_observation();
    entry.invalidate_after_attempt(observation.generation);
    let remote = match result {
        Ok(remote) => remote,
        Err(error) => {
            entry.drop_failed_negative();
            return Err(error);
        }
    };
    entry.validate_created(&remote, bindings::S_IFDIR)?;

    // The acknowledged directory is now one more subdirectory of parent even
    // if a subsequent local inode allocation happens to fail.
    unsafe {
        bindings::inc_nlink(entry.parent.as_ptr());
    }
    let inode = get_inode(&entry.parent.super_block()?, &remote, observation)?;
    // mkdir receives a hashed negative dentry, while d_splice_alias requires
    // an unhashed one. Remote filesystems must drop it first so an already
    // cached directory alias can be moved into place safely. d_splice_alias
    // consumes the inode reference and returns the mkdir result: null for this
    // dentry, an alternate alias, or ERR_PTR.
    let result = unsafe {
        mark_dentry_observed(entry.dentry.as_dentry(), &entry.credentials, observation);
        bindings::d_drop(entry.dentry.as_ptr());
        bindings::d_splice_alias(inode.into_raw(), entry.dentry.as_ptr())
    };
    mark_spliced_dentry_observed(
        entry.dentry.as_dentry(),
        result,
        &entry.credentials,
        observation,
    );
    Ok(result)
}

pub(super) unsafe extern "C" fn zerofs_symlink(
    _idmap: *mut bindings::mnt_idmap,
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
    target: *const ffi::c_char,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS supplies a NUL-terminated target for this callback.
        let target = unsafe { SymlinkTargetRef::from_raw(target) }?;
        // SAFETY: VFS retains the locked parent and negative child dentry.
        let entry = unsafe { NewEntryContext::from_raw(parent, dentry) }?;
        create_symlink_entry(entry, target.bytes())?;
        Ok(0)
    })
}

fn create_symlink_entry(entry: NewEntryContext<'_>, target: &[u8]) -> Result<()> {
    let result = create_symlink(
        &entry.mount.client,
        &entry.parent,
        entry.name()?,
        target,
        &entry.credentials,
        entry.gid,
    );
    entry.finish_non_directory(result, bindings::S_IFLNK)
}

pub(super) unsafe extern "C" fn zerofs_mknod(
    _idmap: *mut bindings::mnt_idmap,
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
    mode: bindings::umode_t,
    device: bindings::dev_t,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the locked parent and negative child dentry.
        let entry = unsafe { NewEntryContext::from_raw(parent, dentry) }?;
        create_special_entry(entry, mode, device)?;
        Ok(0)
    })
}

fn create_special_entry(
    entry: NewEntryContext<'_>,
    mode: bindings::umode_t,
    device: bindings::dev_t,
) -> Result<()> {
    let file_type = mode as u32 & bindings::S_IFMT;
    if !matches!(
        file_type,
        bindings::S_IFCHR | bindings::S_IFBLK | bindings::S_IFIFO | bindings::S_IFSOCK
    ) {
        return Err(EINVAL);
    }
    // dev_t is the kernel's internal 12:20 encoding here, unlike the
    // new_encode_dev() value delivered through FUSE's u32 ABI.
    let device = if matches!(file_type, bindings::S_IFCHR | bindings::S_IFBLK) {
        DeviceNumber {
            major: (device >> 20) as u32,
            minor: (device & 0x000f_ffff) as u32,
        }
    } else {
        DeviceNumber::default()
    };
    let wire_mode = (mode as u32 & MODE_PERMISSIONS) | file_type;
    let result = create_special_node(
        &entry.mount.client,
        &entry.parent,
        entry.name()?,
        &entry.credentials,
        entry.gid,
        wire_mode,
        device,
    );
    entry.finish_non_directory(result, file_type)
}

struct LinkContext<'a> {
    old_dentry: DentryRef<'a>,
    parent: InodeRef<'a>,
    new_dentry: NameDentryRef<'a>,
}

impl<'a> LinkContext<'a> {
    /// # Safety
    ///
    /// All three pointers must satisfy the VFS link callback contract for
    /// `'a`, including stabilization of the destination name.
    unsafe fn from_raw(
        old_dentry: *mut bindings::dentry,
        parent: *mut bindings::inode,
        new_dentry: *mut bindings::dentry,
    ) -> Result<Self> {
        Ok(Self {
            old_dentry: unsafe { DentryRef::from_raw(old_dentry)? },
            parent: unsafe { InodeRef::from_raw(parent)? },
            new_dentry: unsafe { NameDentryRef::from_namei_raw(new_dentry)? },
        })
    }
}

pub(super) unsafe extern "C" fn zerofs_link(
    old_dentry: *mut bindings::dentry,
    parent: *mut bindings::inode,
    new_dentry: *mut bindings::dentry,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the locked source, parent, and destination objects.
        let context = unsafe { LinkContext::from_raw(old_dentry, parent, new_dentry) }?;
        create_hard_link_entry(context)?;
        Ok(0)
    })
}

fn create_hard_link_entry(context: LinkContext<'_>) -> Result<()> {
    let inode_ref = context.old_dentry.inode()?.ok_or_else(|| ENOENT)?;
    let inode = inode_ref.as_ptr();
    if inode_ref.file_type() == bindings::S_IFDIR {
        return Err(EPERM);
    }
    let name = context.new_dentry.name()?;
    let inode_id = inode_ref.remote_id()?;
    let expected_type = expected_qid_type(inode_ref.file_type())?;
    let credentials = RebindCredentials::current()?;
    let state = context.parent.mount()?;
    // Resolved before the RPC so its failure cannot leave a committed link
    // uninvalidated.
    let parent_id = context.parent.remote_id()?;
    let result = create_hard_link(
        &state.client,
        &context.parent,
        &inode_ref,
        name,
        &credentials,
        expected_type,
    );
    let observation = state.begin_cache_observation();
    // A dispatched link can commit even when interruption or disconnect hides
    // its reply. Expire both namespace and source metadata before interpreting
    // the outcome; pre-dispatch failures merely pay a revalidation.
    mark_dentry_invalidated(context.new_dentry.as_dentry(), observation.generation);
    state.fence_readdir_replies();
    state.invalidate_entry_hint(parent_id, name);
    state.invalidate_object_hints(&[parent_id, inode_id]);
    expire_cached_attributes_at(&context.parent, observation.generation);
    expire_cached_attributes_at(&inode_ref, observation.generation);
    let remote = match result {
        Ok(remote) => remote,
        Err(error) => {
            unsafe {
                bindings::d_drop(context.new_dentry.as_ptr());
            }
            return Err(error);
        }
    };
    validate_stat_for_inode(&inode_ref, &remote)?;

    // Both parent and source inode are write-locked by the VFS link path.
    // Publish returned timestamps/ownership when their generations win, but
    // apply the VFS link transition exactly once regardless of
    // whether a newer observation already won cache publication.
    let _ = cache_inode_mutation_attributes_at(&inode_ref, &remote, observation);
    unsafe {
        bindings::inode_set_ctime_current(inode);
        bindings::inc_nlink(inode);
        bindings::ihold(inode);
        mark_dentry_observed(context.new_dentry.as_dentry(), &credentials, observation);
        bindings::d_instantiate(context.new_dentry.as_ptr(), inode);
    }
    Ok(())
}

fn unlink_entry(
    parent: &InodeRef<'_>,
    dentry: &NameDentryRef<'_>,
    remove_directory: bool,
) -> Result<()> {
    let victim_ref = dentry.as_dentry().inode()?.ok_or_else(|| ENOENT)?;
    let victim = victim_ref.as_ptr();
    let victim_is_directory = victim_ref.file_type() == bindings::S_IFDIR;
    if remove_directory && !victim_is_directory {
        return Err(ENOTDIR);
    }
    if !remove_directory && victim_is_directory {
        return Err(EISDIR);
    }
    let name = dentry.name()?;
    let credentials = RebindCredentials::current()?;
    let state = parent.mount()?;
    // Both resolved before the RPC so their failure cannot leave a committed
    // removal uninvalidated.
    let parent_id = parent.remote_id()?;
    let victim_id = victim_ref.remote_id()?;
    let result = remove_entry(&state.client, parent, name, &credentials, remove_directory);
    let observation = state.begin_cache_observation();
    // Removal may have committed before an interrupted or lost reply. Expire
    // the victim dentry and all related hints/attributes for every outcome.
    mark_dentry_invalidated(dentry.as_dentry(), observation.generation);
    state.fence_readdir_replies();
    state.invalidate_entry_hint(parent_id, name);
    state.invalidate_object_hints(&[parent_id, victim_id]);
    expire_cached_attributes_at(parent, observation.generation);
    expire_cached_attributes_at(&victim_ref, observation.generation);
    result?;

    // The VFS removes the dentry after this callback; maintain only inode link
    // counts here. Both parent and victim are exclusively locked by VFS.
    unsafe {
        // Preserve a meaningful fstat snapshot if this was the last namespace
        // link and an open server fid keeps the inode alive.
        bindings::inode_set_ctime_current(victim);
        if remove_directory {
            bindings::clear_nlink(victim);
            bindings::drop_nlink(parent.as_ptr());
        } else {
            bindings::drop_nlink(victim);
        }
    }
    Ok(())
}

pub(super) unsafe extern "C" fn zerofs_unlink(
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the locked parent and stabilizes the victim name.
        let parent = unsafe { InodeRef::from_raw(parent) }?;
        let dentry = unsafe { NameDentryRef::from_namei_raw(dentry) }?;
        unlink_entry(&parent, &dentry, false)?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_rmdir(
    parent: *mut bindings::inode,
    dentry: *mut bindings::dentry,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the locked parent and stabilizes the victim name.
        let parent = unsafe { InodeRef::from_raw(parent) }?;
        let dentry = unsafe { NameDentryRef::from_namei_raw(dentry) }?;
        unlink_entry(&parent, &dentry, true)?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_rename(
    _idmap: *mut bindings::mnt_idmap,
    old_parent: *mut bindings::inode,
    old_dentry: *mut bindings::dentry,
    new_parent: *mut bindings::inode,
    new_dentry: *mut bindings::dentry,
    flags: ffi::c_uint,
) -> ffi::c_int {
    from_result(|| {
        // 9P renameat has no atomic NOREPLACE, EXCHANGE, or WHITEOUT form.
        if flags != 0 {
            return Err(EINVAL);
        }
        // SAFETY: VFS retains both locked parents and stabilizes both dentry names
        // throughout rename.
        let old_parent_ref = unsafe { InodeRef::from_raw(old_parent) }?;
        let new_parent_ref = unsafe { InodeRef::from_raw(new_parent) }?;
        let old_dentry_ref = unsafe { NameDentryRef::from_namei_raw(old_dentry) }?;
        let new_dentry_ref = unsafe { NameDentryRef::from_namei_raw(new_dentry) }?;
        let old_name = old_dentry_ref.name()?;
        let new_name = new_dentry_ref.name()?;
        let credentials = RebindCredentials::current()?;
        let state = old_parent_ref.mount()?;
        // Both resolved before the RPC so their failure cannot leave a committed
        // rename uninvalidated.
        let old_parent_id = old_parent_ref.remote_id()?;
        let new_parent_id = new_parent_ref.remote_id()?;
        // Resolve the affected inode set before dispatch so an ambiguous error can
        // still invalidate every observation the rename may have committed past.
        let source_ref = old_dentry_ref.as_dentry().inode().ok().flatten();
        let target_ref = new_dentry_ref.as_dentry().inode().ok().flatten();
        let mut objects = [0u64; 4];
        objects[0] = old_parent_id;
        objects[1] = new_parent_id;
        let mut object_count = 2;
        for resolved in [source_ref.as_ref(), target_ref.as_ref()] {
            if let Some(identifier) = resolved.and_then(|inode| inode.remote_id().ok()) {
                objects[object_count] = identifier;
                object_count += 1;
            }
        }

        let result = rename_entry(
            &state.client,
            &old_parent_ref,
            old_name,
            &new_parent_ref,
            new_name,
            &credentials,
        );
        let observation = state.begin_cache_observation();
        // A timeout, disconnect or interrupted reply does not prove that rename
        // was rejected. Make both names and every touched inode stale before
        // propagating the outcome.
        mark_dentry_invalidated(old_dentry_ref.as_dentry(), observation.generation);
        mark_dentry_invalidated(new_dentry_ref.as_dentry(), observation.generation);
        state.fence_readdir_replies();
        state.invalidate_rename_hints(
            old_parent_id,
            old_name,
            new_parent_id,
            new_name,
            &objects[..object_count],
        );
        expire_cached_attributes_at(&old_parent_ref, observation.generation);
        if new_parent != old_parent {
            expire_cached_attributes_at(&new_parent_ref, observation.generation);
        }
        if let Some(source) = source_ref.as_ref() {
            expire_cached_attributes_at(source, observation.generation);
        }
        if let Some(target) = target_ref.as_ref() {
            expire_cached_attributes_at(target, observation.generation);
        }
        result?;
        // VFS performs the dcache move after this callback. It already holds every
        // participating inode lock, so only cached link counts need adjustment.
        let source_ref = source_ref.ok_or_else(protocol_error)?;
        let source = source_ref.as_ptr();
        let target = target_ref
            .as_ref()
            .map_or(ptr::null_mut(), InodeRef::as_ptr);
        // VFS moves this dentry to the destination name after the callback.
        mark_dentry_observed(old_dentry_ref.as_dentry(), &credentials, observation);
        // Renaming one hard-link alias over another alias of the same inode is a
        // successful no-op and must not decrement the shared inode's link count.
        if source == target {
            mark_dentry_observed(new_dentry_ref.as_dentry(), &credentials, observation);
            return Ok(0);
        }
        let source_is_directory = source_ref.file_type() == bindings::S_IFDIR;
        let target_is_directory = target_ref
            .as_ref()
            .is_some_and(|target| target.file_type() == bindings::S_IFDIR);
        unsafe {
            if !target.is_null() {
                // The replaced inode may remain reachable only through an open
                // file. getattr deliberately falls back to this local snapshot
                // once nlink reaches zero, so record the namespace mutation's
                // ctime before adjusting its link count.
                bindings::inode_set_ctime_current(target);
                if target_is_directory {
                    bindings::clear_nlink(target);
                } else {
                    bindings::drop_nlink(target);
                }
            }
            if source_is_directory {
                if target_is_directory {
                    bindings::drop_nlink(new_parent);
                }
                if old_parent != new_parent {
                    bindings::drop_nlink(old_parent);
                    bindings::inc_nlink(new_parent);
                }
            }
        }
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_get_link(
    dentry: *mut bindings::dentry,
    inode: *mut bindings::inode,
    done: *mut bindings::delayed_call,
) -> *const ffi::c_char {
    from_const_ptr_result(|| {
        // RCU pathwalk passes a null dentry and forbids this blocking RPC.
        if dentry.is_null() {
            return Err(ECHILD);
        }
        // SAFETY: Non-RCU get_link lends this cleanup slot exclusively.
        let done = unsafe { DelayedCallRef::from_raw(done) }?;
        // SAFETY: The non-RCU get_link path retains the inode for this
        // callback.
        let inode_ref = unsafe { InodeRef::from_raw(inode) }?;
        let credentials = RebindCredentials::current()?;
        let state = inode_ref.mount()?;
        let target = read_symlink(&state.client, &inode_ref, &credentials)?;
        let bytes = target.as_slice();
        if bytes.contains(&b'\0') {
            return Err(protocol_error());
        }
        // SAFETY: The borrowed target bytes are copied into a fresh
        // NUL-terminated allocation the delayed call below takes ownership of.
        let allocation = unsafe {
            bindings::kmemdup_nul(
                bytes.as_ptr().cast::<ffi::c_char>(),
                bytes.len(),
                bindings::GFP_KERNEL,
            )
        };
        let allocation = ptr::NonNull::new(allocation).ok_or(ENOMEM)?;
        let result = allocation.as_ptr();
        done.install_kfree(allocation);
        Ok(result.cast_const())
    })
}
