//! Inode, dentry, and stat cache publication.

use core::{cmp, sync::atomic::Ordering};

use kernel::{
    bindings,
    error::{
        Error, Result,
        code::{EIO, ENOMEM, EOVERFLOW},
    },
    ffi,
};

use crate::{
    client::RebindCredentials,
    protocol::{QID_TYPE_DIR, QID_TYPE_FILE, QID_TYPE_SYMLINK, Qid, Stat},
};

use super::{
    AttributeRefresh, CachedAttributes, DataCacheBaseline, DrainedCoherentMappingGuard, InodeState,
    MountState, NSEC_PER_SEC, RELAXED_CACHE_REVALIDATE_NS, compat,
    io::{DentryRef, InodeRef, KstatOut},
};

pub(super) fn store_data_cache_baseline(
    inode_state: &InodeState,
    cached: &mut CachedAttributes,
    attributes: &Stat,
    observed_ns: u64,
) {
    cached.data_baseline = DataCacheBaseline::from_stat(attributes);
    inode_state
        .last_data_revalidate_ns
        .store(observed_ns, Ordering::Release);
}

fn merge_metadata_attributes(cached: &mut Stat, attributes: &Stat, refresh_link_count: bool) {
    cached.mode = attributes.mode;
    cached.uid = attributes.uid;
    cached.gid = attributes.gid;
    cached.r#gen = attributes.r#gen;
    if refresh_link_count {
        cached.nlink = attributes.nlink;
    }
}

fn merge_data_attributes(cached: &mut Stat, attributes: &Stat) {
    cached.qid.version = attributes.qid.version;
    cached.size = attributes.size;
    cached.blksize = attributes.blksize;
    cached.blocks = attributes.blocks;
    cached.atime_sec = attributes.atime_sec;
    cached.atime_nsec = attributes.atime_nsec;
    cached.mtime_sec = attributes.mtime_sec;
    cached.mtime_nsec = attributes.mtime_nsec;
    cached.ctime_sec = attributes.ctime_sec;
    cached.ctime_nsec = attributes.ctime_nsec;
}

pub(super) fn cache_metadata_observation(
    inode_state: &InodeState,
    cached: &mut CachedAttributes,
    attributes: &Stat,
    observed_ns: u64,
    refresh_link_count: bool,
) {
    merge_metadata_attributes(&mut cached.stat, attributes, refresh_link_count);
    cached.metadata_watermark_ns = observed_ns;
    cached.metadata_valid = true;
    inode_state
        .metadata_fresh_ns
        .store(observed_ns, Ordering::Release);
}

pub(super) fn cache_data_observation(
    cached: &mut CachedAttributes,
    attributes: &Stat,
    observed_ns: u64,
) {
    merge_data_attributes(&mut cached.stat, attributes);
    cached.data_watermark_ns = observed_ns;
    cached.data_valid = true;
}

pub(super) fn stat_matches_inode(inode: &InodeRef<'_>, attributes: &Stat) -> bool {
    let file_type = inode.file_type();
    inode.remote_id().is_ok_and(|remote_inode| {
        attributes.qid.path == remote_inode
            && attributes.mode & bindings::S_IFMT == file_type
            && expected_qid_type(file_type).is_ok_and(|type_| attributes.qid.type_ == type_)
    })
}

pub(super) fn validate_stat_for_inode(inode: &InodeRef<'_>, attributes: &Stat) -> Result<()> {
    validate_stat(attributes)?;
    if stat_matches_inode(inode, attributes) {
        Ok(())
    } else {
        Err(protocol_error())
    }
}

/// Make a lock transition a cache coherency point.
///
/// This expires the revalidation watermark rather than invalidating here:
/// dropping folios needs the invalidation semaphore, which a `->lock` caller
/// must not hold across the `F_SETLKW` wait, and the proven revalidate path
/// already does the work on the next access. Invalidating before the grant, as
/// `fs/9p` does, would leave a window in which a racing read repopulates the
/// cache from the pre-lock state.
pub(super) fn expire_cached_data(inode: &InodeRef<'_>) {
    invalidate_inode_attributes(inode);
    if let Ok(inode_state) = inode.state() {
        inode_state.last_data_revalidate_ns.store(
            monotonic_now_ns().wrapping_sub(RELAXED_CACHE_REVALIDATE_NS),
            Ordering::Release,
        );
    }
}

enum AttributePublishMode {
    Mutation,
    MetadataOnly,
}

/// Publish a mutation response without renewing the page-cache baseline.
pub(super) fn publish_mutation_stat(inode: &InodeRef<'_>, attributes: &Stat) {
    publish_inode_attributes_at(
        inode,
        attributes,
        monotonic_now_ns(),
        AttributePublishMode::Mutation,
    );
}

/// Publish a Stat after its regular-file mapping has been reconciled.
pub(super) fn publish_coherent_data_stat_at(
    coherent: &DrainedCoherentMappingGuard<'_>,
    attributes: &Stat,
    observed_ns: u64,
) -> Result<bool> {
    let inode = coherent.inode();
    if !stat_matches_inode(inode, attributes) {
        return Ok(false);
    }
    let Ok(state) = inode.state() else {
        return Ok(false);
    };
    let mut cached = state.cached_attributes.lock();
    if observed_ns <= cached.data_ordering_watermark_ns {
        return Ok(false);
    }

    let publish_metadata = observed_ns > cached.metadata_watermark_ns;
    inode.refresh_attributes_from_stat(
        attributes,
        AttributeRefresh {
            metadata: publish_metadata,
            data: true,
            link_count: publish_metadata,
        },
    );
    coherent.refresh_size_from_stat(attributes)?;
    if publish_metadata {
        cache_metadata_observation(state, &mut cached, attributes, observed_ns, true);
    }
    cached.data_ordering_watermark_ns = observed_ns;
    if observed_ns > cached.data_watermark_ns {
        cache_data_observation(&mut cached, attributes, observed_ns);
    }
    // Baseline and freshness are the final part of the same cache-mutex
    // transaction as the watermark check and i_size publication. A racing
    // getattr or mutation can only run entirely before or after it.
    store_data_cache_baseline(state, &mut cached, attributes, observed_ns);
    Ok(true)
}

fn publish_inode_attributes_at(
    inode: &InodeRef<'_>,
    attributes: &Stat,
    observed_ns: u64,
    mode: AttributePublishMode,
) -> bool {
    if !stat_matches_inode(inode, attributes) {
        return false;
    }
    let Ok(state) = inode.state() else {
        return false;
    };
    let mut cached = state.cached_attributes.lock();
    let regular_file = inode.file_type() == bindings::S_IFREG;
    let publish_metadata = observed_ns > cached.metadata_watermark_ns;
    let (refresh_file_data, refresh_link_count) = match mode {
        AttributePublishMode::Mutation => (true, true),
        AttributePublishMode::MetadataOnly => (false, false),
    };
    let wants_data = refresh_file_data || !regular_file;
    let publish_data = wants_data && observed_ns > cached.data_ordering_watermark_ns;
    if !publish_metadata && !publish_data {
        return false;
    }

    // Permission/path observations may be newer than a data-coherent refresh
    // (or vice versa). Publish each independent subset only when it wins that
    // subset's watermark, while holding one cache lock across the matching VFS
    // i_lock updates.
    inode.refresh_attributes_from_stat(
        attributes,
        AttributeRefresh {
            metadata: publish_metadata,
            data: publish_data,
            link_count: refresh_link_count && publish_metadata,
        },
    );
    if publish_metadata {
        // A metadata-only remote observation may safely improve the stat-cache
        // nlink while the shared VFS count remains relative to locally
        // serialized namespace updates.
        cache_metadata_observation(
            state,
            &mut cached,
            attributes,
            observed_ns,
            refresh_link_count || !refresh_file_data,
        );
    }
    if publish_data {
        cached.data_ordering_watermark_ns = observed_ns;
        if observed_ns > cached.data_watermark_ns {
            cache_data_observation(&mut cached, attributes, observed_ns);
        }
    }

    if refresh_file_data {
        publish_data
    } else {
        publish_metadata
    }
}

pub(super) fn cache_inode_attributes_at(
    inode: &InodeRef<'_>,
    attributes: &Stat,
    observed_ns: u64,
) -> bool {
    // Permission/path revalidation may overlap dirty regular-file data. Import
    // ownership and mode, but leave regular-file data-derived fields to the
    // full barrier and preserve VFS-relative link-count updates.
    publish_inode_attributes_at(
        inode,
        attributes,
        observed_ns,
        AttributePublishMode::MetadataOnly,
    )
}

/// Cache a full stat result without claiming that its regular-file data was
/// reconciled with the page cache.
///
/// getattr writes dirty data before issuing this request, so its returned
/// fields are suitable for later stat calls. It still lacks the inode/mapping
/// exclusion required to replace i_size or the data-revalidation baseline.
pub(super) fn cache_getattr_attributes_at(
    inode: &InodeRef<'_>,
    attributes: &Stat,
    observed_ns: u64,
) -> bool {
    if !stat_matches_inode(inode, attributes) {
        return false;
    }
    let Ok(state) = inode.state() else {
        return false;
    };
    let mut cached = state.cached_attributes.lock();
    let publish_metadata = observed_ns > cached.metadata_watermark_ns;
    let publish_data = observed_ns > cached.data_watermark_ns;
    if !publish_metadata && !publish_data {
        return false;
    }

    // Only permission-relevant shared fields are safe without the data-cache
    // barrier. The complete Stat remains protected by its own two watermarks.
    inode.refresh_attributes_from_stat(
        attributes,
        AttributeRefresh {
            metadata: publish_metadata,
            data: false,
            link_count: false,
        },
    );
    if publish_metadata {
        cache_metadata_observation(state, &mut cached, attributes, observed_ns, true);
    }
    if publish_data {
        let mapping_changed = !cached.data_baseline.matches(inode, attributes);
        cache_data_observation(&mut cached, attributes, observed_ns);
        // This observation is newer than any in-flight page-cache refresh, but
        // it did not itself reconcile the mapping. Advance the ordering
        // watermark so an older refresh retries; do not advance the relaxed
        // data-freshness timestamp or baseline here.
        cached.data_ordering_watermark_ns =
            cmp::max(cached.data_ordering_watermark_ns, observed_ns);
        if mapping_changed {
            state.last_data_revalidate_ns.store(
                observed_ns.wrapping_sub(RELAXED_CACHE_REVALIDATE_NS),
                Ordering::Release,
            );
        }
    }
    publish_metadata && publish_data
}

pub(super) fn cached_inode_attributes(
    inode: &InodeRef<'_>,
    now_ns: u64,
    allow_expired: bool,
) -> Option<Stat> {
    let state = inode.state().ok()?;
    let cached = state.cached_attributes.lock();
    if !allow_expired
        && (!cached.metadata_valid
            || !cached.data_valid
            || cached.metadata_watermark_ns == 0
            || cached.data_watermark_ns == 0
            || now_ns.wrapping_sub(cached.metadata_watermark_ns) >= RELAXED_CACHE_REVALIDATE_NS
            || now_ns.wrapping_sub(cached.data_watermark_ns) >= RELAXED_CACHE_REVALIDATE_NS)
    {
        return None;
    }
    Some(cached.stat)
}

pub(super) fn invalidate_inode_attributes(inode: &InodeRef<'_>) {
    let Ok(state) = inode.state() else {
        return;
    };
    let invalidated_ns = monotonic_now_ns();
    let mut cached = state.cached_attributes.lock();
    cached.metadata_watermark_ns = cmp::max(cached.metadata_watermark_ns, invalidated_ns);
    cached.data_watermark_ns = cmp::max(cached.data_watermark_ns, invalidated_ns);
    cached.data_ordering_watermark_ns = cmp::max(cached.data_ordering_watermark_ns, invalidated_ns);
    cached.metadata_valid = false;
    cached.data_valid = false;
    state.metadata_fresh_ns.store(0, Ordering::Release);
}

pub(super) fn dentry_cache_is_fresh(
    mount: &MountState,
    dentry: &DentryRef<'_>,
    credentials: &RebindCredentials,
    now_ns: u64,
    force_revalidate: bool,
) -> bool {
    mount.is_relaxed()
        && dentry
            .state()
            .is_some_and(|state| state.is_fresh_for(credentials, now_ns, force_revalidate))
}

pub(super) fn mark_dentry_observed(
    dentry: &DentryRef<'_>,
    credentials: &RebindCredentials,
    observed_ns: u64,
) {
    if let Some(state) = dentry.state() {
        state.record(credentials, observed_ns);
    }
}

pub(super) fn mark_dentry_invalidated(dentry: &DentryRef<'_>, invalidated_ns: u64) {
    if let Some(state) = dentry.state() {
        state.invalidate(invalidated_ns);
    }
}

pub(super) fn mark_spliced_dentry_observed(
    candidate: &DentryRef<'_>,
    result: *mut bindings::dentry,
    credentials: &RebindCredentials,
    observed_ns: u64,
) {
    if kernel::error::from_err_ptr(result).is_err() {
        return;
    }
    if result.is_null() {
        mark_dentry_observed(candidate, credentials, observed_ns);
    } else if let Ok(installed) = unsafe {
        // d_splice_alias returns a live referenced alias on this branch.
        DentryRef::from_raw(result)
    } {
        mark_dentry_observed(&installed, credentials, observed_ns);
    }
}

pub(super) fn fill_kstat(
    idmap: *mut bindings::mnt_idmap,
    request_mask: u32,
    inode: &InodeRef<'_>,
    attributes: &Stat,
    output: &mut KstatOut<'_>,
) -> Result<()> {
    validate_stat(attributes)?;

    let super_block = inode.super_block()?;

    // Let VFS initialize device, mount, and any target-specific generic fields
    // from the immutable inode snapshot, then replace all remotely sourced
    // attributes below.
    unsafe {
        bindings::generic_fillattr(idmap, request_mask, inode.as_ptr(), output.as_ptr());
    }
    let result = output.as_mut();
    result.result_mask =
        (result.result_mask | bindings::STATX_BASIC_STATS) & !bindings::STATX_BTIME;
    result.mode = attributes.mode as bindings::umode_t;
    result.nlink = attributes.nlink as ffi::c_uint;
    result.blksize = attributes.blksize as u32;
    result.ino = inode_number(attributes.qid)? as u64;
    result.rdev = match attributes.mode & bindings::S_IFMT {
        bindings::S_IFCHR | bindings::S_IFBLK => wire_rdev_to_dev(attributes.rdev),
        _ => 0,
    };
    // `kstat.uid/gid` carry the idmapped vfsuid/vfsgid values in layout-
    // compatible kuid/kgid wrappers, matching generic_fillattr().
    let filesystem_namespace = super_block.user_namespace_ptr();
    let kuid = unsafe { compat::make_kuid(filesystem_namespace, attributes.uid) };
    let kgid = unsafe { compat::make_kgid(filesystem_namespace, attributes.gid) };
    let vfsuid = unsafe { bindings::make_vfsuid(idmap, filesystem_namespace, kuid) };
    let vfsgid = unsafe { bindings::make_vfsgid(idmap, filesystem_namespace, kgid) };
    result.uid = bindings::kuid_t { val: vfsuid.val };
    result.gid = bindings::kgid_t { val: vfsgid.val };
    result.size = attributes.size as bindings::loff_t;
    result.atime = bindings::timespec64 {
        tv_sec: attributes.atime_sec as bindings::time64_t,
        tv_nsec: attributes.atime_nsec as ffi::c_long,
    };
    result.mtime = bindings::timespec64 {
        tv_sec: attributes.mtime_sec as bindings::time64_t,
        tv_nsec: attributes.mtime_nsec as ffi::c_long,
    };
    result.ctime = bindings::timespec64 {
        tv_sec: attributes.ctime_sec as bindings::time64_t,
        tv_nsec: attributes.ctime_nsec as ffi::c_long,
    };
    result.blocks = attributes.blocks;
    Ok(())
}

pub(super) fn validate_stat(attributes: &Stat) -> Result<()> {
    let file_type = attributes.mode & bindings::S_IFMT;
    if !matches!(
        file_type,
        bindings::S_IFREG
            | bindings::S_IFDIR
            | bindings::S_IFLNK
            | bindings::S_IFCHR
            | bindings::S_IFBLK
            | bindings::S_IFIFO
            | bindings::S_IFSOCK
    ) {
        return Err(errno!(EOPNOTSUPP));
    }
    if attributes.mode > bindings::umode_t::MAX as u32 {
        return Err(protocol_error());
    }

    let expected_qid_type = expected_qid_type(attributes.mode)?;
    if attributes.qid.type_ != expected_qid_type {
        return Err(protocol_error());
    }

    inode_number(attributes.qid)?;
    if attributes.size > bindings::loff_t::MAX as u64
        || attributes.uid == u32::MAX
        || attributes.gid == u32::MAX
        || attributes.nlink > ffi::c_uint::MAX as u64
        || attributes.rdev > u32::MAX as u64
        || (!matches!(file_type, bindings::S_IFCHR | bindings::S_IFBLK) && attributes.rdev != 0)
        || attributes.blksize == 0
        || attributes.blksize > u32::MAX as u64
        || attributes.r#gen > u32::MAX as u64
        || attributes.atime_sec > bindings::time64_t::MAX as u64
        || attributes.mtime_sec > bindings::time64_t::MAX as u64
        || attributes.ctime_sec > bindings::time64_t::MAX as u64
        || attributes.btime_sec > bindings::time64_t::MAX as u64
        || attributes.atime_nsec >= NSEC_PER_SEC
        || attributes.mtime_nsec >= NSEC_PER_SEC
        || attributes.ctime_nsec >= NSEC_PER_SEC
        || attributes.btime_nsec >= NSEC_PER_SEC
    {
        return Err(protocol_error());
    }
    Ok(())
}

pub(super) fn inode_number(qid: Qid) -> Result<ffi::c_ulong> {
    let number = qid.path.checked_add(1).ok_or_else(protocol_error)?;
    if number > ffi::c_ulong::MAX as u64 {
        return Err(EOVERFLOW);
    }
    Ok(number as ffi::c_ulong)
}

pub(super) fn expected_qid_type(mode: u32) -> Result<u8> {
    match mode & bindings::S_IFMT {
        bindings::S_IFDIR => Ok(QID_TYPE_DIR),
        bindings::S_IFLNK => Ok(QID_TYPE_SYMLINK),
        bindings::S_IFREG
        | bindings::S_IFCHR
        | bindings::S_IFBLK
        | bindings::S_IFIFO
        | bindings::S_IFSOCK => Ok(QID_TYPE_FILE),
        _ => Err(errno!(EOPNOTSUPP)),
    }
}

/// Convert Linux's external `new_encode_dev()` u32 layout into internal dev_t.
pub(super) fn wire_rdev_to_dev(encoded: u64) -> bindings::dev_t {
    let encoded = encoded as u32;
    let major = (encoded & 0x000f_ff00) >> 8;
    let minor = (encoded & 0xff) | ((encoded >> 12) & 0x000f_ff00);
    ((major << 20) | minor) as bindings::dev_t
}

pub(super) fn creation_gid(parent: &InodeRef<'_>) -> Result<u32> {
    let namespace = parent.super_block()?.user_namespace_ptr();
    if namespace.is_null() {
        return Err(EIO);
    }
    let kgid = if parent.mode() & bindings::S_ISGID != 0 {
        parent.gid()
    } else {
        current_fsgid()
    };
    let gid = unsafe { compat::from_kgid(namespace, kgid) };
    if gid == u32::MAX {
        Err(EOVERFLOW)
    } else {
        Ok(gid)
    }
}

fn current_fsgid() -> bindings::kgid_t {
    // SAFETY: get_current() always returns the calling task. Its subjective
    // credential pointer is stable for this immediate snapshot.
    let task = unsafe { bindings::get_current() };
    let credential = unsafe { (*task).cred };
    unsafe { (*credential).fsgid }
}

/// Run a pointer-returning callback, encoding errors as `ERR_PTR`.
pub(super) fn from_ptr_result<T>(body: impl FnOnce() -> Result<*mut T>) -> *mut T {
    body().unwrap_or_else(Error::to_ptr)
}

/// Const-pointer form of [`from_ptr_result`], used by `get_link`.
pub(super) fn from_const_ptr_result(
    body: impl FnOnce() -> Result<*const ffi::c_char>,
) -> *const ffi::c_char {
    body().unwrap_or_else(|error| error.to_ptr::<ffi::c_char>().cast_const())
}

/// Map an unclassified fault-handler error to OOM or SIGBUS.
pub(super) fn from_fault_result(
    body: impl FnOnce() -> Result<bindings::vm_fault_t>,
) -> bindings::vm_fault_t {
    body().unwrap_or_else(|error| {
        if error == ENOMEM {
            bindings::vm_fault_reason_VM_FAULT_OOM
        } else {
            bindings::vm_fault_reason_VM_FAULT_SIGBUS
        }
    })
}

pub(super) fn protocol_error() -> Error {
    errno!(EPROTO)
}

pub(super) fn monotonic_now_ns() -> u64 {
    // This clock accessor has no caller-side safety preconditions.
    unsafe { bindings::ktime_get_mono_fast_ns() }
}
