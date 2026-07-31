//! VFS bridge for the native ZeroFS mount.
//!
//! The Rust-for-Linux filesystem abstractions in the supported kernels do not
//! yet expose an implementation-side filesystem API. The C callbacks therefore
//! enter through generated bindings, but raw kiocb, iterator, and netfslib state
//! is immediately converted to typed adapters before reaching the filesystem
//! logic.

use core::{
    cell::UnsafeCell,
    ffi::c_void,
    marker::PhantomData,
    mem::size_of,
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering, fence},
};

use kernel::{
    alloc::{KBox, KVec, flags::GFP_KERNEL},
    bindings, ffi, new_spinlock,
    prelude::*,
    sync::{
        Mutex, SpinLock,
        aref::{ARef, AlwaysRefCounted},
    },
    types::Opaque,
};

use crate::{
    client::{Client, REBIND_CREDENTIAL_IDENTITY_WORDS, RebindCredentials},
    netfs::{abi as netfs, io::RequestPrivate},
    protocol::Stat,
};

mod attributes;
mod compat;
mod file;
mod inode;
mod io;
mod locks;
mod mount;
mod namespace;
mod netfs_ops;
mod readdir;
mod remote;

use attributes::protocol_error;
use file::{
    zerofs_fallocate, zerofs_filemap_fault, zerofs_filemap_map_pages, zerofs_flush, zerofs_fsync,
    zerofs_llseek, zerofs_mmap, zerofs_open, zerofs_page_mkwrite, zerofs_read_iter, zerofs_release,
    zerofs_vma_close, zerofs_vma_open, zerofs_write_iter,
};
use io::{AttributeRefresh, InodeRef, MappingRef, SetattrRequest};
use locks::{zerofs_flock, zerofs_lock};
pub(crate) use mount::{begin_shutdown, fill_super_with_endpoint};
use mount::{
    zerofs_d_init, zerofs_d_release, zerofs_d_revalidate, zerofs_d_weak_revalidate,
    zerofs_evict_inode, zerofs_put_super, zerofs_show_options, zerofs_statfs, zerofs_sync_fs,
    zerofs_umount_begin,
};
use namespace::{
    zerofs_atomic_open, zerofs_create, zerofs_get_link, zerofs_getattr, zerofs_link, zerofs_lookup,
    zerofs_mkdir, zerofs_mknod, zerofs_permission, zerofs_rename, zerofs_rmdir, zerofs_setattr,
    zerofs_symlink, zerofs_unlink,
};
pub(crate) use netfs_ops::{run_netfs_read_subrequest, run_netfs_write_subrequest};
use netfs_ops::{
    zerofs_netfs_begin_writeback, zerofs_netfs_free_group, zerofs_netfs_free_request,
    zerofs_netfs_init_request, zerofs_netfs_issue_read, zerofs_netfs_issue_write,
    zerofs_netfs_post_modify, zerofs_netfs_prepare_read,
};
use readdir::zerofs_iterate_shared;

const ZEROFS_MAGIC: ffi::c_ulong = 0x5a45_524f;
const READ_REPLY_OVERHEAD: u32 = 11;
// A Treaddirattr record carries substantially more wire metadata than the
// linux_dirent64 emitted to userspace. A 256 KiB batch therefore roughly fills
// a conventional 32 KiB getdents buffer for directories with short names,
// avoiding several serial RPCs on high-latency connections.
const READDIR_BATCH: u32 = 256 * 1024;
const LOOKUP_RCU: ffi::c_uint = 1 << 8;
const SB_RDONLY: ffi::c_ulong = 1 << 0;
const SB_NOSUID: ffi::c_ulong = 1 << 1;
const SB_NODEV: ffi::c_ulong = 1 << 2;
const ST_RDONLY: ffi::c_long = 1 << 0;
const ST_NOSUID: ffi::c_long = 1 << 1;
const ST_NODEV: ffi::c_long = 1 << 2;
const ST_VALID: ffi::c_long = 1 << 5;
const NSEC_PER_SEC: u64 = 1_000_000_000;
const RELAXED_CACHE_REVALIDATE_NS: u64 = NSEC_PER_SEC;
/// Poll interval for the client-side `F_SETLKW` wait, mirroring the userspace
/// mount's `LOCK_POLL`. The server answers a conflicting blocking request
/// immediately rather than queueing it, so the wait belongs to the client.
const LOCK_RETRY_POLL_MS: u32 = 50;
/// Avoid shrinking ordinary sequential readahead below a useful baseline.
const MIN_READAHEAD_WINDOW_BYTES: usize = 1024 * 1024;
/// Bound speculative page-cache and per-request memory even with a very large
/// negotiated msize.
const MAX_READAHEAD_WINDOW_BYTES: usize = 4 * 1024 * 1024;
/// Deeper pipelining stops helping one stream once its request window reaches
/// this many concurrent subrequests, and bounding it also bounds BDI memory
/// pressure.
const MAX_READAHEAD_DEPTH: usize = 8;
const READDIR_HINT_CAPACITY: usize = 512;
const READDIR_HINT_PROBES: usize = 8;
// One credential-bound fid per access=user identity that reaches the inode.
const BOUND_FID_CACHE_CAPACITY: usize = 8;
// Keep ordinary buffered and mmap writes from reopening a new netfs group for
// every file descriptor held by the same access=user identity.
const WRITEBACK_GROUP_CACHE_CAPACITY: usize = BOUND_FID_CACHE_CAPACITY;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const _: () = assert!(READDIR_HINT_CAPACITY.is_power_of_two());
const _: () = assert!(READDIR_HINT_PROBES.is_power_of_two());
const _: () = assert!(READDIR_HINT_PROBES <= READDIR_HINT_CAPACITY);
const LOOKUP_REVAL: ffi::c_uint = 1 << 7;
// These RWF-derived IOCB bits are macros and therefore absent from bindgen's
// generated constants for this target.
const IOCB_DSYNC: ffi::c_int = 1 << 1;
const IOCB_SYNC: ffi::c_int = 1 << 2;
const IOCB_NOWAIT: ffi::c_int = 1 << 3;
// include/linux/fs.h macro, not emitted by bindgen.
const FMODE_CREATED: bindings::fmode_t = 1 << 20;
const IOCB_APPEND: ffi::c_int = 1 << 4;
// FMODE_WRITE is a sparse-style cast macro and therefore absent from bindgen.
const FMODE_WRITE: bindings::fmode_t = 1 << 1;
const MMAP_INVALIDATION_RETRIES: usize = 4;
const MODE_PERMISSIONS: u32 = 0o7777;
const INODE_CACHE_NAME: &[u8] = b"zerofs_inode_cache\0";

/// Per-mount authority policy for remote metadata and file contents.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consistency {
    /// Reuse coherent observations for one second and use the page cache.
    Relaxed,
    /// Revalidate path-addressable remote state and issue file I/O unbuffered.
    Strict,
}

#[pin_data]
struct DentryState {
    // Rare records and invalidations serialize; path-walk readers stay lockless.
    #[pin]
    writer: SpinLock<()>,
    sequence: AtomicU64,
    observed_ns: AtomicU64,
    invalidated_ns: AtomicU64,
    identity_len: AtomicU64,
    identity_words: [AtomicU64; REBIND_CREDENTIAL_IDENTITY_WORDS],
}

impl DentryState {
    fn new() -> impl PinInit<Self> {
        pin_init!(Self {
            writer <- new_spinlock!(()),
            sequence: AtomicU64::new(0),
            observed_ns: AtomicU64::new(0),
            invalidated_ns: AtomicU64::new(0),
            identity_len: AtomicU64::new(0),
            identity_words: [const { AtomicU64::new(0) }; REBIND_CREDENTIAL_IDENTITY_WORDS],
        })
    }

    fn record(&self, credentials: &RebindCredentials, observed_ns: u64) {
        let _writer = self.writer.lock();
        // A lookup is timestamped before its RPC, while a namespace mutation is
        // timestamped after commit. A late lookup must not make stale state
        // fresh again.
        if observed_ns <= self.observed_ns.load(Ordering::Relaxed)
            || observed_ns <= self.invalidated_ns.load(Ordering::Acquire)
        {
            return;
        }
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel);
        let (words, length) = credentials.identity_words();
        for (destination, word) in self.identity_words.iter().zip(words) {
            destination.store(word, Ordering::Relaxed);
        }
        self.identity_len.store(length as u64, Ordering::Relaxed);
        self.observed_ns.store(observed_ns, Ordering::Relaxed);
        self.sequence
            .store(sequence.wrapping_add(2), Ordering::Release);
    }

    fn invalidate(&self, invalidated_ns: u64) {
        let _writer = self.writer.lock();
        if invalidated_ns <= self.invalidated_ns.load(Ordering::Relaxed) {
            return;
        }
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel);
        self.invalidated_ns.store(invalidated_ns, Ordering::Relaxed);
        self.sequence
            .store(sequence.wrapping_add(2), Ordering::Release);
    }

    fn is_fresh_for(
        &self,
        credentials: &RebindCredentials,
        now_ns: u64,
        force_revalidate: bool,
    ) -> bool {
        if force_revalidate {
            return false;
        }
        let (words, length) = credentials.identity_words();
        for _ in 0..4 {
            let before = self.sequence.load(Ordering::Acquire);
            if before & 1 != 0 {
                core::hint::spin_loop();
                continue;
            }
            let stored_length = self.identity_len.load(Ordering::Relaxed) as usize;
            let observed_ns = self.observed_ns.load(Ordering::Relaxed);
            let invalidated_ns = self.invalidated_ns.load(Ordering::Acquire);
            let identity_matches = stored_length == length
                && self
                    .identity_words
                    .iter()
                    .zip(words)
                    .all(|(stored, expected)| stored.load(Ordering::Relaxed) == expected);
            // Order the payload reads before the sequence check.
            fence(Ordering::Acquire);
            if before == self.sequence.load(Ordering::Acquire) {
                return identity_matches
                    && observed_ns != 0
                    && observed_ns > invalidated_ns
                    && now_ns.wrapping_sub(observed_ns) < RELAXED_CACHE_REVALIDATE_NS;
            }
        }
        false
    }
}

fn extend_hint_hash(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Hash a hint's directory entry, deliberately excluding the recording
/// identity.
///
/// Every identity's hint for one entry therefore shares a neighborhood, which
/// is what lets a namespace mutation drop them all with a bounded probe instead
/// of a full-table scan. Identity is still compared on every match, so one
/// user's hint is never served to another.
fn readdir_hint_hash(parent_inode: u64, name: &[u8]) -> u64 {
    let mut hash = extend_hint_hash(FNV_OFFSET_BASIS, &parent_inode.to_le_bytes());
    hash = extend_hint_hash(hash, &(name.len() as u64).to_le_bytes());
    extend_hint_hash(hash, name)
}

struct ReaddirHint {
    key_hash: u64,
    parent_inode: u64,
    name: KVec<u8>,
    attributes: Stat,
    observed_ns: u64,
    generation: u64,
    credentials: RebindCredentials,
}

struct ReaddirHintCache {
    entries: KVec<Option<KBox<ReaddirHint>>>,
    next_replacement: usize,
}

impl ReaddirHintCache {
    fn new() -> Result<Self> {
        let mut entries = KVec::with_capacity(READDIR_HINT_CAPACITY, GFP_KERNEL)?;
        for _ in 0..READDIR_HINT_CAPACITY {
            // The capacity above covers every push, so this cannot allocate.
            entries.push_within_capacity(None).map_err(|_| EOVERFLOW)?;
        }
        Ok(Self {
            entries,
            next_replacement: 0,
        })
    }

    /// Map a bounded probe into the key's power-of-two hash neighborhood.
    fn slot(hash: u64, probe: usize) -> usize {
        (hash as usize).wrapping_add(probe) & (READDIR_HINT_CAPACITY - 1)
    }

    fn insert(&mut self, hint: KBox<ReaddirHint>) {
        let mut empty = None;
        let mut recyclable = None;
        for probe in 0..READDIR_HINT_PROBES {
            let index = Self::slot(hint.key_hash, probe);
            match self.entries[index].as_ref() {
                Some(existing)
                    if existing.key_hash == hint.key_hash
                        && existing.parent_inode == hint.parent_inode
                        && existing.name.as_slice() == hint.name.as_slice()
                        && existing.credentials.eq(&hint.credentials) =>
                {
                    // Deduplicate repeated getdents passes and always retain
                    // the newest observation for this exact key.
                    self.entries[index] = Some(hint);
                    return;
                }
                Some(existing)
                    if existing.generation != hint.generation && recyclable.is_none() =>
                {
                    recyclable = Some(index);
                }
                None if empty.is_none() => empty = Some(index),
                _ => {}
            }
        }

        // This is a best-effort cache: bounded probing keeps lookup O(1).
        // When one hash neighborhood is saturated, replace within that same
        // neighborhood so the entry remains discoverable by `take`. An entry
        // predating the last fence survived every scoped drop since, so it is
        // the least useful thing in the neighborhood to evict.
        let replacement = empty.or(recyclable).unwrap_or_else(|| {
            Self::slot(
                hint.key_hash,
                self.next_replacement & (READDIR_HINT_PROBES - 1),
            )
        });
        self.entries[replacement] = Some(hint);
        self.next_replacement = self.next_replacement.wrapping_add(1);
    }

    fn take(
        &mut self,
        parent_inode: u64,
        name: &[u8],
        credentials: &RebindCredentials,
        now_ns: u64,
    ) -> Option<(Stat, u64)> {
        let key_hash = readdir_hint_hash(parent_inode, name);
        for probe in 0..READDIR_HINT_PROBES {
            let entry = &mut self.entries[Self::slot(key_hash, probe)];
            let stale = entry.as_ref().is_some_and(|hint| {
                now_ns.wrapping_sub(hint.observed_ns) >= RELAXED_CACHE_REVALIDATE_NS
            });
            if stale {
                *entry = None;
                continue;
            }

            let matches = entry.as_ref().is_some_and(|hint| {
                hint.key_hash == key_hash
                    && hint.parent_inode == parent_inode
                    && hint.name.as_slice() == name
                    && hint.credentials.eq(credentials)
            });
            if matches {
                let hint = entry.take()?;
                return Some((hint.attributes, hint.observed_ns));
            }
        }
        None
    }

    /// Drop every hint whose cached Stat describes one of these objects.
    fn drop_objects(&mut self, objects: &[u64]) {
        for entry in self.entries.iter_mut() {
            let affected = entry
                .as_ref()
                .is_some_and(|hint| objects.contains(&hint.attributes.qid.path));
            if affected {
                *entry = None;
            }
        }
    }

    /// Drop every recording identity's hint for one directory entry.
    fn drop_entry(&mut self, parent_inode: u64, name: &[u8]) {
        // `insert` only ever places a hint inside its own neighborhood, and the
        // key hash covers exactly the entry, so this probe sees all of them.
        let key_hash = readdir_hint_hash(parent_inode, name);
        for probe in 0..READDIR_HINT_PROBES {
            let entry = &mut self.entries[Self::slot(key_hash, probe)];
            let affected = entry.as_ref().is_some_and(|hint| {
                hint.parent_inode == parent_inode && hint.name.as_slice() == name
            });
            if affected {
                *entry = None;
            }
        }
    }
}

#[pin_data]
struct MountState {
    client: Client,
    consistency: Consistency,
    #[pin]
    readdir_hints: Mutex<ReaddirHintCache>,
    hint_generation: AtomicU64,
    teardown_started: AtomicBool,
}

#[derive(Clone, Copy)]
struct DataCacheBaseline {
    size: u64,
    mtime_sec: u64,
    mtime_nsec: u64,
    ctime_sec: u64,
    ctime_nsec: u64,
}

impl DataCacheBaseline {
    fn from_stat(attributes: &Stat) -> Self {
        Self {
            size: attributes.size,
            mtime_sec: attributes.mtime_sec,
            mtime_nsec: attributes.mtime_nsec,
            ctime_sec: attributes.ctime_sec,
            ctime_nsec: attributes.ctime_nsec,
        }
    }

    fn matches(&self, inode: &InodeRef<'_>, remote: &Stat) -> bool {
        let local_size = inode.size();
        self.size == remote.size
            && self.mtime_sec == remote.mtime_sec
            && self.mtime_nsec == remote.mtime_nsec
            && self.ctime_sec == remote.ctime_sec
            && self.ctime_nsec == remote.ctime_nsec
            && local_size >= 0
            && local_size as u64 == remote.size
    }
}

#[derive(Clone, Copy)]
struct CachedAttributes {
    stat: Stat,
    // Updated only after the page cache has been reconciled with this Stat.
    data_baseline: DataCacheBaseline,
    // Publication and invalidation order for the two cached Stat subsets.
    metadata_watermark_ns: u64,
    data_watermark_ns: u64,
    // Also advances for data observations not yet reconciled with the mapping.
    data_ordering_watermark_ns: u64,
    metadata_valid: bool,
    data_valid: bool,
}

#[pin_data]
struct InodeState {
    #[pin]
    cached_attributes: Mutex<CachedAttributes>,
    // Zero means stale. A nonzero release-published timestamp makes the
    // i_lock-protected shared metadata available to RCU permission checks.
    metadata_fresh_ns: AtomicU64,
    #[pin]
    bound_fids: Mutex<BoundFidCache>,
    #[pin]
    writeback_groups: Mutex<WritebackGroupCache>,
    // Held across the revalidation getattr so a burst of readers issues one
    // RPC without every reader waiting on i_rwsem for its duration.
    #[pin]
    data_revalidate: Mutex<()>,
    last_data_revalidate_ns: AtomicU64,
}

/// Coherency gate shared by VMAs derived from one relaxed-consistency mapping.
struct MmapRevalidation {
    state: AtomicU64,
}

/// Revalidation outcome for the current generation.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RevalidateStatus {
    Ready,
    Revalidated,
    Pending,
    /// The next fault reports the corresponding MM fault and opens a new
    /// generation for later retries.
    Failed(u16),
}

enum MmapRefresh {
    Complete,
    Retry,
}

impl RevalidateStatus {
    fn is_ready(self) -> bool {
        matches!(self, Self::Ready | Self::Revalidated)
    }

    fn from_error(error: Error) -> Self {
        let errno = -error.to_errno();
        // Pack a positive errno; use EIO outside the state encoding.
        if errno > 0 && errno < i32::from(Self::PENDING_BITS) {
            Self::Failed(errno as u16)
        } else {
            Self::Failed(bindings::EIO as u16)
        }
    }

    fn fault(self) -> bindings::vm_fault_t {
        match self {
            Self::Failed(errno) if errno == bindings::ENOMEM as u16 => {
                bindings::vm_fault_reason_VM_FAULT_OOM
            }
            _ => bindings::vm_fault_reason_VM_FAULT_SIGBUS,
        }
    }

    const READY_BITS: u16 = 0;
    const REVALIDATED_BITS: u16 = u16::MAX - 1;
    const PENDING_BITS: u16 = u16::MAX;

    fn from_bits(bits: u16) -> Self {
        match bits {
            Self::READY_BITS => Self::Ready,
            Self::REVALIDATED_BITS => Self::Revalidated,
            Self::PENDING_BITS => Self::Pending,
            errno => Self::Failed(errno),
        }
    }

    fn to_bits(self) -> u16 {
        match self {
            Self::Ready => Self::READY_BITS,
            Self::Revalidated => Self::REVALIDATED_BITS,
            Self::Pending => Self::PENDING_BITS,
            Self::Failed(errno) => errno,
        }
    }
}

/// Packed status and generation; the generation rejects stale completions.
#[derive(Clone, Copy, PartialEq, Eq)]
struct RevalidateState(u64);

impl RevalidateState {
    const STATUS_MASK: u64 = u16::MAX as u64;
    const GENERATION_STEP: u64 = 1 << 16;

    fn status(self) -> RevalidateStatus {
        RevalidateStatus::from_bits((self.0 & Self::STATUS_MASK) as u16)
    }

    fn with_status(self, status: RevalidateStatus) -> Self {
        Self((self.0 & !Self::STATUS_MASK) | u64::from(status.to_bits()))
    }

    fn next_generation(self, status: RevalidateStatus) -> Self {
        Self(self.0.wrapping_add(Self::GENERATION_STEP)).with_status(status)
    }

    fn same_generation_as(self, other: Self) -> bool {
        (self.0 ^ other.0) & !Self::STATUS_MASK == 0
    }
}

impl MmapRevalidation {
    fn new(status: RevalidateStatus) -> Self {
        Self {
            state: AtomicU64::new(u64::from(status.to_bits())),
        }
    }

    fn load(&self) -> RevalidateState {
        RevalidateState(self.state.load(Ordering::Acquire))
    }

    fn publish(&self, current: RevalidateState, new: RevalidateState) -> bool {
        self.state
            .compare_exchange(current.0, new.0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn complete(&self, pending: RevalidateState) {
        let completed = pending.with_status(RevalidateStatus::Revalidated);
        loop {
            let state = self.load();
            // A newer generation, or a success another task already published,
            // outranks this one.
            if !state.same_generation_as(pending) || state.status().is_ready() {
                return;
            }
            if self.publish(state, completed) {
                return;
            }
        }
    }

    /// Record a failure only while this generation remains pending.
    fn fail(&self, pending: RevalidateState, error: Error) {
        self.publish(
            pending,
            pending.with_status(RevalidateStatus::from_error(error)),
        );
    }
}

struct BoundFidEntry {
    fid: u32,
    credentials: RebindCredentials,
}

struct BoundFidCache {
    // Entries are append-only until inode eviction. A VFS callback's inode
    // reference therefore also pins every borrowed fid, so selecting a raw
    // number cannot race a clunk. This deliberately models 9P access=user:
    // each credential obtains a long-lived path capability after permission
    // checks during path acquisition or open.
    entries: KVec<BoundFidEntry>,
}

struct WritebackGroupCache {
    entries: KVec<ARef<FileState>>,
}

struct BoundFidUse {
    fid: u32,
    cached: bool,
}

impl BoundFidUse {
    fn cleanup(self, client: &Client) -> Result<()> {
        if self.cached {
            Ok(())
        } else {
            client.clunk(self.fid)
        }
    }
}

struct LookupAttributes {
    stat: Stat,
    child_fid: u32,
}

struct OpenedCreation {
    stat: Stat,
    fid: u32,
    iounit: u32,
    parent_cleanup: Result<()>,
}

impl BoundFidCache {
    fn new() -> Self {
        Self {
            entries: KVec::new(),
        }
    }

    /// The capability this identity already retains for this inode, if any.
    ///
    /// One connection makes every retained fid equally reachable, so the first
    /// credential match is as good as any other.
    fn find(&self, credentials: &RebindCredentials) -> Option<u32> {
        self.entries
            .iter()
            .find(|entry| &entry.credentials == credentials)
            .map(|entry| entry.fid)
    }

    fn take_all(&mut self) -> KVec<BoundFidEntry> {
        core::mem::replace(&mut self.entries, KVec::new())
    }
}

impl WritebackGroupCache {
    fn new() -> Self {
        Self {
            entries: KVec::new(),
        }
    }

    fn select(&mut self, file_state: &FileState) -> ARef<FileState> {
        if let Some(retained) = self
            .entries
            .iter()
            .find(|retained| retained.credentials() == file_state.credentials())
        {
            return retained.clone();
        }

        let selected = ARef::from(file_state);
        if self.entries.len() < WRITEBACK_GROUP_CACHE_CAPACITY {
            // Cache pressure and allocation failure affect coalescing only;
            // the current open remains a valid writeback capability.
            let _ = self.entries.push(selected.clone(), GFP_KERNEL);
        }
        selected
    }
}

/// Per-open remote capability retained by netfslib dirty-folio groups.
///
/// `group` is first so netfslib can retain the originating `access=user`
/// identity after the struct file closes.  The final group reference clunks
/// every capability this open holds and frees this allocation.
#[repr(C)]
struct FileState {
    group: UnsafeCell<netfs::netfs_group>,
    mount: *const MountState,
    fid: u32,
    iounit: u32,
    // Strict opens and O_WRONLY|O_DIRECT opens bypass the page cache. The
    // latter must retain that behavior if userspace later clears O_DIRECT,
    // because its remote capability was deliberately not upgraded for
    // read-modify-write.
    force_unbuffered: bool,
    credentials: RebindCredentials,
}

impl FileState {
    fn group_ptr(&self) -> *mut netfs::netfs_group {
        self.group.get()
    }

    fn mount(&self) -> &MountState {
        // SAFETY: A FileState is created from a mounted MountState, and the
        // superblock waits for every file/group reference before freeing it.
        unsafe { &*self.mount }
    }

    fn fid(&self) -> u32 {
        self.fid
    }

    fn iounit(&self) -> u32 {
        self.iounit
    }

    fn force_unbuffered(&self) -> bool {
        self.force_unbuffered
    }

    fn credentials(&self) -> &RebindCredentials {
        &self.credentials
    }
}

// SAFETY: FileState's Rust fields are immutable after publication. Netfslib
// mutates only the UnsafeCell-wrapped group refcount, whose atomic operations
// synchronize the allocation's lifetime across worker contexts.
unsafe impl Send for FileState {}
unsafe impl Sync for FileState {}

// SAFETY: Every ZeroFS netfs request uses its private slot exclusively for one
// retained FileState reference installed and removed through this descriptor.
static REQUEST_FILE_STATE: RequestPrivate<FileState> = unsafe { RequestPrivate::new() };

// SAFETY: Every published FileState has an initialized group reference count.
// Both netfslib and ARef operate on that same refcount and route the final
// release through zerofs_netfs_free_group.
unsafe impl AlwaysRefCounted for FileState {
    fn inc_ref(&self) {
        // SAFETY: The shared reference proves at least one live reference
        // while this additional reference is acquired.
        unsafe {
            bindings::refcount_inc(ptr::addr_of_mut!((*self.group_ptr()).ref_));
        }
    }

    unsafe fn dec_ref(state: ptr::NonNull<Self>) {
        // SAFETY: ARef transfers one live reference to this decrement.
        let group = unsafe { state.as_ref().group_ptr() };
        let release = unsafe { bindings::refcount_dec_and_test(ptr::addr_of_mut!((*group).ref_)) };
        if release {
            // SAFETY: This is the unique zero transition.
            unsafe { zerofs_netfs_free_group(group) };
        }
    }
}

fn new_file_state(
    mount: &MountState,
    fid: u32,
    iounit: u32,
    force_unbuffered: bool,
    credentials: RebindCredentials,
) -> Result<KBox<FileState>> {
    let state = KBox::new(
        FileState {
            group: UnsafeCell::new(netfs::netfs_group {
                ref_: bindings::refcount_t::default(),
                free: Some(zerofs_netfs_free_group),
            }),
            mount: mount as *const MountState,
            fid,
            iounit,
            force_unbuffered,
            credentials,
        },
        GFP_KERNEL,
    )
    .map_err(|_| ENOMEM)?;
    // SAFETY: This allocation is unpublished and exclusively owned here.
    unsafe {
        bindings::refcount_set(ptr::addr_of_mut!((*state.group.get()).ref_), 1);
    }
    Ok(state)
}

/// Filesystem inode wrapper required by netfslib's `container_of()` contract.
///
/// `netfs_inode.inode` is itself the first field, so a VFS inode pointer, a
/// netfs inode pointer, and this allocation all have the same address.
#[repr(C)]
struct ZeroFsInode {
    netfs: netfs::netfs_inode,
}

impl MountState {
    fn is_relaxed(&self) -> bool {
        self.consistency == Consistency::Relaxed
    }

    fn begin_teardown(&self) {
        self.teardown_started.store(true, Ordering::Release);
    }

    fn teardown_started(&self) -> bool {
        self.teardown_started.load(Ordering::Acquire)
    }

    fn remember_readdir_hint(
        &self,
        parent_inode: u64,
        name: &[u8],
        attributes: Stat,
        observed_ns: u64,
        generation: u64,
        credentials: &RebindCredentials,
    ) -> Result<()> {
        if !self.is_relaxed() {
            return Ok(());
        }
        if self.hint_generation.load(Ordering::Acquire) != generation {
            return Ok(());
        }

        let mut owned_name = KVec::from_elem(0u8, name.len(), GFP_KERNEL)?;
        owned_name.as_mut_slice().copy_from_slice(name);
        let hint = KBox::new(
            ReaddirHint {
                key_hash: readdir_hint_hash(parent_inode, name),
                parent_inode,
                name: owned_name,
                attributes,
                observed_ns,
                generation,
                credentials: credentials.clone(),
            },
            GFP_KERNEL,
        )?;

        let mut hints = self.readdir_hints.lock();
        if self.hint_generation.load(Ordering::Acquire) == generation {
            hints.insert(hint);
        }
        Ok(())
    }

    fn take_readdir_hint(
        &self,
        parent_inode: u64,
        name: &[u8],
        credentials: &RebindCredentials,
        now_ns: u64,
    ) -> Option<(Stat, u64)> {
        if !self.is_relaxed() {
            return None;
        }
        let mut hints = self.readdir_hints.lock();
        // Linearize lookup against invalidation while holding the cache lock. A
        // scoped drop that completed before this point has already removed
        // every entry its mutation affected; one racing afterward overlaps this
        // lookup and may legitimately be observed by the next path walk.
        hints.take(parent_inode, name, credentials, now_ns)
    }

    /// Stop a readdir reply that was already in flight from repopulating hints
    /// a committed mutation just made stale.
    ///
    /// Entries already in the table are removed by the scoped invalidators
    /// below, so this only rejects records the server produced before the
    /// mutation. Callers must fence before dropping, or a reply that raced the
    /// drop would reinstate what it removed.
    fn fence_readdir_replies(&self) {
        self.hint_generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Drop hints describing these remote objects.
    ///
    /// The caller must have committed a mutation that changes only these
    /// objects' own attributes: no directory's set of names may have changed,
    /// or an entry hint would survive the name it describes.
    fn invalidate_object_hints(&self, objects: &[u64]) {
        let mut hints = self.readdir_hints.lock();
        hints.drop_objects(objects);
    }

    /// Drop hints for one directory entry, whichever identity recorded it.
    fn invalidate_entry_hint(&self, parent_inode: u64, name: &[u8]) {
        let mut hints = self.readdir_hints.lock();
        hints.drop_entry(parent_inode, name);
    }

    fn invalidate_rename_hints(
        &self,
        old_parent_inode: u64,
        old_name: &[u8],
        new_parent_inode: u64,
        new_name: &[u8],
        objects: &[u64],
    ) {
        let mut hints = self.readdir_hints.lock();
        hints.drop_entry(old_parent_inode, old_name);
        hints.drop_entry(new_parent_inode, new_name);
        hints.drop_objects(objects);
    }
}

struct InodeWriteGuard<'a> {
    inode: *mut bindings::inode,
    semaphore: *mut bindings::rw_semaphore,
    _inode: PhantomData<&'a ()>,
}

/// Exclusive inode ownership with no direct I/O still active.
struct IoExcludedInodeGuard<'a> {
    inner: InodeWriteGuard<'a>,
}

/// Exclusive inode ownership borrowed from the VFS callback contract.
///
/// This marker does not unlock on drop; VFS remains the lock owner.
struct CallbackInodeWriteGuard<'a, State> {
    inode: *mut bindings::inode,
    _inode: PhantomData<&'a ()>,
    _state: PhantomData<State>,
}

enum DirectIoMayBeActive {}
enum DirectIoExcluded {}

/// Change a netfs inode from direct-I/O mode back to buffered/mutation mode.
///
/// # Safety
///
/// The caller must hold this exact inode's i_rwsem exclusively, preventing a
/// new `netfs_start_io_direct()` until the exclusion is released.
unsafe fn block_direct_io_excluded(inode_ptr: *mut bindings::inode) -> Result<()> {
    let context = inode_ptr.cast::<netfs::netfs_inode>();
    let direct_mask = 1usize << netfs::NETFS_ICTX_ODIRECT;
    // SAFETY: The caller supplies exclusive i_rwsem ownership for this inode.
    let direct_mode = unsafe { (*context).flags as usize & direct_mask != 0 };
    let finished = unsafe { bindings::inode_dio_finished(inode_ptr) };
    if !direct_mode && finished {
        return Ok(());
    }

    if direct_mode {
        unsafe {
            bindings::clear_bit(
                netfs::NETFS_ICTX_ODIRECT as ffi::c_ulong,
                ptr::addr_of_mut!((*context).flags),
            );
        }
    }
    if !finished {
        unsafe {
            bindings::inode_dio_wait_interruptible(inode_ptr);
        }
        if !unsafe { bindings::inode_dio_finished(inode_ptr) } {
            // Keep direct mode blocked after an interrupted transition. The
            // caller will release i_rwsem on error; leaving the bit clear
            // forces later direct I/O through netfslib's buffered-data
            // exclusion path instead of falsely publishing direct mode.
            return Err(ERESTARTSYS);
        }
    }
    Ok(())
}

impl InodeWriteGuard<'_> {
    /// Acquire an inode's exclusive write semaphore.
    fn acquire<'a>(inode: &InodeRef<'a>) -> Result<InodeWriteGuard<'a>> {
        let inode_ptr = inode.as_ptr();
        // SAFETY: The typed borrow pins the inode and its embedded i_rwsem
        // until the returned guard is dropped.
        let semaphore = unsafe { ptr::addr_of_mut!((*inode_ptr).i_rwsem) };
        // SAFETY: The semaphore is embedded in the pinned inode above and is
        // not held by this task yet.
        unsafe {
            bindings::down_write(semaphore);
        }
        Ok(InodeWriteGuard {
            inode: inode_ptr,
            semaphore,
            _inode: PhantomData,
        })
    }

    fn protects(&self, inode: &InodeRef<'_>) -> bool {
        self.inode == inode.as_ptr()
    }
}

impl IoExcludedInodeGuard<'_> {
    fn acquire<'a>(inode: &InodeRef<'a>) -> Result<IoExcludedInodeGuard<'a>> {
        let inner = InodeWriteGuard::acquire(inode)?;
        // SAFETY: inner holds this exact inode's i_rwsem exclusively.
        unsafe { block_direct_io_excluded(inner.inode)? };
        Ok(IoExcludedInodeGuard { inner })
    }

    fn protects(&self, inode: &InodeRef<'_>) -> bool {
        self.inner.protects(inode)
    }
}

impl<'a> CallbackInodeWriteGuard<'a, DirectIoMayBeActive> {
    /// Borrow the exclusive i_rwsem held by an ordinary setattr callback.
    ///
    /// # Safety
    ///
    /// VFS must hold this exact inode's i_rwsem exclusively for the complete
    /// marker lifetime.
    unsafe fn from_setattr(
        inode: &InodeRef<'a>,
    ) -> CallbackInodeWriteGuard<'a, DirectIoMayBeActive> {
        CallbackInodeWriteGuard {
            inode: inode.as_ptr(),
            _inode: PhantomData,
            _state: PhantomData,
        }
    }

    fn exclude_direct_io(self) -> Result<CallbackInodeWriteGuard<'a, DirectIoExcluded>> {
        // SAFETY: Construction proves the callback owns this inode's i_rwsem.
        unsafe { block_direct_io_excluded(self.inode)? };
        Ok(CallbackInodeWriteGuard {
            inode: self.inode,
            _inode: PhantomData,
            _state: PhantomData,
        })
    }
}

impl<State> CallbackInodeWriteGuard<'_, State> {
    fn protects(&self, inode: &InodeRef<'_>) -> bool {
        self.inode == inode.as_ptr()
    }

    /// Apply the callback's normalized attributes under its exclusive lock.
    ///
    /// # Safety
    ///
    /// `idmap` and `attributes` must belong to the same live setattr callback
    /// from which this guard was constructed.
    unsafe fn copy_attributes(
        &self,
        idmap: *mut bindings::mnt_idmap,
        attributes: &SetattrRequest<'_>,
    ) {
        unsafe {
            bindings::setattr_copy(idmap, self.inode, attributes.as_ptr());
        }
    }
}

impl Drop for InodeWriteGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: acquire() locked this exact semaphore and the guard's safety
        // contract keeps its containing inode live through this drop.
        unsafe {
            bindings::up_write(self.semaphore);
        }
    }
}

struct MappingInvalidateGuard<'a> {
    mapping: MappingRef<'a>,
    semaphore: *mut bindings::rw_semaphore,
}

impl MappingInvalidateGuard<'_> {
    /// Acquire an address space's exclusive invalidation semaphore.
    fn acquire<'a>(mapping: &MappingRef<'a>) -> MappingInvalidateGuard<'a> {
        // SAFETY: The typed borrow pins the mapping and its embedded
        // invalidate_lock until the returned guard is dropped.
        let semaphore = unsafe { ptr::addr_of_mut!((*mapping.as_ptr()).invalidate_lock) };
        unsafe {
            bindings::down_write(semaphore);
        }
        MappingInvalidateGuard {
            mapping: mapping.reborrow(),
            semaphore,
        }
    }

    /// # Safety
    ///
    /// The caller must also exclude writes and direct I/O for the inode owning
    /// this mapping.
    unsafe fn invalidate_all(&self) -> Result<()> {
        // SAFETY: This guard owns the matching mapping's invalidation lock.
        let status = unsafe {
            bindings::invalidate_inode_pages2_range(self.mapping.as_ptr(), 0, ffi::c_ulong::MAX)
        };
        if status < 0 {
            Err(Error::from_errno(status))
        } else {
            Ok(())
        }
    }

    /// # Safety
    ///
    /// The caller must also exclude writes and direct I/O for the inode owning
    /// this mapping.
    unsafe fn truncate_all(&self) {
        // SAFETY: This guard owns the matching mapping's invalidation lock.
        unsafe {
            bindings::truncate_inode_pages_range(
                self.mapping.as_ptr(),
                0,
                bindings::loff_t::MAX as _,
            );
        }
    }

    /// # Safety
    ///
    /// The caller must also exclude writes and direct I/O for the inode owning
    /// this mapping.
    unsafe fn invalidate_range(&self, position: u64, length: usize) -> Result<()> {
        if length == 0 {
            return Ok(());
        }
        let length = u64::try_from(length).map_err(|_| EFBIG)?;
        let end = position
            .checked_add(length - 1)
            .filter(|end| *end <= bindings::loff_t::MAX as u64)
            .ok_or_else(|| EFBIG)?;
        let page_mask = bindings::PAGE_SIZE as u64 - 1;
        let first = position & !page_mask;
        let last = end | page_mask;
        // SAFETY: This guard owns the matching mapping's invalidation lock.
        unsafe {
            bindings::truncate_inode_pages_range(
                self.mapping.as_ptr(),
                first as bindings::loff_t,
                last as _,
            );
        }
        Ok(())
    }
}

/// Setattr callback ownership after writeback has drained and direct I/O plus
/// mapping activity have been excluded for one regular inode.
struct CallbackDrainedMappingGuard<'a> {
    inode: InodeRef<'a>,
    // Drop the mapping exclusion before the callback-owned inode exclusion.
    mapping_guard: MappingInvalidateGuard<'a>,
    inode_guard: CallbackInodeWriteGuard<'a, DirectIoExcluded>,
}

impl<'a> CallbackDrainedMappingGuard<'a> {
    /// Drain writeback, then acquire mapping exclusion under the callback's
    /// already-exclusive inode ownership.
    fn drain_from_inode_guard(
        inode: InodeRef<'a>,
        mapping: &MappingRef<'a>,
        inode_guard: CallbackInodeWriteGuard<'a, DirectIoExcluded>,
    ) -> Result<Self> {
        if !inode_guard.protects(&inode) || inode.mapping_ptr() != mapping.as_ptr() {
            return Err(EINVAL);
        }
        let writeback = mapping.write_and_wait_all();
        if writeback < 0 {
            return Err(Error::from_errno(writeback));
        }
        Ok(Self {
            inode,
            mapping_guard: MappingInvalidateGuard::acquire(mapping),
            inode_guard,
        })
    }

    fn truncate_all(&self) {
        // SAFETY: This type owns matching inode/DIO and mapping exclusions.
        unsafe {
            self.mapping_guard.truncate_all();
        }
    }

    fn publish_truncated_size(&self, size: u64) -> Result<()> {
        if size > bindings::loff_t::MAX as u64 {
            return Err(EFBIG);
        }
        // SAFETY: This type owns matching inode/DIO and mapping exclusions, and
        // the bound above makes the loff_t conversion exact.
        unsafe {
            bindings::truncate_setsize(self.inode.as_ptr(), size as bindings::loff_t);
        }
        self.inode.set_remote_size(size);
        Ok(())
    }

    /// # Safety
    ///
    /// `idmap` and `attributes` must belong to this live setattr callback.
    unsafe fn copy_attributes(
        &self,
        idmap: *mut bindings::mnt_idmap,
        attributes: &SetattrRequest<'_>,
    ) {
        unsafe {
            self.inode_guard.copy_attributes(idmap, attributes);
        }
    }
}

/// Matching inode and mapping exclusion after successful writeback drainage,
/// required for destructive cache and size publication.
///
/// Construction verifies that both guards protect the same inode mapping, so
/// safe publication code cannot accidentally rely on unrelated ambient lock
/// variables.
struct DrainedCoherentMappingGuard<'a> {
    inode: InodeRef<'a>,
    // Drop the mapping exclusion before the enclosing inode exclusion.
    mapping_guard: MappingInvalidateGuard<'a>,
    _inode_guard: IoExcludedInodeGuard<'a>,
}

impl<'a> DrainedCoherentMappingGuard<'a> {
    /// Drain writeback, then acquire mapping exclusion while retaining the
    /// owned inode/DIO exclusion.
    fn drain_from_inode_guard(
        inode: InodeRef<'a>,
        mapping: MappingRef<'a>,
        inode_guard: IoExcludedInodeGuard<'a>,
    ) -> Result<Self> {
        if !inode_guard.protects(&inode) || inode.mapping_ptr() != mapping.as_ptr() {
            return Err(EINVAL);
        }
        let writeback = mapping.write_and_wait_all();
        if writeback < 0 {
            return Err(Error::from_errno(writeback));
        }
        let mapping_guard = MappingInvalidateGuard::acquire(&mapping);
        Ok(Self {
            inode,
            mapping_guard,
            _inode_guard: inode_guard,
        })
    }

    fn inode(&self) -> &InodeRef<'a> {
        &self.inode
    }

    fn invalidate_all(&self) -> Result<()> {
        // SAFETY: This type owns matching inode/DIO and mapping exclusions.
        unsafe { self.mapping_guard.invalidate_all() }
    }

    fn truncate_all(&self) {
        // SAFETY: This type owns matching inode/DIO and mapping exclusions.
        unsafe {
            self.mapping_guard.truncate_all();
        }
    }

    fn invalidate_range(&self, position: u64, length: usize) -> Result<()> {
        // SAFETY: This type owns matching inode/DIO and mapping exclusions.
        unsafe { self.mapping_guard.invalidate_range(position, length) }
    }

    fn refresh_size_from_stat(&self, attributes: &Stat) -> Result<()> {
        if attributes.size > bindings::loff_t::MAX as u64 {
            return Err(protocol_error());
        }
        // SAFETY: This guard proves matching inode and mapping exclusions.
        unsafe {
            self.inode.refresh_size_from_stat_locked(attributes);
        }
        Ok(())
    }

    fn refresh_size_after_invalidation(&self, size: u64) -> Result<()> {
        if size > bindings::loff_t::MAX as u64 {
            return Err(EFBIG);
        }
        // SAFETY: This guard proves matching inode and mapping exclusions.
        unsafe {
            self.inode.refresh_size_after_invalidation(size);
        }
        Ok(())
    }

    fn extend_size_to(&self, end: u64) -> Result<()> {
        if end > bindings::loff_t::MAX as u64 {
            return Err(EFBIG);
        }
        if end as bindings::loff_t > self.inode.size() {
            // SAFETY: This guard proves matching inode and mapping exclusions,
            // and the bound above makes the loff_t conversion exact.
            unsafe {
                bindings::truncate_setsize(self.inode.as_ptr(), end as bindings::loff_t);
            }
        }
        self.inode.extend_remote_size(end);
        Ok(())
    }

    /// Release mapping exclusion while retaining the inode/DIO exclusion.
    fn into_inode_guard(self) -> IoExcludedInodeGuard<'a> {
        let Self {
            inode,
            mapping_guard,
            _inode_guard,
        } = self;
        drop(mapping_guard);
        drop(inode);
        _inode_guard
    }
}

impl Drop for MappingInvalidateGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: acquire() locked this exact semaphore and the guard's safety
        // contract keeps its containing address space live through this drop.
        unsafe {
            bindings::up_write(self.semaphore);
        }
    }
}

/// One operation table initialized before filesystem registration and never
/// changed while it is published.
struct PublishedOps<T>(Opaque<T>);

// SAFETY: PublishedOps exposes only a stable raw pointer. Module
// initialization writes the value before register_filesystem publishes any
// table, and no write occurs until every user has drained.
unsafe impl<T> Sync for PublishedOps<T> {}

impl<T> PublishedOps<T> {
    const fn uninit() -> Self {
        Self(Opaque::uninit())
    }

    fn as_ptr(&self) -> *mut T {
        self.0.get()
    }

    /// # Safety
    ///
    /// The caller must serialize this sole initialization before publishing
    /// the pointer and must not call it while any reader can exist.
    unsafe fn publish(&self, value: T) {
        unsafe { self.as_ptr().write(value) };
    }
}

static SUPER_OPERATIONS: PublishedOps<bindings::super_operations> = PublishedOps::uninit();
static DENTRY_OPERATIONS: PublishedOps<bindings::dentry_operations> = PublishedOps::uninit();
static DIRECTORY_INODE_OPERATIONS: PublishedOps<bindings::inode_operations> =
    PublishedOps::uninit();
static FILE_INODE_OPERATIONS: PublishedOps<bindings::inode_operations> = PublishedOps::uninit();
static SYMLINK_INODE_OPERATIONS: PublishedOps<bindings::inode_operations> = PublishedOps::uninit();
static DIRECTORY_FILE_OPERATIONS: PublishedOps<bindings::file_operations> = PublishedOps::uninit();
static FILE_FILE_OPERATIONS: PublishedOps<bindings::file_operations> = PublishedOps::uninit();
static FILE_ADDRESS_SPACE_OPERATIONS: PublishedOps<bindings::address_space_operations> =
    PublishedOps::uninit();
static FILE_VM_OPERATIONS: PublishedOps<bindings::vm_operations_struct> = PublishedOps::uninit();
static NETFS_REQUEST_OPERATIONS: PublishedOps<netfs::netfs_request_ops> = PublishedOps::uninit();

struct InodeSlab(AtomicPtr<bindings::kmem_cache>);

impl InodeSlab {
    const fn new() -> Self {
        Self(AtomicPtr::new(ptr::null_mut()))
    }

    fn publish(&self, cache: *mut bindings::kmem_cache) -> core::result::Result<(), ()> {
        self.0
            .compare_exchange(ptr::null_mut(), cache, Ordering::Release, Ordering::Relaxed)
            .map(|_| ())
            .map_err(|_| ())
    }

    fn get(&self) -> *mut bindings::kmem_cache {
        self.0.load(Ordering::Acquire)
    }

    fn take(&self) -> *mut bindings::kmem_cache {
        self.0.swap(ptr::null_mut(), Ordering::AcqRel)
    }
}

static INODE_SLAB: InodeSlab = InodeSlab::new();

/// Initialize operation tables before the containing filesystem is published.
pub(crate) fn initialize(module: &'static ThisModule) -> Result<()> {
    let netfs_request_operations = netfs::netfs_request_ops {
        request_pool: ptr::null_mut(),
        subrequest_pool: ptr::null_mut(),
        init_request: Some(zerofs_netfs_init_request),
        free_request: Some(zerofs_netfs_free_request),
        free_subrequest: None,
        expand_readahead: None,
        prepare_read: Some(zerofs_netfs_prepare_read),
        issue_read: Some(zerofs_netfs_issue_read),
        is_still_valid: None,
        check_write_begin: None,
        done: None,
        update_i_size: None,
        post_modify: Some(zerofs_netfs_post_modify),
        begin_writeback: Some(zerofs_netfs_begin_writeback),
        prepare_write: None,
        issue_write: Some(zerofs_netfs_issue_write),
        retry_request: None,
        invalidate_cache: None,
    };

    // Publish the request table before any inode can point at it.
    unsafe {
        NETFS_REQUEST_OPERATIONS.publish(netfs_request_operations);
    }

    let mut cache_arguments = bindings::kmem_cache_args::default();
    cache_arguments.ctor = Some(zerofs_inode_init_once);
    let slab_flags = compat::inode_slab_flags();
    // SAFETY: The name is static and NUL-terminated; the constructor and
    // object layout remain valid until shutdown destroys the empty cache.
    let inode_cache = unsafe {
        bindings::__kmem_cache_create_args(
            INODE_CACHE_NAME.as_ptr().cast(),
            size_of::<ZeroFsInode>() as u32,
            &mut cache_arguments,
            slab_flags,
        )
    };
    if inode_cache.is_null() {
        return Err(ENOMEM);
    }
    if INODE_SLAB.publish(inode_cache).is_err() {
        // SAFETY: The newly created cache was never published or used.
        unsafe { bindings::kmem_cache_destroy(inode_cache) };
        return Err(EBUSY);
    }

    let super_operations = bindings::super_operations {
        alloc_inode: Some(zerofs_alloc_inode),
        free_inode: Some(zerofs_free_inode),
        write_inode: Some(netfs::netfs_unpin_writeback),
        put_super: Some(zerofs_put_super),
        evict_inode: Some(zerofs_evict_inode),
        sync_fs: Some(zerofs_sync_fs),
        statfs: Some(zerofs_statfs),
        umount_begin: Some(zerofs_umount_begin),
        show_options: Some(zerofs_show_options),
        ..Default::default()
    };

    let dentry_operations = bindings::dentry_operations {
        d_init: Some(zerofs_d_init),
        d_release: Some(zerofs_d_release),
        d_revalidate: Some(zerofs_d_revalidate),
        d_weak_revalidate: Some(zerofs_d_weak_revalidate),
        ..Default::default()
    };

    let directory_inode_operations = bindings::inode_operations {
        lookup: Some(zerofs_lookup),
        atomic_open: Some(zerofs_atomic_open),
        getattr: Some(zerofs_getattr),
        setattr: Some(zerofs_setattr),
        create: Some(zerofs_create),
        link: Some(zerofs_link),
        unlink: Some(zerofs_unlink),
        symlink: Some(zerofs_symlink),
        mkdir: Some(zerofs_mkdir),
        rmdir: Some(zerofs_rmdir),
        mknod: Some(zerofs_mknod),
        rename: Some(zerofs_rename),
        permission: Some(zerofs_permission),
        ..Default::default()
    };

    let file_inode_operations = bindings::inode_operations {
        getattr: Some(zerofs_getattr),
        setattr: Some(zerofs_setattr),
        permission: Some(zerofs_permission),
        ..Default::default()
    };

    let symlink_inode_operations = bindings::inode_operations {
        get_link: Some(zerofs_get_link),
        getattr: Some(zerofs_getattr),
        setattr: Some(zerofs_setattr),
        ..Default::default()
    };

    let directory_file_operations = bindings::file_operations {
        owner: module.as_ptr(),
        llseek: Some(bindings::generic_file_llseek),
        read: Some(bindings::generic_read_dir),
        open: Some(zerofs_open),
        release: Some(zerofs_release),
        iterate_shared: Some(zerofs_iterate_shared),
        fsync: Some(zerofs_fsync),
        ..Default::default()
    };

    let file_file_operations = bindings::file_operations {
        owner: module.as_ptr(),
        llseek: Some(zerofs_llseek),
        open: Some(zerofs_open),
        flush: Some(zerofs_flush),
        release: Some(zerofs_release),
        read_iter: Some(zerofs_read_iter),
        write_iter: Some(zerofs_write_iter),
        splice_read: Some(bindings::copy_splice_read),
        splice_write: Some(bindings::iter_file_splice_write),
        mmap: Some(zerofs_mmap),
        fsync: Some(zerofs_fsync),
        fallocate: Some(zerofs_fallocate),
        // Deliberately not on directory_file_operations: neither
        // v9fs_dir_operations_dotl nor nfs_dir_operations registers these, so
        // directory locks stay VFS-local.
        lock: Some(zerofs_lock),
        flock: Some(zerofs_flock),
        ..Default::default()
    };

    let file_address_space_operations = bindings::address_space_operations {
        read_folio: Some(netfs::netfs_read_folio),
        readahead: Some(netfs::netfs_readahead),
        dirty_folio: Some(netfs::netfs_dirty_folio),
        invalidate_folio: Some(netfs::netfs_invalidate_folio),
        release_folio: Some(netfs::netfs_release_folio),
        writepages: Some(netfs::netfs_writepages),
        // This advertises FMODE_CAN_ODIRECT to do_dentry_open(). The actual
        // data path is selected in read_iter/write_iter and handled by
        // netfslib.
        direct_IO: Some(bindings::noop_direct_IO),
        #[cfg(CONFIG_MIGRATION)]
        migrate_folio: Some(bindings::filemap_migrate_folio),
        ..Default::default()
    };

    let file_vm_operations = bindings::vm_operations_struct {
        open: Some(zerofs_vma_open),
        close: Some(zerofs_vma_close),
        fault: Some(zerofs_filemap_fault),
        map_pages: Some(zerofs_filemap_map_pages),
        page_mkwrite: Some(zerofs_page_mkwrite),
        ..Default::default()
    };

    // SAFETY: Module initialization is serialized. The containing module calls
    // this before register_filesystem(), which is the first publication point.
    unsafe {
        SUPER_OPERATIONS.publish(super_operations);
        DENTRY_OPERATIONS.publish(dentry_operations);
        DIRECTORY_INODE_OPERATIONS.publish(directory_inode_operations);
        FILE_INODE_OPERATIONS.publish(file_inode_operations);
        SYMLINK_INODE_OPERATIONS.publish(symlink_inode_operations);
        DIRECTORY_FILE_OPERATIONS.publish(directory_file_operations);
        FILE_FILE_OPERATIONS.publish(file_file_operations);
        FILE_ADDRESS_SPACE_OPERATIONS.publish(file_address_space_operations);
        FILE_VM_OPERATIONS.publish(file_vm_operations);
    }
    Ok(())
}

/// Release the inode slab after filesystem unregistration has drained users.
pub(crate) fn shutdown() {
    // VFS can defer inode freeing through RCU. Drain those callbacks before
    // destroying the slab that backs the embedded netfs inode.
    unsafe {
        bindings::rcu_barrier();
        let cache = INODE_SLAB.take();
        if !cache.is_null() {
            bindings::kmem_cache_destroy(cache);
        }
    }
}

unsafe extern "C" fn zerofs_inode_init_once(object: *mut c_void) {
    if object.is_null() {
        return;
    }
    let wrapper = object.cast::<ZeroFsInode>();
    // SAFETY: The slab constructor receives a newly allocated object of the
    // exact ZeroFsInode size. inode_init_once initializes the embedded VFS
    // inode for all subsequent alloc_inode/free_inode reuse cycles.
    unsafe {
        bindings::inode_init_once(ptr::addr_of_mut!((*wrapper).netfs.inode));
    }
}

unsafe extern "C" fn zerofs_alloc_inode(
    super_block: *mut bindings::super_block,
) -> *mut bindings::inode {
    if super_block.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: The cache is published before filesystem registration and
    // remains live until after unregistration plus rcu_barrier().
    let cache = INODE_SLAB.get();
    if cache.is_null() {
        return ptr::null_mut();
    }
    // Match alloc_inode_sb(): attaching the object to this superblock's inode
    // LRU is required for correct reclaim and memcg accounting.
    let wrapper = unsafe {
        bindings::kmem_cache_alloc_lru_noprof(
            cache,
            ptr::addr_of_mut!((*super_block).s_inode_lru),
            bindings::GFP_KERNEL,
        )
        .cast::<ZeroFsInode>()
    };
    if wrapper.is_null() {
        return ptr::null_mut();
    }
    unsafe { ptr::addr_of_mut!((*wrapper).netfs.inode) }
}

unsafe extern "C" fn zerofs_free_inode(inode: *mut bindings::inode) {
    if inode.is_null() {
        return;
    }
    // netfs_inode.inode and ZeroFsInode are both first-field embeddings.
    let wrapper = inode.cast::<ZeroFsInode>();
    let cache = INODE_SLAB.get();
    if !cache.is_null() {
        unsafe {
            bindings::kmem_cache_free(cache, wrapper.cast::<c_void>());
        }
    }
}

/// Issue a remote durability barrier while the caller holds inode.i_rwsem.
///
/// The barrier is scoped to the inode, not to `fid`, because one inode is
/// mutated through several fids: the caller's own, the capability netfslib
/// writeback retained after the dirtying file closed, and one credential-bound
/// fid per identity that reached it. A namespace mutation in particular runs on
/// the parent's bound fid, so a barrier scoped to whatever fid the caller holds
/// would verify nothing after a rename.
fn remote_fsync_locked(
    state: &MountState,
    inode: &InodeRef<'_>,
    fid: u32,
    datasync: bool,
) -> Result<()> {
    match inode.remote_id() {
        Ok(remote_inode) => state.client.fsync_inode(remote_inode, fid, datasync),
        // Without an identity to scope by, answer for everything outstanding
        // rather than for nothing.
        Err(_) => state.client.fsync_all(fid, datasync),
    }
}
