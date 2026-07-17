//! `zerofs mount`: mount a (possibly remote) ZeroFS 9P export as a local
//! filesystem using FUSE.
//!
//! This bridges the kernel's FUSE protocol to 9P2000.L: each FUSE callback is
//! translated into one or more 9P requests issued through [`NinePClient`].
//! Because fuser's request dispatch loop is single-threaded but its `Reply`
//! objects are `Send`, every callback simply hands the work (and the reply) off
//! to a Tokio task and returns immediately, so many operations run concurrently
//! over the multiplexed 9P connection.

use crate::ninep::lock_manager::{FileLock, FileLockManager};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use dashmap::DashMap;
use fuser::{
    AccessFlags, Config, CopyFileRangeFlags, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, InitFlags, IoctlFlags, KernelConfig, LockOwner, MountOption,
    OpenFlags, PollEvents, PollFlags, PollNotifier, RenameFlags, ReplyAttr, ReplyBmap, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyLock,
    ReplyLseek, ReplyOpen, ReplyPoll, ReplyStatfs, ReplyWrite, ReplyXattr, Request, Session,
    SessionACL, TimeOrNow,
};
use ninep_client::{ClientError, NinePClient, ReaddirState, SetattrBuilder, SetattrTime, Target};
#[cfg(test)]
use ninep_proto::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_ZERO_RANGE};
use ninep_proto::{
    GETATTR_ALL, LockStatus, LockType, P9_LOCK_FLAGS_BLOCK, Stat, classify_fallocate_mode,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;

const FUSE_ROOT: u64 = 1;
const OFFSET_MAX: u64 = i64::MAX as u64;
const LOCAL_LOCK_FID: u32 = 0;
/// Poll interval while waiting on a blocking lock request (F_SETLKW).
const LOCK_POLL: Duration = Duration::from_millis(50);
/// Cap on the data fetched per Treaddir.
const READDIR_BATCH: u32 = 256 * 1024;
/// POSIX caps a single path component at NAME_MAX bytes. The kernel doesn't
/// enforce this for FUSE, so the bridge rejects over-long names itself —
/// otherwise the server just reports the name as not found (ENOENT) instead of
/// the expected ENAMETOOLONG.
const NAME_MAX: usize = 255;

/// Maximum inline read requested with a ZeroFS open, capped by negotiated msize.
const PREFETCH_WINDOW: u32 = 128 * 1024;

/// Attribute/entry cache lifetime handed to the kernel (relaxed mode), and the
/// staleness bound on a prefetched open+read buffer: a buffer whose first read
/// arrives later than this is dropped and served fresh from the server instead.
const ATTR_TTL: Duration = Duration::from_secs(1);

/// Read-ahead state for one open directory handle.
type DirRead = ReaddirState<ninep_proto::DirEntry>;

/// Read-ahead state for one open directory handle served via readdirplus
/// (entries carry their stat).
type DirReadPlus = ReaddirState<ninep_proto::DirEntryPlus>;

/// Options gathered from the CLI.
pub struct MountOptions {
    pub msize: u32,
    pub read_only: bool,
    pub access: MountAccess,
    pub writeback: bool,
    /// Allow consistency-relaxing client/kernel caches (prefetch fold, cached
    /// symlink targets, attribute cache, page-cached reads). When false, the mount
    /// runs strict: direct-I/O reads, no attribute cache, write-through.
    pub relaxed_consistency: bool,
    /// Server-side directory the mount is rooted at (9P attach aname). Empty
    /// (or "/") mounts the whole filesystem.
    pub aname: String,
}

/// Who may access the mount.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum MountAccess {
    /// Only the mounting user (FUSE default).
    Owner,
    /// The mounting user and root.
    Root,
    /// Any user.
    All,
}

#[derive(Default)]
struct InodeEntry {
    lookup: u64,
    /// One fid per uid that holds this inode. Every request acts as its caller
    /// (v9fs `access=user` semantics), so each user gets its own server-side fid.
    fids: HashMap<u32, u32>,
    /// Open fids for this inode. Verified fsync checks every handle because
    /// `fsync(fd)` persists writes issued through any handle to the file.
    handles: std::collections::HashSet<u32>,
    /// The server inode id behind this FUSE ino when it differs from the
    /// default `ino - 1` bijection: only the mount root of an `--aname` mount,
    /// where FUSE ino 1 maps to the attach subtree's root inode.
    server_inode: Option<u64>,
}

struct Fuse9P {
    client: Arc<NinePClient>,
    rt: Handle,
    /// Maps a FUSE inode number to the per-user 9P fids we hold for it.
    inodes: Arc<DashMap<u64, InodeEntry>>,
    /// Local POSIX byte-range lock arbitration, keyed by FUSE `lock_owner`.
    ///
    /// The 9P server skips conflicts between locks from the same session, and
    /// the whole mount is a single session, so it cannot arbitrate between
    /// processes sharing this mount. We therefore enforce locks locally (like
    /// the v9fs kernel client does) and forward to the server purely to
    /// coordinate with *other* clients.
    locks: Arc<FileLockManager>,
    /// Lock requester identity sent to the server (the node name).
    client_id: Arc<Vec<u8>>,
    /// Per-open-directory read-ahead buffers, keyed by directory file handle.
    dir_reads: Arc<DashMap<u64, Arc<AsyncMutex<DirRead>>>>,
    /// Same, for directories the kernel reads via readdirplus.
    dir_reads_plus: Arc<DashMap<u64, Arc<AsyncMutex<DirReadPlus>>>>,
    /// Complete-file open prefetches keyed by file handle and fetch time.
    prefetch: Arc<DashMap<u64, (Instant, Bytes)>>,
    /// Attribute/entry cache lifetime returned to the kernel.
    ttl: Duration,
    /// When true, request the FUSE writeback cache so the kernel buffers writes
    /// and flushes them asynchronously (instead of synchronous write-through).
    writeback: bool,
    /// Strict consistency (the inverse of `--relaxed-consistency`): no prefetch
    /// fold, no cached symlink targets, a zero attribute-cache TTL, and direct-I/O
    /// file handles so every read/write hits the server. Forces `writeback` off.
    strict: bool,
}

// FUSE inode numbers and server inode ids (== qid.path) are bijective via a +1
// shift: the server root has qid.path 0, while FUSE reserves inode 1 for the
// root. Shifting keeps every other inode collision-free with FUSE_ROOT.
fn ino_of(qid_path: u64) -> u64 {
    qid_path.wrapping_add(1)
}

fn errno(e: &ClientError) -> Errno {
    Errno::from_i32(e.to_errno())
}

fn name_too_long(name: &OsStr) -> bool {
    name.as_bytes().len() > NAME_MAX
}

fn clamp_nsec(nsec: u64) -> u32 {
    nsec.min(999_999_999) as u32
}

fn kind_from_mode(mode: u32) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn kind_from_dt(dt: u8) -> FileType {
    match dt {
        libc::DT_DIR => FileType::Directory,
        libc::DT_REG => FileType::RegularFile,
        libc::DT_LNK => FileType::Symlink,
        libc::DT_CHR => FileType::CharDevice,
        libc::DT_BLK => FileType::BlockDevice,
        libc::DT_FIFO => FileType::NamedPipe,
        libc::DT_SOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn setattr_time(t: TimeOrNow) -> SetattrTime {
    match t {
        TimeOrNow::Now => SetattrTime::Now,
        TimeOrNow::SpecificTime(t) => {
            let (sec, nsec) = split_time(t);
            SetattrTime::At { sec, nsec }
        }
    }
}

fn split_time(t: SystemTime) -> (u64, u64) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs(), d.subsec_nanos() as u64),
        Err(_) => (0, 0),
    }
}

/// Validate the Linux mode combinations implemented by the ZeroFS server and
/// return the unsigned bits carried by the private 9P request.
fn validate_fallocate(offset: u64, length: u64, mode: i32) -> Result<u32, Errno> {
    if mode < 0 {
        return Err(Errno::EOPNOTSUPP);
    }
    if length == 0 {
        return Err(Errno::EINVAL);
    }
    let end = offset.checked_add(length).ok_or(Errno::EFBIG)?;
    if end > OFFSET_MAX {
        return Err(Errno::EFBIG);
    }
    let mode = mode as u32;
    classify_fallocate_mode(mode)
        .map(|_| mode)
        .ok_or(Errno::EOPNOTSUPP)
}

fn stat_to_attr(stat: &Stat) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino_of(stat.qid.path)),
        size: stat.size,
        blocks: stat.blocks,
        atime: UNIX_EPOCH + Duration::new(stat.atime_sec, clamp_nsec(stat.atime_nsec)),
        mtime: UNIX_EPOCH + Duration::new(stat.mtime_sec, clamp_nsec(stat.mtime_nsec)),
        ctime: UNIX_EPOCH + Duration::new(stat.ctime_sec, clamp_nsec(stat.ctime_nsec)),
        crtime: UNIX_EPOCH,
        kind: kind_from_mode(stat.mode),
        perm: (stat.mode & 0o7777) as u16,
        nlink: stat.nlink as u32,
        uid: stat.uid,
        gid: stat.gid,
        rdev: stat.rdev as u32,
        blksize: stat.blksize as u32,
        flags: 0,
    }
}

/// Return a fid bound to `ino` for user `uid`, binding a fresh one by inode id
/// with `Trebind` if this user has no fid for it yet. Mirrors v9fs `access=user`:
/// every request acts as its caller, so the server enforces that user's
/// permissions.
async fn user_fid(
    client: &Arc<NinePClient>,
    inodes: &Arc<DashMap<u64, InodeEntry>>,
    uid: u32,
    ino: u64,
) -> Result<u32, ClientError> {
    let server_inode = match inodes.get(&ino) {
        Some(e) => {
            if let Some(&fid) = e.fids.get(&uid) {
                return Ok(fid);
            }
            // The inverse of `ino_of` (ino - 1), unless the entry pins another
            // server inode (the mount root of an `--aname` mount).
            e.server_inode.unwrap_or(ino.wrapping_sub(1))
        }
        // The kernel only operates on inodes it has looked up; an untracked one
        // is stale.
        None => return Err(ClientError::Errno(libc::ESTALE as u32)),
    };
    // Bind a fresh fid to the inode as `uid`.
    let newfid = client.alloc_fid();
    if let Err(e) = client.rebind(newfid, server_inode, uid).await {
        client.free_fid(newfid);
        return Err(e);
    }
    match inodes.get_mut(&ino) {
        Some(mut e) => {
            if let Some(existing) = e.fids.get(&uid).copied() {
                // Another task bound this user's fid while we were rebinding.
                drop(e);
                let _ = client.clunk(newfid).await;
                client.free_fid(newfid);
                Ok(existing)
            } else {
                e.fids.insert(uid, newfid);
                Ok(newfid)
            }
        }
        // Forgotten while we rebound; don't leak the fid.
        None => {
            let _ = client.clunk(newfid).await;
            client.free_fid(newfid);
            Err(ClientError::Errno(libc::ESTALE as u32))
        }
    }
}

/// Translate a `struct flock` `l_type` into the 9P lock type. Returns `None`
/// for unrecognised values.
fn lock_type_from_typ(typ: i32) -> Option<LockType> {
    match typ {
        libc::F_RDLCK => Some(LockType::ReadLock),
        libc::F_WRLCK => Some(LockType::WriteLock),
        libc::F_UNLCK => Some(LockType::Unlock),
        _ => None,
    }
}

fn typ_from_lock_type(lt: LockType) -> i32 {
    match lt {
        LockType::ReadLock => libc::F_RDLCK,
        LockType::WriteLock => libc::F_WRLCK,
        LockType::Unlock => libc::F_UNLCK,
    }
}

/// FUSE passes an inclusive `[start, end]` byte range (`end == OFFSET_MAX` means
/// "to end of file"); 9P uses `start` + `length` (`length == 0` means "to EOF").
fn to_9p_range(start: u64, end: u64) -> (u64, u64) {
    let length = if end >= OFFSET_MAX {
        0
    } else {
        end - start + 1
    };
    (start, length)
}

/// Inverse of [`to_9p_range`]: 9P `start`/`length` back to an inclusive FUSE range.
fn from_9p_range(start: u64, length: u64) -> (u64, u64) {
    let end = if length == 0 {
        OFFSET_MAX
    } else {
        start + length - 1
    };
    (start, end)
}

/// The local node name, used as the 9P lock `client_id`.
fn node_name() -> Vec<u8> {
    let mut buf = [0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc == 0 {
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        if len > 0 {
            return buf[..len].to_vec();
        }
    }
    b"zerofs-mount".to_vec()
}

/// Register a lookup without binding a fid. `user_fid` binds lazily on use.
fn register_lookup(inodes: &Arc<DashMap<u64, InodeEntry>>, ino: u64) {
    inodes.entry(ino).or_default().lookup += 1;
}

/// Walk `parent_fid` to `name`, getattr the result, and register (or reuse) the
/// inode entry, returning the child's attributes for lookup.
async fn resolve_child(
    client: &Arc<NinePClient>,
    inodes: &Arc<DashMap<u64, InodeEntry>>,
    uid: u32,
    parent_fid: u32,
    name: &[u8],
) -> Result<FileAttr, ClientError> {
    let nf = client.alloc_fid();
    let stat = match client.walk_getattr(parent_fid, nf, &[name]).await {
        Ok((_qids, stat)) => stat,
        Err(e) => {
            client.free_fid(nf);
            return Err(e);
        }
    };
    Ok(register_entry(client, inodes, uid, Some(nf), &stat).await)
}

/// Register a lookup and retain at most one walked fid per user and inode.
/// A missing fid is bound lazily by `user_fid`.
async fn register_entry(
    client: &Arc<NinePClient>,
    inodes: &Arc<DashMap<u64, InodeEntry>>,
    uid: u32,
    walked: Option<u32>,
    stat: &Stat,
) -> FileAttr {
    let ino = ino_of(stat.qid.path);
    let redundant = {
        use std::collections::hash_map::Entry;
        let mut entry = inodes.entry(ino).or_default();
        entry.lookup += 1;
        match walked {
            Some(newfid) => match entry.fids.entry(uid) {
                Entry::Occupied(_) => Some(newfid),
                Entry::Vacant(v) => {
                    v.insert(newfid);
                    None
                }
            },
            None => None,
        }
    };
    if let Some(fid) = redundant {
        let _ = client.clunk(fid).await;
        client.free_fid(fid);
    }

    stat_to_attr(stat)
}

impl Filesystem for Fuse9P {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Ask the kernel to forward fcntl byte-range locks to us; without this
        // capability it handles POSIX locks locally and never calls getlk/setlk.
        let _ = config.add_capabilities(InitFlags::FUSE_POSIX_LOCKS);

        // PARALLEL_DIROPS adds no staleness, so it is always on; it
        // only removes a client-side bottleneck.
        let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);

        if self.strict {
            // Direct-I/O file handles still need this to allow mmap.
            let _ = config.add_capabilities(InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP);
        } else {
            // CACHE_SYMLINKS caches readlink results, skipping repeat Treadlink
            // trips. A symlink is replaced via rename (which invalidates the cache),
            // not rewritten in place, so staleness is moot, but it is still a cache,
            // so strict consistency forgoes it.
            let _ = config.add_capabilities(InitFlags::FUSE_CACHE_SYMLINKS);
        }

        // Readdirplus avoids a lookup/getattr per entry; AUTO lets the kernel
        // use plain readdir when attributes are unnecessary.
        let _ = config
            .add_capabilities(InitFlags::FUSE_DO_READDIRPLUS | InitFlags::FUSE_READDIRPLUS_AUTO);

        if self.writeback
            && let Err(unsupported) = config.add_capabilities(InitFlags::FUSE_WRITEBACK_CACHE)
        {
            debug!(
                "kernel does not support FUSE_WRITEBACK_CACHE ({unsupported:?}); using write-through"
            );
            self.writeback = false;
        }

        let _ = config.set_max_background(64);
        // Let async readahead/writeback keep flowing: don't signal congestion until
        // close to the background cap.
        let _ = config.set_congestion_threshold(60);

        // Bump sequential read-ahead so the kernel pipelines more, larger reads ahead
        // of the app, which hides latency over a remote backend. Read-ahead below
        // msize is pointless, and the kernel only reads ahead on detected-sequential
        // access, so random IO is largely unaffected. Clamp to the kernel's own cap.
        let want_readahead = self
            .client
            .msize()
            .saturating_mul(4)
            .clamp(1 << 20, 4 << 20);
        if let Err(max) = config.set_max_readahead(want_readahead) {
            let _ = config.set_max_readahead(max);
        }
        Ok(())
    }

    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        if name_too_long(name) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let name = name.as_bytes().to_vec();
        let parent = parent.0;
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match resolve_child(&client, &inodes, uid, parent_fid, &name).await {
                Ok(attr) => reply.entry(&ttl, &attr, Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        let ino = ino.0;
        if ino == FUSE_ROOT {
            return;
        }
        if let Some(mut e) = self.inodes.get_mut(&ino) {
            e.lookup = e.lookup.saturating_sub(nlookup);
        }
        // Once the inode's reference count hits zero, drop it and clunk every fid we
        // held for it: the per-user inode fids and any still-registered open handles.
        // Live handles are normally cleared by release/releasedir before the final
        // forget, so `handles` is empty here; clunking it anyway is leak-hygiene if a
        // handle outlived its release.
        if let Some((_, entry)) = self.inodes.remove_if(&ino, |_, e| e.lookup == 0) {
            let client = Arc::clone(&self.client);
            let prefetch = Arc::clone(&self.prefetch);
            self.rt.spawn(async move {
                for fid in entry.fids.into_values().chain(entry.handles) {
                    // Same leak-hygiene as the clunk: drop any prefetch buffer that
                    // outlived its release (keyed by the open-handle fid; a no-op for
                    // the inode fids, which never hold one).
                    prefetch.remove(&(fid as u64));
                    let _ = client.clunk(fid).await;
                    client.free_fid(fid);
                }
            });
        }
    }

    fn getattr(&self, req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let ino = ino.0;
        self.rt.spawn(async move {
            let fid = match user_fid(&client, &inodes, uid, ino).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match client.getattr(fid, GETATTR_ALL).await {
                Ok(stat) => reply.attr(&ttl, &stat_to_attr(&stat)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let caller_uid = req.uid();
        let ino = ino.0;
        self.rt.spawn(async move {
            let fid = match user_fid(&client, &inodes, caller_uid, ino).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            let ts = SetattrBuilder::new(fid)
                .mode(mode)
                .uid(uid)
                .gid(gid)
                .size(size)
                .atime(atime.map(setattr_time))
                .mtime(mtime.map(setattr_time))
                .build();
            match client.setattr_attr(ts).await {
                Ok(stat) => reply.attr(&ttl, &stat_to_attr(&stat)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let uid = req.uid();
        let ino = ino.0;
        self.rt.spawn(async move {
            let fid = match user_fid(&client, &inodes, uid, ino).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match client.readlink(fid).await {
                Ok(target) => reply.data(&target),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if name_too_long(name) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let gid = req.gid();
        let name = name.as_bytes().to_vec();
        let parent = parent.0;
        let mode = (mode & 0o7777 & !umask) | libc::S_IFDIR;
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match client.mkdir_attr(parent_fid, &name, mode, gid).await {
                Ok(stat) => {
                    let attr = register_entry(&client, &inodes, uid, None, &stat).await;
                    reply.entry(&ttl, &attr, Generation(0));
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        if name_too_long(name) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let gid = req.gid();
        let name = name.as_bytes().to_vec();
        let parent = parent.0;
        let mode = (mode & 0o7777 & !umask) | (mode & libc::S_IFMT);
        // FUSE delivers rdev pre-encoded with the kernel's new_encode_dev(); split
        // it into major/minor with new_decode_dev() (matching what v9fs sends as
        // MAJOR()/MINOR()). The previous (rdev >> 8 / rdev & 0xff) split only
        // worked for major < 16 and minor < 256.
        let major = (rdev & 0x000f_ff00) >> 8;
        let minor = (rdev & 0xff) | ((rdev >> 12) & 0x000f_ff00);
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match client
                .mknod_attr(parent_fid, &name, mode, major, minor, gid)
                .await
            {
                Ok(stat) => {
                    let attr = register_entry(&client, &inodes, uid, None, &stat).await;
                    reply.entry(&ttl, &attr, Generation(0));
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        if name_too_long(link_name) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let gid = req.gid();
        let name = link_name.as_bytes().to_vec();
        let target = target.as_os_str().as_bytes().to_vec();
        let parent = parent.0;
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match client.symlink_attr(parent_fid, &name, &target, gid).await {
                Ok(stat) => {
                    let attr = register_entry(&client, &inodes, uid, None, &stat).await;
                    reply.entry(&ttl, &attr, Generation(0));
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.unlink_inner(req.uid(), parent, name, 0, reply);
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.unlink_inner(req.uid(), parent, name, libc::AT_REMOVEDIR as u32, reply);
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if name_too_long(name) || name_too_long(newname) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        // 9P renameat has no flag support (RENAME_NOREPLACE/EXCHANGE/WHITEOUT).
        if !flags.is_empty() {
            reply.error(Errno::EINVAL);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let uid = req.uid();
        let name = name.as_bytes().to_vec();
        let newname = newname.as_bytes().to_vec();
        let parent = parent.0;
        let newparent = newparent.0;
        self.rt.spawn(async move {
            let (Ok(old_fid), Ok(new_fid)) = (
                user_fid(&client, &inodes, uid, parent).await,
                user_fid(&client, &inodes, uid, newparent).await,
            ) else {
                reply.error(Errno::ESTALE);
                return;
            };
            match client.renameat(old_fid, &name, new_fid, &newname).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn link(
        &self,
        req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if name_too_long(newname) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let newname = newname.as_bytes().to_vec();
        let ino = ino.0;
        let newparent = newparent.0;
        self.rt.spawn(async move {
            let (Ok(file_fid), Ok(dir_fid)) = (
                user_fid(&client, &inodes, uid, ino).await,
                user_fid(&client, &inodes, uid, newparent).await,
            ) else {
                reply.error(Errno::ESTALE);
                return;
            };
            match client.link_attr(dir_fid, file_fid, &newname).await {
                Ok(stat) => {
                    let attr = register_entry(&client, &inodes, uid, None, &stat).await;
                    reply.entry(&ttl, &attr, Generation(0));
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        // Prefetch applies to relaxed, read-only, non-truncating opens.
        let raw = flags.0;
        let prefetch = if !self.strict
            && (raw & libc::O_ACCMODE) == libc::O_RDONLY
            && (raw & libc::O_TRUNC) == 0
        {
            PREFETCH_WINDOW.min(self.client.msize())
        } else {
            0
        };
        self.open_inner(
            req.uid(),
            ino,
            flags,
            reply,
            self.writeback,
            prefetch,
            self.strict,
        );
    }

    fn opendir(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.open_inner(req.uid(), ino, flags, reply, false, 0, false);
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let fid = fh.0 as u32;
        // Prefetch expiry matches the attribute cache. Reads past it reach the server.
        if let Some(entry) = self.prefetch.get(&fh.0) {
            let (fetched, buf) = entry.value();
            if fetched.elapsed() <= self.ttl && (offset as usize) <= buf.len() {
                let start = offset as usize;
                let end = (start + size as usize).min(buf.len());
                let consumed = end >= buf.len();
                reply.data(&buf[start..end]);
                drop(entry);
                if consumed {
                    self.prefetch.remove(&fh.0);
                }
                return;
            }
            drop(entry);
            self.prefetch.remove(&fh.0);
        }
        let client = Arc::clone(&self.client);
        self.rt.spawn(async move {
            match client.read(fid, offset, size).await {
                Ok(data) => reply.data(&data),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
        let data = data.to_vec();
        self.rt.spawn(async move {
            match client.write(fid, offset, &data).await {
                // A single FUSE write is bounded by the kernel's max write, so it fits u32.
                Ok(n) => reply.written(n as u32),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        // POSIX requires closing any fd to a file to drop the owner's locks on
        // it, so release them from the local manager. The server's copy is
        // dropped when the handle's fid is clunked in `release` (like v9fs, which
        // sends no explicit unlock on close).
        let _ = fh;
        let owner = lock_owner.0;
        // Common case: the session never took a byte-range lock, so a close has
        // nothing to release.
        if !self.locks.session_has_locks(owner) {
            reply.ok();
            return;
        }
        let locks = Arc::clone(&self.locks);
        let ino = ino.0;
        self.rt.spawn(async move {
            locks.unlock_range(ino, LOCAL_LOCK_FID, 0, 0, owner);
            reply.ok();
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.fsync_inner(ino.0, fh, datasync, reply);
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.fsync_inner(ino.0, fh, datasync, reply);
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.drop_handle(ino.0, fh);
        self.release_inner(fh, reply);
    }

    fn releasedir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.dir_reads.remove(&fh.0);
        self.dir_reads_plus.remove(&fh.0);
        self.drop_handle(ino.0, fh);
        self.release_inner(fh, reply);
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
        let state = self
            .dir_reads
            .entry(fh.0)
            .or_insert_with(|| Arc::new(AsyncMutex::new(DirRead::starting_at(offset))))
            .clone();
        let batch = client.max_io().min(READDIR_BATCH);
        self.rt.spawn(async move {
            let mut st = state.lock().await;
            st.seek(offset);
            loop {
                if st.buf.is_empty() {
                    if st.eof {
                        break;
                    }
                    match client.readdir(fid, st.fetch_cookie, batch).await {
                        Ok(entries) => st.absorb(entries),
                        Err(e) => {
                            reply.error(errno(&e));
                            return;
                        }
                    }
                    continue;
                }
                while let Some(e) = st.buf.pop_front() {
                    let child_ino = INodeNo(ino_of(e.qid.path));
                    let name = OsStr::from_bytes(&e.name.data);
                    if reply.add(child_ino, e.offset, kind_from_dt(e.type_), name) {
                        // Didn't fit; keep it for the next call.
                        st.buf.push_front(e);
                        reply.ok();
                        return;
                    }
                    st.resume_offset = e.offset;
                }
            }
            reply.ok();
        });
    }

    fn readdirplus(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let fid = fh.0 as u32;
        let state = self
            .dir_reads_plus
            .entry(fh.0)
            .or_insert_with(|| Arc::new(AsyncMutex::new(DirReadPlus::starting_at(offset))))
            .clone();
        let batch = client.max_io().min(READDIR_BATCH);
        self.rt.spawn(async move {
            let mut st = state.lock().await;
            st.seek(offset);
            loop {
                if st.buf.is_empty() {
                    if st.eof {
                        break;
                    }
                    match client.readdirplus(fid, st.fetch_cookie, batch).await {
                        Ok(entries) => st.absorb(entries),
                        Err(e) => {
                            reply.error(errno(&e));
                            return;
                        }
                    }
                    continue;
                }
                while let Some(e) = st.buf.pop_front() {
                    let child_ino = ino_of(e.stat.qid.path);
                    let name = OsStr::from_bytes(&e.name.data);
                    let attr = stat_to_attr(&e.stat);
                    if reply.add(
                        INodeNo(child_ino),
                        e.offset,
                        name,
                        &ttl,
                        &attr,
                        Generation(0),
                    ) {
                        // Didn't fit; keep it for the next call (uncounted).
                        st.buf.push_front(e);
                        reply.ok();
                        return;
                    }
                    st.resume_offset = e.offset;
                    // The kernel takes a lookup reference for every delivered entry
                    // except "." and ".."; account for it like `resolve_child` so a
                    // later forget balances. The fid stays unbound (bound lazily by
                    // `user_fid` via Trebind on the first real op).
                    if !matches!(e.name.data.as_slice(), b"." | b"..") {
                        inodes.entry(child_ino).or_default().lookup += 1;
                    }
                }
            }
            reply.ok();
        });
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        if name_too_long(name) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let uid = req.uid();
        let gid = req.gid();
        let name = name.as_bytes().to_vec();
        let parent = parent.0;
        let mode = (mode & 0o7777 & !umask) | libc::S_IFREG;
        // Strip O_APPEND (append is kernel-managed via the write offset; see
        // open_inner) and, as there, upgrade O_WRONLY to O_RDWR under writeback
        // so read-modify-write can read through the backing fid.
        let flags = flags & !libc::O_APPEND;
        let lflags = if self.writeback && (flags & libc::O_ACCMODE) == libc::O_WRONLY {
            (flags & !libc::O_ACCMODE) | libc::O_RDWR
        } else {
            flags
        } as u32;
        // create is always a regular file, so direct I/O follows strict mode.
        let open_flags = handle_open_flags(self.writeback, self.strict);
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };

            let nf = client.alloc_fid();
            match client
                .lcreateattr(parent_fid, nf, &name, lflags, mode, gid)
                .await
            {
                Ok((stat, _iounit)) => {
                    let new_ino = ino_of(stat.qid.path);
                    register_lookup(&inodes, new_ino);
                    if let Some(mut e) = inodes.get_mut(&new_ino) {
                        e.handles.insert(nf);
                    }
                    reply.created(
                        &ttl,
                        &stat_to_attr(&stat),
                        Generation(0),
                        FileHandle(nf as u64),
                        open_flags,
                    );
                }
                Err(e) => {
                    client.free_fid(nf);
                    reply.error(errno(&e));
                }
            }
        });
    }

    fn statfs(&self, req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let uid = req.uid();
        let ino = ino.0;
        self.rt.spawn(async move {
            // statfs is filesystem-wide, so any fid works; fall back to the
            // caller's root if the given inode isn't tracked.
            let fid = match user_fid(&client, &inodes, uid, ino).await {
                Ok(f) => f,
                Err(_) => match user_fid(&client, &inodes, uid, FUSE_ROOT).await {
                    Ok(f) => f,
                    Err(e) => {
                        reply.error(errno(&e));
                        return;
                    }
                },
            };
            match client.statfs(fid).await {
                Ok(s) => reply.statfs(
                    s.blocks, s.bfree, s.bavail, s.files, s.ffree, s.bsize, s.namelen, s.bsize,
                ),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let Some(want) = lock_type_from_typ(typ) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let client = Arc::clone(&self.client);
        let locks = Arc::clone(&self.locks);
        let client_id = Arc::clone(&self.client_id);
        let ino = ino.0;
        let owner = lock_owner.0;
        let server_fid = fh.0 as u32;
        let (s9, len9) = to_9p_range(start, end);
        self.rt.spawn(async move {
            // Conflicts between processes sharing this mount are tracked locally.
            let test = FileLock {
                lock_type: want,
                start: s9,
                length: len9,
                proc_id: pid,
                client_id: (*client_id).clone(),
                fid: LOCAL_LOCK_FID,
                inode_id: ino,
            };
            if let Some(conflict) = locks.check_would_block(ino, &test, owner) {
                let (cs, ce) = from_9p_range(conflict.start, conflict.length);
                reply.locked(
                    cs,
                    ce,
                    typ_from_lock_type(conflict.lock_type),
                    conflict.proc_id,
                );
                return;
            }
            // Otherwise consult the server for locks held by other clients.
            match client
                .getlock(server_fid, want, s9, len9, pid, &client_id)
                .await
            {
                Ok(rg) if !matches!(rg.lock_type, LockType::Unlock) => {
                    let (cs, ce) = from_9p_range(rg.start, rg.length);
                    // The holder is on another client; report its pid as negative,
                    // the convention v9fs uses (flc_pid = -proc_id) to signal a
                    // remote owner that has no meaning in this node's pid space.
                    let pid = rg.proc_id.wrapping_neg();
                    reply.locked(cs, ce, typ_from_lock_type(rg.lock_type), pid);
                }
                Ok(_) => reply.locked(start, end, libc::F_UNLCK, pid),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let Some(lt) = lock_type_from_typ(typ) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let client = Arc::clone(&self.client);
        let locks = Arc::clone(&self.locks);
        let client_id = Arc::clone(&self.client_id);
        let ino = ino.0;
        let owner = lock_owner.0;
        let server_fid = fh.0 as u32;
        let (s9, len9) = to_9p_range(start, end);
        self.rt.spawn(async move {
            if matches!(lt, LockType::Unlock) {
                locks.unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner);
                let _ = client
                    .lock(server_fid, LockType::Unlock, 0, s9, len9, pid, &client_id)
                    .await;
                reply.ok();
                return;
            }

            // F_SETLKW (sleep) is implemented by polling: the server returns
            // "blocked" immediately rather than waiting, and the local manager
            // has no wait primitive, so we retry until the lock is grantable.
            loop {
                let new_lock = FileLock {
                    lock_type: lt,
                    start: s9,
                    length: len9,
                    proc_id: pid,
                    client_id: (*client_id).clone(),
                    fid: LOCAL_LOCK_FID,
                    inode_id: ino,
                };
                if !locks.try_add_lock(owner, new_lock) {
                    if sleep {
                        tokio::time::sleep(LOCK_POLL).await;
                        continue;
                    }
                    reply.error(Errno::EAGAIN);
                    return;
                }

                // Coordinate with other clients via the server.
                let flags = if sleep { P9_LOCK_FLAGS_BLOCK } else { 0 };
                match client
                    .lock(server_fid, lt, flags, s9, len9, pid, &client_id)
                    .await
                {
                    Ok(LockStatus::Success) => {
                        reply.ok();
                        return;
                    }
                    // Cross-client conflict: fall through to roll back and wait/fail.
                    Ok(LockStatus::Blocked) => {}
                    Err(ClientError::Errno(c)) if c == libc::EAGAIN as u32 => {}
                    // Grace period and lock errors are not retryable (v9fs maps
                    // both to ENOLCK); roll back the local grant and fail.
                    Ok(LockStatus::Grace) | Ok(LockStatus::LockError) => {
                        locks.unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner);
                        reply.error(Errno::ENOLCK);
                        return;
                    }
                    Err(e) => {
                        locks.unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner);
                        reply.error(errno(&e));
                        return;
                    }
                }

                // The server reports a conflict with another client. Undo the
                // local grant, then wait and retry (blocking) or report EAGAIN.
                locks.unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner);
                if sleep {
                    tokio::time::sleep(LOCK_POLL).await;
                    continue;
                }
                reply.error(Errno::EAGAIN);
                return;
            }
        });
    }

    // Mostly operations we don't support over the 9P backend. Those callbacks
    // return the same ENOSYS as fuser's default without its per-call warning;
    // fallocate below is the one negotiated private-extension exception.
    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        reply.error(Errno::ENOSYS);
    }

    #[allow(clippy::too_many_arguments)]
    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::ENOSYS);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::ENOSYS);
    }

    fn bmap(&self, _req: &Request, _ino: INodeNo, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        reply.error(Errno::ENOSYS);
    }

    #[allow(clippy::too_many_arguments)]
    fn ioctl(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        reply.error(Errno::ENOSYS);
    }

    #[allow(clippy::too_many_arguments)]
    fn poll(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _ph: PollNotifier,
        _events: PollEvents,
        _flags: PollFlags,
        reply: ReplyPoll,
    ) {
        reply.error(Errno::ENOSYS);
    }

    #[allow(clippy::too_many_arguments)]
    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let mode = match validate_fallocate(offset, length, mode) {
            Ok(mode) => mode,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
        self.rt.spawn(async move {
            match client.fallocate(fid, offset, length, mode).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn lseek(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: i64,
        _whence: i32,
        reply: ReplyLseek,
    ) {
        reply.error(Errno::ENOSYS);
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_file_range(
        &self,
        _req: &Request,
        _ino_in: INodeNo,
        _fh_in: FileHandle,
        _offset_in: u64,
        _ino_out: INodeNo,
        _fh_out: FileHandle,
        _offset_out: u64,
        _len: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        reply.error(Errno::ENOSYS);
    }
}

/// FOPEN flags for a regular-file handle. Strict consistency takes `direct_io`,
/// bypassing the page cache so every read/write hits the server; otherwise the
/// writeback cache keeps pages across opens. Directories pass both false.
fn handle_open_flags(writeback: bool, direct_io: bool) -> FopenFlags {
    if direct_io {
        FopenFlags::FOPEN_DIRECT_IO
    } else if writeback {
        FopenFlags::FOPEN_KEEP_CACHE
    } else {
        FopenFlags::empty()
    }
}

impl Fuse9P {
    #[allow(clippy::too_many_arguments)]
    fn open_inner(
        &self,
        uid: u32,
        ino: INodeNo,
        flags: OpenFlags,
        reply: ReplyOpen,
        writeback: bool,
        prefetch: u32,
        direct_io: bool,
    ) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let prefetch_map = Arc::clone(&self.prefetch);
        let ino = ino.0;
        // Append is handled by the kernel: it computes the write offset from the
        // file size (i_size) and sends each write at that offset, so the backing
        // 9P fid must NOT be in append mode otherwise an append-honoring server
        // would redirect our explicit-offset writes (including writeback
        // read-modify-write of interior pages) to EOF. v9fs strips O_APPEND for
        // the same reason.
        let raw = flags.0 & !libc::O_APPEND;
        // Under the writeback cache the kernel performs read-modify-write on
        // partially-written pages, issuing reads even on a write-only fd, so the
        // backing fid must be readable. Upgrade O_WRONLY to O_RDWR (as v9fs does)
        // and fall back to the original mode if the upgrade is refused.
        let upgrade = writeback && (raw & libc::O_ACCMODE) == libc::O_WRONLY;
        let lflags = if upgrade {
            ((raw & !libc::O_ACCMODE) | libc::O_RDWR) as u32
        } else {
            raw as u32
        };
        let orig = raw as u32;
        let open_flags = handle_open_flags(writeback, direct_io);
        self.rt.spawn(async move {
            let inode_fid = match user_fid(&client, &inodes, uid, ino).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            // Keep the inode fid stable across compound-open fallback.
            let nf = client.alloc_fid();
            let opened = if prefetch > 0 {
                client
                    .open_clone_prefetch(inode_fid, nf, lflags, prefetch)
                    .await
            } else {
                client
                    .open_clone(inode_fid, nf, lflags, upgrade.then_some(orig))
                    .await
                    .map(|(qid, iounit)| (qid, iounit, None))
            };
            match opened {
                Ok((_qid, _iounit, buf)) => {
                    // Track the open handle so a verified fsync of this inode (through
                    // ANY of its handles) presents this one's un-fsync'd writes too.
                    if let Some(mut e) = inodes.get_mut(&ino) {
                        e.handles.insert(nf);
                    }
                    // Stash the folded-in bytes for `read` to serve, keyed by the
                    // handle, stamped now so a late first read can expire it.
                    if let Some(buf) = buf {
                        prefetch_map.insert(nf as u64, (Instant::now(), buf));
                    }
                    reply.opened(FileHandle(nf as u64), open_flags);
                }
                Err(e) => {
                    client.free_fid(nf);
                    reply.error(errno(&e));
                }
            }
        });
    }

    /// Stop tracking an open handle on its inode (on release), so a later fsync
    /// does not present a fid the client may have freed and recycled.
    fn drop_handle(&self, ino: u64, fh: FileHandle) {
        if let Some(mut e) = self.inodes.get_mut(&ino) {
            e.handles.remove(&(fh.0 as u32));
        }
    }

    fn release_inner(&self, fh: FileHandle, reply: ReplyEmpty) {
        // Drop any unread prefetch (an open that never read, or read part of the file).
        self.prefetch.remove(&fh.0);
        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
        self.rt.spawn(async move {
            let _ = client.clunk(fid).await;
            client.free_fid(fid);
            reply.ok();
        });
    }

    fn fsync_inner(&self, ino: u64, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let primary = fh.0 as u32;
        self.rt.spawn(async move {
            // A verified fsync persists the whole inode, but its mutations are spread
            // across fids: data writes ride the open handles (`inodes[ino].handles`,
            // including handles the app did not fsync directly), setattr and
            // directory-entry ops ride the per-user inode fids (`inodes[ino].fids`).
            // Present them all so the fsync verifies every one and ESTALEs if any broke.
            let mut fids = vec![primary];
            if let Some(e) = inodes.get(&ino) {
                fids.extend(
                    e.fids
                        .values()
                        .chain(e.handles.iter())
                        .copied()
                        .filter(|&f| f != primary),
                );
            }
            match client.fsync_inode(&fids, primary, datasync as u32).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn unlink_inner(&self, uid: u32, parent: INodeNo, name: &OsStr, flags: u32, reply: ReplyEmpty) {
        if name_too_long(name) {
            reply.error(Errno::ENAMETOOLONG);
            return;
        }
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
        let name = name.as_bytes().to_vec();
        let parent = parent.0;
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            match client.unlinkat(parent_fid, &name, flags).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }
}

async fn connect(target: &str, msize: u32) -> Result<Arc<NinePClient>> {
    // A comma-separated list is an HA node set (dials the leader, re-routes on failover).
    let targets = Target::parse_list(target).map_err(anyhow::Error::msg)?;
    NinePClient::connect_multi(targets, msize)
        .await
        .with_context(|| format!("connecting to 9P server(s) {target}"))
}

async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).ok();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = async {
            match term.as_mut() {
                Some(t) => { t.recv().await; }
                None => std::future::pending::<()>().await,
            }
        } => {}
    }
}

pub async fn run(target: String, mountpoint: PathBuf, opts: MountOptions) -> Result<()> {
    // Silence `fuser::reply` by default.
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,fuser::reply=off")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let client = connect(&target, opts.msize).await?;
    let msize = client.msize();

    // Every request binds its own per-user fid by inode id; without --aname
    // there is no attach at all and the root is inode 0. With --aname, attach
    // once to resolve the subtree root (and pin the session to it server-side).
    // The attach fid is kept for the life of the mount so reconnect replay
    // re-attaches with the same aname before the per-inode fids are rebound.
    let root_inode = if opts.aname.is_empty() || opts.aname == "/" {
        0
    } else {
        let afid = client.alloc_fid();
        let uid = unsafe { libc::geteuid() };
        let qid = client
            .attach(afid, ninep_client::NOFID, "", &opts.aname, uid)
            .await
            .map_err(|e| anyhow!("attaching to {:?} on {target}: {e}", opts.aname))?;
        qid.path
    };

    // Seed the root inode with an empty per-user fid set.
    let inodes: Arc<DashMap<u64, InodeEntry>> = Arc::new(DashMap::new());
    inodes.insert(
        FUSE_ROOT,
        InodeEntry {
            lookup: u64::MAX,
            fids: HashMap::new(),
            handles: std::collections::HashSet::new(),
            server_inode: Some(root_inode),
        },
    );

    // Strict consistency turns off every cache that can serve stale data, so it
    // also forces write-through (the writeback cache delays write visibility) and
    // a zero attribute-cache TTL (every lookup/getattr revalidates).
    let strict = !opts.relaxed_consistency;
    let fs = Fuse9P {
        client,
        rt: Handle::current(),
        inodes,
        locks: Arc::new(FileLockManager::new()),
        client_id: Arc::new(node_name()),
        dir_reads: Arc::new(DashMap::new()),
        dir_reads_plus: Arc::new(DashMap::new()),
        prefetch: Arc::new(DashMap::new()),
        ttl: if strict { Duration::ZERO } else { ATTR_TTL },
        writeback: opts.writeback && !strict,
        strict,
    };

    // Device string = connection target, so device-matching tools (xfstests'
    // _check_mounted_on) line up. FUSE options are comma-separated, so for an HA
    // node set we use the first spec; the "zerofs" subtype identifies the fs.
    let device = target
        .split(',')
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("zerofs")
        .to_string();

    let mut config = Config::default();
    let mut mount_options = vec![
        MountOption::FSName(device),
        MountOption::Subtype("zerofs".to_string()),
        // Have the kernel enforce permissions against the attributes we report,
        // so path-prefix search bits and O_TRUNC write checks are applied per
        // caller even when the inode is reached via the dentry cache (where the
        // bridge rebinds by inode id and skips a server-side path walk). It also
        // lets the kernel strip SUID/SGID on chmod by a non-owner, like a local fs.
        MountOption::DefaultPermissions,
    ];
    if opts.read_only {
        mount_options.push(MountOption::RO);
    }
    config.mount_options = mount_options;
    config.acl = match opts.access {
        MountAccess::Owner => SessionACL::Owner,
        MountAccess::Root => SessionACL::RootAndOwner,
        MountAccess::All => SessionACL::All,
    };
    // Dispatch kernel requests on a few threads (each with its own /dev/fuse fd)
    // so request parsing/handoff parallelizes. Kept small because each event-loop
    // thread allocates a ~16 MiB receive buffer; the real work runs in the Tokio
    // tasks our callbacks spawn, so a handful of dispatch threads is plenty.
    let dispatch_threads = std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 4))
        .unwrap_or(2);
    config.n_threads = Some(dispatch_threads);
    config.clone_fd = true;

    let session = Session::new(fs, &mountpoint, &config).map_err(|e| {
        let mut err = anyhow!(
            "failed to mount filesystem at {}: {e}",
            mountpoint.display()
        );
        // `--access root`/`all` map to the `allow_other` mount option, which an
        // unprivileged fusermount refuses unless the admin has opted in.
        if opts.access != MountAccess::Owner && unsafe { libc::geteuid() } != 0 {
            err = err.context(
                "--access root/all requires 'user_allow_other' in /etc/fuse.conf (or run as root)",
            );
        }
        err
    })?;
    // `spawn` moves the mount handle into the BackgroundSession, so unmounting
    // must go through `umount_and_join` rather than a SessionUnmounter.
    let bg = session.spawn().context("failed to start FUSE session")?;

    println!(
        "ZeroFS mounted at {} (9P: {}, msize: {} KiB, cache: {})",
        mountpoint.display(),
        target,
        msize / 1024,
        if opts.writeback {
            "writeback"
        } else {
            "writethrough"
        }
    );
    println!("Press Ctrl-C to unmount.");

    wait_for_signal().await;

    eprintln!("Unmounting {}...", mountpoint.display());
    let mp = mountpoint.clone();
    match tokio::task::spawn_blocking(move || bg.umount_and_join())
        .await
        .map_err(|e| anyhow!("unmount task panicked: {e}"))?
    {
        Ok(()) => {}
        // ENOENT/EINVAL mean there is no mount left to clean up: it was
        // already unmounted externally (the CSI node plugin unmounts the
        // target before sending SIGTERM; a user may have run fusermount -u
        // by hand). That is a clean outcome for a cleanup path, not an error.
        Err(e) if matches!(e.raw_os_error(), Some(libc::ENOENT | libc::EINVAL)) => {
            eprintln!("{} was already unmounted.", mp.display());
        }
        Err(e) => {
            return Err(anyhow!(e)).with_context(|| {
                format!("failed to unmount {} (is it still in use?)", mp.display())
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod consistency_tests {
    use super::*;

    #[test]
    fn open_flags_precedence() {
        // Default: let the kernel cache normally.
        assert_eq!(handle_open_flags(false, false), FopenFlags::empty());
        // Writeback keeps the page cache across opens.
        assert_eq!(handle_open_flags(true, false), FopenFlags::FOPEN_KEEP_CACHE);
        // Strict (direct I/O) bypasses the page cache and wins over writeback.
        assert_eq!(handle_open_flags(false, true), FopenFlags::FOPEN_DIRECT_IO);
        assert_eq!(handle_open_flags(true, true), FopenFlags::FOPEN_DIRECT_IO);
    }

    #[test]
    fn supported_fallocate_modes_are_accepted() {
        for mode in [
            0,
            (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE) as i32,
            FALLOC_FL_ZERO_RANGE as i32,
            (FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE) as i32,
        ] {
            assert_eq!(validate_fallocate(10, 20, mode).unwrap(), mode as u32);
        }
    }

    #[test]
    fn fallocate_rejects_invalid_or_unsupported_ranges() {
        assert_eq!(
            validate_fallocate(0, 0, 0).unwrap_err().code(),
            libc::EINVAL
        );
        assert_eq!(
            validate_fallocate(OFFSET_MAX, 1, 0).unwrap_err().code(),
            libc::EFBIG
        );
        assert_eq!(
            validate_fallocate(0, 1, FALLOC_FL_KEEP_SIZE as i32)
                .unwrap_err()
                .code(),
            libc::EOPNOTSUPP
        );
        assert_eq!(
            validate_fallocate(0, 1, 0x80).unwrap_err().code(),
            libc::EOPNOTSUPP
        );
    }
}

#[cfg(test)]
mod client_tests {
    //! Integration tests for `ninep-client` against the real ZeroFS 9P server.
    use crate::fs::ZeroFS;
    use crate::ninep::NinePServer;
    use ninep_client::{ClientError, NOFID, NinePClient};
    use ninep_proto::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::UnixStream;
    use tokio_util::sync::CancellationToken;

    /// Spin up a real `NinePServer` over a Unix socket backed by an in-memory
    /// filesystem and return a connected client plus a shutdown guard.
    async fn connect_with_retry(sock: &std::path::Path) -> Arc<NinePClient> {
        for _ in 0..100 {
            if let Ok(c) = NinePClient::connect_unix(sock, 256 * 1024).await {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("client failed to connect");
    }

    fn start_server(fs: Arc<ZeroFS>, sock: std::path::PathBuf) -> CancellationToken {
        let server = NinePServer::new_unix(fs, sock);
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = server.start(server_shutdown).await;
        });
        shutdown
    }

    async fn setup() -> (
        Arc<NinePClient>,
        CancellationToken,
        tempfile::TempDir,
        std::path::PathBuf,
    ) {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.9p.sock");
        let shutdown = start_server(Arc::clone(&fs), sock.clone());
        let client = connect_with_retry(&sock).await;
        (client, shutdown, dir, sock)
    }

    #[tokio::test]
    async fn full_client_roundtrip() {
        let (client, shutdown, _dir, _sock) = setup().await;

        let root = client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        assert_eq!(root.type_, QID_TYPE_DIR);

        let dq = client
            .mkdir(ROOT_FID_TEST, b"subdir", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        assert_eq!(dq.type_, QID_TYPE_DIR);

        let fid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, fid, &[]).await.unwrap();
        let (fq, _io) = client
            .lcreate(
                fid,
                b"hello.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        assert_eq!(fq.type_, QID_TYPE_FILE);
        assert_eq!(client.write(fid, 0, b"hello world").await.unwrap(), 11);
        assert_eq!(client.read(fid, 0, 1024).await.unwrap(), b"hello world");
        assert_eq!(client.read(fid, 6, 1024).await.unwrap(), b"world");
        client.clunk(fid).await.unwrap();
        client.free_fid(fid);

        let gfid = client.alloc_fid();
        let name: &[u8] = b"hello.txt";
        let qids = client.walk(ROOT_FID_TEST, gfid, &[name]).await.unwrap();
        assert_eq!(qids.len(), 1);
        let st = client.getattr(gfid, GETATTR_ALL).await.unwrap();
        assert_eq!(st.size, 11);
        assert_eq!(st.mode & libc::S_IFMT, libc::S_IFREG);
        client.clunk(gfid).await.unwrap();
        client.free_fid(gfid);

        let dfid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, dfid, &[]).await.unwrap();
        client.lopen(dfid, libc::O_RDONLY as u32).await.unwrap();
        let entries = client.readdir(dfid, 0, 8192).await.unwrap();
        let names: Vec<String> = entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name.data).to_string())
            .collect();
        assert!(names.contains(&"hello.txt".to_string()), "names: {names:?}");
        assert!(names.contains(&"subdir".to_string()), "names: {names:?}");
        client.clunk(dfid).await.unwrap();
        client.free_fid(dfid);

        let sq = client
            .symlink(ROOT_FID_TEST, b"link", b"hello.txt", 0)
            .await
            .unwrap();
        assert_eq!(sq.type_, QID_TYPE_SYMLINK);
        let lfid = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, lfid, &[b"link".as_ref()])
            .await
            .unwrap();
        assert_eq!(client.readlink(lfid).await.unwrap(), b"hello.txt");
        client.clunk(lfid).await.unwrap();
        client.free_fid(lfid);

        client
            .renameat(ROOT_FID_TEST, b"hello.txt", ROOT_FID_TEST, b"renamed.txt")
            .await
            .unwrap();
        client
            .unlinkat(ROOT_FID_TEST, b"renamed.txt", 0)
            .await
            .unwrap();

        let mfid = client.alloc_fid();
        match client.walk(ROOT_FID_TEST, mfid, &[b"nope".as_ref()]).await {
            Err(ClientError::Errno(e)) => assert_eq!(e, libc::ENOENT as u32),
            other => panic!("expected ENOENT, got {other:?}"),
        }
        client.free_fid(mfid);

        let sfs = client.statfs(ROOT_FID_TEST).await.unwrap();
        assert!(sfs.blocks > 0);
        assert_eq!(sfs.namelen, 255);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn atomic_fallocate_modes_roundtrip() {
        let (client, shutdown, _dir, _sock) = setup().await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        let fid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, fid, &[]).await.unwrap();
        client
            .lcreate(
                fid,
                b"fallocate.bin",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(fid, 0, b"abcdefghijkl").await.unwrap();

        client.fallocate(fid, 16, 4, 0).await.unwrap();
        assert_eq!(client.getattr(fid, GETATTR_ALL).await.unwrap().size, 20);

        client
            .fallocate(fid, 2, 3, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)
            .await
            .unwrap();
        let data = client.read(fid, 0, 12).await.unwrap();
        assert_eq!(&data[..2], b"ab");
        assert_eq!(&data[2..5], &[0; 3]);
        assert_eq!(&data[5..], b"fghijkl");

        client
            .fallocate(fid, 8, 20, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE)
            .await
            .unwrap();
        assert_eq!(client.getattr(fid, GETATTR_ALL).await.unwrap().size, 20);

        for unsupported_mode in [FALLOC_FL_KEEP_SIZE, 0x80] {
            match client.fallocate(fid, 0, 1, unsupported_mode).await {
                Err(ClientError::Errno(e)) => assert_eq!(e, libc::EOPNOTSUPP as u32),
                other => panic!("expected EOPNOTSUPP for {unsupported_mode:#x}, got {other:?}"),
            }
        }

        client.clunk(fid).await.unwrap();
        client.free_fid(fid);
        shutdown.cancel();
    }

    /// Per-user (`access=user`) semantics: a fid bound to an inode by id with
    /// `Trebind` acts as the `n_uname` it carries — no shared attach — so files
    /// created through different users' fids are owned by their respective
    /// creators. This is exactly the mechanism `zerofs mount` uses to forward
    /// each caller's uid to the server.
    #[tokio::test]
    async fn per_user_fids_act_as_their_uid() {
        let (client, shutdown, _dir, _sock) = setup().await;

        // Bind the root inode (id 0) as root with no prior attach, then make a
        // world-writable directory both test users can create in.
        let root_su = client.alloc_fid();
        client.rebind(root_su, 0, 0).await.unwrap();
        let shared = client
            .mkdir(root_su, b"shared", libc::S_IFDIR | 0o777, 0)
            .await
            .unwrap();

        // Each user binds the shared dir by inode id and creates a file; the
        // server must record the binding user as the owner.
        for uid in [0u32, 1000u32] {
            let dir = client.alloc_fid();
            client.rebind(dir, shared.path, uid).await.unwrap();
            let file = client.alloc_fid();
            client.walk(dir, file, &[]).await.unwrap();
            let name = format!("uid{uid}.txt");
            client
                .lcreate(
                    file,
                    name.as_bytes(),
                    (libc::O_RDWR | libc::O_CREAT) as u32,
                    libc::S_IFREG | 0o644,
                    uid,
                )
                .await
                .unwrap();
            let st = client.getattr(file, GETATTR_ALL).await.unwrap();
            assert_eq!(st.uid, uid, "{name} should be owned by its creator");
            client.clunk(file).await.unwrap();
            client.free_fid(file);
            client.clunk(dir).await.unwrap();
            client.free_fid(dir);
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn walk_getattr_and_readdirplus() {
        let (client, shutdown, _dir, _sock) = setup().await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        client
            .mkdir(ROOT_FID_TEST, b"d", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();

        // Populate d/ with two 2-byte files.
        let dfid = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, dfid, &[b"d".as_ref()])
            .await
            .unwrap();
        for name in [b"a".as_ref(), b"b".as_ref()] {
            let cf = client.alloc_fid();
            client.walk(dfid, cf, &[]).await.unwrap();
            client
                .lcreate(
                    cf,
                    name,
                    (libc::O_RDWR | libc::O_CREAT) as u32,
                    libc::S_IFREG | 0o644,
                    0,
                )
                .await
                .unwrap();
            assert_eq!(client.write(cf, 0, b"xy").await.unwrap(), 2);
            client.clunk(cf).await.unwrap();
            client.free_fid(cf);
        }
        client.clunk(dfid).await.unwrap();
        client.free_fid(dfid);

        // walk_getattr to d must equal a separate walk + getattr.
        let wf = client.alloc_fid();
        let (qids, stat) = client
            .walk_getattr(ROOT_FID_TEST, wf, &[b"d".as_ref()])
            .await
            .unwrap();
        assert_eq!(qids.len(), 1);
        assert_eq!(stat.qid.path, qids[0].path);
        assert_eq!(stat.mode & libc::S_IFMT, libc::S_IFDIR);
        let gf = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, gf, &[b"d".as_ref()])
            .await
            .unwrap();
        let stat2 = client.getattr(gf, GETATTR_ALL).await.unwrap();
        assert_eq!(stat.qid.path, stat2.qid.path);
        assert_eq!(stat.mode, stat2.mode);
        assert_eq!(stat.nlink, stat2.nlink);
        client.clunk(gf).await.unwrap();
        client.free_fid(gf);
        client.clunk(wf).await.unwrap();
        client.free_fid(wf);

        // readdirplus on d/ returns a and b with their real stats.
        let od = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, od, &[b"d".as_ref()])
            .await
            .unwrap();
        client.lopen(od, libc::O_RDONLY as u32).await.unwrap();
        let entries = client.readdirplus(od, 0, 64 * 1024).await.unwrap();
        let byname: std::collections::HashMap<String, Stat> = entries
            .into_iter()
            .map(|e| (String::from_utf8_lossy(&e.name.data).to_string(), e.stat))
            .collect();
        for name in ["a", "b"] {
            let st = byname.get(name).unwrap_or_else(|| panic!("missing {name}"));
            assert_eq!(st.size, 2, "{name} size");
            assert_eq!(st.mode & libc::S_IFMT, libc::S_IFREG);
        }
        client.clunk(od).await.unwrap();
        client.free_fid(od);

        shutdown.cancel();
    }

    /// Partial open prefetch falls back to the opened fid.
    #[tokio::test]
    async fn open_read_fold() {
        let (client, shutdown, _dir, _sock) = setup().await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        // Helper: create `name` with `body`.
        async fn make(client: &Arc<NinePClient>, name: &[u8], body: &[u8]) {
            let cf = client.alloc_fid();
            client.walk(ROOT_FID_TEST, cf, &[]).await.unwrap();
            client
                .lcreate(
                    cf,
                    name,
                    (libc::O_RDWR | libc::O_CREAT) as u32,
                    libc::S_IFREG | 0o644,
                    0,
                )
                .await
                .unwrap();
            assert_eq!(client.write(cf, 0, body).await.unwrap(), body.len() as u64);
            client.clunk(cf).await.unwrap();
            client.free_fid(cf);
        }

        let small = b"hello prefetch";
        make(&client, b"small.txt", small).await;
        let big = vec![b'Z'; 4096];
        make(&client, b"big.txt", &big).await;

        // Walk to an unopened inode fid for `name`.
        async fn inode_fid(client: &Arc<NinePClient>, name: &[u8]) -> u32 {
            let f = client.alloc_fid();
            client.walk(ROOT_FID_TEST, f, &[name]).await.unwrap();
            f
        }

        // Small file: the whole file folds into the open with eof set.
        let sfid = inode_fid(&client, b"small.txt").await;
        let nf = client.alloc_fid();
        let (qid, _io, data, eof) = client
            .lopenatread(sfid, nf, libc::O_RDONLY as u32, 1024)
            .await
            .unwrap();
        assert_eq!(qid.type_, QID_TYPE_FILE);
        assert_eq!(&data[..], &small[..], "the fold returns the whole file");
        assert!(eof, "a file within the window reports eof");
        client.clunk(nf).await.unwrap();
        client.free_fid(nf);
        // The source fid is untouched (still an unopened inode fid): re-folding works.
        let nf2 = client.alloc_fid();
        let (_q, _i, data2, _e) = client
            .lopenatread(sfid, nf2, libc::O_RDONLY as u32, 1024)
            .await
            .unwrap();
        assert_eq!(&data2[..], &small[..], "source fid left openable");
        client.clunk(nf2).await.unwrap();
        client.free_fid(nf2);
        client.clunk(sfid).await.unwrap();
        client.free_fid(sfid);

        // Large file: a 16-byte window yields a prefix with eof clear, and the opened
        // fid serves the remainder over a normal read.
        let lfid = inode_fid(&client, b"big.txt").await;
        let lnf = client.alloc_fid();
        let (_q, _i, prefix, eof) = client
            .lopenatread(lfid, lnf, libc::O_RDONLY as u32, 16)
            .await
            .unwrap();
        assert_eq!(prefix.len(), 16, "prefetch is clamped to the window");
        assert!(!eof, "a file larger than the window does not report eof");
        let rest = client.read(lnf, 16, 4096).await.unwrap();
        assert_eq!(rest.len(), big.len() - 16);
        assert!(rest.iter().all(|&b| b == b'Z'));
        client.clunk(lnf).await.unwrap();
        client.free_fid(lnf);
        client.clunk(lfid).await.unwrap();
        client.free_fid(lfid);

        // The wrapper keeps a whole-file buffer for a small file...
        let w1 = inode_fid(&client, b"small.txt").await;
        let o1 = client.alloc_fid();
        let (_q, _i, buf) = client
            .open_clone_prefetch(w1, o1, libc::O_RDONLY as u32, 1024)
            .await
            .unwrap();
        assert_eq!(buf.as_deref(), Some(&small[..]));
        client.clunk(o1).await.unwrap();
        client.free_fid(o1);
        client.clunk(w1).await.unwrap();
        client.free_fid(w1);
        // ...and drops a partial prefetch of a large file (the mount reads normally).
        let w2 = inode_fid(&client, b"big.txt").await;
        let o2 = client.alloc_fid();
        let (_q, _i, buf) = client
            .open_clone_prefetch(w2, o2, libc::O_RDONLY as u32, 16)
            .await
            .unwrap();
        assert!(buf.is_none(), "a partial prefetch is dropped");
        client.clunk(o2).await.unwrap();
        client.free_fid(o2);
        client.clunk(w2).await.unwrap();
        client.free_fid(w2);

        shutdown.cancel();
    }

    /// At a small msize the fold must keep the whole Rlopenatread frame
    /// (`P9_RLOPENATREAD_HDR` + data) within the negotiated msize, not just an
    /// Rread's smaller overhead. A file at the old (P9_IOHDRSZ) clamp boundary must
    /// come back as a partial prefetch (no eof), not a reply that overruns the msize.
    #[tokio::test]
    async fn open_read_fold_respects_small_msize() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("small-msize.9p.sock");
        let shutdown = start_server(Arc::clone(&fs), sock.clone());

        // Connect with a small msize so the reply's fixed overhead matters.
        let msize = 8192u32;
        let mut client = None;
        for _ in 0..100 {
            if let Ok(c) = NinePClient::connect_unix(&sock, msize).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let client = client.expect("client failed to connect");
        assert_eq!(client.msize(), msize, "server honors the small msize");
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        // A file at the OLD clamp boundary (msize - P9_IOHDRSZ): the buggy code read
        // it whole and replied with P9_RLOPENATREAD_HDR + (msize - P9_IOHDRSZ) > msize.
        let body = vec![b'q'; (msize - P9_IOHDRSZ) as usize];
        let cf = client.alloc_fid();
        client.walk(ROOT_FID_TEST, cf, &[]).await.unwrap();
        client
            .lcreate(
                cf,
                b"boundary.bin",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        assert_eq!(client.write(cf, 0, &body).await.unwrap(), body.len() as u64);
        client.clunk(cf).await.unwrap();
        client.free_fid(cf);

        let sfid = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, sfid, &[b"boundary.bin".as_ref()])
            .await
            .unwrap();
        let nf = client.alloc_fid();
        let (_q, _io, data, eof) = client
            .lopenatread(sfid, nf, libc::O_RDONLY as u32, msize)
            .await
            .unwrap();
        // The whole reply must fit the negotiated msize.
        assert!(
            data.len() as u32 + P9_RLOPENATREAD_HDR <= msize,
            "reply data {} + hdr {} must fit msize {}",
            data.len(),
            P9_RLOPENATREAD_HDR,
            msize
        );
        // The file exceeds the data budget, so it folds only partially (no eof) and
        // the opened fid serves the remainder over a normal read.
        assert!(!eof, "a file past the msize budget folds only partially");
        let rest = client.read(nf, data.len() as u64, msize).await.unwrap();
        assert_eq!(
            data.len() + rest.len(),
            body.len(),
            "fold + tail = whole file"
        );
        client.clunk(nf).await.unwrap();
        client.free_fid(nf);
        client.clunk(sfid).await.unwrap();
        client.free_fid(sfid);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn compound_create_open_and_attr_replies() {
        let (client, shutdown, _dir, _sock) = setup().await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        // lcreateattr: the root fid stays usable afterwards (it is not turned
        // into the open file like Tlcreate would).
        let f = client.alloc_fid();
        let (stat, _iounit) = client
            .lcreateattr(
                ROOT_FID_TEST,
                f,
                b"file.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        assert_eq!(stat.mode & libc::S_IFMT, libc::S_IFREG);
        assert_eq!(stat.size, 0);
        assert_eq!(client.write(f, 0, b"hello").await.unwrap(), 5);
        assert_eq!(client.read(f, 0, 64).await.unwrap(), b"hello");

        // The returned stat must agree with a fresh walk+getattr.
        let wf = client.alloc_fid();
        let (_, wstat) = client
            .walk_getattr(ROOT_FID_TEST, wf, &[b"file.txt".as_ref()])
            .await
            .unwrap();
        assert_eq!(wstat.qid.path, stat.qid.path);
        assert_eq!(wstat.size, 5);

        // lopenat: the source fid stays unopened, so it can serve repeatedly.
        for _ in 0..2 {
            let of = client.alloc_fid();
            let (qid, _) = client.lopenat(wf, of, libc::O_RDONLY as u32).await.unwrap();
            assert_eq!(qid.path, stat.qid.path);
            assert_eq!(client.read(of, 0, 64).await.unwrap(), b"hello");
            client.clunk(of).await.unwrap();
            client.free_fid(of);
        }

        // setattr_attr returns the post-op stat.
        let post = client
            .setattr_attr(Tsetattr {
                fid: wf,
                valid: SETATTR_MODE,
                mode: 0o600,
                uid: 0,
                gid: 0,
                size: 0,
                atime_sec: 0,
                atime_nsec: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
            })
            .await
            .unwrap();
        assert_eq!(post.mode & 0o7777, 0o600);
        assert_eq!(post.qid.path, stat.qid.path);

        let dstat = client
            .mkdir_attr(ROOT_FID_TEST, b"dir", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        assert_eq!(dstat.mode & libc::S_IFMT, libc::S_IFDIR);

        let sstat = client
            .symlink_attr(ROOT_FID_TEST, b"sym", b"file.txt", 0)
            .await
            .unwrap();
        assert_eq!(sstat.mode & libc::S_IFMT, libc::S_IFLNK);

        let nstat = client
            .mknod_attr(ROOT_FID_TEST, b"fifo", libc::S_IFIFO | 0o644, 0, 0, 0)
            .await
            .unwrap();
        assert_eq!(nstat.mode & libc::S_IFMT, libc::S_IFIFO);

        let lstat = client
            .link_attr(ROOT_FID_TEST, wf, b"hardlink")
            .await
            .unwrap();
        assert_eq!(lstat.qid.path, stat.qid.path);
        assert_eq!(lstat.nlink, 2);

        client.clunk(wf).await.unwrap();
        client.free_fid(wf);
        client.clunk(f).await.unwrap();
        client.free_fid(f);
        shutdown.cancel();
    }

    // Compound fids require replay records.
    #[tokio::test]
    async fn reconnect_replays_compound_fids() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("compound.9p.sock");

        let shutdown = start_server(Arc::clone(&fs), sock.clone());
        let client = connect_with_retry(&sock).await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        let created = client.alloc_fid();
        client
            .lcreateattr(
                ROOT_FID_TEST,
                created,
                b"c.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(created, 0, b"compound").await.unwrap();

        // A second open handle on the same inode via lopenat from a path fid.
        let pf = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, pf, &[b"c.txt".as_ref()])
            .await
            .unwrap();
        let opened = client.alloc_fid();
        client
            .lopenat(pf, opened, libc::O_RDONLY as u32)
            .await
            .unwrap();

        // Drop the server and bring a new one up on the same socket.
        shutdown.cancel();
        for _ in 0..200 {
            if UnixStream::connect(&sock).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = std::fs::remove_file(&sock);
        let shutdown2 = start_server(Arc::clone(&fs), sock.clone());

        let data = tokio::time::timeout(Duration::from_secs(10), client.read(created, 0, 64))
            .await
            .expect("read timed out waiting for reconnect")
            .expect("lcreateattr fid lost across reconnect");
        assert_eq!(data, b"compound");
        let data = tokio::time::timeout(Duration::from_secs(10), client.read(opened, 0, 64))
            .await
            .expect("read timed out waiting for reconnect")
            .expect("lopenat fid lost across reconnect");
        assert_eq!(data, b"compound");

        client.clunk(created).await.unwrap();
        client.clunk(opened).await.unwrap();
        shutdown2.cancel();
    }

    /// Two independent connections (= two server sessions) must see each other's
    /// byte-range locks. This exercises the `lock`/`getlock` client methods and
    /// the server's cross-session conflict arbitration.
    #[tokio::test]
    async fn cross_session_lock_conflict() {
        let (a, shutdown, _dir, sock) = setup().await;
        let b = connect_with_retry(&sock).await;

        a.attach(ROOT_FID_TEST, NOFID, "root", "", 0).await.unwrap();
        b.attach(ROOT_FID_TEST, NOFID, "root", "", 0).await.unwrap();

        let af = a.alloc_fid();
        a.walk(ROOT_FID_TEST, af, &[]).await.unwrap();
        a.lcreate(
            af,
            b"locked.bin",
            (libc::O_RDWR | libc::O_CREAT) as u32,
            libc::S_IFREG | 0o644,
            0,
        )
        .await
        .unwrap();
        let status = a
            .lock(af, LockType::WriteLock, 0, 0, 0, 1234, b"host-a")
            .await
            .unwrap();
        assert!(matches!(status, LockStatus::Success));

        let bf = b.alloc_fid();
        let qids = b
            .walk(ROOT_FID_TEST, bf, &[b"locked.bin".as_ref()])
            .await
            .unwrap();
        assert_eq!(qids.len(), 1);
        match b
            .lock(bf, LockType::WriteLock, 0, 0, 0, 5678, b"host-b")
            .await
        {
            Err(ClientError::Errno(e)) => assert_eq!(e, libc::EAGAIN as u32),
            other => panic!("expected EAGAIN, got {other:?}"),
        }

        let rg = b
            .getlock(bf, LockType::WriteLock, 0, 0, 5678, b"host-b")
            .await
            .unwrap();
        assert!(matches!(rg.lock_type, LockType::WriteLock));
        assert_eq!(rg.proc_id, 1234);

        a.lock(af, LockType::Unlock, 0, 0, 0, 1234, b"host-a")
            .await
            .unwrap();
        let status = b
            .lock(bf, LockType::WriteLock, 0, 0, 0, 5678, b"host-b")
            .await
            .unwrap();
        assert!(matches!(status, LockStatus::Success));

        shutdown.cancel();
    }

    /// A write whose chunk fills the negotiated msize must not produce a frame
    /// larger than the msize. Negotiating the maximum msize removes the headroom
    /// between the msize and the codec's frame limit, so a Twrite sized with the
    /// (smaller) Rread overhead would overflow and the server would drop it.
    #[tokio::test]
    async fn write_fills_msize_without_overflow() {
        let (_warmup, shutdown, _dir, sock) = setup().await;
        let client = NinePClient::connect_unix(&sock, P9_MAX_MSIZE)
            .await
            .unwrap();
        assert_eq!(client.msize(), P9_MAX_MSIZE);

        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let fid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, fid, &[]).await.unwrap();
        client
            .lcreate(
                fid,
                b"big.bin",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();

        // Span just over one msize so the first chunk is a full-size Twrite.
        let payload: Vec<u8> = (0..(P9_MAX_MSIZE as usize + 4096))
            .map(|i| (i % 251) as u8)
            .collect();
        let n = client.write(fid, 0, &payload).await.unwrap();
        assert_eq!(n as usize, payload.len());

        let back = client.read(fid, 0, payload.len() as u32).await.unwrap();
        assert_eq!(back.len(), payload.len());
        assert_eq!(back, payload);

        client.clunk(fid).await.unwrap();
        shutdown.cancel();
    }

    /// Killing the server and bringing a new one up on the same socket (backed
    /// by the same durable filesystem) must transparently restore the session:
    /// the fids created before the drop keep working afterwards.
    #[tokio::test]
    async fn reconnect_replays_session() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("reconnect.9p.sock");

        let shutdown = start_server(Arc::clone(&fs), sock.clone());
        let client = connect_with_retry(&sock).await;

        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        // An open file handle we expect to survive the reconnect.
        let fid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, fid, &[]).await.unwrap();
        client
            .lcreate(
                fid,
                b"persist.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(fid, 0, b"before").await.unwrap();

        // Drop the server and wait for the socket to free up.
        shutdown.cancel();
        for _ in 0..200 {
            if UnixStream::connect(&sock).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = std::fs::remove_file(&sock);

        // Bring a new server up on the same socket, same filesystem.
        let shutdown2 = start_server(Arc::clone(&fs), sock.clone());

        // The fid was replayed (rebound + reopened): the read blocks through the
        // reconnect and then succeeds rather than erroring.
        let data = tokio::time::timeout(Duration::from_secs(10), client.read(fid, 0, 64))
            .await
            .expect("read timed out waiting for reconnect")
            .expect("read failed after reconnect");
        assert_eq!(data, b"before");

        // And we can keep using it.
        assert_eq!(client.write(fid, 6, b"after").await.unwrap(), 5);
        assert_eq!(
            client.read(fid, 0, 64).await.unwrap(),
            b"beforeafter".to_vec()
        );

        client.clunk(fid).await.unwrap();
        shutdown2.cancel();
    }

    /// A file renamed after its fid was established must still be reachable
    /// through that fid after a reconnect (POSIX: an open fd survives rename).
    /// Rebinding by inode id makes this hold regardless of the name change.
    #[tokio::test]
    async fn reconnect_after_rename_keeps_fid() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("rename.9p.sock");

        let shutdown = start_server(Arc::clone(&fs), sock.clone());
        let client = connect_with_retry(&sock).await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        // Create "a" (open), write to it, then rename a -> b while it's open.
        let fid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, fid, &[]).await.unwrap();
        client
            .lcreate(
                fid,
                b"a",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(fid, 0, b"payload").await.unwrap();
        client
            .renameat(ROOT_FID_TEST, b"a", ROOT_FID_TEST, b"b")
            .await
            .unwrap();

        // Force a reconnect.
        shutdown.cancel();
        for _ in 0..200 {
            if UnixStream::connect(&sock).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = std::fs::remove_file(&sock);
        let shutdown2 = start_server(Arc::clone(&fs), sock.clone());

        // The open fid must still read the (now-renamed) file.
        let data = tokio::time::timeout(Duration::from_secs(10), client.read(fid, 0, 64))
            .await
            .expect("read timed out waiting for reconnect")
            .expect("open fid lost across reconnect+rename");
        assert_eq!(data, b"payload");

        client.clunk(fid).await.unwrap();
        shutdown2.cancel();
    }

    /// A fid must survive a reconnect even if the name it was reached through is
    /// unlinked, as long as the inode lives on under another hard link. Rebinding
    /// by inode id handles this for free, where re-walking the name could not.
    #[tokio::test]
    async fn reconnect_after_hardlink_unlink_keeps_fid() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("hardlink.9p.sock");

        let shutdown = start_server(Arc::clone(&fs), sock.clone());
        let client = connect_with_retry(&sock).await;
        client
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();

        // Create "a", give it a second link "b", then open a fid via "a".
        let cfid = client.alloc_fid();
        client.walk(ROOT_FID_TEST, cfid, &[]).await.unwrap();
        client
            .lcreate(
                cfid,
                b"a",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(cfid, 0, b"shared").await.unwrap();
        client.link(ROOT_FID_TEST, cfid, b"b").await.unwrap();

        let fid = client.alloc_fid();
        client
            .walk(ROOT_FID_TEST, fid, &[b"a".as_ref()])
            .await
            .unwrap();
        client.lopen(fid, libc::O_RDONLY as u32).await.unwrap();

        // Drop the name "a" — the inode lives on via "b".
        client.unlinkat(ROOT_FID_TEST, b"a", 0).await.unwrap();

        // Force a reconnect.
        shutdown.cancel();
        for _ in 0..200 {
            if UnixStream::connect(&sock).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = std::fs::remove_file(&sock);
        let shutdown2 = start_server(Arc::clone(&fs), sock.clone());

        // The fid, reached via the now-gone name "a", still reads the inode.
        let data = tokio::time::timeout(Duration::from_secs(10), client.read(fid, 0, 64))
            .await
            .expect("read timed out waiting for reconnect")
            .expect("fid lost across reconnect after its name was unlinked");
        assert_eq!(data, b"shared");

        client.clunk(fid).await.unwrap();
        shutdown2.cancel();
    }

    /// Tattach with an aname roots the session at that directory: the returned
    /// qid is the subtree root's, a leading slash is optional, and anames that
    /// don't name an existing directory fail with the matching errno.
    #[tokio::test]
    async fn attach_with_aname_roots_session_at_subtree() {
        let (admin, shutdown, _dir, sock) = setup().await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let vol_qid = admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let vf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, vf, &[b"vol".as_ref()])
            .await
            .unwrap();
        let inner_qid = admin
            .mkdir(vf, b"inner", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let ff = admin.alloc_fid();
        admin.walk(vf, ff, &[]).await.unwrap();
        admin
            .lcreate(
                ff,
                b"f.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();

        let c2 = connect_with_retry(&sock).await;
        let fid = c2.alloc_fid();
        let q = c2.attach(fid, NOFID, "root", "vol", 0).await.unwrap();
        assert_eq!(q.type_, QID_TYPE_DIR);
        assert_eq!(q.path, vol_qid.path);

        // Leading slash and nesting.
        let fid2 = c2.alloc_fid();
        let q2 = c2
            .attach(fid2, NOFID, "root", "/vol/inner", 0)
            .await
            .unwrap();
        assert_eq!(q2.path, inner_qid.path);

        // "/" keeps the whole-filesystem root.
        let fid3 = c2.alloc_fid();
        let q3 = c2.attach(fid3, NOFID, "root", "/", 0).await.unwrap();
        assert_eq!(q3.path, 0);

        // A missing component fails the attach.
        let bad = c2.alloc_fid();
        match c2.attach(bad, NOFID, "root", "/missing", 0).await {
            Err(ClientError::Errno(e)) => assert_eq!(e, libc::ENOENT as u32),
            other => panic!("expected ENOENT, got {other:?}"),
        }

        // A non-directory aname fails the attach.
        match c2.attach(bad, NOFID, "root", "vol/f.txt", 0).await {
            Err(ClientError::Errno(e)) => assert_eq!(e, libc::ENOTDIR as u32),
            other => panic!("expected ENOTDIR, got {other:?}"),
        }

        shutdown.cancel();
    }

    /// Walking `..` on an aname-rooted session clamps at the subtree root (it
    /// resolves to the subtree root itself), mirroring the inode-0 clamp a
    /// whole-filesystem attach gets, so a fid can never climb out of its
    /// subtree.
    #[tokio::test]
    async fn aname_walk_up_clamps_at_subtree_root() {
        let (admin, shutdown, _dir, sock) = setup().await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let vol_qid = admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let vf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, vf, &[b"vol".as_ref()])
            .await
            .unwrap();
        let inner_qid = admin
            .mkdir(vf, b"inner", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();

        let c2 = connect_with_retry(&sock).await;
        let afid = c2.alloc_fid();
        c2.attach(afid, NOFID, "root", "vol", 0).await.unwrap();

        // `..` below the subtree root walks up normally...
        let f = c2.alloc_fid();
        let qids = c2.walk(afid, f, &[b"inner".as_ref()]).await.unwrap();
        assert_eq!(qids[0].path, inner_qid.path);
        let f2 = c2.alloc_fid();
        let qids = c2.walk(f, f2, &[b"..".as_ref()]).await.unwrap();
        assert_eq!(qids[0].path, vol_qid.path);

        // ...and at the subtree root it resolves to the root itself.
        let f3 = c2.alloc_fid();
        let qids = c2.walk(f2, f3, &[b"..".as_ref()]).await.unwrap();
        assert_eq!(qids[0].path, vol_qid.path);

        // A multi-component escape attempt stays clamped the whole way.
        let f4 = c2.alloc_fid();
        let qids = c2
            .walk(afid, f4, &[b"..".as_ref(), b"..".as_ref(), b"..".as_ref()])
            .await
            .unwrap();
        assert_eq!(qids.len(), 3);
        assert!(qids.iter().all(|q| q.path == vol_qid.path));

        // The whole-filesystem attach clamps `..` at inode 0 the same way.
        let rf = admin.alloc_fid();
        let qids = admin
            .walk(ROOT_FID_TEST, rf, &[b"..".as_ref()])
            .await
            .unwrap();
        assert_eq!(qids[0].path, 0);

        shutdown.cancel();
    }

    /// `..` stays clamped even when an intermediate ancestor is renamed out
    /// of the subtree while a fid below it is held: the parent chain no
    /// longer passes through the attach root, so `..` resolves to the root
    /// itself instead of following the detached chain to inode 0.
    #[tokio::test]
    async fn aname_walk_up_stays_clamped_after_ancestor_renamed_out() {
        let (admin, shutdown, _dir, sock) = setup().await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let vol_qid = admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        admin
            .mkdir(ROOT_FID_TEST, b"secret", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let vf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, vf, &[b"vol".as_ref()])
            .await
            .unwrap();
        admin
            .mkdir(vf, b"a", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let af = admin.alloc_fid();
        admin.walk(vf, af, &[b"a".as_ref()]).await.unwrap();
        let b_qid = admin
            .mkdir(af, b"b", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();

        // A confined session holds a fid deep inside the subtree.
        let c2 = connect_with_retry(&sock).await;
        let afid = c2.alloc_fid();
        c2.attach(afid, NOFID, "root", "vol", 0).await.unwrap();
        let bf = c2.alloc_fid();
        let qids = c2
            .walk(afid, bf, &[b"a".as_ref(), b"b".as_ref()])
            .await
            .unwrap();
        assert_eq!(qids[1].path, b_qid.path);

        // /vol/a moves to /a: b's parent chain now bypasses /vol entirely.
        admin.renameat(vf, b"a", ROOT_FID_TEST, b"a").await.unwrap();

        // `..` from the held fid clamps at the subtree root every step...
        let up = c2.alloc_fid();
        let qids = c2
            .walk(bf, up, &[b"..".as_ref(), b"..".as_ref()])
            .await
            .unwrap();
        assert!(
            qids.iter().all(|q| q.path == vol_qid.path),
            "`..` escaped the subtree after an ancestor rename: {qids:?}"
        );

        // ...so the rest of the filesystem stays unreachable.
        let esc = c2.alloc_fid();
        match c2.walk(up, esc, &[b"secret".as_ref()]).await {
            Err(ClientError::Errno(e)) => assert_eq!(e, libc::ENOENT as u32),
            other => panic!("expected ENOENT walking out of the subtree, got {other:?}"),
        }

        shutdown.cancel();
    }

    /// readdir at the attach root of an aname-rooted session reports ".." as
    /// the root itself (mirroring the inode-0 self-clamp), so the external
    /// parent's inode id never leaks into the subtree.
    #[tokio::test]
    async fn aname_readdir_dotdot_reports_subtree_root() {
        let (admin, shutdown, _dir, sock) = setup().await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let vf = admin.alloc_fid();
        admin.walk(ROOT_FID_TEST, vf, &[]).await.unwrap();
        admin
            .mkdir(ROOT_FID_TEST, b"parent", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let pf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, pf, &[b"parent".as_ref()])
            .await
            .unwrap();
        let vol_qid = admin
            .mkdir(pf, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();

        let c2 = connect_with_retry(&sock).await;
        let afid = c2.alloc_fid();
        c2.attach(afid, NOFID, "root", "parent/vol", 0)
            .await
            .unwrap();
        let df = c2.alloc_fid();
        c2.walk(afid, df, &[]).await.unwrap();
        c2.lopen(df, libc::O_RDONLY as u32).await.unwrap();

        let entries = c2.readdir(df, 0, 8192).await.unwrap();
        let dotdot = entries
            .iter()
            .find(|e| e.name.data.as_slice() == b"..")
            .expect("readdir returned no .. entry");
        assert_eq!(
            dotdot.qid.path, vol_qid.path,
            ".. at the attach root leaked the external parent"
        );

        shutdown.cancel();
    }

    /// Files created through an aname-rooted session land in (and read back
    /// from) the subtree the session is rooted at.
    #[tokio::test]
    async fn aname_session_creates_and_reads_files() {
        let (admin, shutdown, _dir, sock) = setup().await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();

        let c2 = connect_with_retry(&sock).await;
        let afid = c2.alloc_fid();
        c2.attach(afid, NOFID, "root", "vol", 0).await.unwrap();

        let f = c2.alloc_fid();
        c2.walk(afid, f, &[]).await.unwrap();
        c2.lcreate(
            f,
            b"data.txt",
            (libc::O_RDWR | libc::O_CREAT) as u32,
            libc::S_IFREG | 0o644,
            0,
        )
        .await
        .unwrap();
        assert_eq!(c2.write(f, 0, b"via subtree").await.unwrap(), 11);
        assert_eq!(c2.read(f, 0, 64).await.unwrap(), b"via subtree");
        c2.clunk(f).await.unwrap();

        // The file is at /vol/data.txt when seen from the filesystem root.
        let gf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, gf, &[b"vol".as_ref(), b"data.txt".as_ref()])
            .await
            .unwrap();
        let st = admin.getattr(gf, GETATTR_ALL).await.unwrap();
        assert_eq!(st.size, 11);

        shutdown.cancel();
    }

    /// Reconnect replay must re-attach with the original aname: open fids keep
    /// working afterwards, and the restored session is still clamped to its
    /// subtree (i.e. the aname wasn't lost in the replay).
    #[tokio::test]
    async fn reconnect_replays_aname_attach() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("aname.9p.sock");

        let shutdown = start_server(Arc::clone(&fs), sock.clone());
        let admin = connect_with_retry(&sock).await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let vol_qid = admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        drop(admin);

        let client = connect_with_retry(&sock).await;
        let afid = client.alloc_fid();
        client.attach(afid, NOFID, "root", "vol", 0).await.unwrap();

        // An open file handle inside the subtree.
        let fid = client.alloc_fid();
        client.walk(afid, fid, &[]).await.unwrap();
        client
            .lcreate(
                fid,
                b"persist.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(fid, 0, b"subtree").await.unwrap();

        // Drop the server and bring a new one up on the same socket.
        shutdown.cancel();
        for _ in 0..200 {
            if UnixStream::connect(&sock).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = std::fs::remove_file(&sock);
        let shutdown2 = start_server(Arc::clone(&fs), sock.clone());

        // The open fid was rebound + reopened inside the replayed session.
        let data = tokio::time::timeout(Duration::from_secs(10), client.read(fid, 0, 64))
            .await
            .expect("read timed out waiting for reconnect")
            .expect("open fid lost across reconnect");
        assert_eq!(data, b"subtree");

        // The re-attached root is still the subtree: `..` clamps at /vol, not
        // at the filesystem root.
        let nf = client.alloc_fid();
        let qids = client.walk(afid, nf, &[b"..".as_ref()]).await.unwrap();
        assert_eq!(qids[0].path, vol_qid.path);

        client.clunk(fid).await.unwrap();
        shutdown2.cancel();
    }

    /// Reconnect replay must fail CLOSED for a confined aname session. If the
    /// aname directory is renamed away during the outage, the replayed
    /// `Tattach("vol")` no longer resolves; the client must NOT silently drop
    /// the attach fid and rebind the held inode fids onto an unconfined
    /// (whole-filesystem, attach_root=0) session — that would let `..` climb
    /// past inode 0 into the sibling `/secret`. Instead the reconnect aborts
    /// and retries, so confinement is never lost; once the aname resolves again
    /// the session recovers, still clamped to its subtree.
    #[tokio::test]
    async fn reconnect_aname_rename_away_stays_confined() {
        use crate::fs::permissions::Credentials;
        use crate::fs::types::AuthContext;

        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("aname-confine.9p.sock");
        let root_auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: Vec::new(),
        };
        let root_creds = Credentials::from_auth_context(&root_auth);

        let shutdown = start_server(Arc::clone(&fs), sock.clone());

        // /vol/inside and a sibling /secret, set up out of band on the fs.
        let admin = connect_with_retry(&sock).await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let vf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, vf, &[b"vol".as_ref()])
            .await
            .unwrap();
        admin
            .mkdir(vf, b"inside", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let secret_qid = admin
            .mkdir(ROOT_FID_TEST, b"secret", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        drop(admin);

        // A session confined to /vol holds a fid deep inside it.
        let client = connect_with_retry(&sock).await;
        let afid = client.alloc_fid();
        client.attach(afid, NOFID, "root", "vol", 0).await.unwrap();
        let hf = client.alloc_fid();
        client.walk(afid, hf, &[b"inside".as_ref()]).await.unwrap();

        // Drop the server and, while it is down, rename /vol -> /vol_moved so
        // the aname "vol" no longer resolves on the fresh session. The inode
        // keeps its id, only the name changes.
        shutdown.cancel();
        for _ in 0..200 {
            if UnixStream::connect(&sock).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fs.rename(&root_auth, 0, b"vol", 0, b"vol_moved")
            .await
            .unwrap();
        let _ = std::fs::remove_file(&sock);
        let shutdown2 = start_server(Arc::clone(&fs), sock.clone());

        // The reconnect replay re-attaches with aname "vol", which now fails:
        // the client must abort and keep retrying rather than rebind onto an
        // unconfined session. So a request on the held fid must NOT escape to
        // the sibling /secret (or inode 0). With the aname gone for good the
        // reconnect can't complete, so the request hangs — which is the
        // fail-closed outcome; assert it does not return a sibling qid within
        // a generous window.
        let escape = tokio::time::timeout(Duration::from_secs(2), async {
            let up = client.alloc_fid();
            let r = client
                .walk(
                    hf,
                    up,
                    &[b"..".as_ref(), b"..".as_ref(), b"secret".as_ref()],
                )
                .await;
            client.free_fid(up);
            r
        })
        .await;
        if let Ok(Ok(qids)) = &escape {
            assert!(
                qids.iter()
                    .all(|q| q.path != secret_qid.path && q.path != 0),
                "CONFINEMENT ESCAPE after reconnect+aname-rename: reached {qids:?}"
            );
        }

        // Restore the aname: the reconnect can now complete, and the recovered
        // session is still confined — `..` from the held fid clamps at /vol,
        // and the sibling /secret stays unreachable.
        fs.rename(&root_auth, 0, b"vol_moved", 0, b"vol")
            .await
            .unwrap();
        let vol_id = fs.lookup(&root_creds, 0, b"vol").await.unwrap();
        let up = client.alloc_fid();
        let qids = tokio::time::timeout(
            Duration::from_secs(10),
            client.walk(hf, up, &[b"..".as_ref(), b"..".as_ref()]),
        )
        .await
        .expect("walk timed out waiting for reconnect to recover")
        .expect("held fid lost after the aname was restored");
        assert!(
            qids.iter().all(|q| q.path == vol_id),
            "`..` escaped the subtree after reconnect recovery: {qids:?}"
        );
        let esc = client.alloc_fid();
        match client.walk(up, esc, &[b"secret".as_ref()]).await {
            Err(ClientError::Errno(e)) => assert_eq!(e, libc::ENOENT as u32),
            other => panic!("expected ENOENT walking out of the subtree, got {other:?}"),
        }

        shutdown2.cancel();
    }

    /// Trebind on an aname-rooted session only accepts inodes whose parent
    /// chain passes through the attach root; anything outside the subtree
    /// (including the filesystem root itself) is refused.
    #[tokio::test]
    async fn aname_rebind_rejects_out_of_subtree_inodes() {
        let (admin, shutdown, _dir, sock) = setup().await;
        admin
            .attach(ROOT_FID_TEST, NOFID, "root", "", 0)
            .await
            .unwrap();
        let vol_qid = admin
            .mkdir(ROOT_FID_TEST, b"vol", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let other_qid = admin
            .mkdir(ROOT_FID_TEST, b"other", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let vf = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, vf, &[b"vol".as_ref()])
            .await
            .unwrap();
        let sub_qid = admin
            .mkdir(vf, b"sub", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let of = admin.alloc_fid();
        admin
            .walk(ROOT_FID_TEST, of, &[b"other".as_ref()])
            .await
            .unwrap();
        let ef = admin.alloc_fid();
        admin.walk(of, ef, &[]).await.unwrap();
        let (escapee_qid, _) = admin
            .lcreate(
                ef,
                b"escapee.txt",
                (libc::O_RDWR | libc::O_CREAT) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();

        let c2 = connect_with_retry(&sock).await;
        let afid = c2.alloc_fid();
        c2.attach(afid, NOFID, "root", "vol", 0).await.unwrap();

        // Out of subtree: a sibling directory, a file inside it, and the
        // filesystem root itself.
        let rf = c2.alloc_fid();
        for inode_id in [other_qid.path, escapee_qid.path, 0] {
            match c2.rebind(rf, inode_id, 0).await {
                Err(ClientError::Errno(e)) => assert_eq!(e, libc::EACCES as u32),
                other => panic!("rebind of inode {inode_id} should fail, got {other:?}"),
            }
        }

        // Inside the subtree (and the subtree root itself) still rebinds.
        let q = c2.rebind(rf, sub_qid.path, 0).await.unwrap();
        assert_eq!(q.path, sub_qid.path);
        let rf2 = c2.alloc_fid();
        let q = c2.rebind(rf2, vol_qid.path, 0).await.unwrap();
        assert_eq!(q.path, vol_qid.path);

        shutdown.cancel();
    }

    /// fid 0 is used as the attach root in these tests, mirroring the mount client.
    const ROOT_FID_TEST: u32 = 0;
}
