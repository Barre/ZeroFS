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
use dashmap::DashMap;
use fuser::{
    AccessFlags, Config, CopyFileRangeFlags, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, InitFlags, IoctlFlags, KernelConfig, LockOwner, MountOption,
    OpenFlags, PollEvents, PollFlags, PollNotifier, RenameFlags, ReplyAttr, ReplyBmap, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyLock,
    ReplyLseek, ReplyOpen, ReplyPoll, ReplyStatfs, ReplyWrite, ReplyXattr, Request, Session,
    SessionACL, TimeOrNow,
};
use ninep_client::{ClientError, NinePClient};
use ninep_proto::{
    GETATTR_ALL, LockStatus, LockType, P9_LOCK_FLAGS_BLOCK, SETATTR_ATIME, SETATTR_ATIME_SET,
    SETATTR_GID, SETATTR_MODE, SETATTR_MTIME, SETATTR_MTIME_SET, SETATTR_SIZE, SETATTR_UID, Stat,
    Tsetattr,
};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;

const DEFAULT_9P_PORT: u16 = 5564;
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

/// Read-ahead state for one open directory handle
#[derive(Default)]
struct DirRead {
    buf: VecDeque<ninep_proto::DirEntry>,
    /// 9P cookie for the next Treaddir
    fetch_cookie: u64,
    /// FUSE offset the next `readdir` call must carry to continue sequentially
    /// A different offset means the kernel seeked/rewound, so the buffer is discarded and refetched.
    resume_offset: u64,
    /// The server has no more entries past `fetch_cookie`.
    eof: bool,
}

/// Read-ahead state for one open directory handle served via readdirplus
/// (entries carry their stat). Mirrors [`DirRead`] but buffers `DirEntryPlus`.
#[derive(Default)]
struct DirReadPlus {
    buf: VecDeque<ninep_proto::DirEntryPlus>,
    fetch_cookie: u64,
    resume_offset: u64,
    eof: bool,
}

/// Options gathered from the CLI.
pub struct MountOptions {
    pub msize: u32,
    pub read_only: bool,
    pub access: MountAccess,
    pub writeback: bool,
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
    /// Attribute/entry cache lifetime returned to the kernel.
    ttl: Duration,
    /// When true, request the FUSE writeback cache so the kernel buffers writes
    /// and flushes them asynchronously (instead of synchronous write-through).
    writeback: bool,
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

fn split_time(t: SystemTime) -> (u64, u64) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs(), d.subsec_nanos() as u64),
        Err(_) => (0, 0),
    }
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
    match inodes.get(&ino) {
        Some(e) => {
            if let Some(&fid) = e.fids.get(&uid) {
                return Ok(fid);
            }
        }
        // The kernel only operates on inodes it has looked up; an untracked one
        // is stale.
        None => return Err(ClientError::Errno(libc::ESTALE as u32)),
    }
    // inode_id is the inverse of `ino_of` (ino - 1); bind a fresh fid as `uid`.
    let newfid = client.alloc_fid();
    if let Err(e) = client.rebind(newfid, ino.wrapping_sub(1), uid).await {
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

/// Walk `parent_fid` to `name`, getattr the result, and register (or reuse) the
/// inode entry, returning the child's attributes. Shared by lookup and every
/// operation that has to return a freshly created child's entry.
async fn resolve_child(
    client: &Arc<NinePClient>,
    inodes: &Arc<DashMap<u64, InodeEntry>>,
    uid: u32,
    parent_fid: u32,
    name: &[u8],
) -> Result<FileAttr, ClientError> {
    let newfid = client.alloc_fid();
    // With the fast path, walk+getattr is a single round trip; otherwise fall
    // back to a walk followed by a getattr.
    let stat = if client.extensions_enabled() {
        match client.walk_getattr(parent_fid, newfid, &[name]).await {
            // A successful walk_getattr is always a full walk, so the fid exists.
            Ok((_, stat)) => stat,
            Err(e) => {
                client.free_fid(newfid);
                return Err(e);
            }
        }
    } else {
        let qids = match client.walk(parent_fid, newfid, &[name]).await {
            Ok(q) => q,
            Err(e) => {
                client.free_fid(newfid);
                return Err(e);
            }
        };
        if qids.is_empty() {
            client.free_fid(newfid);
            return Err(ClientError::Errno(libc::ENOENT as u32));
        }
        match client.getattr(newfid, GETATTR_ALL).await {
            Ok(s) => s,
            Err(e) => {
                let _ = client.clunk(newfid).await;
                client.free_fid(newfid);
                return Err(e);
            }
        }
    };

    let ino = ino_of(stat.qid.path);
    // Count this lookup against the inode (forget balances it). Keep the freshly
    // walked fid only if this user has none for the inode yet; otherwise (hard
    // link, or already looked up by this user) discard it.
    let redundant = {
        use std::collections::hash_map::Entry;
        let mut entry = inodes.entry(ino).or_default();
        entry.lookup += 1;
        match entry.fids.entry(uid) {
            Entry::Occupied(_) => Some(newfid),
            Entry::Vacant(v) => {
                v.insert(newfid);
                None
            }
        }
    };
    if let Some(fid) = redundant {
        let _ = client.clunk(fid).await;
        client.free_fid(fid);
    }

    Ok(stat_to_attr(&stat))
}

impl Filesystem for Fuse9P {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Ask the kernel to forward fcntl byte-range locks to us; without this
        // capability it handles POSIX locks locally and never calls getlk/setlk.
        let _ = config.add_capabilities(InitFlags::FUSE_POSIX_LOCKS);

        // When the server speaks the fast path, let the kernel use readdirplus so
        // a directory listing returns entries with their attributes in one shot
        // (no lookup/getattr per entry). AUTO lets the kernel pick plain readdir
        // when it won't stat the entries.
        if self.client.extensions_enabled() {
            let _ = config.add_capabilities(
                InitFlags::FUSE_DO_READDIRPLUS | InitFlags::FUSE_READDIRPLUS_AUTO,
            );
        }

        if self.writeback
            && let Err(unsupported) = config.add_capabilities(InitFlags::FUSE_WRITEBACK_CACHE)
        {
            debug!(
                "kernel does not support FUSE_WRITEBACK_CACHE ({unsupported:?}); using write-through"
            );
            self.writeback = false;
        }

        let _ = config.set_max_background(64);
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
        // Once the inode's reference count hits zero, drop it and clunk every
        // per-user fid we held for it.
        if let Some((_, entry)) = self.inodes.remove_if(&ino, |_, e| e.lookup == 0) {
            let client = Arc::clone(&self.client);
            self.rt.spawn(async move {
                for fid in entry.fids.into_values() {
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
            let mut ts = Tsetattr {
                fid,
                valid: 0,
                mode: 0,
                uid: 0,
                gid: 0,
                size: 0,
                atime_sec: 0,
                atime_nsec: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
            };
            if let Some(m) = mode {
                ts.valid |= SETATTR_MODE;
                ts.mode = m;
            }
            if let Some(u) = uid {
                ts.valid |= SETATTR_UID;
                ts.uid = u;
            }
            if let Some(g) = gid {
                ts.valid |= SETATTR_GID;
                ts.gid = g;
            }
            if let Some(s) = size {
                ts.valid |= SETATTR_SIZE;
                ts.size = s;
            }
            match atime {
                Some(TimeOrNow::SpecificTime(t)) => {
                    let (sec, nsec) = split_time(t);
                    ts.valid |= SETATTR_ATIME | SETATTR_ATIME_SET;
                    ts.atime_sec = sec;
                    ts.atime_nsec = nsec;
                }
                Some(TimeOrNow::Now) => ts.valid |= SETATTR_ATIME,
                None => {}
            }
            match mtime {
                Some(TimeOrNow::SpecificTime(t)) => {
                    let (sec, nsec) = split_time(t);
                    ts.valid |= SETATTR_MTIME | SETATTR_MTIME_SET;
                    ts.mtime_sec = sec;
                    ts.mtime_nsec = nsec;
                }
                Some(TimeOrNow::Now) => ts.valid |= SETATTR_MTIME,
                None => {}
            }

            if let Err(e) = client.setattr(ts).await {
                reply.error(errno(&e));
                return;
            }
            match client.getattr(fid, GETATTR_ALL).await {
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
            if let Err(e) = client.mkdir(parent_fid, &name, mode, gid).await {
                reply.error(errno(&e));
                return;
            }
            match resolve_child(&client, &inodes, uid, parent_fid, &name).await {
                Ok(attr) => reply.entry(&ttl, &attr, Generation(0)),
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
            if let Err(e) = client
                .mknod(parent_fid, &name, mode, major, minor, gid)
                .await
            {
                reply.error(errno(&e));
                return;
            }
            match resolve_child(&client, &inodes, uid, parent_fid, &name).await {
                Ok(attr) => reply.entry(&ttl, &attr, Generation(0)),
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
            if let Err(e) = client.symlink(parent_fid, &name, &target, gid).await {
                reply.error(errno(&e));
                return;
            }
            match resolve_child(&client, &inodes, uid, parent_fid, &name).await {
                Ok(attr) => reply.entry(&ttl, &attr, Generation(0)),
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
            if let Err(e) = client.link(dir_fid, file_fid, &newname).await {
                reply.error(errno(&e));
                return;
            }
            match resolve_child(&client, &inodes, uid, dir_fid, &newname).await {
                Ok(attr) => reply.entry(&ttl, &attr, Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        // Files honor the writeback cache; directories never do.
        self.open_inner(req.uid(), ino, flags, reply, self.writeback);
    }

    fn opendir(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.open_inner(req.uid(), ino, flags, reply, false);
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
        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
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
                Ok(n) => reply.written(n),
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
            locks.unlock_range(ino, LOCAL_LOCK_FID, 0, 0, owner).await;
            reply.ok();
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.fsync_inner(fh, datasync, reply);
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.fsync_inner(fh, datasync, reply);
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.release_inner(fh, reply);
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.dir_reads.remove(&fh.0);
        self.dir_reads_plus.remove(&fh.0);
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
            .or_insert_with(|| {
                Arc::new(AsyncMutex::new(DirRead {
                    fetch_cookie: offset,
                    resume_offset: offset,
                    ..Default::default()
                }))
            })
            .clone();
        let batch = client.max_io().min(READDIR_BATCH);
        self.rt.spawn(async move {
            let mut st = state.lock().await;
            // A FUSE offset that doesn't continue where we left off means the
            // kernel seeked or rewound; drop the read-ahead and restart there.
            if offset != st.resume_offset {
                st.buf.clear();
                st.fetch_cookie = offset;
                st.resume_offset = offset;
                st.eof = false;
            }
            loop {
                if st.buf.is_empty() {
                    if st.eof {
                        break;
                    }
                    match client.readdir(fid, st.fetch_cookie, batch).await {
                        Ok(entries) if entries.is_empty() => st.eof = true,
                        Ok(entries) => {
                            st.fetch_cookie = entries.last().map_or(st.fetch_cookie, |e| e.offset);
                            st.buf.extend(entries);
                        }
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
        // Capability is only advertised when the server supports it, but guard
        // anyway: ENOSYS makes the kernel fall back to plain readdir.
        if !client.extensions_enabled() {
            reply.error(Errno::ENOSYS);
            return;
        }
        let inodes = Arc::clone(&self.inodes);
        let ttl = self.ttl;
        let fid = fh.0 as u32;
        let state = self
            .dir_reads_plus
            .entry(fh.0)
            .or_insert_with(|| {
                Arc::new(AsyncMutex::new(DirReadPlus {
                    fetch_cookie: offset,
                    resume_offset: offset,
                    ..Default::default()
                }))
            })
            .clone();
        let batch = client.max_io().min(READDIR_BATCH);
        self.rt.spawn(async move {
            let mut st = state.lock().await;
            // A FUSE offset that doesn't continue where we left off means the
            // kernel seeked or rewound; drop the read-ahead and restart there.
            if offset != st.resume_offset {
                st.buf.clear();
                st.fetch_cookie = offset;
                st.resume_offset = offset;
                st.eof = false;
            }
            loop {
                if st.buf.is_empty() {
                    if st.eof {
                        break;
                    }
                    match client.readdirplus(fid, st.fetch_cookie, batch).await {
                        Ok(entries) if entries.is_empty() => st.eof = true,
                        Ok(entries) => {
                            st.fetch_cookie = entries.last().map_or(st.fetch_cookie, |e| e.offset);
                            st.buf.extend(entries);
                        }
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
        let open_flags = if self.writeback {
            FopenFlags::FOPEN_KEEP_CACHE
        } else {
            FopenFlags::empty()
        };
        self.rt.spawn(async move {
            let parent_fid = match user_fid(&client, &inodes, uid, parent).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };

            // Clone the parent fid; lcreate then turns the clone into the open
            // file handle returned to the kernel.
            let open_fid = client.alloc_fid();
            if let Err(e) = client.walk(parent_fid, open_fid, &[]).await {
                client.free_fid(open_fid);
                reply.error(errno(&e));
                return;
            }
            if let Err(e) = client.lcreate(open_fid, &name, lflags, mode, gid).await {
                let _ = client.clunk(open_fid).await;
                client.free_fid(open_fid);
                reply.error(errno(&e));
                return;
            }

            // Register a separate path fid for the inode so future lookups and
            // getattrs work after this handle is released.
            match resolve_child(&client, &inodes, uid, parent_fid, &name).await {
                Ok(attr) => reply.created(
                    &ttl,
                    &attr,
                    Generation(0),
                    FileHandle(open_fid as u64),
                    open_flags,
                ),
                Err(e) => {
                    let _ = client.clunk(open_fid).await;
                    client.free_fid(open_fid);
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
            if let Some(conflict) = locks.check_would_block(ino, &test, owner).await {
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
                locks
                    .unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner)
                    .await;
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
                if locks.try_add_lock(owner, new_lock).await.is_none() {
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
                        locks
                            .unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner)
                            .await;
                        reply.error(Errno::ENOLCK);
                        return;
                    }
                    Err(e) => {
                        locks
                            .unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner)
                            .await;
                        reply.error(errno(&e));
                        return;
                    }
                }

                // The server reports a conflict with another client. Undo the
                // local grant, then wait and retry (blocking) or report EAGAIN.
                locks
                    .unlock_range(ino, LOCAL_LOCK_FID, s9, len9, owner)
                    .await;
                if sleep {
                    tokio::time::sleep(LOCK_POLL).await;
                    continue;
                }
                reply.error(Errno::EAGAIN);
                return;
            }
        });
    }

    // Operations we don't support over the 9P backend. Each returns the same
    // ENOSYS that fuser's default trait method would, but without the per-call
    // "[Not Implemented]" WARN — callers (cp, xattr tools, etc.) fall back.
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
        _fh: FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOSYS);
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

impl Fuse9P {
    fn open_inner(
        &self,
        uid: u32,
        ino: INodeNo,
        flags: OpenFlags,
        reply: ReplyOpen,
        writeback: bool,
    ) {
        let client = Arc::clone(&self.client);
        let inodes = Arc::clone(&self.inodes);
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
        let open_flags = if writeback {
            FopenFlags::FOPEN_KEEP_CACHE
        } else {
            FopenFlags::empty()
        };
        self.rt.spawn(async move {
            let inode_fid = match user_fid(&client, &inodes, uid, ino).await {
                Ok(f) => f,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            let fid = client.alloc_fid();
            if let Err(e) = client.walk(inode_fid, fid, &[]).await {
                client.free_fid(fid);
                reply.error(errno(&e));
                return;
            }
            let mut res = client.lopen(fid, lflags).await;
            if res.is_err() && upgrade {
                res = client.lopen(fid, orig).await;
            }
            match res {
                Ok(_) => reply.opened(FileHandle(fid as u64), open_flags),
                Err(e) => {
                    let _ = client.clunk(fid).await;
                    client.free_fid(fid);
                    reply.error(errno(&e));
                }
            }
        });
    }

    fn release_inner(&self, fh: FileHandle, reply: ReplyEmpty) {
        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
        self.rt.spawn(async move {
            let _ = client.clunk(fid).await;
            client.free_fid(fid);
            reply.ok();
        });
    }

    fn fsync_inner(&self, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let client = Arc::clone(&self.client);
        let fid = fh.0 as u32;
        self.rt.spawn(async move {
            match client.fsync(fid, datasync as u32).await {
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
    if let Some(rest) = target.strip_prefix("unix:") {
        let path = rest.strip_prefix("//").unwrap_or(rest);
        return NinePClient::connect_unix(path, msize)
            .await
            .with_context(|| format!("connecting to 9P unix socket {path}"));
    }

    let hostport = target.strip_prefix("tcp://").unwrap_or(target);

    // A path-like target without a scheme is treated as a Unix socket.
    if hostport.starts_with('/') || hostport.starts_with('.') {
        return NinePClient::connect_unix(hostport, msize)
            .await
            .with_context(|| format!("connecting to 9P unix socket {hostport}"));
    }

    let addr = resolve_addr(hostport).await?;
    NinePClient::connect_tcp(addr, msize)
        .await
        .with_context(|| format!("connecting to 9P server {addr}"))
}

async fn resolve_addr(s: &str) -> Result<std::net::SocketAddr> {
    if let Ok(addr) = s.parse::<std::net::SocketAddr>() {
        return Ok(addr);
    }
    let with_port = if s.contains(':') {
        s.to_string()
    } else {
        format!("{s}:{DEFAULT_9P_PORT}")
    };
    tokio::net::lookup_host(&with_port)
        .await
        .with_context(|| format!("resolving {with_port}"))?
        .next()
        .ok_or_else(|| anyhow!("no addresses resolved for {with_port}"))
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

    // No attach: every request binds its own per-user fid by inode id (the root
    // is inode 0). Seed the root inode with an empty per-user fid set.
    let inodes: Arc<DashMap<u64, InodeEntry>> = Arc::new(DashMap::new());
    inodes.insert(
        FUSE_ROOT,
        InodeEntry {
            lookup: u64::MAX,
            fids: HashMap::new(),
        },
    );

    let fs = Fuse9P {
        client,
        rt: Handle::current(),
        inodes,
        locks: Arc::new(FileLockManager::new()),
        client_id: Arc::new(node_name()),
        dir_reads: Arc::new(DashMap::new()),
        dir_reads_plus: Arc::new(DashMap::new()),
        ttl: Duration::from_secs(1),
        writeback: opts.writeback,
    };

    let mut config = Config::default();
    let mut mount_options = vec![
        // The device string is the connection target verbatim (like 9p/NFS show
        // the host/socket), so tools that match a mount by its device — e.g.
        // xfstests' _check_mounted_on — line up. The "zerofs" subtype identifies
        // the filesystem instead.
        MountOption::FSName(target.clone()),
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
    tokio::task::spawn_blocking(move || bg.umount_and_join())
        .await
        .map_err(|e| anyhow!("unmount task panicked: {e}"))?
        .with_context(|| format!("failed to unmount {} (is it still in use?)", mp.display()))?;

    Ok(())
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

    /// The fast-path extensions: the real connect path negotiates `9P2000.L.zerofs`,
    /// `walk_getattr` matches a separate walk+getattr, and `readdirplus` returns
    /// each entry with its correct stat.
    #[tokio::test]
    async fn walk_getattr_and_readdirplus() {
        let (client, shutdown, _dir, _sock) = setup().await;
        assert!(
            client.extensions_enabled(),
            "server should negotiate the .zerofs extensions"
        );

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

    /// fid 0 is used as the attach root in these tests, mirroring the mount client.
    const ROOT_FID_TEST: u32 = 0;
}
