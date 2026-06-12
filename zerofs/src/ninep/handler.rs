use super::errors::{P9Error, P9Result};
use super::lock_manager::{FileLock, FileLockManager};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::tracing::FileOperation;
use crate::fs::types::{
    AuthContext, FileAttributes, FileType, InodeWithId, SetAttributes, SetGid, SetMode, SetSize,
    SetTime, SetUid, Timestamp,
};
use bytes::Bytes;
use dashmap::DashMap;
use ninep_proto::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use tracing::debug;

pub const DEFAULT_MSIZE: u32 = 256 * 1024;

pub const AT_REMOVEDIR: u32 = 0x200;
// Linux dirent type constants
pub const DT_DIR: u8 = 4;
pub const DT_REG: u8 = 8;
pub const DT_LNK: u8 = 10;
pub const DT_CHR: u8 = 2;
pub const DT_BLK: u8 = 6;
pub const DT_FIFO: u8 = 1;
pub const DT_SOCK: u8 = 12;

// File mode type bits (S_IF* constants)
pub const S_IFREG: u32 = 0o100000; // Regular file
pub const S_IFDIR: u32 = 0o040000; // Directory
pub const S_IFLNK: u32 = 0o120000; // Symbolic link
pub const S_IFCHR: u32 = 0o020000; // Character device
pub const S_IFBLK: u32 = 0o060000; // Block device
pub const S_IFIFO: u32 = 0o010000; // FIFO
pub const S_IFSOCK: u32 = 0o140000; // Socket

// Default permissions for symbolic links
pub const SYMLINK_DEFAULT_MODE: u32 = 0o777;

// Default block size for stat
pub const DEFAULT_BLKSIZE: u64 = 4096;

// Block size for calculating block count
pub const BLOCK_SIZE: u64 = 512;

/// Upper bound on the number of parent-pointer hops an aname membership walk
/// (`..` clamp and `Trebind` confinement check) will follow before giving up.
/// A single committed snapshot is always acyclic, but these walks read each
/// ancestor unlocked across separate `get`s, so a pathological concurrent
/// rename workload could keep re-pointing two ancestors at each other and make
/// the walk ping-pong forever. Capping it turns that liveness hole into a safe
/// clamp/deny. The bound is far above any real directory depth.
const MAX_ANCESTOR_HOPS: u32 = 100_000;

// Represents an open file handle
#[derive(Debug, Clone)]
pub struct Fid {
    pub path: Vec<bytes::Bytes>,
    pub inode_id: InodeId,
    /// The attach root this fid descends from (the inode the originating
    /// Tattach's aname resolved to; 0 for a whole-filesystem attach). Walking
    /// `..` clamps here, so the fid can never climb out of its subtree.
    pub root: InodeId,
    pub qid: Qid,
    pub opened: bool,
    pub mode: Option<u32>,
    pub creds: Credentials, // Store credentials per fid/session
}

#[derive(Debug)]
pub struct Session {
    pub msize: AtomicU32,
    pub fids: Arc<DashMap<u32, Fid>>,
    /// The inode the session's last Tattach aname resolved to (0 when the
    /// session is rooted at the whole filesystem). Trebind validates rebound
    /// inodes against this subtree.
    pub attach_root: AtomicU64,
}

impl From<&Tsetattr> for SetAttributes {
    fn from(ts: &Tsetattr) -> Self {
        SetAttributes {
            mode: if ts.valid & SETATTR_MODE != 0 {
                SetMode::Set(ts.mode)
            } else {
                SetMode::NoChange
            },
            uid: if ts.valid & SETATTR_UID != 0 {
                SetUid::Set(ts.uid)
            } else {
                SetUid::NoChange
            },
            gid: if ts.valid & SETATTR_GID != 0 {
                SetGid::Set(ts.gid)
            } else {
                SetGid::NoChange
            },
            size: if ts.valid & SETATTR_SIZE != 0 {
                SetSize::Set(ts.size)
            } else {
                SetSize::NoChange
            },
            atime: if ts.valid & SETATTR_ATIME_SET != 0 {
                SetTime::SetToClientTime(Timestamp {
                    seconds: ts.atime_sec,
                    nanoseconds: ts.atime_nsec as u32,
                })
            } else if ts.valid & SETATTR_ATIME != 0 {
                SetTime::SetToServerTime
            } else {
                SetTime::NoChange
            },
            mtime: if ts.valid & SETATTR_MTIME_SET != 0 {
                SetTime::SetToClientTime(Timestamp {
                    seconds: ts.mtime_sec,
                    nanoseconds: ts.mtime_nsec as u32,
                })
            } else if ts.valid & SETATTR_MTIME != 0 {
                SetTime::SetToServerTime
            } else {
                SetTime::NoChange
            },
        }
    }
}

#[derive(Clone)]
pub struct NinePHandler {
    filesystem: Arc<ZeroFS>,
    session: Arc<Session>,
    lock_manager: Arc<FileLockManager>,
    handler_id: u64,
    /// If set, overrides the client-provided credentials on attach.
    credential_override: Option<(u32, u32)>,
}

impl NinePHandler {
    pub fn new(filesystem: Arc<ZeroFS>, lock_manager: Arc<FileLockManager>) -> Self {
        static HANDLER_COUNTER: AtomicU64 = AtomicU64::new(1);

        let session = Arc::new(Session {
            msize: AtomicU32::new(DEFAULT_MSIZE),
            fids: Arc::new(DashMap::new()),
            attach_root: AtomicU64::new(0),
        });

        Self {
            filesystem,
            session,
            lock_manager,
            handler_id: HANDLER_COUNTER.fetch_add(1, AtomicOrdering::SeqCst),
            credential_override: None,
        }
    }

    /// Set a (uid, gid) override that will be used for all sessions instead of
    /// trusting the client-provided credentials in Tattach.
    #[cfg(feature = "webui")]
    pub fn with_credential_override(mut self, uid: u32, gid: u32) -> Self {
        self.credential_override = Some((uid, gid));
        self
    }

    pub fn handler_id(&self) -> u64 {
        self.handler_id
    }

    /// Returns the effective GID for a create-type operation. When a credential
    /// override is active, the server-configured GID is used instead of
    /// whatever the client sent.
    fn effective_gid(&self, client_gid: u32, fid_creds: &Credentials) -> u32 {
        if self.credential_override.is_some() {
            fid_creds.gid
        } else {
            client_gid
        }
    }

    /// Per 9P spec, iounit may be zero, in which case the client calculates
    /// the maximum I/O size based on the negotiated msize.
    fn iounit(&self) -> u32 {
        0
    }

    fn get_fid(&self, fid: u32) -> P9Result<Fid> {
        self.session
            .fids
            .get(&fid)
            .ok_or(P9Error::BadFid)
            .map(|f| f.clone())
    }

    pub async fn handle_message(&self, tag: u16, msg: Message) -> P9Message {
        let result = match msg {
            Message::Tversion(tv) => self.version(tv).await,
            Message::Tattach(ta) => self.attach(ta).await,
            Message::Twalk(tw) => self.walk(tw).await,
            Message::Tlopen(tl) => self.lopen(tl).await,
            Message::Tlcreate(tc) => self.lcreate(tc).await,
            Message::Tread(tr) => self.read(tr).await,
            Message::Twrite(tw) => self.write(tw).await,
            Message::Tclunk(tc) => Ok(self.clunk(tc).await),
            Message::Treaddir(tr) => self.readdir(tr).await,
            Message::Tgetattr(tg) => self.getattr(tg).await,
            Message::Tsetattr(ts) => self.setattr(ts).await,
            Message::Tmkdir(tm) => self.mkdir(tm).await,
            Message::Tsymlink(ts) => self.symlink(ts).await,
            Message::Tmknod(tm) => self.mknod(tm).await,
            Message::Treadlink(tr) => self.readlink(tr).await,
            Message::Tlink(tl) => self.link(tl).await,
            Message::Trename(tr) => self.rename(tr).await,
            Message::Trenameat(tr) => self.renameat(tr).await,
            Message::Tunlinkat(tu) => self.unlinkat(tu).await,
            Message::Tfsync(tf) => self.fsync(tf).await,
            Message::Tflush(_) => Ok(Message::Rflush(Rflush)),
            Message::Txattrwalk(_) => Err(P9Error::NotSupported),
            Message::Tstatfs(ts) => self.statfs(ts).await,
            Message::Tlock(tl) => self.lock(tl).await,
            Message::Tgetlock(tg) => self.getlock(tg).await,
            Message::Trebind(tr) => self.rebind(tr).await,
            Message::Twalkgetattr(tw) => self.walk_getattr(tw).await,
            Message::Treaddirattr(tr) => self.readdir_attr(tr).await,
            Message::Tlopenat(tl) => self.lopenat(tl).await,
            Message::Tlcreateattr(tc) => self.lcreateattr(tc).await,
            Message::Tmkdirattr(tm) => self.mkdir_attr(tm).await,
            Message::Tsymlinkattr(ts) => self.symlink_attr(ts).await,
            Message::Tmknodattr(tm) => self.mknod_attr(tm).await,
            Message::Tlinkattr(tl) => self.link_attr(tl).await,
            Message::Tsetattrattr(ts) => self.setattr_attr(ts).await,
            _ => Err(P9Error::NotImplemented),
        };

        match result {
            Ok(body) => P9Message::new(tag, body),
            Err(e) => P9Message::new(
                tag,
                Message::Rlerror(Rlerror {
                    ecode: e.to_errno(),
                }),
            ),
        }
    }

    async fn version(&self, tv: Tversion) -> P9Result<Message> {
        let version_str = tv.version.as_str().map_err(|e| {
            debug!("Invalid version string encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;

        debug!("Client requested version: {}", version_str);

        // Per 9P spec, Tversion resets all connection state.
        // All fids are implicitly clunked and the session is reset.
        self.session.fids.clear();
        self.session.attach_root.store(0, AtomicOrdering::Relaxed);

        if !version_str.contains("9P2000.L") {
            // We only support 9P2000.L
            debug!("Client doesn't support 9P2000.L, returning unknown");
            return Ok(Message::Rversion(Rversion {
                msize: tv.msize,
                version: P9String::new(b"unknown".to_vec()),
            }));
        }

        let msize = tv.msize.min(P9_MAX_MSIZE);
        self.session.msize.store(msize, AtomicOrdering::Relaxed);

        // Advertise the highest ZeroFS extension level the client asked for
        // v9fs proposes plain `9P2000.L`, so it gets
        // the plain reply and the extension handlers are not used. An old client
        // proposing `.zerofs` gets `.zerofs` back and never sends the compound
        // messages.
        let version = if version_str.contains(".zerofs2") {
            VERSION_9P2000L_ZEROFS2
        } else if version_str.contains(".zerofs") {
            VERSION_9P2000L_ZEROFS
        } else {
            VERSION_9P2000L
        };

        Ok(Message::Rversion(Rversion {
            msize,
            version: P9String::new(version.to_vec()),
        }))
    }

    /// Build per-fid credentials from a client-supplied `n_uname` (the 9P2000.L
    /// numeric uid), honoring any credential override. `username` is only
    /// consulted for the `n_uname == -1` ("unspecified") fallback.
    fn creds_for(&self, n_uname: u32, username: &str) -> Credentials {
        let (uid, gid) = if let Some((override_uid, override_gid)) = self.credential_override {
            (override_uid, override_gid)
        } else {
            // In 9P2000.L we trust the client and use UID as GID as a reasonable
            // default. n_uname = -1 (0xFFFFFFFF) means "unspecified": map by name.
            let uid = if n_uname == 0xFFFFFFFF {
                match username {
                    "root" => 0,
                    _ => P9_NOBODY_UID,
                }
            } else {
                n_uname
            };
            (uid, uid)
        };
        let mut groups = [0u32; P9_MAX_GROUPS];
        groups[0] = gid; // User is always a member of their primary group.
        Credentials {
            uid,
            gid,
            groups,
            groups_count: 1,
        }
    }

    async fn attach(&self, ta: Tattach) -> P9Result<Message> {
        let username = ta.uname.as_str().map_err(|e| {
            debug!("Invalid username encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;

        debug!(
            "attach: fid={}, afid={}, uname={}, aname={:?}, n_uname={}",
            ta.fid,
            ta.afid,
            username,
            ta.aname.as_str().ok(),
            ta.n_uname
        );

        let creds = self.creds_for(ta.n_uname, username);

        if self.session.fids.contains_key(&ta.fid) {
            return Err(P9Error::FidInUse);
        }

        // aname selects the subtree the session is rooted at: a path from the
        // filesystem root whose components must all be existing directories.
        // "" and "/" (no components) keep the whole-filesystem root.
        let aname = ta.aname.as_str().map_err(|e| {
            debug!("Invalid aname encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;
        let components: Vec<&str> = aname.split('/').filter(|c| !c.is_empty()).collect();

        let mut root_id: InodeId = 0;
        for name in &components {
            root_id = self
                .filesystem
                .lookup(&creds, root_id, name.as_bytes())
                .await?;
        }
        let root_inode = self.filesystem.inode_store.get(root_id).await?;
        if !matches!(root_inode, Inode::Directory(_)) {
            return Err(P9Error::NotADirectory);
        }

        let qid = inode_to_qid(&root_inode, root_id);

        self.session
            .attach_root
            .store(root_id, AtomicOrdering::Relaxed);
        self.session.fids.insert(
            ta.fid,
            Fid {
                // The fid's path stays absolute (rename resolves it from
                // inode 0), so it starts at the aname components.
                path: components
                    .iter()
                    .map(|c| Bytes::copy_from_slice(c.as_bytes()))
                    .collect(),
                inode_id: root_id,
                root: root_id,
                qid: qid.clone(),
                opened: false,
                mode: None,
                creds,
            },
        );

        Ok(Message::Rattach(Rattach { qid }))
    }

    /// Bind a fresh fid to an existing inode by id, for a client recovering its
    /// session after a reconnect. `ENOENT` if the inode is gone (deleted during
    /// the outage), which the client takes as "this fid is gone".
    async fn rebind(&self, tr: Trebind) -> P9Result<Message> {
        // Like attach/walk, refuse to clobber a fid that's already in use.
        if self.session.fids.contains_key(&tr.fid) {
            return Err(P9Error::FidInUse);
        }
        let inode = self.filesystem.inode_store.get(tr.inode_id).await?;

        // On an aname-rooted session, only inodes still under the attach
        // subtree may be rebound: walk the parent chain up to the attach root.
        // Best-effort hygiene, not a security boundary (aname is client-chosen):
        // a hardlinked file has no parent chain (`parent()` is `None`) and is
        // refused here even if one of its links lives inside the subtree.
        let attach_root = self.session.attach_root.load(AtomicOrdering::Relaxed);
        if attach_root != 0 {
            let mut current_id = tr.inode_id;
            let mut current = inode.clone();
            let mut hops = 0u32;
            while current_id != attach_root {
                if hops >= MAX_ANCESTOR_HOPS {
                    // The chain is implausibly long or being ping-ponged by a
                    // concurrent rename workload: deny (the safe direction)
                    // rather than spin.
                    return Err(P9Error::Fs(FsError::PermissionDenied));
                }
                hops += 1;
                if current_id == 0 {
                    // Reached the filesystem root without passing the attach
                    // root: the inode is outside the subtree.
                    return Err(P9Error::Fs(FsError::PermissionDenied));
                }
                current_id = current
                    .parent()
                    .ok_or(P9Error::Fs(FsError::PermissionDenied))?;
                current = self.filesystem.inode_store.get(current_id).await?;
            }
        }

        let creds = self.creds_for(tr.n_uname, "");
        let path = self
            .filesystem
            .inode_store
            .resolve_path_components(tr.inode_id)
            .await
            .into_iter()
            .map(Bytes::from)
            .collect();
        let qid = inode_to_qid(&inode, tr.inode_id);

        self.session.fids.insert(
            tr.fid,
            Fid {
                path,
                inode_id: tr.inode_id,
                root: attach_root,
                qid: qid.clone(),
                opened: false,
                mode: None,
                creds,
            },
        );

        Ok(Message::Rrebind(Rrebind { qid }))
    }

    /// Resolve one walk component from `current_id`. `..` resolves through the
    /// directory's parent pointer, clamped at `root_id` (the fid's attach
    /// root): walking `..` at the subtree root yields the subtree root itself,
    /// exactly mirroring the inode-0 clamp readdir applies at the filesystem
    /// root. Everything else is a plain lookup.
    async fn walk_component(
        &self,
        creds: &Credentials,
        root_id: InodeId,
        current_id: InodeId,
        name: &[u8],
    ) -> P9Result<InodeId> {
        if name != b".." {
            return Ok(self.filesystem.lookup(creds, current_id, name).await?);
        }
        let inode = self.filesystem.inode_store.get(current_id).await?;
        let Inode::Directory(dir) = &inode else {
            return Err(P9Error::NotADirectory);
        };
        check_access(&inode, creds, AccessMode::Execute)?;
        if current_id == root_id {
            return Ok(current_id);
        }
        // On an aname-rooted fid the parent pointer alone is not enough: a
        // concurrent rename can re-point an ancestor outside the subtree, so
        // `..` is only honored while the parent chain still passes through
        // the attach root. A detached chain clamps to the root itself, the
        // same way `..` at the root does (mirrors the membership walk rebind
        // performs).
        if root_id != 0 {
            let mut ancestor = dir.parent;
            let mut hops = 0u32;
            while ancestor != root_id {
                if ancestor == 0 || hops >= MAX_ANCESTOR_HOPS {
                    // Reached the filesystem root (or an implausibly long /
                    // concurrently re-pointed chain) without passing the attach
                    // root: clamp to the root, the same safe direction as a
                    // detached chain.
                    return Ok(root_id);
                }
                hops += 1;
                let Inode::Directory(d) = self.filesystem.inode_store.get(ancestor).await? else {
                    return Ok(root_id);
                };
                ancestor = d.parent;
            }
        }
        Ok(dir.parent)
    }

    /// Keep a fid's absolute path in step with a walk: `..` pops a component
    /// (unless clamped at the attach root, where `current_id == root`), any
    /// other name appends. Shared by `walk` and `walk_getattr`.
    fn step_path(path: &mut Vec<Bytes>, name: Bytes, current_id: InodeId, root: InodeId) {
        if name.as_ref() == b".." {
            if current_id != root {
                path.pop();
            }
        } else {
            path.push(name);
        }
    }

    async fn walk(&self, tw: Twalk) -> P9Result<Message> {
        let src_fid = self.get_fid(tw.fid)?;

        let mut current_path = src_fid.path.clone();
        let mut current_id = src_fid.inode_id;
        let mut wqids = Vec::new();

        for (i, wname) in tw.wnames.iter().enumerate() {
            let name_bytes = Bytes::copy_from_slice(&wname.data);

            let creds = src_fid.creds;
            let child_id = match self
                .walk_component(&creds, src_fid.root, current_id, &name_bytes)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    // Per 9P spec: if first element fails, return error.
                    // If later element fails, return partial Rwalk with qids so far.
                    if i == 0 {
                        return Err(e);
                    }
                    // Partial walk - return what we have so far (newfid is NOT created)
                    return Ok(Message::Rwalk(Rwalk {
                        nwqid: wqids.len() as u16,
                        wqids,
                    }));
                }
            };

            let child_inode = match self.filesystem.inode_store.get(child_id).await {
                Ok(inode) => inode,
                Err(e) => {
                    if i == 0 {
                        return Err(e.into());
                    }
                    return Ok(Message::Rwalk(Rwalk {
                        nwqid: wqids.len() as u16,
                        wqids,
                    }));
                }
            };

            Self::step_path(&mut current_path, name_bytes, current_id, src_fid.root);
            wqids.push(inode_to_qid(&child_inode, child_id));
            current_id = child_id;
        }

        // Only create newfid if the walk fully succeeded
        if tw.newfid != tw.fid || !tw.wnames.is_empty() {
            // Check if newfid is already in use
            if tw.newfid != tw.fid && self.session.fids.contains_key(&tw.newfid) {
                return Err(P9Error::FidInUse);
            }

            let new_fid = Fid {
                path: current_path,
                inode_id: current_id,
                root: src_fid.root,
                qid: wqids.last().cloned().unwrap_or(src_fid.qid),
                opened: false,
                mode: None,
                creds: src_fid.creds, // Inherit credentials from source fid
            };
            self.session.fids.insert(tw.newfid, new_fid);
        }

        Ok(Message::Rwalk(Rwalk {
            nwqid: wqids.len() as u16,
            wqids,
        }))
    }

    // ZeroFS fast path: a full walk that also returns the final inode's stat, so
    // the client's lookup costs one round trip instead of walk + getattr. Unlike
    // Twalk there is no partial result. Any miss is a hard error.
    async fn walk_getattr(&self, tw: Twalkgetattr) -> P9Result<Message> {
        let src_fid = self.get_fid(tw.fid)?;

        let mut current_path = src_fid.path.clone();
        let mut current_id = src_fid.inode_id;
        let mut wqids = Vec::new();
        let mut last_inode = None;

        for wname in tw.wnames.iter() {
            let name_bytes = Bytes::copy_from_slice(&wname.data);
            let child_id = self
                .walk_component(&src_fid.creds, src_fid.root, current_id, &name_bytes)
                .await?;
            let child_inode = self.filesystem.inode_store.get(child_id).await?;
            Self::step_path(&mut current_path, name_bytes, current_id, src_fid.root);
            wqids.push(inode_to_qid(&child_inode, child_id));
            current_id = child_id;
            last_inode = Some(child_inode);
        }

        if tw.newfid != tw.fid && self.session.fids.contains_key(&tw.newfid) {
            return Err(P9Error::FidInUse);
        }
        self.session.fids.insert(
            tw.newfid,
            Fid {
                path: current_path,
                inode_id: current_id,
                root: src_fid.root,
                qid: wqids.last().cloned().unwrap_or_else(|| src_fid.qid.clone()),
                opened: false,
                mode: None,
                creds: src_fid.creds,
            },
        );

        // For a named walk the final inode is already in hand; an empty walk
        // (clone) returns the source inode's stat.
        let stat_inode = match last_inode {
            Some(inode) => inode,
            None => self.filesystem.inode_store.get(current_id).await?,
        };
        Ok(Message::Rwalkgetattr(Rwalkgetattr {
            nwqid: wqids.len() as u16,
            wqids,
            stat: inode_to_stat(&stat_inode, current_id),
        }))
    }

    async fn lopen(&self, tl: Tlopen) -> P9Result<Message> {
        let fid_entry = self.get_fid(tl.fid)?;

        if fid_entry.opened {
            return Err(P9Error::FidAlreadyOpen);
        }

        let inode_id = fid_entry.inode_id;
        let creds = fid_entry.creds;

        debug!(
            "lopen: fid={}, inode_id={}, uid={}, gid={}, flags={:#x}",
            tl.fid, inode_id, creds.uid, creds.gid, tl.flags
        );

        let inode = self.filesystem.inode_store.get(inode_id).await?;

        let qid = inode_to_qid(&inode, inode_id);

        if let Some(mut fid_entry) = self.session.fids.get_mut(&tl.fid) {
            fid_entry.qid = qid.clone();
            fid_entry.opened = true;
            fid_entry.mode = Some(tl.flags);
        }

        Ok(Message::Rlopen(Rlopen {
            qid,
            iounit: self.iounit(),
        }))
    }

    // ZeroFS fast path: open `fid`'s inode on a fresh `newfid`, leaving `fid`
    // untouched, so the client's open costs one round trip instead of the
    // Twalk(clone) + Tlopen pair that standard Tlopen's fid-mutating semantics
    // force on it.
    async fn lopenat(&self, tl: Tlopenat) -> P9Result<Message> {
        let src_fid = self.get_fid(tl.fid)?;

        if tl.newfid != tl.fid && self.session.fids.contains_key(&tl.newfid) {
            return Err(P9Error::FidInUse);
        }

        let inode = self.filesystem.inode_store.get(src_fid.inode_id).await?;
        let qid = inode_to_qid(&inode, src_fid.inode_id);

        self.session.fids.insert(
            tl.newfid,
            Fid {
                path: src_fid.path.clone(),
                inode_id: src_fid.inode_id,
                root: src_fid.root,
                qid: qid.clone(),
                opened: true,
                mode: Some(tl.flags),
                creds: src_fid.creds,
            },
        );

        Ok(Message::Rlopenat(Rlopenat {
            qid,
            iounit: self.iounit(),
        }))
    }

    async fn clunk(&self, tc: Tclunk) -> Message {
        if let Some((_, fid_entry)) = self.session.fids.remove(&tc.fid) {
            self.lock_manager
                .unlock_range(fid_entry.inode_id, tc.fid, 0, 0, self.handler_id)
                .await;
        }
        Message::Rclunk(Rclunk)
    }

    /// At the attach root of an aname-rooted session, rewrite the synthesized
    /// ".." readdir entry to the root itself, mirroring the self-clamp inode 0
    /// gets, so the external parent's inode id and attributes don't leak out
    /// of the subtree.
    async fn clamp_dotdot_at_attach_root(
        &self,
        fid: &Fid,
        entries: &mut [crate::fs::types::DirEntry],
    ) -> P9Result<()> {
        if fid.root == 0 || fid.inode_id != fid.root {
            return Ok(());
        }
        if let Some(entry) = entries.iter_mut().find(|e| e.name == b"..") {
            let inode = self.filesystem.inode_store.get(fid.inode_id).await?;
            entry.fileid = fid.inode_id;
            entry.attr = InodeWithId {
                inode: &inode,
                id: fid.inode_id,
            }
            .into();
        }
        Ok(())
    }

    async fn readdir(&self, tr: Treaddir) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;

        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        // Clamp count to fit response within negotiated msize
        let msize = self.session.msize.load(AtomicOrdering::Relaxed);
        let max_count = msize.saturating_sub(P9_IOHDRSZ);
        let count = tr.count.min(max_count);

        let auth = AuthContext::from(&fid_entry.creds);

        // tr.offset is the cookie from the last entry the client received (0 for first call)
        // Pass it directly to readdir which handles . and .. with cookies 1 and 2
        let mut result = self
            .filesystem
            .readdir(&auth, fid_entry.inode_id, tr.offset, P9_READDIR_BATCH_SIZE)
            .await?;
        self.clamp_dotdot_at_attach_root(&fid_entry, &mut result.entries)
            .await?;

        let mut dir_entries = Vec::new();
        let mut total_size = 0usize;

        for entry in result.entries {
            let dirent = DirEntry {
                qid: attrs_to_qid(&entry.attr, entry.fileid),
                offset: entry.cookie, // Use cookie as offset for client to resume
                type_: filetype_to_dt(entry.attr.file_type),
                name: P9String::new(entry.name),
            };

            let entry_size = dirent.wire_size();

            if total_size + entry_size > count as usize {
                break;
            }

            total_size += entry_size;
            dir_entries.push(dirent);
        }

        Ok(Message::Rreaddir(
            Rreaddir::from_entries(dir_entries).unwrap_or(Rreaddir {
                count: 0,
                data: DekuBytes::default(),
            }),
        ))
    }

    // ZeroFS fast path (readdirplus): like readdir, but each entry carries its
    // full stat. The attributes come straight from the readdir result, so there
    // are no extra fetches; it lets the client populate the kernel's attr cache
    // and skip a getattr/lookup per entry.
    async fn readdir_attr(&self, tr: Treaddirattr) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;
        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        let msize = self.session.msize.load(AtomicOrdering::Relaxed);
        let max_count = msize.saturating_sub(P9_IOHDRSZ);
        let count = tr.count.min(max_count);

        let auth = AuthContext::from(&fid_entry.creds);
        let mut result = self
            .filesystem
            .readdir(&auth, fid_entry.inode_id, tr.offset, P9_READDIR_BATCH_SIZE)
            .await?;
        self.clamp_dotdot_at_attach_root(&fid_entry, &mut result.entries)
            .await?;

        let mut entries = Vec::new();
        let mut total_size = 0usize;
        for entry in result.entries {
            let plus = DirEntryPlus {
                qid: attrs_to_qid(&entry.attr, entry.fileid),
                offset: entry.cookie,
                type_: filetype_to_dt(entry.attr.file_type),
                stat: attrs_to_stat(&entry.attr, entry.fileid),
                name: P9String::new(entry.name),
            };

            let entry_size = plus.wire_size();
            if total_size + entry_size > count as usize {
                break;
            }
            total_size += entry_size;
            entries.push(plus);
        }

        Ok(Message::Rreaddirattr(
            Rreaddirattr::from_entries(entries).unwrap_or(Rreaddirattr {
                count: 0,
                data: DekuBytes::default(),
            }),
        ))
    }

    /// Create a regular file under `parent_fid`, returning the new inode's id
    /// and post-op attributes. Shared by Tlcreate and Tlcreateattr.
    async fn create_child(
        &self,
        parent_fid: &Fid,
        name: &[u8],
        mode: u32,
        client_gid: u32,
    ) -> P9Result<(InodeId, FileAttributes)> {
        let gid = self.effective_gid(client_gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .create(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                name,
                &SetAttributes {
                    mode: SetMode::Set(mode),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn lcreate(&self, tc: Tlcreate) -> P9Result<Message> {
        let parent_fid = self.get_fid(tc.fid)?;

        if parent_fid.opened {
            return Err(P9Error::FidAlreadyOpen);
        }

        let (child_id, post_attr) = self
            .create_child(&parent_fid, &tc.name.data, tc.mode, tc.gid)
            .await?;

        let qid = attrs_to_qid(&post_attr, child_id);

        let mut fid_entry = self.session.fids.get_mut(&tc.fid).ok_or(P9Error::BadFid)?;
        fid_entry.path.push(Bytes::from(tc.name.data));
        fid_entry.inode_id = child_id;
        fid_entry.qid = qid.clone();
        fid_entry.opened = true;
        fid_entry.mode = Some(tc.flags);

        Ok(Message::Rlcreate(Rlcreate {
            qid,
            iounit: self.iounit(),
        }))
    }

    // ZeroFS fast path: Tlcreate + the follow-up walk/getattr in one round trip.
    // Unlike Tlcreate, the directory fid is NOT mutated; the created file is
    // opened on `newfid` and the reply carries its full post-op stat.
    async fn lcreateattr(&self, tc: Tlcreateattr) -> P9Result<Message> {
        let parent_fid = self.get_fid(tc.dfid)?;

        if tc.newfid != tc.dfid && self.session.fids.contains_key(&tc.newfid) {
            return Err(P9Error::FidInUse);
        }

        let (child_id, post_attr) = self
            .create_child(&parent_fid, &tc.name.data, tc.mode, tc.gid)
            .await?;

        let mut path = parent_fid.path.clone();
        path.push(Bytes::from(tc.name.data));
        self.session.fids.insert(
            tc.newfid,
            Fid {
                path,
                inode_id: child_id,
                root: parent_fid.root,
                qid: attrs_to_qid(&post_attr, child_id),
                opened: true,
                mode: Some(tc.flags),
                creds: parent_fid.creds,
            },
        );

        Ok(Message::Rlcreateattr(Rlcreateattr {
            iounit: self.iounit(),
            stat: attrs_to_stat(&post_attr, child_id),
        }))
    }

    async fn read(&self, tr: Tread) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;

        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        // Clamp count to fit response within negotiated msize
        let msize = self.session.msize.load(AtomicOrdering::Relaxed);
        let max_count = msize.saturating_sub(P9_IOHDRSZ);
        let count = tr.count.min(max_count);

        let auth = AuthContext::from(&fid_entry.creds);

        let (data, _eof) = self
            .filesystem
            .read_file(&auth, fid_entry.inode_id, tr.offset, count)
            .await?;

        Ok(Message::Rread(Rread {
            count: data.len() as u32,
            data: DekuBytes::from(data),
        }))
    }

    async fn write(&self, tw: Twrite) -> P9Result<Message> {
        let fid_entry = self.get_fid(tw.fid)?;

        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        debug!(
            "write: fid={}, inode_id={}, uid={}, gid={}, offset={}, data_len={}",
            tw.fid,
            fid_entry.inode_id,
            fid_entry.creds.uid,
            fid_entry.creds.gid,
            tw.offset,
            tw.data.len()
        );

        let auth = AuthContext::from(&fid_entry.creds);
        let data_len = tw.data.len();
        let data = Bytes::from(tw.data);

        self.filesystem
            .write(&auth, fid_entry.inode_id, tw.offset, &data)
            .await
            .inspect_err(|&e| {
                debug!("write: failed with error: {:?}", e);
            })?;

        debug!("write: succeeded");
        Ok(Message::Rwrite(Rwrite {
            count: data_len as u32,
        }))
    }

    async fn getattr(&self, tg: Tgetattr) -> P9Result<Message> {
        let fid_entry = self.get_fid(tg.fid)?;

        let inode = self.filesystem.inode_store.get(fid_entry.inode_id).await?;

        Ok(Message::Rgetattr(Rgetattr {
            valid: tg.request_mask & GETATTR_ALL,
            stat: inode_to_stat(&inode, fid_entry.inode_id),
        }))
    }

    async fn setattr(&self, ts: Tsetattr) -> P9Result<Message> {
        let fid_entry = self.get_fid(ts.fid)?;
        let attr = SetAttributes::from(&ts);

        self.filesystem
            .setattr(&fid_entry.creds, fid_entry.inode_id, &attr)
            .await?;
        Ok(Message::Rsetattr(Rsetattr))
    }

    // ZeroFS fast path: Tsetattr whose reply carries the post-op stat (which the
    // filesystem computes anyway), sparing the client its follow-up Tgetattr.
    async fn setattr_attr(&self, ts: Tsetattr) -> P9Result<Message> {
        let fid_entry = self.get_fid(ts.fid)?;
        let attr = SetAttributes::from(&ts);

        let post_attr = self
            .filesystem
            .setattr(&fid_entry.creds, fid_entry.inode_id, &attr)
            .await?;
        Ok(Message::Rsetattrattr(Rsetattrattr {
            stat: attrs_to_stat(&post_attr, fid_entry.inode_id),
        }))
    }

    /// The body of Tmkdir, returning the new directory's id and post-op
    /// attributes. Shared by Tmkdir and Tmkdirattr.
    async fn make_dir(&self, tm: &Tmkdir) -> P9Result<(InodeId, FileAttributes)> {
        let parent_fid = self.get_fid(tm.dfid)?;

        debug!(
            "mkdir: parent_id={}, name={:?}, dfid={}, mode={:o}, gid={}, fid uid={}, fid gid={}",
            parent_fid.inode_id,
            &tm.name.data,
            tm.dfid,
            tm.mode,
            tm.gid,
            parent_fid.creds.uid,
            parent_fid.creds.gid
        );

        let gid = self.effective_gid(tm.gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .mkdir(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                &tm.name.data,
                &SetAttributes {
                    mode: SetMode::Set(tm.mode),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn mkdir(&self, tm: Tmkdir) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_dir(&tm).await?;
        Ok(Message::Rmkdir(Rmkdir {
            qid: attrs_to_qid(&post_attr, new_id),
        }))
    }

    async fn mkdir_attr(&self, tm: Tmkdir) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_dir(&tm).await?;
        Ok(Message::Rmkdirattr(Rmkdirattr {
            stat: attrs_to_stat(&post_attr, new_id),
        }))
    }

    /// The body of Tsymlink, returning the new link's id and post-op
    /// attributes. Shared by Tsymlink and Tsymlinkattr.
    async fn make_symlink(&self, ts: &Tsymlink) -> P9Result<(InodeId, FileAttributes)> {
        let parent_fid = self.get_fid(ts.dfid)?;

        let gid = self.effective_gid(ts.gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .symlink(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                &ts.name.data,
                &ts.symtgt.data,
                &SetAttributes {
                    mode: SetMode::Set(SYMLINK_DEFAULT_MODE),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn symlink(&self, ts: Tsymlink) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_symlink(&ts).await?;
        Ok(Message::Rsymlink(Rsymlink {
            qid: attrs_to_qid(&post_attr, new_id),
        }))
    }

    async fn symlink_attr(&self, ts: Tsymlink) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_symlink(&ts).await?;
        Ok(Message::Rsymlinkattr(Rsymlinkattr {
            stat: attrs_to_stat(&post_attr, new_id),
        }))
    }

    /// The body of Tmknod, returning the new node's id and post-op attributes.
    /// Shared by Tmknod and Tmknodattr.
    async fn make_node(&self, tm: &Tmknod) -> P9Result<(InodeId, FileAttributes)> {
        let parent_fid = self.get_fid(tm.dfid)?;

        let file_type = tm.mode & 0o170000; // S_IFMT
        let device_type = match file_type {
            S_IFCHR => FileType::CharDevice,
            S_IFBLK => FileType::BlockDevice,
            S_IFIFO => FileType::Fifo,
            S_IFSOCK => FileType::Socket,
            _ => return Err(P9Error::InvalidDeviceType),
        };

        let gid = self.effective_gid(tm.gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .mknod(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                &tm.name.data,
                device_type,
                &SetAttributes {
                    mode: SetMode::Set(tm.mode & 0o7777),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
                match device_type {
                    FileType::CharDevice | FileType::BlockDevice => Some((tm.major, tm.minor)),
                    _ => None,
                },
            )
            .await?)
    }

    async fn mknod(&self, tm: Tmknod) -> P9Result<Message> {
        let (child_id, post_attr) = self.make_node(&tm).await?;
        Ok(Message::Rmknod(Rmknod {
            qid: attrs_to_qid(&post_attr, child_id),
        }))
    }

    async fn mknod_attr(&self, tm: Tmknod) -> P9Result<Message> {
        let (child_id, post_attr) = self.make_node(&tm).await?;
        Ok(Message::Rmknodattr(Rmknodattr {
            stat: attrs_to_stat(&post_attr, child_id),
        }))
    }

    async fn readlink(&self, tr: Treadlink) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;

        let inode = self.filesystem.inode_store.get(fid_entry.inode_id).await?;

        match inode {
            Inode::Symlink(s) => Ok(Message::Rreadlink(Rreadlink {
                target: P9String::new(s.target.clone()),
            })),
            _ => Err(P9Error::NotASymlink),
        }
    }

    /// The body of Tlink, returning the linked inode's id. Shared by Tlink and
    /// Tlinkattr.
    async fn make_link(&self, tl: &Tlink) -> P9Result<InodeId> {
        let dir_fid = self.get_fid(tl.dfid)?;
        let file_fid = self.get_fid(tl.fid)?;

        let dir_id = dir_fid.inode_id;
        let file_id = file_fid.inode_id;
        let creds = dir_fid.creds;
        let name_bytes = &tl.name.data;

        debug!(
            "link: file_id={}, dir_id={}, name={:?}, uid={}, gid={}",
            file_id, dir_id, name_bytes, creds.uid, creds.gid
        );

        let auth = AuthContext::from(&creds);

        self.filesystem
            .link(&auth, file_id, dir_id, name_bytes)
            .await?;

        Ok(file_id)
    }

    async fn link(&self, tl: Tlink) -> P9Result<Message> {
        self.make_link(&tl).await?;
        Ok(Message::Rlink(Rlink))
    }

    async fn link_attr(&self, tl: Tlink) -> P9Result<Message> {
        let file_id = self.make_link(&tl).await?;
        let inode = self.filesystem.inode_store.get(file_id).await?;
        Ok(Message::Rlinkattr(Rlinkattr {
            stat: inode_to_stat(&inode, file_id),
        }))
    }

    async fn rename(&self, tr: Trename) -> P9Result<Message> {
        let source_fid = self.get_fid(tr.fid)?;
        let dest_fid = self.get_fid(tr.dfid)?;

        if source_fid.path.is_empty() {
            return Err(P9Error::InvalidArgument);
        }

        let source_name = source_fid.path.last().unwrap();
        let source_parent_path = source_fid.path[..source_fid.path.len() - 1].to_vec();
        let dest_parent_id = dest_fid.inode_id;
        let creds = source_fid.creds;

        let mut source_parent_id = 0;
        for name in &source_parent_path {
            source_parent_id = self
                .filesystem
                .lookup(&creds, source_parent_id, name)
                .await?;
        }

        let new_name_bytes = Bytes::copy_from_slice(&tr.name.data);

        let auth = AuthContext::from(&creds);

        self.filesystem
            .rename(
                &auth,
                source_parent_id,
                source_name,
                dest_parent_id,
                &new_name_bytes,
            )
            .await?;

        Ok(Message::Rrename(Rrename))
    }

    async fn renameat(&self, tr: Trenameat) -> P9Result<Message> {
        let old_dir_fid = self.get_fid(tr.olddirfid)?;
        let new_dir_fid = self.get_fid(tr.newdirfid)?;

        let auth = AuthContext::from(&old_dir_fid.creds);

        self.filesystem
            .rename(
                &auth,
                old_dir_fid.inode_id,
                &tr.oldname.data,
                new_dir_fid.inode_id,
                &tr.newname.data,
            )
            .await?;

        Ok(Message::Rrenameat(Rrenameat))
    }

    async fn unlinkat(&self, tu: Tunlinkat) -> P9Result<Message> {
        let dir_fid = self.get_fid(tu.dirfid)?;

        let parent_id = dir_fid.inode_id;
        let creds = dir_fid.creds;

        let child_id = self
            .filesystem
            .lookup(&creds, parent_id, &tu.name.data)
            .await?;

        let child_inode = self.filesystem.inode_store.get(child_id).await?;

        let is_dir = matches!(child_inode, Inode::Directory(_));

        // If AT_REMOVEDIR is set, we must be removing a directory
        if (tu.flags & AT_REMOVEDIR) != 0 && !is_dir {
            return Err(P9Error::NotADirectory);
        }

        // If AT_REMOVEDIR is not set, we must not be removing a directory
        if (tu.flags & AT_REMOVEDIR) == 0 && is_dir {
            return Err(P9Error::IsADirectory);
        }

        let auth = AuthContext::from(&creds);

        self.filesystem
            .remove(&auth, parent_id, &tu.name.data)
            .await?;

        Ok(Message::Runlinkat(Runlinkat))
    }

    async fn fsync(&self, tf: Tfsync) -> P9Result<Message> {
        let fid = self.get_fid(tf.fid)?;
        let fid_path = fid.path.clone();

        self.filesystem.flush_coordinator.flush().await?;

        {
            let path = if fid_path.is_empty() {
                "/".to_string()
            } else {
                format!(
                    "/{}",
                    fid_path
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect::<Vec<_>>()
                        .join("/")
                )
            };
            self.filesystem
                .tracer
                .emit_with_path(path, FileOperation::Fsync);
        }

        Ok(Message::Rfsync(Rfsync))
    }

    async fn statfs(&self, ts: Tstatfs) -> P9Result<Message> {
        if !self.session.fids.contains_key(&ts.fid) {
            return Err(P9Error::BadFid);
        }

        let (used_bytes, used_inodes) = self.filesystem.global_stats.get_totals();

        const BLOCK_SIZE: u32 = 4096; // 4KB blocks

        let total_bytes = self.filesystem.max_bytes;

        let total_blocks = total_bytes.div_ceil(BLOCK_SIZE as u64);
        let used_blocks = used_bytes.div_ceil(BLOCK_SIZE as u64);
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        let next_inode_id = self.filesystem.inode_store.next_id();

        let available_inodes = u64::MAX.saturating_sub(next_inode_id);

        let total_inodes = used_inodes + available_inodes;

        let statfs = Rstatfs {
            r#type: 0x5a45524f,
            bsize: BLOCK_SIZE,
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: total_inodes,
            ffree: available_inodes,
            fsid: 0,
            namelen: P9_MAX_NAME_LEN,
        };

        Ok(Message::Rstatfs(statfs))
    }

    async fn lock(&self, tl: Tlock) -> P9Result<Message> {
        let fid = self.get_fid(tl.fid)?;

        if matches!(tl.lock_type, LockType::Unlock) {
            self.lock_manager
                .unlock_range(fid.inode_id, tl.fid, tl.start, tl.length, self.handler_id)
                .await;

            return Ok(Message::Rlock(Rlock {
                status: LockStatus::Success,
            }));
        }

        let new_lock = FileLock {
            lock_type: tl.lock_type,
            start: tl.start,
            length: tl.length,
            proc_id: tl.proc_id,
            client_id: tl.client_id.data.clone(),
            fid: tl.fid,
            inode_id: fid.inode_id,
        };

        if self
            .lock_manager
            .try_add_lock(self.handler_id, new_lock)
            .await
            .is_none()
        {
            debug!("Lock conflict on inode {}", fid.inode_id);
            if (tl.flags & P9_LOCK_FLAGS_BLOCK) != 0 {
                return Ok(Message::Rlock(Rlock {
                    status: LockStatus::Blocked,
                }));
            } else {
                return Err(P9Error::LockConflict);
            }
        }

        Ok(Message::Rlock(Rlock {
            status: LockStatus::Success,
        }))
    }

    async fn getlock(&self, tg: Tgetlock) -> P9Result<Message> {
        let fid = self.get_fid(tg.fid)?;

        let test_lock = FileLock {
            lock_type: tg.lock_type,
            start: tg.start,
            length: tg.length,
            proc_id: tg.proc_id,
            client_id: tg.client_id.data.clone(),
            fid: tg.fid,
            inode_id: fid.inode_id,
        };

        if let Some(conflicting_lock) = self
            .lock_manager
            .check_would_block(fid.inode_id, &test_lock, self.handler_id)
            .await
        {
            Ok(Message::Rgetlock(Rgetlock {
                lock_type: conflicting_lock.lock_type,
                start: conflicting_lock.start,
                length: conflicting_lock.length,
                proc_id: conflicting_lock.proc_id,
                client_id: P9String::new(conflicting_lock.client_id.clone()),
            }))
        } else {
            Ok(Message::Rgetlock(Rgetlock {
                lock_type: LockType::Unlock,
                start: tg.start,
                length: tg.length,
                proc_id: 0,
                client_id: P9String::new(Vec::new()),
            }))
        }
    }
}

pub fn inode_to_qid(inode: &Inode, inode_id: u64) -> Qid {
    let type_ = match inode {
        Inode::Directory(_) => QID_TYPE_DIR,
        Inode::Symlink(_) => QID_TYPE_SYMLINK,
        _ => QID_TYPE_FILE,
    };

    Qid {
        type_,
        version: inode.mtime() as u32,
        path: inode_id,
    }
}

pub fn attrs_to_qid(attrs: &FileAttributes, fileid: u64) -> Qid {
    let type_ = match attrs.file_type {
        FileType::Directory => QID_TYPE_DIR,
        FileType::Symlink => QID_TYPE_SYMLINK,
        _ => QID_TYPE_FILE,
    };

    Qid {
        type_,
        version: attrs.mtime.seconds as u32,
        path: fileid,
    }
}

pub fn filetype_to_dt(ft: FileType) -> u8 {
    match ft {
        FileType::Directory => DT_DIR,
        FileType::Regular => DT_REG,
        FileType::Symlink => DT_LNK,
        FileType::CharDevice => DT_CHR,
        FileType::BlockDevice => DT_BLK,
        FileType::Fifo => DT_FIFO,
        FileType::Socket => DT_SOCK,
    }
}

// Build a wire Stat from the readdir-supplied attributes (already fetched, so
// readdirplus costs no extra round trips). Mirrors `inode_to_stat`.
pub fn attrs_to_stat(attrs: &FileAttributes, fileid: u64) -> Stat {
    let type_bits = match attrs.file_type {
        FileType::Regular => S_IFREG,
        FileType::Directory => S_IFDIR,
        FileType::Symlink => S_IFLNK,
        FileType::CharDevice => S_IFCHR,
        FileType::BlockDevice => S_IFBLK,
        FileType::Fifo => S_IFIFO,
        FileType::Socket => S_IFSOCK,
    };
    let rdev = attrs
        .rdev
        .map_or(0, |(maj, min)| ((maj as u64) << 8) | (min as u64));
    Stat {
        qid: attrs_to_qid(attrs, fileid),
        mode: (attrs.mode & 0o7777) | type_bits,
        uid: attrs.uid,
        gid: attrs.gid,
        nlink: attrs.nlink as u64,
        rdev,
        size: attrs.size,
        blksize: DEFAULT_BLKSIZE,
        blocks: attrs.size.div_ceil(BLOCK_SIZE),
        atime_sec: attrs.atime.seconds,
        atime_nsec: attrs.atime.nanoseconds as u64,
        mtime_sec: attrs.mtime.seconds,
        mtime_nsec: attrs.mtime.nanoseconds as u64,
        ctime_sec: attrs.ctime.seconds,
        ctime_nsec: attrs.ctime.nanoseconds as u64,
        btime_sec: 0,
        btime_nsec: 0,
        r#gen: 0,
        data_version: 0,
    }
}

pub fn inode_to_stat(inode: &Inode, inode_id: u64) -> Stat {
    let (type_bits, size, rdev) = match inode {
        Inode::File(f) => (S_IFREG, f.size, 0),
        Inode::Directory(_) => (S_IFDIR, 0, 0),
        Inode::Symlink(s) => (S_IFLNK, s.target.len() as u64, 0),
        Inode::CharDevice(d) => (
            S_IFCHR,
            0,
            d.rdev
                .map_or(0, |(maj, min)| ((maj as u64) << 8) | (min as u64)),
        ),
        Inode::BlockDevice(d) => (
            S_IFBLK,
            0,
            d.rdev
                .map_or(0, |(maj, min)| ((maj as u64) << 8) | (min as u64)),
        ),
        Inode::Fifo(_) => (S_IFIFO, 0, 0),
        Inode::Socket(_) => (S_IFSOCK, 0, 0),
    };

    Stat {
        qid: inode_to_qid(inode, inode_id),
        mode: inode.mode() | type_bits,
        uid: inode.uid(),
        gid: inode.gid(),
        nlink: inode.nlink() as u64,
        rdev,
        size,
        blksize: DEFAULT_BLKSIZE,
        blocks: size.div_ceil(BLOCK_SIZE),
        atime_sec: inode.atime(),
        atime_nsec: inode.atime_nsec() as u64,
        mtime_sec: inode.mtime(),
        mtime_nsec: inode.mtime_nsec() as u64,
        ctime_sec: inode.ctime(),
        ctime_nsec: inode.ctime_nsec() as u64,
        btime_sec: 0,
        btime_nsec: 0,
        r#gen: 0,
        data_version: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::FileLockManager;
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::SetAttributes;
    use libc::O_RDONLY;
    use std::sync::Arc;

    #[tokio::test]
    async fn version_negotiation_gates_zerofs_extensions() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs, Arc::new(FileLockManager::new()));

        // A stock v9fs client proposes plain 9P2000.L and must get exactly that
        // back — the server must never upgrade to a version it wasn't offered.
        let resp = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            )
            .await;
        match resp.body {
            Message::Rversion(rv) => assert_eq!(rv.version.data, VERSION_9P2000L),
            other => panic!("expected Rversion, got {other:?}"),
        }

        // An older ZeroFS client opts in via the .zerofs suffix and gets exactly
        // that echoed back not the newer .zerofs2 it doesn't understand.
        let resp = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            )
            .await;
        match resp.body {
            Message::Rversion(rv) => assert_eq!(rv.version.data, VERSION_9P2000L_ZEROFS),
            other => panic!("expected Rversion, got {other:?}"),
        }

        // A current client proposes .zerofs2 and gets the full extension set.
        let resp = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L_ZEROFS2.to_vec()),
                }),
            )
            .await;
        match resp.body {
            Message::Rversion(rv) => assert_eq!(rv.version.data, VERSION_9P2000L_ZEROFS2),
            other => panic!("expected Rversion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_statfs() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs.clone(), lock_manager);

        let version_msg = Message::Tversion(Tversion {
            msize: DEFAULT_MSIZE,
            version: P9String::new(VERSION_9P2000L.to_vec()),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(Vec::new()),
            n_uname: 1000,
        });
        let attach_resp = handler.handle_message(1, attach_msg).await;

        match &attach_resp.body {
            Message::Rattach(_) => {}
            _ => panic!("Expected Rattach, got {:?}", attach_resp.body),
        }

        let statfs_msg = Message::Tstatfs(Tstatfs { fid: 1 });
        let statfs_resp = handler.handle_message(2, statfs_msg).await;

        match &statfs_resp.body {
            Message::Rstatfs(rstatfs) => {
                assert_eq!(rstatfs.r#type, 0x5a45524f); // "ZERO" filesystem type
                assert_eq!(rstatfs.bsize, 4096);
                assert!(rstatfs.blocks > 0);
                assert!(rstatfs.bfree > 0);
                assert_eq!(rstatfs.bavail, rstatfs.bfree);
                assert!(rstatfs.files > 0);
                assert!(rstatfs.ffree > 0);
                assert_eq!(rstatfs.namelen, 255);

                // Verify totals match our constants
                const TOTAL_BYTES: u64 = u64::MAX;
                assert_eq!(rstatfs.blocks, TOTAL_BYTES.div_ceil(4096));
                // files = used_inodes + available_inodes, ffree = available_inodes
                // Since no files created yet (only root inode), files should equal ffree + used
                assert!(rstatfs.files > 0);
                assert!(rstatfs.ffree > 0);
            }
            _ => panic!("Expected Rstatfs, got {:?}", statfs_resp.body),
        }

        // Test statfs with invalid fid
        let invalid_statfs_msg = Message::Tstatfs(Tstatfs { fid: 999 });
        let invalid_resp = handler.handle_message(3, invalid_statfs_msg).await;

        match &invalid_resp.body {
            Message::Rlerror(rerror) => {
                assert_eq!(rerror.ecode, libc::EBADF as u32);
            }
            _ => panic!("Expected Rlerror, got {:?}", invalid_resp.body),
        }
    }

    #[tokio::test]
    async fn test_statfs_with_files() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs.clone(), lock_manager);

        // Set up a session
        let version_msg = Message::Tversion(Tversion {
            msize: DEFAULT_MSIZE,
            version: P9String::new(VERSION_9P2000L.to_vec()),
        });
        handler.handle_message(0, version_msg).await;

        // Attach to the filesystem
        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(Vec::new()),
            n_uname: 1000,
        });
        handler.handle_message(1, attach_msg).await;

        // Get initial statfs
        let statfs_msg = Message::Tstatfs(Tstatfs { fid: 1 });
        let initial_resp = handler.handle_message(2, statfs_msg.clone()).await;

        let (initial_free_blocks, _initial_free_inodes) = match &initial_resp.body {
            Message::Rstatfs(rstatfs) => (rstatfs.bfree, rstatfs.ffree),
            _ => panic!("Expected Rstatfs"),
        };

        // Walk to create a new fid for the file we'll create
        let walk_msg = Message::Twalk(Twalk {
            fid: 1,
            newfid: 2,
            nwname: 0,
            wnames: vec![],
        });
        handler.handle_message(3, walk_msg).await;

        // Create a file using the new fid
        let create_msg = Message::Tlcreate(Tlcreate {
            fid: 2,
            name: P9String::new(b"test.txt".to_vec()),
            flags: 0x8002, // O_RDWR | O_CREAT
            mode: 0o644,
            gid: 1000,
        });
        handler.handle_message(4, create_msg).await;

        // Write 10KB of data
        let data = vec![0u8; 10240];
        let write_msg = Message::Twrite(Twrite {
            fid: 2,
            offset: 0,
            count: data.len() as u32,
            data: DekuBytes::from(data),
        });
        handler.handle_message(5, write_msg).await;

        // Get statfs after write (using original fid which still points to root)
        let after_resp = handler.handle_message(6, statfs_msg).await;

        match &after_resp.body {
            Message::Rstatfs(rstatfs) => {
                // Should have fewer available inodes since we allocated one for the file
                // Note: Available inodes are based on next_inode_id, not currently used inodes
                let next_inode_id = handler.filesystem.inode_store.next_id();
                assert_eq!(rstatfs.ffree, u64::MAX - next_inode_id);

                // Should have fewer free blocks (10KB written = 3 blocks of 4KB)
                let expected_blocks_used = 10240_u64.div_ceil(4096); // Round up
                assert_eq!(rstatfs.bfree, initial_free_blocks - expected_blocks_used);
            }
            _ => panic!("Expected Rstatfs"),
        }
    }

    #[tokio::test]
    async fn test_readdir_random_pagination() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        for i in 0..10 {
            fs.create(
                &creds,
                0,
                format!("file{i:02}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);

        let version_msg = Message::Tversion(Tversion {
            msize: 8192,
            version: P9String::new(b"9P2000.L".to_vec()),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(b"/".to_vec()),
            n_uname: 1000,
        });
        handler.handle_message(1, attach_msg).await;

        let open_msg = Message::Tlopen(Tlopen {
            fid: 1,
            flags: O_RDONLY as u32,
        });
        handler.handle_message(200, open_msg).await;

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 0,
            count: 8192,
        });
        let resp = handler.handle_message(201, readdir_msg).await;

        let entries_count = match &resp.body {
            Message::Rreaddir(rreaddir) => {
                let entries = rreaddir.to_entries().unwrap();
                assert!(!entries.is_empty());
                entries.len()
            }
            _ => panic!("Expected Rreaddir"),
        };

        // Should have at least . and .. plus the created files
        assert_eq!(
            entries_count, 12,
            "Expected 12 entries (. .. and 10 files), got {entries_count}"
        );

        // Test reading from random offset (skip first 5 entries)
        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 5,
            count: 8192,
        });
        let resp = handler.handle_message(202, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                // Should have fewer entries when starting from offset 5
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(entries.len(), entries_count - 5);
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_readdir_backwards_seek() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        // Create a few files
        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        for i in 0..5 {
            fs.create(
                &creds,
                0,
                format!("file{i}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);

        // Initialize
        let version_msg = Message::Tversion(Tversion {
            msize: 8192,
            version: P9String::new(b"9P2000.L".to_vec()),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(b"/".to_vec()),
            n_uname: 1000,
        });
        handler.handle_message(1, attach_msg).await;

        // Open directory
        let open_msg = Message::Tlopen(Tlopen {
            fid: 1,
            flags: O_RDONLY as u32,
        });
        handler.handle_message(20, open_msg).await;

        // Read from offset 3
        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 3,
            count: 8192,
        });
        handler.handle_message(21, readdir_msg).await;

        // Now read from offset 1 (backwards seek)
        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 1,
            count: 8192,
        });
        let resp = handler.handle_message(22, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                // Should successfully read from offset 1
                let entries = rreaddir.to_entries().unwrap();
                assert!(!entries.is_empty());

                // Should have 6 entries from offset 1 (skipping only ".")
                assert_eq!(entries.len(), 6, "Expected 6 entries from offset 1");
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_readdir_pagination_duplicates_at_boundary() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };

        for i in 0..1002 {
            fs.create(
                &creds,
                0,
                format!("file_{:06}.txt", i).as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);

        let version_msg = Message::Tversion(Tversion {
            msize: DEFAULT_MSIZE,
            version: P9String::new(VERSION_9P2000L.to_vec()),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(Vec::new()),
            n_uname: 1000,
        });
        handler.handle_message(1, attach_msg).await;

        let open_msg = Message::Tlopen(Tlopen {
            fid: 1,
            flags: O_RDONLY as u32,
        });
        handler.handle_message(2, open_msg).await;

        let mut all_names = Vec::new();
        let mut seen_offsets = std::collections::HashSet::new();
        let mut current_offset = 0u64;
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > 10 {
                panic!("Too many iterations, likely infinite loop");
            }

            println!(
                "Iteration {}: Reading from offset {}",
                iterations, current_offset
            );

            let readdir_msg = Message::Treaddir(Treaddir {
                fid: 1,
                offset: current_offset,
                count: 8192, // Typical buffer size
            });
            let resp = handler
                .handle_message(iterations as u16 + 2, readdir_msg)
                .await;

            match &resp.body {
                Message::Rreaddir(rreaddir) => {
                    let entries = rreaddir.to_entries().unwrap();
                    if entries.is_empty() {
                        println!("Got empty response, ending");
                        break;
                    }

                    // Parse entries
                    let mut batch_count = 0;

                    for entry in &entries {
                        let entry_offset = entry.offset;
                        let name = entry.name.as_str().unwrap_or("").to_string();

                        // Check for duplicate offsets
                        if !seen_offsets.insert(entry_offset) {
                            println!(
                                "WARNING: Duplicate offset {} for entry: {}",
                                entry_offset, name
                            );
                        }

                        if name != "." && name != ".." {
                            all_names.push(name.clone());
                            batch_count += 1;

                            // Debug: print entries near the boundary
                            if (998..=1004).contains(&entry_offset) {
                                println!("  Entry at offset {}: {}", entry_offset, name);
                            }
                        }

                        current_offset = entry_offset;
                    }

                    println!(
                        "Got {} entries in this batch, last offset: {}",
                        batch_count, current_offset
                    );

                    // If we got less than a reasonable amount, we might be at the end
                    if batch_count == 0 {
                        break;
                    }
                }
                _ => panic!("Expected Rreaddir"),
            };
        }

        // Check for duplicates
        let mut name_counts = std::collections::HashMap::new();
        for name in &all_names {
            *name_counts.entry(name.clone()).or_insert(0) += 1;
        }

        let mut duplicates = Vec::new();
        for (name, count) in &name_counts {
            if *count > 1 {
                duplicates.push((name.clone(), *count));
            }
        }

        if !duplicates.is_empty() {
            println!("Found {} duplicate entries:", duplicates.len());
            for (name, count) in &duplicates {
                println!("  {} appears {} times", name, count);
            }
        }

        // We should have exactly 1002 unique files
        assert_eq!(
            duplicates.len(),
            0,
            "Found duplicate entries: {:?}",
            duplicates
        );
        assert_eq!(
            all_names.len(),
            1002,
            "Expected 1002 entries, got {}",
            all_names.len()
        );
    }

    #[tokio::test]
    async fn test_readdir_empty_directory() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        let (_empty_dir_id, _) = fs
            .mkdir(&creds, 0, b"emptydir", &SetAttributes::default())
            .await
            .unwrap();

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);

        let version_msg = Message::Tversion(Tversion {
            msize: 8192,
            version: P9String::new(b"9P2000.L".to_vec()),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(b"/".to_vec()),
            n_uname: 1000,
        });
        handler.handle_message(1, attach_msg).await;

        let walk_msg = Message::Twalk(Twalk {
            fid: 1,
            newfid: 2,
            nwname: 1,
            wnames: vec![P9String::new(b"emptydir".to_vec())],
        });
        handler.handle_message(2, walk_msg).await;

        let open_msg = Message::Tlopen(Tlopen {
            fid: 2,
            flags: O_RDONLY as u32,
        });
        handler.handle_message(3, open_msg).await;

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 2,
            offset: 0,
            count: 8192,
        });
        let resp = handler.handle_message(4, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                // Should have . and .. entries
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(entries.len(), 2, "Expected 2 entries (. and ..)");
            }
            _ => panic!("Expected Rreaddir"),
        };

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 2,
            offset: 2,
            count: 8192,
        });
        let resp = handler.handle_message(5, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(
                    entries.len(),
                    0,
                    "Expected empty response for offset past end"
                );
            }
            _ => panic!("Expected Rreaddir"),
        };

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 2,
            offset: 2,
            count: 8192,
        });
        let resp = handler.handle_message(6, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(
                    entries.len(),
                    0,
                    "Expected empty response for sequential read past end"
                );
            }
            _ => panic!("Expected Rreaddir"),
        };
    }
}
