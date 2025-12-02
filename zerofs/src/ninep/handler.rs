use bytes::Bytes;
use dashmap::DashMap;
use deku::DekuContainerWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use tracing::debug;

use super::lock_manager::{FileLock, FileLockManager};
use super::protocol::*;
use super::protocol::{P9_MAX_GROUPS, P9_MAX_NAME_LEN, P9_NOBODY_UID, P9_READDIR_BATCH_SIZE};
use crate::fs::ZeroFS;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::Credentials;
use crate::fs::types::{
    FileAttributes, FileType, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid, Timestamp,
};

pub const DEFAULT_MSIZE: u32 = 256 * 1024;
pub const DEFAULT_IOUNIT: u32 = 256 * 1024;

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

// Represents an open file handle
#[derive(Debug, Clone)]
pub struct Fid {
    pub path: Vec<bytes::Bytes>,
    pub inode_id: InodeId,
    pub qid: Qid,
    pub iounit: u32,
    pub opened: bool,
    pub mode: Option<u32>,
    pub creds: Credentials, // Store credentials per fid/session
}

#[derive(Debug)]
pub struct Session {
    pub msize: AtomicU32,
    pub fids: Arc<DashMap<u32, Fid>>,
}

#[derive(Clone)]
pub struct NinePHandler {
    filesystem: Arc<ZeroFS>,
    session: Arc<Session>,
    lock_manager: Arc<FileLockManager>,
    handler_id: u64, // Unique ID for this handler/connection
}

impl NinePHandler {
    pub fn new(filesystem: Arc<ZeroFS>, lock_manager: Arc<FileLockManager>) -> Self {
        static HANDLER_COUNTER: AtomicU64 = AtomicU64::new(1);

        let session = Arc::new(Session {
            msize: AtomicU32::new(DEFAULT_MSIZE),
            fids: Arc::new(DashMap::new()),
        });

        Self {
            filesystem,
            session,
            lock_manager,
            handler_id: HANDLER_COUNTER.fetch_add(1, AtomicOrdering::SeqCst),
        }
    }

    pub fn handler_id(&self) -> u64 {
        self.handler_id
    }

    fn make_auth_context(&self, creds: &Credentials) -> zerofs_nfsserve::vfs::AuthContext {
        zerofs_nfsserve::vfs::AuthContext {
            uid: creds.uid,
            gid: creds.gid,
            gids: creds.groups[..creds.groups_count].to_vec(),
        }
    }

    pub async fn handle_message(&self, tag: u16, msg: Message) -> P9Message {
        match msg {
            Message::Tversion(tv) => self.handle_version(tag, tv).await,
            Message::Tattach(ta) => self.handle_attach(tag, ta).await,
            Message::Twalk(tw) => self.handle_walk(tag, tw).await,
            Message::Tlopen(tl) => self.handle_lopen(tag, tl).await,
            Message::Tlcreate(tc) => self.handle_lcreate(tag, tc).await,
            Message::Tread(tr) => self.handle_read(tag, tr).await,
            Message::Twrite(tw) => self.handle_write(tag, tw).await,
            Message::Tclunk(tc) => self.handle_clunk(tag, tc).await,
            Message::Treaddir(tr) => self.handle_readdir(tag, tr).await,
            Message::Tgetattr(tg) => self.handle_getattr(tag, tg).await,
            Message::Tsetattr(ts) => self.handle_setattr(tag, ts).await,
            Message::Tmkdir(tm) => self.handle_mkdir(tag, tm).await,
            Message::Tsymlink(ts) => self.handle_symlink(tag, ts).await,
            Message::Tmknod(tm) => self.handle_mknod(tag, tm).await,
            Message::Treadlink(tr) => self.handle_readlink(tag, tr).await,
            Message::Tlink(tl) => self.handle_link(tag, tl).await,
            Message::Trename(tr) => self.handle_rename(tag, tr).await,
            Message::Trenameat(tr) => self.handle_renameat(tag, tr).await,
            Message::Tunlinkat(tu) => self.handle_unlinkat(tag, tu).await,
            Message::Tfsync(tf) => self.handle_fsync(tag, tf).await,
            Message::Tflush(tf) => self.handle_tflush(tag, tf).await,
            Message::Txattrwalk(tx) => self.handle_txattrwalk(tag, tx).await,
            Message::Tstatfs(ts) => self.handle_statfs(tag, ts).await,
            Message::Tlock(tl) => self.handle_lock(tag, tl).await,
            Message::Tgetlock(tg) => self.handle_getlock(tag, tg).await,
            _ => P9Message::error(tag, libc::ENOSYS as u32),
        }
    }

    async fn handle_version(&self, tag: u16, tv: Tversion) -> P9Message {
        let version_str = match tv.version.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        debug!("Client requested version: {}", version_str);

        if !version_str.contains("9P2000.L") {
            // We only support 9P2000.L
            debug!("Client doesn't support 9P2000.L, returning unknown");
            return P9Message::new(
                tag,
                Message::Rversion(Rversion {
                    msize: tv.msize,
                    version: P9String::new(b"unknown".to_vec()),
                }),
            );
        }

        let msize = tv.msize.min(DEFAULT_MSIZE);
        self.session.msize.store(msize, AtomicOrdering::Relaxed);

        P9Message::new(
            tag,
            Message::Rversion(Rversion {
                msize,
                version: P9String::new(VERSION_9P2000L.to_vec()),
            }),
        )
    }

    async fn handle_attach(&self, tag: u16, ta: Tattach) -> P9Message {
        let username = match ta.uname.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        debug!(
            "handle_attach: fid={}, afid={}, uname={}, aname={:?}, n_uname={}",
            ta.fid,
            ta.afid,
            username,
            ta.aname.as_str().ok(),
            ta.n_uname
        );

        // In 9P2000.L, we trust the client and use UID as GID as a reasonable default
        // Operations that support it can override the GID
        // Special case: n_uname=-1 (0xFFFFFFFF) means "unspecified", use mapping based on uname
        let uid = if ta.n_uname == 0xFFFFFFFF {
            // When n_uname is -1, map based on the string username
            match username {
                "root" => 0,
                _ => {
                    // For other users, we could look them up, but for now just use nobody
                    debug!(
                        "Unknown user '{}' with n_uname=-1, using nobody ({})",
                        username, P9_NOBODY_UID
                    );
                    P9_NOBODY_UID
                }
            }
        } else {
            ta.n_uname
        };

        let mut groups = [0u32; P9_MAX_GROUPS];
        groups[0] = uid; // User is always member of their own group
        let creds = Credentials {
            uid,
            gid: uid, // Primary GID defaults to UID
            groups,
            groups_count: 1,
        };

        let root_inode = match self.filesystem.get_inode_cached(0).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        let qid = inode_to_qid(&root_inode, 0);

        if self.session.fids.contains_key(&ta.fid) {
            return P9Message::error(tag, libc::EINVAL as u32);
        }

        self.session.fids.insert(
            ta.fid,
            Fid {
                path: vec![],
                inode_id: 0,
                qid: qid.clone(),
                iounit: DEFAULT_IOUNIT,
                opened: false,
                mode: None,
                creds,
            },
        );

        P9Message::new(tag, Message::Rattach(Rattach { qid }))
    }

    async fn handle_walk(&self, tag: u16, tw: Twalk) -> P9Message {
        let src_fid = match self.session.fids.get(&tw.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let mut current_path = src_fid.path.clone();
        let mut current_id = src_fid.inode_id;
        let mut wqids = Vec::new();

        for wname in &tw.wnames {
            let name_bytes = Bytes::copy_from_slice(&wname.data);
            current_path.push(name_bytes.clone());

            // lookup() already verifies current_id is a directory and returns ENOTDIR if not
            let creds = src_fid.creds;
            let child_id = match self
                .filesystem
                .lookup(&creds, current_id, &name_bytes)
                .await
            {
                Ok(id) => id,
                Err(e) => return P9Message::error(tag, e.to_errno()),
            };

            let child_inode = match self.filesystem.get_inode_cached(child_id).await {
                Ok(i) => i,
                Err(e) => return P9Message::error(tag, e.to_errno()),
            };

            wqids.push(inode_to_qid(&child_inode, child_id));
            current_id = child_id;
        }

        if tw.newfid != tw.fid || !tw.wnames.is_empty() {
            // Check if newfid is already in use
            if tw.newfid != tw.fid && self.session.fids.contains_key(&tw.newfid) {
                return P9Message::error(tag, libc::EINVAL as u32);
            }

            let new_fid = Fid {
                path: current_path,
                inode_id: current_id,
                qid: wqids.last().cloned().unwrap_or(src_fid.qid),
                iounit: src_fid.iounit,
                opened: false,
                mode: None,
                creds: src_fid.creds, // Inherit credentials from source fid
            };
            self.session.fids.insert(tw.newfid, new_fid);
        }

        P9Message::new(
            tag,
            Message::Rwalk(Rwalk {
                nwqid: wqids.len() as u16,
                wqids,
            }),
        )
    }

    async fn handle_lopen(&self, tag: u16, tl: Tlopen) -> P9Message {
        let fid_entry = match self.session.fids.get(&tl.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if fid_entry.opened {
            return P9Message::error(tag, libc::EBUSY as u32);
        }

        let inode_id = fid_entry.inode_id;
        let creds = fid_entry.creds;
        let iounit = fid_entry.iounit;

        debug!(
            "handle_lopen: fid={}, inode_id={}, uid={}, gid={}, flags={:#x}",
            tl.fid, inode_id, creds.uid, creds.gid, tl.flags
        );

        let inode = match self.filesystem.get_inode_cached(inode_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        let qid = inode_to_qid(&inode, inode_id);

        if let Some(mut fid_entry) = self.session.fids.get_mut(&tl.fid) {
            fid_entry.qid = qid.clone();
            fid_entry.opened = true;
            fid_entry.mode = Some(tl.flags);
        }

        P9Message::new(tag, Message::Rlopen(Rlopen { qid, iounit }))
    }

    async fn handle_clunk(&self, tag: u16, tc: Tclunk) -> P9Message {
        if let Some((_, fid_entry)) = self.session.fids.remove(&tc.fid) {
            self.lock_manager
                .unlock_range(fid_entry.inode_id, tc.fid, 0, 0, self.handler_id)
                .await;
        }
        P9Message::new(tag, Message::Rclunk(Rclunk))
    }

    async fn handle_readdir(&self, tag: u16, tr: Treaddir) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if !fid_entry.opened {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        // Clamp count to fit response within negotiated msize
        let msize = self.session.msize.load(AtomicOrdering::Relaxed);
        let max_count = msize.saturating_sub(P9_IOHDRSZ);
        let count = tr.count.min(max_count);

        let auth = self.make_auth_context(&fid_entry.creds);

        // tr.offset is the cookie from the last entry the client received (0 for first call)
        // Pass it directly to readdir which handles . and .. with cookies 1 and 2
        match self
            .filesystem
            .readdir(
                &(&auth).into(),
                fid_entry.inode_id,
                tr.offset,
                P9_READDIR_BATCH_SIZE,
            )
            .await
        {
            Ok(result) => {
                let mut dir_entries = Vec::new();
                let mut total_size = 0usize;

                for entry in result.entries {
                    let dirent = DirEntry {
                        qid: attrs_to_qid(&entry.attr, entry.fileid),
                        offset: entry.cookie, // Use cookie as offset for client to resume
                        type_: filetype_to_dt(entry.attr.file_type),
                        name: P9String::new(entry.name),
                    };

                    let entry_size = dirent.to_bytes().map(|b| b.len()).unwrap_or(0);

                    if total_size + entry_size > count as usize {
                        break;
                    }

                    total_size += entry_size;
                    dir_entries.push(dirent);
                }

                P9Message::new(
                    tag,
                    Message::Rreaddir(Rreaddir::from_entries(dir_entries).unwrap_or(Rreaddir {
                        count: 0,
                        data: Vec::new(),
                    })),
                )
            }
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_lcreate(&self, tag: u16, tc: Tlcreate) -> P9Message {
        let parent_fid = {
            match self.session.fids.get(&tc.fid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            }
        };

        if parent_fid.opened {
            return P9Message::error(tag, libc::EBUSY as u32);
        }

        let mut temp_creds = parent_fid.creds;
        temp_creds.gid = tc.gid;

        match self
            .filesystem
            .create(
                &temp_creds,
                parent_fid.inode_id,
                &tc.name.data,
                &SetAttributes {
                    mode: SetMode::Set(tc.mode),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(tc.gid),
                    ..Default::default()
                },
            )
            .await
        {
            Ok((child_id, post_attr)) => {
                let qid = attrs_to_qid(&post_attr, child_id);

                let mut fid_entry = match self.session.fids.get_mut(&tc.fid) {
                    Some(entry) => entry,
                    None => return P9Message::error(tag, libc::EBADF as u32),
                };
                fid_entry.path.push(Bytes::from(tc.name.data));
                fid_entry.inode_id = child_id;
                fid_entry.qid = qid.clone();
                fid_entry.opened = true;
                fid_entry.mode = Some(tc.flags);

                P9Message::new(
                    tag,
                    Message::Rlcreate(Rlcreate {
                        qid,
                        iounit: DEFAULT_IOUNIT,
                    }),
                )
            }
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_read(&self, tag: u16, tr: Tread) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if !fid_entry.opened {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        // Clamp count to fit response within negotiated msize
        let msize = self.session.msize.load(AtomicOrdering::Relaxed);
        let max_count = msize.saturating_sub(P9_IOHDRSZ);
        let count = tr.count.min(max_count);

        let auth = self.make_auth_context(&fid_entry.creds);

        match self
            .filesystem
            .read_file(&(&auth).into(), fid_entry.inode_id, tr.offset, count)
            .await
        {
            Ok((data, _eof)) => P9Message::new(
                tag,
                Message::Rread(Rread {
                    count: data.len() as u32,
                    data: data.to_vec(),
                }),
            ),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_write(&self, tag: u16, tw: Twrite) -> P9Message {
        let fid_entry = match self.session.fids.get(&tw.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if !fid_entry.opened {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        let msize = self.session.msize.load(AtomicOrdering::Relaxed);
        if (tw.data.len() as u64) + P9_IOHDRSZ as u64 > msize as u64 {
            debug!(
                "handle_write: rejecting write of {} bytes (exceeds msize {} - IOHDRSZ {})",
                tw.data.len(),
                msize,
                P9_IOHDRSZ
            );
            return P9Message::error(tag, libc::EIO as u32);
        }

        debug!(
            "handle_write: fid={}, inode_id={}, uid={}, gid={}, offset={}, data_len={}",
            tw.fid,
            fid_entry.inode_id,
            fid_entry.creds.uid,
            fid_entry.creds.gid,
            tw.offset,
            tw.data.len()
        );

        let auth = self.make_auth_context(&fid_entry.creds);
        let data_len = tw.data.len();
        let data = Bytes::from(tw.data);

        match self
            .filesystem
            .write(&(&auth).into(), fid_entry.inode_id, tw.offset, &data)
            .await
        {
            Ok(_post_attr) => {
                debug!("handle_write: write succeeded");
                P9Message::new(
                    tag,
                    Message::Rwrite(Rwrite {
                        count: data_len as u32,
                    }),
                )
            }
            Err(e) => {
                debug!("handle_write: write failed with error: {:?}", e);
                P9Message::error(tag, e.to_errno())
            }
        }
    }

    async fn handle_getattr(&self, tag: u16, tg: Tgetattr) -> P9Message {
        let fid_entry = match self.session.fids.get(&tg.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        match self.filesystem.get_inode_cached(fid_entry.inode_id).await {
            Ok(inode) => P9Message::new(
                tag,
                Message::Rgetattr(Rgetattr {
                    valid: tg.request_mask & GETATTR_ALL,
                    stat: inode_to_stat(&inode, fid_entry.inode_id),
                }),
            ),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_setattr(&self, tag: u16, ts: Tsetattr) -> P9Message {
        let (inode_id, creds) = {
            let fid_entry = match self.session.fids.get(&ts.fid) {
                Some(f) => f,
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            (fid_entry.inode_id, fid_entry.creds)
        };

        let attr = SetAttributes {
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
        };

        match self.filesystem.setattr(&creds, inode_id, &attr).await {
            Ok(_post_attr) => P9Message::new(tag, Message::Rsetattr(Rsetattr)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_mkdir(&self, tag: u16, tm: Tmkdir) -> P9Message {
        let parent_fid = match self.session.fids.get(&tm.dfid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let parent_id = parent_fid.inode_id;
        let creds = parent_fid.creds;

        debug!(
            "handle_mkdir: parent_id={}, name={:?}, dfid={}, mode={:o}, gid={}, fid uid={}, fid gid={}",
            parent_id, &tm.name.data, tm.dfid, tm.mode, tm.gid, creds.uid, creds.gid
        );

        let mut temp_creds = creds;
        temp_creds.gid = tm.gid;

        match self
            .filesystem
            .mkdir(
                &temp_creds,
                parent_id,
                &tm.name.data,
                &SetAttributes {
                    mode: SetMode::Set(tm.mode),
                    uid: SetUid::Set(creds.uid),
                    gid: SetGid::Set(tm.gid),
                    ..Default::default()
                },
            )
            .await
        {
            Ok((new_id, post_attr)) => {
                let qid = attrs_to_qid(&post_attr, new_id);
                P9Message::new(tag, Message::Rmkdir(Rmkdir { qid }))
            }
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_symlink(&self, tag: u16, ts: Tsymlink) -> P9Message {
        let parent_fid = match self.session.fids.get(&ts.dfid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let parent_id = parent_fid.inode_id;
        let creds = parent_fid.creds;

        let mut temp_creds = creds;
        temp_creds.gid = ts.gid;

        match self
            .filesystem
            .symlink(
                &temp_creds,
                parent_id,
                &ts.name.data,
                &ts.symtgt.data,
                &SetAttributes {
                    mode: SetMode::Set(SYMLINK_DEFAULT_MODE),
                    uid: SetUid::Set(creds.uid),
                    gid: SetGid::Set(ts.gid),
                    ..Default::default()
                },
            )
            .await
        {
            Ok((new_id, post_attr)) => {
                let qid = attrs_to_qid(&post_attr, new_id);
                P9Message::new(tag, Message::Rsymlink(Rsymlink { qid }))
            }
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_mknod(&self, tag: u16, tm: Tmknod) -> P9Message {
        let parent_fid = match self.session.fids.get(&tm.dfid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let mut temp_creds = parent_fid.creds;
        temp_creds.gid = tm.gid;

        let file_type = tm.mode & 0o170000; // S_IFMT
        let device_type = match file_type {
            S_IFCHR => FileType::CharDevice,
            S_IFBLK => FileType::BlockDevice,
            S_IFIFO => FileType::Fifo,
            S_IFSOCK => FileType::Socket,
            _ => return P9Message::error(tag, libc::EINVAL as u32),
        };

        match self
            .filesystem
            .mknod(
                &temp_creds,
                parent_fid.inode_id,
                &tm.name.data,
                device_type,
                &SetAttributes {
                    mode: SetMode::Set(tm.mode & 0o7777),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(tm.gid),
                    ..Default::default()
                },
                match device_type {
                    FileType::CharDevice | FileType::BlockDevice => Some((tm.major, tm.minor)),
                    _ => None,
                },
            )
            .await
        {
            Ok((child_id, post_attr)) => P9Message::new(
                tag,
                Message::Rmknod(Rmknod {
                    qid: attrs_to_qid(&post_attr, child_id),
                }),
            ),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_readlink(&self, tag: u16, tr: Treadlink) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let inode = match self.filesystem.get_inode_cached(fid_entry.inode_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        match inode {
            Inode::Symlink(s) => P9Message::new(
                tag,
                Message::Rreadlink(Rreadlink {
                    target: P9String::new(s.target.clone()),
                }),
            ),
            _ => P9Message::error(tag, libc::EINVAL as u32),
        }
    }

    async fn handle_link(&self, tag: u16, tl: Tlink) -> P9Message {
        let (dir_fid, file_fid) = {
            let dir_fid = match self.session.fids.get(&tl.dfid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            let file_fid = match self.session.fids.get(&tl.fid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            };

            (dir_fid, file_fid)
        };

        let dir_id = dir_fid.inode_id;
        let file_id = file_fid.inode_id;
        let creds = dir_fid.creds;
        let name_bytes = &tl.name.data;

        debug!(
            "handle_link: file_id={}, dir_id={}, name={:?}, uid={}, gid={}",
            file_id, dir_id, name_bytes, creds.uid, creds.gid
        );

        let auth = self.make_auth_context(&creds);

        match self
            .filesystem
            .link(&(&auth).into(), file_id, dir_id, name_bytes)
            .await
        {
            Ok(_post_attr) => P9Message::new(tag, Message::Rlink(Rlink)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_rename(&self, tag: u16, tr: Trename) -> P9Message {
        let (source_fid, dest_fid) = {
            let source_fid = match self.session.fids.get(&tr.fid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            let dest_fid = match self.session.fids.get(&tr.dfid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            (source_fid, dest_fid)
        };

        if source_fid.path.is_empty() {
            return P9Message::error(tag, libc::EINVAL as u32);
        }

        let source_name = source_fid.path.last().unwrap();
        let source_parent_path = source_fid.path[..source_fid.path.len() - 1].to_vec();
        let dest_parent_id = dest_fid.inode_id;
        let creds = source_fid.creds;

        let mut source_parent_id = 0;
        for name in &source_parent_path {
            match self.filesystem.lookup(&creds, source_parent_id, name).await {
                Ok(real_id) => {
                    source_parent_id = real_id;
                }
                Err(e) => return P9Message::error(tag, e.to_errno()),
            }
        }

        let new_name_bytes = Bytes::copy_from_slice(&tr.name.data);

        let auth = self.make_auth_context(&creds);

        match self
            .filesystem
            .rename(
                &(&auth).into(),
                source_parent_id,
                source_name,
                dest_parent_id,
                &new_name_bytes,
            )
            .await
        {
            Ok(_) => P9Message::new(tag, Message::Rrename(Rrename)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_renameat(&self, tag: u16, tr: Trenameat) -> P9Message {
        let (old_dir_fid, new_dir_fid) = {
            let old_dir_fid = match self.session.fids.get(&tr.olddirfid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            let new_dir_fid = match self.session.fids.get(&tr.newdirfid) {
                Some(f) => f.clone(),
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            (old_dir_fid, new_dir_fid)
        };

        let auth = self.make_auth_context(&old_dir_fid.creds);

        match self
            .filesystem
            .rename(
                &(&auth).into(),
                old_dir_fid.inode_id,
                &tr.oldname.data,
                new_dir_fid.inode_id,
                &tr.newname.data,
            )
            .await
        {
            Ok(_) => P9Message::new(tag, Message::Rrenameat(Rrenameat)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_unlinkat(&self, tag: u16, tu: Tunlinkat) -> P9Message {
        let dir_fid = match self.session.fids.get(&tu.dirfid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let parent_id = dir_fid.inode_id;
        let creds = dir_fid.creds;

        let child_id = match self
            .filesystem
            .lookup(&creds, parent_id, &tu.name.data)
            .await
        {
            Ok(id) => id,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        let child_inode = match self.filesystem.get_inode_cached(child_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        let is_dir = matches!(child_inode, Inode::Directory(_));

        // If AT_REMOVEDIR is set, we must be removing a directory
        if (tu.flags & AT_REMOVEDIR) != 0 && !is_dir {
            return P9Message::error(tag, libc::ENOTDIR as u32);
        }

        // If AT_REMOVEDIR is not set, we must not be removing a directory
        if (tu.flags & AT_REMOVEDIR) == 0 && is_dir {
            return P9Message::error(tag, libc::EISDIR as u32);
        }

        let auth = self.make_auth_context(&creds);

        match self
            .filesystem
            .remove(&(&auth).into(), parent_id, &tu.name.data)
            .await
        {
            Ok(_) => P9Message::new(tag, Message::Runlinkat(Runlinkat)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_fsync(&self, tag: u16, tf: Tfsync) -> P9Message {
        if !self.session.fids.contains_key(&tf.fid) {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        match self.filesystem.flush_coordinator.flush().await {
            Ok(_) => P9Message::new(tag, Message::Rfsync(Rfsync)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_statfs(&self, tag: u16, ts: Tstatfs) -> P9Message {
        if !self.session.fids.contains_key(&ts.fid) {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        let (used_bytes, used_inodes) = self.filesystem.global_stats.get_totals();

        const TOTAL_INODES: u64 = 1 << 48; // ~281 trillion inodes
        const BLOCK_SIZE: u32 = 4096; // 4KB blocks

        let total_bytes = self.filesystem.max_bytes;

        let total_blocks = total_bytes.div_ceil(BLOCK_SIZE as u64);
        let used_blocks = used_bytes.div_ceil(BLOCK_SIZE as u64);
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        let next_inode_id = self.filesystem.inode_store.next_id();

        let available_inodes = TOTAL_INODES.saturating_sub(next_inode_id);

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

        P9Message::new(tag, Message::Rstatfs(statfs))
    }

    async fn handle_txattrwalk(&self, tag: u16, _tx: Txattrwalk) -> P9Message {
        // We don't support extended attributes
        P9Message::error(tag, libc::ENOTSUP as u32)
    }

    async fn handle_tflush(&self, tag: u16, _tf: Tflush) -> P9Message {
        // We don't support canceling operations
        P9Message::error(tag, libc::ENOTSUP as u32)
    }

    async fn handle_lock(&self, tag: u16, tl: Tlock) -> P9Message {
        let fid = match self.session.fids.get(&tl.fid) {
            Some(fid) => fid.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if matches!(tl.lock_type, LockType::Unlock) {
            self.lock_manager
                .unlock_range(fid.inode_id, tl.fid, tl.start, tl.length, self.handler_id)
                .await;

            return P9Message::new(
                tag,
                Message::Rlock(Rlock {
                    status: LockStatus::Success,
                }),
            );
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

        match self
            .lock_manager
            .try_add_lock(self.handler_id, new_lock)
            .await
        {
            Ok(_lock_id) => {
                // Lock acquired successfully
            }
            Err(_) => {
                // Conflict detected
                if (tl.flags & P9_LOCK_FLAGS_BLOCK) != 0 {
                    return P9Message::new(
                        tag,
                        Message::Rlock(Rlock {
                            status: LockStatus::Blocked,
                        }),
                    );
                } else {
                    return P9Message::error(tag, libc::EAGAIN as u32);
                }
            }
        }

        P9Message::new(
            tag,
            Message::Rlock(Rlock {
                status: LockStatus::Success,
            }),
        )
    }

    async fn handle_getlock(&self, tag: u16, tg: Tgetlock) -> P9Message {
        let fid = match self.session.fids.get(&tg.fid) {
            Some(fid) => fid.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

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
            P9Message::new(
                tag,
                Message::Rgetlock(Rgetlock {
                    lock_type: conflicting_lock.lock_type,
                    start: conflicting_lock.start,
                    length: conflicting_lock.length,
                    proc_id: conflicting_lock.proc_id,
                    client_id: P9String::new(conflicting_lock.client_id.clone()),
                }),
            )
        } else {
            P9Message::new(
                tag,
                Message::Rgetlock(Rgetlock {
                    lock_type: LockType::Unlock,
                    start: tg.start,
                    length: tg.length,
                    proc_id: 0,
                    client_id: P9String::new(Vec::new()),
                }),
            )
        }
    }
}

pub fn inode_to_qid(inode: &Inode, inode_id: u64) -> Qid {
    let type_ = match inode {
        Inode::Directory(_) => QID_TYPE_DIR,
        Inode::Symlink(_) => QID_TYPE_SYMLINK,
        _ => QID_TYPE_FILE,
    };

    let mtime_secs = match inode {
        Inode::File(f) => f.mtime,
        Inode::Directory(d) => d.mtime,
        Inode::Symlink(s) => s.mtime,
        Inode::Fifo(s) => s.mtime,
        Inode::Socket(s) => s.mtime,
        Inode::CharDevice(s) => s.mtime,
        Inode::BlockDevice(s) => s.mtime,
    };

    Qid {
        type_,
        version: mtime_secs as u32,
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

pub fn inode_to_stat(inode: &Inode, inode_id: u64) -> Stat {
    let (
        mode,
        uid,
        gid,
        size,
        atime_sec,
        atime_nsec,
        mtime_sec,
        mtime_nsec,
        ctime_sec,
        ctime_nsec,
        nlink,
        rdev,
    ) = match inode {
        Inode::File(f) => (
            f.mode | S_IFREG,
            f.uid,
            f.gid,
            f.size,
            f.atime,
            f.atime_nsec,
            f.mtime,
            f.mtime_nsec,
            f.ctime,
            f.ctime_nsec,
            f.nlink,
            None,
        ),
        Inode::Directory(d) => (
            d.mode | S_IFDIR,
            d.uid,
            d.gid,
            0,
            d.atime,
            d.atime_nsec,
            d.mtime,
            d.mtime_nsec,
            d.ctime,
            d.ctime_nsec,
            d.nlink,
            None,
        ),
        Inode::Symlink(s) => (
            s.mode | S_IFLNK,
            s.uid,
            s.gid,
            s.target.len() as u64,
            s.atime,
            s.atime_nsec,
            s.mtime,
            s.mtime_nsec,
            s.ctime,
            s.ctime_nsec,
            1,
            None,
        ),
        Inode::CharDevice(d) => (
            d.mode | S_IFCHR,
            d.uid,
            d.gid,
            0,
            d.atime,
            d.atime_nsec,
            d.mtime,
            d.mtime_nsec,
            d.ctime,
            d.ctime_nsec,
            d.nlink,
            d.rdev
                .map(|(major, minor)| ((major as u64) << 8) | (minor as u64)),
        ),
        Inode::BlockDevice(d) => (
            d.mode | S_IFBLK,
            d.uid,
            d.gid,
            0,
            d.atime,
            d.atime_nsec,
            d.mtime,
            d.mtime_nsec,
            d.ctime,
            d.ctime_nsec,
            d.nlink,
            d.rdev
                .map(|(major, minor)| ((major as u64) << 8) | (minor as u64)),
        ),
        Inode::Fifo(s) => (
            s.mode | S_IFIFO,
            s.uid,
            s.gid,
            0,
            s.atime,
            s.atime_nsec,
            s.mtime,
            s.mtime_nsec,
            s.ctime,
            s.ctime_nsec,
            s.nlink,
            None,
        ),
        Inode::Socket(s) => (
            s.mode | S_IFSOCK,
            s.uid,
            s.gid,
            0,
            s.atime,
            s.atime_nsec,
            s.mtime,
            s.mtime_nsec,
            s.ctime,
            s.ctime_nsec,
            s.nlink,
            None,
        ),
    };

    Stat {
        qid: inode_to_qid(inode, inode_id),
        mode,
        uid,
        gid,
        nlink: nlink as u64,
        rdev: rdev.unwrap_or(0),
        size,
        blksize: DEFAULT_BLKSIZE,
        blocks: size.div_ceil(BLOCK_SIZE),
        atime_sec,
        atime_nsec: atime_nsec as u64,
        mtime_sec,
        mtime_nsec: mtime_nsec as u64,
        ctime_sec,
        ctime_nsec: ctime_nsec as u64,
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
                const TOTAL_BYTES: u64 = 8 << 60; // 8 EiB
                const TOTAL_INODES: u64 = 1 << 48;
                assert_eq!(rstatfs.blocks * 4096, TOTAL_BYTES);
                // Total files = used + free, which will be <= TOTAL_INODES
                assert!(rstatfs.files <= TOTAL_INODES);
                assert_eq!(rstatfs.files, rstatfs.ffree); // Since no files created yet, all are free
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
            data,
        });
        handler.handle_message(5, write_msg).await;

        // Get statfs after write (using original fid which still points to root)
        let after_resp = handler.handle_message(6, statfs_msg).await;

        match &after_resp.body {
            Message::Rstatfs(rstatfs) => {
                // Should have fewer available inodes since we allocated one for the file
                // Note: Available inodes are based on next_inode_id, not currently used inodes
                const TOTAL_INODES: u64 = 1 << 48;
                let next_inode_id = handler.filesystem.inode_store.next_id();
                assert_eq!(rstatfs.ffree, TOTAL_INODES - next_inode_id);

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
