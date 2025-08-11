use dashmap::DashMap;
use deku::DekuContainerWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use tracing::debug;

use super::protocol::*;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::Credentials;
use crate::fs::types::{
    FileType, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid, Timestamp,
};
use crate::fs::{EncodedFileId, ZeroFS};
use zerofs_nfsserve::vfs::NFSFileSystem;

pub const DEFAULT_MSIZE: u32 = 1_048_576; // 1MB
pub const DEFAULT_IOUNIT: u32 = 1_048_576; // 1MB

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
    pub path: Vec<String>,
    pub inode_id: InodeId,
    pub qid: Qid,
    pub iounit: u32,
    pub opened: bool,
    pub mode: Option<u32>,
    pub creds: Credentials, // Store credentials per fid/session
    // For directory reads: track position for sequential reads
    pub dir_last_offset: u64, // Last offset we returned entries for
    pub dir_last_cookie: u64, // Last cookie from process_readdir for continuation
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
}

impl NinePHandler {
    pub fn new(filesystem: Arc<ZeroFS>) -> Self {
        let session = Arc::new(Session {
            msize: AtomicU32::new(DEFAULT_MSIZE),
            fids: Arc::new(DashMap::new()),
        });

        Self {
            filesystem,
            session,
        }
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
                    version: P9String::new("unknown"),
                }),
            );
        }

        let msize = tv.msize.min(DEFAULT_MSIZE);
        self.session.msize.store(msize, AtomicOrdering::Relaxed);

        P9Message::new(
            tag,
            Message::Rversion(Rversion {
                msize,
                version: P9String::new(VERSION_9P2000L),
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
                        "Unknown user '{}' with n_uname=-1, using nobody (65534)",
                        username
                    );
                    65534
                }
            }
        } else {
            ta.n_uname
        };

        let mut groups = [0u32; 16];
        groups[0] = uid; // User is always member of their own group
        let creds = Credentials {
            uid,
            gid: uid, // Primary GID defaults to UID
            groups,
            groups_count: 1,
        };

        let root_inode = match self.filesystem.load_inode(0).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        let qid = inode_to_qid(&root_inode, 0);

        // Check if fid is already in use
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
                dir_last_offset: 0,
                dir_last_cookie: 0,
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
            let name = match wname.as_str() {
                Ok(s) => s,
                Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
            };

            current_path.push(name.to_string());

            let inode = match self.filesystem.load_inode(current_id).await {
                Ok(i) => i,
                Err(e) => return P9Message::error(tag, e.to_errno()),
            };

            match inode {
                Inode::Directory(ref _dir) => {
                    let auth = self.make_auth_context(&src_fid.creds);
                    let encoded_current_id = EncodedFileId::from_inode(current_id).into();
                    match self
                        .filesystem
                        .lookup(&auth, encoded_current_id, &name.as_bytes().into())
                        .await
                    {
                        Ok(encoded_id) => {
                            let child_id = EncodedFileId::from(encoded_id).inode_id();
                            let child_inode = match self.filesystem.load_inode(child_id).await {
                                Ok(i) => i,
                                Err(e) => {
                                    return P9Message::error(tag, e.to_errno());
                                }
                            };

                            wqids.push(inode_to_qid(&child_inode, child_id));
                            current_id = child_id;
                        }
                        Err(e) => return P9Message::error(tag, FsError::from(e).to_errno()),
                    }
                }
                _ => return P9Message::error(tag, libc::ENOTDIR as u32),
            }
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
                dir_last_offset: 0,
                dir_last_cookie: 0,
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

        let inode = match self.filesystem.load_inode(inode_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        let qid = inode_to_qid(&inode, inode_id);

        if let Some(mut fid_entry) = self.session.fids.get_mut(&tl.fid) {
            fid_entry.qid = qid.clone();
            fid_entry.opened = true;
            fid_entry.mode = Some(tl.flags);
            if matches!(inode, Inode::Directory(_)) {
                fid_entry.dir_last_offset = 0;
                fid_entry.dir_last_cookie = 0;
            }
        }

        P9Message::new(tag, Message::Rlopen(Rlopen { qid, iounit }))
    }

    async fn handle_clunk(&self, tag: u16, tc: Tclunk) -> P9Message {
        self.session.fids.remove(&tc.fid);
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

        let auth = self.make_auth_context(&fid_entry.creds);

        let is_sequential = tr.offset == fid_entry.dir_last_offset;

        let mut entries_to_return = Vec::new();
        let mut current_offset;

        let parent_id = match self.filesystem.load_inode(fid_entry.inode_id).await {
            Ok(Inode::Directory(dir)) => {
                if fid_entry.inode_id == 0 {
                    0
                } else {
                    dir.parent
                }
            }
            _ => 0,
        };

        // Handle special entries . and .. based on offset
        if tr.offset == 0 {
            // Add both . and ..
            entries_to_return.push((0, ".".to_string(), fid_entry.inode_id));
            entries_to_return.push((1, "..".to_string(), parent_id));
            current_offset = 2;
        } else if tr.offset == 1 {
            // Add only ..
            entries_to_return.push((1, "..".to_string(), parent_id));
            current_offset = 2;
        } else {
            // Start from offset 2 (after special entries)
            current_offset = 2;
        }

        // Now read regular entries if needed
        let mut cookie = if tr.offset == 0 {
            // Always reset cookie when rewinding to beginning
            0
        } else if is_sequential && tr.offset >= 2 && fid_entry.dir_last_cookie != 0 {
            fid_entry.dir_last_cookie
        } else {
            // Always start from 0 to get all entries, we'll filter special ones
            0
        };

        // Read regular entries - continue until we hit the end
        loop {
            const BATCH_SIZE: usize = 1000;

            match self
                .filesystem
                .process_readdir(&(&auth).into(), fid_entry.inode_id, cookie, BATCH_SIZE)
                .await
            {
                Ok(result) => {
                    if result.entries.is_empty() && result.end {
                        break;
                    }

                    for entry in result.entries {
                        let name = String::from_utf8_lossy(&entry.name).to_string();

                        // Skip special entries - we handle them manually
                        if name == "." || name == ".." {
                            cookie = entry.fileid;
                            continue;
                        }

                        if current_offset < tr.offset {
                            current_offset += 1;
                        } else {
                            // We've reached the target offset, start collecting
                            entries_to_return.push((current_offset, name, entry.fileid));
                            current_offset += 1;
                        }

                        cookie = entry.fileid;
                    }

                    // Only break if we have collected entries past the requested offset
                    if result.end || (!entries_to_return.is_empty() && current_offset > tr.offset) {
                        break; // Either end of dir or we have enough entries
                    }
                }
                Err(e) => return P9Message::error(tag, e.to_errno()),
            }
        }

        // Update FID state for next sequential read

        if let Some(mut fid) = self.session.fids.get_mut(&tr.fid) {
            fid.dir_last_offset = current_offset;
            fid.dir_last_cookie = cookie;
        }

        let mut data = Vec::new();

        for (offset, name, _) in &entries_to_return {
            let (child_id, child_inode) = if name == "." {
                let inode = match self.filesystem.load_inode(fid_entry.inode_id).await {
                    Ok(i) => i,
                    Err(_) => continue,
                };
                (fid_entry.inode_id, inode)
            } else if name == ".." {
                let current_inode = match self.filesystem.load_inode(fid_entry.inode_id).await {
                    Ok(i) => i,
                    Err(_) => continue,
                };
                let parent_id = match &current_inode {
                    Inode::Directory(dir) => {
                        if fid_entry.inode_id == 0 {
                            0
                        } else {
                            dir.parent
                        }
                    }
                    _ => unreachable!("readdir called on non-directory"),
                };
                let parent_inode = match self.filesystem.load_inode(parent_id).await {
                    Ok(i) => i,
                    Err(_) => continue,
                };
                (parent_id, parent_inode)
            } else {
                // NFS lookup expects encoded IDs
                let encoded_parent_id = EncodedFileId::from_inode(fid_entry.inode_id).into();
                match self
                    .filesystem
                    .lookup(&auth, encoded_parent_id, &name.as_bytes().into())
                    .await
                {
                    Ok(encoded_id) => {
                        let real_id = EncodedFileId::from(encoded_id).inode_id();
                        let inode = match self.filesystem.load_inode(real_id).await {
                            Ok(i) => i,
                            Err(_) => continue,
                        };
                        (real_id, inode)
                    }
                    Err(_) => continue,
                }
            };

            let dirent = DirEntry {
                qid: inode_to_qid(&child_inode, child_id),
                offset: offset + 1,
                type_: match child_inode {
                    Inode::Directory(_) => DT_DIR,
                    Inode::File(_) => DT_REG,
                    Inode::Symlink(_) => DT_LNK,
                    Inode::CharDevice(_) => DT_CHR,
                    Inode::BlockDevice(_) => DT_BLK,
                    Inode::Fifo(_) => DT_FIFO,
                    Inode::Socket(_) => DT_SOCK,
                },
                name: P9String::new(name),
            };

            let encoded = dirent
                .to_bytes()
                .map_err(|_| libc::EIO as u32)
                .unwrap_or_default();

            if data.len() + encoded.len() > tr.count as usize {
                break;
            }

            data.extend_from_slice(&encoded);
        }

        P9Message::new(
            tag,
            Message::Rreaddir(Rreaddir {
                count: data.len() as u32,
                data,
            }),
        )
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

        let name = match tc.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let mut temp_creds = parent_fid.creds;
        temp_creds.gid = tc.gid;

        let filename = name.to_string();
        match self
            .filesystem
            .process_create(
                &temp_creds,
                parent_fid.inode_id,
                filename.as_bytes(),
                &SetAttributes {
                    mode: SetMode::Set(tc.mode),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(tc.gid),
                    ..Default::default()
                },
            )
            .await
        {
            Ok((child_id, _post_attr)) => {
                let child_inode = match self.filesystem.load_inode(child_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, e.to_errno()),
                };

                let qid = inode_to_qid(&child_inode, child_id);

                let mut fid_entry = self.session.fids.get_mut(&tc.fid).unwrap();
                fid_entry.path.push(name.to_string());
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

        let auth = self.make_auth_context(&fid_entry.creds);

        match self
            .filesystem
            .process_read_file(&(&auth).into(), fid_entry.inode_id, tr.offset, tr.count)
            .await
        {
            Ok((data, _eof)) => P9Message::new(
                tag,
                Message::Rread(Rread {
                    count: data.len() as u32,
                    data,
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

        match self
            .filesystem
            .process_write(&(&auth).into(), fid_entry.inode_id, tw.offset, &tw.data)
            .await
        {
            Ok(_post_attr) => {
                debug!("handle_write: write succeeded");
                P9Message::new(
                    tag,
                    Message::Rwrite(Rwrite {
                        count: tw.data.len() as u32,
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

        match self.filesystem.load_inode(fid_entry.inode_id).await {
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

        match self
            .filesystem
            .process_setattr(&creds, inode_id, &attr)
            .await
        {
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

        let name = match tm.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        debug!(
            "handle_mkdir: parent_id={}, name={}, dfid={}, mode={:o}, gid={}, fid uid={}, fid gid={}",
            parent_id, name, tm.dfid, tm.mode, tm.gid, creds.uid, creds.gid
        );

        let mut temp_creds = creds;
        temp_creds.gid = tm.gid;

        match self
            .filesystem
            .process_mkdir(
                &temp_creds,
                parent_id,
                name.as_bytes(),
                &SetAttributes {
                    mode: SetMode::Set(tm.mode),
                    uid: SetUid::Set(creds.uid),
                    gid: SetGid::Set(tm.gid),
                    ..Default::default()
                },
            )
            .await
        {
            Ok((new_id, _post_attr)) => {
                let new_inode = match self.filesystem.load_inode(new_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, e.to_errno()),
                };

                let qid = inode_to_qid(&new_inode, new_id);
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

        let name = match ts.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let target = match ts.symtgt.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let mut temp_creds = creds;
        temp_creds.gid = ts.gid;

        match self
            .filesystem
            .process_symlink(
                &temp_creds,
                parent_id,
                name.as_bytes(),
                target.as_bytes(),
                &SetAttributes {
                    mode: SetMode::Set(SYMLINK_DEFAULT_MODE),
                    uid: SetUid::Set(creds.uid),
                    gid: SetGid::Set(ts.gid),
                    ..Default::default()
                },
            )
            .await
        {
            Ok((new_id, _post_attr)) => {
                let new_inode = match self.filesystem.load_inode(new_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, e.to_errno()),
                };

                let qid = inode_to_qid(&new_inode, new_id);
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

        let name = match tm.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

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
            .process_mknod(
                &temp_creds,
                parent_fid.inode_id,
                name.as_bytes(),
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
            Ok((child_id, _post_attr)) => {
                let child_inode = match self.filesystem.load_inode(child_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, e.to_errno()),
                };

                P9Message::new(
                    tag,
                    Message::Rmknod(Rmknod {
                        qid: inode_to_qid(&child_inode, child_id),
                    }),
                )
            }
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_readlink(&self, tag: u16, tr: Treadlink) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let inode_id = fid_entry.inode_id;

        let inode = match self.filesystem.load_inode(inode_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, e.to_errno()),
        };

        match inode {
            Inode::Symlink(s) => P9Message::new(
                tag,
                Message::Rreadlink(Rreadlink {
                    target: P9String::new(&String::from_utf8_lossy(&s.target)),
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

        let name = match tl.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        debug!(
            "handle_link: file_id={}, dir_id={}, name={}, uid={}, gid={}",
            file_id, dir_id, name, creds.uid, creds.gid
        );

        // Create hard link
        let auth = self.make_auth_context(&creds);

        match self
            .filesystem
            .process_link(&(&auth).into(), file_id, dir_id, name.as_bytes())
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
        let auth = self.make_auth_context(&creds);
        for name in &source_parent_path {
            let encoded_parent_id = EncodedFileId::from_inode(source_parent_id).into();
            match self
                .filesystem
                .lookup(&auth, encoded_parent_id, &name.as_bytes().into())
                .await
            {
                Ok(encoded_id) => {
                    let real_id = EncodedFileId::from(encoded_id).inode_id();
                    source_parent_id = real_id;
                }
                Err(e) => return P9Message::error(tag, FsError::from(e).to_errno()),
            }
        }

        let new_name = match tr.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let auth = self.make_auth_context(&creds);

        match self
            .filesystem
            .process_rename(
                &(&auth).into(),
                source_parent_id,
                source_name.as_bytes(),
                dest_parent_id,
                new_name.as_bytes(),
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

        let old_parent_id = old_dir_fid.inode_id;
        let new_parent_id = new_dir_fid.inode_id;
        let creds = old_dir_fid.creds;

        let old_name = match tr.oldname.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let new_name = match tr.newname.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let auth = self.make_auth_context(&creds);

        match self
            .filesystem
            .process_rename(
                &(&auth).into(),
                old_parent_id,
                old_name.as_bytes(),
                new_parent_id,
                new_name.as_bytes(),
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

        let name = match tu.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let auth = self.make_auth_context(&creds);
        let encoded_parent_id = EncodedFileId::from_inode(parent_id).into();
        let child_id = match self
            .filesystem
            .lookup(&auth, encoded_parent_id, &name.as_bytes().into())
            .await
        {
            Ok(encoded_id) => EncodedFileId::from(encoded_id).inode_id(),
            Err(e) => return P9Message::error(tag, FsError::from(e).to_errno()),
        };

        let child_inode = match self.filesystem.load_inode(child_id).await {
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
            .process_remove(&(&auth).into(), parent_id, name.as_bytes())
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

        match self.filesystem.flush().await {
            Ok(_) => P9Message::new(tag, Message::Rfsync(Rfsync)),
            Err(e) => P9Message::error(tag, e.to_errno()),
        }
    }

    async fn handle_statfs(&self, tag: u16, ts: Tstatfs) -> P9Message {
        if !self.session.fids.contains_key(&ts.fid) {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        let (used_bytes, used_inodes) = self.filesystem.global_stats.get_totals();

        // Constants matching NFS implementation
        const TOTAL_BYTES: u64 = 8 << 60; // 8 EiB
        const TOTAL_INODES: u64 = 1 << 48; // ~281 trillion inodes
        const BLOCK_SIZE: u32 = 4096; // 4KB blocks

        // Calculate block counts (round up for used blocks)
        let total_blocks = TOTAL_BYTES / BLOCK_SIZE as u64;
        let used_blocks = used_bytes.div_ceil(BLOCK_SIZE as u64);
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        // Get the next inode ID to determine how many more IDs can be allocated
        let next_inode_id = self
            .filesystem
            .next_inode_id
            .load(std::sync::atomic::Ordering::Relaxed);

        // Available inodes = total possible inodes - allocated inode IDs
        // This represents how many more inodes can be created (never increases since IDs aren't reused)
        let available_inodes = TOTAL_INODES.saturating_sub(next_inode_id);

        // Total inodes for df = currently used + available to allocate
        // This will be less than TOTAL_INODES if some allocated IDs have been freed
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
            namelen: 255,
        };

        P9Message::new(tag, Message::Rstatfs(statfs))
    }

    async fn handle_txattrwalk(&self, tag: u16, _tx: Txattrwalk) -> P9Message {
        // We don't support extended attributes
        P9Message::error(tag, libc::ENOTSUP as u32)
    }

    async fn handle_tflush(&self, tag: u16, _tf: Tflush) -> P9Message {
        // We don't support canceling operations
        // Return ENOTSUP error
        P9Message::error(tag, libc::ENOTSUP as u32)
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
    use super::*;
    use crate::fs::ZeroFS;
    use libc::O_RDONLY;
    use std::sync::Arc;
    use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

    #[tokio::test]
    async fn test_statfs() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs.clone());

        let version_msg = Message::Tversion(Tversion {
            msize: DEFAULT_MSIZE,
            version: P9String::new(VERSION_9P2000L),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new("test"),
            aname: P9String::new(""),
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
        let handler = NinePHandler::new(fs.clone());

        // Set up a session
        let version_msg = Message::Tversion(Tversion {
            msize: DEFAULT_MSIZE,
            version: P9String::new(VERSION_9P2000L),
        });
        handler.handle_message(0, version_msg).await;

        // Attach to the filesystem
        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new("test"),
            aname: P9String::new(""),
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
            name: P9String::new("test.txt"),
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
                let next_inode_id = handler
                    .filesystem
                    .next_inode_id
                    .load(std::sync::atomic::Ordering::Relaxed);
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

        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };
        for i in 0..10 {
            fs.create(
                &auth,
                0,
                &format!("file{i:02}.txt").as_bytes().into(),
                SetAttributes::default().into(),
            )
            .await
            .unwrap();
        }

        let handler = NinePHandler::new(fs);

        let version_msg = Message::Tversion(Tversion {
            msize: 8192,
            version: P9String::new("9P2000.L"),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new("test"),
            aname: P9String::new("/"),
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
                assert!(!rreaddir.data.is_empty());
                let mut count = 0;
                let mut offset = 0;
                while offset < rreaddir.data.len() {
                    // Each entry: qid(13) + offset(8) + type(1) + name_len(2) + name
                    if offset + 24 > rreaddir.data.len() {
                        break;
                    }
                    let name_len = u16::from_le_bytes([
                        rreaddir.data[offset + 22],
                        rreaddir.data[offset + 23],
                    ]) as usize;
                    offset += 24 + name_len;
                    count += 1;
                }
                count
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
                let mut count = 0;
                let mut offset = 0;
                while offset < rreaddir.data.len() {
                    if offset + 24 > rreaddir.data.len() {
                        break;
                    }
                    let name_len = u16::from_le_bytes([
                        rreaddir.data[offset + 22],
                        rreaddir.data[offset + 23],
                    ]) as usize;
                    offset += 24 + name_len;
                    count += 1;
                }
                assert_eq!(count, entries_count - 5);
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_readdir_backwards_seek() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        // Create a few files
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };
        for i in 0..5 {
            fs.create(
                &auth,
                0,
                &format!("file{i}.txt").as_bytes().into(),
                SetAttributes::default().into(),
            )
            .await
            .unwrap();
        }

        let handler = NinePHandler::new(fs);

        // Initialize
        let version_msg = Message::Tversion(Tversion {
            msize: 8192,
            version: P9String::new("9P2000.L"),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new("test"),
            aname: P9String::new("/"),
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
                assert!(!rreaddir.data.is_empty());

                // Count entries
                let mut count = 0;
                let mut offset = 0;
                while offset < rreaddir.data.len() {
                    if offset + 24 > rreaddir.data.len() {
                        break;
                    }
                    let name_len = u16::from_le_bytes([
                        rreaddir.data[offset + 22],
                        rreaddir.data[offset + 23],
                    ]) as usize;
                    offset += 24 + name_len;
                    count += 1;
                }
                // Should have 6 entries from offset 1 (skipping only ".")
                assert_eq!(count, 6, "Expected 6 entries from offset 1");
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_readdir_empty_directory() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };
        let (_empty_dir_id, _) = fs
            .mkdir(
                &auth,
                0,
                &b"emptydir".to_vec().into(),
                &SetAttributes::default().into(),
            )
            .await
            .unwrap();

        let handler = NinePHandler::new(fs);

        let version_msg = Message::Tversion(Tversion {
            msize: 8192,
            version: P9String::new("9P2000.L"),
        });
        handler.handle_message(0, version_msg).await;

        let attach_msg = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new("test"),
            aname: P9String::new("/"),
            n_uname: 1000,
        });
        handler.handle_message(1, attach_msg).await;

        let walk_msg = Message::Twalk(Twalk {
            fid: 1,
            newfid: 2,
            nwname: 1,
            wnames: vec![P9String::new("emptydir")],
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
                let mut count = 0;
                let mut offset = 0;
                while offset < rreaddir.data.len() {
                    if offset + 24 > rreaddir.data.len() {
                        break;
                    }
                    let name_len = u16::from_le_bytes([
                        rreaddir.data[offset + 22],
                        rreaddir.data[offset + 23],
                    ]) as usize;
                    offset += 24 + name_len;
                    count += 1;
                }
                assert_eq!(count, 2, "Expected 2 entries (. and ..)");
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
                assert_eq!(
                    rreaddir.data.len(),
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
                assert_eq!(
                    rreaddir.data.len(),
                    0,
                    "Expected empty response for sequential read past end"
                );
            }
            _ => panic!("Expected Rreaddir"),
        };
    }
}
