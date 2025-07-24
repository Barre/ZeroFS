use deku::DekuContainerWrite;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use super::protocol::*;
use crate::filesystem::SlateDbFs;
use crate::inode::{Inode, InodeId};
use crate::permissions::Credentials;
use zerofs_nfsserve::nfs::nfsstat3;
use zerofs_nfsserve::vfs::NFSFileSystem;

pub const DEFAULT_MSIZE: u32 = 65536;
pub const DEFAULT_IOUNIT: u32 = 8192;
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
}

#[derive(Debug, Clone)]
pub struct Session {
    pub msize: u32,
    pub fids: HashMap<u32, Fid>,
}

pub struct NinePHandler {
    filesystem: Arc<SlateDbFs>,
    session: Session,
}

impl NinePHandler {
    pub fn new(filesystem: Arc<SlateDbFs>) -> Self {
        let session = Session {
            msize: DEFAULT_MSIZE,
            fids: HashMap::new(),
        };

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

    pub async fn handle_message(&mut self, tag: u16, msg: Message) -> P9Message {
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
            _ => P9Message::error(tag, libc::ENOSYS as u32),
        }
    }

    async fn handle_version(&mut self, tag: u16, tv: Tversion) -> P9Message {
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

        self.session.msize = tv.msize.min(DEFAULT_MSIZE);

        P9Message::new(
            tag,
            Message::Rversion(Rversion {
                msize: self.session.msize,
                version: P9String::new(VERSION_9P2000L),
            }),
        )
    }

    async fn handle_attach(&mut self, tag: u16, ta: Tattach) -> P9Message {
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
            Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
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
            },
        );

        P9Message::new(tag, Message::Rattach(Rattach { qid }))
    }

    async fn handle_walk(&mut self, tag: u16, tw: Twalk) -> P9Message {
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
                Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
            };

            match inode {
                Inode::Directory(ref _dir) => {
                    let auth = self.make_auth_context(&src_fid.creds);
                    match self
                        .filesystem
                        .lookup(&auth, current_id, &name.as_bytes().into())
                        .await
                    {
                        Ok(child_id) => {
                            let child_inode = match self.filesystem.load_inode(child_id).await {
                                Ok(i) => i,
                                Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
                            };

                            wqids.push(inode_to_qid(&child_inode, child_id));
                            current_id = child_id;
                        }
                        Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
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

    async fn handle_lopen(&mut self, tag: u16, tl: Tlopen) -> P9Message {
        let fid_entry = match self.session.fids.get(&tl.fid) {
            Some(f) => f,
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
            Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
        };

        // Skip permission checking in lopen - let the actual read/write operations handle it
        // This matches how NFS works and ensures consistent behavior

        let qid = inode_to_qid(&inode, inode_id);

        if let Some(fid_entry) = self.session.fids.get_mut(&tl.fid) {
            fid_entry.qid = qid.clone();
            fid_entry.opened = true;
            fid_entry.mode = Some(tl.flags);
        }

        P9Message::new(tag, Message::Rlopen(Rlopen { qid, iounit }))
    }

    async fn handle_clunk(&mut self, tag: u16, tc: Tclunk) -> P9Message {
        self.session.fids.remove(&tc.fid);
        P9Message::new(tag, Message::Rclunk(Rclunk))
    }

    async fn handle_readdir(&self, tag: u16, tr: Treaddir) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if !fid_entry.opened {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        let auth = self.make_auth_context(&fid_entry.creds);

        match self
            .filesystem
            .process_readdir(&auth, fid_entry.inode_id, 0, usize::MAX)
            .await
        {
            Ok(entries) => {
                let mut data = Vec::new();
                let mut current_offset = 0u64;

                for (i, entry) in entries.entries.into_iter().enumerate() {
                    if (i as u64) < tr.offset {
                        current_offset += 1;
                        continue;
                    }

                    let child_inode = match self.filesystem.load_inode(entry.fileid).await {
                        Ok(i) => i,
                        Err(_) => continue,
                    };

                    let dirent = DirEntry {
                        qid: inode_to_qid(&child_inode, entry.fileid),
                        offset: current_offset + 1,
                        type_: match child_inode {
                            Inode::Directory(_) => DT_DIR,
                            Inode::File(_) => DT_REG,
                            Inode::Symlink(_) => DT_LNK,
                            Inode::CharDevice(_) => DT_CHR,
                            Inode::BlockDevice(_) => DT_BLK,
                            Inode::Fifo(_) => DT_FIFO,
                            Inode::Socket(_) => DT_SOCK,
                        },
                        name: P9String::new(&String::from_utf8_lossy(&entry.name)),
                    };

                    let encoded = dirent
                        .to_bytes()
                        .map_err(|_| libc::EIO as u32)
                        .unwrap_or_default();

                    if data.len() + encoded.len() > tr.count as usize {
                        break;
                    }

                    data.extend_from_slice(&encoded);
                    current_offset += 1;
                }

                P9Message::new(
                    tag,
                    Message::Rreaddir(Rreaddir {
                        count: data.len() as u32,
                        data,
                    }),
                )
            }
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_lcreate(&mut self, tag: u16, tc: Tlcreate) -> P9Message {
        let parent_fid = match self.session.fids.get(&tc.fid) {
            Some(f) => f.clone(),
            None => return P9Message::error(tag, libc::EBADF as u32),
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
        let auth = self.make_auth_context(&temp_creds);

        let filename = name.to_string();
        match self
            .filesystem
            .process_create(
                &auth,
                parent_fid.inode_id,
                filename.as_bytes(),
                zerofs_nfsserve::nfs::sattr3 {
                    mode: zerofs_nfsserve::nfs::set_mode3::mode(tc.mode),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(parent_fid.creds.uid),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(tc.gid),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                },
            )
            .await
        {
            Ok((child_id, _post_attr)) => {
                let child_inode = match self.filesystem.load_inode(child_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
                };

                let qid = inode_to_qid(&child_inode, child_id);

                let fid_entry = self.session.fids.get_mut(&tc.fid).unwrap();
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
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_read(&self, tag: u16, tr: Tread) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        if !fid_entry.opened {
            return P9Message::error(tag, libc::EBADF as u32);
        }

        let auth = self.make_auth_context(&fid_entry.creds);

        match self
            .filesystem
            .process_read_file(&auth, fid_entry.inode_id, tr.offset, tr.count)
            .await
        {
            Ok((data, _eof)) => P9Message::new(
                tag,
                Message::Rread(Rread {
                    count: data.len() as u32,
                    data,
                }),
            ),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_write(&self, tag: u16, tw: Twrite) -> P9Message {
        let fid_entry = match self.session.fids.get(&tw.fid) {
            Some(f) => f,
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
            .process_write(&auth, fid_entry.inode_id, tw.offset, &tw.data)
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
                P9Message::error(tag, errno_from_nfsstat(e))
            }
        }
    }

    async fn handle_getattr(&self, tag: u16, tg: Tgetattr) -> P9Message {
        let fid_entry = match self.session.fids.get(&tg.fid) {
            Some(f) => f,
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
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_setattr(&mut self, tag: u16, ts: Tsetattr) -> P9Message {
        let (inode_id, creds) = {
            let fid_entry = match self.session.fids.get(&ts.fid) {
                Some(f) => f,
                None => return P9Message::error(tag, libc::EBADF as u32),
            };
            (fid_entry.inode_id, fid_entry.creds)
        };

        let auth = self.make_auth_context(&creds);

        let attr = zerofs_nfsserve::nfs::sattr3 {
            mode: if ts.valid & SETATTR_MODE != 0 {
                zerofs_nfsserve::nfs::set_mode3::mode(ts.mode)
            } else {
                zerofs_nfsserve::nfs::set_mode3::Void
            },
            uid: if ts.valid & SETATTR_UID != 0 {
                zerofs_nfsserve::nfs::set_uid3::uid(ts.uid)
            } else {
                zerofs_nfsserve::nfs::set_uid3::Void
            },
            gid: if ts.valid & SETATTR_GID != 0 {
                zerofs_nfsserve::nfs::set_gid3::gid(ts.gid)
            } else {
                zerofs_nfsserve::nfs::set_gid3::Void
            },
            size: if ts.valid & SETATTR_SIZE != 0 {
                zerofs_nfsserve::nfs::set_size3::size(ts.size)
            } else {
                zerofs_nfsserve::nfs::set_size3::Void
            },
            atime: if ts.valid & SETATTR_ATIME_SET != 0 {
                zerofs_nfsserve::nfs::set_atime::SET_TO_CLIENT_TIME(
                    zerofs_nfsserve::nfs::nfstime3 {
                        seconds: ts.atime_sec as u32,
                        nseconds: ts.atime_nsec as u32,
                    },
                )
            } else if ts.valid & SETATTR_ATIME != 0 {
                zerofs_nfsserve::nfs::set_atime::SET_TO_SERVER_TIME
            } else {
                zerofs_nfsserve::nfs::set_atime::DONT_CHANGE
            },
            mtime: if ts.valid & SETATTR_MTIME_SET != 0 {
                zerofs_nfsserve::nfs::set_mtime::SET_TO_CLIENT_TIME(
                    zerofs_nfsserve::nfs::nfstime3 {
                        seconds: ts.mtime_sec as u32,
                        nseconds: ts.mtime_nsec as u32,
                    },
                )
            } else if ts.valid & SETATTR_MTIME != 0 {
                zerofs_nfsserve::nfs::set_mtime::SET_TO_SERVER_TIME
            } else {
                zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE
            },
        };

        match self.filesystem.process_setattr(&auth, inode_id, attr).await {
            Ok(_post_attr) => P9Message::new(tag, Message::Rsetattr(Rsetattr)),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_mkdir(&mut self, tag: u16, tm: Tmkdir) -> P9Message {
        let parent_fid = match self.session.fids.get(&tm.dfid) {
            Some(f) => f,
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
        let auth = self.make_auth_context(&temp_creds);

        match self
            .filesystem
            .process_mkdir(
                &auth,
                parent_id,
                name.as_bytes(),
                &zerofs_nfsserve::nfs::sattr3 {
                    mode: zerofs_nfsserve::nfs::set_mode3::mode(tm.mode),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(creds.uid),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(tm.gid),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                },
            )
            .await
        {
            Ok((new_id, _post_attr)) => {
                let new_inode = match self.filesystem.load_inode(new_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
                };

                let qid = inode_to_qid(&new_inode, new_id);
                P9Message::new(tag, Message::Rmkdir(Rmkdir { qid }))
            }
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_symlink(&mut self, tag: u16, ts: Tsymlink) -> P9Message {
        let parent_fid = match self.session.fids.get(&ts.dfid) {
            Some(f) => f,
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
        let auth = self.make_auth_context(&temp_creds);

        match self
            .filesystem
            .process_symlink(
                &auth,
                parent_id,
                name.as_bytes(),
                target.as_bytes(),
                zerofs_nfsserve::nfs::sattr3 {
                    mode: zerofs_nfsserve::nfs::set_mode3::mode(SYMLINK_DEFAULT_MODE),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(creds.uid),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(ts.gid),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                },
            )
            .await
        {
            Ok((new_id, _post_attr)) => {
                let new_inode = match self.filesystem.load_inode(new_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
                };

                let qid = inode_to_qid(&new_inode, new_id);
                P9Message::new(tag, Message::Rsymlink(Rsymlink { qid }))
            }
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_mknod(&mut self, tag: u16, tm: Tmknod) -> P9Message {
        let parent_fid = match self.session.fids.get(&tm.dfid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let mut temp_creds = parent_fid.creds;
        temp_creds.gid = tm.gid;
        let auth = self.make_auth_context(&temp_creds);

        let name = match tm.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let file_type = tm.mode & 0o170000; // S_IFMT
        let device_type = match file_type {
            S_IFCHR => zerofs_nfsserve::nfs::ftype3::NF3CHR,
            S_IFBLK => zerofs_nfsserve::nfs::ftype3::NF3BLK,
            S_IFIFO => zerofs_nfsserve::nfs::ftype3::NF3FIFO,
            S_IFSOCK => zerofs_nfsserve::nfs::ftype3::NF3SOCK,
            _ => return P9Message::error(tag, libc::EINVAL as u32),
        };

        match self
            .filesystem
            .process_mknod(
                &auth,
                parent_fid.inode_id,
                name.as_bytes(),
                device_type,
                &zerofs_nfsserve::nfs::sattr3 {
                    mode: zerofs_nfsserve::nfs::set_mode3::mode(tm.mode & 0o7777),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(parent_fid.creds.uid),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(tm.gid),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                },
                match device_type {
                    zerofs_nfsserve::nfs::ftype3::NF3CHR | zerofs_nfsserve::nfs::ftype3::NF3BLK => {
                        Some((tm.major, tm.minor))
                    }
                    _ => None,
                },
            )
            .await
        {
            Ok((child_id, _post_attr)) => {
                let child_inode = match self.filesystem.load_inode(child_id).await {
                    Ok(i) => i,
                    Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
                };

                P9Message::new(
                    tag,
                    Message::Rmknod(Rmknod {
                        qid: inode_to_qid(&child_inode, child_id),
                    }),
                )
            }
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_readlink(&self, tag: u16, tr: Treadlink) -> P9Message {
        let fid_entry = match self.session.fids.get(&tr.fid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let inode_id = fid_entry.inode_id;

        let inode = match self.filesystem.load_inode(inode_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
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
        let dir_fid = match self.session.fids.get(&tl.dfid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let file_fid = match self.session.fids.get(&tl.fid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
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
            .process_link(&auth, file_id, dir_id, name.as_bytes())
            .await
        {
            Ok(_post_attr) => P9Message::new(tag, Message::Rlink(Rlink)),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_rename(&self, tag: u16, tr: Trename) -> P9Message {
        let source_fid = match self.session.fids.get(&tr.fid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let dest_fid = match self.session.fids.get(&tr.dfid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
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
            match self
                .filesystem
                .lookup(&auth, source_parent_id, &name.as_bytes().into())
                .await
            {
                Ok(id) => source_parent_id = id,
                Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
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
                &auth,
                source_parent_id,
                source_name.as_bytes(),
                dest_parent_id,
                new_name.as_bytes(),
            )
            .await
        {
            Ok(_) => P9Message::new(tag, Message::Rrename(Rrename)),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_renameat(&self, tag: u16, tr: Trenameat) -> P9Message {
        let old_dir_fid = match self.session.fids.get(&tr.olddirfid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let new_dir_fid = match self.session.fids.get(&tr.newdirfid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
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
                &auth,
                old_parent_id,
                old_name.as_bytes(),
                new_parent_id,
                new_name.as_bytes(),
            )
            .await
        {
            Ok(_) => P9Message::new(tag, Message::Rrenameat(Rrenameat)),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_unlinkat(&self, tag: u16, tu: Tunlinkat) -> P9Message {
        let dir_fid = match self.session.fids.get(&tu.dirfid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        let parent_id = dir_fid.inode_id;
        let creds = dir_fid.creds;

        let name = match tu.name.as_str() {
            Ok(s) => s,
            Err(_) => return P9Message::error(tag, libc::EINVAL as u32),
        };

        let auth = self.make_auth_context(&creds);
        let child_id = match self
            .filesystem
            .lookup(&auth, parent_id, &name.as_bytes().into())
            .await
        {
            Ok(id) => id,
            Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
        };

        let child_inode = match self.filesystem.load_inode(child_id).await {
            Ok(i) => i,
            Err(e) => return P9Message::error(tag, errno_from_nfsstat(e)),
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
            .process_remove(&auth, parent_id, name.as_bytes())
            .await
        {
            Ok(_) => P9Message::new(tag, Message::Runlinkat(Runlinkat)),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
    }

    async fn handle_fsync(&self, tag: u16, tf: Tfsync) -> P9Message {
        let _fid_entry = match self.session.fids.get(&tf.fid) {
            Some(f) => f,
            None => return P9Message::error(tag, libc::EBADF as u32),
        };

        match self.filesystem.flush().await {
            Ok(_) => P9Message::new(tag, Message::Rfsync(Rfsync)),
            Err(e) => P9Message::error(tag, errno_from_nfsstat(e)),
        }
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

pub fn errno_from_nfsstat(e: nfsstat3) -> u32 {
    match e {
        nfsstat3::NFS3_OK => 0,
        nfsstat3::NFS3ERR_PERM => libc::EPERM as u32,
        nfsstat3::NFS3ERR_NOENT => libc::ENOENT as u32,
        nfsstat3::NFS3ERR_IO => libc::EIO as u32,
        nfsstat3::NFS3ERR_NXIO => libc::ENXIO as u32,
        nfsstat3::NFS3ERR_ACCES => libc::EACCES as u32,
        nfsstat3::NFS3ERR_EXIST => libc::EEXIST as u32,
        nfsstat3::NFS3ERR_XDEV => libc::EXDEV as u32,
        nfsstat3::NFS3ERR_NODEV => libc::ENODEV as u32,
        nfsstat3::NFS3ERR_NOTDIR => libc::ENOTDIR as u32,
        nfsstat3::NFS3ERR_ISDIR => libc::EISDIR as u32,
        nfsstat3::NFS3ERR_INVAL => libc::EINVAL as u32,
        nfsstat3::NFS3ERR_FBIG => libc::EFBIG as u32,
        nfsstat3::NFS3ERR_NOSPC => libc::ENOSPC as u32,
        nfsstat3::NFS3ERR_ROFS => libc::EROFS as u32,
        nfsstat3::NFS3ERR_MLINK => libc::EMLINK as u32,
        nfsstat3::NFS3ERR_NAMETOOLONG => libc::ENAMETOOLONG as u32,
        nfsstat3::NFS3ERR_NOTEMPTY => libc::ENOTEMPTY as u32,
        nfsstat3::NFS3ERR_DQUOT => libc::EDQUOT as u32,
        nfsstat3::NFS3ERR_STALE => libc::ESTALE as u32,
        nfsstat3::NFS3ERR_REMOTE => libc::EREMOTE as u32,
        nfsstat3::NFS3ERR_BADHANDLE => libc::EBADF as u32,
        nfsstat3::NFS3ERR_NOT_SYNC => libc::EIO as u32,
        nfsstat3::NFS3ERR_BAD_COOKIE => libc::EINVAL as u32,
        nfsstat3::NFS3ERR_NOTSUPP => libc::ENOSYS as u32,
        nfsstat3::NFS3ERR_TOOSMALL => libc::EINVAL as u32,
        nfsstat3::NFS3ERR_SERVERFAULT => libc::EIO as u32,
        nfsstat3::NFS3ERR_BADTYPE => libc::EINVAL as u32,
        nfsstat3::NFS3ERR_JUKEBOX => libc::EAGAIN as u32,
    }
}
