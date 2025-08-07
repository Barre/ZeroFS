use serde::{Deserialize, Serialize};
use zerofs_nfsserve::nfs::{fattr3, ftype3, nfstime3, specdata3};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    Regular,
    Directory,
    Symlink,
    Fifo,
    Socket,
    CharDevice,
    BlockDevice,
}

impl From<FileType> for ftype3 {
    fn from(ft: FileType) -> Self {
        match ft {
            FileType::Regular => ftype3::NF3REG,
            FileType::Directory => ftype3::NF3DIR,
            FileType::Symlink => ftype3::NF3LNK,
            FileType::Fifo => ftype3::NF3FIFO,
            FileType::Socket => ftype3::NF3SOCK,
            FileType::CharDevice => ftype3::NF3CHR,
            FileType::BlockDevice => ftype3::NF3BLK,
        }
    }
}

impl From<ftype3> for FileType {
    fn from(ft: ftype3) -> Self {
        match ft {
            ftype3::NF3REG => FileType::Regular,
            ftype3::NF3DIR => FileType::Directory,
            ftype3::NF3LNK => FileType::Symlink,
            ftype3::NF3FIFO => FileType::Fifo,
            ftype3::NF3SOCK => FileType::Socket,
            ftype3::NF3CHR => FileType::CharDevice,
            ftype3::NF3BLK => FileType::BlockDevice,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    pub seconds: u64,
    pub nanoseconds: u32,
}

impl Timestamp {
    pub fn now() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        Self {
            seconds: now.as_secs(),
            nanoseconds: now.subsec_nanos(),
        }
    }
}

impl From<Timestamp> for nfstime3 {
    fn from(ts: Timestamp) -> Self {
        nfstime3 {
            seconds: ts.seconds as u32,
            nseconds: ts.nanoseconds,
        }
    }
}

impl From<nfstime3> for Timestamp {
    fn from(time: nfstime3) -> Self {
        Timestamp {
            seconds: time.seconds as u64,
            nanoseconds: time.nseconds,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileAttributes {
    pub file_type: FileType,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub used: u64,
    pub rdev: Option<(u32, u32)>,
    pub fsid: u64,
    pub fileid: u64,
    pub atime: Timestamp,
    pub mtime: Timestamp,
    pub ctime: Timestamp,
}

impl From<&FileAttributes> for fattr3 {
    fn from(attrs: &FileAttributes) -> Self {
        fattr3 {
            ftype: attrs.file_type.into(),
            mode: attrs.mode,
            nlink: attrs.nlink,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            used: attrs.used,
            rdev: specdata3 {
                specdata1: attrs.rdev.map(|(major, _)| major).unwrap_or(0),
                specdata2: attrs.rdev.map(|(_, minor)| minor).unwrap_or(0),
            },
            fsid: attrs.fsid,
            fileid: attrs.fileid,
            atime: attrs.atime.into(),
            mtime: attrs.mtime.into(),
            ctime: attrs.ctime.into(),
        }
    }
}

pub struct InodeWithId<'a> {
    pub inode: &'a super::inode::Inode,
    pub id: u64,
}

impl From<InodeWithId<'_>> for fattr3 {
    fn from(inode_with_id: InodeWithId<'_>) -> Self {
        let attrs: FileAttributes = inode_with_id.into();
        (&attrs).into()
    }
}

impl From<InodeWithId<'_>> for FileAttributes {
    fn from(inode_with_id: InodeWithId<'_>) -> Self {
        let inode = inode_with_id.inode;
        let inode_id = inode_with_id.id;
        use super::inode::Inode;

        match inode {
            Inode::File(file) => FileAttributes {
                file_type: FileType::Regular,
                mode: file.mode,
                nlink: file.nlink,
                uid: file.uid,
                gid: file.gid,
                size: file.size,
                used: file.size,
                rdev: None,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: file.atime,
                    nanoseconds: file.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: file.mtime,
                    nanoseconds: file.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: file.ctime,
                    nanoseconds: file.ctime_nsec,
                },
            },
            Inode::Directory(dir) => FileAttributes {
                file_type: FileType::Directory,
                mode: dir.mode,
                nlink: dir.nlink,
                uid: dir.uid,
                gid: dir.gid,
                size: 4096,
                used: 4096,
                rdev: None,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: dir.atime,
                    nanoseconds: dir.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: dir.mtime,
                    nanoseconds: dir.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: dir.ctime,
                    nanoseconds: dir.ctime_nsec,
                },
            },
            Inode::Symlink(sym) => FileAttributes {
                file_type: FileType::Symlink,
                mode: sym.mode,
                nlink: sym.nlink,
                uid: sym.uid,
                gid: sym.gid,
                size: sym.target.len() as u64,
                used: sym.target.len() as u64,
                rdev: None,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: sym.atime,
                    nanoseconds: sym.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: sym.mtime,
                    nanoseconds: sym.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: sym.ctime,
                    nanoseconds: sym.ctime_nsec,
                },
            },
            Inode::Fifo(special) => FileAttributes {
                file_type: FileType::Fifo,
                mode: special.mode,
                nlink: special.nlink,
                uid: special.uid,
                gid: special.gid,
                size: 0,
                used: 0,
                rdev: special.rdev,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: special.atime,
                    nanoseconds: special.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: special.mtime,
                    nanoseconds: special.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: special.ctime,
                    nanoseconds: special.ctime_nsec,
                },
            },
            Inode::Socket(special) => FileAttributes {
                file_type: FileType::Socket,
                mode: special.mode,
                nlink: special.nlink,
                uid: special.uid,
                gid: special.gid,
                size: 0,
                used: 0,
                rdev: special.rdev,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: special.atime,
                    nanoseconds: special.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: special.mtime,
                    nanoseconds: special.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: special.ctime,
                    nanoseconds: special.ctime_nsec,
                },
            },
            Inode::CharDevice(special) => FileAttributes {
                file_type: FileType::CharDevice,
                mode: special.mode,
                nlink: special.nlink,
                uid: special.uid,
                gid: special.gid,
                size: 0,
                used: 0,
                rdev: special.rdev,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: special.atime,
                    nanoseconds: special.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: special.mtime,
                    nanoseconds: special.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: special.ctime,
                    nanoseconds: special.ctime_nsec,
                },
            },
            Inode::BlockDevice(special) => FileAttributes {
                file_type: FileType::BlockDevice,
                mode: special.mode,
                nlink: special.nlink,
                uid: special.uid,
                gid: special.gid,
                size: 0,
                used: 0,
                rdev: special.rdev,
                fsid: 0,
                fileid: inode_id,
                atime: Timestamp {
                    seconds: special.atime,
                    nanoseconds: special.atime_nsec,
                },
                mtime: Timestamp {
                    seconds: special.mtime,
                    nanoseconds: special.mtime_nsec,
                },
                ctime: Timestamp {
                    seconds: special.ctime,
                    nanoseconds: special.ctime_nsec,
                },
            },
        }
    }
}
