use serde::{Deserialize, Serialize};

pub type InodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInode {
    pub size: u64,
    pub mtime: u64,
    pub mtime_nsec: u32,
    pub ctime: u64,
    pub ctime_nsec: u32,
    pub atime: u64,
    pub atime_nsec: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub parent: InodeId,
    pub nlink: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryInode {
    pub mtime: u64,
    pub mtime_nsec: u32,
    pub ctime: u64,
    pub ctime_nsec: u32,
    pub atime: u64,
    pub atime_nsec: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub entry_count: u64,
    pub parent: InodeId,
    pub nlink: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkInode {
    pub target: Vec<u8>,
    pub mtime: u64,
    pub mtime_nsec: u32,
    pub ctime: u64,
    pub ctime_nsec: u32,
    pub atime: u64,
    pub atime_nsec: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub parent: InodeId,
    pub nlink: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecialInode {
    pub mtime: u64,
    pub mtime_nsec: u32,
    pub ctime: u64,
    pub ctime_nsec: u32,
    pub atime: u64,
    pub atime_nsec: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub parent: InodeId,
    pub nlink: u32,
    pub rdev: Option<(u32, u32)>, // For character and block devices (major, minor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Inode {
    File(FileInode),
    Directory(DirectoryInode),
    Symlink(SymlinkInode),
    Fifo(SpecialInode),
    Socket(SpecialInode),
    CharDevice(SpecialInode),
    BlockDevice(SpecialInode),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::types::InodeWithId;

    use zerofs_nfsserve::nfs::{fattr3, ftype3};

    #[test]
    fn test_file_inode_to_fattr3() {
        let file_inode = FileInode {
            size: 1024,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: 0,
            nlink: 1,
        };

        let inode = Inode::File(file_inode);
        let fattr: fattr3 = InodeWithId {
            inode: &inode,
            id: 42,
        }
        .into();

        assert!(matches!(fattr.ftype, ftype3::NF3REG));
        assert_eq!(fattr.mode, 0o644);
        assert_eq!(fattr.size, 1024);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.fileid, 42);
        assert_eq!(fattr.mtime.seconds, 1234567890);
        assert_eq!(fattr.ctime.seconds, 1234567891);
    }

    #[test]
    fn test_directory_inode_to_fattr3() {
        let dir_inode = DirectoryInode {
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o755,
            uid: 1000,
            gid: 1000,
            entry_count: 2,
            parent: 0,
            nlink: 2,
        };

        let inode = Inode::Directory(dir_inode);
        let fattr: fattr3 = InodeWithId {
            inode: &inode,
            id: 1,
        }
        .into();

        assert!(matches!(fattr.ftype, ftype3::NF3DIR));
        assert_eq!(fattr.mode, 0o755);
        assert_eq!(fattr.size, 4096);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.fileid, 1);
        assert_eq!(fattr.nlink, 2);
    }

    #[test]
    fn test_symlink_inode_to_fattr3() {
        let symlink_inode = SymlinkInode {
            target: b"/path/to/target".to_vec(),
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o777,
            uid: 1000,
            gid: 1000,
            parent: 0,
            nlink: 1,
        };

        let inode = Inode::Symlink(symlink_inode.clone());
        let fattr: fattr3 = InodeWithId {
            inode: &inode,
            id: 99,
        }
        .into();

        assert!(matches!(fattr.ftype, ftype3::NF3LNK));
        assert_eq!(fattr.mode, 0o777);
        assert_eq!(fattr.size, symlink_inode.target.len() as u64);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.fileid, 99);
        assert_eq!(fattr.nlink, 1);
    }

    #[test]
    fn test_inode_serialization() {
        let file_inode = FileInode {
            size: 2048,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: 0,
            nlink: 1,
        };

        let inode = Inode::File(file_inode.clone());

        let serialized = bincode::serialize(&inode).unwrap();
        let deserialized: Inode = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            Inode::File(f) => {
                assert_eq!(f.size, file_inode.size);
                assert_eq!(f.mtime, file_inode.mtime);
                assert_eq!(f.ctime, file_inode.ctime);
                assert_eq!(f.mode, file_inode.mode);
                assert_eq!(f.uid, file_inode.uid);
                assert_eq!(f.gid, file_inode.gid);
            }
            _ => panic!("Expected File inode"),
        }
    }
}
