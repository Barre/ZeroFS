use crate::inode::Inode;
use nfsserve::nfs::nfsstat3;
use nfsserve::vfs::AuthContext;

const S_IRUSR: u32 = 0o400;
const S_IWUSR: u32 = 0o200;
const S_IXUSR: u32 = 0o100;
const S_IRGRP: u32 = 0o040;
const S_IWGRP: u32 = 0o020;
const S_IXGRP: u32 = 0o010;
const S_IROTH: u32 = 0o004;
const S_IWOTH: u32 = 0o002;
const S_IXOTH: u32 = 0o001;
const S_ISVTX: u32 = 0o1000;

#[derive(Debug, Clone, Copy)]
pub struct Credentials {
    pub uid: u32,
    pub gid: u32,
    pub groups: [u32; 16],
    pub groups_count: usize,
}

impl Credentials {
    pub fn from_auth_context(auth: &AuthContext) -> Self {
        let mut creds = Self {
            uid: auth.uid,
            gid: auth.gid,
            groups: [0; 16],
            groups_count: auth.gids.len().min(16),
        };

        for (i, gid) in auth.gids.iter().take(16).enumerate() {
            creds.groups[i] = *gid;
        }

        creds
    }

    pub fn root() -> Self {
        Self {
            uid: 0,
            gid: 0,
            groups: [0; 16],
            groups_count: 0,
        }
    }

    pub fn is_member_of_group(&self, gid: u32) -> bool {
        if self.gid == gid {
            return true;
        }
        for i in 0..self.groups_count {
            if self.groups[i] == gid {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    Read,
    Write,
    Execute,
}

pub fn check_access(inode: &Inode, creds: &Credentials, mode: AccessMode) -> Result<(), nfsstat3> {
    let (uid, gid, file_mode) = match inode {
        Inode::File(f) => (f.uid, f.gid, f.mode),
        Inode::Directory(d) => (d.uid, d.gid, d.mode),
        Inode::Symlink(s) => (s.uid, s.gid, s.mode),
        Inode::Fifo(s) => (s.uid, s.gid, s.mode),
        Inode::Socket(s) => (s.uid, s.gid, s.mode),
        Inode::CharDevice(s) => (s.uid, s.gid, s.mode),
        Inode::BlockDevice(s) => (s.uid, s.gid, s.mode),
    };

    if creds.uid == 0 {
        if let AccessMode::Execute = mode {
            if file_mode & 0o111 == 0 {
                return Err(nfsstat3::NFS3ERR_ACCES);
            }
        }
        return Ok(());
    }

    let permission_bits = match mode {
        AccessMode::Read => (S_IRUSR, S_IRGRP, S_IROTH),
        AccessMode::Write => (S_IWUSR, S_IWGRP, S_IWOTH),
        AccessMode::Execute => (S_IXUSR, S_IXGRP, S_IXOTH),
    };

    if creds.uid == uid {
        if file_mode & permission_bits.0 != 0 {
            return Ok(());
        }
    } else if creds.is_member_of_group(gid) {
        if file_mode & permission_bits.1 != 0 {
            return Ok(());
        }
    } else if file_mode & permission_bits.2 != 0 {
        return Ok(());
    }

    Err(nfsstat3::NFS3ERR_ACCES)
}

pub fn check_ownership(inode: &Inode, creds: &Credentials) -> Result<(), nfsstat3> {
    let uid = match inode {
        Inode::File(f) => f.uid,
        Inode::Directory(d) => d.uid,
        Inode::Symlink(s) => s.uid,
        Inode::Fifo(s) => s.uid,
        Inode::Socket(s) => s.uid,
        Inode::CharDevice(s) => s.uid,
        Inode::BlockDevice(s) => s.uid,
    };

    if creds.uid == 0 || creds.uid == uid {
        Ok(())
    } else {
        Err(nfsstat3::NFS3ERR_PERM)
    }
}

pub fn check_sticky_bit_delete(
    parent: &Inode,
    target: &Inode,
    creds: &Credentials,
) -> Result<(), nfsstat3> {
    if let Inode::Directory(parent_dir) = parent {
        if parent_dir.mode & S_ISVTX != 0 {
            let target_uid = match target {
                Inode::File(f) => f.uid,
                Inode::Directory(d) => d.uid,
                Inode::Symlink(s) => s.uid,
                Inode::Fifo(s) => s.uid,
                Inode::Socket(s) => s.uid,
                Inode::CharDevice(s) => s.uid,
                Inode::BlockDevice(s) => s.uid,
            };

            if creds.uid != 0 && creds.uid != parent_dir.uid && creds.uid != target_uid {
                return Err(nfsstat3::NFS3ERR_PERM);
            }
        }
    }
    Ok(())
}

pub fn validate_mode(mode: u32) -> u32 {
    mode & 0o7777
}

pub fn can_set_times(
    inode: &Inode,
    creds: &Credentials,
    setting_to_current_time: bool,
) -> Result<(), nfsstat3> {
    let uid = match inode {
        Inode::File(f) => f.uid,
        Inode::Directory(d) => d.uid,
        Inode::Symlink(s) => s.uid,
        Inode::Fifo(s) => s.uid,
        Inode::Socket(s) => s.uid,
        Inode::CharDevice(s) => s.uid,
        Inode::BlockDevice(s) => s.uid,
    };

    if creds.uid == 0 || creds.uid == uid {
        return Ok(());
    }

    if setting_to_current_time {
        return check_access(inode, creds, AccessMode::Write);
    }

    Err(nfsstat3::NFS3ERR_PERM)
}
