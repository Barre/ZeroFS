// 9P2000.L uses Linux ABI constants regardless of the client host.
pub(crate) const EPERM: i32 = 1;
pub(crate) const ENOENT: i32 = 2;
pub(crate) const EIO: i32 = 5;
pub(crate) const EBADF: i32 = 9;
pub(crate) const EAGAIN: i32 = 11;
pub(crate) const EACCES: i32 = 13;
pub(crate) const EEXIST: i32 = 17;
pub(crate) const ENOTDIR: i32 = 20;
pub(crate) const EISDIR: i32 = 21;
pub(crate) const EINVAL: i32 = 22;
pub(crate) const ENAMETOOLONG: i32 = 36;
pub(crate) const ENOTEMPTY: i32 = 39;
pub(crate) const ELOOP: i32 = 40;
pub(crate) const ESTALE: i32 = 116;

pub(crate) const O_RDONLY: u32 = 0;
pub(crate) const O_WRONLY: u32 = 1;
pub(crate) const O_RDWR: u32 = 2;
pub(crate) const O_CREAT: u32 = 0o100;
pub(crate) const O_DIRECTORY: u32 = 0o200000;
pub(crate) const AT_REMOVEDIR: u32 = 0x200;

pub(crate) const S_IFMT: u32 = 0o170000;
pub(crate) const S_IFIFO: u32 = 0o010000;
pub(crate) const S_IFCHR: u32 = 0o020000;
pub(crate) const S_IFDIR: u32 = 0o040000;
pub(crate) const S_IFBLK: u32 = 0o060000;
pub(crate) const S_IFREG: u32 = 0o100000;
pub(crate) const S_IFLNK: u32 = 0o120000;
pub(crate) const S_IFSOCK: u32 = 0o140000;

pub(crate) const DT_FIFO: u8 = 1;
pub(crate) const DT_CHR: u8 = 2;
pub(crate) const DT_DIR: u8 = 4;
pub(crate) const DT_BLK: u8 = 6;
pub(crate) const DT_REG: u8 = 8;
pub(crate) const DT_LNK: u8 = 10;
pub(crate) const DT_SOCK: u8 = 12;
