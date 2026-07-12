// 9P2000.L carries Linux ABI values on every host, so these constants must not
// come from the compilation target's libc (notably, browsers have no libc).
pub(crate) const EIO: i32 = 5;
pub(crate) const ENOENT: u32 = 2;
pub(crate) const EOPNOTSUPP: u32 = 95;
pub(crate) const ESTALE: u32 = 116;
pub(crate) const O_CREAT: u32 = 0o100;
pub(crate) const O_EXCL: u32 = 0o200;
pub(crate) const O_TRUNC: u32 = 0o1000;
