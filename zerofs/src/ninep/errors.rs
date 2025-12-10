use crate::fs::errors::FsError;

#[derive(Debug, Clone, Copy)]
pub enum P9Error {
    BadFid,
    FidNotOpen,
    FidAlreadyOpen,
    FidInUse,
    InvalidEncoding,
    InvalidArgument,
    NotADirectory,
    IsADirectory,
    NotASymlink,
    InvalidDeviceType,
    LockConflict,
    NotSupported,
    NotImplemented,
    Fs(FsError),
}

pub type P9Result<T> = Result<T, P9Error>;

impl P9Error {
    pub fn to_errno(self) -> u32 {
        match self {
            P9Error::BadFid | P9Error::FidNotOpen => libc::EBADF as u32,
            P9Error::FidAlreadyOpen => libc::EBUSY as u32,
            P9Error::FidInUse
            | P9Error::InvalidEncoding
            | P9Error::InvalidArgument
            | P9Error::NotASymlink
            | P9Error::InvalidDeviceType => libc::EINVAL as u32,
            P9Error::NotADirectory => libc::ENOTDIR as u32,
            P9Error::IsADirectory => libc::EISDIR as u32,
            P9Error::LockConflict => libc::EAGAIN as u32,
            P9Error::NotSupported => libc::ENOTSUP as u32,
            P9Error::NotImplemented => libc::ENOSYS as u32,
            P9Error::Fs(e) => e.to_errno(),
        }
    }
}

impl From<FsError> for P9Error {
    fn from(e: FsError) -> Self {
        P9Error::Fs(e)
    }
}
