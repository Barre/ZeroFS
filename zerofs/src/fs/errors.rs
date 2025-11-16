use thiserror::Error;
use zerofs_nfsserve::nfs::nfsstat3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FsError {
    #[error("Permission denied")]
    PermissionDenied,
    #[error("Operation not permitted")]
    OperationNotPermitted,
    #[error("Not found")]
    NotFound,
    #[error("Already exists")]
    Exists,
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("I/O error")]
    IoError,
    #[error("Directory not empty")]
    NotEmpty,
    #[error("Too many links")]
    TooManyLinks,
    #[error("No space left")]
    NoSpace,
    #[error("Is a directory")]
    IsDirectory,
    #[error("Not a directory")]
    NotDirectory,
    #[error("Name too long")]
    NameTooLong,
    #[error("Not supported")]
    NotSupported,
    #[error("Stale file handle")]
    StaleHandle,
    #[error("Invalid data")]
    InvalidData,
    #[error("Read-only file system")]
    ReadOnlyFilesystem,
}

impl From<bincode::Error> for FsError {
    fn from(_: bincode::Error) -> Self {
        FsError::IoError
    }
}

impl From<FsError> for nfsstat3 {
    fn from(err: FsError) -> Self {
        match err {
            FsError::PermissionDenied => nfsstat3::NFS3ERR_ACCES,
            FsError::OperationNotPermitted => nfsstat3::NFS3ERR_PERM,
            FsError::NotFound => nfsstat3::NFS3ERR_NOENT,
            FsError::Exists => nfsstat3::NFS3ERR_EXIST,
            FsError::InvalidArgument => nfsstat3::NFS3ERR_INVAL,
            FsError::IoError => nfsstat3::NFS3ERR_IO,
            FsError::NotEmpty => nfsstat3::NFS3ERR_NOTEMPTY,
            FsError::TooManyLinks => nfsstat3::NFS3ERR_MLINK,
            FsError::NoSpace => nfsstat3::NFS3ERR_NOSPC,
            FsError::IsDirectory => nfsstat3::NFS3ERR_ISDIR,
            FsError::NotDirectory => nfsstat3::NFS3ERR_NOTDIR,
            FsError::NameTooLong => nfsstat3::NFS3ERR_NAMETOOLONG,
            FsError::NotSupported => nfsstat3::NFS3ERR_NOTSUPP,
            FsError::StaleHandle => nfsstat3::NFS3ERR_STALE,
            FsError::InvalidData => nfsstat3::NFS3ERR_IO,
            FsError::ReadOnlyFilesystem => nfsstat3::NFS3ERR_ROFS,
        }
    }
}

impl From<nfsstat3> for FsError {
    fn from(status: nfsstat3) -> Self {
        match status {
            nfsstat3::NFS3ERR_PERM | nfsstat3::NFS3ERR_ACCES => FsError::PermissionDenied,
            nfsstat3::NFS3ERR_NOENT => FsError::NotFound,
            nfsstat3::NFS3ERR_EXIST => FsError::Exists,
            nfsstat3::NFS3ERR_INVAL | nfsstat3::NFS3ERR_BADTYPE => FsError::InvalidArgument,
            nfsstat3::NFS3ERR_IO => FsError::IoError,
            nfsstat3::NFS3ERR_ROFS => FsError::ReadOnlyFilesystem,
            nfsstat3::NFS3ERR_NOTEMPTY => FsError::NotEmpty,
            nfsstat3::NFS3ERR_MLINK => FsError::TooManyLinks,
            nfsstat3::NFS3ERR_NOSPC | nfsstat3::NFS3ERR_DQUOT => FsError::NoSpace,
            nfsstat3::NFS3ERR_ISDIR => FsError::IsDirectory,
            nfsstat3::NFS3ERR_NOTDIR => FsError::NotDirectory,
            nfsstat3::NFS3ERR_NAMETOOLONG => FsError::NameTooLong,
            nfsstat3::NFS3ERR_NOTSUPP => FsError::NotSupported,
            nfsstat3::NFS3ERR_STALE => FsError::StaleHandle,
            _ => FsError::IoError, // Default for unmapped errors
        }
    }
}

impl FsError {
    pub fn to_errno(self) -> u32 {
        match self {
            FsError::PermissionDenied => libc::EACCES as u32,
            FsError::OperationNotPermitted => libc::EPERM as u32,
            FsError::NotFound => libc::ENOENT as u32,
            FsError::Exists => libc::EEXIST as u32,
            FsError::InvalidArgument => libc::EINVAL as u32,
            FsError::IoError => libc::EIO as u32,
            FsError::NotEmpty => libc::ENOTEMPTY as u32,
            FsError::TooManyLinks => libc::EMLINK as u32,
            FsError::NoSpace => libc::ENOSPC as u32,
            FsError::IsDirectory => libc::EISDIR as u32,
            FsError::NotDirectory => libc::ENOTDIR as u32,
            FsError::NameTooLong => libc::ENAMETOOLONG as u32,
            FsError::NotSupported => libc::ENOSYS as u32,
            FsError::StaleHandle => libc::ESTALE as u32,
            FsError::InvalidData => libc::EIO as u32,
            FsError::ReadOnlyFilesystem => libc::EROFS as u32,
        }
    }
}
