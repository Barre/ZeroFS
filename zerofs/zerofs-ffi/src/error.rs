//! FFI mirror of [`zerofs_client::ZeroFsError`].
//!
//! The exhaustive conversion requires updates for every upstream variant.

/// The single error type, flat and exhaustive. The variant↔errno mapping is
/// 1:1; any other server errno surfaces as [`ZeroFsError::Io`].
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum ZeroFsError {
    /// No entry at the path (ENOENT).
    #[error("not found: {path}")]
    NotFound {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// Access denied by permission bits (EACCES).
    #[error("permission denied: {path}")]
    PermissionDenied {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// The operation requires ownership or privilege (EPERM).
    #[error("operation not permitted: {path}")]
    NotPermitted {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// The target already exists (EEXIST).
    #[error("already exists: {path}")]
    AlreadyExists {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// A path component is not a directory (ENOTDIR).
    #[error("not a directory: {path}")]
    NotADirectory {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// The target is a directory where a non-directory was required (EISDIR).
    #[error("is a directory: {path}")]
    IsADirectory {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// A directory removal found the directory non-empty (ENOTEMPTY).
    #[error("directory not empty: {path}")]
    DirectoryNotEmpty {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// A name exceeds 255 bytes (ENAMETOOLONG).
    #[error("name too long: {name}")]
    NameTooLong {
        /// The offending name (lossy display).
        name: String,
    },
    /// Bad input, client-side or EINVAL from the server.
    #[error("invalid argument: {message}")]
    InvalidArgument {
        /// What was wrong with the input.
        message: String,
    },
    /// Symlink resolution exceeded the 40-hop cap (ELOOP).
    #[error("too many levels of symbolic links: {path}")]
    TooManySymlinks {
        /// The path whose resolution looped (lossy display).
        path: String,
    },
    /// Handle or client used after `close()` (EBADF).
    #[error("handle is closed")]
    Closed,
    /// The initial connection or attach failed.
    #[error("connection failed: {message}")]
    ConnectFailed {
        /// What failed during connect/attach.
        message: String,
    },
    /// The target is no longer the HA leader (`P9_ENOTLEADER`).
    #[error("not the leader (re-route): {path}")]
    NotLeader {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// Stale handle (`ESTALE`). From sync methods, prior acknowledged writes may
    /// be non-durable and require replacement. Replay loss requires a new client.
    #[error("stale handle (fsync: prior writes may not be durable): {path}")]
    Stale {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// Any other server errno, preserved verbatim.
    #[error("i/o error (errno {errno}): {path}: {message}")]
    Io {
        /// The Linux errno the server returned.
        errno: i32,
        /// The path the operation targeted (lossy display).
        path: String,
        /// Human-readable rendering of the errno.
        message: String,
    },
    /// Wire-level failure: codec error, unexpected reply, failed negotiation.
    #[error("protocol error: {message}")]
    Protocol {
        /// What went wrong on the wire.
        message: String,
    },
}

/// Linux errno for an error. `Io` retains its server-provided errno.
#[uniffi::export]
pub fn error_to_errno(error: &ZeroFsError) -> i32 {
    error.to_errno()
}

impl ZeroFsError {
    /// Linux errno for this error (mirrors the Rust client's `to_errno`).
    pub fn to_errno(&self) -> i32 {
        match self {
            Self::NotFound { .. } => libc::ENOENT,
            Self::PermissionDenied { .. } => libc::EACCES,
            Self::NotPermitted { .. } => libc::EPERM,
            Self::AlreadyExists { .. } => libc::EEXIST,
            Self::NotADirectory { .. } => libc::ENOTDIR,
            Self::IsADirectory { .. } => libc::EISDIR,
            Self::DirectoryNotEmpty { .. } => libc::ENOTEMPTY,
            Self::NameTooLong { .. } => libc::ENAMETOOLONG,
            Self::InvalidArgument { .. } => libc::EINVAL,
            Self::TooManySymlinks { .. } => libc::ELOOP,
            Self::Closed => libc::EBADF,
            Self::ConnectFailed { .. } => libc::EIO,
            Self::NotLeader { .. } => 108,
            Self::Stale { .. } => libc::ESTALE,
            Self::Io { errno, .. } => *errno,
            Self::Protocol { .. } => libc::EIO,
        }
    }
}

impl From<zerofs_client::ZeroFsError> for ZeroFsError {
    fn from(e: zerofs_client::ZeroFsError) -> Self {
        use zerofs_client::ZeroFsError as E;
        match e {
            E::NotFound { path } => Self::NotFound { path },
            E::PermissionDenied { path } => Self::PermissionDenied { path },
            E::NotPermitted { path } => Self::NotPermitted { path },
            E::AlreadyExists { path } => Self::AlreadyExists { path },
            E::NotADirectory { path } => Self::NotADirectory { path },
            E::IsADirectory { path } => Self::IsADirectory { path },
            E::DirectoryNotEmpty { path } => Self::DirectoryNotEmpty { path },
            E::NameTooLong { name } => Self::NameTooLong { name },
            E::InvalidArgument { message } => Self::InvalidArgument { message },
            E::TooManySymlinks { path } => Self::TooManySymlinks { path },
            E::Closed => Self::Closed,
            E::ConnectFailed { message } => Self::ConnectFailed { message },
            E::NotLeader { path } => Self::NotLeader { path },
            E::Stale { path } => Self::Stale { path },
            E::Io {
                errno,
                path,
                message,
            } => Self::Io {
                errno,
                path,
                message,
            },
            E::Protocol { message } => Self::Protocol { message },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zerofs_client::ZeroFsError as C;

    /// Verifies the FFI errno table against the Rust client.
    #[test]
    fn errno_matches_upstream_for_every_variant() {
        let p = || "p".to_string();
        let m = || "m".to_string();
        let cases = [
            C::NotFound { path: p() },
            C::PermissionDenied { path: p() },
            C::NotPermitted { path: p() },
            C::AlreadyExists { path: p() },
            C::NotADirectory { path: p() },
            C::IsADirectory { path: p() },
            C::DirectoryNotEmpty { path: p() },
            C::NameTooLong { name: p() },
            C::InvalidArgument { message: m() },
            C::TooManySymlinks { path: p() },
            C::Closed,
            C::ConnectFailed { message: m() },
            C::NotLeader { path: p() },
            C::Stale { path: p() },
            // A distinct errno checks the `Io` passthrough, not a fixed mapping.
            C::Io {
                errno: libc::EXDEV,
                path: p(),
                message: m(),
            },
            C::Protocol { message: m() },
        ];
        for c in cases {
            let want = c.to_errno();
            let got = error_to_errno(&ZeroFsError::from(c));
            assert_eq!(got, want, "errno parity mismatch (want {want}, got {got})");
        }
    }
}
