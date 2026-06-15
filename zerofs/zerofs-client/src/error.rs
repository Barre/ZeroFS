use ninep_client::ClientError;

/// The single error type: flat and exhaustive (deliberately NOT
/// `#[non_exhaustive]`, so `zerofs-ffi` can apply uniffi remote derives and
/// match exhaustively; a new variant must break its build, not silently
/// degrade to a catch-all).
///
/// `path`/`name` payloads are lossy display strings (invalid bytes become
/// U+FFFD), never round-trippable inputs.
///
/// The variant↔errno table is strict and 1:1; every server errno without a
/// dedicated variant surfaces as [`ZeroFsError::Io`] verbatim, so
/// [`ZeroFsError::to_errno`] is lossless by construction.
#[derive(Debug, Clone)]
pub enum ZeroFsError {
    /// No entry at the path (ENOENT).
    NotFound {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// Access denied by permission bits (EACCES).
    PermissionDenied {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// EPERM, distinct from EACCES: the operation requires ownership or
    /// privilege (e.g. chown by a non-owner).
    NotPermitted {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// The target already exists (EEXIST).
    AlreadyExists {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// A path component is not a directory (ENOTDIR).
    NotADirectory {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// The target is a directory where a non-directory was required (EISDIR).
    IsADirectory {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// A directory removal found the directory non-empty (ENOTEMPTY).
    DirectoryNotEmpty {
        /// The path the operation targeted (lossy display).
        path: String,
    },
    /// Name exceeds 255 bytes.
    NameTooLong {
        /// The offending name (lossy display).
        name: String,
    },
    /// Bad input detected client-side (e.g. `..` component, conflicting open
    /// options) or EINVAL from the server.
    InvalidArgument {
        /// What was wrong with the input.
        message: String,
    },
    /// Symlink resolution exceeded the 40-hop cap (cycle or pathological chain).
    TooManySymlinks {
        /// The path whose resolution looped (lossy display).
        path: String,
    },
    /// Handle or client used after `close()`.
    Closed,
    /// The initial connection or attach failed (including connect timeout
    /// expiry). Connectivity errors only ever surface here: after a successful
    /// connect, calls block through outages instead of failing.
    ConnectFailed {
        /// What failed during connect/attach.
        message: String,
    },
    /// Any other server errno, preserved verbatim.
    Io {
        /// The Linux errno the server returned.
        errno: i32,
        /// The path the operation targeted (lossy display).
        path: String,
        /// Human-readable rendering of the errno.
        message: String,
    },
    /// Wire-level failure: codec error, unexpected reply type, failed negotiation.
    Protocol {
        /// What went wrong on the wire.
        message: String,
    },
}

impl std::fmt::Display for ZeroFsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { path } => write!(f, "not found: {path}"),
            Self::PermissionDenied { path } => write!(f, "permission denied: {path}"),
            Self::NotPermitted { path } => write!(f, "operation not permitted: {path}"),
            Self::AlreadyExists { path } => write!(f, "already exists: {path}"),
            Self::NotADirectory { path } => write!(f, "not a directory: {path}"),
            Self::IsADirectory { path } => write!(f, "is a directory: {path}"),
            Self::DirectoryNotEmpty { path } => write!(f, "directory not empty: {path}"),
            Self::NameTooLong { name } => write!(f, "name too long: {name}"),
            Self::InvalidArgument { message } => write!(f, "invalid argument: {message}"),
            Self::TooManySymlinks { path } => {
                write!(f, "too many levels of symbolic links: {path}")
            }
            Self::Closed => write!(f, "handle is closed"),
            Self::ConnectFailed { message } => write!(f, "connection failed: {message}"),
            Self::Io {
                errno,
                path,
                message,
            } => write!(f, "i/o error (errno {errno}): {path}: {message}"),
            Self::Protocol { message } => write!(f, "protocol error: {message}"),
        }
    }
}

impl std::error::Error for ZeroFsError {}

/// Lossless conversion for interop with std/tokio I/O: the errno round-trips
/// through `from_raw_os_error` (so `ErrorKind` is set), and the original
/// `ZeroFsError` is preserved as the source (recoverable via `downcast`).
impl From<ZeroFsError> for std::io::Error {
    fn from(e: ZeroFsError) -> Self {
        let kind = std::io::Error::from_raw_os_error(e.to_errno()).kind();
        std::io::Error::new(kind, e)
    }
}

impl ZeroFsError {
    /// Linux errno per the strict 1:1 table; `Io` returns its errno unchanged.
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
            Self::Io { errno, .. } => *errno,
            Self::Protocol { .. } => libc::EIO,
        }
    }

    /// Map a server errno onto the variant table, keeping `path` as context.
    pub(crate) fn from_errno(errno: i32, path: &str) -> Self {
        let path = path.to_string();
        match errno {
            libc::ENOENT => Self::NotFound { path },
            libc::EACCES => Self::PermissionDenied { path },
            libc::EPERM => Self::NotPermitted { path },
            libc::EEXIST => Self::AlreadyExists { path },
            libc::ENOTDIR => Self::NotADirectory { path },
            libc::EISDIR => Self::IsADirectory { path },
            libc::ENOTEMPTY => Self::DirectoryNotEmpty { path },
            libc::ENAMETOOLONG => Self::NameTooLong { name: path },
            libc::EINVAL => Self::InvalidArgument {
                message: format!("{path}: invalid argument"),
            },
            libc::ELOOP => Self::TooManySymlinks { path },
            errno => Self::Io {
                message: std::io::Error::from_raw_os_error(errno).to_string(),
                errno,
                path,
            },
        }
    }

    /// Map a transport-level result onto the public error surface. Server
    /// errnos go through the variant table; everything else (codec errors,
    /// unexpected replies) is a wire-level failure.
    pub(crate) fn from_client(e: &ClientError, path: &str) -> Self {
        match e {
            ClientError::Errno(code) => Self::from_errno(*code as i32, path),
            other => Self::Protocol {
                message: format!("{path}: {other}"),
            },
        }
    }
}

/// Attach path context while mapping a transport result onto the public error.
pub(crate) trait ClientResultExt<T> {
    fn ctx(self, path: &str) -> Result<T, ZeroFsError>;
}

impl<T> ClientResultExt<T> for Result<T, ClientError> {
    fn ctx(self, path: &str) -> Result<T, ZeroFsError> {
        self.map_err(|e| ZeroFsError::from_client(&e, path))
    }
}
