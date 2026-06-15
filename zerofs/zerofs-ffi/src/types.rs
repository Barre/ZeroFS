//! FFI-shaped mirrors of the client's data records, with conversions in both
//! directions (`From` for outputs, helper methods for inputs).
//!
//! Times cross as `std::time::SystemTime` (uniffi's built-in timestamp), so
//! bindings get native time types (`datetime` in Python, `Instant` in Kotlin,
//! `Date` in Swift/JS) rather than a raw `{secs, nanos}` record.

use std::time::SystemTime;

/// File type derived from the mode/dirent type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FileType {
    /// Regular file.
    File,
    /// Directory.
    Dir,
    /// Symbolic link.
    Symlink,
    /// Named pipe (FIFO).
    Fifo,
    /// Unix-domain socket node.
    Socket,
    /// Character device.
    CharDevice,
    /// Block device.
    BlockDevice,
    /// Unrecognized type.
    Unknown,
}

impl From<zerofs_client::FileType> for FileType {
    fn from(t: zerofs_client::FileType) -> Self {
        use zerofs_client::FileType as T;
        match t {
            T::File => Self::File,
            T::Dir => Self::Dir,
            T::Symlink => Self::Symlink,
            T::Fifo => Self::Fifo,
            T::Socket => Self::Socket,
            T::CharDevice => Self::CharDevice,
            T::BlockDevice => Self::BlockDevice,
            T::Unknown => Self::Unknown,
        }
    }
}

/// A time to set: the server's current clock, or an explicit instant.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum SetTime {
    /// Use the server's current clock.
    Now,
    /// Set to this explicit instant.
    At {
        /// The instant to set.
        time: SystemTime,
    },
}

impl From<SetTime> for zerofs_client::SetTime {
    fn from(t: SetTime) -> Self {
        match t {
            SetTime::Now => zerofs_client::SetTime::Now,
            SetTime::At { time } => zerofs_client::SetTime::At { time: time.into() },
        }
    }
}

/// Metadata changes; `None` fields are untouched. All-`None` is a no-op.
#[derive(Clone, Copy, Debug, Default, uniffi::Record)]
pub struct SetAttrs {
    /// New permission bits (low 12 bits used).
    #[uniffi(default = None)]
    pub mode: Option<u32>,
    /// New owner uid.
    #[uniffi(default = None)]
    pub uid: Option<u32>,
    /// New owner gid.
    #[uniffi(default = None)]
    pub gid: Option<u32>,
    /// New length (truncates or extends).
    #[uniffi(default = None)]
    pub size: Option<u64>,
    /// New access time.
    #[uniffi(default = None)]
    pub atime: Option<SetTime>,
    /// New modification time.
    #[uniffi(default = None)]
    pub mtime: Option<SetTime>,
}

impl From<SetAttrs> for zerofs_client::SetAttrs {
    fn from(a: SetAttrs) -> Self {
        zerofs_client::SetAttrs {
            mode: a.mode,
            uid: a.uid,
            gid: a.gid,
            size: a.size,
            atime: a.atime.map(Into::into),
            mtime: a.mtime.map(Into::into),
        }
    }
}

/// Kind of special node for `mknod`.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum NodeKind {
    /// Named pipe (FIFO).
    Fifo,
    /// Unix-domain socket node.
    Socket,
    /// Block device with the given major/minor numbers.
    BlockDevice {
        /// Device major number.
        major: u32,
        /// Device minor number.
        minor: u32,
    },
    /// Character device with the given major/minor numbers.
    CharDevice {
        /// Device major number.
        major: u32,
        /// Device minor number.
        minor: u32,
    },
}

impl From<NodeKind> for zerofs_client::NodeKind {
    fn from(k: NodeKind) -> Self {
        match k {
            NodeKind::Fifo => zerofs_client::NodeKind::Fifo,
            NodeKind::Socket => zerofs_client::NodeKind::Socket,
            NodeKind::BlockDevice { major, minor } => {
                zerofs_client::NodeKind::BlockDevice { major, minor }
            }
            NodeKind::CharDevice { major, minor } => {
                zerofs_client::NodeKind::CharDevice { major, minor }
            }
        }
    }
}

/// Options for opening (and optionally creating) a file. Defaults: all flags
/// false, mode 0o644.
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct OpenOptions {
    /// Open for reading.
    #[uniffi(default = false)]
    pub read: bool,
    /// Open for writing.
    #[uniffi(default = false)]
    pub write: bool,
    /// Open-or-create.
    #[uniffi(default = false)]
    pub create: bool,
    /// Fail unless this call creates the file (the atomic exclusive primitive).
    #[uniffi(default = false)]
    pub create_new: bool,
    /// Truncate to zero length on open.
    #[uniffi(default = false)]
    pub truncate: bool,
    /// Permission bits when the open creates the file.
    #[uniffi(default = 420)]
    pub mode: u32,
}

impl From<OpenOptions> for zerofs_client::OpenOptions {
    fn from(o: OpenOptions) -> Self {
        zerofs_client::OpenOptions {
            read: o.read,
            write: o.write,
            create: o.create,
            create_new: o.create_new,
            truncate: o.truncate,
            mode: o.mode,
        }
    }
}

/// Connection identity, attach target, and tuning. Defaults mirror the Rust
/// client (`connect_timeout_ms` 30000, `msize` 1 MiB, identity from the process).
#[derive(Clone, Debug, uniffi::Record)]
pub struct ConnectOptions {
    /// Numeric uid asserted at attach (`None` = process euid).
    #[uniffi(default = None)]
    pub uid: Option<u32>,
    /// Group for files created through this client (`None` = process egid).
    #[uniffi(default = None)]
    pub gid: Option<u32>,
    /// Username sent at attach (`None` = `$USER`); informational.
    #[uniffi(default = None)]
    pub uname: Option<String>,
    /// Attach name (export selector); empty selects the default export.
    #[uniffi(default = "")]
    pub aname: String,
    /// Requested 9P message size.
    #[uniffi(default = 1048576)]
    pub msize: u32,
    /// Bound on the initial connect+attach in ms; `None` = wait indefinitely.
    #[uniffi(default = Some(30000))]
    pub connect_timeout_ms: Option<u32>,
}

impl From<ConnectOptions> for zerofs_client::ConnectOptions {
    fn from(o: ConnectOptions) -> Self {
        zerofs_client::ConnectOptions {
            uid: o.uid,
            gid: o.gid,
            uname: o.uname,
            aname: o.aname,
            msize: o.msize,
            connect_timeout_ms: o.connect_timeout_ms,
        }
    }
}

/// Live snapshot of negotiated session properties (may change across reconnects).
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct Capabilities {
    /// ZeroFS v1 extensions active.
    pub extensions_v1: bool,
    /// ZeroFS v2 extensions active.
    pub extensions_v2: bool,
    /// Negotiated 9P message size in bytes.
    pub msize: u32,
    /// Largest single-message read payload.
    pub max_read_chunk: u32,
    /// Largest single-message write payload.
    pub max_write_chunk: u32,
}

impl From<zerofs_client::Capabilities> for Capabilities {
    fn from(c: zerofs_client::Capabilities) -> Self {
        Self {
            extensions_v1: c.extensions_v1,
            extensions_v2: c.extensions_v2,
            msize: c.msize,
            max_read_chunk: c.max_read_chunk,
            max_write_chunk: c.max_write_chunk,
        }
    }
}

/// Filesystem usage and limits.
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct StatFs {
    /// Optimal transfer block size.
    pub block_size: u32,
    /// Total data blocks in the filesystem.
    pub blocks: u64,
    /// Free blocks.
    pub blocks_free: u64,
    /// Free blocks available to unprivileged users.
    pub blocks_available: u64,
    /// Total inodes (files).
    pub files: u64,
    /// Free inodes.
    pub files_free: u64,
    /// Filesystem id.
    pub filesystem_id: u64,
    /// Maximum filename length.
    pub max_name_len: u32,
}

impl From<zerofs_client::StatFs> for StatFs {
    fn from(s: zerofs_client::StatFs) -> Self {
        Self {
            block_size: s.block_size,
            blocks: s.blocks,
            blocks_free: s.blocks_free,
            blocks_available: s.blocks_available,
            files: s.files,
            files_free: s.files_free,
            filesystem_id: s.filesystem_id,
            max_name_len: s.max_name_len,
        }
    }
}

/// POSIX-shaped attributes.
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct Metadata {
    /// Stable inode number (ZeroFS never reuses inode ids).
    pub ino: u64,
    /// File type.
    pub file_type: FileType,
    /// Full st_mode (type bits + permission bits).
    pub mode: u32,
    /// Hard-link count.
    pub nlink: u64,
    /// Owner uid.
    pub uid: u32,
    /// Owner gid.
    pub gid: u32,
    /// Size in bytes.
    pub size: u64,
    /// Preferred I/O block size reported by the server.
    pub block_size: u64,
    /// Number of 512-byte blocks allocated.
    pub blocks: u64,
    /// Device id for char/block nodes; 0 otherwise.
    pub rdev: u64,
    /// Last access time.
    pub atime: SystemTime,
    /// Last modification time.
    pub mtime: SystemTime,
    /// Last status-change time.
    pub ctime: SystemTime,
    /// RESERVED: creation time (servers currently report 0 = the epoch).
    pub btime: SystemTime,
    /// RESERVED: content-change counter (servers currently report 0).
    pub data_version: u64,
}

impl From<zerofs_client::Metadata> for Metadata {
    fn from(m: zerofs_client::Metadata) -> Self {
        Self {
            ino: m.ino,
            file_type: m.file_type.into(),
            mode: m.mode,
            nlink: m.nlink,
            uid: m.uid,
            gid: m.gid,
            size: m.size,
            block_size: m.block_size,
            blocks: m.blocks,
            rdev: m.rdev,
            atime: m.atime.into(),
            mtime: m.mtime.into(),
            ctime: m.ctime.into(),
            btime: m.btime.into(),
            data_version: m.data_version,
        }
    }
}

/// One directory entry; `.` and `..` are filtered out.
#[derive(Clone, Debug, uniffi::Record)]
pub struct DirEntry {
    /// Name decoded as UTF-8 (lossy: invalid bytes become U+FFFD).
    pub name: String,
    /// Exact on-wire name bytes; feed into the `Dir` `*_at` methods verbatim.
    pub name_bytes: Vec<u8>,
    /// True when `name` losslessly round-trips to `name_bytes`.
    pub name_is_utf8: bool,
    /// Entry type, known without a stat.
    pub file_type: FileType,
    /// Stable inode number.
    pub ino: u64,
    /// Full metadata when readdirplus is negotiated (v1+); `None` otherwise.
    pub metadata: Option<Metadata>,
}

impl From<zerofs_client::DirEntry> for DirEntry {
    fn from(e: zerofs_client::DirEntry) -> Self {
        Self {
            name: e.name,
            name_bytes: e.name_bytes,
            name_is_utf8: e.name_is_utf8,
            file_type: e.file_type.into(),
            ino: e.ino,
            metadata: e.metadata.map(Into::into),
        }
    }
}
