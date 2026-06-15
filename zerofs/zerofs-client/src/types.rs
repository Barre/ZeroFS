use ninep_proto::{Rstatfs, Stat};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Options for [`crate::Client::connect_with`]. All defaults are
/// literal-expressible so uniffi record defaults can reproduce them in every
/// binding; `None` identity fields are resolved Rust-side (euid/egid/`$USER`)
/// inside connect.
///
/// There is deliberately no per-operation timeout: bound waits with your
/// language's async facilities (`tokio::time::timeout`, `asyncio.wait_for`,
/// `withTimeout`, `Promise.race`); every public future is cancel-safe.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConnectOptions {
    /// Numeric uid asserted at attach (`None` = process euid); the server
    /// enforces permissions as this user.
    pub uid: Option<u32>,
    /// Group assigned to files/directories created through this client
    /// (`None` = process egid).
    pub gid: Option<u32>,
    /// Username string sent at attach (`None` = `$USER`, else the uid rendered
    /// as text); informational; `uid` is authoritative.
    pub uname: Option<String>,
    /// Attach name (export selector); empty selects the default export.
    pub aname: String,
    /// Requested 9P message size; the negotiated value appears in [`Capabilities`].
    pub msize: u32,
    /// Bound on the initial connect+attach; expiry surfaces as
    /// [`crate::ZeroFsError::ConnectFailed`]. `None` = wait indefinitely.
    pub connect_timeout_ms: Option<u32>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            uid: None,
            gid: None,
            uname: None,
            aname: String::new(),
            msize: 1024 * 1024,
            connect_timeout_ms: Some(30_000),
        }
    }
}

/// Live snapshot of negotiated session properties. msize and the extension
/// level are re-negotiated on every transparent reconnect; treat this as
/// advisory at the instant of the call, not a constant for the life of the
/// client.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Capabilities {
    /// ZeroFS v1 extensions active: one-round-trip lookup+stat and readdirplus.
    pub extensions_v1: bool,
    /// ZeroFS v2 extensions active: stat-carrying create/open/setattr fast paths.
    pub extensions_v2: bool,
    /// Negotiated 9P message size in bytes.
    pub msize: u32,
    /// Largest single-message read payload (larger reads chunk transparently).
    pub max_read_chunk: u32,
    /// Largest single-message write payload (larger writes chunk transparently).
    pub max_write_chunk: u32,
}

/// Plain record so it crosses FFI as a dictionary; constructors are Rust sugar.
/// All field defaults are literals (bools false, mode 420 = 0o644), so
/// keyword-style construction in the bindings does the right thing. There is
/// deliberately no `append` flag: the server ignores open flags on writes, so
/// appending is the explicit [`crate::Client::append`] composition.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OpenOptions {
    /// Open for reading.
    pub read: bool,
    /// Open for writing.
    pub write: bool,
    /// Open-or-create. Not a single wire op: composed as open, falling back to
    /// exclusive create, retrying on race.
    pub create: bool,
    /// Fail with `AlreadyExists` unless this call creates the file. This is
    /// the atomic primitive (server creates are natively exclusive).
    pub create_new: bool,
    /// Truncate to zero length on open. For a pre-existing file this is an
    /// extra setattr round trip after the open, NOT atomic with it.
    pub truncate: bool,
    /// Permission bits when the open creates the file. Default 0o644.
    pub mode: u32,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
            create_new: false,
            truncate: false,
            mode: 0o644,
        }
    }
}

impl OpenOptions {
    /// `read` only.
    pub fn read_only() -> Self {
        Self {
            read: true,
            ..Self::default()
        }
    }

    /// `write` only.
    pub fn write_only() -> Self {
        Self {
            write: true,
            ..Self::default()
        }
    }

    /// `read` + `write`.
    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            ..Self::default()
        }
    }

    /// Builder-style toggle for `create`.
    pub fn create(mut self, yes: bool) -> Self {
        self.create = yes;
        self
    }

    /// Builder-style toggle for `create_new`.
    pub fn create_new(mut self, yes: bool) -> Self {
        self.create_new = yes;
        self
    }

    /// Builder-style toggle for `truncate`.
    pub fn truncate(mut self, yes: bool) -> Self {
        self.truncate = yes;
        self
    }

    /// Builder-style setter for the creation mode.
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
}

/// File type derived from the mode/dirent type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

impl FileType {
    pub(crate) fn from_mode(mode: u32) -> Self {
        match mode & libc::S_IFMT as u32 {
            x if x == libc::S_IFREG as u32 => Self::File,
            x if x == libc::S_IFDIR as u32 => Self::Dir,
            x if x == libc::S_IFLNK as u32 => Self::Symlink,
            x if x == libc::S_IFIFO as u32 => Self::Fifo,
            x if x == libc::S_IFSOCK as u32 => Self::Socket,
            x if x == libc::S_IFCHR as u32 => Self::CharDevice,
            x if x == libc::S_IFBLK as u32 => Self::BlockDevice,
            _ => Self::Unknown,
        }
    }

    pub(crate) fn from_dt(dt: u8) -> Self {
        match dt {
            libc::DT_REG => Self::File,
            libc::DT_DIR => Self::Dir,
            libc::DT_LNK => Self::Symlink,
            libc::DT_FIFO => Self::Fifo,
            libc::DT_SOCK => Self::Socket,
            libc::DT_CHR => Self::CharDevice,
            libc::DT_BLK => Self::BlockDevice,
            _ => Self::Unknown,
        }
    }
}

/// Nanosecond UNIX timestamp as explicit fields (predictable across all bindings).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Timestamp {
    /// Whole seconds since the UNIX epoch (negative before it).
    pub secs: i64,
    /// Nanoseconds within the second, `0..1_000_000_000`.
    pub nanos: u32,
}

impl From<SystemTime> for Timestamp {
    fn from(t: SystemTime) -> Self {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Timestamp {
                secs: d.as_secs() as i64,
                nanos: d.subsec_nanos(),
            },
            // Pre-epoch: split so `secs + nanos/1e9` still equals the instant
            // (the inverse of the decoding in `systime`).
            Err(e) => {
                let d = e.duration();
                let (secs, nanos) = (d.as_secs() as i64, d.subsec_nanos());
                if nanos == 0 {
                    Timestamp {
                        secs: -secs,
                        nanos: 0,
                    }
                } else {
                    Timestamp {
                        secs: -secs - 1,
                        nanos: 1_000_000_000 - nanos,
                    }
                }
            }
        }
    }
}

impl From<Timestamp> for SystemTime {
    fn from(t: Timestamp) -> Self {
        systime(t)
    }
}

/// A time to set: the server's current clock, or an explicit instant.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SetTime {
    /// Use the server's current clock.
    Now,
    /// Set to this explicit instant.
    At {
        /// The instant to set.
        time: Timestamp,
    },
}

impl From<SystemTime> for SetTime {
    fn from(t: SystemTime) -> Self {
        SetTime::At { time: t.into() }
    }
}

/// Metadata changes; `None` fields are untouched. All-`None` is a no-op.
#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SetAttrs {
    /// New permission bits (low 12 bits used).
    pub mode: Option<u32>,
    /// New owner uid.
    pub uid: Option<u32>,
    /// New owner gid.
    pub gid: Option<u32>,
    /// New length (truncates or extends).
    pub size: Option<u64>,
    /// New access time.
    pub atime: Option<SetTime>,
    /// New modification time.
    pub mtime: Option<SetTime>,
}

/// Kind of special node for `mknod`; a tagged enum so callers never pack
/// `S_IF*` bits or pass meaningless major/minor for fifos and sockets.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

/// POSIX-shaped attributes; plain data record everywhere.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    pub atime: Timestamp,
    /// Last modification time.
    pub mtime: Timestamp,
    /// Last status-change time.
    pub ctime: Timestamp,
    /// RESERVED. The wire carries a creation time, but current ZeroFS servers
    /// always report 0 (the epoch). Do not display as a real birth time yet.
    pub btime: Timestamp,
    /// RESERVED. The wire carries a content-change counter, but current ZeroFS
    /// servers always report 0; do NOT build cache invalidation on it. For
    /// change detection today, use `mtime`.
    pub data_version: u64,
}

impl Metadata {
    pub(crate) fn from_stat(st: &Stat) -> Self {
        let ts = |secs: u64, nanos: u64| Timestamp {
            secs: secs as i64,
            nanos: nanos as u32,
        };
        Self {
            ino: st.qid.path,
            file_type: FileType::from_mode(st.mode),
            mode: st.mode,
            nlink: st.nlink,
            uid: st.uid,
            gid: st.gid,
            size: st.size,
            block_size: st.blksize,
            blocks: st.blocks,
            rdev: st.rdev,
            atime: ts(st.atime_sec, st.atime_nsec),
            mtime: ts(st.mtime_sec, st.mtime_nsec),
            ctime: ts(st.ctime_sec, st.ctime_nsec),
            btime: ts(st.btime_sec, st.btime_nsec),
            data_version: st.data_version,
        }
    }

    /// True if this is a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type == FileType::File
    }

    /// True if this is a directory.
    pub fn is_dir(&self) -> bool {
        self.file_type == FileType::Dir
    }

    /// True if this is a symbolic link.
    pub fn is_symlink(&self) -> bool {
        self.file_type == FileType::Symlink
    }

    /// Permission bits only.
    pub fn permissions(&self) -> u32 {
        self.mode & 0o7777
    }

    /// `mtime` as a `SystemTime` (Rust-only sugar).
    pub fn modified(&self) -> SystemTime {
        systime(self.mtime)
    }

    /// `atime` as a `SystemTime` (Rust-only sugar).
    pub fn accessed(&self) -> SystemTime {
        systime(self.atime)
    }
}

fn systime(t: Timestamp) -> SystemTime {
    // Defend against out-of-range / un-normalized wire data: clamp sub-second
    // nanos and saturate rather than panic on an unrepresentable instant.
    let nanos = t.nanos.min(999_999_999);
    if t.secs >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::new(t.secs as u64, nanos))
            .unwrap_or(UNIX_EPOCH)
    } else {
        let base = UNIX_EPOCH
            .checked_sub(Duration::new(t.secs.unsigned_abs(), 0))
            .unwrap_or(UNIX_EPOCH);
        base.checked_add(Duration::new(0, nanos)).unwrap_or(base)
    }
}

/// Filesystem usage, from 9P statfs.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

impl StatFs {
    pub(crate) fn from_wire(r: &Rstatfs) -> Self {
        Self {
            block_size: r.bsize,
            blocks: r.blocks,
            blocks_free: r.bfree,
            blocks_available: r.bavail,
            files: r.files,
            files_free: r.ffree,
            filesystem_id: r.fsid,
            max_name_len: r.namelen,
        }
    }
}

/// One directory entry; the library filters out `.` and `..`.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DirEntry {
    /// Name decoded as UTF-8 (lossy: invalid bytes become U+FFFD).
    pub name: String,
    /// Exact on-wire name bytes; authoritative, feed into the `Dir::*_at`
    /// methods verbatim.
    pub name_bytes: Vec<u8>,
    /// True when `name` losslessly round-trips to `name_bytes`.
    pub name_is_utf8: bool,
    /// Entry type, known without a stat.
    pub file_type: FileType,
    /// Stable inode number (ZeroFS never reuses inode ids).
    pub ino: u64,
    /// Full metadata when readdirplus is negotiated (v1+); `None` on plain
    /// 9P2000.L.
    pub metadata: Option<Metadata>,
}

impl DirEntry {
    pub(crate) fn from_plus(e: &ninep_proto::DirEntryPlus) -> Self {
        let name_bytes = e.name.data.clone();
        Self {
            name: String::from_utf8_lossy(&name_bytes).into_owned(),
            name_is_utf8: std::str::from_utf8(&name_bytes).is_ok(),
            file_type: FileType::from_mode(e.stat.mode),
            ino: e.qid.path,
            metadata: Some(Metadata::from_stat(&e.stat)),
            name_bytes,
        }
    }

    pub(crate) fn from_plain(e: &ninep_proto::DirEntry) -> Self {
        let name_bytes = e.name.data.clone();
        Self {
            name: String::from_utf8_lossy(&name_bytes).into_owned(),
            name_is_utf8: std::str::from_utf8(&name_bytes).is_ok(),
            file_type: FileType::from_dt(e.type_),
            ino: e.qid.path,
            metadata: None,
            name_bytes,
        }
    }
}
