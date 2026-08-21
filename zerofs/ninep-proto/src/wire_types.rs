//! Fixed 9P wire values shared with the native kernel client.
//!
//! The kernel includes this source without Deku. Userspace adds Deku derives
//! while retaining exactly the same Rust representation and field order.

#[cfg(all(not(MODULE), feature = "owned"))]
use deku::{DekuRead, DekuWrite, ctx::Endian};

pub const VERSION_9P2000L: &[u8] = b"9P2000.L";
pub const VERSION_9P2000L_ZEROFS: &[u8] = b"9P2000.L.Z";
pub const NOFID: u32 = u32::MAX;

pub const QID_TYPE_DIR: u8 = 0x80;
pub const QID_TYPE_SYMLINK: u8 = 0x02;
pub const QID_TYPE_FILE: u8 = 0;

pub const GETATTR_ALL: u64 = 0x0000_3fff;

pub const SETATTR_MODE: u32 = 0x0000_0001;
pub const SETATTR_UID: u32 = 0x0000_0002;
pub const SETATTR_GID: u32 = 0x0000_0004;
pub const SETATTR_SIZE: u32 = 0x0000_0008;
pub const SETATTR_ATIME: u32 = 0x0000_0010;
pub const SETATTR_MTIME: u32 = 0x0000_0020;
/// Accepted and ignored: every mutation stamps ctime from the server clock, and
/// the Linux 9p client sets this bit alongside mode, owner and size changes.
pub const SETATTR_CTIME: u32 = 0x0000_0040;
pub const SETATTR_ATIME_SET: u32 = 0x0000_0080;
pub const SETATTR_MTIME_SET: u32 = 0x0000_0100;
/// ZeroFS extension: persist an atime selected by the server for a successful
/// VFS access without treating it as an explicit metadata change.
pub const SETATTR_ATIME_ACCESS: u32 = 0x8000_0000;
pub const SETATTR_KNOWN: u32 = SETATTR_MODE
    | SETATTR_UID
    | SETATTR_GID
    | SETATTR_SIZE
    | SETATTR_ATIME
    | SETATTR_MTIME
    | SETATTR_CTIME
    | SETATTR_ATIME_SET
    | SETATTR_MTIME_SET;

pub const FALLOC_FL_KEEP_SIZE: u32 = 0x01;
pub const FALLOC_FL_PUNCH_HOLE: u32 = 0x02;
pub const FALLOC_FL_ZERO_RANGE: u32 = 0x10;

/// Standard data-only durability request bit. ZeroFS currently performs the
/// same full durability barrier whether this compatibility bit is set or not.
pub const P9_FSYNC_DATASYNC: u32 = 1 << 0;
/// `Tfsyncdur.datasync` limits the verified barrier to the inode named by its fid.
/// Without this flag the barrier is filesystem-wide, preserving compatibility
/// with clients that predate scoped verified fsync.
pub const P9_FSYNC_INODE: u32 = 1 << 1;
pub const P9_FSYNC_KNOWN_FLAGS: u32 = P9_FSYNC_DATASYNC | P9_FSYNC_INODE;

pub const P9_ENOTLEADER: u32 = 108;
pub const P9_ENOTLEADER_CLEAN: u32 = 107;
pub const P9_EOPIDSTALE: u32 = 116;
pub const P9_OP_FLAG_RETRY: u8 = 1 << 0;
pub const P9_OP_KNOWN_FLAGS: u8 = P9_OP_FLAG_RETRY;

pub const P9_MAX_MSIZE: u32 = 10 * 1024 * 1024;
pub const P9_SIZE_FIELD_LEN: usize = core::mem::size_of::<u32>();
pub const P9_TYPE_FIELD_LEN: usize = core::mem::size_of::<u8>();
pub const P9_TAG_FIELD_LEN: usize = core::mem::size_of::<u16>();
pub const P9_COUNT_FIELD_LEN: usize = core::mem::size_of::<u32>();
pub const P9_HEADER_SIZE: usize = P9_SIZE_FIELD_LEN + P9_TYPE_FIELD_LEN + P9_TAG_FIELD_LEN;
pub const P9_MIN_MESSAGE_SIZE: u32 = P9_HEADER_SIZE as u32;
pub const P9_IOHDRSZ: u32 = (P9_HEADER_SIZE + P9_COUNT_FIELD_LEN) as u32;
pub const P9_TWRITE_HDR: u32 = (P9_HEADER_SIZE + 4 + 8 + P9_COUNT_FIELD_LEN) as u32;
pub const P9_RLOPENATREAD_HDR: u32 =
    (P9_HEADER_SIZE + Qid::WIRE_SIZE + 4 + 1 + P9_COUNT_FIELD_LEN) as u32;

pub const P9_MAX_GROUPS: usize = 16;
pub const P9_MAX_NAME_LEN: u32 = 255;

pub const P9_REBIND_REPLAY: u8 = 1 << 0;
pub const P9_REBIND_OPENED: u8 = 1 << 1;
pub const P9_REBIND_KNOWN_FLAGS: u8 = P9_REBIND_REPLAY | P9_REBIND_OPENED;
pub const P9_REBIND_CREDENTIAL_SENTINEL: u8 = 0xff;
pub const P9_REBIND_CREDENTIAL_VERSION: u8 = 1;
/// High bit of the credential group-count byte. The sender supplied the
/// primary gid, could not encode the complete supplementary-group list, and
/// already performed the caller's group DAC check locally.
pub const P9_REBIND_CREDENTIAL_GROUPS_INCOMPLETE: u8 = 1 << 7;
pub const P9_REBIND_CREDENTIAL_HEADER_SIZE: usize = 7;
pub const P9_REBIND_CREDENTIAL_MAX_SIZE: usize =
    P9_REBIND_CREDENTIAL_HEADER_SIZE + P9_MAX_GROUPS * 4;

pub const P9_OP_ID_LEN: usize = 16;
pub const P9_OP_FLAGS_LEN: usize = 1;
pub const P9_OP_ORIGIN_EPOCH_LEN: usize = 8;
pub const P9_OP_ENVELOPE_LEN: usize = P9_OP_ID_LEN + P9_OP_FLAGS_LEN + P9_OP_ORIGIN_EPOCH_LEN;

/// Storage backing a variable-length wire field.
///
/// The shared representation only needs a byte slice. Ownership and
/// allocation policy are supplied by userspace (`Vec`/`Bytes`) or the kernel
/// (borrowed slices and kernel-owned buffers).
pub trait ByteStorage: AsRef<[u8]> {}

impl<T: AsRef<[u8]> + ?Sized> ByteStorage for T {}

/// A variable-length byte field whose length is carried by its enclosing
/// message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WireBytes<B: ByteStorage>(pub B);

impl<B: ByteStorage> WireBytes<B> {
    pub fn new(bytes: B) -> Self {
        Self(bytes)
    }

    pub fn len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }
}

impl<B: ByteStorage> From<B> for WireBytes<B> {
    fn from(bytes: B) -> Self {
        Self(bytes)
    }
}

impl<B: ByteStorage> AsRef<[u8]> for WireBytes<B> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<B: ByteStorage> core::ops::Deref for WireBytes<B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A length-prefixed 9P string using caller-selected byte storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireString<B: ByteStorage> {
    pub len: u16,
    pub data: B,
}

impl<B: ByteStorage> WireString<B> {
    pub fn from_storage(data: B) -> Self {
        Self {
            len: data.as_ref().len() as u16,
            data,
        }
    }

    pub fn as_str(&self) -> Result<&str, core::str::Utf8Error> {
        core::str::from_utf8(self.data.as_ref())
    }

    pub fn len(&self) -> usize {
        self.data.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.as_ref().is_empty()
    }

    /// Serialized size on the wire: u16 length prefix + bytes.
    pub fn wire_size(&self) -> usize {
        2 + self.len()
    }
}

impl<B: ByteStorage> AsRef<[u8]> for WireString<B> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

/// Idempotency and HA-origin fields carried by a ZeroFS mutation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MutationEnvelope {
    pub op_id: [u8; 16],
    pub flags: u8,
    pub origin_writer_epoch: u64,
}

/// 9P2000.L and ZeroFS-private message identifiers.
pub mod message_type {
    pub const RLERROR: u8 = 7;
    pub const TSTATFS: u8 = 8;
    pub const RSTATFS: u8 = 9;
    pub const TLOPEN: u8 = 12;
    pub const RLOPEN: u8 = 13;
    pub const TLCREATE: u8 = 14;
    pub const RLCREATE: u8 = 15;
    pub const TSYMLINK: u8 = 16;
    pub const RSYMLINK: u8 = 17;
    pub const TMKNOD: u8 = 18;
    pub const RMKNOD: u8 = 19;
    pub const TRENAME: u8 = 20;
    pub const RRENAME: u8 = 21;
    pub const TREADLINK: u8 = 22;
    pub const RREADLINK: u8 = 23;
    pub const TGETATTR: u8 = 24;
    pub const RGETATTR: u8 = 25;
    pub const TSETATTR: u8 = 26;
    pub const RSETATTR: u8 = 27;
    pub const TXATTRWALK: u8 = 30;
    pub const RXATTRWALK: u8 = 31;
    pub const TREADDIR: u8 = 40;
    pub const RREADDIR: u8 = 41;
    pub const TFSYNC: u8 = 50;
    pub const RFSYNC: u8 = 51;
    pub const TLOCK: u8 = 52;
    pub const RLOCK: u8 = 53;
    pub const TGETLOCK: u8 = 54;
    pub const RGETLOCK: u8 = 55;
    pub const TLINK: u8 = 70;
    pub const RLINK: u8 = 71;
    pub const TMKDIR: u8 = 72;
    pub const RMKDIR: u8 = 73;
    pub const TRENAMEAT: u8 = 74;
    pub const RRENAMEAT: u8 = 75;
    pub const TUNLINKAT: u8 = 76;
    pub const RUNLINKAT: u8 = 77;
    pub const TVERSION: u8 = 100;
    pub const RVERSION: u8 = 101;
    pub const TATTACH: u8 = 104;
    pub const RATTACH: u8 = 105;
    pub const TFLUSH: u8 = 108;
    pub const RFLUSH: u8 = 109;
    pub const TWALK: u8 = 110;
    pub const RWALK: u8 = 111;
    pub const TREAD: u8 = 116;
    pub const RREAD: u8 = 117;
    pub const TWRITE: u8 = 118;
    pub const RWRITE: u8 = 119;
    pub const TCLUNK: u8 = 120;
    pub const RCLUNK: u8 = 121;
    pub const TFALLOCATE: u8 = 228;
    pub const RFALLOCATE: u8 = 229;
    pub const TLOPENATREAD: u8 = 230;
    pub const RLOPENATREAD: u8 = 231;
    pub const TFSYNCDUR: u8 = 232;
    pub const TGETLINEAGE: u8 = 233;
    pub const RGETLINEAGE: u8 = 234;
    pub const TLOPENAT: u8 = 236;
    pub const RLOPENAT: u8 = 237;
    pub const TLCREATEATTR: u8 = 238;
    pub const RLCREATEATTR: u8 = 239;
    pub const TMKDIRATTR: u8 = 240;
    pub const RMKDIRATTR: u8 = 241;
    pub const TSYMLINKATTR: u8 = 242;
    pub const RSYMLINKATTR: u8 = 243;
    pub const TMKNODATTR: u8 = 244;
    pub const RMKNODATTR: u8 = 245;
    pub const TLINKATTR: u8 = 246;
    pub const RLINKATTR: u8 = 247;
    pub const TSETATTRATTR: u8 = 248;
    pub const RSETATTRATTR: u8 = 249;
    pub const TREBIND: u8 = 250;
    pub const RREBIND: u8 = 251;
    pub const TWALKGETATTR: u8 = 252;
    pub const RWALKGETATTR: u8 = 253;
    pub const TREADDIRATTR: u8 = 254;
    pub const RREADDIRATTR: u8 = 255;
}

/// A ZeroFS inode identity.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[cfg_attr(
    all(not(MODULE), feature = "owned"),
    deku(
        endian = "endian",
        ctx = "endian: Endian",
        ctx_default = "Endian::Little"
    )
)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Qid {
    pub type_: u8,
    pub version: u32,
    pub path: u64,
}

impl Qid {
    /// Serialized size on the wire: type u8 + version u32 + path u64.
    pub const WIRE_SIZE: usize = 1 + 4 + 8;
}

/// Attributes returned by ZeroFS.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stat {
    pub qid: Qid,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u64,
    pub rdev: u64,
    pub size: u64,
    pub blksize: u64,
    pub blocks: u64,
    pub atime_sec: u64,
    pub atime_nsec: u64,
    pub mtime_sec: u64,
    pub mtime_nsec: u64,
    pub ctime_sec: u64,
    pub ctime_nsec: u64,
    pub btime_sec: u64,
    pub btime_nsec: u64,
    pub r#gen: u64,
    /// Standard 9P2000.L content-change counter; ZeroFS currently emits zero.
    pub data_version: u64,
}

impl Stat {
    /// Serialized size on the wire: qid + three u32s + fifteen u64s.
    pub const WIRE_SIZE: usize = Qid::WIRE_SIZE + 3 * 4 + 15 * 8;
}

// Replies whose every field is fixed width need no storage parameter, so one
// declaration serves the owned and the borrowed codec alike. Only the Deku
// derives are conditional, exactly as for `Qid` and `Stat` above.

/// Durability lineage and the active HA writer epoch.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rgetlineage {
    pub token: u64,
    /// HA writer epoch used as mutation-envelope origin; zero when standalone.
    pub writer_epoch: u64,
}

/// Identity of a fid rebound by inode id.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rrebind {
    pub qid: Qid,
}

/// Attributes for a fid, with the mask of fields the server answered.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rgetattr {
    #[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
    pub valid: u64,
    pub stat: Stat,
}

/// Standard open reply layout, also used on the wire by private `Rlopenat`.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rlopen {
    pub qid: Qid,
    #[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
    pub iounit: u32,
}

/// Post-operation stat of a created and opened regular file.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rlcreateattr {
    #[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
    pub iounit: u32,
    pub stat: Stat,
}

/// Bytes accepted by a write.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rwrite {
    pub count: u32,
}

/// Remote filesystem capacity and limits.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rstatfs {
    /// Filesystem type.
    pub r#type: u32,
    /// Optimal transfer block size.
    pub bsize: u32,
    /// Total data blocks.
    pub blocks: u64,
    /// Free data blocks.
    pub bfree: u64,
    /// Free blocks available to non-superusers.
    pub bavail: u64,
    /// Total file nodes.
    pub files: u64,
    /// Free file nodes.
    pub ffree: u64,
    /// Filesystem id.
    pub fsid: u64,
    /// Maximum filename length.
    pub namelen: u32,
}

/// Outcome of a `Tlock` request.
///
/// [`Rlock`] keeps the wire field raw so an unknown status fails that request
/// without turning a decodable frame into a session-wide protocol fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LockStatus {
    /// The range is held by the requesting fid.
    Success = 0,
    /// A conflicting lock exists and the request wanted to block.
    Blocked = 1,
    /// The server refused the lock.
    LockError = 2,
    /// The server is in its post-restart grace period.
    Grace = 3,
}

impl LockStatus {
    pub const fn as_wire(self) -> u8 {
        self as u8
    }

    /// The status this byte names, or `None` for one this protocol does not
    /// define.
    pub const fn from_wire(status: u8) -> Option<Self> {
        match status {
            0 => Some(Self::Success),
            1 => Some(Self::Blocked),
            2 => Some(Self::LockError),
            3 => Some(Self::Grace),
            _ => None,
        }
    }
}

/// Outcome of a lock request, as the raw wire byte. Map it with
/// [`LockStatus::from_wire`].
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rlock {
    pub status: u8,
}

/// A failed request's Linux errno.
#[cfg_attr(all(not(MODULE), feature = "owned"), derive(DekuRead, DekuWrite))]
#[cfg_attr(all(not(MODULE), feature = "owned"), deku(endian = "little"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rlerror {
    pub ecode: u32,
}
