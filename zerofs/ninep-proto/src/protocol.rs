use crate::deku_bytes::DekuBytes;
use deku::ctx::Endian;
use deku::prelude::*;

pub const VERSION_9P2000L: &[u8] = b"9P2000.L";
/// ZeroFS private dialect identifier. `.Z` distinguishes this wire generation
/// from legacy `.zerofs*` layouts. The dialect includes compound operations,
/// stat-bearing replies, replay, mutation envelopes, durability lineage,
/// open-prefetch, and atomic fallocate.
pub const VERSION_9P2000L_ZEROFS: &[u8] = b"9P2000.L.Z";
/// The 9P sentinel for an absent fid, including an unauthenticated `Tattach.afid`.
pub const NOFID: u32 = u32::MAX;

// QID type constants
pub const QID_TYPE_DIR: u8 = 0x80;
pub const QID_TYPE_SYMLINK: u8 = 0x02;
pub const QID_TYPE_FILE: u8 = 0x00;

pub const GETATTR_ALL: u64 = 0x00003fff;

// Setattr valid bits
pub const SETATTR_MODE: u32 = 0x00000001;
pub const SETATTR_UID: u32 = 0x00000002;
pub const SETATTR_GID: u32 = 0x00000004;
pub const SETATTR_SIZE: u32 = 0x00000008;
pub const SETATTR_ATIME: u32 = 0x00000010;
pub const SETATTR_MTIME: u32 = 0x00000020;
pub const SETATTR_ATIME_SET: u32 = 0x00000080;
pub const SETATTR_MTIME_SET: u32 = 0x00000100;

// Linux fallocate mode bits carried verbatim by Tfallocate.
pub const FALLOC_FL_KEEP_SIZE: u32 = 0x01;
pub const FALLOC_FL_PUNCH_HOLE: u32 = 0x02;
pub const FALLOC_FL_ZERO_RANGE: u32 = 0x10;

/// Supported semantic operation encoded by the Linux fallocate mode bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallocateKind {
    Allocate,
    PunchHole,
    ZeroRange { keep_size: bool },
}

/// Parse the exact fallocate mode combinations supported by ZeroFS.
pub fn classify_fallocate_mode(mode: u32) -> Option<FallocateKind> {
    match mode {
        0 => Some(FallocateKind::Allocate),
        mode if mode == (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE) => {
            Some(FallocateKind::PunchHole)
        }
        FALLOC_FL_ZERO_RANGE => Some(FallocateKind::ZeroRange { keep_size: false }),
        mode if mode == (FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE) => {
            Some(FallocateKind::ZeroRange { keep_size: true })
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, DekuRead, DekuWrite)]
#[deku(
    id_type = "u8",
    ctx = "_endian: Endian",
    ctx_default = "Endian::Little"
)]
pub enum LockType {
    #[deku(id = "0")]
    ReadLock, // F_RDLCK
    #[deku(id = "1")]
    WriteLock, // F_WRLCK
    #[deku(id = "2")]
    Unlock, // F_UNLCK
}

pub const P9_LOCK_FLAGS_BLOCK: u32 = 1; // blocking request

/// `Rlerror` code for leadership loss with a potentially dispatched request.
/// Resends retain `P9_OP_FLAG_RETRY`. Numeric value: Linux `ESHUTDOWN`.
pub const P9_ENOTLEADER: u32 = 108;

/// `Rlerror` code proving no logical effect. A CLEAN response to FIRST permits
/// another FIRST; CLEAN on RETRY does not clear earlier ambiguity. Numeric
/// value: Linux `ENOTCONN`.
pub const P9_ENOTLEADER_CLEAN: u32 = 107;

/// `Rlerror` code for a RETRY absent from the in-flight and completed ledgers.
/// The request must not be dispatched. Numeric value: Linux `ESTALE`.
pub const P9_EOPIDSTALE: u32 = 116;

/// The frame is an ambiguous resend of an op-id, not its sole initial attempt.
/// An unseen request carrying this flag is invalid and must not be dispatched.
pub const P9_OP_FLAG_RETRY: u8 = 1 << 0;
/// All currently defined op-envelope flag bits. Unknown bits fail closed.
pub const P9_OP_KNOWN_FLAGS: u8 = P9_OP_FLAG_RETRY;

/// Maximum negotiated message size and codec frame size.
pub const P9_MAX_MSIZE: u32 = 10 * 1024 * 1024;

pub const P9_CHANNEL_SIZE: usize = 1000;
pub const P9_SIZE_FIELD_LEN: usize = std::mem::size_of::<u32>();
pub const P9_TYPE_FIELD_LEN: usize = std::mem::size_of::<u8>();
pub const P9_TAG_FIELD_LEN: usize = std::mem::size_of::<u16>();
pub const P9_COUNT_FIELD_LEN: usize = std::mem::size_of::<u32>();
/// Header size: size[4] + type[1] + tag[2]
pub const P9_HEADER_SIZE: usize = P9_SIZE_FIELD_LEN + P9_TYPE_FIELD_LEN + P9_TAG_FIELD_LEN;
pub const P9_MIN_MESSAGE_SIZE: u32 = P9_HEADER_SIZE as u32;
/// IO header overhead for Rread/Rreaddir: header + count[4]
pub const P9_IOHDRSZ: u32 = (P9_HEADER_SIZE + P9_COUNT_FIELD_LEN) as u32;
/// Wire overhead of a Twrite request preceding its data payload:
/// header + fid[4] + offset[8] + count[4]. A Twrite carrying `count` bytes is
/// `P9_TWRITE_HDR + count` bytes on the wire and must not exceed the msize.
pub const P9_TWRITE_HDR: u32 = (P9_HEADER_SIZE + 4 + 8 + P9_COUNT_FIELD_LEN) as u32;
/// `Rlopenatread` overhead before data: header + qid[13] + iounit[4] + eof[1]
/// + count[4]. Prefetch limits use this value.
pub const P9_RLOPENATREAD_HDR: u32 = (P9_HEADER_SIZE + 13 + 4 + 1 + P9_COUNT_FIELD_LEN) as u32;
pub const P9_DEBUG_BUFFER_SIZE: usize = 40;
pub const P9_READDIR_BATCH_SIZE: usize = 1000;
pub const P9_MAX_GROUPS: usize = 16;
pub const P9_NOBODY_UID: u32 = 65534;
pub const P9_MAX_NAME_LEN: u32 = 255;

#[derive(Debug, Clone, Copy, DekuRead, DekuWrite)]
#[deku(id_type = "u8")]
pub enum LockStatus {
    #[deku(id = "0")]
    Success,
    #[deku(id = "1")]
    Blocked,
    #[deku(id = "2")]
    LockError,
    #[deku(id = "3")]
    Grace,
}

// Basic structures
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    ctx = "endian: Endian",
    ctx_default = "Endian::Little"
)]
pub struct Qid {
    pub type_: u8,
    pub version: u32,
    pub path: u64,
}

impl Qid {
    /// Serialized size on the wire: type u8 + version u32 + path u64.
    pub const WIRE_SIZE: usize = 1 + 4 + 8;
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
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
    pub data_version: u64,
}

impl Stat {
    /// Serialized size on the wire: qid + three u32s + fifteen u64s.
    pub const WIRE_SIZE: usize = Qid::WIRE_SIZE + 3 * 4 + 15 * 8;
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    ctx = "endian: Endian",
    ctx_default = "Endian::Little"
)]
pub struct P9String {
    #[deku(update = "self.data.len()")]
    pub len: u16,
    #[deku(count = "len")]
    pub data: Vec<u8>,
}

impl P9String {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            len: data.len() as u16,
            data,
        }
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.data)
    }

    /// Serialized size on the wire: u16 length prefix + bytes.
    pub fn wire_size(&self) -> usize {
        2 + self.data.len()
    }
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct DirEntry {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub offset: u64,
    pub type_: u8,
    pub name: P9String,
}

impl DirEntry {
    /// Serialized wire size.
    pub fn wire_size(&self) -> usize {
        Qid::WIRE_SIZE + 8 + 1 + self.name.wire_size()
    }
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tversion {
    #[deku(endian = "little")]
    pub msize: u32,
    pub version: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tattach {
    pub fid: u32,
    pub afid: u32,
    pub uname: P9String,
    pub aname: P9String,
    pub n_uname: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Twalk {
    pub fid: u32,
    pub newfid: u32,
    #[deku(update = "self.wnames.len()")]
    pub nwname: u16,
    #[deku(count = "nwname")]
    pub wnames: Vec<P9String>,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlopen {
    pub fid: u32,
    pub flags: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlcreate {
    pub fid: u32,
    pub name: P9String,
    pub flags: u32,
    pub mode: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tread {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Twrite {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
    pub count: u32,
    #[deku(ctx = "count")]
    pub data: DekuBytes,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tclunk {
    #[deku(endian = "little")]
    pub fid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Treaddir {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tgetattr {
    pub fid: u32,
    pub request_mask: u64,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tsetattr {
    pub fid: u32,
    pub valid: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime_sec: u64,
    pub atime_nsec: u64,
    pub mtime_sec: u64,
    pub mtime_nsec: u64,
}

/// ZeroFS-private atomic fallocate request.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tfallocate {
    pub fid: u32,
    pub offset: u64,
    pub length: u64,
    pub mode: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rfallocate;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tmkdir {
    pub dfid: u32,
    pub name: P9String,
    pub mode: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tsymlink {
    pub dfid: u32,
    pub name: P9String,
    pub symtgt: P9String,
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tmknod {
    pub dfid: u32,
    pub name: P9String,
    pub mode: u32,
    pub major: u32,
    pub minor: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlink {
    pub dfid: u32,
    pub fid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Trename {
    pub fid: u32,
    pub dfid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Trenameat {
    pub olddirfid: u32,
    pub oldname: P9String,
    pub newdirfid: u32,
    pub newname: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tunlinkat {
    pub dirfid: u32,
    pub name: P9String,
    pub flags: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tfsync {
    pub fid: u32,
    pub datasync: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Treadlink {
    #[deku(endian = "little")]
    pub fid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tstatfs {
    #[deku(endian = "little")]
    pub fid: u32,
}

// Core 9P structures
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tflush {
    #[deku(endian = "little")]
    pub oldtag: u16,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rflush;

// Extended attributes
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Txattrwalk {
    pub fid: u32,
    pub newfid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlock {
    pub fid: u32,
    pub lock_type: LockType,
    pub flags: u32,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tgetlock {
    pub fid: u32,
    pub lock_type: LockType,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rxattrwalk {
    #[deku(endian = "little")]
    pub size: u64,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlock {
    pub status: LockStatus,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Rgetlock {
    pub lock_type: LockType,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: P9String,
}

// Response messages
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rversion {
    #[deku(endian = "little")]
    pub msize: u32,
    pub version: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rattach {
    pub qid: Qid,
}

// ZeroFS-private reconnect extension: binds a fresh fid to an existing inode by
// id (not by re-walking a path), so reconnection survives renames/hardlinks.
// Only our own client sends it.
pub const P9_REBIND_REPLAY: u8 = 1 << 0;
pub const P9_REBIND_OPENED: u8 = 1 << 1;
pub const P9_REBIND_KNOWN_FLAGS: u8 = P9_REBIND_REPLAY | P9_REBIND_OPENED;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Trebind {
    pub fid: u32,
    pub inode_id: u64,
    /// Attach-root inode. A root-self request has `inode_id == root_inode`.
    pub root_inode: u64,
    /// Replay flags. `P9_REBIND_OPENED` requires `P9_REBIND_REPLAY` and applies
    /// only to still-linked inodes.
    pub flags: u8,
    /// Original `Tattach` username for `n_uname == -1` credential lookup.
    pub uname: P9String,
    pub n_uname: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rrebind {
    pub qid: Qid,
}

// Private compound requests require the ZeroFS dialect.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Twalkgetattr {
    pub fid: u32,
    pub newfid: u32,
    #[deku(update = "self.wnames.len()")]
    pub nwname: u16,
    #[deku(count = "nwname")]
    pub wnames: Vec<P9String>,
}

// Returned only on a full walk; on any miss the server replies Rlerror.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rwalkgetattr {
    #[deku(endian = "little", update = "self.wqids.len()")]
    pub nwqid: u16,
    #[deku(count = "nwqid")]
    pub wqids: Vec<Qid>,
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Treaddirattr {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct DirEntryPlus {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub offset: u64,
    pub type_: u8,
    pub name: P9String,
    pub stat: Stat,
}

impl DirEntryPlus {
    /// Serialized wire size.
    pub fn wire_size(&self) -> usize {
        Qid::WIRE_SIZE + 8 + 1 + self.name.wire_size() + Stat::WIRE_SIZE
    }
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rreaddirattr {
    #[deku(endian = "little", update = "self.data.len()")]
    pub count: u32,
    #[deku(ctx = "count")]
    pub data: DekuBytes,
}

// Tlopenat preserves `fid` and opens `newfid`. Tlcreateattr preserves `dfid`,
// creates and opens `newfid`, and returns stat. Other *attr replies add stat to
// the corresponding standard request layout.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlopenat {
    pub fid: u32,
    pub newfid: u32,
    pub flags: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlopenat {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub iounit: u32,
}

// Tlopenatread combines Tlopenat with a best-effort Tread at offset zero.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlopenatread {
    pub fid: u32,
    pub newfid: u32,
    pub flags: u32,
    /// Bytes to prefetch from offset 0. The server clamps this to fit msize.
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlopenatread {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub iounit: u32,
    /// One when `data` reaches EOF; zero for an incomplete prefetch.
    pub eof: u8,
    #[deku(endian = "little", update = "self.data.len()")]
    pub count: u32,
    #[deku(ctx = "count")]
    pub data: DekuBytes,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tlcreateattr {
    pub dfid: u32,
    pub newfid: u32,
    pub name: P9String,
    pub flags: u32,
    pub mode: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlcreateattr {
    #[deku(endian = "little")]
    pub iounit: u32,
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rmkdirattr {
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rsymlinkattr {
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rmknodattr {
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlinkattr {
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rsetattrattr {
    pub stat: Stat,
}

impl Rreaddirattr {
    pub fn from_entries(entries: Vec<DirEntryPlus>) -> Result<Self, DekuError> {
        use deku::DekuContainerWrite;
        let mut data = Vec::with_capacity(entries.iter().map(DirEntryPlus::wire_size).sum());
        for entry in entries {
            data.extend_from_slice(&entry.to_bytes()?);
        }
        Ok(Rreaddirattr {
            count: data.len() as u32,
            data: DekuBytes::from(data),
        })
    }

    pub fn to_entries(&self) -> Result<Vec<DirEntryPlus>, DekuError> {
        use deku::DekuContainerRead;
        let mut entries = Vec::new();
        let mut input = (&self.data.0[..], 0);
        while !input.0.is_empty() {
            let (remaining, entry) = DirEntryPlus::from_bytes(input)?;
            input = remaining;
            entries.push(entry);
        }
        Ok(entries)
    }
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rwalk {
    #[deku(endian = "little", update = "self.wqids.len()")]
    pub nwqid: u16,
    #[deku(count = "nwqid")]
    pub wqids: Vec<Qid>,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlopen {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub iounit: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlcreate {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub iounit: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rread {
    #[deku(endian = "little", update = "self.data.len()")]
    pub count: u32,
    #[deku(ctx = "count")]
    pub data: DekuBytes,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rwrite {
    #[deku(endian = "little")]
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rreaddir {
    #[deku(endian = "little", update = "self.data.len()")]
    pub count: u32,
    #[deku(ctx = "count")]
    pub data: DekuBytes,
}

impl Rreaddir {
    pub fn from_entries(entries: Vec<DirEntry>) -> Result<Self, DekuError> {
        use deku::DekuContainerWrite;

        let mut data = Vec::with_capacity(entries.iter().map(DirEntry::wire_size).sum());
        for entry in entries {
            let bytes = entry.to_bytes()?;
            data.extend_from_slice(&bytes);
        }

        Ok(Rreaddir {
            count: data.len() as u32,
            data: DekuBytes::from(data),
        })
    }

    pub fn to_entries(&self) -> Result<Vec<DirEntry>, DekuError> {
        use deku::DekuContainerRead;

        let mut entries = Vec::new();
        let mut input = (&self.data.0[..], 0);
        while !input.0.is_empty() {
            let (remaining, entry) = DirEntry::from_bytes(input)?;
            input = remaining;
            entries.push(entry);
        }

        Ok(entries)
    }
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rgetattr {
    #[deku(endian = "little")]
    pub valid: u64,
    pub stat: Stat,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rmkdir {
    pub qid: Qid,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rsymlink {
    pub qid: Qid,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rmknod {
    pub qid: Qid,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rreadlink {
    pub target: P9String,
}

// Error response
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlerror {
    #[deku(endian = "little")]
    pub ecode: u32,
}

// Empty responses
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rclunk;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rsetattr;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rrename;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlink;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rrenameat;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Runlinkat;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rfsync;

/// Queries the connection's durability lineage and writer epoch.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tgetlineage;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Rgetlineage {
    pub token: u64,
    /// HA writer epoch used as mutation-envelope origin; zero when standalone.
    pub writer_epoch: u64,
}

/// Durability-verified fsync carrying the oldest unsynced lineage token.
/// Token zero denotes no pending write. A lineage mismatch returns `ESTALE`.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Tfsyncdur {
    pub fid: u32,
    pub datasync: u32,
    pub token: u64,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(endian = "little")]
pub struct Rstatfs {
    pub r#type: u32,  // filesystem type
    pub bsize: u32,   // optimal transfer block size
    pub blocks: u64,  // total data blocks in filesystem
    pub bfree: u64,   // free blocks in filesystem
    pub bavail: u64,  // free blocks available to non-superuser
    pub files: u64,   // total file nodes in filesystem
    pub ffree: u64,   // free file nodes in filesystem
    pub fsid: u64,    // filesystem id
    pub namelen: u32, // maximum length of filenames
}

// These IDs define mutation-envelope coverage and must match the enum layout.
pub const T_LCREATE: u8 = 14;
pub const T_SYMLINK: u8 = 16;
pub const T_MKNOD: u8 = 18;
pub const T_RENAME: u8 = 20;
pub const T_SETATTR: u8 = 26;
pub const T_WRITE: u8 = 118;
pub const T_LINK: u8 = 70;
pub const T_MKDIR: u8 = 72;
pub const T_RENAMEAT: u8 = 74;
pub const T_UNLINKAT: u8 = 76;
pub const T_LCREATEATTR: u8 = 238;
pub const T_MKDIRATTR: u8 = 240;
pub const T_SYMLINKATTR: u8 = 242;
pub const T_MKNODATTR: u8 = 244;
pub const T_LINKATTR: u8 = 246;
pub const T_SETATTRATTR: u8 = 248;
// Delayed fallocate retries can reorder with writes and therefore carry op-ids.
pub const T_FALLOCATE: u8 = 228;
pub const R_FALLOCATE: u8 = 229;

// Main message enum
#[derive(Debug, Clone, DekuRead, DekuWrite)]
#[deku(ctx = "_type: u8", id = "_type")]
pub enum Message {
    #[deku(id = "100")]
    Tversion(Tversion),
    #[deku(id = "101")]
    Rversion(Rversion),
    #[deku(id = "104")]
    Tattach(Tattach),
    #[deku(id = "105")]
    Rattach(Rattach),
    #[deku(id = "110")]
    Twalk(Twalk),
    #[deku(id = "111")]
    Rwalk(Rwalk),
    #[deku(id = "12")]
    Tlopen(Tlopen),
    #[deku(id = "13")]
    Rlopen(Rlopen),
    #[deku(id = "T_LCREATE")]
    Tlcreate(Tlcreate),
    #[deku(id = "15")]
    Rlcreate(Rlcreate),
    #[deku(id = "116")]
    Tread(Tread),
    #[deku(id = "117")]
    Rread(Rread),
    #[deku(id = "T_WRITE")]
    Twrite(Twrite),
    #[deku(id = "119")]
    Rwrite(Rwrite),
    #[deku(id = "120")]
    Tclunk(Tclunk),
    #[deku(id = "121")]
    Rclunk(Rclunk),
    #[deku(id = "40")]
    Treaddir(Treaddir),
    #[deku(id = "41")]
    Rreaddir(Rreaddir),
    #[deku(id = "24")]
    Tgetattr(Tgetattr),
    #[deku(id = "25")]
    Rgetattr(Rgetattr),
    #[deku(id = "T_SETATTR")]
    Tsetattr(Tsetattr),
    #[deku(id = "27")]
    Rsetattr(Rsetattr),
    #[deku(id = "T_FALLOCATE")]
    Tfallocate(Tfallocate),
    #[deku(id = "R_FALLOCATE")]
    Rfallocate(Rfallocate),
    #[deku(id = "T_MKDIR")]
    Tmkdir(Tmkdir),
    #[deku(id = "73")]
    Rmkdir(Rmkdir),
    #[deku(id = "T_SYMLINK")]
    Tsymlink(Tsymlink),
    #[deku(id = "17")]
    Rsymlink(Rsymlink),
    #[deku(id = "T_MKNOD")]
    Tmknod(Tmknod),
    #[deku(id = "19")]
    Rmknod(Rmknod),
    #[deku(id = "22")]
    Treadlink(Treadlink),
    #[deku(id = "23")]
    Rreadlink(Rreadlink),
    #[deku(id = "T_LINK")]
    Tlink(Tlink),
    #[deku(id = "71")]
    Rlink(Rlink),
    #[deku(id = "T_RENAME")]
    Trename(Trename),
    #[deku(id = "21")]
    Rrename(Rrename),
    #[deku(id = "T_RENAMEAT")]
    Trenameat(Trenameat),
    #[deku(id = "75")]
    Rrenameat(Rrenameat),
    #[deku(id = "T_UNLINKAT")]
    Tunlinkat(Tunlinkat),
    #[deku(id = "77")]
    Runlinkat(Runlinkat),
    #[deku(id = "50")]
    Tfsync(Tfsync),
    #[deku(id = "51")]
    Rfsync(Rfsync),
    #[deku(id = "232")]
    Tfsyncdur(Tfsyncdur),
    #[deku(id = "233")]
    Tgetlineage(Tgetlineage),
    #[deku(id = "234")]
    Rgetlineage(Rgetlineage),
    #[deku(id = "52")]
    Tlock(Tlock),
    #[deku(id = "53")]
    Rlock(Rlock),
    #[deku(id = "54")]
    Tgetlock(Tgetlock),
    #[deku(id = "55")]
    Rgetlock(Rgetlock),
    #[deku(id = "7")]
    Rlerror(Rlerror),
    #[deku(id = "108")]
    Tflush(Tflush),
    #[deku(id = "109")]
    Rflush(Rflush),
    #[deku(id = "30")]
    Txattrwalk(Txattrwalk),
    #[deku(id = "31")]
    Rxattrwalk(Rxattrwalk),
    #[deku(id = "8")]
    Tstatfs(Tstatfs),
    #[deku(id = "9")]
    Rstatfs(Rstatfs),
    // Private compound extensions use IDs outside the standard 9P range. The
    // *attr requests keep the standard request layout and return a richer reply.
    #[deku(id = "236")]
    Tlopenat(Tlopenat),
    #[deku(id = "237")]
    Rlopenat(Rlopenat),
    #[deku(id = "230")]
    Tlopenatread(Tlopenatread),
    #[deku(id = "231")]
    Rlopenatread(Rlopenatread),
    #[deku(id = "T_LCREATEATTR")]
    Tlcreateattr(Tlcreateattr),
    #[deku(id = "239")]
    Rlcreateattr(Rlcreateattr),
    #[deku(id = "T_MKDIRATTR")]
    Tmkdirattr(Tmkdir),
    #[deku(id = "241")]
    Rmkdirattr(Rmkdirattr),
    #[deku(id = "T_SYMLINKATTR")]
    Tsymlinkattr(Tsymlink),
    #[deku(id = "243")]
    Rsymlinkattr(Rsymlinkattr),
    #[deku(id = "T_MKNODATTR")]
    Tmknodattr(Tmknod),
    #[deku(id = "245")]
    Rmknodattr(Rmknodattr),
    #[deku(id = "T_LINKATTR")]
    Tlinkattr(Tlink),
    #[deku(id = "247")]
    Rlinkattr(Rlinkattr),
    #[deku(id = "T_SETATTRATTR")]
    Tsetattrattr(Tsetattr),
    #[deku(id = "249")]
    Rsetattrattr(Rsetattrattr),
    // ZeroFS-private reconnect extension (ids outside the standard 9P range).
    #[deku(id = "250")]
    Trebind(Trebind),
    #[deku(id = "251")]
    Rrebind(Rrebind),
    #[deku(id = "252")]
    Twalkgetattr(Twalkgetattr),
    #[deku(id = "253")]
    Rwalkgetattr(Rwalkgetattr),
    #[deku(id = "254")]
    Treaddirattr(Treaddirattr),
    #[deku(id = "255")]
    Rreaddirattr(Rreaddirattr),
}

impl Message {
    /// Whether this request requires the private ZeroFS dialect.
    pub fn is_zerofs_private_request(&self) -> bool {
        match self {
            Message::Tfallocate(_)
            | Message::Tfsyncdur(_)
            | Message::Tgetlineage(_)
            | Message::Tlopenat(_)
            | Message::Tlopenatread(_)
            | Message::Tlcreateattr(_)
            | Message::Tmkdirattr(_)
            | Message::Tsymlinkattr(_)
            | Message::Tmknodattr(_)
            | Message::Tlinkattr(_)
            | Message::Tsetattrattr(_)
            | Message::Trebind(_)
            | Message::Twalkgetattr(_)
            | Message::Treaddirattr(_) => true,
            Message::Tversion(_)
            | Message::Rversion(_)
            | Message::Tattach(_)
            | Message::Rattach(_)
            | Message::Twalk(_)
            | Message::Rwalk(_)
            | Message::Tlopen(_)
            | Message::Rlopen(_)
            | Message::Tlcreate(_)
            | Message::Rlcreate(_)
            | Message::Tread(_)
            | Message::Rread(_)
            | Message::Twrite(_)
            | Message::Rwrite(_)
            | Message::Tclunk(_)
            | Message::Rclunk(_)
            | Message::Treaddir(_)
            | Message::Rreaddir(_)
            | Message::Tgetattr(_)
            | Message::Rgetattr(_)
            | Message::Tsetattr(_)
            | Message::Rsetattr(_)
            | Message::Rfallocate(_)
            | Message::Tmkdir(_)
            | Message::Rmkdir(_)
            | Message::Tsymlink(_)
            | Message::Rsymlink(_)
            | Message::Tmknod(_)
            | Message::Rmknod(_)
            | Message::Treadlink(_)
            | Message::Rreadlink(_)
            | Message::Tlink(_)
            | Message::Rlink(_)
            | Message::Trename(_)
            | Message::Rrename(_)
            | Message::Trenameat(_)
            | Message::Rrenameat(_)
            | Message::Tunlinkat(_)
            | Message::Runlinkat(_)
            | Message::Tfsync(_)
            | Message::Rfsync(_)
            | Message::Rgetlineage(_)
            | Message::Tlock(_)
            | Message::Rlock(_)
            | Message::Tgetlock(_)
            | Message::Rgetlock(_)
            | Message::Rlerror(_)
            | Message::Tflush(_)
            | Message::Rflush(_)
            | Message::Txattrwalk(_)
            | Message::Rxattrwalk(_)
            | Message::Tstatfs(_)
            | Message::Rstatfs(_)
            | Message::Rlopenat(_)
            | Message::Rlopenatread(_)
            | Message::Rlcreateattr(_)
            | Message::Rmkdirattr(_)
            | Message::Rsymlinkattr(_)
            | Message::Rmknodattr(_)
            | Message::Rlinkattr(_)
            | Message::Rsetattrattr(_)
            | Message::Rrebind(_)
            | Message::Rwalkgetattr(_)
            | Message::Rreaddirattr(_) => false,
        }
    }

    /// Fids referenced or allocated by this request. The exhaustive match requires
    /// each new message to declare its fid footprint.
    pub fn request_fids(&self) -> impl Iterator<Item = u32> {
        let fids = match self {
            Message::Tattach(m) => [Some(m.fid), (m.afid != NOFID).then_some(m.afid)],
            Message::Twalk(m) => [Some(m.fid), Some(m.newfid)],
            Message::Tlopen(m) => [Some(m.fid), None],
            Message::Tlcreate(m) => [Some(m.fid), None],
            Message::Tread(m) => [Some(m.fid), None],
            Message::Twrite(m) => [Some(m.fid), None],
            Message::Tclunk(m) => [Some(m.fid), None],
            Message::Treaddir(m) => [Some(m.fid), None],
            Message::Tgetattr(m) => [Some(m.fid), None],
            Message::Tsetattr(m) | Message::Tsetattrattr(m) => [Some(m.fid), None],
            Message::Tfallocate(m) => [Some(m.fid), None],
            Message::Tmkdir(m) | Message::Tmkdirattr(m) => [Some(m.dfid), None],
            Message::Tsymlink(m) | Message::Tsymlinkattr(m) => [Some(m.dfid), None],
            Message::Tmknod(m) | Message::Tmknodattr(m) => [Some(m.dfid), None],
            Message::Treadlink(m) => [Some(m.fid), None],
            Message::Tlink(m) | Message::Tlinkattr(m) => [Some(m.dfid), Some(m.fid)],
            Message::Trename(m) => [Some(m.fid), Some(m.dfid)],
            Message::Trenameat(m) => [Some(m.olddirfid), Some(m.newdirfid)],
            Message::Tunlinkat(m) => [Some(m.dirfid), None],
            Message::Tfsync(m) => [Some(m.fid), None],
            Message::Tfsyncdur(m) => [Some(m.fid), None],
            Message::Tlock(m) => [Some(m.fid), None],
            Message::Tgetlock(m) => [Some(m.fid), None],
            Message::Txattrwalk(m) => [Some(m.fid), Some(m.newfid)],
            Message::Tstatfs(m) => [Some(m.fid), None],
            Message::Tlopenat(m) => [Some(m.fid), Some(m.newfid)],
            Message::Tlopenatread(m) => [Some(m.fid), Some(m.newfid)],
            Message::Tlcreateattr(m) => [Some(m.dfid), Some(m.newfid)],
            Message::Trebind(m) => [Some(m.fid), None],
            Message::Twalkgetattr(m) => [Some(m.fid), Some(m.newfid)],
            Message::Treaddirattr(m) => [Some(m.fid), None],
            Message::Tversion(_)
            | Message::Rversion(_)
            | Message::Rattach(_)
            | Message::Rwalk(_)
            | Message::Rlopen(_)
            | Message::Rlcreate(_)
            | Message::Rread(_)
            | Message::Rwrite(_)
            | Message::Rclunk(_)
            | Message::Rreaddir(_)
            | Message::Rgetattr(_)
            | Message::Rsetattr(_)
            | Message::Rfallocate(_)
            | Message::Rmkdir(_)
            | Message::Rsymlink(_)
            | Message::Rmknod(_)
            | Message::Rreadlink(_)
            | Message::Rlink(_)
            | Message::Rrename(_)
            | Message::Rrenameat(_)
            | Message::Runlinkat(_)
            | Message::Rfsync(_)
            | Message::Tgetlineage(_)
            | Message::Rgetlineage(_)
            | Message::Rlock(_)
            | Message::Rgetlock(_)
            | Message::Rlerror(_)
            | Message::Tflush(_)
            | Message::Rflush(_)
            | Message::Rxattrwalk(_)
            | Message::Rstatfs(_)
            | Message::Rlopenat(_)
            | Message::Rlopenatread(_)
            | Message::Rlcreateattr(_)
            | Message::Rmkdirattr(_)
            | Message::Rsymlinkattr(_)
            | Message::Rmknodattr(_)
            | Message::Rlinkattr(_)
            | Message::Rsetattrattr(_)
            | Message::Rrebind(_)
            | Message::Rwalkgetattr(_)
            | Message::Rreaddirattr(_) => [None, None],
        };
        fids.into_iter().flatten()
    }

    /// Whether this request creates an obligation for durability-verified fsync.
    pub fn is_mutation(&self) -> bool {
        self.durability_fid().is_some()
    }

    /// Fid owning the mutation's primary durability obligation.
    pub fn durability_fid(&self) -> Option<u32> {
        match self {
            Message::Twrite(m) => Some(m.fid),
            Message::Tsetattr(m) => Some(m.fid),
            Message::Tsetattrattr(m) => Some(m.fid),
            Message::Tfallocate(m) => Some(m.fid),
            Message::Tlcreate(m) => Some(m.fid),
            Message::Tlcreateattr(m) => Some(m.dfid),
            Message::Tmkdir(m) => Some(m.dfid),
            Message::Tmkdirattr(m) => Some(m.dfid),
            Message::Tsymlink(m) => Some(m.dfid),
            Message::Tsymlinkattr(m) => Some(m.dfid),
            Message::Tmknod(m) => Some(m.dfid),
            Message::Tmknodattr(m) => Some(m.dfid),
            Message::Tlink(m) => Some(m.dfid),
            Message::Tlinkattr(m) => Some(m.dfid),
            Message::Trename(m) => Some(m.dfid),
            Message::Trenameat(m) => Some(m.newdirfid),
            Message::Tunlinkat(m) => Some(m.dirfid),
            _ => None,
        }
    }

    /// Fids changed by the mutation. `Trenameat` includes both directories.
    pub fn durability_fids(&self) -> impl Iterator<Item = u32> {
        let extra = match self {
            Message::Trenameat(m) => Some(m.olddirfid),
            _ => None,
        };
        self.durability_fid().into_iter().chain(extra)
    }
}

/// Byte offset of the `type` field within a 9P frame (after the u32 size).
pub const P9_TYPE_OFFSET: usize = 4;
/// Length of the standard 9P frame header: size(u32) + type(u8) + tag(u16).
pub const P9_FRAME_HEADER_LEN: usize = 7;
/// Length of the idempotency op-id in the ZeroFS request envelope.
pub const P9_OP_ID_LEN: usize = 16;
/// Length of the attempt flags in the ZeroFS request envelope.
pub const P9_OP_FLAGS_LEN: usize = 1;
/// Length of the originating writer epoch in the ZeroFS request envelope.
pub const P9_OP_ORIGIN_EPOCH_LEN: usize = 8;
/// Total bytes spliced after the tag for a mutation in the ZeroFS dialect.
pub const P9_OP_ENVELOPE_LEN: usize = P9_OP_ID_LEN + P9_OP_FLAGS_LEN + P9_OP_ORIGIN_EPOCH_LEN;

// Deku retains standard framing; private mutation envelopes are spliced after tag.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct P9Message {
    #[deku(endian = "little")]
    pub size: u32,
    pub type_: u8,
    #[deku(endian = "little")]
    pub tag: u16,
    #[deku(skip, default = "[0u8; 16]")]
    pub op_id: [u8; 16],
    #[deku(skip, default = "0")]
    pub op_flags: u8,
    #[deku(skip, default = "0")]
    pub op_origin_epoch: u64,
    #[deku(ctx = "*type_")]
    pub body: Message,
}

impl P9Message {
    pub fn to_bytes(&self) -> Result<Vec<u8>, DekuError> {
        self.to_bytes_ctx(false)
    }

    /// Encodes covered mutations with a private envelope after the tag.
    pub fn to_bytes_ctx(&self, op_id_enabled: bool) -> Result<Vec<u8>, DekuError> {
        // Deku produces the standard frame; op fields are skipped.
        let mut bytes = DekuContainerWrite::to_bytes(self)?;
        if op_id_enabled && Self::carries_op_id(self.type_) {
            bytes.splice(
                P9_FRAME_HEADER_LEN..P9_FRAME_HEADER_LEN,
                self.op_id
                    .iter()
                    .copied()
                    .chain(std::iter::once(self.op_flags))
                    .chain(self.op_origin_epoch.to_le_bytes()),
            );
        }
        let size = bytes.len() as u32;
        bytes[0..4].copy_from_slice(&size.to_le_bytes());
        Ok(bytes)
    }

    /// Decodes a standard frame with an optional private mutation envelope.
    pub fn from_bytes_ctx(input: &[u8], op_id_enabled: bool) -> Result<P9Message, DekuError> {
        if op_id_enabled
            && input.len() > P9_FRAME_HEADER_LEN
            && Self::carries_op_id(input[P9_TYPE_OFFSET])
        {
            if input.len() < P9_FRAME_HEADER_LEN + P9_OP_ENVELOPE_LEN {
                return Err(DekuError::Parse(
                    "frame too short to contain an op envelope".into(),
                ));
            }
            let mut op_id = [0u8; 16];
            op_id.copy_from_slice(&input[P9_FRAME_HEADER_LEN..P9_FRAME_HEADER_LEN + P9_OP_ID_LEN]);
            let op_flags = input[P9_FRAME_HEADER_LEN + P9_OP_ID_LEN];
            let origin_start = P9_FRAME_HEADER_LEN + P9_OP_ID_LEN + P9_OP_FLAGS_LEN;
            let op_origin_epoch = u64::from_le_bytes(
                input[origin_start..origin_start + P9_OP_ORIGIN_EPOCH_LEN]
                    .try_into()
                    .expect("the envelope length was checked"),
            );
            // Deku parses the body from standard framing.
            let mut standard = Vec::with_capacity(input.len() - P9_OP_ENVELOPE_LEN);
            standard.extend_from_slice(&input[..P9_FRAME_HEADER_LEN]);
            standard.extend_from_slice(&input[P9_FRAME_HEADER_LEN + P9_OP_ENVELOPE_LEN..]);
            let (_, mut msg) = P9Message::from_bytes((&standard, 0))?;
            msg.op_id = op_id;
            msg.op_flags = op_flags;
            msg.op_origin_epoch = op_origin_epoch;
            Ok(msg)
        } else {
            let (_, msg) = P9Message::from_bytes((input, 0))?;
            Ok(msg)
        }
    }

    /// Whether this request type carries the private mutation envelope.
    pub fn carries_op_id(type_: u8) -> bool {
        matches!(
            type_,
            // base 9P2000.L mutations
            T_LCREATE | T_SYMLINK | T_MKNOD | T_RENAME | T_SETATTR | T_WRITE | T_LINK | T_MKDIR | T_RENAMEAT | T_UNLINKAT | T_FALLOCATE
            // compound mutation forms
            | T_LCREATEATTR | T_MKDIRATTR | T_SYMLINKATTR | T_MKNODATTR | T_LINKATTR
            // setattr compound form
            | T_SETATTRATTR
        )
    }

    pub fn new(tag: u16, body: Message) -> Self {
        let type_ = body
            .deku_id()
            .expect("every Message variant has a fixed 9P type id");

        Self {
            size: 0,
            type_,
            tag,
            op_id: [0u8; 16],
            op_flags: 0,
            op_origin_epoch: 0,
            body,
        }
    }

    /// Construct a request carrying an idempotency op-id (encode with `to_bytes_ctx(true)`).
    pub fn new_with_op_id(tag: u16, op_id: [u8; 16], body: Message) -> Self {
        Self::new_with_op_id_flags_and_origin(tag, op_id, 0, 0, body)
    }

    /// Constructs an op-id request with flags and origin writer epoch.
    pub fn new_with_op_id_flags_and_origin(
        tag: u16,
        op_id: [u8; 16],
        op_flags: u8,
        op_origin_epoch: u64,
        body: Message,
    ) -> Self {
        let mut msg = Self::new(tag, body);
        msg.op_id = op_id;
        msg.op_flags = op_flags;
        msg.op_origin_epoch = op_origin_epoch;
        msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deku::DekuContainerWrite;

    fn tmkdir() -> Message {
        Message::Tmkdir(Tmkdir {
            dfid: 1,
            name: P9String::new(b"d".to_vec()),
            mode: 0o755,
            gid: 0,
        })
    }

    #[test]
    fn durability_fids_yields_both_directories_for_a_renameat() {
        // A renameat changes both directories (source removal, dest add) in one atomic
        // op, so a verified fsync of either must account for it: durability_fids yields
        // both dir fids, while durability_fid (the primary) is only the dest.
        let m = Message::Trenameat(Trenameat {
            olddirfid: 11,
            oldname: P9String::new(b"a".to_vec()),
            newdirfid: 22,
            newname: P9String::new(b"b".to_vec()),
        });
        assert_eq!(
            m.durability_fid(),
            Some(22),
            "the primary fid is the dest dir"
        );
        let mut fids: Vec<u32> = m.durability_fids().collect();
        fids.sort_unstable();
        assert_eq!(fids, vec![11, 22], "both source and dest dirs are covered");
    }

    #[test]
    fn durability_fids_is_the_single_fid_for_non_rename_ops() {
        let fids: Vec<u32> = tmkdir().durability_fids().collect();
        assert_eq!(
            fids,
            vec![1],
            "a single-directory op yields just its own fid"
        );
    }

    #[test]
    fn request_fids_omits_the_nofid_attach_sentinel() {
        let attach = |afid| {
            Message::Tattach(Tattach {
                fid: 7,
                afid,
                uname: P9String::new(Vec::new()),
                aname: P9String::new(Vec::new()),
                n_uname: 0,
            })
        };

        assert_eq!(attach(NOFID).request_fids().collect::<Vec<_>>(), vec![7]);
        assert_eq!(attach(8).request_fids().collect::<Vec<_>>(), vec![7, 8]);
    }

    #[test]
    fn request_fids_includes_both_rename_directories() {
        let request = Message::Trenameat(Trenameat {
            olddirfid: 11,
            oldname: P9String::new(b"old".to_vec()),
            newdirfid: 22,
            newname: P9String::new(b"new".to_vec()),
        });

        assert_eq!(request.request_fids().collect::<Vec<_>>(), vec![11, 22]);
    }

    #[test]
    fn request_fids_is_empty_for_fidless_messages() {
        assert!(
            Message::Tflush(Tflush { oldtag: 1 })
                .request_fids()
                .next()
                .is_none()
        );
        assert!(Message::Rflush(Rflush).request_fids().next().is_none());
    }

    #[test]
    fn fallocate_round_trips_and_tracks_its_file_fid() {
        assert_eq!(classify_fallocate_mode(0), Some(FallocateKind::Allocate));
        assert_eq!(
            classify_fallocate_mode(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE),
            Some(FallocateKind::PunchHole)
        );
        assert_eq!(
            classify_fallocate_mode(FALLOC_FL_ZERO_RANGE),
            Some(FallocateKind::ZeroRange { keep_size: false })
        );
        assert_eq!(
            classify_fallocate_mode(FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE),
            Some(FallocateKind::ZeroRange { keep_size: true })
        );
        assert_eq!(classify_fallocate_mode(FALLOC_FL_KEEP_SIZE), None);

        let msg = P9Message::new(
            9,
            Message::Tfallocate(Tfallocate {
                fid: 42,
                offset: 100,
                length: 200,
                mode: FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            }),
        );
        let bytes = msg.to_bytes().unwrap();
        let (_, decoded) = P9Message::from_bytes((&bytes, 0)).unwrap();
        assert_eq!(decoded.type_, T_FALLOCATE);
        assert_eq!(decoded.body.durability_fid(), Some(42));
        match decoded.body {
            Message::Tfallocate(tf) => {
                assert_eq!(tf.offset, 100);
                assert_eq!(tf.length, 200);
                assert_eq!(tf.mode, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE);
            }
            _ => panic!("expected Tfallocate"),
        }
    }

    #[test]
    fn op_id_round_trips_on_a_zerofs_request() {
        let op_id = [7u8; 16];
        let msg =
            P9Message::new_with_op_id_flags_and_origin(5, op_id, P9_OP_FLAG_RETRY, 42, tmkdir());
        let bytes = msg.to_bytes_ctx(true).unwrap();
        let decoded = P9Message::from_bytes_ctx(&bytes, true).unwrap();
        assert_eq!(
            decoded.op_id, op_id,
            "the op-id must round-trip in the ZeroFS dialect"
        );
        assert_eq!(decoded.op_flags, P9_OP_FLAG_RETRY);
        assert_eq!(decoded.op_origin_epoch, 42);
        assert_eq!(decoded.tag, 5);
        assert!(matches!(decoded.body, Message::Tmkdir(_)));
    }

    #[test]
    fn op_id_write_payload_at_advertised_boundary_fits_msize() {
        let msize = 4096u32;
        let count = msize - P9_TWRITE_HDR - P9_OP_ENVELOPE_LEN as u32;
        let msg = P9Message::new_with_op_id(
            5,
            [7u8; 16],
            Message::Twrite(Twrite {
                fid: 9,
                offset: 0,
                count,
                data: vec![0u8; count as usize].into(),
            }),
        );
        let bytes = msg.to_bytes_ctx(true).unwrap();
        assert_eq!(bytes.len(), msize as usize);
        let decoded = P9Message::from_bytes_ctx(&bytes, true).unwrap();
        assert_eq!(decoded.op_id, [7u8; 16]);
        assert_eq!(decoded.op_flags, 0);
        assert_eq!(decoded.op_origin_epoch, 0);
        assert!(matches!(decoded.body, Message::Twrite(_)));
    }

    #[test]
    fn op_id_is_absent_in_standard_framing() {
        let op_id = [7u8; 16];
        let msg = P9Message::new_with_op_id(5, op_id, tmkdir());
        let with = msg.to_bytes_ctx(true).unwrap();
        let without = msg.to_bytes_ctx(false).unwrap();
        assert_eq!(
            with.len(),
            without.len() + P9_OP_ENVELOPE_LEN,
            "the op envelope is present only in the ZeroFS dialect"
        );
        let decoded = P9Message::from_bytes_ctx(&without, false).unwrap();
        assert_eq!(
            decoded.op_id, [0u8; 16],
            "standard framing carries no op-id"
        );
        assert_eq!(decoded.op_flags, 0);
        let (_, plain) = P9Message::from_bytes((&without, 0)).unwrap();
        assert_eq!(plain.tag, 5);
    }

    #[test]
    fn idempotent_requests_carry_no_op_id() {
        let msg = P9Message::new_with_op_id(5, [7u8; 16], Message::Tclunk(Tclunk { fid: 9 }));
        let with = msg.to_bytes_ctx(true).unwrap();
        let without = msg.to_bytes_ctx(false).unwrap();
        assert_eq!(with, without, "an uncovered op must not carry an op-id");
    }

    #[test]
    fn rebind_round_trips_explicit_root_inode() {
        let msg = P9Message::new(
            17,
            Message::Trebind(Trebind {
                fid: 9,
                inode_id: 42,
                root_inode: 7,
                flags: P9_REBIND_REPLAY | P9_REBIND_OPENED,
                uname: P9String::new(b"root".to_vec()),
                n_uname: 1000,
            }),
        );
        let bytes = msg.to_bytes().unwrap();
        let (_, decoded) = P9Message::from_bytes((&bytes, 0)).unwrap();
        match decoded.body {
            Message::Trebind(rebind) => {
                assert_eq!(rebind.fid, 9);
                assert_eq!(rebind.inode_id, 42);
                assert_eq!(rebind.root_inode, 7);
                assert_eq!(rebind.flags, P9_REBIND_REPLAY | P9_REBIND_OPENED);
                assert_eq!(rebind.uname.as_str().unwrap(), "root");
                assert_eq!(rebind.n_uname, 1000);
            }
            _ => panic!("expected Trebind"),
        }
    }

    #[test]
    fn truncated_large_payload_is_rejected() {
        let frame = P9Message::new(
            7,
            Message::Twrite(Twrite {
                fid: 1,
                offset: 0,
                count: u32::MAX,
                data: vec![0xde, 0xad, 0xbe, 0xef].into(),
            }),
        )
        .to_bytes()
        .unwrap();
        assert!(frame.len() < 64);
        assert!(P9Message::from_bytes_ctx(&frame, false).is_err());
    }

    fn qid() -> Qid {
        Qid {
            type_: 0x80,
            version: 7,
            path: 42,
        }
    }

    fn stat() -> Stat {
        Stat {
            qid: qid(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
            nlink: 2,
            rdev: 0,
            size: 4096,
            blksize: 32768,
            blocks: 8,
            atime_sec: 1,
            atime_nsec: 2,
            mtime_sec: 3,
            mtime_nsec: 4,
            ctime_sec: 5,
            ctime_nsec: 6,
            btime_sec: 7,
            btime_nsec: 8,
            r#gen: 9,
            data_version: 10,
        }
    }

    #[test]
    fn wire_size_matches_serialization() {
        for name in [&b""[..], b"a", "h\u{e9}llo-\u{4e16}\u{754c}.txt".as_bytes()] {
            let entry = DirEntry {
                qid: qid(),
                offset: 99,
                type_: 4,
                name: P9String::new(name.to_vec()),
            };
            assert_eq!(entry.wire_size(), entry.to_bytes().unwrap().len());

            let plus = DirEntryPlus {
                qid: qid(),
                offset: 99,
                type_: 4,
                name: P9String::new(name.to_vec()),
                stat: stat(),
            };
            assert_eq!(plus.wire_size(), plus.to_bytes().unwrap().len());
        }
    }
}
