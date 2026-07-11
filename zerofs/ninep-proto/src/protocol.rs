use crate::deku_bytes::DekuBytes;
use deku::prelude::*;

pub const VERSION_9P2000L: &[u8] = b"9P2000.L";
/// `.zerofs` fast paths (Twalkgetattr/Treaddirattr). A stock v9fs client never
/// proposes this, so the server falls back to plain `9P2000.L`.
pub const VERSION_9P2000L_ZEROFS: &[u8] = b"9P2000.L.zerofs";
/// `.zerofs2`: adds the compound create/open messages and the stat-carrying
/// mkdir/symlink/mknod/link/setattr replies. Substring matching lets an old
/// server reply `.zerofs` so a new client degrades instead of sending unparseable
/// messages.
pub const VERSION_9P2000L_ZEROFS2: &[u8] = b"9P2000.L.zerofs2";
/// `.zerofs3`: adds a per-request idempotency op-id in the frame (after the tag)
/// for safe retry across a reconnect/failover. Gated by negotiation, so an older
/// peer never sees the extra field and standard framing is unchanged for it.
pub const VERSION_9P2000L_ZEROFS3: &[u8] = b"9P2000.L.zerofs3";
/// `.zerofs4`: durability-verified fsync. The client learns its connection's
/// durability lineage token via Tgetlineage and presents it on Tfsyncdur; the
/// server flushes and returns Rfsync only if that token is still the live
/// durable lineage, else Rlerror(ESTALE). So a successful fsync implies every
/// write the client acked before it is durable (survives a crash/failover),
/// unless the administrator explicitly configured `ignore_fsync`.
pub const VERSION_9P2000L_ZEROFS4: &[u8] = b"9P2000.L.zerofs4";
/// `.zerofs5`: folds a file open and its first read into one round trip.
/// Tlopenatread opens `fid`'s inode on a fresh `newfid` (exactly like Tlopenat)
/// and also returns up to `count` bytes from offset 0, so reading a file that
/// fits in the prefetch window needs no follow-up Tread. The inline read is
/// best-effort and never affects the open: on a read error the reply carries the
/// open result with empty data, and the client issues a normal Tread. Implies
/// `.zerofs4` because the levels are cumulative.
pub const VERSION_9P2000L_ZEROFS5: &[u8] = b"9P2000.L.zerofs5";
/// `.zerofs6`: adds the atomic ZeroFS-private Tfallocate message.
pub const VERSION_9P2000L_ZEROFS6: &[u8] = b"9P2000.L.zerofs6";

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
#[deku(id_type = "u8")]
pub enum LockType {
    #[deku(id = "0")]
    ReadLock, // F_RDLCK
    #[deku(id = "1")]
    WriteLock, // F_WRLCK
    #[deku(id = "2")]
    Unlock, // F_UNLCK
}

pub const P9_LOCK_FLAGS_BLOCK: u32 = 1; // blocking request

/// `Rlerror` ecode an HA node returns when it is no longer the serving leader
/// (lease lapsed or fenced). A distinct signal, not generic `EIO`, so a
/// failover-aware client re-probes and resends rather than failing. The value is
/// `ESHUTDOWN`, which no filesystem op otherwise produces and degrades sensibly
/// for clients that don't special-case it.
pub const P9_ENOTLEADER: u32 = 108;

/// Maximum 9P message size. This bounds the negotiated msize: the server clamps
/// every client's requested msize to this value, and both the server and client
/// reader codecs reject frames larger than it. Shared by the 9P server and the
/// `zerofs mount` client.
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
/// Wire overhead of an Rlopenatread reply preceding its data payload:
/// header + qid[13] + iounit[4] + eof[1] + count[4]. This is 18 bytes larger than
/// `P9_IOHDRSZ` (the extra qid+iounit+eof), so the `.zerofs5` open+read fold must
/// clamp its prefetch with this, not `P9_IOHDRSZ`, to respect a small msize.
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
pub struct Qid {
    pub type_: u8,
    #[deku(endian = "little")]
    pub version: u32,
    #[deku(endian = "little")]
    pub path: u64,
}

impl Qid {
    /// Serialized size on the wire: type u8 + version u32 + path u64.
    pub const WIRE_SIZE: usize = 1 + 4 + 8;
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Stat {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub mode: u32,
    #[deku(endian = "little")]
    pub uid: u32,
    #[deku(endian = "little")]
    pub gid: u32,
    #[deku(endian = "little")]
    pub nlink: u64,
    #[deku(endian = "little")]
    pub rdev: u64,
    #[deku(endian = "little")]
    pub size: u64,
    #[deku(endian = "little")]
    pub blksize: u64,
    #[deku(endian = "little")]
    pub blocks: u64,
    #[deku(endian = "little")]
    pub atime_sec: u64,
    #[deku(endian = "little")]
    pub atime_nsec: u64,
    #[deku(endian = "little")]
    pub mtime_sec: u64,
    #[deku(endian = "little")]
    pub mtime_nsec: u64,
    #[deku(endian = "little")]
    pub ctime_sec: u64,
    #[deku(endian = "little")]
    pub ctime_nsec: u64,
    #[deku(endian = "little")]
    pub btime_sec: u64,
    #[deku(endian = "little")]
    pub btime_nsec: u64,
    #[deku(endian = "little")]
    pub r#gen: u64,
    #[deku(endian = "little")]
    pub data_version: u64,
}

impl Stat {
    /// Serialized size on the wire: qid + three u32s + fifteen u64s.
    pub const WIRE_SIZE: usize = Qid::WIRE_SIZE + 3 * 4 + 15 * 8;
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct P9String {
    #[deku(endian = "little", update = "self.data.len()")]
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
    /// Serialized size on the wire, without paying for an encode pass.
    /// Pinned to the deku layout by `wire_size_matches_serialization`.
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
pub struct Tattach {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub afid: u32,
    pub uname: P9String,
    pub aname: P9String,
    #[deku(endian = "little")]
    pub n_uname: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Twalk {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub newfid: u32,
    #[deku(endian = "little", update = "self.wnames.len()")]
    pub nwname: u16,
    #[deku(count = "nwname")]
    pub wnames: Vec<P9String>,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlopen {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub flags: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlcreate {
    #[deku(endian = "little")]
    pub fid: u32,
    pub name: P9String,
    #[deku(endian = "little")]
    pub flags: u32,
    #[deku(endian = "little")]
    pub mode: u32,
    #[deku(endian = "little")]
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tread {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
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
pub struct Treaddir {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tgetattr {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub request_mask: u64,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tsetattr {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub valid: u32,
    #[deku(endian = "little")]
    pub mode: u32,
    #[deku(endian = "little")]
    pub uid: u32,
    #[deku(endian = "little")]
    pub gid: u32,
    #[deku(endian = "little")]
    pub size: u64,
    #[deku(endian = "little")]
    pub atime_sec: u64,
    #[deku(endian = "little")]
    pub atime_nsec: u64,
    #[deku(endian = "little")]
    pub mtime_sec: u64,
    #[deku(endian = "little")]
    pub mtime_nsec: u64,
}

/// ZeroFS-private fallocate request, negotiated via `.zerofs6`.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tfallocate {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
    pub length: u64,
    #[deku(endian = "little")]
    pub mode: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rfallocate;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tmkdir {
    #[deku(endian = "little")]
    pub dfid: u32,
    pub name: P9String,
    #[deku(endian = "little")]
    pub mode: u32,
    #[deku(endian = "little")]
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tsymlink {
    #[deku(endian = "little")]
    pub dfid: u32,
    pub name: P9String,
    pub symtgt: P9String,
    #[deku(endian = "little")]
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tmknod {
    #[deku(endian = "little")]
    pub dfid: u32,
    pub name: P9String,
    #[deku(endian = "little")]
    pub mode: u32,
    #[deku(endian = "little")]
    pub major: u32,
    #[deku(endian = "little")]
    pub minor: u32,
    #[deku(endian = "little")]
    pub gid: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlink {
    #[deku(endian = "little")]
    pub dfid: u32,
    #[deku(endian = "little")]
    pub fid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Trename {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub dfid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Trenameat {
    #[deku(endian = "little")]
    pub olddirfid: u32,
    pub oldname: P9String,
    #[deku(endian = "little")]
    pub newdirfid: u32,
    pub newname: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tunlinkat {
    #[deku(endian = "little")]
    pub dirfid: u32,
    pub name: P9String,
    #[deku(endian = "little")]
    pub flags: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tfsync {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
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
pub struct Txattrwalk {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub newfid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlock {
    #[deku(endian = "little")]
    pub fid: u32,
    pub lock_type: LockType,
    #[deku(endian = "little")]
    pub flags: u32,
    #[deku(endian = "little")]
    pub start: u64,
    #[deku(endian = "little")]
    pub length: u64,
    #[deku(endian = "little")]
    pub proc_id: u32,
    pub client_id: P9String,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tgetlock {
    #[deku(endian = "little")]
    pub fid: u32,
    pub lock_type: LockType,
    #[deku(endian = "little")]
    pub start: u64,
    #[deku(endian = "little")]
    pub length: u64,
    #[deku(endian = "little")]
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
pub struct Rgetlock {
    pub lock_type: LockType,
    #[deku(endian = "little")]
    pub start: u64,
    #[deku(endian = "little")]
    pub length: u64,
    #[deku(endian = "little")]
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
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Trebind {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub inode_id: u64,
    #[deku(endian = "little")]
    pub n_uname: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rrebind {
    pub qid: Qid,
}

// ZeroFS-private fast-path extensions, negotiated via the "9P2000.L.zerofs"
// version. Twalkgetattr folds walk+getattr into one round trip; Treaddirattr
// returns each directory entry together with its full stat (readdirplus).
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Twalkgetattr {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub newfid: u32,
    #[deku(endian = "little", update = "self.wnames.len()")]
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
pub struct Treaddirattr {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub offset: u64,
    #[deku(endian = "little")]
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
    /// Serialized size on the wire, without paying for an encode pass.
    /// Pinned to the deku layout by `wire_size_matches_serialization`.
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

// ZeroFS-private compound messages, negotiated via "9P2000.L.zerofs2". Each
// folds a fixed client-side sequence into one round trip:
//
//   Tlopenat     = Twalk(clone) + Tlopen — opens `fid`'s inode on a fresh
//                  `newfid`, leaving `fid` untouched.
//   Tlcreateattr = Twalk(clone) + Tlcreate + Tgetattr — creates and opens the
//                  child on `newfid` (the directory fid is NOT mutated, unlike
//                  Tlcreate) and returns the child's full stat.
//
// The remaining *attr variants reuse the standard request layout but reply
// with the post-operation stat, which the server computes anyway and the
// standard replies throw away (forcing the client into a follow-up getattr).
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlopenat {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub newfid: u32,
    #[deku(endian = "little")]
    pub flags: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlopenat {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub iounit: u32,
}

// Tlopenatread = Tlopenat + Tread(offset 0, count), negotiated via ".zerofs5".
// Opens `fid`'s inode on `newfid` and prefetches the start of the file in the
// same round trip. The read is best-effort: the server returns whatever it could
// read (possibly nothing) without failing the open.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlopenatread {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub newfid: u32,
    #[deku(endian = "little")]
    pub flags: u32,
    /// Bytes to prefetch from offset 0. The server clamps this to fit msize.
    #[deku(endian = "little")]
    pub count: u32,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rlopenatread {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub iounit: u32,
    /// 1 when `data` reaches end of file (the whole file fit in `count`), so the
    /// client may serve the file's reads entirely from `data`; 0 otherwise, when
    /// the client discards the partial prefetch and reads normally.
    pub eof: u8,
    #[deku(endian = "little", update = "self.data.len()")]
    pub count: u32,
    #[deku(ctx = "count")]
    pub data: DekuBytes,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tlcreateattr {
    #[deku(endian = "little")]
    pub dfid: u32,
    #[deku(endian = "little")]
    pub newfid: u32,
    pub name: P9String,
    #[deku(endian = "little")]
    pub flags: u32,
    #[deku(endian = "little")]
    pub mode: u32,
    #[deku(endian = "little")]
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
        let mut offset = 0;
        while offset < self.data.len() {
            let remaining = &self.data.0[offset..];
            let (_, entry) = DirEntryPlus::from_bytes((remaining, 0))?;
            offset += entry.to_bytes()?.len();
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
        let mut offset = 0;

        while offset < self.data.len() {
            let remaining = &self.data.0[offset..];
            let (_, entry) = DirEntry::from_bytes((remaining, 0))?;

            // Calculate how many bytes were consumed
            let entry_bytes = entry.to_bytes()?;
            offset += entry_bytes.len();

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

/// `.zerofs4`: query the connection's current durability lineage token. Sent
/// once per (re)connection after negotiating `.zerofs4`; off the hot path.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tgetlineage;

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rgetlineage {
    #[deku(endian = "little")]
    pub token: u64,
}

/// `.zerofs4` durability-verified fsync. Like Tfsync but carries the lineage
/// token of the client's oldest un-fsync'd write (`0` = nothing un-fsync'd). The
/// server flushes, then replies Rfsync iff the token is still the live durable
/// lineage, else Rlerror(ESTALE).
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Tfsyncdur {
    #[deku(endian = "little")]
    pub fid: u32,
    #[deku(endian = "little")]
    pub datasync: u32,
    #[deku(endian = "little")]
    pub token: u64,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Rstatfs {
    #[deku(endian = "little")]
    pub r#type: u32, // filesystem type
    #[deku(endian = "little")]
    pub bsize: u32, // optimal transfer block size
    #[deku(endian = "little")]
    pub blocks: u64, // total data blocks in filesystem
    #[deku(endian = "little")]
    pub bfree: u64, // free blocks in filesystem
    #[deku(endian = "little")]
    pub bavail: u64, // free blocks available to non-superuser
    #[deku(endian = "little")]
    pub files: u64, // total file nodes in filesystem
    #[deku(endian = "little")]
    pub ffree: u64, // free file nodes in filesystem
    #[deku(endian = "little")]
    pub fsid: u64, // filesystem id
    #[deku(endian = "little")]
    pub namelen: u32, // maximum length of filenames
}

// The 9P2000.L request types that carry an op-id under `.zerofs3`. Named because
// each is referenced in three places that must agree, the variant `deku(id)`
// below, the type byte in `P9Message::new`, and `carries_op_id`, so a single
// const is the source of truth. The first group is the base non-idempotent
// mutations; the second is their `.zerofs2` compound (create-and-return-stat)
// forms. The remaining message types are single-use and stay as literal ids.
pub const T_LCREATE: u8 = 14;
pub const T_SYMLINK: u8 = 16;
pub const T_MKNOD: u8 = 18;
pub const T_RENAME: u8 = 20;
pub const T_LINK: u8 = 70;
pub const T_MKDIR: u8 = 72;
pub const T_RENAMEAT: u8 = 74;
pub const T_UNLINKAT: u8 = 76;
pub const T_LCREATEATTR: u8 = 238;
pub const T_MKDIRATTR: u8 = 240;
pub const T_SYMLINKATTR: u8 = 242;
pub const T_MKNODATTR: u8 = 244;
pub const T_LINKATTR: u8 = 246;
// The supported fallocate modes are idempotent with respect to file data,
// size, and quota, so retries do not require an op-id. The request still
// participates in fsync durability tracking.
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
    #[deku(id = "118")]
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
    #[deku(id = "26")]
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
    // ZeroFS-private compound extensions (ids outside the standard 9P range),
    // negotiated via "9P2000.L.zerofs2". The *attr requests reuse the standard
    // request layout; only their replies differ (they carry the post-op stat).
    #[deku(id = "236")]
    Tlopenat(Tlopenat),
    #[deku(id = "237")]
    Rlopenat(Rlopenat),
    // .zerofs5 open+first-read fold.
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
    #[deku(id = "248")]
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
    /// Whether this REQUEST mutates durable filesystem state, so a `.zerofs4`
    /// client must track it as un-fsync'd for durability-verified fsync. Defined as
    /// "has a durability fid"; see [`Self::durability_fid`].
    pub fn is_mutation(&self) -> bool {
        self.durability_fid().is_some()
    }

    /// The fid this mutation's durability binds to: a verified `fsync` covers the write
    /// only if it presents this fid. It is the fid of the mutated inode, so the change
    /// is persisted by `fsync` on the object POSIX holds responsible. File ops
    /// (write/setattr) use the file fid; directory mutations (mkdir/symlink/mknod/link/
    /// rename/unlink, and a create's directory entry) use the directory fid, since the
    /// entry lives in the parent and only `fsync(parent)` persists it. A created file's
    /// data is covered by `fsync` on its open fid. The FUSE mount aggregates per inode,
    /// so an obligation on any fid bound to the fsync'd handle's inode is honoured.
    /// Non-mutating ops (fsync, read, walk, open, clunk) return `None`; truncate-on-open
    /// arrives as a separate `Tsetattr`, so `Tlopen`/`Tlopenat` are excluded.
    pub fn durability_fid(&self) -> Option<u32> {
        match self {
            Message::Twrite(m) => Some(m.fid),
            Message::Tsetattr(m) => Some(m.fid),
            Message::Tsetattrattr(m) => Some(m.fid),
            Message::Tfallocate(m) => Some(m.fid),
            Message::Tlcreate(m) => Some(m.fid),
            // The directory ENTRY is in the parent; `fsync(parent)` must cover it.
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

    /// Every fid whose inode this mutation changes, so a verified `fsync` of any of them
    /// accounts for it. For almost all ops this is just [`Self::durability_fid`]; a
    /// `Trenameat` changes both directories in one atomic op (the source loses the entry,
    /// the dest gains it), so it also yields the source dir fid, letting `fsync` on either
    /// directory verify the rename.
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
/// Length of the idempotency op-id spliced into the frame under `.zerofs3`.
pub const P9_OP_ID_LEN: usize = 16;

// Standard 9P framing is size+type+tag+body; the op-id is deliberately NOT part
// of the deku layout. Under `.zerofs3` it is spliced in after the tag (requests
// only), so existing call sites keep producing/parsing standard frames.
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct P9Message {
    #[deku(endian = "little")]
    pub size: u32,
    pub type_: u8,
    #[deku(endian = "little")]
    pub tag: u16,
    #[deku(skip, default = "[0u8; 16]")]
    pub op_id: [u8; 16],
    #[deku(ctx = "*type_")]
    pub body: Message,
}

impl P9Message {
    pub fn to_bytes(&self) -> Result<Vec<u8>, DekuError> {
        self.to_bytes_ctx(false)
    }

    /// Encode. Under `.zerofs3` the op-id is spliced in after the tag (requests
    /// only); otherwise this is standard 9P framing.
    pub fn to_bytes_ctx(&self, op_id_enabled: bool) -> Result<Vec<u8>, DekuError> {
        // deku produces the standard frame (op_id is #[deku(skip)]).
        let mut bytes = DekuContainerWrite::to_bytes(self)?;
        if op_id_enabled && Self::carries_op_id(self.type_) {
            bytes.splice(
                P9_FRAME_HEADER_LEN..P9_FRAME_HEADER_LEN,
                self.op_id.iter().copied(),
            );
        }
        let size = bytes.len() as u32;
        bytes[0..4].copy_from_slice(&size.to_le_bytes());
        Ok(bytes)
    }

    /// Decode. Under `.zerofs3` the op-id is read from the frame (requests only)
    /// and stripped before the body is parsed; otherwise standard 9P framing.
    pub fn from_bytes_ctx(input: &[u8], op_id_enabled: bool) -> Result<P9Message, DekuError> {
        if op_id_enabled
            && input.len() > P9_FRAME_HEADER_LEN
            && Self::carries_op_id(input[P9_TYPE_OFFSET])
        {
            if input.len() < P9_FRAME_HEADER_LEN + P9_OP_ID_LEN {
                return Err(DekuError::Parse(
                    "frame too short to contain an op-id".into(),
                ));
            }
            let mut op_id = [0u8; 16];
            op_id.copy_from_slice(&input[P9_FRAME_HEADER_LEN..P9_FRAME_HEADER_LEN + P9_OP_ID_LEN]);
            // Rebuild the standard frame (without the op-id) to parse the body.
            let mut standard = Vec::with_capacity(input.len() - P9_OP_ID_LEN);
            standard.extend_from_slice(&input[..P9_FRAME_HEADER_LEN]);
            standard.extend_from_slice(&input[P9_FRAME_HEADER_LEN + P9_OP_ID_LEN..]);
            let (_, mut msg) = P9Message::from_bytes((&standard, 0))?;
            msg.op_id = op_id;
            Ok(msg)
        } else {
            let (_, msg) = P9Message::from_bytes((input, 0))?;
            Ok(msg)
        }
    }

    /// Whether a request of this 9P type carries an op-id under `.zerofs3`. The
    /// single source of truth shared by encode and decode, so the two always
    /// agree and a missed type is only a coverage gap, never a framing break.
    /// Covers the non-idempotent mutations (create family, rename, unlink);
    /// idempotent ops (reads/walks/attach/clunk/write/setattr/Tlopenat) are out.
    /// The second group is those same mutations in their `.zerofs2` compound
    /// (create-and-return-stat) form; the op-id is a `.zerofs3` feature applied to
    /// both groups, not something `.zerofs2` carried.
    fn carries_op_id(type_: u8) -> bool {
        matches!(
            type_,
            // base 9P2000.L mutations
            T_LCREATE | T_SYMLINK | T_MKNOD | T_RENAME | T_LINK | T_MKDIR | T_RENAMEAT | T_UNLINKAT
            // their .zerofs2 compound (create-and-return-stat) forms
            | T_LCREATEATTR | T_MKDIRATTR | T_SYMLINKATTR | T_MKNODATTR | T_LINKATTR
        )
    }

    pub fn new(tag: u16, body: Message) -> Self {
        let type_ = match &body {
            Message::Tversion(_) => 100,
            Message::Rversion(_) => 101,
            Message::Tattach(_) => 104,
            Message::Rattach(_) => 105,
            Message::Twalk(_) => 110,
            Message::Rwalk(_) => 111,
            Message::Tlopen(_) => 12,
            Message::Rlopen(_) => 13,
            Message::Tlcreate(_) => T_LCREATE,
            Message::Rlcreate(_) => 15,
            Message::Tread(_) => 116,
            Message::Rread(_) => 117,
            Message::Twrite(_) => 118,
            Message::Rwrite(_) => 119,
            Message::Tclunk(_) => 120,
            Message::Rclunk(_) => 121,
            Message::Treaddir(_) => 40,
            Message::Rreaddir(_) => 41,
            Message::Tgetattr(_) => 24,
            Message::Rgetattr(_) => 25,
            Message::Tsetattr(_) => 26,
            Message::Rsetattr(_) => 27,
            Message::Tfallocate(_) => T_FALLOCATE,
            Message::Rfallocate(_) => R_FALLOCATE,
            Message::Tmkdir(_) => T_MKDIR,
            Message::Rmkdir(_) => 73,
            Message::Tsymlink(_) => T_SYMLINK,
            Message::Rsymlink(_) => 17,
            Message::Tmknod(_) => T_MKNOD,
            Message::Rmknod(_) => 19,
            Message::Treadlink(_) => 22,
            Message::Rreadlink(_) => 23,
            Message::Tlink(_) => T_LINK,
            Message::Rlink(_) => 71,
            Message::Trename(_) => T_RENAME,
            Message::Rrename(_) => 21,
            Message::Trenameat(_) => T_RENAMEAT,
            Message::Rrenameat(_) => 75,
            Message::Tunlinkat(_) => T_UNLINKAT,
            Message::Runlinkat(_) => 77,
            Message::Tfsync(_) => 50,
            Message::Rfsync(_) => 51,
            Message::Tfsyncdur(_) => 232,
            Message::Tgetlineage(_) => 233,
            Message::Rgetlineage(_) => 234,
            Message::Tlock(_) => 52,
            Message::Rlock(_) => 53,
            Message::Tgetlock(_) => 54,
            Message::Rgetlock(_) => 55,
            Message::Rlerror(_) => 7,
            Message::Tflush(_) => 108,
            Message::Rflush(_) => 109,
            Message::Txattrwalk(_) => 30,
            Message::Rxattrwalk(_) => 31,
            Message::Tstatfs(_) => 8,
            Message::Rstatfs(_) => 9,
            Message::Tlopenat(_) => 236,
            Message::Rlopenat(_) => 237,
            Message::Tlopenatread(_) => 230,
            Message::Rlopenatread(_) => 231,
            Message::Tlcreateattr(_) => T_LCREATEATTR,
            Message::Rlcreateattr(_) => 239,
            Message::Tmkdirattr(_) => T_MKDIRATTR,
            Message::Rmkdirattr(_) => 241,
            Message::Tsymlinkattr(_) => T_SYMLINKATTR,
            Message::Rsymlinkattr(_) => 243,
            Message::Tmknodattr(_) => T_MKNODATTR,
            Message::Rmknodattr(_) => 245,
            Message::Tlinkattr(_) => T_LINKATTR,
            Message::Rlinkattr(_) => 247,
            Message::Tsetattrattr(_) => 248,
            Message::Rsetattrattr(_) => 249,
            Message::Trebind(_) => 250,
            Message::Rrebind(_) => 251,
            Message::Twalkgetattr(_) => 252,
            Message::Rwalkgetattr(_) => 253,
            Message::Treaddirattr(_) => 254,
            Message::Rreaddirattr(_) => 255,
        };

        Self {
            size: 0, // Will be calculated during serialization
            type_,
            tag,
            op_id: [0u8; 16],
            body,
        }
    }

    /// Construct a request carrying an idempotency op-id (encode with `to_bytes_ctx(true)`).
    pub fn new_with_op_id(tag: u16, op_id: [u8; 16], body: Message) -> Self {
        let mut msg = Self::new(tag, body);
        msg.op_id = op_id;
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
    fn op_id_round_trips_on_a_request_under_zerofs3() {
        // Tmkdir is a covered (non-idempotent) request, so it carries the op-id.
        let op_id = [7u8; 16];
        let msg = P9Message::new_with_op_id(5, op_id, tmkdir());
        let bytes = msg.to_bytes_ctx(true).unwrap();
        let decoded = P9Message::from_bytes_ctx(&bytes, true).unwrap();
        assert_eq!(
            decoded.op_id, op_id,
            "the op-id must round-trip under .zerofs3"
        );
        assert_eq!(decoded.tag, 5);
        assert!(matches!(decoded.body, Message::Tmkdir(_)));
    }

    #[test]
    fn op_id_is_absent_in_standard_framing() {
        let op_id = [7u8; 16];
        let msg = P9Message::new_with_op_id(5, op_id, tmkdir());
        let with = msg.to_bytes_ctx(true).unwrap();
        let without = msg.to_bytes_ctx(false).unwrap();
        assert_eq!(
            with.len(),
            without.len() + 16,
            "the op-id adds exactly 16 bytes on the wire, and nothing without .zerofs3"
        );
        // Standard 9P framing (no extension) carries no op-id.
        let decoded = P9Message::from_bytes_ctx(&without, false).unwrap();
        assert_eq!(
            decoded.op_id, [0u8; 16],
            "standard framing carries no op-id"
        );
        // And a stock decoder (default ctx) reads the standard frame unchanged.
        let (_, plain) = P9Message::from_bytes((&without, 0)).unwrap();
        assert_eq!(plain.tag, 5);
    }

    #[test]
    fn idempotent_requests_carry_no_op_id() {
        // A read-side request (Tclunk) is not covered, so even under .zerofs3 it
        // is framed exactly as standard 9P (no op-id bytes).
        let msg = P9Message::new_with_op_id(5, [7u8; 16], Message::Tclunk(Tclunk { fid: 9 }));
        let with = msg.to_bytes_ctx(true).unwrap();
        let without = msg.to_bytes_ctx(false).unwrap();
        assert_eq!(with, without, "an uncovered op must not carry an op-id");
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
