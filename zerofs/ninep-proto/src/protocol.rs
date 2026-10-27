use crate::slice_codec::{self, CodecError, LockType};
use crate::wire_types::*;
use alloc::vec::Vec;
use bytes::{Buf, Bytes};
#[path = "owned_codec.rs"]
mod owned_codec;

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

pub const P9_LOCK_FLAGS_BLOCK: u32 = 1; // blocking request

pub const P9_CHANNEL_SIZE: usize = 1000;
pub const P9_DEBUG_BUFFER_SIZE: usize = 40;
pub const P9_READDIR_BATCH_SIZE: usize = 1000;
pub const P9_NOBODY_UID: u32 = 65534;

/// Userspace string backed by shared frame storage.
pub type P9String = WireString<Bytes>;
pub type P9Bytes = WireBytes<Bytes>;

impl WireString<Bytes> {
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self::from_storage(data.into())
    }
}

impl From<Vec<u8>> for WireBytes<Bytes> {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<WireBytes<Bytes>> for Bytes {
    fn from(value: WireBytes<Bytes>) -> Self {
        value.0
    }
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub qid: Qid,
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

#[derive(Debug, Clone)]
pub struct Tversion {
    pub msize: u32,
    pub version: P9String,
}

#[derive(Debug, Clone)]
pub struct Tattach {
    pub fid: u32,
    pub afid: u32,
    pub uname: P9String,
    pub aname: P9String,
    pub n_uname: u32,
}

#[derive(Debug, Clone)]
pub struct Twalk {
    pub fid: u32,
    pub newfid: u32,
    pub nwname: u16,
    pub wnames: Vec<P9String>,
}

#[derive(Debug, Clone)]
pub struct Tlopen {
    pub fid: u32,
    pub flags: u32,
}

#[derive(Debug, Clone)]
pub struct Tlcreate {
    pub fid: u32,
    pub name: P9String,
    pub flags: u32,
    pub mode: u32,
    pub gid: u32,
}

#[derive(Debug, Clone)]
pub struct Tread {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct Twrite {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
    pub data: P9Bytes,
}

#[derive(Debug, Clone)]
pub struct Tclunk {
    pub fid: u32,
}

#[derive(Debug, Clone)]
pub struct Treaddir {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct Tgetattr {
    pub fid: u32,
    pub request_mask: u64,
}

#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct Tfallocate {
    pub fid: u32,
    pub offset: u64,
    pub length: u64,
    pub mode: u32,
}

#[derive(Debug, Clone)]
pub struct Rfallocate;

#[derive(Debug, Clone)]
pub struct Tmkdir {
    pub dfid: u32,
    pub name: P9String,
    pub mode: u32,
    pub gid: u32,
}

#[derive(Debug, Clone)]
pub struct Tsymlink {
    pub dfid: u32,
    pub name: P9String,
    pub symtgt: P9String,
    pub gid: u32,
}

#[derive(Debug, Clone)]
pub struct Tmknod {
    pub dfid: u32,
    pub name: P9String,
    pub mode: u32,
    pub major: u32,
    pub minor: u32,
    pub gid: u32,
}

#[derive(Debug, Clone)]
pub struct Tlink {
    pub dfid: u32,
    pub fid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone)]
pub struct Trename {
    pub fid: u32,
    pub dfid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone)]
pub struct Trenameat {
    pub olddirfid: u32,
    pub oldname: P9String,
    pub newdirfid: u32,
    pub newname: P9String,
}

#[derive(Debug, Clone)]
pub struct Tunlinkat {
    pub dirfid: u32,
    pub name: P9String,
    pub flags: u32,
}

#[derive(Debug, Clone)]
pub struct Tfsync {
    pub fid: u32,
    pub datasync: u32,
}

#[derive(Debug, Clone)]
pub struct Treadlink {
    pub fid: u32,
}

#[derive(Debug, Clone)]
pub struct Tstatfs {
    pub fid: u32,
}

// Core 9P structures
#[derive(Debug, Clone)]
pub struct Tflush {
    pub oldtag: u16,
}

#[derive(Debug, Clone)]
pub struct Rflush;

// Extended attributes
#[derive(Debug, Clone)]
pub struct Txattrwalk {
    pub fid: u32,
    pub newfid: u32,
    pub name: P9String,
}

#[derive(Debug, Clone)]
pub struct Tlock {
    pub fid: u32,
    pub lock_type: LockType,
    pub flags: u32,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: P9String,
}

#[derive(Debug, Clone)]
pub struct Tgetlock {
    pub fid: u32,
    pub lock_type: LockType,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: P9String,
}

#[derive(Debug, Clone)]
pub struct Rxattrwalk {
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct Rgetlock {
    pub lock_type: LockType,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: P9String,
}

// Response messages
#[derive(Debug, Clone)]
pub struct Rversion {
    pub msize: u32,
    pub version: P9String,
}

#[derive(Debug, Clone)]
pub struct Rattach {
    pub qid: Qid,
}

// ZeroFS-private reconnect extension: binds a fresh fid to an existing inode by
// id (not by re-walking a path), so reconnection survives renames/hardlinks.
#[derive(Debug, Clone)]
pub struct Trebind {
    pub fid: u32,
    pub inode_id: u64,
    /// Attach-root inode. A root-self request has `inode_id == root_inode`.
    pub root_inode: u64,
    /// Replay flags. `P9_REBIND_OPENED` requires `P9_REBIND_REPLAY`, marks an
    /// expected in-place reopen, and grants neither access nor an inode pin.
    pub flags: u8,
    /// Original `Tattach` username for legacy clients, or a binary credential
    /// payload beginning with `P9_REBIND_CREDENTIAL_SENTINEL`. The group-count
    /// byte may carry `P9_REBIND_CREDENTIAL_GROUPS_INCOMPLETE`.
    pub uname: P9String,
    /// Numeric uid; kernel credential payloads carry fsuid here.
    pub n_uname: u32,
}

// Private compound requests require the ZeroFS dialect.
#[derive(Debug, Clone)]
pub struct Twalkgetattr {
    pub fid: u32,
    pub newfid: u32,
    pub nwname: u16,
    pub wnames: Vec<P9String>,
}

// Returned only on a full walk; on any miss the server replies Rlerror.
#[derive(Debug, Clone)]
pub struct Rwalkgetattr {
    pub nwqid: u16,
    pub wqids: Vec<Qid>,
    pub stat: Stat,
}

#[derive(Debug, Clone)]
pub struct Treaddirattr {
    pub fid: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct DirEntryPlus {
    pub qid: Qid,
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

#[derive(Debug, Clone)]
pub struct Rreaddirattr {
    pub count: u32,
    pub data: P9Bytes,
}

// Tlopenat preserves `fid` and opens `newfid`. Tlcreateattr preserves `dfid`,
// creates and opens `newfid`, and returns stat. Other *attr replies add stat to
// the corresponding standard request layout.
#[derive(Debug, Clone)]
pub struct Tlopenat {
    pub fid: u32,
    pub newfid: u32,
    pub flags: u32,
}

#[derive(Debug, Clone)]
pub struct Rlopenat {
    pub qid: Qid,
    pub iounit: u32,
}

// Tlopenatread combines Tlopenat with a best-effort Tread at offset zero.
#[derive(Debug, Clone)]
pub struct Tlopenatread {
    pub fid: u32,
    pub newfid: u32,
    pub flags: u32,
    /// Bytes to prefetch from offset 0. The server clamps this to fit msize.
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct Rlopenatread {
    pub qid: Qid,
    pub iounit: u32,
    /// One when `data` reaches EOF; zero for an incomplete prefetch.
    pub eof: u8,
    pub count: u32,
    pub data: P9Bytes,
}

#[derive(Debug, Clone)]
pub struct Tlcreateattr {
    pub dfid: u32,
    pub newfid: u32,
    pub name: P9String,
    pub flags: u32,
    pub mode: u32,
    pub gid: u32,
}

#[derive(Debug, Clone)]
pub struct Rmkdirattr {
    pub stat: Stat,
}

#[derive(Debug, Clone)]
pub struct Rsymlinkattr {
    pub stat: Stat,
}

#[derive(Debug, Clone)]
pub struct Rmknodattr {
    pub stat: Stat,
}

#[derive(Debug, Clone)]
pub struct Rlinkattr {
    pub stat: Stat,
}

#[derive(Debug, Clone)]
pub struct Rsetattrattr {
    pub stat: Stat,
}

impl Rreaddirattr {
    pub fn from_entries(entries: Vec<DirEntryPlus>) -> Result<Self, CodecError> {
        let size = entries.iter().try_fold(0usize, |n, entry| {
            n.checked_add(entry.wire_size())
                .ok_or(CodecError::LengthOverflow)
        })?;
        let mut data = alloc::vec![0; size];
        let mut cursor = 0;
        for entry in entries {
            cursor += entry.to_slice(&mut data[cursor..])?;
        }
        Ok(Self {
            count: size.try_into()?,
            data: data.into(),
        })
    }
    pub fn to_entries(&self) -> Result<Vec<DirEntryPlus>, CodecError> {
        let mut input = self.data.0.clone();
        let mut entries = Vec::new();
        while !input.is_empty() {
            let (entry, length) = DirEntryPlus::decode(&input)?;
            entries.push(entry);
            input.advance(length);
        }
        Ok(entries)
    }
}

#[derive(Debug, Clone)]
pub struct Rwalk {
    pub nwqid: u16,
    pub wqids: Vec<Qid>,
}

#[derive(Debug, Clone)]
pub struct Rlcreate {
    pub qid: Qid,
    pub iounit: u32,
}

#[derive(Debug, Clone)]
pub struct Rread {
    pub count: u32,
    pub data: P9Bytes,
}

#[derive(Debug, Clone)]
pub struct Rreaddir {
    pub count: u32,
    pub data: P9Bytes,
}

impl Rreaddir {
    pub fn from_entries(entries: Vec<DirEntry>) -> Result<Self, CodecError> {
        let size = entries.iter().try_fold(0usize, |n, entry| {
            n.checked_add(entry.wire_size())
                .ok_or(CodecError::LengthOverflow)
        })?;
        let mut data = alloc::vec![0; size];
        let mut cursor = 0;
        for entry in entries {
            cursor += entry.to_slice(&mut data[cursor..])?;
        }
        Ok(Self {
            count: size.try_into()?,
            data: data.into(),
        })
    }
    pub fn to_entries(&self) -> Result<Vec<DirEntry>, CodecError> {
        let mut input = self.data.0.clone();
        let mut entries = Vec::new();
        while !input.is_empty() {
            let (entry, length) = DirEntry::decode(&input)?;
            entries.push(entry);
            input.advance(length);
        }
        Ok(entries)
    }
}

#[derive(Debug, Clone)]
pub struct Rmkdir {
    pub qid: Qid,
}

#[derive(Debug, Clone)]
pub struct Rsymlink {
    pub qid: Qid,
}

#[derive(Debug, Clone)]
pub struct Rmknod {
    pub qid: Qid,
}

#[derive(Debug, Clone)]
pub struct Rreadlink {
    pub target: P9String,
}

// Empty responses
#[derive(Debug, Clone)]
pub struct Rclunk;

#[derive(Debug, Clone)]
pub struct Rsetattr;

#[derive(Debug, Clone)]
pub struct Rrename;

#[derive(Debug, Clone)]
pub struct Rlink;

#[derive(Debug, Clone)]
pub struct Rrenameat;

#[derive(Debug, Clone)]
pub struct Runlinkat;

#[derive(Debug, Clone)]
pub struct Rfsync;

/// Queries the connection's durability lineage and writer epoch.
#[derive(Debug, Clone)]
pub struct Tgetlineage;

/// Durability-verified fsync carrying the oldest unverified lineage token.
/// `datasync` contains `P9_FSYNC_*` flags; without `P9_FSYNC_INODE` the barrier
/// is filesystem-wide. Token zero denotes no pending mutation in that scope. A
/// lineage mismatch returns `ESTALE`.
#[derive(Debug, Clone)]
pub struct Tfsyncdur {
    pub fid: u32,
    pub datasync: u32,
    pub token: u64,
}

// These IDs define mutation-envelope coverage and must match the enum layout.
pub const T_LCREATE: u8 = message_type::TLCREATE;
pub const T_SYMLINK: u8 = message_type::TSYMLINK;
pub const T_MKNOD: u8 = message_type::TMKNOD;
pub const T_RENAME: u8 = message_type::TRENAME;
pub const T_SETATTR: u8 = message_type::TSETATTR;
pub const T_WRITE: u8 = message_type::TWRITE;
pub const T_LINK: u8 = message_type::TLINK;
pub const T_MKDIR: u8 = message_type::TMKDIR;
pub const T_RENAMEAT: u8 = message_type::TRENAMEAT;
pub const T_UNLINKAT: u8 = message_type::TUNLINKAT;
pub const T_LCREATEATTR: u8 = message_type::TLCREATEATTR;
pub const T_MKDIRATTR: u8 = message_type::TMKDIRATTR;
pub const T_SYMLINKATTR: u8 = message_type::TSYMLINKATTR;
pub const T_MKNODATTR: u8 = message_type::TMKNODATTR;
pub const T_LINKATTR: u8 = message_type::TLINKATTR;
pub const T_SETATTRATTR: u8 = message_type::TSETATTRATTR;
// Delayed fallocate retries can reorder with writes and therefore carry op-ids.
pub const T_FALLOCATE: u8 = message_type::TFALLOCATE;
pub const R_FALLOCATE: u8 = message_type::RFALLOCATE;

// Main message enum
#[derive(Debug, Clone)]
pub enum Message {
    Tversion(Tversion),
    Rversion(Rversion),
    Tattach(Tattach),
    Rattach(Rattach),
    Twalk(Twalk),
    Rwalk(Rwalk),
    Tlopen(Tlopen),
    Rlopen(Rlopen),
    Tlcreate(Tlcreate),
    Rlcreate(Rlcreate),
    Tread(Tread),
    Rread(Rread),
    Twrite(Twrite),
    Rwrite(Rwrite),
    Tclunk(Tclunk),
    Rclunk(Rclunk),
    Treaddir(Treaddir),
    Rreaddir(Rreaddir),
    Tgetattr(Tgetattr),
    Rgetattr(Rgetattr),
    Tsetattr(Tsetattr),
    Rsetattr(Rsetattr),
    Tfallocate(Tfallocate),
    Rfallocate(Rfallocate),
    Tmkdir(Tmkdir),
    Rmkdir(Rmkdir),
    Tsymlink(Tsymlink),
    Rsymlink(Rsymlink),
    Tmknod(Tmknod),
    Rmknod(Rmknod),
    Treadlink(Treadlink),
    Rreadlink(Rreadlink),
    Tlink(Tlink),
    Rlink(Rlink),
    Trename(Trename),
    Rrename(Rrename),
    Trenameat(Trenameat),
    Rrenameat(Rrenameat),
    Tunlinkat(Tunlinkat),
    Runlinkat(Runlinkat),
    Tfsync(Tfsync),
    Rfsync(Rfsync),
    Tfsyncdur(Tfsyncdur),
    Tgetlineage(Tgetlineage),
    Rgetlineage(Rgetlineage),
    Tlock(Tlock),
    Rlock(Rlock),
    Tgetlock(Tgetlock),
    Rgetlock(Rgetlock),
    Rlerror(Rlerror),
    Tflush(Tflush),
    Rflush(Rflush),
    Txattrwalk(Txattrwalk),
    Rxattrwalk(Rxattrwalk),
    Tstatfs(Tstatfs),
    Rstatfs(Rstatfs),
    // Private compound extensions use IDs outside the standard 9P range. The
    // *attr requests keep the standard request layout and return a richer reply.
    Tlopenat(Tlopenat),
    Rlopenat(Rlopenat),
    Tlopenatread(Tlopenatread),
    Rlopenatread(Rlopenatread),
    Tlcreateattr(Tlcreateattr),
    Rlcreateattr(Rlcreateattr),
    Tmkdirattr(Tmkdir),
    Rmkdirattr(Rmkdirattr),
    Tsymlinkattr(Tsymlink),
    Rsymlinkattr(Rsymlinkattr),
    Tmknodattr(Tmknod),
    Rmknodattr(Rmknodattr),
    Tlinkattr(Tlink),
    Rlinkattr(Rlinkattr),
    Tsetattrattr(Tsetattr),
    Rsetattrattr(Rsetattrattr),
    // ZeroFS-private reconnect extension (ids outside the standard 9P range).
    Trebind(Trebind),
    Rrebind(Rrebind),
    Twalkgetattr(Twalkgetattr),
    Rwalkgetattr(Rwalkgetattr),
    Treaddirattr(Treaddirattr),
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

/// An owned 9P frame.
#[derive(Debug, Clone)]
pub struct P9Message {
    pub size: u32,
    pub type_: u8,
    pub tag: u16,
    pub op_id: [u8; 16],
    pub op_flags: u8,
    pub op_origin_epoch: u64,
    pub body: Message,
}

impl P9Message {
    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        self.to_bytes_ctx(false)
    }

    pub fn to_bytes_ctx(&self, op_id_enabled: bool) -> Result<Vec<u8>, CodecError> {
        Self::encode_body(self.tag, self.envelope(), &self.body, op_id_enabled)
    }

    pub fn encode_body(
        tag: u16,
        envelope: MutationEnvelope,
        body: &Message,
        op_id_enabled: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let view = body.as_view();
        let size = slice_codec::encoded_frame_size(&view, op_id_enabled)?;
        if size > P9_MAX_MSIZE as usize {
            return Err(CodecError::MessageTooLarge);
        }
        let mut output = alloc::vec![0; size];
        slice_codec::encode_frame(
            &mut output,
            P9_MAX_MSIZE,
            tag,
            envelope,
            &view,
            op_id_enabled,
        )?;
        Ok(output)
    }

    pub fn to_slice_ctx(
        &self,
        output: &mut [u8],
        op_id_enabled: bool,
    ) -> Result<usize, CodecError> {
        slice_codec::encode_frame(
            output,
            P9_MAX_MSIZE,
            self.tag,
            self.envelope(),
            &self.body.as_view(),
            op_id_enabled,
        )
    }

    fn envelope(&self) -> MutationEnvelope {
        MutationEnvelope {
            op_id: self.op_id,
            flags: self.op_flags,
            origin_writer_epoch: self.op_origin_epoch,
        }
    }

    pub fn from_bytes_ctx(input: &[u8], op_id_enabled: bool) -> Result<Self, CodecError> {
        let header = slice_codec::decode_header(input, P9_MAX_MSIZE)?;
        if header.size as usize != input.len() {
            return Err(CodecError::FrameSizeMismatch);
        }
        Self::from_owned_bytes_ctx(Bytes::copy_from_slice(input), op_id_enabled)
    }

    pub fn from_owned_bytes_ctx(input: Bytes, op_id_enabled: bool) -> Result<Self, CodecError> {
        let frame = slice_codec::decode_frame(&input, P9_MAX_MSIZE, op_id_enabled)?;
        Ok(Self {
            size: frame.header.size,
            type_: frame.header.type_,
            tag: frame.header.tag,
            op_id: frame.envelope.op_id,
            op_flags: frame.envelope.flags,
            op_origin_epoch: frame.envelope.origin_writer_epoch,
            body: Message::from_view(frame.body, &input)?,
        })
    }

    pub fn carries_op_id(type_: u8) -> bool {
        slice_codec::carries_op_id(type_)
    }

    pub fn new(tag: u16, body: Message) -> Self {
        let type_ = body.as_view().type_id();

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
    use alloc::vec;

    #[test]
    fn caller_owned_output_matches_allocating_encoder() {
        let message = P9Message::new(
            42,
            Message::Tgetattr(Tgetattr {
                fid: 7,
                request_mask: GETATTR_ALL,
            }),
        );
        let expected = message.to_bytes_ctx(false).unwrap();
        let mut output = [0u8; 64];

        let written = message.to_slice_ctx(&mut output, false).unwrap();

        assert_eq!(&output[..written], expected);
    }

    fn counted_payload(message: &P9Message) -> (u32, &Bytes) {
        match &message.body {
            Message::Rread(message) => (message.count, &message.data),
            Message::Rreaddir(message) => (message.count, &message.data),
            Message::Rlopenatread(message) => (message.count, &message.data),
            Message::Twrite(message) => (message.count, &message.data),
            Message::Rreaddirattr(message) => (message.count, &message.data),
            _ => panic!("expected a counted-payload message"),
        }
    }

    fn assert_owned_payload_aliases(
        message: P9Message,
        op_id_enabled: bool,
        payload_offset: usize,
        expected_payload: &[u8],
    ) {
        let frame = Bytes::from(message.to_bytes_ctx(op_id_enabled).unwrap());
        let expected_ptr = frame.as_ptr().wrapping_add(payload_offset);
        let expected_frame = frame.clone();
        let decoded = P9Message::from_owned_bytes_ctx(frame, op_id_enabled).unwrap();
        let (decoded_count, decoded_payload) = counted_payload(&decoded);

        assert_eq!(decoded.size, expected_frame.len() as u32);
        assert_eq!(decoded_count, expected_payload.len() as u32);
        assert_eq!(decoded_payload.as_ref(), expected_payload);
        assert_eq!(
            decoded_payload.as_ptr(),
            expected_ptr,
            "the decoded payload must be a slice of the owned frame"
        );
        assert_eq!(
            decoded.to_bytes_ctx(op_id_enabled).unwrap(),
            expected_frame,
            "owned decoding must preserve every scalar field"
        );
    }

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
        let decoded = P9Message::from_bytes_ctx(&bytes, false).unwrap();
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
    fn owned_rread_payload_aliases_the_input_frame() {
        let payload = b"rread payload retained from its frame";
        let message = P9Message::new(
            7,
            Message::Rread(Rread {
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
        );

        assert_owned_payload_aliases(message, false, P9_IOHDRSZ as usize, payload);
    }

    #[test]
    fn owned_rlopenatread_payload_aliases_the_input_frame() {
        let payload = b"prefetched bytes retained from their frame";
        let message = P9Message::new(
            11,
            Message::Rlopenatread(Rlopenatread {
                qid: qid(),
                iounit: 64 * 1024,
                eof: 1,
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
        );

        assert_owned_payload_aliases(message, false, P9_RLOPENATREAD_HDR as usize, payload);
    }

    #[test]
    fn owned_enveloped_twrite_payload_aliases_the_input_frame() {
        let payload = b"write bytes retained from their enveloped frame";
        let message = P9Message::new_with_op_id_flags_and_origin(
            19,
            [0xa5; P9_OP_ID_LEN],
            P9_OP_FLAG_RETRY,
            42,
            Message::Twrite(Twrite {
                fid: 23,
                offset: 29,
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
        );

        assert_owned_payload_aliases(
            message,
            true,
            P9_TWRITE_HDR as usize + P9_OP_ENVELOPE_LEN,
            payload,
        );
    }

    #[test]
    fn owned_standard_twrite_payload_aliases_the_input_frame() {
        let payload = b"write bytes retained from a standard frame";
        let message = P9Message::new(
            21,
            Message::Twrite(Twrite {
                fid: 23,
                offset: 29,
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
        );

        assert_owned_payload_aliases(message, false, P9_TWRITE_HDR as usize, payload);
    }

    #[test]
    fn owned_directory_payloads_alias_the_input_frame() {
        let payload = b"encoded directory entries";
        for body in [
            Message::Rreaddir(Rreaddir {
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
            Message::Rreaddirattr(Rreaddirattr {
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
        ] {
            assert_owned_payload_aliases(
                P9Message::new(25, body),
                false,
                P9_IOHDRSZ as usize,
                payload,
            );
        }
    }

    #[test]
    fn owned_metadata_and_directory_names_alias_the_input_frame() {
        let name = P9String::new(Bytes::from_static(b"name\xff"));
        let bodies = [
            Message::Twalk(Twalk {
                fid: 1,
                newfid: 2,
                nwname: 1,
                wnames: vec![name.clone()],
            }),
            Message::Tsymlink(Tsymlink {
                dfid: 1,
                name: name.clone(),
                symtgt: P9String::new(Bytes::from_static(b"target")),
                gid: 0,
            }),
            Message::Rreaddir(
                Rreaddir::from_entries(vec![DirEntry {
                    qid: qid(),
                    offset: 1,
                    type_: 0,
                    name: name.clone(),
                }])
                .unwrap(),
            ),
            Message::Rreaddirattr(
                Rreaddirattr::from_entries(vec![DirEntryPlus {
                    qid: qid(),
                    offset: 1,
                    type_: 0,
                    name,
                    stat: stat(),
                }])
                .unwrap(),
            ),
        ];
        for body in bodies {
            let message = P9Message::new(1, body);
            for dialect in [false, true] {
                let frame = Bytes::from(message.to_bytes_ctx(dialect).unwrap());
                let start = frame.as_ptr() as usize;
                let end = start + frame.len();
                let decoded = P9Message::from_owned_bytes_ctx(frame, dialect).unwrap();
                let names = match decoded.body {
                    Message::Twalk(walk) => walk.wnames,
                    Message::Tsymlink(link) => vec![link.name, link.symtgt],
                    Message::Rreaddir(dir) => dir
                        .to_entries()
                        .unwrap()
                        .into_iter()
                        .map(|entry| entry.name)
                        .collect(),
                    Message::Rreaddirattr(dir) => dir
                        .to_entries()
                        .unwrap()
                        .into_iter()
                        .map(|entry| entry.name)
                        .collect(),
                    _ => unreachable!(),
                };
                assert!(!names.is_empty());
                for name in names {
                    let ptr = name.data.as_ptr() as usize;
                    assert!(!name.is_empty());
                    assert!(ptr >= start && ptr + name.len() <= end);
                }
            }
        }
    }

    #[test]
    fn owned_and_borrowed_decoders_reject_trailing_data() {
        let mut frame = P9Message::new(
            27,
            Message::Rread(Rread {
                count: 7,
                data: b"payload".to_vec().into(),
            }),
        )
        .to_bytes()
        .unwrap();
        frame[P9_HEADER_SIZE..P9_IOHDRSZ as usize].copy_from_slice(&3u32.to_le_bytes());

        assert_eq!(
            P9Message::from_bytes_ctx(&frame, false).unwrap_err(),
            CodecError::TrailingData
        );
        assert_eq!(
            P9Message::from_owned_bytes_ctx(Bytes::from(frame), false).unwrap_err(),
            CodecError::TrailingData
        );
    }

    #[test]
    fn owned_counted_decoder_rejects_truncated_payloads() {
        let payload = b"payload";
        let rread = P9Message::new(
            31,
            Message::Rread(Rread {
                count: payload.len() as u32,
                data: payload.to_vec().into(),
            }),
        );
        let valid = rread.to_bytes().unwrap();

        let count_cases = [
            (rread, false, P9_IOHDRSZ as usize - P9_COUNT_FIELD_LEN),
            (
                P9Message::new(
                    32,
                    Message::Rlopenatread(Rlopenatread {
                        qid: qid(),
                        iounit: 4096,
                        eof: 0,
                        count: payload.len() as u32,
                        data: payload.to_vec().into(),
                    }),
                ),
                false,
                P9_RLOPENATREAD_HDR as usize - P9_COUNT_FIELD_LEN,
            ),
            (
                P9Message::new_with_op_id(
                    33,
                    [0x5a; P9_OP_ID_LEN],
                    Message::Twrite(Twrite {
                        fid: 1,
                        offset: 2,
                        count: payload.len() as u32,
                        data: payload.to_vec().into(),
                    }),
                ),
                true,
                P9_TWRITE_HDR as usize + P9_OP_ENVELOPE_LEN - P9_COUNT_FIELD_LEN,
            ),
        ];

        for (message, op_id_enabled, count_offset) in count_cases {
            let mut truncated = message.to_bytes_ctx(op_id_enabled).unwrap();
            truncated[count_offset..count_offset + P9_COUNT_FIELD_LEN]
                .copy_from_slice(&((payload.len() + 1) as u32).to_le_bytes());
            assert!(
                P9Message::from_owned_bytes_ctx(Bytes::from(truncated), op_id_enabled).is_err()
            );
        }

        let mut truncated_count = valid[..P9_HEADER_SIZE + P9_COUNT_FIELD_LEN - 1].to_vec();
        let truncated_size = truncated_count.len() as u32;
        truncated_count[..P9_SIZE_FIELD_LEN].copy_from_slice(&truncated_size.to_le_bytes());
        assert!(P9Message::from_owned_bytes_ctx(Bytes::from(truncated_count), false).is_err());
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
        let plain = P9Message::from_bytes_ctx(&without, false).unwrap();
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
        let decoded = P9Message::from_bytes_ctx(&bytes, false).unwrap();
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
    fn rebind_round_trips_binary_credentials() {
        let credentials = vec![
            P9_REBIND_CREDENTIAL_SENTINEL,
            P9_REBIND_CREDENTIAL_VERSION,
            0xd2,
            0x04,
            0,
            0,
            1,
            0x2e,
            0x16,
            0,
            0,
        ];
        let msg = P9Message::new(
            18,
            Message::Trebind(Trebind {
                fid: 10,
                inode_id: 43,
                root_inode: 0,
                flags: 0,
                uname: P9String::new(credentials.clone()),
                n_uname: 1001,
            }),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = P9Message::from_bytes_ctx(&bytes, false).unwrap();
        match decoded.body {
            Message::Trebind(rebind) => {
                assert_eq!(rebind.uname.data, credentials);
                assert_eq!(rebind.n_uname, 1001);
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
