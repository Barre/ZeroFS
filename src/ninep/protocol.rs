use deku::prelude::*;

pub const VERSION_9P2000L: &str = "9P2000.L";

#[derive(Debug, Clone, Copy, PartialEq, DekuRead, DekuWrite)]
#[deku(id_type = "u8", endian = "little")]
pub enum MessageType {
    #[deku(id = "100")]
    Tversion,
    #[deku(id = "101")]
    Rversion,
    #[deku(id = "104")]
    Tattach,
    #[deku(id = "105")]
    Rattach,
    #[deku(id = "108")]
    Tflush,
    #[deku(id = "109")]
    Rflush,
    #[deku(id = "110")]
    Twalk,
    #[deku(id = "111")]
    Rwalk,
    #[deku(id = "114")]
    Tread,
    #[deku(id = "115")]
    Rread,
    #[deku(id = "116")]
    Twrite,
    #[deku(id = "117")]
    Rwrite,
    #[deku(id = "118")]
    Tclunk,
    #[deku(id = "119")]
    Rclunk,

    // 9P2000.L extensions
    #[deku(id = "12")]
    Tlopen,
    #[deku(id = "13")]
    Rlopen,
    #[deku(id = "14")]
    Tlcreate,
    #[deku(id = "15")]
    Rlcreate,
    #[deku(id = "16")]
    Tsymlink,
    #[deku(id = "17")]
    Rsymlink,
    #[deku(id = "18")]
    Tmknod,
    #[deku(id = "19")]
    Rmknod,
    #[deku(id = "20")]
    Trename,
    #[deku(id = "21")]
    Rrename,
    #[deku(id = "22")]
    Treadlink,
    #[deku(id = "23")]
    Rreadlink,
    #[deku(id = "24")]
    Tgetattr,
    #[deku(id = "25")]
    Rgetattr,
    #[deku(id = "26")]
    Tsetattr,
    #[deku(id = "27")]
    Rsetattr,
    #[deku(id = "30")]
    Txattrwalk,
    #[deku(id = "31")]
    Rxattrwalk,
    #[deku(id = "40")]
    Treaddir,
    #[deku(id = "41")]
    Rreaddir,
    #[deku(id = "50")]
    Tfsync,
    #[deku(id = "51")]
    Rfsync,
    #[deku(id = "70")]
    Tlink,
    #[deku(id = "71")]
    Rlink,
    #[deku(id = "72")]
    Tmkdir,
    #[deku(id = "73")]
    Rmkdir,
    #[deku(id = "74")]
    Trenameat,
    #[deku(id = "75")]
    Rrenameat,
    #[deku(id = "76")]
    Tunlinkat,
    #[deku(id = "77")]
    Runlinkat,
    #[deku(id = "8")]
    Tstatfs,
    #[deku(id = "9")]
    Rstatfs,
}

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

// Basic structures
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct Qid {
    pub type_: u8,
    #[deku(endian = "little")]
    pub version: u32,
    #[deku(endian = "little")]
    pub path: u64,
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

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct P9String {
    #[deku(endian = "little", update = "self.data.len()")]
    pub len: u16,
    #[deku(count = "len")]
    pub data: Vec<u8>,
}

impl P9String {
    pub fn new(s: &str) -> Self {
        let data = s.as_bytes().to_vec();
        Self {
            len: data.len() as u16,
            data,
        }
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.data)
    }
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct MessageHeader {
    #[deku(endian = "little")]
    pub size: u32,
    pub type_: MessageType,
    #[deku(endian = "little")]
    pub tag: u16,
}

#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct DirEntry {
    pub qid: Qid,
    #[deku(endian = "little")]
    pub offset: u64,
    pub type_: u8,
    pub name: P9String,
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
    #[deku(count = "count")]
    pub data: Vec<u8>,
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
pub struct Rxattrwalk {
    #[deku(endian = "little")]
    pub size: u64,
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
    #[deku(count = "count")]
    pub data: Vec<u8>,
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
    #[deku(count = "count")]
    pub data: Vec<u8>,
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
    #[deku(id = "14")]
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
    #[deku(id = "72")]
    Tmkdir(Tmkdir),
    #[deku(id = "73")]
    Rmkdir(Rmkdir),
    #[deku(id = "16")]
    Tsymlink(Tsymlink),
    #[deku(id = "17")]
    Rsymlink(Rsymlink),
    #[deku(id = "18")]
    Tmknod(Tmknod),
    #[deku(id = "19")]
    Rmknod(Rmknod),
    #[deku(id = "22")]
    Treadlink(Treadlink),
    #[deku(id = "23")]
    Rreadlink(Rreadlink),
    #[deku(id = "70")]
    Tlink(Tlink),
    #[deku(id = "71")]
    Rlink(Rlink),
    #[deku(id = "20")]
    Trename(Trename),
    #[deku(id = "21")]
    Rrename(Rrename),
    #[deku(id = "74")]
    Trenameat(Trenameat),
    #[deku(id = "75")]
    Rrenameat(Rrenameat),
    #[deku(id = "76")]
    Tunlinkat(Tunlinkat),
    #[deku(id = "77")]
    Runlinkat(Runlinkat),
    #[deku(id = "50")]
    Tfsync(Tfsync),
    #[deku(id = "51")]
    Rfsync(Rfsync),
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
}

// Complete 9P message with header
#[derive(Debug, Clone, DekuRead, DekuWrite)]
pub struct P9Message {
    #[deku(endian = "little")]
    pub size: u32,
    pub type_: u8,
    #[deku(endian = "little")]
    pub tag: u16,
    #[deku(ctx = "*type_")]
    pub body: Message,
}

impl P9Message {
    pub fn to_bytes(&self) -> Result<Vec<u8>, DekuError> {
        use deku::prelude::*;
        use std::io::Cursor;

        // First serialize without the size field
        let mut temp = self.clone();
        temp.size = 0;
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let mut writer = Writer::new(&mut cursor);
        temp.to_writer(&mut writer, ())?;
        drop(writer);

        // Update the size field with the actual size
        let size = bytes.len() as u32;
        bytes[0..4].copy_from_slice(&size.to_le_bytes());

        Ok(bytes)
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
            Message::Tlcreate(_) => 14,
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
            Message::Tmkdir(_) => 72,
            Message::Rmkdir(_) => 73,
            Message::Tsymlink(_) => 16,
            Message::Rsymlink(_) => 17,
            Message::Tmknod(_) => 18,
            Message::Rmknod(_) => 19,
            Message::Treadlink(_) => 22,
            Message::Rreadlink(_) => 23,
            Message::Tlink(_) => 70,
            Message::Rlink(_) => 71,
            Message::Trename(_) => 20,
            Message::Rrename(_) => 21,
            Message::Trenameat(_) => 74,
            Message::Rrenameat(_) => 75,
            Message::Tunlinkat(_) => 76,
            Message::Runlinkat(_) => 77,
            Message::Tfsync(_) => 50,
            Message::Rfsync(_) => 51,
            Message::Rlerror(_) => 7,
            Message::Tflush(_) => 108,
            Message::Rflush(_) => 109,
            Message::Txattrwalk(_) => 30,
            Message::Rxattrwalk(_) => 31,
            Message::Tstatfs(_) => 8,
            Message::Rstatfs(_) => 9,
        };

        Self {
            size: 0, // Will be calculated during serialization
            type_,
            tag,
            body,
        }
    }

    pub fn error(tag: u16, ecode: u32) -> Self {
        Self::new(tag, Message::Rlerror(Rlerror { ecode }))
    }
}
