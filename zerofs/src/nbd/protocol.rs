use deku::prelude::*;

// NBD Magic numbers
pub const NBD_MAGIC: u64 = 0x4e42444d41474943;
pub const NBD_IHAVEOPT: u64 = 0x49484156454F5054;
pub const NBD_REQUEST_MAGIC: u32 = 0x25609513;
pub const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
pub const NBD_REPLY_MAGIC: u64 = 0x3e889045565a9;

// Handshake flags
pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client flags
pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;
pub const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

// Transmission flags
pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
pub const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
pub const NBD_FLAG_CAN_MULTI_CONN: u16 = 1 << 8;
pub const NBD_FLAG_SEND_CACHE: u16 = 1 << 10;
pub const NBD_FLAG_CAN_FAST_ZERO: u16 = 1 << 11;

// Command flags
pub const NBD_CMD_FLAG_FUA: u16 = 1 << 0;

pub const TRANSMISSION_FLAGS: u16 = NBD_FLAG_HAS_FLAGS
    | NBD_FLAG_SEND_FLUSH
    | NBD_FLAG_SEND_FUA
    | NBD_FLAG_SEND_TRIM
    | NBD_FLAG_SEND_WRITE_ZEROES
    | NBD_FLAG_CAN_MULTI_CONN
    | NBD_FLAG_SEND_CACHE
    | NBD_FLAG_CAN_FAST_ZERO;

pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_LIST: u32 = 3;
pub const NBD_OPT_INFO: u32 = 6;
pub const NBD_OPT_GO: u32 = 7;
pub const NBD_OPT_STRUCTURED_REPLY: u32 = 8;

// Option reply types
pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_SERVER: u32 = 2;
pub const NBD_REP_INFO: u32 = 3;
pub const NBD_REP_ERR_UNSUP: u32 = 0x80000001;
pub const NBD_REP_ERR_INVALID: u32 = 0x80000003;
pub const NBD_REP_ERR_UNKNOWN: u32 = 0x80000006;

// Info types
pub const NBD_INFO_EXPORT: u16 = 0;

// Error codes
pub const NBD_SUCCESS: u32 = 0;
pub const NBD_EIO: u32 = 5;
pub const NBD_EINVAL: u32 = 22;
pub const NBD_ENOSPC: u32 = 28;

// Protocol sizes
pub const NBD_EXPORT_NAME_PADDING: usize = 124;
pub const NBD_OPTION_HEADER_SIZE: usize = 16;
pub const NBD_REQUEST_HEADER_SIZE: usize = 28;

// Server configuration
pub const NBD_READDIR_DEFAULT_LIMIT: usize = 1000;
pub const NBD_ZERO_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, DekuRead, DekuWrite)]
#[deku(id_type = "u16", endian = "big")]
pub enum NBDCommand {
    #[deku(id = "0")]
    Read,
    #[deku(id = "1")]
    Write,
    #[deku(id = "2")]
    Disconnect,
    #[deku(id = "3")]
    Flush,
    #[deku(id = "4")]
    Trim,
    #[deku(id = "5")]
    Cache,
    #[deku(id = "6")]
    WriteZeroes,
    #[deku(id_pat = "_")]
    Unknown(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, DekuRead, DekuWrite)]
#[deku(id_type = "u32")]
pub enum NBDOption {
    #[deku(id = "NBD_OPT_EXPORT_NAME")]
    ExportName,
    #[deku(id = "NBD_OPT_ABORT")]
    Abort,
    #[deku(id = "NBD_OPT_LIST")]
    List,
    #[deku(id = "NBD_OPT_INFO")]
    Info,
    #[deku(id = "NBD_OPT_GO")]
    Go,
    #[deku(id = "NBD_OPT_STRUCTURED_REPLY")]
    StructuredReply,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDServerHandshake {
    #[deku(assert_eq = "NBD_MAGIC")]
    pub magic: u64,
    #[deku(assert_eq = "NBD_IHAVEOPT")]
    pub ihaveopt: u64,
    pub handshake_flags: u16,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDClientFlags {
    pub flags: u32,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDOptionHeader {
    #[deku(assert_eq = "NBD_IHAVEOPT")]
    pub magic: u64,
    pub option: u32,
    pub length: u32,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDOptionReply {
    pub magic: u64,
    pub option: u32,
    pub reply_type: u32,
    pub length: u32,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDExportInfo {
    pub size: u64,
    pub transmission_flags: u16,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDInfoExport {
    pub info_type: u16,
    pub size: u64,
    pub transmission_flags: u16,
}

#[derive(Debug, DekuRead, DekuWrite)]
pub struct NBDRequest {
    #[deku(endian = "big", assert_eq = "NBD_REQUEST_MAGIC")]
    pub magic: u32,
    #[deku(endian = "big")]
    pub flags: u16,
    pub cmd_type: NBDCommand,
    #[deku(endian = "big")]
    pub cookie: u64,
    #[deku(endian = "big")]
    pub offset: u64,
    #[deku(endian = "big")]
    pub length: u32,
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct NBDSimpleReply {
    pub magic: u32,
    pub error: u32,
    pub cookie: u64,
}

impl NBDServerHandshake {
    pub fn new(flags: u16) -> Self {
        Self {
            magic: NBD_MAGIC,
            ihaveopt: NBD_IHAVEOPT,
            handshake_flags: flags,
        }
    }
}

impl NBDOptionReply {
    pub fn new(option: u32, reply_type: u32, length: u32) -> Self {
        Self {
            magic: NBD_REPLY_MAGIC,
            option,
            reply_type,
            length,
        }
    }
}

impl NBDSimpleReply {
    pub fn new(cookie: u64, error: u32) -> Self {
        Self {
            magic: NBD_SIMPLE_REPLY_MAGIC,
            error,
            cookie,
        }
    }
}

impl NBDExportInfo {
    pub fn new(size: u64, flags: u16) -> Self {
        Self {
            size,
            transmission_flags: flags,
        }
    }
}
