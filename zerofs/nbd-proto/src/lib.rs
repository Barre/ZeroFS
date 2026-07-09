//! NBD (Network Block Device) wire types and constants.

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

#[derive(Debug, Clone, Copy, PartialEq, DekuRead, DekuWrite)]
#[deku(id_type = "u16", id_endian = "big")]
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
#[deku(id_type = "u32", id_endian = "big")]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_stable<T>(value: &T)
    where
        T: DekuContainerWrite + for<'a> DekuContainerRead<'a>,
    {
        let once = value.to_bytes().unwrap();
        let (_, decoded) = T::from_bytes((&once, 0)).unwrap();
        let twice = decoded.to_bytes().unwrap();
        assert_eq!(once, twice, "encode/decode is not a stable canonical form");
    }

    #[test]
    fn request_round_trips() {
        let request = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: NBD_CMD_FLAG_FUA,
            cmd_type: NBDCommand::Write,
            cookie: 0x0102_0304_0506_0708,
            offset: 4096,
            length: 512,
        };
        let bytes = request.to_bytes().unwrap();
        assert_eq!(&bytes[6..8], &[0, 1]);
        assert_stable(&request);
    }

    #[test]
    fn unknown_command_round_trips_big_endian() {
        let mut bytes = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: NBDCommand::Read,
            cookie: 0,
            offset: 0,
            length: 0,
        }
        .to_bytes()
        .unwrap();
        // cmd_type occupies the two bytes after magic (u32) + flags (u16).
        bytes[6] = 0x12;
        bytes[7] = 0x34;
        let (_, decoded) = NBDRequest::from_bytes((&bytes, 0)).unwrap();
        assert_eq!(decoded.cmd_type, NBDCommand::Unknown(0x1234));
        assert_eq!(decoded.to_bytes().unwrap(), bytes);
    }

    #[test]
    fn reply_types_round_trip() {
        assert_stable(&NBDSimpleReply::new(0x1122_3344_5566_7788, NBD_SUCCESS));
        assert_stable(&NBDOptionReply::new(NBD_OPT_GO, NBD_REP_ACK, 0));
    }

    #[test]
    fn option_header_round_trips() {
        assert_stable(&NBDOptionHeader {
            magic: NBD_IHAVEOPT,
            option: NBD_OPT_GO,
            length: 32,
        });
        assert_eq!(NBDOption::Go.to_bytes().unwrap(), NBD_OPT_GO.to_be_bytes());
    }

    #[test]
    fn short_buffers_error_without_panicking() {
        assert!(NBDRequest::from_bytes((&[0u8; 5], 0)).is_err());
        assert!(NBDOptionHeader::from_bytes((&[0u8; 3], 0)).is_err());
    }

    #[test]
    fn wrong_magic_is_rejected() {
        let mut bytes = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: NBDCommand::Read,
            cookie: 0,
            offset: 0,
            length: 0,
        }
        .to_bytes()
        .unwrap();
        bytes[0] ^= 0xff;
        assert!(NBDRequest::from_bytes((&bytes, 0)).is_err());
    }
}
