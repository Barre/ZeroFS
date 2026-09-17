//! NBD (Network Block Device) wire types and constants.

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NBDCommand {
    Read,
    Write,
    Disconnect,
    Flush,
    Trim,
    Cache,
    WriteZeroes,
    Unknown(u16),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NBDOption {
    ExportName,
    Abort,
    List,
    Info,
    Go,
    StructuredReply,
}

#[derive(Debug)]
pub struct NBDServerHandshake {
    pub magic: u64,
    pub ihaveopt: u64,
    pub handshake_flags: u16,
}

#[derive(Debug)]
pub struct NBDClientFlags {
    pub flags: u32,
}

#[derive(Debug)]
pub struct NBDOptionHeader {
    pub magic: u64,
    pub option: u32,
    pub length: u32,
}

#[derive(Debug)]
pub struct NBDOptionReply {
    pub magic: u64,
    pub option: u32,
    pub reply_type: u32,
    pub length: u32,
}

#[derive(Debug)]
pub struct NBDExportInfo {
    pub size: u64,
    pub transmission_flags: u16,
}

#[derive(Debug)]
pub struct NBDInfoExport {
    pub info_type: u16,
    pub size: u64,
    pub transmission_flags: u16,
}

#[derive(Debug)]
pub struct NBDRequest {
    pub magic: u32,
    pub flags: u16,
    pub cmd_type: NBDCommand,
    pub cookie: u64,
    pub offset: u64,
    pub length: u32,
}

#[derive(Debug)]
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

/// A malformed or incomplete fixed-width NBD value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    Truncated,
    BufferTooSmall,
    InvalidMagic,
    InvalidOption,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NBD codec error: {self:?}")
    }
}

impl std::error::Error for CodecError {}

/// Big-endian fixed-width codec. Callers may encode into stack or reusable buffers.
pub trait WireCodec: Sized {
    const WIRE_SIZE: usize;
    fn decode(input: &[u8]) -> Result<Self, CodecError>;
    fn encode(&self, output: &mut [u8]) -> Result<usize, CodecError>;
    fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        let mut output = vec![0; Self::WIRE_SIZE];
        self.encode(&mut output)?;
        Ok(output)
    }
}

struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let (bytes, rest) = self.0.split_first_chunk().ok_or(CodecError::Truncated)?;
        self.0 = rest;
        Ok(*bytes)
    }
}

struct Writer<'a> {
    output: &'a mut [u8],
    offset: usize,
}

impl Writer<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .ok_or(CodecError::BufferTooSmall)?;
        self.output
            .get_mut(self.offset..end)
            .ok_or(CodecError::BufferTooSmall)?
            .copy_from_slice(bytes);
        self.offset = end;
        Ok(())
    }
}

trait Scalar: Sized {
    const SIZE: usize;
    fn read(r: &mut Reader<'_>) -> Result<Self, CodecError>;
    fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError>;
}

macro_rules! scalar {
    ($ty:ty) => {
        impl Scalar for $ty {
            const SIZE: usize = std::mem::size_of::<Self>();
            fn read(r: &mut Reader<'_>) -> Result<Self, CodecError> {
                Ok(Self::from_be_bytes(r.take()?))
            }
            fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
                w.put(&self.to_be_bytes())
            }
        }
    };
}
scalar!(u16);
scalar!(u32);
scalar!(u64);
impl Scalar for NBDCommand {
    const SIZE: usize = 2;
    fn read(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(match u16::read(r)? {
            0 => Self::Read,
            1 => Self::Write,
            2 => Self::Disconnect,
            3 => Self::Flush,
            4 => Self::Trim,
            5 => Self::Cache,
            6 => Self::WriteZeroes,
            value => Self::Unknown(value),
        })
    }

    fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
        let value: u16 = match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Disconnect => 2,
            Self::Flush => 3,
            Self::Trim => 4,
            Self::Cache => 5,
            Self::WriteZeroes => 6,
            Self::Unknown(value) => *value,
        };
        value.write(w)
    }
}

impl Scalar for NBDOption {
    const SIZE: usize = 4;
    fn read(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        match u32::read(r)? {
            NBD_OPT_EXPORT_NAME => Ok(Self::ExportName),
            NBD_OPT_ABORT => Ok(Self::Abort),
            NBD_OPT_LIST => Ok(Self::List),
            NBD_OPT_INFO => Ok(Self::Info),
            NBD_OPT_GO => Ok(Self::Go),
            NBD_OPT_STRUCTURED_REPLY => Ok(Self::StructuredReply),
            _ => Err(CodecError::InvalidOption),
        }
    }

    fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
        let value = match self {
            Self::ExportName => NBD_OPT_EXPORT_NAME,
            Self::Abort => NBD_OPT_ABORT,
            Self::List => NBD_OPT_LIST,
            Self::Info => NBD_OPT_INFO,
            Self::Go => NBD_OPT_GO,
            Self::StructuredReply => NBD_OPT_STRUCTURED_REPLY,
        };
        value.write(w)
    }
}

macro_rules! scalar_codec {
    ($ty:ty) => {
        impl WireCodec for $ty {
            const WIRE_SIZE: usize = <Self as Scalar>::SIZE;
            fn decode(input: &[u8]) -> Result<Self, CodecError> {
                Self::read(&mut Reader(input))
            }
            fn encode(&self, output: &mut [u8]) -> Result<usize, CodecError> {
                let mut w = Writer { output, offset: 0 };
                self.write(&mut w)?;
                Ok(w.offset)
            }
        }
    };
}
scalar_codec!(NBDCommand);
scalar_codec!(NBDOption);
macro_rules! fixed_codec {
    ($ty:ident { $($field:ident: $kind:ty $(= $expected:expr)?),* $(,)? }) => {
        impl WireCodec for $ty {
            const WIRE_SIZE: usize = 0 $(+ <$kind as Scalar>::SIZE)*;

            fn decode(input: &[u8]) -> Result<Self, CodecError> {
                let mut r = Reader(input);
                let value = Self { $($field: <$kind as Scalar>::read(&mut r)?),* };
                $($(if value.$field != $expected {
                    return Err(CodecError::InvalidMagic);
                })?)*
                Ok(value)
            }

            fn encode(&self, output: &mut [u8]) -> Result<usize, CodecError> {
                $($(if self.$field != $expected {
                    return Err(CodecError::InvalidMagic);
                })?)*
                let mut w = Writer { output, offset: 0 };
                $(self.$field.write(&mut w)?;)*
                Ok(w.offset)
            }
        }
    };
}

fixed_codec!(NBDServerHandshake {
    magic: u64 = NBD_MAGIC,
    ihaveopt: u64 = NBD_IHAVEOPT,
    handshake_flags: u16,
});
fixed_codec!(NBDClientFlags { flags: u32 });
fixed_codec!(NBDOptionHeader {
    magic: u64 = NBD_IHAVEOPT,
    option: u32,
    length: u32,
});
fixed_codec!(NBDOptionReply {
    magic: u64,
    option: u32,
    reply_type: u32,
    length: u32,
});
fixed_codec!(NBDExportInfo {
    size: u64,
    transmission_flags: u16,
});
fixed_codec!(NBDInfoExport {
    info_type: u16,
    size: u64,
    transmission_flags: u16,
});
fixed_codec!(NBDRequest {
    magic: u32 = NBD_REQUEST_MAGIC,
    flags: u16,
    cmd_type: NBDCommand,
    cookie: u64,
    offset: u64,
    length: u32,
});
fixed_codec!(NBDSimpleReply {
    magic: u32,
    error: u32,
    cookie: u64,
});

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_stable<T>(value: &T)
    where
        T: WireCodec,
    {
        let once = value.to_bytes().unwrap();
        let decoded = T::decode(&once).unwrap();
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
        let decoded = NBDRequest::decode(&bytes).unwrap();
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
        assert!(NBDRequest::decode(&[0u8; 5]).is_err());
        assert!(NBDOptionHeader::decode(&[0u8; 3]).is_err());
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
        assert!(NBDRequest::decode(&bytes).is_err());
    }
}
