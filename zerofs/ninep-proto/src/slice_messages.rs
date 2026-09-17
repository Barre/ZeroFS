//! Borrowed messages and their wire encoding.

use super::wire_types::*;
use super::{CodecError, HEADER_SIZE, OP_ENVELOPE_SIZE, Reader, Writer};

/// 9P byte-range lock type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockType {
    ReadLock = 0,
    WriteLock = 1,
    Unlock = 2,
}

impl TryFrom<u8> for LockType {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ReadLock),
            1 => Ok(Self::WriteLock),
            2 => Ok(Self::Unlock),
            _ => Err(CodecError::InvalidLockType),
        }
    }
}

pub(super) trait Fixed: Sized {
    const SIZE: usize;
    fn read(reader: &mut Reader<'_>) -> Result<Self, CodecError>;
    fn write(&self, writer: &mut Writer<'_>) -> Result<(), CodecError>;
}

macro_rules! scalar {
    ($ty:ty, $read:ident, $write:ident) => {
        impl Fixed for $ty {
            const SIZE: usize = core::mem::size_of::<Self>();
            fn read(r: &mut Reader<'_>) -> Result<Self, CodecError> {
                r.$read()
            }
            fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
                w.$write(*self)
            }
        }
    };
}
scalar!(u8, get_u8, put_u8);
scalar!(u16, get_u16, put_u16);
scalar!(u32, get_u32, put_u32);
scalar!(u64, get_u64, put_u64);
impl Fixed for LockType {
    const SIZE: usize = 1;

    fn read(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        r.get_u8()?.try_into()
    }

    fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
        w.put_u8(*self as u8)
    }
}

macro_rules! fixed_struct {
    ($ty:ident { $($field:ident: $kind:ty),* $(,)? }) => {
        impl Fixed for $ty {
            const SIZE: usize = 0 $(+ <$kind as Fixed>::SIZE)*;

            fn read(r: &mut Reader<'_>) -> Result<Self, CodecError> {
                Ok(Self { $($field: <$kind as Fixed>::read(r)?),* })
            }

            fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
                $(self.$field.write(w)?;)*
                Ok(())
            }
        }
    };
}
fixed_struct!(Qid {
    type_: u8,
    version: u32,
    path: u64
});
fixed_struct!(Stat {
    qid: Qid,
    mode: u32,
    uid: u32,
    gid: u32,
    nlink: u64,
    rdev: u64,
    size: u64,
    blksize: u64,
    blocks: u64,
    atime_sec: u64,
    atime_nsec: u64,
    mtime_sec: u64,
    mtime_nsec: u64,
    ctime_sec: u64,
    ctime_nsec: u64,
    btime_sec: u64,
    btime_nsec: u64,
    r#gen: u64,
    data_version: u64
});

/// Walk names in wire storage or in caller-owned input. Iteration never allocates.
#[derive(Debug, Clone, Copy)]
pub enum Names<'a> {
    Encoded {
        bytes: &'a [u8],
        count: u16,
    },
    Slices(&'a [&'a [u8]]),
    #[cfg(all(not(MODULE), feature = "owned"))]
    Owned(&'a [crate::P9String]),
}

impl<'a> Names<'a> {
    fn read(r: &mut Reader<'a>, count: u16) -> Result<Self, CodecError> {
        let start = r.position;
        for _ in 0..count {
            r.get_string()?;
        }
        Ok(Self::Encoded {
            bytes: &r.input[start..r.position],
            count,
        })
    }

    pub fn iter(self) -> impl Iterator<Item = Result<&'a [u8], CodecError>> {
        let mut reader = match self {
            Self::Encoded { bytes, .. } => Reader::new(bytes),
            _ => Reader::new(&[]),
        };
        (0..self.len()).map(move |i| match self {
            Self::Encoded { .. } => reader.get_string(),
            Self::Slices(names) => Ok(names[i]),
            #[cfg(all(not(MODULE), feature = "owned"))]
            Self::Owned(names) => Ok(names[i].as_ref()),
        })
    }

    pub fn len(self) -> usize {
        match self {
            Self::Encoded { count, .. } => count as usize,
            Self::Slices(names) => names.len(),
            #[cfg(all(not(MODULE), feature = "owned"))]
            Self::Owned(names) => names.len(),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    fn size(self) -> Result<usize, CodecError> {
        if self.len() > u16::MAX as usize {
            return Err(CodecError::TooManyNames);
        }
        let size = self.iter().try_fold(0usize, |size, name| {
            size.checked_add(super::string_size(name?)?)
                .ok_or(CodecError::LengthOverflow)
        })?;
        match self {
            Self::Encoded { bytes, .. } if size != bytes.len() => {
                return Err(CodecError::TrailingData);
            }
            _ => {}
        }
        Ok(size)
    }

    fn write(self, w: &mut Writer<'_>) -> Result<(), CodecError> {
        if let Self::Encoded { bytes, .. } = self {
            return w.put(bytes);
        }
        for name in self.iter() {
            w.put_string(name?)?;
        }
        Ok(())
    }
}

/// QIDs borrowed from a frame or from application storage.
#[derive(Debug, Clone, Copy)]
pub enum QidList<'a> {
    Encoded(&'a [u8]),
    Values(&'a [Qid]),
}

impl<'a> QidList<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Encoded(b) => b.len() / Qid::WIRE_SIZE,
            Self::Values(q) => q.len(),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub fn iter(self) -> impl Iterator<Item = Qid> + 'a {
        (0..self.len()).map(move |i| match self {
            Self::Encoded(b) => {
                Qid::read(&mut Reader::new(&b[i * Qid::WIRE_SIZE..])).expect("validated QID list")
            }
            Self::Values(q) => q[i],
        })
    }

    fn size(self) -> Result<usize, CodecError> {
        match self {
            Self::Encoded(bytes) if !bytes.len().is_multiple_of(Qid::WIRE_SIZE) => {
                return Err(CodecError::Truncated);
            }
            _ => {}
        }
        if self.len() > u16::MAX as usize {
            return Err(CodecError::TooManyNames);
        }
        self.len()
            .checked_mul(Qid::WIRE_SIZE)
            .ok_or(CodecError::LengthOverflow)
    }

    fn write(self, w: &mut Writer<'_>) -> Result<(), CodecError> {
        if let Self::Encoded(bytes) = self {
            return w.put(bytes);
        }
        for qid in self.iter() {
            qid.write(w)?;
        }
        Ok(())
    }
}

macro_rules! field_type {
    ((str)) => { &'a [u8] };
    ((bytes $count:ident)) => { &'a [u8] };
    ((names $count:ident)) => { Names<'a> };
    ((qids $count:ident)) => { QidList<'a> };
    ($ty:ty) => { $ty };
}

macro_rules! read_field {
    ($r:ident, (str)) => {
        $r.get_string()?
    };
    ($r:ident, (bytes $count:ident)) => {
        $r.take($count as usize)?
    };
    ($r:ident, (names $count:ident)) => {
        Names::read($r, $count)?
    };
    ($r:ident, (qids $count:ident)) => {
        QidList::Encoded(
            $r.take(
                usize::from($count)
                    .checked_mul(Qid::WIRE_SIZE)
                    .ok_or(CodecError::LengthOverflow)?,
            )?,
        )
    };
    ($r:ident, $ty:ty) => {
        <$ty as Fixed>::read($r)?
    };
}

macro_rules! write_field {
    ($w:ident, $v:ident, (str)) => {
        $w.put_string($v)?
    };
    ($w:ident, $v:ident, (bytes $count:ident)) => {
        $w.put($v)?
    };
    ($w:ident, $v:ident, (names $count:ident)) => {
        $v.write($w)?
    };
    ($w:ident, $v:ident, (qids $count:ident)) => {
        $v.write($w)?
    };
    ($w:ident, $v:ident, $ty:ty) => {
        $v.write($w)?
    };
}

macro_rules! size_field {
    ($v:ident, (str)) => {
        super::string_size($v)?
    };
    ($v:ident, (bytes $count:ident)) => {
        $v.len()
    };
    ($v:ident, (names $count:ident)) => {
        $v.size()?
    };
    ($v:ident, (qids $count:ident)) => {
        $v.size()?
    };
    ($v:ident, $ty:ty) => {{
        let _ = $v;
        <$ty as Fixed>::SIZE
    }};
}

macro_rules! messages {
    ($($variant:ident, $owned:ident, $id:ident, $mutation:literal, { $($field:ident: $kind:tt),* });* $(;)?) => {
        /// A 9P message borrowing its strings and payloads.
        #[derive(Debug, Clone, Copy)]
        pub enum Message<'a> {
            $($variant { $($field: field_type!($kind)),* },)*
        }

        impl<'a> Message<'a> {
            pub fn type_id(&self) -> u8 {
                match self {
                    $(Self::$variant { .. } => message_type::$id,)*
                }
            }

            pub fn body_size(&self) -> Result<usize, CodecError> {
                match self {
                    $(Self::$variant { $($field),* } => {
                        let size = 0usize;
                        $(let size = size.checked_add(size_field!($field, $kind))
                            .ok_or(CodecError::LengthOverflow)?;)*
                        Ok(size)
                    },)*
                }
            }

            fn read(type_: u8, r: &mut Reader<'a>) -> Result<Self, CodecError> {
                match type_ {
                    $(message_type::$id => {
                        $(let $field = read_field!(r, $kind);)*
                        Ok(Self::$variant { $($field),* })
                    },)*
                    other => Err(CodecError::UnexpectedMessageType(other)),
                }
            }

            fn write(&self, w: &mut Writer<'_>) -> Result<(), CodecError> {
                match self {
                    $(Self::$variant { $($field),* } => {
                        $(write_field!(w, $field, $kind);)*
                    },)*
                }
                Ok(())
            }
        }

        pub fn carries_op_id(type_: u8) -> bool {
            match type_ {
                $(message_type::$id => $mutation,)*
                _ => false,
            }
        }
    };
}
super::wire_messages::for_each_message!(messages);

#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    pub header: super::Header,
    pub envelope: MutationEnvelope,
    pub body: Message<'a>,
}

/// Decode one complete frame. Mutation envelopes are enabled only after dialect negotiation.
pub fn decode_frame(
    frame: &[u8],
    max_msize: u32,
    op_id_enabled: bool,
) -> Result<Frame<'_>, CodecError> {
    let header = super::decode_header(frame, max_msize)?;
    if header.size as usize != frame.len() {
        return Err(CodecError::FrameSizeMismatch);
    }
    let mut r = Reader::new(&frame[HEADER_SIZE..]);
    let envelope = if op_id_enabled && carries_op_id(header.type_) {
        MutationEnvelope {
            op_id: r.take(16)?.try_into().map_err(|_| CodecError::Truncated)?,
            flags: r.get_u8()?,
            origin_writer_epoch: r.get_u64()?,
        }
    } else {
        MutationEnvelope::default()
    };
    let body = Message::read(header.type_, &mut r)?;
    r.require_end()?;
    Ok(Frame {
        header,
        envelope,
        body,
    })
}

pub fn encoded_frame_size(body: &Message<'_>, op_id_enabled: bool) -> Result<usize, CodecError> {
    let envelope_len = if op_id_enabled && carries_op_id(body.type_id()) {
        OP_ENVELOPE_SIZE
    } else {
        0
    };
    let body_size = body.body_size()?;
    HEADER_SIZE
        .checked_add(envelope_len)
        .and_then(|n| n.checked_add(body_size))
        .ok_or(CodecError::LengthOverflow)
}

/// Encode directly into caller-owned storage. The message borrows all variable fields.
pub fn encode_frame(
    output: &mut [u8],
    max_msize: u32,
    tag: u16,
    envelope: MutationEnvelope,
    body: &Message<'_>,
    op_id_enabled: bool,
) -> Result<usize, CodecError> {
    let size = encoded_frame_size(body, op_id_enabled)?;
    if size > max_msize as usize || size > u32::MAX as usize {
        return Err(CodecError::MessageTooLarge);
    }
    let mut w = Writer::new(output);
    w.put_u32(size as u32)?;
    w.put_u8(body.type_id())?;
    w.put_u16(tag)?;
    if op_id_enabled && carries_op_id(body.type_id()) {
        super::put_mutation_envelope(&mut w, envelope)?;
    }
    body.write(&mut w)?;
    Ok(w.position)
}

/// A directory record borrowing its name from the payload.
#[derive(Debug, Clone, Copy)]
pub struct DirectoryEntry<'a> {
    pub qid: Qid,
    pub offset: u64,
    pub type_: u8,
    pub name: &'a [u8],
    pub stat: Option<Stat>,
}

pub fn decode_entry(
    data: &[u8],
    with_attributes: bool,
) -> Result<(DirectoryEntry<'_>, usize), CodecError> {
    let mut r = Reader::new(data);
    let qid = Qid::read(&mut r)?;
    let offset = r.get_u64()?;
    let type_ = r.get_u8()?;
    let name = r.get_string()?;
    let stat = if with_attributes {
        Some(Stat::read(&mut r)?)
    } else {
        None
    };
    Ok((
        DirectoryEntry {
            qid,
            offset,
            type_,
            name,
            stat,
        },
        r.position,
    ))
}

pub fn encode_entry(output: &mut [u8], entry: DirectoryEntry<'_>) -> Result<usize, CodecError> {
    let mut w = Writer::new(output);
    entry.qid.write(&mut w)?;
    w.put_u64(entry.offset)?;
    w.put_u8(entry.type_)?;
    w.put_string(entry.name)?;
    if let Some(stat) = entry.stat {
        stat.write(&mut w)?;
    }
    Ok(w.position)
}
