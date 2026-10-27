//! Conversions between owned messages and borrowed wire views.

use super::*;

macro_rules! borrow_field {
    ($v:expr, (str)) => {
        $v.as_ref()
    };
    ($v:expr, (bytes $count:ident)) => {
        $v.as_ref()
    };
    ($v:expr, (names $count:ident)) => {
        slice_codec::Names::Owned($v)
    };
    ($v:expr, (qids $count:ident)) => {
        slice_codec::QidList::Values($v)
    };
    ($v:expr, $ty:ty) => {
        *$v
    };
}

macro_rules! own_field {
    ($v:ident, $frame:ident, (str)) => {
        P9String::new($frame.slice_ref($v))
    };
    ($v:ident, $frame:ident, (bytes $count:ident)) => {
        P9Bytes::from($frame.slice_ref($v))
    };
    ($v:ident, $frame:ident, (names $count:ident)) => {
        $v.iter()
            .map(|s| s.map(|s| P9String::new($frame.slice_ref(s))))
            .collect::<Result<Vec<_>, _>>()?
    };
    ($v:ident, $frame:ident, (qids $count:ident)) => {
        $v.iter().collect()
    };
    ($v:ident, $frame:ident, $ty:ty) => {
        $v
    };
}

macro_rules! owned_adapters {
    ($($variant:ident, $owned:ident, $id:ident, $mutation:literal, { $($field:ident: $kind:tt),* });* $(;)?) => {
        impl Message {
            pub fn as_view(&self) -> slice_codec::Message<'_> {
                match self {
                    $(Self::$variant($owned { $($field),* }) => slice_codec::Message::$variant {
                        $($field: borrow_field!($field, $kind)),*
                    },)*
                }
            }

            pub(super) fn from_view(view: slice_codec::Message<'_>, frame: &Bytes) -> Result<Self, CodecError> {
                Ok(match view {
                    $(slice_codec::Message::$variant { $($field),* } => Self::$variant($owned {
                        $($field: own_field!($field, frame, $kind)),*
                    }),)*
                })
            }
        }
    };
}
crate::wire_messages::for_each_message!(owned_adapters);

impl DirEntry {
    pub fn to_slice(&self, output: &mut [u8]) -> Result<usize, CodecError> {
        slice_codec::encode_entry(
            output,
            slice_codec::DirectoryEntry {
                qid: self.qid,
                offset: self.offset,
                type_: self.type_,
                name: self.name.as_ref(),
                stat: None,
            },
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        let mut output = alloc::vec![0; self.wire_size()];
        self.to_slice(&mut output)?;
        Ok(output)
    }

    pub(super) fn decode(frame: &Bytes) -> Result<(Self, usize), CodecError> {
        let (entry, length) = slice_codec::decode_entry(frame, false)?;
        Ok((
            Self {
                qid: entry.qid,
                offset: entry.offset,
                type_: entry.type_,
                name: P9String::new(frame.slice_ref(entry.name)),
            },
            length,
        ))
    }
}

impl DirEntryPlus {
    pub fn to_slice(&self, output: &mut [u8]) -> Result<usize, CodecError> {
        slice_codec::encode_entry(
            output,
            slice_codec::DirectoryEntry {
                qid: self.qid,
                offset: self.offset,
                type_: self.type_,
                name: self.name.as_ref(),
                stat: Some(self.stat),
            },
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, CodecError> {
        let mut output = alloc::vec![0; self.wire_size()];
        self.to_slice(&mut output)?;
        Ok(output)
    }

    pub(super) fn decode(frame: &Bytes) -> Result<(Self, usize), CodecError> {
        let (entry, length) = slice_codec::decode_entry(frame, true)?;
        Ok((
            Self {
                qid: entry.qid,
                offset: entry.offset,
                type_: entry.type_,
                name: P9String::new(frame.slice_ref(entry.name)),
                stat: entry.stat.expect("attribute record"),
            },
            length,
        ))
    }
}
