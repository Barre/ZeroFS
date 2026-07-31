use alloc::vec::Vec;
use bytes::Bytes;
use deku::ctx::Order;
use deku::no_std_io::{Read, Seek, Write};
use deku::prelude::*;
use deku::reader::Reader;
use deku::writer::Writer;

use crate::{ByteStorage, WireBytes};

/// Userspace counted payload. Other environments use [`WireBytes`] directly.
pub type DekuBytes<B = Bytes> = WireBytes<B>;

impl From<Vec<u8>> for WireBytes<Bytes> {
    fn from(vec: Vec<u8>) -> Self {
        Self(Bytes::from(vec))
    }
}

impl From<WireBytes<Bytes>> for Bytes {
    fn from(deku_bytes: WireBytes<Bytes>) -> Self {
        deku_bytes.0
    }
}

const READ_CHUNK_SIZE: usize = 64 * 1024;

impl<'a, B> DekuReader<'a, &u32> for WireBytes<B>
where
    B: ByteStorage + From<Vec<u8>>,
{
    fn from_reader_with_ctx<R: Read + Seek>(
        reader: &mut Reader<R>,
        count: &u32,
    ) -> Result<Self, DekuError> {
        let count = *count as usize;
        // Do not allocate an untrusted length until the input supplies it.
        let mut buf = Vec::with_capacity(count.min(READ_CHUNK_SIZE));
        let mut remaining = count;
        while remaining > 0 {
            let chunk = remaining.min(READ_CHUNK_SIZE);
            let start = buf.len();
            buf.resize(start + chunk, 0);
            reader.read_bytes(chunk, &mut buf[start..], Order::Lsb0)?;
            remaining -= chunk;
        }
        Ok(Self(B::from(buf)))
    }
}

impl<B: ByteStorage> DekuWriter<&u32> for WireBytes<B> {
    fn to_writer<W: Write + Seek>(
        &self,
        writer: &mut Writer<W>,
        _count: &u32,
    ) -> Result<(), DekuError> {
        writer.write_bytes(self.0.as_ref())?;
        Ok(())
    }
}
