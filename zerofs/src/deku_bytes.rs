use bytes::Bytes;
use deku::ctx::Order;
use deku::prelude::*;
use deku::reader::Reader;
use deku::writer::Writer;
use std::io::{Read, Seek, Write};

#[derive(Debug, Clone, Default)]
pub struct DekuBytes(pub Bytes);

impl DekuBytes {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Bytes> for DekuBytes {
    fn from(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl From<Vec<u8>> for DekuBytes {
    fn from(vec: Vec<u8>) -> Self {
        Self(Bytes::from(vec))
    }
}

impl From<DekuBytes> for Bytes {
    fn from(deku_bytes: DekuBytes) -> Self {
        deku_bytes.0
    }
}

impl AsRef<[u8]> for DekuBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::ops::Deref for DekuBytes {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DekuReader<'a, &u32> for DekuBytes {
    fn from_reader_with_ctx<R: Read + Seek>(
        reader: &mut Reader<R>,
        count: &u32,
    ) -> Result<Self, DekuError> {
        let count = *count as usize;
        let mut buf = vec![0u8; count];

        reader.read_bytes(count, &mut buf, Order::Lsb0)?;
        Ok(Self(Bytes::from(buf)))
    }
}

impl DekuWriter<&u32> for DekuBytes {
    fn to_writer<W: Write + Seek>(
        &self,
        writer: &mut Writer<W>,
        _count: &u32,
    ) -> Result<(), DekuError> {
        writer.write_bytes(&self.0)?;
        Ok(())
    }
}
