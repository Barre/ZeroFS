use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::KeyCodec;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use zerofs_nfsserve::nfs::fileid3;

/// Maximum inode ID (48-bit limit for encoding scheme)
pub const MAX_INODE_ID: u64 = (1u64 << 48) - 1;

/// Maximum hardlinks per inode (16-bit limit for position encoding)
pub const MAX_HARDLINKS_PER_INODE: u32 = u16::MAX as u32;

/// Encoded file ID: High 48 bits = inode ID, Low 16 bits = hardlink position
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EncodedFileId(u64);

impl EncodedFileId {
    pub fn new(inode_id: u64, position: u16) -> Result<Self, FsError> {
        if inode_id > MAX_INODE_ID {
            return Err(FsError::InvalidData);
        }
        Ok(Self((inode_id << 16) | (position as u64)))
    }

    pub fn from_inode(inode_id: u64) -> Result<Self, FsError> {
        Self::new(inode_id, 0)
    }

    pub fn decode(self) -> (u64, u16) {
        let inode = self.0 >> 16;
        let position = (self.0 & 0xFFFF) as u16;
        (inode, position)
    }

    pub fn as_raw(self) -> u64 {
        self.0
    }

    pub fn inode_id(self) -> u64 {
        self.0 >> 16
    }

    pub fn position(self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }
}

impl From<fileid3> for EncodedFileId {
    fn from(id: fileid3) -> Self {
        Self(id)
    }
}

impl From<EncodedFileId> for fileid3 {
    fn from(id: EncodedFileId) -> Self {
        id.0
    }
}

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<EncryptedDb>,
    next_id: Arc<AtomicU64>,
}

impl InodeStore {
    pub fn new(db: Arc<EncryptedDb>, initial_next_id: u64) -> Self {
        Self {
            db,
            next_id: Arc::new(AtomicU64::new(initial_next_id)),
        }
    }

    pub fn allocate(&self) -> Result<InodeId, FsError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        if id > MAX_INODE_ID {
            self.next_id.store(MAX_INODE_ID + 2, Ordering::SeqCst);
            return Err(FsError::NoSpace);
        }

        Ok(id)
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub async fn get(&self, id: InodeId) -> Result<Inode, FsError> {
        let key = KeyCodec::inode_key(id);

        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                tracing::error!(
                    "InodeStore::get({}): database get_bytes failed: {:?}",
                    id,
                    e
                );
                FsError::IoError
            })?
            .ok_or_else(|| {
                tracing::warn!(
                    "InodeStore::get({}): inode key not found in database (key={:?}).",
                    id,
                    key
                );
                FsError::NotFound
            })?;

        bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "InodeStore::get({}): failed to deserialize inode data (len={}): {:?}.",
                id,
                data.len(),
                e
            );
            FsError::InvalidData
        })
    }

    pub fn save(
        &self,
        txn: &mut EncryptedTransaction,
        id: InodeId,
        inode: &Inode,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let key = KeyCodec::inode_key(id);
        let data = bincode::serialize(inode)?;
        txn.put_bytes(&key, Bytes::from(data));
        Ok(())
    }

    pub fn delete(&self, txn: &mut EncryptedTransaction, id: InodeId) {
        let key = KeyCodec::inode_key(id);
        txn.delete_bytes(&key);
    }

    pub fn save_counter(&self, txn: &mut EncryptedTransaction) {
        let key = KeyCodec::system_counter_key();
        let next_id = self.next_id.load(Ordering::SeqCst);
        txn.put_bytes(&key, KeyCodec::encode_counter(next_id));
    }

    #[cfg(test)]
    pub fn set_next_id_for_testing(&self, id: u64) {
        self.next_id.store(id, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoded_file_id() {
        let encoded = EncodedFileId::from(0u64);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 0);
        assert_eq!(pos, 0);

        let encoded = EncodedFileId::new(42, 0).unwrap();
        assert_eq!(encoded.inode_id(), 42);
        assert_eq!(encoded.position(), 0);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 42);
        assert_eq!(pos, 0);

        let encoded = EncodedFileId::new(100, 5).unwrap();
        assert_eq!(encoded.inode_id(), 100);
        assert_eq!(encoded.position(), 5);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 100);
        assert_eq!(pos, 5);

        let encoded = EncodedFileId::new(1000, 65535).unwrap();
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 1000);
        assert_eq!(pos, 65535);

        let encoded = EncodedFileId::new(MAX_INODE_ID, 0).unwrap();
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, MAX_INODE_ID);
        assert_eq!(pos, 0);

        let raw_value = (42u64 << 16) | 5;
        let from_raw = EncodedFileId::from(raw_value);
        assert_eq!(from_raw.inode_id(), 42);
        assert_eq!(from_raw.position(), 5);
        assert_eq!(from_raw.as_raw(), raw_value);
    }

    #[test]
    fn test_encoded_file_id_max_values() {
        let encoded = EncodedFileId::new(MAX_INODE_ID, u16::MAX).unwrap();
        assert_eq!(encoded.inode_id(), MAX_INODE_ID);
        assert_eq!(encoded.position(), u16::MAX);
    }

    #[test]
    fn test_encoded_file_id_inode_overflow() {
        let overflow_id = MAX_INODE_ID + 1;
        let result = EncodedFileId::new(overflow_id, 0);
        assert!(result.is_err());
    }
}
