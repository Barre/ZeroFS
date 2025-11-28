use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::{KeyCodec, ParsedKey};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    pub name: Vec<u8>,
    pub inode_id: InodeId,
}

#[derive(Clone)]
pub struct DirectoryStore {
    db: Arc<EncryptedDb>,
}

impl DirectoryStore {
    pub fn new(db: Arc<EncryptedDb>) -> Self {
        Self { db }
    }

    pub async fn get(&self, dir_id: InodeId, name: &[u8]) -> Result<InodeId, FsError> {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);

        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .ok_or(FsError::NotFound)?;

        KeyCodec::decode_dir_entry(&entry_data)
    }

    pub async fn exists(&self, dir_id: InodeId, name: &[u8]) -> Result<bool, FsError> {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);

        let result = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(result.is_some())
    }

    pub async fn list(
        &self,
        dir_id: InodeId,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirEntryInfo, FsError>> + Send + '_>>, FsError>
    {
        let start_key = Bytes::from(KeyCodec::dir_scan_prefix(dir_id));
        let end_key = KeyCodec::dir_scan_end_key(dir_id);

        let iter = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(iter, |mut iter| async {
            match iter.next().await {
                Some(Ok((key, _value))) => match KeyCodec::parse_key(&key) {
                    ParsedKey::DirScan { entry_id, name } => Some((
                        Ok(DirEntryInfo {
                            name,
                            inode_id: entry_id,
                        }),
                        iter,
                    )),
                    _ => Some((Err(FsError::InvalidData), iter)),
                },
                Some(Err(_)) => Some((Err(FsError::IoError), iter)),
                None => None,
            }
        })))
    }

    pub async fn list_from(
        &self,
        dir_id: InodeId,
        resume_from: InodeId,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirEntryInfo, FsError>> + Send + '_>>, FsError>
    {
        let start_key = KeyCodec::dir_scan_resume_key(dir_id, resume_from);
        let end_key = KeyCodec::dir_scan_end_key(dir_id);

        let iter = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(iter, |mut iter| async {
            match iter.next().await {
                Some(Ok((key, _value))) => match KeyCodec::parse_key(&key) {
                    ParsedKey::DirScan { entry_id, name } => Some((
                        Ok(DirEntryInfo {
                            name,
                            inode_id: entry_id,
                        }),
                        iter,
                    )),
                    _ => Some((Err(FsError::InvalidData), iter)),
                },
                Some(Err(_)) => Some((Err(FsError::IoError), iter)),
                None => None,
            }
        })))
    }

    pub fn add(
        &self,
        txn: &mut EncryptedTransaction,
        dir_id: InodeId,
        name: &[u8],
        entry_id: InodeId,
    ) {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        txn.put_bytes(&entry_key, KeyCodec::encode_dir_entry(entry_id));

        let scan_key = KeyCodec::dir_scan_key(dir_id, entry_id, name);
        txn.put_bytes(&scan_key, KeyCodec::encode_dir_entry(entry_id));
    }

    pub fn remove(
        &self,
        txn: &mut EncryptedTransaction,
        dir_id: InodeId,
        name: &[u8],
        entry_id: InodeId,
    ) {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        txn.delete_bytes(&entry_key);

        let scan_key = KeyCodec::dir_scan_key(dir_id, entry_id, name);
        txn.delete_bytes(&scan_key);
    }
}
