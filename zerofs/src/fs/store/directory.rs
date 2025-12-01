use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::{KeyCodec, ParsedKey};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;

/// Reserved cookie values
/// 0 is reserved for "start from beginning" (not a valid entry cookie)
pub const COOKIE_DOT: u64 = 1;
pub const COOKIE_DOTDOT: u64 = 2;
/// First cookie value for regular entries
pub const COOKIE_FIRST_ENTRY: u64 = 3;

#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    pub name: Vec<u8>,
    pub inode_id: InodeId,
    pub cookie: u64,
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

        let (inode_id, _cookie) = KeyCodec::decode_dir_entry(&entry_data)?;
        Ok(inode_id)
    }

    pub async fn allocate_cookie(
        &self,
        dir_id: InodeId,
        txn: &mut EncryptedTransaction,
    ) -> Result<u64, FsError> {
        let counter_key = KeyCodec::dir_cookie_counter_key(dir_id);
        let current = match self.db.get_bytes(&counter_key).await {
            Ok(Some(data)) => KeyCodec::decode_counter(&data)?,
            Ok(None) => COOKIE_FIRST_ENTRY,
            Err(_) => return Err(FsError::IoError),
        };
        txn.put_bytes(&counter_key, KeyCodec::encode_counter(current + 1));
        Ok(current)
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
                Some(Ok((key, value))) => {
                    let cookie = match KeyCodec::parse_key(&key) {
                        ParsedKey::DirScan { cookie } => cookie,
                        _ => return Some((Err(FsError::InvalidData), iter)),
                    };
                    match KeyCodec::decode_dir_scan_value(&value) {
                        Ok((inode_id, name)) => Some((
                            Ok(DirEntryInfo {
                                name,
                                inode_id,
                                cookie,
                            }),
                            iter,
                        )),
                        Err(e) => Some((Err(e), iter)),
                    }
                }
                Some(Err(_)) => Some((Err(FsError::IoError), iter)),
                None => None,
            }
        })))
    }

    pub async fn list_from(
        &self,
        dir_id: InodeId,
        resume_after_cookie: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirEntryInfo, FsError>> + Send + '_>>, FsError>
    {
        let start_key = KeyCodec::dir_scan_resume_key(dir_id, resume_after_cookie);
        let end_key = KeyCodec::dir_scan_end_key(dir_id);

        let iter = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(iter, |mut iter| async {
            match iter.next().await {
                Some(Ok((key, value))) => {
                    let cookie = match KeyCodec::parse_key(&key) {
                        ParsedKey::DirScan { cookie } => cookie,
                        _ => return Some((Err(FsError::InvalidData), iter)),
                    };
                    match KeyCodec::decode_dir_scan_value(&value) {
                        Ok((inode_id, name)) => Some((
                            Ok(DirEntryInfo {
                                name,
                                inode_id,
                                cookie,
                            }),
                            iter,
                        )),
                        Err(e) => Some((Err(e), iter)),
                    }
                }
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
        cookie: u64,
    ) {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        txn.put_bytes(&entry_key, KeyCodec::encode_dir_entry(entry_id, cookie));

        let scan_key = KeyCodec::dir_scan_key(dir_id, cookie);
        txn.put_bytes(&scan_key, KeyCodec::encode_dir_scan_value(entry_id, name));
    }

    pub fn unlink_entry(
        &self,
        txn: &mut EncryptedTransaction,
        dir_id: InodeId,
        name: &[u8],
        cookie: u64,
    ) {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        txn.delete_bytes(&entry_key);

        let scan_key = KeyCodec::dir_scan_key(dir_id, cookie);
        txn.delete_bytes(&scan_key);
    }

    pub fn delete_directory(&self, txn: &mut EncryptedTransaction, dir_id: InodeId) {
        let counter_key = KeyCodec::dir_cookie_counter_key(dir_id);
        txn.delete_bytes(&counter_key);
    }

    pub async fn get_entry_with_cookie(
        &self,
        dir_id: InodeId,
        name: &[u8],
    ) -> Result<(InodeId, u64), FsError> {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);

        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .ok_or(FsError::NotFound)?;

        KeyCodec::decode_dir_entry(&entry_data)
    }
}
