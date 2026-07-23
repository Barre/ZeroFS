use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::{KeyCodec, ParsedKey};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use tracing::warn;

/// Reserved cookie values
/// 0 is reserved for "start from beginning" (not a valid entry cookie)
pub const COOKIE_DOT: u64 = 1;
pub const COOKIE_DOTDOT: u64 = 2;
/// First cookie value for regular entries
pub const COOKIE_FIRST_ENTRY: u64 = 3;

/// Value stored in directory scan entries.
/// For entries with nlink=1, we embed the full inode to avoid separate lookups.
/// For hardlinked entries (nlink>1), we store just a reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirScanValue {
    /// Full inode embedded: used when nlink == 1
    WithInode { inode_id: InodeId, inode: Inode },
    /// Reference only: used when nlink > 1 (hardlinks)
    Reference { inode_id: InodeId },
}

impl DirScanValue {
    pub fn inode_id(&self) -> InodeId {
        match self {
            DirScanValue::WithInode { inode_id, .. } => *inode_id,
            DirScanValue::Reference { inode_id } => *inode_id,
        }
    }

    pub fn into_inode(self) -> Option<Inode> {
        match self {
            DirScanValue::WithInode { inode, .. } => Some(inode),
            DirScanValue::Reference { .. } => None,
        }
    }
}

#[derive(Serialize)]
enum DirScanValueRef<'a> {
    WithInode { inode_id: InodeId, inode: &'a Inode },
    Reference { inode_id: InodeId },
}

/// Encode directory scan entry value: name + DirScanValue
fn encode_dir_scan_value(name: &[u8], value: &DirScanValueRef) -> Bytes {
    let value_bytes =
        bincode::serialize(value).expect("DirScanValue serialization should not fail");
    let mut buf = Vec::with_capacity(4 + name.len() + value_bytes.len());
    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
    buf.extend_from_slice(name);
    buf.extend_from_slice(&value_bytes);
    Bytes::from(buf)
}

/// Decode directory scan entry value: returns (name, DirScanValue)
fn decode_dir_scan_value(data: &[u8]) -> Result<(Vec<u8>, DirScanValue), FsError> {
    if data.len() < 4 {
        return Err(FsError::InvalidData);
    }
    let name_len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    if data.len() < 4 + name_len {
        return Err(FsError::InvalidData);
    }
    let name = data[4..4 + name_len].to_vec();
    let value: DirScanValue =
        bincode::deserialize(&data[4 + name_len..]).map_err(|_| FsError::InvalidData)?;
    Ok((name, value))
}

#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    pub name: Vec<u8>,
    pub inode_id: InodeId,
    pub cookie: u64,
    /// Embedded inode if available (None for hardlinked entries)
    pub inode: Option<Inode>,
}

const ENTRY_CACHE_CAPACITY: usize = 64 * 1024;

#[derive(Clone)]
pub struct DirectoryStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    /// Cache of (dir_id, name) -> (entry inode id, cookie), sparing
    /// `update_inode_in_entry` a database point-get on every file write. A
    /// cookie is immutable for the life of its entry, so coherence only
    /// requires that inserts and invalidations are ordered against entry
    /// mutations; both happen exclusively under the entry inode's lock (see
    /// `update_inode_in_entry`). Unlocked readers (the prelock lookups in
    /// remove/rename) must never populate this cache: they can read
    /// pre-commit state and would re-insert a value another task just
    /// invalidated, defeating the under-lock re-verification.
    entry_cache: foyer::Cache<(InodeId, Bytes), (InodeId, u64)>,
}

impl DirectoryStore {
    pub fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>) -> Self {
        let entry_cache = foyer::CacheBuilder::new(ENTRY_CACHE_CAPACITY)
            .with_name("zerofs-dir-entry-cache")
            .build();
        Self {
            db,
            key_codec,
            entry_cache,
        }
    }

    pub async fn get(&self, dir_id: InodeId, name: &[u8]) -> Result<InodeId, FsError> {
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);

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
        txn: &mut Transaction,
    ) -> Result<u64, FsError> {
        let current = self.read_cookie(dir_id).await?;
        self.stage_cookie_increment(dir_id, current, txn);
        Ok(current)
    }

    /// Read the next directory cookie without staging a mutation. Callers that
    /// overlap this point read with other validation must still hold the same
    /// directory lock as `allocate_cookie` before staging the increment.
    pub(crate) async fn read_cookie(&self, dir_id: InodeId) -> Result<u64, FsError> {
        let counter_key = self.key_codec.dir_cookie_counter_key(dir_id);
        let current = match self.db.get_bytes(&counter_key).await {
            Ok(Some(data)) => KeyCodec::decode_counter(&data)?,
            Ok(None) => COOKIE_FIRST_ENTRY,
            Err(e) => {
                warn!("Failed to get cookie counter for dir {}: {:?}", dir_id, e);
                return Err(FsError::IoError);
            }
        };
        Ok(current)
    }

    pub(crate) fn stage_cookie_increment(
        &self,
        dir_id: InodeId,
        current: u64,
        txn: &mut Transaction,
    ) {
        let counter_key = self.key_codec.dir_cookie_counter_key(dir_id);
        txn.put_bytes(&counter_key, KeyCodec::encode_counter(current + 1));
    }

    pub async fn exists(&self, dir_id: InodeId, name: &[u8]) -> Result<bool, FsError> {
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);

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
        let prefix = Bytes::from(self.key_codec.dir_scan_prefix(dir_id));
        let codec = self.key_codec.clone();

        let iter = self
            .db
            .scan_prefix(prefix, None, 256 * 1024)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(
            (iter, codec),
            |(mut iter, codec)| async move {
                match iter.next().await {
                    Some(Ok((key, value))) => {
                        let cookie = match codec.parse_key(&key) {
                            ParsedKey::DirScan { cookie } => cookie,
                            _ => return Some((Err(FsError::InvalidData), (iter, codec))),
                        };
                        match decode_dir_scan_value(&value) {
                            Ok((name, scan_value)) => {
                                let inode_id = scan_value.inode_id();
                                Some((
                                    Ok(DirEntryInfo {
                                        name,
                                        inode_id,
                                        cookie,
                                        inode: scan_value.into_inode(),
                                    }),
                                    (iter, codec),
                                ))
                            }
                            Err(e) => Some((Err(e), (iter, codec))),
                        }
                    }
                    Some(Err(_)) => Some((Err(FsError::IoError), (iter, codec))),
                    None => None,
                }
            },
        )))
    }

    pub async fn list_from(
        &self,
        dir_id: InodeId,
        resume_after_cookie: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirEntryInfo, FsError>> + Send + '_>>, FsError>
    {
        let prefix = Bytes::from(self.key_codec.dir_scan_prefix(dir_id));
        let seek_to = self
            .key_codec
            .dir_scan_resume_key(dir_id, resume_after_cookie);
        let codec = self.key_codec.clone();

        let iter = self
            .db
            .scan_prefix(prefix, Some(seek_to), 256 * 1024)
            .await
            .map_err(|_| FsError::IoError)?;

        Ok(Box::pin(futures::stream::unfold(
            (iter, codec),
            |(mut iter, codec)| async move {
                match iter.next().await {
                    Some(Ok((key, value))) => {
                        let cookie = match codec.parse_key(&key) {
                            ParsedKey::DirScan { cookie } => cookie,
                            _ => return Some((Err(FsError::InvalidData), (iter, codec))),
                        };
                        match decode_dir_scan_value(&value) {
                            Ok((name, scan_value)) => {
                                let inode_id = scan_value.inode_id();
                                Some((
                                    Ok(DirEntryInfo {
                                        name,
                                        inode_id,
                                        cookie,
                                        inode: scan_value.into_inode(),
                                    }),
                                    (iter, codec),
                                ))
                            }
                            Err(e) => Some((Err(e), (iter, codec))),
                        }
                    }
                    Some(Err(_)) => Some((Err(FsError::IoError), (iter, codec))),
                    None => None,
                }
            },
        )))
    }

    /// Add a directory entry.
    /// If `inode` is provided, it will be embedded in the scan entry (for nlink=1 entries).
    /// If `inode` is None, only a reference is stored (for hardlinked entries).
    pub fn add(
        &self,
        txn: &mut Transaction,
        dir_id: InodeId,
        name: &[u8],
        entry_id: InodeId,
        cookie: u64,
        inode: Option<&Inode>,
    ) {
        self.entry_cache
            .remove(&(dir_id, Bytes::copy_from_slice(name)));

        let entry_key = self.key_codec.dir_entry_key(dir_id, name);
        txn.put_bytes(&entry_key, KeyCodec::encode_dir_entry(entry_id, cookie));

        let scan_value = match inode {
            Some(inode) => DirScanValueRef::WithInode {
                inode_id: entry_id,
                inode,
            },
            None => DirScanValueRef::Reference { inode_id: entry_id },
        };

        let scan_key = self.key_codec.dir_scan_key(dir_id, cookie);
        txn.put_bytes(&scan_key, encode_dir_scan_value(name, &scan_value));
    }

    pub fn unlink_entry(&self, txn: &mut Transaction, dir_id: InodeId, name: &[u8], cookie: u64) {
        self.entry_cache
            .remove(&(dir_id, Bytes::copy_from_slice(name)));

        let entry_key = self.key_codec.dir_entry_key(dir_id, name);
        txn.delete_bytes(&entry_key);

        let scan_key = self.key_codec.dir_scan_key(dir_id, cookie);
        txn.delete_bytes(&scan_key);
    }

    pub fn delete_directory(&self, txn: &mut Transaction, dir_id: InodeId) {
        let counter_key = self.key_codec.dir_cookie_counter_key(dir_id);
        txn.delete_bytes(&counter_key);
    }

    pub async fn get_entry_with_cookie(
        &self,
        dir_id: InodeId,
        name: &[u8],
    ) -> Result<(InodeId, u64), FsError> {
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);

        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .ok_or(FsError::NotFound)?;

        KeyCodec::decode_dir_entry(&entry_data)
    }

    /// Update the embedded inode in a directory scan entry.
    /// Used when inode attributes change (write, setattr, etc.).
    ///
    /// Every caller holds `inode_id`'s lock, and so does every mutator of the
    /// entry (remove and rename lock the entry's inode), which is what lets
    /// the cookie be served from `entry_cache`: inserts here and the
    /// invalidations in `add`/`unlink_entry` are serialized by that lock. The
    /// cache is only trusted (and only populated) when the entry actually
    /// points at `inode_id`; any mismatch falls back to the database read,
    /// preserving the uncached behavior.
    pub async fn update_inode_in_entry(
        &self,
        txn: &mut Transaction,
        dir_id: InodeId,
        name: &[u8],
        inode_id: InodeId,
        inode: &Inode,
    ) -> Result<(), FsError> {
        let cache_key = (dir_id, Bytes::copy_from_slice(name));
        let cached_cookie = self
            .entry_cache
            .get(&cache_key)
            .map(|entry| *entry.value())
            .filter(|&(entry_id, _)| entry_id == inode_id)
            .map(|(_, cookie)| cookie);

        let cookie = match cached_cookie {
            Some(cookie) => cookie,
            None => {
                let (entry_id, cookie) = self.get_entry_with_cookie(dir_id, name).await?;
                if entry_id == inode_id {
                    self.entry_cache.insert(cache_key, (entry_id, cookie));
                }
                cookie
            }
        };

        let scan_value = DirScanValueRef::WithInode { inode_id, inode };
        let scan_key = self.key_codec.dir_scan_key(dir_id, cookie);
        txn.put_bytes(&scan_key, encode_dir_scan_value(name, &scan_value));

        Ok(())
    }

    /// Convert a directory scan entry to a Reference (for hardlinks).
    /// Used when nlink goes from 1 to 2+.
    pub async fn convert_to_reference(
        &self,
        txn: &mut Transaction,
        dir_id: InodeId,
        name: &[u8],
        inode_id: InodeId,
    ) -> Result<(), FsError> {
        let (_, cookie) = self.get_entry_with_cookie(dir_id, name).await?;
        let scan_value = DirScanValueRef::Reference { inode_id };
        let scan_key = self.key_codec.dir_scan_key(dir_id, cookie);
        txn.put_bytes(&scan_key, encode_dir_scan_value(name, &scan_value));

        Ok(())
    }
}
