use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::{KeyCodec, ParsedKey};
use crate::fs::store::read_cache::{InvalidationGuard, MetadataCache};
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

const ENTRY_CACHE_BYTES: usize = 8 * 1024 * 1024;

type EntryCacheKey = (InodeId, Bytes);
type EntryCacheValue = (InodeId, u64);
type DirectoryEntryCache = MetadataCache<EntryCacheKey, EntryCacheValue>;

fn entry_cache_weight(key: &EntryCacheKey, _: &EntryCacheValue) -> usize {
    std::mem::size_of::<EntryCacheKey>() + key.1.len() + std::mem::size_of::<EntryCacheValue>()
}

#[derive(Clone)]
pub struct DirectoryStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    entry_cache: DirectoryEntryCache,
}

impl DirectoryStore {
    pub fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>) -> Self {
        let entry_cache = DirectoryEntryCache::new(
            db.clone(),
            ENTRY_CACHE_BYTES,
            "zerofs-dir-entry-cache",
            entry_cache_weight,
        );
        Self {
            db,
            key_codec,
            entry_cache,
        }
    }

    pub async fn get(&self, dir_id: InodeId, name: &[u8]) -> Result<InodeId, FsError> {
        self.get_entry_with_cookie(dir_id, name)
            .await
            .map(|(inode_id, _)| inode_id)
    }

    async fn load_entry(&self, dir_id: InodeId, name: &[u8]) -> Result<EntryCacheValue, FsError> {
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);

        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|error| FsError::from_db_error(&error))?
            .ok_or(FsError::NotFound)?;

        KeyCodec::decode_dir_entry(&entry_data)
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
        let cache_key = (dir_id, Bytes::copy_from_slice(name));
        if self.entry_cache.get(&cache_key)?.is_some() {
            return Ok(true);
        }

        // Malformed values still represent existing keys.
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);
        let exists = self
            .db
            .get_bytes(&entry_key)
            .await
            .map(|entry| entry.is_some())
            .map_err(|error| FsError::from_db_error(&error))?;
        self.db.check_serving_authority()?;
        Ok(exists)
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
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);
        txn.put_bytes(&entry_key, KeyCodec::encode_dir_entry(entry_id, cookie));
        txn.invalidate_cached_directory_entry(dir_id, Bytes::copy_from_slice(name));

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
        let entry_key = self.key_codec.dir_entry_key(dir_id, name);
        txn.delete_bytes(&entry_key);
        txn.invalidate_cached_directory_entry(dir_id, Bytes::copy_from_slice(name));

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
        let key = (dir_id, Bytes::copy_from_slice(name));
        self.entry_cache
            .get_or_load(key, || self.load_entry(dir_id, name))
            .await
    }

    pub(crate) fn invalidate_cache(
        &self,
        keys: impl IntoIterator<Item = EntryCacheKey>,
    ) -> InvalidationGuard<EntryCacheKey, EntryCacheValue> {
        self.entry_cache.invalidate(keys)
    }

    #[cfg(test)]
    pub(crate) fn cached_entry(&self, dir_id: InodeId, name: &[u8]) -> Option<(InodeId, u64)> {
        self.entry_cache
            .peek(&(dir_id, Bytes::copy_from_slice(name)))
    }

    #[cfg(test)]
    pub(crate) fn cache_enabled(&self) -> bool {
        self.entry_cache.is_enabled()
    }

    /// Update the embedded inode in a directory scan entry.
    /// Used when inode attributes change (write, setattr, etc.).
    pub async fn update_inode_in_entry(
        &self,
        txn: &mut Transaction,
        dir_id: InodeId,
        name: &[u8],
        inode_id: InodeId,
        inode: &Inode,
    ) -> Result<(), FsError> {
        let (_, cookie) = self.get_entry_with_cookie(dir_id, name).await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::inode::test_file_inode;
    use crate::fs::store::InodeStore;
    use crate::replication::Lease;
    use futures::TryStreamExt;
    use slatedb::config::{PutOptions, WriteOptions};
    use std::time::Duration;

    #[tokio::test]
    async fn commits_invalidate_entries_without_write_through_pollution() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        assert!(fs.directory_store.cache_enabled());

        let mut create = Transaction::new();
        fs.directory_store
            .add(&mut create, 0, b"name", 10, COOKIE_FIRST_ENTRY, None);
        fs.write_coordinator.commit(create).await.unwrap();
        assert_eq!(fs.directory_store.cached_entry(0, b"name"), None);
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((10, COOKIE_FIRST_ENTRY))
        );
        assert_eq!(
            fs.directory_store.cached_entry(0, b"name"),
            Some((10, COOKIE_FIRST_ENTRY))
        );

        let mut replace = Transaction::new();
        fs.directory_store
            .unlink_entry(&mut replace, 0, b"name", COOKIE_FIRST_ENTRY);
        fs.directory_store
            .add(&mut replace, 0, b"name", 20, 7, None);
        fs.write_coordinator.commit(replace).await.unwrap();
        assert_eq!(fs.directory_store.cached_entry(0, b"name"), None);
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((20, 7))
        );

        let mut unlink = Transaction::new();
        fs.directory_store.unlink_entry(&mut unlink, 0, b"name", 7);
        fs.write_coordinator.commit(unlink).await.unwrap();
        assert!(fs.directory_store.cached_entry(0, b"name").is_none());
        assert_eq!(
            fs.directory_store.get(0, b"name").await,
            Err(FsError::NotFound)
        );
    }

    #[tokio::test]
    async fn pre_apply_failure_leaves_the_existing_entry_cache_untouched() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let mut create = Transaction::new();
        fs.directory_store
            .add(&mut create, 0, b"name", 10, COOKIE_FIRST_ENTRY, None);
        fs.write_coordinator.commit(create).await.unwrap();
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((10, COOKIE_FIRST_ENTRY))
        );

        let codec = KeyCodec::new();
        let seg_key = codec.segcount_key(13, 13);
        fs.db
            .put_with_options(
                &seg_key,
                b"bogus",
                &PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        let mut replace = Transaction::new();
        fs.directory_store
            .add(&mut replace, 0, b"name", 99, 9, None);
        replace.add_seg_delta(&seg_key, 1, 1);
        fs.write_coordinator.commit(replace).await.unwrap_err();

        assert_eq!(
            fs.directory_store.cached_entry(0, b"name"),
            Some((10, COOKIE_FIRST_ENTRY))
        );
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((10, COOKIE_FIRST_ENTRY))
        );
    }

    #[tokio::test]
    async fn lookup_and_readdir_agree_after_apply_before_invalidation_ends() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let mut create = Transaction::new();
        fs.directory_store
            .add(&mut create, 0, b"name", 10, COOKIE_FIRST_ENTRY, None);
        fs.write_coordinator.commit(create).await.unwrap();
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((10, COOKIE_FIRST_ENTRY))
        );

        let cache_key = (0, Bytes::from_static(b"name"));
        let cache_guard = fs.directory_store.invalidate_cache([cache_key]);

        let codec = KeyCodec::new();
        let mut batch = slatedb::WriteBatch::new();
        batch.put_bytes(
            codec.dir_entry_key(0, b"name"),
            KeyCodec::encode_dir_entry(20, COOKIE_FIRST_ENTRY),
        );
        batch.put_bytes(
            codec.dir_scan_key(0, COOKIE_FIRST_ENTRY),
            encode_dir_scan_value(b"name", &DirScanValueRef::Reference { inode_id: 20 }),
        );
        fs.db
            .write_with_options(batch, &WriteOptions::default())
            .await
            .unwrap();

        let entries: Vec<_> = fs
            .directory_store
            .list(0)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].inode_id, 20);
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((20, COOKIE_FIRST_ENTRY))
        );
        assert_eq!(
            fs.directory_store.cached_entry(0, b"name"),
            None,
            "loads cannot refill while the database apply is being published"
        );

        drop(cache_guard);
        assert_eq!(
            fs.directory_store.get_entry_with_cookie(0, b"name").await,
            Ok((20, COOKIE_FIRST_ENTRY))
        );
    }

    #[tokio::test]
    async fn exists_checks_raw_presence_without_decoding_the_entry() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let key = KeyCodec::new().dir_entry_key(0, b"corrupt");
        fs.db
            .put_with_options(&key, b"x", &PutOptions::default(), &WriteOptions::default())
            .await
            .unwrap();

        assert_eq!(fs.directory_store.exists(0, b"corrupt").await, Ok(true));
        assert!(
            fs.directory_store
                .get_entry_with_cookie(0, b"corrupt")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cached_metadata_is_refused_after_serving_authority_is_lost() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let raw_db = Arc::new(
            slatedb::DbBuilder::new(
                slatedb::object_store::path::Path::from("cache-authority-test"),
                object_store,
            )
            .build()
            .await
            .unwrap(),
        );
        let lease = Lease::new();
        lease.renew(Duration::from_secs(60));
        let db = Arc::new(Db::new(raw_db, None).with_lease(lease.clone()));
        let codec = Arc::new(KeyCodec::new());
        let inode_store = InodeStore::new(db.clone(), codec.clone(), 2);
        let directory_store = DirectoryStore::new(db.clone(), codec.clone());
        let inode_id = 1;
        let inode = test_file_inode(10);

        db.put_with_options(
            &codec.inode_key(inode_id),
            &bincode::serialize(&inode).unwrap(),
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.put_with_options(
            &codec.dir_entry_key(0, b"cached"),
            &KeyCodec::encode_dir_entry(inode_id, COOKIE_FIRST_ENTRY),
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        assert!(matches!(
            inode_store.get(inode_id).await,
            Ok(Inode::File(file)) if file.size == 10
        ));
        assert_eq!(
            directory_store.get_entry_with_cookie(0, b"cached").await,
            Ok((inode_id, COOKIE_FIRST_ENTRY))
        );
        assert_eq!(
            directory_store.cached_entry(0, b"cached"),
            Some((inode_id, COOKIE_FIRST_ENTRY))
        );

        lease.revoke();

        assert!(matches!(
            inode_store.get(inode_id).await,
            Err(FsError::LeaderLeaseExpired)
        ));
        assert_eq!(
            directory_store.get_entry_with_cookie(0, b"cached").await,
            Err(FsError::LeaderLeaseExpired)
        );
        assert_eq!(
            directory_store.exists(0, b"cached").await,
            Err(FsError::LeaderLeaseExpired)
        );
    }

    #[tokio::test]
    async fn cached_metadata_is_refused_after_database_close() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let raw_db = Arc::new(
            slatedb::DbBuilder::new(
                slatedb::object_store::path::Path::from("cache-close-test"),
                object_store,
            )
            .build()
            .await
            .unwrap(),
        );
        let db = Arc::new(Db::new(raw_db, None));
        let codec = Arc::new(KeyCodec::new());
        let inode_store = InodeStore::new(db.clone(), codec.clone(), 2);
        let directory_store = DirectoryStore::new(db.clone(), codec.clone());
        let inode_id = 1;
        let inode = test_file_inode(10);

        db.put_with_options(
            &codec.inode_key(inode_id),
            &bincode::serialize(&inode).unwrap(),
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.put_with_options(
            &codec.dir_entry_key(0, b"cached"),
            &KeyCodec::encode_dir_entry(inode_id, COOKIE_FIRST_ENTRY),
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        inode_store.get(inode_id).await.unwrap();
        directory_store
            .get_entry_with_cookie(0, b"cached")
            .await
            .unwrap();
        db.close().await.unwrap();

        assert!(matches!(
            inode_store.get(inode_id).await,
            Err(FsError::LeaderLeaseExpired)
        ));
        assert_eq!(
            directory_store.get_entry_with_cookie(0, b"cached").await,
            Err(FsError::LeaderLeaseExpired)
        );
    }
}
