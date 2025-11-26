pub mod cache;
pub mod errors;
pub mod flush_coordinator;
pub mod inode;
pub mod key_codec;
pub mod lock_manager;
pub mod metrics;
pub mod operations;
pub mod permissions;
pub mod stats;
pub mod types;
pub mod write_coordinator;

use self::cache::{CacheKey, CacheValue, UnifiedCache};
use self::flush_coordinator::FlushCoordinator;
use self::key_codec::{KeyCodec, ParsedKey};
use self::lock_manager::LockManager;
use self::metrics::FileSystemStats;
use self::stats::{FileSystemGlobalStats, StatsShardData};
use self::write_coordinator::WriteCoordinator;
use crate::encryption::{EncryptedDb, EncryptedWriteBatch, EncryptionManager};
use slatedb::config::{PutOptions, WriteOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use zerofs_nfsserve::nfs::fileid3;

pub use self::write_coordinator::SequenceGuard;

use self::errors::FsError;
use self::inode::{DirectoryInode, Inode, InodeId};
use std::time::{SystemTime, UNIX_EPOCH};

fn get_current_uid_gid() -> (u32, u32) {
    (0, 0)
}

pub fn get_current_time() -> (u64, u32) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs(), now.subsec_nanos())
}

pub const CHUNK_SIZE: usize = 256 * 1024;
pub const STATS_SHARDS: usize = 100;

// Maximum hardlinks per inode - limited by our encoding scheme (16 bits for position)
pub const MAX_HARDLINKS_PER_INODE: u32 = u16::MAX as u32;
// Maximum inode ID - limited by our encoding scheme (48 bits for inode ID)
pub const MAX_INODE_ID: u64 = (1u64 << 48) - 1;

// Encoded file ID for NFS operations: High 48 bits = inode ID, Low 16 bits = position for hardlinks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EncodedFileId(u64);

impl EncodedFileId {
    pub fn new(inode_id: u64, position: u16) -> Self {
        Self((inode_id << 16) | (position as u64))
    }

    pub fn from_inode(inode_id: u64) -> Self {
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
pub struct ZeroFS {
    pub db: Arc<EncryptedDb>,
    pub lock_manager: Arc<LockManager>,
    pub next_inode_id: Arc<AtomicU64>,
    pub cache: Arc<UnifiedCache>,
    pub stats: Arc<FileSystemStats>,
    pub global_stats: Arc<FileSystemGlobalStats>,
    flush_coordinator: FlushCoordinator,
    pub write_coordinator: Arc<WriteCoordinator>,
    pub max_bytes: u64,
}

#[derive(Clone)]
pub struct CacheConfig {
    pub root_folder: String,
    pub max_cache_size_gb: f64,
    pub memory_cache_size_gb: Option<f64>,
}

impl ZeroFS {
    pub async fn new_with_slatedb(
        slatedb: crate::encryption::SlateDbHandle,
        encryption_key: [u8; 32],
        max_bytes: u64,
    ) -> anyhow::Result<Self> {
        let encryptor = Arc::new(EncryptionManager::new(&encryption_key));

        let lock_manager = Arc::new(LockManager::new());

        // Cache is only active in read-write mode. In read-only mode, DbReader reads from WAL
        // between checkpoints but we have no way to invalidate the cache when WAL entries appear.
        let is_read_write = matches!(slatedb, crate::encryption::SlateDbHandle::ReadWrite(_));
        let unified_cache = Arc::new(UnifiedCache::new(is_read_write)?);

        let db = Arc::new(match slatedb {
            crate::encryption::SlateDbHandle::ReadWrite(db) => EncryptedDb::new(db, encryptor),
            crate::encryption::SlateDbHandle::ReadOnly(reader) => {
                EncryptedDb::new_read_only(reader, encryptor)
            }
        });

        let counter_key = KeyCodec::system_counter_key();
        let next_inode_id = match db.get_bytes(&counter_key).await? {
            Some(data) => KeyCodec::decode_counter(&data)?,
            None => 1,
        };

        let root_inode_key = KeyCodec::inode_key(0);
        if db.get_bytes(&root_inode_key).await?.is_none() {
            if db.is_read_only() {
                return Err(anyhow::anyhow!(
                    "Cannot initialize filesystem in read-only mode. Root inode does not exist."
                ));
            }

            let (uid, gid) = get_current_uid_gid();
            let (now_sec, now_nsec) = get_current_time();
            let root_dir = DirectoryInode {
                mtime: now_sec,
                mtime_nsec: now_nsec,
                ctime: now_sec,
                ctime_nsec: now_nsec,
                atime: now_sec,
                atime_nsec: now_nsec,
                mode: 0o1777,
                uid,
                gid,
                entry_count: 0,
                parent: 0,
                nlink: 2, // . and ..
            };
            let serialized = bincode::serialize(&Inode::Directory(root_dir))?;
            db.put_with_options(
                &root_inode_key,
                &serialized,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await?;
        }

        let global_stats = Arc::new(FileSystemGlobalStats::new());

        for i in 0..STATS_SHARDS {
            let shard_key = KeyCodec::stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await?
                && let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data)
            {
                global_stats.load_shard(i, &shard_data);
            }
        }

        let flush_coordinator = FlushCoordinator::new(db.clone());
        let write_coordinator = Arc::new(WriteCoordinator::new());

        let fs = Self {
            db: db.clone(),
            lock_manager,
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
            cache: unified_cache,
            stats: Arc::new(FileSystemStats::new()),
            global_stats,
            flush_coordinator,
            write_coordinator,
            max_bytes,
        };

        Ok(fs)
    }

    pub async fn allocate_inode(&self) -> Result<InodeId, FsError> {
        let id = self.next_inode_id.fetch_add(1, Ordering::SeqCst);

        if id > MAX_INODE_ID {
            self.next_inode_id.store(MAX_INODE_ID + 2, Ordering::SeqCst);
            return Err(FsError::NoSpace);
        }

        Ok(id)
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        self.flush_coordinator.flush().await
    }

    /// Allocate a sequence number for ordered commits.
    /// Must be called FIRST before any operation-specific work.
    pub fn allocate_sequence(&self) -> SequenceGuard {
        self.write_coordinator.allocate_sequence()
    }

    pub fn new_write_batch(&self) -> Result<EncryptedWriteBatch, FsError> {
        self.db
            .new_write_batch()
            .map_err(|_| FsError::ReadOnlyFilesystem)
    }

    pub async fn commit_batch_ordered(
        &self,
        mut batch: EncryptedWriteBatch,
        seq_guard: &mut SequenceGuard,
    ) -> Result<(), FsError> {
        seq_guard.wait_for_predecessors().await;

        let counter_key = KeyCodec::system_counter_key();
        let next_id = self.next_inode_id.load(Ordering::SeqCst);
        batch.put_bytes(&counter_key, KeyCodec::encode_counter(next_id));

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        seq_guard.mark_committed();
        Ok(())
    }

    pub async fn load_inode(&self, inode_id: InodeId) -> Result<Inode, FsError> {
        let cache_key = CacheKey::Metadata(inode_id);
        if let Some(CacheValue::Metadata(cached_inode)) = self.cache.get(cache_key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((*cached_inode).clone());
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        let key = KeyCodec::inode_key(inode_id);
        let data = self.db.get_bytes(&key).await.map_err(|e| {
            tracing::error!(
                "load_inode({}): database get_bytes failed: {:?}",
                inode_id,
                e
            );
            FsError::IoError
        })?;

        let data = match data {
            Some(d) => d,
            None => {
                tracing::warn!(
                    "load_inode({}): inode key not found in database (key={:?}).",
                    inode_id,
                    key
                );
                return Err(FsError::NotFound);
            }
        };

        let inode: Inode = bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "load_inode({}): failed to deserialize inode data (len={}): {:?}.",
                inode_id,
                data.len(),
                e
            );
            FsError::InvalidData
        })?;

        let cache_key = CacheKey::Metadata(inode_id);
        let cache_value = CacheValue::Metadata(Arc::new(inode.clone()));
        self.cache.insert(cache_key, cache_value);

        Ok(inode)
    }

    pub async fn save_inode(&self, inode_id: InodeId, inode: &Inode) -> Result<(), FsError> {
        let key = KeyCodec::inode_key(inode_id);
        let data = bincode::serialize(inode)?;

        self.db
            .put_with_options(
                &key,
                &data,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        self.cache.remove(CacheKey::Metadata(inode_id));

        Ok(())
    }

    pub async fn run_garbage_collection(&self) -> Result<(), FsError> {
        const MAX_CHUNKS_PER_ROUND: usize = 10_000;

        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);

        loop {
            let (start, end) = KeyCodec::prefix_range(key_codec::KeyPrefix::Tombstone);
            let range = start..end;

            let mut tombstones_to_update = Vec::new();
            let mut chunks_deleted_this_round = 0;
            let mut tombstones_completed_this_round = 0;
            let mut found_incomplete_tombstones = false;

            let iter = self.db.scan(range).await.map_err(|_| FsError::IoError)?;
            futures::pin_mut!(iter);

            let mut chunks_remaining_in_round = MAX_CHUNKS_PER_ROUND;

            while let Some(Ok((key, value))) = futures::StreamExt::next(&mut iter).await {
                if chunks_remaining_in_round == 0 {
                    found_incomplete_tombstones = true;
                    break;
                }

                // Parse tombstone key
                let inode_id = match KeyCodec::parse_key(&key) {
                    ParsedKey::Tombstone { inode_id } => inode_id,
                    _ => continue,
                };

                // Safe to unwrap: tombstone values are always 8 bytes written by encode_tombstone_size
                let size = KeyCodec::decode_tombstone_size(&value)
                    .expect("tombstone size should always be valid 8-byte value");

                if size == 0 {
                    // No chunks left, just delete the tombstone
                    tombstones_to_update.push((key, inode_id, 0, 0, true));
                    continue;
                }

                let total_chunks = size.div_ceil(CHUNK_SIZE as u64) as usize;
                let chunks_to_delete = total_chunks.min(chunks_remaining_in_round);
                let start_chunk = total_chunks.saturating_sub(chunks_to_delete);

                let is_final_batch = chunks_to_delete == total_chunks;
                if !is_final_batch {
                    found_incomplete_tombstones = true;
                }
                tombstones_to_update.push((key, inode_id, size, start_chunk, is_final_batch));

                let mut batch = self
                    .db
                    .new_write_batch()
                    .map_err(|_| FsError::ReadOnlyFilesystem)?;
                for chunk_idx in start_chunk..total_chunks {
                    let chunk_key = KeyCodec::chunk_key(inode_id, chunk_idx as u64);
                    batch.delete_bytes(&chunk_key);
                }

                if chunks_to_delete > 0 {
                    self.db
                        .write_with_options(
                            batch,
                            &WriteOptions {
                                await_durable: false,
                            },
                        )
                        .await
                        .map_err(|_| FsError::IoError)?;

                    chunks_deleted_this_round += chunks_to_delete;
                    chunks_remaining_in_round -= chunks_to_delete;

                    if is_final_batch {
                        tombstones_completed_this_round += 1;
                    }

                    if chunks_deleted_this_round % 1000 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }

            if !tombstones_to_update.is_empty() {
                let mut batch = self
                    .db
                    .new_write_batch()
                    .map_err(|_| FsError::ReadOnlyFilesystem)?;

                for (key, _inode_id, old_size, start_chunk, delete_tombstone) in
                    tombstones_to_update
                {
                    if delete_tombstone {
                        batch.delete_bytes(&key);
                    } else {
                        let remaining_chunks = start_chunk;
                        let remaining_size = (remaining_chunks as u64) * (CHUNK_SIZE as u64);
                        let actual_remaining = remaining_size.min(old_size);
                        batch.put_bytes(&key, KeyCodec::encode_tombstone_size(actual_remaining));
                    }
                }

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| FsError::IoError)?;

                self.stats
                    .tombstones_processed
                    .fetch_add(tombstones_completed_this_round, Ordering::Relaxed);
            }

            if chunks_deleted_this_round > 0 || tombstones_completed_this_round > 0 {
                self.stats
                    .gc_chunks_deleted
                    .fetch_add(chunks_deleted_this_round as u64, Ordering::Relaxed);

                tracing::debug!(
                    "GC: processed {} tombstones, deleted {} chunks",
                    tombstones_completed_this_round,
                    chunks_deleted_this_round,
                );
            }

            if !found_incomplete_tombstones {
                break;
            }

            tokio::task::yield_now().await;
        }

        Ok(())
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        // Use a fixed test key for in-memory tests
        let test_key = [0u8; 32];
        Self::new_in_memory_with_encryption(test_key).await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_encryption(encryption_key: [u8; 32]) -> anyhow::Result<Self> {
        use slatedb::DbBuilder;
        use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
        use slatedb::object_store::path::Path;

        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(object_store);

        let settings = slatedb::config::Settings {
            wal_enabled: false,
            compression_codec: None, // Disable compression - we handle it in encryption layer
            compactor_options: Some(slatedb::config::CompactorOptions {
                ..Default::default()
            }),
            ..Default::default()
        };

        let test_cache_bytes = 50_000_000u64;
        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: test_cache_bytes,
        }));

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_memory_cache(cache)
                .build()
                .await?,
        );

        Self::new_with_slatedb(
            crate::encryption::SlateDbHandle::ReadWrite(slatedb),
            encryption_key,
            crate::config::FilesystemConfig::DEFAULT_MAX_BYTES,
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory_read_only(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        encryption_key: [u8; 32],
    ) -> anyhow::Result<Self> {
        use arc_swap::ArcSwap;
        use slatedb::DbReader;
        use slatedb::config::DbReaderOptions;
        use slatedb::object_store::path::Path;

        let db_path = Path::from("test_slatedb");
        let reader = Arc::new(
            DbReader::open(db_path, object_store, None, DbReaderOptions::default()).await?,
        );

        Self::new_with_slatedb(
            crate::encryption::SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
            encryption_key,
            crate::config::FilesystemConfig::DEFAULT_MAX_BYTES,
        )
        .await
    }
}

impl ZeroFS {
    /// Helper method to lookup an entry by name in a directory
    pub async fn lookup_by_name(&self, dir_id: u64, name: &[u8]) -> Result<u64, errors::FsError> {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| errors::FsError::IoError)?
            .ok_or(errors::FsError::NotFound)?;

        KeyCodec::decode_dir_entry(&entry_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::inode::FileInode;

    #[tokio::test]
    async fn test_create_filesystem() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let root_inode = fs.load_inode(0).await.unwrap();
        match root_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.mode, 0o1777);
                let (expected_uid, expected_gid) = get_current_uid_gid();
                assert_eq!(dir.uid, expected_uid);
                assert_eq!(dir.gid, expected_gid);
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Root should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_allocate_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let inode1 = fs.allocate_inode().await.unwrap();
        let inode2 = fs.allocate_inode().await.unwrap();
        let inode3 = fs.allocate_inode().await.unwrap();

        assert_ne!(inode1, 0);
        assert_ne!(inode2, 0);
        assert_ne!(inode3, 0);
        assert_ne!(inode1, inode2);
        assert_ne!(inode2, inode3);
        assert_ne!(inode1, inode3);
    }

    #[tokio::test]
    async fn test_save_and_load_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let file_inode = FileInode {
            size: 1024,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: Some(0),
            nlink: 1,
        };

        let inode = Inode::File(file_inode.clone());
        let inode_id = fs.allocate_inode().await.unwrap();

        fs.save_inode(inode_id, &inode).await.unwrap();

        let loaded_inode = fs.load_inode(inode_id).await.unwrap();
        match loaded_inode {
            Inode::File(f) => {
                assert_eq!(f.size, file_inode.size);
                assert_eq!(f.mtime, file_inode.mtime);
                assert_eq!(f.ctime, file_inode.ctime);
                assert_eq!(f.mode, file_inode.mode);
                assert_eq!(f.uid, file_inode.uid);
                assert_eq!(f.gid, file_inode.gid);
            }
            _ => panic!("Expected File inode"),
        }
    }

    #[tokio::test]
    async fn test_inode_key_generation() {
        use crate::fs::key_codec::{KeyCodec, KeyPrefix};
        // Test binary key format: [PREFIX_INODE | inode_id(8 bytes BE)]
        let key0 = KeyCodec::inode_key(0);
        assert_eq!(key0[0], u8::from(KeyPrefix::Inode));
        assert_eq!(&key0[1..9], &0u64.to_be_bytes());

        let key42 = KeyCodec::inode_key(42);
        assert_eq!(key42[0], u8::from(KeyPrefix::Inode));
        assert_eq!(&key42[1..9], &42u64.to_be_bytes());

        let key999 = KeyCodec::inode_key(999);
        assert_eq!(key999[0], u8::from(KeyPrefix::Inode));
        assert_eq!(&key999[1..9], &999u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_chunk_key_generation() {
        use crate::fs::key_codec::{KeyCodec, KeyPrefix};
        // Test binary key format: [PREFIX_CHUNK | inode_id(8 bytes BE) | chunk_index(8 bytes BE)]
        let key = KeyCodec::chunk_key(1, 0);
        assert_eq!(key[0], u8::from(KeyPrefix::Chunk));
        assert_eq!(&key[1..9], &1u64.to_be_bytes());
        assert_eq!(&key[9..17], &0u64.to_be_bytes());

        let key = KeyCodec::chunk_key(42, 10);
        assert_eq!(key[0], u8::from(KeyPrefix::Chunk));
        assert_eq!(&key[1..9], &42u64.to_be_bytes());
        assert_eq!(&key[9..17], &10u64.to_be_bytes());

        let key = KeyCodec::chunk_key(999, 999);
        assert_eq!(key[0], u8::from(KeyPrefix::Chunk));
        assert_eq!(&key[1..9], &999u64.to_be_bytes());
        assert_eq!(&key[9..17], &999u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_load_nonexistent_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let result = fs.load_inode(999).await;
        match result {
            Err(FsError::NotFound) => {} // Expected
            other => panic!("Expected NFS3ERR_NOENT, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_max_inode_id_limit() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Set the counter to MAX_INODE_ID (next allocation will get this value)
        fs.next_inode_id.store(MAX_INODE_ID, Ordering::SeqCst);

        // Should be able to allocate one more inode
        let id = fs.allocate_inode().await.unwrap();
        assert_eq!(id, MAX_INODE_ID);

        // Next allocation should fail
        let result = fs.allocate_inode().await;
        assert!(matches!(result, Err(FsError::NoSpace)));

        // Verify the counter is set to indicate we're full
        assert!(fs.next_inode_id.load(Ordering::SeqCst) > MAX_INODE_ID);
    }

    #[test]
    fn test_dir_entry_encoding() {
        // Test case 1: Root directory (0)
        let encoded = EncodedFileId::from(0u64);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 0);
        assert_eq!(pos, 0);

        // Test case 2: Regular inode with position 0
        let encoded = EncodedFileId::new(42, 0);
        assert_eq!(encoded.inode_id(), 42);
        assert_eq!(encoded.position(), 0);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 42);
        assert_eq!(pos, 0);

        // Test case 3: Hardlink with position
        let encoded = EncodedFileId::new(100, 5);
        assert_eq!(encoded.inode_id(), 100);
        assert_eq!(encoded.position(), 5);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 100);
        assert_eq!(pos, 5);

        // Test case 4: Maximum position
        let encoded = EncodedFileId::new(1000, 65535);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, 1000);
        assert_eq!(pos, 65535);

        // Test case 5: Large inode ID (near max)
        let large_id = MAX_INODE_ID;
        let encoded = EncodedFileId::new(large_id, 0);
        let (inode, pos) = encoded.decode();
        assert_eq!(inode, large_id);
        assert_eq!(pos, 0);

        // Test case 6: Converting from raw u64
        let raw_value = (42u64 << 16) | 5; // Should be 2752517
        let from_raw = EncodedFileId::from(raw_value);
        assert_eq!(from_raw.inode_id(), 42);
        assert_eq!(from_raw.position(), 5);
        assert_eq!(from_raw.as_raw(), raw_value);
    }

    #[tokio::test]
    async fn test_read_only_mode_operations() {
        use slatedb::DbBuilder;
        use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
        use slatedb::object_store::path::Path;

        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(object_store);

        let test_key = [0u8; 32];
        let settings = slatedb::config::Settings {
            wal_enabled: false,
            compression_codec: None,
            compactor_options: Some(slatedb::config::CompactorOptions {
                ..Default::default()
            }),
            ..Default::default()
        };

        let test_cache_bytes = 50_000_000u64;
        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: test_cache_bytes,
        }));

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path.clone(), object_store.clone())
                .with_settings(settings)
                .with_memory_cache(cache)
                .build()
                .await
                .unwrap(),
        );

        let fs_rw = ZeroFS::new_with_slatedb(
            crate::encryption::SlateDbHandle::ReadWrite(slatedb),
            test_key,
            crate::config::FilesystemConfig::DEFAULT_MAX_BYTES,
        )
        .await
        .unwrap();

        let test_inode_id = fs_rw.allocate_inode().await.unwrap();
        let file_inode = FileInode {
            size: 2048,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: Some(0),
            nlink: 1,
        };
        fs_rw
            .save_inode(test_inode_id, &Inode::File(file_inode.clone()))
            .await
            .unwrap();

        fs_rw.flush().await.unwrap();
        drop(fs_rw);

        let fs_ro = ZeroFS::new_in_memory_read_only(object_store, test_key)
            .await
            .unwrap();

        let root_inode = fs_ro.load_inode(0).await.unwrap();
        assert!(matches!(root_inode, Inode::Directory(_)));

        let loaded_inode = fs_ro.load_inode(test_inode_id).await.unwrap();
        match loaded_inode {
            Inode::File(f) => {
                assert_eq!(f.size, file_inode.size);
                assert_eq!(f.mode, file_inode.mode);
            }
            _ => panic!("Expected File inode"),
        }

        let new_file = FileInode {
            size: 100,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: Some(0),
            nlink: 1,
        };
        let result = fs_ro
            .save_inode(test_inode_id, &Inode::File(new_file))
            .await;
        assert!(result.is_err(), "save_inode should fail in read-only mode");

        let mut modified_file = file_inode.clone();
        modified_file.size = 4096;
        let result = fs_ro
            .save_inode(test_inode_id, &Inode::File(modified_file))
            .await;
        assert!(
            result.is_err(),
            "modifying inode should fail in read-only mode"
        );
    }
}
