pub mod cache;
pub mod errors;
pub mod inode;
pub mod lock_manager;
pub mod metrics;
pub mod operations;
pub mod permissions;
pub mod recovery;
pub mod stats;
pub mod types;

use self::cache::{CacheKey, CacheValue, UnifiedCache};
use self::lock_manager::LockManager;
use self::metrics::FileSystemStats;
use self::stats::{FileSystemGlobalStats, StatsShardData};
use crate::encryption::{EncryptedDb, EncryptionManager};
use bytes::Bytes;
use slatedb::config::ObjectStoreCacheOptions;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::object_store::{ObjectStore, path::Path};
use slatedb::{
    DbBuilder,
    config::{PutOptions, WriteOptions},
};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Runtime;
use zerofs_nfsserve::nfs::fileid3;

use self::errors::FsError;
use self::inode::{DirectoryInode, Inode, InodeId};
use std::time::{SystemTime, UNIX_EPOCH};

pub const PREFIX_INODE: u8 = 0x01;
pub const PREFIX_CHUNK: u8 = 0x02;
pub const PREFIX_DIR_ENTRY: u8 = 0x03;
pub const PREFIX_DIR_SCAN: u8 = 0x04;
pub const PREFIX_TOMBSTONE: u8 = 0x05;
pub const PREFIX_STATS: u8 = 0x06;
pub const PREFIX_SYSTEM: u8 = 0x07;
pub const PREFIX_RECOVERY: u8 = 0x08;

#[derive(Debug)]
pub enum ParsedKey {
    Chunk { inode_id: InodeId, chunk_index: u64 },
    DirScan { entry_id: InodeId, name: String },
    Tombstone { inode_id: InodeId },
}

impl ParsedKey {
    pub fn parse(key: &[u8]) -> Option<Self> {
        if key.is_empty() {
            return None;
        }

        match key[0] {
            PREFIX_CHUNK if key.len() == 17 => {
                let inode_id = u64::from_be_bytes(key[1..9].try_into().ok()?);
                let chunk_index = u64::from_be_bytes(key[9..17].try_into().ok()?);
                Some(ParsedKey::Chunk {
                    inode_id,
                    chunk_index,
                })
            }
            PREFIX_DIR_SCAN if key.len() > 17 => {
                let entry_id = u64::from_be_bytes(key[9..17].try_into().ok()?);
                let name = String::from_utf8(key[17..].to_vec()).ok()?;
                Some(ParsedKey::DirScan { entry_id, name })
            }
            PREFIX_TOMBSTONE if key.len() == 17 => {
                let inode_id = u64::from_be_bytes(key[9..17].try_into().ok()?);
                Some(ParsedKey::Tombstone { inode_id })
            }
            _ => None,
        }
    }
}

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
    pub last_flush: Arc<Mutex<std::time::Instant>>,
}

// DANGEROUS: Temporary unencrypted access only for initial key setup and password changes
pub struct DangerousUnencryptedZeroFS {
    pub db: Arc<slatedb::Db>,
}

#[derive(Clone)]
pub struct CacheConfig {
    pub root_folder: String,
    pub max_cache_size_gb: f64,
    pub memory_cache_size_gb: Option<f64>,
}

impl ZeroFS {
    pub async fn new_with_object_store(
        object_store: Arc<dyn ObjectStore>,
        cache_config: CacheConfig,
        db_path: String,
        encryption_key: [u8; 32],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let total_disk_cache_gb = cache_config.max_cache_size_gb;
        let total_memory_cache_gb = cache_config.memory_cache_size_gb.unwrap_or(0.25);

        let slatedb_object_cache_gb = total_disk_cache_gb;

        // Split memory cache: 50% for ZeroFS, 50% for SlateDB block cache
        let zerofs_memory_cache_gb = total_memory_cache_gb * 0.5;
        let slatedb_memory_cache_gb = total_memory_cache_gb * 0.5;

        tracing::info!(
            "Cache allocation - Total disk: {:.2}GB, SlateDB object store: {:.2}GB, Memory - SlateDB: {:.2}GB, ZeroFS: {:.2}GB",
            total_disk_cache_gb,
            slatedb_object_cache_gb,
            slatedb_memory_cache_gb,
            zerofs_memory_cache_gb
        );

        let slatedb_object_cache_bytes = (slatedb_object_cache_gb * 1_000_000_000.0) as usize;
        let slatedb_memory_cache_bytes = (slatedb_memory_cache_gb * 1_000_000_000.0) as u64;

        tracing::info!(
            "SlateDB in-memory block cache: {} MB",
            slatedb_memory_cache_bytes / 1_000_000
        );
        let slatedb_cache_dir = format!("{}/slatedb", cache_config.root_folder);

        let settings = slatedb::config::Settings {
            object_store_cache_options: ObjectStoreCacheOptions {
                root_folder: Some(slatedb_cache_dir.clone().into()),
                max_cache_size_bytes: Some(slatedb_object_cache_bytes),
                ..Default::default()
            },
            flush_interval: Some(std::time::Duration::from_secs(5)),
            l0_sst_size_bytes: 128 * 1024 * 1024,
            l0_max_ssts: 30,
            max_unflushed_bytes: 512 * 1024 * 1024,
            compactor_options: Some(slatedb::config::CompactorOptions {
                max_sst_size: 256 * 1024 * 1024,
                max_concurrent_compactions: 32,
                ..Default::default()
            }),
            compression_codec: None, // Disable compression - we handle it in encryption layer
            ..Default::default()
        };

        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: slatedb_memory_cache_bytes,
        }));

        let db_path = Path::from(db_path);

        let (runtime_handle, _runtime_keeper) = tokio::task::spawn_blocking(|| {
            let runtime = Runtime::new().unwrap();
            let handle = runtime.handle().clone();

            let runtime_keeper = std::thread::spawn(move || {
                runtime.block_on(async { std::future::pending::<()>().await });
            });

            (handle, runtime_keeper)
        })
        .await?;

        let slatedb = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_gc_runtime(runtime_handle.clone())
                .with_compaction_runtime(runtime_handle.clone())
                .with_block_cache(cache)
                .build()
                .await?,
        );

        let encryptor = Arc::new(EncryptionManager::new(&encryption_key));
        let db = Arc::new(EncryptedDb::new(slatedb.clone(), encryptor.clone()));

        let counter_key = Self::counter_key();
        let next_inode_id = match db.get_bytes(&counter_key).await? {
            Some(data) => {
                let bytes: [u8; 8] = data[..8].try_into().map_err(|_| "Invalid counter data")?;
                u64::from_le_bytes(bytes)
            }
            None => 1,
        };

        let root_inode_key = Self::inode_key(0);
        if db.get_bytes(&root_inode_key).await?.is_none() {
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

        let lock_manager = Arc::new(LockManager::new());

        let zerofs_cache_dir = format!("{}/zerofs", cache_config.root_folder);
        let unified_cache = Arc::new(
            UnifiedCache::new(&zerofs_cache_dir, 0.0, Some(zerofs_memory_cache_gb)).await?,
        );

        let db = Arc::new(
            EncryptedDb::new(slatedb.clone(), encryptor).with_cache(unified_cache.clone()),
        );

        let global_stats = Arc::new(FileSystemGlobalStats::new());

        for i in 0..STATS_SHARDS {
            let shard_key = Self::stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await?
                && let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data)
            {
                global_stats.load_shard(i, &shard_data);
            }
        }

        let mut fs = Self {
            db: db.clone(),
            lock_manager,
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
            cache: unified_cache,
            stats: Arc::new(FileSystemStats::new()),
            global_stats,
            last_flush: Arc::new(Mutex::new(std::time::Instant::now())),
        };

        fs.run_recovery_if_needed().await?;

        Ok(fs)
    }

    pub fn inode_key(inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(9);
        key.push(PREFIX_INODE);
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn chunk_key_by_index(inode_id: InodeId, chunk_index: usize) -> Bytes {
        let mut key = Vec::with_capacity(17);
        key.push(PREFIX_CHUNK);
        key.extend_from_slice(&inode_id.to_be_bytes());
        key.extend_from_slice(&(chunk_index as u64).to_be_bytes());
        Bytes::from(key)
    }

    pub fn counter_key() -> Bytes {
        Bytes::from(vec![PREFIX_SYSTEM, 0x01]) // 0x01 for counter subtype
    }

    pub fn clean_shutdown_key() -> Bytes {
        Bytes::from(vec![PREFIX_RECOVERY])
    }

    pub fn dir_entry_key(dir_inode_id: InodeId, name: &str) -> Bytes {
        let mut key = Vec::with_capacity(9 + name.len());
        key.push(PREFIX_DIR_ENTRY);
        key.extend_from_slice(&dir_inode_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        Bytes::from(key)
    }

    pub fn dir_scan_key(dir_inode_id: InodeId, entry_inode_id: InodeId, name: &str) -> Bytes {
        let mut key = Vec::with_capacity(17 + name.len());
        key.push(PREFIX_DIR_SCAN);
        key.extend_from_slice(&dir_inode_id.to_be_bytes());
        key.extend_from_slice(&entry_inode_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        Bytes::from(key)
    }

    pub fn dir_scan_prefix(dir_inode_id: InodeId) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(9);
        prefix.push(PREFIX_DIR_SCAN);
        prefix.extend_from_slice(&dir_inode_id.to_be_bytes());
        prefix
    }

    pub fn tombstone_key(timestamp: u64, inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(17);
        key.push(PREFIX_TOMBSTONE);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn stats_shard_key(shard_id: usize) -> Bytes {
        let mut key = Vec::with_capacity(9);
        key.push(PREFIX_STATS);
        key.extend_from_slice(&(shard_id as u64).to_be_bytes());
        Bytes::from(key)
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
        let result = self.db.flush().await.map_err(|_| FsError::IoError);
        if result.is_ok() {
            *self.last_flush.lock().unwrap() = std::time::Instant::now();
        }
        result
    }

    pub fn start_auto_flush(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("Starting automatic flush task (flushes every 30 seconds if needed)");
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                let should_flush = {
                    let last_flush = self.last_flush.lock().unwrap();
                    last_flush.elapsed() >= std::time::Duration::from_secs(30)
                };

                if should_flush {
                    match self.flush().await {
                        Ok(_) => {
                            tracing::debug!("Automatic flush completed successfully");
                        }
                        Err(e) => {
                            tracing::error!("Automatic flush failed: {:?}", e);
                        }
                    }
                }
            }
        })
    }

    pub async fn load_inode(&self, inode_id: InodeId) -> Result<Inode, FsError> {
        let cache_key = CacheKey::Metadata(inode_id);
        if let Some(CacheValue::Metadata(cached_inode)) = self.cache.get(cache_key).await {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((*cached_inode).clone());
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        let key = Self::inode_key(inode_id);
        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                tracing::error!("Failed to get inode {}: {:?}", inode_id, e);
                FsError::IoError
            })?
            .ok_or(FsError::NotFound)?;

        let inode: Inode = bincode::deserialize(&data)?;

        let cache_key = CacheKey::Metadata(inode_id);
        let cache_value = CacheValue::Metadata(Arc::new(inode.clone()));
        self.cache.insert(cache_key, cache_value, false).await;

        Ok(inode)
    }

    pub async fn save_inode(&self, inode_id: InodeId, inode: &Inode) -> Result<(), FsError> {
        let key = Self::inode_key(inode_id);
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

        let mut keys_to_remove = vec![CacheKey::Metadata(inode_id)];

        if let Inode::File(file) = inode
            && file.size <= self::cache::SMALL_FILE_THRESHOLD_BYTES
        {
            keys_to_remove.push(CacheKey::SmallFile(inode_id));
        }

        self.cache.remove_batch(keys_to_remove).await;

        Ok(())
    }

    pub async fn run_garbage_collection(&self) -> Result<(), FsError> {
        const MAX_CHUNKS_PER_ROUND: usize = 10_000;

        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);

        loop {
            // Binary tombstone key prefix
            let prefix = vec![PREFIX_TOMBSTONE];
            let end_prefix = vec![PREFIX_TOMBSTONE + 1];
            let range = Bytes::from(prefix.clone())..Bytes::from(end_prefix);

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
                let inode_id = match ParsedKey::parse(&key) {
                    Some(ParsedKey::Tombstone { inode_id }) => inode_id,
                    _ => continue,
                };

                if value.len() != 8 {
                    panic!(
                        "Corrupted tombstone found for inode {}: expected 8 bytes, got {}",
                        inode_id,
                        value.len()
                    );
                }

                let size_bytes: [u8; 8] = value.as_ref().try_into().unwrap();
                let size = u64::from_le_bytes(size_bytes);

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

                let mut batch = self.db.new_write_batch();
                for chunk_idx in start_chunk..total_chunks {
                    let chunk_key = Self::chunk_key_by_index(inode_id, chunk_idx);
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
                let mut batch = self.db.new_write_batch();

                for (key, _inode_id, old_size, start_chunk, delete_tombstone) in
                    tombstones_to_update
                {
                    if delete_tombstone {
                        batch.delete_bytes(&key);
                    } else {
                        let remaining_chunks = start_chunk;
                        let remaining_size = (remaining_chunks as u64) * (CHUNK_SIZE as u64);
                        let actual_remaining = remaining_size.min(old_size);
                        batch.put_bytes(&key, &actual_remaining.to_le_bytes());
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
    pub async fn new_in_memory() -> Result<Self, Box<dyn std::error::Error>> {
        // Use a fixed test key for in-memory tests
        let test_key = [0u8; 32];
        Self::new_in_memory_with_encryption(test_key).await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_encryption(
        encryption_key: [u8; 32],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);

        let settings = slatedb::config::Settings {
            compression_codec: None, // Disable compression - we handle it in encryption layer
            compactor_options: Some(slatedb::config::CompactorOptions {
                max_concurrent_compactions: 32,
                ..Default::default()
            }),
            ..Default::default()
        };

        // For tests, use 50MB cache
        let test_cache_bytes = 50_000_000u64;
        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: test_cache_bytes,
        }));

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_block_cache(cache)
                .build()
                .await?,
        );

        let encryptor = Arc::new(EncryptionManager::new(&encryption_key));
        let db = Arc::new(EncryptedDb::new(slatedb.clone(), encryptor.clone()));

        let next_inode_id = 1;

        let root_inode_key = Self::inode_key(0);
        if db.get_bytes(&root_inode_key).await?.is_none() {
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

        let lock_manager = Arc::new(LockManager::new());

        // For tests, use memory-only cache (no disk backing needed)
        let unified_cache = Arc::new(UnifiedCache::new("", 0.0, Some(0.1)).await?);

        let db = Arc::new(
            EncryptedDb::new(slatedb.clone(), encryptor).with_cache(unified_cache.clone()),
        );

        let global_stats = Arc::new(FileSystemGlobalStats::new());

        for i in 0..STATS_SHARDS {
            let shard_key = Self::stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await?
                && let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data)
            {
                global_stats.load_shard(i, &shard_data);
            }
        }

        let fs = Self {
            db: db.clone(),
            lock_manager,
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
            cache: unified_cache,
            stats: Arc::new(FileSystemStats::new()),
            global_stats,
            last_flush: Arc::new(Mutex::new(std::time::Instant::now())),
        };

        Ok(fs)
    }
}

impl ZeroFS {
    /// Helper method to lookup an entry by name in a directory
    pub async fn lookup_by_name(&self, dir_id: u64, name: &str) -> Result<u64, errors::FsError> {
        let entry_key = Self::dir_entry_key(dir_id, name);
        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| errors::FsError::IoError)?
            .ok_or(errors::FsError::NotFound)?;

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        Ok(u64::from_le_bytes(bytes))
    }

    /// DANGEROUS: Creates an unencrypted database connection. Only use for key management!
    pub async fn dangerous_new_with_object_store_unencrypted_for_key_management_only(
        object_store: Arc<dyn ObjectStore>,
        cache_config: CacheConfig,
        db_path: String,
    ) -> Result<DangerousUnencryptedZeroFS, Box<dyn std::error::Error>> {
        let total_cache_size_gb = cache_config.max_cache_size_gb;

        // For key management, all disk cache goes to SlateDB
        let slatedb_cache_size_gb = total_cache_size_gb;

        tracing::info!(
            "Cache allocation for key management - Total: {:.2}GB, SlateDB (disk): {:.2}GB",
            total_cache_size_gb,
            slatedb_cache_size_gb
        );

        let settings = slatedb::config::Settings {
            ..Default::default()
        };

        let unencrypted_cache_bytes = 250_000_000u64;
        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: unencrypted_cache_bytes,
        }));

        let slatedb = Arc::new(
            DbBuilder::new(Path::from(db_path), object_store)
                .with_settings(settings)
                .with_block_cache(cache)
                .build()
                .await?,
        );

        Ok(DangerousUnencryptedZeroFS { db: slatedb })
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
            parent: 0,
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
        // Test binary key format: [PREFIX_INODE | inode_id(8 bytes BE)]
        let key0 = ZeroFS::inode_key(0);
        assert_eq!(key0[0], PREFIX_INODE);
        assert_eq!(&key0[1..9], &0u64.to_be_bytes());

        let key42 = ZeroFS::inode_key(42);
        assert_eq!(key42[0], PREFIX_INODE);
        assert_eq!(&key42[1..9], &42u64.to_be_bytes());

        let key999 = ZeroFS::inode_key(999);
        assert_eq!(key999[0], PREFIX_INODE);
        assert_eq!(&key999[1..9], &999u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_chunk_key_generation() {
        // Test binary key format: [PREFIX_CHUNK | inode_id(8 bytes BE) | chunk_index(8 bytes BE)]
        let key = ZeroFS::chunk_key_by_index(1, 0);
        assert_eq!(key[0], PREFIX_CHUNK);
        assert_eq!(&key[1..9], &1u64.to_be_bytes());
        assert_eq!(&key[9..17], &0u64.to_be_bytes());

        let key = ZeroFS::chunk_key_by_index(42, 10);
        assert_eq!(key[0], PREFIX_CHUNK);
        assert_eq!(&key[1..9], &42u64.to_be_bytes());
        assert_eq!(&key[9..17], &10u64.to_be_bytes());

        let key = ZeroFS::chunk_key_by_index(999, 999);
        assert_eq!(key[0], PREFIX_CHUNK);
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
}
