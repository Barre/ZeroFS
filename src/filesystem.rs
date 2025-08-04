use crate::cache::{CacheKey, CacheValue, UnifiedCache};
use crate::encryption::{EncryptedDb, EncryptionManager};
use crate::filesystem_stats::{FileSystemGlobalStats, StatsShardData};
use crate::lock_manager::LockManager;
use crate::stats::FileSystemStats;
use bytes::Bytes;
use foyer::{
    DirectFsDeviceOptions, Engine, HybridCacheBuilder, RuntimeOptions, TokioRuntimeOptions,
};
use slatedb::config::ObjectStoreCacheOptions;
use slatedb::db_cache::CachedEntry;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::object_store::{ObjectStore, path::Path};
use slatedb::{
    DbBuilder,
    config::{PutOptions, WriteOptions},
};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Runtime;
use zerofs_nfsserve::nfs::{fileid3, nfsstat3};

use crate::inode::{DirectoryInode, Inode, InodeId};
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

/// Encoded file ID for NFS operations
/// High 48 bits: real inode ID
/// Low 16 bits: position within entries that share the same inode ID (for hardlinks)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EncodedFileId(u64);

impl EncodedFileId {
    /// Create a new encoded file ID from inode and position
    pub fn new(inode_id: u64, position: u16) -> Self {
        Self((inode_id << 16) | (position as u64))
    }

    /// Create an encoded file ID for a regular file (position 0)
    pub fn from_inode(inode_id: u64) -> Self {
        Self::new(inode_id, 0)
    }

    /// Decode into (inode_id, position)
    pub fn decode(self) -> (u64, u16) {
        let inode = self.0 >> 16;
        let position = (self.0 & 0xFFFF) as u16;
        (inode, position)
    }

    /// Get the raw u64 value
    pub fn as_raw(self) -> u64 {
        self.0
    }

    /// Get just the inode ID part
    pub fn inode_id(self) -> u64 {
        self.0 >> 16
    }

    /// Get just the position part
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
pub struct SlateDbFs {
    pub db: Arc<EncryptedDb>,
    pub lock_manager: Arc<LockManager>,
    pub next_inode_id: Arc<AtomicU64>,
    pub metadata_cache: Arc<UnifiedCache>,
    pub small_file_cache: Arc<UnifiedCache>,
    pub dir_entry_cache: Arc<UnifiedCache>,
    pub stats: Arc<FileSystemStats>,
    pub global_stats: Arc<FileSystemGlobalStats>,
    pub last_flush: Arc<Mutex<std::time::Instant>>,
}

// Struct for temporary unencrypted access (only for key management)
// Only use for initial key setup and password changes
pub struct DangerousUnencryptedSlateDbFs {
    pub db: Arc<slatedb::Db>,
}

#[derive(Clone)]
pub struct CacheConfig {
    pub root_folder: String,
    pub max_cache_size_gb: f64,
    pub memory_cache_size_gb: Option<f64>,
}

impl SlateDbFs {
    pub async fn new_with_object_store(
        object_store: Arc<dyn ObjectStore>,
        cache_config: CacheConfig,
        db_path: String,
        encryption_key: [u8; 32],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let slatedb_disk_cache_size_gb = cache_config.max_cache_size_gb;
        let zerofs_memory_cache_gb = cache_config.memory_cache_size_gb.unwrap_or(0.25);

        // SlateDB in-memory block cache: use 1/4 of ZeroFS memory cache size, with min 50MB
        let slatedb_memory_cache_gb = (zerofs_memory_cache_gb * 0.25).max(0.05);

        tracing::info!(
            "Cache allocation - Disk: {:.2}GB (all to SlateDB), Memory: SlateDB block cache: {:.2}GB, ZeroFS cache: {:.2}GB",
            slatedb_disk_cache_size_gb,
            slatedb_memory_cache_gb,
            zerofs_memory_cache_gb
        );

        let slatedb_disk_cache_size_bytes = (slatedb_disk_cache_size_gb * 1_000_000_000.0) as usize;
        let slatedb_memory_cache_bytes = (slatedb_memory_cache_gb * 1_000_000_000.0) as u64;

        tracing::info!(
            "SlateDB in-memory block cache: {} MB",
            slatedb_memory_cache_bytes / 1_000_000
        );
        let slatedb_cache_dir = format!("{}/slatedb", cache_config.root_folder);

        let settings = slatedb::config::Settings {
            object_store_cache_options: ObjectStoreCacheOptions {
                root_folder: Some(slatedb_cache_dir.clone().into()),
                max_cache_size_bytes: Some(slatedb_disk_cache_size_bytes / 2),
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

        let hybrid_cache = Arc::new(FoyerHybridCache::new_with_cache(
            HybridCacheBuilder::new()
                .memory(slatedb_memory_cache_bytes as usize / CHUNK_SIZE)
                .with_weighter(|_, v: &CachedEntry| v.size())
                .storage(Engine::Large)
                .with_runtime_options(RuntimeOptions::Separated {
                    read_runtime_options: TokioRuntimeOptions::default(),
                    write_runtime_options: TokioRuntimeOptions::default(),
                })
                .with_device_options(
                    DirectFsDeviceOptions::new(slatedb_cache_dir)
                        .with_capacity(slatedb_disk_cache_size_bytes / 2), // Half for SlateDB , half for ZeroFS
                )
                .build()
                .await?,
        ));

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
                .with_block_cache(hybrid_cache)
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

        // ZeroFS now uses only in-memory cache, no need for disk cache directory
        let unified_cache = Arc::new(
            UnifiedCache::new(
                "",  // Not used for in-memory cache
                0.0, // Not used for in-memory cache
                cache_config.memory_cache_size_gb,
            )
            .await?,
        );

        let db = Arc::new(
            EncryptedDb::new(slatedb.clone(), encryptor).with_cache(unified_cache.clone()),
        );

        let metadata_cache = unified_cache.clone();
        let small_file_cache = unified_cache.clone();
        let dir_entry_cache = unified_cache.clone();

        let global_stats = Arc::new(FileSystemGlobalStats::new());

        for i in 0..STATS_SHARDS {
            let shard_key = Self::stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await? {
                if let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data) {
                    global_stats.load_shard(i, &shard_data);
                }
            }
        }

        let fs = Self {
            db: db.clone(),
            lock_manager,
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
            metadata_cache,
            small_file_cache,
            dir_entry_cache,
            stats: Arc::new(FileSystemStats::new()),
            global_stats,
            last_flush: Arc::new(Mutex::new(std::time::Instant::now())),
        };

        Ok(fs)
    }

    pub fn inode_key(inode_id: InodeId) -> Bytes {
        Bytes::from(format!("inode:{inode_id}"))
    }

    pub fn chunk_key_by_index(inode_id: InodeId, chunk_index: usize) -> Bytes {
        Bytes::from(format!("chunk:{inode_id}/{chunk_index}"))
    }

    pub fn counter_key() -> Bytes {
        Bytes::from("system:next_inode_id")
    }

    pub fn dir_entry_key(dir_inode_id: InodeId, name: &str) -> Bytes {
        Bytes::from(format!("direntry:{dir_inode_id}/{name}"))
    }

    pub fn dir_scan_key(dir_inode_id: InodeId, entry_inode_id: InodeId, name: &str) -> Bytes {
        Bytes::from(format!(
            "dirscan:{dir_inode_id}/{entry_inode_id:020}/{name}",
        ))
    }

    pub fn dir_scan_prefix(dir_inode_id: InodeId) -> String {
        format!("dirscan:{dir_inode_id}/")
    }

    pub fn tombstone_key(timestamp: u64, inode_id: InodeId) -> Bytes {
        Bytes::from(format!("tombstone:{timestamp}:{inode_id}"))
    }

    pub fn stats_shard_key(shard_id: usize) -> Bytes {
        Bytes::from(format!("stats:shard:{shard_id}"))
    }

    pub async fn allocate_inode(&self) -> Result<InodeId, nfsstat3> {
        let id = self.next_inode_id.fetch_add(1, Ordering::SeqCst);

        // Check if we exceeded the maximum
        if id > MAX_INODE_ID {
            // We've gone over the limit, try to set it back to indicate we're full
            self.next_inode_id.store(MAX_INODE_ID + 2, Ordering::SeqCst);
            return Err(nfsstat3::NFS3ERR_NOSPC);
        }

        Ok(id)
    }

    pub async fn flush(&self) -> Result<(), nfsstat3> {
        let result = self.db.flush().await.map_err(|_| nfsstat3::NFS3ERR_IO);
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

                // Check if it's been 30 seconds since last flush
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

    pub async fn load_inode(&self, inode_id: InodeId) -> Result<Inode, nfsstat3> {
        let cache_key = CacheKey::Metadata(inode_id);
        if let Some(CacheValue::Metadata(cached_inode)) = self.metadata_cache.get(cache_key).await {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((*cached_inode).clone());
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        let key = Self::inode_key(inode_id);
        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let inode: Inode = bincode::deserialize(&data).map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let cache_key = CacheKey::Metadata(inode_id);
        let cache_value = CacheValue::Metadata(Arc::new(inode.clone()));
        self.metadata_cache.insert(cache_key, cache_value, false);

        Ok(inode)
    }

    pub async fn save_inode(&self, inode_id: InodeId, inode: &Inode) -> Result<(), nfsstat3> {
        let key = Self::inode_key(inode_id);
        let data = bincode::serialize(inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;

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
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        self.metadata_cache.remove(CacheKey::Metadata(inode_id));

        if let Inode::File(file) = inode {
            if file.size <= crate::cache::SMALL_FILE_THRESHOLD_BYTES {
                self.small_file_cache.remove(CacheKey::SmallFile(inode_id));
            }
        }

        Ok(())
    }

    pub async fn run_garbage_collection(&self) -> Result<(), nfsstat3> {
        const MAX_CHUNKS_PER_ROUND: usize = 10_000;

        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);

        loop {
            let prefix = Bytes::from("tombstone:");
            let range = prefix.clone()..Bytes::from("tombstone:~");

            let mut tombstones_to_update = Vec::new();
            let mut chunks_deleted_this_round = 0;
            let mut tombstones_completed_this_round = 0;
            let mut found_incomplete_tombstones = false;

            let iter = self
                .db
                .scan(range)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            futures::pin_mut!(iter);

            let mut chunks_remaining_in_round = MAX_CHUNKS_PER_ROUND;

            while let Some(Ok((key, value))) = futures::StreamExt::next(&mut iter).await {
                if chunks_remaining_in_round == 0 {
                    found_incomplete_tombstones = true;
                    break;
                }

                // Parse inode_id from key: "tombstone:<timestamp>:<inode_id>"
                let key_str = String::from_utf8_lossy(&key);
                let parts: Vec<&str> = key_str.split(':').collect();
                if parts.len() != 3 {
                    continue;
                }

                let inode_id = match parts[2].parse::<u64>() {
                    Ok(id) => id,
                    Err(_) => continue,
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
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;

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
                        batch
                            .put_bytes(&key, &actual_remaining.to_le_bytes())
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
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
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

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

        // ZeroFS uses only in-memory cache for tests (100MB)
        let unified_cache = Arc::new(UnifiedCache::new("", 0.0, Some(0.1)).await?);

        let db = Arc::new(
            EncryptedDb::new(slatedb.clone(), encryptor).with_cache(unified_cache.clone()),
        );

        let metadata_cache = unified_cache.clone();
        let small_file_cache = unified_cache.clone();
        let dir_entry_cache = unified_cache.clone();

        let global_stats = Arc::new(FileSystemGlobalStats::new());

        for i in 0..STATS_SHARDS {
            let shard_key = Self::stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await? {
                if let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data) {
                    global_stats.load_shard(i, &shard_data);
                }
            }
        }

        let fs = Self {
            db: db.clone(),
            lock_manager,
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
            metadata_cache,
            small_file_cache,
            dir_entry_cache,
            stats: Arc::new(FileSystemStats::new()),
            global_stats,
            last_flush: Arc::new(Mutex::new(std::time::Instant::now())),
        };

        Ok(fs)
    }
}

impl SlateDbFs {
    /// DANGEROUS: Creates an unencrypted database connection. Only use for key management!
    pub async fn dangerous_new_with_object_store_unencrypted_for_key_management_only(
        object_store: Arc<dyn ObjectStore>,
        cache_config: CacheConfig,
        db_path: String,
    ) -> Result<DangerousUnencryptedSlateDbFs, Box<dyn std::error::Error>> {
        let total_cache_size_gb = cache_config.max_cache_size_gb;

        // Since ZeroFS now uses only in-memory cache, allocate all disk cache to SlateDB
        let slatedb_cache_size_gb = total_cache_size_gb;

        tracing::info!(
            "Cache allocation - Total: {:.2}GB, SlateDB (disk): {:.2}GB, ZeroFS (memory): {:.2}GB",
            total_cache_size_gb,
            slatedb_cache_size_gb,
            cache_config.memory_cache_size_gb.unwrap_or(0.25)
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

        Ok(DangerousUnencryptedSlateDbFs { db: slatedb })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::FileInode;

    #[tokio::test]
    async fn test_create_filesystem() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

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
        let fs = SlateDbFs::new_in_memory().await.unwrap();

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
        let fs = SlateDbFs::new_in_memory().await.unwrap();

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
        assert_eq!(SlateDbFs::inode_key(0), Bytes::from("inode:0"));
        assert_eq!(SlateDbFs::inode_key(42), Bytes::from("inode:42"));
        assert_eq!(SlateDbFs::inode_key(999), Bytes::from("inode:999"));
    }

    #[tokio::test]
    async fn test_chunk_key_generation() {
        assert_eq!(
            SlateDbFs::chunk_key_by_index(1, 0),
            Bytes::from("chunk:1/0")
        );
        assert_eq!(
            SlateDbFs::chunk_key_by_index(42, 10),
            Bytes::from("chunk:42/10")
        );
        assert_eq!(
            SlateDbFs::chunk_key_by_index(999, 999),
            Bytes::from("chunk:999/999")
        );
    }

    #[tokio::test]
    async fn test_load_nonexistent_inode() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let result = fs.load_inode(999).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_max_inode_id_limit() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Set the counter to MAX_INODE_ID (next allocation will get this value)
        fs.next_inode_id.store(MAX_INODE_ID, Ordering::SeqCst);

        // Should be able to allocate one more inode
        let id = fs.allocate_inode().await.unwrap();
        assert_eq!(id, MAX_INODE_ID);

        // Next allocation should fail
        let result = fs.allocate_inode().await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOSPC)));

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
