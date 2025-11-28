pub mod cache;
pub mod errors;
pub mod flush_coordinator;
pub mod gc;
pub mod inode;
pub mod key_codec;
pub mod lock_manager;
pub mod metrics;
pub mod operations;
pub mod permissions;
pub mod stats;
pub mod store;
pub mod types;
pub mod write_coordinator;

use self::cache::UnifiedCache;
use self::flush_coordinator::FlushCoordinator;
use self::key_codec::KeyCodec;
use self::lock_manager::LockManager;
use self::metrics::FileSystemStats;
use self::stats::{FileSystemGlobalStats, StatsShardData};
use self::store::{ChunkStore, DirectoryStore, InodeStore, TombstoneStore};
use self::write_coordinator::WriteCoordinator;
use crate::encryption::{EncryptedDb, EncryptedTransaction, EncryptionManager};
use slatedb::config::{PutOptions, WriteOptions};
use std::sync::Arc;

pub use self::gc::GarbageCollector;
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

#[derive(Clone)]
pub struct ZeroFS {
    pub db: Arc<EncryptedDb>,
    pub chunk_store: ChunkStore,
    pub directory_store: DirectoryStore,
    pub inode_store: InodeStore,
    pub tombstone_store: TombstoneStore,
    pub lock_manager: Arc<LockManager>,
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
        let stats = Arc::new(FileSystemStats::new());
        let chunk_store = ChunkStore::new(db.clone());
        let directory_store = DirectoryStore::new(db.clone());
        let inode_store = InodeStore::new(
            db.clone(),
            unified_cache.clone(),
            stats.clone(),
            next_inode_id,
        );
        let tombstone_store = TombstoneStore::new(db.clone());

        let fs = Self {
            db: db.clone(),
            chunk_store,
            directory_store,
            inode_store,
            tombstone_store,
            lock_manager,
            cache: unified_cache,
            stats,
            global_stats,
            flush_coordinator,
            write_coordinator,
            max_bytes,
        };

        Ok(fs)
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        self.flush_coordinator.flush().await
    }

    pub async fn commit_transaction(
        &self,
        mut txn: EncryptedTransaction,
        seq_guard: &mut SequenceGuard,
    ) -> Result<(), FsError> {
        self.inode_store.save_counter(&mut txn);

        let (encrypt_result, _) = tokio::join!(txn.into_inner(), seq_guard.wait_for_predecessors());
        let (inner_batch, _cache_ops) = encrypt_result.map_err(|_| FsError::IoError)?;

        self.db
            .write_raw_batch(
                inner_batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        seq_guard.mark_committed();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::inode::FileInode;
    use crate::fs::store::inode::MAX_INODE_ID;

    #[tokio::test]
    async fn test_create_filesystem() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let root_inode = fs.inode_store.get(0).await.unwrap();
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

        let inode1 = fs.inode_store.allocate().unwrap();
        let inode2 = fs.inode_store.allocate().unwrap();
        let inode3 = fs.inode_store.allocate().unwrap();

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
        let inode_id = fs.inode_store.allocate().unwrap();

        let mut txn = fs.db.new_transaction().unwrap();
        fs.inode_store.save(&mut txn, inode_id, &inode).unwrap();
        let mut seq_guard = fs.write_coordinator.allocate_sequence();
        fs.commit_transaction(txn, &mut seq_guard).await.unwrap();

        let loaded_inode = fs.inode_store.get(inode_id).await.unwrap();
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

        let result = fs.inode_store.get(999).await;
        match result {
            Err(FsError::NotFound) => {} // Expected
            other => panic!("Expected NFS3ERR_NOENT, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_max_inode_id_limit() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        fs.inode_store.set_next_id_for_testing(MAX_INODE_ID);

        let id = fs.inode_store.allocate().unwrap();
        assert_eq!(id, MAX_INODE_ID);

        let result = fs.inode_store.allocate();
        assert!(matches!(result, Err(FsError::NoSpace)));

        assert!(fs.inode_store.next_id() > MAX_INODE_ID);
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

        let test_inode_id = fs_rw.inode_store.allocate().unwrap();
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
        let mut txn = fs_rw.db.new_transaction().unwrap();
        fs_rw
            .inode_store
            .save(&mut txn, test_inode_id, &Inode::File(file_inode.clone()))
            .unwrap();
        let mut seq_guard = fs_rw.write_coordinator.allocate_sequence();
        fs_rw.commit_transaction(txn, &mut seq_guard).await.unwrap();

        fs_rw.flush().await.unwrap();
        drop(fs_rw);

        let fs_ro = ZeroFS::new_in_memory_read_only(object_store, test_key)
            .await
            .unwrap();

        let root_inode = fs_ro.inode_store.get(0).await.unwrap();
        assert!(matches!(root_inode, Inode::Directory(_)));

        let loaded_inode = fs_ro.inode_store.get(test_inode_id).await.unwrap();
        match loaded_inode {
            Inode::File(f) => {
                assert_eq!(f.size, file_inode.size);
                assert_eq!(f.mode, file_inode.mode);
            }
            _ => panic!("Expected File inode"),
        }

        // Verify that creating transactions fails in read-only mode
        let result = fs_ro.db.new_transaction();
        assert!(
            result.is_err(),
            "new_transaction should fail in read-only mode"
        );
    }
}
