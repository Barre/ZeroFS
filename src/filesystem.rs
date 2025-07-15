use bytes::Bytes;
use nfsserve::nfs::nfsstat3;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use slatedb::config::ObjectStoreCacheOptions;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::object_store::{ObjectStore, path::Path};
use slatedb::{
    Db, DbBuilder,
    config::{PutOptions, WriteOptions},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

use crate::inode::{DirectoryInode, Inode, InodeId};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
fn get_current_uid_gid() -> (u32, u32) {
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(not(unix))]
fn get_current_uid_gid() -> (u32, u32) {
    (0, 0)
}

#[cfg(unix)]
pub fn get_umask() -> u32 {
    unsafe {
        let current_umask = libc::umask(0);
        libc::umask(current_umask);
        current_umask as u32
    }
}

#[cfg(not(unix))]
pub fn get_umask() -> u32 {
    0o022
}

pub fn get_current_time() -> (u64, u32) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs(), now.subsec_nanos())
}

pub const CHUNK_SIZE: usize = 64 * 1024;
pub const LOCK_SHARD_COUNT: usize = 1024;

#[derive(Clone)]
pub struct SlateDbFs {
    pub db: Arc<Db>,
    pub inode_locks: Arc<Vec<Arc<Mutex<()>>>>,
    pub next_inode_id: Arc<AtomicU64>,
}

pub struct S3Config {
    pub endpoint: String,
    pub bucket_name: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub allow_http: bool,
}

pub struct CacheConfig {
    pub root_folder: String,
    pub max_cache_size_gb: f64,
}

impl SlateDbFs {
    pub async fn new_with_s3(
        s3_config: S3Config,
        cache_config: CacheConfig,
        db_path: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&s3_config.bucket_name)
            .with_region(&s3_config.region)
            .with_access_key_id(&s3_config.access_key_id)
            .with_secret_access_key(&s3_config.secret_access_key)
            .with_allow_http(s3_config.allow_http)
            .with_conditional_put(S3ConditionalPut::ETagMatch);

        if !s3_config.endpoint.is_empty() {
            builder = builder.with_endpoint(&s3_config.endpoint);
        }

        let object_store = builder.build()?;
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);

        let cache_size_bytes = (cache_config.max_cache_size_gb * 1_000_000_000.0) as usize;

        let settings = slatedb::config::Settings {
            object_store_cache_options: ObjectStoreCacheOptions {
                root_folder: Some(cache_config.root_folder.into()),
                max_cache_size_bytes: Some(cache_size_bytes),
                ..Default::default()
            },
            compactor_options: Some(slatedb::config::CompactorOptions {
                max_concurrent_compactions: 16,
                ..Default::default()
            }),
            ..Default::default()
        };

        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 10_000,
        }));

        let db_path = Path::from(db_path);
        let db: Arc<Db> = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_block_cache(cache)
                .build()
                .await?,
        );

        let counter_key = Self::counter_key();
        let next_inode_id = match db.get(&counter_key).await? {
            Some(data) => {
                let bytes: [u8; 8] = data[..8].try_into().map_err(|_| "Invalid counter data")?;
                u64::from_le_bytes(bytes)
            }
            None => 1,
        };

        let root_inode_key = Self::inode_key(0);
        if db.get(&root_inode_key).await?.is_none() {
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

        let mut locks = Vec::with_capacity(LOCK_SHARD_COUNT);

        for _ in 0..LOCK_SHARD_COUNT {
            locks.push(Arc::new(Mutex::new(())));
        }

        let fs = Self {
            db: db.clone(),
            inode_locks: Arc::new(locks),
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
        };

        Ok(fs)
    }

    pub fn inode_key(inode_id: InodeId) -> Bytes {
        Bytes::from(format!("inode:{}", inode_id))
    }

    pub fn get_inode_lock(&self, inode_id: InodeId) -> Arc<Mutex<()>> {
        let shard = (inode_id as usize) % LOCK_SHARD_COUNT;
        self.inode_locks[shard].clone()
    }

    pub fn chunk_key_by_index(inode_id: InodeId, chunk_index: usize) -> Bytes {
        Bytes::from(format!("chunk:{}/{}", inode_id, chunk_index))
    }

    pub fn counter_key() -> Bytes {
        Bytes::from("system:next_inode_id")
    }

    pub fn dir_entry_key(dir_inode_id: InodeId, name: &str) -> Bytes {
        Bytes::from(format!("direntry:{}/{}", dir_inode_id, name))
    }

    pub fn dir_scan_key(dir_inode_id: InodeId, entry_inode_id: InodeId) -> Bytes {
        Bytes::from(format!("dirscan:{}/{:020}", dir_inode_id, entry_inode_id))
    }

    pub fn dir_scan_prefix(dir_inode_id: InodeId) -> String {
        format!("dirscan:{}/", dir_inode_id)
    }

    pub async fn allocate_inode(&self) -> Result<InodeId, nfsstat3> {
        let id = self.next_inode_id.fetch_add(1, Ordering::SeqCst);
        Ok(id)
    }

    pub async fn load_inode(&self, inode_id: InodeId) -> Result<Inode, nfsstat3> {
        let key = Self::inode_key(inode_id);
        let data = self
            .db
            .get(&key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let inode: Inode = bincode::deserialize(&data).map_err(|_| nfsstat3::NFS3ERR_IO)?;

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

        Ok(())
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self, Box<dyn std::error::Error>> {
        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);

        let settings = slatedb::config::Settings {
            compression_codec: Some(slatedb::config::CompressionCodec::Lz4),
            compactor_options: Some(slatedb::config::CompactorOptions {
                max_concurrent_compactions: 32,
                ..Default::default()
            }),
            ..Default::default()
        };

        let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 500_000,
        }));

        let db_path = Path::from("test_slatedb");
        let db: Arc<Db> = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_block_cache(cache)
                .build()
                .await?,
        );

        let next_inode_id = 1;

        let root_inode_key = Self::inode_key(0);
        if db.get(&root_inode_key).await?.is_none() {
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

        let mut locks = Vec::with_capacity(LOCK_SHARD_COUNT);
        for _ in 0..LOCK_SHARD_COUNT {
            locks.push(Arc::new(Mutex::new(())));
        }

        let fs = Self {
            db: db.clone(),
            inode_locks: Arc::new(locks),
            next_inode_id: Arc::new(AtomicU64::new(next_inode_id)),
        };

        Ok(fs)
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
    async fn test_get_inode_lock() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let lock1 = fs.get_inode_lock(0);
        let lock2 = fs.get_inode_lock(LOCK_SHARD_COUNT as u64);

        assert!(Arc::ptr_eq(&lock1, &lock2));

        let lock3 = fs.get_inode_lock(1);
        let lock4 = fs.get_inode_lock(1 + LOCK_SHARD_COUNT as u64);

        assert!(Arc::ptr_eq(&lock3, &lock4));
    }

    #[tokio::test]
    async fn test_load_nonexistent_inode() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let result = fs.load_inode(999).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }
}
