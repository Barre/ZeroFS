pub mod errors;
pub mod filter_policy;
pub mod flush_coordinator;
pub mod gc;
pub mod inode;
pub mod key_codec;
pub mod lock_manager;
pub mod metrics;
pub mod permissions;
pub mod stats;
pub mod store;
pub mod tracing;
pub mod types;
pub mod write_coordinator;

mod boot;
mod handle;
mod ops;
#[cfg(test)]
mod test_util;

use self::flush_coordinator::FlushCoordinator;
use self::lock_manager::KeyedLockManager;
use self::metrics::FileSystemStats;
use self::stats::FileSystemGlobalStats;
use self::store::{DirectoryStore, ExtentStore, InodeStore, OrphanStore, TombstoneStore};
use self::tracing::AccessTracer;
use self::write_coordinator::WriteCoordinator;
use crate::db::Db;
use crate::object_trace::ObjectTracer;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub use self::gc::GarbageCollector;
pub use handle::OpenHandle;

use self::errors::FsError;
use self::inode::InodeId;
use ::tracing::warn;
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Owner for nodes the server creates without client credentials (the root
/// inode at format time).
fn get_current_uid_gid() -> (u32, u32) {
    (0, 0)
}

/// Wall-clock now as `(seconds, nanoseconds)` since the Unix epoch; a
/// pre-epoch clock clamps to zero.
pub fn get_current_time() -> (u64, u32) {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(e) => {
            warn!("System time is before UNIX epoch: {:?}", e);
            std::time::Duration::ZERO
        }
    };
    (now.as_secs(), now.subsec_nanos())
}

pub const EXTENT_SIZE: usize = 32 * 1024;
pub const STATS_SHARDS: usize = 100;
pub const SMALL_FILE_TOMBSTONE_THRESHOLD: usize = 10;
pub const NAME_MAX: usize = 255;
/// Total inode capacity reported by statfs. A fixed, signed-safe value (< 2^63):
/// statfs must not report a u64 >= 2^63, because GNU `stat`/`df` render such a
/// count as a negative signed value and miscompute inode usage on the mount.
pub const TOTAL_INODES: u64 = 1 << 48;

/// Names are opaque bytes to the fs; only the [`NAME_MAX`] length is enforced.
pub fn validate_filename(filename: &[u8]) -> Result<(), FsError> {
    if filename.len() > NAME_MAX {
        Err(FsError::NameTooLong)
    } else {
        Ok(())
    }
}
#[derive(Clone)]
pub struct ZeroFS {
    pub db: Arc<Db>,
    pub extent_store: ExtentStore,
    pub directory_store: DirectoryStore,
    pub inode_store: InodeStore,
    pub tombstone_store: TombstoneStore,
    pub orphan_store: OrphanStore,
    /// In-memory, process-global count of open files pinning each inode.
    /// Empty after a restart by construction (no handle survives a crash)
    /// Its only role is to pick the defer-vs-delete branch in `remove`/`rename`
    pub open_handles: Arc<DashMap<InodeId, u64>>,
    /// Reclaim queue: an `OpenHandle` drop that takes a count to zero pushes the
    /// inode here for `start_reclaim_drainer` to reclaim.
    pub reclaim_tx: UnboundedSender<InodeId>,
    /// Receiver, parked until `start_reclaim_drainer` takes it (tests that don't
    /// start the drainer just let sends buffer). `Arc<Mutex<..>>` keeps the
    /// vestigial `ZeroFS: Clone` derive working.
    reclaim_rx: Arc<Mutex<Option<UnboundedReceiver<InodeId>>>>,
    pub lock_manager: Arc<KeyedLockManager<InodeId>>,
    pub stats: Arc<FileSystemStats>,
    pub global_stats: Arc<FileSystemGlobalStats>,
    pub flush_coordinator: FlushCoordinator,
    pub write_coordinator: WriteCoordinator,
    /// When set, a client `fsync`/COMMIT returns without forcing a flush to object
    /// storage; semi-sync replication is relied on for durability. See `client_fsync`.
    pub ignore_fsync: bool,
    /// Durability lineage token (see `client_fsync_verified`). Identifies the current
    /// unbroken durable lineage; set once at bring-up, constant for this process's life.
    /// A `.zerofs4` client carries it, and a verified fsync succeeds only while it is
    /// still live, so a successful fsync implies the client's writes are durable.
    pub lineage_token: u64,
    pub max_bytes: u64,
    pub tracer: AccessTracer,
    /// Traces backend object-store requests (the `otrace` feature). Created in
    /// `Prepared::prepare` and shared with the `TracingObjectStore` wrappers so
    /// the RPC server can stream what the wrappers see.
    pub object_tracer: ObjectTracer,
    /// Idempotency cache for retried non-idempotent ops. Shared across all
    /// connections (a retry may arrive on a new one); on a standby it is also fed
    /// by the replication stream.
    pub dedup: Arc<crate::dedup::DedupCache>,
}

#[derive(Clone)]
pub struct CacheConfig {
    pub root_folder: PathBuf,
    pub max_cache_size_gb: f64,
    pub memory_cache_size_gb: Option<f64>,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::fs::inode::FileInode;

    use crate::fs::inode::Inode;

    #[tokio::test]
    async fn test_allocate_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let inode1 = fs.inode_store.allocate();
        let inode2 = fs.inode_store.allocate();
        let inode3 = fs.inode_store.allocate();

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
            name: Some(b"test.txt".to_vec()),
            nlink: 1,
        };

        let inode = Inode::File(file_inode.clone());
        let inode_id = fs.inode_store.allocate();

        let mut txn = fs.db.new_transaction().unwrap();
        fs.inode_store.save(&mut txn, inode_id, &inode).unwrap();
        fs.write_coordinator.commit(txn).await.unwrap();

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

    #[test]
    fn test_inode_key_generation() {
        use crate::fs::key_codec::{KeyCodec, KeyPrefix};
        let codec = KeyCodec::new();
        // Segmented layout: [b"meta" | PREFIX_INODE | inode_id(8 BE)].
        let ko = codec.kind_offset(KeyPrefix::Inode);
        let io = codec.id_offset(KeyPrefix::Inode);
        for id in [0u64, 42, 999] {
            let key = codec.inode_key(id);
            assert_eq!(key[ko], u8::from(KeyPrefix::Inode));
            assert_eq!(&key[io..io + 8], &id.to_be_bytes());
        }
    }

    #[test]
    fn test_extent_key_generation() {
        use crate::fs::key_codec::{KeyCodec, KeyPrefix};
        let codec = KeyCodec::new();
        // Segmented layout: [b"extent" | PREFIX_EXTENT | inode(8 BE) | index(8 BE)].
        let ko = codec.kind_offset(KeyPrefix::Extent);
        let io = codec.id_offset(KeyPrefix::Extent);
        for (ino, idx) in [(1u64, 0u64), (42, 10), (999, 999)] {
            let key = codec.extent_key(ino, idx);
            assert_eq!(key[ko], u8::from(KeyPrefix::Extent));
            assert_eq!(&key[io..io + 8], &ino.to_be_bytes());
            assert_eq!(&key[io + 8..io + 16], &idx.to_be_bytes());
        }
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
}
