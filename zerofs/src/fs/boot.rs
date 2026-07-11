//! Construction and durability surface: the slatedb and in-memory
//! constructors, lineage-token bring-up, and the client fsync entry points.

use crate::db::{Db, SlateDbHandle};
use crate::frame_codec::FrameCodec;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::inode::{DirectoryInode, Inode};
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::metrics::FileSystemStats;
use crate::fs::stats::{FileSystemGlobalStats, StatsShardData};
use crate::fs::store::{DirectoryStore, ExtentStore, InodeStore, OrphanStore, TombstoneStore};
use crate::fs::tracing::AccessTracer;
use crate::fs::write_coordinator::WriteCoordinator;
use crate::fs::{STATS_SHARDS, ZeroFS, get_current_time, get_current_uid_gid};
use crate::object_trace::ObjectTracer;
use crate::segment_store::SegmentStore;
use dashmap::DashMap;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::sync::Arc;
use std::sync::Mutex;

impl ZeroFS {
    /// Thin no-lease wrapper retained for the single-node constructors and tests;
    /// the replication-aware server path calls `new_with_slatedb_and_lease`.
    #[allow(dead_code)]
    pub async fn new_with_slatedb(
        slatedb: SlateDbHandle,
        max_bytes: u64,
        metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
        sync_writes: bool,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        segment_codec: FrameCodec,
    ) -> anyhow::Result<Self> {
        Self::new_with_slatedb_and_lease(
            slatedb,
            max_bytes,
            metrics_recorder,
            sync_writes,
            false,
            None,
            None,
            Arc::new(crate::dedup::DedupCache::new(65_536)),
            false, // single-node / test: never a live takeover, always regenerate
            ObjectTracer::new(),
            object_store,
            segment_codec,
            None,
        )
        .await
    }

    /// Like [`new_with_slatedb`](Self::new_with_slatedb) but attaches an HA leader
    /// `lease`: the data `Db` refuses reads/writes while the lease is invalid (not
    /// the confirmed leader). Single-node callers pass no lease.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_slatedb_and_lease(
        slatedb: SlateDbHandle,
        max_bytes: u64,
        metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
        sync_writes: bool,
        ignore_fsync: bool,
        lease: Option<Arc<crate::replication::Lease>>,
        replicator: Option<Arc<crate::replication::Replicator>>,
        dedup: Arc<crate::dedup::DedupCache>,
        // True only when this node is a standby promoting after receiving replication
        // from a live leader; false on a cold bootstrap / config leader / single node.
        // Decides whether the durability lineage carries forward (resolve_lineage_token).
        is_live_takeover: bool,
        object_tracer: ObjectTracer,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        segment_codec: FrameCodec,
        segment_warm: Option<crate::segment_store::SegmentWarmHook>,
    ) -> anyhow::Result<Self> {
        let lock_manager = Arc::new(KeyedLockManager::new());
        let key_codec = Arc::new(KeyCodec::new());

        // The data-db `writer_epoch` (monotonic object-store CAS, bumped on every
        // open). Used as a fresh, never-reused lineage token whenever the durability
        // lineage must be regenerated (see `resolve_lineage_token`).
        let writer_epoch = match &slatedb {
            SlateDbHandle::ReadWrite(db) => db.subscribe().borrow().current_manifest.writer_epoch(),
            SlateDbHandle::ReadOnly(_) => 0,
        };

        let db = Arc::new(match slatedb {
            SlateDbHandle::ReadWrite(db) => {
                let db = Db::new(db, metrics_recorder);
                match lease {
                    Some(lease) => db.with_lease(lease),
                    None => db,
                }
            }
            SlateDbHandle::ReadOnly(reader) => Db::new_read_only(reader),
        });

        let counter_key = key_codec.system_counter_key();
        let next_inode_id = match db.get_bytes(&counter_key).await? {
            Some(data) => KeyCodec::decode_counter(&data)?,
            None => 1,
        };

        let root_inode_key = key_codec.inode_key(0);
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
                name: None, // Root has no name
                nlink: 2,   // . and ..
            };
            let serialized = bincode::serialize(&Inode::Directory(root_dir))?;
            db.put_with_options(
                &root_inode_key,
                &serialized,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await?;
        }

        let global_stats = Arc::new(FileSystemGlobalStats::new(key_codec.clone()));

        for i in 0..STATS_SHARDS {
            let shard_key = key_codec.stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await?
                && let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data)
            {
                global_stats.load_shard(i, &shard_data);
            }
        }

        let flush_coordinator = FlushCoordinator::new(db.clone());
        let stats = Arc::new(FileSystemStats::new());
        let segment_store = Arc::new(SegmentStore::new(
            object_store,
            segment_codec,
            writer_epoch,
            segment_warm,
        ));
        let extent_store = ExtentStore::new(
            db.clone(),
            key_codec.clone(),
            segment_store,
            lock_manager.clone(),
        );
        // Seed the monitor's segment footprint gauges from the existing on-store
        // segments before any write; from here they are maintained incrementally
        // off the commit path, so the panel never scans to stay current.
        extent_store.seed_footprint().await?;
        // The flush path seals the open data-plane segment before flushing the
        // manifest, so a durable manifest never references an un-PUT segment.
        flush_coordinator.set_sealer({
            let es = extent_store.clone();
            Arc::new(move || {
                let es = es.clone();
                Box::pin(async move { es.seal_open().await })
            })
        });
        let directory_store = DirectoryStore::new(db.clone(), key_codec.clone());
        let inode_store = InodeStore::new(db.clone(), key_codec.clone(), next_inode_id);
        let tombstone_store = TombstoneStore::new(db.clone(), key_codec.clone());
        let orphan_store = OrphanStore::new(db.clone(), key_codec.clone());

        // Resolve the durability lineage token: carry it forward across an untainted
        // live takeover (transparent failover), else regenerate it (cold bootstrap or
        // Solo-tainted takeover). Must precede the WriteCoordinator, which taints it.
        let lineage_token =
            Self::resolve_lineage_token(&db, &key_codec, writer_epoch, is_live_takeover).await?;

        let write_coordinator = WriteCoordinator::new(
            db.clone(),
            inode_store.clone(),
            flush_coordinator.clone(),
            key_codec.clone(),
            global_stats.clone(),
            sync_writes,
            replicator,
            dedup.clone(),
            lineage_token,
            extent_store.clone(),
        );

        // Route the data plane's GC/compaction seg-count txns through the single
        // commit worker (its sole writer). Weak, so it can't keep the worker alive.
        extent_store.set_coordinator(write_coordinator.downgrade());

        let (reclaim_tx, reclaim_rx) = tokio::sync::mpsc::unbounded_channel();

        let fs = Self {
            db: db.clone(),
            extent_store,
            directory_store,
            inode_store,
            tombstone_store,
            orphan_store,
            open_handles: Arc::new(DashMap::new()),
            reclaim_tx,
            reclaim_rx: Arc::new(Mutex::new(Some(reclaim_rx))),
            lock_manager,
            stats,
            global_stats,
            flush_coordinator,
            write_coordinator,
            ignore_fsync,
            lineage_token,
            max_bytes,
            tracer: AccessTracer::new(),
            object_tracer,
            dedup,
        };

        // Drain the durable orphan set left by any prior life: open-unlinked
        // inodes whose handles did not survive the crash/shutdown. Post-crash
        // there are zero open handles, so every orphan is reclaimable.
        if !db.is_read_only() {
            fs.drain_orphans().await?;
        }

        Ok(fs)
    }

    /// Resolve the durability lineage token at bring-up. Carry the stored token
    /// forward only on a live-standby takeover of an untainted lineage — the standby
    /// provably has every acked write (ship-before-apply, and a Solo episode would
    /// have left a taint), so a client's un-fsync'd writes stay verifiable and its
    /// fsync is transparent across the failover. Otherwise regenerate a fresh token
    /// (`writer_epoch`: monotonic, never reused) and persist it durably: a cold
    /// bootstrap (the lineage broke — un-fsync'd writes are gone) or a Solo-tainted
    /// takeover (acked Solo writes the standby never received), where a client's
    /// pre-break writes must fail their verified fsync rather than match a stale token.
    async fn resolve_lineage_token(
        db: &Db,
        key_codec: &KeyCodec,
        writer_epoch: u64,
        is_live_takeover: bool,
    ) -> anyhow::Result<u64> {
        if db.is_read_only() {
            return Ok(0);
        }
        let stored = db
            .get_bytes(&key_codec.lineage_key())
            .await?
            .and_then(|b| KeyCodec::decode_u64(&b));
        // The Solo taint records the token that was live when the leader went Solo.
        let taint = db
            .get_bytes(&key_codec.taint_key())
            .await?
            .and_then(|b| KeyCodec::decode_u64(&b));
        let keep = is_live_takeover && stored.is_some() && stored != taint;
        if keep {
            return Ok(stored.unwrap());
        }
        let token = writer_epoch;
        db.put_with_options(
            &key_codec.lineage_key(),
            &KeyCodec::encode_u64(token),
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await?;
        // await_durable:false + explicit flush (a durable write here can stall
        // bring-up on file://); the token must be durable before this node serves.
        db.flush().await?;
        Ok(token)
    }

    /// Client durability barrier (9P `Tfsync`, NFS COMMIT, NBD flush). A no-op when
    /// `ignore_fsync` is set.
    pub async fn client_fsync(&self) -> Result<(), crate::fs::errors::FsError> {
        if self.ignore_fsync {
            return Ok(());
        }
        self.flush_coordinator.flush().await
    }

    /// Durability-verified fsync (9P `Tfsyncdur`, `.zerofs4`). `client_token` is the
    /// lineage token of the client's oldest un-fsync'd write (`0` = nothing
    /// un-fsync'd). We flush first (so every write made on this instance becomes
    /// durable), then verify the lineage: OK iff the client wrote nothing, or its
    /// writes belong to the live lineage (so the flush just made them durable, or a
    /// clean takeover carried them). Otherwise the client's writes were made under a
    /// lineage that broke (a cold restart or a Solo-tainted takeover) and may be
    /// lost, so we return ESTALE rather than a false success. With `ignore_fsync`,
    /// the administrator explicitly opts out and this barrier returns immediately.
    pub async fn client_fsync_verified(
        &self,
        client_token: u64,
    ) -> Result<(), crate::fs::errors::FsError> {
        if self.ignore_fsync {
            return Ok(());
        }
        self.flush_coordinator.flush().await?;
        if client_token == 0 || client_token == self.lineage_token {
            Ok(())
        } else {
            Err(crate::fs::errors::FsError::StaleHandle)
        }
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        Self::new_in_memory_with_sync_writes(false).await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_sync_writes(sync_writes: bool) -> anyhow::Result<Self> {
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use slatedb::BlockTransformer;
        use slatedb::DbBuilder;
        use slatedb::object_store::path::Path;

        let test_key = [0u8; 32];
        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(object_store);

        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path, object_store.clone())
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await?,
        );

        let segment_codec = crate::frame_codec::FrameCodec::new(
            &test_key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        );
        Self::new_with_slatedb(
            SlateDbHandle::ReadWrite(slatedb),
            u64::MAX,
            None,
            sync_writes,
            object_store,
            segment_codec,
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory_read_only(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    ) -> anyhow::Result<Self> {
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use arc_swap::ArcSwap;
        use slatedb::BlockTransformer;
        use slatedb::DbReader;
        use slatedb::object_store::path::Path;

        let test_key = [0u8; 32];
        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());

        let db_path = Path::from("test_slatedb");
        let reader = Arc::new(
            DbReader::builder(db_path, object_store.clone())
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await?,
        );

        Self::new_with_slatedb(
            SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
            u64::MAX,
            None,
            false,
            object_store,
            crate::frame_codec::FrameCodec::new(
                &test_key,
                crate::segment::SEGMENT_INFO,
                CompressionConfig::default(),
            ),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::inode::FileInode;

    use crate::fs::*;

    use crate::db::SlateDbHandle;
    use crate::fs::inode::Inode;

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
    async fn ignore_fsync_applies_to_verified_barriers() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.ignore_fsync = true;
        fs.client_fsync_verified(fs.lineage_token.wrapping_add(1))
            .await
            .expect("the explicit ignore_fsync opt-out bypasses lineage verification");
    }

    #[tokio::test]
    async fn test_read_only_mode_operations() {
        use slatedb::DbBuilder;
        use slatedb::object_store::path::Path;

        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use slatedb::BlockTransformer;

        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(object_store);

        let test_key = [0u8; 32];
        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path.clone(), object_store.clone())
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );

        let fs_rw = ZeroFS::new_with_slatedb(
            SlateDbHandle::ReadWrite(slatedb),
            u64::MAX,
            None,
            false,
            object_store.clone(),
            crate::frame_codec::FrameCodec::new(
                &test_key,
                crate::segment::SEGMENT_INFO,
                CompressionConfig::default(),
            ),
        )
        .await
        .unwrap();

        let test_inode_id = fs_rw.inode_store.allocate();
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
            name: Some(b"test.txt".to_vec()),
            nlink: 1,
        };
        let mut txn = fs_rw.db.new_transaction().unwrap();
        fs_rw
            .inode_store
            .save(&mut txn, test_inode_id, &Inode::File(file_inode.clone()))
            .unwrap();
        fs_rw.write_coordinator.commit(txn).await.unwrap();

        fs_rw.flush_coordinator.flush().await.unwrap();
        drop(fs_rw);

        let fs_ro = ZeroFS::new_in_memory_read_only(object_store).await.unwrap();

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

    // === Tests from operations.rs ===
}
