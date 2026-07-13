mod consistency;

use bytes::Bytes;
use futures::TryStreamExt;
use slatedb::DbBuilder;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use std::sync::Arc;
use zerofs::db::SlateDbHandle;
use zerofs::fs::ZeroFS;
use zerofs::fs::permissions::Credentials;
use zerofs::fs::store::ExtentStore;
use zerofs::fs::types::{AuthContext, SetAttributes};
use zerofs::segment::Segid;

use consistency::verify_consistency;
use zerofs::failpoints as fp;
use zerofs::fs::gc::GarbageCollector;
use zerofs::fs::inode::Inode;
use zerofs::fs::types::FileType;

fn test_creds() -> Credentials {
    Credentials {
        uid: 1000,
        gid: 1000,
        groups: [1000; 16],
        groups_count: 1,
    }
}

async fn list_segments(object_store: &Arc<dyn ObjectStore>) -> Vec<Segid> {
    object_store
        .list(Some(&Path::from("segments")))
        .map_ok(|meta| Segid::from_object_key(meta.location.as_ref()))
        .try_filter_map(|segid| std::future::ready(Ok(segid)))
        .try_collect()
        .await
        .unwrap()
}

async fn reclaim_now(store: &ExtentStore) -> anyhow::Result<(usize, usize)> {
    let outcome = store
        .reclaim_segments_gated(
            || std::future::ready(Ok(Some((chrono::Utc::now(), None)))),
            Some(zerofs::config::GcConfig::DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT),
            std::time::Duration::from_secs(5 * 60),
            |_| false,
            256 << 20,
        )
        .await?;
    Ok((outcome.deleted, outcome.relocated))
}

/// Test context holding filesystem and in-memory object store.
/// The object store persists across restarts, simulating a real crash where
/// only the database state (SlateDB) is lost but storage remains.
struct CrashTestContext {
    /// In-memory object store that persists across "restarts"
    object_store: Arc<dyn ObjectStore>,
}

impl CrashTestContext {
    fn new() -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
        }
    }

    /// Create a new filesystem instance
    async fn create_fs(&self) -> Arc<ZeroFS> {
        let db_path = Path::from("slatedb");
        // Mirror the production durability-critical SlateDB config (see
        // cli/server.rs): WAL off + l0_sst_size_bytes MAX means the only path to a
        // durable manifest is our seal-gated flush. Without this the harness would
        // use SlateDB's default WAL recovery, which resurrects un-flushed writes on
        // restart and so never exercises the barrier the data plane relies on.
        let settings = slatedb::config::Settings {
            wal_enabled: false,
            l0_sst_size_bytes: usize::MAX,
            l0_max_ssts: 256,
            l0_max_ssts_per_key: 256,
            ..Default::default()
        };
        let slatedb = Arc::new(
            DbBuilder::new(db_path, Arc::clone(&self.object_store))
                .with_settings(settings)
                .with_filter_policies(zerofs::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(zerofs::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );

        Arc::new(
            ZeroFS::new_with_slatedb_and_lease(
                SlateDbHandle::ReadWrite(slatedb),
                u64::MAX,
                None,
                false,
                false,
                None,
                None,
                Arc::new(zerofs::dedup::DedupCache::new(65_536)),
                None,
                zerofs::object_trace::ObjectTracer::new(),
                Arc::clone(&self.object_store),
                zerofs::frame_codec::FrameCodec::new(
                    &[7u8; 32],
                    zerofs::segment::SEGMENT_INFO,
                    zerofs::config::CompressionConfig::default(),
                ),
                None,
                None,
            )
            .await
            .unwrap(),
        )
    }

    /// Simulate crash and restart by dropping and recreating ZeroFS.
    /// The object store persists, so all flushed data is retained.
    async fn restart_fs(&self) -> Arc<ZeroFS> {
        self.create_fs().await
    }
}

struct TestSetup {
    ctx: CrashTestContext,
    fs: Arc<ZeroFS>,
    creds: Credentials,
    auth: AuthContext,
}

impl TestSetup {
    async fn new() -> (fail::FailScenario<'static>, Self) {
        let scenario = fail::FailScenario::setup();
        let ctx = CrashTestContext::new();
        let fs = ctx.create_fs().await;
        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };
        (
            scenario,
            Self {
                ctx,
                fs,
                creds,
                auth,
            },
        )
    }
}

#[tokio::test]
async fn test_basic_consistency_after_clean_restart() {
    let _scenario = fail::FailScenario::setup();
    let ctx = CrashTestContext::new();
    let fs = ctx.create_fs().await;
    let creds = Credentials {
        uid: 1000,
        gid: 1000,
        groups: [1000; 16],
        groups_count: 1,
    };
    let auth = AuthContext {
        uid: 1000,
        gid: 1000,
        gids: vec![1000],
    };

    let (file_id, _) = fs
        .create(&creds, 0, b"test.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    let (dir_id, _) = fs
        .mkdir(&creds, 0, b"testdir", &SetAttributes::default())
        .await
        .unwrap();

    let (nested_file_id, _) = fs
        .create(&creds, dir_id, b"nested.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, nested_file_id, 0, &Bytes::from(vec![2u8; 500]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    println!("{}", report);
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after clean restart"
    );
}

#[tokio::test]
async fn test_crash_write_after_extent() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"test.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::WRITE_AFTER_EXTENT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .write(&auth_clone, file_id, 0, &Bytes::from(vec![1u8; 1000]))
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::WRITE_AFTER_EXTENT, "off").unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let inode_id = fs_after.lookup(&creds, 0, b"test.txt").await.unwrap();
    match fs_after.inode_store.get(inode_id).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.size, 0,
                "File size should be 0 since write didn't commit"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_write_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"test.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::WRITE_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .write(&auth_clone, file_id, 1000, &Bytes::from(vec![2u8; 500]))
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::WRITE_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let inode_id = fs_after.lookup(&creds, 0, b"test.txt").await.unwrap();
    match fs_after.inode_store.get(inode_id).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.size, 1000,
                "File size should be 1000 since append didn't commit"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_create_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::CREATE_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .create(
                &creds_clone,
                0,
                b"crash_test.txt",
                &SetAttributes::default(),
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::CREATE_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"crash_test.txt").await;
    assert!(
        lookup_result.is_err(),
        "File should not exist since create didn't commit"
    );
}

#[tokio::test]
async fn test_crash_create_after_dir_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::CREATE_AFTER_DIR_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .create(
                &creds_clone,
                0,
                b"crash_test.txt",
                &SetAttributes::default(),
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::CREATE_AFTER_DIR_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"crash_test.txt").await;
    assert!(
        lookup_result.is_err(),
        "File should not exist since create didn't commit"
    );
}

#[tokio::test]
async fn test_crash_create_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::CREATE_AFTER_COMMIT, "panic").unwrap();

    // Use spawn to isolate the panic - JoinHandle returns Err if task panics
    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .create(
                &creds_clone,
                0,
                b"crash_test.txt",
                &SetAttributes::default(),
            )
            .await
    });
    let _ = handle.await; // Ignore result - task may have panicked

    fail::cfg(fp::CREATE_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"crash_test.txt").await;
    assert!(
        lookup_result.is_err(),
        "File should not exist since commit wasn't flushed before crash"
    );
}

#[tokio::test]
async fn test_crash_remove_after_inode_delete() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"victim.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 5000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::REMOVE_AFTER_INODE_DELETE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"victim.txt").await });
    let _ = handle.await;
    fail::cfg(fp::REMOVE_AFTER_INODE_DELETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after crash at REMOVE_AFTER_INODE_DELETE:\n{}",
        report
    );
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after crash at remove_after_inode_delete: {:?}",
        report.errors
    );
    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"victim.txt").await;
    assert!(
        lookup_result.is_ok(),
        "File should still exist since remove didn't commit"
    );
    let inode_id = lookup_result.unwrap();
    match fs_after.inode_store.get(inode_id).await.unwrap() {
        Inode::File(file) => assert_eq!(file.size, 5000, "File size should be unchanged"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_remove_after_tombstone() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"victim.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 5000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::REMOVE_AFTER_TOMBSTONE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"victim.txt").await });
    let _ = handle.await;

    fail::cfg(fp::REMOVE_AFTER_TOMBSTONE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"victim.txt").await;
    assert!(
        lookup_result.is_ok(),
        "File should still exist since remove didn't commit"
    );
    let inode_id = lookup_result.unwrap();
    match fs_after.inode_store.get(inode_id).await.unwrap() {
        Inode::File(file) => assert_eq!(file.size, 5000, "File size should be unchanged"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_remove_after_dir_unlink() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"file_to_remove.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::REMOVE_AFTER_DIR_UNLINK, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(
            async move { fs_clone.remove(&auth_clone, 0, b"file_to_remove.txt").await },
        );
    let _ = handle.await;
    fail::cfg(fp::REMOVE_AFTER_DIR_UNLINK, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"file_to_remove.txt").await;
    assert!(
        lookup_result.is_ok(),
        "File should still exist since remove didn't commit"
    );
    let inode_id = lookup_result.unwrap();
    match fs_after.inode_store.get(inode_id).await.unwrap() {
        Inode::File(file) => assert_eq!(file.size, 1000, "File size should be unchanged"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_remove_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"victim.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::REMOVE_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"victim.txt").await });
    let _ = handle.await;

    fail::cfg(fp::REMOVE_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let lookup_result = fs_after.lookup(&creds, 0, b"victim.txt").await;
    assert!(
        lookup_result.is_ok(),
        "File should still exist since remove wasn't flushed before crash"
    );
}

#[tokio::test]
async fn test_crash_rename_after_source_unlink() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"source.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_SOURCE_UNLINK, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(&auth_clone, 0, b"source.txt", 0, b"dest.txt")
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_SOURCE_UNLINK, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after crash at RENAME_AFTER_SOURCE_UNLINK:\n{}",
        report
    );
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after crash at rename_after_source_unlink: {:?}",
        report.errors
    );
    let creds = test_creds();
    let source_lookup = fs_after.lookup(&creds, 0, b"source.txt").await;
    let dest_lookup = fs_after.lookup(&creds, 0, b"dest.txt").await;
    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename didn't commit"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest file should not exist since rename didn't commit"
    );
}

#[tokio::test]
async fn test_crash_rename_after_new_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"source.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_NEW_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(&auth_clone, 0, b"source.txt", 0, b"dest.txt")
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::RENAME_AFTER_NEW_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let source_lookup = fs_after.lookup(&creds, 0, b"source.txt").await;
    let dest_lookup = fs_after.lookup(&creds, 0, b"dest.txt").await;
    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename didn't commit"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest file should not exist since rename didn't commit"
    );
}

#[tokio::test]
async fn test_crash_rename_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"source.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(&auth_clone, 0, b"source.txt", 0, b"dest.txt")
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::RENAME_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let source_lookup = fs_after.lookup(&creds, 0, b"source.txt").await;
    let dest_lookup = fs_after.lookup(&creds, 0, b"dest.txt").await;
    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename wasn't flushed"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest file should not exist since rename wasn't flushed"
    );
}

#[tokio::test]
async fn test_crash_rename_overwrite_after_target_delete() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (src_id, _) = fs
        .create(&creds, 0, b"source.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, src_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    let (tgt_id, _) = fs
        .create(&creds, 0, b"target.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, tgt_id, 0, &Bytes::from(vec![2u8; 2000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_TARGET_DELETE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(&auth_clone, 0, b"source.txt", 0, b"target.txt")
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_TARGET_DELETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after crash at RENAME_AFTER_TARGET_DELETE:\n{}",
        report
    );
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after crash at rename_after_target_delete: {:?}",
        report.errors
    );
    let creds = test_creds();
    let source_lookup = fs_after.lookup(&creds, 0, b"source.txt").await;
    let target_lookup = fs_after.lookup(&creds, 0, b"target.txt").await;
    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename didn't commit"
    );
    assert!(
        target_lookup.is_ok(),
        "Target file should still exist since rename didn't commit"
    );
    let target_inode = target_lookup.unwrap();
    match fs_after.inode_store.get(target_inode).await.unwrap() {
        Inode::File(file) => assert_eq!(file.size, 2000, "Target file should have original size"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_gc_after_extent_delete() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"large_file.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 200_000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fs.remove(&auth, 0, b"large_file.txt").await.unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::GC_AFTER_EXTENT_DELETE, "panic").unwrap();

    let gc = Arc::new(GarbageCollector::new(
        Arc::clone(&fs.db),
        fs.tombstone_store.clone(),
        fs.extent_store.clone(),
        Arc::clone(&fs.stats),
        None,
        zerofs::fs::gc::GcTuning::default(),
    ));
    let handle = tokio::task::spawn(async move { gc.run().await });
    let _ = handle.await;

    fail::cfg(fp::GC_AFTER_EXTENT_DELETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
}

#[tokio::test]
async fn test_crash_gc_after_tombstone_update() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"to_delete.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 100_000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();
    fs.remove(&auth, 0, b"to_delete.txt").await.unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::GC_AFTER_TOMBSTONE_UPDATE, "panic").unwrap();

    let gc = Arc::new(GarbageCollector::new(
        Arc::clone(&fs.db),
        fs.tombstone_store.clone(),
        fs.extent_store.clone(),
        Arc::clone(&fs.stats),
        None,
        zerofs::fs::gc::GcTuning::default(),
    ));
    let handle = tokio::task::spawn(async move { gc.run().await });
    let _ = handle.await;

    fail::cfg(fp::GC_AFTER_TOMBSTONE_UPDATE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after crash at GC_AFTER_TOMBSTONE_UPDATE:\n{}",
        report
    );
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after crash at gc_after_tombstone_update: {:?}",
        report.errors
    );
}

#[tokio::test]
async fn test_multiple_successful_operations_then_crash() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    for i in 0..5 {
        let name = format!("file{}.txt", i);
        let (id, _) = fs
            .create(&creds, 0, name.as_bytes(), &SetAttributes::default())
            .await
            .unwrap();
        fs.write(
            &auth,
            id,
            0,
            &Bytes::from(vec![(i + 1) as u8; 1000 * (i + 1)]),
        )
        .await
        .unwrap();
    }

    let (dir1, _) = fs
        .mkdir(&creds, 0, b"dir1", &SetAttributes::default())
        .await
        .unwrap();
    let (dir2, _) = fs
        .mkdir(&creds, dir1, b"subdir", &SetAttributes::default())
        .await
        .unwrap();
    let (nested_file, _) = fs
        .create(&creds, dir2, b"nested.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, nested_file, 0, &Bytes::from(vec![0xAB; 500]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::WRITE_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        let (id, _) = fs_clone
            .create(&creds_clone, 0, b"final.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs_clone
            .write(&auth_clone, id, 0, &Bytes::from(vec![0xFF; 100]))
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::WRITE_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after multiple ops then crash at WRITE_AFTER_COMMIT:\n{}",
        report
    );

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent: {:?}",
        report.errors
    );
}

#[tokio::test]
async fn test_crash_link_after_dir_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::LINK_AFTER_DIR_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .link(&auth_clone, file_id, 0, b"hardlink.txt")
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::LINK_AFTER_DIR_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let original = fs_after.lookup(&creds, 0, b"original.txt").await;
    let hardlink = fs_after.lookup(&creds, 0, b"hardlink.txt").await;
    assert!(original.is_ok(), "Original file should still exist");
    assert!(
        hardlink.is_err(),
        "Hardlink should not exist since link didn't commit"
    );
    match fs_after.inode_store.get(original.unwrap()).await.unwrap() {
        Inode::File(file) => assert_eq!(file.nlink, 1, "Original should still have nlink=1"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_link_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::LINK_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .link(&auth_clone, file_id, 0, b"hardlink.txt")
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::LINK_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let original = fs_after.lookup(&creds, 0, b"original.txt").await;
    let hardlink = fs_after.lookup(&creds, 0, b"hardlink.txt").await;
    assert!(original.is_ok(), "Original file should still exist");
    assert!(
        hardlink.is_err(),
        "Hardlink should not exist since link didn't commit"
    );
    match fs_after.inode_store.get(original.unwrap()).await.unwrap() {
        Inode::File(file) => assert_eq!(file.nlink, 1, "Original should still have nlink=1"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_link_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::LINK_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .link(&auth_clone, file_id, 0, b"hardlink.txt")
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::LINK_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let original = fs_after.lookup(&creds, 0, b"original.txt").await;
    let hardlink = fs_after.lookup(&creds, 0, b"hardlink.txt").await;
    assert!(
        original.is_ok(),
        "Original file should exist (was flushed before link)"
    );
    assert!(
        hardlink.is_err(),
        "Hardlink should not exist since commit wasn't flushed before crash"
    );
    match fs_after.inode_store.get(original.unwrap()).await.unwrap() {
        Inode::File(file) => assert_eq!(file.nlink, 1, "Original should still have nlink=1"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_symlink_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::SYMLINK_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .symlink(
                &creds_clone,
                0,
                b"mylink",
                b"/target/path",
                &SetAttributes::default(),
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::SYMLINK_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"mylink").await;
    assert!(
        lookup.is_err(),
        "Symlink should not exist since create didn't commit"
    );
}

#[tokio::test]
async fn test_crash_symlink_after_dir_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::SYMLINK_AFTER_DIR_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .symlink(
                &creds_clone,
                0,
                b"mylink",
                b"/target/path",
                &SetAttributes::default(),
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::SYMLINK_AFTER_DIR_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"mylink").await;
    assert!(
        lookup.is_err(),
        "Symlink should not exist since create didn't commit"
    );
}

#[tokio::test]
async fn test_crash_symlink_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::SYMLINK_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .symlink(
                &creds_clone,
                0,
                b"mylink",
                b"/target/path",
                &SetAttributes::default(),
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::SYMLINK_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"mylink").await;
    assert!(
        lookup.is_err(),
        "Symlink should not exist since commit wasn't flushed before crash"
    );
}

#[tokio::test]
async fn test_crash_mkdir_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::MKDIR_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .mkdir(&creds_clone, 0, b"newdir", &SetAttributes::default())
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::MKDIR_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"newdir").await;
    assert!(
        lookup.is_err(),
        "Directory should not exist since mkdir didn't commit"
    );
}

#[tokio::test]
async fn test_crash_mkdir_after_dir_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::MKDIR_AFTER_DIR_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .mkdir(&creds_clone, 0, b"newdir", &SetAttributes::default())
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::MKDIR_AFTER_DIR_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"newdir").await;
    assert!(
        lookup.is_err(),
        "Directory should not exist since mkdir didn't commit"
    );
}

#[tokio::test]
async fn test_crash_mkdir_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::MKDIR_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .mkdir(&creds_clone, 0, b"newdir", &SetAttributes::default())
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::MKDIR_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"newdir").await;
    assert!(
        lookup.is_err(),
        "Directory should not exist since commit wasn't flushed before crash"
    );
}

#[tokio::test]
async fn test_crash_truncate_after_extents() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"bigfile.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 100_000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::TRUNCATE_AFTER_EXTENTS, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .setattr(
                &creds_clone,
                file_id,
                &SetAttributes {
                    size: zerofs::fs::types::SetSize::Set(1000),
                    ..Default::default()
                },
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::TRUNCATE_AFTER_EXTENTS, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let inode = fs_after.lookup(&creds, 0, b"bigfile.txt").await.unwrap();
    match fs_after.inode_store.get(inode).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.size, 100_000,
                "File should have original size since truncate didn't commit"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_truncate_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"bigfile.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 100_000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::TRUNCATE_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .setattr(
                &creds_clone,
                file_id,
                &SetAttributes {
                    size: zerofs::fs::types::SetSize::Set(1000),
                    ..Default::default()
                },
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::TRUNCATE_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let inode = fs_after.lookup(&creds, 0, b"bigfile.txt").await.unwrap();
    match fs_after.inode_store.get(inode).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.size, 100_000,
                "File should have original size since truncate didn't commit"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_truncate_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"bigfile.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 100_000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::TRUNCATE_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .setattr(
                &creds_clone,
                file_id,
                &SetAttributes {
                    size: zerofs::fs::types::SetSize::Set(1000),
                    ..Default::default()
                },
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::TRUNCATE_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let inode = fs_after.lookup(&creds, 0, b"bigfile.txt").await.unwrap();
    match fs_after.inode_store.get(inode).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.size, 100_000,
                "File should have original size since truncate wasn't flushed before crash"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_mknod_after_inode() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::MKNOD_AFTER_INODE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .mknod(
                &creds_clone,
                0,
                b"myfifo",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::MKNOD_AFTER_INODE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"myfifo").await;
    assert!(
        lookup.is_err(),
        "Fifo should not exist since mknod didn't commit"
    );
}

#[tokio::test]
async fn test_crash_mknod_after_dir_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::MKNOD_AFTER_DIR_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .mknod(
                &creds_clone,
                0,
                b"myfifo",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::MKNOD_AFTER_DIR_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"myfifo").await;
    assert!(
        lookup.is_err(),
        "Fifo should not exist since mknod didn't commit"
    );
}

#[tokio::test]
async fn test_crash_mknod_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth: _,
        },
    ) = TestSetup::new().await;

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::MKNOD_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let creds_clone = creds;
    let handle = tokio::task::spawn(async move {
        fs_clone
            .mknod(
                &creds_clone,
                0,
                b"myfifo",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::MKNOD_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"myfifo").await;
    assert!(
        lookup.is_err(),
        "Fifo should not exist since commit wasn't flushed before crash"
    );
}

#[tokio::test]
async fn test_crash_rmdir_after_inode_delete() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (_dir_id, _) = fs
        .mkdir(&creds, 0, b"emptydir", &SetAttributes::default())
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RMDIR_AFTER_INODE_DELETE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"emptydir").await });
    let _ = handle.await;

    fail::cfg(fp::RMDIR_AFTER_INODE_DELETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after crash at RMDIR_AFTER_INODE_DELETE:\n{}",
        report
    );
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after crash at rmdir_after_inode_delete: {:?}",
        report.errors
    );
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"emptydir").await;
    assert!(
        lookup.is_ok(),
        "Directory should still exist since rmdir didn't commit"
    );
}

#[tokio::test]
async fn test_crash_rmdir_after_dir_cleanup() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (_dir_id, _) = fs
        .mkdir(&creds, 0, b"emptydir", &SetAttributes::default())
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RMDIR_AFTER_DIR_CLEANUP, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"emptydir").await });
    let _ = handle.await;

    fail::cfg(fp::RMDIR_AFTER_DIR_CLEANUP, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"emptydir").await;
    assert!(
        lookup.is_ok(),
        "Directory should still exist since rmdir didn't commit"
    );
}

#[tokio::test]
async fn test_crash_rmdir_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (_dir_id, _) = fs
        .mkdir(&creds, 0, b"emptydir", &SetAttributes::default())
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::REMOVE_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"emptydir").await });
    let _ = handle.await;

    fail::cfg(fp::REMOVE_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    fs_after.flush_coordinator.flush().await.unwrap();
    let report = verify_consistency(&fs_after).await.unwrap();

    println!(
        "Report after crash at RMDIR (REMOVE_AFTER_COMMIT):\n{}",
        report
    );
    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after crash at rmdir commit: {:?}",
        report.errors
    );
    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"emptydir").await;
    assert!(
        lookup.is_ok(),
        "Directory should still exist since rmdir wasn't flushed before crash"
    );
}

#[tokio::test]
async fn test_create_persists_after_flush() {
    let (_scenario, TestSetup { ctx, fs, creds, .. }) = TestSetup::new().await;

    fs.create(&creds, 0, b"flushed_file.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"flushed_file.txt").await;
    assert!(lookup.is_ok(), "File should exist after flush and restart");
}

#[tokio::test]
async fn test_write_persists_after_flush() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"flushed_file.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![0xAB; 5000]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let inode = fs_after
        .lookup(&creds, 0, b"flushed_file.txt")
        .await
        .unwrap();
    match fs_after.inode_store.get(inode).await.unwrap() {
        Inode::File(file) => assert_eq!(file.size, 5000, "File size should persist after flush"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_remove_persists_after_flush() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    fs.create(&creds, 0, b"to_delete.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fs.remove(&auth, 0, b"to_delete.txt").await.unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"to_delete.txt").await;
    assert!(lookup.is_err(), "File should be gone after flushed remove");
}

#[tokio::test]
async fn test_rename_persists_after_flush() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    fs.create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fs.rename(&auth, 0, b"original.txt", 0, b"renamed.txt")
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    assert!(
        fs_after.lookup(&creds, 0, b"original.txt").await.is_err(),
        "Original name should be gone"
    );
    assert!(
        fs_after.lookup(&creds, 0, b"renamed.txt").await.is_ok(),
        "New name should exist"
    );
}

#[tokio::test]
async fn test_link_persists_after_flush() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fs.link(&auth, file_id, 0, b"hardlink.txt").await.unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let original = fs_after.lookup(&creds, 0, b"original.txt").await.unwrap();
    let hardlink = fs_after.lookup(&creds, 0, b"hardlink.txt").await.unwrap();
    assert_eq!(original, hardlink, "Hardlink should point to same inode");
    match fs_after.inode_store.get(original).await.unwrap() {
        Inode::File(file) => assert_eq!(file.nlink, 2, "nlink should be 2 after hardlink"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_mkdir_persists_after_flush() {
    let (_scenario, TestSetup { ctx, fs, creds, .. }) = TestSetup::new().await;

    fs.mkdir(&creds, 0, b"newdir", &SetAttributes::default())
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let lookup = fs_after.lookup(&creds, 0, b"newdir").await;
    assert!(lookup.is_ok(), "Directory should exist after flush");
}

#[tokio::test]
async fn test_truncate_persists_after_flush() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"bigfile.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 100_000]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fs.setattr(
        &creds,
        file_id,
        &SetAttributes {
            size: zerofs::fs::types::SetSize::Set(1000),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let inode = fs_after.lookup(&creds, 0, b"bigfile.txt").await.unwrap();
    match fs_after.inode_store.get(inode).await.unwrap() {
        Inode::File(file) => assert_eq!(file.size, 1000, "Truncated size should persist"),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_after_flush_complete() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"flushed_file.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![0xCD; 3000]))
        .await
        .unwrap();

    fail::cfg(fp::FLUSH_AFTER_COMPLETE, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let handle = tokio::task::spawn(async move { fs_clone.flush_coordinator.flush().await });
    let _ = handle.await;

    fail::cfg(fp::FLUSH_AFTER_COMPLETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");

    let creds = test_creds();
    let inode = fs_after
        .lookup(&creds, 0, b"flushed_file.txt")
        .await
        .unwrap();
    match fs_after.inode_store.get(inode).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(file.size, 3000, "File should persist since flush completed")
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_cross_dir_rename_file_after_source_unlink() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (src_dir_id, _) = fs
        .mkdir(&creds, 0, b"src_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (dst_dir_id, _) = fs
        .mkdir(&creds, 0, b"dst_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (file_id, _) = fs
        .create(&creds, src_dir_id, b"file.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_SOURCE_UNLINK, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(
                &auth_clone,
                src_dir_id,
                b"file.txt",
                dst_dir_id,
                b"moved.txt",
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_SOURCE_UNLINK, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after cross-dir rename crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let src_dir = fs_after.lookup(&creds, 0, b"src_dir").await.unwrap();
    let dst_dir = fs_after.lookup(&creds, 0, b"dst_dir").await.unwrap();

    let source_lookup = fs_after.lookup(&creds, src_dir, b"file.txt").await;
    let dest_lookup = fs_after.lookup(&creds, dst_dir, b"moved.txt").await;

    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename didn't commit"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest file should not exist since rename didn't commit"
    );
}

#[tokio::test]
async fn test_crash_cross_dir_rename_file_after_new_entry() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (src_dir_id, _) = fs
        .mkdir(&creds, 0, b"src_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (dst_dir_id, _) = fs
        .mkdir(&creds, 0, b"dst_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (file_id, _) = fs
        .create(&creds, src_dir_id, b"file.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_NEW_ENTRY, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(
                &auth_clone,
                src_dir_id,
                b"file.txt",
                dst_dir_id,
                b"moved.txt",
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_NEW_ENTRY, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after cross-dir rename crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let src_dir = fs_after.lookup(&creds, 0, b"src_dir").await.unwrap();
    let dst_dir = fs_after.lookup(&creds, 0, b"dst_dir").await.unwrap();

    let source_lookup = fs_after.lookup(&creds, src_dir, b"file.txt").await;
    let dest_lookup = fs_after.lookup(&creds, dst_dir, b"moved.txt").await;

    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename didn't commit"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest file should not exist since rename didn't commit"
    );
}

#[tokio::test]
async fn test_crash_cross_dir_rename_file_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (src_dir_id, _) = fs
        .mkdir(&creds, 0, b"src_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (dst_dir_id, _) = fs
        .mkdir(&creds, 0, b"dst_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (file_id, _) = fs
        .create(&creds, src_dir_id, b"file.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(
                &auth_clone,
                src_dir_id,
                b"file.txt",
                dst_dir_id,
                b"moved.txt",
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after cross-dir rename crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let src_dir = fs_after.lookup(&creds, 0, b"src_dir").await.unwrap();
    let dst_dir = fs_after.lookup(&creds, 0, b"dst_dir").await.unwrap();

    let source_lookup = fs_after.lookup(&creds, src_dir, b"file.txt").await;
    let dest_lookup = fs_after.lookup(&creds, dst_dir, b"moved.txt").await;

    assert!(
        source_lookup.is_ok(),
        "Source file should still exist since rename wasn't flushed"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest file should not exist since rename wasn't flushed"
    );
}

#[tokio::test]
async fn test_crash_cross_dir_rename_subdir_after_source_unlink() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (src_dir_id, _) = fs
        .mkdir(&creds, 0, b"src_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (dst_dir_id, _) = fs
        .mkdir(&creds, 0, b"dst_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (_subdir_id, _) = fs
        .mkdir(&creds, src_dir_id, b"subdir", &SetAttributes::default())
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_SOURCE_UNLINK, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(
                &auth_clone,
                src_dir_id,
                b"subdir",
                dst_dir_id,
                b"moved_subdir",
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_SOURCE_UNLINK, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after cross-dir subdir rename crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let src_dir = fs_after.lookup(&creds, 0, b"src_dir").await.unwrap();
    let dst_dir = fs_after.lookup(&creds, 0, b"dst_dir").await.unwrap();

    let source_lookup = fs_after.lookup(&creds, src_dir, b"subdir").await;
    let dest_lookup = fs_after.lookup(&creds, dst_dir, b"moved_subdir").await;

    assert!(
        source_lookup.is_ok(),
        "Source subdir should still exist since rename didn't commit"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest subdir should not exist since rename didn't commit"
    );
}

#[tokio::test]
async fn test_crash_cross_dir_rename_subdir_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (src_dir_id, _) = fs
        .mkdir(&creds, 0, b"src_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (dst_dir_id, _) = fs
        .mkdir(&creds, 0, b"dst_dir", &SetAttributes::default())
        .await
        .unwrap();

    let (_subdir_id, _) = fs
        .mkdir(&creds, src_dir_id, b"subdir", &SetAttributes::default())
        .await
        .unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RENAME_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .rename(
                &auth_clone,
                src_dir_id,
                b"subdir",
                dst_dir_id,
                b"moved_subdir",
            )
            .await
    });
    let _ = handle.await;
    fail::cfg(fp::RENAME_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after cross-dir subdir rename crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let src_dir = fs_after.lookup(&creds, 0, b"src_dir").await.unwrap();
    let dst_dir = fs_after.lookup(&creds, 0, b"dst_dir").await.unwrap();

    let source_lookup = fs_after.lookup(&creds, src_dir, b"subdir").await;
    let dest_lookup = fs_after.lookup(&creds, dst_dir, b"moved_subdir").await;

    assert!(
        source_lookup.is_ok(),
        "Source subdir should still exist since rename wasn't flushed"
    );
    assert!(
        dest_lookup.is_err(),
        "Dest subdir should not exist since rename wasn't flushed"
    );

    // Verify nlinks remain unchanged since rename wasn't flushed
    match fs_after.inode_store.get(src_dir).await.unwrap() {
        Inode::Directory(dir) => {
            assert_eq!(
                dir.nlink, 3,
                "src_dir should still have nlink=3 (subdir not moved)"
            );
        }
        _ => unreachable!(),
    }
    match fs_after.inode_store.get(dst_dir).await.unwrap() {
        Inode::Directory(dir) => {
            assert_eq!(
                dir.nlink, 2,
                "dst_dir should still have nlink=2 (no subdir moved in)"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_hardlink_unlink_after_inode_update() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.link(&auth, file_id, 0, b"hardlink.txt").await.unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    // Verify nlink is 2 before unlink
    match fs.inode_store.get(file_id).await.unwrap() {
        Inode::File(file) => assert_eq!(file.nlink, 2),
        _ => unreachable!(),
    }

    fail::cfg(fp::REMOVE_AFTER_DIR_UNLINK, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"hardlink.txt").await });
    let _ = handle.await;
    fail::cfg(fp::REMOVE_AFTER_DIR_UNLINK, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after hardlink unlink crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let original_lookup = fs_after.lookup(&creds, 0, b"original.txt").await;
    let hardlink_lookup = fs_after.lookup(&creds, 0, b"hardlink.txt").await;

    assert!(original_lookup.is_ok(), "Original file should still exist");
    assert!(
        hardlink_lookup.is_ok(),
        "Hardlink should still exist since unlink didn't commit"
    );

    let file_id = original_lookup.unwrap();
    match fs_after.inode_store.get(file_id).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.nlink, 2,
                "nlink should still be 2 since unlink didn't commit"
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_crash_hardlink_unlink_after_commit() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"original.txt", &SetAttributes::default())
        .await
        .unwrap();

    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 1000]))
        .await
        .unwrap();

    fs.link(&auth, file_id, 0, b"hardlink.txt").await.unwrap();

    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::REMOVE_AFTER_COMMIT, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"hardlink.txt").await });
    let _ = handle.await;
    fail::cfg(fp::REMOVE_AFTER_COMMIT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();

    assert!(
        report.is_consistent(),
        "Filesystem should be consistent after hardlink unlink crash: {:?}",
        report.errors
    );

    let creds = test_creds();
    let original_lookup = fs_after.lookup(&creds, 0, b"original.txt").await;
    let hardlink_lookup = fs_after.lookup(&creds, 0, b"hardlink.txt").await;

    assert!(original_lookup.is_ok(), "Original file should still exist");
    assert!(
        hardlink_lookup.is_ok(),
        "Hardlink should still exist since unlink wasn't flushed"
    );

    let file_id = original_lookup.unwrap();
    match fs_after.inode_store.get(file_id).await.unwrap() {
        Inode::File(file) => {
            assert_eq!(
                file.nlink, 2,
                "nlink should still be 2 since unlink wasn't flushed"
            );
        }
        _ => unreachable!(),
    }
}

async fn orphan_set_empty(fs: &ZeroFS) -> bool {
    use futures::StreamExt;
    let stream = fs.orphan_store.list().await.unwrap();
    futures::pin_mut!(stream);
    stream.next().await.is_none()
}

/// The consistency checker, run against a LIVE (non-drained) filesystem, must
/// exempt a legitimate open-unlinked orphan (nlink==0, in the orphan set) yet
/// still flag a genuine leak (nlink==0, unreachable, NOT in the set). Every
/// other consistency assertion runs post-restart, after the orphan set is
/// already drained, so without this the exemption branch is never exercised.
#[tokio::test]
async fn test_consistency_live_orphan_exempt_but_leak_flagged() {
    let (
        _scenario,
        TestSetup {
            fs, creds, auth, ..
        },
    ) = TestSetup::new().await;

    // Legitimate live orphan: open-unlinked, kept in the orphan set.
    let (good_id, _) = fs
        .create(&creds, 0, b"good", &SetAttributes::default())
        .await
        .unwrap();
    fs.open_handle_inc(good_id);
    fs.remove(&auth, 0, b"good").await.unwrap();

    // The find_orphaned_inodes exemption must skip the live orphan: no
    // OrphanedInode error for it. (verify_consistency as a whole is a
    // post-restart check -- it additionally reports LeakedOrphan + a stats
    // mismatch for a still-deferred orphan; those are expected online and are
    // not what this test guards.)
    let report = verify_consistency(&fs).await.unwrap();
    assert!(
        !report.errors.iter().any(|e| matches!(
            e,
            consistency::ConsistencyError::OrphanedInode { inode_id } if *inode_id == good_id
        )),
        "live open-unlinked orphan wrongly flagged as OrphanedInode: {:?}",
        report.errors
    );

    // Genuine leak: defer an inode, then drop its orphan key WITHOUT reclaiming,
    // leaving nlink==0 + unreachable + not in the orphan set.
    let (leak_id, _) = fs
        .create(&creds, 0, b"leak", &SetAttributes::default())
        .await
        .unwrap();
    fs.open_handle_inc(leak_id);
    fs.remove(&auth, 0, b"leak").await.unwrap();
    {
        let mut txn = fs.db.new_transaction().unwrap();
        fs.orphan_store.remove(&mut txn, leak_id);
        fs.write_coordinator.commit(txn).await.unwrap();
    }

    let report = verify_consistency(&fs).await.unwrap();
    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            consistency::ConsistencyError::OrphanedInode { inode_id } if *inode_id == leak_id
        )),
        "genuine orphan leak must be flagged; errors: {:?}",
        report.errors
    );
}

/// A panic at REMOVE_AFTER_ORPHAN_ADD is pre-commit: the orphan-add, nlink=0,
/// and dir-entry removal all live in the not-yet-committed txn, so a crash
/// there loses the whole unlink. After restart the file is fully present and
/// reachable, and the orphan set is empty.
#[tokio::test]
async fn test_crash_open_unlink_after_orphan_add() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"victim.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![7u8; 5000]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    // Pin the inode open so remove() takes the defer branch.
    fs.open_handle_inc(file_id);

    fail::cfg(fp::REMOVE_AFTER_ORPHAN_ADD, "panic").unwrap();
    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle =
        tokio::task::spawn(async move { fs_clone.remove(&auth_clone, 0, b"victim.txt").await });
    let _ = handle.await;
    fail::cfg(fp::REMOVE_AFTER_ORPHAN_ADD, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(
        report.is_consistent(),
        "Inconsistent after crash at remove_after_orphan_add: {:?}",
        report.errors
    );
    assert!(
        orphan_set_empty(&fs_after).await,
        "orphan set must be empty"
    );

    // Unlink never committed: the file is still present and reachable.
    let creds = test_creds();
    let id = fs_after
        .lookup(&creds, 0, b"victim.txt")
        .await
        .expect("file should still exist");
    match fs_after.inode_store.get(id).await.unwrap() {
        Inode::File(f) => assert_eq!(f.size, 5000),
        _ => unreachable!(),
    }
}

/// Defer commits cleanly, then the process crashes before the open fid is
/// clunked. The startup drain reclaims the orphan: inode + extents gone, orphan
/// set empty, namespace effect (name absent) preserved.
#[tokio::test]
async fn test_crash_open_unlink_committed_then_crash_drains() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"victim.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![9u8; 5000]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fs.open_handle_inc(file_id);
    // Defer commits (no failpoint armed).
    fs.remove(&auth, 0, b"victim.txt").await.unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    drop(fs); // crash before the clunk that would have reclaimed it

    let fs_after = ctx.restart_fs().await; // drain runs in new_with_slatedb
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(
        report.is_consistent(),
        "Inconsistent after open-unlink+crash drain: {:?}",
        report.errors
    );
    assert!(
        orphan_set_empty(&fs_after).await,
        "startup drain must empty the orphan set"
    );
    // Namespace effect preserved and inode reclaimed.
    assert!(matches!(
        fs_after.lookup(&test_creds(), 0, b"victim.txt").await,
        Err(zerofs::fs::errors::FsError::NotFound)
    ));
    assert!(matches!(
        fs_after.inode_store.get(file_id).await,
        Err(zerofs::fs::errors::FsError::NotFound)
    ));
}

/// A panic at CLUNK_AFTER_RECLAIM_INODE_DELETE is pre-commit: the reclaim txn
/// (inode delete + extent delete + stats delta + orphan-remove) never lands, so
/// the orphan key survives. The next startup drain re-runs reclaim exactly
/// once (idempotent), leaving a consistent, drained filesystem.
#[tokio::test]
async fn test_crash_reclaim_after_inode_delete_redrains() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"victim.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![3u8; 5000]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    let open_handle = fs.new_open_handle(file_id);
    fs.remove(&auth, 0, b"victim.txt").await.unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    // Last clunk triggers reclaim; panic mid-reclaim before commit.
    fail::cfg(fp::CLUNK_AFTER_RECLAIM_INODE_DELETE, "panic").unwrap();
    drop(open_handle);
    let fs_clone = Arc::clone(&fs);
    let handle = tokio::task::spawn(async move { fs_clone.reclaim_if_unreferenced(file_id).await });
    let _ = handle.await;
    fail::cfg(fp::CLUNK_AFTER_RECLAIM_INODE_DELETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await; // drain re-runs reclaim
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(
        report.is_consistent(),
        "Inconsistent after crash mid-reclaim: {:?}",
        report.errors
    );
    assert!(
        orphan_set_empty(&fs_after).await,
        "drain must reclaim the orphan after the lost reclaim txn"
    );
    assert!(matches!(
        fs_after.inode_store.get(file_id).await,
        Err(zerofs::fs::errors::FsError::NotFound)
    ));
}

// ---------------------------------------------------------------------------
// Data plane (extent-over-segments) crash windows.
// ---------------------------------------------------------------------------

/// Crash mid-flush, after the open segment is sealed + PUT but before the metadata
/// manifest is durable. The manifest never captures this write, so on restart it
/// is absent (the PUT segment is orphaned and reclaimable), never a durable
/// FrameLoc pointing at a missing segment.
#[tokio::test]
async fn test_crash_flush_after_seal_before_manifest() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"f.txt", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 200_000]))
        .await
        .unwrap();

    // The flush coordinator task panics between seal and manifest flush; the flush
    // call then errors (its reply sender was dropped).
    fail::cfg(fp::FLUSH_AFTER_SEAL_BEFORE_MANIFEST, "panic").unwrap();
    let _ = fs.flush_coordinator.flush().await;
    fail::cfg(fp::FLUSH_AFTER_SEAL_BEFORE_MANIFEST, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");
}

/// Crash mid-compaction, after the packed segment is sealed + PUT but before any
/// extent is repointed to it. The repoint never commits, so every source frame stays
/// live and readable (no relocated data lost); the packed segment is orphaned.
#[tokio::test]
async fn test_crash_compact_after_seal_before_repoint() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"f.txt", &SetAttributes::default())
        .await
        .unwrap();
    // Write + seal segment A, then overwrite extent 0 + seal segment B, so B holds
    // the only live copy of extent 0 and A's copy is dead.
    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 4096]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![2u8; 4096]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    let segids = list_segments(&ctx.object_store).await;
    fail::cfg(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT, "panic").unwrap();
    let es = fs.extent_store.clone();
    let handle =
        tokio::task::spawn(async move { es.compact_segments(&segids, &[], 256 << 20).await });
    let _ = handle.await;
    fail::cfg(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    // Extent 0 still resolves to B's live frame: the current content survived.
    let data = fs_after.extent_store.read(file_id, 0, 4096).await.unwrap();
    assert_eq!(
        &data[..],
        &vec![2u8; 4096][..],
        "compaction crash lost data"
    );
}

/// An overwrite whose commit lands between compaction's gather and its repoint,
/// with both frames in the same source segment (any rewrite within one seal
/// window). The gather resolves the pointer to the old frame; the repoint's
/// conditional swap must then reject the move, or it reverts the extent to the
/// old frame's content and the acked overwrite is lost. The pause failpoint
/// models the commit worker stalled (semi-sync ship, sync-writes flush queued
/// behind the GC barrier) while the pass runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_compact_repoint_rejects_overwrite_committed_during_pack() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"f.txt", &SetAttributes::default())
        .await
        .unwrap();
    // Frame a: committed. Frame b: staged into the same open segment (appended
    // to its buffer with the pointer-put left uncommitted, as when the commit
    // worker is backed up).
    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 4096]))
        .await
        .unwrap();
    let mut txn2 = fs.db.new_transaction().unwrap();
    fs.extent_store
        .write(&mut txn2, file_id, 0, &Bytes::from(vec![2u8; 4096]), 4096)
        .await
        .unwrap();
    // Seal: one segment holding both frames, committed pointer still frame a.
    fs.extent_store.seal_open().await.unwrap();
    let sources = list_segments(&ctx.object_store).await;
    assert_eq!(sources.len(), 1, "both frames must share one segment");

    // Compact the source; the gather resolves frame a and packs its bytes,
    // then parks after the packed PUT, before the repoint.
    fail::cfg(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT, "pause").unwrap();
    let es = fs.extent_store.clone();
    let handle =
        tokio::task::spawn(async move { es.compact_segments(&sources, &[], 256 << 20).await });
    let mut waited = 0;
    while list_segments(&ctx.object_store).await.len() < 2 {
        waited += 1;
        assert!(waited < 500, "compaction never reached the packed PUT");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // The overwrite's commit now lands, still pointing into the source segment.
    fs.write_coordinator.commit(txn2).await.unwrap();
    let data = fs.extent_store.read(file_id, 0, 4096).await.unwrap();
    assert_eq!(
        &data[..],
        &vec![2u8; 4096][..],
        "overwrite visible pre-repoint"
    );

    fail::cfg(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT, "off").unwrap();
    handle.await.unwrap().unwrap();

    let data = fs.extent_store.read(file_id, 0, 4096).await.unwrap();
    assert_eq!(
        &data[..],
        &vec![2u8; 4096][..],
        "repoint reverted an acked overwrite to the gathered stale frame"
    );
}

/// Reclaim classifies deadness from the durable view, and this harness runs the
/// production SlateDB config (WAL off, size-freeze off), where durable rows come
/// only from the barrier's own flush. A dead segment must still be deleted in
/// one pass here proving the durable scan sees barrier-flushed deaths while
/// a kill committed after the barrier must defer and not delete.
#[tokio::test]
async fn test_reclaim_deletes_from_the_durable_view_without_a_wal() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"f.txt", &SetAttributes::default())
        .await
        .unwrap();
    // Segment A (superseded), then segment B holding the live copy.
    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 4096]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![2u8; 4096]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    assert_eq!(list_segments(&ctx.object_store).await.len(), 2);

    let (deleted, _) = reclaim_now(&fs.extent_store).await.unwrap();
    assert_eq!(
        deleted, 1,
        "the durable scan must see the barrier-flushed death and delete in one pass"
    );
    assert_eq!(list_segments(&ctx.object_store).await.len(), 1);
    let data = fs.extent_store.read(file_id, 0, 4096).await.unwrap();
    assert_eq!(&data[..], &vec![2u8; 4096][..]);
}

/// Crash mid-reclaim, after a dead segment's object is deleted but before its
/// `segcount` counter key is dropped. The segment was directory-verified dead, so no
/// FrameLoc dangles; the stale counter is a benign leak (a later pass ignores it, as
/// the object is no longer listed).
#[tokio::test]
async fn test_crash_reclaim_after_segment_delete() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"f.txt", &SetAttributes::default())
        .await
        .unwrap();
    // Seal segment A, then overwrite so A becomes fully dead, and seal B.
    fs.write(&auth, file_id, 0, &Bytes::from(vec![1u8; 4096]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    fs.write(&auth, file_id, 0, &Bytes::from(vec![2u8; 4096]))
        .await
        .unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    fail::cfg(fp::RECLAIM_AFTER_SEGMENT_DELETE, "panic").unwrap();
    let es = fs.extent_store.clone();
    let handle = tokio::task::spawn(async move { reclaim_now(&es).await });
    let _ = handle.await;
    fail::cfg(fp::RECLAIM_AFTER_SEGMENT_DELETE, "off").unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let data = fs_after.extent_store.read(file_id, 0, 4096).await.unwrap();
    assert_eq!(&data[..], &vec![2u8; 4096][..], "reclaim crash lost data");
}

/// A non-crash seal error on the flush path (the directory AEAD seal fails) must
/// not drop the open buffer. The just-written extents and their already-committed
/// FrameLocs stay readable, and once the fault clears a retry seals them durably.
/// Guards a regression: the buffer was taken and the segid rotated before
/// finalize ran, so a seal error stranded the extents behind a dangling FrameLoc
/// (404 -> EIO) with no way to recover them.
#[tokio::test]
async fn test_seal_error_preserves_open_buffer() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    let (file_id, _) = fs
        .create(&creds, 0, b"f.txt", &SetAttributes::default())
        .await
        .unwrap();
    // Two extents' worth, well below the seal threshold, so it sits in the un-sealed
    // open buffer with its FrameLocs already committed.
    let payload = vec![7u8; 40_000];
    fs.write(&auth, file_id, 0, &Bytes::from(payload.clone()))
        .await
        .unwrap();

    // Force the seal to fail: the flush must error (no durable manifest) and leave
    // the open buffer intact.
    fail::cfg(fp::SEAL_OPEN_FAIL, "return").unwrap();
    assert!(
        fs.flush_coordinator.flush().await.is_err(),
        "a seal error must fail the flush, not silently commit"
    );
    // The extent still reads back from the preserved open buffer. Before the fix
    // this hit a rotated-away, never-PUT segid and errored.
    let during = fs
        .extent_store
        .read(file_id, 0, payload.len() as u64)
        .await
        .expect("seal error dropped the open buffer");
    assert_eq!(&during[..], &payload[..], "open buffer content changed");

    // Fault clears: the retry seals the preserved buffer and the data is durable.
    fail::cfg(fp::SEAL_OPEN_FAIL, "off").unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    drop(fs);

    let fs_after = ctx.restart_fs().await;
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(report.is_consistent(), "Inconsistent:\n{report}");
    let after = fs_after
        .extent_store
        .read(file_id, 0, payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(&after[..], &payload[..], "retried seal lost the data");
}

/// With the WAL off and l0_sst_size_bytes at MAX (the production config this
/// harness mirrors), the only path to a durable manifest is the seal-gated
/// flush, which PUTs the open segment before committing. So an un-flushed write is
/// lost cleanly on a crash rather than leaving a durable FrameLoc pointing at a
/// segment that was never PUT (a dangling 404 -> EIO). A durably-flushed file is
/// unaffected. This is a consistency check of the un-flushed-write path under the
/// real config; the seal-before-flush barrier that makes an *explicit* flush safe
/// is covered by the `*_persists_after_flush` tests.
#[tokio::test]
async fn test_unflushed_write_leaves_no_dangling_frameloc() {
    let (
        _scenario,
        TestSetup {
            ctx,
            fs,
            creds,
            auth,
        },
    ) = TestSetup::new().await;

    // File A: written and flushed, so its open segment is durably PUT.
    let (a_id, _) = fs
        .create(&creds, 0, b"a.txt", &SetAttributes::default())
        .await
        .unwrap();
    let a_data = Bytes::from(vec![0xA5u8; 20_000]);
    fs.write(&auth, a_id, 0, &a_data).await.unwrap();
    fs.flush_coordinator.flush().await.unwrap();

    // File B: several extents written without a flush. Their FrameLocs live only in
    // the memtable and B's open segment is never sealed/PUT.
    let (b_id, _) = fs
        .create(&creds, 0, b"b.txt", &SetAttributes::default())
        .await
        .unwrap();
    for i in 0..8u64 {
        fs.write(&auth, b_id, i * 32_768, &Bytes::from(vec![7u8; 30_000]))
            .await
            .unwrap();
    }

    // Crash: drop without flushing. With the WAL off, B's un-flushed state is lost
    // cleanly rather than resurrected against a segment that was never PUT.
    drop(fs);
    let fs_after = ctx.restart_fs().await;

    // No dangling FrameLoc anywhere: verify_consistency reads every file's extents
    // back and flags any that 404 into EIO.
    let report = verify_consistency(&fs_after).await.unwrap();
    assert!(
        report.is_consistent(),
        "un-flushed write left a dangling FrameLoc:\n{report}"
    );

    // The flushed file survived intact (its segment was PUT), so the check above
    // didn't pass vacuously.
    let got = fs_after
        .extent_store
        .read(a_id, 0, a_data.len() as u64)
        .await
        .unwrap();
    assert_eq!(
        &got[..],
        &a_data[..],
        "flushed file lost data across the crash"
    );
}
