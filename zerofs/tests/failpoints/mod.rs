mod consistency;

use bytes::Bytes;
use slatedb::DbBuilder;
use slatedb::config::Settings;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use std::sync::Arc;
use zerofs::config::CompressionConfig;
use zerofs::encryption::SlateDbHandle;
use zerofs::fs::ZeroFS;
use zerofs::fs::permissions::Credentials;
use zerofs::fs::types::{AuthContext, SetAttributes};

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

/// Test context holding filesystem and in-memory object store.
/// The object store persists across restarts, simulating a real crash where
/// only the database state (SlateDB) is lost but storage remains.
struct CrashTestContext {
    /// In-memory object store that persists across "restarts"
    object_store: Arc<dyn ObjectStore>,
    /// Encryption key for the filesystem
    encryption_key: [u8; 32],
}

impl CrashTestContext {
    fn new() -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            encryption_key: [0u8; 32], // Test key
        }
    }

    /// Create a new filesystem instance
    async fn create_fs(&self) -> Arc<ZeroFS> {
        let settings = Settings {
            compression_codec: None,
            compactor_options: Some(slatedb::config::CompactorOptions::default()),
            ..Default::default()
        };

        let db_path = Path::from("slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path, Arc::clone(&self.object_store))
                .with_settings(settings)
                .build()
                .await
                .unwrap(),
        );

        Arc::new(
            ZeroFS::new_with_slatedb(
                SlateDbHandle::ReadWrite(slatedb),
                self.encryption_key,
                u64::MAX,
                CompressionConfig::default(),
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
async fn test_crash_write_after_chunk() {
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

    fail::cfg(fp::WRITE_AFTER_CHUNK, "panic").unwrap();

    let fs_clone = Arc::clone(&fs);
    let auth_clone = auth.clone();
    let handle = tokio::task::spawn(async move {
        fs_clone
            .write(&auth_clone, file_id, 0, &Bytes::from(vec![1u8; 1000]))
            .await
    });
    let _ = handle.await;

    fail::cfg(fp::WRITE_AFTER_CHUNK, "off").unwrap();

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
async fn test_crash_gc_after_chunk_delete() {
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

    fail::cfg(fp::GC_AFTER_CHUNK_DELETE, "panic").unwrap();

    let gc = Arc::new(GarbageCollector::new(
        Arc::clone(&fs.db),
        fs.tombstone_store.clone(),
        fs.chunk_store.clone(),
        Arc::clone(&fs.stats),
    ));
    let handle = tokio::task::spawn(async move { gc.run().await });
    let _ = handle.await;

    fail::cfg(fp::GC_AFTER_CHUNK_DELETE, "off").unwrap();
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
        fs.chunk_store.clone(),
        Arc::clone(&fs.stats),
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
async fn test_crash_truncate_after_chunks() {
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

    fail::cfg(fp::TRUNCATE_AFTER_CHUNKS, "panic").unwrap();

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

    fail::cfg(fp::TRUNCATE_AFTER_CHUNKS, "off").unwrap();
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
