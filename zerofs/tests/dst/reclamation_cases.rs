//! Forced reclamation windows complement the random maintenance actors.

use super::*;
use crate::reclamation::cleaner;
use futures::TryStreamExt;
use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, DbReaderOptions};
use slatedb::{DbReader, DbReaderMode};
use slatedb_common::SystemClock;
use zerofs::fs::key_codec::KeyCodec;
use zerofs::fs::store::extent::reclaim::{repack, run_with_checkpoints};
use zerofs::segment::{FrameLoc, Segid};
use zerofs::segment_store::SegmentStore;

fn run_case<F: std::future::Future<Output = ()>>(
    seed: u64,
    case: impl FnOnce(WorldConfig, Digest) -> F,
) {
    zerofs::fs::DST_FIXED_TIME.store(true, Relaxed);
    zerofs::db::DST_PANIC_ON_WRITE_ERROR.store(true, Relaxed);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()))
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    let config = WorldConfig {
        seed,
        failpoint_crashes: false,
        rounds: 0,
        ops_per_file: 0,
        reclamation_scans: 0,
        crash_pct: 0,
        fault_ppm: 0,
        scale: Scale {
            file_cap: 16 * EXTENT_SIZE,
            seal_threshold: 4 * 1024 * 1024,
        },
        file_mix: FileOpMix::new(1, 1, 1, 1),
    };
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(3600), case(config, Digest::default()))
            .await
            .expect("targeted reclamation case exceeded one hour of virtual time");
    });
}

async fn create_file(fs: &ZeroFS, name: &[u8], data: &[u8]) -> InodeId {
    let (id, _) = fs
        .create(&creds(), 0, name, &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth(), id, 0, &Bytes::copy_from_slice(data))
        .await
        .unwrap();
    id
}

async fn location(fs: &ZeroFS, id: InodeId, extent: u64) -> FrameLoc {
    FrameLoc::decode(
        &fs.db
            .get_bytes(&KeyCodec::new().extent_key(id, extent))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

fn segments(storage: &Storage) -> SegmentStore {
    // No filesystem read cache: a deleted object must not be hidden by a hit.
    SegmentStore::new(storage.backing.clone(), crate::segment_codec(), 0)
}

async fn segment_ids(storage: &Storage) -> Vec<Segid> {
    segments(storage)
        .list_segments_stream()
        .try_collect()
        .await
        .unwrap()
}

struct RepackFixture {
    live: Vec<(InodeId, Vec<u8>)>,
    victim: InodeId,
    source: Segid,
}

impl RepackFixture {
    async fn new(fs: &ZeroFS, seed: u64) -> Self {
        let mut live = Vec::new();
        for index in 0..2 {
            let data = pattern(seed ^ index, 12 * EXTENT_SIZE);
            let id = create_file(fs, format!("keep{index}").as_bytes(), &data).await;
            live.push((id, data));
        }
        let victim = create_file(fs, b"remove", &pattern(seed ^ 2, EXTENT_SIZE)).await;
        // Two populated extents, but enough logical extents for multiple cleanup
        // commits. A crash must preserve progress and counter debits atomically.
        fs.write(
            &auth(),
            victim,
            10_010 * EXTENT_SIZE as u64,
            &Bytes::from(pattern(seed ^ 3, EXTENT_SIZE)),
        )
        .await
        .unwrap();
        for index in 0..34 {
            let data = pattern(seed ^ (index + 4), EXTENT_SIZE);
            fs.write(&auth(), live[0].0, 0, &Bytes::copy_from_slice(&data))
                .await
                .unwrap();
            live[0].1[..EXTENT_SIZE].copy_from_slice(&data);
        }
        fs.client_fsync().await.unwrap();
        let source = location(fs, live[0].0, 0).await.segid;
        assert_eq!(source, location(fs, live[1].0, 0).await.segid);
        assert_eq!(source, location(fs, victim, 10_010).await.segid);
        fs.remove(&auth(), 0, b"remove").await.unwrap();
        fs.client_fsync().await.unwrap();
        assert!(
            fs.tombstone_store
                .list()
                .await
                .unwrap()
                .next()
                .await
                .is_some()
        );
        Self {
            live,
            victim,
            source,
        }
    }

    async fn verify(&self, storage: &Storage, seed: u64) {
        let fs = storage.fs();
        fs.write_coordinator.barrier().await.unwrap();
        for (id, expected) in &self.live {
            assert_eq!(FileSnapshot::read(fs, *id).await.as_bytes(), expected);
            for (extent, bytes) in expected.chunks(EXTENT_SIZE).enumerate() {
                let loc = location(fs, *id, extent as u64).await;
                assert_eq!(
                    segments(storage)
                        .read_extent(loc, *id, extent as u64)
                        .await
                        .unwrap()
                        .as_ref(),
                    bytes
                );
            }
        }
        Checks::new(fs, seed, 0).verify().await;
        crate::checks::verify_referenced_segments(fs, storage.backing.clone(), seed, 0).await;
    }

    async fn finish_cleanup(&self, storage: &Storage, seed: u64) {
        let fs = storage.fs();
        cleaner(fs).run().await.unwrap();
        assert!(
            fs.tombstone_store
                .list()
                .await
                .unwrap()
                .next()
                .await
                .is_none()
        );
        assert!(
            fs.db
                .get_bytes(&KeyCodec::new().extent_key(self.victim, 0))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            fs.db
                .get_bytes(&KeyCodec::new().extent_key(self.victim, 10_010))
                .await
                .unwrap()
                .is_none()
        );
        self.verify(storage, seed).await;
    }
}

#[test]
fn cleanup_races_repack_and_same_epoch_sweep() {
    for seed in [11, 29, 47] {
        run_case(seed, |config, digest| async move {
            let mut storage = Storage::new(&config, &digest).await;
            let fixture = RepackFixture::new(storage.fs(), config.seed).await;
            let fs = storage.fs().clone();
            let cleanup = cleaner(&fs);
            let cancel = CancellationToken::new();
            let sources = [fixture.source];
            let (cleaned, packed, swept) = tokio::join!(
                cleanup.run(),
                repack::run(&fs.extent_store, &sources, 4 * 1024 * 1024),
                cycle::sweep_orphans(&fs.extent_store, &cancel),
            );
            cleaned.unwrap();
            packed.unwrap();
            swept.unwrap();
            assert!(fs.stats.tombstone_cleanup_extents_deleted.load(Relaxed) >= 10_011);
            let output = location(&fs, fixture.live[0].0, 0).await.segid;
            assert_ne!(output, fixture.source, "repack must publish live pointers");
            assert_eq!(
                output.epoch, fixture.source.epoch,
                "no writer restart during the race"
            );
            fixture.finish_cleanup(&storage, config.seed).await;
            fs.client_fsync().await.unwrap();
            drop(fs);
            storage.crash_and_reopen(&config, &digest, 0).await;
            fixture.finish_cleanup(&storage, config.seed).await;
        });
    }
}

#[test]
fn cancelled_queued_repoint_survives_same_epoch_sweep() {
    run_case(53, |config, digest| async move {
        let mut storage = Storage::new(&config, &digest).await;
        let fixture = RepackFixture::new(storage.fs(), config.seed).await;
        let fs = storage.fs().clone();
        let write_barrier = fs.db.flush_barrier().write_owned().await;
        let store = fs.extent_store.clone();
        let source = fixture.source;
        let repack =
            tokio::spawn(async move { repack::run(&store, &[source], 4 * 1024 * 1024).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // Yield to simulated I/O without keeping the paused runtime
                // continuously runnable and preventing its clock from advancing.
                tokio::time::sleep(Duration::from_millis(1)).await;
                match tokio::time::timeout(
                    Duration::from_millis(50),
                    fs.write_coordinator.barrier(),
                )
                .await
                {
                    Ok(result) => {
                        result.unwrap();
                        assert!(
                            !repack.is_finished(),
                            "repack stopped before submitting a commit"
                        );
                    }
                    Err(_) => break,
                }
            }
        })
        .await
        .expect("repoint did not reach the commit queue");
        repack.abort();
        assert!(matches!(repack.await, Err(error) if error.is_cancelled()));
        let mut inode_lock = Box::pin(fs.lock_manager.acquire(fixture.live[0].0));
        assert!(
            futures::poll!(inode_lock.as_mut()).is_pending(),
            "queued repoint must retain its inode lock"
        );
        drop(inode_lock);
        let cancel = CancellationToken::new();
        let mut sweep = Box::pin(cycle::sweep_orphans(&fs.extent_store, &cancel));
        assert!(futures::poll!(sweep.as_mut()).is_pending());
        // The sweep must still be waiting for the queued repoint's reference
        // pin, not already waiting on the flush barrier. Downgrading permits
        // reads; a prematurely queued flush writer would block this try_read.
        // No other task can apply the repoint between these synchronous steps.
        let read_barrier = tokio::sync::OwnedRwLockWriteGuard::downgrade(write_barrier);
        assert!(
            fs.db.flush_barrier().try_read().is_ok(),
            "sweep crossed the reference barrier before the cancelled repoint applied"
        );
        drop(read_barrier);
        sweep.await.unwrap();
        let output = location(&fs, fixture.live[0].0, 0).await.segid;
        assert_ne!(
            output, source,
            "the cancelled caller's queued commit must apply"
        );
        assert_eq!(
            location(&fs, fixture.live[1].0, 0).await.segid,
            source,
            "later repoints were cancelled"
        );
        assert_eq!(output.epoch, source.epoch);
        fixture.verify(&storage, config.seed).await;
        fixture.finish_cleanup(&storage, config.seed).await;
        fs.client_fsync().await.unwrap();
        drop(fs);
        storage.crash_and_reopen(&config, &digest, 0).await;
        fixture.finish_cleanup(&storage, config.seed).await;
    });
}

#[cfg(feature = "failpoints")]
#[test]
fn cancelled_unpublished_repack_is_swept_without_restart() {
    let scenario = fail::FailScenario::setup();
    fp_crash::register_callbacks();
    run_case(67, |config, digest| async move {
        let storage = Storage::new(&config, &digest).await;
        let fixture = RepackFixture::new(storage.fs(), config.seed).await;
        let fs = storage.fs();
        let before = segment_ids(&storage).await;
        let point = zerofs::failpoints::REPACK_AFTER_SEAL_BEFORE_REPOINT;
        let (fired, mut hit) = tokio::sync::watch::channel(false);
        *fp_crash::ARMED.lock().unwrap() = Some(fp_crash::Armed {
            point,
            thread: std::thread::current().id(),
            hits_left: 1,
            fire: Box::new(move || {
                fired.send_replace(true);
            }),
        });
        *zerofs::failpoints::WIDEN.lock().unwrap() =
            Some((point, std::thread::current().id(), 1000));
        let sources = [fixture.source];
        tokio::select! {
            biased;
            _ = hit.changed() => {}
            _ = repack::run(&fs.extent_store, &sources, 4 * 1024 * 1024) => panic!("repack escaped the forced cancellation window"),
        }
        *zerofs::failpoints::WIDEN.lock().unwrap() = None;
        assert!(*hit.borrow(), "the after-PUT failpoint must fire");
        let after = segment_ids(&storage).await;
        let orphan: Vec<_> = after
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(
            orphan.len(),
            1,
            "cancellation must leave one sealed, unpublished output"
        );
        assert_eq!(orphan[0].epoch, fixture.source.epoch);
        assert_eq!(
            location(fs, fixture.live[0].0, 0).await.segid,
            fixture.source
        );
        let swept = cycle::sweep_orphans(&fs.extent_store, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(swept.deleted(), 1);
        assert!(!segment_ids(&storage).await.contains(&orphan[0]));
        fixture.finish_cleanup(&storage, config.seed).await;
    });
    scenario.teardown();
}

#[cfg(feature = "failpoints")]
#[test]
fn cleanup_and_repack_crash_windows_recover_referenced_data() {
    let scenario = fail::FailScenario::setup();
    fp_crash::register_callbacks();
    for (point, hits, cleanup) in [
        (zerofs::failpoints::TOMBSTONE_CLEANUP_BEFORE_COMMIT, 1, true),
        (zerofs::failpoints::TOMBSTONE_CLEANUP_AFTER_COMMIT, 1, true),
        (
            zerofs::failpoints::REPACK_AFTER_SEAL_BEFORE_REPOINT,
            1,
            false,
        ),
        (zerofs::failpoints::REPACK_BETWEEN_REPOINTS, 2, false),
    ] {
        run_case(71, |config, digest| async move {
            let mut storage = Storage::new(&config, &digest).await;
            let fixture = RepackFixture::new(storage.fs(), config.seed).await;
            let (fired, mut hit) = tokio::sync::watch::channel(false);
            let sim = storage.sim().clone();
            *fp_crash::ARMED.lock().unwrap() = Some(fp_crash::Armed {
                point,
                thread: std::thread::current().id(),
                hits_left: hits,
                fire: Box::new(move || {
                    sim.hooks.isolated.store(true, Relaxed);
                    fired.send_replace(true);
                }),
            });
            *zerofs::failpoints::WIDEN.lock().unwrap() =
                Some((point, std::thread::current().id(), 1000));
            tokio::select! {
                biased;
                _ = hit.changed() => {}
                _ = async {
                    if cleanup {
                        let _ = cleaner(storage.fs()).run().await;
                    } else {
                        let _ = repack::run(&storage.fs().extent_store, &[fixture.source], 4 * 1024 * 1024).await;
                    }
                } => {}
            }
            *zerofs::failpoints::WIDEN.lock().unwrap() = None;
            assert!(
                *hit.borrow(),
                "required crash window was not reached: {point}"
            );
            storage.crash_and_reopen(&config, &digest, 0).await;
            // Check cold references before either recovery cleanup can remove them.
            fixture.verify(&storage, config.seed).await;
            cycle::sweep_orphans(&storage.fs().extent_store, &CancellationToken::new())
                .await
                .unwrap();
            fixture.finish_cleanup(&storage, config.seed).await;
            eprintln!("dst: verified crash at {point} (hit {hits})");
        });
    }
    scenario.teardown();
}

fn policy() -> cycle::CyclePolicy {
    cycle::CyclePolicy {
        repack_min_dead_percent: 50,
        job_bytes: 4 * 1024 * 1024,
        max_concurrent_repacks: 2,
    }
}

async fn reader_for(storage: &Storage, seed: u64, mode: DbReaderMode, digest: &Digest) -> DbReader {
    // The reader has its own store wrapper and survives writer isolation/restart.
    let store = Arc::new(SimStore::new(
        storage.backing.clone(),
        seed ^ 0xC0FFEE,
        0,
        digest.clone(),
    ));
    DbReader::builder("slatedb", store)
        .with_reader_mode(mode)
        .with_seed(seed)
        .with_system_clock(storage.clock.clone())
        .with_db_cache_disabled()
        .with_filter_policies(zerofs::fs::filter_policy::filter_policies())
        .with_segment_extractor(Arc::new(zerofs::segment_extractor::ZeroFsSegmentExtractor))
        .with_options(DbReaderOptions {
            skip_wal_replay: true,
            ..Default::default()
        })
        .build()
        .await
        .unwrap()
}

async fn verify_reader(reader: &DbReader, storage: &Storage, id: InodeId, expected: &[u8]) {
    for (extent, bytes) in expected.chunks(EXTENT_SIZE).enumerate() {
        let loc = FrameLoc::decode(
            &reader
                .get(KeyCodec::new().extent_key(id, extent as u64))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            segments(storage)
                .read_extent(loc, id, extent as u64)
                .await
                .unwrap()
                .as_ref(),
            bytes
        );
    }
}

#[test]
fn managed_reader_renewal_and_replacement_protect_referenced_segments() {
    run_case(83, |config, digest| async move {
        let mut storage = Storage::new(&config, &digest).await;
        let old = pattern(config.seed, 12 * EXTENT_SIZE);
        let new = pattern(config.seed ^ 1, old.len());
        let id = create_file(storage.fs(), b"reader", &old).await;
        storage.fs().client_fsync().await.unwrap();
        let source = location(storage.fs(), id, 0).await.segid;
        let admin = AdminBuilder::new("slatedb", storage.backing.clone())
            .with_seed(config.seed)
            .with_system_clock(storage.clock.clone())
            .build();
        let reader = reader_for(
            &storage,
            config.seed,
            DbReaderMode::ManagedCheckpoint,
            &digest,
        )
        .await;
        let initial = admin.list_checkpoints(None).await.unwrap().pop().unwrap();
        verify_reader(&reader, &storage, id, &old).await;

        // The real reader renews its unchanged snapshot automatically. No manual
        // refresh of an artificially pinned, expiring checkpoint is involved.
        tokio::time::sleep(Duration::from_secs(320)).await;
        let renewed = admin.list_checkpoints(None).await.unwrap().pop().unwrap();
        assert_eq!(
            renewed.id, initial.id,
            "idle reader should renew, not replace"
        );
        assert!(renewed.expire_time > initial.expire_time);
        verify_reader(&reader, &storage, id, &old).await;

        storage
            .fs()
            .write(&auth(), id, 0, &Bytes::from(new.clone()))
            .await
            .unwrap();
        storage.fs().client_fsync().await.unwrap();
        verify_reader(&reader, &storage, id, &old).await;
        let out = run_with_checkpoints(
            &storage.fs().extent_store,
            &admin,
            policy(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(out.deleted, 0, "reader's old view is still protected");
        verify_reader(&reader, &storage, id, &old).await;
        let delete_after = tokio::time::Instant::now()
            + (renewed.expire_time.unwrap() - renewed.create_time)
                .to_std()
                .unwrap()
            + Duration::from_secs(31);

        tokio::time::sleep(Duration::from_secs(20)).await;
        let replaced = admin.list_checkpoints(None).await.unwrap().pop().unwrap();
        assert_ne!(
            replaced.id, renewed.id,
            "writer flush must cause automatic checkpoint replacement"
        );
        verify_reader(&reader, &storage, id, &new).await;
        let deadline = renewed.expire_time.unwrap() + chrono::Duration::seconds(30);
        let delay = (deadline - storage.clock.now()).to_std().unwrap();
        tokio::time::sleep(delay + Duration::from_secs(1)).await;
        let out = run_with_checkpoints(
            &storage.fs().extent_store,
            &admin,
            policy(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            out.deleted, 0,
            "wall-clock expiry does not end the full-span wait"
        );
        assert!(segment_ids(&storage).await.contains(&source));
        tokio::time::sleep_until(delete_after).await;
        let out = run_with_checkpoints(
            &storage.fs().extent_store,
            &admin,
            policy(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(
            out.deleted > 0,
            "elapsed old-view protection must allow actual deletion"
        );
        assert!(!segment_ids(&storage).await.contains(&source));
        verify_reader(&reader, &storage, id, &new).await;
        Checks::new(storage.fs(), config.seed, 0).verify().await;
        storage.fs().client_fsync().await.unwrap();
        storage.crash_and_reopen(&config, &digest, 0).await;
        assert_eq!(FileSnapshot::read(storage.fs(), id).await.as_bytes(), new);
        verify_reader(&reader, &storage, id, &new).await;
        reader.close().await.unwrap();
    });
}

#[test]
fn permanent_checkpoint_pins_old_contents_until_removed() {
    run_case(97, |config, digest| async move {
        let storage = Storage::new(&config, &digest).await;
        let old = pattern(config.seed, 12 * EXTENT_SIZE);
        let id = create_file(storage.fs(), b"checkpoint", &old).await;
        storage.fs().client_fsync().await.unwrap();
        let source = location(storage.fs(), id, 0).await.segid;
        let admin = AdminBuilder::new("slatedb", storage.backing.clone())
            .with_seed(config.seed)
            .with_system_clock(storage.clock.clone())
            .build();
        let checkpoint = admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some("permanent".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            admin.list_checkpoints(Some("permanent")).await.unwrap()[0]
                .expire_time
                .is_none()
        );
        let reader = reader_for(
            &storage,
            config.seed,
            DbReaderMode::Checkpoint(checkpoint.id),
            &digest,
        )
        .await;
        let new = pattern(config.seed ^ 1, old.len());
        storage
            .fs()
            .write(&auth(), id, 0, &Bytes::from(new.clone()))
            .await
            .unwrap();
        storage.fs().client_fsync().await.unwrap();
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_secs(700)).await;
            let out = run_with_checkpoints(
                &storage.fs().extent_store,
                &admin,
                policy(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
            assert_eq!((out.deleted, out.relocated), (0, 0));
            cycle::sweep_orphans(&storage.fs().extent_store, &CancellationToken::new())
                .await
                .unwrap();
            verify_reader(&reader, &storage, id, &old).await;
        }
        reader.close().await.unwrap();
        admin.delete_checkpoint(checkpoint.id).await.unwrap();
        run_with_checkpoints(
            &storage.fs().extent_store,
            &admin,
            policy(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(61)).await;
        let out = run_with_checkpoints(
            &storage.fs().extent_store,
            &admin,
            policy(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(out.deleted > 0);
        assert!(!segment_ids(&storage).await.contains(&source));
        assert_eq!(FileSnapshot::read(storage.fs(), id).await.as_bytes(), new);
        Checks::new(storage.fs(), config.seed, 0).verify().await;
    });
}
