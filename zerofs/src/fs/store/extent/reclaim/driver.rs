use super::super::ExtentStore;
use super::cycle::{self, CyclePolicy, SegmentProtection};
use crate::config::ReclaimConfig;
use crate::fs::FsError;
use crate::task::{spawn_named, spawn_named_on};
use slatedb::admin::Admin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Floor the delete horizon past now, covering the writer's in-flight reads
/// when no checkpoint pins an older view.
const INFLIGHT_FLOOR_SECS: u64 = 60;
/// Extra grace after waiting the checkpoint's full recorded lifetime.
const CHECKPOINT_GRACE: Duration = Duration::from_secs(30);
/// Monotonic cadence of the slow orphan sweep, the only segment namespace LIST.
const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Backoff after a failed scan or while a persistent checkpoint blocks work.
const RECLAIM_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Derive this cycle's segment protection from the live checkpoint list.
pub(super) async fn checkpoint_protection(
    checkpoint_admin: Option<&Admin>,
) -> Result<SegmentProtection, FsError> {
    let Some(admin) = checkpoint_admin else {
        return Ok(SegmentProtection::Until(
            Instant::now() + Duration::from_secs(INFLIGHT_FLOOR_SECS),
        ));
    };
    let checkpoints = match admin.list_checkpoints(None).await {
        Ok(checkpoints) => checkpoints,
        Err(error) => {
            tracing::warn!("segment reclamation skipped: cannot list checkpoints: {error}");
            return Err(FsError::IoError);
        }
    };
    if checkpoints
        .iter()
        .any(|checkpoint| checkpoint.expire_time.is_none())
    {
        return Ok(SegmentProtection::Indefinite);
    }
    // Both timestamps belong to the checkpoint: never subtract the writer's
    // wall clock from a reader's expiry. Waiting the entire recorded span from
    // this observation conservatively covers its remaining lifetime, regardless
    // of their clock offset. Renewals leave create_time unchanged, so old renewed
    // checkpoints can over-retain; do not cap that span using an assumed TTL.
    let now = Instant::now();
    let mut delete_horizon = now + Duration::from_secs(INFLIGHT_FLOOR_SECS);
    for checkpoint in checkpoints {
        let expiry = checkpoint
            .expire_time
            .expect("persistent checkpoints checked");
        let deadline = (expiry - checkpoint.create_time)
            .to_std()
            .ok()
            .and_then(|lifetime| lifetime.checked_add(CHECKPOINT_GRACE))
            .and_then(|delay| now.checked_add(delay))
            .ok_or_else(|| {
                tracing::warn!(
                    checkpoint_id = %checkpoint.id,
                    create_time = %checkpoint.create_time,
                    expire_time = %expiry,
                    "segment reclamation skipped: invalid checkpoint retention span"
                );
                FsError::IoError
            })?;
        delete_horizon = delete_horizon.max(deadline);
    }
    Ok(SegmentProtection::Until(delete_horizon))
}

async fn run_cycle(
    store: &ExtentStore,
    checkpoint_admin: Option<&Admin>,
    policy: CyclePolicy,
    shutdown: &CancellationToken,
) -> bool {
    let protection = || checkpoint_protection(checkpoint_admin);
    match cycle::run(store, protection, policy, shutdown).await {
        Ok(_) => true,
        Err(error) => {
            tracing::error!("segment reclamation failed: {error:?}");
            false
        }
    }
}

async fn initial_orphan_deadline(store: &ExtentStore) -> Instant {
    let delay = match cycle::orphan_sweep_startup_delay(store, ORPHAN_SWEEP_INTERVAL).await {
        Ok(delay) => delay,
        Err(error) => {
            // The record is a scheduling hint. A failed read cannot establish
            // a delay, so attempt a sweep after the usual error backoff.
            tracing::error!("cannot recover orphan sweep schedule: {error:?}");
            RECLAIM_RETRY_DELAY
        }
    };
    Instant::now() + delay
}

fn next_delete_deadline(store: &ExtentStore) -> Option<Instant> {
    store.delete_at.lock().unwrap().values().min().copied()
}

fn earlier(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn run_orphan_sweep(store: &ExtentStore, shutdown: &CancellationToken) -> Instant {
    match cycle::sweep_orphans_and_record(store, shutdown).await {
        Ok(sweep) => {
            store
                .segment_reclaim_stats()
                .record_orphans_reclaimed(sweep.deleted() as u64);
            match sweep {
                cycle::OrphanSweep::Completed { .. } => Instant::now() + ORPHAN_SWEEP_INTERVAL,
                cycle::OrphanSweep::Interrupted { .. } => Instant::now(),
            }
        }
        Err(error) => {
            tracing::error!("slow orphan sweep failed: {error:?}");
            Instant::now() + RECLAIM_RETRY_DELAY
        }
    }
}

pub(crate) fn start_reclaimer(
    store: ExtentStore,
    checkpoint_admin: Option<Arc<Admin>>,
    config: ReclaimConfig,
    activity_delay: Duration,
    shutdown: CancellationToken,
    runtime: Option<tokio::runtime::Handle>,
) -> JoinHandle<()> {
    let policy = CyclePolicy::from(&config);
    let task = async move {
        info!(
            "Starting activity-driven segment reclamation task ({} concurrent repack job(s), {} MiB/job nominal gather budget, {}s activity coalescing)",
            policy.max_concurrent_repacks,
            policy.job_bytes >> 20,
            activity_delay.as_secs(),
        );
        let mut orphan_due = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            deadline = initial_orphan_deadline(&store) => deadline,
        };
        let mut scan_not_before = Instant::now();
        let mut activity_due = Some(scan_not_before);

        loop {
            let requested = earlier(activity_due, next_delete_deadline(&store));
            let scan_due = requested.map(|deadline| deadline.max(scan_not_before));
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = wait_until(scan_due) => {
                    let succeeded = run_cycle(
                        &store,
                        checkpoint_admin.as_deref(),
                        policy,
                        &shutdown,
                    ).await;
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let pinned = store
                        .segment_reclaim_stats()
                        .checkpoint_pinned
                        .load(Ordering::Relaxed);
                    let delay = if succeeded && !pinned {
                        activity_delay
                    } else {
                        RECLAIM_RETRY_DELAY
                    };
                    scan_not_before = Instant::now() + delay;
                    activity_due = store
                        .reclaim_activity
                        .pending()
                        .then_some(scan_not_before);
                }
                _ = tokio::time::sleep_until(orphan_due) => {
                    orphan_due = run_orphan_sweep(&store, &shutdown).await;
                    if store.reclaim_activity.pending() && activity_due.is_none() {
                        activity_due = Some(Instant::now() + activity_delay);
                    }
                }
                _ = store.reclaim_activity.notified() => {
                    if store.reclaim_activity.pending() && activity_due.is_none() {
                        activity_due = Some(Instant::now() + activity_delay);
                    }
                }
            }
        }
    };

    match runtime {
        Some(runtime) => spawn_named_on("segment-reclaimer", task, &runtime),
        None => spawn_named("segment-reclaimer", task),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::test_util::{
        commit, frameloc_of, incompressible, make, make_with_compression, test_policy,
        write_and_check,
    };
    use super::*;
    use crate::config::CompressionConfig;
    use crate::fs::ZeroFS;
    use crate::fs::key_codec::KeyCodec;
    use bytes::Bytes;
    use chrono::Utc;
    use slatedb::admin::AdminBuilder;
    use slatedb::config::CheckpointOptions;
    use slatedb::object_store::path::Path;
    use slatedb_common::{DefaultSystemClock, SystemClock, SystemClockTicker};
    use std::sync::atomic::AtomicI64;

    #[derive(Debug, Default)]
    struct JumpingClock {
        base: DefaultSystemClock,
        offset_secs: AtomicI64,
    }

    impl SystemClock for JumpingClock {
        fn now(&self) -> chrono::DateTime<Utc> {
            self.base.now() + chrono::Duration::seconds(self.offset_secs.load(Ordering::Relaxed))
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            self.base.sleep(duration)
        }

        fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
            SystemClockTicker::new(self, duration)
        }
    }

    async fn write_sweep_record(store: &ExtentStore, value: Bytes) {
        let mut txn = store.db.new_transaction().unwrap();
        txn.put_bytes(&store.key_codec.last_orphan_sweep_key(), value);
        commit(store, txn).await;
    }

    #[tokio::test(start_paused = true)]
    async fn orphan_startup_delay_recovers_and_clamps_timestamps() {
        let (store, _db) = make().await;
        let clock = Arc::new(JumpingClock::default());
        store.set_reclaim_clock(clock.clone());
        assert_eq!(
            cycle::orphan_sweep_startup_delay(&store, ORPHAN_SWEEP_INTERVAL)
                .await
                .unwrap(),
            Duration::ZERO,
        );
        for (offset_secs, expected) in [
            (-2 * 86_400, Duration::ZERO),
            (-3600, Duration::from_secs(23 * 3600)),
            (0, ORPHAN_SWEEP_INTERVAL),
            (7 * 86_400, ORPHAN_SWEEP_INTERVAL),
        ] {
            let last = clock.now().timestamp() + offset_secs;
            write_sweep_record(&store, KeyCodec::encode_u64(last as u64)).await;
            let delay = cycle::orphan_sweep_startup_delay(&store, ORPHAN_SWEEP_INTERVAL)
                .await
                .unwrap();
            // Persisted timestamps have second precision.
            assert!(delay <= expected, "offset {offset_secs}: {delay:?}");
            assert!(delay >= expected.saturating_sub(Duration::from_secs(1)));
        }
        for invalid in [
            Bytes::from_static(b"malformed"),
            KeyCodec::encode_u64(u64::MAX),
            KeyCodec::encode_u64(i64::MAX as u64),
        ] {
            write_sweep_record(&store, invalid).await;
            assert_eq!(
                cycle::orphan_sweep_startup_delay(&store, ORPHAN_SWEEP_INTERVAL)
                    .await
                    .unwrap(),
                Duration::ZERO,
            );
        }
    }

    async fn put_orphan(store: &ExtentStore) -> crate::segment::Segid {
        store
            .segments
            .seal(&[(9999, 0, Bytes::from(vec![3u8; crate::fs::EXTENT_SIZE]))])
            .await
            .unwrap()[0]
            .2
            .segid
    }

    async fn wait_for_orphans(store: &ExtentStore, expected: u64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while store
                .segment_reclaim_stats()
                .orphans_reclaimed
                .load(Ordering::Relaxed)
                < expected
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduled orphan sweep did not finish");
    }

    #[tokio::test(start_paused = true)]
    async fn orphan_sweeps_ignore_clock_jumps_after_startup() {
        let (store, _db) = make().await;
        let clock = Arc::new(JumpingClock::default());
        store.set_reclaim_clock(clock.clone());
        let future = clock.now().timestamp() + 7 * 86_400;
        write_sweep_record(&store, KeyCodec::encode_u64(future as u64)).await;
        let first = put_orphan(&store).await;
        let shutdown = CancellationToken::new();
        let handle = start_reclaimer(
            (*store).clone(),
            None,
            ReclaimConfig::default(),
            Duration::from_secs(30),
            shutdown.clone(),
            None,
        );
        wait_for_cycles(&store, 1).await;

        // The future record is capped at one day, and a forward clock jump
        // cannot make that recovered monotonic deadline arrive earlier.
        clock.offset_secs.store(30 * 86_400, Ordering::Relaxed);
        tokio::time::advance(ORPHAN_SWEEP_INTERVAL - Duration::from_secs(2)).await;
        assert_eq!(
            store
                .segment_reclaim_stats()
                .orphans_reclaimed
                .load(Ordering::Relaxed),
            0
        );
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&first)
        );

        // Once due, run even though the wall clock says the stored record is
        // still over a month in the future. Record the completed sweep's time.
        clock.offset_secs.store(-30 * 86_400, Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(3)).await;
        wait_for_orphans(&store, 1).await;
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&first)
        );
        let recorded = store
            .db
            .get_bytes(&store.key_codec.last_orphan_sweep_key())
            .await
            .unwrap()
            .unwrap();
        let recorded = KeyCodec::decode_u64(&recorded).unwrap() as i64;
        assert!((clock.now().timestamp() - recorded).abs() <= 1);

        // Subsequent sweeps also use only elapsed monotonic time, despite
        // another forward jump followed by a larger backward correction.
        let second = put_orphan(&store).await;
        clock.offset_secs.store(30 * 86_400, Ordering::Relaxed);
        tokio::time::advance(ORPHAN_SWEEP_INTERVAL - Duration::from_secs(2)).await;
        assert_eq!(
            store
                .segment_reclaim_stats()
                .orphans_reclaimed
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&second)
        );
        clock.offset_secs.store(-60 * 86_400, Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(3)).await;
        wait_for_orphans(&store, 2).await;
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&second)
        );

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_orphan_sweep_preserves_restart_record() {
        let (store, _db) = make().await;
        let record = KeyCodec::encode_u64(42);
        write_sweep_record(&store, record.clone()).await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            cycle::sweep_orphans_and_record(&store, &cancel)
                .await
                .unwrap(),
            cycle::OrphanSweep::Interrupted { deleted: 0 },
        ));
        assert_eq!(
            store
                .db
                .get_bytes(&store.key_codec.last_orphan_sweep_key())
                .await
                .unwrap(),
            Some(record),
        );
    }

    async fn check_delete_deadline_with_clock_jumps(
        checkpoint_lifetime: Option<Duration>,
        reader_offset_secs: i64,
        writer_offset_secs: i64,
        renewal_after: Duration,
    ) {
        let (store, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        let clock = Arc::new(JumpingClock::default());
        clock
            .offset_secs
            .store(writer_offset_secs, Ordering::Relaxed);
        store.set_reclaim_clock(clock.clone());
        let reader_clock = Arc::new(JumpingClock::default());
        reader_clock
            .offset_secs
            .store(reader_offset_secs, Ordering::Relaxed);
        let admin = AdminBuilder::new(Path::from("t"), object_store)
            .with_system_clock(reader_clock)
            .build();
        let checkpoint_admin = checkpoint_lifetime.map(|_| &admin);
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        db.flush().await.unwrap();
        let dead = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        if let Some(lifetime) = checkpoint_lifetime {
            let checkpoint = admin
                .create_detached_checkpoint(&CheckpointOptions {
                    lifetime: Some(lifetime),
                    ..Default::default()
                })
                .await
                .unwrap();
            if !renewal_after.is_zero() {
                tokio::time::sleep(renewal_after).await;
                admin
                    .refresh_checkpoint(checkpoint.id, Some(lifetime))
                    .await
                    .unwrap();
            }
        }
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        let cancel = CancellationToken::new();

        let outcome = cycle::run(
            &store,
            || async {
                let protection = checkpoint_protection(checkpoint_admin).await?;
                // Jump between deriving protection and classifying the segment.
                clock.offset_secs.store(24 * 60 * 60, Ordering::Relaxed);
                Ok(protection)
            },
            test_policy(),
            &cancel,
        )
        .await
        .unwrap();
        assert_eq!(outcome.deleted, 0);
        let deadline = store.delete_at.lock().unwrap()[&dead];
        let expected_wait = checkpoint_lifetime
            .map_or(Duration::from_secs(INFLIGHT_FLOOR_SECS), |lifetime| {
                renewal_after + lifetime + CHECKPOINT_GRACE
            });
        let remaining = deadline - Instant::now();
        assert!(remaining <= expected_wait);
        assert!(remaining > expected_wait - Duration::from_secs(1));

        // Neither scheduling nor a later activity-driven scan may shorten the
        // recorded deadline when wall time has jumped forward.
        assert_eq!(next_delete_deadline(&store), Some(deadline));
        assert!(run_cycle(&store, checkpoint_admin, test_policy(), &cancel).await);
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&dead)
        );
        assert_eq!(next_delete_deadline(&store), Some(deadline));

        // Jump backward, then reach the deadline solely through monotonic time.
        clock.offset_secs.store(-24 * 60 * 60, Ordering::Relaxed);
        tokio::time::sleep_until(deadline - Duration::from_secs(1)).await;
        assert!(run_cycle(&store, checkpoint_admin, test_policy(), &cancel).await);
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&dead)
        );
        assert_eq!(next_delete_deadline(&store), Some(deadline));
        tokio::time::sleep_until(deadline).await;
        assert!(run_cycle(&store, checkpoint_admin, test_policy(), &cancel).await);
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&dead)
        );
        assert_eq!(next_delete_deadline(&store), None);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    #[tokio::test(start_paused = true)]
    async fn inflight_deadline_survives_wall_clock_jumps() {
        check_delete_deadline_with_clock_jumps(None, 0, 0, Duration::ZERO).await;
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_deadline_survives_wall_clock_jumps() {
        check_delete_deadline_with_clock_jumps(
            Some(Duration::from_secs(120)),
            0,
            0,
            Duration::ZERO,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_deadline_ignores_initial_clock_skew() {
        for (reader_offset, writer_offset) in [(0, 86_400), (-86_400, 0), (86_400, 0)] {
            check_delete_deadline_with_clock_jumps(
                Some(Duration::from_secs(120)),
                reader_offset,
                writer_offset,
                Duration::ZERO,
            )
            .await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn renewed_checkpoint_retains_its_full_span() {
        check_delete_deadline_with_clock_jumps(
            Some(Duration::from_secs(120)),
            -86_400,
            0,
            Duration::from_secs(90),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_checkpoint_span_blocks_reclamation() {
        let (_store, _db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        let clock = Arc::new(JumpingClock::default());
        let admin = AdminBuilder::new(Path::from("t"), object_store)
            .with_system_clock(clock.clone())
            .build();
        let lifetime = Some(Duration::from_secs(120));
        let checkpoint = admin
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime,
                ..Default::default()
            })
            .await
            .unwrap();
        clock.offset_secs.store(-86_400, Ordering::Relaxed);
        admin
            .refresh_checkpoint(checkpoint.id, lifetime)
            .await
            .unwrap();
        assert!(checkpoint_protection(Some(&admin)).await.is_err());
    }

    async fn wait_for_cycles(store: &ExtentStore, expected: u64) {
        // Real time bounds the wait even when a test has paused Tokio's clock.
        let started = std::time::Instant::now();
        loop {
            let completed = store.segment_reclaim_stats().cycles.load(Ordering::Relaxed);
            if completed >= expected {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "segment reclaimer completed {completed} cycles, expected {expected}"
            );
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn idle_store_waits_for_activity() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let store = fs.extent_store.clone();
        store
            .reclaim_activity
            .acknowledge(store.reclaim_activity.generation());
        let shutdown = CancellationToken::new();
        let handle = start_reclaimer(
            store.clone(),
            None,
            ReclaimConfig::default(),
            Duration::from_millis(30),
            shutdown.clone(),
            None,
        );
        wait_for_cycles(&store, 1).await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            store.segment_reclaim_stats().cycles.load(Ordering::Relaxed),
            1
        );

        store.record_reclaim_activity();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            store.segment_reclaim_stats().cycles.load(Ordering::Relaxed),
            1
        );
        wait_for_cycles(&store, 2).await;

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn repacking_settles_without_further_writes() {
        const SOURCE_COUNT: usize = 4;
        const SOURCE_BYTES: usize = 16 * crate::fs::EXTENT_SIZE;
        let (store, db) = make().await;
        let mut model = Vec::new();
        for source in 0..SOURCE_COUNT {
            write_and_check(
                &store,
                &db,
                &mut model,
                source * SOURCE_BYTES,
                &incompressible(source, SOURCE_BYTES),
            )
            .await;
            store.seal_open().await.unwrap();
        }
        // Keep the independent daily sweep out of this scheduler test.
        write_sweep_record(
            &store,
            KeyCodec::encode_u64(store.reclaim_now().timestamp() as u64),
        )
        .await;
        let shutdown = CancellationToken::new();
        let handle = start_reclaimer(
            (*store).clone(),
            None,
            ReclaimConfig::default(),
            Duration::from_secs(30),
            shutdown.clone(),
            None,
        );
        wait_for_cycles(&store, 1).await;
        let stats = store.segment_reclaim_stats();
        assert_eq!(stats.repack_jobs.load(Ordering::Relaxed), 1);
        let output = frameloc_of(&store, &db, 1, 0).await.unwrap();

        // Repacking wakes a scan to age its drained sources, then their deadline
        // wakes a deletion cycle. Neither follow-up may repack the new output.
        tokio::time::advance(Duration::from_secs(31)).await;
        wait_for_cycles(&store, 2).await;
        tokio::time::advance(Duration::from_secs(61)).await;
        wait_for_cycles(&store, 3).await;
        assert_eq!(
            stats.segments_deleted.load(Ordering::Relaxed),
            SOURCE_COUNT as u64
        );
        assert!(!store.reclaim_activity.pending());

        for _ in 0..3 {
            assert_eq!(
                store.read(1, 0, model.len() as u64).await.unwrap().as_ref(),
                model.as_slice()
            );
            tokio::time::advance(Duration::from_secs(3600)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                stats.cycles.load(Ordering::Relaxed),
                3,
                "cleanup must settle"
            );
            assert_eq!(stats.repack_jobs.load(Ordering::Relaxed), 1);
            assert_eq!(frameloc_of(&store, &db, 1, 0).await.unwrap(), output);
        }
        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_before_start_skips_startup_work() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let store = fs.extent_store.clone();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        start_reclaimer(
            store.clone(),
            None,
            ReclaimConfig::default(),
            Duration::from_secs(30),
            shutdown,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            store.segment_reclaim_stats().cycles.load(Ordering::Relaxed),
            0
        );
        assert!(
            store
                .db
                .get_bytes(&store.key_codec.last_orphan_sweep_key())
                .await
                .unwrap()
                .is_none()
        );
    }
}
