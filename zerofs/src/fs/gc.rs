use crate::db::Db;
use crate::fs::EXTENT_SIZE;
use crate::fs::errors::FsError;
use crate::fs::metrics::FileSystemStats;
use crate::fs::store::{ExtentStore, TombstoneStore};
use crate::task::{spawn_named, spawn_named_on};
use bytes::Bytes;
use chrono::Utc;
use slatedb::admin::Admin;
use slatedb::config::WriteOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

const MAX_EXTENTS_PER_ROUND: usize = 10_000;
const MAX_TOMBSTONES_PER_ROUND: usize = 10_000;
/// Interval between segment reclamation passes (on its own task; see `start`).
/// Deletion safety rides on the per-segment delete horizon, not this interval, so
/// it's purely a responsiveness knob. Kept at/above the periodic flush cadence
/// (~30s) so the pass's seal+flush is a near no-op (the flush task already sealed
/// the open buffer and flushed the memtable).
const SEGMENT_GC_INTERVAL_SECS: u64 = 60;

/// Floor the delete horizon this far past now, so reclamation still covers the
/// writer's own in-flight reads when no checkpoint is currently pinning a view.
const INFLIGHT_FLOOR_SECS: i64 = 60;

/// Added past a checkpoint's declared `expire_time` before its pinned segments
/// may be deleted. `expire_time` is the reader's wall clock; this margin covers
/// clock skew (and the reader finishing its last read) so a stalled follower on a
/// slower clock can't have a segment deleted out from under it.
const CLOCK_SKEW_MARGIN_SECS: i64 = 30;

/// Margin past a persistent checkpoint's create time before a segment's object
/// mtime is considered safely newer than the pin (and thus reclaimable). Larger
/// than the expiry margin: this gates deletion of data a snapshot may reference,
/// so err well on the safe side of clock skew between the writer and the store.
const CHECKPOINT_SKEW_MARGIN_SECS: i64 = 300;

/// Cadence of the slow orphan sweep (the only `list("segments")`). Gated by a
/// persisted wall-clock timestamp, not process uptime, so it fires on this interval
/// across restarts. Coarse on purpose: the fast reclaim runs entirely off the local
/// segcount scan; this sweep exists only to reclaim counter-less orphan objects (a
/// crash between sealing an object and its crediting commit, or a compaction whose
/// repoints all lost the CAS), which are rare.
const ORPHAN_SWEEP_INTERVAL_SECS: i64 = 12 * 60 * 60;

pub struct GarbageCollector {
    db: Arc<Db>,
    tombstone_store: TombstoneStore,
    extent_store: ExtentStore,
    stats: Arc<FileSystemStats>,
    /// A read-only admin used to list active checkpoints, to gate segment
    /// reclamation (skip while any pins an older manifest whose view may
    /// reference more). `None` disables the check (tests).
    checkpoint_admin: Option<Admin>,
}

impl GarbageCollector {
    pub fn new(
        db: Arc<Db>,
        tombstone_store: TombstoneStore,
        extent_store: ExtentStore,
        stats: Arc<FileSystemStats>,
        checkpoint_admin: Option<Admin>,
    ) -> Self {
        Self {
            db,
            tombstone_store,
            extent_store,
            stats,
            checkpoint_admin,
        }
    }

    /// Reclaim dead segment objects, unless a checkpoint pins an older manifest
    /// (whose view may still reference segments dead in the current view).
    async fn maybe_reclaim_segments(&self) {
        // The horizon gate is evaluated by reclaim_segments_gated after its durable
        // seal+flush barrier, not here: a checkpoint created between listing and the
        // flush would otherwise pin the pre-flush manifest (still referencing a
        // to-be-dead segment) yet be invisible to the horizon. Listing post-barrier,
        // any such checkpoint is either seen here (and extends the horizon) or pins
        // the post-flush manifest in which the reclaimed segments are unreferenced.
        let checkpoint_admin = &self.checkpoint_admin;
        let gate = move || async move {
            // now/floor are sampled post-barrier (inside the gate), so the floor
            // covers the writer's in-flight reads relative to the actual scan time.
            let floor = Utc::now() + chrono::Duration::seconds(INFLIGHT_FLOOR_SECS);
            match checkpoint_admin {
                Some(admin) => match admin.list_checkpoints(None).await {
                    Ok(cps) => {
                        // Wait out the latest-expiring ephemeral checkpoint (compactor
                        // / following reader): until it expires it may still reference
                        // a just-superseded segment.
                        let horizon = cps
                            .iter()
                            .filter_map(|c| c.expire_time)
                            .max()
                            .map(|e| {
                                (e + chrono::Duration::seconds(CLOCK_SKEW_MARGIN_SECS)).max(floor)
                            })
                            .unwrap_or(floor);
                        // A persistent checkpoint (no expiry — backup / pinned reader)
                        // pins a view that can't be timed out. Rather than pausing all
                        // reclamation, protect segments that could predate the earliest
                        // such checkpoint (by object mtime + a generous margin) and
                        // reclaim newer garbage.
                        let protect_before = cps
                            .iter()
                            .filter(|c| c.expire_time.is_none())
                            .map(|c| c.create_time)
                            .min()
                            .map(|t| t + chrono::Duration::seconds(CHECKPOINT_SKEW_MARGIN_SECS));
                        if let Some(pb) = protect_before {
                            info!(
                                "segment GC: persistent checkpoint present; protecting segments created before {pb}, reclaiming newer garbage"
                            );
                        }
                        Ok(Some((horizon, protect_before)))
                    }
                    Err(e) => {
                        tracing::warn!("segment GC skipped: cannot list checkpoints: {}", e);
                        Ok(None)
                    }
                },
                None => Ok(Some((floor, None))),
            }
        };
        // reclaim_segments_gated emits its own per-pass summary at info.
        if let Err(e) = self.extent_store.reclaim_segments_gated(gate).await {
            tracing::error!("segment GC failed: {:?}", e);
        }
    }

    /// Drive the slow orphan sweep on its wall-clock cadence. The gate (read the
    /// persisted last-sweep timestamp, run only if `>= ORPHAN_SWEEP_INTERVAL_SECS`
    /// elapsed, then persist the new timestamp) lives in
    /// [`ExtentStore::sweep_orphans_if_due`], which owns the db + key codec; here we
    /// just supply the interval and log the outcome.
    async fn maybe_sweep_orphans(&self) {
        let interval = chrono::Duration::seconds(ORPHAN_SWEEP_INTERVAL_SECS);
        match self.extent_store.sweep_orphans_if_due(interval).await {
            Ok(Some(n)) => info!("slow orphan sweep reclaimed {} orphan(s)", n),
            Ok(None) => {} // not yet due this tick
            Err(e) => tracing::error!("slow orphan sweep failed: {:?}", e),
        }
    }

    pub fn start(
        self: Arc<Self>,
        shutdown: CancellationToken,
        runtime: Option<tokio::runtime::Handle>,
    ) -> JoinHandle<()> {
        // Segment reclamation runs on its own task, independent of the tombstone
        // pass. `run()` drains its entire backlog before returning, so gating
        // reclaim behind it in one loop would let a large backlog starve (and
        // silence) reclamation; a separate task keeps it ticking regardless.
        // Runs once immediately, then every interval.
        {
            let gc = Arc::clone(&self);
            let shutdown = shutdown.clone();
            let reclaim = async move {
                info!("Starting segment reclamation task");
                loop {
                    gc.maybe_reclaim_segments().await;
                    // Right after the fast reclaim so no in-flight compaction can
                    // leave a not-yet-credited packed segment eligible.
                    gc.maybe_sweep_orphans().await;
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(
                            SEGMENT_GC_INTERVAL_SECS,
                        )) => {}
                    }
                }
            };
            // Detached: the task stops on the shutdown token, not on its handle.
            match &runtime {
                Some(rt) => {
                    spawn_named_on("segment-gc", reclaim, rt);
                }
                None => {
                    spawn_named("segment-gc", reclaim);
                }
            }
        }

        let fut = async move {
            info!("Starting garbage collection task (runs continuously)");
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("GC task shutting down");
                        break;
                    }
                    result = self.run() => {
                        if let Err(e) = result {
                            tracing::error!("Garbage collection failed: {:?}", e);
                        }
                    }
                }

                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("GC task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}
                }
            }
        };

        if let Some(rt) = runtime {
            spawn_named_on("gc", fut, &rt)
        } else {
            spawn_named("gc", fut)
        }
    }

    pub async fn run(&self) -> Result<(), FsError> {
        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);

        loop {
            let mut tombstones_to_update: Vec<(Bytes, u64, usize, bool)> = Vec::new();
            let mut extents_deleted_this_round = 0;
            let mut tombstones_completed_this_round = 0;
            let mut tombstones_processed_this_round = 0;
            let mut found_incomplete_tombstones = false;

            let iter = self.tombstone_store.list().await?;
            futures::pin_mut!(iter);

            let mut extents_remaining_in_round = MAX_EXTENTS_PER_ROUND;

            while let Some(result) = futures::StreamExt::next(&mut iter).await {
                let entry = result?;
                tombstones_processed_this_round += 1;

                if tombstones_processed_this_round % 100 == 0 {
                    tokio::task::yield_now().await;
                }

                if tombstones_processed_this_round >= MAX_TOMBSTONES_PER_ROUND {
                    found_incomplete_tombstones = true;
                    break;
                }

                if entry.remaining_size == 0 {
                    tombstones_to_update.push((entry.key, 0, 0, true));
                    continue;
                }

                if extents_remaining_in_round == 0 {
                    found_incomplete_tombstones = true;
                    break;
                }

                let total_extents = entry.remaining_size.div_ceil(EXTENT_SIZE as u64) as usize;
                let extents_to_delete = total_extents.min(extents_remaining_in_round);
                let start_extent = total_extents.saturating_sub(extents_to_delete);

                let is_final_batch = extents_to_delete == total_extents;
                if !is_final_batch {
                    found_incomplete_tombstones = true;
                }
                tombstones_to_update.push((
                    entry.key,
                    entry.remaining_size,
                    start_extent,
                    is_final_batch,
                ));

                if extents_to_delete > 0 {
                    // Under the inode's write lock so it serializes with the
                    // compaction repoint (which takes the same lock).
                    self.extent_store
                        .delete_extents(entry.inode_id, start_extent as u64, total_extents as u64)
                        .await?;

                    #[cfg(feature = "failpoints")]
                    fail_point!(fp::GC_AFTER_EXTENT_DELETE);

                    extents_deleted_this_round += extents_to_delete;
                    extents_remaining_in_round -= extents_to_delete;

                    if is_final_batch {
                        tombstones_completed_this_round += 1;
                    }

                    if extents_deleted_this_round % 1000 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }

            if !tombstones_to_update.is_empty() {
                let mut txn = self.db.new_transaction()?;

                for (key, old_size, start_extent, delete_tombstone) in tombstones_to_update {
                    if delete_tombstone {
                        self.tombstone_store.remove(&mut txn, &key);
                    } else {
                        let remaining_extents = start_extent;
                        let remaining_size = (remaining_extents as u64) * (EXTENT_SIZE as u64);
                        let actual_remaining = remaining_size.min(old_size);
                        self.tombstone_store
                            .update(&mut txn, &key, actual_remaining);
                    }
                }

                self.db
                    .write_with_options(
                        txn.into_inner(),
                        &WriteOptions {
                            await_durable: false,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|_| FsError::IoError)?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::GC_AFTER_TOMBSTONE_UPDATE);

                self.stats
                    .tombstones_processed
                    .fetch_add(tombstones_completed_this_round, Ordering::Relaxed);
            }

            if extents_deleted_this_round > 0 || tombstones_completed_this_round > 0 {
                self.stats
                    .gc_extents_deleted
                    .fetch_add(extents_deleted_this_round as u64, Ordering::Relaxed);

                tracing::debug!(
                    "GC: processed {} tombstones, deleted {} extents",
                    tombstones_completed_this_round,
                    extents_deleted_this_round,
                );
            }

            if !found_incomplete_tombstones {
                break;
            }

            tokio::task::yield_now().await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::key_codec::KeyCodec;
    use crate::fs::store::tombstone::TombstoneEntry;

    fn no_wait() -> WriteOptions {
        WriteOptions {
            await_durable: false,
            ..Default::default()
        }
    }

    async fn gc_for(fs: &ZeroFS) -> GarbageCollector {
        GarbageCollector::new(
            Arc::clone(&fs.db),
            fs.tombstone_store.clone(),
            fs.extent_store.clone(),
            Arc::clone(&fs.stats),
            None,
        )
    }

    async fn tombstones(store: &TombstoneStore) -> Vec<TombstoneEntry> {
        let iter = store.list().await.unwrap();
        futures::pin_mut!(iter);
        let mut out = Vec::new();
        while let Some(r) = futures::StreamExt::next(&mut iter).await {
            out.push(r.unwrap());
        }
        out
    }

    // The core sweep: a tombstone for a deleted file's extents is reclaimed — every
    // extent deleted, the tombstone removed, and the stats counters advanced.
    #[tokio::test]
    async fn gc_deletes_extents_and_removes_the_tombstone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;

        let inode_id = 4242u64;
        let size = (EXTENT_SIZE * 3) as u64;

        let mut txn = fs.db.new_transaction().unwrap();
        fs.extent_store
            .write(
                &mut txn,
                inode_id,
                0,
                &Bytes::from(vec![1u8; size as usize]),
                0,
            )
            .await
            .unwrap();
        fs.tombstone_store.add(&mut txn, inode_id, size);
        // The write staged seg-count deltas; commit through the worker (their sole
        // writer) rather than dropping them via into_inner.
        fs.write_coordinator.commit(txn).await.unwrap();

        for idx in 0..3 {
            assert!(
                fs.extent_store.get(inode_id, idx).await.unwrap().is_some(),
                "extent {idx} should exist before GC"
            );
        }

        gc.run().await.unwrap();

        for idx in 0..3 {
            assert!(
                fs.extent_store.get(inode_id, idx).await.unwrap().is_none(),
                "extent {idx} must be reclaimed by GC"
            );
        }
        assert!(
            tombstones(&fs.tombstone_store).await.is_empty(),
            "tombstone must be removed"
        );
        assert_eq!(fs.stats.gc_extents_deleted.load(Ordering::Relaxed), 3);
        assert_eq!(fs.stats.tombstones_processed.load(Ordering::Relaxed), 1);
        assert!(fs.stats.gc_runs.load(Ordering::Relaxed) >= 1);
    }

    // A zero-remaining-size tombstone (its extents already gone) is just removed.
    #[tokio::test]
    async fn gc_removes_a_zero_size_tombstone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;

        let mut txn = fs.db.new_transaction().unwrap();
        fs.tombstone_store.add(&mut txn, 9u64, 0);
        fs.db
            .write_with_options(txn.into_inner(), &no_wait())
            .await
            .unwrap();

        gc.run().await.unwrap();

        assert!(tombstones(&fs.tombstone_store).await.is_empty());
        assert_eq!(fs.stats.gc_extents_deleted.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn gc_on_an_empty_store_is_a_noop() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;

        gc.run().await.unwrap();

        assert_eq!(fs.stats.gc_extents_deleted.load(Ordering::Relaxed), 0);
        assert!(fs.stats.gc_runs.load(Ordering::Relaxed) >= 1);
    }

    // The persisted timestamp gates the slow orphan sweep on a wall-clock cadence: a
    // recent timestamp defers it (the timestamp is left untouched); a stale one fires
    // it and advances the timestamp. Observed via the stored timestamp, so it doesn't
    // depend on an actual orphan being present.
    #[tokio::test]
    async fn orphan_sweep_timestamp_gate_defers_then_fires() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;
        let key = KeyCodec::new().last_orphan_sweep_key();

        let read = |fs: &ZeroFS, key: Bytes| {
            let db = Arc::clone(&fs.db);
            async move {
                db.get_bytes(&key)
                    .await
                    .unwrap()
                    .and_then(|b| KeyCodec::decode_u64(&b))
                    .unwrap()
            }
        };
        let seed = |fs: &ZeroFS, key: Bytes, secs: u64| {
            let db = Arc::clone(&fs.db);
            async move {
                let mut txn = db.new_transaction().unwrap();
                txn.put_bytes(&key, KeyCodec::encode_u64(secs));
                db.write_with_options(txn.into_inner(), &no_wait())
                    .await
                    .unwrap();
            }
        };

        // A recent sweep: within the interval, so the gate defers and leaves the
        // timestamp exactly as seeded.
        let recent = Utc::now().timestamp() as u64;
        seed(&fs, key.clone(), recent).await;
        gc.maybe_sweep_orphans().await;
        assert_eq!(
            read(&fs, key.clone()).await,
            recent,
            "a recent timestamp must defer the sweep (timestamp untouched)"
        );

        // A stale sweep (older than the interval): the gate fires and advances the
        // timestamp to ~now.
        let stale = (Utc::now().timestamp() - (ORPHAN_SWEEP_INTERVAL_SECS + 60)) as u64;
        seed(&fs, key.clone(), stale).await;
        gc.maybe_sweep_orphans().await;
        assert!(
            read(&fs, key.clone()).await > stale,
            "a stale timestamp must trigger a sweep and advance the timestamp"
        );
    }
}
