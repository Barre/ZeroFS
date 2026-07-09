use crate::config::GcConfig;
use crate::db::Db;
use crate::fs::EXTENT_SIZE;
use crate::fs::errors::FsError;
use crate::fs::metrics::FileSystemStats;
use crate::fs::store::{
    ChainOutcome, ExtentStore, PassOutcome, PassStatus, QUIESCENT_AFTER_DEFAULT, TombstoneStore,
};
use crate::task::{spawn_named, spawn_named_on};
use bytes::Bytes;
use chrono::Utc;
use slatedb::admin::Admin;
use slatedb::config::WriteOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

const MAX_EXTENTS_PER_ROUND: usize = 10_000;
const MAX_TOMBSTONES_PER_ROUND: usize = 10_000;

/// Floor the delete horizon this far past now, so reclamation still covers the
/// writer's own in-flight reads when no checkpoint is pinning a view.
const INFLIGHT_FLOOR_SECS: i64 = 60;

/// Margin past a checkpoint's `expire_time` (the reader's wall clock) before
/// its pinned segments may be deleted, covering clock skew and the reader
/// finishing its last read.
const CLOCK_SKEW_MARGIN_SECS: i64 = 30;

/// Cadence of the slow orphan sweep, the only `list("segments")`. Gated by a
/// persisted wall-clock timestamp so it holds across restarts. Coarse on
/// purpose: it exists only to reclaim counter-less orphan objects, which are
/// rare; the fast reclaim covers everything else.
const ORPHAN_SWEEP_INTERVAL_SECS: i64 = 24 * 60 * 60;

/// Resolved segment-GC tuning
#[derive(Debug, Clone, Copy)]
pub struct GcTuning {
    /// Pass interval while the store is active.
    pub interval: Duration,
    /// Pass interval while a saturated backlog meets an idle store.
    pub idle_interval: Duration,
    /// Pass interval while the store is active but its dead backlog is large
    /// (>= `busy_backlog_dead_percent`). `>= interval` disables the tier.
    pub busy_backlog_interval: Duration,
    /// Dead-space percent at or above which `busy_backlog_interval` applies.
    pub busy_backlog_dead_percent: u64,
    /// Compaction batches a pass runs before it gates continuation on
    /// foreground idleness (the busy-store throughput floor).
    pub min_batches_per_pass: usize,
    /// Per-round live-byte budget (selection cap, half-reserve for heat, and
    /// stored-byte gather cap). Raising it packs over-reserve seams and lifts
    /// per-batch dead-space throughput, at proportional gather RAM.
    pub round_bytes: u64,
    /// Whether reads feed compaction at all (nominations, seam heat, chains).
    pub read_directed: bool,
    /// Tail-scrub floor, percent dead; `None` = scrub off.
    pub tail_min_dead_percent: Option<u64>,
    /// How long the open counter must sit unchanged before the whole store
    /// counts as write-cold.
    pub quiescent_after: Duration,
}

impl Default for GcTuning {
    fn default() -> Self {
        GcConfig::default().into()
    }
}

impl From<GcConfig> for GcTuning {
    fn from(c: GcConfig) -> Self {
        Self {
            interval: Duration::from_secs(c.interval_secs()),
            idle_interval: Duration::from_secs(c.idle_interval_secs()),
            busy_backlog_interval: Duration::from_secs(c.busy_backlog_interval_secs()),
            busy_backlog_dead_percent: c.busy_backlog_dead_percent(),
            min_batches_per_pass: c.min_batches_per_pass(),
            round_bytes: c.compact_round_bytes(),
            read_directed: c.read_directed(),
            tail_min_dead_percent: c.tail_scrub_min_dead_percent(),
            quiescent_after: QUIESCENT_AFTER_DEFAULT,
        }
    }
}

/// Sampled activity counters for the idle test.
#[derive(Debug, Clone, Copy)]
struct Activity {
    bytes_read: u64,
    bytes_written: u64,
    write_ops: u64,
    read_ops: u64,
    total_ops: u64,
    internal_mutations: u64,
}

impl Activity {
    fn sample(stats: &FileSystemStats) -> Self {
        Self {
            bytes_read: stats.bytes_read.load(Ordering::Relaxed),
            bytes_written: stats.bytes_written.load(Ordering::Relaxed),
            write_ops: stats.write_operations.load(Ordering::Relaxed),
            read_ops: stats.read_operations.load(Ordering::Relaxed),
            total_ops: stats.total_operations.load(Ordering::Relaxed),
            internal_mutations: stats.tombstones_created.load(Ordering::Relaxed)
                + stats.tombstones_processed.load(Ordering::Relaxed)
                + stats.gc_extents_deleted.load(Ordering::Relaxed)
                + stats.files_deleted.load(Ordering::Relaxed),
        }
    }

    fn idle_since(&self, prev: &Self) -> bool {
        let d_read_bytes = self.bytes_read.saturating_sub(prev.bytes_read);
        let d_write_bytes = self.bytes_written.saturating_sub(prev.bytes_written);
        let d_write_ops = self.write_ops.saturating_sub(prev.write_ops);
        let d_read_ops = self.read_ops.saturating_sub(prev.read_ops);
        let d_total = self.total_ops.saturating_sub(prev.total_ops);
        let d_meta_commits = d_total
            .saturating_sub(d_read_ops)
            .saturating_sub(d_write_ops);
        let d_internal = self
            .internal_mutations
            .saturating_sub(prev.internal_mutations);
        d_read_bytes == 0
            && d_write_bytes == 0
            && d_write_ops == 0
            && d_meta_commits == 0
            && d_internal == 0
    }
}

/// The cadence tier the planner picked, coarsest to finest drain rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    /// Base interval: nothing pressing, or an active store with a small backlog.
    Base,
    /// Active store, large backlog (dead space or reserve-deferred seams): drain
    /// faster than base without waiting for the store to go idle.
    Drain,
    /// Idle store with backlog: fastest drain.
    Fast,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::Base => "base",
            Tier::Drain => "drain",
            Tier::Fast => "fast",
        }
    }
}

/// The finished pass distilled to what the cadence/budget decision reads. Built
/// from a [`PassOutcome`] plus the post-pass idle sample.
#[derive(Debug, Clone, Copy)]
struct GcState {
    /// Ran to a classification (not Skipped/Pinned).
    completed: bool,
    /// Actionable backlog remains.
    saturated: bool,
    /// Store-wide dead space at scan time.
    dead_percent: u64,
    /// Hot-seam disposition. `chains.deferred` are packable seams squeezed out
    /// of this pass's reserve that a fresh pass would take.
    chains: ChainOutcome,
    /// No foreground activity across the pass window.
    idle: bool,
}

impl GcState {
    fn after(outcome: &PassOutcome, idle: bool) -> Self {
        let (completed, saturated) = match outcome.status {
            PassStatus::Completed { saturated } => (true, saturated),
            PassStatus::Skipped | PassStatus::Pinned => (false, false),
        };
        Self {
            completed,
            saturated,
            dead_percent: outcome.dead_percent,
            chains: outcome.chains,
            idle,
        }
    }
}

/// The next pass's shape: when to run it, how hard, and why.
#[derive(Debug, Clone)]
struct GcPlan {
    /// Sleep before the next pass.
    next_interval: Duration,
    /// Batches the next pass runs before it starts yielding to foreground load.
    min_batches: usize,
    tier: Tier,
    /// The decision, for the log.
    reason: String,
}

impl GcPlan {
    /// A base-cadence plan with the configured floor and a given reason.
    fn base(tuning: &GcTuning, reason: &str) -> Self {
        Self {
            next_interval: tuning.interval,
            min_batches: tuning.min_batches_per_pass,
            tier: Tier::Base,
            reason: reason.to_string(),
        }
    }

    /// Pre-first-pass plan.
    fn initial(tuning: &GcTuning) -> Self {
        Self::base(tuning, "startup")
    }
}

/// Turn a finished pass into the next pass's plan: the single place the cadence
/// and budget knobs combine. Store-free, so the policy is unit-testable.
///
/// Cadence tiers (all need a completed, saturated pass):
/// - idle store: `idle_interval` (Fast);
/// - active store, and (dead space >= `busy_backlog_dead_percent` OR
///   reserve-deferred seams remain): `busy_backlog_interval` (Drain);
/// - else: `interval` (Base).
///
/// The seam trigger fires only on `chains.deferred` (packable seams a fresh
/// reserve would take), never on over-reserve (`unpackable`) or `warm` seams: a
/// faster cadence cannot pack a seam that overflows even a fresh reserve, nor
/// one still being written, so those wait on the reserve/round-budget dials and
/// on write-coldness respectively.
fn plan_next(state: &GcState, tuning: &GcTuning) -> GcPlan {
    let min_batches = tuning.min_batches_per_pass;
    let c = &state.chains;

    let (interval, tier, why) = if !state.completed {
        (
            tuning.interval,
            Tier::Base,
            "pass not completed".to_string(),
        )
    } else if !state.saturated {
        (tuning.interval, Tier::Base, "backlog drained".to_string())
    } else if state.idle {
        (
            tuning.idle_interval,
            Tier::Fast,
            "saturated backlog, store idle".to_string(),
        )
    } else {
        let tier_enabled = tuning.busy_backlog_interval < tuning.interval;
        let dead_trigger = state.dead_percent >= tuning.busy_backlog_dead_percent;
        let seam_trigger = c.deferred > 0;
        let mut parts: Vec<String> = Vec::new();
        if dead_trigger {
            parts.push(format!("{}% dead", state.dead_percent));
        }
        if seam_trigger {
            parts.push(format!("{} seams deferred", c.deferred));
        }
        if tier_enabled && (dead_trigger || seam_trigger) {
            (
                tuning.busy_backlog_interval,
                Tier::Drain,
                format!("{}, store active", parts.join(" + ")),
            )
        } else if dead_trigger || seam_trigger {
            // Backlog is large but the drain tier is off (interval == base).
            (
                tuning.interval,
                Tier::Base,
                format!("{}, store active, drain tier disabled", parts.join(" + ")),
            )
        } else {
            (
                tuning.interval,
                Tier::Base,
                format!("{}% dead, store active, small backlog", state.dead_percent),
            )
        }
    };

    // Over-reserve seams never pack at any cadence; append the count so a Base
    // pass stuck on them shows why. The reason is the why alone; tier, interval,
    // and batch count are separate log fields.
    let over_reserve = if c.unpackable > 0 {
        format!("; {} seams over-reserve", c.unpackable)
    } else {
        String::new()
    };
    GcPlan {
        next_interval: interval,
        min_batches,
        tier,
        reason: format!("{why}{over_reserve}"),
    }
}

pub struct GarbageCollector {
    db: Arc<Db>,
    tombstone_store: TombstoneStore,
    extent_store: ExtentStore,
    stats: Arc<FileSystemStats>,
    /// Read-only admin for listing active checkpoints, which gate segment
    /// reclamation. `None` disables the check (tests).
    checkpoint_admin: Option<Admin>,
    tuning: GcTuning,
}

impl GarbageCollector {
    pub fn new(
        db: Arc<Db>,
        tombstone_store: TombstoneStore,
        extent_store: ExtentStore,
        stats: Arc<FileSystemStats>,
        checkpoint_admin: Option<Admin>,
        tuning: GcTuning,
    ) -> Self {
        Self {
            db,
            tombstone_store,
            extent_store,
            stats,
            checkpoint_admin,
            tuning,
        }
    }

    /// Reclaim dead segment objects, unless a checkpoint pins an older manifest
    /// (whose view may still reference segments dead in the current view).
    /// `keep_going` approves continuing a multi-batch drain mid-pass.
    async fn maybe_reclaim_segments(
        &self,
        keep_going: impl Fn(usize) -> bool,
    ) -> Option<PassOutcome> {
        // The gate runs inside reclaim_segments_gated, after its durable
        // barrier, see that method for the checkpoint race this ordering
        // closes. `floor` is sampled there too, so it covers in-flight reads
        // relative to the actual scan time.
        let checkpoint_admin = &self.checkpoint_admin;
        let gate = move || async move {
            let floor = Utc::now() + chrono::Duration::seconds(INFLIGHT_FLOOR_SECS);
            match checkpoint_admin {
                Some(admin) => match admin.list_checkpoints(None).await {
                    Ok(cps) => {
                        // Wait out the latest-expiring ephemeral checkpoint:
                        // until then it may still reference a just-superseded
                        // segment.
                        let horizon = cps
                            .iter()
                            .filter_map(|c| c.expire_time)
                            .max()
                            .map(|e| {
                                (e + chrono::Duration::seconds(CLOCK_SKEW_MARGIN_SECS)).max(floor)
                            })
                            .unwrap_or(floor);
                        // A persistent checkpoint can't be timed out; its
                        // presence pauses all deletion and compaction (see
                        // reclaim_segments_gated).
                        let protect_before = cps
                            .iter()
                            .filter(|c| c.expire_time.is_none())
                            .map(|c| c.create_time)
                            .min();
                        if let Some(created) = protect_before {
                            info!(
                                "segment GC: persistent checkpoint present (earliest created {created}); segment deletion and compaction paused while one exists"
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
        match self
            .extent_store
            .reclaim_segments_gated(
                gate,
                self.tuning.tail_min_dead_percent,
                self.tuning.quiescent_after,
                keep_going,
                self.tuning.round_bytes,
            )
            .await
        {
            Ok(outcome) => Some(outcome),
            Err(e) => {
                tracing::error!("segment GC failed: {:?}", e);
                None
            }
        }
    }

    /// Drive the slow orphan sweep. The timestamp gate lives in
    /// [`ExtentStore::sweep_orphans_if_due`]; this just supplies the interval
    /// and logs the outcome.
    async fn maybe_sweep_orphans(&self) {
        let interval = chrono::Duration::seconds(ORPHAN_SWEEP_INTERVAL_SECS);
        match self.extent_store.sweep_orphans_if_due(interval).await {
            Ok(Some(n)) => {
                info!("slow orphan sweep reclaimed {} orphan(s)", n);
                self.extent_store
                    .segment_gc_stats()
                    .record_orphans_reclaimed(n as u64);
            }
            Ok(None) => {} // not yet due this tick
            Err(e) => tracing::error!("slow orphan sweep failed: {:?}", e),
        }
    }

    /// Spawns both GC loops
    pub fn start(
        self: Arc<Self>,
        shutdown: CancellationToken,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Vec<JoinHandle<()>> {
        // Read replicas and checkpoint mounts never start GC, so reads never
        // track nominations there; `read_directed = false` works the same way.
        if self.tuning.read_directed {
            self.extent_store.enable_nominations();
        }

        let reclaim_handle = {
            let gc = Arc::clone(&self);
            let shutdown = shutdown.clone();
            let reclaim = async move {
                let t = &gc.tuning;
                info!(
                    "Starting segment reclamation task (round budget {} MiB, floor {} batch(es); cadence base {}s / drain {}s (>= {}% dead) / idle {}s)",
                    t.round_bytes >> 20,
                    t.min_batches_per_pass,
                    t.interval.as_secs(),
                    t.busy_backlog_interval.as_secs(),
                    t.busy_backlog_dead_percent,
                    t.idle_interval.as_secs(),
                );
                let mut prev: Option<Activity> = None;
                let seg_stats = gc.extent_store.segment_gc_stats();
                // The planner owns cadence + budget; `plan` carries the next
                // pass's floor forward. The first pass uses the initial floor.
                let mut plan = GcPlan::initial(&gc.tuning);
                loop {
                    // Batch approval: no seal in flight, not shutting down, and
                    // (past the throughput floor) as idle as pass start. A busy
                    // store still drains `plan.min_batches` batches; beyond the
                    // floor a client op ends the drain within one batch.
                    let pass_base = Activity::sample(&gc.stats);
                    let floor = plan.min_batches;
                    let keep_going = |batches: usize| {
                        // Shutdown and an in-flight seal (RAM safety) hard-stop
                        // every batch. Foreground idleness only gates batches
                        // past the throughput floor.
                        !shutdown.is_cancelled()
                            && gc.extent_store.seals_quiet()
                            && (batches < floor
                                || Activity::sample(&gc.stats).idle_since(&pass_base))
                    };
                    let outcome = gc.maybe_reclaim_segments(keep_going).await;
                    // Shutdown must not wait out the orphan sweep's daily
                    // LIST; the final flush waits on this task.
                    if shutdown.is_cancelled() {
                        break;
                    }
                    // Right after the fast reclaim so no in-flight compaction can
                    // leave a not-yet-credited packed segment eligible.
                    gc.maybe_sweep_orphans().await;
                    // The window spans the pass, so mid-pass ops read as
                    // busy; the first decision after startup is always base.
                    let sample = Activity::sample(&gc.stats);
                    let idle = prev.is_some_and(|p| sample.idle_since(&p))
                        && gc.extent_store.seals_quiet();
                    prev = Some(sample);
                    plan = match outcome {
                        Some(o) => plan_next(&GcState::after(&o, idle), &gc.tuning),
                        // Reclaim errored or was skipped (already logged); back off at base.
                        None => GcPlan::base(&gc.tuning, "reclaim skipped or errored"),
                    };
                    let tier_code = match plan.tier {
                        Tier::Base => 1,
                        Tier::Drain => 2,
                        Tier::Fast => 3,
                    };
                    seg_stats.record_plan(tier_code, gc.tuning.read_directed, &plan.reason);
                    info!(
                        tier = plan.tier.label(),
                        "segment GC plan: next {}s, {} batch{}: {}",
                        plan.next_interval.as_secs(),
                        plan.min_batches,
                        if plan.min_batches == 1 { "" } else { "es" },
                        plan.reason,
                    );
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(plan.next_interval) => {}
                    }
                }
            };
            // Stops on the shutdown token; the handle is awaited at shutdown.
            match &runtime {
                Some(rt) => spawn_named_on("segment-gc", reclaim, rt),
                None => spawn_named("segment-gc", reclaim),
            }
        };

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

        let tombstone_handle = if let Some(rt) = runtime {
            spawn_named_on("gc", fut, &rt)
        } else {
            spawn_named("gc", fut)
        };
        vec![reclaim_handle, tombstone_handle]
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

    /// A completed pass with the given saturation, dead%, and deferred-seam
    /// count; idle as passed. `chains.deferred` also drives `saturated` in the
    /// real pass, so tests set them consistently.
    fn state(saturated: bool, dead_percent: u64, deferred: usize, idle: bool) -> GcState {
        GcState {
            completed: true,
            saturated,
            dead_percent,
            chains: ChainOutcome {
                deferred,
                ..Default::default()
            },
            idle,
        }
    }

    #[test]
    fn cadence_tiers_by_idle_saturation_and_backlog() {
        let t = GcTuning::default();
        assert!(
            t.busy_backlog_interval < t.interval,
            "tier enabled by default"
        );
        let hi = t.busy_backlog_dead_percent;
        let plan = |s: GcState| plan_next(&s, &t);

        // Idle + saturated: fastest, regardless of dead%.
        let p = plan(state(true, 0, 0, true));
        assert_eq!((p.tier, p.next_interval), (Tier::Fast, t.idle_interval));
        // Busy + saturated + large dead backlog: drain tier.
        let p = plan(state(true, hi, 0, false));
        assert_eq!(
            (p.tier, p.next_interval),
            (Tier::Drain, t.busy_backlog_interval)
        );
        // Busy + saturated + small backlog: base.
        let p = plan(state(true, hi - 1, 0, false));
        assert_eq!((p.tier, p.next_interval), (Tier::Base, t.interval));
        // Not saturated: base even when idle.
        let p = plan(state(false, hi, 0, true));
        assert_eq!((p.tier, p.next_interval), (Tier::Base, t.interval));
    }

    // The seam trigger: reserve-deferred seams drain faster even with little
    // dead space, but over-reserve (unpackable) seams do not (no cadence packs
    // them).
    #[test]
    fn deferred_seams_trigger_drain_but_over_reserve_seams_do_not() {
        let t = GcTuning::default();
        // Low dead%, but a packable seam was squeezed out: drain.
        let p = plan_next(&state(true, 0, 3, false), &t);
        assert_eq!(p.tier, Tier::Drain);
        assert!(p.reason.contains("3 seams deferred"), "{}", p.reason);

        // Low dead%, only over-reserve seams (not saturated, deferred == 0): base.
        let mut s = state(false, 0, 0, false);
        s.chains.assembled = 59;
        s.chains.unpackable = 59;
        let p = plan_next(&s, &t);
        assert_eq!(p.tier, Tier::Base);
        assert!(p.reason.contains("59 seams over-reserve"), "{}", p.reason);
    }

    // Status → (completed, saturated) mapping, and error/skip back off to base.
    #[test]
    fn plan_maps_pass_status_and_backs_off_on_skip() {
        let t = GcTuning::default();
        let outcome = |status| PassOutcome {
            deleted: 0,
            relocated: 0,
            status,
            chains: ChainOutcome::default(),
            dead_percent: t.busy_backlog_dead_percent + 10,
        };
        // Skipped/Pinned are not "completed": base even when idle with dead space.
        for status in [PassStatus::Skipped, PassStatus::Pinned] {
            let p = plan_next(&GcState::after(&outcome(status), true), &t);
            assert_eq!(p.tier, Tier::Base);
        }
        // Completed + saturated + idle maps through to Fast.
        let p = plan_next(
            &GcState::after(&outcome(PassStatus::Completed { saturated: true }), true),
            &t,
        );
        assert_eq!(p.tier, Tier::Fast);
    }

    // Equal busy-backlog and base intervals disable the middle tier: a busy,
    // dirty store falls back to the base interval.
    #[test]
    fn busy_backlog_tier_disabled_when_interval_equals_base() {
        let mut t = GcTuning::default();
        t.busy_backlog_interval = t.interval;
        let p = plan_next(&state(true, t.busy_backlog_dead_percent + 10, 5, false), &t);
        assert_eq!((p.tier, p.next_interval), (Tier::Base, t.interval));
    }

    proptest::proptest! {
        // plan_next is pure, so one property covers the whole tier lattice: over
        // arbitrary states and config-realistic tunings, tier and interval agree
        // with the exact tier conditions, the cadence never exceeds base, and an
        // incomplete or drained pass never accelerates.
        #[test]
        fn plan_next_invariants(
            completed in proptest::bool::ANY,
            saturated in proptest::bool::ANY,
            idle in proptest::bool::ANY,
            dead_percent in 0u64..=100,
            dead_thresh in 0u64..=100,
            deferred in 0usize..=50,
            warm in 0usize..=50,
            unpackable in 0usize..=50,
            assembled in 0usize..=150,
            packed in 0usize..=150,
            interval_s in GcConfig::MIN_INTERVAL_SECS..=600,
            idle_raw in 1u64..=600,
            busy_raw in GcConfig::MIN_INTERVAL_SECS..=600,
            min_batches in 1usize..=64,
        ) {
            // Build a tuning respecting the same relations From<GcConfig> guarantees:
            // idle_interval <= interval, busy_backlog_interval in [MIN, interval].
            let idle_interval = Duration::from_secs(idle_raw.min(interval_s).max(1));
            let busy_backlog_interval =
                Duration::from_secs(busy_raw.min(interval_s).max(GcConfig::MIN_INTERVAL_SECS));
            let interval = Duration::from_secs(interval_s);
            let tuning = GcTuning {
                interval,
                idle_interval,
                busy_backlog_interval,
                busy_backlog_dead_percent: dead_thresh,
                min_batches_per_pass: min_batches,
                round_bytes: 256 << 20,
                read_directed: true,
                tail_min_dead_percent: None,
                quiescent_after: Duration::from_secs(300),
            };
            let st = GcState {
                completed,
                saturated,
                dead_percent,
                chains: ChainOutcome { assembled, packed, deferred, warm, unpackable },
                idle,
            };
            let p = plan_next(&st, &tuning);

            // Budget floor passes through verbatim; cadence is one of the three tiers.
            proptest::prop_assert_eq!(p.min_batches, min_batches);
            proptest::prop_assert!(
                p.next_interval == idle_interval
                    || p.next_interval == busy_backlog_interval
                    || p.next_interval == interval
            );
            proptest::prop_assert!(p.next_interval <= interval, "never sleeps longer than base");

            // Tier <-> interval agree.
            match p.tier {
                Tier::Fast => proptest::prop_assert_eq!(p.next_interval, idle_interval),
                Tier::Drain => proptest::prop_assert_eq!(p.next_interval, busy_backlog_interval),
                Tier::Base => proptest::prop_assert_eq!(p.next_interval, interval),
            }

            // Exact tier conditions.
            let want_fast = completed && saturated && idle;
            let tier_enabled = busy_backlog_interval < interval;
            let want_drain = completed && saturated && !idle && tier_enabled
                && (dead_percent >= dead_thresh || deferred > 0);
            proptest::prop_assert_eq!(p.tier == Tier::Fast, want_fast);
            proptest::prop_assert_eq!(p.tier == Tier::Drain, want_drain);
            proptest::prop_assert_eq!(p.tier == Tier::Base, !(want_fast || want_drain));

            // Safety: an incomplete or drained pass never accelerates.
            if !(completed && saturated) {
                proptest::prop_assert_eq!(p.tier, Tier::Base);
            }

            // The reason is populated and deterministic (pure function).
            proptest::prop_assert!(!p.reason.is_empty());
            proptest::prop_assert_eq!(&p.reason, &plan_next(&st, &tuning).reason);
        }
    }

    #[test]
    fn idle_requires_strictly_no_reads_writes_or_metadata_commits() {
        let base = Activity {
            bytes_read: 100,
            bytes_written: 100,
            write_ops: 10,
            read_ops: 50,
            total_ops: 60,
            internal_mutations: 7,
        };
        // Nothing moved: idle.
        assert!(base.idle_since(&base));
        // Lookup/readdir noise (read + total in lockstep, no bytes): idle =
        // a monitoring agent must not pin GC at the base interval.
        let lookups = Activity {
            read_ops: 55,
            total_ops: 65,
            ..base
        };
        assert!(lookups.idle_since(&base));
        // A data read can mint seam heat: not idle.
        let data_read = Activity {
            bytes_read: 101,
            read_ops: 51,
            total_ops: 61,
            ..base
        };
        assert!(!data_read.idle_since(&base));
        // A data write: not idle.
        let write = Activity {
            bytes_written: 164,
            write_ops: 11,
            total_ops: 61,
            ..base
        };
        assert!(!write.idle_since(&base));
        // A metadata-only commit (setattr bumps only total_operations) still
        // dirties the memtable; a fast pass would flush it as a tiny L0 SST.
        let setattr = Activity {
            total_ops: 61,
            ..base
        };
        assert!(!setattr.idle_since(&base));
        // The store's own tombstone-GC / orphan-reclaim commits dirty the
        // memtable while every foreground counter sits still — a post-unlink
        // drain must not read as idle.
        let internal = Activity {
            internal_mutations: 8,
            ..base
        };
        assert!(!internal.idle_since(&base));
    }

    proptest::proptest! {
        #[test]
        fn idle_iff_only_lookup_traffic(
            lookups in 0u64..100,
            read_bytes in 0u64..3,
            write_bytes in 0u64..3,
            write_ops in 0u64..3,
            meta_commits in 0u64..3,
            internal in 0u64..3,
        ) {
            let prev = Activity {
                bytes_read: 1000,
                bytes_written: 1000,
                write_ops: 10,
                read_ops: 50,
                total_ops: 70,
                internal_mutations: 5,
            };
            let data_read_ops = read_bytes.min(1); // a data read is also a read op
            let cur = Activity {
                bytes_read: prev.bytes_read + read_bytes,
                bytes_written: prev.bytes_written + write_bytes,
                write_ops: prev.write_ops + write_ops,
                read_ops: prev.read_ops + lookups + data_read_ops,
                total_ops: prev.total_ops + lookups + data_read_ops + write_ops + meta_commits,
                internal_mutations: prev.internal_mutations + internal,
            };
            let expect = read_bytes == 0
                && write_bytes == 0
                && write_ops == 0
                && meta_commits == 0
                && internal == 0;
            proptest::prop_assert_eq!(cur.idle_since(&prev), expect);
        }
    }

    async fn gc_for(fs: &ZeroFS) -> GarbageCollector {
        GarbageCollector::new(
            Arc::clone(&fs.db),
            fs.tombstone_store.clone(),
            fs.extent_store.clone(),
            Arc::clone(&fs.stats),
            None,
            GcTuning::default(),
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
