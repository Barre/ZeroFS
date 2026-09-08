//! Counter-based reclamation cycles and the slow orphan sweep. Cycles scan
//! durable counters, verify and delete dead segments, and run repack jobs; the
//! separately scheduled orphan sweep is the only path that LISTs the store.

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

use super::super::{ExtentStore, PARALLEL_EXTENT_OPS, human_bytes};
use super::select::{CandidatePool, SegStat};
use super::{REPACK_JOB_BYTES, repack};
use crate::config::ReclaimConfig;
use crate::fs::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::metrics::SegmentReclaimCycle;
use crate::segment::{FrameLoc, MAX_SEGMENT_OBJECT_BYTES, Segid};
use crate::segment_store::SegmentStoreError;
use bytes::Bytes;
use chrono::DateTime;
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Fully-dead segments are independent after the safe cutoff. Keep this below
/// the per-segment pointer-lookup fan-out so one deletion set cannot swamp the
/// metadata store while still overlapping object GETs and DELETEs.
const PARALLEL_DEAD_SEGMENT_JOBS: usize = 8;

/// Bound one counter-cleanup transaction without bounding the amount of dead
/// work completed by a scan.
const SEGCOUNT_DROPS_PER_COMMIT: usize = 1024;

/// How long segments sealed before this cycle's cutoff must be kept for
/// readers that may still resolve them. Derived from the checkpoint list
/// after the durable barrier, so a concurrent checkpoint either extends the
/// horizon or pins the already-flushed manifest, which cannot reference a
/// reclaimed segment.
#[derive(Clone, Copy, Debug)]
pub enum SegmentProtection {
    /// A persistent checkpoint pins every segment: neither delete nor repack.
    Indefinite,
    /// Segments first seen dead this cycle become deletable at this time.
    /// Monotonic: checkpoint retention is derived before the counter scan.
    Until(Instant),
}

/// Per-cycle tuning with an internal byte budget, reduced in tests.
#[derive(Clone, Copy, Debug)]
pub struct CyclePolicy {
    pub repack_min_dead_percent: u64,
    /// Nominal per-job gather budget; a job's first source may exceed it.
    pub job_bytes: u64,
    pub max_concurrent_repacks: usize,
}

impl CyclePolicy {
    fn clamped(self) -> Self {
        Self {
            repack_min_dead_percent: self.repack_min_dead_percent.clamp(1, 99),
            job_bytes: self.job_bytes.clamp(1, MAX_SEGMENT_OBJECT_BYTES),
            max_concurrent_repacks: self
                .max_concurrent_repacks
                .clamp(1, ReclaimConfig::MAX_CONCURRENT_REPACKS),
        }
    }
}

impl From<&ReclaimConfig> for CyclePolicy {
    fn from(config: &ReclaimConfig) -> Self {
        Self {
            repack_min_dead_percent: config.repack_min_dead_percent(),
            job_bytes: REPACK_JOB_BYTES,
            max_concurrent_repacks: config.max_concurrent_repacks(),
        }
    }
}

/// What one reclaim cycle did.
#[derive(Debug, Default)]
pub struct ReclaimOutcome {
    pub deleted: usize,
    pub relocated: usize,
}

fn sealed_before(segid: &Segid, epoch: u64, cutoff: u64) -> bool {
    segid.epoch < epoch || (segid.epoch == epoch && segid.counter < cutoff)
}

/// One durable snapshot's segment allocation and reclaim-activity cutoffs.
struct DurableCutoff {
    epoch: u64,
    counter: u64,
    activity: u64,
}

async fn durable_reclaim_cutoff(store: &ExtentStore) -> Result<DurableCutoff, FsError> {
    // Match the commit path's lock order.
    let _refs = store.extent_ref_barrier.clone().write_owned().await;
    let _barrier = store.db.flush_barrier().write_owned().await;
    store.seal_open().await?;
    store.db.flush().await.map_err(|_| FsError::IoError)?;
    // Repacking allocates IDs independently of the open buffer. Advance the
    // empty buffer past completed allocations while publishers remain blocked.
    let counter = {
        let mut open = store.open.lock().unwrap();
        debug_assert!(open.dir.is_empty());
        open.segid = store.segments.next_segid();
        open.segid.counter
    };
    Ok(DurableCutoff {
        epoch: store.segments.epoch(),
        counter,
        activity: store.reclaim_activity.generation(),
    })
}

/// Run one reclaim cycle: barrier, protection check, durable counter scan,
/// then dead deletion and repack side by side, then counter cleanup.
/// `protection` runs after the barrier; `cancel` stops new work, admitted
/// jobs finish.
pub async fn run<F, Fut>(
    store: &ExtentStore,
    protection: F,
    policy: CyclePolicy,
    cancel: &CancellationToken,
) -> Result<ReclaimOutcome, FsError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<SegmentProtection, FsError>>,
{
    let _cycle_guard = store.segment_reclaim_lock.lock().await;
    let policy = policy.clamped();
    let stats = store.segment_reclaim_stats();
    stats.repack_memory_budget_bytes.store(
        policy
            .job_bytes
            .saturating_mul(policy.max_concurrent_repacks as u64),
        Ordering::Relaxed,
    );
    let cutoff = durable_reclaim_cutoff(store).await?;
    debug!("segment reclamation: durable barrier done (sealed + flushed), scanning counters");

    let delete_horizon = match protection().await? {
        SegmentProtection::Until(delete_horizon) => delete_horizon,
        SegmentProtection::Indefinite => {
            // Footprint gauges stay authoritative; keep the pending-deletion
            // gauges while the pin pauses reclamation.
            stats.record_cycle(&SegmentReclaimCycle {
                awaiting_delete: stats.awaiting_delete.load(Ordering::Relaxed),
                awaiting_delete_bytes: stats.awaiting_delete_bytes.load(Ordering::Relaxed),
                checkpoint_pinned: true,
                ..SegmentReclaimCycle::default()
            });
            info!("segment reclamation: paused by persistent checkpoint");
            return Ok(ReclaimOutcome::default());
        }
    };

    #[cfg(feature = "failpoints")]
    {
        fail_point!(fp::RECLAIM_AFTER_BARRIER_BEFORE_SCAN);
        fp::widen(fp::RECLAIM_AFTER_BARRIER_BEFORE_SCAN).await;
    }

    let scan = scan_counters(
        store,
        cutoff.epoch,
        cutoff.counter,
        policy.repack_min_dead_percent,
    )
    .await?;
    debug!(
        "segment reclamation: counter scan done, {} segments ({} dead, {} repack candidates)",
        scan.scanned,
        scan.dead.len(),
        scan.candidates.len(),
    );
    let candidates_seen = scan.candidates.len();
    let due = classify_dead(store, &scan.dead, delete_horizon);

    let (dead, repack) = tokio::join!(
        delete_due_segments(store, due.segids, cancel),
        run_repack_jobs(
            store,
            CandidatePool::from_stats(scan.candidates),
            &policy,
            cancel
        ),
    );
    drop_dead_counters(store, &dead.freed).await?;

    let report = CycleReport {
        scanned: scan.scanned,
        total_live: scan.total_live,
        total_appended: scan.total_appended,
        candidates_seen,
        awaiting: store.delete_at.lock().unwrap().len(),
        awaiting_bytes: due.awaiting_bytes.saturating_sub(dead.reclaimed_bytes),
        dead,
        repack,
    };
    report.log();
    stats.record_cycle(&report.stats());
    if let Some(error) = report.error() {
        return Err(error);
    }
    store.reclaim_activity.acknowledge(cutoff.activity);
    Ok(report.outcome())
}

struct CounterScan {
    scanned: usize,
    total_live: u64,
    total_appended: u64,
    dead: Vec<SegStat>,
    candidates: Vec<SegStat>,
}

/// Classify every durable counter row. DELETE is irreversible, so this never
/// reads unflushed state. Only segments sealed before the cutoff are
/// eligible: the open segment's credits may not be visible yet, and a segment
/// sealed later can still gain references.
async fn scan_counters(
    store: &ExtentStore,
    cur_epoch: u64,
    cutoff: u64,
    repack_min_dead_percent: u64,
) -> Result<CounterScan, FsError> {
    let (sc_start, sc_end) = store.key_codec.segcount_prefix_range();
    let mut stream = store.db.scan_durable(sc_start..sc_end).await.map_err(|e| {
        error!("segment-reclamation counter scan failed: {}", e);
        FsError::IoError
    })?;
    let mut scan = CounterScan {
        scanned: 0,
        total_live: 0,
        total_appended: 0,
        dead: Vec::new(),
        candidates: Vec::new(),
    };
    while let Some(result) = stream.next().await {
        let (key, value) = result.map_err(|_| FsError::IoError)?;
        let Some((epoch, counter)) = store.key_codec.parse_segcount_key(&key) else {
            continue;
        };
        let segid = Segid::new(epoch, counter);
        let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
            error!(
                "segment reclamation: malformed counter for {segid:?}; refusing to classify \
                 this scan"
            );
            return Err(FsError::IoError);
        };
        scan.scanned += 1;
        scan.total_live += live;
        scan.total_appended += total;
        if !sealed_before(&segid, cur_epoch, cutoff) {
            continue;
        }
        let stat = SegStat { segid, total, live };
        if live == 0 {
            scan.dead.push(stat);
        } else if stat.qualifies_for_repack(repack_min_dead_percent) {
            scan.candidates.push(stat);
        }
    }
    Ok(scan)
}

#[derive(Default)]
struct DueDeletions {
    segids: Vec<Segid>,
    awaiting_bytes: u64,
}

/// Give each newly dead segment this scan's horizon and pick the ones past
/// theirs. A deadline is kept until the counter is dropped, so a later
/// checkpoint cannot push it out and a transient failure does not restart it.
fn classify_dead(store: &ExtentStore, dead: &[SegStat], delete_horizon: Instant) -> DueDeletions {
    let now = Instant::now();
    let mut delete_at = store.delete_at.lock().unwrap();
    let mut due = DueDeletions::default();
    for stat in dead {
        due.awaiting_bytes += stat.total;
        if now >= *delete_at.entry(stat.segid).or_insert(delete_horizon) {
            due.segids.push(stat.segid);
        }
    }
    // Segments no longer dead or listed leave the map, so it cannot grow
    // unbounded.
    let still_dead: HashSet<Segid> = dead.iter().map(|stat| stat.segid).collect();
    delete_at.retain(|segid, _| still_dead.contains(segid));
    due
}

/// What became of one scan-classified dead segment.
enum DeadOutcome {
    /// Object deleted; drop its counter.
    Deleted { segid: Segid, total: u64 },
    /// Object already absent (a crash between DELETE and the counter drop);
    /// drop the leaked counter.
    CounterOnly { segid: Segid, total: u64 },
    /// Still referenced, or its counter changed underneath the scan: keep
    /// object and counter and report the invariant breach.
    Kept,
}

#[derive(Default)]
struct DeadSweep {
    /// Counters to drop with their `total`.
    freed: Vec<(Segid, u64)>,
    reclaimed_bytes: u64,
    deleted: usize,
    deleted_bytes: u64,
    /// A verification or DELETE failed; the segment keeps its deadline.
    failed: bool,
}

/// Verify and delete due segments with bounded concurrency.
async fn delete_due_segments(
    store: &ExtentStore,
    due: Vec<Segid>,
    cancel: &CancellationToken,
) -> DeadSweep {
    let results = stream::iter(due)
        .take_while(|_| std::future::ready(!cancel.is_cancelled()))
        .map(|segid| reclaim_dead_segment(store, segid))
        .buffer_unordered(PARALLEL_DEAD_SEGMENT_JOBS);
    futures::pin_mut!(results);
    let mut sweep = DeadSweep::default();
    while let Some(result) = results.next().await {
        match result {
            Ok(DeadOutcome::Deleted { segid, total }) => {
                sweep.freed.push((segid, total));
                sweep.reclaimed_bytes += total;
                sweep.deleted += 1;
                sweep.deleted_bytes += total;
            }
            Ok(DeadOutcome::CounterOnly { segid, total }) => {
                sweep.freed.push((segid, total));
                sweep.reclaimed_bytes += total;
            }
            Ok(DeadOutcome::Kept) => {}
            Err(_) => sweep.failed = true,
        }
    }
    sweep
}

#[derive(Default)]
struct RepackSummary {
    sources: usize,
    frames_relocated: usize,
    jobs: usize,
    error: Option<FsError>,
}

/// Run up to `max_concurrent_repacks` jobs concurrently until no selection
/// meets the minimum space savings or combined output size. Selection uses
/// scanned live-byte counts; each job checks its budget against exact stored
/// bytes and returns unused sources to the pool. A failed job stops new jobs;
/// the next scan retries from the database.
async fn run_repack_jobs(
    store: &ExtentStore,
    mut pool: CandidatePool,
    policy: &CyclePolicy,
    cancel: &CancellationToken,
) -> RepackSummary {
    let mut summary = RepackSummary::default();
    let mut active = FuturesUnordered::new();
    let mut admitting = true;
    loop {
        while admitting && active.len() < policy.max_concurrent_repacks {
            if cancel.is_cancelled() {
                admitting = false;
                break;
            }
            let Some(selection) = pool.select(policy.job_bytes) else {
                break;
            };
            // A declined selection leaves this snapshot so it cannot hide a
            // worthwhile source behind it; the next scan reconsiders it.
            if !selection.worth_repacking() {
                continue;
            }
            summary.jobs += 1;
            let job_bytes = policy.job_bytes;
            active.push(async move {
                let result = repack::run(store, &selection.segids(), job_bytes).await;
                (selection, result)
            });
        }

        let Some((selection, result)) = active.next().await else {
            break;
        };
        match result {
            Ok(progress) => {
                summary.frames_relocated += progress.relocated;
                summary.sources += progress.consumed;
                pool.restore(selection.unconsumed(progress.consumed));
            }
            Err(error) => {
                summary.error.get_or_insert(error);
                admitting = false;
            }
        }
    }
    summary
}

/// Drop deleted segments' counters, else one segcount key leaks per segment
/// ever created. Verification rechecked each row after proving the segment
/// unreferenced, and eligibility keeps `(0, total)` stable: packs mint fresh
/// ids and writers credit only the open segment.
async fn drop_dead_counters(store: &ExtentStore, freed: &[(Segid, u64)]) -> Result<(), FsError> {
    for chunk in freed.chunks(SEGCOUNT_DROPS_PER_COMMIT) {
        let mut txn = store.db.new_transaction()?;
        for &(segid, total) in chunk {
            let key = store.key_codec.segcount_key(segid.epoch, segid.counter);
            txn.delete_segcount(&key, 0, total);
        }
        store.commit_transaction(txn).await?;
        // Only a committed drop may forget the elapsed deadline; an error
        // above leaves it, so the retry does not wait out a fresh horizon.
        let mut delete_at = store.delete_at.lock().unwrap();
        for &(segid, _) in chunk {
            delete_at.remove(&segid);
        }
    }
    Ok(())
}

struct CycleReport {
    scanned: usize,
    total_live: u64,
    total_appended: u64,
    candidates_seen: usize,
    awaiting: usize,
    awaiting_bytes: u64,
    dead: DeadSweep,
    repack: RepackSummary,
}

impl CycleReport {
    fn reclaimable(&self) -> u64 {
        self.total_appended.saturating_sub(self.total_live)
    }

    /// One info line per cycle: durable logical footprint and completed work.
    /// The counters exclude segment directory/footer framing, so this is not
    /// exact object-store usage.
    fn log(&self) {
        let dead_pct = self
            .reclaimable()
            .saturating_mul(100)
            .checked_div(self.total_appended)
            .unwrap_or(0);
        let idle = self.dead.deleted == 0 && self.repack.sources == 0 && self.awaiting == 0;
        let action = if idle {
            "idle".to_string()
        } else {
            format!(
                "deleted {} dead (~{}), repacked {} of {} candidates in {} job(s) ({} frames), {} pending deletion (~{})",
                self.dead.deleted,
                human_bytes(self.dead.deleted_bytes),
                self.repack.sources,
                self.candidates_seen,
                self.repack.jobs,
                self.repack.frames_relocated,
                self.awaiting,
                human_bytes(self.awaiting_bytes)
            )
        };
        info!(
            "segment reclamation: {} segments, {} appended ({} live, ~{} reclaimable, {}% dead); {}",
            self.scanned,
            human_bytes(self.total_appended),
            human_bytes(self.total_live),
            human_bytes(self.reclaimable()),
            dead_pct,
            action
        );
    }

    fn stats(&self) -> SegmentReclaimCycle {
        SegmentReclaimCycle {
            awaiting_delete: self.awaiting as u64,
            awaiting_delete_bytes: self.awaiting_bytes,
            checkpoint_pinned: false,
            segments_deleted: self.dead.deleted as u64,
            deleted_bytes: self.dead.deleted_bytes,
            repack_sources: self.repack.sources as u64,
            frames_relocated: self.repack.frames_relocated as u64,
            repack_jobs: self.repack.jobs as u64,
        }
    }

    fn error(&self) -> Option<FsError> {
        self.repack
            .error
            .or(self.dead.failed.then_some(FsError::IoError))
    }

    fn outcome(&self) -> ReclaimOutcome {
        ReclaimOutcome {
            deleted: self.dead.deleted,
            relocated: self.repack.frames_relocated,
        }
    }
}

/// Result of one orphan sweep.
#[derive(Debug)]
pub enum OrphanSweep {
    Completed {
        deleted: usize,
    },
    /// Stopped by `cancel`; the persisted cadence is not advanced.
    Interrupted {
        deleted: usize,
    },
}

impl OrphanSweep {
    pub fn deleted(&self) -> usize {
        match self {
            Self::Completed { deleted } | Self::Interrupted { deleted } => *deleted,
        }
    }
}

/// Sweep objects with no `segcount` key. Counter credit commits atomically with
/// the first FrameLoc, so an absent counter means no extent references the
/// object. This is the only segment-namespace LIST.
///
/// Candidates must be sealed before the cutoff and pass directory verification.
/// The reclaim lock excludes concurrent repacks whose output PUT has not yet
/// been credited; a queued repoint still carries its extent-ref pin, which the
/// cutoff barrier waits for.
pub async fn sweep_orphans(
    store: &ExtentStore,
    cancel: &CancellationToken,
) -> Result<OrphanSweep, FsError> {
    let _reclaim_guard = store.segment_reclaim_lock.lock().await;
    if cancel.is_cancelled() {
        return Ok(OrphanSweep::Interrupted { deleted: 0 });
    }
    let cutoff = durable_reclaim_cutoff(store).await?;
    // Verify candidates as they arrive, traversing the entire listing even
    // when individual objects cannot be verified or deleted.
    let mut deleted = 0;
    let mut scanned = 0usize;
    let stream = store.segments.list_segments_stream();
    futures::pin_mut!(stream);
    while let Some(result) = stream.next().await {
        if cancel.is_cancelled() {
            info!("orphan sweep: shutdown after reclaiming {deleted} orphan(s)");
            return Ok(OrphanSweep::Interrupted { deleted });
        }
        let segid = result.map_err(|_| FsError::IoError)?;
        scanned += 1;
        if !sealed_before(&segid, cutoff.epoch, cutoff.counter) {
            continue;
        }
        let key = store.key_codec.segcount_key(segid.epoch, segid.counter);
        if store
            .db
            .get_bytes(&key)
            .await
            .map_err(|_| FsError::IoError)?
            .is_some()
        {
            continue;
        }
        if cancel.is_cancelled() {
            info!("orphan sweep: shutdown after reclaiming {deleted} orphan(s)");
            return Ok(OrphanSweep::Interrupted { deleted });
        }
        // Confirm via the directory before deleting; any error keeps the
        // object for a later sweep.
        match verify_segment_reclaimable(store, segid).await {
            Ok(SegmentDeadVerdict::Reclaim) => {}
            // Deleted concurrently; an orphan has no counter to drop.
            Ok(SegmentDeadVerdict::ObjectAbsent) => continue,
            Ok(SegmentDeadVerdict::Referenced) => {
                error!(
                    "orphan sweep BUG: refused to delete {segid:?}: it has no counter, \
                     but an extent still references it. Data was kept. Please report this at \
                     https://github.com/Barre/ZeroFS/issues"
                );
                continue;
            }
            Err(_) => {
                error!("orphan sweep: verification of {segid:?} failed; retrying next sweep");
                continue;
            }
        }
        let delete_result = {
            let _delete = store.segment_reclaim_stats.begin_delete();
            store.segments.delete_segment(segid).await
        };
        if let Err(e) = delete_result {
            error!("orphan sweep: delete of {segid:?} failed: {e}; skipping (retried next sweep)");
            continue;
        }
        info!(
            "orphan sweep: deleted orphan segment {:?} at {}",
            segid,
            segid.object_key()
        );
        deleted += 1;
    }
    info!("orphan sweep: scanned {scanned} segment objects, reclaimed {deleted} orphan(s)");
    Ok(OrphanSweep::Completed { deleted })
}

/// Recover only the startup delay from the last completed sweep. A future
/// timestamp delays startup by at most one interval; missing or invalid records
/// make it immediately due. The running scheduler must use monotonic time.
pub(super) async fn orphan_sweep_startup_delay(
    store: &ExtentStore,
    interval: Duration,
) -> Result<Duration, FsError> {
    let last = store
        .db
        .get_bytes(&store.key_codec.last_orphan_sweep_key())
        .await
        .map_err(|_| FsError::IoError)?
        .and_then(|b| KeyCodec::decode_u64(&b))
        .and_then(|secs| i64::try_from(secs).ok())
        .and_then(|secs| DateTime::from_timestamp(secs, 0));
    let Some(last) = last else {
        return Ok(Duration::ZERO);
    };
    let elapsed = (store.reclaim_now() - last).to_std().unwrap_or_default();
    Ok(interval.saturating_sub(elapsed))
}

/// Run a scheduled sweep without a wall-clock eligibility check. Only a
/// completed sweep updates the timestamp used to recover after a restart.
pub(super) async fn sweep_orphans_and_record(
    store: &ExtentStore,
    cancel: &CancellationToken,
) -> Result<OrphanSweep, FsError> {
    let sweep = sweep_orphans(store, cancel).await?;
    let OrphanSweep::Completed { .. } = sweep else {
        return Ok(sweep);
    };
    // Persist the timestamp after the sweep, so a crash mid-sweep re-runs
    // it sooner rather than skipping a cadence.
    let mut txn = store.db.new_transaction()?;
    txn.put_bytes(
        &store.key_codec.last_orphan_sweep_key(),
        KeyCodec::encode_u64(u64::try_from(store.reclaim_now().timestamp()).unwrap_or_default()),
    );
    store.commit_transaction(txn).await?;
    Ok(sweep)
}

/// Verify and retire one scan-classified dead segment. `Err` means a
/// transient failure; the segment keeps its deadline for the next scan.
async fn reclaim_dead_segment(store: &ExtentStore, segid: Segid) -> Result<DeadOutcome, FsError> {
    let verdict = match verify_segment_reclaimable(store, segid).await {
        Ok(verdict) => verdict,
        Err(error) => {
            error!("segment reclamation: verification of {segid:?} failed; retrying next scan");
            return Err(error);
        }
    };
    match verdict {
        SegmentDeadVerdict::Referenced => {
            // The counter under-counted: leak beats loss.
            error!(
                "segment reclamation BUG: refused to delete {segid:?}: its counter says zero-live, \
                 but an extent still references it. Data was kept. Please report this at \
                 https://github.com/Barre/ZeroFS/issues"
            );
            return Ok(DeadOutcome::Kept);
        }
        SegmentDeadVerdict::Reclaim | SegmentDeadVerdict::ObjectAbsent => {}
    }

    // The publication barrier makes a new credit impossible for an eligible
    // segment, but never turn an observed invariant breach into object loss.
    let counter_key = store.key_codec.segcount_key(segid.epoch, segid.counter);
    let total = match store.db.get_bytes(&counter_key).await {
        Ok(Some(value)) => match KeyCodec::decode_segcount(&value) {
            Some((0, total)) => total,
            Some((live, _)) => {
                error!(
                    "segment reclamation BUG: refused to delete {segid:?}: its counter became live \
                     ({live} bytes) after the dead scan. Data was kept. Please report this at \
                     https://github.com/Barre/ZeroFS/issues"
                );
                return Ok(DeadOutcome::Kept);
            }
            None => {
                error!("segment reclamation: undecodable counter for {segid:?}; skipping delete");
                return Ok(DeadOutcome::Kept);
            }
        },
        Ok(None) => {
            error!("segment reclamation: counter for {segid:?} disappeared; skipping delete");
            return Ok(DeadOutcome::Kept);
        }
        Err(e) => {
            error!(
                "segment reclamation: counter recheck for {segid:?} failed: {e}; skipping delete"
            );
            return Err(FsError::IoError);
        }
    };

    if matches!(verdict, SegmentDeadVerdict::ObjectAbsent) {
        return Ok(DeadOutcome::CounterOnly { segid, total });
    }

    #[cfg(feature = "failpoints")]
    {
        fail_point!(fp::RECLAIM_AFTER_VERIFY_BEFORE_DELETE);
        fp::widen(fp::RECLAIM_AFTER_VERIFY_BEFORE_DELETE).await;
    }
    let delete_result = {
        let _delete = store.segment_reclaim_stats.begin_delete();
        store.segments.delete_segment(segid).await
    };
    if let Err(e) = delete_result {
        error!(
            "segment reclamation: delete of {segid:?} failed: {e}; skipping (retried next scan)"
        );
        return Err(FsError::IoError);
    }

    #[cfg(feature = "failpoints")]
    fail_point!(fp::RECLAIM_AFTER_SEGMENT_DELETE);

    // Audit line for the irreversible delete; the object key joins against
    // object-store access logs.
    info!(
        "segment reclamation: deleted dead segment {:?} at {} (~{})",
        segid,
        segid.object_key(),
        human_bytes(total)
    );
    Ok(DeadOutcome::Deleted { segid, total })
}

/// Outcome of checking whether a segment the counter calls dead is truly
/// reclaimable.
enum SegmentDeadVerdict {
    /// Object present, directory read OK, no extent still points here.
    Reclaim,
    /// Object already gone (NotFound).
    ObjectAbsent,
    /// A live frame still points here.
    Referenced,
}

/// Confirm a dead segment truly holds no live-referenced frame. Each pointer is
/// checked in both the current and durable views; any uncertainty keeps the
/// segment.
async fn verify_segment_reclaimable(
    store: &ExtentStore,
    segid: Segid,
) -> Result<SegmentDeadVerdict, FsError> {
    let dir_result = {
        let _fetch = store.segment_reclaim_stats.begin_fetch();
        store.segments.read_directory(segid).await
    };
    let dir = match dir_result {
        Ok(d) => d,
        Err(SegmentStoreError::NotFound) => return Ok(SegmentDeadVerdict::ObjectAbsent),
        // A transient read error keeps the segment for the next scan.
        Err(_) => return Err(FsError::IoError),
    };
    // Unique extents the directory names (an extent can recur across
    // rewrites). Ordered so the lookup fan-out issues deterministically.
    let want: BTreeSet<(InodeId, u64)> = dir.iter().map(|e| (e.inode, e.extent)).collect();
    let still_referenced = stream::iter(want)
        .map(|(inode, extent)| async move {
            let _permit = store
                .reclaim_metadata_sem
                .acquire()
                .await
                .map_err(|_| FsError::IoError)?;
            let key = store.key_codec.extent_key(inode, extent);
            let current = store
                .db
                .get_bytes(&key)
                .await
                .map_err(|_| FsError::IoError)?;
            let durable = store
                .db
                .get_bytes_durable(&key)
                .await
                .map_err(|_| FsError::IoError)?;
            let points_here = |encoded: Option<Bytes>, view: &str| match encoded {
                None => Ok(false),
                Some(encoded) => match FrameLoc::decode(&encoded) {
                    Some(loc) => Ok(loc.segid == segid),
                    None => {
                        error!(
                            "segment reclamation: refused to delete {segid:?}: {view} extent pointer \
                             {inode}:{extent} is malformed, so its target cannot be verified. \
                             Data was kept"
                        );
                        Err(FsError::IoError)
                    }
                },
            };
            Ok::<bool, FsError>(
                points_here(current, "current")? || points_here(durable, "durable")?,
            )
        })
        .buffer_unordered(PARALLEL_EXTENT_OPS)
        .try_any(|referenced| async move { referenced })
        .await?;
    Ok(if still_referenced {
        SegmentDeadVerdict::Referenced
    } else {
        SegmentDeadVerdict::Reclaim
    })
}

#[cfg(test)]
mod tests {
    use super::super::super::test_util::*;
    use super::super::SMALL_SEGMENT_BYTES;
    use super::*;
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::db::Db;
    use crate::fault_store::FaultStore;
    use crate::fs::EXTENT_SIZE;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{ObjectStore, path::Path};
    use slatedb::{BlockTransformer, DbBuilder};
    use std::sync::Arc;
    use std::time::Duration;

    const ECONOMIC_SOURCE_LIVE_FRAMES: u64 = 2;

    fn expires_now() -> SegmentProtection {
        SegmentProtection::Until(Instant::now())
    }

    async fn sweep_all_orphans(store: &ExtentStore) -> Result<usize, FsError> {
        Ok(sweep_orphans(store, &CancellationToken::new())
            .await?
            .deleted())
    }

    /// Build one fragmented source with 35 physical frames but only two live
    /// slots. Its dead bytes clear the production 1 MiB payoff floor.
    async fn make_economic_fragmented_source(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        seed: usize,
    ) -> Segid {
        for rewrite in 0..34u64 {
            committed_write(
                store,
                db,
                inode,
                0,
                Bytes::from(incompressible(seed + rewrite as usize, EXTENT_SIZE)),
                if rewrite == 0 { 0 } else { EXTENT_SIZE as u64 },
            )
            .await;
        }
        for extent in 1..ECONOMIC_SOURCE_LIVE_FRAMES {
            committed_write(
                store,
                db,
                inode,
                extent * EXTENT_SIZE as u64,
                Bytes::from(incompressible(seed + 100 + extent as usize, EXTENT_SIZE)),
                extent * EXTENT_SIZE as u64,
            )
            .await;
        }
        store.seal_open().await.unwrap();
        let segid = frameloc_of(store, db, inode, 0).await.unwrap().segid;
        let (live, total) = segcount_pair_of(store, db, segid).await;
        assert!(total - live >= SMALL_SEGMENT_BYTES);
        segid
    }

    #[tokio::test]
    async fn completed_scan_acknowledges_activity() {
        let (store, _) = make().await;
        assert!(store.reclaim_activity.pending());

        reclaim(&store, Instant::now(), true).await.unwrap();
        assert!(store.reclaim_activity.pending());

        reclaim(&store, Instant::now(), false).await.unwrap();
        assert!(!store.reclaim_activity.pending());
    }

    #[tokio::test]
    async fn reclaims_dead_repack_outputs_without_further_writes() {
        use crate::fs::types::SetAttributes;
        use crate::fs::{TombstoneCleaner, ZeroFS};
        use crate::test_helpers::test_helpers_mod::test_auth;

        const EXTENTS: usize = 11; // Unlink uses tombstone cleanup above ten extents.
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let store = &fs.extent_store;
        let auth = (&test_auth()).into();
        let mut files = Vec::new();
        for (i, name) in [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .enumerate()
        {
            let (inode, _) = fs
                .create(
                    &crate::fs::test_util::test_creds(),
                    0,
                    name,
                    &SetAttributes::default(),
                )
                .await
                .unwrap();
            fs.write(
                &auth,
                inode,
                0,
                &Bytes::from(incompressible(i * 1000, EXTENTS * EXTENT_SIZE)),
            )
            .await
            .unwrap();
            // Make each source independently worth repacking.
            for rewrite in 0..34 {
                fs.write(
                    &auth,
                    inode,
                    0,
                    &Bytes::from(incompressible(i * 1000 + rewrite + 1, EXTENT_SIZE)),
                )
                .await
                .unwrap();
            }
            fs.flush_coordinator.flush().await.unwrap();
            let source = frameloc_of(store, &fs.db, inode, 0).await.unwrap().segid;
            files.push((name, inode, source));
        }

        let expired = Instant::now();
        let outcome = run(
            store,
            || std::future::ready(Ok(SegmentProtection::Until(expired))),
            CyclePolicy {
                job_bytes: 1, // One source per job, producing two outputs.
                max_concurrent_repacks: 2,
                ..test_policy()
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(outcome.deleted, 0);
        assert_eq!(outcome.relocated, files.len() * EXTENTS);

        let open = store.open.lock().unwrap().segid;
        assert!(store.open.lock().unwrap().dir.is_empty());
        let mut outputs = Vec::new();
        for &(name, inode, source) in &files {
            let output = frameloc_of(store, &fs.db, inode, 0).await.unwrap().segid;
            assert_ne!(output, source);
            assert_eq!(output.epoch, open.epoch);
            assert!(output.counter > open.counter);
            assert!(!outputs.contains(&output));
            outputs.push(output);
            fs.remove(&auth, 0, name).await.unwrap();
        }
        assert_eq!(
            fs.tombstone_store
                .list()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            files.len()
        );
        TombstoneCleaner::new(
            fs.tombstone_store.clone(),
            store.clone(),
            Arc::clone(&fs.stats),
        )
        .run()
        .await
        .unwrap();
        assert!(
            fs.tombstone_store
                .list()
                .await
                .unwrap()
                .try_next()
                .await
                .unwrap()
                .is_none()
        );
        fs.flush_coordinator.flush().await.unwrap();
        assert_eq!(store.open.lock().unwrap().segid, open);
        for &output in &outputs {
            let (live, total) = segcount_pair_of(store, &fs.db, output).await;
            assert_eq!(live, 0);
            assert!(total > 0);
        }

        // No data writes or restart: the next cycle must reclaim both outputs
        // along with their now-dead sources.
        let (deleted, relocated) = reclaim(store, expired, false).await.unwrap();
        assert_eq!(deleted, files.len() + outputs.len());
        assert_eq!(relocated, 0);
        assert!(store.segments.list_segments().await.unwrap().is_empty());
        for output in outputs {
            let key = store.key_codec.segcount_key(output.epoch, output.counter);
            assert!(fs.db.get_bytes(&key).await.unwrap().is_none());
        }
    }

    // Directory verification refuses to delete a segment the counter wrongly
    // calls dead while a frame is still referenced.
    #[tokio::test]
    async fn directory_verify_blocks_deleting_an_undercounted_live_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);

        // Simulate an under-count: force live to zero while the extent lives (total
        // is left nonzero, as a real segment's would be).
        let key = store.key_codec.segcount_key(seg.epoch, seg.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(&key, KeyCodec::encode_segcount(0, 1000));
        commit(&store, txn).await;

        // Reclaim sees count 0 but the directory-verify finds extent 0 still points
        // here, so it must not delete the segment.
        let (deleted, _) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert_eq!(
            deleted, 0,
            "verify must block deleting a referenced segment"
        );
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[1u8; 1000]);
    }

    #[tokio::test]
    async fn directory_verify_keeps_a_segment_with_a_malformed_extent_pointer() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let (_, total) = segcount_pair_of(&store, &db, seg).await;

        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(
            &store.key_codec.segcount_key(seg.epoch, seg.counter),
            KeyCodec::encode_segcount(0, total),
        );
        txn.put_bytes(
            &store.key_codec.extent_key(1, 0),
            Bytes::from_static(b"malformed FrameLoc"),
        );
        commit(&store, txn).await;

        assert!(
            reclaim(&store, Instant::now(), false).await.is_err(),
            "an undecodable pointer must make deletion retryable"
        );
        assert!(store.segments.list_segments().await.unwrap().contains(&seg));
        assert!(
            store
                .db
                .get_bytes(&store.key_codec.segcount_key(seg.epoch, seg.counter))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn transient_dead_segment_verify_retries_without_resetting_its_deadline() {
        let (old_store, db, object_store) =
            make_with_compression(crate::config::CompressionConfig::Lz4).await;
        drop(old_store);
        let (faulty, faults) = FaultStore::new(object_store);
        let store = make_store(faulty, db.clone(), crate::config::CompressionConfig::Lz4, 8).await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let dead = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();

        let expired = Instant::now();
        faults.fail_gets(1);
        assert!(
            reclaim(&store, expired, false).await.is_err(),
            "a transient verification failure must make the scan retryable"
        );
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);
        assert_eq!(store.delete_at.lock().unwrap().get(&dead), Some(&expired));

        let (deleted, _) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert!(!store.delete_at.lock().unwrap().contains_key(&dead));
    }

    /// A transaction may append a frame before its FrameLoc/counter commit.
    /// Reclaim must drain that publisher before it seals and classifies the
    /// segment; otherwise an immediate horizon can delete the object and let
    /// the delayed transaction publish a dangling pointer afterward.
    #[tokio::test]
    async fn reclaim_drains_staged_frame_publishers_before_classification() {
        let (store, db) = make().await;

        // Give the current open segment a durable, fully-dead counter row.
        let mut seed = db.new_transaction().unwrap();
        store
            .write(&mut seed, 1, 0, &Bytes::from(vec![1u8; 1000]), 0)
            .await
            .unwrap();
        commit(&store, seed).await;
        let segid = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let mut remove = db.new_transaction().unwrap();
        store.delete_range(&mut remove, 1, 0, 1).await.unwrap();
        commit(&store, remove).await;
        let (live, total) = segcount_pair_of(&store, &db, segid).await;
        assert_eq!(live, 0);
        assert!(total > 0);

        // Append another frame to that same still-open segment, but deliberately
        // retain its transaction instead of committing it.
        let expected = Bytes::from(vec![7u8; 1000]);
        let mut pending = db.new_transaction().unwrap();
        store.write(&mut pending, 2, 0, &expected, 0).await.unwrap();
        assert!(pending.has_extent_ref_guard());
        assert!(
            store.extent_ref_barrier.try_write().is_err(),
            "the staged FrameLoc must hold the reference-read side"
        );

        // With an already-expired horizon this scan would previously seal the
        // segment, observe live=0/no pointer, and delete it before `pending`
        // committed. It must now block on the reference-write barrier.
        let reclaim_store = store.clone();
        let mut reclaim =
            tokio::spawn(async move { reclaim(&reclaim_store, Instant::now(), false).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut reclaim)
                .await
                .is_err(),
            "reclaim crossed the barrier while a FrameLoc publisher was outstanding"
        );

        commit(&store, pending).await;
        let (deleted, _) = tokio::time::timeout(Duration::from_secs(5), reclaim)
            .await
            .expect("reclaim completes after publisher commit")
            .expect("reclaim task")
            .expect("reclaim scan");
        assert_eq!(deleted, 0, "the newly referenced segment must be kept");
        assert_eq!(store.read(2, 0, 1000).await.unwrap(), expected);

        let footprint = store.sample_footprint().await.unwrap();
        let gauges = store.segment_reclaim_stats();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(footprint.segment_count, gauges.segment_count.load(Relaxed));
        assert_eq!(
            footprint.appended_bytes,
            gauges.appended_bytes.load(Relaxed)
        );
        assert_eq!(footprint.live_bytes, gauges.live_bytes.load(Relaxed));
    }

    // The verify checks the durable view too: a durable pointer masked in memory
    // by an unflushed overwrite must keep the segment (a crash before the flush
    // would revive that pointer over a deleted object). WAL off + no size-freeze,
    // as production, so only explicit flushes make rows durable and the overwrite
    // deterministically stays memory-only.
    #[tokio::test]
    async fn directory_verify_keeps_a_durably_referenced_segment_masked_by_an_unflushed_overwrite()
    {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bt: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::try_new_arc(&[0u8; 32], CompressionConfig::default())
                .expect("test key should be lockable");
        let settings = slatedb::config::Settings {
            wal_enabled: false,
            l0_sst_size_bytes: crate::manifest_publication::COORDINATED_L0_SST_SIZE_BYTES,
            max_unflushed_bytes: crate::manifest_publication::COORDINATED_MAX_UNFLUSHED_BYTES,
            ..Default::default()
        };
        let slatedb = Arc::new(
            DbBuilder::new(Path::from("t"), object_store.clone())
                .with_settings(settings)
                .with_block_transformer(bt)
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        let db = Arc::new(Db::new(slatedb, None));
        let store = make_store(object_store, db.clone(), CompressionConfig::Lz4, 7).await;

        // Extent 0 -> segment S, durable.
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        db.flush().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;

        // Overwrite extent 0: the memory view moves the pointer off S, the
        // durable view still references it.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        assert!(
            matches!(
                verify_segment_reclaimable(&store, seg).await,
                Ok(SegmentDeadVerdict::Referenced)
            ),
            "a durable reference must keep the segment while the overwrite is unflushed"
        );

        // Flushed, both views agree the pointer moved: reclaimable.
        store.seal_open().await.unwrap();
        db.flush().await.unwrap();
        assert!(matches!(
            verify_segment_reclaimable(&store, seg).await,
            Ok(SegmentDeadVerdict::Reclaim)
        ));
    }

    // Counter reclaim cannot see an absent-counter orphan; only the listing
    // sweep can reclaim it, and live data must remain untouched.
    #[tokio::test]
    async fn counter_reclaim_leaves_a_no_counter_orphan() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        // Overwrite so segment A's frame is dead (extent 0 points elsewhere), seal B.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Drop A's counter entirely so it looks like a failed-repoint orphan.
        let key = store.key_codec.segcount_key(seg.epoch, seg.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;

        // The counter scan cannot see A, so it remains untouched.
        let (deleted, _) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert_eq!(
            deleted, 0,
            "counter reclaim must not delete an absent-counter orphan"
        );
        assert!(
            store.segments.list_segments().await.unwrap().contains(&seg),
            "no-counter orphan A must survive counter reclaim for the listing sweep"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // A crash after object deletion can leave its dead counter behind. Reclaim
    // must recognize NotFound and drop that counter.
    #[tokio::test]
    async fn reclaim_drops_a_counter_whose_object_is_gone() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let dead = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        // Overwrite + seal so `dead`'s frame is superseded (live == 0) but its
        // counter stays.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();

        // Simulate a crash between deleting the object and dropping the counter:
        // delete the object directly, leaving its (live == 0) counter behind.
        store.segments.delete_segment(dead).await.unwrap();
        let key = store.key_codec.segcount_key(dead.epoch, dead.counter);
        assert!(
            store.db.get_bytes(&key).await.unwrap().is_some(),
            "precondition: the leaked counter is present"
        );

        // Counter-based reclaim sees the row (live == 0 -> dead), finds the object
        // absent, and drops the leaked counter instead of getting stuck.
        reclaim(&store, Instant::now(), false).await.unwrap();
        assert!(
            store.db.get_bytes(&key).await.unwrap().is_none(),
            "the leaked counter (object already gone) must be dropped"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // A missing counter must not make the orphan sweep delete a segment that
    // an extent still references.
    #[tokio::test]
    async fn sweep_orphans_keeps_an_uncounted_referenced_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[7u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let key = store.key_codec.segcount_key(seg.epoch, seg.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;

        let reclaimed = sweep_all_orphans(&store).await.unwrap();
        assert_eq!(
            reclaimed, 0,
            "directory verification must keep a referenced orphan"
        );
        assert!(store.segments.list_segments().await.unwrap().contains(&seg));
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[7u8; 1000]);
    }

    #[tokio::test]
    async fn sweep_orphans_reclaims_repack_outputs_without_further_writes() {
        let (store, db) = make().await;
        let open = store.open.lock().unwrap().segid;
        for inode in 1..=3 {
            // Seal through the repack allocator without publishing pointers,
            // as when repacking stops after its PUT and before repointing.
            let locs = store
                .segments
                .seal(&[(inode, 0, Bytes::from_static(b"orphan"))])
                .await
                .unwrap();
            assert_eq!(locs[0].2.segid.epoch, open.epoch);
            assert!(locs[0].2.segid.counter > open.counter);
        }
        assert!(store.open.lock().unwrap().dir.is_empty());
        assert_eq!(sweep_all_orphans(&store).await.unwrap(), 3);
        assert!(store.segments.list_segments().await.unwrap().is_empty());

        // Future writes use the new boundary, which remains excluded from
        // this sweep's cutoff. Advancing it must not PUT an empty object.
        let boundary = store.open.lock().unwrap().segid;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, b"new data").await;
        let loc = frameloc_of(&store, &db, 1, 0).await.unwrap();
        assert_eq!(loc.segid, boundary);
        assert!(!sealed_before(&loc.segid, boundary.epoch, boundary.counter));
        assert!(store.segments.list_segments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sweep_orphans_continues_after_a_verification_failure() {
        let (old_store, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        for inode in 1..=3 {
            old_store
                .segments
                .seal(&[(inode, 0, Bytes::from_static(b"orphan"))])
                .await
                .unwrap();
        }
        drop(old_store);
        let (faulty, faults) = FaultStore::new(object_store);
        let store = make_store(faulty, db, CompressionConfig::Lz4, 8).await;
        faults.fail_gets(1);

        assert_eq!(sweep_all_orphans(&store).await.unwrap(), 2);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(sweep_all_orphans(&store).await.unwrap(), 1);
        assert!(store.segments.list_segments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sweep_orphans_streams_deletes_and_reports_progress_on_shutdown() {
        const ORPHANS: usize = 64;
        let (old_store, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        for inode in 1..=ORPHANS as u64 {
            old_store
                .segments
                .seal(&[(inode, 0, Bytes::from_static(b"orphan"))])
                .await
                .unwrap();
        }
        drop(old_store);
        let observed = InFlightObjectStore::new(object_store, Duration::from_millis(50));
        let store = make_store(observed.clone(), db, CompressionConfig::Lz4, 8).await;
        let cancel = CancellationToken::new();
        let sweep = tokio::spawn({
            let store = store.clone();
            let cancel = cancel.clone();
            async move { sweep_orphans(&store, &cancel).await }
        });
        let listed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(listed) = observed.listed_at_first_delete() {
                    break listed;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sweep never started deleting");
        cancel.cancel();
        assert!(
            listed < ORPHANS,
            "sweep buffered the complete listing before deleting"
        );
        let OrphanSweep::Interrupted { deleted } = sweep.await.unwrap().unwrap() else {
            panic!("cancelled sweep should not advance its persisted cadence");
        };
        assert!(deleted > 0 && deleted < ORPHANS);
        assert_eq!(
            store.segments.list_segments().await.unwrap().len(),
            ORPHANS - deleted
        );
    }

    #[tokio::test]
    async fn sweep_orphans_reclaims_an_orphan_after_restart() {
        let (store, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let orphan = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let key = store.key_codec.segcount_key(orphan.epoch, orphan.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;
        drop(store);

        // The "restarted" store shares the object store and db but none of the
        // in-RAM sweep state, and opens under the next epoch.
        let restarted = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8).await;
        assert_eq!(
            sweep_all_orphans(&restarted).await.unwrap(),
            1,
            "a fresh process's first sweep must reclaim an orphan"
        );
        assert!(
            !restarted
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );
        assert_eq!(
            restarted.read(1, 0, 1000).await.unwrap().as_ref(),
            &[2u8; 1000]
        );
    }

    // Reclaim debits gauges immediately. Dense, fully-live B prevents this
    // scan from doing anything except deleting dead segment A.
    #[tokio::test]
    async fn footprint_gauges_drop_when_a_segment_is_reclaimed() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        let big = 48 * EXTENT_SIZE; // ~1.5 MiB > SMALL_SEGMENT_BYTES
        write_and_check(&store, &db, &mut model, 0, &incompressible(1, big)).await;
        store.seal_open().await.unwrap();
        // Overwrite the whole range: segment A becomes fully dead, B is segment 2.
        write_and_check(&store, &db, &mut model, 0, &incompressible(2, big)).await;
        store.seal_open().await.unwrap();

        use std::sync::atomic::Ordering::Relaxed;
        let m = store.segment_reclaim_stats();
        let appended_before = m.appended_bytes.load(Relaxed);
        assert_eq!(m.segment_count.load(Relaxed), 2);
        assert!(m.footprint().reclaimable_bytes > 0, "segment A is dead");

        let (deleted, relocated) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(relocated, 0, "B is large and fully live: nothing to repack");

        assert!(
            m.appended_bytes.load(Relaxed) < appended_before,
            "A's bytes debited"
        );
        assert_eq!(m.segment_count.load(Relaxed), 1);
        assert!(m.cycles.load(Relaxed) >= 1);
        assert_eq!(m.segments_deleted.load(Relaxed), 1);
        assert!(m.deleted_bytes.load(Relaxed) > 0);
        assert_read_matches(&store, &model).await;
        // Still consistent with a fresh scan of the post-reclaim state.
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f, m.footprint());
    }

    #[tokio::test]
    async fn reclaim_waits_for_delete_horizon() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A dead segment: write+seal (A), overwrite+seal (B); A is now unreferenced.
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // The first scan records a future delete horizon for A and holds it.
        let horizon = Instant::now() + Duration::from_millis(80);
        let outcome = reclaim_tuned(
            &store,
            horizon,
            false,
            ReclaimConfig::DEFAULT_REPACK_MIN_DEAD_PERCENT,
        )
        .await
        .unwrap();
        assert_eq!(outcome.deleted, 0);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Past the recorded horizon the next scan reclaims it (the new arg is
        // irrelevant — A keeps its first-seen deadline).
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(reclaim(&store, Instant::now(), false).await.unwrap().0, 1);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    #[tokio::test]
    async fn repack_leaves_a_few_tiny_segments_alone() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A handful of tiny, fully-live segments: below the merge floor with no dead
        // space. Packing them would just produce an equally-tiny segment that gets
        // repacked again next scan — pure churn — so reclamation must leave them be.
        for i in 0..5u8 {
            let off = i as usize * EXTENT_SIZE;
            write_and_check(&store, &db, &mut model, off, &[i + 1; 100]).await;
            store.seal_open().await.unwrap();
        }
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 5);

        let (deleted, relocated) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert_eq!(
            relocated, 0,
            "tiny dense segments are left alone, not churned"
        );
        assert_eq!(deleted, 0);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 5);
        assert_read_matches(&store, &model).await;
    }

    #[tokio::test]
    async fn tiny_repack_output_stays_stable_across_cycles_and_restart() {
        async fn assert_stable(store: &ExtentStore, db: &Db, output: FrameLoc) {
            let jobs = store
                .segment_reclaim_stats()
                .repack_jobs
                .load(Ordering::Relaxed);
            for (repack_min_dead_percent, max_concurrent_repacks) in [(1, 1), (10, 4), (99, 16)] {
                let outcome = run(
                    store,
                    || std::future::ready(Ok(expires_now())),
                    CyclePolicy::from(&ReclaimConfig {
                        repack_min_dead_percent: Some(repack_min_dead_percent),
                        max_concurrent_repacks: Some(max_concurrent_repacks),
                    }),
                    &CancellationToken::new(),
                )
                .await
                .unwrap();
                assert_eq!(outcome.relocated, 0, "a tiny dense output must not churn");
                assert_eq!(frameloc_of(store, db, 1, 0).await.unwrap(), output);
                assert_eq!(
                    store
                        .segment_reclaim_stats()
                        .repack_jobs
                        .load(Ordering::Relaxed),
                    jobs
                );
                assert!(!store.reclaim_activity.pending());
            }
        }

        for compression in [CompressionConfig::Lz4, CompressionConfig::Zstd(3)] {
            let (store, db, object_store) = make_with_compression(compression).await;
            let source = make_economic_fragmented_source(&store, &db, 1, 0).await;
            let outcome = run(
                &store,
                || std::future::ready(Ok(expires_now())),
                CyclePolicy::from(&ReclaimConfig::default()),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
            assert_eq!(outcome.relocated, ECONOMIC_SOURCE_LIVE_FRAMES as usize);
            let output = frameloc_of(&store, &db, 1, 0).await.unwrap();
            assert_ne!(output.segid, source);
            let (live, total) = segcount_pair_of(&store, &db, output.segid).await;
            assert_eq!(live, total);
            assert!(total < SMALL_SEGMENT_BYTES, "exercise a still-small output");
            assert_stable(&store, &db, output).await;

            // The payoff rule must survive losing every in-memory scheduling hint.
            drop(store);
            let restarted = make_store(object_store, db.clone(), compression, 8).await;
            assert_stable(&restarted, &db, output).await;
            let expected = [
                incompressible(33, EXTENT_SIZE),
                incompressible(101, EXTENT_SIZE),
            ]
            .concat();
            assert_eq!(
                restarted
                    .read(1, 0, expected.len() as u64)
                    .await
                    .unwrap()
                    .as_ref(),
                expected.as_slice()
            );
        }
    }

    #[tokio::test]
    async fn repack_combines_small_segments_once_they_clear_the_floor() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Four small (< 1 MiB) but incompressible segments that together exceed the
        // merge floor, so packing yields a segment above the small threshold.
        let seg_bytes = 28 * EXTENT_SIZE; // 896 KiB < SMALL_SEGMENT_BYTES
        let n = 4usize; // ~3.5 MiB total > SMALL_SEGMENT_BYTES
        for i in 0..n {
            let off = i * seg_bytes;
            write_and_check(
                &store,
                &db,
                &mut model,
                off,
                &incompressible(off, seg_bytes),
            )
            .await;
            store.seal_open().await.unwrap();
        }
        let before = store.segments.list_segments().await.unwrap().len();
        assert_eq!(before, n);

        // One scan packs them; the next reclaims the drained sources.
        let (_, relocated) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert!(relocated > 0, "accumulated small segments get packed");
        reclaim(&store, Instant::now(), false).await.unwrap();
        let after = store.segments.list_segments().await.unwrap().len();
        assert!(
            after < before,
            "packed into fewer segments ({before} -> {after})"
        );
        assert_read_matches(&store, &model).await;

        // The packed output clears the small threshold, so a further scan is a no-op.
        let (_, relocated3) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert_eq!(
            relocated3, 0,
            "packed output is not repacked again (no churn)"
        );
    }

    #[tokio::test]
    async fn persistent_checkpoint_protects_older_segments() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // One write -> segment S; then zero the extent -> its extent is a hole and S
        // is fully dead.
        write_and_check(&store, &db, &mut model, 0, &[1u8; 100]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        write_and_check(&store, &db, &mut model, 0, &[0u8; 100]).await;
        store.seal_open().await.unwrap();

        // Any persistent checkpoint blocks both deletion and repack of S.
        let (deleted, relocated) = reclaim(&store, Instant::now(), true).await.unwrap();
        assert_eq!((deleted, relocated), (0, 0), "S is protected by the pin");
        assert!(!store.segments.list_segments().await.unwrap().is_empty());

        // Without the pin, the same dead segment is reclaimed.
        let (deleted, _) = reclaim(&store, Instant::now(), false).await.unwrap();
        assert!(deleted >= 1, "S is reclaimed once no longer protected");
    }

    /// The configured floor controls whether a dense segment is repacked.
    #[tokio::test]
    async fn repack_threshold_reclaims_mildly_dead_dense_segments() {
        let (store, db) = make().await;
        let store = store.with_seal_threshold(6 * 1024 * 1024);
        // 100 incompressible extents -> one dense ~3.2 MiB segment S.
        let n = 100u64;
        let dead = 32u64;
        for extent in 0..n {
            write_extent(
                &store,
                &db,
                extent,
                &incompressible(extent as usize, EXTENT_SIZE),
            )
            .await;
        }
        store.seal_open().await.unwrap();
        let s = frameloc_of(&store, &db, 1, 50).await.unwrap().segid;
        // Overwrite 32 extents (~32% dead, over 1 MiB).
        for extent in 0..dead {
            committed_write(
                &store,
                &db,
                1,
                extent * EXTENT_SIZE as u64,
                Bytes::from(incompressible(9000 + extent as usize, EXTENT_SIZE)),
                n * EXTENT_SIZE as u64,
            )
            .await;
        }
        store.seal_open().await.unwrap();

        // ~32% dead is under a 35% floor.
        let outcome = reclaim_tuned(&store, Instant::now(), false, 35)
            .await
            .unwrap();
        assert_eq!(outcome.relocated, 0, "below the floor: not repacked");
        assert_eq!(frameloc_of(&store, &db, 1, 50).await.unwrap().segid, s);

        let outcome = reclaim_tuned(
            &store,
            Instant::now(),
            false,
            ReclaimConfig::DEFAULT_REPACK_MIN_DEAD_PERCENT,
        )
        .await
        .unwrap();
        assert_eq!(outcome.relocated, (n - dead) as usize, "repack ran");
        assert_ne!(frameloc_of(&store, &db, 1, 50).await.unwrap().segid, s);

        // Integrity: the file reads back with the overwrites applied.
        let mut expect = Vec::new();
        for extent in 0..n {
            let seed = if extent < dead {
                9000 + extent as usize
            } else {
                extent as usize
            };
            expect.extend_from_slice(&incompressible(seed, EXTENT_SIZE));
        }
        assert_eq!(
            store
                .read(1, 0, n * EXTENT_SIZE as u64)
                .await
                .unwrap()
                .as_ref(),
            expect.as_slice()
        );
    }

    /// Two full repack jobs run from one scan. The remaining candidates are
    /// skipped because they neither reclaim enough space nor combine enough
    /// live data.
    #[tokio::test]
    async fn full_candidate_snapshot_drains_beyond_one_job_budget() {
        let (store, db) = make().await;
        let store = store.with_seal_threshold(4 * 1024 * 1024);
        let mut segids = Vec::new();
        for i in 0..4u64 {
            segids
                .push(make_economic_fragmented_source(&store, &db, i + 1, i as usize * 1000).await);
        }
        committed_write(&store, &db, 99, 0, Bytes::from(vec![9u8; 1000]), 0).await;
        store.seal_open().await.unwrap();
        let max_live =
            futures::future::join_all(segids.iter().map(|&segid| segcount_of(&store, &db, segid)))
                .await
                .into_iter()
                .max()
                .unwrap();
        let job_bytes = 2 * max_live;
        let outcome = run(
            &store,
            || std::future::ready(Ok(expires_now())),
            CyclePolicy {
                job_bytes,
                ..test_policy()
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.relocated,
            4 * ECONOMIC_SOURCE_LIVE_FRAMES as usize,
            "two full jobs process one snapshot"
        );
    }

    /// The real scan executor overlaps disjoint repack jobs. Delaying only
    /// SegmentStore I/O makes overlap deterministic without slowing SlateDB.
    #[tokio::test]
    async fn executor_overlaps_disjoint_repack_jobs_and_segment_io() {
        let (old_store, db, object_store) =
            make_with_compression(crate::config::CompressionConfig::Lz4).await;
        drop(old_store);
        let observed = InFlightObjectStore::new(object_store, Duration::from_millis(1));
        let store = make_store(
            observed.clone(),
            db.clone(),
            crate::config::CompressionConfig::Lz4,
            8,
        )
        .await
        .with_seal_threshold(4 * 1024 * 1024);
        for inode in 1..=4u64 {
            make_economic_fragmented_source(&store, &db, inode, inode as usize * 1000).await;
        }
        observed.reset_peaks();

        let outcome = run(
            &store,
            || std::future::ready(Ok(expires_now())),
            CyclePolicy {
                job_bytes: 1,
                max_concurrent_repacks: 2,
                ..test_policy()
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.relocated, 4 * ECONOMIC_SOURCE_LIVE_FRAMES as usize);
        assert_eq!(observed.peak_gets(), 2, "source GETs should overlap");
        assert_eq!(observed.peak_puts(), 2, "packed PUTs should overlap");
        let stats = store.segment_reclaim_stats();
        assert_eq!(stats.active_repacks.load(Ordering::SeqCst), 0);
        assert_eq!(stats.active_fetches.load(Ordering::SeqCst), 0);
        assert_eq!(stats.active_puts.load(Ordering::SeqCst), 0);
        assert_eq!(stats.repack_memory_reserved_bytes.load(Ordering::SeqCst), 0);
    }

    /// Fully-dead objects have no dependency on one another after the safe
    /// cutoff, so their verification and DELETE calls share bounded concurrency.
    #[tokio::test]
    async fn reclaim_deletes_dead_segments_concurrently() {
        let (old_store, db, object_store) =
            make_with_compression(crate::config::CompressionConfig::Lz4).await;
        drop(old_store);
        let observed = InFlightObjectStore::new(object_store, Duration::from_millis(1));
        let store = make_store(
            observed.clone(),
            db.clone(),
            crate::config::CompressionConfig::Lz4,
            8,
        )
        .await;
        let versions = PARALLEL_DEAD_SEGMENT_JOBS + 2;
        for i in 0..versions {
            write_extent(&store, &db, 0, &incompressible(i, EXTENT_SIZE)).await;
            store.seal_open().await.unwrap();
        }
        observed.reset_peaks();

        let outcome = run(
            &store,
            || std::future::ready(Ok(SegmentProtection::Until(Instant::now()))),
            test_policy(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.deleted, versions - 1);
        assert_eq!(
            observed.peak_deletes(),
            PARALLEL_DEAD_SEGMENT_JOBS,
            "dead-segment DELETEs should fill, but not cross, their bound"
        );
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(
            store.read(1, 0, EXTENT_SIZE as u64).await.unwrap().as_ref(),
            incompressible(versions - 1, EXTENT_SIZE)
        );
    }
}
