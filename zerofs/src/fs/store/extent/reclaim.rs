//! The reclaim pass driver: segcount scan, horizon-aged deletion of dead
//! segments (directory-verified), compaction batch selection and drive,
//! and the slow orphan sweep (the only path that LISTs the object store).

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

#[cfg(test)]
use super::select::MAX_COMPACT_BYTES_PER_ROUND;
use super::select::{ColdCtx, HotChain, SegStat, chain_components, live_permille, select_round};
use super::{ExtentStore, PARALLEL_EXTENT_OPS, human_bytes};
use crate::fs::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::metrics::SegmentGcPass;
use crate::segment::{FrameLoc, Segid};
use crate::segment_store::SegmentStoreError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use slatedb::config::WriteOptions;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::{error, info};

/// Compaction thresholds. A live segment is a candidate when *fragmented*
/// (live bytes below this percent of total) or *small* (below
/// SMALL_SEGMENT_BYTES). Dense, full-size segments are left alone: re-sealing
/// one 1:1 reclaims nothing.
const FRAG_LIVE_PERCENT: u64 = 50;
pub(super) const SMALL_SEGMENT_BYTES: u64 = 1 << 20; // 1 MiB

/// A compaction round must free at least this much dead space to run (unless
/// enough small-segment live bytes have accumulated to pack a dense segment;
/// see `reclaim_segments_gated`). A near-1:1 repack is churn.
const MIN_FREED_BYTES: u64 = SMALL_SEGMENT_BYTES;

/// Cap on buffered compaction candidates, so a huge store can't make the list
/// O(#segments). When reached, the most-fragmented half is kept.
const MAX_CANDIDATES_BUFFERED: usize = 8192;

/// Store-wide write-cold proof: open counter unchanged this long ⇒ no writes
/// anywhere. Covers write-then-read-only stores whose counters never age.
/// Monotonic Instant (NTP must not fake it); a duration, so fast passes
/// cannot compress the proof. GcTuning default and test-wrapper value.
pub(crate) const QUIESCENT_AFTER_DEFAULT: Duration = Duration::from_secs(5 * 60);

/// Tail-scrub floor for the config-less wrapper; aliased so the literal
/// exists once.
#[cfg(test)]
pub(super) const TAIL_SCRUB_DEFAULT_PERCENT: u64 =
    crate::config::GcConfig::DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT;

/// Tail-candidate bound (keep-most-dead half on overflow). A separate list:
/// tail pressure must not displace fragmented candidates.
const TAIL_CANDIDATES_BUFFERED: usize = 8192;

/// Batches per pass while `keep_going` approves. RAM stays bounded per
/// batch; barrier, gate, and scan run once per pass.
const MAX_BATCHES_PER_PASS: usize = 32;

/// Cap on segments verified + deleted per reclaim pass; each delete costs a
/// dir read plus O(frames) point-lookups. Deferred segments keep their
/// (already-elapsed) deadline and are retried next pass.
const MAX_SEGMENT_DELETES_PER_PASS: usize = 1024;

/// Orphan-sweep age floor: an uncounted segment object must have been PUT at
/// least this long ago before it is deletable. Guards the window where an
/// object exists but its crediting commit is still in flight (a compaction
/// seal PUTs before its repoint commits). Judged from the object's
/// `last_modified`, not process-local state, so a restarted process still
/// reclaims old orphans on its first sweep.
const ORPHAN_MIN_AGE_SECS: i64 = 60 * 60;

/// Outcome of checking whether a segment the counter calls dead is truly
/// reclaimable, distinguishing the three cases the caller must handle differently.
enum SegmentDeadVerdict {
    /// Object present, directory read OK, no extent still points here: delete it.
    Reclaim,
    /// Object already gone (NotFound). Nothing to delete; the caller drops the
    /// leaked counter a crash left behind (object deleted, counter-drop uncommitted).
    ObjectAbsent,
    /// A live frame still points here, or a transient read error: keep (fail-closed).
    Keep,
}

/// What one reclaim pass did and whether it left actionable work behind —
/// the input to the GC loop's cadence decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassOutcome {
    pub deleted: usize,
    pub relocated: usize,
    pub status: PassStatus,
    /// Hot-seam chain disposition, same counts as the summary line.
    pub chains: ChainOutcome,
    /// Store-wide dead-space percent at scan time (0 on a skipped pass). The
    /// cadence uses it to keep a busy store draining while the backlog is large.
    pub dead_percent: u64,
}

/// Per-pass chain accounting: assembled, how many packed, and the rest by
/// skip cause. `deferred` folds gather-cap deferrals into reserve deferrals
/// (both retry a later pass); `assembled == packed + deferred + warm +
/// unpackable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChainOutcome {
    pub assembled: usize,
    pub packed: usize,
    pub deferred: usize,
    pub warm: usize,
    pub unpackable: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassStatus {
    /// `saturated`: a budget cap cut actionable work — the fast-cadence
    /// trigger.
    Completed { saturated: bool },
    /// The gate declined (checkpoint list unreadable): nothing classified.
    Skipped,
    /// Persistent checkpoint pin; explicit so it never reads as a drained
    /// backlog.
    Pinned,
}

impl ExtentStore {
    /// Reclaim object-store space: delete fully-dead segments and compact the
    /// most-fragmented ones, driven entirely off the local `segcount` scan.
    /// Returns (segments deleted, frames relocated); production goes through
    /// [`Self::reclaim_segments_gated`], which also reports saturation.
    #[cfg(test)]
    pub async fn reclaim_segments(
        &self,
        delete_horizon: DateTime<Utc>,
        // Set when a persistent checkpoint pins a view; see the `pinned` gate
        // in reclaim_segments_gated.
        protect_before: Option<DateTime<Utc>>,
    ) -> Result<(usize, usize), FsError> {
        let outcome = self
            .reclaim_segments_gated(
                move || std::future::ready(Ok(Some((delete_horizon, protect_before)))),
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                |_| false,
                MAX_COMPACT_BYTES_PER_ROUND,
            )
            .await?;
        Ok((outcome.deleted, outcome.relocated))
    }

    /// As [`Self::reclaim_segments`] with explicit tuning, for tests that need
    /// a short quiescence window, a different scrub floor, or multi-batch.
    #[cfg(test)]
    pub(super) async fn reclaim_segments_tuned(
        &self,
        delete_horizon: DateTime<Utc>,
        protect_before: Option<DateTime<Utc>>,
        tail_min_dead_percent: Option<u64>,
        quiescent_after: Duration,
        keep_going: impl Fn() -> bool,
    ) -> Result<PassOutcome, FsError> {
        self.reclaim_segments_gated(
            move || std::future::ready(Ok(Some((delete_horizon, protect_before)))),
            tail_min_dead_percent,
            quiescent_after,
            // Tests drive the drain purely via keep_going; the batch count (the
            // production throughput floor) is ignored here.
            move |_batches| keep_going(),
            MAX_COMPACT_BYTES_PER_ROUND,
        )
        .await
    }

    /// One full reclaim pass; the horizon gate is computed by `gate` after the
    /// durable barrier (seal + flush). That ordering closes a race: a
    /// concurrently-created checkpoint is either visible to `gate` (extending
    /// the horizon) or pins the already-flushed manifest, in which everything
    /// this pass reclaims is unreferenced. Listed before the barrier, it could
    /// pin the pre-flush manifest and a to-be-dead segment with it. `gate`
    /// returning `Ok(None)` skips the pass; `Err` aborts.
    ///
    /// `tail_min_dead_percent` (config: `[gc] tail_scrub_min_dead_percent`) is
    /// the pass-3 scrub floor. `None` disables the scrub (no tail buffering,
    /// no pass 3); `quiescent_after` is how long the open counter must sit
    /// unchanged before the whole store counts as write-cold.
    ///
    /// `keep_going(batches_done)`, polled between batches, approves continuing a
    /// multi-batch drain; `|_| false` gives the historical one-round pass. The
    /// batch count lets the caller run a throughput floor before it starts
    /// gating on foreground idleness (see the segment-GC loop).
    ///
    /// `round_bytes` (config `compact_round_max_mib`) is the per-round live-byte
    /// budget: the selection cap, the heat reserve (half), and the stored-byte
    /// gather cap (frames stay compressed through the gather, so its RAM tracks
    /// stored size whatever the data's compression ratio). Raising it packs
    /// over-reserve seams and lifts dead-space throughput per batch, at
    /// proportional gather RAM.
    pub async fn reclaim_segments_gated<F, Fut, K>(
        &self,
        gate: F,
        tail_min_dead_percent: Option<u64>,
        quiescent_after: Duration,
        keep_going: K,
        round_bytes: u64,
    ) -> Result<PassOutcome, FsError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<
                Output = Result<Option<(DateTime<Utc>, Option<DateTime<Utc>>)>, FsError>,
            >,
        K: Fn(usize) -> bool,
    {
        // Drain FrameLoc publishers before seal+flush+cutoff. Lock order matches
        // the commit path: extent-reference barrier, then DB flush barrier.
        let (cur_epoch, cutoff) = {
            let _refs = self.extent_ref_barrier.clone().write_owned().await;
            let _barrier = self.db.flush_barrier().write_owned().await;
            self.seal_open().await?;
            self.db.flush().await.map_err(|_| FsError::IoError)?;
            (
                self.segments.epoch(),
                self.open.lock().unwrap().segid.counter,
            )
        };
        tracing::debug!("segment GC: durable barrier done (sealed + flushed), scanning extents");

        // Post-barrier on purpose; see the doc comment for the race this closes.
        let (delete_horizon, protect_before) = match gate().await? {
            Some(v) => v,
            None => {
                return Ok(PassOutcome {
                    deleted: 0,
                    relocated: 0,
                    status: PassStatus::Skipped,
                    chains: ChainOutcome::default(),
                    dead_percent: 0,
                });
            }
        };

        #[cfg(feature = "failpoints")]
        {
            fail_point!(fp::RECLAIM_AFTER_BARRIER_BEFORE_SCAN);
            fp::widen(fp::RECLAIM_AFTER_BARRIER_BEFORE_SCAN).await;
        }

        // Exclusive counter cutoff for eligibility: the still-open segment's
        // own counter, not `next_counter()`. seal_open() just rotated in a
        // fresh open segment that is still accepting frames whose credits may
        // not be visible to the scan below; `next_counter()` would include it,
        // and a mid-pass background seal could then mis-classify that fully
        // live segment as dead.
        // A persistent checkpoint pins a view this scan-driven path can't
        // bound by object mtime (it never LISTs). Skip classification entirely
        // while one exists; the scan still runs for the footprint log.
        let pinned = protect_before.is_some();

        // A segment is reclaimable only once it can no longer gain references: it was
        // sealed before this round (older epoch, or below the counter cutoff).
        let eligible =
            |s: &Segid| s.epoch < cur_epoch || (s.epoch == cur_epoch && s.counter < cutoff);

        // Drain nominations; pinned passes leave them queued. Gone or dense
        // segids drop out at the scan and are re-nominated if still relevant.
        let (nominated, nom_dropped) = if pinned {
            (HashSet::new(), 0)
        } else {
            self.nominations.lock().unwrap().drain()
        };

        // Pass clock (episode dedup; ticks through pins) and write
        // quiescence: cutoff unchanged since first seen => no write anywhere.
        self.gc_round.fetch_add(1, Ordering::Relaxed);
        let now_mono = Instant::now();
        let quiescent = {
            let mut q = self.quiescence.lock().unwrap();
            if (q.0, q.1) != (cur_epoch, cutoff) {
                *q = (cur_epoch, cutoff, now_mono);
            }
            now_mono.saturating_duration_since(q.2) >= quiescent_after
        };

        // Hot seams -> chains, the one path that may select dense segments.
        // The scan below fills endpoint stats; assembly happens after it, over
        // pairs whose both endpoints were scan-seen live.
        let (hot_pairs, pairs_dropped) = if pinned {
            (Vec::new(), 0)
        } else {
            self.pair_stats.lock().unwrap().sweep_and_hot(now_mono)
        };
        let pair_endpoints: HashSet<Segid> = hot_pairs.iter().flat_map(|&(a, b)| [a, b]).collect();
        let mut endpoint_stats: HashMap<Segid, (u64, u64)> = HashMap::new(); // (total, live)

        // Used by the tail candidacy below; re-verified inside select_round.
        let cold_ctx = ColdCtx {
            cur_epoch,
            cutoff,
            quiescent,
        };
        let write_cold = |s: &Segid| cold_ctx.write_cold(s);

        // The commit worker keeps each counter's `(live, total)` current, so
        // `live == 0` is the dead test and `live/total` the fragmentation
        // ratio — no listing needed. Absent counters never appear in this scan;
        // the slow orphan sweep owns those. Every delete is still
        // directory-verified below.
        //
        // Read at the durable (object-storage) level, not the in-memory view: a
        // delete removes the object irreversibly and at once, but a segment's
        // death (the overwrite/trim debit driving `live` to 0) is only durable
        // once flushed.
        //
        // Stream the scan, keeping only dead segids plus a bounded candidate
        // set, so per-pass RAM is O(dead + cap), not O(#segments).
        let (sc_start, sc_end) = self.key_codec.segcount_prefix_range();
        let mut candidates: Vec<(Segid, u64, u64)> = Vec::new(); // (segid, total, live)
        let mut tail: Vec<SegStat> = Vec::new(); // pass-3 scrub candidates
        let mut dead: Vec<(Segid, u64)> = Vec::new(); // (segid, total ~= bytes freed)
        let mut scanned = 0usize;
        let mut total_live = 0u64;
        let mut total_appended = 0u64;
        let mut stream = self.db.scan_durable(sc_start..sc_end).await.map_err(|e| {
            error!("GC segcount scan failed: {}", e);
            FsError::IoError
        })?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            let Some((epoch, counter)) = self.key_codec.parse_segcount_key(&key) else {
                continue;
            };
            let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
                continue;
            };
            let segid = Segid::new(epoch, counter);
            scanned += 1;
            total_live += live;
            total_appended += total;
            if !eligible(&segid) {
                continue;
            }
            if pinned {
                continue;
            }
            if live == 0 {
                dead.push((segid, total));
            } else {
                if pair_endpoints.contains(&segid) {
                    endpoint_stats.insert(segid, (total, live));
                }
                let fragmented = live.saturating_mul(100) < total.saturating_mul(FRAG_LIVE_PERCENT);
                let small = total < SMALL_SEGMENT_BYTES;
                if fragmented || small {
                    candidates.push((segid, total, live));
                    if candidates.len() >= MAX_CANDIDATES_BUFFERED {
                        // Keep the most-fragmented (lowest live fraction) half,
                        // nominated candidates first so they survive the cut:
                        // read heat must outlive exactly the overflow that
                        // would otherwise erase it (the set caps at
                        // NOMINATION_SET_CAP << the kept half).
                        candidates.sort_by_key(|&(s, tot, lb)| {
                            (!nominated.contains(&s), live_permille(lb, tot))
                        });
                        candidates.truncate(MAX_CANDIDATES_BUFFERED / 2);
                    }
                } else if let Some(floor) = tail_min_dead_percent
                    && total.saturating_sub(live).saturating_mul(100) > total.saturating_mul(floor)
                    && write_cold(&segid)
                {
                    // Tail-scrub candidate: the (floor, 50%] band candidacy
                    // strands. Separate list; nominations get no say here.
                    tail.push((segid, total, live));
                    if tail.len() >= TAIL_CANDIDATES_BUFFERED {
                        // Keep the most-dead half.
                        tail.sort_by_key(|&(_, tot, lb)| live_permille(lb, tot));
                        tail.truncate(TAIL_CANDIDATES_BUFFERED / 2);
                    }
                }
            }
        }
        drop(stream);
        tracing::debug!(
            "segment GC: segcount scan done, {} segments ({} dead, {} candidates)",
            scanned,
            dead.len(),
            candidates.len()
        );

        // Classify dead segments under the dead-tracking lock (no await held): a
        // fully-dead segment accrues its horizon and is deleted only once past it.
        let now = Utc::now();
        let mut deleted_bytes = 0u64;
        let mut awaiting_bytes = 0u64;
        // Past-due deletes cut by the per-pass cap are backlog; the
        // not-yet-due set is not (horizon floor now+60 s counting it would
        // spin a dozen no-op fast passes after every deletion burst).
        let mut due_deferred = 0usize;
        let to_delete = {
            let mut delete_at = self.delete_at.lock().unwrap();
            let mut to_delete: Vec<(Segid, u64)> = Vec::new();
            let mut still_waiting: HashSet<Segid> = HashSet::new();
            for (segid, size) in dead {
                // Record this pass's horizon the first time the segment is seen dead;
                // keep that deadline on later passes so a fresh checkpoint can't push
                // it out.
                let due = *delete_at.entry(segid).or_insert(delete_horizon);
                if now >= due && to_delete.len() < MAX_SEGMENT_DELETES_PER_PASS {
                    to_delete.push((segid, size));
                    delete_at.remove(&segid);
                } else {
                    // Not yet due, or over this pass's budget. Keep the deadline
                    // either way, so a budget-deferred segment retries at once.
                    due_deferred += (now >= due) as usize;
                    still_waiting.insert(segid);
                    awaiting_bytes += size;
                }
            }
            // Forget segments no longer dead/listed, so the map can't grow
            // unbounded.
            delete_at.retain(|s, _| still_waiting.contains(s));
            to_delete
        };

        let mut deleted = 0;
        let mut freed_counters: Vec<Segid> = Vec::new();
        for (segid, size) in to_delete {
            // A segment is deleted only once its directory confirms no
            // frame is still referenced.
            let verdict = match self.verify_segment_reclaimable(segid).await {
                SegmentDeadVerdict::Keep => {
                    // The counter under-counted (a live frame remains) or the read
                    // was transient — leak beats loss.
                    error!(
                        "segment GC: counter calls {segid:?} dead but a live frame remains; \
                         skipping delete (leak, not loss)"
                    );
                    continue;
                }
                verdict => verdict,
            };
            // The publication barrier makes a new credit impossible for an
            // eligible segment, but never turn an observed invariant breach
            // into object loss.
            let counter_key = self.key_codec.segcount_key(segid.epoch, segid.counter);
            let still_dead = match self.db.get_bytes(&counter_key).await {
                Ok(Some(value)) => match KeyCodec::decode_segcount(&value) {
                    Some((0, _)) => true,
                    Some((live, _)) => {
                        error!(
                            "segment GC: {segid:?} became live ({live} bytes) after its dead scan; \
                             skipping delete"
                        );
                        false
                    }
                    None => {
                        error!("segment GC: undecodable counter for {segid:?}; skipping delete");
                        false
                    }
                },
                Ok(None) => {
                    error!("segment GC: counter for {segid:?} disappeared; skipping delete");
                    false
                }
                Err(e) => {
                    error!(
                        "segment GC: counter recheck for {segid:?} failed: {e}; skipping delete"
                    );
                    false
                }
            };
            if !still_dead {
                continue;
            }
            if matches!(verdict, SegmentDeadVerdict::ObjectAbsent) {
                // A crash between deleting the object and committing the
                // counter drop left the counter behind; drop it so it stops
                // re-appearing as dead each pass.
                freed_counters.push(segid);
                continue;
            }
            #[cfg(feature = "failpoints")]
            {
                fail_point!(fp::RECLAIM_AFTER_VERIFY_BEFORE_DELETE);
                fp::widen(fp::RECLAIM_AFTER_VERIFY_BEFORE_DELETE).await;
            }
            if let Err(e) = self.segments.delete_segment(segid).await {
                // Don't abort the pass: already-deleted segments still need
                // their counters dropped. This one retries next pass.
                error!("segment GC: delete of {segid:?} failed: {e}; skipping (retried next pass)");
                continue;
            }

            #[cfg(feature = "failpoints")]
            fail_point!(fp::RECLAIM_AFTER_SEGMENT_DELETE);

            // Audit line for the irreversible delete; the object key joins
            // against object-store access logs.
            info!(
                "segment GC: deleted dead segment {:?} at {} (~{})",
                segid,
                segid.object_key(),
                human_bytes(size)
            );
            freed_counters.push(segid);
            deleted_bytes += size;
            deleted += 1;
        }
        // Drop the counters of deleted segments, else one segcount key leaks
        // per segment ever created.
        if !freed_counters.is_empty() {
            // Debit the gauges by each row's value at drop time, not the
            // scan-time total: a pack segment's per-inode credit txns can
            // straddle the scan, so the row may hold more than the scan saw.
            // Stable to read here: after the directory verify nothing credits
            // a retired segment (packs mint fresh ids, writers credit only
            // the open segment).
            let mut dropped = 0i64;
            let mut freed_appended = 0u64;
            let mut freed_live = 0u64;
            let mut txn = self.db.new_transaction()?;
            for segid in &freed_counters {
                let key = self.key_codec.segcount_key(segid.epoch, segid.counter);
                let value = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?;
                let Some(value) = value else {
                    continue;
                };
                let (live, total) = KeyCodec::decode_segcount(&value).ok_or(FsError::IoError)?;
                dropped += 1;
                freed_live += live;
                freed_appended += total;
                txn.delete_bytes(&key);
            }
            if dropped > 0 {
                self.commit_via_coordinator(txn).await?;
                // Use the exact rows removed rather than assuming the earlier
                // scan still describes them. `freed_live` should be zero.
                self.segment_gc_stats.apply_footprint_delta(
                    -dropped,
                    -(freed_appended as i64),
                    -(freed_live as i64),
                );
            }
        }

        // Chains from hot pairs whose both endpoints are scan-seen live —
        // components are complete by construction. A dead/unseen endpoint
        // drops the pair from assembly only, never from the PairStats table
        // (staleness ages it out; "not yet scanned" is indistinguishable
        // from "deleted").
        let live_pairs: Vec<(Segid, Segid)> = hot_pairs
            .iter()
            .filter(|(a, b)| endpoint_stats.contains_key(a) && endpoint_stats.contains_key(b))
            .copied()
            .collect();
        let chains: Vec<HotChain> = chain_components(&live_pairs)
            .into_iter()
            .map(|c| HotChain {
                members: c
                    .members
                    .into_iter()
                    .map(|s| {
                        let (total, live) = endpoint_stats[&s];
                        (s, total, live)
                    })
                    .collect(),
                edges: c.edges,
            })
            .collect();

        // Batch selection + compaction while budget-cut and `keep_going`
        // approves: barrier, gate, and scan amortize across an idle drain.
        // Heat rides batch 1; later batches are rank + tail. Remaining
        // counters stay valid compaction only drains selected segments.
        let candidate_backlog = candidates.len();
        // Freed-bytes lookup for the pass summary, so only consumed sources
        // are counted (an unconsumed pick re-selects next batch).
        let stat_of: HashMap<Segid, (u64, u64)> = candidates
            .iter()
            .chain(tail.iter())
            .chain(chains.iter().flat_map(|c| c.members.iter()))
            .map(|&(s, total, live)| (s, (total, live)))
            .collect();
        let mut remaining_candidates = candidates;
        let mut remaining_tail = tail;
        let chain_count = chains.len();
        let mut first_chains = Some(chains);
        let no_noms: HashSet<Segid> = HashSet::new();
        let mut compacted_segments = 0;
        let mut packed_segments = 0;
        let mut frames_relocated = 0;
        let mut freed_total: u64 = 0;
        let mut nominated_selected = 0;
        let mut chains_packed = 0;
        let mut tail_selected = 0;
        let mut batches = 0usize;
        let mut saturated_work;
        let mut chains_deferred = 0usize;
        let mut chains_warm = 0usize;
        let mut chains_unpackable = 0usize;
        let mut chain_groups_admitted = 0usize;
        // Picks compaction has not yet gathered (its RAM cap stops between
        // groups). Candidates/tail re-enter later selections; a chain pick
        // can't (chains ride batch 1 only), so a nonempty set at pass end is
        // backlog.
        let mut pending: HashSet<Segid> = HashSet::new();
        loop {
            let batch_noms = if batches == 0 { &nominated } else { &no_noms };
            let batch_chains = first_chains.take().unwrap_or_default();
            let sel = select_round(
                remaining_candidates.clone(),
                batch_noms,
                &batch_chains,
                remaining_tail.clone(),
                tail_min_dead_percent,
                cold_ctx,
                round_bytes,
            );
            chains_deferred += sel.chains_deferred;
            chains_warm += sel.chains_warm;
            chains_unpackable += sel.chains_unpackable;
            let freed = sel.sel_size.saturating_sub(sel.sel_live);
            // Gate: real dead space; a quarter-segment of small-live (so the
            // output isn't itself "small" and re-compacted); or a chain
            // group, which frees ~0 but dissolves a proven seam. The gather
            // is group-atomic and batch 1 starts at zero gathered bytes, so
            // the leading group always packs whole — and a whole-packed
            // group's merged output cannot re-heat its seams.
            let gate_fired = freed >= MIN_FREED_BYTES
                || sel.sel_live >= self.seal_threshold() as u64 / 4
                || !sel.chain_groups.is_empty();
            if !gate_fired {
                // Declined leftovers recur identically: not backlog, and the
                // batch-loop terminator.
                saturated_work = false;
                break;
            }
            batches += 1;
            let (n, p, consumed) = self
                .compact_segments(&sel.selected, &sel.chain_groups, round_bytes)
                .await?;
            let consumed_ids = &sel.selected[..consumed];
            frames_relocated += n;
            packed_segments += p;
            compacted_segments += consumed;
            freed_total += consumed_ids
                .iter()
                .filter_map(|s| stat_of.get(s))
                .map(|&(total, live)| total.saturating_sub(live))
                .sum::<u64>();
            nominated_selected += sel.nominated_selected;
            chain_groups_admitted += sel.chain_groups.len();
            // A chain counts as packed only when every member was gathered;
            // groups lead `selected`, so cumulative size <= consumed is
            // exact membership.
            let mut lead = 0usize;
            chains_packed += sel
                .chain_groups
                .iter()
                .take_while(|&&g| {
                    lead += g;
                    lead <= consumed
                })
                .count();
            tail_selected += sel.tail_selected;
            for s in consumed_ids {
                pending.remove(s);
            }
            pending.extend(sel.selected[consumed..].iter().copied());
            // Only consumed sources leave the drain; the rest stay for later
            // batches of this pass.
            let consumed_set: HashSet<Segid> = consumed_ids.iter().copied().collect();
            remaining_candidates.retain(|(s, _, _)| !consumed_set.contains(s));
            remaining_tail.retain(|(s, _, _)| !consumed_set.contains(s));
            saturated_work = sel.saturated;
            let drained = !sel.saturated && pending.is_empty();
            if drained || batches >= MAX_BATCHES_PER_PASS || !keep_going(batches) {
                // Drained, capped, or no longer idle; a cut with work
                // remaining stays saturated, so the cadence follows up.
                break;
            }
        }
        let saturated =
            due_deferred > 0 || saturated_work || chains_deferred > 0 || !pending.is_empty();
        // Admitted groups the gather's RAM cap cut retry from `pending` next
        // pass: deferred, from the operator's seat.
        let chains = ChainOutcome {
            assembled: chain_count,
            packed: chains_packed,
            deferred: chains_deferred + (chain_groups_admitted - chains_packed),
            warm: chains_warm,
            unpackable: chains_unpackable,
        };

        // One info line per pass: footprint, reclaimable bytes, and what the
        // pass did. `total_appended` excludes segment framing overhead, so it
        // slightly exceeds on-store bytes.
        let reclaimable = total_appended.saturating_sub(total_live);
        let dead_pct = reclaimable
            .saturating_mul(100)
            .checked_div(total_appended)
            .unwrap_or(0);
        let awaiting = self.delete_at.lock().unwrap().len();
        let action = if deleted > 0
            || compacted_segments > 0
            || candidate_backlog > 0
            || awaiting > 0
            || !nominated.is_empty()
            || !hot_pairs.is_empty()
            || tail_selected > 0
        {
            // Internal (self-pair) seams are broken out because their repack
            // economics are weaker than cross-segment seams' (same-object
            // reads keep the prefetch ramp); production judges them apart.
            let self_seams = hot_pairs.iter().filter(|(a, b)| a == b).count();
            format!(
                "deleted {} dead (~{}), compacted {}→{} of {} candidates in {} batch(es) ({} frames, ~{} to reclaim), {} tail-scrubbed, {} read-nominated ({} selected, {} dropped), {} hot seams ({} internal) → {} of {} chains packed ({} deferred, {} warm, {} over-reserve; {} pairs dropped), {} awaiting (~{})",
                deleted,
                human_bytes(deleted_bytes),
                compacted_segments,
                packed_segments,
                candidate_backlog,
                batches,
                frames_relocated,
                human_bytes(freed_total),
                tail_selected,
                nominated.len(),
                nominated_selected,
                nom_dropped,
                hot_pairs.len(),
                self_seams,
                chains.packed,
                chains.assembled,
                chains.deferred,
                chains.warm,
                chains.unpackable,
                pairs_dropped,
                awaiting,
                human_bytes(awaiting_bytes)
            )
        } else {
            "idle".to_string()
        };
        info!(
            "segment GC: {} segments, {} appended ({} live, ~{} reclaimable, {}% dead); {}",
            scanned,
            human_bytes(total_appended),
            human_bytes(total_live),
            human_bytes(reclaimable),
            dead_pct,
            action
        );
        self.segment_gc_stats.record_pass(&SegmentGcPass {
            awaiting_delete: awaiting as u64,
            awaiting_delete_bytes: awaiting_bytes,
            candidate_backlog: candidate_backlog as u64,
            chains_deferred: chains.deferred as u64,
            saturated,
            pinned,
            segments_deleted: deleted as u64,
            deleted_bytes,
            segments_compacted: compacted_segments as u64,
            segments_packed: packed_segments as u64,
            frames_relocated: frames_relocated as u64,
            compaction_freed_bytes: freed_total,
            batches: batches as u64,
            tail_scrubbed: tail_selected as u64,
            chains_packed: chains.packed as u64,
            chains_assembled: chains.assembled as u64,
            nominations: nominated.len() as u64,
            nominations_dropped: nom_dropped,
            hot_seams: hot_pairs.len() as u64,
        });
        Ok(PassOutcome {
            deleted,
            relocated: frames_relocated,
            status: if pinned {
                PassStatus::Pinned
            } else {
                PassStatus::Completed { saturated }
            },
            chains,
            dead_percent: dead_pct,
        })
    }

    /// Sweep orphan segment objects: present on the store with no `segcount`
    /// key (a crash between seal and the crediting commit, or a compaction
    /// whose repoints all lost the CAS). A frame's counter credit commits
    /// atomically with its FrameLoc, so an absent counter means no extent
    /// references the object. This is the only path that LISTs the object
    /// store — hence the slow cadence; the fast reclaim never sees
    /// counter-less objects.
    ///
    /// Candidates pass the same `eligible()` gate as the fast reclaim, an age
    /// gate — only objects `last_modified` before `modified_before` are
    /// deletable, so a PUT whose crediting commit is still in flight is never
    /// swept — and the directory verify. The age gate is stateless (the
    /// object's mtime, no process-local first-seen map), so a restarted
    /// process reclaims old orphans on its first sweep. The caller runs it
    /// after the fast reclaim, never concurrently, so an in-flight compaction
    /// can't leave a not-yet-credited packed segment eligible. Returns the
    /// number of orphans reclaimed.
    pub async fn sweep_orphans(&self, modified_before: DateTime<Utc>) -> Result<usize, FsError> {
        // Use the fast-reclaim barrier order and capture the cutoff while new
        // FrameLoc publishers are excluded.
        let (cur_epoch, cutoff) = {
            let _refs = self.extent_ref_barrier.clone().write_owned().await;
            let _barrier = self.db.flush_barrier().write_owned().await;
            self.seal_open().await?;
            self.db.flush().await.map_err(|_| FsError::IoError)?;
            (
                self.segments.epoch(),
                self.open.lock().unwrap().segid.counter,
            )
        };
        // Same eligibility cutoff as reclaim_segments_gated. A freshly-packed
        // compaction segment always carries a higher counter, so it is never
        // eligible even mid-compaction.
        let eligible =
            |s: &Segid| s.epoch < cur_epoch || (s.epoch == cur_epoch && s.counter < cutoff);

        // The one and only list("segments"). Point-get each object's counter
        // as the listing streams: present => the fast loop owns it; absent =>
        // orphan candidate. Cap the buffered set; the rest sweep next cadence.
        let mut orphans: Vec<Segid> = Vec::new();
        let mut scanned = 0usize;
        let stream = self.segments.list_segments_stream();
        futures::pin_mut!(stream);
        while let Some(result) = futures::StreamExt::next(&mut stream).await {
            let (segid, _size, mtime) = result.map_err(|_| FsError::IoError)?;
            scanned += 1;
            if !eligible(&segid) {
                continue;
            }
            // Age gate: an uncounted object younger than the cutoff may be a
            // PUT whose crediting commit is still in flight; it sweeps next
            // cadence once safely old.
            if mtime >= modified_before {
                continue;
            }
            let key = self.key_codec.segcount_key(segid.epoch, segid.counter);
            if self
                .db
                .get_bytes(&key)
                .await
                .map_err(|_| FsError::IoError)?
                .is_some()
            {
                continue;
            }
            orphans.push(segid);
            if orphans.len() >= MAX_SEGMENT_DELETES_PER_PASS {
                break;
            }
        }

        let mut deleted = 0;
        for segid in orphans {
            // Fail-closed, as in the fast reclaim: confirm via the directory
            // before deleting.
            match self.verify_segment_reclaimable(segid).await {
                SegmentDeadVerdict::Reclaim => {}
                // Deleted concurrently; an orphan has no counter to drop.
                SegmentDeadVerdict::ObjectAbsent => continue,
                SegmentDeadVerdict::Keep => {
                    error!(
                        "orphan sweep: {segid:?} has no counter but a live frame remains; \
                         skipping delete (leak, not loss)"
                    );
                    continue;
                }
            }
            if let Err(e) = self.segments.delete_segment(segid).await {
                error!(
                    "orphan sweep: delete of {segid:?} failed: {e}; skipping (retried next sweep)"
                );
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
        Ok(deleted)
    }

    /// Run the slow orphan sweep at most once per `interval`, gated by a
    /// persisted wall-clock timestamp so the cadence survives restarts (an
    /// uptime timer would never fire on a frequently-restarting deployment).
    /// Returns the orphans reclaimed, or `None` when not yet due.
    pub async fn sweep_orphans_if_due(
        &self,
        interval: chrono::Duration,
    ) -> Result<Option<usize>, FsError> {
        let now = Utc::now();
        let last = self
            .db
            .get_bytes(&self.key_codec.last_orphan_sweep_key())
            .await
            .map_err(|_| FsError::IoError)?
            .and_then(|b| KeyCodec::decode_u64(&b))
            .and_then(|secs| DateTime::from_timestamp(secs as i64, 0));
        if let Some(last) = last
            && now < last + interval
        {
            return Ok(None);
        }
        // No manifest or checkpoint ever references an orphan, so a generous
        // in-flight age floor is the only gate needed.
        let deleted = self
            .sweep_orphans(now - chrono::Duration::seconds(ORPHAN_MIN_AGE_SECS))
            .await?;
        // Persist the timestamp after the sweep, so a crash mid-sweep re-runs
        // it sooner rather than skipping a cadence.
        let mut txn = self.db.new_transaction()?;
        txn.put_bytes(
            &self.key_codec.last_orphan_sweep_key(),
            KeyCodec::encode_u64(now.timestamp() as u64),
        );
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
        Ok(Some(deleted))
    }

    /// Confirm a segment the counter calls dead truly holds no live-referenced
    /// frame, by reading its directory and checking each named extent against the
    /// forward map. Fail-closed: a read error, or any frame the forward map still
    /// points here, returns [`SegmentDeadVerdict::Keep`]. Sound because an eligible
    /// segment can never regain a reference, so the verdict is permanent.
    ///
    /// Each pointer is checked in both the in-memory and the durable view: the
    /// delete is irreversible, and a reference can hide from either view alone —
    /// a committed-but-unflushed reference exists only in memory, while a durable
    /// reference is masked in memory by an unflushed overwrite (the undercount
    /// case this verify exists to catch, where a crash after the delete would
    /// leave the durable pointer dangling).
    async fn verify_segment_reclaimable(&self, segid: Segid) -> SegmentDeadVerdict {
        let dir = match self.segments.read_directory(segid).await {
            Ok(d) => d,
            Err(SegmentStoreError::NotFound) => return SegmentDeadVerdict::ObjectAbsent,
            // Transient read error: fail-closed, keep the segment.
            Err(_) => return SegmentDeadVerdict::Keep,
        };
        // Unique extents the directory names (an extent can recur across
        // rewrites). Ordered so the lookup fan-out issues deterministically.
        let want: BTreeSet<(InodeId, u64)> = dir.iter().map(|e| (e.inode, e.extent)).collect();
        // A FrameLoc pointing here in either view means the segment is live; a
        // point-read error is treated as still-referenced (fail-closed).
        let points_here = |enc: Result<Option<Bytes>, anyhow::Error>| match enc {
            Ok(Some(enc)) => FrameLoc::decode(&enc).is_some_and(|loc| loc.segid == segid),
            Ok(None) => false,
            Err(_) => true,
        };
        let still_referenced = stream::iter(want)
            .map(|(inode, extent)| {
                let store = self.clone();
                async move {
                    let key = store.key_codec.extent_key(inode, extent);
                    points_here(store.db.get_bytes(&key).await)
                        || points_here(store.db.get_bytes_durable(&key).await)
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .any(|referenced| async move { referenced })
            .await;
        if still_referenced {
            SegmentDeadVerdict::Keep
        } else {
            SegmentDeadVerdict::Reclaim
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::select::{MAX_COMPACT_SEGMENTS_PER_ROUND, NOMINATE_PER_CALL_CAP, PairStats};
    use super::super::test_util::*;
    use super::*;
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::db::Db;
    use crate::fs::EXTENT_SIZE;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{ObjectStore, path::Path};
    use slatedb::{BlockTransformer, DbBuilder};
    use std::sync::Arc;

    // The fail-closed directory-verify refuses to delete a segment the counter wrongly
    // calls dead (under-count) while a frame is still referenced: leak, never loss.
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
        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            deleted, 0,
            "verify must block deleting a referenced segment"
        );
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[1u8; 1000]);
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

        // With an already-expired horizon this pass would previously seal the
        // segment, observe live=0/no pointer, and delete it before `pending`
        // committed. It must now block on the reference-write barrier.
        let reclaim_store = store.clone();
        let mut reclaim = tokio::spawn(async move {
            reclaim_store
                .reclaim_segments(Utc::now() - chrono::Duration::days(1), None)
                .await
        });
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
            .expect("reclaim pass");
        assert_eq!(deleted, 0, "the newly referenced segment must be kept");
        assert_eq!(store.read(2, 0, 1000).await.unwrap(), expected);

        let footprint = store.sample_footprint().await.unwrap();
        let gauges = store.segment_gc_stats();
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
            ZeroFsBlockTransformer::new_arc(&[0u8; 32], CompressionConfig::default());
        let settings = slatedb::config::Settings {
            wal_enabled: false,
            l0_sst_size_bytes: usize::MAX,
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
        let store = make_store(object_store, db.clone(), CompressionConfig::Lz4, 7);

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
                store.verify_segment_reclaimable(seg).await,
                SegmentDeadVerdict::Keep
            ),
            "a durable reference must keep the segment while the overwrite is unflushed"
        );

        // Flushed, both views agree the pointer moved: reclaimable.
        store.seal_open().await.unwrap();
        db.flush().await.unwrap();
        assert!(matches!(
            store.verify_segment_reclaimable(seg).await,
            SegmentDeadVerdict::Reclaim
        ));
    }

    // A dead segment whose `segcount` counter is entirely ABSENT (a compaction whose
    // repoints all lost the CAS, or a crash before the crediting commit) is invisible
    // to the scan-driven fast loop: with no counter it never appears in the segcount
    // scan. Reclaiming these absent-counter orphans is the slow listing loop's job
    // (`sweep_orphans`); the fast loop must leave the object (and live data) untouched.
    #[tokio::test]
    async fn fast_reclaim_leaves_a_no_counter_orphan() {
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

        // The fast loop scans counters, so A (no counter) is invisible and untouched.
        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            deleted, 0,
            "fast loop must not reclaim an absent-counter orphan"
        );
        assert!(
            store.segments.list_segments().await.unwrap().contains(&seg),
            "no-counter orphan A must survive the fast loop (the slow sweep reclaims it)"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // The slow sweep reclaims the no-counter orphan the fast loop leaves alone:
    // it lists the objects, point-gets each counter, and deletes those with none
    // (directory-verified first). Live data is untouched.
    #[tokio::test]
    async fn sweep_orphans_reclaims_a_no_counter_orphan() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let orphan = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        // Overwrite + seal so the first segment's frame is dead, then drop its counter
        // so the object carries none (a failed-repoint / crash-before-credit orphan).
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let key = store.key_codec.segcount_key(orphan.epoch, orphan.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );

        let reclaimed = store.sweep_orphans(Utc::now()).await.unwrap();
        assert_eq!(reclaimed, 1, "the no-counter orphan must be swept");
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan),
            "the orphan object must be deleted by the sweep"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // The mirror leak: a counter whose OBJECT is gone (a crash between deleting a
    // dead segment's object and committing its counter drop). Because dead
    // candidates come from the counter scan, such a counter re-appears as dead every
    // pass; the fast loop must recognize the object is absent (NotFound) and drop the
    // leaked counter, not get stuck failing the directory verify forever.
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

        // The fast reclaim sees the counter (live == 0 -> dead), finds the object
        // absent, and drops the leaked counter instead of getting stuck.
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(
            store.db.get_bytes(&key).await.unwrap().is_none(),
            "the leaked counter (object already gone) must be dropped"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // A normally-counted segment (counter present) is not an orphan: the sweep
    // point-gets its counter, finds it, and leaves the object alone.
    #[tokio::test]
    async fn sweep_orphans_leaves_a_counted_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[7u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        assert!(segcount_of(&store, &db, seg).await > 0);

        let reclaimed = store.sweep_orphans(Utc::now()).await.unwrap();
        assert_eq!(
            reclaimed, 0,
            "a counted segment must not be swept as an orphan"
        );
        assert!(store.segments.list_segments().await.unwrap().contains(&seg));
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[7u8; 1000]);
    }

    // The sweep's age gate defers a too-young candidate: a cutoff predating the
    // orphan's PUT leaves it in place (its crediting commit could still be in
    // flight); a cutoff past its mtime reclaims it in that same pass.
    #[tokio::test]
    async fn sweep_orphans_waits_for_the_age_floor() {
        let (store, db) = make().await;
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

        // A cutoff older than the orphan's PUT: too young, kept.
        let before_put = Utc::now() - chrono::Duration::seconds(60);
        assert_eq!(store.sweep_orphans(before_put).await.unwrap(), 0);
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );

        // A cutoff past its mtime: old enough, reclaimed in one pass.
        assert_eq!(store.sweep_orphans(Utc::now()).await.unwrap(), 1);
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // The age gate reads the object's mtime, not process-lifetime state, so a
    // restarted process (fresh ExtentStore over the same backing store) reclaims
    // a pre-existing orphan on its very first sweep. The cutoff is past the
    // orphan's mtime, as production's `now - age floor` is for a day-old orphan.
    #[tokio::test]
    async fn sweep_orphans_reclaims_an_old_orphan_after_restart() {
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
        let restarted = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8);
        let cutoff = Utc::now() + chrono::Duration::seconds(60);
        assert_eq!(
            restarted.sweep_orphans(cutoff).await.unwrap(),
            1,
            "a fresh process's first sweep must reclaim an old-enough orphan"
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

    #[tokio::test]
    async fn reclaim_deletes_a_dead_superseded_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Write + seal (segment A), then overwrite + seal (segment B): A is dead
        // (fully superseded).
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(deleted, 1, "the superseded segment must be reclaimed");
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);

        // The pass populated the Prometheus-bridged stats holder: the delete is
        // counted, and the scan's footprint gauges reflect the surviving segment.
        let m = store.segment_gc_stats();
        use std::sync::atomic::Ordering::Relaxed;
        assert!(m.passes.load(Relaxed) >= 1);
        assert_eq!(m.segments_deleted.load(Relaxed), 1);
        assert!(m.deleted_bytes.load(Relaxed) > 0);
        assert!(m.segment_count.load(Relaxed) >= 1);
        assert!(m.appended_bytes.load(Relaxed) > 0);
        assert!(m.live_bytes.load(Relaxed) > 0);

        // Live data still reads after GC, and a second reclaim is a no-op.
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
        assert_eq!(store.reclaim_segments(Utc::now(), None).await.unwrap().0, 0);
    }

    // Reclaiming a dead segment must debit its bytes and count from the gauges,
    // so freed space shows up without waiting for a re-seed. B is large and
    // fully live (above SMALL_SEGMENT_BYTES, 0% dead) so the pass compacts
    // nothing and only deletes the dead segment A.
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
        let m = store.segment_gc_stats();
        let appended_before = m.appended_bytes.load(Relaxed);
        assert_eq!(m.segment_count.load(Relaxed), 2);
        assert!(m.reclaimable_bytes.load(Relaxed) > 0, "segment A is dead");

        let (deleted, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(
            relocated, 0,
            "B is large and fully live: nothing to compact"
        );

        assert!(
            m.appended_bytes.load(Relaxed) < appended_before,
            "A's bytes debited"
        );
        assert_eq!(m.segment_count.load(Relaxed), 1);
        // Still consistent with a fresh scan of the post-reclaim state.
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, m.appended_bytes.load(Relaxed));
        assert_eq!(f.segment_count, m.segment_count.load(Relaxed));
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
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

        // First pass records a future delete horizon for A and holds it.
        let horizon = Utc::now() + chrono::Duration::milliseconds(80);
        assert_eq!(store.reclaim_segments(horizon, None).await.unwrap().0, 0);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Past the recorded horizon the next pass reclaims it (the new arg is
        // irrelevant — A keeps its first-seen deadline).
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(store.reclaim_segments(Utc::now(), None).await.unwrap().0, 1);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    #[tokio::test]
    async fn compaction_leaves_a_few_tiny_segments_alone() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A handful of tiny, fully-live segments: below the merge floor with no dead
        // space. Packing them would just produce an equally-tiny segment that gets
        // re-compacted next pass — pure churn — so the GC must leave them be.
        for i in 0..5u8 {
            let off = i as usize * EXTENT_SIZE;
            write_and_check(&store, &db, &mut model, off, &[i + 1; 100]).await;
            store.seal_open().await.unwrap();
        }
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 5);

        let (deleted, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            relocated, 0,
            "tiny dense segments are left alone, not churned"
        );
        assert_eq!(deleted, 0);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 5);
        assert_read_matches(&store, &model).await;
    }

    #[tokio::test]
    async fn compaction_packs_small_segments_once_they_clear_the_floor() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Four small (< 1 MiB) but incompressible segments that together exceed the
        // merge floor, so packing yields a segment above the small threshold.
        let seg_bytes = 28 * EXTENT_SIZE; // 896 KiB < SMALL_SEGMENT_BYTES
        let n = 4usize; // ~3.5 MiB total > MIN_FREED_BYTES
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

        // Pass 1 packs them; pass 2 reclaims the drained sources.
        let (_, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(relocated > 0, "accumulated small segments get packed");
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        let after = store.segments.list_segments().await.unwrap().len();
        assert!(
            after < before,
            "packed into fewer segments ({before} -> {after})"
        );
        assert_read_matches(&store, &model).await;

        // The packed output clears the small threshold, so a further pass is a no-op.
        let (_, relocated3) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            relocated3, 0,
            "packed output is not re-compacted (no churn)"
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

        // Any persistent-checkpoint pin (protect_before set) blocks both deletion
        // and compaction of S.
        let (deleted, relocated) = store
            .reclaim_segments(Utc::now(), Some(Utc::now() + chrono::Duration::minutes(5)))
            .await
            .unwrap();
        assert_eq!((deleted, relocated), (0, 0), "S is protected by the pin");
        assert!(!store.segments.list_segments().await.unwrap().is_empty());

        // Without the pin, the same dead segment is reclaimed.
        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(deleted >= 1, "S is reclaimed once no longer protected");
    }

    /// Dense segments (never scan candidates) split by a hot seam: the chain
    /// path must pack them together — but only once the seam is proven hot in
    /// two distinct passes AND the members are write-cold. This is the one
    /// path that creates candidacy, so it carries the full gate lifecycle.
    #[tokio::test]
    async fn hot_chain_packs_dense_segments_once_write_cold() {
        let (store, db) = make().await;
        store.enable_nominations();
        let per_seg = (SMALL_SEGMENT_BYTES as usize).div_ceil(EXTENT_SIZE) as u64 + 2;
        for seg in 0..2u64 {
            for e in 0..per_seg {
                let extent = seg * per_seg + e;
                write_extent(
                    &store,
                    &db,
                    extent,
                    &incompressible(extent as usize, EXTENT_SIZE),
                )
                .await;
            }
            store.seal_open().await.unwrap();
        }
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, per_seg).await.unwrap().segid;
        let whole = 2 * per_seg * EXTENT_SIZE as u64;

        // First pass over the seam: one episode, no chain.
        store.read(1, 0, whole).await.unwrap();
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        // Second episode in a later round: the seam is now hot — but the
        // members are freshly sealed (counter-young) and the store isn't
        // quiescent yet, so the write-cold gate must hold the chain back.
        store.read(1, 0, whole).await.unwrap();
        let (_, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(relocated, 0, "hot but not write-cold: chain held back");
        assert_eq!(frameloc_of(&store, &db, 1, 0).await.unwrap().segid, seg_a);

        // Idle wall-clock accumulates the quiescence proof (the negative
        // assert above used the default 5-minute window; the pack uses a short
        // test window it has already outlived). Once the store is provably
        // write-cold the chain packs, and both segments' data lands in one
        // output segment: the seam is gone.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                Duration::from_millis(500),
                || false,
            )
            .await
            .unwrap();
        assert_eq!(outcome.relocated as u64, 2 * per_seg, "chain packed");
        let now_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let now_b = frameloc_of(&store, &db, 1, per_seg).await.unwrap().segid;
        assert_ne!(now_a, seg_a);
        assert_ne!(now_b, seg_b);
        assert_eq!(now_a, now_b, "co-read data is co-located: no more seam");
        assert_read_matches(&store, &{
            let mut m = Vec::new();
            for extent in 0..2 * per_seg {
                m.extend_from_slice(&incompressible(extent as usize, EXTENT_SIZE));
            }
            m
        })
        .await;
    }

    /// Interleaved writes scatter two files' frames within one dense segment,
    /// so a whole-file read costs one ranged GET per frame despite touching a
    /// single object. A hot internal seam (self-pair) must trigger the 1:1
    /// repack that re-groups the frames — the intra-segment analog of a
    /// chain — under the same episode and write-cold gates.
    #[tokio::test]
    async fn hot_self_seam_repacks_scattered_segment() {
        let (store, db) = make().await;
        store.enable_nominations();
        // 17 extents per inode, interleaved, incompressible: the sealed
        // segment is dense (> SMALL_SEGMENT_BYTES, fully live) — never a
        // scan candidate, so only the self-seam path can fix it.
        let per_file = 17u64;
        let mut sizes = [0u64, 0u64];
        for extent in 0..per_file {
            for (i, inode) in [1u64, 2u64].into_iter().enumerate() {
                let mut txn = db.new_transaction().unwrap();
                let tu = store
                    .write(
                        &mut txn,
                        inode,
                        extent * EXTENT_SIZE as u64,
                        &Bytes::from(incompressible(
                            (inode * 1000 + extent) as usize,
                            EXTENT_SIZE,
                        )),
                        sizes[i],
                    )
                    .await
                    .unwrap();
                commit(&store, txn).await;
                store.apply_tail_update(inode, tu);
                sizes[i] = (extent + 1) * EXTENT_SIZE as u64;
            }
        }
        store.seal_open().await.unwrap();
        let s = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let len = per_file * EXTENT_SIZE as u64;

        // Scattered layout: the whole-file read is one GET per frame, and all
        // its internal seams collapse into one self-pair bump.
        let before = store.segments.read_calls();
        store.read(1, 0, len).await.unwrap();
        assert_eq!(store.segments.read_calls() - before, per_file);
        assert_eq!(
            store.pair_stats.lock().unwrap().map[&PairStats::key(s, s)].count,
            1
        );

        // Second episode in a later round arms the seam; the repack still
        // waits out the write-cold gate, then fires.
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        store.read(1, 0, len).await.unwrap();
        let (_, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(relocated, 0, "hot but not write-cold: held back");
        tokio::time::sleep(Duration::from_millis(600)).await;
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                Duration::from_millis(500),
                || false,
            )
            .await
            .unwrap();
        assert_eq!(outcome.relocated as u64, 2 * per_file, "1:1 repack ran");
        assert_ne!(frameloc_of(&store, &db, 1, 0).await.unwrap().segid, s);

        // Re-grouped: the file now reads back correct in a single GET.
        let before = store.segments.read_calls();
        let got = store.read(1, 0, len).await.unwrap();
        assert_eq!(store.segments.read_calls() - before, 1, "seam dissolved");
        let expect: Vec<u8> = (0..per_file)
            .flat_map(|e| incompressible((1000 + e) as usize, EXTENT_SIZE))
            .collect();
        assert_eq!(got.as_ref(), expect.as_slice());
    }

    #[tokio::test]
    async fn one_pass_of_reads_never_forges_chain_heat() {
        let (store, db) = make().await;
        store.enable_nominations();
        let per_seg = (SMALL_SEGMENT_BYTES as usize).div_ceil(EXTENT_SIZE) as u64 + 2;
        for seg in 0..2u64 {
            for e in 0..per_seg {
                let extent = seg * per_seg + e;
                write_extent(
                    &store,
                    &db,
                    extent,
                    &incompressible(extent as usize, EXTENT_SIZE),
                )
                .await;
            }
            store.seal_open().await.unwrap();
        }
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;

        // A single sequential pass (a backup) crosses the seam exactly once.
        // Even once the store is provably quiescent, count == 1 never chains.
        store
            .read(1, 0, 2 * per_seg * EXTENT_SIZE as u64)
            .await
            .unwrap();
        // First pass records the quiescence baseline; the sleep outlives the
        // short window, so the later passes run with quiescent == true.
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
        for _ in 0..3 {
            let outcome = store
                .reclaim_segments_tuned(
                    Utc::now(),
                    None,
                    Some(TAIL_SCRUB_DEFAULT_PERCENT),
                    Duration::from_millis(500),
                    || false,
                )
                .await
                .unwrap();
            assert_eq!(
                outcome.relocated, 0,
                "one-time scans must not trigger repacks"
            );
        }
        assert_eq!(frameloc_of(&store, &db, 1, 0).await.unwrap().segid, seg_a);
    }

    /// The tail scrub reclaims the band normal candidacy strands forever:
    /// mildly-dead, write-cold, dense segments. Gated on the floor and on
    /// write-cold, paid only from leftover budget, and independent of read
    /// nominations (never enabled here).
    #[tokio::test]
    async fn tail_scrub_reclaims_mildly_dead_write_cold_segments() {
        let (store, db) = make().await;
        // 75 incompressible extents -> one dense ~2.4 MiB segment S.
        let n = 75u64;
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
        let s = frameloc_of(&store, &db, 1, 20).await.unwrap().segid;
        // Overwrite 8 extents (~10.7% dead): S is neither fragmented nor
        // small, so the counter policy alone would strand its dead bytes.
        for extent in 0..8u64 {
            let mut txn = db.new_transaction().unwrap();
            let tu = store
                .write(
                    &mut txn,
                    1,
                    extent * EXTENT_SIZE as u64,
                    &Bytes::from(incompressible(9000 + extent as usize, EXTENT_SIZE)),
                    n * EXTENT_SIZE as u64,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
            store.apply_tail_update(1, tu);
        }
        store.seal_open().await.unwrap();

        // Records the quiescence baseline. Nothing compacts: the only
        // candidate is the tiny overwrite segment, below the economics gate,
        // and S is not yet provably write-cold.
        let (_, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(relocated, 0);
        tokio::time::sleep(Duration::from_millis(600)).await;

        // 10.7% dead is under a 15% floor: still stranded.
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(15),
                Duration::from_millis(500),
                || false,
            )
            .await
            .unwrap();
        assert_eq!(outcome.relocated, 0, "below the floor: not scrubbed");
        assert_eq!(frameloc_of(&store, &db, 1, 20).await.unwrap().segid, s);

        // None (config 0) disables the scrub outright.
        let outcome = store
            .reclaim_segments_tuned(Utc::now(), None, None, Duration::from_millis(500), || false)
            .await
            .unwrap();
        assert_eq!(outcome.relocated, 0, "scrub disabled: not scrubbed");
        assert_eq!(frameloc_of(&store, &db, 1, 20).await.unwrap().segid, s);

        // At the default floor the write-cold segment scrubs, together with
        // the small overwrite segment: all 75 live frames relocate.
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                Duration::from_millis(500),
                || false,
            )
            .await
            .unwrap();
        assert_eq!(outcome.relocated, 75, "tail scrub ran");
        assert_ne!(frameloc_of(&store, &db, 1, 20).await.unwrap().segid, s);
        assert_eq!(outcome.status, PassStatus::Completed { saturated: false });

        // Integrity: the file reads back with the overwrites applied.
        let mut expect = Vec::new();
        for extent in 0..n {
            let seed = if extent < 8 {
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

    /// A dead segment waiting out its delete horizon is not actionable
    /// backlog: reporting it as saturation would spin the fast cadence
    /// through ~a dozen no-op barrier passes after every deletion burst.
    #[tokio::test]
    async fn not_yet_due_dead_segments_do_not_saturate() {
        let (store, db) = make().await;
        write_extent(&store, &db, 0, &[1u8; EXTENT_SIZE]).await;
        store.seal_open().await.unwrap();
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                1,
                0,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        store.apply_tail_update(1, tu);
        store.seal_open().await.unwrap();

        let outcome = store
            .reclaim_segments_tuned(
                Utc::now() + chrono::Duration::hours(1),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                || false,
            )
            .await
            .unwrap();
        assert_eq!(outcome.deleted, 0);
        assert_eq!(outcome.status, PassStatus::Completed { saturated: false });
    }

    /// Selection cut by the round budget reports saturation — and stops
    /// reporting it once the leftovers no longer clear the economics gate
    /// (the fast-cadence spin guard).
    #[tokio::test]
    async fn budget_capped_selection_saturates_until_drained() {
        let (store, db) = make().await;
        let n = MAX_COMPACT_SEGMENTS_PER_ROUND as u64 + 2;
        for i in 0..n {
            write_extent(&store, &db, i, &incompressible(i as usize, EXTENT_SIZE)).await;
            store.seal_open().await.unwrap();
        }
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                || false,
            )
            .await
            .unwrap();
        assert!(outcome.relocated > 0);
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: true },
            "budget cut actionable work"
        );

        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                || false,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: false },
            "leftovers below the gate are not backlog"
        );
    }

    /// While `keep_going` approves, one pass drains back-to-back batches:
    /// two full round budgets compact in a single pass, and the loop stops on
    /// its own when the remainder no longer clears the economics gate — the
    /// spin guard doubling as the terminator.
    #[tokio::test]
    async fn idle_multi_batch_pass_drains_beyond_one_round_budget() {
        let (store, db) = make().await;
        let n = 2 * MAX_COMPACT_SEGMENTS_PER_ROUND as u64 + 4;
        for i in 0..n {
            write_extent(&store, &db, i, &incompressible(i as usize, EXTENT_SIZE)).await;
            store.seal_open().await.unwrap();
        }
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                || true,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.relocated,
            2 * MAX_COMPACT_SEGMENTS_PER_ROUND,
            "two full batches in one pass"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: false },
            "the gate-declined remainder is not backlog"
        );
    }

    /// A callback cut mid-drain ends the pass within one batch and reports
    /// the remaining work as saturated, so the outer cadence follows up.
    #[tokio::test]
    async fn callback_cut_ends_batching_and_stays_saturated() {
        let (store, db) = make().await;
        let n = MAX_COMPACT_SEGMENTS_PER_ROUND as u64 + 2;
        for i in 0..n {
            write_extent(&store, &db, i, &incompressible(i as usize, EXTENT_SIZE)).await;
            store.seal_open().await.unwrap();
        }
        let calls = std::cell::Cell::new(0u32);
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                || {
                    calls.set(calls.get() + 1);
                    false
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome.relocated, MAX_COMPACT_SEGMENTS_PER_ROUND);
        assert_eq!(
            calls.get(),
            1,
            "polled once, after the first saturated batch"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: true },
            "a cut with work remaining stays saturated"
        );
    }

    /// Throughput floor: under load (keep_going reporting the store busy past
    /// the floor, as the production closure does when idle_since is false), a
    /// pass still drains exactly `floor` batches, then stops with the remainder
    /// saturated. Driven through reclaim_segments_gated directly, since
    /// reclaim_segments_tuned discards the batch count.
    #[tokio::test]
    async fn throughput_floor_drains_floor_batches_under_load() {
        const FLOOR: usize = 2;
        let (store, db) = make().await;
        // More than FLOOR full (slot-capped) batches of gate-clearing work.
        let n = 3 * MAX_COMPACT_SEGMENTS_PER_ROUND as u64;
        for i in 0..n {
            write_extent(&store, &db, i, &incompressible(i as usize, EXTENT_SIZE)).await;
            store.seal_open().await.unwrap();
        }
        let polls = std::cell::Cell::new(0u32);
        let outcome = store
            .reclaim_segments_gated(
                || std::future::ready(Ok(Some((Utc::now(), None::<DateTime<Utc>>)))),
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                |batches| {
                    polls.set(polls.get() + 1);
                    batches < FLOOR // below the floor push through; at it, "busy" stops us
                },
                MAX_COMPACT_BYTES_PER_ROUND,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.relocated,
            FLOOR * MAX_COMPACT_SEGMENTS_PER_ROUND,
            "exactly FLOOR batches ran before the load gate stopped the drain"
        );
        assert_eq!(
            polls.get(),
            FLOOR as u32,
            "polled once per completed batch, stopping at the floor"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: true },
            "a full batch remained past the floor, so the pass stays saturated"
        );
    }

    #[tokio::test]
    async fn nominated_candidates_win_selection_under_budget_pressure() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Two more identical small fully-live segments (two incompressible
        // extents each) than one round's segment budget. All tie on the live
        // fraction, so the stable ranking keeps scan (counter) order and the
        // last two would never make the cut on their own.
        let n = MAX_COMPACT_SEGMENTS_PER_ROUND + 2;
        let mut segids = Vec::new();
        for i in 0..n as u64 {
            for e in 0..2u64 {
                let extent = i * 2 + e;
                write_extent(
                    &store,
                    &db,
                    extent,
                    &incompressible(extent as usize, EXTENT_SIZE),
                )
                .await;
            }
            store.seal_open().await.unwrap();
            segids.push(frameloc_of(&store, &db, 1, i * 2).await.unwrap().segid);
        }

        // One read fans out across the two youngest segments -> nominated.
        let (last, prev) = (n as u64 - 1, n as u64 - 2);
        store
            .read(1, prev * 2 * EXTENT_SIZE as u64, 4 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert_eq!(store.nominations.lock().unwrap().set.len(), 2);

        // The round compacts a full budget of segments, and the nominated two
        // displace the two the ranking would otherwise have kept (the oldest
        // 62 tie ahead of them either way).
        let (_, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(relocated, 2 * MAX_COMPACT_SEGMENTS_PER_ROUND);
        assert!(store.nominations.lock().unwrap().set.is_empty());
        for hot in [last, prev] {
            let now = frameloc_of(&store, &db, 1, hot * 2).await.unwrap().segid;
            assert_ne!(now, segids[hot as usize], "nominated segment was packed");
        }
        for cold in [prev - 1, prev - 2] {
            let now = frameloc_of(&store, &db, 1, cold * 2).await.unwrap().segid;
            assert_eq!(
                now, segids[cold as usize],
                "displaced by the nominated pair"
            );
        }
    }

    #[tokio::test]
    async fn pinned_pass_leaves_nominations_queued_and_stale_ones_drop() {
        let (store, db) = make().await;
        store.enable_nominations();
        for extent in 0..2u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
            store.seal_open().await.unwrap();
        }
        store.read(1, 0, 2 * EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(store.nominations.lock().unwrap().set.len(), 2);

        // A persistent-checkpoint pin classifies nothing and must leave the
        // nominations queued for a later, unpinned pass.
        store
            .reclaim_segments(Utc::now(), Some(Utc::now() + chrono::Duration::minutes(5)))
            .await
            .unwrap();
        assert_eq!(store.nominations.lock().unwrap().set.len(), 2);

        // Stale nominations — a counter-less segid and one from a dead epoch —
        // never appear in the segcount scan, so the pass drains and drops them
        // without erroring.
        {
            let mut noms = store.nominations.lock().unwrap();
            noms.push(Segid::new(3, 5));
            noms.push(Segid::new(7, 999_999));
        }
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());
    }

    #[tokio::test]
    async fn nominations_never_create_candidacy() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Two dense, fully-live segments: never candidates. A fanned-out
        // read nominates them; the pass must still compact nothing —
        // nominations prioritize existing work, never create it.
        let per_seg = (SMALL_SEGMENT_BYTES as usize).div_ceil(EXTENT_SIZE) as u64 + 2;
        for seg in 0..2u64 {
            for e in 0..per_seg {
                let extent = seg * per_seg + e;
                write_extent(
                    &store,
                    &db,
                    extent,
                    &incompressible(extent as usize, EXTENT_SIZE),
                )
                .await;
            }
            store.seal_open().await.unwrap();
        }
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, per_seg).await.unwrap().segid;

        store
            .read(1, 0, 2 * per_seg * EXTENT_SIZE as u64)
            .await
            .unwrap();
        {
            let noms = store.nominations.lock().unwrap();
            assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
        }

        let (deleted, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!((deleted, relocated), (0, 0), "dense segments stay put");
        assert_eq!(frameloc_of(&store, &db, 1, 0).await.unwrap().segid, seg_a);
        assert_eq!(
            frameloc_of(&store, &db, 1, per_seg).await.unwrap().segid,
            seg_b
        );
        assert!(
            store.nominations.lock().unwrap().set.is_empty(),
            "drained, not re-queued"
        );
    }

    #[tokio::test]
    async fn reserve_leaves_half_the_round_to_scan_candidates() {
        let (store, db) = make().await;
        store.enable_nominations();
        // 40 read-hot tiny segments (one fully-live extent each) plus 30
        // colder but far more fragmented ones (three extents, two
        // overwritten). Pure nominated-first selection would spend 40 of the
        // 64-segment round on heat and strand part of the fragmented backlog;
        // the reserve caps heat at 32, so every fragmented segment packs.
        let hot_n = 40u64;
        for extent in 0..hot_n {
            write_extent(
                &store,
                &db,
                extent,
                &incompressible(extent as usize, EXTENT_SIZE),
            )
            .await;
            store.seal_open().await.unwrap();
        }
        let mut hot = Vec::new();
        for extent in 0..hot_n {
            hot.push(frameloc_of(&store, &db, 1, extent).await.unwrap().segid);
        }
        let cold_n = 30u64;
        let mut cold = Vec::new();
        for i in 0..cold_n {
            let base = hot_n + i * 3;
            for e in 0..3 {
                write_extent(
                    &store,
                    &db,
                    base + e,
                    &incompressible((base + e) as usize, EXTENT_SIZE),
                )
                .await;
            }
            store.seal_open().await.unwrap();
            cold.push(frameloc_of(&store, &db, 1, base).await.unwrap().segid);
        }
        // Overwrite two of each cold segment's three extents (the replacement
        // frames seal into one dense, non-candidate segment), leaving the
        // cold segments 1/3 live — ranked well ahead of the fully-live hot ones.
        let file_size = (hot_n + cold_n * 3) * EXTENT_SIZE as u64;
        for i in 0..cold_n {
            let base = hot_n + i * 3;
            for e in 1..3 {
                let extent = base + e;
                let mut txn = db.new_transaction().unwrap();
                let tu = store
                    .write(
                        &mut txn,
                        1,
                        extent * EXTENT_SIZE as u64,
                        &Bytes::from(incompressible(7000 + extent as usize, EXTENT_SIZE)),
                        file_size,
                    )
                    .await
                    .unwrap();
                commit(&store, txn).await;
                store.apply_tail_update(1, tu);
            }
        }
        store.seal_open().await.unwrap();

        // Nominate all 40 hot segments (the per-call cap forces three reads).
        for start in [0u64, 16, 32] {
            let len = (hot_n - start).min(NOMINATE_PER_CALL_CAP as u64);
            store
                .read(1, start * EXTENT_SIZE as u64, len * EXTENT_SIZE as u64)
                .await
                .unwrap();
        }
        assert_eq!(store.nominations.lock().unwrap().set.len(), hot_n as usize);

        store.reclaim_segments(Utc::now(), None).await.unwrap();

        // Every fragmented segment made the round (its live extent moved)…
        for (i, segid) in cold.iter().enumerate() {
            let extent = hot_n + i as u64 * 3;
            assert_ne!(
                frameloc_of(&store, &db, 1, extent).await.unwrap().segid,
                *segid,
                "fragmented backlog must not be starved by nominations"
            );
        }
        // …while heat got its 32-slot reserve plus the 2 leftover round slots
        // it wins on rank: 64 selected = 32 reserved hot + 30 fragmented + 2.
        let mut moved_hot = 0;
        for (i, segid) in hot.iter().enumerate() {
            if frameloc_of(&store, &db, 1, i as u64).await.unwrap().segid != *segid {
                moved_hot += 1;
            }
        }
        assert_eq!(moved_hot, 34);
    }
}
