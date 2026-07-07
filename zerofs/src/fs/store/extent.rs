//! Persistent extent data store.
//!
//! The per-extent `b"extent"` key holds a small [`FrameLoc`] pointer; the extent
//! bytes themselves live outside the LSM, in immutable `segments/` objects
//! written via [`SegmentStore`]. Writes are read-modify-write over full extents,
//! with sparse holes, all-zero elision, and a tail cache for sequential appends.
//!
//! Writes append sealed frames to an in-RAM open segment and commit the extent
//! pointer eagerly (no PUT on the write path). The open segment is PUT in the
//! background when it crosses a size threshold, and synchronously by the flush
//! path (the fsync barrier) before the metadata it references is made durable —
//! so a durable manifest never points at an un-PUT segment.

use crate::db::{Db, Transaction};
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::frame_codec::FrameCodec;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::metrics::{SegmentFootprint, SegmentGcPass, SegmentGcStats};
use crate::fs::{EXTENT_SIZE, FsError};
use crate::replication::ReplOp;
use crate::segment::{DirEntry, FrameLoc, Segid};
use crate::segment_store::{SegmentStore, SegmentStoreError};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use foyer::{Cache, CacheBuilder};
use futures::stream::{self, StreamExt, TryStreamExt};
use slatedb::config::WriteOptions;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

const PARALLEL_EXTENT_OPS: usize = 20;

/// Frames per write batch before pre-compression fans out on rayon; below
/// this the dispatch overhead outweighs the parallelism.
const PARALLEL_COMPRESS_MIN_FRAMES: usize = 8;
const ZERO_EXTENT: &[u8] = &[0u8; EXTENT_SIZE];
const TAIL_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Logical (file-offset) read-ahead distance for a confirmed-sequential
/// stream. Follows the file's extents across segments, which the physical
/// (per-object) prefetcher can't do.
const READ_AHEAD_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
/// Consecutive sequential reads before prefetch kicks in, so a one-off read
/// doesn't drag in a whole window.
const READ_AHEAD_MIN_SEQ: u32 = 2;
/// Global cap on concurrent in-flight read-ahead fetches.
const READ_AHEAD_MAX_CONCURRENT: usize = 16;
/// Bound on the per-inode read-ahead state map (LRU-evicted; ~24 B/entry).
const READ_AHEAD_TRACK_BYTES: usize = 4 * 1024 * 1024;

/// Given the per-inode read-ahead state `(last_read_end, prefetched_to, seq_run)`
/// and a read of `[offset, offset+length)`, return the new state and, when a
/// prefetch is warranted, the `[start, end)` file range to fetch.
fn plan_read_ahead(
    (last_end, prefetched_to, seq): (u64, u64, u32),
    offset: u64,
    length: u64,
) -> ((u64, u64, u32), Option<(u64, u64)>) {
    let read_end = offset + length;
    // A jump starts a new (unconfirmed) sequence.
    if offset != last_end {
        return ((read_end, read_end, 1), None);
    }
    let seq = seq.saturating_add(1);
    if seq < READ_AHEAD_MIN_SEQ {
        return ((read_end, read_end, seq), None);
    }
    // Refill only once less than half a window remains ahead: few large
    // fetches, not one tiny fetch per read.
    let ahead = prefetched_to.saturating_sub(read_end);
    if ahead >= READ_AHEAD_WINDOW_BYTES / 2 {
        return ((read_end, prefetched_to, seq), None);
    }
    let target = read_end + READ_AHEAD_WINDOW_BYTES;
    let start = prefetched_to.max(read_end);
    ((read_end, target, seq), Some((start, target)))
}

/// Seal (PUT) the open segment once its packed frames reach this size, bounding
/// the in-RAM buffer between flushes. The seal PUT is concurrent multipart
/// (`SegmentStore::put_segment`), so its fsync-path latency stays bounded
/// despite the size.
const SEAL_THRESHOLD: usize = 256 * 1024 * 1024;

/// Max segments sealing (PUT in flight) concurrently. Bounds the un-PUT RAM in
/// `sealing` to ~this × SEAL_THRESHOLD; acquiring all permits is the fsync
/// drain barrier.
const MAX_INFLIGHT_SEALS: usize = 4;

/// Compaction thresholds. A live segment is a candidate when *fragmented*
/// (live bytes below this percent of total) or *small* (below
/// SMALL_SEGMENT_BYTES). Dense, full-size segments are left alone: re-sealing
/// one 1:1 reclaims nothing.
const FRAG_LIVE_PERCENT: u64 = 50;
const SMALL_SEGMENT_BYTES: u64 = 1 << 20; // 1 MiB

/// Live frames evacuated from candidates are repacked into segments of this
/// target size, in source on-store bytes — the unit candidacy and the
/// selection budget use. Plaintext cuts would split a compressible chain
/// across outputs, manufacturing a fresh seam for the same reads to re-heat.
/// The compaction sealer is one object per batch (no auto-split), so an
/// output may exceed this only by re-framing delta plus a folded sliver.
const PACK_TARGET_BYTES: u64 = SEAL_THRESHOLD as u64;

/// How far a batch cut may back off the target to land on a file-adjacency
/// break instead of splitting a contiguous run (see [`plan_batches`]).
const PACK_CUT_SLACK_BYTES: u64 = 32 << 20;

/// Per-round compaction bounds, so a large backlog is worked down incrementally.
const MAX_COMPACT_SEGMENTS_PER_ROUND: usize = 64;
const MAX_COMPACT_BYTES_PER_ROUND: u64 = 256 << 20; // 256 MiB (~one PACK_TARGET segment/round)

/// A compaction round must free at least this much dead space to run (unless
/// enough small-segment live bytes have accumulated to pack a dense segment;
/// see `reclaim_segments_gated`). A near-1:1 repack is churn.
const MIN_FREED_BYTES: u64 = SMALL_SEGMENT_BYTES;

/// Cap on buffered compaction candidates, so a huge store can't make the list
/// O(#segments). When reached, the most-fragmented half is kept.
const MAX_CANDIDATES_BUFFERED: usize = 8192;

/// Distinct on-store segments a demand read must span to nominate them.
/// Nominations re-order selection among scan-qualified candidates; they
/// never create candidacy.
const NOMINATE_MIN_FANOUT: usize = 2;
/// Per-read nomination cap. One fragmented read must not evict the whole set.
const NOMINATE_PER_CALL_CAP: usize = 16;
/// Nomination set bound; oldest evicted.
const NOMINATION_SET_CAP: usize = 1024;
/// Heat-reserve slot cap (nominations + chains): at most half a round.
/// Nominations skew high-live; unreserved they would starve the most-dead
/// candidates. The byte half of the reserve is `round_bytes / 2`, runtime since
/// the round budget is configurable (`compact_round_max_mib`).
const NOMINATED_MAX_PER_ROUND: usize = MAX_COMPACT_SEGMENTS_PER_ROUND / 2;

/// Distinct passes a seam must be crossed in to become hot. Pair-keyed and
/// once-per-pass: a one-time sequential scan crosses each boundary once and
/// cannot forge heat.
const PAIR_HOT_MIN: u32 = 2;
/// Heat lifetime; a bump past the gap restarts the count. Wall clock, not
/// passes — the pass rate is variable.
const PAIR_STALE_AFTER: Duration = Duration::from_secs(30 * 60);
/// Pair-table bound; at capacity new pairs are dropped (the stale sweep
/// frees space, the next crossing retries).
const PAIR_STATS_CAP: usize = 4096;
/// Crossing bumps per read call (same rationale as NOMINATE_PER_CALL_CAP).
const PAIR_BUMPS_PER_CALL: usize = 16;
/// Write-cold counter distance. Counters advance ~once per flush, so
/// distance approximates write recency; a packed output carries a fresh
/// counter, making the same rule the anti-churn cooldown.
const CHAIN_OLD_COUNTER_DISTANCE: u64 = 30;
/// Store-wide write-cold proof: open counter unchanged this long ⇒ no writes
/// anywhere. Covers write-then-read-only stores whose counters never age.
/// Monotonic Instant (NTP must not fake it); a duration, so fast passes
/// cannot compress the proof. GcTuning default and test-wrapper value.
pub(crate) const QUIESCENT_AFTER_DEFAULT: Duration = Duration::from_secs(5 * 60);

/// Tail-scrub floor for the config-less wrapper; aliased so the literal
/// exists once.
const TAIL_SCRUB_DEFAULT_PERCENT: u64 =
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

/// A coalesced run of contiguous live frames within one source segment, for the
/// compaction gather: (byte_offset, total byte_len, first frame index, the
/// (inode, extent) slots it covers). Read back in one ranged GET.
type CompactRun = (u64, u32, u32, Vec<(InodeId, u64)>);

/// A segment as the segcount scan saw it: (segid, total bytes, live bytes).
type SegStat = (Segid, u64, u64);

/// One packed batch awaiting seal: frames and their gathered source locations.
type PackBatch = (Vec<(InodeId, u64, Bytes)>, Vec<FrameLoc>);

/// Write-cold inputs, computed once per pass after the barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ColdCtx {
    cur_epoch: u64,
    cutoff: u64,
    quiescent: bool,
}

impl ColdCtx {
    /// Counter distance is valid only within the current epoch (counters
    /// restart per open, and a restart is no evidence of write-coldness);
    /// prior-epoch segments qualify only through quiescence.
    fn write_cold(&self, s: &Segid) -> bool {
        (s.epoch == self.cur_epoch
            && self.cutoff.saturating_sub(s.counter) >= CHAIN_OLD_COUNTER_DISTANCE)
            || self.quiescent
    }
}

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

/// Human-readable byte size for log lines, e.g. "3.1 GiB". Display-only.
pub(crate) fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// What a `write` leaves for the tail cache. Applied by the caller only after
/// the transaction commits, so the cache never runs ahead of durable state.
pub enum TailUpdate {
    Set { extent_idx: u64, data: Bytes },
    Clear,
    Keep,
}

/// The in-RAM open segment. Frames are sealed (compressed+encrypted) and appended
/// here at write time, so an extent's location is known and committed eagerly;
/// the segment object is PUT only on flush or when it crosses [`SEAL_THRESHOLD`].
struct OpenSegment {
    segid: Segid,
    buf: Vec<u8>,
    dir: Vec<DirEntry>,
}

/// Read-nominated compaction hints: bounded, deduping, insertion-ordered.
#[derive(Default)]
struct NominationSet {
    set: HashSet<Segid>,
    order: VecDeque<Segid>,
    /// Evictions since the last drain, for the pass summary.
    dropped: u64,
}

impl NominationSet {
    /// Deduped insert; re-nominating keeps the original queue position. At
    /// capacity the oldest nomination is evicted.
    fn push(&mut self, segid: Segid) {
        if !self.set.insert(segid) {
            return;
        }
        self.order.push_back(segid);
        if self.order.len() > NOMINATION_SET_CAP
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
            self.dropped += 1;
        }
    }

    /// Take everything; unselected segids are re-nominated by future reads.
    fn drain(&mut self) -> (HashSet<Segid>, u64) {
        self.order.clear();
        (
            std::mem::take(&mut self.set),
            std::mem::take(&mut self.dropped),
        )
    }
}

/// `last_round` dedups episodes per pass (fast passes un only without reads,
//  so they cannot mint episodes); `last_seen` drives
/// staleness, a duration that must not compress with cadence.
struct PairStat {
    count: u32,
    last_round: u64,
    last_seen: Instant,
}

/// Crossing-pair statistics. A self-pair is an internal seam (adjacent
/// extents scattered within one segment).
#[derive(Default)]
struct PairStats {
    map: HashMap<(Segid, Segid), PairStat>,
    /// Pairs dropped at capacity since the last sweep, for the pass summary.
    dropped: u64,
}

impl PairStats {
    /// Pairs are unordered.
    fn key(a: Segid, b: Segid) -> (Segid, Segid) {
        if (b.epoch, b.counter) < (a.epoch, a.counter) {
            (b, a)
        } else {
            (a, b)
        }
    }

    fn bump(&mut self, a: Segid, b: Segid, round: u64, now: Instant) {
        let key = Self::key(a, b);
        if let Some(s) = self.map.get_mut(&key) {
            if round == s.last_round {
                // Once per pass; freshness still updates, or an interval
                // >= PAIR_STALE_AFTER could never reach two episodes.
                s.last_seen = now;
                return;
            }
            s.count = if now.saturating_duration_since(s.last_seen) > PAIR_STALE_AFTER {
                1 // stale: restart, don't resume
            } else {
                s.count.saturating_add(1)
            };
            s.last_round = round;
            s.last_seen = now;
        } else if self.map.len() >= PAIR_STATS_CAP {
            self.dropped += 1;
        } else {
            self.map.insert(
                key,
                PairStat {
                    count: 1,
                    last_round: round,
                    last_seen: now,
                },
            );
        }
    }

    /// Drop stale entries; return hot pairs.
    fn sweep_and_hot(&mut self, now: Instant) -> (Vec<(Segid, Segid)>, u64) {
        self.map
            .retain(|_, s| now.saturating_duration_since(s.last_seen) <= PAIR_STALE_AFTER);
        let hot = self
            .map
            .iter()
            .filter(|(_, s)| s.count >= PAIR_HOT_MIN)
            .map(|(k, _)| *k)
            .collect();
        (hot, std::mem::take(&mut self.dropped))
    }
}

/// Live fraction in permille, the fragmentation rank key (lower = more dead).
fn live_permille(live: u64, total: u64) -> u64 {
    live.saturating_mul(1000) / total.max(1)
}

/// A hot-seam connected component: ordered members plus every input hot
/// pair between two members, as index pairs into `members`.
#[derive(Debug, Clone, PartialEq)]
struct ChainComponent {
    members: Vec<Segid>,
    edges: Vec<(usize, usize)>,
}

/// Union hot pairs into connected components.
fn chain_components(hot_pairs: &[(Segid, Segid)]) -> Vec<ChainComponent> {
    let key = |s: &Segid| (s.epoch, s.counter);
    let mut adj: HashMap<Segid, Vec<Segid>> = HashMap::new();
    for &(a, b) in hot_pairs {
        adj.entry(a).or_default().push(b);
        adj.entry(b).or_default().push(a);
    }
    for nbrs in adj.values_mut() {
        nbrs.sort_by_key(key);
        nbrs.dedup();
    }
    let mut nodes: Vec<Segid> = adj.keys().copied().collect();
    nodes.sort_by_key(key);
    let mut visited: HashSet<Segid> = HashSet::new();
    let mut components: Vec<Vec<Segid>> = Vec::new();
    for &start in &nodes {
        if !visited.insert(start) {
            continue;
        }
        let mut members = vec![start];
        let mut queue = VecDeque::from([start]);
        while let Some(n) = queue.pop_front() {
            for &m in &adj[&n] {
                if visited.insert(m) {
                    members.push(m);
                    queue.push_back(m);
                }
            }
        }
        components.push(members);
    }
    let mut index_of: HashMap<Segid, (usize, usize)> = HashMap::new();
    for (ci, members) in components.iter().enumerate() {
        for (mi, &s) in members.iter().enumerate() {
            index_of.insert(s, (ci, mi));
        }
    }
    let mut edges: Vec<Vec<(usize, usize)>> = vec![Vec::new(); components.len()];
    for &(a, b) in hot_pairs {
        let (ci, ia) = index_of[&a];
        let (_, ib) = index_of[&b]; // same component by construction
        edges[ci].push((ia.min(ib), ia.max(ib)));
    }
    for e in &mut edges {
        e.sort_unstable();
        e.dedup();
    }
    components
        .into_iter()
        .zip(edges)
        .map(|(members, edges)| ChainComponent { members, edges })
        .collect()
}

/// Batch-cut plan for repacking gathered extents: over `(inode, extent
/// index, source store bytes)` metas, return end-exclusive batch boundaries
/// (strictly increasing, last == len).
///
/// Fills greedily to `target` store bytes, then backtracks up to `slack`
/// bytes to the latest file-adjacency break; if none, defers the whole
/// containing run when it fits a batch by itself.
fn plan_batches(metas: &[(InodeId, u64, u64)], target: u64, slack: u64) -> Vec<usize> {
    let n = metas.len();
    let mut bounds = Vec::new();
    // Whether metas[k - 1] and metas[k] are file-adjacent.
    let adj = |k: usize| {
        let (ia, ea, _) = metas[k - 1];
        let (ib, eb, _) = metas[k];
        ia == ib && ea.checked_add(1) == Some(eb)
    };
    let mut start = 0usize;
    while start < n {
        let mut sum = 0u64;
        let mut i = start;
        while i < n && (i == start || sum.saturating_add(metas[i].2) <= target) {
            sum = sum.saturating_add(metas[i].2);
            i += 1;
        }
        if i == n {
            bounds.push(n);
            break;
        }
        // metas[i] no longer fits. Latest break within the slack window wins.
        let mut cut = None;
        let mut deferred = 0u64;
        let mut j = i;
        while j > start {
            if !adj(j) {
                cut = Some(j);
                break;
            }
            deferred = deferred.saturating_add(metas[j - 1].2);
            if deferred > slack {
                break;
            }
            j -= 1;
        }
        let cut = cut.unwrap_or_else(|| {
            let mut r = i;
            while r > 0 && adj(r) {
                r -= 1;
            }
            let mut e = i + 1;
            while e < n && adj(e) {
                e += 1;
            }
            let run: u64 = metas[r..e].iter().map(|m| m.2).sum();
            // A run that fits a batch defers whole (underfilling here beats
            // a manufactured seam); one that can't fit anywhere hard-cuts.
            if r > start && run <= target { r } else { i }
        });
        bounds.push(cut);
        start = cut;
    }
    bounds
}

/// One reclaim pass's compaction pick (see [`select_round`]).
#[derive(Debug, PartialEq)]
struct RoundSelection {
    selected: Vec<Segid>,
    sel_size: u64,
    sel_live: u64,
    /// Reserve accounting: selections admitted ahead of the fragmentation
    /// ranking (chains + nominated candidates), capped at half the round.
    reserve_slots: usize,
    reserve_live: u64,
    nominated_selected: usize,
    /// Admitted chain groups' member counts, in selection order. Chain
    /// admissions are the leading prefix of `selected`, so this is also
    /// `compact_segments`' atomic-group prefix. A group is *packed* only
    /// once the gather consumes all its members.
    chain_groups: Vec<usize>,
    /// Tail-scrub admissions for the pass summary.
    tail_selected: usize,
    /// A budget cap cut actionable work. Counts toward
    /// saturation only when the gate fires and declined leftovers recur
    /// identically.
    saturated: bool,
    /// Chains skipped, by cause, for the pass summary. Only `deferred`
    /// (reserve contention; a fresh pass retries it) saturates — tracked
    /// apart from `saturated` since chains ride batch 1 only and later
    /// batches must not erase it. `warm` retries once write-cold;
    /// `unpackable` (cheapest pair over even a fresh reserve) is permanent
    /// and must not pin the fast cadence.
    chains_deferred: usize,
    chains_warm: usize,
    chains_unpackable: usize,
}

impl RoundSelection {
    fn admit(&mut self, segid: Segid, size: u64, live_b: u64) {
        self.selected.push(segid);
        self.sel_size += size;
        self.sel_live += live_b;
    }
}

/// A chain component dressed with the scan's stats, the pass-0 selection
/// input: members in [`chain_components`] order, edges as member-index
/// pairs.
#[derive(Debug, Clone, PartialEq)]
struct HotChain {
    members: Vec<SegStat>,
    edges: Vec<(usize, usize)>,
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

/// Selection policy for one batch, store-free so its invariants are
/// property-testable.
///
/// Input: `candidates` are scan-qualified (fragmented || small), each
/// segid once, live in 1..=total; `chains` are ordered member lists built
/// from scan-seen-live hot pairs (see [`chain_components`]) carrying the
/// scan's stats.
///
/// Pass 0: hot chains — the only path that may select segments candidacy
/// skipped.
///
/// Pass 1: nominated candidates take the remaining reserve. Byte admission
/// is a fit test: two ~127 MiB-live candidates must not claim the round.
///
/// Pass 2: the rest, most-fragmented-first. The unreserved half keeps heat
/// (skewed high-live) from starving the highest-yield reclaim.
///
/// Pass 3: tail scrub on leftover budget: write-cold, > floor dead, not
/// otherwise candidates.
fn select_round(
    mut candidates: Vec<SegStat>,
    nominated: &HashSet<Segid>,
    chains: &[HotChain],
    mut tail: Vec<SegStat>,
    tail_min_dead_percent: Option<u64>,
    ctx: ColdCtx,
    // Per-round live-byte budget (config `compact_round_max_mib`); the heat
    // reserve is half. Raising it lets over-reserve seams pack, at proportional
    // gather RAM. Slot caps stay fixed.
    round_bytes: u64,
) -> RoundSelection {
    let reserve_bytes = round_bytes / 2;
    candidates.sort_by_key(|&(_, size, live_b)| live_permille(live_b, size));
    let mut sel = RoundSelection {
        selected: Vec::new(),
        sel_size: 0,
        sel_live: 0,
        reserve_slots: 0,
        reserve_live: 0,
        nominated_selected: 0,
        chain_groups: Vec::new(),
        tail_selected: 0,
        saturated: false,
        chains_deferred: 0,
        chains_warm: 0,
        chains_unpackable: 0,
    };
    let write_cold = |s: &Segid| ctx.write_cold(s);
    let mut selected_set: HashSet<Segid> = HashSet::new();
    let admit_reserved = |sel: &mut RoundSelection, segid: Segid, size: u64, live_b: u64| {
        sel.reserve_slots += 1;
        sel.reserve_live += live_b;
        sel.nominated_selected += nominated.contains(&segid) as usize;
        sel.admit(segid, size, live_b);
    };

    // Pass 0: chains, in the caller's order (prefix admission relies on it).
    for chain in chains {
        let members = &chain.members;
        if !members.iter().all(|(s, _, _)| write_cold(s)) {
            sel.chains_warm += 1;
            continue;
        }
        let chain_live: u64 = members.iter().map(|(_, _, l)| l).sum();
        let fits_alone = members.len() <= NOMINATED_MAX_PER_ROUND && chain_live <= reserve_bytes;
        let fits_now = sel.reserve_slots + members.len() <= NOMINATED_MAX_PER_ROUND
            && sel.reserve_live + chain_live <= reserve_bytes;
        if !fits_now && fits_alone {
            sel.chains_deferred += 1; // whole chain waits for a freer pass
            continue;
        }
        let mut fit = 0usize;
        let (mut fit_slots, mut fit_live) = (sel.reserve_slots, sel.reserve_live);
        for &(_, _, live_b) in members {
            if fit_slots >= NOMINATED_MAX_PER_ROUND || fit_live + live_b > reserve_bytes {
                break;
            }
            fit_slots += 1;
            fit_live += live_b;
            fit += 1;
        }
        let pair_buf;
        let group: &[SegStat] = if fit >= 2 || (members.len() == 1 && fit == 1) {
            &members[..fit]
        } else {
            // A sub-2 prefix dissolves no seam: its ~1:1 repack re-heats
            // against the fresh output and loops forever on a quiescent
            // store. Fall back to the cheapest edge pair; the rest of the
            // component re-forms from later reads. (A self-seam's 1:1 repack
            // does dissolve its scatter and has no edge pair — hence the
            // single-member arm above.)
            let Some(&(a, b)) = chain
                .edges
                .iter()
                .filter(|&&(a, b)| a != b)
                .min_by_key(|&&(a, b)| (members[a].2 + members[b].2, a, b))
            else {
                // Single member over even a fresh reserve (contention was
                // handled by the fits-alone arm above).
                sel.chains_unpackable += 1;
                continue;
            };
            let pair_live = members[a].2 + members[b].2;
            if sel.reserve_slots + 2 > NOMINATED_MAX_PER_ROUND
                || sel.reserve_live + pair_live > reserve_bytes
            {
                // Squeezed out by reserve contention: retry under a fresh
                // reserve. A pair too big even for a fresh reserve is
                // unpackable at this round budget (raise compact_round_max_mib).
                if pair_live <= reserve_bytes {
                    sel.chains_deferred += 1;
                } else {
                    sel.chains_unpackable += 1;
                }
                continue;
            }
            pair_buf = [members[a], members[b]];
            &pair_buf
        };
        let mut admitted = 0usize;
        for &(segid, size, live_b) in group {
            if !selected_set.insert(segid) {
                continue;
            }
            admit_reserved(&mut sel, segid, size, live_b);
            admitted += 1;
        }
        if admitted > 0 {
            sel.chain_groups.push(admitted);
        }
    }

    // Pass 1: nominated candidates onto the remaining reserve.
    let mut rest: Vec<SegStat> = Vec::new();
    for cand in candidates {
        let (segid, size, live_b) = cand;
        if selected_set.contains(&segid) {
            continue; // already selected by a chain this pass
        }
        if !(nominated.contains(&segid)
            && sel.reserve_slots < NOMINATED_MAX_PER_ROUND
            && sel.reserve_live + live_b <= reserve_bytes)
        {
            rest.push(cand);
            continue;
        }
        selected_set.insert(segid);
        admit_reserved(&mut sel, segid, size, live_b);
    }

    // Pass 2: fragmentation rank fills the rest of the round.
    for (segid, size, live_b) in rest {
        if sel.selected.len() >= MAX_COMPACT_SEGMENTS_PER_ROUND || sel.sel_live >= round_bytes {
            sel.saturated = true; // actionable candidates cut by the budget
            break;
        }
        // No-duplicate selection must not depend on candidates/tail
        // disjointness.
        if !selected_set.insert(segid) {
            continue;
        }
        sel.nominated_selected += nominated.contains(&segid) as usize;
        sel.admit(segid, size, live_b);
    }

    // Pass 3: tail scrub on leftover budget, most-dead-first; `None` skips
    // the pass whole.
    if let Some(floor) = tail_min_dead_percent {
        tail.sort_by_key(|&(s, size, live_b)| (live_permille(live_b, size), s.epoch, s.counter));
        for (segid, size, live_b) in tail {
            let dead_over_floor =
                (size - live_b.min(size)).saturating_mul(100) > size.saturating_mul(floor);
            if !dead_over_floor || !write_cold(&segid) {
                continue;
            }
            if sel.selected.len() >= MAX_COMPACT_SEGMENTS_PER_ROUND
                || sel.sel_live + live_b > round_bytes
            {
                sel.saturated = true; // tail cut by budget
                break;
            }
            if !selected_set.insert(segid) {
                continue;
            }
            sel.tail_selected += 1;
            sel.admit(segid, size, live_b);
        }
    }
    sel
}

#[derive(Clone)]
pub struct ExtentStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    segments: Arc<SegmentStore>,
    /// Same per-inode write lock the foreground path uses, so the coalescer's
    /// conditional swap can't be clobbered by a concurrent write.
    lock_manager: Arc<KeyedLockManager<InodeId>>,
    codec: Arc<FrameCodec>,
    open: Arc<Mutex<OpenSegment>>,
    /// Finalized bytes of segments whose PUT is in flight (or failed and pending a
    /// re-PUT). Reads consult these before the object store.
    sealing: Arc<Mutex<HashMap<Segid, Bytes>>>,
    /// Permits = max in-flight seals; acquiring all is the fsync drain barrier.
    seal_sem: Arc<Semaphore>,
    /// Deadline after which each currently-dead segment may be deleted:
    /// recorded the first pass it's seen dead, from the latest expiry of the
    /// checkpoints active then, so reclamation outlasts anything that could
    /// still reference it.
    delete_at: Arc<Mutex<HashMap<Segid, DateTime<Utc>>>>,
    /// Read-nominated compaction hints (see [`NominationSet`]): pushed by the
    /// demand-read path, drained by each reclaim pass to prioritize selection.
    nominations: Arc<Mutex<NominationSet>>,
    /// Whether reads nominate at all. Set once when segment GC starts.
    nominations_enabled: Arc<AtomicBool>,
    /// Crossing-pair heat (see [`PairStats`]): bumped by demand reads that
    /// fetch adjacent file data across two on-store segments, read by each
    /// reclaim pass to form chain-compaction selections.
    pair_stats: Arc<Mutex<PairStats>>,
    /// Monotone reclaim-pass counter: the clock for pair-stat episode dedup
    /// (staleness is wall-clock). Bumped once per pass, pinned passes included.
    gc_round: Arc<std::sync::atomic::AtomicU64>,
    /// Write-quiescence tracking: the (epoch, cutoff) the previous pass saw
    /// and the monotonic instant it was first seen unchanged. Touched only by
    /// the single segment-gc task.
    quiescence: Arc<Mutex<(u64, u64, Instant)>>,
    /// Per-inode copy of the most-recently-written (extent_idx, full extent), so a
    /// sequential append splices into it rather than re-decoding the buffered/sealed
    /// frame. Eviction only ever costs a re-fetch.
    tail_cache: Cache<InodeId, (u64, Bytes)>,
    /// Per-inode logical read-ahead state: (last_read_end, prefetched_to, seq_run).
    read_ahead: Cache<InodeId, (u64, u64, u32)>,
    /// Global bound on concurrent read-ahead fetches.
    prefetch_sem: Arc<Semaphore>,
    /// Buffer size that triggers a background seal (i.e. the segment object size).
    /// Defaults to the const; tests lower it so seal paths don't allocate 256 MiB.
    seal_threshold: usize,
    /// Weak handle to the commit worker, injected post-construction (the worker owns
    /// an `ExtentStore` clone, so a strong handle would cycle). Set in production;
    /// unset in the extent unit tests, where `commit_via_coordinator` is the sole
    /// segcount writer and commits directly.
    coordinator: Arc<std::sync::OnceLock<crate::fs::write_coordinator::WeakWriteCoordinator>>,
    /// Reclaim/compaction counters and footprint gauges, bridged to Prometheus.
    /// Written only by the segment-GC task (see `reclaim_segments_gated`).
    segment_gc_stats: Arc<SegmentGcStats>,
}

impl ExtentStore {
    pub fn new(
        db: Arc<Db>,
        key_codec: Arc<KeyCodec>,
        segments: Arc<SegmentStore>,
        lock_manager: Arc<KeyedLockManager<InodeId>>,
    ) -> Self {
        let tail_cache = CacheBuilder::new(TAIL_CACHE_BYTES)
            .with_weighter(|_id: &InodeId, (_idx, data): &(u64, Bytes)| data.len())
            .build();
        let read_ahead = CacheBuilder::new(READ_AHEAD_TRACK_BYTES)
            .with_weighter(|_: &InodeId, _: &(u64, u64, u32)| 24)
            .build();
        let codec = segments.codec();
        let open = Arc::new(Mutex::new(OpenSegment {
            segid: segments.next_segid(),
            buf: Vec::new(),
            dir: Vec::new(),
        }));
        Self {
            db,
            key_codec,
            segments,
            lock_manager,
            codec,
            open,
            sealing: Arc::new(Mutex::new(HashMap::new())),
            seal_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_SEALS)),
            delete_at: Arc::new(Mutex::new(HashMap::new())),
            nominations: Arc::new(Mutex::new(NominationSet::default())),
            nominations_enabled: Arc::new(AtomicBool::new(false)),
            pair_stats: Arc::new(Mutex::new(PairStats::default())),
            gc_round: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            quiescence: Arc::new(Mutex::new((0, 0, Instant::now()))),
            tail_cache,
            read_ahead,
            prefetch_sem: Arc::new(Semaphore::new(READ_AHEAD_MAX_CONCURRENT)),
            seal_threshold: SEAL_THRESHOLD,
            coordinator: Arc::new(std::sync::OnceLock::new()),
            segment_gc_stats: Arc::new(SegmentGcStats::default()),
        }
    }

    /// Reclaim/compaction metrics holder, for the Prometheus bridge.
    pub fn segment_gc_stats(&self) -> Arc<SegmentGcStats> {
        Arc::clone(&self.segment_gc_stats)
    }

    /// One-time footprint scan: sums the segcount rows into the aggregate the
    /// monitor gauges track. Used to seed those gauges at open (after which they
    /// are maintained incrementally off the commit path) and as the ground data
    /// the incremental path is tested against.
    pub async fn sample_footprint(&self) -> Result<SegmentFootprint, FsError> {
        let (sc_start, sc_end) = self.key_codec.segcount_prefix_range();
        let mut stream = self.db.scan(sc_start..sc_end).await.map_err(|e| {
            error!("segment footprint scan failed: {}", e);
            FsError::IoError
        })?;
        let (mut segment_count, mut live_bytes, mut appended_bytes) = (0u64, 0u64, 0u64);
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if self.key_codec.parse_segcount_key(&key).is_none() {
                continue;
            }
            let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
                continue;
            };
            segment_count += 1;
            live_bytes += live;
            appended_bytes += total;
        }
        Ok(SegmentFootprint {
            segment_count,
            appended_bytes,
            live_bytes,
            reclaimable_bytes: appended_bytes.saturating_sub(live_bytes),
        })
    }

    /// Seed the monitor footprint gauges from a one-time scan. Call at store
    /// open, before writes begin, so the incremental deltas start from the
    /// existing on-store footprint.
    pub async fn seed_footprint(&self) -> Result<(), FsError> {
        let f = self.sample_footprint().await?;
        self.segment_gc_stats.seed_footprint(&f);
        Ok(())
    }

    /// Bytes held in RAM, not yet PUT to the object store: the open write
    /// buffer plus any sealed segments whose PUT is still in flight. This is
    /// the write-back buffer, the recently-written data a crash would lose
    /// without a flush. Read fresh (it is volatile); cheap in-memory lengths.
    pub fn unflushed_bytes(&self) -> u64 {
        let open = self.open.lock().unwrap().buf.len() as u64;
        let sealing: u64 = self
            .sealing
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum();
        open + sealing
    }

    /// Inject the commit worker's weak handle so this store's GC/compaction
    /// seg-delta txns route through the single writer. Idempotent.
    pub fn set_coordinator(&self, coord: crate::fs::write_coordinator::WeakWriteCoordinator) {
        let _ = self.coordinator.set(coord);
    }

    /// Enable read-path compaction nominations.
    pub fn enable_nominations(&self) {
        self.nominations_enabled.store(true, Ordering::Relaxed);
    }

    /// No seal PUT in flight or pending re-PUT. Fast passes must not queue
    /// the barrier's all-permits drain behind a seal burst.
    pub fn seals_quiet(&self) -> bool {
        self.seal_sem.available_permits() == MAX_INFLIGHT_SEALS
            && self.sealing.lock().unwrap().is_empty()
    }

    /// Commit a txn that may carry seg-count deltas. In production this hands off to
    /// the commit worker (the sole segcount writer); with no coordinator (unit tests)
    /// we are the only writer, so materialize the deltas and commit directly.
    async fn commit_via_coordinator(&self, mut txn: Transaction) -> Result<(), FsError> {
        if let Some(coord) = self.coordinator.get() {
            return coord.commit(txn).await;
        }
        let deltas = txn.take_seg_deltas();
        let mut batch = txn.into_inner();
        let (_, footprint_delta) =
            crate::fs::write_coordinator::stage_seg_deltas(&self.db, deltas, &mut batch).await?;
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;
        // Committed: fold the batch's net footprint into the monitor gauges,
        // mirroring the write coordinator's apply on its own path.
        self.segment_gc_stats.apply_footprint_delta(
            footprint_delta.d_segments,
            footprint_delta.d_appended,
            footprint_delta.d_live,
        );
        Ok(())
    }

    /// Test-only: lower the seal threshold so seal-path tests don't build a full
    /// 256 MiB segment.
    #[cfg(test)]
    fn with_seal_threshold(mut self, n: usize) -> Self {
        self.seal_threshold = n;
        self
    }

    fn tail_get(&self, id: InodeId) -> Option<(u64, Bytes)> {
        self.tail_cache.get(&id).map(|e| (*e).clone())
    }
    fn tail_set(&self, id: InodeId, extent_idx: u64, data: Bytes) {
        self.tail_cache.insert(id, (extent_idx, data));
    }
    fn tail_invalidate(&self, id: InodeId) {
        self.tail_cache.remove(&id);
    }

    /// Apply a `write`'s tail-cache effect. Call only after its commit succeeds.
    pub fn apply_tail_update(&self, id: InodeId, update: TailUpdate) {
        match update {
            TailUpdate::Set { extent_idx, data } => self.tail_set(id, extent_idx, data),
            TailUpdate::Clear => self.tail_invalidate(id),
            TailUpdate::Keep => {}
        }
    }

    /// Raw `[len][sealed]` bytes of a frame still resident in RAM (the open buffer
    /// or an in-flight seal), or `None` once its segment is PUT (the standby reads
    /// the shared store directly). Used to ship un-PUT segments' bytes for HA.
    fn read_frame_for_ship(&self, segid: Segid, byte_offset: u64, byte_len: u32) -> Option<Bytes> {
        let start = byte_offset as usize;
        let end = start.checked_add(byte_len as usize)?;
        {
            let open = self.open.lock().unwrap();
            if open.segid == segid {
                return open.buf.get(start..end).map(Bytes::copy_from_slice);
            }
        }
        let sealing = self.sealing.lock().unwrap();
        let bytes = sealing.get(&segid)?;
        (end <= bytes.len()).then(|| bytes.slice(start..end))
    }

    /// Enrich a batch's replication ops: an extent-write `Put` whose segment is
    /// still un-PUT becomes a `PutFrame` carrying the sealed frame bytes, so
    /// the standby can materialize that segment on takeover. Already-PUT
    /// segments stay plain `Put`. Called by the commit worker when replicating.
    pub fn enrich_repl_ops(&self, ops: Vec<ReplOp>) -> Vec<ReplOp> {
        ops.into_iter()
            .map(|op| match op {
                ReplOp::Put(k, v) => {
                    if self.key_codec.parse_extent_key(&k).is_some()
                        && let Some(loc) = FrameLoc::decode(&v)
                        && let Some(frame) =
                            self.read_frame_for_ship(loc.segid, loc.byte_offset, loc.byte_len)
                    {
                        ReplOp::PutFrame(k, v, frame)
                    } else {
                        ReplOp::Put(k, v)
                    }
                }
                other => other,
            })
            .collect()
    }

    /// The full-extent (EXTENT_SIZE) plaintext for `(id, extent)`, or `None` for a
    /// hole. Resolves the extent key's `FrameLoc` then fetches the frame.
    pub async fn get(&self, id: InodeId, extent_idx: u64) -> Result<Option<Bytes>, FsError> {
        let key = self.key_codec.extent_key(id, extent_idx);
        let encoded = match self.db.get_bytes(&key).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to read extent (inode={}, extent={}): {}",
                    id, extent_idx, e
                );
                return Err(FsError::IoError);
            }
        };
        let Some(encoded) = encoded else {
            return Ok(None);
        };
        let loc = FrameLoc::decode(&encoded).ok_or_else(|| {
            error!("Corrupt extent value (inode={}, extent={})", id, extent_idx);
            FsError::IoError
        })?;
        if let Some(mut frames) = self.read_frames_in_ram(
            loc.segid,
            loc.byte_offset,
            loc.byte_len,
            loc.frame_index,
            &[(id, extent_idx)],
        )? {
            let frame = frames.pop().expect("one frame");
            Self::validate_extent_frame(id, extent_idx, &frame)?;
            return Ok(Some(frame));
        }
        match self.segments.read_extent(loc, id, extent_idx).await {
            Ok(b) => {
                Self::validate_extent_frame(id, extent_idx, &b)?;
                Ok(Some(b))
            }
            Err(first_err) => {
                // GC compaction repoints an extent to a freshly-sealed segment and
                // then deletes the drained source; a read that resolved the old
                // FrameLoc just before that delete can race it and 404. Re-resolve
                // the extent key once: if the pointer moved, read the new location;
                // if the extent was deleted concurrently (truncate/unlink) it is now
                // a hole; otherwise the error is real.
                let reresolved = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?;
                match Self::decode_reresolved_extent(id, extent_idx, reresolved.as_ref())? {
                    Some(new_loc) if new_loc.segid != loc.segid => {
                        if let Some(mut frames) = self.read_frames_in_ram(
                            new_loc.segid,
                            new_loc.byte_offset,
                            new_loc.byte_len,
                            new_loc.frame_index,
                            &[(id, extent_idx)],
                        )? {
                            let frame = frames.pop().expect("one frame");
                            Self::validate_extent_frame(id, extent_idx, &frame)?;
                            return Ok(Some(frame));
                        }
                        match self.segments.read_extent(new_loc, id, extent_idx).await {
                            Ok(b) => {
                                Self::validate_extent_frame(id, extent_idx, &b)?;
                                Ok(Some(b))
                            }
                            Err(e) => {
                                error!(
                                    "Failed to read frame after repoint retry \
                                     (inode={}, extent={}): {}",
                                    id, extent_idx, e
                                );
                                Err(FsError::IoError)
                            }
                        }
                    }
                    None => Ok(None),
                    _ => {
                        error!(
                            "Failed to read frame (inode={}, extent={}): {}",
                            id, extent_idx, first_err
                        );
                        Err(FsError::IoError)
                    }
                }
            }
        }
    }

    /// Every stored frame decodes to exactly one full [`EXTENT_SIZE`] extent.
    /// Enforce that before callers slice the plaintext, so a bad frame that
    /// still passed AEAD surfaces as EIO instead of a panic.
    fn validate_extent_frame(id: InodeId, extent: u64, data: &Bytes) -> Result<(), FsError> {
        if data.len() != EXTENT_SIZE {
            error!(
                "Extent frame (inode={}, extent={}) decoded to {} bytes, expected {}",
                id,
                extent,
                data.len(),
                EXTENT_SIZE
            );
            return Err(FsError::IoError);
        }
        Ok(())
    }

    /// Classify the re-resolved extent value in `get`'s GC-repoint retry.
    /// An absent key means a concurrent truncate/unlink made the extent a
    /// genuine hole; a present but undecodable value is the same
    /// corrupt-value EIO as the primary path — never a fabricated hole of
    /// zeros.
    fn decode_reresolved_extent(
        id: InodeId,
        extent: u64,
        enc: Option<&Bytes>,
    ) -> Result<Option<FrameLoc>, FsError> {
        match enc {
            None => Ok(None),
            Some(enc) => match FrameLoc::decode(enc) {
                Some(loc) => Ok(Some(loc)),
                None => {
                    error!("Corrupt extent value (inode={}, extent={})", id, extent);
                    Err(FsError::IoError)
                }
            },
        }
    }

    /// Read a contiguous run of frames from the open or an in-flight sealing
    /// buffer (read-your-writes), or `None` if `segid` is already on the object
    /// store. Frame offsets are identical in the buffer and the finalized
    /// segment, so the same slice works for both.
    fn read_frames_in_ram(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Option<Vec<Bytes>>, FsError> {
        // The range comes from a db-stored FrameLoc: bounds-checked, never
        // trusted, so a corrupt value surfaces as EIO instead of an
        // out-of-range panic that would poison the open-segment lock for
        // every later writer.
        fn region(
            buf: &[u8],
            segid: Segid,
            byte_offset: u64,
            byte_len: u32,
        ) -> Result<&[u8], FsError> {
            let start = byte_offset as usize;
            start
                .checked_add(byte_len as usize)
                .and_then(|end| buf.get(start..end))
                .ok_or_else(|| {
                    error!(
                        "Corrupt FrameLoc for in-RAM {segid:?}: {byte_len} bytes at {byte_offset} \
                         exceed the {}-byte buffer",
                        buf.len()
                    );
                    FsError::IoError
                })
        }
        {
            let open = self.open.lock().unwrap();
            if segid == open.segid {
                let frames = crate::segment::read_frames_from_region(
                    &self.codec,
                    region(&open.buf, segid, byte_offset, byte_len)?,
                    segid,
                    first_frame,
                    slots,
                )
                .map_err(|_| FsError::IoError)?;
                return Ok(Some(frames.into_iter().map(Bytes::from).collect()));
            }
        }
        {
            let sealing = self.sealing.lock().unwrap();
            if let Some(bytes) = sealing.get(&segid) {
                let frames = crate::segment::read_frames_from_region(
                    &self.codec,
                    region(bytes.as_ref(), segid, byte_offset, byte_len)?,
                    segid,
                    first_frame,
                    slots,
                )
                .map_err(|_| FsError::IoError)?;
                return Ok(Some(frames.into_iter().map(Bytes::from).collect()));
            }
        }
        Ok(None)
    }

    pub fn delete(&self, txn: &mut Transaction, id: InodeId, extent_idx: u64) {
        let key = self.key_codec.extent_key(id, extent_idx);
        txn.delete_bytes(&key);
    }

    /// Stage live/total byte deltas for `segid`'s counter onto the txn; the
    /// commit worker folds them into the absolute `(live, total)`. A debit
    /// passes `total_delta == 0` (`total` is monotonic).
    fn seg_delta(&self, txn: &mut Transaction, segid: Segid, live_delta: i64, total_delta: i64) {
        txn.add_seg_delta(
            &self.key_codec.segcount_key(segid.epoch, segid.counter),
            live_delta,
            total_delta,
        );
    }

    pub async fn delete_range(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        start: u64,
        end: u64,
    ) -> Result<(), FsError> {
        self.tail_invalidate(id);
        if start >= end {
            return Ok(());
        }
        // Debit each removed extent's bytes from its segment's live-byte counter.
        // One forward-map scan (cheaper than a GET per extent); the caller's inode
        // write lock serialises this read-then-delete against a concurrent write to
        // the same extent, so no segment is debited twice for one frame.
        let start_key = self.key_codec.extent_key(id, start);
        let end_key = self.key_codec.extent_key(id, end);
        let mut stream = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(|_| FsError::IoError)?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if self.key_codec.parse_extent_key(&key).is_some()
                && let Some(loc) = FrameLoc::decode(&value)
            {
                // Delete debit: live only, total untouched (monotonic).
                self.seg_delta(txn, loc.segid, -(loc.byte_len as i64), 0);
            }
        }
        for extent_idx in start..end {
            self.delete(txn, id, extent_idx);
        }
        Ok(())
    }

    /// Append the non-zero extents of `edits` to the open-segment buffer (no PUT)
    /// and stage their extent pointers; stage extent-key deletes for the holes.
    /// Kicks off a background seal when the buffer crosses [`SEAL_THRESHOLD`].
    async fn stage_edits(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        edits: &[(u64, Option<Bytes>)],
    ) -> Result<(), FsError> {
        let mut old_debits: Vec<(Segid, u32)> = Vec::with_capacity(edits.len());
        if let (Some(min), Some(max)) = (
            edits.iter().map(|(e, _)| *e).min(),
            edits.iter().map(|(e, _)| *e).max(),
        ) {
            let edited: HashSet<u64> = edits.iter().map(|(e, _)| *e).collect();
            let start_key = self.key_codec.extent_key(id, min);
            let end_key = self.key_codec.extent_key(id, max.saturating_add(1));
            let mut stream = self
                .db
                .scan(start_key..end_key)
                .await
                .map_err(|_| FsError::IoError)?;
            while let Some(result) = stream.next().await {
                let (key, value) = result.map_err(|_| FsError::IoError)?;
                if let Some(extent_idx) = self.key_codec.parse_extent_key(&key)
                    && edited.contains(&extent_idx)
                    && let Some(loc) = FrameLoc::decode(&value)
                {
                    old_debits.push((loc.segid, loc.byte_len));
                }
            }
        }
        // Compress before taking the open-segment lock: compression is the
        // expensive half of the codec and depends only on the plaintext, while
        // the AEAD binds (segid, frame_index), assigned under the lock. Large
        // batches fan out on rayon; block_in_place needs the multi-thread
        // runtime (tests run current-thread), and small batches stay inline.
        let payloads: Vec<&Bytes> = edits.iter().filter_map(|(_, e)| e.as_ref()).collect();
        let compressed: Vec<Vec<u8>> = if payloads.len() >= PARALLEL_COMPRESS_MIN_FRAMES
            && tokio::runtime::Handle::current().runtime_flavor()
                == tokio::runtime::RuntimeFlavor::MultiThread
        {
            tokio::task::block_in_place(|| {
                use rayon::prelude::*;
                payloads
                    .par_iter()
                    .map(|p| self.codec.compress(p))
                    .collect::<Result<_, _>>()
            })
            .map_err(|_| FsError::IoError)?
        } else {
            payloads
                .iter()
                .map(|p| self.codec.compress(p))
                .collect::<Result<_, _>>()
                .map_err(|_| FsError::IoError)?
        };
        let mut compressed = compressed.into_iter();
        {
            let mut open = self.open.lock().unwrap();
            for (extent, edit) in edits {
                match edit {
                    Some(_) => {
                        let frame_index = open.dir.len() as u32;
                        let segid = open.segid;
                        let sealed = crate::segment::seal_compressed_frame(
                            &self.codec,
                            segid,
                            frame_index,
                            id,
                            *extent,
                            compressed.next().expect("one compressed payload per edit"),
                        )
                        .map_err(|_| FsError::IoError)?;
                        let byte_offset = open.buf.len() as u64;
                        let sealed_len = sealed.len() as u32;
                        open.buf.extend_from_slice(&sealed_len.to_le_bytes());
                        open.buf.extend_from_slice(&sealed);
                        open.dir.push(DirEntry {
                            byte_offset,
                            len: sealed_len,
                            inode: id,
                            extent: *extent,
                        });
                        let loc = FrameLoc {
                            segid,
                            frame_index,
                            byte_offset,
                            byte_len: 4 + sealed_len,
                        };
                        txn.put_bytes(
                            &self.key_codec.extent_key(id, *extent),
                            Bytes::copy_from_slice(&loc.encode()),
                        );
                        // Credit the frame just appended: both live and total.
                        self.seg_delta(txn, segid, loc.byte_len as i64, loc.byte_len as i64);
                    }
                    None => self.delete(txn, id, *extent),
                }
            }
        }
        for (segid, byte_len) in old_debits {
            // Overwrite debit of the superseded frame: live only, total untouched.
            self.seg_delta(txn, segid, -(byte_len as i64), 0);
        }
        let over_threshold = self.open.lock().unwrap().buf.len() >= self.seal_threshold;
        if over_threshold {
            self.spawn_seal().await;
        }
        Ok(())
    }

    /// The durability barrier (called by the flush path before the manifest is
    /// flushed): wait for every in-flight background seal, re-PUT any that failed,
    /// then synchronously seal the current open buffer. After this returns, every
    /// segment referenced by a committed extent is durable on the object store.
    pub async fn seal_open(&self) -> Result<(), FsError> {
        // Acquire all permits: waits for in-flight background seals to finish, and
        // holds new ones off until we release at end of scope.
        let _all = self
            .seal_sem
            .acquire_many(MAX_INFLIGHT_SEALS as u32)
            .await
            .map_err(|_| FsError::IoError)?;

        // Re-PUT any seal whose background attempt failed (still in `sealing`).
        let pending: Vec<(Segid, Bytes)> = {
            let s = self.sealing.lock().unwrap();
            s.iter().map(|(seg, b)| (*seg, b.clone())).collect()
        };
        for (segid, bytes) in pending {
            self.segments
                .put_segment(segid, bytes)
                .await
                .map_err(|_| FsError::IoError)?;
            self.sealing.lock().unwrap().remove(&segid);
        }

        // Synchronously seal the current open buffer. Register it in `sealing`
        // under the open lock and remove it only on success, so the rotated
        // segment is never absent from both maps: a concurrent read would 404
        // the not-yet-PUT object, and a failed PUT would strand it. Mirrors
        // spawn_seal.
        let current = {
            let mut open = self.open.lock().unwrap();
            if open.dir.is_empty() {
                None
            } else {
                let segid = open.segid;
                #[cfg(feature = "failpoints")]
                fail_point!(fp::SEAL_OPEN_FAIL, |_| Err(FsError::IoError));
                // Seal the directory first: on error the open buffer and its
                // committed FrameLocs stay intact for retry, instead of being
                // dropped into a dangling pointer.
                let sealed_dir = crate::segment::seal_directory(&self.codec, segid, &open.dir)
                    .map_err(|_| FsError::IoError)?;
                let k = open.dir.len() as u32;
                let buf = std::mem::take(&mut open.buf);
                open.dir.clear();
                open.segid = self.segments.next_segid();
                debug_assert_ne!(
                    open.segid, segid,
                    "rotated open segid must differ from the sealed one"
                );
                let bytes = Bytes::from(crate::segment::assemble_segment(
                    segid,
                    buf,
                    k,
                    &sealed_dir,
                    segid.counter,
                ));
                self.sealing.lock().unwrap().insert(segid, bytes.clone());
                Some((segid, bytes))
            }
        };
        if let Some((segid, bytes)) = current {
            self.segments
                .put_segment(segid, bytes)
                .await
                .map_err(|_| FsError::IoError)?;
            self.sealing.lock().unwrap().remove(&segid);
        }
        Ok(())
    }

    /// Rotate the open buffer and PUT it in the background (the size-threshold
    /// path). Acquires a permit first, so a writer that outruns the object store
    /// blocks here (backpressure) instead of growing RAM without bound. The
    /// rotated buffer stays readable via `sealing` until its PUT lands.
    async fn spawn_seal(&self) {
        let permit = match Arc::clone(&self.seal_sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let prepared = {
            let mut open = self.open.lock().unwrap();
            if open.dir.is_empty() {
                return;
            }
            let segid = open.segid;
            // Directory first, as in seal_open: an error must leave the open
            // buffer intact for the next seal/flush to retry.
            let sealed_dir = match crate::segment::seal_directory(&self.codec, segid, &open.dir) {
                Ok(s) => s,
                Err(e) => {
                    error!("failed to seal open segment directory {:?}: {}", segid, e);
                    return;
                }
            };
            let k = open.dir.len() as u32;
            let buf = std::mem::take(&mut open.buf);
            open.dir.clear();
            open.segid = self.segments.next_segid();
            debug_assert_ne!(
                open.segid, segid,
                "rotated open segid must differ from the sealed one"
            );
            let bytes = Bytes::from(crate::segment::assemble_segment(
                segid,
                buf,
                k,
                &sealed_dir,
                segid.counter,
            ));
            // Insert into `sealing` while still holding `open`, so the segid is
            // never absent from both maps (a concurrent read would miss it).
            self.sealing.lock().unwrap().insert(segid, bytes.clone());
            Some((segid, bytes))
        };
        let Some((segid, bytes)) = prepared else {
            return;
        };
        let segments = self.segments.clone();
        let sealing = self.sealing.clone();
        crate::task::spawn_named("segment-seal", async move {
            match segments.put_segment(segid, bytes).await {
                Ok(()) => {
                    sealing.lock().unwrap().remove(&segid);
                }
                Err(e) => {
                    error!(
                        "background seal PUT failed for {:?}: {}; retried on flush",
                        segid, e
                    );
                }
            }
            drop(permit);
        });
    }

    /// Read `[offset, offset+length)`, then kick off the bounded,
    /// sequential-only logical read-ahead so the next read lands warm in the
    /// parts cache.
    pub async fn read(&self, id: InodeId, offset: u64, length: u64) -> Result<Bytes, FsError> {
        let data = self.read_range(id, offset, length, true).await?;
        self.trigger_read_ahead(id, offset, length);
        Ok(data)
    }

    /// Sequential-read detection + a bounded forward prefetch. Best-effort: the
    /// per-inode state is racy under concurrent readers of one file, which only
    /// costs a slightly-off prefetch.
    fn trigger_read_ahead(
        &self,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if length == 0 {
            return None;
        }
        let prev = self.read_ahead.get(&id).map(|e| *e).unwrap_or((0, 0, 0));
        let (state, plan) = plan_read_ahead(prev, offset, length);
        let Some((start, end)) = plan else {
            self.read_ahead.insert(id, state);
            return None;
        };
        // Skip when at the concurrency cap rather than queueing (read-ahead is
        // best-effort). Coverage is committed only once the fetch is spawned:
        // on a skip, `prefetched_to` stays at the true high-water mark
        // (`start`), so the next read re-plans and re-tries the permit.
        let Ok(permit) = Arc::clone(&self.prefetch_sem).try_acquire_owned() else {
            let (read_end, _, seq) = state;
            self.read_ahead.insert(id, (read_end, start, seq));
            return None;
        };
        self.read_ahead.insert(id, state);
        let this = self.clone();
        let read_end = offset + length;
        Some(crate::task::spawn_named("read-ahead", async move {
            let _permit = permit;
            // Cross-segment only: within one segment the per-object prefetcher
            // already reads ahead, and a second interleaved stream would
            // fragment its window ramp into tiny GETs. Prefetch only when the
            // window reaches a segment it can't follow into.
            let cur_ext = read_end.saturating_sub(1) / EXTENT_SIZE as u64;
            let tgt_ext = end.saturating_sub(1) / EXTENT_SIZE as u64;
            let cur_seg = this.segment_at(id, cur_ext).await;
            if cur_seg.is_some() && cur_seg == this.segment_at(id, tgt_ext).await {
                return;
            }
            let _ = this.read_range(id, start, end - start, false).await;
        }))
    }

    /// The segment an extent's current frame lives in, or `None` for a hole.
    async fn segment_at(&self, id: InodeId, extent: u64) -> Option<Segid> {
        let key = self.key_codec.extent_key(id, extent);
        self.db
            .get_bytes(&key)
            .await
            .ok()
            .flatten()
            .and_then(|b| FrameLoc::decode(&b))
            .map(|loc| loc.segid)
    }

    /// A byte-range read with no read-ahead side effect (the raw path; also what
    /// the read-ahead task calls, so it never recurses). Only demand reads
    /// (`is_demand`) feed compaction nominations, so prefetch traffic can't
    /// nominate.
    async fn read_range(
        &self,
        id: InodeId,
        offset: u64,
        length: u64,
        is_demand: bool,
    ) -> Result<Bytes, FsError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = offset + length;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end - 1) / EXTENT_SIZE as u64;
        let start_offset = (offset % EXTENT_SIZE as u64) as usize;

        if start_extent == end_extent {
            let extent_end = start_offset + length as usize;
            return Ok(match self.get(id, start_extent).await? {
                Some(data) => data.slice(start_offset..extent_end),
                None => Bytes::copy_from_slice(&ZERO_EXTENT[start_offset..extent_end]),
            });
        }

        let start_key = self.key_codec.extent_key(id, start_extent);
        let end_key = self.key_codec.extent_key(id, end_extent + 1);
        let mut loc_map: HashMap<u64, FrameLoc> = HashMap::new();
        let mut stream = self.db.scan(start_key..end_key).await.map_err(|e| {
            error!("Failed to scan extents (inode={}): {}", id, e);
            FsError::IoError
        })?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|e| {
                error!("Failed to read extent during scan (inode={}): {}", id, e);
                FsError::IoError
            })?;
            if let Some(extent_idx) = self.key_codec.parse_extent_key(&key) {
                // A present-but-undecodable value is the same corrupt-value
                // EIO as the single-extent path (`get`) — skipping it would
                // serve the extent as a fabricated hole of zeros.
                let loc = FrameLoc::decode(&value).ok_or_else(|| {
                    error!("Corrupt extent value (inode={}, extent={})", id, extent_idx);
                    FsError::IoError
                })?;
                loc_map.insert(extent_idx, loc);
            }
        }

        // Assemble, coalescing each maximal run of extents that are contiguous in
        // one segment (consecutive frame index + adjacent byte range) into a
        // single ranged GET. This is the read-amplification win: a region written
        // together reads back in one GET instead of one-per-extent.
        let mut result = BytesMut::with_capacity(length as usize);
        let slice = |c: u64| -> (usize, usize) {
            let cs = if c == start_extent { start_offset } else { 0 };
            let ce = if c == end_extent {
                ((end - 1) % EXTENT_SIZE as u64 + 1) as usize
            } else {
                EXTENT_SIZE
            };
            (cs, ce)
        };
        let mut nominate: Vec<Segid> = Vec::new();
        // Seams this read paid. `prev_nonram` = (segid, end extent) of the
        // previous on-store run; holes and RAM runs break adjacency.
        let mut crossings: Vec<(Segid, Segid)> = Vec::new();
        let mut prev_nonram: Option<(Segid, u64)> = None;
        let track = is_demand && self.nominations_enabled.load(Ordering::Relaxed);
        let mut extent = start_extent;
        while extent <= end_extent {
            let Some(first) = loc_map.get(&extent).copied() else {
                // Hole: this extent contributes zeros.
                let (cs, ce) = slice(extent);
                result.extend_from_slice(&ZERO_EXTENT[cs..ce]);
                prev_nonram = None;
                extent += 1;
                continue;
            };
            let mut n = 1u64;
            let mut total_len = first.byte_len as u64;
            let mut prev = first;
            let mut slots = vec![(id, extent)];
            while extent + n <= end_extent {
                match loc_map.get(&(extent + n)).copied() {
                    Some(loc)
                        if loc.segid == prev.segid
                            && loc.frame_index == prev.frame_index + 1
                            && loc.byte_offset == prev.byte_offset + prev.byte_len as u64 =>
                    {
                        total_len += loc.byte_len as u64;
                        prev = loc;
                        slots.push((id, extent + n));
                        n += 1;
                    }
                    _ => break,
                }
            }
            // Serve the run from RAM (open or in-flight sealing buffer); else GET.
            let frames = match self.read_frames_in_ram(
                first.segid,
                first.byte_offset,
                total_len as u32,
                first.frame_index,
                &slots,
            )? {
                Some(f) => {
                    prev_nonram = None;
                    Some(f)
                }
                None => {
                    // Non-RAM ⇒ PUT complete ⇒ directory durably readable:
                    // safe to nominate.
                    if track {
                        if nominate.len() < NOMINATE_PER_CALL_CAP
                            && !nominate.contains(&first.segid)
                        {
                            nominate.push(first.segid);
                        }
                        // A seam: adjacent file data split across objects, or
                        // (same segid) scattered within one (a self-pair).
                        if let Some((prev_segid, prev_end)) = prev_nonram
                            && prev_end == extent
                            && crossings.len() < PAIR_BUMPS_PER_CALL
                        {
                            let pair = PairStats::key(prev_segid, first.segid);
                            if !crossings.contains(&pair) {
                                crossings.push(pair);
                            }
                        }
                    }
                    prev_nonram = Some((first.segid, extent + n));
                    // A GET error is swallowed: a compaction repoint+delete can 404
                    // this run's segment out from under us. Fall back to per-extent
                    // reads via `get`, which re-resolves each FrameLoc.
                    self.segments
                        .read_run(
                            first.segid,
                            first.byte_offset,
                            total_len as u32,
                            first.frame_index,
                            &slots,
                        )
                        .await
                        .ok()
                }
            };
            match frames {
                Some(frames) => {
                    for (i, frame) in frames.iter().enumerate() {
                        let idx = extent + i as u64;
                        Self::validate_extent_frame(id, idx, frame)?;
                        let (cs, ce) = slice(idx);
                        result.extend_from_slice(&frame[cs..ce]);
                    }
                }
                None => {
                    for (i, (fid, fext)) in slots.iter().enumerate() {
                        let idx = extent + i as u64;
                        let data = match self.get(*fid, *fext).await? {
                            Some(d) => d,
                            None => Bytes::from_static(ZERO_EXTENT),
                        };
                        let (cs, ce) = slice(idx);
                        result.extend_from_slice(&data[cs..ce]);
                    }
                }
            }
            extent += n;
        }
        // Fan-out is the read-amplification signal; pushed as a set so one
        // pass co-packs what was co-read.
        if nominate.len() >= NOMINATE_MIN_FANOUT {
            let mut noms = self.nominations.lock().unwrap();
            for segid in nominate {
                noms.push(segid);
            }
        }
        // Recorded below the nomination floor too: the pair itself is the
        // fan-out; once per pass regardless of read rate.
        if !crossings.is_empty() {
            let round = self.gc_round.load(Ordering::Relaxed);
            let now = Instant::now();
            let mut stats = self.pair_stats.lock().unwrap();
            for (a, b) in crossings {
                stats.bump(a, b, round, now);
            }
        }
        Ok(result.freeze())
    }

    pub async fn write(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        old_size: u64,
    ) -> Result<TailUpdate, FsError> {
        if data.is_empty() {
            return Ok(TailUpdate::Keep);
        }
        let end_offset = offset + data.len() as u64;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end_offset - 1) / EXTENT_SIZE as u64;

        let cached = self.tail_get(id);

        // Read the existing content of any partially-overwritten extent (full
        // overwrites and extents past EOF need no read).
        let existing_extents: HashMap<u64, Bytes> = stream::iter(start_extent..=end_extent)
            .map(|extent_idx| {
                let extent_start = extent_idx * EXTENT_SIZE as u64;
                let extent_end = extent_start + EXTENT_SIZE as u64;
                let will_overwrite_fully = offset <= extent_start && end_offset >= extent_end;
                let beyond_eof = extent_start >= old_size;
                let store = self.clone();
                let cached = cached.clone();
                async move {
                    let data = if will_overwrite_fully || beyond_eof {
                        Bytes::from_static(ZERO_EXTENT)
                    } else if let Some((_, bytes)) = cached.filter(|(ci, _)| *ci == extent_idx) {
                        bytes
                    } else {
                        store
                            .get(id, extent_idx)
                            .await?
                            .unwrap_or_else(|| Bytes::from_static(ZERO_EXTENT))
                    };
                    Ok::<(u64, Bytes), FsError>((extent_idx, data))
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .try_collect()
            .await?;

        let cache_tail = end_offset >= old_size && !end_offset.is_multiple_of(EXTENT_SIZE as u64);

        let mut data_offset = 0usize;
        let mut edits: Vec<(u64, Option<Bytes>)> =
            Vec::with_capacity((end_extent - start_extent + 1) as usize);
        let mut tail: Option<Bytes> = None;
        for extent_idx in start_extent..=end_extent {
            let extent_start = extent_idx * EXTENT_SIZE as u64;
            let extent_end = extent_start + EXTENT_SIZE as u64;
            let write_start = if offset > extent_start {
                (offset - extent_start) as usize
            } else {
                0
            };
            let write_end = if end_offset < extent_end {
                (end_offset - extent_start) as usize
            } else {
                EXTENT_SIZE
            };
            let write_len = write_end - write_start;
            let extent: Bytes = if write_start == 0 && write_end == EXTENT_SIZE {
                data.slice(data_offset..data_offset + write_len)
            } else {
                let mut buf = BytesMut::from(existing_extents[&extent_idx].as_ref());
                buf[write_start..write_end]
                    .copy_from_slice(&data[data_offset..data_offset + write_len]);
                buf.freeze()
            };
            data_offset += write_len;

            if extent.as_ref() == ZERO_EXTENT {
                edits.push((extent_idx, None));
            } else {
                if extent_idx == end_extent && cache_tail {
                    tail = Some(extent.clone());
                }
                edits.push((extent_idx, Some(extent)));
            }
        }

        self.stage_edits(txn, id, &edits).await?;

        Ok(match tail {
            Some(data) => TailUpdate::Set {
                extent_idx: end_extent,
                data,
            },
            None => TailUpdate::Clear,
        })
    }

    pub async fn truncate(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        old_size: u64,
        new_size: u64,
    ) -> Result<(), FsError> {
        if new_size >= old_size {
            return Ok(());
        }

        let old_extents = old_size.div_ceil(EXTENT_SIZE as u64);
        let new_extents = new_size.div_ceil(EXTENT_SIZE as u64);
        self.delete_range(txn, id, new_extents, old_extents).await?;

        if new_size > 0 {
            let last_extent_idx = new_extents - 1;
            let clear_from = (new_size % EXTENT_SIZE as u64) as usize;
            if clear_from > 0 {
                let existing = self.get(id, last_extent_idx).await?;
                let mut extent =
                    BytesMut::from(existing.as_ref().map(|b| b.as_ref()).unwrap_or(ZERO_EXTENT));
                extent[clear_from..].fill(0);
                let edit = if extent.as_ref() == ZERO_EXTENT {
                    (last_extent_idx, None)
                } else {
                    (last_extent_idx, Some(extent.freeze()))
                };
                self.stage_edits(txn, id, &[edit]).await?;
            }
        }
        Ok(())
    }

    pub async fn zero_range(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        length: u64,
        file_size: u64,
    ) -> Result<(), FsError> {
        if length == 0 {
            return Ok(());
        }
        self.tail_invalidate(id);

        let end_offset = offset + length;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end_offset - 1) / EXTENT_SIZE as u64;

        let mut edits: Vec<(u64, Option<Bytes>)> = Vec::new();
        for extent_idx in start_extent..=end_extent {
            let extent_start = extent_idx * EXTENT_SIZE as u64;
            let extent_end = extent_start + EXTENT_SIZE as u64;
            if extent_start >= file_size {
                continue;
            }
            if offset <= extent_start && end_offset >= extent_end {
                edits.push((extent_idx, None));
            } else if let Some(existing_data) = self.get(id, extent_idx).await? {
                let zero_start = if offset > extent_start {
                    (offset - extent_start) as usize
                } else {
                    0
                };
                let zero_end = if end_offset < extent_end {
                    (end_offset - extent_start) as usize
                } else {
                    EXTENT_SIZE
                };
                let mut extent_data = BytesMut::from(existing_data.as_ref());
                extent_data[zero_start..zero_end].fill(0);
                if extent_data.as_ref() == ZERO_EXTENT {
                    edits.push((extent_idx, None));
                } else {
                    edits.push((extent_idx, Some(extent_data.freeze())));
                }
            }
        }
        self.stage_edits(txn, id, &edits).await
    }

    /// Delete an extent range under the inode's write lock, in its own transaction.
    /// The tombstone GC calls this so its deletes serialize with the compaction
    /// repoint, which takes the same lock: otherwise a repoint that read an extent
    /// live, then lost the race to a concurrent tombstone delete, would re-commit
    /// the extent last-writer-wins (the LSM has no CAS), resurrecting a deleted
    /// inode's extent and pinning the repacked segment forever.
    pub async fn delete_extents(
        &self,
        inode: InodeId,
        start_extent: u64,
        total_extents: u64,
    ) -> Result<(), FsError> {
        let _guard = self.lock_manager.acquire(inode).await;
        let mut txn = self.db.new_transaction()?;
        self.delete_range(&mut txn, inode, start_extent, total_extents)
            .await?;
        self.commit_via_coordinator(txn).await?;
        Ok(())
    }

    /// List the PUT segment ids. Exposed for the failpoints crash tests, which drive
    /// `compact_segments` directly (the reclaim floor won't compact tiny test data).
    #[cfg(feature = "failpoints")]
    #[allow(dead_code)] // used by the failpoints integration test, not the lib/bin
    pub async fn list_segments(&self) -> Result<Vec<Segid>, FsError> {
        self.segments
            .list_segments()
            .await
            .map_err(|_| FsError::IoError)
    }

    /// Reclaim with an immediate horizon (so a dead segment is deleted this pass).
    /// For the failpoints crash tests, which have no `chrono` in scope.
    #[cfg(feature = "failpoints")]
    #[allow(dead_code)] // used by the failpoints integration test, not the lib/bin
    pub async fn reclaim_now(&self) -> Result<(usize, usize), FsError> {
        self.reclaim_segments(chrono::Utc::now(), None).await
    }

    /// Reclaim object-store space: delete fully-dead segments and compact the
    /// most-fragmented ones, driven entirely off the local `segcount` scan.
    /// Returns (segments deleted, frames relocated); production goes through
    /// [`Self::reclaim_segments_gated`], which also reports saturation.
    #[allow(dead_code)] // failpoints + tests
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
    async fn reclaim_segments_tuned(
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
    /// budget: the selection cap, the heat reserve (half), and the plaintext
    /// gather RAM cap. Raising it packs over-reserve seams and lifts dead-space
    /// throughput per batch, at proportional gather RAM.
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
        // The seal makes the flushed manifest reference only PUT segments; the
        // flush makes "dead in the in-memory view" mean "dead in the durable view".
        // Under the flush barrier (write side), same as the flush coordinator, so a
        // concurrent commit can't durably reference the just-rotated open buffer.
        {
            let _barrier = self.db.flush_barrier().write_owned().await;
            self.seal_open().await?;
            self.db.flush().await.map_err(|_| FsError::IoError)?;
        }
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

        let cur_epoch = self.segments.epoch();
        // Exclusive counter cutoff for eligibility: the still-open segment's
        // own counter, not `next_counter()`. seal_open() just rotated in a
        // fresh open segment that is still accepting frames whose credits may
        // not be visible to the scan below; `next_counter()` would include it,
        // and a mid-pass background seal could then mis-classify that fully
        // live segment as dead.
        let cutoff = self.open.lock().unwrap().segid.counter;

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
        // Total bytes of every dropped counter (deleted segments plus leaked
        // counters), to debit from the monitor's incremental footprint gauges.
        let mut freed_appended = 0u64;
        for (segid, size) in to_delete {
            // Fail-closed: a segment is deleted only once its directory confirms no
            // frame is still referenced. Sound because an eligible segment can never
            // regain a reference, so "no frame points here" is a permanent verdict.
            match self.verify_segment_reclaimable(segid).await {
                SegmentDeadVerdict::Keep => {
                    // The counter under-counted (a live frame remains) or the read
                    // was transient — leak beats loss.
                    error!(
                        "segment GC: counter calls {segid:?} dead but a live frame remains; \
                         skipping delete (leak, not loss)"
                    );
                    continue;
                }
                SegmentDeadVerdict::ObjectAbsent => {
                    // A crash between deleting the object and committing the
                    // counter drop left the counter behind; drop it so it stops
                    // re-appearing as dead each pass.
                    freed_counters.push(segid);
                    freed_appended += size;
                    continue;
                }
                SegmentDeadVerdict::Reclaim => {}
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
            freed_appended += size;
            deleted_bytes += size;
            deleted += 1;
        }
        // Drop the counters of deleted segments, else one segcount key leaks
        // per segment ever created.
        if !freed_counters.is_empty() {
            let dropped = freed_counters.len() as i64;
            let mut txn = self.db.new_transaction()?;
            for segid in &freed_counters {
                txn.delete_bytes(&self.key_codec.segcount_key(segid.epoch, segid.counter));
            }
            self.commit_via_coordinator(txn).await?;
            // Committed: debit the dropped counters from the footprint gauges.
            // The freed segments are dead (live == 0), so only segment_count and
            // appended fall; reclaimable falls with appended.
            self.segment_gc_stats
                .apply_footprint_delta(-dropped, -(freed_appended as i64), 0);
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
                || sel.sel_live >= self.seal_threshold as u64 / 4
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
        // Durable barrier, as in the fast reclaim: seal the open buffer and flush so
        // the eligibility cutoff and the directory verify observe the durable view.
        {
            let _barrier = self.db.flush_barrier().write_owned().await;
            self.seal_open().await?;
            self.db.flush().await.map_err(|_| FsError::IoError)?;
        }

        let cur_epoch = self.segments.epoch();
        // Same eligibility cutoff as reclaim_segments_gated. A freshly-packed
        // compaction segment always carries a higher counter, so it is never
        // eligible even mid-compaction.
        let cutoff = self.open.lock().unwrap().segid.counter;
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
        // Unique extents the directory names (an extent can recur across rewrites).
        let want: HashSet<(InodeId, u64)> = dir.iter().map(|e| (e.inode, e.extent)).collect();
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

    /// Compact a set of fragmented/small segments: gather their still-live frames
    /// and repack them into fresh ~[`PACK_TARGET_BYTES`] segments, repointing each
    /// relocated extent. The drained sources become fully dead and are deleted by
    /// a later pass once past their horizon, so in-flight reads of the old
    /// locations stay valid.
    ///
    /// `atomic_prefix` lists the member counts of the leading atomic groups of
    /// `segids` (the admitted chain groups, in selection order); every source
    /// past their sum is an implicit singleton group. Pass `&[]` for
    /// all-singleton behavior.
    ///
    /// Returns (frames relocated, packed segments created, sources consumed):
    /// the gather stops between GROUPS at its RAM cap — a started group always
    /// completes, so a chain is never split into a seam-preserving partial
    /// repack — so `consumed` is the fully-gathered, group-granular prefix of
    /// `segids`; always >= the first group's size for a nonempty input,
    /// guaranteeing per-call progress. The caller must not drop the unconsumed
    /// rest from its backlog.
    ///
    /// The repoint is conditional and per-inode-locked: it holds the same write
    /// lock the foreground path uses and moves an extent only if it still points
    /// at the source frame, so a concurrent overwrite is never clobbered. The new
    /// segment is durably PUT (by `seal`) before any extent references it, so a
    /// crash between seal and repoint just leaves the source in place.
    pub async fn compact_segments(
        &self,
        segids: &[Segid],
        atomic_prefix: &[usize],
        // Plaintext gather RAM cap (config `compact_round_max_mib`); a leading
        // atomic group is still gathered whole, so this bounds RAM to one group.
        round_bytes: u64,
    ) -> Result<(usize, usize, usize), FsError> {
        debug_assert!(atomic_prefix.iter().sum::<usize>() <= segids.len());
        // Gather still-live frames across all sources, tagged with the exact
        // source FrameLoc the liveness read resolved (the repoint CAS's expected
        // value).
        let mut frames: Vec<(InodeId, u64, FrameLoc, Bytes)> = Vec::new();
        let mut gathered: u64 = 0;
        let mut consumed = 0usize;
        // Gather each live (inode, extent) at most once: an extent rewritten K times
        // within one seal window leaves K directory entries resolving to the same
        // current frame, and an extent can appear in more than one source directory.
        let mut seen: HashSet<(InodeId, u64)> = HashSet::new();
        let mut group_sizes = atomic_prefix
            .iter()
            .copied()
            .filter(|&g| g > 0)
            .chain(std::iter::repeat(1));
        while consumed < segids.len() {
            // Plaintext RAM bound only, unlike the selection budget (on-store
            // live bytes): checked before each group, so the first group is
            // always gathered whole and every call makes progress. A group
            // stopped mid-way would 1:1-repack a chain prefix that dissolves
            // no seam, so the cap never splits one; if it trips at a group
            // boundary the whole gather stops (`consumed` stays a prefix).
            if gathered >= round_bytes {
                break;
            }
            let group_end = (consumed + group_sizes.next().unwrap_or(1)).min(segids.len());
            while consumed < group_end {
                self.gather_source(segids[consumed], &mut seen, &mut frames, &mut gathered)
                    .await?;
                consumed += 1;
            }
        }

        info!(
            "compaction: gathered {} live frames ({}) from {} of {} source segment(s)",
            frames.len(),
            human_bytes(gathered),
            consumed,
            segids.len(),
        );

        // Group by (inode, extent) so a file's extents land contiguously in the new
        // segment (consecutive frame index + adjacent bytes), collapsing its reads
        // to a single ranged GET instead of one per scattered run. Sequential inode
        // ids also cluster same-directory files into the same/adjacent segments.
        frames.sort_by_key(|(inode, extent, _, _)| (*inode, *extent));

        // Cut into target-sized batches by SOURCE on-store bytes, never
        // between file-adjacent extents unless a single run alone exceeds the
        // target (see plan_batches). A final sliver under the small threshold
        // folds into its predecessor: sealed alone it would re-enter small
        // candidacy and be rewritten again.
        let metas: Vec<(InodeId, u64, u64)> = frames
            .iter()
            .map(|&(inode, extent, loc, _)| (inode, extent, loc.byte_len as u64))
            .collect();
        let mut bounds = plan_batches(&metas, PACK_TARGET_BYTES, PACK_CUT_SLACK_BYTES);
        if bounds.len() > 1 {
            let last_start = bounds[bounds.len() - 2];
            let last_store: u64 = metas[last_start..].iter().map(|m| m.2).sum();
            if last_store < SMALL_SEGMENT_BYTES {
                bounds.remove(bounds.len() - 2);
            }
        }
        let mut batches: Vec<PackBatch> = Vec::with_capacity(bounds.len());
        let mut iter = frames.into_iter();
        let mut prev = 0usize;
        for &end in &bounds {
            let mut batch = Vec::with_capacity(end - prev);
            let mut batch_src = Vec::with_capacity(end - prev);
            for _ in prev..end {
                let (inode, extent, src, bytes) = iter.next().expect("bounds within frames");
                batch.push((inode, extent, bytes));
                batch_src.push(src);
            }
            batches.push((batch, batch_src));
            prev = end;
        }

        // Seal + repoint each batch. A batch that repoints nothing (every
        // frame overwritten out from under us) left only an orphan segment,
        // so it doesn't count as packed output.
        let mut relocated = 0;
        let mut packed = 0;
        for (batch, batch_src) in batches {
            let n = self.seal_and_repoint(&batch, &batch_src).await?;
            relocated += n;
            packed += usize::from(n > 0);
        }
        Ok((relocated, packed, consumed))
    }

    /// Gather one source segment's still-live frames for [`Self::compact_segments`]:
    /// append them to `frames` (tagged with the resolved source [`FrameLoc`]),
    /// growing `seen` and the plaintext `gathered` total.
    async fn gather_source(
        &self,
        segid: Segid,
        seen: &mut HashSet<(InodeId, u64)>,
        frames: &mut Vec<(InodeId, u64, FrameLoc, Bytes)>,
        gathered: &mut u64,
    ) -> Result<(), FsError> {
        let dir = self
            .segments
            .read_directory(segid)
            .await
            .map_err(|_| FsError::IoError)?;

        // The unique, not-yet-gathered extents this source directory names.
        let mut want: Vec<(InodeId, u64)> = Vec::new();
        let mut local: HashSet<(InodeId, u64)> = HashSet::new();
        for e in &dir {
            let k = (e.inode, e.extent);
            if !seen.contains(&k) && local.insert(k) {
                want.push(k);
            }
        }

        // Resolve liveness in parallel: an extent is live in this source iff its
        // current FrameLoc still points here (point reads, mostly cache hits). A
        // not-live extent is left un-`seen` so a later source can still claim it.
        let mut live: Vec<(InodeId, u64, FrameLoc)> = stream::iter(want)
            .map(|(inode, extent)| {
                let store = self.clone();
                async move {
                    let key = store.key_codec.extent_key(inode, extent);
                    let enc = store
                        .db
                        .get_bytes(&key)
                        .await
                        .map_err(|_| FsError::IoError)?;
                    Ok::<_, FsError>(
                        enc.and_then(|b| FrameLoc::decode(&b))
                            .filter(|loc| loc.segid == segid)
                            .map(|loc| (inode, extent, loc)),
                    )
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect();

        // Coalesce contiguous live frames (consecutive index + adjacent bytes)
        // into runs, then read the runs in parallel — one ranged GET per run
        // instead of one per frame.
        live.sort_by_key(|(_, _, loc)| loc.frame_index);
        // Resolved location per slot, carried through to the batch cut and the
        // repoint CAS.
        let loc_of: HashMap<(InodeId, u64), FrameLoc> = live
            .iter()
            .map(|&(inode, extent, loc)| ((inode, extent), loc))
            .collect();
        let mut runs: Vec<CompactRun> = Vec::new();
        for (inode, extent, loc) in live {
            match runs.last_mut() {
                Some((byte_offset, total_len, first_frame, slots))
                    if *first_frame + slots.len() as u32 == loc.frame_index
                        && *byte_offset + *total_len as u64 == loc.byte_offset =>
                {
                    *total_len += loc.byte_len;
                    slots.push((inode, extent));
                }
                _ => runs.push((
                    loc.byte_offset,
                    loc.byte_len,
                    loc.frame_index,
                    vec![(inode, extent)],
                )),
            }
        }
        let read: Vec<Vec<(InodeId, u64, Bytes)>> = stream::iter(runs)
            .map(|(byte_offset, total_len, first_frame, slots)| {
                let store = self.clone();
                async move {
                    let bytes = store
                        .segments
                        .read_run(segid, byte_offset, total_len, first_frame, &slots)
                        .await
                        .map_err(|_| FsError::IoError)?;
                    Ok::<_, FsError>(
                        slots
                            .into_iter()
                            .zip(bytes)
                            .map(|((inode, extent), b)| (inode, extent, b))
                            .collect::<Vec<_>>(),
                    )
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .try_collect::<Vec<_>>()
            .await?;

        for (inode, extent, bytes) in read.into_iter().flatten() {
            seen.insert((inode, extent));
            *gathered += bytes.len() as u64;
            frames.push((inode, extent, loc_of[&(inode, extent)], bytes));
        }
        Ok(())
    }

    /// Seal one packed batch into a new segment and repoint the extents that still
    /// reference their source frame. `src[i]` is the gathered source [`FrameLoc`]
    /// of `batch[i]`, the swap's expected value.
    async fn seal_and_repoint(
        &self,
        batch: &[(InodeId, u64, Bytes)],
        src: &[FrameLoc],
    ) -> Result<usize, FsError> {
        let new_locs = self
            .segments
            .seal(batch)
            .await
            .map_err(|_| FsError::IoError)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT);

        // All frames in one seal go to one new segment.
        let new_segid = new_locs.first().map(|(_, _, loc)| loc.segid);

        // Group by inode so each conditional swap is taken under that inode's write
        // lock (excludes a concurrent foreground write to the same extent).
        let mut by_inode: HashMap<InodeId, Vec<(u64, FrameLoc, FrameLoc)>> = HashMap::new();
        for (i, (inode, extent, new_loc)) in new_locs.into_iter().enumerate() {
            by_inode
                .entry(inode)
                .or_default()
                .push((extent, src[i], new_loc));
        }

        let mut swapped = 0;
        for (inode, items) in by_inode {
            let _guard = self.lock_manager.acquire(inode).await;
            let mut txn = self.db.new_transaction()?;
            let mut any = false;
            for (extent, old_loc, new_loc) in items {
                let key = self.key_codec.extent_key(inode, extent);
                if let Some(enc) = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?
                    && let Some(loc) = FrameLoc::decode(&enc)
                    // Full-loc equality, not just the segid: a rewrite staged
                    // into the same source segment whose commit lands between
                    // the gather and this swap moves the pointer to a sibling
                    // frame, and swapping on the segid alone would revert the
                    // extent to the gathered (superseded) frame's bytes.
                    && loc == old_loc
                {
                    txn.put_bytes(&key, Bytes::copy_from_slice(&new_loc.encode()));
                    // Move the bytes only on a won CAS: after a lost race the
                    // relocated frame is dead weight (no live pointer) and must
                    // not be credited. Source debit is live-only; the packed
                    // frame credits both live and total.
                    self.seg_delta(&mut txn, old_loc.segid, -(loc.byte_len as i64), 0);
                    self.seg_delta(
                        &mut txn,
                        new_loc.segid,
                        new_loc.byte_len as i64,
                        new_loc.byte_len as i64,
                    );
                    swapped += 1;
                    any = true;
                }
            }
            if any {
                self.commit_via_coordinator(txn).await?;
            }
        }

        // Nothing repointed: every gathered frame was overwritten before the
        // CAS, leaving a pure orphan (PUT, no pointer, no counter). Delete it
        // now, best-effort; a crash here leaves it for the orphan sweep.
        let orphaned = swapped == 0;
        let mut orphan_note = "";
        if orphaned && let Some(segid) = new_segid {
            orphan_note = match self.segments.delete_segment(segid).await {
                Ok(()) => " (orphan, deleted)",
                Err(e) => {
                    warn!(
                        "compaction: delete of orphaned {segid:?} failed: {e}; \
                         left for the orphan sweep"
                    );
                    " (orphan, delete failed)"
                }
            };
        }
        info!(
            "compaction: packed {:?}: {} frames, {} repointed, {} discarded to concurrent writes{}",
            new_segid,
            batch.len(),
            swapped,
            batch.len() - swapped,
            orphan_note,
        );
        Ok(swapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::frame_codec::FrameCodec;
    use crate::segment::SEGMENT_INFO;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{ObjectStore, path::Path};
    use slatedb::{BlockTransformer, DbBuilder};

    async fn make() -> (ExtentStore, Arc<Db>) {
        let (store, db, _object_store) = make_with_compression(CompressionConfig::Lz4).await;
        (store, db)
    }

    async fn make_with_compression(
        compression: CompressionConfig,
    ) -> (ExtentStore, Arc<Db>, Arc<dyn ObjectStore>) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bt: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&[0u8; 32], CompressionConfig::default());
        let slatedb = Arc::new(
            DbBuilder::new(Path::from("t"), object_store.clone())
                .with_block_transformer(bt)
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        let db = Arc::new(Db::new(slatedb, None));
        let store = make_store(object_store.clone(), db.clone(), compression, 7);
        (store, db, object_store)
    }

    /// A store over given backing state, as a restarted process would build:
    /// fresh in-RAM state, and a higher epoch than any earlier incarnation's.
    fn make_store(
        object_store: Arc<dyn ObjectStore>,
        db: Arc<Db>,
        compression: CompressionConfig,
        epoch: u64,
    ) -> ExtentStore {
        let key_codec = Arc::new(KeyCodec::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, compression);
        let segments = Arc::new(SegmentStore::new(object_store, codec, epoch, None));
        ExtentStore::new(db, key_codec, segments, Arc::new(KeyedLockManager::new()))
            .with_seal_threshold(8 * 1024 * 1024)
    }

    async fn commit(store: &ExtentStore, txn: Transaction) {
        // No commit worker in these tests, so commit_via_coordinator takes its
        // fallback path: this test task is the sole segcount writer.
        store.commit_via_coordinator(txn).await.unwrap();
    }

    /// Apply a write through the store and to a byte-array model, asserting the
    /// full file reads back identically.
    async fn write_and_check(
        store: &ExtentStore,
        db: &Db,
        model: &mut Vec<u8>,
        offset: usize,
        bytes: &[u8],
    ) {
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                1,
                offset as u64,
                &Bytes::copy_from_slice(bytes),
                model.len() as u64,
            )
            .await
            .unwrap();
        commit(store, txn).await;
        store.apply_tail_update(1, tu);
        let end = offset + bytes.len();
        if model.len() < end {
            model.resize(end, 0);
        }
        model[offset..end].copy_from_slice(bytes);
        assert_read_matches(store, model).await;
    }

    async fn assert_read_matches(store: &ExtentStore, model: &[u8]) {
        if !model.is_empty() {
            let got = store.read(1, 0, model.len() as u64).await.unwrap();
            assert_eq!(got.as_ref(), model, "read does not match model");
        }
    }

    async fn frameloc_of(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        extent: u64,
    ) -> Option<FrameLoc> {
        let key = store.key_codec.extent_key(inode, extent);
        db.get_bytes(&key)
            .await
            .unwrap()
            .and_then(|b| FrameLoc::decode(&b))
    }

    /// The live component of a segment's `(live, total)` counter.
    async fn segcount_of(store: &ExtentStore, db: &Db, segid: Segid) -> u64 {
        segcount_pair_of(store, db, segid).await.0
    }

    /// The full `(live, total)` counter for a segment.
    async fn segcount_pair_of(store: &ExtentStore, db: &Db, segid: Segid) -> (u64, u64) {
        let key = store.key_codec.segcount_key(segid.epoch, segid.counter);
        db.get_bytes(&key)
            .await
            .unwrap()
            .and_then(|b| KeyCodec::decode_segcount(&b))
            .unwrap_or((0, 0))
    }

    /// The counter's ground truth: sum of live frame bytes an inode still points at
    /// in `segid` over `extents`.
    async fn live_bytes(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        extents: std::ops::Range<u64>,
        segid: Segid,
    ) -> u64 {
        let mut sum = 0;
        for e in extents {
            if let Some(l) = frameloc_of(store, db, inode, e).await
                && l.segid == segid
            {
                sum += l.byte_len as u64;
            }
        }
        sum
    }

    #[tokio::test]
    async fn segcount_tracks_live_bytes_across_overwrite_and_delete() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Three full extents land in the open segment; the counter equals their bytes.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 3 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..3, seg).await,
        );

        // Overwrite extent 1: old debited, new credited, the stale frame is dead
        // weight the counter does not count.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                EXTENT_SIZE as u64,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                3 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert_eq!(frameloc_of(&store, &db, inode, 1).await.unwrap().segid, seg);
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..3, seg).await,
        );

        // Delete extent 2: its bytes leave the counter.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 2, 3).await.unwrap();
        commit(&store, txn).await;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..2, seg).await,
        );

        // Delete the rest: the counter reaches exactly zero.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 0, 2).await.unwrap();
        commit(&store, txn).await;
        assert_eq!(segcount_of(&store, &db, seg).await, 0);
    }

    // The debit scan must find and debit *every* prior frame of a multi-extent
    // overwrite, not just the range endpoints: a missed debit over-counts live
    // bytes (a space leak), a double debit under-counts (GC could drop a live
    // segment). Fresh-write and single-extent-overwrite tests never make the
    // scan return more than one key, so cover the multi-key case explicitly.
    #[tokio::test]
    async fn segcount_debits_every_extent_of_a_multi_extent_overwrite() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Four full extents into the open segment.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 4 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..4, seg).await,
        );

        // Overwrite all four in one write: the scan returns four keys, and each
        // superseded frame must be debited. The counter then equals only the
        // four live (current) frames; any missed or extra debit breaks equality.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![2u8; 4 * EXTENT_SIZE]),
                4 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..4, seg).await,
        );
    }

    // `total` is the cumulative-appended-bytes denominator: credited with every frame,
    // never debited. So a delete drops `live` but leaves `total` put, and `total - live`
    // is the segment's dead (reclaimable) bytes.
    #[tokio::test]
    async fn segcount_total_is_monotonic_never_debited() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Two extents into the open segment: fresh, so every appended byte is live.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 2 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        let ext1_len = frameloc_of(&store, &db, inode, 1).await.unwrap().byte_len as u64;
        let (live0, total0) = segcount_pair_of(&store, &db, seg).await;
        assert_eq!(live0, total0, "fresh segment: every appended byte is live");

        // Delete extent 1: live loses its bytes; total is never debited.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 1, 2).await.unwrap();
        commit(&store, txn).await;
        let (live1, total1) = segcount_pair_of(&store, &db, seg).await;
        assert_eq!(
            total1, total0,
            "total is monotonic: a delete never debits it"
        );
        assert_eq!(live1, live0 - ext1_len, "live drops by the deleted frame");
        assert_eq!(
            total1,
            live1 + ext1_len,
            "total - live is the dead-frame bytes (the fragmentation denominator)"
        );
    }

    // Compaction moves live bytes from the drained sources onto the packed segment.
    #[tokio::test]
    async fn compaction_moves_counter_from_sources_to_packed() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Two sealed segments, one live extent each.
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        write_and_check(&store, &db, &mut model, EXTENT_SIZE, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, 1).await.unwrap().segid;
        assert_ne!(seg_a, seg_b);
        assert!(segcount_of(&store, &db, seg_a).await > 0);
        assert!(segcount_of(&store, &db, seg_b).await > 0);

        store
            .compact_segments(&[seg_a, seg_b], &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();

        // Sources drain to zero; the extents now point at one packed segment that
        // holds exactly their live bytes.
        assert_eq!(segcount_of(&store, &db, seg_a).await, 0);
        assert_eq!(segcount_of(&store, &db, seg_b).await, 0);
        let packed = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        assert_eq!(frameloc_of(&store, &db, 1, 1).await.unwrap().segid, packed);
        assert_ne!(packed, seg_a);
        assert_eq!(
            segcount_of(&store, &db, packed).await,
            live_bytes(&store, &db, 1, 0..2, packed).await,
        );
    }

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
    async fn single_and_multi_extent_write_read() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, b"hello").await;
        write_and_check(
            &store,
            &db,
            &mut model,
            3,
            &vec![7u8; EXTENT_SIZE * 2 + 100],
        )
        .await;
        write_and_check(&store, &db, &mut model, EXTENT_SIZE - 5, &[9u8; 20]).await;
    }

    #[tokio::test]
    async fn sparse_hole_reads_zero() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Write at a high offset; the gap before it is a hole.
        write_and_check(&store, &db, &mut model, 5 * EXTENT_SIZE, b"tail").await;
        // Read across the hole.
        let got = store.read(1, 0, 100).await.unwrap();
        assert_eq!(got.as_ref(), &vec![0u8; 100][..]);
    }

    #[tokio::test]
    async fn all_zero_write_makes_hole() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; EXTENT_SIZE + 10]).await;
        // Overwrite the first extent with zeros -> becomes a hole (key deleted).
        write_and_check(&store, &db, &mut model, 0, &vec![0u8; EXTENT_SIZE]).await;
        // The extent key for extent 0 must be gone.
        let key = store.key_codec.extent_key(1, 0);
        assert!(db.get_bytes(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncate_shrink_then_regrow_reads_zero() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![5u8; 3 * EXTENT_SIZE]).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .truncate(&mut txn, 1, model.len() as u64, 100)
            .await
            .unwrap();
        commit(&store, txn).await;
        model.truncate(100);
        assert_read_matches(&store, &model).await;

        // Regrow via a write past EOF; the gap must read as zeros.
        write_and_check(&store, &db, &mut model, 2 * EXTENT_SIZE, b"z").await;
    }

    #[tokio::test]
    async fn zero_range_punches_hole() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![8u8; 2 * EXTENT_SIZE + 50]).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .zero_range(&mut txn, 1, 100, EXTENT_SIZE as u64, model.len() as u64)
            .await
            .unwrap();
        commit(&store, txn).await;
        let end = (100 + EXTENT_SIZE).min(model.len());
        model[100..end].fill(0);
        assert_read_matches(&store, &model).await;
    }

    #[tokio::test]
    async fn contiguous_multiextent_read_is_one_ranged_get() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Four extents in ONE write -> one segment, frames contiguous.
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; 4 * EXTENT_SIZE]).await;
        store.seal_open().await.unwrap();

        let before = store.segments.read_calls();
        let got = store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        let gets = store.segments.read_calls() - before;

        assert_eq!(got.len(), 4 * EXTENT_SIZE);
        assert_eq!(got.as_ref(), model.as_slice());
        assert_eq!(
            gets, 1,
            "a contiguous 4-extent read must coalesce into one ranged GET"
        );
    }

    #[tokio::test]
    async fn writes_buffer_until_seal_with_read_your_writes() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A write commits its extent but issues no PUT and serves reads from RAM.
        write_and_check(&store, &db, &mut model, 0, &[7u8; 100]).await;
        assert_eq!(
            store.segments.list_segments().await.unwrap().len(),
            0,
            "a write must not PUT a segment"
        );
        assert_eq!(
            store.segments.read_calls(),
            0,
            "the read was served from the open buffer"
        );

        // Sealing PUTs exactly one segment. With no in-RAM segment cache, the read
        // after seal is a ranged GET (in production the object store's parts cache,
        // warmed at seal time, absorbs it; these tests wire up no such cache).
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 100).await.unwrap().as_ref(), &model[..]);
        assert_eq!(
            store.segments.read_calls(),
            1,
            "a read of a sealed segment is one ranged GET"
        );
    }

    // A stored FrameLoc whose byte range lies outside the open buffer (a torn or
    // corrupt LSM value — any 32-byte value decodes) must surface as EIO, not an
    // out-of-range panic that poisons the open-segment lock for every later writer.
    #[tokio::test]
    async fn corrupt_frameloc_into_open_segment_is_eio_not_panic() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 100]).await;

        // Fabricate a pointer at the current open segment with an impossible range.
        let bogus = FrameLoc {
            segid: store.open.lock().unwrap().segid,
            frame_index: 0,
            byte_offset: 1 << 40,
            byte_len: 4096,
        };
        let key = store.key_codec.extent_key(1, 5);
        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(&key, Bytes::copy_from_slice(&bogus.encode()));
        commit(&store, txn).await;

        assert!(matches!(store.get(1, 5).await, Err(FsError::IoError)));
        // The open-segment lock survived (not poisoned): writes still work.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 100]).await;
    }

    // A corrupt extent value inside a multi-extent scan is the same EIO as the
    // single-extent path — skipping it would serve that extent as fabricated
    // zeros while a single-extent read of the same key errors.
    #[tokio::test]
    async fn multi_extent_read_of_a_corrupt_value_is_eio_not_zeros() {
        let (store, db) = make().await;

        // Plant a torn (undecodable) value at extent 1, then read across
        // extents 0..=2 so the ranged-scan path (not `get`) resolves it.
        let key = store.key_codec.extent_key(1, 1);
        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(&key, Bytes::from_static(&[0u8; FrameLoc::ENCODED_LEN - 1]));
        commit(&store, txn).await;

        let r = store.read_range(1, 0, 3 * EXTENT_SIZE as u64, true).await;
        assert!(matches!(r, Err(FsError::IoError)));
    }

    // In `get`'s GC-repoint retry, a re-resolved value that fails to decode is the
    // same corrupt-value EIO as the primary path — treating it like an absent key
    // would fabricate a hole of zeros from a torn LSM value.
    #[test]
    fn reresolve_distinguishes_a_hole_from_a_corrupt_value() {
        // Absent: a concurrent truncate/unlink made the extent a genuine hole.
        assert!(matches!(
            ExtentStore::decode_reresolved_extent(1, 0, None),
            Ok(None)
        ));
        // Present but undecodable (torn/truncated): EIO, never a hole.
        let torn = Bytes::from_static(&[0u8; FrameLoc::ENCODED_LEN - 1]);
        assert!(matches!(
            ExtentStore::decode_reresolved_extent(1, 0, Some(&torn)),
            Err(FsError::IoError)
        ));
        // A decodable value passes through for the segid comparison.
        let loc = FrameLoc {
            segid: Segid::new(1, 2),
            frame_index: 3,
            byte_offset: 4,
            byte_len: 5,
        };
        let enc = Bytes::copy_from_slice(&loc.encode());
        assert_eq!(
            ExtentStore::decode_reresolved_extent(1, 0, Some(&enc)).unwrap(),
            Some(loc)
        );
    }

    // Not a correctness test: measures single-stream engine-side write
    // throughput (codec + open-buffer append + seal + commit, no protocol, no
    // inode lock) against an in-memory object store.
    //   cargo test --release --lib -- --ignored --nocapture bench_sequential_write
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_sequential_write_throughput() {
        for (name, compression) in [
            ("lz4", CompressionConfig::Lz4),
            ("zstd(3)", CompressionConfig::Zstd(3)),
        ] {
            for (shape, data) in [
                ("incompressible", incompressible(1, 256 * 1024)),
                ("zeros", vec![0u8; 256 * 1024]),
            ] {
                let (store, db, _object_store) = make_with_compression(compression).await;
                let chunk = data.len();
                let total: usize = 512 * 1024 * 1024;
                let payload = Bytes::from(data);
                let start = std::time::Instant::now();
                let mut size = 0u64;
                for i in 0..(total / chunk) {
                    let mut txn = db.new_transaction().unwrap();
                    let tu = store
                        .write(&mut txn, 1, (i * chunk) as u64, &payload, size)
                        .await
                        .unwrap();
                    commit(&store, txn).await;
                    store.apply_tail_update(1, tu);
                    size += chunk as u64;
                }
                let secs = start.elapsed().as_secs_f64();
                eprintln!(
                    "engine sequential write [{name}, {shape}, 256 KiB ops]: {:.0} MB/s",
                    total as f64 / secs / 1e6
                );
            }
        }
    }

    // A batch of >= PARALLEL_COMPRESS_MIN_FRAMES frames takes the rayon
    // pre-compression fan-out (multi-thread runtimes only; the other tests run
    // current-thread and take the inline branch) and must roundtrip identically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_batch_write_takes_parallel_compression_path() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        let data = incompressible(3, PARALLEL_COMPRESS_MIN_FRAMES * 2 * EXTENT_SIZE);
        write_and_check(&store, &db, &mut model, 0, &data).await;
    }

    #[tokio::test]
    async fn large_write_seals_async_then_drains() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Incompressible bytes, so the sealed buffer actually crosses the threshold
        // (a repeating pattern would compress below it and never seal).
        let n = store.seal_threshold + 4 * EXTENT_SIZE;
        let data = incompressible(0, n);
        // One >threshold write triggers a background seal; the data reads back
        // correctly whether still in flight (sealing buffer) or already sealed.
        write_and_check(&store, &db, &mut model, 0, &data).await;

        // The flush barrier drains every in-flight seal to the object store.
        store.seal_open().await.unwrap();
        assert!(!store.segments.list_segments().await.unwrap().is_empty());
        assert_eq!(
            store.read(1, 0, n as u64).await.unwrap().as_ref(),
            model.as_slice()
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

    // The footprint gauges are maintained incrementally off the commit path, so
    // they must track writes and overwrites with no reclaim pass, and always
    // agree with an authoritative scan of the same state.
    #[tokio::test]
    async fn footprint_gauges_track_writes_incrementally() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        use std::sync::atomic::Ordering::Relaxed;
        let m = store.segment_gc_stats();
        assert_eq!(m.appended_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 0);

        // Write 3 extents: appended + live grow, all live, no reclaim pass.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![7u8; 3 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let appended = m.appended_bytes.load(Relaxed);
        assert!(appended > 0);
        assert_eq!(m.live_bytes.load(Relaxed), appended, "all live");
        assert_eq!(m.reclaimable_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 1);
        // The incremental gauges match an authoritative scan of the same state.
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, appended);
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.segment_count, 1);

        // Overwrite extent 0: a new frame is appended and the old one becomes
        // dead weight, visible immediately with no reclaim pass.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                3 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert!(
            m.appended_bytes.load(Relaxed) > appended,
            "new frame appended"
        );
        assert!(m.reclaimable_bytes.load(Relaxed) > 0, "old frame now dead");
        assert!(m.live_bytes.load(Relaxed) < m.appended_bytes.load(Relaxed));
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, m.appended_bytes.load(Relaxed));
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.reclaimable_bytes, m.reclaimable_bytes.load(Relaxed));
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
    async fn compact_relocates_live_frames_and_skips_dead() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // One 3-extent write -> one segment S holding extents 0,1,2.
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; 3 * EXTENT_SIZE]).await;
        store.seal_open().await.unwrap();
        let s = store.segments.list_segments().await.unwrap();
        assert_eq!(s.len(), 1);
        let s = s[0];

        // Full-overwrite extent 1, seal -> a new segment; extent 1's frame moves off
        // S, so S is now partially dead (extents 0,2 live, extent 1 dead).
        write_and_check(
            &store,
            &db,
            &mut model,
            EXTENT_SIZE,
            &vec![2u8; EXTENT_SIZE],
        )
        .await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Compact S: extents 0,2 relocate; extent 1 (already moved off S) is skipped.
        let (swapped, packed, consumed) = store
            .compact_segments(&[s], &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();
        assert_eq!(consumed, 1);
        assert_eq!(
            swapped, 2,
            "two live frames relocated, the dead one skipped"
        );
        assert_eq!(packed, 1, "relocated into one packed segment");

        // S is now fully dead and reclaimable; data is unchanged.
        assert!(store.reclaim_segments(Utc::now(), None).await.unwrap().0 >= 1);
        let n = model.len() as u64;
        assert_eq!(
            store.read(1, 0, n).await.unwrap().as_ref(),
            model.as_slice()
        );
    }

    /// High-entropy (xorshift64), Lz4-incompressible bytes so segment sizes are
    /// predictable in size-threshold tests (the test codec compresses).
    fn incompressible(seed: usize, n: usize) -> Vec<u8> {
        let mut s = (seed as u64) ^ 0x243F_6A88_85A3_08D3;
        (0..n)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
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

    #[tokio::test]
    async fn compaction_groups_a_files_extents_for_one_get_reads() {
        let (store, db) = make().await;
        let d = |b: u8| Bytes::from(vec![b; EXTENT_SIZE]);
        // Interleave two files' extents in one open buffer -> one segment whose
        // frames are in write order: A0, B0, A1, B1, A2, B2.
        let mut sizes = [0u64, 0u64];
        for extent in 0..3u64 {
            for (i, inode) in [1u64, 2u64].into_iter().enumerate() {
                let mut txn = db.new_transaction().unwrap();
                let tu = store
                    .write(
                        &mut txn,
                        inode,
                        extent * EXTENT_SIZE as u64,
                        &d(inode as u8 * 10 + extent as u8),
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
        let s = store.segments.list_segments().await.unwrap();
        assert_eq!(s.len(), 1);

        // Compact -> frames regrouped by (inode, extent); drop the drained source.
        store
            .compact_segments(&s, &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();
        store.reclaim_segments(Utc::now(), None).await.unwrap();

        // File A's three extents are now contiguous, so the whole-file read is a
        // single ranged GET, not three.
        let before = store.segments.read_calls();
        let got = store.read(1, 0, 3 * EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(
            store.segments.read_calls() - before,
            1,
            "a file's extents are contiguous after compaction -> one GET"
        );
        let expect: Vec<u8> = (0..3u8).flat_map(|c| vec![10 + c; EXTENT_SIZE]).collect();
        assert_eq!(got.as_ref(), expect.as_slice());
    }

    /// Write `data` as inode 1's extent `extent` (extents written in increasing
    /// order, so the prior file size is `extent * EXTENT_SIZE`).
    async fn write_extent(store: &ExtentStore, db: &Db, extent: u64, data: &[u8]) {
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                1,
                extent * EXTENT_SIZE as u64,
                &Bytes::copy_from_slice(data),
                extent * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(store, txn).await;
        store.apply_tail_update(1, tu);
    }

    #[test]
    fn nomination_set_dedups_bounds_and_drains() {
        let mut s = NominationSet::default();
        s.push(Segid::new(1, 1));
        s.push(Segid::new(1, 1));
        assert_eq!(s.set.len(), 1, "re-nomination dedups");

        // Overflow by 10 past the cap: the 11 oldest (the (1,1) plus (2,0..=9))
        // are evicted, insertion-order first.
        for c in 0..(NOMINATION_SET_CAP as u64 + 10) {
            s.push(Segid::new(2, c));
        }
        assert_eq!(s.set.len(), NOMINATION_SET_CAP);
        assert!(!s.set.contains(&Segid::new(1, 1)));
        assert!(!s.set.contains(&Segid::new(2, 9)));
        assert!(s.set.contains(&Segid::new(2, 10)));

        let (drained, dropped) = s.drain();
        assert_eq!(drained.len(), NOMINATION_SET_CAP);
        assert_eq!(dropped, 11);
        assert!(s.set.is_empty() && s.order.is_empty());
        assert_eq!(s.dropped, 0, "drain resets the drop counter");
    }

    #[test]
    fn pair_stats_bump_once_per_round_reset_on_stale_and_drop_at_cap() {
        // Synthetic timeline: base + steps only (an Instant can't go backward).
        let t0 = Instant::now();
        let mut p = PairStats::default();
        let (a, b) = (Segid::new(1, 1), Segid::new(1, 2));
        p.bump(a, b, 5, t0);
        p.bump(b, a, 5, t0); // unordered + once-per-round: no second count
        assert_eq!(p.map[&(a, b)].count, 1);
        p.bump(a, b, 6, t0 + Duration::from_secs(60));
        assert_eq!(p.map[&(a, b)].count, 2);

        // A bump past the staleness window restarts the count, however many
        // rounds elapsed (staleness is wall-clock, not passes).
        p.bump(a, b, 7, t0 + Duration::from_secs(60) + PAIR_STALE_AFTER * 2);
        assert_eq!(p.map[&(a, b)].count, 1);

        // Self-pairs (internal seams) ride the same machinery.
        p.bump(a, a, 7, t0);
        p.bump(a, a, 8, t0);
        assert_eq!(p.map[&(a, a)].count, 2);

        // At capacity new pairs are dropped and counted; the sweep clears
        // stale entries and reports the drops.
        let mut p = PairStats::default();
        for c in 0..PAIR_STATS_CAP as u64 {
            p.bump(Segid::new(2, c), Segid::new(3, c), 100, t0);
        }
        p.bump(Segid::new(4, 1), Segid::new(4, 2), 100, t0);
        assert_eq!(p.map.len(), PAIR_STATS_CAP);
        let (hot, dropped) = p.sweep_and_hot(t0 + PAIR_STALE_AFTER * 2);
        assert_eq!(dropped, 1);
        assert!(hot.is_empty() && p.map.is_empty(), "stale entries swept");

        // Same-round re-crossings keep the seam fresh: with a long configured
        // pass interval, a pair continuously crossed within one round must
        // survive to its second episode instead of expiring mid-round.
        let mut p = PairStats::default();
        let minute = Duration::from_secs(60);
        p.bump(a, b, 1, t0);
        p.bump(a, b, 1, t0 + PAIR_STALE_AFTER - minute); // refresh, no episode
        p.sweep_and_hot(t0 + PAIR_STALE_AFTER + minute);
        assert_eq!(p.map[&(a, b)].count, 1, "survived the sweep via refresh");
        p.bump(a, b, 2, t0 + PAIR_STALE_AFTER + minute * 2);
        assert_eq!(p.map[&(a, b)].count, 2, "second episode reached");
    }

    /// A restart must not declare everything write-cold: prior-epoch segments
    /// prove nothing about write recency (a deploy mid-overwrite-workload),
    /// and cross-epoch counter distance is meaningless. Only quiescence covers
    /// them.
    #[test]
    fn prior_epoch_segments_are_not_write_cold_without_quiescence() {
        let ctx = ColdCtx {
            cur_epoch: 7,
            cutoff: 10,
            quiescent: false,
        };
        assert!(!ctx.write_cold(&Segid::new(6, 999)));
        assert!(!ctx.write_cold(&Segid::new(7, 9)));
        let aged = ColdCtx {
            cur_epoch: 7,
            cutoff: 50,
            quiescent: false,
        };
        assert!(aged.write_cold(&Segid::new(7, 10)));
        let quiescent = ColdCtx {
            quiescent: true,
            ..ctx
        };
        assert!(quiescent.write_cold(&Segid::new(6, 999)));
    }

    /// An oversized hot chain whose reserve-fitting prefix is a single member
    /// must be skipped, not trimmed: packing one member ~1:1 dissolves no
    /// seam, and on a quiescent store the seam re-heats against the fresh
    /// output and loops forever. A chain whose smallest edge would not fit
    /// even a fresh reserve is unpackable by this compactor: skipped without
    /// the deferral flag, or the pass would report phantom saturation forever.
    #[test]
    fn oversized_chain_never_admits_a_lone_member() {
        let a = (Segid::new(SEL_EPOCH, 100), 125 << 20, 120 << 20);
        let b = (Segid::new(SEL_EPOCH, 101), 125 << 20, 120 << 20);
        let sel = select_round(
            Vec::new(),
            &HashSet::new(),
            &[HotChain {
                members: vec![a, b],
                edges: vec![(0, 1)],
            }],
            Vec::new(),
            Some(5),
            ColdCtx {
                cur_epoch: SEL_EPOCH,
                cutoff: SEL_CUTOFF,
                quiescent: true,
            },
            MAX_COMPACT_BYTES_PER_ROUND,
        );
        assert!(sel.selected.is_empty(), "a 1:1 prefix repack must not run");
        assert!(sel.chain_groups.is_empty());
        assert_eq!(sel.chains_deferred, 0, "over-fresh-reserve is not backlog");
        assert_eq!(sel.chains_unpackable, 1);
    }

    /// An over-reserve BFS root must not strand a packable downstream
    /// sub-seam: with A too big for the whole reserve, the cheapest hot edge
    /// (B, C) admits as an atomic pair and dissolves that one seam.
    #[test]
    fn over_reserve_chain_root_falls_back_to_cheapest_edge() {
        let a = (Segid::new(SEL_EPOCH, 100), 200 << 20, 200 << 20);
        let b = (Segid::new(SEL_EPOCH, 101), 4 << 20, 4 << 20);
        let c = (Segid::new(SEL_EPOCH, 102), 4 << 20, 4 << 20);
        let sel = select_round(
            Vec::new(),
            &HashSet::new(),
            &[HotChain {
                members: vec![a, b, c],
                edges: vec![(0, 1), (1, 2)],
            }],
            Vec::new(),
            Some(5),
            ColdCtx {
                cur_epoch: SEL_EPOCH,
                cutoff: SEL_CUTOFF,
                quiescent: true,
            },
            MAX_COMPACT_BYTES_PER_ROUND,
        );
        assert_eq!(sel.selected, vec![b.0, c.0], "exactly the (B, C) pair");
        assert_eq!(sel.chain_groups, vec![2]);
        assert_eq!(sel.chains_deferred, 0);
    }

    /// A raised round budget packs a seam whose cheapest edge pair is over the
    /// default reserve: same chain, unpackable at the default 256 MiB round
    /// (128 MiB reserve), packed at a 512 MiB round (256 MiB reserve).
    #[test]
    fn raised_round_budget_packs_an_over_reserve_seam() {
        // The only edge pair carries 200 MiB live: over the 128 MiB default
        // reserve, within a 256 MiB one.
        let a = (Segid::new(SEL_EPOCH, 100), 100 << 20, 100 << 20);
        let b = (Segid::new(SEL_EPOCH, 101), 100 << 20, 100 << 20);
        let chain = [HotChain {
            members: vec![a, b],
            edges: vec![(0, 1)],
        }];
        let ctx = ColdCtx {
            cur_epoch: SEL_EPOCH,
            cutoff: SEL_CUTOFF,
            quiescent: true,
        };
        let pick = |round_bytes| {
            select_round(
                Vec::new(),
                &HashSet::new(),
                &chain,
                Vec::new(),
                Some(5),
                ctx,
                round_bytes,
            )
        };

        let sel = pick(MAX_COMPACT_BYTES_PER_ROUND);
        assert!(
            sel.selected.is_empty(),
            "over-reserve at the default budget"
        );
        assert_eq!(sel.chains_unpackable, 1);

        let sel = pick(512 << 20);
        assert_eq!(sel.selected, vec![a.0, b.0], "the pair packs at 512 MiB");
        assert_eq!(sel.chain_groups, vec![2]);
        assert_eq!(sel.chains_unpackable, 0);
    }

    /// A chain squeezed under two members by reserve CONTENTION (an earlier
    /// chain ate the reserve) whose cheapest edge would fit a fresh reserve
    /// must count as deferred, so the pass saturates and a fresh pass
    /// retries it.
    #[test]
    fn contention_squeezed_chain_defers_for_a_fresh_reserve() {
        let eater = HotChain {
            members: vec![
                (Segid::new(SEL_EPOCH, 100), 60 << 20, 60 << 20),
                (Segid::new(SEL_EPOCH, 101), 60 << 20, 60 << 20),
            ],
            edges: vec![(0, 1)],
        };
        // Over-reserve alone (140 MiB live), so it is not whole-deferred; its
        // cheapest edge (B, C) = 40 MiB fits a fresh reserve but not the 8
        // MiB the eater left.
        let squeezed = HotChain {
            members: vec![
                (Segid::new(SEL_EPOCH, 200), 100 << 20, 100 << 20),
                (Segid::new(SEL_EPOCH, 201), 20 << 20, 20 << 20),
                (Segid::new(SEL_EPOCH, 202), 20 << 20, 20 << 20),
            ],
            edges: vec![(0, 1), (1, 2)],
        };
        let sel = select_round(
            Vec::new(),
            &HashSet::new(),
            &[eater.clone(), squeezed.clone()],
            Vec::new(),
            Some(5),
            ColdCtx {
                cur_epoch: SEL_EPOCH,
                cutoff: SEL_CUTOFF,
                quiescent: true,
            },
            MAX_COMPACT_BYTES_PER_ROUND,
        );
        let eaten: Vec<Segid> = eater.members.iter().map(|&(s, _, _)| s).collect();
        assert_eq!(sel.selected, eaten, "only the first chain admitted");
        assert_eq!(sel.chain_groups, vec![2]);
        assert_eq!(
            sel.chains_deferred, 1,
            "a fresh-reserve-packable edge squeezed out by contention is backlog"
        );
    }

    #[tokio::test]
    async fn crossings_count_seams_not_reads_and_holes_break_adjacency() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Segments A (extents 0-1), B (2-3), C (4-5), all sealed.
        for extent in 0..6u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
            if extent % 2 == 1 {
                store.seal_open().await.unwrap();
            }
        }
        let a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let b = frameloc_of(&store, &db, 1, 2).await.unwrap().segid;
        let c = frameloc_of(&store, &db, 1, 4).await.unwrap().segid;

        // One pass over A|B|C pays each seam once; re-reading in the same GC
        // round adds nothing (burst reads are not episodes).
        store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
        store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
        {
            let stats = store.pair_stats.lock().unwrap();
            assert_eq!(stats.map.len(), 2);
            assert_eq!(stats.map[&PairStats::key(a, b)].count, 1);
            assert_eq!(stats.map[&PairStats::key(b, c)].count, 1);
        }

        // A hole between two segments is not a seam: the data isn't adjacent.
        let (store2, db2) = make().await;
        store2.enable_nominations();
        for extent in 0..2u64 {
            write_extent(&store2, &db2, extent, &[1u8; EXTENT_SIZE]).await;
        }
        store2.seal_open().await.unwrap();
        for extent in 3..5u64 {
            // Writing extent 3 with the file at 2 extents leaves extent 2 a hole.
            let mut txn = db2.new_transaction().unwrap();
            let tu = store2
                .write(
                    &mut txn,
                    1,
                    extent * EXTENT_SIZE as u64,
                    &Bytes::from(vec![2u8; EXTENT_SIZE]),
                    2 * EXTENT_SIZE as u64,
                )
                .await
                .unwrap();
            commit(&store2, txn).await;
            store2.apply_tail_update(1, tu);
        }
        store2.seal_open().await.unwrap();
        store2.read(1, 0, 5 * EXTENT_SIZE as u64).await.unwrap();
        assert!(store2.pair_stats.lock().unwrap().map.is_empty());
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

    /// A world where the plaintext RAM cap trips long before the on-store
    /// selection budget: two constant-fill (highly compressible) segments
    /// whose live plaintext alone overflows the gather cap, then three
    /// incompressible small segments that clear the economics gate on their
    /// own. All five tie on the live fraction, so selection keeps scan
    /// (counter) order and the compressible pair is always gathered first.
    async fn ram_capped_world() -> (ExtentStore, Arc<Db>, Vec<Segid>) {
        let (store, db) = make().await;
        let comp_extents = (MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 32;
        for (inode, fill) in [(1u64, 0x11u8), (2, 0x22)] {
            let mut txn = db.new_transaction().unwrap();
            let tu = store
                .write(
                    &mut txn,
                    inode,
                    0,
                    &Bytes::from(vec![fill; comp_extents * EXTENT_SIZE]),
                    0,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
            store.apply_tail_update(inode, tu);
            store.seal_open().await.unwrap();
        }
        for inode in 3u64..6 {
            let mut txn = db.new_transaction().unwrap();
            let tu = store
                .write(
                    &mut txn,
                    inode,
                    0,
                    &Bytes::from(incompressible(inode as usize, 28 * EXTENT_SIZE)),
                    0,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
            store.apply_tail_update(inode, tu);
            store.seal_open().await.unwrap();
        }
        let mut segids = Vec::new();
        for inode in 1u64..6 {
            let segid = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
            let (live, total) = segcount_pair_of(&store, &db, segid).await;
            assert_eq!(live, total, "world setup: fully live");
            assert!(
                total < SMALL_SEGMENT_BYTES,
                "world setup: every segment must be a small candidate"
            );
            segids.push(segid);
        }
        (store, db, segids)
    }

    /// The gather's RAM cap stops between sources; the never-gathered picks
    /// must stay in the drain and be consumed by later batches of the same
    /// pass, ending unsaturated once everything is packed.
    #[tokio::test]
    async fn ram_capped_gather_defers_sources_to_later_batches_of_the_pass() {
        let (store, db, segids) = ram_capped_world().await;
        let comp_extents = (MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 32;
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
            2 * comp_extents + 3 * 28,
            "later batches drained the RAM-cap leftovers"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: false },
            "fully drained: no backlog left"
        );
        for (i, inode) in (1u64..6).enumerate() {
            assert_ne!(
                frameloc_of(&store, &db, inode, 0).await.unwrap().segid,
                segids[i],
                "every source drained within one pass"
            );
        }
    }

    /// A pass cut before the leftovers are re-selected must report them as
    /// saturation (the fast cadence follows up), not silently drop them.
    #[tokio::test]
    async fn ram_capped_cut_with_unconsumed_leftovers_stays_saturated() {
        let (store, db, segids) = ram_capped_world().await;
        let comp_extents = (MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 32;
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
            outcome.relocated,
            2 * comp_extents,
            "one batch: only the gathered prefix packed"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: true },
            "un-consumed picks are backlog"
        );
        for inode in 1u64..3 {
            assert_ne!(
                frameloc_of(&store, &db, inode, 0).await.unwrap().segid,
                segids[(inode - 1) as usize],
                "gathered sources packed"
            );
        }
        for inode in 3u64..6 {
            assert_eq!(
                frameloc_of(&store, &db, inode, 0).await.unwrap().segid,
                segids[(inode - 1) as usize],
                "never-gathered sources left in place, not lost"
            );
        }
    }

    /// Write `bytes` to `inode` at extent offset `extent_off` (chunks
    /// appended in order, so the prior file size is the offset).
    async fn write_chunk_at(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        extent_off: u64,
        bytes: Bytes,
    ) {
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                extent_off * EXTENT_SIZE as u64,
                &bytes,
                extent_off * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(store, txn).await;
        store.apply_tail_update(inode, tu);
    }

    /// Capture this thread's INFO log lines while the returned guard lives
    /// (current-thread test runtime: every poll runs under the guard).
    /// DEFECT-1 regression: a chain whose HEAD member alone decodes past the
    /// gather's 256 MiB plaintext RAM cap must still gather whole (the group
    /// is atomic), not stop at consumed=1 — a 1:1 repack that dissolves no
    /// seam, spends the reserve, and re-heats forever on a quiescent store.
    /// Constant-fill (compressible) data keeps the members inside the
    /// on-store reserve while their plaintext overflows the cap; all-zero
    /// writes would elide into holes.
    #[tokio::test]
    async fn chain_with_head_over_ram_cap_packs_whole_in_one_pass() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Segment A: incompressible ballast for density (dense + fully live
        // means never a scan candidate — only the chain path may move it)
        // plus constant fill decoding past the cap alone.
        let ballast = 36u64;
        let comp = (MAX_COMPACT_BYTES_PER_ROUND as usize / EXTENT_SIZE + 64) as u64;
        let ballast_buf = incompressible(1, ballast as usize * EXTENT_SIZE);
        write_chunk_at(&store, &db, 1, 0, Bytes::from(ballast_buf.clone())).await;
        write_chunk_at(
            &store,
            &db,
            1,
            ballast,
            Bytes::from(vec![0x11u8; comp as usize * EXTENT_SIZE]),
        )
        .await;
        store.seal_open().await.unwrap();
        // Segment B: the same file continues into a dense second member.
        let a_extents = ballast + comp;
        let b_extents = 40u64;
        let b_buf = incompressible(2, b_extents as usize * EXTENT_SIZE);
        write_chunk_at(&store, &db, 1, a_extents, Bytes::from(b_buf.clone())).await;
        store.seal_open().await.unwrap();

        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, a_extents).await.unwrap().segid;
        assert_ne!(seg_a, seg_b);
        for s in [seg_a, seg_b] {
            let (live, total) = segcount_pair_of(&store, &db, s).await;
            assert_eq!(live, total, "world setup: fully live");
            assert!(total >= SMALL_SEGMENT_BYTES, "world setup: dense");
        }

        // Two boundary-crossing reads in distinct GC rounds arm the seam;
        // the negative control pins that one episode packs nothing.
        let boundary = (a_extents - 1) * EXTENT_SIZE as u64;
        store
            .read(1, boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        let (deleted, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!((deleted, relocated), (0, 0), "one episode: not hot yet");
        store
            .read(1, boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
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
        assert_eq!(
            outcome.relocated,
            (a_extents + b_extents) as usize,
            "consumed covers both members: the whole group gathered"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: false },
            "nothing deferred, nothing pending"
        );
        assert_eq!(
            outcome.chains,
            ChainOutcome {
                assembled: 1,
                packed: 1,
                deferred: 0,
                warm: 0,
                unpackable: 0,
            },
            "consumed-based count: the whole group packed as one chain",
        );

        // Merged: the whole file lives in one fresh segment — the seam and
        // both old members are gone.
        let now_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let now_b = frameloc_of(&store, &db, 1, a_extents).await.unwrap().segid;
        assert_ne!(now_a, seg_a);
        assert_ne!(now_b, seg_b);
        assert_eq!(now_a, now_b, "co-read data co-located: no more seam");

        // Integrity spot-checks: ballast, fill, and the seam region.
        let got = store.read(1, 0, EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(got.as_ref(), &ballast_buf[..EXTENT_SIZE]);
        let got = store
            .read(1, boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert_eq!(&got[..EXTENT_SIZE], &vec![0x11u8; EXTENT_SIZE][..]);
        assert_eq!(&got[EXTENT_SIZE..], &b_buf[..EXTENT_SIZE]);
    }

    /// DEFECT-1/2 regression with two chains: the first exhausts the RAM
    /// cap, so the second defers WHOLE — members un-consumed (in pending),
    /// the pass saturated, and only the actually-packed group counted — and
    /// packs on the next pass.
    #[tokio::test]
    async fn ram_cap_defers_second_chain_whole_and_packs_it_next_pass() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Chain 1 (inode 1): A1|A2 jointly decode past the cap. A1 carries
        // incompressible ballast so the packed output stays dense (no
        // small-candidate churn muddying the next pass).
        let ballast = 40u64;
        let half = ((MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 64) as u64;
        write_chunk_at(
            &store,
            &db,
            1,
            0,
            Bytes::from(incompressible(1, ballast as usize * EXTENT_SIZE)),
        )
        .await;
        write_chunk_at(
            &store,
            &db,
            1,
            ballast,
            Bytes::from(vec![0x11u8; half as usize * EXTENT_SIZE]),
        )
        .await;
        store.seal_open().await.unwrap();
        let a1_extents = ballast + half;
        write_chunk_at(
            &store,
            &db,
            1,
            a1_extents,
            Bytes::from(vec![0x22u8; half as usize * EXTENT_SIZE]),
        )
        .await;
        store.seal_open().await.unwrap();
        // Chain 2 (inode 2): B1|B2, dense and tiny-plaintext (never scan
        // candidates: only the chain path can move them).
        let b_extents = 40u64;
        write_chunk_at(
            &store,
            &db,
            2,
            0,
            Bytes::from(incompressible(2, b_extents as usize * EXTENT_SIZE)),
        )
        .await;
        store.seal_open().await.unwrap();
        write_chunk_at(
            &store,
            &db,
            2,
            b_extents,
            Bytes::from(incompressible(3, b_extents as usize * EXTENT_SIZE)),
        )
        .await;
        store.seal_open().await.unwrap();

        let a1 = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let a2 = frameloc_of(&store, &db, 1, a1_extents).await.unwrap().segid;
        let b1 = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        let b2 = frameloc_of(&store, &db, 2, b_extents).await.unwrap().segid;
        for s in [b1, b2] {
            let (live, total) = segcount_pair_of(&store, &db, s).await;
            assert_eq!(live, total, "world setup: fully live");
            assert!(total >= SMALL_SEGMENT_BYTES, "world setup: dense");
        }

        // Arm both seams across two GC rounds.
        let a_boundary = (a1_extents - 1) * EXTENT_SIZE as u64;
        let b_boundary = (b_extents - 1) * EXTENT_SIZE as u64;
        store
            .read(1, a_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        store
            .read(2, b_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        store
            .read(1, a_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        store
            .read(2, b_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Pass: chain 1 gathers whole and trips the cap at the group
        // boundary; chain 2 must defer whole, not split.
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
            outcome.relocated,
            (a1_extents + half) as usize,
            "only chain 1's members gathered"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: true },
            "the deferred chain's un-consumed picks are backlog"
        );
        assert_eq!(
            outcome.chains,
            ChainOutcome {
                assembled: 2,
                packed: 1,
                deferred: 1,
                warm: 0,
                unpackable: 0,
            },
            "the gather-cap-deferred group must not count as packed",
        );
        let now_a1 = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let now_a2 = frameloc_of(&store, &db, 1, a1_extents).await.unwrap().segid;
        assert_ne!(now_a1, a1);
        assert_ne!(now_a2, a2);
        assert_eq!(now_a1, now_a2, "chain 1 merged");
        assert_eq!(
            frameloc_of(&store, &db, 2, 0).await.unwrap().segid,
            b1,
            "deferred chain member left un-consumed"
        );
        assert_eq!(
            frameloc_of(&store, &db, 2, b_extents).await.unwrap().segid,
            b2,
            "deferred chain member left un-consumed"
        );

        // Next pass: the still-hot seam re-forms the chain, which now leads
        // an empty gather and packs whole.
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
            outcome.relocated,
            2 * b_extents as usize,
            "the deferred chain packed next pass"
        );
        assert_eq!(
            (outcome.chains.assembled, outcome.chains.packed),
            (1, 1),
            "the re-formed chain packs whole",
        );
        let now_b1 = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        let now_b2 = frameloc_of(&store, &db, 2, b_extents).await.unwrap().segid;
        assert_ne!(now_b1, b1);
        assert_eq!(now_b1, now_b2, "chain 2 merged: the seam is gone");

        // Integrity across both seams.
        let got = store
            .read(1, a_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert_eq!(&got[..EXTENT_SIZE], &vec![0x11u8; EXTENT_SIZE][..]);
        assert_eq!(&got[EXTENT_SIZE..], &vec![0x22u8; EXTENT_SIZE][..]);
        let got = store
            .read(2, b_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        let expect_b1 = incompressible(2, b_extents as usize * EXTENT_SIZE);
        let expect_b2 = incompressible(3, b_extents as usize * EXTENT_SIZE);
        assert_eq!(
            &got[..EXTENT_SIZE],
            &expect_b1[(b_extents as usize - 1) * EXTENT_SIZE..]
        );
        assert_eq!(&got[EXTENT_SIZE..], &expect_b2[..EXTENT_SIZE]);
    }

    #[tokio::test]
    async fn reads_nominate_only_enabled_fanned_out_and_on_store_segments() {
        let (store, db) = make().await;
        // Segment A holds extents 0-1, segment B extents 2-3.
        for extent in 0..4u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
            if extent % 2 == 1 {
                store.seal_open().await.unwrap();
            }
        }
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, 2).await.unwrap().segid;

        // Disabled (replica / pre-GC shape): a fanned-out read tracks nothing.
        store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());

        store.enable_nominations();

        // A read served by one segment is below the fan-out floor.
        store.read(1, 0, 2 * EXTENT_SIZE as u64).await.unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());

        // A read fanning out across both nominates both; re-reading dedups.
        store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        {
            let noms = store.nominations.lock().unwrap();
            assert_eq!(noms.set.len(), 2);
            assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
        }

        // RAM-served runs never count: extents 4-5 live in the open buffer, so
        // a read across B + open is one on-store segment — below the fan-out
        // floor (RAM runs cost no GETs, so the read isn't suffering).
        store.nominations.lock().unwrap().drain();
        for extent in 4..6u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
        }
        store
            .read(1, 2 * EXTENT_SIZE as u64, 4 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());

        // A read across A + B + open clears the floor on the two on-store
        // segments and still never nominates the open segid.
        store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
        {
            let open_segid = store.open.lock().unwrap().segid;
            let noms = store.nominations.lock().unwrap();
            assert_eq!(noms.set.len(), 2);
            assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
            assert!(!noms.set.contains(&open_segid));
        }
    }

    #[tokio::test]
    async fn per_call_cap_bounds_nominations() {
        let (store, db) = make().await;
        store.enable_nominations();
        // More single-extent segments than the per-call cap.
        let n = NOMINATE_PER_CALL_CAP as u64 + 2;
        for extent in 0..n {
            write_extent(&store, &db, extent, &[1u8; EXTENT_SIZE]).await;
            store.seal_open().await.unwrap();
        }
        store.read(1, 0, n * EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(
            store.nominations.lock().unwrap().set.len(),
            NOMINATE_PER_CALL_CAP
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

    // ---- Property tests over the pure policy pieces. The example tests
    // above pin concrete lifecycles; these pin the invariants for arbitrary
    // inputs, including the shapes too big to build from real segments
    // (oversized chains, reserve saturation).

    use proptest::prelude::*;

    const SEL_EPOCH: u64 = 7;
    const SEL_CUTOFF: u64 = 1_000_000;

    /// Raw generated world, assembled into typed inputs by `build_sel_world`:
    /// candidates as (live, extra_dead), the nomination mask, phantom
    /// nomination count, chain specs (members as ((live, extra_dead),
    /// parent_seed), extra edge seeds, young, overlap-a-candidate,
    /// prior-epoch-member), tail specs (live, extra_dead, young,
    /// prior-epoch, overlap-a-candidate), the scrub floor, and quiescence.
    /// Prior-epoch entries pin the restart rule (a pre-restart segment is
    /// write-cold only through quiescence); overlap entries pin no-duplicate
    /// selection when one segid legitimately appears in more than one input
    /// list. `parent_seed` attaches member k to an earlier member,
    /// reproducing the BFS-shape invariant of [`chain_components`] output
    /// and the hot pairs behind it; extra edge seeds add non-tree hot pairs
    /// (self-pairs included), as full components carry them.
    type SelWorldRaw = (
        Vec<(u64, u64)>,
        Vec<bool>,
        usize,
        Vec<(
            Vec<((u64, u64), usize)>,
            Vec<(usize, usize)>,
            bool,
            bool,
            bool,
        )>,
        Vec<(u64, u64, bool, bool, bool)>,
        Option<u64>,
        bool,
    );

    fn sel_world_raw() -> impl Strategy<Value = SelWorldRaw> {
        (
            prop::collection::vec((1u64..=(96 << 20), 0u64..=(200 << 20)), 0..70),
            prop::collection::vec(any::<bool>(), 70),
            0usize..5,
            prop::collection::vec(
                (
                    prop::collection::vec(
                        ((1u64..=(160 << 20), 0u64..=(64 << 20)), any::<usize>()),
                        1..6,
                    ),
                    prop::collection::vec((any::<usize>(), any::<usize>()), 0..4),
                    any::<bool>(),
                    any::<bool>(),
                    any::<bool>(),
                ),
                0..5,
            ),
            prop::collection::vec(
                (
                    1u64..=(200 << 20),
                    0u64..=(64 << 20),
                    any::<bool>(),
                    any::<bool>(),
                    any::<bool>(),
                ),
                0..20,
            ),
            // None = scrub disabled; rare, so the pass-3 laws keep coverage.
            prop::option::weighted(0.9, 1u64..=50),
            any::<bool>(),
        )
    }

    struct SelWorld {
        candidates: Vec<SegStat>,
        nominated: HashSet<Segid>,
        chains: Vec<HotChain>,
        tail: Vec<SegStat>,
        floor: Option<u64>,
        quiescent: bool,
    }

    fn build_sel_world(raw: SelWorldRaw) -> SelWorld {
        let (cands, nom_mask, phantoms, chain_specs, tail_specs, floor, quiescent) = raw;
        let candidates: Vec<SegStat> = cands
            .iter()
            .enumerate()
            .map(|(i, &(live, dead))| (Segid::new(SEL_EPOCH, i as u64), live + dead, live))
            .collect();
        let mut nominated: HashSet<Segid> = candidates
            .iter()
            .zip(&nom_mask)
            .filter_map(|(&(s, _, _), &m)| m.then_some(s))
            .collect();
        for i in 0..phantoms {
            nominated.insert(Segid::new(SEL_EPOCH, 900_000 + i as u64));
        }
        let mut chains: Vec<HotChain> = Vec::new();
        for (j, (members, extra_edges, young, overlap, prior)) in
            chain_specs.into_iter().enumerate()
        {
            let n = members.len();
            let mut built: Vec<SegStat> = members
                .iter()
                .enumerate()
                .map(|(k, &((live, dead), _))| {
                    (
                        Segid::new(SEL_EPOCH, 10_000 + (j * 100 + k) as u64),
                        live + dead,
                        live,
                    )
                })
                .collect();
            // Overlap: chain j may share candidate j (same segid, same stats —
            // one scan row backs both views).
            let overlapped = overlap && j < candidates.len();
            if overlapped {
                built[0] = candidates[j];
            }
            // Prior-epoch: the first member was sealed before the last
            // restart — write-cold only through quiescence.
            if prior && !overlapped {
                let (_, total, live) = built[0];
                built[0] = (Segid::new(SEL_EPOCH - 1, 40_000 + j as u64), total, live);
            }
            // Young: the last member is counter-young (unique per chain),
            // unless it is the overlap/prior slot.
            if young && !((overlapped || prior) && n == 1) {
                let (_, total, live) = built[n - 1];
                built[n - 1] = (
                    Segid::new(SEL_EPOCH, SEL_CUTOFF - 5 - j as u64),
                    total,
                    live,
                );
            }
            // Member k > 0 pairs with an earlier member (the BFS tree); a
            // lone member is a self-seam. Extras add non-tree hot pairs,
            // normalized like [`chain_components`] emits them.
            let mut edges: Vec<(usize, usize)> = if n == 1 {
                vec![(0, 0)]
            } else {
                members
                    .iter()
                    .enumerate()
                    .skip(1)
                    .map(|(k, &(_, seed))| (seed % k, k))
                    .collect()
            };
            for (x, y) in extra_edges {
                let (a, b) = (x % n, y % n);
                edges.push((a.min(b), a.max(b)));
            }
            edges.sort_unstable();
            edges.dedup();
            chains.push(HotChain {
                members: built,
                edges,
            });
        }
        // Tail entries live in their own counter range; a young one sits just
        // under the cutoff (distance 10..30, disjoint from chain-young 5..10);
        // a prior-epoch one models pre-restart data; an overlap one shares a
        // candidate's identity and stats.
        let tail: Vec<SegStat> = tail_specs
            .into_iter()
            .enumerate()
            .map(|(i, (live, dead, young, prior, overlap))| {
                if overlap && i < candidates.len() {
                    return candidates[i];
                }
                let (epoch, counter) = if prior {
                    (SEL_EPOCH - 1, 30_000 + i as u64)
                } else if young {
                    (SEL_EPOCH, SEL_CUTOFF - 10 - (i as u64 % 20))
                } else {
                    (SEL_EPOCH, 20_000 + i as u64)
                };
                (Segid::new(epoch, counter), live + dead, live)
            })
            .collect();
        SelWorld {
            candidates,
            nominated,
            chains,
            tail,
            floor,
            quiescent,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn select_round_invariants(raw in sel_world_raw(), round_mib in 64u64..=4096) {
            // The configurable per-round budget (compact_round_max_mib range).
            let round_bytes = round_mib << 20;
            let w = build_sel_world(raw);
            let sel = select_round(
                w.candidates.clone(),
                &w.nominated,
                &w.chains,
                w.tail.clone(),
                w.floor,
                ColdCtx {
                    cur_epoch: SEL_EPOCH,
                    cutoff: SEL_CUTOFF,
                    quiescent: w.quiescent,
                },
                round_bytes,
            );

            // Stats lookup over all inputs (overlap entries agree by construction).
            let stats: HashMap<Segid, (u64, u64)> = w
                .candidates
                .iter()
                .chain(w.chains.iter().flat_map(|c| c.members.iter()))
                .chain(w.tail.iter())
                .map(|&(s, total, live)| (s, (total, live)))
                .collect();

            // Budgets. The byte reserve is exact (fit test); the global byte
            // budget keeps the one-candidate overshoot envelope.
            prop_assert!(sel.selected.len() <= MAX_COMPACT_SEGMENTS_PER_ROUND);
            prop_assert!(sel.reserve_slots <= NOMINATED_MAX_PER_ROUND);
            prop_assert!(sel.reserve_live <= round_bytes / 2);
            let max_live = stats.values().map(|&(_, l)| l).max().unwrap_or(0);
            prop_assert!(sel.sel_live <= round_bytes.saturating_add(max_live));

            // No duplicates; conservation; sums match the inputs.
            let uniq: HashSet<&Segid> = sel.selected.iter().collect();
            prop_assert_eq!(uniq.len(), sel.selected.len());
            let (mut sum_size, mut sum_live) = (0u64, 0u64);
            for s in &sel.selected {
                let &(total, live) = stats.get(s).expect("selected segid not in inputs");
                sum_size += total;
                sum_live += live;
            }
            prop_assert_eq!(sum_size, sel.sel_size);
            prop_assert_eq!(sum_live, sel.sel_live);

            // Per-chain laws.
            let selected: HashSet<Segid> = sel.selected.iter().copied().collect();
            let cand_ids: HashSet<Segid> = w.candidates.iter().map(|&(s, _, _)| s).collect();
            let cold = |s: &Segid| {
                (s.epoch == SEL_EPOCH
                    && SEL_CUTOFF.saturating_sub(s.counter) >= CHAIN_OLD_COUNTER_DISTANCE)
                    || w.quiescent
            };
            let mut packable_chains = 0usize;
            for chain in &w.chains {
                let members = &chain.members;
                let all_cold = members.iter().all(|(s, _, _)| cold(s));
                if !all_cold {
                    // Vetoed: pass 0 admits nothing, so a member is selected
                    // only if it is independently a candidate.
                    for (s, _, _) in members {
                        if !cand_ids.contains(s) {
                            prop_assert!(!selected.contains(s), "vetoed chain member selected");
                        }
                    }
                    continue;
                }
                packable_chains += 1;
                // Pass-0 admission is a prefix of the GIVEN order or exactly
                // one 2-member hot-edge pair; over non-candidate members
                // (which no later pass can select) that shape is observable.
                let picked: Vec<usize> = members
                    .iter()
                    .enumerate()
                    .filter(|(_, (s, _, _))| !cand_ids.contains(s) && selected.contains(s))
                    .map(|(k, _)| k)
                    .collect();
                let unpicked_nc: Vec<usize> = members
                    .iter()
                    .enumerate()
                    .filter(|(_, (s, _, _))| !cand_ids.contains(s) && !selected.contains(s))
                    .map(|(k, _)| k)
                    .collect();
                let prefix_shaped = picked
                    .iter()
                    .all(|&k| unpicked_nc.iter().all(|&u| u > k));
                let edge_pair = picked.len() <= 2
                    && chain
                        .edges
                        .iter()
                        .any(|&(a, b)| a != b && picked.iter().all(|&k| k == a || k == b));
                prop_assert!(
                    prefix_shaped || edge_pair,
                    "pass-0 admission neither prefix nor hot-edge pair"
                );
                let chain_live: u64 = members.iter().map(|&(_, _, l)| l).sum();
                let fits_alone =
                    members.len() <= NOMINATED_MAX_PER_ROUND && chain_live <= round_bytes / 2;
                if fits_alone {
                    // All-or-nothing (modulo members that are also candidates,
                    // which later passes may select on their own merit).
                    let all_in = members.iter().all(|(s, _, _)| selected.contains(s));
                    if !all_in {
                        for (s, _, _) in members {
                            if !cand_ids.contains(s) {
                                prop_assert!(
                                    !selected.contains(s),
                                    "fits-alone chain was split"
                                );
                            }
                        }
                    }
                }
                // Seam dissolution, exact on candidate-disjoint chains: the
                // admitted group is a prefix or one hot-edge pair; one of
                // size >= 2 contains a hot edge; a lone admission is legal
                // only for a self-seam.
                if members.iter().all(|(s, _, _)| !cand_ids.contains(s)) {
                    let admitted: Vec<Segid> = members
                        .iter()
                        .map(|&(s, _, _)| s)
                        .filter(|s| selected.contains(s))
                        .collect();
                    let prefix: Vec<Segid> = members
                        .iter()
                        .take(admitted.len())
                        .map(|&(s, _, _)| s)
                        .collect();
                    let in_group: HashSet<Segid> = admitted.iter().copied().collect();
                    let exact_edge_pair = admitted.len() == 2
                        && chain.edges.iter().any(|&(a, b)| {
                            a != b
                                && in_group.contains(&members[a].0)
                                && in_group.contains(&members[b].0)
                        });
                    prop_assert!(
                        admitted == prefix || exact_edge_pair,
                        "admitted chain group is neither a prefix nor a hot-edge pair"
                    );
                    if admitted.len() >= 2 {
                        prop_assert!(
                            chain.edges.iter().any(|&(a, b)| a != b
                                && in_group.contains(&members[a].0)
                                && in_group.contains(&members[b].0)),
                            "admitted group of >= 2 contains no hot edge"
                        );
                    }
                    if admitted.len() == 1 {
                        prop_assert_eq!(members.len(), 1, "chain admission dissolved no seam");
                    }
                }
            }
            prop_assert!(sel.chain_groups.len() <= packable_chains);

            // chain_groups partitions the leading chain admissions: each
            // group's members come from one chain; a group of >= 2 carries a
            // hot edge; a lone group is a single-member self-seam.
            prop_assert!(sel.chain_groups.iter().sum::<usize>() <= sel.selected.len());
            let mut off = 0usize;
            for &g in &sel.chain_groups {
                prop_assert!(g >= 1);
                let group = &sel.selected[off..off + g];
                let chain = w
                    .chains
                    .iter()
                    .find(|c| c.members.iter().any(|&(s, _, _)| s == group[0]))
                    .expect("chain group member not from any chain");
                let midx: HashMap<Segid, usize> = chain
                    .members
                    .iter()
                    .enumerate()
                    .map(|(k, &(s, _, _))| (s, k))
                    .collect();
                let idxs: HashSet<usize> = group
                    .iter()
                    .map(|s| *midx.get(s).expect("chain group spans chains"))
                    .collect();
                if g >= 2 {
                    prop_assert!(
                        chain
                            .edges
                            .iter()
                            .any(|&(a, b)| a != b && idxs.contains(&a) && idxs.contains(&b)),
                        "packed group carries no hot edge"
                    );
                } else {
                    prop_assert_eq!(chain.members.len(), 1, "lone group from a multi-member chain");
                }
                off += g;
            }

            // Pass-3 laws. Tail segids are disjoint from candidates and chains
            // by construction, so every selected tail id was a pass-3
            // admission: it must be write-cold, above the floor, and counted;
            // and pass 3 never draws the reserve — with no candidates and no
            // chains, reserve accounting stays zero whatever the tail holds.
            // Overlap entries (a segid also in `candidates`) ride the
            // candidate passes and owe pass 3 nothing; the pass-3 laws apply
            // to tail-only ids, which no other pass can select.
            let mut tail_picked = 0usize;
            for &(s, total, live) in &w.tail {
                if cand_ids.contains(&s) {
                    continue;
                }
                if selected.contains(&s) {
                    tail_picked += 1;
                    prop_assert!(cold(&s), "tail admission not write-cold");
                    prop_assert!(w.floor.is_some(), "tail admission with the scrub disabled");
                    prop_assert!(
                        (total - live.min(total)).saturating_mul(100)
                            > total.saturating_mul(w.floor.unwrap()),
                        "tail admission at or below the floor"
                    );
                }
            }
            prop_assert_eq!(tail_picked, sel.tail_selected);
            if w.candidates.is_empty() && w.chains.is_empty() {
                prop_assert_eq!(sel.reserve_slots, 0);
                prop_assert_eq!(sel.reserve_live, 0);
                prop_assert_eq!(sel.chains_deferred, 0);
            }

            // Determinism.
            let again = select_round(
                w.candidates.clone(),
                &w.nominated,
                &w.chains,
                w.tail.clone(),
                w.floor,
                ColdCtx {
                    cur_epoch: SEL_EPOCH,
                    cutoff: SEL_CUTOFF,
                    quiescent: w.quiescent,
                },
                round_bytes,
            );
            prop_assert_eq!(sel, again);
        }

        // Deferral means "retry under a fresh reserve", so it can only arise
        // from contention: a lone chain already has the whole reserve — it
        // either admits something or is unpackable and must not set the
        // flag (phantom saturation would spin the fast cadence forever).
        #[test]
        fn lone_chain_on_a_fresh_reserve_never_defers(
            raw in sel_world_raw(),
            round_mib in 64u64..=4096,
        ) {
            let round_bytes = round_mib << 20;
            let w = build_sel_world(raw);
            for chain in &w.chains {
                let sel = select_round(
                    Vec::new(),
                    &HashSet::new(),
                    std::slice::from_ref(chain),
                    Vec::new(),
                    w.floor,
                    ColdCtx {
                        cur_epoch: SEL_EPOCH,
                        cutoff: SEL_CUTOFF,
                        quiescent: w.quiescent,
                    },
                    round_bytes,
                );
                if sel.selected.is_empty() {
                    prop_assert_eq!(sel.chains_deferred, 0, "lone chain deferred on a fresh reserve");
                }
            }
        }

        // The anti-forgery core: however many times a pass's reads bump a
        // pair within one round, it counts once — heat needs distinct rounds.
        #[test]
        fn pair_heat_needs_distinct_rounds(
            bumps in prop::collection::vec((0u64..16, 0u64..16), 1..300),
            round in 0u64..1000,
        ) {
            let mut p = PairStats::default();
            let t0 = Instant::now();
            for (a, b) in bumps {
                p.bump(Segid::new(1, a), Segid::new(1, b), round, t0);
            }
            for s in p.map.values() {
                prop_assert_eq!(s.count, 1);
            }
        }

        // Count never exceeds the number of distinct rounds a pair was bumped
        // in, regardless of interleaving; the map stays bounded; symmetry.
        #[test]
        fn pair_stats_bounded_and_episode_capped(
            ops in prop::collection::vec((0u64..8, 0u64..8, 0u64..4), 0..300),
        ) {
            let mut p = PairStats::default();
            let mut round = 0u64;
            // Synthetic timeline at the base cadence: one minute per round.
            let t0 = Instant::now();
            let at = |round: u64| t0 + Duration::from_secs(round * 60);
            let mut rounds_seen: HashMap<(Segid, Segid), HashSet<u64>> = HashMap::new();
            for (a, b, advance) in ops {
                round += advance;
                let (sa, sb) = (Segid::new(1, a), Segid::new(1, b));
                p.bump(sa, sb, round, at(round));
                rounds_seen
                    .entry(PairStats::key(sa, sb))
                    .or_default()
                    .insert(round);
            }
            prop_assert!(p.map.len() <= PAIR_STATS_CAP);
            for (k, s) in &p.map {
                prop_assert!(s.count >= 1);
                prop_assert!((s.count as usize) <= rounds_seen[k].len());
            }
            // Sweep leaves nothing stale.
            let now = at(round);
            let (hot, _) = p.sweep_and_hot(now);
            for s in p.map.values() {
                prop_assert!(now.saturating_duration_since(s.last_seen) <= PAIR_STALE_AFTER);
            }
            for pair in &hot {
                prop_assert!(p.map[pair].count >= PAIR_HOT_MIN);
            }
        }

        // last_seen must track the LATEST crossing, deduplicated bumps
        // included, or a long pass interval would expire actively-crossed
        // seams mid-round.
        #[test]
        fn pair_last_seen_tracks_the_latest_crossing(
            ops in prop::collection::vec((0u64..6, 0u64..6, 0u64..3, 0u64..600u64), 1..200),
        ) {
            let mut p = PairStats::default();
            let t0 = Instant::now();
            let (mut round, mut secs) = (0u64, 0u64);
            let mut latest: HashMap<(Segid, Segid), u64> = HashMap::new();
            for (a, b, round_adv, secs_adv) in ops {
                round += round_adv;
                secs += secs_adv;
                let (sa, sb) = (Segid::new(1, a), Segid::new(1, b));
                p.bump(sa, sb, round, t0 + Duration::from_secs(secs));
                latest.insert(PairStats::key(sa, sb), secs);
            }
            for (k, s) in &p.map {
                prop_assert_eq!(s.last_seen, t0 + Duration::from_secs(latest[k]));
            }
        }

        #[test]
        fn nomination_set_bounded_and_consistent(
            pushes in prop::collection::vec(0u64..2000, 0..3000),
        ) {
            let mut s = NominationSet::default();
            for &c in &pushes {
                s.push(Segid::new(1, c));
            }
            prop_assert!(s.set.len() <= NOMINATION_SET_CAP);
            prop_assert_eq!(s.set.len(), s.order.len());
            for sg in &s.order {
                prop_assert!(s.set.contains(sg));
            }
            // A segid can be pushed, evicted, and freshly pushed again, so
            // `dropped` is bounded below by distinct − cap, not equal to it.
            let distinct: HashSet<u64> = pushes.iter().copied().collect();
            if distinct.len() <= NOMINATION_SET_CAP {
                prop_assert_eq!(s.set.len(), distinct.len());
                prop_assert_eq!(s.dropped, 0);
            } else {
                prop_assert_eq!(s.set.len(), NOMINATION_SET_CAP);
                prop_assert!(s.dropped as usize >= distinct.len() - NOMINATION_SET_CAP);
            }
            let (drained, _) = s.drain();
            prop_assert_eq!(drained.len(), distinct.len().min(NOMINATION_SET_CAP));
            prop_assert!(s.set.is_empty() && s.order.is_empty());
        }

        // Oracle: components must equal graph connectivity (BFS reference),
        // and the member order must carry the connected-prefix property.
        #[test]
        fn chain_components_match_bfs(
            raw in prop::collection::vec((0u64..30, 0u64..30), 0..40),
        ) {
            let pairs: Vec<(Segid, Segid)> = raw
                .into_iter()
                .map(|(a, b)| (Segid::new(1, a), Segid::new(1, b)))
                .collect();
            let comps = chain_components(&pairs);

            // Partition: every pair endpoint in exactly one component.
            let nodes: HashSet<Segid> = pairs.iter().flat_map(|&(a, b)| [a, b]).collect();
            let mut comp_of: HashMap<Segid, usize> = HashMap::new();
            for (ci, comp) in comps.iter().enumerate() {
                prop_assert!(!comp.members.is_empty());
                for &s in &comp.members {
                    prop_assert!(comp_of.insert(s, ci).is_none(), "segid in two components");
                }
            }
            prop_assert_eq!(&comp_of.keys().copied().collect::<HashSet<_>>(), &nodes);

            // Edge laws: endpoints within the component, normalized and
            // deduped; every edge is an input pair; every input pair appears
            // among its component's edges (self-pairs as (i, i)).
            for comp in &comps {
                for &(a, b) in &comp.edges {
                    prop_assert!(a <= b && b < comp.members.len(), "edge outside component");
                    prop_assert!(
                        pairs.iter().any(|&(x, y)| (x, y)
                            == (comp.members[a], comp.members[b])
                            || (y, x) == (comp.members[a], comp.members[b])),
                        "edge without a backing hot pair"
                    );
                }
                prop_assert!(comp.edges.windows(2).all(|w| w[0] < w[1]), "edges not sorted+deduped");
            }
            for &(x, y) in &pairs {
                let comp = &comps[comp_of[&x]];
                prop_assert_eq!(comp_of[&y], comp_of[&x], "pair split across components");
                let ix = comp.members.iter().position(|s| *s == x).unwrap();
                let iy = comp.members.iter().position(|s| *s == y).unwrap();
                prop_assert!(
                    comp.edges.contains(&(ix.min(iy), ix.max(iy))),
                    "input hot pair missing from component edges"
                );
            }

            // BFS connectivity oracle: same component iff connected.
            let mut adj: HashMap<Segid, HashSet<Segid>> = HashMap::new();
            for &(a, b) in &pairs {
                adj.entry(a).or_default().insert(b);
                adj.entry(b).or_default().insert(a);
            }
            let mut bfs_comp: HashMap<Segid, usize> = HashMap::new();
            let mut next = 0usize;
            let mut sorted_nodes: Vec<Segid> = nodes.iter().copied().collect();
            sorted_nodes.sort_by_key(|s| (s.epoch, s.counter));
            for &start in &sorted_nodes {
                if bfs_comp.contains_key(&start) {
                    continue;
                }
                let mut queue = vec![start];
                while let Some(n) = queue.pop() {
                    if bfs_comp.insert(n, next).is_none() {
                        queue.extend(adj[&n].iter().copied());
                    }
                }
                next += 1;
            }
            for x in &nodes {
                for y in &nodes {
                    prop_assert_eq!(
                        comp_of[x] == comp_of[y],
                        bfs_comp[x] == bfs_comp[y],
                        "component partition diverges from connectivity"
                    );
                }
            }

            // Order laws: first member is the component's smallest; every
            // later member shares a hot pair with an earlier member (so any
            // >= 2 prefix dissolves a seam).
            for comp in &comps {
                let members = &comp.members;
                let min = members.iter().min_by_key(|s| (s.epoch, s.counter)).unwrap();
                prop_assert_eq!(&members[0], min);
                for (k, s) in members.iter().enumerate().skip(1) {
                    prop_assert!(
                        members[..k].iter().any(|e| adj[s].contains(e)),
                        "member not adjacent to any earlier member"
                    );
                }
            }

            // Determinism: pair order must not matter.
            let mut rev = pairs.clone();
            rev.reverse();
            prop_assert_eq!(&chain_components(&rev), &comps);
        }

        // The batch-cut plan: exact partition, seam-safe boundaries, bounded
        // batches, determinism.
        #[test]
        fn plan_batches_invariants(
            raw in prop::collection::vec((0u64..4, 0u64..12, 1u64..2000), 0..200),
            target in 500u64..4000,
            slack in 0u64..1500,
        ) {
            let mut metas: Vec<(InodeId, u64, u64)> = raw;
            metas.sort();
            metas.dedup_by(|a, b| (a.0, a.1) == (b.0, b.1));
            let bounds = plan_batches(&metas, target, slack);
            if metas.is_empty() {
                prop_assert!(bounds.is_empty());
                return Ok(());
            }

            // Partition: strictly increasing boundaries ending at len — every
            // extent in exactly one batch, order preserved, no empty batch.
            prop_assert_eq!(*bounds.last().unwrap(), metas.len());
            let mut prev = 0usize;
            for &b in &bounds {
                prop_assert!(b > prev, "empty or out-of-order batch");
                prev = b;
            }

            let adj = |k: usize| {
                let (ia, ea, _) = metas[k - 1];
                let (ib, eb, _) = metas[k];
                ia == ib && ea.checked_add(1) == Some(eb)
            };

            // Seam-safety: a boundary between file-adjacent extents is legal
            // only when their containing run alone exceeds the target.
            for &b in &bounds[..bounds.len() - 1] {
                if adj(b) {
                    let mut r = b;
                    while r > 0 && adj(r) {
                        r -= 1;
                    }
                    let mut e = b + 1;
                    while e < metas.len() && adj(e) {
                        e += 1;
                    }
                    let run: u64 = metas[r..e].iter().map(|m| m.2).sum();
                    prop_assert!(run > target, "run split although it fits a batch");
                }
            }

            // Bounded batches: only a single extent may exceed the target.
            let mut start = 0usize;
            for &b in &bounds {
                let sum: u64 = metas[start..b].iter().map(|m| m.2).sum();
                prop_assert!(sum <= target || b - start == 1, "over-target multi-extent batch");
                start = b;
            }

            // Determinism.
            prop_assert_eq!(&plan_batches(&metas, target, slack), &bounds);
        }
    }

    #[test]
    fn read_ahead_planner() {
        let w = READ_AHEAD_WINDOW_BYTES;
        // First read of a stream: unconfirmed, no prefetch.
        let (s, p) = plan_read_ahead((0, 0, 0), 0, 1000);
        assert_eq!(s, (1000, 1000, 1));
        assert_eq!(p, None);
        // Second sequential read: confirmed -> prefetch a window ahead.
        let (s, p) = plan_read_ahead(s, 1000, 1000);
        assert_eq!(s, (2000, 2000 + w, 2));
        assert_eq!(p, Some((2000, 2000 + w)));
        // Still deep in the buffered-ahead: no new prefetch.
        let (s, p) = plan_read_ahead(s, 2000, 1000);
        assert_eq!(s, (3000, 2000 + w, 3));
        assert_eq!(p, None);
        // A non-contiguous jump resets to unconfirmed.
        let (s, p) = plan_read_ahead(s, 5_000_000, 1000);
        assert_eq!(s, (5_001_000, 5_001_000, 1));
        assert_eq!(p, None);
        // Less than half a window buffered ahead -> refill.
        let low = (2000 + w / 2 + 1, 2000 + w, 9);
        let (_, p) = plan_read_ahead(low, 2000 + w / 2 + 1, 100);
        assert!(
            p.is_some(),
            "refills when under half a window remains ahead"
        );
    }

    #[tokio::test]
    async fn read_ahead_spawns_only_on_confirmed_sequential() {
        let (store, _db) = make().await;
        // First read of a stream: nothing spawned (could be a one-off).
        assert!(store.trigger_read_ahead(1, 0, 1000).is_none());
        // Second sequential read: a read-ahead task is spawned.
        let h = store.trigger_read_ahead(1, 1000, 1000);
        assert!(h.is_some(), "confirmed sequential -> read-ahead");
        h.unwrap().await.unwrap();
        // A non-contiguous jump: nothing spawned.
        assert!(store.trigger_read_ahead(1, 9_000_000, 1000).is_none());
    }

    #[tokio::test]
    async fn read_ahead_skip_at_cap_keeps_coverage_honest() {
        let (store, _db) = make().await;
        // Hold every permit so the planned prefetch is skipped at the cap.
        let held: Vec<_> = (0..READ_AHEAD_MAX_CONCURRENT)
            .map(|_| Arc::clone(&store.prefetch_sem).try_acquire_owned().unwrap())
            .collect();
        assert!(store.trigger_read_ahead(1, 0, 1000).is_none());
        assert!(
            store.trigger_read_ahead(1, 1000, 1000).is_none(),
            "at the cap -> skip"
        );
        // The skip must not count the window as covered: `prefetched_to`
        // stays at the read end, so the next read re-plans.
        let (_, prefetched_to, _) = store.read_ahead.get(&1).map(|e| *e).unwrap();
        assert_eq!(prefetched_to, 2000, "skipped window recorded as covered");
        drop(held);
        let h = store.trigger_read_ahead(1, 2000, 1000);
        assert!(h.is_some(), "freed permit -> the next read re-triggers");
        h.unwrap().await.unwrap();
    }

    #[tokio::test]
    async fn sequential_append_via_tail_cache() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Many small appends into the same tail extent: exercises the tail cache
        // (each append RMWs the cached tail rather than re-fetching the frame).
        for _ in 0..50 {
            let off = model.len();
            write_and_check(&store, &db, &mut model, off, b"0123456789").await;
        }
        assert_eq!(model.len(), 500);
    }
}
