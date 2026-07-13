//! Compaction selection policy, store-free and property-testable: read
//! heat (nominations, crossing-pair stats), hot-seam chain assembly,
//! write-cold classification, and the per-round selection.

use crate::segment::Segid;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Per-round compaction bounds, so a large backlog is worked down incrementally.
pub(super) const MAX_COMPACT_SEGMENTS_PER_ROUND: usize = 64;
#[cfg(test)]
pub(super) const MAX_COMPACT_BYTES_PER_ROUND: u64 = 256 << 20; // 256 MiB (~one PACK_TARGET segment/round)

/// Distinct on-store segments a demand read must span to nominate them.
/// Nominations re-order selection among scan-qualified candidates; they
/// never create candidacy.
pub(super) const NOMINATE_MIN_FANOUT: usize = 2;
/// Per-read nomination cap. One fragmented read must not evict the whole set.
pub(super) const NOMINATE_PER_CALL_CAP: usize = 16;
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
pub(super) const PAIR_BUMPS_PER_CALL: usize = 16;
/// Write-cold counter distance. Counters advance ~once per flush, so
/// distance approximates write recency; a packed output carries a fresh
/// counter, making the same rule the anti-churn cooldown.
const CHAIN_OLD_COUNTER_DISTANCE: u64 = 30;

/// A segment as the segcount scan saw it: (segid, total bytes, live bytes).
pub(super) type SegStat = (Segid, u64, u64);

/// Write-cold inputs, computed once per pass after the barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ColdCtx {
    pub(super) cur_epoch: u64,
    pub(super) cutoff: u64,
    pub(super) quiescent: bool,
}

impl ColdCtx {
    /// Counter distance is valid only within the current epoch (counters
    /// restart per open, and a restart is no evidence of write-coldness);
    /// prior-epoch segments qualify only through quiescence.
    pub(super) fn write_cold(&self, s: &Segid) -> bool {
        (s.epoch == self.cur_epoch
            && self.cutoff.saturating_sub(s.counter) >= CHAIN_OLD_COUNTER_DISTANCE)
            || self.quiescent
    }
}

/// Read-nominated compaction hints: bounded, deduping, insertion-ordered.
#[derive(Default)]
pub(super) struct NominationSet {
    pub(super) set: HashSet<Segid>,
    pub(super) order: VecDeque<Segid>,
    /// Evictions since the last drain, for the pass summary.
    pub(super) dropped: u64,
}

impl NominationSet {
    /// Deduped insert; re-nominating keeps the original queue position. At
    /// capacity the oldest nomination is evicted.
    pub(super) fn push(&mut self, segid: Segid) {
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
    pub(super) fn drain(&mut self) -> (HashSet<Segid>, u64) {
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
pub(super) struct PairStat {
    pub(super) count: u32,
    pub(super) last_round: u64,
    pub(super) last_seen: Instant,
}

/// Crossing-pair statistics. A self-pair is an internal seam (adjacent
/// extents scattered within one segment).
#[derive(Default)]
pub(super) struct PairStats {
    pub(super) map: HashMap<(Segid, Segid), PairStat>,
    /// Pairs dropped at capacity since the last sweep, for the pass summary.
    pub(super) dropped: u64,
}

impl PairStats {
    /// Pairs are unordered.
    pub(super) fn key(a: Segid, b: Segid) -> (Segid, Segid) {
        if (b.epoch, b.counter) < (a.epoch, a.counter) {
            (b, a)
        } else {
            (a, b)
        }
    }

    pub(super) fn bump(&mut self, a: Segid, b: Segid, round: u64, now: Instant) {
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
    pub(super) fn sweep_and_hot(&mut self, now: Instant) -> (Vec<(Segid, Segid)>, u64) {
        self.map
            .retain(|_, s| now.saturating_duration_since(s.last_seen) <= PAIR_STALE_AFTER);
        let mut hot: Vec<(Segid, Segid)> = self
            .map
            .iter()
            .filter(|(_, s)| s.count >= PAIR_HOT_MIN)
            .map(|(k, _)| *k)
            .collect();
        // Map order is arbitrary; assembly order must not be.
        hot.sort_unstable();
        (hot, std::mem::take(&mut self.dropped))
    }
}

/// Live fraction in permille, the fragmentation rank key (lower = more dead).
pub(super) fn live_permille(live: u64, total: u64) -> u64 {
    live.saturating_mul(1000) / total.max(1)
}

/// A hot-seam connected component: ordered members plus every input hot
/// pair between two members, as index pairs into `members`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ChainComponent {
    pub(super) members: Vec<Segid>,
    pub(super) edges: Vec<(usize, usize)>,
}

/// Union hot pairs into connected components.
pub(super) fn chain_components(hot_pairs: &[(Segid, Segid)]) -> Vec<ChainComponent> {
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

/// One reclaim pass's compaction pick (see [`select_round`]).
#[derive(Debug, PartialEq)]
pub(super) struct RoundSelection {
    pub(super) selected: Vec<Segid>,
    pub(super) sel_size: u64,
    pub(super) sel_live: u64,
    /// Reserve accounting: selections admitted ahead of the fragmentation
    /// ranking (chains + nominated candidates), capped at half the round.
    pub(super) reserve_slots: usize,
    pub(super) reserve_live: u64,
    pub(super) nominated_selected: usize,
    /// Admitted chain groups' member counts, in selection order. Chain
    /// admissions are the leading prefix of `selected`, so this is also
    /// `compact_segments`' atomic-group prefix. A group is *packed* only
    /// once the gather consumes all its members.
    pub(super) chain_groups: Vec<usize>,
    /// Tail-scrub admissions for the pass summary.
    pub(super) tail_selected: usize,
    /// A budget cap cut actionable work. Counts toward
    /// saturation only when the gate fires and declined leftovers recur
    /// identically.
    pub(super) saturated: bool,
    /// Chains skipped, by cause, for the pass summary. Only `deferred`
    /// (reserve contention; a fresh pass retries it) saturates — tracked
    /// apart from `saturated` since chains ride batch 1 only and later
    /// batches must not erase it. `warm` retries once write-cold;
    /// `unpackable` (cheapest pair over even a fresh reserve) is permanent
    /// and must not pin the fast cadence.
    pub(super) chains_deferred: usize,
    pub(super) chains_warm: usize,
    pub(super) chains_unpackable: usize,
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
pub(super) struct HotChain {
    pub(super) members: Vec<SegStat>,
    pub(super) edges: Vec<(usize, usize)>,
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
pub(super) fn select_round(
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

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
