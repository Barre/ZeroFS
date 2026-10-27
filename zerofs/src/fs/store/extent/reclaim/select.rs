//! Ranks segments and selects repack jobs within a byte budget.
//! Jobs must reclaim at least 1 MiB or combine multiple segments
//! containing at least 1 MiB of live data.

use super::SMALL_SEGMENT_BYTES;
use crate::segment::Segid;
use std::collections::VecDeque;

/// One segment's durable counter row as the scan saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SegStat {
    pub(super) segid: Segid,
    /// Frame bytes ever appended (monotonic).
    pub(super) total: u64,
    /// Frame bytes still referenced.
    pub(super) live: u64,
}

impl SegStat {
    pub(super) fn dead(&self) -> u64 {
        self.total.saturating_sub(self.live)
    }

    /// Small segments always qualify; dense ones once past the dead floor.
    pub(super) fn qualifies_for_repack(&self, min_dead_percent: u64) -> bool {
        self.total < SMALL_SEGMENT_BYTES
            || self.dead().saturating_mul(100) > self.total.saturating_mul(min_dead_percent)
    }

    /// Live fraction in permille, the fragmentation rank key (lower packs first).
    fn live_permille(&self) -> u64 {
        self.live.saturating_mul(1000) / self.total.max(1)
    }

    fn priority(&self) -> (u64, u64, u64) {
        (self.live_permille(), self.segid.epoch, self.segid.counter)
    }
}

/// One scan's repack candidates, most fragmented at the front.
pub(super) struct CandidatePool(VecDeque<SegStat>);

impl CandidatePool {
    pub(super) fn from_stats(mut stats: Vec<SegStat>) -> Self {
        stats.sort_unstable_by_key(SegStat::priority);
        Self(stats.into())
    }

    /// Take the next job's sources in priority order until the next one would
    /// push scan-time live bytes past `job_bytes`. An empty selection admits
    /// one oversized source so a segment larger than the budget still makes
    /// progress. Planning later re-budgets by exact stored bytes and may
    /// consume only a prefix; [`Self::restore`] returns the rest.
    pub(super) fn select(&mut self, job_bytes: u64) -> Option<JobSelection> {
        let mut selection = JobSelection::default();
        while let Some(next) = self.0.front() {
            let over_budget = selection.live.saturating_add(next.live) > job_bytes;
            if !selection.sources.is_empty() && over_budget {
                break;
            }
            let stat = self.0.pop_front().expect("front exists");
            selection.total += stat.total;
            selection.live += stat.live;
            selection.sources.push(stat);
        }
        (!selection.sources.is_empty()).then_some(selection)
    }

    /// Return an unconsumed suffix to the front, keeping priority order.
    pub(super) fn restore(&mut self, suffix: Vec<SegStat>) {
        for stat in suffix.into_iter().rev() {
            self.0.push_front(stat);
        }
    }

    #[cfg(test)]
    fn pop(&mut self) -> Option<SegStat> {
        self.0.pop_front()
    }
}

/// One repack job's selected sources and their scan-time byte totals.
#[derive(Default)]
pub(super) struct JobSelection {
    pub(super) sources: Vec<SegStat>,
    pub(super) total: u64,
    pub(super) live: u64,
}

impl JobSelection {
    pub(super) fn segids(&self) -> Vec<Segid> {
        self.sources.iter().map(|stat| stat.segid).collect()
    }

    pub(super) fn reclaimable(&self) -> u64 {
        self.total.saturating_sub(self.live)
    }

    /// A job must free at least `SMALL_SEGMENT_BYTES`, or combine several
    /// sources into an output that large. The latter floor keeps a dense
    /// packed output from qualifying as small again.
    pub(super) fn worth_repacking(&self) -> bool {
        let stable_small_pack = self.sources.len() > 1 && self.live >= SMALL_SEGMENT_BYTES;
        self.reclaimable() >= SMALL_SEGMENT_BYTES || stable_small_pack
    }

    /// The sources a job did not consume.
    pub(super) fn unconsumed(mut self, consumed: usize) -> Vec<SegStat> {
        self.sources.split_off(consumed.min(self.sources.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::REPACK_JOB_BYTES;
    use super::*;

    const EPOCH: u64 = 7;

    fn stat(counter: u64, total: u64, live: u64) -> SegStat {
        SegStat {
            segid: Segid::new(EPOCH, counter),
            total,
            live,
        }
    }

    #[test]
    fn candidates_are_ranked_by_reclaim_yield_and_byte_bounded() {
        let least_dead = stat(1, 100, 90);
        let most_dead = stat(2, 100, 20);
        let middle = stat(3, 100, 30);

        let mut pool = CandidatePool::from_stats(vec![least_dead, most_dead, middle]);
        let selection = pool.select(50).unwrap();

        assert_eq!(selection.sources, vec![most_dead, middle]);
        assert_eq!(selection.total, 200);
        assert_eq!(selection.live, 50);
        assert_eq!(pool.pop(), Some(least_dead));
    }

    #[test]
    fn an_oversized_first_candidate_still_makes_progress() {
        let oversized = stat(1, 300 << 20, 300 << 20);
        let next = stat(2, 64 << 20, 64 << 20);

        let mut pool = CandidatePool::from_stats(vec![oversized, next]);
        let selection = pool.select(REPACK_JOB_BYTES).unwrap();

        assert_eq!(selection.sources, vec![oversized]);
        assert_eq!(selection.live, oversized.live);
        assert_eq!(pool.pop(), Some(next));
    }

    #[test]
    fn unconsumed_suffix_is_restored_in_order() {
        let first = stat(1, 100, 20);
        let second = stat(2, 100, 30);
        let third = stat(960, 100, 40);
        let mut pool = CandidatePool::from_stats(vec![first, second, third]);
        let selection = pool.select(100).unwrap();

        pool.restore(selection.unconsumed(1));

        assert_eq!(pool.pop(), Some(second));
        assert_eq!(pool.pop(), Some(third));
        assert!(pool.select(1).is_none());
    }

    #[test]
    fn payoff_gate_needs_dead_bytes_or_a_stable_multi_source_pack() {
        let lone_dense = JobSelection {
            sources: vec![stat(1, 5 << 20, 5 << 20)],
            total: 5 << 20,
            live: 5 << 20,
        };
        assert!(!lone_dense.worth_repacking());

        let tiny_pair = JobSelection {
            sources: vec![stat(1, 100, 100), stat(2, 100, 100)],
            total: 200,
            live: 200,
        };
        assert!(!tiny_pair.worth_repacking());

        let small_pack = JobSelection {
            sources: vec![stat(1, 600 << 10, 600 << 10), stat(2, 600 << 10, 600 << 10)],
            total: 1200 << 10,
            live: 1200 << 10,
        };
        assert!(small_pack.worth_repacking());

        let fragmented = JobSelection {
            sources: vec![stat(1, 3 << 20, 1 << 20)],
            total: 3 << 20,
            live: 1 << 20,
        };
        assert!(fragmented.worth_repacking());
    }

    #[test]
    fn candidacy_is_small_or_past_the_dead_floor() {
        assert!(stat(1, SMALL_SEGMENT_BYTES - 1, SMALL_SEGMENT_BYTES - 1).qualifies_for_repack(10));
        assert!(!stat(1, 10 << 20, 9 << 20).qualifies_for_repack(10));
        assert!(stat(1, 10 << 20, 8 << 20).qualifies_for_repack(10));
    }
}
