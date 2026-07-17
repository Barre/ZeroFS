//! In-memory standby tail keyed by writer-term ship sequence. Promotion
//! reconciles the tail with durable state and replays the retained suffix.
//! Dedup results become visible after their batch is pruned or replayed.
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::Instant;

use crate::dedup::{DedupCache, DedupEntry};
use crate::replication::types::{
    CoverageFrontier, HaStamp, PruneWatermark, ShipSeqno, WriterEpoch,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
    /// Extent Put with sealed bytes for materializing a segment absent at takeover.
    PutFrame(Bytes, Bytes, Bytes),
}

#[derive(Default)]
pub struct TailBuffer {
    batches: BTreeMap<u64, TailBatch>,
    /// Writer term of the buffered batches.
    epoch: u64,
    /// Highest ship sequence number observed in `epoch`, including batches that
    /// were subsequently pruned.
    max_seqno: u64,
    /// Highest sequence number the leader proved durable on shared storage.
    /// Missing batches at or below this frontier do not break lineage coverage.
    durable_through: u64,
    /// Highest contiguous ship sequence received from sequence 1 in `epoch`.
    /// Leader-provided durability does not advance this dedup-result frontier.
    dedup_received_through: u64,
    /// Set after any non-contiguous sequence is observed in the term.
    dedup_lineage_broken: bool,
    /// Earliest expiry of a dedup result published while pruning this term.
    dedup_coverage_deadline: Option<Instant>,
}

struct TailBatch {
    ops: Vec<ReplOp>,
    /// Published to dedup only after prune or successful replay.
    dedup_entries: Vec<DedupEntry>,
}

impl TailBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    fn retains_contiguous_range(&self, first: u64, last: u64) -> bool {
        (first..=last).eq(self.batches.range(first..=last).map(|(&seqno, _)| seqno))
    }

    /// Returns whether every sequence before `seqno` is durable or retained in
    /// this tail. The incoming batch is excluded and the tail is not modified.
    pub(crate) fn covers_predecessors(
        &self,
        epoch: WriterEpoch,
        seqno: ShipSeqno,
        prune_watermark: PruneWatermark,
    ) -> bool {
        let predecessor = seqno.get() - 1;
        if predecessor == 0 {
            return true;
        }

        let durable_through = if self.epoch == epoch.get() {
            self.durable_through.max(prune_watermark.get())
        } else {
            prune_watermark.get()
        };
        if durable_through >= predecessor {
            return true;
        }
        if self.epoch != epoch.get() {
            return false;
        }

        let first_retained = durable_through + 1;
        self.retains_contiguous_range(first_retained, predecessor)
    }

    /// Records heartbeat coverage and returns whether every applied peer-copy
    /// attempt is durable or retained. An incomplete applied frontier remains
    /// recorded until repaired.
    pub(crate) fn observe_coverage(
        &mut self,
        epoch: WriterEpoch,
        frontier: CoverageFrontier,
    ) -> (bool, Vec<DedupEntry>) {
        let epoch = epoch.get();
        let applied = frontier.applied_through().map_or(0, ShipSeqno::get);
        let durable = frontier.durable_through().map_or(0, ShipSeqno::get);
        if epoch < self.epoch {
            return (false, Vec::new());
        }
        // A zero applied frontier does not supersede an older term's tail.
        if epoch > self.epoch && applied == 0 {
            return (true, Vec::new());
        }
        if epoch > self.epoch {
            *self = Self {
                epoch,
                ..Self::default()
            };
        }

        self.max_seqno = self.max_seqno.max(applied);
        let durable_entries = self.prune(durable);
        let required_through = self.max_seqno;
        let durable_through = self.durable_through;
        if durable_through >= required_through {
            return (true, durable_entries);
        }
        let first = durable_through + 1;
        let covered = self.retains_contiguous_range(first, required_through);
        (covered, durable_entries)
    }

    /// Buffers a term-scoped batch. A newer term replaces the prior tail;
    /// re-shipping a sequence replaces its batch.
    pub(crate) fn accept_validated(
        &mut self,
        epoch: WriterEpoch,
        seqno: ShipSeqno,
        ops: Vec<ReplOp>,
        dedup_entries: Vec<DedupEntry>,
    ) {
        let epoch = epoch.get();
        let seqno = seqno.get();
        if epoch < self.epoch {
            // Preserve the current term on a stale batch that bypassed admission.
            self.dedup_lineage_broken = true;
            return;
        }
        if epoch > self.epoch {
            *self = Self {
                epoch,
                ..Self::default()
            };
        }
        if seqno > self.dedup_received_through {
            if !self.dedup_lineage_broken
                && self.dedup_received_through.checked_add(1) == Some(seqno)
            {
                self.dedup_received_through = seqno;
            } else {
                self.dedup_lineage_broken = true;
            }
        }
        self.max_seqno = self.max_seqno.max(seqno);
        self.batches.insert(seqno, TailBatch { ops, dedup_entries });
    }

    /// Test helper for direct tail construction.
    #[cfg(test)]
    pub fn accept(
        &mut self,
        epoch: u64,
        seqno: u64,
        ops: Vec<ReplOp>,
        dedup_entries: Vec<DedupEntry>,
    ) {
        self.accept_validated(
            WriterEpoch::new(epoch).expect("test writer epoch must be nonzero"),
            ShipSeqno::new(seqno).expect("test ship sequence must be nonzero"),
            ops,
            dedup_entries,
        );
    }

    /// Term of the buffered batches (0 before anything was accepted).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Drops all batches after durable state supersedes the tail.
    pub fn discard(&mut self) {
        *self = Self::default();
    }

    /// Drops batches through `watermark` and returns their dedup results.
    pub fn prune(&mut self, watermark: u64) -> Vec<DedupEntry> {
        self.durable_through = self.durable_through.max(watermark);
        let mut durable_entries = Vec::new();
        self.batches.retain(|&seqno, batch| {
            if seqno <= watermark {
                durable_entries.extend(batch.dedup_entries.iter().cloned());
                false
            } else {
                true
            }
        });
        durable_entries
    }

    /// Returns whether every ship in `observed_epoch` is durable or retained.
    /// Heartbeat observation alone does not establish tail coverage.
    pub(crate) fn preserves_lineage(&self, observed_epoch: u64) -> bool {
        if observed_epoch == 0 || self.epoch != observed_epoch || self.max_seqno == 0 {
            return false;
        }
        if self.durable_through >= self.max_seqno {
            return true;
        }

        let first = self.durable_through + 1;
        self.retains_contiguous_range(first, self.max_seqno)
    }

    /// Returns whether the applied frontier exceeds durable and retained coverage.
    pub(crate) fn has_uncovered_lineage(&self, observed_epoch: u64) -> bool {
        self.epoch == observed_epoch
            && self.max_seqno > 0
            && !self.preserves_lineage(observed_epoch)
    }

    /// Returns whether this receiver observed every dedup result from sequence 1
    /// through the known frontier. Durable data does not prove receipt of dedup
    /// results, and pruning does not advance this frontier.
    pub(crate) fn preserves_dedup_lineage(&self, observed_epoch: u64) -> bool {
        observed_epoch != 0
            && self.epoch == observed_epoch
            && self.dedup_received_through > 0
            && self.dedup_received_through >= self.durable_through
            && !self.dedup_lineage_broken
    }

    /// Publishes results for durable entries and records their earliest expiry.
    pub(crate) fn publish_durable(
        &mut self,
        dedup: &DedupCache,
        entries: impl IntoIterator<Item = DedupEntry>,
    ) {
        for entry in entries {
            if let Some(expires_at) = dedup.record_entry_with_expiry(entry) {
                self.dedup_coverage_deadline = Some(
                    self.dedup_coverage_deadline
                        .map_or(expires_at, |current| current.min(expires_at)),
                );
            }
        }
    }

    pub(crate) fn dedup_coverage_deadline(&self) -> Option<Instant> {
        self.dedup_coverage_deadline
    }

    /// Clears a replayed tail and returns its now-durable results.
    pub fn finish_replay(&mut self) -> Vec<DedupEntry> {
        let batches = std::mem::take(self).batches;
        batches
            .into_values()
            .flat_map(|batch| batch.dedup_entries)
            .collect()
    }

    /// Ascending seqno order, for replay on takeover.
    pub fn batches_in_order(&self) -> impl Iterator<Item = (u64, &[ReplOp])> {
        self.batches
            .iter()
            .map(|(&seqno, batch)| (seqno, batch.ops.as_slice()))
    }

    pub fn len(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Replay action for a buffered tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDecision {
    /// Batches through this applied-attempt frontier are already represented in
    /// the durable head; the retained suffix must replay. Zero means the full
    /// tail replays.
    PruneTo(u64),
    /// The durable head supersedes the tail; replaying would regress it.
    Discard,
}

/// Selects the replay boundary from the durable HA stamp.
///
/// A later durable term discards the tail. For the same term, batches through
/// `applied_through` are already represented by durable state after Solo
/// progress; their dedup results remain publishable. Failed peer-copy applies
/// poison the writer, and reconnect flushes intervening Solo applies before the
/// next ship. The suffix above `applied_through` is therefore contiguous and
/// newer than the durable head. An older or absent stamp replays the complete
/// tail.
pub(crate) fn replay_decision(stamp: Option<&HaStamp>, tail_epoch: u64) -> ReplayDecision {
    let Some(stamp) = stamp else {
        return ReplayDecision::PruneTo(0);
    };
    let stamp_epoch = stamp.writer_epoch().get();
    if stamp_epoch > tail_epoch {
        return ReplayDecision::Discard;
    }
    if stamp_epoch < tail_epoch {
        return ReplayDecision::PruneTo(0);
    }
    if stamp.solo_history().ran_solo() {
        ReplayDecision::PruneTo(stamp.applied_through().map_or(0, ShipSeqno::get))
    } else {
        ReplayDecision::PruneTo(stamp.last_shipped().map_or(0, ShipSeqno::get))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::types::SoloHistory;

    fn put(k: &[u8], v: &[u8]) -> ReplOp {
        ReplOp::Put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
    }

    fn dedup_entry(n: u8) -> DedupEntry {
        DedupEntry {
            op_id: [n; 16],
            result: crate::dedup::DedupResult::Applied,
        }
    }

    fn entry_ids(entries: Vec<DedupEntry>) -> Vec<[u8; 16]> {
        entries.into_iter().map(|entry| entry.op_id).collect()
    }

    fn ha_stamp(
        writer_epoch: u64,
        last_shipped: u64,
        solo_history: SoloHistory,
        applied_through: u64,
    ) -> HaStamp {
        HaStamp::new(
            WriterEpoch::new(writer_epoch).unwrap(),
            ShipSeqno::new(last_shipped),
            solo_history,
            ShipSeqno::new(applied_through),
        )
        .unwrap()
    }

    #[derive(Clone, Copy)]
    enum TailStep {
        A(u64, u64),  // accept
        R(u64, u64),  // accept with an exact result
        P(u64),       // prune
        D,            // discard
        L(u64, bool), // data lineage
        X(u64, bool), // exact-result lineage
        E(bool),      // empty
    }

    fn run_tail_case(name: &str, steps: &[TailStep]) {
        use TailStep::*;
        let mut tail = TailBuffer::new();
        for &step in steps {
            match step {
                A(epoch, seqno) => tail.accept(epoch, seqno, vec![put(b"k", b"v")], vec![]),
                R(epoch, seqno) => tail.accept(
                    epoch,
                    seqno,
                    vec![put(b"k", b"v")],
                    vec![dedup_entry(seqno as u8)],
                ),
                P(seqno) => drop(tail.prune(seqno)),
                D => tail.discard(),
                L(epoch, expected) => {
                    assert_eq!(tail.preserves_lineage(epoch), expected, "{name}")
                }
                X(epoch, expected) => {
                    assert_eq!(tail.preserves_dedup_lineage(epoch), expected, "{name}")
                }
                E(expected) => assert_eq!(tail.is_empty(), expected, "{name}"),
            }
        }
    }

    #[test]
    fn buffers_in_seqno_order() {
        let mut tb = TailBuffer::new();
        assert!(tb.is_empty());
        tb.accept(1, 3, vec![put(b"c", b"3")], vec![]);
        tb.accept(1, 1, vec![put(b"a", b"1")], vec![]);
        tb.accept(1, 2, vec![put(b"b", b"2")], vec![]);

        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(
            seqnos,
            vec![1, 2, 3],
            "replay must be in ascending seqno order"
        );
        assert_eq!(tb.len(), 3);
    }

    #[test]
    fn prune_is_inclusive() {
        let mut tb = TailBuffer::new();
        for s in 1..=5 {
            tb.accept(1, s, vec![put(b"k", format!("{s}").as_bytes())], vec![]);
        }
        assert!(tb.prune(3).is_empty());
        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(seqnos, vec![4, 5]);

        assert!(tb.prune(0).is_empty());
        assert_eq!(tb.len(), 2);

        assert!(tb.prune(100).is_empty());
        assert!(tb.is_empty());
    }

    #[test]
    fn reship_replaces_same_seqno() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 7, vec![put(b"k", b"first")], vec![dedup_entry(1)]);
        tb.accept(1, 7, vec![put(b"k", b"first")], vec![dedup_entry(2)]);
        assert_eq!(tb.len(), 1, "a re-shipped seqno must not duplicate");
        assert_eq!(
            entry_ids(tb.finish_replay()),
            vec![[2; 16]],
            "the replacement batch owns the seqno's pending dedup results"
        );
    }

    #[test]
    fn dedup_results_follow_durable_prune_and_successful_replay() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 1, vec![put(b"a", b"1")], vec![dedup_entry(1)]);
        tb.accept(1, 2, vec![put(b"b", b"2")], vec![dedup_entry(2)]);

        assert_eq!(
            entry_ids(tb.prune(1)),
            vec![[1; 16]],
            "only the confirmed-durable prefix may publish"
        );
        assert_eq!(tb.len(), 1);
        assert_eq!(
            entry_ids(tb.finish_replay()),
            vec![[2; 16]],
            "successful replay publishes the remaining suffix"
        );
        assert!(tb.is_empty(), "successful replay releases the tail storage");
    }

    #[test]
    fn discard_drops_pending_dedup_results() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 1, vec![put(b"a", b"1")], vec![dedup_entry(1)]);

        tb.discard();

        assert!(tb.is_empty());
        assert!(
            tb.finish_replay().is_empty(),
            "discarded dedup results must never be returned as successfully applied"
        );
    }

    #[test]
    fn replay_decisions_preserve_durable_history() {
        use ReplayDecision::*;
        let cases = [
            (
                "solo prefix",
                Some((5, 41, SoloHistory::ever(1), 43)),
                PruneTo(43),
            ),
            (
                "solo from birth",
                Some((5, 0, SoloHistory::ever(7), 0)),
                PruneTo(0),
            ),
            (
                "newer solo term",
                Some((6, 0, SoloHistory::ever(3), 0)),
                Discard,
            ),
            (
                "newer connected term",
                Some((6, 2, SoloHistory::never(), 2)),
                Discard,
            ),
            (
                "connected crash",
                Some((5, 3, SoloHistory::never(), 3)),
                PruneTo(3),
            ),
            ("no durable progress", None, PruneTo(0)),
            (
                "older connected term",
                Some((4, 9, SoloHistory::never(), 9)),
                PruneTo(0),
            ),
            (
                "older solo term",
                Some((4, 9, SoloHistory::ever(6), 12)),
                PruneTo(0),
            ),
        ];

        for (name, coordinates, expected) in cases {
            let stamp = coordinates.map(|(e, s, solo, applied)| ha_stamp(e, s, solo, applied));
            assert_eq!(replay_decision(stamp.as_ref(), 5), expected, "{name}");
        }
    }

    #[test]
    fn newer_epoch_replaces_prior_tail() {
        let mut tb = TailBuffer::new();
        tb.accept(5, 50, vec![put(b"f", b"old")], vec![dedup_entry(5)]);
        tb.accept(5, 51, vec![put(b"f", b"old")], vec![dedup_entry(6)]);
        tb.accept(6, 1, vec![put(b"f", b"new")], vec![dedup_entry(7)]);
        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(
            seqnos,
            vec![1],
            "the prior term's high-seqno batches must be dropped, not left to replay last"
        );
        assert_eq!(
            entry_ids(tb.finish_replay()),
            vec![[7; 16]],
            "pending dedup results from the replaced term must be dropped with its data"
        );
    }

    #[test]
    fn predecessor_coverage_requires_a_durable_or_contiguous_prefix() {
        let mut tb = TailBuffer::new();
        let epoch = WriterEpoch::new(7).unwrap();
        let seqno = ShipSeqno::new(2).unwrap();
        assert!(!tb.covers_predecessors(epoch, seqno, PruneWatermark::for_ship(0, seqno).unwrap()));
        assert!(tb.covers_predecessors(epoch, seqno, PruneWatermark::for_ship(1, seqno).unwrap()));

        tb.accept(7, 2, vec![put(b"y", b"2")], vec![]);
        let seqno = ShipSeqno::new(4).unwrap();
        assert!(!tb.covers_predecessors(epoch, seqno, PruneWatermark::for_ship(1, seqno).unwrap()));
        tb.accept(7, 3, vec![put(b"z", b"3")], vec![]);
        assert!(tb.covers_predecessors(epoch, seqno, PruneWatermark::for_ship(1, seqno).unwrap()));
        assert!(
            !tb.covers_predecessors(
                WriterEpoch::new(8).unwrap(),
                seqno,
                PruneWatermark::for_ship(1, seqno).unwrap()
            ),
            "a new writer term cannot inherit volatile prefix coverage"
        );
    }

    #[test]
    fn lineage_coverage_matrix() {
        use TailStep::*;
        let cases: &[(&str, &[TailStep])] = &[
            ("fresh receiver gap", &[A(7, 2), L(7, false)]),
            ("durable prefix watermark", &[A(7, 2), P(1), L(7, true)]),
            ("pruned-empty proof", &[A(7, 1), P(1), E(true), L(7, true)]),
            ("internal gap", &[A(7, 1), A(7, 3), L(7, false)]),
            (
                "epoch-local data proof",
                &[A(7, 1), A(7, 2), L(7, true), L(8, false)],
            ),
            (
                "watermark cannot mint exact prefix",
                &[
                    R(7, 2),
                    P(1),
                    L(7, true),
                    X(7, false),
                    P(2),
                    E(true),
                    X(7, false),
                ],
            ),
            (
                "watermark beyond receipt",
                &[R(7, 1), X(7, true), P(3), X(7, false)],
            ),
            (
                "pruned exact proof stays epoch-local",
                &[
                    R(7, 1),
                    P(1),
                    R(7, 2),
                    P(2),
                    E(true),
                    X(7, true),
                    X(8, false),
                ],
            ),
            (
                "late batch cannot repair gap",
                &[A(7, 1), A(7, 3), X(7, false), A(7, 2), X(7, false)],
            ),
            (
                "each term proves itself from one",
                &[
                    A(7, 1),
                    A(7, 2),
                    X(7, true),
                    A(8, 2),
                    X(8, false),
                    A(8, 1),
                    X(8, false),
                    A(9, 1),
                    X(9, true),
                ],
            ),
            (
                "empty/discarded proves nothing",
                &[L(7, false), A(7, 1), D, L(7, false)],
            ),
        ];

        for (name, steps) in cases {
            run_tail_case(name, steps);
        }
    }

    #[test]
    fn stale_cross_term_input_invalidates_exact_dedup_lineage() {
        let mut tb = TailBuffer::new();
        tb.accept(8, 1, vec![put(b"new", b"1")], vec![dedup_entry(8)]);
        assert!(tb.preserves_dedup_lineage(8));

        tb.accept(7, 99, vec![put(b"stale", b"value")], vec![dedup_entry(7)]);
        assert!(
            !tb.preserves_dedup_lineage(8),
            "mixed-term receipt history must fail closed even if admission should reject it"
        );
        assert!(
            tb.preserves_lineage(8),
            "a rejected stale seqno must not extend the current term's data frontier"
        );
        assert_eq!(
            tb.batches_in_order()
                .map(|(seqno, ops)| (seqno, ops.to_vec()))
                .collect::<Vec<_>>(),
            vec![(1, vec![put(b"new", b"1")])],
            "a stale-term batch must not enter or replace the current tail"
        );
        assert_eq!(
            entry_ids(tb.finish_replay()),
            vec![[8; 16]],
            "a stale term's retry result must not enter the current tail"
        );
    }
}
