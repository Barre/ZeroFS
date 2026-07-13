//! The standby's in-memory replication tail buffer: committed batches the leader
//! shipped but that may not yet be durable on object storage, keyed by the
//! leader's monotonic batch seqno. On takeover the new leader reconciles and
//! replays the batches that survived in this receiver incarnation.
//!
//! Replay is idempotent (last-write-wins per key in seqno order), so keeping an
//! already-durable batch is harmless. Pruning must therefore stay conservative:
//! prune too little and you waste memory, prune too much and you lose acked writes.
//! Op IDs follow their batch: prune/replay publishes them; discard or term
//! replacement drops them.
use bytes::Bytes;
use std::collections::BTreeMap;

use crate::dedup::OpId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
    /// An extent-write Put (key, FrameLoc value) carrying its sealed frame bytes, so
    /// the standby can materialize the still-un-PUT segment on takeover. Applied to
    /// the db exactly like `Put`; the frame bytes are used only for materialization.
    PutFrame(Bytes, Bytes, Bytes),
}

#[derive(Default)]
pub struct TailBuffer {
    batches: BTreeMap<u64, TailBatch>,
    /// Leader term of the buffered batches. A ship from a newer term drops them
    /// first (see [`Self::accept`]).
    epoch: u64,
    /// Highest ship sequence number observed in `epoch`, including batches that
    /// were subsequently pruned.
    max_seqno: u64,
    /// Highest sequence number the leader proved durable on shared storage.
    /// Missing batches at or below this frontier do not break lineage coverage.
    durable_through: u64,
}

struct TailBatch {
    ops: Vec<ReplOp>,
    /// Published to dedup only after prune or successful replay.
    op_ids: Vec<OpId>,
}

impl TailBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffer a term-scoped batch. A newer term replaces the old tail because
    /// seqnos restart per term; the decision uses the tail epoch so a heartbeat
    /// cannot suppress it. Re-shipping the same seqno replaces its batch.
    pub fn accept(&mut self, epoch: u64, seqno: u64, ops: Vec<ReplOp>, op_ids: Vec<OpId>) {
        if epoch > self.epoch {
            *self = Self {
                epoch,
                ..Self::default()
            };
        }
        self.max_seqno = self.max_seqno.max(seqno);
        self.batches.insert(seqno, TailBatch { ops, op_ids });
    }

    /// Term of the buffered batches (0 before anything was accepted).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Drop every buffered batch. Only takeover reconciliation calls this,
    /// when the durable head supersedes the tail; the receive path's cross-term
    /// drop lives in [`Self::accept`].
    pub fn discard(&mut self) {
        *self = Self::default();
    }

    /// Drop batches with seqno <= `watermark` (confirmed durable), returning
    /// their now-safe-to-publish op-ids.
    pub fn prune(&mut self, watermark: u64) -> Vec<OpId> {
        self.durable_through = self.durable_through.max(watermark);
        let mut durable_op_ids = Vec::new();
        self.batches.retain(|&seqno, batch| {
            if seqno <= watermark {
                durable_op_ids.extend(batch.op_ids.iter().copied());
                false
            } else {
                true
            }
        });
        durable_op_ids
    }

    /// Whether this receiver can account for every ship in `observed_epoch`:
    /// each sequence number is either proven durable through the watermark or
    /// present in the volatile tail that takeover is about to replay.
    ///
    /// Seeing only a heartbeat is deliberately insufficient. A replacement
    /// receiver can inherit a reconnecting transport without inheriting the old
    /// process's tail; the first post-restart ship exposes that as an uncovered
    /// sequence gap unless its watermark proves the missing prefix durable.
    pub(crate) fn preserves_lineage(&self, observed_epoch: u64) -> bool {
        if observed_epoch == 0 || self.epoch != observed_epoch || self.max_seqno == 0 {
            return false;
        }
        if self.durable_through >= self.max_seqno {
            return true;
        }

        let first = self.durable_through + 1;
        (first..=self.max_seqno).eq(self
            .batches
            .range(first..=self.max_seqno)
            .map(|(&seqno, _)| seqno))
    }

    /// Successful takeover replay made every remaining batch durable. Clear the
    /// tail's coverage state and return the op-ids that may now enter dedup.
    pub fn finish_replay(&mut self) -> Vec<OpId> {
        let batches = std::mem::take(self).batches;
        batches
            .into_values()
            .flat_map(|batch| batch.op_ids)
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

/// What a takeover may do with the buffered tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDecision {
    /// Nothing of the tail's term ever flushed: the tail is the only copy of
    /// the batches this receiver still holds.
    ReplayAll,
    /// The durable head is the tail's term with no solo progress: batches up to
    /// the stamped seqno are already durable, the rest replays.
    PruneTo(u64),
    /// The durable head supersedes the tail; replaying would regress it.
    Discard,
}

/// Validate the tail against the db's durable provenance stamp (epoch, last
/// acked ship's seqno, solo commits since; committed with every replicated
/// batch): replay must only extend durable history.
///
/// A stamp from a later term means that term flushed progress the tail predates.
/// Solo progress in the tail's own term means the leader outlived these ships:
/// each buffered batch past the stamped seqno was re-committed locally as a solo
/// write, so its content is either already durable here, superseded by a newer
/// solo commit, or died honestly with the leader; replaying it could resurrect
/// an old value over newer durable state. A stamp from an older term (or none)
/// means nothing of the tail's term flushed and the whole tail replays.
pub fn replay_decision(stamp: Option<(u64, u64, u64)>, tail_epoch: u64) -> ReplayDecision {
    match stamp {
        None => ReplayDecision::ReplayAll,
        Some((epoch, _, _)) if epoch > tail_epoch => ReplayDecision::Discard,
        Some((epoch, _, _)) if epoch < tail_epoch => ReplayDecision::ReplayAll,
        Some((_, _, solo)) if solo > 0 => ReplayDecision::Discard,
        Some((_, seqno, _)) => ReplayDecision::PruneTo(seqno),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(k: &[u8], v: &[u8]) -> ReplOp {
        ReplOp::Put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
    }

    fn op_id(n: u8) -> OpId {
        [n; 16]
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
    fn prune_is_conservative_and_inclusive() {
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
    fn reship_same_seqno_overwrites_not_duplicates() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 7, vec![put(b"k", b"first")], vec![op_id(1)]);
        tb.accept(1, 7, vec![put(b"k", b"first")], vec![op_id(2)]);
        assert_eq!(tb.len(), 1, "a re-shipped seqno must not duplicate");
        assert_eq!(
            tb.finish_replay(),
            vec![op_id(2)],
            "the replacement batch owns the seqno's pending op-ids"
        );
    }

    #[test]
    fn op_ids_follow_durable_prune_and_successful_replay() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 1, vec![put(b"a", b"1")], vec![op_id(1)]);
        tb.accept(1, 2, vec![put(b"b", b"2")], vec![op_id(2)]);

        assert_eq!(
            tb.prune(1),
            vec![op_id(1)],
            "only the confirmed-durable prefix may publish"
        );
        assert_eq!(tb.len(), 1);
        assert_eq!(
            tb.finish_replay(),
            vec![op_id(2)],
            "successful replay publishes the remaining suffix"
        );
        assert!(tb.is_empty(), "successful replay releases the tail storage");
    }

    #[test]
    fn discarded_tail_drops_pending_op_ids_without_completing_them() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 1, vec![put(b"a", b"1")], vec![op_id(1)]);

        tb.discard();

        assert!(tb.is_empty());
        assert!(
            tb.finish_replay().is_empty(),
            "discarded op-ids must never be returned as successfully applied"
        );
    }

    // A lost ship ack followed by solo progress leaves the buffered batch stale.
    #[test]
    fn solo_progress_in_the_tails_term_discards_the_outlived_ships() {
        assert_eq!(
            replay_decision(Some((5, 41, 1)), 5),
            ReplayDecision::Discard,
            "one solo commit after ship 41: batches past 41 were superseded"
        );
        assert_eq!(replay_decision(Some((5, 0, 7)), 5), ReplayDecision::Discard);
    }

    // The never-shipped-term residual: a later leader (epoch 6) never reached the
    // standby (Solo from birth), flushed durable progress, and died. The standby
    // still holds epoch-5 batches (no epoch-6 ship ever cleared them); replaying
    // them would regress epoch 6's durable writes. Any durable stamp from a term
    // newer than the tail supersedes it.
    #[test]
    fn a_later_terms_durable_progress_discards_an_older_tail() {
        assert_eq!(replay_decision(Some((6, 0, 3)), 5), ReplayDecision::Discard);
        assert_eq!(replay_decision(Some((6, 2, 0)), 5), ReplayDecision::Discard);
    }

    // The normal crash of a connected leader: the durable head is a shipped stamp
    // of the tail's own term with no solo progress. Batches up to it are already
    // durable; everything after is the acked-but-unflushed suffix and replays.
    #[test]
    fn a_connected_crash_prunes_the_durable_prefix_and_replays_the_rest() {
        assert_eq!(
            replay_decision(Some((5, 3, 0)), 5),
            ReplayDecision::PruneTo(3)
        );
    }

    // The semisync no-acked-loss guarantee: a leader that shipped its term's
    // writes but died before any flush leaves no stamp from that term (absent, or
    // an older term's). The tail is the only copy of those acked writes and must
    // replay in full.
    #[test]
    fn a_term_with_no_durable_progress_replays_its_full_tail() {
        assert_eq!(replay_decision(None, 5), ReplayDecision::ReplayAll);
        assert_eq!(
            replay_decision(Some((4, 9, 0)), 5),
            ReplayDecision::ReplayAll
        );
        assert_eq!(
            replay_decision(Some((4, 9, 6)), 5),
            ReplayDecision::ReplayAll,
            "an older term's solo progress predates the tail's term entirely"
        );
    }

    // A batch from a newer term drops the prior term's tail, whatever its seqnos:
    // per-term seqnos restart, so a stale high-seqno batch would otherwise replay
    // (in seqno order) over the new term's low-seqno write.
    #[test]
    fn a_newer_epoch_drops_the_prior_terms_tail() {
        let mut tb = TailBuffer::new();
        tb.accept(5, 50, vec![put(b"f", b"old")], vec![op_id(5)]);
        tb.accept(5, 51, vec![put(b"f", b"old")], vec![op_id(6)]);
        tb.accept(6, 1, vec![put(b"f", b"new")], vec![op_id(7)]);
        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(
            seqnos,
            vec![1],
            "the prior term's high-seqno batches must be dropped, not left to replay last"
        );
        assert_eq!(
            tb.finish_replay(),
            vec![op_id(7)],
            "pending op-ids from the replaced term must be dropped with its data"
        );
    }

    #[test]
    fn fresh_receiver_does_not_preserve_lineage_across_an_uncovered_gap() {
        let mut tb = TailBuffer::new();
        tb.accept(7, 2, vec![put(b"y", b"2")], vec![]);

        assert!(
            !tb.preserves_lineage(7),
            "seqno 1 may have died with the receiver's prior incarnation"
        );
    }

    #[test]
    fn durable_watermark_covers_a_missing_prefix_after_receiver_restart() {
        let mut tb = TailBuffer::new();
        tb.accept(7, 2, vec![put(b"y", b"2")], vec![]);
        tb.prune(1);

        assert!(
            tb.preserves_lineage(7),
            "the durable frontier accounts for seqno 1 and the tail holds seqno 2"
        );
    }

    #[test]
    fn durably_pruned_empty_tail_retains_coverage_proof() {
        let mut tb = TailBuffer::new();
        tb.accept(7, 1, vec![put(b"x", b"1")], vec![]);
        tb.prune(1);

        assert!(tb.is_empty());
        assert!(
            tb.preserves_lineage(7),
            "pruned-empty is proven durable, unlike a fresh empty receiver"
        );
    }

    #[test]
    fn internal_sequence_gap_breaks_lineage_coverage() {
        let mut tb = TailBuffer::new();
        tb.accept(7, 1, vec![put(b"x", b"1")], vec![]);
        tb.accept(7, 3, vec![put(b"z", b"3")], vec![]);

        assert!(!tb.preserves_lineage(7), "seqno 2 is unaccounted for");
    }

    #[test]
    fn contiguous_tail_preserves_only_its_own_observed_epoch() {
        let mut tb = TailBuffer::new();
        tb.accept(7, 1, vec![put(b"x", b"1")], vec![]);
        tb.accept(7, 2, vec![put(b"y", b"2")], vec![]);

        assert!(tb.preserves_lineage(7));
        assert!(
            !tb.preserves_lineage(8),
            "a newer heartbeat without a ship gives no tail coverage for that term"
        );
    }

    #[test]
    fn empty_or_discarded_tail_cannot_establish_lineage() {
        let mut tb = TailBuffer::new();
        assert!(!tb.preserves_lineage(7), "heartbeat-only is not proof");

        tb.accept(7, 1, vec![put(b"x", b"1")], vec![]);
        tb.discard();
        assert!(!tb.preserves_lineage(7));
    }
}
