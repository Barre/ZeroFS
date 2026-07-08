//! The standby's in-memory replication tail buffer: committed batches the leader
//! shipped but that may not yet be durable on object storage, keyed by the
//! leader's monotonic batch seqno. On takeover the new leader replays this tail
//! so no acknowledged write is lost.
//!
//! Under the segment data plane (RFC-0025) an extent write's value is a `FrameLoc`
//! pointer whose bytes live in an external `segments/` object that may still be
//! the leader's un-PUT open buffer. `PutFrame` closes that gap: it ships the
//! sealed frame bytes alongside the `FrameLoc`, and on takeover the new leader
//! materializes any missing segment on the shared store from the buffered frames
//! (HEAD-guarded so a leader-sealed segment is never overwritten). Segment
//! objects are immutable and deterministically keyed, so that PUT is idempotent
//! with the leader's.
//!
//! Replay is idempotent (last-write-wins per key in seqno order), so keeping an
//! already-durable batch is harmless. Pruning must therefore stay conservative:
//! prune too little and you waste memory, prune too much and you lose acked writes.
use bytes::Bytes;
use std::collections::BTreeMap;

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
    batches: BTreeMap<u64, Vec<ReplOp>>,
    /// Leader term of the buffered batches. A ship from a newer term drops them
    /// first (see [`Self::accept`]).
    epoch: u64,
}

impl TailBuffer {
    pub fn new() -> Self {
        Self {
            batches: BTreeMap::new(),
            epoch: 0,
        }
    }

    /// Buffer a batch tagged with its leader term. A batch from a newer term
    /// drops the prior term's batches first: seqnos restart per term, so a stale
    /// high-seqno batch would otherwise replay (in seqno order) over a new
    /// low-seqno one. Keyed on the tail's own epoch, never an externally tracked
    /// one, so a heartbeat that already advanced the fencing epoch cannot suppress
    /// the drop. Re-shipping the same (epoch, seqno) overwrites the prior copy.
    pub fn accept(&mut self, epoch: u64, seqno: u64, ops: Vec<ReplOp>) {
        if epoch > self.epoch {
            self.batches.clear();
            self.epoch = epoch;
        }
        self.batches.insert(seqno, ops);
    }

    /// Term of the buffered batches (0 before anything was accepted).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Drop every buffered batch. Only the takeover replay guard calls this,
    /// when the durable head supersedes the tail; the receive path's cross-term
    /// drop lives in [`Self::accept`].
    pub fn discard(&mut self) {
        self.batches.clear();
    }

    /// Drop batches with seqno <= `watermark` (confirmed durable).
    pub fn prune(&mut self, watermark: u64) {
        self.batches.retain(|&seqno, _| seqno > watermark);
    }

    /// Ascending seqno order, for replay on takeover.
    pub fn batches_in_order(&self) -> impl Iterator<Item = (u64, &[ReplOp])> {
        self.batches
            .iter()
            .map(|(&seqno, ops)| (seqno, ops.as_slice()))
    }

    pub fn highest_seqno(&self) -> Option<u64> {
        self.batches.keys().next_back().copied()
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
    /// that term's acked writes.
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

    #[test]
    fn buffers_in_order_and_reports_highest() {
        let mut tb = TailBuffer::new();
        assert!(tb.is_empty());
        tb.accept(1, 3, vec![put(b"c", b"3")]);
        tb.accept(1, 1, vec![put(b"a", b"1")]);
        tb.accept(1, 2, vec![put(b"b", b"2")]);

        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(
            seqnos,
            vec![1, 2, 3],
            "replay must be in ascending seqno order"
        );
        assert_eq!(tb.highest_seqno(), Some(3));
        assert_eq!(tb.len(), 3);
    }

    #[test]
    fn prune_is_conservative_and_inclusive() {
        let mut tb = TailBuffer::new();
        for s in 1..=5 {
            tb.accept(1, s, vec![put(b"k", format!("{s}").as_bytes())]);
        }
        tb.prune(3);
        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(seqnos, vec![4, 5]);

        tb.prune(0);
        assert_eq!(tb.len(), 2);

        tb.prune(100);
        assert!(tb.is_empty());
    }

    #[test]
    fn reship_same_seqno_overwrites_not_duplicates() {
        let mut tb = TailBuffer::new();
        tb.accept(1, 7, vec![put(b"k", b"first")]);
        tb.accept(1, 7, vec![put(b"k", b"first")]);
        assert_eq!(tb.len(), 1, "a re-shipped seqno must not duplicate");
    }

    // Finding A: a ship is delivered but its ack is lost, so the leader counts it
    // failed, goes Solo, and keeps committing durable progress (stamp solo > 0).
    // The buffered batch's content was re-committed locally, so replaying it after
    // the leader dies would resurrect an old value over newer durable state.
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
        tb.accept(5, 50, vec![put(b"f", b"old")]);
        tb.accept(5, 51, vec![put(b"f", b"old")]);
        tb.accept(6, 1, vec![put(b"f", b"new")]);
        let seqnos: Vec<u64> = tb.batches_in_order().map(|(s, _)| s).collect();
        assert_eq!(
            seqnos,
            vec![1],
            "the prior term's high-seqno batches must be dropped, not left to replay last"
        );
    }
}
