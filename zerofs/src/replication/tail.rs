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
