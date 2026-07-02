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
#![allow(dead_code)]

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
}

impl TailBuffer {
    pub fn new() -> Self {
        Self {
            batches: BTreeMap::new(),
        }
    }

    /// Re-shipping the same seqno (a retry) overwrites the prior copy: a given
    /// seqno always carries the same batch from the same leader term.
    pub fn accept(&mut self, seqno: u64, ops: Vec<ReplOp>) {
        self.batches.insert(seqno, ops);
    }

    /// Drop batches with seqno <= `watermark` (confirmed durable).
    pub fn prune(&mut self, watermark: u64) {
        self.batches.retain(|&seqno, _| seqno > watermark);
    }

    /// Discard the buffered tail. Called when a higher writer epoch arrives: a new
    /// leader term restarts seqno at 1, so the prior term's batches would otherwise
    /// replay (in seqno order) over the new term's writes.
    pub fn clear(&mut self) {
        self.batches.clear();
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
        tb.accept(3, vec![put(b"c", b"3")]);
        tb.accept(1, vec![put(b"a", b"1")]);
        tb.accept(2, vec![put(b"b", b"2")]);

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
            tb.accept(s, vec![put(b"k", format!("{s}").as_bytes())]);
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
        tb.accept(7, vec![put(b"k", b"first")]);
        tb.accept(7, vec![put(b"k", b"first")]);
        assert_eq!(tb.len(), 1, "a re-shipped seqno must not duplicate");
    }
}
