//! Shared actor plumbing: typed durability slots, fs-wide fsync snapshots, and
//! the common filesystem/digest/crash context carried by mutation actors.

use crate::digest::Digest;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use zerofs::fs::ZeroFS;

#[derive(Clone, Copy, Debug)]
pub(crate) struct FloorSlot(usize);

impl FloorSlot {
    pub(crate) fn new(index: usize) -> Self {
        Self(index)
    }

    fn index(self) -> usize {
        self.0
    }
}

pub(crate) struct FsyncSnapshot(Vec<usize>);

/// Cross-actor fsync floors. A client fsync flushes the whole filesystem, so
/// an acknowledgement raises every actor's floor to its pre-fsync ack count.
pub(crate) struct Floors {
    acked: Vec<AtomicUsize>,
    floor: Vec<AtomicUsize>,
}

impl Floors {
    pub(crate) fn new(states: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let (acked, floor) = states
            .into_iter()
            .map(|(acked, floor)| (AtomicUsize::new(acked), AtomicUsize::new(floor)))
            .unzip();
        Self { acked, floor }
    }

    pub(crate) fn publish_ack(&self, slot: FloorSlot, acked: usize) {
        self.acked[slot.index()].store(acked, Relaxed);
    }

    pub(crate) fn floor(&self, slot: FloorSlot) -> usize {
        self.floor[slot.index()].load(Relaxed)
    }

    pub(crate) fn snapshot(&self) -> FsyncSnapshot {
        FsyncSnapshot(self.acked.iter().map(|acked| acked.load(Relaxed)).collect())
    }

    pub(crate) fn acknowledge_fsync(&self, snapshot: FsyncSnapshot) {
        for (floor, acked) in self.floor.iter().zip(snapshot.0) {
            floor.fetch_max(acked, Relaxed);
        }
    }

    pub(crate) fn reanchor(&self, slot: FloorSlot, acked: usize, floor: usize) {
        self.acked[slot.index()].store(acked, Relaxed);
        self.floor[slot.index()].store(floor, Relaxed);
    }
}

pub(crate) struct ActorContext {
    pub(crate) fs: Arc<ZeroFS>,
    pub(crate) digest: Digest,
    floors: Arc<Floors>,
    slot: FloorSlot,
    pub(crate) crashed: tokio::sync::watch::Receiver<bool>,
}

impl ActorContext {
    pub(crate) fn new(
        fs: Arc<ZeroFS>,
        digest: Digest,
        floors: Arc<Floors>,
        slot: FloorSlot,
        crashed: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            fs,
            digest,
            floors,
            slot,
            crashed,
        }
    }

    pub(crate) fn is_crashed(&self) -> bool {
        *self.crashed.borrow()
    }

    pub(crate) fn publish_ack(&self, acked: usize) {
        self.floors.publish_ack(self.slot, acked);
    }

    pub(crate) fn trace_slot(&self) -> usize {
        self.slot.index()
    }

    pub(crate) fn begin_fsync(&self) -> FsyncSnapshot {
        self.floors.snapshot()
    }

    pub(crate) fn finish_fsync(&self, snapshot: FsyncSnapshot) {
        self.floors.acknowledge_fsync(snapshot);
    }
}
