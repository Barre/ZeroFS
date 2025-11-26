use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;

pub type SequenceNumber = u64;

#[derive(Clone, Copy, Debug, PartialEq)]
enum SequenceState {
    Pending,
    Abandoned,
    Committed,
}

pub struct WriteCoordinator {
    next_sequence: AtomicU64,
    /// All sequences <= this value are complete
    committed_watermark: AtomicU64,
    pending_sequences: DashMap<SequenceNumber, SequenceState>,
    watermark_sender: watch::Sender<u64>,
    watermark_receiver: watch::Receiver<u64>,
}

impl WriteCoordinator {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(0);
        Self {
            next_sequence: AtomicU64::new(1),
            committed_watermark: AtomicU64::new(0),
            pending_sequences: DashMap::new(),
            watermark_sender: tx,
            watermark_receiver: rx,
        }
    }

    pub fn allocate_sequence(self: &Arc<Self>) -> SequenceGuard {
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        self.pending_sequences.insert(seq, SequenceState::Pending);
        SequenceGuard {
            sequence: seq,
            coordinator: Arc::clone(self),
            completed: false,
        }
    }

    pub async fn wait_for_predecessors(&self, seq: SequenceNumber) {
        let target = seq.saturating_sub(1);

        if self.committed_watermark.load(Ordering::SeqCst) >= target {
            return;
        }

        let mut rx = self.watermark_receiver.clone();
        loop {
            if self.committed_watermark.load(Ordering::SeqCst) >= target {
                return;
            }

            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    fn mark_committed(&self, seq: SequenceNumber) {
        if let Some(mut slot) = self.pending_sequences.get_mut(&seq) {
            *slot = SequenceState::Committed;
        }
        self.try_advance_watermark();
    }

    fn mark_abandoned(&self, seq: SequenceNumber) {
        if let Some(mut slot) = self.pending_sequences.get_mut(&seq) {
            *slot = SequenceState::Abandoned;
        }
        self.try_advance_watermark();
    }

    fn try_advance_watermark(&self) {
        let mut current = self.committed_watermark.load(Ordering::SeqCst);

        loop {
            let next = current + 1;

            let can_advance = match self.pending_sequences.get(&next) {
                Some(slot) => matches!(*slot, SequenceState::Committed | SequenceState::Abandoned),
                None => false,
            };

            if can_advance {
                self.pending_sequences.remove(&next);
                self.committed_watermark.store(next, Ordering::SeqCst);
                let _ = self.watermark_sender.send(next);
                current = next;
            } else {
                break;
            }
        }
    }
}

/// RAII guard: marks sequence as abandoned on drop if not committed
pub struct SequenceGuard {
    sequence: SequenceNumber,
    coordinator: Arc<WriteCoordinator>,
    completed: bool,
}

impl SequenceGuard {
    pub async fn wait_for_predecessors(&self) {
        self.coordinator.wait_for_predecessors(self.sequence).await;
    }

    pub fn mark_committed(&mut self) {
        if !self.completed {
            self.completed = true;
            self.coordinator.mark_committed(self.sequence);
        }
    }
}

impl Drop for SequenceGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.coordinator.mark_abandoned(self.sequence);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_sequential_commits() {
        let coordinator = Arc::new(WriteCoordinator::new());

        let mut guard1 = coordinator.allocate_sequence();
        let mut guard2 = coordinator.allocate_sequence();
        let mut guard3 = coordinator.allocate_sequence();

        assert_eq!(guard1.sequence, 1);
        assert_eq!(guard2.sequence, 2);
        assert_eq!(guard3.sequence, 3);

        guard1.wait_for_predecessors().await;
        guard1.mark_committed();

        guard2.wait_for_predecessors().await;
        guard2.mark_committed();

        guard3.wait_for_predecessors().await;
        guard3.mark_committed();

        assert_eq!(coordinator.committed_watermark.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_out_of_order_waits() {
        let coordinator = Arc::new(WriteCoordinator::new());

        let mut guard1 = coordinator.allocate_sequence();
        let mut guard2 = coordinator.allocate_sequence();

        let coord_clone = Arc::clone(&coordinator);
        let handle = tokio::spawn(async move {
            guard2.wait_for_predecessors().await;
            guard2.mark_committed();
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(coord_clone.committed_watermark.load(Ordering::SeqCst), 0);

        guard1.wait_for_predecessors().await;
        guard1.mark_committed();

        handle.await.unwrap();

        assert_eq!(coord_clone.committed_watermark.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_abandoned_sequence_unblocks_successors() {
        let coordinator = Arc::new(WriteCoordinator::new());

        let guard1 = coordinator.allocate_sequence();
        let mut guard2 = coordinator.allocate_sequence();

        drop(guard1);

        guard2.wait_for_predecessors().await;
        guard2.mark_committed();

        assert_eq!(coordinator.committed_watermark.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_first_sequence_no_wait() {
        let coordinator = Arc::new(WriteCoordinator::new());

        let mut guard1 = coordinator.allocate_sequence();

        guard1.wait_for_predecessors().await;
        guard1.mark_committed();

        assert_eq!(coordinator.committed_watermark.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_many_concurrent_sequences() {
        let coordinator = Arc::new(WriteCoordinator::new());
        let num_sequences = 100;

        let guards: Vec<_> = (0..num_sequences)
            .map(|_| coordinator.allocate_sequence())
            .collect();

        let handles: Vec<_> = guards
            .into_iter()
            .map(|mut guard| {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_micros((guard.sequence * 7) % 100)).await;
                    guard.wait_for_predecessors().await;
                    guard.mark_committed();
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(
            coordinator.committed_watermark.load(Ordering::SeqCst),
            num_sequences as u64
        );
    }
}
