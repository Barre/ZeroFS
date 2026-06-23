//! The leader's replicator: ships each committed batch to the standby and waits
//! for its durable ack BEFORE the write coordinator applies the batch
//! (commit-then-apply), so anything a read can see is already replicated.
//!
//! Dynamic: ships while a standby is reachable; on a ship failure or timeout it
//! downgrades to solo (single-node durable) so a standby outage never blocks
//! writes, and a reconnect loop re-establishes shipping when the standby returns.
//! Downgrading is split-brain-safe: a downgraded leader still heartbeats (holds
//! its lease), so the standby never takes over.
//!
//! Also drives the prune watermark: only confirmed-durable batches are pruned, so
//! the watermark can never outrun durability.

use crate::replication::tail::ReplOp;
use crate::replication::transport::ReplicationSender;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

/// How long a single ship may take before the leader downgrades to solo.
const SHIP_TIMEOUT: Duration = Duration::from_secs(5);
/// How often a solo leader retries connecting to its standby.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

pub struct Replicator {
    peer_endpoint: String,
    writer_epoch: u64,
    /// Next batch seqno, assigned only when a batch is actually shipped.
    seqno: AtomicU64,
    /// Highest batch seqno confirmed durable on object storage.
    watermark: AtomicU64,
    /// Applied-but-not-yet-durable shipped batches: (batch seqno, SlateDB seqno).
    pending: StdMutex<VecDeque<(u64, u64)>>,
    /// Live sender, `None` in solo mode. Set only by the reconnect loop, cleared
    /// only by a failed ship.
    sender: Mutex<Option<ReplicationSender>>,
}

impl Replicator {
    /// Starts solo; the reconnect loop establishes the connection.
    pub fn new(peer_endpoint: String, writer_epoch: u64) -> Arc<Self> {
        Arc::new(Self {
            peer_endpoint,
            writer_epoch,
            seqno: AtomicU64::new(1),
            watermark: AtomicU64::new(0),
            pending: StdMutex::new(VecDeque::new()),
            sender: Mutex::new(None),
        })
    }

    /// Ship a batch and wait for its durable ack. `Some(seqno)` if shipped (the
    /// caller records durability for the prune watermark), `None` if solo. Either
    /// way the caller applies and acks; only a shipped write is two-node durable.
    pub async fn ship(&self, ops: &[ReplOp], op_ids: &[crate::dedup::OpId]) -> Option<u64> {
        let mut guard = self.sender.lock().await;
        let sender = guard.as_ref()?;
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);
        let watermark = self.watermark.load(Ordering::Relaxed);
        let shipped = tokio::time::timeout(
            SHIP_TIMEOUT,
            sender.ship(seqno, ops, op_ids, watermark, self.writer_epoch),
        )
        .await;
        match shipped {
            Ok(Ok(true)) => Some(seqno),
            Ok(Ok(false)) => {
                tracing::error!(
                    "HA: standby rejected a ship (stale writer epoch?); downgrading to solo"
                );
                *guard = None;
                None
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "HA: standby ship failed; downgrading to solo (new writes are \
                     single-node durable until the standby returns): {e:#}"
                );
                *guard = None;
                None
            }
            Err(_) => {
                tracing::warn!("HA: standby ship timed out; downgrading to solo");
                *guard = None;
                None
            }
        }
    }

    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    pub fn mark_applied(&self, seqno: u64, slatedb_seqno: u64) {
        self.pending
            .lock()
            .unwrap()
            .push_back((seqno, slatedb_seqno));
    }

    /// Advance the watermark over applied batches now durable (<= `durable_seq`),
    /// stopping at the first not-yet-durable one so it never outruns durability.
    pub fn advance_watermark(&self, durable_seq: u64) {
        let mut pending = self.pending.lock().unwrap();
        let mut w = self.watermark.load(Ordering::Relaxed);
        while let Some(&(batch_seqno, slatedb_seqno)) = pending.front() {
            if slatedb_seqno <= durable_seq {
                w = w.max(batch_seqno);
                pending.pop_front();
            } else {
                break;
            }
        }
        self.watermark.store(w, Ordering::Relaxed);
    }
}

/// Re-establish shipping whenever the leader is solo and the standby is
/// reachable. Runs until the replicator is dropped.
pub async fn run_reconnect(replicator: Weak<Replicator>) {
    let mut ticker = tokio::time::interval(RECONNECT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let Some(replicator) = replicator.upgrade() else {
            break;
        };
        if replicator.sender.lock().await.is_some() {
            continue;
        }
        let endpoint = replicator.peer_endpoint.clone();
        match ReplicationSender::connect(endpoint).await {
            Ok(sender) => {
                let mut guard = replicator.sender.lock().await;
                // Reconnect loop is the only writer of `Some`, so no race.
                if guard.is_none() {
                    *guard = Some(sender);
                    tracing::info!("HA: connected to standby; semi-sync replication resumed");
                }
            }
            Err(_) => { /* standby still down; retry next tick */ }
        }
    }
}

/// Drive the prune watermark from the data db's `durable_seq` until the status
/// channel closes.
pub async fn run_watermark(
    replicator: Arc<Replicator>,
    mut durable: tokio::sync::watch::Receiver<slatedb::DbStatus>,
) {
    replicator.advance_watermark(durable.borrow().durable_seq);
    while durable.changed().await.is_ok() {
        let seq = durable.borrow().durable_seq;
        replicator.advance_watermark(seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_is_conservative() {
        let r = Replicator::new("http://unused".to_string(), 0);
        for (b, s) in [(1u64, 10u64), (2, 20), (3, 30)] {
            r.mark_applied(b, s);
        }
        r.advance_watermark(15); // only batch 1 (slatedb 10) durable
        assert_eq!(r.watermark.load(Ordering::Relaxed), 1);
        r.advance_watermark(25); // batch 2 (20) durable; batch 3 (30) not
        assert_eq!(r.watermark.load(Ordering::Relaxed), 2);
        r.advance_watermark(100); // all durable
        assert_eq!(r.watermark.load(Ordering::Relaxed), 3);
        assert!(r.pending.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ship_is_solo_when_disconnected() {
        let r = Replicator::new("http://127.0.0.1:1".to_string(), 0);
        assert_eq!(
            r.ship(&[ReplOp::Put("k".into(), "v".into())], &[]).await,
            None,
            "with no standby connected, ship must run solo (None)"
        );
    }
}
