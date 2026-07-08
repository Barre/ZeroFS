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
    /// Last seqno whose ship was acked, and commits since without one. Together
    /// with the epoch they form the provenance stamp each batch carries, which
    /// the takeover replay guard checks the buffered tail against.
    last_shipped: AtomicU64,
    solo_count: AtomicU64,
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
            last_shipped: AtomicU64::new(0),
            solo_count: AtomicU64::new(0),
            watermark: AtomicU64::new(0),
            pending: StdMutex::new(VecDeque::new()),
            sender: Mutex::new(None),
        })
    }

    /// Ship a batch and wait for its durable ack. `Some(seqno)` if shipped (the
    /// caller records durability for the prune watermark), `None` if solo. Either
    /// way the caller applies and acks; only a shipped write is two-node durable.
    pub async fn ship(&self, ops: &[ReplOp], op_ids: &[crate::dedup::OpId]) -> Option<u64> {
        let shipped = self.ship_inner(ops, op_ids).await;
        match shipped {
            Some(seqno) => {
                self.last_shipped.store(seqno, Ordering::Relaxed);
                self.solo_count.store(0, Ordering::Relaxed);
            }
            None => {
                self.solo_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        shipped
    }

    async fn ship_inner(&self, ops: &[ReplOp], op_ids: &[crate::dedup::OpId]) -> Option<u64> {
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

    /// The provenance stamp for the batch just shipped (or run solo): (epoch,
    /// last acked ship's seqno, solo commits since). Committed atomically with
    /// the batch by the write coordinator, whose single worker is the only
    /// `ship` caller, so the counters always describe the current batch.
    pub fn stamp(&self) -> (u64, u64, u64) {
        (
            self.writer_epoch,
            self.last_shipped.load(Ordering::Relaxed),
            self.solo_count.load(Ordering::Relaxed),
        )
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

    #[tokio::test]
    async fn ship_is_solo_when_disconnected() {
        let r = Replicator::new("http://127.0.0.1:1".to_string(), 0);
        assert_eq!(
            r.ship(&[ReplOp::Put("k".into(), "v".into())], &[]).await,
            None,
            "with no standby connected, ship must run solo (None)"
        );
    }

    use crate::dedup::DedupCache;
    use crate::replication::tail::TailBuffer;
    use crate::replication::transport::ReplicationReceiver;
    use std::sync::atomic::AtomicBool;
    use tokio::net::TcpListener;

    /// A running receiver. Hold it to keep the standby up; `stop()` it (or drop it)
    /// to model a standby outage that closes the existing connection.
    struct ServerHandle {
        shutdown: tokio::sync::oneshot::Sender<()>,
        join: tokio::task::JoinHandle<()>,
    }

    impl ServerHandle {
        /// Shut down gracefully and wait for connections to close, so a subsequent
        /// ship on the old connection is guaranteed to fail.
        async fn stop(self) {
            let _ = self.shutdown.send(());
            let _ = self.join.await;
        }
    }

    /// Stand up a real receiver over a loopback socket. Returns its endpoint, the
    /// shared tail buffer, and a handle controlling the server's lifetime.
    async fn spawn_receiver() -> (String, Arc<Mutex<TailBuffer>>, ServerHandle) {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = ReplicationReceiver::new(
            buffer.clone(),
            Arc::new(DedupCache::new(64)),
            Arc::new(AtomicBool::new(false)),
            None,
            "standby-under-test".to_string(),
        )
        .into_server();
        let (shutdown, rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        (
            format!("http://{addr}"),
            buffer,
            ServerHandle { shutdown, join },
        )
    }

    async fn connect(endpoint: &str) -> ReplicationSender {
        for _ in 0..100 {
            if let Ok(s) = ReplicationSender::connect(endpoint.to_string()).await {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("could not connect to receiver");
    }

    fn put() -> [ReplOp; 1] {
        [ReplOp::Put("k".into(), "v".into())]
    }

    #[tokio::test]
    async fn ship_assigns_increasing_seqnos_and_buffers_on_the_standby() {
        let (endpoint, buffer, _server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 7);
        *r.sender.lock().await = Some(connect(&endpoint).await);

        assert_eq!(r.ship(&put(), &[]).await, Some(1));
        assert_eq!(r.ship(&put(), &[]).await, Some(2));

        let seqnos: Vec<u64> = buffer
            .lock()
            .await
            .batches_in_order()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(
            seqnos,
            vec![1, 2],
            "shipped batches land on the standby in order"
        );
    }

    // A seqno is consumed only by a real ship, so they stay contiguous per term: a
    // solo write must not burn one (else a later takeover replays with a gap).
    #[tokio::test]
    async fn solo_ship_does_not_consume_a_seqno() {
        let (endpoint, _buffer, _server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 1);

        assert_eq!(r.ship(&put(), &[]).await, None, "no sender yet: solo");

        *r.sender.lock().await = Some(connect(&endpoint).await);
        assert_eq!(
            r.ship(&put(), &[]).await,
            Some(1),
            "first real ship is seqno 1"
        );
    }

    // A standby outage must not block writes: a failed ship downgrades to solo
    // (clears the sender) and the write still completes single-node durable.
    #[tokio::test]
    async fn ship_downgrades_to_solo_when_the_standby_is_unreachable() {
        let (endpoint, _buffer, server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 1);
        *r.sender.lock().await = Some(connect(&endpoint).await);
        assert_eq!(r.ship(&put(), &[]).await, Some(1));

        server.stop().await;
        assert_eq!(r.ship(&put(), &[]).await, None, "a failed ship runs solo");
        assert!(
            r.sender.lock().await.is_none(),
            "a failed ship must clear the sender so reconnect can re-establish it"
        );
    }

    // A deposed leader's ship is rejected by the standby (its epoch is below the
    // highest seen). The leader must downgrade, not loop re-shipping a rejected batch.
    #[tokio::test]
    async fn ship_downgrades_to_solo_on_stale_epoch_rejection() {
        let (endpoint, _buffer, _server) = spawn_receiver().await;
        // A newer leader (epoch 5) ships first, bumping the receiver's highest epoch.
        let newer = connect(&endpoint).await;
        assert!(
            newer
                .ship(1, &[ReplOp::Put("a".into(), "b".into())], &[], 0, 5)
                .await
                .unwrap()
        );

        let r = Replicator::new(endpoint.clone(), 1);
        *r.sender.lock().await = Some(connect(&endpoint).await);
        assert_eq!(r.ship(&put(), &[]).await, None, "a rejected ship runs solo");
        assert!(
            r.sender.lock().await.is_none(),
            "a rejected ship must clear the sender"
        );
    }

    // The provenance stamp tracks the last acked ship and counts solo commits
    // since; an acked ship resets the count. The replay guard reads solo > 0 at
    // the durable head as ships the leader outlived.
    #[tokio::test]
    async fn stamp_counts_solo_commits_and_resets_on_an_acked_ship() {
        let (endpoint, _buffer, server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 9);
        assert_eq!(r.stamp(), (9, 0, 0), "fresh replicator: nothing shipped");

        assert_eq!(r.ship(&put(), &[]).await, None, "starts solo (no sender)");
        assert_eq!(r.stamp(), (9, 0, 1));

        *r.sender.lock().await = Some(connect(&endpoint).await);
        assert_eq!(r.ship(&put(), &[]).await, Some(1));
        assert_eq!(r.stamp(), (9, 1, 0), "an acked ship resets the solo count");

        server.stop().await;
        assert_eq!(r.ship(&put(), &[]).await, None, "failed ship runs solo");
        assert_eq!(r.ship(&put(), &[]).await, None);
        assert_eq!(r.stamp(), (9, 1, 2), "solo commits count from the last ack");
    }

    #[tokio::test]
    async fn run_reconnect_reestablishes_the_sender() {
        let (endpoint, _buffer, _server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 1);
        assert!(r.sender.lock().await.is_none(), "starts solo");

        let recon = tokio::spawn(run_reconnect(Arc::downgrade(&r)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while r.sender.lock().await.is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("run_reconnect must establish the sender to a reachable standby");

        assert_eq!(
            r.ship(&put(), &[]).await,
            Some(1),
            "shipping resumes after reconnect"
        );
        recon.abort();
    }
}
