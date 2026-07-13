//! The leader's replicator: ships each committed batch to the standby and waits
//! for its durable ack BEFORE the write coordinator applies the batch
//! (commit-then-apply), so anything a read can see is already replicated.
//!
//! Dynamic: ships while a peer is usable; on a ship failure or timeout it
//! downgrades to solo (single-node durable) so an outage never blocks writes,
//! and a reconnect loop re-establishes shipping when the peer returns.
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

/// Outcome of shipping one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShipOutcome {
    /// The standby durably acked: two-node durable; the seqno feeds the prune
    /// watermark.
    Shipped(u64),
    /// No usable peer: the caller applies and acks single-node durable.
    Solo,
    /// The standby rejected the ship: authoritative evidence a newer writer
    /// exists and this leader is deposed. Terminal; the caller must fail the
    /// batch, never ack it.
    Deposed,
}

enum ReplicationState {
    Solo,
    Connected(Box<ReplicationSender>),
    Deposed,
}

/// How long a single ship may take before the leader downgrades to solo.
const SHIP_TIMEOUT: Duration = Duration::from_secs(5);
/// How often a solo leader retries connecting to its standby.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

pub struct Replicator {
    peer_endpoint: String,
    writer_epoch: u64,
    /// Next seqno for a ship attempt; attempted seqnos are never reused.
    seqno: AtomicU64,
    /// Last seqno whose ship was acked, and commits since without one. Together
    /// with the epoch they form the provenance stamp each batch carries, which
    /// takeover reconciliation checks the buffered tail against.
    last_shipped: AtomicU64,
    solo_count: AtomicU64,
    /// Highest batch seqno confirmed durable on object storage.
    watermark: AtomicU64,
    /// Applied-but-not-yet-durable shipped batches: (batch seqno, SlateDB seqno).
    pending: StdMutex<VecDeque<(u64, u64)>>,
    state: Mutex<ReplicationState>,
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
            state: Mutex::new(ReplicationState::Solo),
        })
    }

    /// Ship a batch and wait for its durable ack. On `Shipped`/`Solo` the caller
    /// applies and acks (only a shipped write is two-node durable); on `Deposed`
    /// it must fail the batch.
    pub async fn ship(&self, ops: &[ReplOp], op_ids: &[crate::dedup::OpId]) -> ShipOutcome {
        let outcome = self.ship_inner(ops, op_ids).await;
        match outcome {
            ShipOutcome::Shipped(seqno) => {
                self.last_shipped.store(seqno, Ordering::Relaxed);
                self.solo_count.store(0, Ordering::Relaxed);
            }
            ShipOutcome::Solo => {
                self.solo_count.fetch_add(1, Ordering::Relaxed);
            }
            ShipOutcome::Deposed => {}
        }
        outcome
    }

    async fn ship_inner(&self, ops: &[ReplOp], op_ids: &[crate::dedup::OpId]) -> ShipOutcome {
        let mut state = self.state.lock().await;
        let sender = match &*state {
            ReplicationState::Solo => return ShipOutcome::Solo,
            ReplicationState::Connected(sender) => sender,
            ReplicationState::Deposed => return ShipOutcome::Deposed,
        };
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);
        let watermark = self.watermark.load(Ordering::Relaxed);
        let shipped = tokio::time::timeout(
            SHIP_TIMEOUT,
            sender.ship(seqno, ops, op_ids, watermark, self.writer_epoch),
        )
        .await;
        match shipped {
            Ok(Ok(true)) => ShipOutcome::Shipped(seqno),
            Ok(Ok(false)) => {
                tracing::error!("HA: standby rejected a ship: this leader is deposed");
                *state = ReplicationState::Deposed;
                ShipOutcome::Deposed
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "HA: standby ship failed; downgrading to solo (new writes are \
                     single-node durable until the standby returns): {e:#}"
                );
                *state = ReplicationState::Solo;
                ShipOutcome::Solo
            }
            Err(_) => {
                tracing::warn!("HA: standby ship timed out; downgrading to solo");
                *state = ReplicationState::Solo;
                ShipOutcome::Solo
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

#[cfg(test)]
impl Replicator {
    /// Test wiring for transitions normally driven by the reconnect loop.
    pub(crate) async fn set_sender_for_tests(&self, sender: Option<ReplicationSender>) {
        *self.state.lock().await = match sender {
            Some(sender) => ReplicationState::Connected(Box::new(sender)),
            None => ReplicationState::Solo,
        };
    }

    async fn is_connected_for_tests(&self) -> bool {
        matches!(&*self.state.lock().await, ReplicationState::Connected(_))
    }
}

/// Re-establish shipping whenever the leader is solo and the standby is
/// reachable. Stops when the replicator is dropped or deposed.
pub async fn run_reconnect(replicator: Weak<Replicator>) {
    let mut ticker = tokio::time::interval(RECONNECT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let Some(replicator) = replicator.upgrade() else {
            break;
        };
        {
            let state = replicator.state.lock().await;
            match &*state {
                ReplicationState::Solo => {}
                ReplicationState::Connected(_) => continue,
                ReplicationState::Deposed => break,
            }
        }
        let endpoint = replicator.peer_endpoint.clone();
        match ReplicationSender::connect(endpoint).await {
            Ok(sender) => {
                let mut state = replicator.state.lock().await;
                if matches!(&*state, ReplicationState::Solo) {
                    *state = ReplicationState::Connected(Box::new(sender));
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
            ShipOutcome::Solo,
            "with no standby connected, ship must run solo"
        );
    }

    use crate::dedup::DedupCache;
    use crate::replication::transport::{ReceiverControl, ReplicationReceiver};
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

    async fn spawn_receiver_at(
        local_writer_epoch: Option<u64>,
    ) -> (String, ReceiverControl, ServerHandle) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let receiver = ReplicationReceiver::new(
            Arc::new(DedupCache::new(64)),
            None,
            "standby-under-test".to_string(),
        );
        let control = receiver.control();
        if let Some(epoch) = local_writer_epoch {
            control
                .begin_promotion(epoch)
                .await
                .unwrap()
                .complete()
                .await
                .unwrap();
        }
        let server = receiver.into_server();
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
            control,
            ServerHandle { shutdown, join },
        )
    }

    async fn spawn_receiver() -> (String, ReceiverControl, ServerHandle) {
        spawn_receiver_at(None).await
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
        let (endpoint, control, _server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 7);
        r.set_sender_for_tests(Some(connect(&endpoint).await)).await;

        assert_eq!(r.ship(&put(), &[]).await, ShipOutcome::Shipped(1));
        assert_eq!(r.ship(&put(), &[]).await, ShipOutcome::Shipped(2));

        let seqnos: Vec<u64> = control
            .inspect_standby_for_tests(|tail, _| tail.batches_in_order().map(|(s, _)| s).collect())
            .await;
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
        let (endpoint, _control, _server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 1);

        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Solo,
            "no sender yet: solo"
        );

        r.set_sender_for_tests(Some(connect(&endpoint).await)).await;
        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Shipped(1),
            "first real ship is seqno 1"
        );
    }

    // A standby outage must not block writes: a failed ship downgrades to solo
    // (clears the sender) and the write still completes single-node durable.
    #[tokio::test]
    async fn ship_downgrades_to_solo_when_the_standby_is_unreachable() {
        let (endpoint, _control, server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 1);
        r.set_sender_for_tests(Some(connect(&endpoint).await)).await;
        assert_eq!(r.ship(&put(), &[]).await, ShipOutcome::Shipped(1));

        server.stop().await;
        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Solo,
            "a failed ship runs solo"
        );
        assert!(
            !r.is_connected_for_tests().await,
            "a failed ship must clear the sender so reconnect can re-establish it"
        );
    }

    // A rejected ship (epoch below the standby's highest seen) is deposal
    // evidence, not an outage: the outcome is terminal, and the reconnect loop
    // must not re-establish shipping for a fenced leader.
    #[tokio::test]
    async fn ship_reports_deposed_on_stale_epoch_rejection_and_stays_deposed() {
        let (endpoint, _control, _server) = spawn_receiver().await;
        // A newer leader (epoch 5) ships first, bumping the receiver's highest epoch.
        let newer = connect(&endpoint).await;
        assert!(
            newer
                .ship(1, &[ReplOp::Put("a".into(), "b".into())], &[], 0, 5)
                .await
                .unwrap()
        );

        let r = Replicator::new(endpoint.clone(), 1);
        r.set_sender_for_tests(Some(connect(&endpoint).await)).await;
        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Deposed,
            "a rejected ship is deposal evidence"
        );
        assert!(
            !r.is_connected_for_tests().await,
            "a rejected ship must clear the sender"
        );
        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Deposed,
            "deposal is sticky"
        );

        // The reconnect loop sees the deposal and stops instead of re-dialing.
        let recon = tokio::spawn(run_reconnect(Arc::downgrade(&r)));
        tokio::time::timeout(Duration::from_secs(5), recon)
            .await
            .expect("run_reconnect must stop for a deposed replicator")
            .unwrap();
        assert!(
            !r.is_connected_for_tests().await,
            "a deposed replicator must not get a new sender"
        );
    }

    // A fenced former leader can remain reachable until its next failed flush.
    // Its stale leading state must not depose the newer writer.
    #[tokio::test]
    async fn newer_writer_is_not_deposed_by_a_stale_leading_peer() {
        let (endpoint, _control, _server) = spawn_receiver_at(Some(7)).await;
        let r = Replicator::new(endpoint.clone(), 8);
        r.set_sender_for_tests(Some(connect(&endpoint).await)).await;

        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Solo,
            "an epoch-8 writer must treat a stale epoch-7 receiver as unavailable"
        );
        assert!(
            !r.is_connected_for_tests().await,
            "the stale receiver must be disconnected"
        );
        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Solo,
            "the ambiguous rejection must not latch terminal deposal"
        );
    }

    // The provenance stamp tracks the last acked ship and counts solo commits
    // since; an acked ship resets the count. Takeover replay reads solo > 0 at
    // the durable head as ships the leader outlived.
    #[tokio::test]
    async fn stamp_counts_solo_commits_and_resets_on_an_acked_ship() {
        let (endpoint, _control, server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 9);
        assert_eq!(r.stamp(), (9, 0, 0), "fresh replicator: nothing shipped");

        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Solo,
            "starts solo (no sender)"
        );
        assert_eq!(r.stamp(), (9, 0, 1));

        r.set_sender_for_tests(Some(connect(&endpoint).await)).await;
        assert_eq!(r.ship(&put(), &[]).await, ShipOutcome::Shipped(1));
        assert_eq!(r.stamp(), (9, 1, 0), "an acked ship resets the solo count");

        server.stop().await;
        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Solo,
            "failed ship runs solo"
        );
        assert_eq!(r.ship(&put(), &[]).await, ShipOutcome::Solo);
        assert_eq!(r.stamp(), (9, 1, 2), "solo commits count from the last ack");
    }

    #[tokio::test]
    async fn run_reconnect_reestablishes_the_sender() {
        let (endpoint, _control, _server) = spawn_receiver().await;
        let r = Replicator::new(endpoint.clone(), 1);
        assert!(!r.is_connected_for_tests().await, "starts solo");

        let recon = tokio::spawn(run_reconnect(Arc::downgrade(&r)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !r.is_connected_for_tests().await {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("run_reconnect must establish the sender to a reachable standby");

        assert_eq!(
            r.ship(&put(), &[]).await,
            ShipOutcome::Shipped(1),
            "shipping resumes after reconnect"
        );
        recon.abort();
    }
}
