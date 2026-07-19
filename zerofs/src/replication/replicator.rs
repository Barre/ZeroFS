//! Leader-side ship-before-apply sequencing. Transport failure changes the
//! writer to Solo; reconnect restores replication. Prune watermarks include only
//! batches confirmed durable by the local database.

use crate::replication::tail::ReplOp;
use crate::replication::transport::{ReplicationSender, ShipResult};
use crate::replication::types::{
    CoverageFrontier, HaStamp, PruneWatermark, ShipSeqno, SlateDbSeqno, SoloHistory, WriterEpoch,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

/// One step in the ordered ship -> local-apply protocol.
pub enum ShipOutcome<'a> {
    /// Permit and HA stamp for the current local apply.
    Apply(ApplyPermit<'a>),
    /// Requires a durability barrier for an earlier local prefix. The current
    /// ship sequence remains unconsumed after `BaseNotCovered`.
    NeedsBaseFlush(BaseFlushRequired<'a>),
    /// The standby reports that this writer epoch is deposed.
    Deposed,
    /// The sequencer cannot preserve replication lineage.
    Poisoned,
}

enum ReplicationState {
    Solo,
    // The library target does not compile the binary-only reconnect driver.
    #[allow(dead_code)]
    Connected(Box<ReplicationSender>),
}

/// Ship RPC timeout before transition to Solo.
const SHIP_TIMEOUT: Duration = Duration::from_secs(5);
/// Solo reconnect interval.
#[allow(dead_code)] // Used by the binary CLI, which owns the reconnect task.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// Cloneable control plane for reconnect, watermark, and deposal tasks. Mutation
/// shipping remains on the non-cloneable [`Replicator`].
pub(crate) struct ReplicationControl {
    #[allow(dead_code)] // Used by the binary CLI's reconnect task.
    peer_endpoint: String,
    /// Writer term this control plane and its heartbeat ACK channel belong to.
    writer_epoch: WriterEpoch,
    state: Mutex<ReplicationState>,
    deposed: CancellationToken,
    durability: StdMutex<DurabilityTracker>,
    /// Coverage-valid heartbeat acknowledgements for this writer epoch.
    heartbeat_acks: watch::Sender<Option<HeartbeatAck>>,
    #[allow(dead_code)] // Sent by the binary-only heartbeat supervisor.
    repair_sender: mpsc::UnboundedSender<BaseRepairRequest>,
}

/// Exact-epoch acknowledgement anchored at local request start time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeartbeatAck {
    pub(crate) started: tokio::time::Instant,
}

/// Mutation and ship sequencer for one writer term. `ship` holds its exclusive
/// borrow until the returned capability is resolved.
pub struct Replicator {
    control: Arc<ReplicationControl>,
    writer_epoch: WriterEpoch,
    next_seqno: Option<ShipSeqno>,
    last_shipped: Option<ShipSeqno>,
    solo_history: SoloHistory,
    applied_through: Option<ShipSeqno>,
    /// Highest local DB sequence not yet covered by a mandatory repair flush.
    /// Includes Solo/Ambiguous progress and a connected receiver's missing
    /// prefix after it reports `BaseNotCovered`.
    base_dirty_through: Option<SlateDbSeqno>,
    /// Highest local database application in this writer term, regardless of
    /// whether it was shipped, ambiguous, or Solo.
    last_local_applied: Option<SlateDbSeqno>,
    repair_receiver: mpsc::UnboundedReceiver<BaseRepairRequest>,
    poisoned: bool,
}

/// Completion channel for a durability barrier ordered through the sequencer.
pub(crate) type BaseRepairRequest = oneshot::Sender<()>;

#[derive(Default)]
struct DurabilityTracker {
    observed_durable: u64,
    applied_through: Option<ShipSeqno>,
    watermark: Option<ShipSeqno>,
    pending: VecDeque<(ShipSeqno, SlateDbSeqno)>,
}

impl DurabilityTracker {
    #[allow(dead_code)] // Driven by the binary CLI's SlateDB status subscription.
    fn observe_durable(&mut self, durable: SlateDbSeqno) {
        self.observed_durable = self.observed_durable.max(durable.get());
        self.advance();
    }

    fn record_applied(&mut self, ship: ShipSeqno, local: SlateDbSeqno) {
        assert!(
            self.applied_through.is_none_or(|previous| previous < ship),
            "HA peer-copy attempts must be registered in sequence order"
        );
        self.applied_through = Some(ship);
        self.pending.push_back((ship, local));
        // Account for durability notifications received before registration.
        self.advance();
    }

    fn advance(&mut self) {
        while let Some(&(ship, local)) = self.pending.front() {
            if local.get() > self.observed_durable {
                break;
            }
            self.watermark = Some(self.watermark.map_or(ship, |current| current.max(ship)));
            self.pending.pop_front();
        }
    }

    fn watermark_for(&self, current: ShipSeqno) -> PruneWatermark {
        PruneWatermark::for_ship(self.watermark.map_or(0, ShipSeqno::get), current)
            .expect("HA durability watermark must precede the current ship")
    }

    fn coverage_frontier(&self) -> CoverageFrontier {
        CoverageFrontier::new(self.applied_through, self.watermark)
            .expect("HA durable frontier must not outrun applied attempts")
    }
}

impl ReplicationControl {
    /// Stops shipping and reconnect after observing a newer writer epoch.
    #[allow(dead_code)] // Called by the binary CLI's leadership supervisor.
    pub(crate) async fn depose(&self) {
        self.deposed.cancel();
        // Reconnect and ship recheck `deposed` while holding `state`.
        *self.state.lock().await = ReplicationState::Solo;
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn deposal_token(&self) -> CancellationToken {
        self.deposed.clone()
    }

    #[allow(dead_code)] // Awaited by the binary-only heartbeat supervisor.
    pub(crate) async fn deposed(&self) {
        self.deposed.cancelled().await;
    }

    fn record_applied(&self, ship: ShipSeqno, local: SlateDbSeqno) {
        self.durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_applied(ship, local);
    }

    #[allow(dead_code)] // Driven by the binary CLI's SlateDB status subscription.
    fn observe_durable(&self, durable: SlateDbSeqno) {
        self.durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observe_durable(durable);
    }

    fn watermark_for(&self, current: ShipSeqno) -> PruneWatermark {
        self.durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .watermark_for(current)
    }

    /// Applied and durable frontiers advertised in heartbeats.
    pub(crate) fn coverage_frontier(&self) -> CoverageFrontier {
        self.durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .coverage_frontier()
    }

    /// Subscribe to epoch-validated heartbeat acknowledgements from the peer.
    #[allow(dead_code)] // Consumed by the binary-only authority supervisor.
    pub(crate) fn heartbeat_acks(&self) -> watch::Receiver<Option<HeartbeatAck>> {
        self.heartbeat_acks.subscribe()
    }

    /// Publishes an acknowledgement for the control plane's writer epoch.
    pub(crate) fn acknowledge_heartbeat(
        &self,
        writer_epoch: WriterEpoch,
        started: tokio::time::Instant,
    ) {
        if writer_epoch != self.writer_epoch {
            tracing::error!(
                expected_epoch = self.writer_epoch.get(),
                acknowledged_epoch = writer_epoch.get(),
                "HA heartbeat acknowledgement has the wrong writer epoch"
            );
            return;
        }
        self.heartbeat_acks
            .send_replace(Some(HeartbeatAck { started }));
    }

    /// Flushes all applications ordered before this request and advances the
    /// durability watermark before completion.
    #[allow(dead_code)] // Called by the binary-only heartbeat supervisor.
    pub(crate) async fn repair_base(&self) -> anyhow::Result<()> {
        let (completion, completed) = oneshot::channel();
        self.repair_sender
            .send(completion)
            .map_err(|_| anyhow::anyhow!("HA base-repair sequencer stopped"))?;
        completed
            .await
            .map_err(|_| anyhow::anyhow!("HA base-repair sequencer stopped before completion"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyKind {
    Shipped(ShipSeqno),
    Solo,
    Ambiguous(ShipSeqno),
}

impl ApplyKind {
    fn peer_copy(self) -> Option<ShipSeqno> {
        match self {
            Self::Shipped(seqno) | Self::Ambiguous(seqno) => Some(seqno),
            Self::Solo => None,
        }
    }

    fn ran_solo(self) -> bool {
        matches!(self, Self::Solo | Self::Ambiguous(_))
    }
}

/// Capability binding one ship result to exactly one local database apply.
#[must_use = "the shipped batch must be applied or explicitly failed"]
pub struct ApplyPermit<'a> {
    sequencer: &'a mut Replicator,
    kind: ApplyKind,
    candidate: HaStamp,
    resolved: bool,
}

impl ApplyPermit<'_> {
    pub(crate) fn stamp(&self) -> &HaStamp {
        &self.candidate
    }

    pub(crate) fn requires_solo_taint(&self) -> bool {
        self.kind.ran_solo()
    }

    /// Commits provenance after its stamped batch applies locally.
    pub(crate) fn applied(mut self, local_seqno: SlateDbSeqno) {
        self.sequencer.last_shipped = self.candidate.last_shipped();
        self.sequencer.solo_history = self.candidate.solo_history();
        self.sequencer.applied_through = self.candidate.applied_through();
        self.sequencer.last_local_applied = Some(
            self.sequencer
                .last_local_applied
                .map_or(local_seqno, |through| through.max(local_seqno)),
        );
        match self.kind {
            ApplyKind::Shipped(ship_seqno) | ApplyKind::Ambiguous(ship_seqno) => {
                self.sequencer
                    .control
                    .record_applied(ship_seqno, local_seqno);
            }
            ApplyKind::Solo => {}
        }
        if self.kind.ran_solo() {
            self.sequencer.base_dirty_through = Some(
                self.sequencer
                    .base_dirty_through
                    .map_or(local_seqno, |through| through.max(local_seqno)),
            );
        }
        self.resolved = true;
    }

    /// Resolves a failed local apply. A peer-copy attempt poisons the sequencer
    /// and returns its ship sequence.
    pub(crate) fn failed(mut self) -> Result<(), PeerCopyApplyFailure> {
        let peer_copy = self.kind.peer_copy();
        if peer_copy.is_some() {
            self.sequencer.poisoned = true;
        }
        self.resolved = true;
        match peer_copy {
            Some(seqno) => Err(PeerCopyApplyFailure { seqno }),
            None => Ok(()),
        }
    }
}

impl Drop for ApplyPermit<'_> {
    fn drop(&mut self) {
        if !self.resolved && self.kind.peer_copy().is_some() {
            self.sequencer.poisoned = true;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("HA batch {seqno} may be buffered on the standby but failed local apply")]
pub(crate) struct PeerCopyApplyFailure {
    seqno: ShipSeqno,
}

impl PeerCopyApplyFailure {
    pub(crate) const fn seqno(self) -> ShipSeqno {
        self.seqno
    }
}

/// Flush obligation for a local-only prefix required before reconnect shipping.
#[must_use = "the HA base remains dirty until a flush receipt discharges it"]
pub struct BaseFlushRequired<'a> {
    sequencer: &'a mut Replicator,
    through: SlateDbSeqno,
}

impl BaseFlushRequired<'_> {
    pub(crate) const fn through(&self) -> SlateDbSeqno {
        self.through
    }

    pub(crate) fn complete(self, _receipt: crate::fs::flush_coordinator::FlushReceipt) {
        assert!(
            self.sequencer
                .base_dirty_through
                .is_some_and(|dirty| dirty <= self.through),
            "HA flush receipt does not cover the dirty replication base"
        );
        // Publish the flush boundary before a retry ship.
        self.sequencer.control.observe_durable(self.through);
        self.sequencer.base_dirty_through = None;
    }
}

impl Replicator {
    /// Starts in Solo and returns its background-task control plane.
    #[allow(dead_code)] // Constructed by the binary CLI; unit tests use it too.
    pub(crate) fn new(
        peer_endpoint: String,
        writer_epoch: WriterEpoch,
    ) -> (Self, Arc<ReplicationControl>) {
        let (repair_sender, repair_receiver) = mpsc::unbounded_channel();
        let control = Arc::new(ReplicationControl {
            peer_endpoint,
            writer_epoch,
            state: Mutex::new(ReplicationState::Solo),
            deposed: CancellationToken::new(),
            durability: StdMutex::new(DurabilityTracker::default()),
            heartbeat_acks: watch::channel(None).0,
            repair_sender,
        });
        (
            Self {
                control: control.clone(),
                writer_epoch,
                next_seqno: ShipSeqno::new(1),
                last_shipped: None,
                solo_history: SoloHistory::never(),
                applied_through: None,
                base_dirty_through: None,
                last_local_applied: None,
                repair_receiver,
                poisoned: false,
            },
            control,
        )
    }

    /// Ships a batch and returns the next ordered action. `Apply` permits local
    /// apply; `NeedsBaseFlush` requires a durability barrier and retry;
    /// `Deposed` and `Poisoned` reject the batch.
    pub(crate) async fn ship(
        &mut self,
        ops: &[ReplOp],
        dedup_entries: &[crate::dedup::DedupEntry],
    ) -> ShipOutcome<'_> {
        if self.poisoned {
            return ShipOutcome::Poisoned;
        }
        if self.control.deposed.is_cancelled() {
            return ShipOutcome::Deposed;
        }

        let mut state = self.control.state.lock().await;
        if self.control.deposed.is_cancelled() {
            *state = ReplicationState::Solo;
            return ShipOutcome::Deposed;
        }
        let sender = match &*state {
            ReplicationState::Solo => {
                drop(state);
                return self
                    .make_apply_permit(ApplyKind::Solo)
                    .map_or(ShipOutcome::Poisoned, ShipOutcome::Apply);
            }
            ReplicationState::Connected(sender) => sender,
        };
        if let Some(through) = self.base_dirty_through {
            drop(state);
            return ShipOutcome::NeedsBaseFlush(BaseFlushRequired {
                sequencer: self,
                through,
            });
        }
        let Some(seqno) = self.next_seqno else {
            self.poisoned = true;
            return ShipOutcome::Poisoned;
        };
        // RPC attempts consume their sequence before transmission.
        self.next_seqno = seqno.checked_next();
        let watermark = self.control.watermark_for(seqno);
        let shipped = tokio::time::timeout(
            SHIP_TIMEOUT,
            sender.ship(seqno, ops, dedup_entries, watermark, self.writer_epoch),
        )
        .await;
        let kind = match shipped {
            Ok(Ok(ShipResult::Accepted)) => ApplyKind::Shipped(seqno),
            Ok(Ok(ShipResult::SenderDeposed)) => {
                tracing::error!("HA standby deposed this writer");
                *state = ReplicationState::Solo;
                self.control.deposed.cancel();
                return ShipOutcome::Deposed;
            }
            Ok(Ok(ShipResult::BaseNotCovered)) => {
                // BaseNotCovered is a pre-append rejection; retain this sequence.
                self.next_seqno = Some(seqno);
                let Some(through) = self.last_local_applied else {
                    tracing::error!(
                        "HA receiver rejected sequence {seqno} for missing predecessors, but \
                         this sequencer has no earlier local application"
                    );
                    self.poisoned = true;
                    return ShipOutcome::Poisoned;
                };
                self.base_dirty_through = Some(
                    self.base_dirty_through
                        .map_or(through, |dirty| dirty.max(through)),
                );
                drop(state);
                return ShipOutcome::NeedsBaseFlush(BaseFlushRequired {
                    sequencer: self,
                    through,
                });
            }
            Ok(Err(e)) => {
                tracing::warn!("HA standby ship failed; switching to Solo: {e:#}");
                *state = ReplicationState::Solo;
                ApplyKind::Ambiguous(seqno)
            }
            Err(_) => {
                tracing::warn!("HA standby ship timed out; switching to Solo");
                *state = ReplicationState::Solo;
                ApplyKind::Ambiguous(seqno)
            }
        };
        drop(state);
        self.make_apply_permit(kind)
            .map_or(ShipOutcome::Poisoned, ShipOutcome::Apply)
    }

    /// Receives the next heartbeat repair request in sequencer order.
    pub(crate) async fn next_base_repair(&mut self) -> Option<BaseRepairRequest> {
        self.repair_receiver.recv().await
    }

    /// Creates a flush obligation through the latest local apply.
    pub(crate) fn begin_base_repair(&mut self) -> Option<BaseFlushRequired<'_>> {
        let through = self.last_local_applied?;
        self.base_dirty_through = Some(
            self.base_dirty_through
                .map_or(through, |dirty| dirty.max(through)),
        );
        Some(BaseFlushRequired {
            sequencer: self,
            through,
        })
    }

    fn make_apply_permit(&mut self, kind: ApplyKind) -> Option<ApplyPermit<'_>> {
        let (last_shipped, solo_history, applied_through) = match kind {
            ApplyKind::Shipped(seqno) => (
                Some(seqno),
                if self.solo_history.ran_solo() {
                    SoloHistory::ever(0)
                } else {
                    SoloHistory::never()
                },
                Some(
                    self.applied_through
                        .map_or(seqno, |current| current.max(seqno)),
                ),
            ),
            ApplyKind::Solo | ApplyKind::Ambiguous(_) => {
                let Some(solo_count) = self.solo_history.commits_since_last_ship().checked_add(1)
                else {
                    self.poisoned = true;
                    return None;
                };
                let applied_through = match kind {
                    ApplyKind::Ambiguous(seqno) => Some(
                        self.applied_through
                            .map_or(seqno, |current| current.max(seqno)),
                    ),
                    ApplyKind::Solo => self.applied_through,
                    ApplyKind::Shipped(_) => unreachable!(),
                };
                (
                    self.last_shipped,
                    SoloHistory::ever(solo_count),
                    applied_through,
                )
            }
        };
        let candidate = match HaStamp::new(
            self.writer_epoch,
            last_shipped,
            solo_history,
            applied_through,
        ) {
            Ok(candidate) => candidate,
            Err(error) => {
                tracing::error!("HA sequencer produced invalid provenance: {error}");
                self.poisoned = true;
                return None;
            }
        };
        Some(ApplyPermit {
            sequencer: self,
            kind,
            candidate,
            resolved: false,
        })
    }
}

#[cfg(test)]
impl ReplicationControl {
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

    pub(crate) fn is_deposed_for_tests(&self) -> bool {
        self.deposed.is_cancelled()
    }
}

/// Reconnects a Solo writer to its standby until drop or deposal.
#[allow(dead_code)] // Spawned only by the binary CLI.
pub(crate) async fn run_reconnect(replicator: Weak<ReplicationControl>) {
    let mut ticker = tokio::time::interval(RECONNECT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let Some(replicator) = replicator.upgrade() else {
            break;
        };
        if replicator.deposed.is_cancelled() {
            break;
        }
        {
            let state = replicator.state.lock().await;
            match &*state {
                ReplicationState::Solo => {}
                ReplicationState::Connected(_) => continue,
            }
        }
        let endpoint = replicator.peer_endpoint.clone();
        if let Ok(sender) = ReplicationSender::connect(endpoint).await {
            let mut state = replicator.state.lock().await;
            if !replicator.deposed.is_cancelled() && matches!(&*state, ReplicationState::Solo) {
                *state = ReplicationState::Connected(Box::new(sender));
                tracing::info!("HA standby connected; replication resumed");
            }
        }
    }
}

/// Advances the prune watermark from the data database's `durable_seq`.
#[allow(dead_code)] // Spawned only by the binary CLI.
pub(crate) async fn run_watermark(
    replicator: Arc<ReplicationControl>,
    mut durable: tokio::sync::watch::Receiver<slatedb::DbStatus>,
) {
    replicator.observe_durable(SlateDbSeqno::new(durable.borrow().durable_seq));
    while durable.changed().await.is_ok() {
        let seq = durable.borrow().durable_seq;
        replicator.observe_durable(SlateDbSeqno::new(seq));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::flush_coordinator::FlushReceipt;

    fn epoch(value: u64) -> WriterEpoch {
        WriterEpoch::new(value).unwrap()
    }

    fn seqno(value: u64) -> ShipSeqno {
        ShipSeqno::new(value).unwrap()
    }

    fn expect_apply(outcome: ShipOutcome<'_>) -> ApplyPermit<'_> {
        match outcome {
            ShipOutcome::Apply(permit) => permit,
            ShipOutcome::NeedsBaseFlush(_) => panic!("expected an apply permit, got base flush"),
            ShipOutcome::Deposed => panic!("expected an apply permit, got deposal"),
            ShipOutcome::Poisoned => panic!("expected an apply permit, got poison"),
        }
    }

    fn stamp_parts(stamp: &HaStamp) -> (u64, u64, u64, u64, bool) {
        (
            stamp.writer_epoch().get(),
            stamp.last_shipped().map_or(0, ShipSeqno::get),
            stamp.solo_history().commits_since_last_ship(),
            stamp.applied_through().map_or(0, ShipSeqno::get),
            stamp.solo_history().ran_solo(),
        )
    }

    #[test]
    fn durability_event_order_preserves_watermark() {
        let mut observed_first = DurabilityTracker::default();
        observed_first.observe_durable(SlateDbSeqno::new(10));
        observed_first.record_applied(seqno(1), SlateDbSeqno::new(10));
        assert_eq!(observed_first.watermark, Some(seqno(1)));
        assert!(observed_first.pending.is_empty());

        let mut applied_first = DurabilityTracker::default();
        applied_first.record_applied(seqno(1), SlateDbSeqno::new(10));
        assert_eq!(applied_first.watermark, None);
        applied_first.observe_durable(SlateDbSeqno::new(10));
        assert_eq!(applied_first.watermark, Some(seqno(1)));
        assert!(applied_first.pending.is_empty());

        for (ship, local) in [(2, 20), (3, 30)] {
            applied_first.record_applied(seqno(ship), SlateDbSeqno::new(local));
        }
        applied_first.observe_durable(SlateDbSeqno::new(25));
        assert_eq!(applied_first.watermark, Some(seqno(2)));
        assert_eq!(applied_first.pending.len(), 1);
    }

    #[tokio::test]
    async fn ship_is_solo_when_disconnected() {
        let (mut sequencer, _control) = Replicator::new("http://127.0.0.1:1".to_string(), epoch(1));
        let permit = expect_apply(
            sequencer
                .ship(&[ReplOp::Put("k".into(), "v".into())], &[])
                .await,
        );
        assert_eq!(permit.kind, ApplyKind::Solo);
        assert_eq!(stamp_parts(permit.stamp()), (1, 0, 1, 0, true));
        permit.applied(SlateDbSeqno::new(1));
    }

    use crate::dedup::DedupCache;
    use crate::replication::transport::{ReceiverControl, ReplicationReceiver};
    use tokio::net::TcpListener;

    /// Running test receiver.
    struct ServerHandle {
        shutdown: tokio::sync::oneshot::Sender<()>,
        join: tokio::task::JoinHandle<()>,
    }

    impl ServerHandle {
        /// Stops the server and closes existing connections.
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
            Arc::new(DedupCache::new()),
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
        let handle = serve_receiver_on(listener, receiver).await;
        (format!("http://{addr}"), control, handle)
    }

    async fn serve_receiver_on(
        listener: TcpListener,
        receiver: ReplicationReceiver,
    ) -> ServerHandle {
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
        ServerHandle { shutdown, join }
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
        let (mut sequencer, repl_control) = Replicator::new(endpoint.clone(), epoch(7));
        repl_control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;

        for (ship, local) in [(1, 10), (2, 20)] {
            let permit = expect_apply(sequencer.ship(&put(), &[]).await);
            assert_eq!(permit.kind, ApplyKind::Shipped(seqno(ship)));
            permit.applied(SlateDbSeqno::new(local));
        }

        let seqnos: Vec<u64> = control
            .inspect_standby_for_tests(|tail, _| tail.batches_in_order().map(|(s, _)| s).collect())
            .await;
        assert_eq!(
            seqnos,
            vec![1, 2],
            "shipped batches land on the standby in order"
        );
    }

    #[tokio::test]
    async fn replacement_receiver_cannot_accept_a_torn_suffix() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = first_listener.local_addr().unwrap();
        let endpoint = format!("http://{addr}");
        let first_receiver =
            ReplicationReceiver::new(Arc::new(DedupCache::new()), None, "standby-a".to_string());
        let first_server = serve_receiver_on(first_listener, first_receiver).await;

        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(7));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        let first = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(first.kind, ApplyKind::Shipped(seqno(1)));
        first.applied(SlateDbSeqno::new(10));

        first_server.stop().await;
        let second_listener = TcpListener::bind(addr).await.unwrap();
        let second_receiver =
            ReplicationReceiver::new(Arc::new(DedupCache::new()), None, "standby-b".to_string());
        let second_control = second_receiver.control();
        let _second_server = serve_receiver_on(second_listener, second_receiver).await;

        let repair = match sequencer.ship(&put(), &[]).await {
            ShipOutcome::NeedsBaseFlush(required) => required,
            _ => panic!("fresh receiver must reject sequence 2 until sequence 1 is durable"),
        };
        assert_eq!(repair.through(), SlateDbSeqno::new(10));
        assert!(
            second_control
                .inspect_standby_for_tests(|tail, _| tail.is_empty())
                .await,
            "BaseNotCovered must be a definitive pre-append rejection"
        );
        repair.complete(FlushReceipt::for_tests());

        let second = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(second.kind, ApplyKind::Shipped(seqno(2)));
        second.applied(SlateDbSeqno::new(11));
        assert_eq!(
            second_control
                .inspect_standby_for_tests(|tail, _| {
                    tail.batches_in_order()
                        .map(|(seqno, _)| seqno)
                        .collect::<Vec<_>>()
                })
                .await,
            vec![2],
            "the durable watermark covers sequence 1 before sequence 2 enters the fresh tail"
        );
    }

    #[tokio::test]
    async fn heartbeat_repair_is_serialized_and_advances_coverage_immediately() {
        let (endpoint, _receiver, _server) = spawn_receiver().await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(7));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        let first = expect_apply(sequencer.ship(&put(), &[]).await);
        first.applied(SlateDbSeqno::new(10));
        assert_eq!(
            control.coverage_frontier(),
            CoverageFrontier::new(Some(seqno(1)), None).unwrap()
        );

        let repair_waiter = tokio::spawn({
            let control = control.clone();
            async move { control.repair_base().await }
        });
        let request = sequencer.next_base_repair().await.unwrap();
        let required = sequencer
            .begin_base_repair()
            .expect("the applied batch needs a durability repair");
        assert_eq!(required.through(), SlateDbSeqno::new(10));
        required.complete(FlushReceipt::for_tests());
        let _ = request.send(());
        repair_waiter.await.unwrap().unwrap();

        assert_eq!(
            control.coverage_frontier(),
            CoverageFrontier::new(Some(seqno(1)), Some(seqno(1))).unwrap(),
            "the next heartbeat must see the repaired watermark without waiting for DbStatus"
        );
    }

    #[tokio::test]
    async fn solo_ship_does_not_consume_a_seqno() {
        let (endpoint, control, _server) = spawn_receiver().await;
        let (mut sequencer, repl_control) = Replicator::new(endpoint.clone(), epoch(1));

        let solo = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(solo.kind, ApplyKind::Solo, "no sender yet: solo");
        solo.applied(SlateDbSeqno::new(10));

        repl_control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        let first_barrier = match sequencer.ship(&put(), &[]).await {
            ShipOutcome::NeedsBaseFlush(required) => required,
            _ => panic!("reconnect must stop before allocating or sending seqno 1"),
        };
        assert_eq!(first_barrier.through(), SlateDbSeqno::new(10));
        drop(first_barrier);
        assert!(
            control
                .inspect_standby_for_tests(|tail, _| tail.is_empty())
                .await,
            "the standby must remain untouched until the Solo base is durable"
        );

        let second_barrier = match sequencer.ship(&put(), &[]).await {
            ShipOutcome::NeedsBaseFlush(required) => required,
            _ => panic!("the dirty base must remain latched without a receipt"),
        };
        second_barrier.complete(FlushReceipt::for_tests());

        let shipped = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(
            shipped.kind,
            ApplyKind::Shipped(seqno(1)),
            "first real ship is seqno 1"
        );
        shipped.applied(SlateDbSeqno::new(11));
    }

    #[tokio::test]
    async fn ship_downgrades_to_solo_when_the_standby_is_unreachable() {
        let (endpoint, _receiver, server) = spawn_receiver().await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(1));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        let first = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(first.kind, ApplyKind::Shipped(seqno(1)));
        first.applied(SlateDbSeqno::new(10));

        server.stop().await;
        let second = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(second.kind, ApplyKind::Ambiguous(seqno(2)));
        second.applied(SlateDbSeqno::new(11));
        assert!(
            !control.is_connected_for_tests().await,
            "a failed ship must clear the sender so reconnect can re-establish it"
        );
    }

    #[tokio::test]
    async fn ship_reports_deposed_on_stale_epoch_rejection_and_stays_deposed() {
        let (endpoint, _control, _server) = spawn_receiver().await;
        let newer = connect(&endpoint).await;
        let first_seqno = seqno(1);
        assert_eq!(
            newer
                .ship(
                    first_seqno,
                    &[ReplOp::Put("a".into(), "b".into())],
                    &[],
                    PruneWatermark::for_ship(0, first_seqno).unwrap(),
                    epoch(5),
                )
                .await
                .unwrap(),
            ShipResult::Accepted
        );

        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(1));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        assert!(matches!(
            sequencer.ship(&put(), &[]).await,
            ShipOutcome::Deposed
        ));
        assert!(
            !control.is_connected_for_tests().await,
            "a rejected ship must clear the sender"
        );
        assert!(matches!(
            sequencer.ship(&put(), &[]).await,
            ShipOutcome::Deposed
        ));

        let recon = tokio::spawn(run_reconnect(Arc::downgrade(&control)));
        tokio::time::timeout(Duration::from_secs(5), recon)
            .await
            .expect("run_reconnect must stop for a deposed replicator")
            .unwrap();
        assert!(
            !control.is_connected_for_tests().await,
            "a deposed replicator must not get a new sender"
        );
    }

    #[tokio::test]
    async fn newer_writer_is_not_deposed_by_a_stale_leading_peer() {
        let (endpoint, _control, _server) = spawn_receiver_at(Some(7)).await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(8));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;

        let ambiguous = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(ambiguous.kind, ApplyKind::Ambiguous(seqno(1)));
        ambiguous.applied(SlateDbSeqno::new(10));
        assert!(
            !control.is_connected_for_tests().await,
            "the stale receiver must be disconnected"
        );
        let solo = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(solo.kind, ApplyKind::Solo);
        solo.applied(SlateDbSeqno::new(11));
    }

    #[tokio::test]
    async fn stamp_counts_solo_commits_but_latches_solo_history() {
        let (endpoint, _receiver, server) = spawn_receiver().await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(9));
        assert_eq!(sequencer.last_shipped, None);
        assert_eq!(sequencer.solo_history, SoloHistory::never());
        assert_eq!(sequencer.applied_through, None);

        let solo = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(stamp_parts(solo.stamp()), (9, 0, 1, 0, true));
        assert_eq!(solo.sequencer.solo_history, SoloHistory::never());
        solo.applied(SlateDbSeqno::new(10));

        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        let barrier = match sequencer.ship(&put(), &[]).await {
            ShipOutcome::NeedsBaseFlush(required) => required,
            _ => panic!("the Solo base must be flushed before reconnect shipping"),
        };
        barrier.complete(FlushReceipt::for_tests());

        let shipped = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(
            stamp_parts(shipped.stamp()),
            (9, 1, 0, 1, true),
            "the atomic candidate proves the acknowledged attempt applied"
        );
        assert_eq!(shipped.sequencer.last_shipped, None);
        assert_eq!(shipped.sequencer.solo_history, SoloHistory::ever(1));
        shipped.applied(SlateDbSeqno::new(11));
        assert_eq!(sequencer.last_shipped, Some(seqno(1)));
        assert_eq!(sequencer.applied_through, Some(seqno(1)));
        assert_eq!(sequencer.solo_history, SoloHistory::ever(0));

        server.stop().await;
        let ambiguous = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(
            stamp_parts(ambiguous.stamp()),
            (9, 1, 1, 2, true),
            "the ambiguous attempt is proven only by its current atomic batch"
        );
        assert_eq!(ambiguous.sequencer.applied_through, Some(seqno(1)));
        ambiguous.applied(SlateDbSeqno::new(12));

        let next_solo = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(
            stamp_parts(next_solo.stamp()),
            (9, 1, 2, 2, true),
            "Solo commits retain the last applied possibly-buffered attempt"
        );
        next_solo.applied(SlateDbSeqno::new(13));
    }

    #[tokio::test]
    async fn run_reconnect_reestablishes_the_sender() {
        let (endpoint, _control, _server) = spawn_receiver().await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(1));
        assert!(!control.is_connected_for_tests().await, "starts solo");

        let recon = tokio::spawn(run_reconnect(Arc::downgrade(&control)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !control.is_connected_for_tests().await {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("run_reconnect must establish the sender to a reachable standby");

        let permit = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(permit.kind, ApplyKind::Shipped(seqno(1)));
        permit.applied(SlateDbSeqno::new(10));
        recon.abort();
    }

    #[tokio::test]
    async fn unresolved_peer_copy_poison_is_distinct_from_deposal() {
        let (endpoint, _receiver, _server) = spawn_receiver().await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(7));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;

        let permit = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(permit.kind, ApplyKind::Shipped(seqno(1)));
        drop(permit);
        assert!(matches!(
            sequencer.ship(&put(), &[]).await,
            ShipOutcome::Poisoned
        ));
        assert!(control.is_connected_for_tests().await);
    }

    #[tokio::test]
    async fn failed_ambiguous_apply_poison_prevents_a_later_ship() {
        let (endpoint, _receiver, server) = spawn_receiver().await;
        let (mut sequencer, control) = Replicator::new(endpoint.clone(), epoch(7));
        control
            .set_sender_for_tests(Some(connect(&endpoint).await))
            .await;
        let first = expect_apply(sequencer.ship(&put(), &[]).await);
        first.applied(SlateDbSeqno::new(10));

        server.stop().await;
        let ambiguous = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(ambiguous.kind, ApplyKind::Ambiguous(seqno(2)));
        assert_eq!(ambiguous.failed().unwrap_err().seqno(), seqno(2));
        assert!(matches!(
            sequencer.ship(&put(), &[]).await,
            ShipOutcome::Poisoned
        ));
    }

    #[tokio::test]
    async fn failed_solo_apply_does_not_commit_candidate_or_poison() {
        let (mut sequencer, _control) = Replicator::new("http://unused".to_string(), epoch(7));
        let solo = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(stamp_parts(solo.stamp()), (7, 0, 1, 0, true));
        solo.failed().unwrap();
        assert_eq!(sequencer.solo_history, SoloHistory::never());
        assert_eq!(sequencer.base_dirty_through, None);

        let retry = expect_apply(sequencer.ship(&put(), &[]).await);
        assert_eq!(stamp_parts(retry.stamp()), (7, 0, 1, 0, true));
        retry.applied(SlateDbSeqno::new(10));
    }
}
