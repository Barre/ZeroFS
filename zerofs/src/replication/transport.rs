//! The replication transport: the receiver-side role state machine and the
//! leader's gRPC sender.

use crate::dedup::OpId;
use crate::replication::tail::{ReplOp, TailBuffer};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, watch};
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

pub mod proto {
    tonic::include_proto!("zerofs.replication");
}

use proto::replication_service_client::ReplicationServiceClient;
use proto::replication_service_server::{ReplicationService, ReplicationServiceServer};
use proto::{
    HeartbeatRequest, HeartbeatResponse, HelloRequest, HelloResponse, ReplOp as ProtoOp,
    ReplicateRequest, ReplicateResponse,
};

/// Bounds a gRPC dial so a peer that accepts the TCP connection but never
/// finishes the HTTP/2 handshake cannot hang the caller forever (leader startup
/// `hello_peer`, the heartbeat sender, or the shipping reconnect loop). Without
/// it a half-open standby silently wedges the leader's control plane.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Bounds a unary control RPC (Hello) so a peer that accepts the call but never
/// answers does not stall the leader's startup role election.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// Decode ceiling for an incoming `ReplicateRequest`. tonic defaults to 4 MiB.
const MAX_SHIP_DECODE_BYTES: usize = usize::MAX;

/// One decoded replication batch at the message boundary.
struct IncomingShip {
    seqno: u64,
    ops: Vec<ReplOp>,
    op_ids: Vec<OpId>,
    prune_watermark: u64,
    writer_epoch: u64,
}

/// Receiver-side disposition of a decoded ship.
enum ShipDisposition {
    /// The batch entered the standby tail.
    Accepted,
    /// The sender is at or below this receiver's writer epoch, or below the
    /// highest epoch already observed. This is authoritative deposal evidence.
    SenderDeposed,
    /// The sender is newer than this process's locally acquired writer epoch.
    /// The receiver is stale, so this is an availability failure for the sender,
    /// not evidence that the sender is deposed.
    ReceiverStale {
        local_epoch: u64,
        incoming_epoch: u64,
    },
}

fn ship_rpc_response(disposition: ShipDisposition) -> Result<Response<ReplicateResponse>, Status> {
    match disposition {
        ShipDisposition::Accepted => Ok(Response::new(ReplicateResponse { accepted: true })),
        ShipDisposition::SenderDeposed => Ok(Response::new(ReplicateResponse { accepted: false })),
        ShipDisposition::ReceiverStale {
            local_epoch,
            incoming_epoch,
        } => Err(Status::unavailable(format!(
            "receiver is a stale writer at epoch {local_epoch}; incoming writer \
             epoch is {incoming_epoch}"
        ))),
    }
}

struct StandbyState {
    observed_epoch: u64,
    tail: TailBuffer,
}

enum ReceiverPhase {
    Standby(StandbyState),
    /// The local writer epoch is acquired and the old standby tail is owned by
    /// takeover reconciliation. Admission stays closed if it fails or is cancelled.
    Promoting {
        writer_epoch: u64,
    },
    Leading {
        writer_epoch: u64,
    },
}

struct ReceiverCore {
    phase: Mutex<ReceiverPhase>,
    dedup: Arc<crate::dedup::DedupCache>,
}

/// Handle for atomically freezing a receiver's standby state during promotion.
#[derive(Clone)]
pub struct ReceiverControl {
    core: Arc<ReceiverCore>,
}

/// The tail and observed epoch frozen by one promotion transition. It is
/// intentionally non-cloneable: reconciliation consumes the only copy.
#[must_use = "a promotion snapshot must be reconciled before serving"]
pub struct PromotionSnapshot {
    control: ReceiverControl,
    writer_epoch: u64,
    observed_epoch: u64,
    tail: TailBuffer,
}

impl ReceiverControl {
    /// Fence receiver admission and take ownership of the standby tail.
    pub async fn begin_promotion(&self, writer_epoch: u64) -> anyhow::Result<PromotionSnapshot> {
        anyhow::ensure!(writer_epoch > 0, "receiver writer epoch must be nonzero");
        let mut phase = self.core.phase.lock().await;
        match &*phase {
            ReceiverPhase::Standby(_) => {}
            ReceiverPhase::Promoting {
                writer_epoch: existing,
            }
            | ReceiverPhase::Leading {
                writer_epoch: existing,
            } => anyhow::bail!(
                "receiver already fenced at writer epoch {existing}; cannot promote \
                 at epoch {writer_epoch}"
            ),
        }
        let ReceiverPhase::Standby(standby) =
            std::mem::replace(&mut *phase, ReceiverPhase::Promoting { writer_epoch })
        else {
            unreachable!("standby phase checked above")
        };
        Ok(PromotionSnapshot {
            control: self.clone(),
            writer_epoch,
            observed_epoch: standby.observed_epoch,
            tail: standby.tail,
        })
    }

    async fn complete_promotion(&self, writer_epoch: u64) -> anyhow::Result<()> {
        let mut phase = self.core.phase.lock().await;
        match &*phase {
            ReceiverPhase::Promoting {
                writer_epoch: current,
            } if *current == writer_epoch => {
                *phase = ReceiverPhase::Leading { writer_epoch };
                Ok(())
            }
            ReceiverPhase::Promoting {
                writer_epoch: current,
            } => anyhow::bail!("receiver promotion epoch changed from {writer_epoch} to {current}"),
            ReceiverPhase::Standby(_) => {
                anyhow::bail!("receiver returned to standby during promotion")
            }
            ReceiverPhase::Leading {
                writer_epoch: current,
            } => anyhow::bail!("receiver already leading at epoch {current}"),
        }
    }

    #[cfg(test)]
    pub(crate) async fn inspect_standby_for_tests<R>(
        &self,
        inspect: impl FnOnce(&TailBuffer, u64) -> R,
    ) -> R {
        let phase = self.core.phase.lock().await;
        let ReceiverPhase::Standby(standby) = &*phase else {
            panic!("receiver is no longer a standby")
        };
        inspect(&standby.tail, standby.observed_epoch)
    }
}

impl PromotionSnapshot {
    pub(crate) fn observed_epoch(&self) -> u64 {
        self.observed_epoch
    }

    pub(crate) fn tail_mut(&mut self) -> &mut TailBuffer {
        &mut self.tail
    }

    pub(crate) fn dedup(&self) -> Arc<crate::dedup::DedupCache> {
        self.control.core.dedup.clone()
    }

    pub(crate) async fn complete(self) -> anyhow::Result<u64> {
        self.control.complete_promotion(self.writer_epoch).await?;
        Ok(self.writer_epoch)
    }
}

fn to_repl_ops(ops: Vec<ProtoOp>) -> Vec<ReplOp> {
    ops.into_iter()
        .map(|o| {
            if o.delete {
                ReplOp::Delete(Bytes::from(o.key))
            } else if !o.frame.is_empty() {
                ReplOp::PutFrame(
                    Bytes::from(o.key),
                    Bytes::from(o.value),
                    Bytes::from(o.frame),
                )
            } else {
                ReplOp::Put(Bytes::from(o.key), Bytes::from(o.value))
            }
        })
        .collect()
}

fn to_proto_ops(ops: &[ReplOp]) -> Vec<ProtoOp> {
    ops.iter()
        .map(|o| match o {
            ReplOp::Put(k, v) => ProtoOp {
                key: k.to_vec(),
                value: v.to_vec(),
                delete: false,
                frame: Vec::new(),
            },
            ReplOp::PutFrame(k, v, f) => ProtoOp {
                key: k.to_vec(),
                value: v.to_vec(),
                delete: false,
                frame: f.to_vec(),
            },
            ReplOp::Delete(k) => ProtoOp {
                key: k.to_vec(),
                value: Vec::new(),
                delete: true,
                frame: Vec::new(),
            },
        })
        .collect()
}

/// Receives replication into an owned standby tail and fences stale writer epochs.
pub struct ReplicationReceiver {
    core: Arc<ReceiverCore>,
    /// Ticks on each accepted heartbeat; the failover watch follows it.
    heartbeats: watch::Sender<u64>,
    /// Fired when a deposed leader asks (Hello) and we are a standby holding a
    /// tail, so we take over at once instead of waiting out the heartbeat gap.
    takeover_trigger: Option<Arc<Notify>>,
    /// This node's configured id, reported via Hello so a booting peer can
    /// refuse startup on a duplicated node_id (identity keys the latest-leader
    /// record, so a collision is a config error).
    node_id: String,
    /// Test-only gate between the fast checks and the phase lock.
    #[cfg(test)]
    before_phase_lock_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
    /// Test-only gate inside the admission critical section (fence check passed,
    /// append not yet done), to prove epoch publication cannot interleave there.
    #[cfg(test)]
    before_append_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
}

impl ReplicationReceiver {
    pub fn new(
        dedup: Arc<crate::dedup::DedupCache>,
        takeover_trigger: Option<Arc<Notify>>,
        node_id: String,
    ) -> Self {
        Self {
            core: Arc::new(ReceiverCore {
                phase: Mutex::new(ReceiverPhase::Standby(StandbyState {
                    observed_epoch: 0,
                    tail: TailBuffer::new(),
                })),
                dedup,
            }),
            heartbeats: watch::channel(0u64).0,
            takeover_trigger,
            node_id,
            #[cfg(test)]
            before_phase_lock_pause: None,
            #[cfg(test)]
            before_append_pause: None,
        }
    }

    #[cfg(test)]
    fn pause_epoch_before_phase_lock(
        mut self,
        epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> Self {
        self.before_phase_lock_pause = Some((epoch, reached, resume));
        self
    }

    #[cfg(test)]
    fn pause_epoch_before_append(
        mut self,
        epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> Self {
        self.before_append_pause = Some((epoch, reached, resume));
        self
    }

    /// Subscribe before [`Self::into_server`] consumes the receiver.
    pub fn liveness(&self) -> watch::Receiver<u64> {
        self.heartbeats.subscribe()
    }

    /// Return the handle used to begin promotion after opening the writer db.
    pub fn control(&self) -> ReceiverControl {
        ReceiverControl {
            core: self.core.clone(),
        }
    }

    /// A promoted receiver never buffers peer writes. A lower or equal sender
    /// is deposed; a higher sender means this receiver is the stale process.
    fn fenced_disposition(local_epoch: u64, incoming_epoch: u64) -> ShipDisposition {
        if incoming_epoch > local_epoch {
            ShipDisposition::ReceiverStale {
                local_epoch,
                incoming_epoch,
            }
        } else {
            ShipDisposition::SenderDeposed
        }
    }

    /// Cheap advisory rejection before converting the decoded payload. The
    /// authoritative decision is repeated while mutating `ReceiverPhase`.
    async fn preflight_ship(&self, incoming_epoch: u64) -> Option<ShipDisposition> {
        let phase = self.core.phase.lock().await;
        match &*phase {
            ReceiverPhase::Standby(standby) => {
                (incoming_epoch < standby.observed_epoch).then_some(ShipDisposition::SenderDeposed)
            }
            ReceiverPhase::Promoting { writer_epoch } | ReceiverPhase::Leading { writer_epoch } => {
                Some(Self::fenced_disposition(*writer_epoch, incoming_epoch))
            }
        }
    }

    /// Admit one decoded replication batch. Fencing, observed-peer epoch,
    /// tail mutation, pruning, and dedup publication share one state transition.
    async fn receive_ship(&self, ship: IncomingShip) -> ShipDisposition {
        let IncomingShip {
            seqno,
            ops,
            op_ids,
            prune_watermark,
            writer_epoch,
        } = ship;

        let op_ids: Vec<OpId> = op_ids.into_iter().filter(crate::dedup::has_op_id).collect();
        #[cfg(test)]
        if let Some((epoch, reached, resume)) = &self.before_phase_lock_pause
            && writer_epoch == *epoch
        {
            reached.notify_one();
            resume.notified().await;
        }
        let mut phase = self.core.phase.lock().await;
        match &mut *phase {
            ReceiverPhase::Standby(standby) => {
                if writer_epoch < standby.observed_epoch {
                    return ShipDisposition::SenderDeposed;
                }
                standby.observed_epoch = standby.observed_epoch.max(writer_epoch);
                #[cfg(test)]
                if let Some((epoch, reached, resume)) = &self.before_append_pause
                    && writer_epoch == *epoch
                {
                    reached.notify_one();
                    resume.notified().await;
                }
                // Cross-term replacement uses the tail epoch; a heartbeat may
                // already have advanced `observed_epoch`.
                standby.tail.accept(writer_epoch, seqno, ops, op_ids);
                if prune_watermark > 0 {
                    for op_id in standby.tail.prune(prune_watermark) {
                        self.core.dedup.record(op_id, Bytes::new());
                    }
                }
                ShipDisposition::Accepted
            }
            ReceiverPhase::Promoting {
                writer_epoch: local_epoch,
            }
            | ReceiverPhase::Leading {
                writer_epoch: local_epoch,
            } => Self::fenced_disposition(*local_epoch, writer_epoch),
        }
    }

    /// Publish a heartbeat's observed writer epoch and report whether it should
    /// tick liveness. Promotion moves the complete standby state out under this
    /// same lock, so a late beat has no path to the frozen snapshot.
    async fn receive_heartbeat(&self, writer_epoch: u64) -> bool {
        #[cfg(test)]
        if let Some((epoch, reached, resume)) = &self.before_phase_lock_pause
            && writer_epoch == *epoch
        {
            reached.notify_one();
            resume.notified().await;
        }
        let mut phase = self.core.phase.lock().await;
        match &mut *phase {
            ReceiverPhase::Standby(standby) if writer_epoch >= standby.observed_epoch => {
                standby.observed_epoch = writer_epoch;
                true
            }
            ReceiverPhase::Standby(_)
            | ReceiverPhase::Promoting { .. }
            | ReceiverPhase::Leading { .. } => false,
        }
    }

    pub fn into_server(self) -> ReplicationServiceServer<Self> {
        ReplicationServiceServer::new(self).max_decoding_message_size(MAX_SHIP_DECODE_BYTES)
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationReceiver {
    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        // Reject cheaply before converting/copying the unbounded decoded payload.
        // Advisory only: `receive_ship` repeats the decision while mutating the
        // phase, which remains the authoritative admission fence.
        if let Some(disposition) = self.preflight_ship(req.writer_epoch).await {
            return ship_rpc_response(disposition);
        }
        let ship = IncomingShip {
            seqno: req.seqno,
            ops: to_repl_ops(req.ops),
            op_ids: req
                .op_ids
                .into_iter()
                .filter_map(|raw| <[u8; 16]>::try_from(raw.as_slice()).ok())
                .collect(),
            prune_watermark: req.prune_watermark,
            writer_epoch: req.writer_epoch,
        };
        ship_rpc_response(self.receive_ship(ship).await)
    }

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let mut stream = request.into_inner();
        // A beat from a deposed leader (epoch below the highest seen) is ignored,
        // so a zombie cannot keep the standby from taking over. Once this node is
        // itself the writer, ignore all beats: a same-epoch zombie must not keep
        // this node's liveness view ticking (it is deposed by our promotion).
        while let Some(beat) = stream.message().await? {
            if self.receive_heartbeat(beat.writer_epoch).await {
                self.heartbeats.send_modify(|c| *c = c.wrapping_add(1));
            }
        }
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn hello(
        &self,
        _request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        let (peer_active, trigger_takeover) = {
            let phase = self.core.phase.lock().await;
            match &*phase {
                ReceiverPhase::Standby(standby) => {
                    let has_tail = !standby.tail.is_empty();
                    (has_tail, has_tail)
                }
                ReceiverPhase::Promoting { .. } | ReceiverPhase::Leading { .. } => (true, false),
            }
        };
        // A standby holding the tail takes over now, without waiting out the
        // heartbeat gap.
        if trigger_takeover && let Some(trigger) = &self.takeover_trigger {
            trigger.notify_one();
        }
        Ok(Response::new(HelloResponse {
            peer_active,
            node_id: self.node_id.clone(),
        }))
    }
}

/// Ships batches and reports whether the peer accepted them. Transport errors
/// propagate to the replicator, which decides whether to run solo.
pub struct ReplicationSender {
    client: Mutex<ReplicationServiceClient<Channel>>,
}

impl ReplicationSender {
    pub async fn connect(endpoint: String) -> anyhow::Result<Self> {
        let client =
            tokio::time::timeout(CONNECT_TIMEOUT, ReplicationServiceClient::connect(endpoint))
                .await
                .map_err(|_| anyhow::anyhow!("replication dial timed out"))??;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    pub async fn ship(
        &self,
        seqno: u64,
        ops: &[ReplOp],
        op_ids: &[crate::dedup::OpId],
        prune_watermark: u64,
        writer_epoch: u64,
    ) -> anyhow::Result<bool> {
        let req = ReplicateRequest {
            seqno,
            ops: to_proto_ops(ops),
            prune_watermark,
            writer_epoch,
            op_ids: op_ids.iter().map(|o| o.to_vec()).collect(),
        };
        let resp = self.client.lock().await.replicate(req).await?;
        Ok(resp.into_inner().accepted)
    }
}

/// Ask a peer (Hello) whether the caller should defer instead of opening as
/// writer. `peer_active` is true if the peer is promoting, leading, or preserving
/// an un-replayed tail. `node_id` is empty on peers predating that field.
pub struct HelloAnswer {
    pub peer_active: bool,
    pub node_id: String,
}

pub async fn hello_peer(endpoint: String) -> anyhow::Result<HelloAnswer> {
    let mut client =
        tokio::time::timeout(CONNECT_TIMEOUT, ReplicationServiceClient::connect(endpoint))
            .await
            .map_err(|_| anyhow::anyhow!("Hello dial timed out"))??;
    let resp = tokio::time::timeout(RPC_TIMEOUT, client.hello(HelloRequest {}))
        .await
        .map_err(|_| anyhow::anyhow!("Hello RPC timed out"))??;
    let resp = resp.into_inner();
    Ok(HelloAnswer {
        peer_active: resp.peer_active,
        node_id: resp.node_id,
    })
}

/// Stream heartbeats at `interval` until the connection breaks. `epoch` is the
/// leader's data-db writer epoch, so the standby ignores a deposed leader's beats.
pub async fn run_heartbeat_sender(
    endpoint: String,
    epoch: u64,
    interval: Duration,
) -> anyhow::Result<()> {
    // Only the dial is bounded; the heartbeat stream itself is long-lived by
    // design (it runs until the connection breaks).
    let mut client =
        tokio::time::timeout(CONNECT_TIMEOUT, ReplicationServiceClient::connect(endpoint))
            .await
            .map_err(|_| anyhow::anyhow!("heartbeat dial timed out"))??;
    let beats = futures::stream::unfold(true, move |first| async move {
        // First beat immediately (so a watching standby observes the leader at
        // once); pace the rest by `interval`.
        if !first {
            tokio::time::sleep(interval).await;
        }
        Some((
            HeartbeatRequest {
                writer_epoch: epoch,
                seq: 0,
            },
            false,
        ))
    });
    client.heartbeat(beats).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpListener;

    fn new_receiver(dedup: Arc<crate::dedup::DedupCache>) -> ReplicationReceiver {
        ReplicationReceiver::new(dedup, None, "standby-under-test".to_string())
    }

    async fn serve(receiver: ReplicationReceiver) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(receiver.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    async fn connect(endpoint: &str) -> ReplicationSender {
        for _ in 0..100 {
            if let Ok(sender) = ReplicationSender::connect(endpoint.to_string()).await {
                return sender;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("could not connect to receiver");
    }

    async fn spawn_receiver() -> (String, ReceiverControl) {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new(64)));
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    async fn spawn_receiver_with_dedup(
        dedup: Arc<crate::dedup::DedupCache>,
    ) -> (String, ReceiverControl) {
        let receiver = new_receiver(dedup);
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    /// Receiver paused inside the admission critical section (fence check passed,
    /// append pending) for `paused_epoch`.
    async fn spawn_receiver_paused_before_append(
        paused_epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> (String, ReceiverControl) {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new(64)))
            .pause_epoch_before_append(paused_epoch, reached, resume);
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    async fn spawn_receiver_paused_before_phase_lock(
        dedup: Arc<crate::dedup::DedupCache>,
        paused_epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> (String, ReceiverControl) {
        let receiver =
            new_receiver(dedup).pause_epoch_before_phase_lock(paused_epoch, reached, resume);
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    fn put(k: &[u8], v: &[u8]) -> ReplOp {
        ReplOp::Put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
    }

    #[tokio::test]
    async fn ships_buffers_prunes_and_fences_stale_epoch() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        assert!(sender.ship(1, &[put(b"a", b"1")], &[], 0, 1).await.unwrap());
        assert!(sender.ship(2, &[put(b"b", b"2")], &[], 0, 1).await.unwrap());
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| tail.len())
                .await,
            2
        );

        // A ship carrying a prune watermark drops the durable prefix.
        assert!(sender.ship(3, &[put(b"c", b"3")], &[], 1, 1).await.unwrap());
        let seqnos: Vec<u64> = control
            .inspect_standby_for_tests(|tail, _| tail.batches_in_order().map(|(s, _)| s).collect())
            .await;
        assert_eq!(seqnos, vec![2, 3], "watermark 1 prunes seqno 1");

        // A ship from an older writer epoch (a deposed leader) is rejected.
        assert!(!sender.ship(4, &[put(b"x", b"x")], &[], 0, 0).await.unwrap());
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| tail.len())
                .await,
            2,
            "a stale-epoch ship must not buffer"
        );
    }

    // A watermark is the first proof that a volatile tail batch is durable.
    #[tokio::test]
    async fn pending_op_ids_publish_only_when_pruned_as_durable() {
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let (endpoint, _control) = spawn_receiver_with_dedup(dedup.clone()).await;
        let sender = connect(&endpoint).await;
        let durable = [1u8; 16];
        let pending = [2u8; 16];

        assert!(
            sender
                .ship(1, &[put(b"a", b"1")], &[durable], 0, 1)
                .await
                .unwrap()
        );
        assert!(
            dedup.get(&durable).is_none(),
            "an accepted-but-unpruned tail batch is not known applied yet"
        );

        assert!(
            sender
                .ship(2, &[put(b"b", b"2")], &[pending], 1, 1)
                .await
                .unwrap()
        );
        assert!(
            dedup.get(&durable).is_some(),
            "watermark pruning proves the first batch durable and publishes its op-id"
        );
        assert!(
            dedup.get(&pending).is_none(),
            "the unpruned suffix must remain pending"
        );
    }

    // Pending op-ids share the disposition of their tail batch across terms.
    #[tokio::test]
    async fn cross_term_tail_replacement_drops_pending_op_ids() {
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let (endpoint, _control) = spawn_receiver_with_dedup(dedup.clone()).await;
        let sender = connect(&endpoint).await;
        let durable_old = [3u8; 16];
        let old = [4u8; 16];
        let durable_new = [5u8; 16];
        let pending_new = [6u8; 16];

        assert!(
            sender
                .ship(49, &[put(b"durable-old", b"value")], &[durable_old], 0, 5)
                .await
                .unwrap()
        );
        assert!(
            sender
                .ship(50, &[put(b"old", b"value")], &[old], 49, 5)
                .await
                .unwrap()
        );
        assert!(
            sender
                .ship(1, &[put(b"new", b"value")], &[durable_new], 0, 6)
                .await
                .unwrap()
        );
        assert!(
            sender
                .ship(2, &[put(b"next", b"value")], &[pending_new], 1, 6)
                .await
                .unwrap()
        );

        assert!(
            dedup.get(&durable_old).is_some(),
            "an already-pruned old-term id remains valid across term replacement"
        );
        assert!(
            dedup.get(&old).is_none(),
            "an old-term op-id must disappear with its replaced tail batch"
        );
        assert!(
            dedup.get(&durable_new).is_some(),
            "the pruned new-term batch is proven durable"
        );
        assert!(
            dedup.get(&pending_new).is_none(),
            "the unpruned new-term suffix remains pending"
        );
    }

    // A coalesced ship past tonic's 4 MiB default decode limit must be accepted:
    // at the default the standby rejects it before the handler, the leader sees a
    // failed ship, and it downgrades to solo and taints the lineage under ordinary
    // write load. `into_server` raises the limit; this proves the wiring.
    #[tokio::test]
    async fn ship_larger_than_the_default_decode_limit_is_accepted() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        // One op whose payload alone clears the 4 MiB default with headroom.
        let big = ReplOp::Put(
            Bytes::from_static(b"k"),
            Bytes::from(vec![0u8; 6 * 1024 * 1024]),
        );
        assert!(
            sender.ship(1, &[big], &[], 0, 1).await.unwrap(),
            "a ship past the 4 MiB default decode limit must be accepted, not rejected"
        );
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| tail.len())
                .await,
            1,
            "the large batch must buffer"
        );
    }

    // Hello reports the responder's configured node_id, so a booting peer can
    // refuse startup on a duplicated identity (it keys the latest-leader record).
    #[tokio::test]
    async fn hello_reports_the_responders_node_id() {
        let (endpoint, _control) = spawn_receiver().await;
        let answer = loop {
            match hello_peer(endpoint.clone()).await {
                Ok(a) => break a,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        assert_eq!(answer.node_id, "standby-under-test");
        assert!(!answer.peer_active, "idle standby: empty tail, not leading");
    }

    #[tokio::test]
    async fn heartbeats_tick_liveness_and_stop_on_break() {
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new(64)),
            None,
            "standby-under-test".to_string(),
        );
        let mut liveness = receiver.liveness();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(receiver.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let endpoint = format!("http://{addr}");

        let sender = tokio::spawn(async move {
            let _ = run_heartbeat_sender(endpoint, 1, Duration::from_millis(20)).await;
        });

        tokio::time::timeout(Duration::from_secs(3), liveness.changed())
            .await
            .expect("a heartbeat must tick liveness")
            .unwrap();
        assert!(*liveness.borrow() > 0, "liveness advanced on heartbeats");

        // Breaking the sender stops the ticks (the handler's stream ends).
        sender.abort();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let after = *liveness.borrow();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            *liveness.borrow(),
            after,
            "no more ticks after the sender stops"
        );
    }

    // Repro: a leader (epoch 5) ships high-seqno batches still un-durable in the
    // standby tail, then RESTARTS (epoch bumps to 6, Replicator seqno resets to 1)
    // and re-ships new writes at low seqnos while the standby keeps its term-5
    // tail. A later takeover replays in seqno order, so the stale term-5 batches
    // (high seqno) replay after the term-6 write (low seqno) and must not clobber
    // it. seqno is per-term, not global, so the buffer must drop a superseded epoch.
    #[tokio::test]
    async fn stale_prior_epoch_tail_does_not_clobber_newer_writes() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        // Term 1 (writer epoch 5): un-durable tail at seqno 50/51, "/f" = "old".
        assert!(
            sender
                .ship(50, &[put(b"/f", b"old")], &[], 0, 5)
                .await
                .unwrap()
        );
        assert!(
            sender
                .ship(51, &[put(b"/f", b"old")], &[], 0, 5)
                .await
                .unwrap()
        );

        // Restarted leader (writer epoch 6, seqno reset to 1): "/f" = "new".
        assert!(
            sender
                .ship(1, &[put(b"/f", b"new")], &[], 0, 6)
                .await
                .unwrap(),
            "a higher-epoch ship is accepted"
        );

        let (epoch, seqnos) = control
            .inspect_standby_for_tests(|tail, _| {
                (
                    tail.epoch(),
                    tail.batches_in_order()
                        .map(|(seqno, _)| seqno)
                        .collect::<Vec<_>>(),
                )
            })
            .await;
        assert_eq!(epoch, 6);
        assert_eq!(
            seqnos,
            vec![1],
            "the newer term must replace the old high-seqno tail"
        );
    }

    // Same cross-term scenario as above, but the restarted leader's heartbeat
    // reaches the standby before its first ship.
    #[tokio::test]
    async fn heartbeat_epoch_bump_must_not_defeat_the_cross_term_tail_clear() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        // Term (epoch 5): un-durable tail at seqno 50/51, "/f" = "old".
        assert!(
            sender
                .ship(50, &[put(b"/f", b"old")], &[], 0, 5)
                .await
                .unwrap()
        );
        assert!(
            sender
                .ship(51, &[put(b"/f", b"old")], &[], 0, 5)
                .await
                .unwrap()
        );

        // The restarted leader (epoch 6) heartbeats before it ships. Drive a real
        // heartbeat and wait until the receiver has observed epoch 6.
        let hb = tokio::spawn(run_heartbeat_sender(
            endpoint.clone(),
            6,
            Duration::from_secs(3600),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while control
                .inspect_standby_for_tests(|_, observed_epoch| observed_epoch)
                .await
                < 6
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the heartbeat must publish observed epoch 6");

        // Now the post-restart (epoch 6) write ships at the reset seqno 1.
        assert!(
            sender
                .ship(1, &[put(b"/f", b"new")], &[], 0, 6)
                .await
                .unwrap(),
            "a higher-epoch ship is accepted"
        );
        hb.abort();

        let (epoch, seqnos) = control
            .inspect_standby_for_tests(|tail, _| {
                (
                    tail.epoch(),
                    tail.batches_in_order()
                        .map(|(seqno, _)| seqno)
                        .collect::<Vec<_>>(),
                )
            })
            .await;
        assert_eq!(epoch, 6);
        assert_eq!(
            seqnos,
            vec![1],
            "a heartbeat epoch bump must not suppress cross-term replacement"
        );
    }

    // A fenced receiver must distinguish a zombie sender from a peer that has
    // acquired a strictly newer writer epoch. Neither may enter its frozen tail,
    // but only the former is evidence that the sender itself is deposed.
    #[tokio::test]
    async fn fenced_receiver_rejects_zombies_but_reports_newer_writer_unavailable() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        // Before promotion, an epoch-7 ship is accepted and buffered.
        assert!(sender.ship(1, &[put(b"a", b"1")], &[], 0, 7).await.unwrap());
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| tail.len())
                .await,
            1
        );

        // This process acquired its writer epoch and atomically froze the tail.
        let promotion = control.begin_promotion(7).await.unwrap();

        // The deposed leader (still epoch 7) re-ships: it must be rejected, and
        // nothing must enter the tail or dedup.
        assert!(
            !sender
                .ship(2, &[put(b"b", b"2")], &[[9u8; 16]], 0, 7)
                .await
                .unwrap(),
            "a fenced node must reject an equal-epoch zombie's ship"
        );
        assert_eq!(
            promotion.tail.len(),
            1,
            "a rejected zombie ship must not enter the frozen promotion tail"
        );

        assert!(
            !sender.ship(3, &[put(b"c", b"3")], &[], 0, 6).await.unwrap(),
            "a lower-epoch zombie must also be authoritatively rejected"
        );

        let err = sender
            .ship(1, &[put(b"new", b"writer")], &[], 0, 8)
            .await
            .expect_err("a newer writer must see this stale receiver as unavailable");
        assert_eq!(
            err.downcast_ref::<tonic::Status>().map(tonic::Status::code),
            Some(tonic::Code::Unavailable)
        );
        assert_eq!(
            promotion.tail.len(),
            1,
            "the newer writer must not enter a stale process's frozen tail"
        );
        promotion.complete().await.unwrap();
    }

    // Replay failure or cancellation drops the only tail owner. The receiver
    // must remain fenced rather than silently reopening admission without it.
    #[tokio::test]
    async fn dropped_promotion_snapshot_leaves_receiver_fail_closed() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        assert!(sender.ship(1, &[put(b"a", b"1")], &[], 0, 7).await.unwrap());
        let promotion = control.begin_promotion(8).await.unwrap();
        drop(promotion);

        assert!(
            !sender
                .ship(2, &[put(b"late", b"write")], &[], 0, 7)
                .await
                .unwrap(),
            "dropping the replay capability must not reopen receiver admission"
        );
        assert!(
            control.begin_promotion(9).await.is_err(),
            "a failed promotion is terminal for this receiver incarnation"
        );
        assert!(
            hello_peer(endpoint).await.unwrap().peer_active,
            "a fail-closed promoter must still make a booting peer defer"
        );
    }

    // Promotion may land between the advisory check and the phase lock.
    #[tokio::test]
    async fn ship_losing_the_promotion_race_is_rejected() {
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, control) = spawn_receiver_paused_before_phase_lock(
            dedup.clone(),
            7,
            reached.clone(),
            resume.clone(),
        )
        .await;
        let sender = connect(&endpoint).await;
        let op_id = [9u8; 16];
        let ship = tokio::spawn(async move {
            sender
                .ship(1, &[put(b"late", b"write")], &[op_id], 0, 7)
                .await
                .unwrap()
        });

        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the ship must reach the phase-lock boundary");

        // Promotion changes phase and moves the tail out in one transition.
        let promotion = control.begin_promotion(8).await.unwrap();
        resume.notify_one();

        let accepted = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the ship must finish after promotion releases the phase lock")
            .unwrap();
        assert!(
            !accepted,
            "a ship that loses the phase-lock race to promotion must be rejected"
        );
        assert!(
            promotion.tail.is_empty(),
            "the post-replay ship must not enter the tail"
        );
        assert!(
            dedup.get(&op_id).is_none(),
            "a rejected post-replay ship must not poison dedup"
        );
        promotion.complete().await.unwrap();
    }

    // A newer sender makes a promoter with an older local epoch the stale side.
    #[tokio::test]
    async fn newer_ship_losing_the_promotion_race_sees_stale_receiver() {
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, control) = spawn_receiver_paused_before_phase_lock(
            dedup.clone(),
            9,
            reached.clone(),
            resume.clone(),
        )
        .await;
        let sender = connect(&endpoint).await;
        let op_id = [10u8; 16];
        let ship = tokio::spawn(async move {
            sender
                .ship(1, &[put(b"newer", b"write")], &[op_id], 0, 9)
                .await
        });

        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the newer ship must reach the phase-lock boundary");

        let promotion = control.begin_promotion(8).await.unwrap();
        resume.notify_one();

        let err = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the newer ship must finish after promotion releases the phase lock")
            .unwrap()
            .expect_err("the stale promoter must report itself unavailable");
        assert_eq!(
            err.downcast_ref::<tonic::Status>().map(tonic::Status::code),
            Some(tonic::Code::Unavailable)
        );
        assert!(
            promotion.tail.is_empty(),
            "a stale promoter must not append after its one-time replay"
        );
        assert!(
            dedup.get(&op_id).is_none(),
            "a stale promoter must not publish the rejected op-id"
        );
        promotion.complete().await.unwrap();
    }

    // Heartbeat epoch publication shares the ship admission critical section.
    #[tokio::test]
    async fn heartbeat_epoch_update_waits_for_ship_admission() {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, control) =
            spawn_receiver_paused_before_append(5, reached.clone(), resume.clone()).await;
        let sender = connect(&endpoint).await;

        let ship = tokio::spawn(async move {
            sender
                .ship(50, &[put(b"f", b"old")], &[], 0, 5)
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the ship must reach the admission critical section");

        // A restarted leader (epoch 6) heartbeats while the epoch-5 ship holds
        // the admission critical section.
        let hb = tokio::spawn(run_heartbeat_sender(
            endpoint.clone(),
            6,
            Duration::from_secs(3600),
        ));
        let published_mid_admission = tokio::time::timeout(Duration::from_millis(500), async {
            while control
                .inspect_standby_for_tests(|_, observed_epoch| observed_epoch)
                .await
                < 6
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        assert!(
            !published_mid_admission,
            "a heartbeat published a newer epoch inside another ship's admission \
             critical section"
        );

        // The ship was admitted before any newer epoch was published: accepted.
        resume.notify_one();
        let accepted = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the ship must finish once resumed")
            .unwrap();
        assert!(accepted);

        // Once the admission releases the lock the beat publishes, and a further
        // stale ship is fenced.
        tokio::time::timeout(Duration::from_secs(5), async {
            while control
                .inspect_standby_for_tests(|_, observed_epoch| observed_epoch)
                .await
                < 6
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the heartbeat must publish after the admission releases");
        hb.abort();
        let sender2 = connect(&endpoint).await;
        assert!(
            !sender2
                .ship(51, &[put(b"f", b"old2")], &[], 0, 5)
                .await
                .unwrap(),
            "a stale ship after the published beat must be fenced"
        );
    }

    // A heartbeat already in flight before promotion must not publish a new
    // observed epoch after takeover freezes the receiver state.
    #[tokio::test]
    async fn heartbeat_losing_the_promotion_race_cannot_change_replay_snapshot() {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let receiver = Arc::new(
            new_receiver(Arc::new(crate::dedup::DedupCache::new(64)))
                .pause_epoch_before_phase_lock(8, reached.clone(), resume.clone()),
        );
        let control = receiver.control();

        assert!(matches!(
            receiver
                .receive_ship(IncomingShip {
                    seqno: 1,
                    ops: vec![put(b"covered", b"write")],
                    op_ids: Vec::new(),
                    prune_watermark: 0,
                    writer_epoch: 7,
                })
                .await,
            ShipDisposition::Accepted
        ));

        let heartbeat = tokio::spawn({
            let receiver = receiver.clone();
            async move { receiver.receive_heartbeat(8).await }
        });
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the heartbeat must reach the phase-lock boundary");

        let promotion = control.begin_promotion(8).await.unwrap();
        resume.notify_one();

        let observed_epoch = promotion.observed_epoch();
        assert_eq!(observed_epoch, 7);
        assert!(
            promotion.tail.preserves_lineage(observed_epoch),
            "takeover must freeze the tail and observed epoch in one owned value"
        );

        let ticked = tokio::time::timeout(Duration::from_secs(3), heartbeat)
            .await
            .expect("the heartbeat must finish after promotion releases the phase lock")
            .unwrap();
        assert!(!ticked, "a promoted receiver must ignore the late beat");
        assert_eq!(
            promotion.observed_epoch(),
            7,
            "the late heartbeat has no path to the frozen snapshot"
        );
        promotion.complete().await.unwrap();
    }

    // A stale ship must be re-fenced if a newer term wins the phase lock.
    #[tokio::test]
    async fn stale_ship_losing_the_phase_lock_race_is_rejected() {
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, control) = spawn_receiver_paused_before_phase_lock(
            dedup.clone(),
            5,
            reached.clone(),
            resume.clone(),
        )
        .await;

        let stale_sender = connect(&endpoint).await;
        let current_sender = connect(&endpoint).await;

        let stale_op_id = [5u8; 16];
        let stale_ship = tokio::spawn(async move {
            stale_sender
                .ship(50, &[put(b"stale", b"write")], &[stale_op_id], 0, 5)
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the stale ship must reach the phase-lock boundary");

        assert!(
            current_sender
                .ship(1, &[put(b"current", b"write")], &[], 0, 6)
                .await
                .unwrap(),
            "the newer epoch ship must be accepted first"
        );
        resume.notify_one();

        let stale_accepted = tokio::time::timeout(Duration::from_secs(3), stale_ship)
            .await
            .expect("the stale ship must finish after it resumes")
            .unwrap();
        assert!(
            !stale_accepted,
            "a stale ship must be re-fenced if a newer epoch won the phase lock"
        );
        let (epoch, seqnos) = control
            .inspect_standby_for_tests(|tail, _| {
                (
                    tail.epoch(),
                    tail.batches_in_order()
                        .map(|(seqno, _)| seqno)
                        .collect::<Vec<_>>(),
                )
            })
            .await;
        assert_eq!(epoch, 6);
        assert_eq!(
            seqnos,
            vec![1],
            "the stale epoch batch must not enter the newer term's tail"
        );
        assert!(
            dedup.get(&stale_op_id).is_none(),
            "a re-fenced stale ship must not poison dedup"
        );
    }
}
