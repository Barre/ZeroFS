//! The replication transport: the standby's receiver (a tonic service over the
//! shared tail buffer) and the leader's sender (a tonic client), over gRPC.

use crate::replication::tail::{ReplOp, TailBuffer};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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

/// The writer epoch this process acquired, published once when it promotes.
/// It remains available after fencing so peer rejections can identify which
/// process is stale.
#[derive(Default)]
pub(crate) struct ReceiverRole {
    writer_epoch: OnceLock<u64>,
}

impl ReceiverRole {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn publish_writer(&self, writer_epoch: u64) {
        assert!(
            self.writer_epoch.set(writer_epoch).is_ok(),
            "receiver writer epoch published twice"
        );
    }

    fn leading_epoch(&self) -> Option<u64> {
        self.writer_epoch.get().copied()
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

/// The standby side: buffers shipped batches into the shared tail buffer, prunes
/// on the leader's watermark, and rejects ships from a stale writer epoch.
pub struct ReplicationReceiver {
    buffer: Arc<Mutex<TailBuffer>>,
    highest_epoch: Arc<AtomicU64>,
    /// Idempotency cache for batches proven durable by pruning or replay.
    dedup: Arc<crate::dedup::DedupCache>,
    /// Ticks on each accepted heartbeat; the failover watch follows it.
    heartbeats: watch::Sender<u64>,
    /// This process's locally opened writer epoch, if it has promoted.
    role: Arc<ReceiverRole>,
    /// Fired when a deposed leader asks (Hello) and we are a standby holding a
    /// tail, so we take over at once instead of waiting out the heartbeat gap.
    takeover_trigger: Option<Arc<Notify>>,
    /// This node's configured id, reported via Hello so a booting peer can
    /// refuse startup on a duplicated node_id (identity keys the latest-leader
    /// record, so a collision is a config error).
    node_id: String,
    /// Test-only gate between the fast checks and the tail lock.
    #[cfg(test)]
    before_tail_lock_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
    /// Test-only gate inside the admission critical section (fence check passed,
    /// append not yet done), to prove epoch publication cannot interleave there.
    #[cfg(test)]
    before_append_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
}

impl ReplicationReceiver {
    pub fn new(
        buffer: Arc<Mutex<TailBuffer>>,
        dedup: Arc<crate::dedup::DedupCache>,
        takeover_trigger: Option<Arc<Notify>>,
        node_id: String,
    ) -> Self {
        Self {
            buffer,
            highest_epoch: Arc::new(AtomicU64::new(0)),
            dedup,
            heartbeats: watch::channel(0u64).0,
            role: Arc::new(ReceiverRole::default()),
            takeover_trigger,
            node_id,
            #[cfg(test)]
            before_tail_lock_pause: None,
            #[cfg(test)]
            before_append_pause: None,
        }
    }

    #[cfg(test)]
    fn pause_epoch_before_tail_lock(
        mut self,
        epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> Self {
        self.before_tail_lock_pause = Some((epoch, reached, resume));
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

    /// Highest writer epoch seen from the leader (ships + heartbeats). At takeover
    /// the promoted standby uses it to confirm the db's HA tail watermark was
    /// written by this same term before pruning its tail by it.
    pub fn highest_epoch(&self) -> Arc<AtomicU64> {
        self.highest_epoch.clone()
    }

    /// Handle used to publish the local writer epoch before takeover replay.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn role(&self) -> Arc<ReceiverRole> {
        self.role.clone()
    }

    /// A promoted receiver never buffers peer writes. A lower or equal sender
    /// is deposed; a higher sender means this receiver is the stale process.
    fn leading_rejection(&self, incoming_epoch: u64) -> Result<Option<ReplicateResponse>, Status> {
        let Some(local_epoch) = self.role.leading_epoch() else {
            return Ok(None);
        };
        if incoming_epoch > local_epoch {
            return Err(Status::unavailable(format!(
                "receiver is a stale writer at epoch {local_epoch}; incoming writer epoch is {incoming_epoch}"
            )));
        }
        Ok(Some(ReplicateResponse { accepted: false }))
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
        if let Some(rejection) = self.leading_rejection(req.writer_epoch)? {
            return Ok(Response::new(rejection));
        }
        // Fast-path reject for a deposed leader (epoch below the highest seen).
        // Advisory only, no publication: the authoritative fence runs under the
        // tail lock below, atomically with the append.
        if req.writer_epoch < self.highest_epoch.load(Ordering::Acquire) {
            return Ok(Response::new(ReplicateResponse { accepted: false }));
        }
        let ops = to_repl_ops(req.ops);
        let op_ids: Vec<crate::dedup::OpId> = req
            .op_ids
            .into_iter()
            .filter_map(|raw| <[u8; 16]>::try_from(raw.as_slice()).ok())
            .filter(crate::dedup::has_op_id)
            .collect();
        #[cfg(test)]
        if let Some((epoch, reached, resume)) = &self.before_tail_lock_pause
            && req.writer_epoch == *epoch
        {
            reached.notify_one();
            resume.notified().await;
        }
        {
            let mut buf = self.buffer.lock().await;
            // Serialize promotion/replay against the append.
            if let Some(rejection) = self.leading_rejection(req.writer_epoch)? {
                return Ok(Response::new(rejection));
            }
            // The authoritative epoch fence. Publication (here and in `heartbeat`)
            // happens only under the tail lock, so no newer epoch can land between
            // this check and the append.
            let prev = self
                .highest_epoch
                .fetch_max(req.writer_epoch, Ordering::AcqRel);
            if req.writer_epoch < prev {
                return Ok(Response::new(ReplicateResponse { accepted: false }));
            }
            #[cfg(test)]
            if let Some((epoch, reached, resume)) = &self.before_append_pause
                && req.writer_epoch == *epoch
            {
                reached.notify_one();
                resume.notified().await;
            }
            // Cross-term replacement uses the tail epoch; `highest_epoch` may
            // already have advanced on a heartbeat.
            buf.accept(req.writer_epoch, req.seqno, ops, op_ids);
            if req.prune_watermark > 0 {
                // Publish under the tail lock so takeover sees one disposition.
                for op_id in buf.prune(req.prune_watermark) {
                    self.dedup.record(op_id, Bytes::new());
                }
            }
        }
        Ok(Response::new(ReplicateResponse { accepted: true }))
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
            if self.role.leading_epoch().is_some() {
                continue;
            }
            // Publication shares the tail lock with ship admission (see
            // `replicate`): a beat's epoch bump must not land inside another
            // ship's fence-check-to-append window.
            let prev = {
                let _buf = self.buffer.lock().await;
                self.highest_epoch
                    .fetch_max(beat.writer_epoch, Ordering::AcqRel)
            };
            if beat.writer_epoch >= prev {
                self.heartbeats.send_modify(|c| *c = c.wrapping_add(1));
            }
        }
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn hello(
        &self,
        _request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        let leading = self.role.leading_epoch().is_some();
        let has_tail = !self.buffer.lock().await.is_empty();
        // A standby holding the tail takes over NOW so the deposed leader's defer
        // doesn't wait out the heartbeat gap (a leader already serves).
        if !leading
            && has_tail
            && let Some(trigger) = &self.takeover_trigger
        {
            trigger.notify_one();
        }
        // Active = leading, or a standby holding an un-replayed tail (it will take
        // over and preserve it). Either way the caller must defer, not open as writer.
        Ok(Response::new(HelloResponse {
            peer_active: leading || has_tail,
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
/// writer. `peer_active` is true if the peer is leading or holds an un-replayed
/// tail; `node_id` is the peer's configured id (empty from a peer predating the
/// field), which the caller checks against its own to catch a duplicated
/// node_id at startup.
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

    fn new_receiver(
        buffer: Arc<Mutex<TailBuffer>>,
        dedup: Arc<crate::dedup::DedupCache>,
    ) -> ReplicationReceiver {
        ReplicationReceiver::new(buffer, dedup, None, "standby-under-test".to_string())
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

    async fn spawn_receiver(buffer: Arc<Mutex<TailBuffer>>) -> String {
        serve(new_receiver(
            buffer,
            Arc::new(crate::dedup::DedupCache::new(64)),
        ))
        .await
    }

    async fn spawn_receiver_with_dedup(
        buffer: Arc<Mutex<TailBuffer>>,
        dedup: Arc<crate::dedup::DedupCache>,
    ) -> String {
        serve(new_receiver(buffer, dedup)).await
    }

    /// Like [`spawn_receiver`] but also hands back the receiver's `highest_epoch`,
    /// so a test can observe a heartbeat's effect on it.
    async fn spawn_receiver_exposing_epoch(
        buffer: Arc<Mutex<TailBuffer>>,
    ) -> (String, Arc<AtomicU64>) {
        let receiver = new_receiver(buffer, Arc::new(crate::dedup::DedupCache::new(64)));
        let highest_epoch = receiver.highest_epoch();
        (serve(receiver).await, highest_epoch)
    }

    async fn spawn_receiver_with_role(
        buffer: Arc<Mutex<TailBuffer>>,
    ) -> (String, Arc<ReceiverRole>) {
        let receiver = new_receiver(buffer, Arc::new(crate::dedup::DedupCache::new(64)));
        let role = receiver.role();
        (serve(receiver).await, role)
    }

    /// Receiver paused inside the admission critical section (fence check passed,
    /// append pending) for `paused_epoch`, exposing `highest_epoch` so the test
    /// can observe whether a concurrent publication lands during the pause.
    async fn spawn_receiver_paused_before_append(
        buffer: Arc<Mutex<TailBuffer>>,
        paused_epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> (String, Arc<AtomicU64>) {
        let receiver = new_receiver(buffer, Arc::new(crate::dedup::DedupCache::new(64)))
            .pause_epoch_before_append(paused_epoch, reached, resume);
        let highest_epoch = receiver.highest_epoch();
        (serve(receiver).await, highest_epoch)
    }

    async fn spawn_receiver_paused_before_tail_lock(
        buffer: Arc<Mutex<TailBuffer>>,
        dedup: Arc<crate::dedup::DedupCache>,
        paused_epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> (String, Arc<ReceiverRole>) {
        let receiver =
            new_receiver(buffer, dedup).pause_epoch_before_tail_lock(paused_epoch, reached, resume);
        let role = receiver.role();
        (serve(receiver).await, role)
    }

    fn put(k: &[u8], v: &[u8]) -> ReplOp {
        ReplOp::Put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
    }

    #[tokio::test]
    async fn ships_buffers_prunes_and_fences_stale_epoch() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let endpoint = spawn_receiver(buffer.clone()).await;
        let sender = connect(&endpoint).await;

        assert!(sender.ship(1, &[put(b"a", b"1")], &[], 0, 1).await.unwrap());
        assert!(sender.ship(2, &[put(b"b", b"2")], &[], 0, 1).await.unwrap());
        assert_eq!(buffer.lock().await.len(), 2);

        // A ship carrying a prune watermark drops the durable prefix.
        assert!(sender.ship(3, &[put(b"c", b"3")], &[], 1, 1).await.unwrap());
        {
            let buf = buffer.lock().await;
            let seqnos: Vec<u64> = buf.batches_in_order().map(|(s, _)| s).collect();
            assert_eq!(seqnos, vec![2, 3], "watermark 1 prunes seqno 1");
        }

        // A ship from an older writer epoch (a deposed leader) is rejected.
        assert!(!sender.ship(4, &[put(b"x", b"x")], &[], 0, 0).await.unwrap());
        assert_eq!(
            buffer.lock().await.len(),
            2,
            "a stale-epoch ship must not buffer"
        );
    }

    // A watermark is the first proof that a volatile tail batch is durable.
    #[tokio::test]
    async fn pending_op_ids_publish_only_when_pruned_as_durable() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let endpoint = spawn_receiver_with_dedup(buffer, dedup.clone()).await;
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
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let endpoint = spawn_receiver_with_dedup(buffer, dedup.clone()).await;
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
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let endpoint = spawn_receiver(buffer.clone()).await;
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
        assert_eq!(buffer.lock().await.len(), 1, "the large batch must buffer");
    }

    // Hello reports the responder's configured node_id, so a booting peer can
    // refuse startup on a duplicated identity (it keys the latest-leader record).
    #[tokio::test]
    async fn hello_reports_the_responders_node_id() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let endpoint = spawn_receiver(buffer).await;
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
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let receiver = ReplicationReceiver::new(
            buffer,
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
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let endpoint = spawn_receiver(buffer.clone()).await;
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

        let buf = buffer.lock().await;
        assert_eq!(buf.epoch(), 6);
        assert_eq!(
            buf.batches_in_order()
                .map(|(seqno, _)| seqno)
                .collect::<Vec<_>>(),
            vec![1],
            "the newer term must replace the old high-seqno tail"
        );
    }

    // Same cross-term scenario as above, but the restarted leader's heartbeat
    // reaches the standby before its first ship.
    #[tokio::test]
    async fn heartbeat_epoch_bump_must_not_defeat_the_cross_term_tail_clear() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let (endpoint, highest_epoch) = spawn_receiver_exposing_epoch(buffer.clone()).await;
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
        // heartbeat and wait until the receiver has processed it (highest_epoch == 6).
        let hb = tokio::spawn(run_heartbeat_sender(
            endpoint.clone(),
            6,
            Duration::from_secs(3600),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while highest_epoch.load(Ordering::Acquire) < 6 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the heartbeat must bump highest_epoch to 6");

        // Now the post-restart (epoch 6) write ships at the reset seqno 1.
        assert!(
            sender
                .ship(1, &[put(b"/f", b"new")], &[], 0, 6)
                .await
                .unwrap(),
            "a higher-epoch ship is accepted"
        );
        hb.abort();

        let buf = buffer.lock().await;
        assert_eq!(buf.epoch(), 6);
        assert_eq!(
            buf.batches_in_order()
                .map(|(seqno, _)| seqno)
                .collect::<Vec<_>>(),
            vec![1],
            "a heartbeat epoch bump must not suppress cross-term replacement"
        );
    }

    // A leading receiver must distinguish a zombie sender from a peer that has
    // acquired a strictly newer writer epoch. Neither may enter its replayed tail,
    // but only the former is evidence that the sender itself is deposed.
    #[tokio::test]
    async fn leading_receiver_rejects_zombies_but_reports_newer_writer_unavailable() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let (endpoint, role) = spawn_receiver_with_role(buffer.clone()).await;
        let sender = connect(&endpoint).await;

        // Before promotion, an epoch-7 ship is accepted and buffered.
        assert!(sender.ship(1, &[put(b"a", b"1")], &[], 0, 7).await.unwrap());
        assert_eq!(buffer.lock().await.len(), 1);

        // This process publishes the writer epoch it acquired on open.
        role.publish_writer(7);

        // The deposed leader (still epoch 7) re-ships: it must be rejected, and
        // nothing must enter the tail or dedup.
        assert!(
            !sender
                .ship(2, &[put(b"b", b"2")], &[[9u8; 16]], 0, 7)
                .await
                .unwrap(),
            "a promoted node must reject an equal-epoch zombie's ship"
        );
        assert_eq!(
            buffer.lock().await.len(),
            1,
            "a rejected zombie ship must not buffer into the promoted node's tail"
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
            buffer.lock().await.len(),
            1,
            "the newer writer must not enter a stale process's already-replayed tail"
        );
    }

    // Promotion may land between the fast role check and the tail lock.
    #[tokio::test]
    async fn ship_losing_the_promotion_race_is_rejected() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, role) = spawn_receiver_paused_before_tail_lock(
            buffer.clone(),
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
            .expect("the ship must reach the tail-lock boundary");

        // Promotion publishes the new role before taking this same lock for its
        // one-time replay. Keep it held until the waiting handler is released.
        role.publish_writer(8);
        let replay_guard = buffer.lock().await;
        resume.notify_one();
        tokio::task::yield_now().await;
        drop(replay_guard);

        let accepted = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the ship must finish after replay releases the tail lock")
            .unwrap();
        assert!(
            !accepted,
            "a ship that loses the tail-lock race to promotion must be rejected"
        );
        assert!(
            buffer.lock().await.is_empty(),
            "the post-replay ship must not enter the tail"
        );
        assert!(
            dedup.get(&op_id).is_none(),
            "a rejected post-replay ship must not poison dedup"
        );
    }

    // A newer sender makes a promoter with an older local epoch the stale side.
    #[tokio::test]
    async fn newer_ship_losing_the_promotion_race_sees_stale_receiver() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, role) = spawn_receiver_paused_before_tail_lock(
            buffer.clone(),
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
            .expect("the newer ship must reach the tail-lock boundary");

        role.publish_writer(8);
        let replay_guard = buffer.lock().await;
        resume.notify_one();
        tokio::task::yield_now().await;
        drop(replay_guard);

        let err = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the newer ship must finish after replay releases the tail lock")
            .unwrap()
            .expect_err("the stale promoter must report itself unavailable");
        assert_eq!(
            err.downcast_ref::<tonic::Status>().map(tonic::Status::code),
            Some(tonic::Code::Unavailable)
        );
        assert!(
            buffer.lock().await.is_empty(),
            "a stale promoter must not append after its one-time replay"
        );
        assert!(
            dedup.get(&op_id).is_none(),
            "a stale promoter must not publish the rejected op-id"
        );
    }

    // Heartbeat epoch publication shares the ship admission critical section.
    #[tokio::test]
    async fn heartbeat_epoch_update_waits_for_ship_admission() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, highest_epoch) =
            spawn_receiver_paused_before_append(buffer.clone(), 5, reached.clone(), resume.clone())
                .await;
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
            while highest_epoch.load(Ordering::Acquire) < 6 {
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
            while highest_epoch.load(Ordering::Acquire) < 6 {
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

    // A stale ship must be re-fenced if a newer term wins the tail lock.
    #[tokio::test]
    async fn stale_ship_losing_the_tail_lock_race_is_rejected() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let dedup = Arc::new(crate::dedup::DedupCache::new(64));
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, _role) = spawn_receiver_paused_before_tail_lock(
            buffer.clone(),
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
            .expect("the stale ship must reach the tail-lock boundary");

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
            "a stale ship must be re-fenced if a newer epoch won the tail lock"
        );
        let buf = buffer.lock().await;
        assert_eq!(buf.epoch(), 6);
        assert_eq!(
            buf.batches_in_order()
                .map(|(seqno, _)| seqno)
                .collect::<Vec<_>>(),
            vec![1],
            "the stale epoch batch must not enter the newer term's tail"
        );
        drop(buf);
        assert!(
            dedup.get(&stale_op_id).is_none(),
            "a re-fenced stale ship must not poison dedup"
        );
    }
}
