//! The replication transport: the standby's receiver (a tonic service over the
//! shared tail buffer) and the leader's sender (a tonic client), over gRPC.

use crate::replication::tail::{ReplOp, TailBuffer};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// Idempotency cache kept warm by recording each shipped op-id, so a client's
    /// retry after this node promotes is recognized as already applied.
    dedup: Arc<crate::dedup::DedupCache>,
    /// Ticks on each accepted heartbeat; the failover watch follows it.
    heartbeats: watch::Sender<u64>,
    /// True once this node is the writer. Reported via Hello so a (re)starting
    /// peer defers to it instead of opening as writer.
    leading: Arc<AtomicBool>,
    /// Fired when a deposed leader asks (Hello) and we are a standby holding a
    /// tail, so we take over at once instead of waiting out the heartbeat gap.
    takeover_trigger: Option<Arc<Notify>>,
    /// This node's configured id, reported via Hello so a booting peer can
    /// refuse startup on a duplicated node_id (identity keys the latest-leader
    /// record, so a collision is a config error).
    node_id: String,
}

impl ReplicationReceiver {
    pub fn new(
        buffer: Arc<Mutex<TailBuffer>>,
        dedup: Arc<crate::dedup::DedupCache>,
        leading: Arc<AtomicBool>,
        takeover_trigger: Option<Arc<Notify>>,
        node_id: String,
    ) -> Self {
        Self {
            buffer,
            highest_epoch: Arc::new(AtomicU64::new(0)),
            dedup,
            heartbeats: watch::channel(0u64).0,
            leading,
            takeover_trigger,
            node_id,
        }
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
        // Once this node has promoted (it is now the writer), it must never accept
        // a ship: a deposed leader that shares the same writer epoch (its fence in
        // the object store hasn't bitten yet) would otherwise be accepted here,
        // recorded into dedup, and buffered into an already-replayed tail — the old
        // leader would then ack that write as "replicated" moments before it dies,
        // losing an acked write and falsely deduping the client's retry. Being the
        // writer is the authoritative signal that any other term is deposed.
        if self.leading.load(Ordering::Acquire) {
            return Ok(Response::new(ReplicateResponse { accepted: false }));
        }
        // Reject a deposed leader: a ship whose epoch is below the highest seen.
        let prev = self
            .highest_epoch
            .fetch_max(req.writer_epoch, Ordering::AcqRel);
        if req.writer_epoch < prev {
            return Ok(Response::new(ReplicateResponse { accepted: false }));
        }
        let ops = to_repl_ops(req.ops);
        {
            let mut buf = self.buffer.lock().await;
            // `accept` drops a prior term's tail when this ship's epoch exceeds the
            // tail's own. Keyed there, not on `prev` (the `highest_epoch` fence,
            // which heartbeats also advance): a beat from the new term arrives
            // before its first ship, so `prev` would already equal this epoch and
            // the drop would be skipped, letting a stale batch replay over the new
            // term's writes.
            buf.accept(req.writer_epoch, req.seqno, ops);
            if req.prune_watermark > 0 {
                buf.prune(req.prune_watermark);
            }
        }
        // Keep the dedup cache warm so a retry after promotion is recognized.
        for raw in req.op_ids {
            if let Ok(op_id) = <[u8; 16]>::try_from(raw.as_slice()) {
                self.dedup.record(op_id, Bytes::new());
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
            if self.leading.load(Ordering::Acquire) {
                continue;
            }
            let prev = self
                .highest_epoch
                .fetch_max(beat.writer_epoch, Ordering::AcqRel);
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
        let leading = self.leading.load(Ordering::Acquire);
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

/// The leader side: ships batches and reports whether the standby accepted them.
/// Errors propagate so the caller does not ack the client on a failed ship.
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

    async fn spawn_receiver(buffer: Arc<Mutex<TailBuffer>>) -> String {
        spawn_receiver_with_leading(buffer, Arc::new(AtomicBool::new(false))).await
    }

    /// Like [`spawn_receiver`] but also hands back the receiver's `highest_epoch`,
    /// so a test can observe a heartbeat's effect on it.
    async fn spawn_receiver_exposing_epoch(
        buffer: Arc<Mutex<TailBuffer>>,
    ) -> (String, Arc<AtomicU64>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let receiver = ReplicationReceiver::new(
            buffer,
            Arc::new(crate::dedup::DedupCache::new(64)),
            Arc::new(AtomicBool::new(false)),
            None,
            "standby-under-test".to_string(),
        );
        let highest_epoch = receiver.highest_epoch();
        let server = receiver.into_server();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (format!("http://{addr}"), highest_epoch)
    }

    async fn spawn_receiver_with_leading(
        buffer: Arc<Mutex<TailBuffer>>,
        leading: Arc<AtomicBool>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = ReplicationReceiver::new(
            buffer,
            Arc::new(crate::dedup::DedupCache::new(64)),
            leading,
            None,
            "standby-under-test".to_string(),
        )
        .into_server();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    fn put(k: &[u8], v: &[u8]) -> ReplOp {
        ReplOp::Put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
    }

    #[tokio::test]
    async fn ships_buffers_prunes_and_fences_stale_epoch() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let endpoint = spawn_receiver(buffer.clone()).await;
        let sender = loop {
            match ReplicationSender::connect(endpoint.clone()).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };

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

    // A coalesced ship past tonic's 4 MiB default decode limit must be accepted:
    // at the default the standby rejects it before the handler, the leader sees a
    // failed ship, and it downgrades to solo and taints the lineage under ordinary
    // write load. `into_server` raises the limit; this proves the wiring.
    #[tokio::test]
    async fn ship_larger_than_the_default_decode_limit_is_accepted() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let endpoint = spawn_receiver(buffer.clone()).await;
        let sender = loop {
            match ReplicationSender::connect(endpoint.clone()).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };

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
            Arc::new(AtomicBool::new(false)),
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
        let sender = loop {
            match ReplicationSender::connect(endpoint.clone()).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };

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

        // Replay the tail in seqno order, exactly as a takeover does.
        let buf = buffer.lock().await;
        let mut state: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for (_seqno, ops) in buf.batches_in_order() {
            for op in ops {
                match op {
                    ReplOp::Put(k, v) | ReplOp::PutFrame(k, v, _) => {
                        state.insert(k.to_vec(), v.to_vec());
                    }
                    ReplOp::Delete(k) => {
                        state.remove(k.as_ref());
                    }
                }
            }
        }
        assert_eq!(
            state.get(b"/f".as_ref()).map(|v| v.as_slice()),
            Some(b"new".as_ref()),
            "the post-restart (epoch 6) write must win, but a stale epoch-5 batch at a \
             higher seqno replayed last and clobbered it (cross-term seqno collision)"
        );
    }

    // Same cross-term scenario as above, but the restarted leader's heartbeat
    // reaches the standby before its first ship.
    #[tokio::test]
    async fn heartbeat_epoch_bump_must_not_defeat_the_cross_term_tail_clear() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let (endpoint, highest_epoch) = spawn_receiver_exposing_epoch(buffer.clone()).await;
        let sender = loop {
            match ReplicationSender::connect(endpoint.clone()).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };

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

        // Replay the tail in seqno order, exactly as a takeover does.
        let buf = buffer.lock().await;
        let mut state: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for (_seqno, ops) in buf.batches_in_order() {
            for op in ops {
                match op {
                    ReplOp::Put(k, v) | ReplOp::PutFrame(k, v, _) => {
                        state.insert(k.to_vec(), v.to_vec());
                    }
                    ReplOp::Delete(k) => {
                        state.remove(k.as_ref());
                    }
                }
            }
        }
        assert_eq!(
            state.get(b"/f".as_ref()).map(|v| v.as_slice()),
            Some(b"new".as_ref()),
            "the epoch-6 write must win: a heartbeat bumped highest_epoch, so the \
             epoch-6 ship did not clear the stale epoch-5 tail, which replayed last"
        );
    }

    // A standby promotes (leading=true), then a deposed leader sharing the
    // same writer epoch re-ships. The ship must be rejected — see the
    // `leading` check in `replicate`.
    #[tokio::test]
    async fn promoted_node_rejects_equal_epoch_zombie_ship() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let leading = Arc::new(AtomicBool::new(false));
        let endpoint = spawn_receiver_with_leading(buffer.clone(), leading.clone()).await;
        let sender = loop {
            match ReplicationSender::connect(endpoint.clone()).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };

        // Before promotion, an epoch-7 ship is accepted and buffered.
        assert!(sender.ship(1, &[put(b"a", b"1")], &[], 0, 7).await.unwrap());
        assert_eq!(buffer.lock().await.len(), 1);

        // This node promotes (opens the db as writer at a new epoch).
        leading.store(true, Ordering::Release);

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
    }
}
