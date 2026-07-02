//! The replication transport: the standby's receiver (a tonic service over the
//! shared tail buffer) and the leader's sender (a tonic client), over gRPC.
#![allow(dead_code)]

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
}

impl ReplicationReceiver {
    pub fn new(
        buffer: Arc<Mutex<TailBuffer>>,
        dedup: Arc<crate::dedup::DedupCache>,
        leading: Arc<AtomicBool>,
        takeover_trigger: Option<Arc<Notify>>,
    ) -> Self {
        Self {
            buffer,
            highest_epoch: Arc::new(AtomicU64::new(0)),
            dedup,
            heartbeats: watch::channel(0u64).0,
            leading,
            takeover_trigger,
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
        ReplicationServiceServer::new(self)
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
            // A higher epoch is a new leader term: drop the prior term's tail so its
            // batches can't replay (in seqno order) over the new term's writes.
            if req.writer_epoch > prev {
                buf.clear();
            }
            buf.accept(req.seqno, ops);
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
        }))
    }
}

/// The leader side: ships batches and reports whether the standby accepted them.
/// Errors propagate so the caller does NOT ack the client on a failed ship.
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
/// writer: true if the peer is leading or holds an un-replayed tail.
pub async fn hello_peer(endpoint: String) -> anyhow::Result<bool> {
    let mut client =
        tokio::time::timeout(CONNECT_TIMEOUT, ReplicationServiceClient::connect(endpoint))
            .await
            .map_err(|_| anyhow::anyhow!("Hello dial timed out"))??;
    let resp = tokio::time::timeout(RPC_TIMEOUT, client.hello(HelloRequest {}))
        .await
        .map_err(|_| anyhow::anyhow!("Hello RPC timed out"))??;
    Ok(resp.into_inner().peer_active)
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

    #[tokio::test]
    async fn heartbeats_tick_liveness_and_stop_on_break() {
        let buffer = Arc::new(Mutex::new(TailBuffer::new()));
        let receiver = ReplicationReceiver::new(
            buffer,
            Arc::new(crate::dedup::DedupCache::new(64)),
            Arc::new(AtomicBool::new(false)),
            None,
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
    // (high seqno) replay AFTER the term-6 write (low seqno) and must not clobber
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

    // Repro: a standby promotes (leading=true), then a deposed leader that shares
    // the same writer epoch (its object-store fence hasn't bitten yet) re-ships.
    // A leader must NEVER accept a ship: doing so would let the zombie ack an
    // acked-but-lost write and poison this node's dedup. Being the writer is the
    // authoritative "every other term is deposed" signal.
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
