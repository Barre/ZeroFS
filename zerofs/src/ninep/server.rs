use super::errors::P9Error;
use super::handler::NinePHandler;
use super::lock_manager::FileLockManager;
use super::protocol::{
    Message, P9_CHANNEL_SIZE, P9_DEBUG_BUFFER_SIZE, P9_MAX_MSIZE, P9_MIN_MESSAGE_SIZE,
    P9_SIZE_FIELD_LEN, P9Message, Rlerror,
};
use crate::fs::ZeroFS;
use crate::task::spawn_named;
use dashmap::DashMap;
use deku::prelude::*;
use futures::StreamExt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub enum Transport {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

pub struct NinePServer {
    filesystem: Arc<ZeroFS>,
    transport: Transport,
    lock_manager: Arc<FileLockManager>,
}

impl NinePServer {
    pub fn new(filesystem: Arc<ZeroFS>, addr: SocketAddr) -> Self {
        Self {
            filesystem,
            transport: Transport::Tcp(addr),
            lock_manager: Arc::new(FileLockManager::new()),
        }
    }

    pub fn new_unix(filesystem: Arc<ZeroFS>, path: PathBuf) -> Self {
        Self {
            filesystem,
            transport: Transport::Unix(path),
            lock_manager: Arc::new(FileLockManager::new()),
        }
    }

    fn spawn_client_handler<S>(&self, stream: S, shutdown: &CancellationToken, client_name: String)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let filesystem = Arc::clone(&self.filesystem);
        let lock_manager = Arc::clone(&self.lock_manager);
        let client_shutdown = shutdown.child_token();

        spawn_named("9p-client", async move {
            if let Err(e) =
                handle_client_stream(stream, filesystem, lock_manager, client_shutdown).await
            {
                error!("Error handling 9P client {}: {}", client_name, e);
            }
        });
    }

    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        match &self.transport {
            Transport::Tcp(addr) => {
                let listener = TcpListener::bind(addr).await?;
                info!("9P server listening on TCP {}", addr);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("9P TCP server shutting down on {}", addr);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, peer_addr) = result?;
                            info!("9P client connected from {}", peer_addr);
                            stream.set_nodelay(true)?;
                            self.spawn_client_handler(stream, &shutdown, peer_addr.to_string());
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                let _ = std::fs::remove_file(path);

                let listener = UnixListener::bind(path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to bind Unix socket at {:?}: {}", path, e),
                    )
                })?;
                info!("9P server listening on Unix socket {:?}", path);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("9P Unix socket server shutting down at {:?}", path);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, _) = result?;
                            info!("9P client connected via Unix socket");
                            self.spawn_client_handler(stream, &shutdown, "unix".to_string());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_client_stream<S>(
    stream: S,
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let handler = Arc::new(NinePHandler::new(filesystem, lock_manager.clone()));
    let handler_id = handler.handler_id();

    let (read_stream, mut write_stream) = tokio::io::split(stream);

    let (tx, mut rx) = mpsc::channel::<(u16, Vec<u8>)>(P9_CHANNEL_SIZE);

    let writer_task = spawn_named("9p-writer", async move {
        while let Some((tag, response_bytes)) = rx.recv().await {
            if let Err(e) = write_stream.write_all(&response_bytes).await {
                error!("Failed to write response for tag {}: {}", tag, e);
                break;
            }
        }
    });

    let result = handle_client_loop(handler, read_stream, tx, shutdown).await;

    lock_manager.release_session_locks(handler_id).await;

    let _ = writer_task.await;

    result
}

/// Tracks in-flight requests by tag, with a Notify to signal completion.
type InflightRequests = Arc<DashMap<u16, Arc<tokio::sync::Notify>>>;

/// Tracks the latest Tflush tag for each oldtag. Only the last flush gets a response.
/// Per 9P spec: "should a server receive a request and then multiple flushes for that
/// request, it need respond only to the last flush."
type PendingFlushes = Arc<DashMap<u16, u16>>;

async fn handle_client_loop<R>(
    handler: Arc<NinePHandler>,
    read_stream: R,
    tx: mpsc::Sender<(u16, Vec<u8>)>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    let inflight: InflightRequests = Arc::new(DashMap::new());
    let pending_flushes: PendingFlushes = Arc::new(DashMap::new());

    // 9P framing: 4-byte little-endian size field at offset 0, where
    // size includes the size field itself (e.g., size=21 means 21 total bytes).
    let codec = LengthDelimitedCodec::builder()
        .little_endian()
        .length_field_offset(0)
        .length_field_length(P9_SIZE_FIELD_LEN)
        .length_adjustment(0)
        .num_skip(0)
        .max_frame_length(P9_MAX_MSIZE as usize)
        .new_read(read_stream);

    tokio::pin!(codec);

    loop {
        let full_buf = tokio::select! {
            _ = shutdown.cancelled() => {
                debug!("9P client handler shutting down");
                return Ok(());
            }
            result = codec.next() => {
                match result {
                    Some(Ok(buf)) => buf,
                    Some(Err(e)) => {
                        return Err(e.into());
                    }
                    None => {
                        debug!("Client disconnected");
                        return Ok(());
                    }
                }
            }
        };

        if full_buf.len() < P9_MIN_MESSAGE_SIZE as usize {
            error!("Message too short: {} bytes", full_buf.len());
            return Err(anyhow::anyhow!("Message too short"));
        }

        match P9Message::from_bytes((&full_buf, 0)) {
            Ok((_, parsed)) => {
                debug!(
                    "Received message type {} tag {}: {:?}",
                    parsed.type_, parsed.tag, parsed.body
                );

                let tag = parsed.tag;
                let body = parsed.body;

                let flush_oldtag = if let Message::Tflush(ref tflush) = body {
                    // Register this as the latest flush for oldtag (overwrites any previous)
                    pending_flushes.insert(tflush.oldtag, tag);
                    Some(tflush.oldtag)
                } else {
                    None
                };

                let notify = if flush_oldtag.is_none() {
                    let notify = Arc::new(tokio::sync::Notify::new());
                    inflight.insert(tag, Arc::clone(&notify));
                    Some(notify)
                } else {
                    None
                };

                let handler = Arc::clone(&handler);
                let tx = tx.clone();
                let inflight = Arc::clone(&inflight);
                let pending_flushes = Arc::clone(&pending_flushes);

                spawn_named("9p-request", async move {
                    let response = handler.handle_message(tag, body).await;

                    // For Tflush: wait for the flushed request to complete before sending Rflush.
                    // This ensures the flushed request's response is sent first, per 9P spec:
                    // "The server may respond to the pending request before responding to Tflush"
                    if let Some(oldtag) = flush_oldtag {
                        if let Some(old_notify) =
                            inflight.get(&oldtag).map(|r| Arc::clone(r.value()))
                        {
                            debug!("Tflush: waiting for oldtag {} to complete", oldtag);
                            old_notify.notified().await;
                            debug!("Tflush: oldtag {} completed", oldtag);
                        }

                        // Only send Rflush if we're still the latest flush for this oldtag.
                        // If a newer Tflush arrived, it supersedes us and we stay silent.
                        let is_latest = pending_flushes
                            .get(&oldtag)
                            .is_some_and(|latest_tag| *latest_tag == tag);

                        if !is_latest {
                            debug!(
                                "Tflush: tag {} superseded by newer flush for oldtag {}",
                                tag, oldtag
                            );
                            return;
                        }

                        // We're the latest - clean up and send response
                        pending_flushes.remove(&oldtag);
                    }

                    match response.to_bytes() {
                        Ok(response_bytes) => {
                            if let Err(e) = tx.send((tag, response_bytes)).await {
                                warn!("Failed to send response for tag {}: {}", tag, e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to serialize response for tag {}: {:?}", tag, e);
                        }
                    }

                    if let Some(notify) = notify {
                        inflight.remove(&tag);
                        notify.notify_waiters();
                    }
                });
            }
            Err(e) => {
                // Failed to parse message - check if we can at least get the tag
                if full_buf.len() >= P9_MIN_MESSAGE_SIZE as usize {
                    let tag = u16::from_le_bytes([full_buf[5], full_buf[6]]);
                    let msg_type = full_buf[4];
                    debug!(
                        "Failed to parse message type {} (0x{:02x}) tag {}: {:?}",
                        msg_type, msg_type, tag, e
                    );
                    debug!(
                        "Message size: {}, buffer (first {} bytes): {:?}",
                        full_buf.len(),
                        P9_DEBUG_BUFFER_SIZE,
                        &full_buf[0..std::cmp::min(P9_DEBUG_BUFFER_SIZE, full_buf.len())]
                    );
                    let error_msg = P9Message::new(
                        tag,
                        Message::Rlerror(Rlerror {
                            ecode: P9Error::NotImplemented.to_errno(),
                        }),
                    );
                    let response_bytes = error_msg.to_bytes().expect("Failed to serialize Rlerror");

                    if let Err(e) = tx.send((tag, response_bytes)).await {
                        error!("Failed to send error response: {}", e);
                        return Err(e.into());
                    }
                } else {
                    debug!("Message too short to parse: {:?}", e);
                    return Err(anyhow::anyhow!("Failed to parse message: {e:?}"));
                }
            }
        };
    }
}
