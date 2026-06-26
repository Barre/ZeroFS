use super::errors::P9Error;
pub(crate) use super::handler::NinePHandler;
pub(crate) use super::lock_manager::FileLockManager;
use crate::fs::ZeroFS;
use crate::task::spawn_named;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use ninep_proto::{
    Message, P9_CHANNEL_SIZE, P9_DEBUG_BUFFER_SIZE, P9_HEADER_SIZE, P9_MAX_MSIZE,
    P9_MIN_MESSAGE_SIZE, P9_SIZE_FIELD_LEN, P9Message, Rlerror,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 9P message type byte for Tflush. Kept here so the reader can recognise a
/// flush from the raw frame header without deku-parsing the whole body.
const TFLUSH_TYPE: u8 = 108;

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

    fn spawn_client_handler<R, W>(
        &self,
        read_stream: R,
        write_stream: W,
        shutdown: &CancellationToken,
        client_name: String,
    ) where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let filesystem = Arc::clone(&self.filesystem);
        let lock_manager = Arc::clone(&self.lock_manager);
        let client_shutdown = shutdown.child_token();

        spawn_named("9p-client", async move {
            if let Err(e) = handle_client_stream(
                read_stream,
                write_stream,
                filesystem,
                lock_manager,
                client_shutdown,
            )
            .await
            {
                error!("Error handling 9P client {}: {}", client_name, e);
            }
        });
    }

    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        match &self.transport {
            Transport::Tcp(addr) => {
                let listener = TcpListener::bind(addr)
                    .await
                    .map_err(|e| crate::net_util::tcp_bind_error("9P", addr, &e))?;
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
                            let (read_half, write_half) = stream.into_split();
                            self.spawn_client_handler(read_half, write_half, &shutdown, peer_addr.to_string());
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
                            let (read_half, write_half) = stream.into_split();
                            self.spawn_client_handler(read_half, write_half, &shutdown, "unix".to_string());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_client_stream<R, W>(
    read_stream: R,
    write_stream: W,
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let handler = Arc::new(NinePHandler::new(filesystem, lock_manager.clone()));
    let handler_id = handler.handler_id();

    let (tx, mut rx) = mpsc::channel::<(u16, Vec<u8>)>(P9_CHANNEL_SIZE);

    let writer_task = spawn_named("9p-writer", async move {
        let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, write_stream);
        loop {
            let (tag, response_bytes) = match rx.recv().await {
                Some(msg) => msg,
                None => break,
            };
            if let Err(e) = writer.write_all(&response_bytes).await {
                error!("Failed to write response for tag {}: {}", tag, e);
                return;
            }
            while let Ok((_, more)) = rx.try_recv() {
                if let Err(e) = writer.write_all(&more).await {
                    error!("Failed to write response: {}", e);
                    return;
                }
            }
            if let Err(e) = writer.flush().await {
                error!("Failed to flush writer: {}", e);
                return;
            }
        }
    });

    // Release every still-open fid's open-handle count on session teardown.
    struct HandleReleaseGuard(Option<Arc<NinePHandler>>);
    impl HandleReleaseGuard {
        fn disarm(&mut self) -> Option<Arc<NinePHandler>> {
            self.0.take()
        }
    }
    impl Drop for HandleReleaseGuard {
        fn drop(&mut self) {
            if let Some(handler) = self.0.take() {
                // Backstop for an abnormal exit (the normal path disarms us and
                // does this itself). Just a synchronous table clear — the slot
                // guards do the rest on drop.
                handler.close_all_open_handles();
            }
        }
    }
    let mut release_guard = HandleReleaseGuard(Some(Arc::clone(&handler)));

    let result = handle_client_loop(handler, read_stream, tx, shutdown).await;

    if let Some(handler) = release_guard.disarm() {
        handler.close_all_open_handles();
    }

    lock_manager.release_session_locks(handler_id);

    let _ = writer_task.await;

    result
}

/// Tracks in-flight requests by tag. The `Notify` is allocated lazily when a
/// Tflush actually arrives for the tag — most requests are never flushed, so the
/// common path stores just `None` and avoids a per-request heap allocation.
pub(crate) type InflightRequests = Arc<DashMap<u16, Option<Arc<tokio::sync::Notify>>>>;

/// Tracks the latest Tflush tag for each oldtag. Only the last flush gets a response.
/// Per 9P spec: "should a server receive a request and then multiple flushes for that
/// request, it need respond only to the last flush."
pub(crate) type PendingFlushes = Arc<DashMap<u16, u16>>;

/// Dispatch a single 9P frame buffer. Shared between TCP (via LengthDelimitedCodec)
/// and WebSocket transports.
///
/// The reader task only does the cheap header extraction and inflight bookkeeping; the deku parse is
/// performed inside the spawned per-request task so the single per-connection
/// reader doesn't bottleneck on it.
pub(crate) fn dispatch_9p_frame(
    frame: Bytes,
    handler: &Arc<NinePHandler>,
    tx: &mpsc::Sender<(u16, Vec<u8>)>,
    inflight: &InflightRequests,
    pending_flushes: &PendingFlushes,
) -> anyhow::Result<()> {
    if frame.len() < P9_MIN_MESSAGE_SIZE as usize {
        error!("Message too short: {} bytes", frame.len());
        return Err(anyhow::anyhow!("Message too short"));
    }

    let type_byte = frame[4];
    let tag = u16::from_le_bytes([frame[5], frame[6]]);

    // Tflush's body is just a u16 oldtag at offset P9_HEADER_SIZE. Peel it
    // here so the reader can register pending_flushes before yielding,
    // otherwise a later flush for the same oldtag could be dropped if it
    // arrives before this frame's spawned task runs.
    let flush_oldtag = if type_byte == TFLUSH_TYPE {
        if frame.len() < P9_HEADER_SIZE + 2 {
            error!("Tflush message too short: {} bytes", frame.len());
            return Err(anyhow::anyhow!("Tflush message too short"));
        }
        let oldtag = u16::from_le_bytes([frame[P9_HEADER_SIZE], frame[P9_HEADER_SIZE + 1]]);
        pending_flushes.insert(oldtag, tag);
        Some(oldtag)
    } else {
        // Track the in-flight tag without allocating a Notify. A Tflush
        // handler will lazily upgrade the entry to Some(Notify) if one
        // ever arrives for this tag.
        inflight.insert(tag, None);
        None
    };

    let handler = Arc::clone(handler);
    let tx = tx.clone();
    let inflight = Arc::clone(inflight);
    let pending_flushes = Arc::clone(pending_flushes);

    spawn_named("9p-request", async move {
        // Parse off the reader task. On parse failure we still need to honour
        // the inflight/flush bookkeeping installed above, so we synthesise an
        // Rlerror response rather than bailing early. Decode with the negotiated
        // framing: under `.zerofs3` a request carries a frame op-id, threaded into
        // the op for dedup.
        let response_bytes = match P9Message::from_bytes_ctx(&frame, handler.op_id_enabled()) {
            Ok(parsed) => {
                debug!(
                    "Received message type {} tag {}: {:?}",
                    parsed.type_, parsed.tag, parsed.body
                );
                Box::pin(handler.handle_message_with_op_id(tag, parsed.op_id, parsed.body))
                    .await
                    .to_bytes()
                    .ok()
            }
            Err(e) => {
                debug!(
                    "Failed to parse message type {} (0x{:02x}) tag {}: {:?}",
                    type_byte, type_byte, tag, e
                );
                debug!(
                    "Message size: {}, buffer (first {} bytes): {:?}",
                    frame.len(),
                    P9_DEBUG_BUFFER_SIZE,
                    &frame[0..std::cmp::min(P9_DEBUG_BUFFER_SIZE, frame.len())]
                );
                P9Message::new(
                    tag,
                    Message::Rlerror(Rlerror {
                        ecode: P9Error::NotImplemented.to_errno(),
                    }),
                )
                .to_bytes()
                .ok()
            }
        };

        if let Some(oldtag) = flush_oldtag {
            // Lazily install a Notify in the target's inflight entry.
            // If the entry is gone, the target already completed.
            let target_notify = inflight.get_mut(&oldtag).map(|mut entry| {
                let slot = entry.value_mut();
                if let Some(n) = slot.as_ref() {
                    Arc::clone(n)
                } else {
                    let n = Arc::new(tokio::sync::Notify::new());
                    *slot = Some(Arc::clone(&n));
                    n
                }
            });

            if let Some(notify) = target_notify {
                debug!("Tflush: waiting for oldtag {} to complete", oldtag);
                let permit = notify.notified();
                tokio::pin!(permit);
                // Arm the waiter before any further checks so that a
                // concurrent notify_waiters from the target cannot be
                // lost; if it already fired, enable() returns Ready.
                permit.as_mut().enable();
                permit.await;
                debug!("Tflush: oldtag {} completed", oldtag);
            }

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

            pending_flushes.remove(&oldtag);
        }

        match response_bytes {
            Some(response_bytes) => {
                if let Err(e) = tx.send((tag, response_bytes)).await {
                    warn!("Failed to send response for tag {}: {}", tag, e);
                }
            }
            None => {
                error!("Failed to serialize response for tag {}", tag);
            }
        }

        if flush_oldtag.is_none()
            && let Some((_, Some(notify))) = inflight.remove(&tag)
        {
            notify.notify_waiters();
        }
    });
    Ok(())
}

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
                    Some(Ok(buf)) => buf.freeze(),
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

        dispatch_9p_frame(full_buf, &handler, &tx, &inflight, &pending_flushes)?;
    }
}
