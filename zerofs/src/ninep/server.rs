use super::errors::P9Error;
pub(crate) use super::handler::NinePHandler;
use super::handler::SessionReleaseGuard;
pub(crate) use super::lock_manager::FileLockManager;
use crate::fs::ZeroFS;
use crate::task::spawn_named;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use ninep_proto::{
    Message, P9_CHANNEL_SIZE, P9_DEBUG_BUFFER_SIZE, P9_HEADER_SIZE, P9_MAX_MSIZE,
    P9_MIN_MESSAGE_SIZE, P9_OP_ENVELOPE_LEN, P9_OP_ID_LEN, P9_SIZE_FIELD_LEN, P9Message, Rlerror,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, warn};

/// 9P message type byte for Tflush. Kept here so the reader can recognise a
/// flush from the raw frame header without deku-parsing the whole body.
const TFLUSH_TYPE: u8 = 108;
/// `Rlerror` is permitted after serving-authority loss.
const RLERROR_TYPE: u8 = 7;
/// `Rversion` is permitted after serving-authority loss.
const RVERSION_TYPE: u8 = 101;
/// `Tversion` is a receive-order barrier for the entire session.
const TVERSION_TYPE: u8 = 100;
/// 9P type for `Tclunk`, a receive-order barrier for its fid.
const TCLUNK_TYPE: u8 = 120;
const RESPONSE_BUFFER_CAPACITY: usize = 64 * 1024;
/// Bounded response drain after connection retirement.
const CLIENT_DRAIN_TIMEOUT: std::time::Duration = crate::replication::RESPONSE_DRAIN_TIMEOUT;
/// TCP keepalive idle interval.
const TCP_KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(10);
const TCP_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const TCP_KEEPALIVE_RETRIES: u32 = 4;
/// TCP timeout for unacknowledged data.
#[cfg(any(
    target_os = "android",
    target_os = "cygwin",
    target_os = "fuchsia",
    target_os = "linux"
))]
const TCP_USER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Whether a serialized response requires serving authority.
/// Standard and private 9P frames store the type byte at offset four.
pub(crate) fn response_requires_serving_authority(response_bytes: &[u8]) -> bool {
    !matches!(
        response_bytes.get(4),
        Some(&RLERROR_TYPE) | Some(&RVERSION_TYPE)
    )
}

/// Check serving authority at the final response boundary.
pub(crate) fn response_may_be_emitted(db: &crate::db::Db, response_bytes: &[u8]) -> bool {
    !response_requires_serving_authority(response_bytes) || db.permits_successful_response()
}

fn configure_accepted_tcp_stream(stream: &tokio::net::TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;

    let socket = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_IDLE)
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);
    socket.set_tcp_keepalive(&keepalive)?;

    #[cfg(any(
        target_os = "android",
        target_os = "cygwin",
        target_os = "fuchsia",
        target_os = "linux"
    ))]
    socket.set_tcp_user_timeout(Some(TCP_USER_TIMEOUT))?;

    Ok(())
}

/// Await a task until `deadline`, then abort and join it.
async fn join_with_timeout<T>(
    mut task: AbortOnDropHandle<T>,
    timeout: std::time::Duration,
) -> Option<Result<T, tokio::task::JoinError>> {
    match tokio::time::timeout(timeout, &mut task).await {
        Ok(result) => Some(result),
        Err(_) => {
            task.abort();
            let _ = task.await;
            None
        }
    }
}

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
    ) -> AbortOnDropHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let filesystem = Arc::clone(&self.filesystem);
        let lock_manager = Arc::clone(&self.lock_manager);
        let client_shutdown = shutdown.child_token();

        AbortOnDropHandle::new(spawn_named("9p-client", async move {
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
        }))
    }

    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        let mut clients = FuturesUnordered::new();
        let clients_shutdown = shutdown.child_token();
        let serve_result = match &self.transport {
            Transport::Tcp(addr) => {
                let listener = TcpListener::bind(addr)
                    .await
                    .map_err(|e| crate::net_util::tcp_bind_error("9P", addr, &e))?;
                info!("9P server listening on TCP {}", addr);

                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            info!("9P TCP server shutting down on {}", addr);
                            break Ok(());
                        }
                        finished = clients.next(), if !clients.is_empty() => {
                            if let Some(Err(e)) = finished {
                                warn!("9P client task failed: {e}");
                            }
                        }
                        result = listener.accept() => {
                            let (stream, peer_addr) = match result {
                                Ok(accepted) => accepted,
                                Err(e) => break Err(e),
                            };
                            info!("9P client connected from {}", peer_addr);
                            if let Err(e) = configure_accepted_tcp_stream(&stream) {
                                warn!("Failed to configure 9P TCP client {peer_addr}: {e}");
                                continue;
                            }
                            let (read_half, write_half) = stream.into_split();
                            clients.push(
                                self.spawn_client_handler(
                                    read_half,
                                    write_half,
                                    &clients_shutdown,
                                    peer_addr.to_string(),
                                ),
                            );
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
                        biased;
                        _ = shutdown.cancelled() => {
                            info!("9P Unix socket server shutting down at {:?}", path);
                            break Ok(());
                        }
                        finished = clients.next(), if !clients.is_empty() => {
                            if let Some(Err(e)) = finished {
                                warn!("9P client task failed: {e}");
                            }
                        }
                        result = listener.accept() => {
                            let (stream, _) = match result {
                                Ok(accepted) => accepted,
                                Err(e) => break Err(e),
                            };
                            info!("9P client connected via Unix socket");
                            let (read_half, write_half) = stream.into_split();
                            clients.push(
                                self.spawn_client_handler(
                                    read_half,
                                    write_half,
                                    &clients_shutdown,
                                    "unix".to_string(),
                                ),
                            );
                        }
                    }
                }
            }
        };

        // Listener exit drains existing connection tasks.
        clients_shutdown.cancel();

        // Each connection bounds and joins its writer before returning.
        while let Some(result) = clients.next().await {
            if let Err(e) = result {
                warn!("9P client task failed while draining: {e}");
            }
        }

        serve_result
    }
}

#[derive(Clone)]
enum ResponseAuthority {
    Database(Arc<crate::db::Db>),
    #[cfg(test)]
    Always,
}

impl ResponseAuthority {
    fn from_database(db: Arc<crate::db::Db>) -> Self {
        Self::Database(db)
    }

    #[cfg(test)]
    fn always() -> Self {
        Self::Always
    }

    fn permits_successful_response(&self) -> bool {
        match self {
            Self::Database(db) => db.permits_successful_response(),
            #[cfg(test)]
            Self::Always => true,
        }
    }

    fn may_emit(&self, response_bytes: &[u8]) -> bool {
        match self {
            Self::Database(db) => response_may_be_emitted(db, response_bytes),
            #[cfg(test)]
            Self::Always => true,
        }
    }
}

fn spawn_response_writer<W>(
    write_stream: W,
    mut rx: mpsc::Receiver<(u16, Vec<u8>)>,
    authority: ResponseAuthority,
    connection_shutdown: CancellationToken,
) -> AbortOnDropHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    AbortOnDropHandle::new(spawn_named("9p-writer", async move {
        // Stop request dispatch when the response writer exits.
        let _cancel_connection_on_exit = connection_shutdown.drop_guard();
        let mut writer =
            tokio::io::BufWriter::with_capacity(RESPONSE_BUFFER_CAPACITY, write_stream);
        loop {
            let first = match rx.recv().await {
                Some(msg) => msg,
                None => break,
            };

            let mut batch = vec![first];
            while let Ok(more) = rx.try_recv() {
                batch.push(more);
            }

            let mut buffered_authority_gated_success = false;
            let mut dropped_authority_gated_success = false;
            for (tag, response_bytes) in batch {
                let requires_authority = response_requires_serving_authority(&response_bytes);
                if requires_authority && !authority.may_emit(&response_bytes) {
                    warn!(
                        "Dropping successful 9P response for tag {tag} after serving authority \
                         was lost; closing the connection"
                    );
                    dropped_authority_gated_success = true;
                    continue;
                }
                // `write_all` may flush buffered frames; recheck authority first.
                if buffered_authority_gated_success && !authority.permits_successful_response() {
                    warn!(
                        "Dropping buffered successful 9P responses before writing response for \
                         tag {tag} after serving authority was lost; closing the connection"
                    );
                    return;
                }
                buffered_authority_gated_success |= requires_authority;
                if let Err(e) = writer.write_all(&response_bytes).await {
                    error!("Failed to write response for tag {}: {}", tag, e);
                    return;
                }
            }

            // Recheck authority before the final buffer flush.
            if buffered_authority_gated_success && !authority.permits_successful_response() {
                warn!(
                    "Dropping buffered successful 9P responses after serving authority was \
                     lost; closing the connection"
                );
                return;
            }
            if let Err(e) = writer.flush().await {
                error!("Failed to flush writer: {}", e);
                return;
            }
            if dropped_authority_gated_success {
                return;
            }
        }
    }))
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
    let handler = Arc::new(NinePHandler::new(Arc::clone(&filesystem), lock_manager));

    let (tx, rx) = mpsc::channel::<(u16, Vec<u8>)>(P9_CHANNEL_SIZE);
    let connection_shutdown = shutdown.child_token();
    let writer_task = spawn_response_writer(
        write_stream,
        rx,
        ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
        connection_shutdown.clone(),
    );

    // Retire resource guards and locks before draining received requests.
    let mut release_guard = SessionReleaseGuard::new(Arc::clone(&handler));

    let result = handle_client_loop(handler, read_stream, tx, connection_shutdown).await;
    release_guard.release();

    match join_with_timeout(writer_task, CLIENT_DRAIN_TIMEOUT).await {
        Some(Ok(())) => {}
        Some(Err(e)) => warn!("9P writer task failed: {e}"),
        None => warn!("timed out draining 9P client responses; writer aborted"),
    }

    result
}

/// Fid footprint used by receive-order barriers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FidFootprint {
    #[default]
    None,
    One(u32),
    Two(u32, u32),
    All,
}

impl FidFootprint {
    fn contains(self, fid: u32) -> bool {
        match self {
            Self::None => false,
            Self::One(first) => first == fid,
            Self::Two(first, second) => first == fid || second == fid,
            Self::All => true,
        }
    }
}

/// Request completion signal and fid footprint.
struct RequestState {
    completed: CancellationToken,
    fids: FidFootprint,
}

impl RequestState {
    fn new(fids: FidFootprint) -> Self {
        Self {
            completed: CancellationToken::new(),
            fids,
        }
    }

    fn complete(&self) {
        self.completed.cancel();
    }

    async fn wait(&self) {
        self.completed.cancelled().await;
    }
}

type CompletionWaiter = Arc<RequestState>;

/// Per-connection request registry.
#[derive(Default)]
struct InflightRegistryInner {
    /// One active request per wire tag, as required by 9P.
    entries: DashMap<u16, Arc<RequestState>>,
    /// Tail request of the receive-ordered Tflush chain for each oldtag.
    flush_tails: DashMap<u16, Arc<RequestState>>,
}

#[derive(Clone, Default)]
pub(crate) struct InflightRegistry {
    inner: Arc<InflightRegistryInner>,
}

impl InflightRegistry {
    fn register(&self, tag: u16, fids: FidFootprint) -> anyhow::Result<RequestLease> {
        let Entry::Vacant(entry) = self.inner.entries.entry(tag) else {
            anyhow::bail!("client reused in-flight 9P tag {tag}");
        };
        let state = Arc::new(RequestState::new(fids));
        entry.insert(Arc::clone(&state));
        Ok(RequestLease {
            registry: self.clone(),
            tag,
            state,
            flush_oldtag: None,
        })
    }

    fn waiter(&self, tag: u16) -> Option<CompletionWaiter> {
        self.inner
            .entries
            .get(&tag)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Register a whole-session barrier after snapshotting earlier requests.
    fn register_after_prior_snapshot(
        &self,
        tag: u16,
    ) -> anyhow::Result<(RequestLease, Vec<CompletionWaiter>)> {
        let waiters = self
            .inner
            .entries
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        Ok((self.register(tag, FidFootprint::All)?, waiters))
    }

    /// Register a fid barrier after snapshotting earlier requests for that fid.
    fn register_after_fid_snapshot(
        &self,
        tag: u16,
        fid: u32,
    ) -> anyhow::Result<(RequestLease, Vec<CompletionWaiter>)> {
        let waiters = self
            .inner
            .entries
            .iter()
            .filter(|entry| entry.value().fids.contains(fid))
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        Ok((self.register(tag, FidFootprint::One(fid))?, waiters))
    }

    /// Append `Tflush` to the target tag's response-order chain.
    fn register_flush(&self, tag: u16, oldtag: u16) -> anyhow::Result<FlushRegistration> {
        let target = self.waiter(oldtag);
        let mut lease = self.register(tag, FidFootprint::None)?;
        let predecessor = self
            .inner
            .flush_tails
            .insert(oldtag, Arc::clone(&lease.state));
        lease.flush_oldtag = Some(oldtag);
        Ok(FlushRegistration {
            lease,
            target,
            predecessor,
        })
    }
}

struct FlushRegistration {
    lease: RequestLease,
    target: Option<CompletionWaiter>,
    predecessor: Option<CompletionWaiter>,
}

/// Non-cloneable authority to retire one request.
#[must_use = "dropping the lease completes and retires its request"]
struct RequestLease {
    registry: InflightRegistry,
    tag: u16,
    state: Arc<RequestState>,
    flush_oldtag: Option<u16>,
}

impl RequestLease {
    /// Retire this tag without notifying request-order waiters.
    /// Queue capacity must already be reserved so tag reuse cannot race removal.
    fn retire_tag_for_response(&self) {
        self.registry
            .inner
            .entries
            .remove_if(&self.tag, |_, current| Arc::ptr_eq(current, &self.state));
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.retire_tag_for_response();
        if let Some(oldtag) = self.flush_oldtag {
            self.registry
                .inner
                .flush_tails
                .remove_if(&oldtag, |_, current| Arc::ptr_eq(current, &self.state));
        }
        self.state.complete();
    }
}

/// Reserve response capacity, retire the tag, then enqueue the terminal response.
/// Tag retirement and enqueue have no intervening yield.
async fn enqueue_terminal_response(
    tx: &mpsc::Sender<(u16, Vec<u8>)>,
    tag: u16,
    response_bytes: Vec<u8>,
    request_lease: &RequestLease,
) -> Result<(), mpsc::error::SendError<()>> {
    let permit = tx.reserve().await?;
    request_lease.retire_tag_for_response();
    permit.send((tag, response_bytes));
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ShallowFrameMetadata {
    fids: FidFootprint,
    received_op: Option<([u8; P9_OP_ID_LEN], u8)>,
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn fixed_one_fid(body: &[u8]) -> FidFootprint {
    read_u32_at(body, 0).map_or(FidFootprint::None, FidFootprint::One)
}

fn fixed_two_fids(body: &[u8]) -> FidFootprint {
    match (read_u32_at(body, 0), read_u32_at(body, 4)) {
        (Some(first), Some(second)) => FidFootprint::Two(first, second),
        _ => FidFootprint::None,
    }
}

/// Decode the fid-bearing prefix needed for receive-order barriers.
fn request_fid_footprint(type_byte: u8, body: &[u8]) -> FidFootprint {
    match type_byte {
        TVERSION_TYPE => FidFootprint::All,

        20 | 30 | 70 | 104 | 110 | 230 | 236 | 238 | 246 | 252 => fixed_two_fids(body),

        // `Trenameat.newdirfid` follows the variable-length old name.
        74 => {
            let Some(old_dir) = read_u32_at(body, 0) else {
                return FidFootprint::None;
            };
            let Some(name_len) = body
                .get(4..6)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u16::from_le_bytes)
            else {
                return FidFootprint::None;
            };
            let new_dir_offset = 6usize.saturating_add(usize::from(name_len));
            read_u32_at(body, new_dir_offset).map_or(FidFootprint::None, |new_dir| {
                FidFootprint::Two(old_dir, new_dir)
            })
        }

        8 | 12 | 14 | 16 | 18 | 22 | 24 | 26 | 40 | 50 | 52 | 54 | 72 | 76 | 116 | 118
        | TCLUNK_TYPE | 228 | 232 | 240 | 242 | 244 | 248 | 250 | 254 => fixed_one_fid(body),
        _ => FidFootprint::None,
    }
}

/// Inspect framing, mutation envelope, and fid prefix without copying the body.
fn inspect_frame_metadata(frame: &[u8], zerofs_protocol: bool) -> ShallowFrameMetadata {
    let type_byte = frame[4];
    let carries_op = zerofs_protocol && P9Message::carries_op_id(type_byte);
    let body_offset = P9_HEADER_SIZE + usize::from(carries_op) * P9_OP_ENVELOPE_LEN;

    let received_op = if carries_op && frame.len() >= body_offset {
        let mut op_id = [0; P9_OP_ID_LEN];
        op_id.copy_from_slice(&frame[P9_HEADER_SIZE..P9_HEADER_SIZE + P9_OP_ID_LEN]);
        Some((op_id, frame[P9_HEADER_SIZE + P9_OP_ID_LEN]))
    } else {
        None
    };

    ShallowFrameMetadata {
        fids: frame.get(body_offset..).map_or(FidFootprint::None, |body| {
            request_fid_footprint(type_byte, body)
        }),
        received_op,
    }
}

/// Dispatch a single 9P frame buffer. Shared between TCP (via LengthDelimitedCodec)
/// and WebSocket transports.
///
/// Reserve receive-time dedup state before task detachment. Full decoding runs
/// in the detached task.
pub(crate) fn dispatch_9p_frame(
    frame: Bytes,
    handler: &Arc<NinePHandler>,
    tx: &mpsc::Sender<(u16, Vec<u8>)>,
    inflight: &InflightRegistry,
) -> anyhow::Result<()> {
    if frame.len() < P9_MIN_MESSAGE_SIZE as usize {
        error!("Message too short: {} bytes", frame.len());
        return Err(anyhow::anyhow!("Message too short"));
    }

    let type_byte = frame[4];
    let tag = u16::from_le_bytes([frame[5], frame[6]]);
    let zerofs_protocol = handler.zerofs_protocol_enabled();
    let metadata = inspect_frame_metadata(&frame, zerofs_protocol);

    // Capture `oldtag` before any yield or tag reuse.
    let (flush_oldtag, flush_waiters, request_lease, prior_waiters) = if type_byte == TFLUSH_TYPE {
        if frame.len() < P9_HEADER_SIZE + 2 {
            error!("Tflush message too short: {} bytes", frame.len());
            return Err(anyhow::anyhow!("Tflush message too short"));
        }
        let oldtag = u16::from_le_bytes([frame[P9_HEADER_SIZE], frame[P9_HEADER_SIZE + 1]]);
        let FlushRegistration {
            lease,
            target,
            predecessor,
        } = inflight.register_flush(tag, oldtag)?;
        let waiters = target.into_iter().chain(predecessor).collect();
        (Some(oldtag), waiters, lease, Vec::new())
    } else {
        // `Tclunk` waits on its fid; `Tversion` waits on the whole session.
        let (request_lease, prior_waiters) = match (type_byte, metadata.fids) {
            (TVERSION_TYPE, _) => inflight.register_after_prior_snapshot(tag)?,
            (TCLUNK_TYPE, FidFootprint::One(fid)) => {
                inflight.register_after_fid_snapshot(tag, fid)?
            }
            _ => (inflight.register(tag, metadata.fids)?, Vec::new()),
        };
        (None, Vec::new(), request_lease, prior_waiters)
    };

    // Reserve a FIRST envelope before EOF can admit a replacement RETRY.
    let received_guard = metadata.received_op.and_then(|(op_id, op_flags)| {
        handler.try_reserve_received_first(op_id, op_flags, type_byte)
    });

    let handler = Arc::clone(handler);
    let tx = tx.clone();

    spawn_named("9p-request", async move {
        // Decode large payloads after detachment from the connection reader.
        let parsed = P9Message::from_bytes_ctx(&frame, zerofs_protocol);

        // Barrier waiters were captured synchronously in receive order.
        for waiter in prior_waiters {
            waiter.wait().await;
        }

        // Parse failures stay registered until their `Rlerror` is enqueued.
        let response_bytes = match parsed {
            Ok(parsed) => {
                debug!(
                    "Received message type {} tag {}: {:?}",
                    parsed.type_, parsed.tag, parsed.body
                );
                let response = if let Some(received_guard) = received_guard {
                    Box::pin(handler.handle_message_with_received_admission(
                        tag,
                        parsed.op_id,
                        parsed.op_flags,
                        parsed.op_origin_epoch,
                        parsed.body,
                        Some(received_guard),
                    ))
                    .await
                } else {
                    Box::pin(handler.handle_message_with_op_envelope_origin(
                        tag,
                        parsed.op_id,
                        parsed.op_flags,
                        parsed.op_origin_epoch,
                        parsed.body,
                    ))
                    .await
                };
                response.to_bytes().ok()
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
            debug!("Tflush: waiting for oldtag {} to complete", oldtag);
            for waiter in flush_waiters {
                waiter.wait().await;
            }
            debug!("Tflush: oldtag {} completed", oldtag);
        }

        match response_bytes {
            Some(response_bytes) => {
                if let Err(e) =
                    enqueue_terminal_response(&tx, tag, response_bytes, &request_lease).await
                {
                    warn!("Failed to send response for tag {}: {}", tag, e);
                }
            }
            None => {
                error!("Failed to serialize response for tag {}", tag);
            }
        }

        drop(request_lease);
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
    let inflight = InflightRegistry::default();

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
            biased;
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

        dispatch_9p_frame(full_buf, &handler, &tx, &inflight)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::permissions::Credentials;
    use crate::ninep::lock_manager::FileLock;
    use ninep_proto::{
        DekuBytes, GETATTR_ALL, LockType, P9String, Rclunk, Rflush, Rlopenat, Rread, Tattach,
        Tclunk, Tflush, Tgetattr, Tlopenat, Tmkdir, Tversion, Twrite, VERSION_9P2000L_ZEROFS,
    };
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);
    const QUIET_TIMEOUT: Duration = Duration::from_millis(20);

    fn frame(tag: u16, body: Message) -> Bytes {
        Bytes::from(P9Message::new(tag, body).to_bytes().unwrap())
    }

    fn decode(bytes: &[u8]) -> P9Message {
        P9Message::from_bytes_ctx(bytes, false).unwrap()
    }

    async fn in_memory_handler() -> (Arc<ZeroFS>, Arc<NinePHandler>) {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        (filesystem, handler)
    }

    async fn negotiate(handler: &NinePHandler) {
        let response = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: super::super::handler::DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            )
            .await;
        assert!(matches!(response.body, Message::Rversion(_)));
    }

    async fn establish_session(
        handler: &NinePHandler,
        attach_tag: u16,
        user: &[u8],
        uid: u32,
        op_id: Option<[u8; 16]>,
    ) {
        negotiate(handler).await;
        let request = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(user.to_vec()),
            aname: P9String::new(b"/".to_vec()),
            n_uname: uid,
        });
        let response = match op_id {
            Some(op_id) => {
                handler
                    .handle_message_with_op_id(attach_tag, op_id, request)
                    .await
            }
            None => handler.handle_message(attach_tag, request).await,
        };
        assert!(matches!(response.body, Message::Rattach(_)));
    }

    struct DispatchFixture {
        handler: Arc<NinePHandler>,
        tx: mpsc::Sender<(u16, Vec<u8>)>,
        rx: mpsc::Receiver<(u16, Vec<u8>)>,
        inflight: InflightRegistry,
    }

    impl DispatchFixture {
        fn new(handler: Arc<NinePHandler>, capacity: usize) -> Self {
            let (tx, rx) = mpsc::channel(capacity);
            Self {
                handler,
                tx,
                rx,
                inflight: InflightRegistry::default(),
            }
        }

        async fn in_memory(capacity: usize) -> Self {
            let (_, handler) = in_memory_handler().await;
            Self::new(handler, capacity)
        }

        fn dispatch(&self, tag: u16, body: Message) {
            self.dispatch_frame(frame(tag, body));
        }

        fn dispatch_frame(&self, bytes: Bytes) {
            dispatch_9p_frame(bytes, &self.handler, &self.tx, &self.inflight).unwrap();
        }

        async fn recv(&mut self, context: &str) -> (u16, P9Message) {
            let (tag, bytes) = tokio::time::timeout(TEST_TIMEOUT, self.rx.recv())
                .await
                .expect(context)
                .expect("response channel");
            (tag, decode(&bytes))
        }

        async fn recv_flush(&mut self, context: &str) -> u16 {
            let (tag, response) = self.recv(context).await;
            assert!(matches!(response.body, Message::Rflush(Rflush)));
            tag
        }

        async fn expect_quiet(&mut self, message: &str) {
            assert!(
                tokio::time::timeout(QUIET_TIMEOUT, self.rx.recv())
                    .await
                    .is_err(),
                "{message}"
            );
        }
    }

    async fn expect_completion(waiter: CompletionWaiter, message: &str) {
        tokio::time::timeout(TEST_TIMEOUT, waiter.wait())
            .await
            .expect(message);
    }

    async fn expect_pending(waiter: &CompletionWaiter, message: &str) {
        let probe = Arc::clone(waiter);
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, probe.wait())
                .await
                .is_err(),
            "{message}"
        );
    }

    async fn drain_writer(writer: AbortOnDropHandle<()>, message: &str) {
        join_with_timeout(writer, TEST_TIMEOUT)
            .await
            .expect(message)
            .expect("writer task");
    }

    fn response_queue<const N: usize>(
        responses: [(u16, Vec<u8>); N],
    ) -> mpsc::Receiver<(u16, Vec<u8>)> {
        let (tx, rx) = mpsc::channel(N);
        for response in responses {
            tx.try_send(response).unwrap();
        }
        rx
    }

    fn not_leader(tag: u16) -> Vec<u8> {
        frame(
            tag,
            Message::Rlerror(Rlerror {
                ecode: ninep_proto::P9_ENOTLEADER,
            }),
        )
        .to_vec()
    }

    async fn race_lopenat_with(
        followup_tag: u16,
        followup: Message,
        quiet_message: &str,
        response_context: &str,
    ) -> (Arc<ZeroFS>, DispatchFixture, P9Message, P9Message) {
        let (filesystem, handler) = in_memory_handler().await;
        establish_session(&handler, 0, b"test", 1000, Some([0; 16])).await;

        // Pause before `Tlopenat` installs `newfid`.
        let inode_lock = filesystem.lock_manager.acquire(0).await;
        let mut io = DispatchFixture::new(handler, 4);
        io.dispatch(
            10,
            Message::Tlopenat(Tlopenat {
                fid: 1,
                newfid: 2,
                flags: libc::O_RDONLY as u32,
            }),
        );
        io.dispatch(followup_tag, followup);
        io.expect_quiet(quiet_message).await;
        drop(inode_lock);

        let (_, open) = io.recv("open response").await;
        let (_, followup) = io.recv(response_context).await;
        (filesystem, io, open, followup)
    }

    async fn leased_filesystem_for_response_gate() -> (
        Arc<ZeroFS>,
        Arc<crate::replication::Lease>,
        Arc<slatedb::Db>,
    ) {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let raw_db = Arc::new(
            slatedb::DbBuilder::new(
                slatedb::object_store::path::Path::from("ninep-response-authority"),
                object_store,
            )
            .build()
            .await
            .unwrap(),
        );
        let lease = crate::replication::Lease::new();
        assert!(lease.activate_from(tokio::time::Instant::now(), Duration::from_secs(30)));

        // `Tflush` does not access the database; the response gate is sufficient.
        let mut filesystem = ZeroFS::new_in_memory().await.unwrap();
        filesystem.db =
            Arc::new(crate::db::Db::new(Arc::clone(&raw_db), None).with_lease(Arc::clone(&lease)));
        (Arc::new(filesystem), lease, raw_db)
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    struct RevokeOnFirstWrite<W> {
        inner: W,
        lease: Arc<crate::replication::Lease>,
        revoked: bool,
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for RevokeOnFirstWrite<W> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if !self.revoked {
                self.lease.revoke();
                self.revoked = true;
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn accepted_tcp_stream_has_bounded_liveness_options() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let (client, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
        let _client = client.expect("connect loopback client");
        let (server, _) = accepted.expect("accept loopback client");

        configure_accepted_tcp_stream(&server).expect("configure accepted TCP stream");

        let socket = socket2::SockRef::from(&server);
        assert!(server.nodelay().expect("TCP_NODELAY"));
        assert!(socket.keepalive().expect("SO_KEEPALIVE"));
        assert_eq!(
            socket.tcp_keepalive_time().expect("TCP_KEEPIDLE"),
            TCP_KEEPALIVE_IDLE
        );
        assert_eq!(
            socket.tcp_keepalive_interval().expect("TCP_KEEPINTVL"),
            TCP_KEEPALIVE_INTERVAL
        );
        assert_eq!(
            socket.tcp_keepalive_retries().expect("TCP_KEEPCNT"),
            TCP_KEEPALIVE_RETRIES
        );
        assert_eq!(
            socket.tcp_user_timeout().expect("TCP_USER_TIMEOUT"),
            Some(TCP_USER_TIMEOUT)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timed_join_aborts_and_awaits_the_task_before_returning() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, mut dropped_rx) = oneshot::channel();
        let task = AbortOnDropHandle::new(spawn_named("timed-abort-test", async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.expect("task started");

        assert!(
            join_with_timeout(task, TEST_TIMEOUT).await.is_none(),
            "a stuck task must take the timeout path"
        );
        dropped_rx
            .try_recv()
            .expect("the aborted task must be joined before timeout returns");
    }

    #[tokio::test]
    async fn queued_response_is_flushed_before_writer_drain_returns() {
        let filesystem = ZeroFS::new_in_memory().await.unwrap();
        let (mut client, server) = tokio::io::duplex(4096);
        let response = frame(21, Message::Rflush(Rflush)).to_vec();
        let writer = spawn_response_writer(
            server,
            response_queue([(21, response.clone())]),
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            CancellationToken::new(),
        );

        drain_writer(writer, "writer must drain before the deadline").await;
        let mut received = vec![0; response.len()];
        client
            .read_exact(&mut received)
            .await
            .expect("queued response bytes");
        assert_eq!(received, response);
        assert!(matches!(decode(&received).body, Message::Rflush(Rflush)));

        let mut trailing = [0];
        assert_eq!(
            client.read(&mut trailing).await.expect("writer EOF"),
            0,
            "a completed drain must drop the write half before returning"
        );
    }

    #[tokio::test]
    async fn revocation_filters_success_not_errors() {
        let (filesystem, lease, raw_db) = leased_filesystem_for_response_gate().await;
        let handler = NinePHandler::new(Arc::clone(&filesystem), Arc::new(FileLockManager::new()));

        // Revoke authority after response serialization and before transport write.
        let completed = handler
            .handle_message(41, Message::Tflush(Tflush { oldtag: 40 }))
            .await;
        assert!(matches!(completed.body, Message::Rflush(Rflush)));
        let completed_bytes = completed.to_bytes().unwrap();
        let (mut client, server) = tokio::io::duplex(4096);
        let rx = response_queue([(41, completed_bytes)]);
        lease.revoke();

        let connection_shutdown = CancellationToken::new();
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            connection_shutdown.clone(),
        );
        drain_writer(writer, "writer must close after dropping stale success").await;
        assert!(connection_shutdown.is_cancelled());
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert!(
            received.is_empty(),
            "a success completed before revocation must not reach the wire afterward"
        );

        // Protocol errors remain permitted after authority loss.
        let error_bytes = not_leader(42);
        let (mut client, server) = tokio::io::duplex(4096);
        let writer = spawn_response_writer(
            server,
            response_queue([(42, error_bytes.clone())]),
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            CancellationToken::new(),
        );
        drain_writer(writer, "error response writer must drain").await;
        let mut received = vec![0; error_bytes.len()];
        client.read_exact(&mut received).await.unwrap();
        assert_eq!(received, error_bytes);
        assert!(matches!(decode(&received).body, Message::Rlerror(_)));

        raw_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn revocation_blocks_implicit_success_flush() {
        let (filesystem, lease, raw_db) = leased_filesystem_for_response_gate().await;
        let first_error = not_leader(51);
        let trailing_error = not_leader(53);
        let buffered_success = frame(
            52,
            Message::Rread(Rread {
                count: 0,
                data: DekuBytes::from(vec![0; RESPONSE_BUFFER_CAPACITY - 17]),
            }),
        )
        .to_vec();
        assert_eq!(buffered_success.len(), RESPONSE_BUFFER_CAPACITY - 6);
        assert!(first_error.len() + buffered_success.len() > RESPONSE_BUFFER_CAPACITY);
        assert!(buffered_success.len() + trailing_error.len() > RESPONSE_BUFFER_CAPACITY);

        let (mut client, server) = tokio::io::duplex(RESPONSE_BUFFER_CAPACITY * 2);
        let write_stream = RevokeOnFirstWrite {
            inner: server,
            lease: Arc::clone(&lease),
            revoked: false,
        };
        let writer = spawn_response_writer(
            write_stream,
            response_queue([
                (51, first_error.clone()),
                (52, buffered_success),
                (53, trailing_error),
            ]),
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            CancellationToken::new(),
        );
        drain_writer(writer, "writer must close without flushing stale success").await;

        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert_eq!(
            received, first_error,
            "the safe error may flush, but the success buffered after revocation must not"
        );
        raw_db.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn received_first_precedes_reconnect_retry() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let old = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        let replacement =
            NinePHandler::new(Arc::clone(&filesystem), Arc::new(FileLockManager::new()));

        for handler in [&*old, &replacement] {
            establish_session(handler, 1, b"root", 0, None).await;
        }

        let op_id = [0x63; 16];
        let mkdir = Message::Tmkdir(Tmkdir {
            dfid: 1,
            name: P9String::new(b"survived-disconnect".to_vec()),
            mode: 0o755,
            gid: 0,
        });
        let first = P9Message::new_with_op_id(10, op_id, mkdir.clone())
            .to_bytes_ctx(true)
            .unwrap();
        let (tx, _rx) = mpsc::channel(2);
        let inflight = InflightRegistry::default();

        // The current-thread runtime leaves the received FIRST task unpolled.
        dispatch_9p_frame(Bytes::from(first), &old, &tx, &inflight).unwrap();
        old.close_for_disconnect();

        // RETRY must wait for the received FIRST and replay its mkdir result.
        let retried = tokio::time::timeout(
            TEST_TIMEOUT,
            replacement.handle_message_with_op_envelope_origin(
                11,
                op_id,
                ninep_proto::P9_OP_FLAG_RETRY,
                0,
                mkdir,
            ),
        )
        .await
        .expect("replacement retry must follow the queued FIRST");
        assert!(
            matches!(retried.body, Message::Rmkdir(_)),
            "the retry must replay success, got {:?}",
            retried.body
        );

        let cached_inode = match filesystem.dedup.get(&op_id) {
            Some(crate::dedup::DedupResult::Mkdir { inode_id, .. }) => inode_id,
            other => panic!("queued FIRST must publish Mkdir, not a fid error: {other:?}"),
        };
        let creds = Credentials {
            uid: 0,
            gid: 0,
            groups: [0; 16],
            groups_count: 0,
        };
        assert_eq!(
            filesystem
                .lookup(&creds, 0, b"survived-disconnect")
                .await
                .unwrap(),
            cached_inode
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_response_sender_times_out_and_closes_writer() {
        let (mut client, server) = tokio::io::duplex(64);
        let (stuck_tx, rx) = mpsc::channel(1);
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::always(),
            CancellationToken::new(),
        );

        assert!(
            join_with_timeout(writer, CLIENT_DRAIN_TIMEOUT)
                .await
                .is_none(),
            "a live sender with no response must hit the bounded abort path"
        );
        let mut byte = [0];
        assert_eq!(
            client.read(&mut byte).await.expect("writer EOF"),
            0,
            "the aborted writer must be dropped before timeout returns"
        );
        drop(stuck_tx);
    }

    #[tokio::test]
    async fn writer_failure_cancels_further_request_dispatch() {
        let (client, server) = tokio::io::duplex(64);
        drop(client);
        let (tx, rx) = mpsc::channel(1);
        let connection_shutdown = CancellationToken::new();
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::always(),
            connection_shutdown.clone(),
        );

        tx.send((21, vec![1, 2, 3]))
            .await
            .expect("response enqueue");
        tokio::time::timeout(TEST_TIMEOUT, connection_shutdown.cancelled())
            .await
            .expect("writer exit must stop the connection reader");
        drain_writer(writer, "writer exited").await;
    }

    #[tokio::test]
    async fn session_release_drop_sweeps_byte_range_locks() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(filesystem, Arc::clone(&lock_manager)));
        let handler_id = handler.handler_id();
        assert!(lock_manager.try_add_lock(
            handler_id,
            FileLock {
                lock_type: LockType::WriteLock,
                start: 0,
                length: 0,
                proc_id: 1,
                client_id: b"cancelled-session".to_vec(),
                fid: 1,
                inode_id: 0,
            },
        ));
        assert!(lock_manager.session_has_locks(handler_id));

        drop(SessionReleaseGuard::new(handler));

        assert!(
            !lock_manager.session_has_locks(handler_id),
            "abnormal session teardown must release byte-range locks"
        );
    }

    #[tokio::test]
    async fn early_completion_is_observable() {
        let inflight = InflightRegistry::default();
        let lease = inflight.register(7, FidFootprint::None).unwrap();
        let waiter = inflight.waiter(7).expect("request is in flight");

        drop(lease);

        expect_completion(
            waiter,
            "the persistent completion predicate must not lose an early completion",
        )
        .await;
    }

    #[tokio::test]
    async fn occupied_tag_is_rejected_and_reusable_after_completion() {
        let inflight = InflightRegistry::default();
        let lease = inflight.register(7, FidFootprint::None).unwrap();
        let waiter = inflight.waiter(7).expect("original request");

        assert!(inflight.register(7, FidFootprint::None).is_err());
        assert!(
            Arc::ptr_eq(&waiter, &inflight.waiter(7).expect("original remains")),
            "duplicate rejection must not replace the original request"
        );
        expect_pending(
            &waiter,
            "duplicate rejection must not complete the original",
        )
        .await;

        drop(lease);
        expect_completion(waiter, "the original request must complete normally").await;

        let replacement = inflight
            .register(7, FidFootprint::None)
            .expect("completed tag is reusable");
        drop(replacement);
    }

    #[tokio::test]
    async fn backpressured_response_retires_tag_before_publication() {
        let inflight = InflightRegistry::default();
        let original = inflight.register(7, FidFootprint::None).unwrap();
        let original_waiter = inflight.waiter(7).expect("original request");
        let response = vec![1, 2, 3];
        let (tx, mut rx) = mpsc::channel(1);
        tx.send((99, vec![0])).await.unwrap();

        let mut enqueue = Box::pin(enqueue_terminal_response(
            &tx,
            7,
            response.clone(),
            &original,
        ));
        tokio::select! {
            biased;
            result = &mut enqueue => panic!("full response queue accepted a send: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        assert!(
            inflight.register(7, FidFootprint::None).is_err(),
            "queue backpressure must keep the tag occupied until capacity is reserved"
        );
        assert_eq!(rx.recv().await.unwrap(), (99, vec![0]));

        tokio::time::timeout(TEST_TIMEOUT, &mut enqueue)
            .await
            .expect("response enqueue")
            .unwrap();
        drop(enqueue);
        assert_eq!(rx.recv().await.unwrap(), (7, response));

        let replacement = inflight
            .register(7, FidFootprint::None)
            .expect("a tag is reusable as soon as its response is observable");
        expect_pending(
            &original_waiter,
            "response publication need not wait for the old task to reach Drop",
        )
        .await;

        drop(original);
        expect_completion(original_waiter, "the old request must still complete").await;
        assert!(
            Arc::ptr_eq(
                &replacement.state,
                &inflight.waiter(7).expect("replacement remains registered")
            ),
            "dropping the old lease must not remove the reused tag"
        );
        drop(replacement);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_wire_tag_stops_the_connection_reader() {
        let (_, handler) = in_memory_handler().await;
        let request = frame(7, Message::Tflush(Tflush { oldtag: 99 }));
        let mut input = Vec::with_capacity(request.len() * 2);
        input.extend_from_slice(&request);
        input.extend_from_slice(&request);

        let (mut client, server) = tokio::io::duplex(4096);
        client.write_all(&input).await.unwrap();
        let (tx, _rx) = mpsc::channel(2);

        let error = handle_client_loop(handler, server, tx, CancellationToken::new())
            .await
            .expect_err("duplicate tag must terminate the reader");
        assert!(error.to_string().contains("reused in-flight 9P tag 7"));

        let mut byte = [0];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn barrier_snapshot_waits_for_all_prior_tags() {
        let inflight = InflightRegistry::default();
        let first = inflight.register(7, FidFootprint::None).unwrap();
        let second = inflight.register(8, FidFootprint::None).unwrap();
        let (barrier, prior_waiters) = inflight.register_after_prior_snapshot(9).unwrap();
        assert_eq!(prior_waiters.len(), 2);

        drop(first);
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, async {
                for waiter in &prior_waiters {
                    waiter.wait().await;
                }
            })
            .await
            .is_err(),
            "the barrier must still wait for the second request"
        );

        drop(second);
        for waiter in prior_waiters {
            expect_completion(waiter, "the barrier must observe every prior request").await;
        }
        expect_pending(
            &inflight.waiter(9).expect("barrier request"),
            "prior completion must not retire the barrier itself",
        )
        .await;
        drop(barrier);
    }

    #[tokio::test]
    async fn fid_snapshot_waits_only_for_matching_prior_requests() {
        let inflight = InflightRegistry::default();
        let matching = inflight.register(7, FidFootprint::Two(1, 2)).unwrap();
        let unrelated = inflight.register(8, FidFootprint::One(3)).unwrap();
        let global = inflight.register(9, FidFootprint::All).unwrap();

        let (clunk, waiters) = inflight.register_after_fid_snapshot(10, 2).unwrap();
        assert_eq!(waiters.len(), 2, "same-fid and global requests must wait");
        let unrelated_state = inflight.waiter(8).unwrap();
        assert!(
            waiters
                .iter()
                .all(|waiter| !Arc::ptr_eq(waiter, &unrelated_state)),
            "an unrelated fid must not enter the Tclunk barrier"
        );

        drop(unrelated);
        for waiter in &waiters {
            expect_pending(waiter, "matching requests are still in flight").await;
        }
        drop(matching);
        drop(global);
        for waiter in waiters {
            expect_completion(waiter, "matching request must complete").await;
        }

        let (_, later_waiters) = inflight.register_after_fid_snapshot(11, 2).unwrap();
        assert_eq!(
            later_waiters.len(),
            1,
            "the earlier Tclunk is itself the same-fid tail"
        );
        drop(clunk);
    }

    #[test]
    fn shallow_inspection_does_not_decode_a_write_payload() {
        let op_id = [0x5a; P9_OP_ID_LEN];
        let frame = P9Message::new_with_op_id(
            7,
            op_id,
            Message::Twrite(Twrite {
                fid: 42,
                offset: 0,
                count: u32::MAX,
                data: vec![1, 2, 3, 4].into(),
            }),
        )
        .to_bytes_ctx(true)
        .unwrap();
        assert!(
            P9Message::from_bytes_ctx(&frame, true).is_err(),
            "the declared payload is truncated"
        );

        assert_eq!(
            inspect_frame_metadata(&frame, true),
            ShallowFrameMetadata {
                fids: FidFootprint::One(42),
                received_op: Some((op_id, 0)),
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_first_releases_reservation() {
        let (filesystem, handler) = in_memory_handler().await;
        negotiate(&handler).await;
        let frame = P9Message::new_with_op_id(
            7,
            [0x6b; P9_OP_ID_LEN],
            Message::Twrite(Twrite {
                fid: 1,
                offset: 0,
                count: u32::MAX,
                data: vec![1, 2, 3, 4].into(),
            }),
        )
        .to_bytes_ctx(true)
        .unwrap();

        let mut io = DispatchFixture::new(handler, 1);
        io.dispatch_frame(Bytes::from(frame));
        assert!(matches!(
            io.recv("malformed request response").await.1.body,
            Message::Rlerror(_)
        ));
        assert_eq!(
            filesystem.dedup.stats().inflight_ids,
            0,
            "parse failure must drop the synchronous FIRST reservation"
        );
    }

    #[test]
    fn rejected_flush_does_not_install_a_tail() {
        let inflight = InflightRegistry::default();
        let occupied = inflight.register(7, FidFootprint::None).unwrap();

        assert!(inflight.register_flush(7, 20).is_err());
        let valid = inflight.register_flush(8, 20).unwrap();
        assert!(valid.predecessor.is_none());

        drop(valid);
        drop(occupied);
    }

    #[tokio::test]
    async fn flush_tail_cleanup_preserves_the_newer_tail() {
        let inflight = InflightRegistry::default();
        let target_lease = inflight.register(20, FidFootprint::None).unwrap();
        let first = inflight.register_flush(21, 20).unwrap();
        let second = inflight.register_flush(22, 20).unwrap();

        drop(first.lease);
        let third = inflight.register_flush(23, 20).unwrap();
        let third_predecessor = third
            .predecessor
            .expect("the second flush must remain the chain tail");

        drop(target_lease);
        expect_pending(
            &third_predecessor,
            "retiring the old flush must not let a later flush bypass its predecessor",
        )
        .await;

        drop(second.lease);
        expect_completion(
            third_predecessor,
            "completing the actual predecessor releases the chain",
        )
        .await;
        drop(third.lease);
    }

    #[tokio::test]
    async fn flush_targets_stay_bound_across_oldtag_reuse() {
        let inflight = InflightRegistry::default();
        let old_target_lease = inflight.register(20, FidFootprint::None).unwrap();
        let first_flush = inflight.register_flush(21, 20).unwrap();
        let first_target = first_flush.target.expect("old target request");

        drop(old_target_lease);
        expect_completion(first_target, "the first flush follows the old target").await;

        let new_target_lease = inflight.register(20, FidFootprint::None).unwrap();
        let second_flush = inflight.register_flush(22, 20).unwrap();
        let second_target = second_flush.target.expect("new target request");
        let second_predecessor = second_flush
            .predecessor
            .expect("the first flush remains the response-order predecessor");

        drop(first_flush.lease);
        expect_completion(
            second_predecessor,
            "the second flush observes the first flush response",
        )
        .await;
        expect_pending(
            &second_target,
            "the old target must not release a flush of the reused tag",
        )
        .await;

        drop(new_target_lease);
        expect_completion(
            second_target,
            "the second flush follows the replacement target",
        )
        .await;
        drop(second_flush.lease);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tflush_captures_its_target_before_dispatch_returns() {
        let mut io = DispatchFixture::in_memory(2).await;
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();
        io.dispatch(21, Message::Tflush(Tflush { oldtag: 20 }));
        io.expect_quiet("Rflush must wait for the captured target generation")
            .await;
        drop(target_lease);

        io.recv_flush("flush response").await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_flushes_each_receive_a_response() {
        let mut io = DispatchFixture::in_memory(2).await;
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();

        for tag in [21, 22] {
            io.dispatch(tag, Message::Tflush(Tflush { oldtag: 20 }));
        }

        io.expect_quiet("both flushes must wait for the same target generation")
            .await;
        drop(target_lease);

        let mut response_tags = Vec::new();
        for _ in 0..2 {
            response_tags.push(io.recv_flush("flush response").await);
        }
        assert_eq!(response_tags, [21, 22]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flushing_a_flush_waits_for_its_response() {
        let mut io = DispatchFixture::in_memory(2).await;
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();
        io.dispatch(21, Message::Tflush(Tflush { oldtag: 20 }));
        io.dispatch(22, Message::Tflush(Tflush { oldtag: 21 }));
        io.expect_quiet("the second flush must observe the first flush as in flight")
            .await;
        drop(target_lease);

        for expected_tag in [21, 22] {
            assert_eq!(io.recv_flush("flush response").await, expected_tag);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_of_an_unknown_tag_responds_immediately() {
        let mut io = DispatchFixture::in_memory(1).await;
        io.dispatch(21, Message::Tflush(Tflush { oldtag: 20 }));

        assert_eq!(io.recv_flush("immediate flush response").await, 21);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zerofs_tflush_keeps_the_standard_body_offset() {
        let (_, handler) = in_memory_handler().await;
        negotiate(&handler).await;

        let frame = P9Message::new(21, Message::Tflush(Tflush { oldtag: 20 }))
            .to_bytes_ctx(true)
            .unwrap();
        assert_eq!(frame.len(), P9_HEADER_SIZE + 2);
        assert_eq!(&frame[P9_HEADER_SIZE..], 20u16.to_le_bytes());

        let mut io = DispatchFixture::new(handler, 2);
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();
        io.dispatch_frame(Bytes::from(frame));

        drop(target_lease);
        io.recv_flush("flush response").await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tclunk_does_not_wait_for_an_unrelated_fid() {
        let (filesystem, handler) = in_memory_handler().await;
        establish_session(&handler, 0, b"test", 1000, Some([0; 16])).await;
        let attach = handler
            .handle_message(
                1,
                Message::Tattach(Tattach {
                    fid: 3,
                    afid: u32::MAX,
                    uname: P9String::new(b"test".to_vec()),
                    aname: P9String::new(b"/".to_vec()),
                    n_uname: 1000,
                }),
            )
            .await;
        assert!(matches!(attach.body, Message::Rattach(_)));

        let inode_lock = filesystem.lock_manager.acquire(0).await;
        let mut io = DispatchFixture::new(handler, 4);
        io.dispatch(
            10,
            Message::Tlopenat(Tlopenat {
                fid: 1,
                newfid: 2,
                flags: libc::O_RDONLY as u32,
            }),
        );
        io.dispatch(11, Message::Tclunk(Tclunk { fid: 3 }));

        let (tag, clunk) = io.recv("unrelated clunk response").await;
        assert_eq!(tag, 11);
        assert!(matches!(clunk.body, Message::Rclunk(Rclunk)));
        io.expect_quiet("the locked open must still be pending")
            .await;

        drop(inode_lock);
        let (tag, open) = io.recv("open response").await;
        assert_eq!(tag, 10);
        assert!(matches!(open.body, Message::Rlopenat(Rlopenat { .. })));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tclunk_cannot_overtake_an_earlier_lopenat_install() {
        let (filesystem, mut io, open, clunk) = race_lopenat_with(
            11,
            Message::Tclunk(Tclunk { fid: 2 }),
            "Rclunk must wait for the earlier open before removing the fid",
            "clunk response",
        )
        .await;
        assert!(matches!(open.body, Message::Rlopenat(Rlopenat { .. })));
        assert!(matches!(clunk.body, Message::Rclunk(Rclunk)));

        // `Rclunk` precedes both fid removal and handle release.
        io.dispatch(
            12,
            Message::Tgetattr(Tgetattr {
                fid: 2,
                request_mask: GETATTR_ALL,
            }),
        );
        assert!(matches!(
            io.recv("getattr response").await.1.body,
            Message::Rlerror(_)
        ));
        assert!(filesystem.open_handles.get(&0).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tversion_waits_for_prior_lopenat() {
        let (filesystem, io, open, version) = race_lopenat_with(
            u16::MAX,
            Message::Tversion(Tversion {
                msize: super::super::handler::DEFAULT_MSIZE,
                version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
            }),
            "Rversion must wait for old-session requests before clearing their fids",
            "version response",
        )
        .await;
        assert!(matches!(open.body, Message::Rlopenat(Rlopenat { .. })));
        assert!(matches!(version.body, Message::Rversion(_)));

        let getattr = io
            .handler
            .handle_message(
                12,
                Message::Tgetattr(Tgetattr {
                    fid: 2,
                    request_mask: GETATTR_ALL,
                }),
            )
            .await;
        assert!(matches!(getattr.body, Message::Rlerror(_)));
        assert!(filesystem.open_handles.get(&0).is_none());
    }
}
