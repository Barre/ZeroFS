//! Asynchronous 9P2000.L client.
//!
//! # Reconnection
//!
//! Fids and byte-range locks are connection-scoped. The client records their
//! replay state, reconnects with backoff, and restores that state before
//! releasing blocked requests. Reorder-sensitive mutations retain one op-id
//! across resends. Resends stop at the protocol retry horizon; expiry returns a
//! disconnect error with an ambiguous outcome.
//!
//! An op-id is local to one public mutation future and is not reusable after the
//! future completes or is cancelled.
//!
//! Dropping a future after a fid- or lock-state request is dispatched retires
//! the connection that carried it. If that connection is still current,
//! requests wait for reconnect and replay before resuming.
//!
//! The negotiated message size is fixed for the logical session. Reconnect
//! candidates must negotiate the same value.
//!
//! Replay restores recorded fids and locks before the replacement connection
//! becomes live. An opened fid that no longer exists becomes an `ESTALE`
//! tombstone; its operations fail without affecting other fids. Failure to
//! restore a held lock terminates the logical session.

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use deku::prelude::*;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
pub use ninep_proto::NOFID;
use ninep_proto::retry::MUTATION_RETRY_HORIZON;
use ninep_proto::*;
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::net::{IpAddr, SocketAddr};
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
#[cfg(not(target_arch = "wasm32"))]
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{Notify, mpsc, oneshot};
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, info, warn};
use uuid::Uuid;

mod linux;
mod runtime;
#[cfg(target_arch = "wasm32")]
mod web_transport;

/// The 9P "no tag" sentinel. We never allocate it for a normal request.
const NOTAG: u16 = 0xFFFF;
/// Default TCP port used when a target omits one.
#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_9P_PORT: u16 = 5564;

const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(50);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);
/// Per-target dial and negotiation timeout.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// Reply timeout before liveness checks begin.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// Maximum age of a decoded frame accepted as proof of liveness.
const LIVENESS_WINDOW: Duration = Duration::from_secs(3);
/// Additional reply windows allowed while the connection remains live.
const MAX_LIVENESS_EXTRA_WINDOWS: u32 = 7;

const _: () = assert!(
    LIVENESS_WINDOW.as_nanos() < REQUEST_TIMEOUT.as_nanos(),
    "the liveness window must be shorter than the request timeout"
);

/// FIRST/RETRY state for one request future.
#[derive(Default)]
struct OpAttemptState {
    /// Epoch of the first dispatched frame. `Some` marks subsequent frames as
    /// retries and remains stable across writers.
    origin_epoch: Option<u64>,
    started: Option<runtime::Clock>,
}

impl OpAttemptState {
    /// Dispatches a frame and records its ambiguity without an await boundary.
    fn dispatch_frame<T>(
        &mut self,
        has_op_id: bool,
        connection_epoch: u64,
        dispatch: impl FnOnce(u8, u64) -> ClientResult<T>,
    ) -> ClientResult<(u8, T)> {
        let flags = if has_op_id && self.origin_epoch.is_some() {
            P9_OP_FLAG_RETRY
        } else {
            0
        };
        let origin_epoch = if has_op_id {
            self.origin_epoch.unwrap_or(connection_epoch)
        } else {
            0
        };
        let result = dispatch(flags, origin_epoch)?;
        if has_op_id {
            self.origin_epoch.get_or_insert(origin_epoch);
            if self.started.is_none() {
                self.started = Some(runtime::Clock::now());
            }
        }
        Ok((flags, result))
    }

    fn proven_predispatch(&mut self, sent_flags: u8) {
        if sent_flags & P9_OP_FLAG_RETRY == 0 {
            self.origin_epoch = None;
            self.started = None;
        }
    }

    fn retry_budget(&self) -> ClientResult<Option<Duration>> {
        let Some(started) = self.started.as_ref() else {
            return Ok(None);
        };
        let elapsed = Duration::from_millis(started.elapsed_millis());
        let retry_horizon = MUTATION_RETRY_HORIZON;
        if elapsed >= retry_horizon {
            return Err(ClientError::Disconnected);
        }
        Ok(Some(retry_horizon - elapsed))
    }
}

/// Retires the connection when a cancelled stateful request may have changed
/// server state that is absent from [`SessionState`].
struct StatefulCancellationGuard<'a> {
    client: &'a NinePClient,
    dispatched_conn: &'a Mutex<Option<Arc<Conn>>>,
    armed: bool,
}

impl StatefulCancellationGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StatefulCancellationGuard<'_> {
    fn drop(&mut self) {
        let conn = self
            .armed
            .then(|| self.dispatched_conn.lock().unwrap().take())
            .flatten();
        if let Some(conn) = conn {
            self.client.force_reprobe(&conn);
        }
    }
}

/// Timeout for rebuilding one candidate session. Expiry discards the candidate.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum ClientError {
    /// The server returned an `Rlerror` with this Linux errno.
    Errno(u32),
    /// The connection was lost (or never established).
    Disconnected,
    /// The server sent a reply we did not expect for the request.
    Unexpected(&'static str),
    /// A message failed to (de)serialise.
    Codec(DekuError),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Errno(e) => write!(f, "server error: errno {e}"),
            ClientError::Disconnected => write!(f, "9P connection lost"),
            ClientError::Unexpected(m) => write!(f, "unexpected 9P reply to {m}"),
            ClientError::Codec(e) => write!(f, "9P codec error: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    /// Map to a Linux errno suitable for a FUSE reply. Transport-level problems
    /// surface as `EIO`.
    pub fn to_errno(&self) -> i32 {
        match self {
            ClientError::Errno(e) => *e as i32,
            _ => linux::EIO,
        }
    }
}

pub type ClientResult<T> = Result<T, ClientError>;

/// Applies the remaining retry horizon while no request is in flight.
/// In-flight responses may settle after the horizon.
async fn await_resend_bounded<T>(
    attempt: &OpAttemptState,
    future: impl std::future::Future<Output = T>,
) -> ClientResult<T> {
    match attempt.retry_budget()? {
        Some(remaining) => runtime::timeout(remaining, future)
            .await
            .map_err(|_| ClientError::Disconnected),
        None => Ok(future.await),
    }
}

/// Cumulative wire traffic for this logical client across reconnects.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrafficStats {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub operations: u64,
}

#[derive(Default)]
struct TrafficCounters {
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    operations: AtomicU64,
}

mod ops;
pub use ops::{DirEntryCookie, ReaddirState, SetattrBuilder, SetattrTime};

/// A 9P endpoint. Multi-target clients probe for the serving leader.
///
/// Native IPv6 targets accept `[address]:port` and legacy unbracketed
/// `address:port`. `fd00::10:5564` maps to `[fd00::10]:5564`;
/// `[fd00::10:5564]` maps to the complete literal on the default port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    #[cfg(not(target_arch = "wasm32"))]
    Tcp(SocketAddr),
    /// A `host:port` TCP endpoint resolved on each probe.
    #[cfg(not(target_arch = "wasm32"))]
    TcpHost(String),
    #[cfg(not(target_arch = "wasm32"))]
    Unix(PathBuf),
    /// A browser WebSocket carrying one complete 9P frame per binary message.
    #[cfg(target_arch = "wasm32")]
    WebSocket(String),
}

impl Target {
    /// Parse a comma-separated target set. Whitespace and empty segments are
    /// ignored; an entirely empty set is rejected.
    pub fn parse_list(spec: &str) -> Result<Vec<Self>, String> {
        let targets = spec
            .split(',')
            .map(str::trim)
            .filter(|spec| !spec.is_empty())
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()?;
        if targets.is_empty() {
            Err("no 9P target given".into())
        } else {
            Ok(targets)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn parse_numeric_tcp_endpoint(endpoint: &str) -> Option<SocketAddr> {
    if let Ok(addr) = endpoint.parse::<SocketAddr>() {
        return Some(addr);
    }

    // Brackets without a port unambiguously denote the complete IPv6 literal.
    if let Some(literal) = endpoint
        .strip_prefix('[')
        .and_then(|literal| literal.strip_suffix(']'))
        && let Ok(ip @ IpAddr::V6(_)) = literal.parse::<IpAddr>()
    {
        return Some(SocketAddr::new(ip, DEFAULT_9P_PORT));
    }

    // Legacy unbracketed IPv6 host:port splits only when the prefix is a valid
    // IPv6 address.
    if !endpoint.starts_with('[')
        && let Some((host, port)) = endpoint.rsplit_once(':')
        && let Ok(ip @ IpAddr::V6(_)) = host.parse::<IpAddr>()
        && let Ok(port) = port.parse::<u16>()
    {
        return Some(SocketAddr::new(ip, port));
    }

    endpoint
        .parse::<IpAddr>()
        .ok()
        .map(|ip| SocketAddr::new(ip, DEFAULT_9P_PORT))
}

impl std::str::FromStr for Target {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Err("empty 9P target".into());
        }

        #[cfg(target_arch = "wasm32")]
        {
            return if spec.starts_with("ws://") || spec.starts_with("wss://") {
                Ok(Self::WebSocket(spec.into()))
            } else {
                Err(format!(
                    "browser clients require a ws:// or wss:// target, got {spec:?}"
                ))
            };
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(path) = spec.strip_prefix("unix:") {
                return Ok(Self::Unix(path.strip_prefix("//").unwrap_or(path).into()));
            }
            let endpoint = spec.strip_prefix("tcp://").unwrap_or(spec);
            if endpoint.starts_with('/') || endpoint.starts_with('.') {
                return Ok(Self::Unix(endpoint.into()));
            }
            if let Some(addr) = parse_numeric_tcp_endpoint(endpoint) {
                return Ok(Self::Tcp(addr));
            }
            Ok(Self::TcpHost(if endpoint.contains(':') {
                endpoint.into()
            } else {
                format!("{endpoint}:{DEFAULT_9P_PORT}")
            }))
        }
    }
}

#[cfg(test)]
mod target_parse_tests {
    use super::*;

    #[test]
    fn target_grammar_matrix() {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cases = [
                ("unix:///run/z.sock", r#"Unix("/run/z.sock")"#),
                ("./z.sock", r#"Unix("./z.sock")"#),
                ("tcp://127.0.0.1:6000", "Tcp(127.0.0.1:6000)"),
                ("127.0.0.1", "Tcp(127.0.0.1:5564)"),
                ("::1", "Tcp([::1]:5564)"),
                ("leader.example", r#"TcpHost("leader.example:5564")"#),
                ("leader.example:6000", r#"TcpHost("leader.example:6000")"#),
            ];
            for (spec, expected) in cases {
                assert_eq!(format!("{:?}", spec.parse::<Target>().unwrap()), expected);
            }
            assert_eq!(
                format!(
                    "{:?}",
                    Target::parse_list("retired.invalid, ,leader.example:6000,").unwrap()
                ),
                r#"[TcpHost("retired.invalid:5564"), TcpHost("leader.example:6000")]"#
            );
        }

        #[cfg(target_arch = "wasm32")]
        {
            let targets = Target::parse_list("ws://node-a:5564, ,wss://node-b/9p").unwrap();
            assert_eq!(targets.len(), 2);
            assert!(matches!(&targets[0], Target::WebSocket(url) if url == "ws://node-a:5564"));
            assert!("tcp://node-a:5564".parse::<Target>().is_err());
        }

        assert!(Target::parse_list(" , ").is_err());
        assert!(" ".parse::<Target>().is_err());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn legacy_ipv6_ambiguity_matches_documented_mapping() {
        let cases = [
            ("::1", "[::1]:5564"),
            ("::1:5564", "[::1]:5564"),
            ("fd00::10", "[fd00::10]:5564"),
            ("fd00::10:5564", "[fd00::10]:5564"),
            ("tcp://fd00::10:6000", "[fd00::10]:6000"),
            ("[fd00::10]:6000", "[fd00::10]:6000"),
            ("[fd00::10]", "[fd00::10]:5564"),
            ("tcp://[::1]", "[::1]:5564"),
            ("[fd00::10:5564]", "[fd00::10:5564]:5564"),
        ];

        for (spec, expected) in cases {
            assert_eq!(
                spec.parse::<Target>(),
                Ok(Target::Tcp(expected.parse::<SocketAddr>().unwrap())),
                "target {spec:?}"
            );
        }
    }
}

/// One transport and its reader/writer tasks.
struct Conn {
    writer_tx: mpsc::Sender<Vec<u8>>,
    pending: DashMap<u16, oneshot::Sender<Bytes>>,
    tag_ctr: AtomicU16,
    /// Durability lineage returned by `Tgetlineage`.
    lineage_token: AtomicU64,
    /// Writer epoch returned by `Tgetlineage`; zero for standalone servers.
    writer_epoch: AtomicU64,
    /// Set by whichever of the reader/writer tasks first sees the socket fail.
    dead: AtomicBool,
    /// Monotonic base for [`Conn::last_alive`].
    base: runtime::Clock,
    /// Milliseconds since `base` when the last frame was decoded.
    last_alive: AtomicU64,
    /// Serializes explicit liveness probes.
    probe_lock: tokio::sync::Mutex<()>,
    /// Signals the (possibly idle) writer task to stop when the reader exits.
    writer_shutdown: Notify,
    /// Stops the reader when a healthy connection is discarded before install.
    reader_shutdown: Notify,
    counters: Arc<TrafficCounters>,
}

impl Conn {
    /// Stops both transport tasks.
    fn shutdown(&self) {
        self.dead.store(true, Ordering::Release);
        self.reader_shutdown.notify_one();
        self.writer_shutdown.notify_one();
    }

    /// Records receipt of a frame.
    fn mark_alive(&self) {
        self.last_alive
            .store(self.base.elapsed_millis(), Ordering::Relaxed);
    }

    /// Saturating strict-window comparison.
    fn within(now_ms: u64, last_ms: u64, window: Duration) -> bool {
        now_ms.saturating_sub(last_ms) < window.as_millis() as u64
    }

    /// Whether a frame was decoded within `window`.
    fn heard_within(&self, window: Duration) -> bool {
        Self::within(
            self.base.elapsed_millis(),
            self.last_alive.load(Ordering::Relaxed),
            window,
        )
    }

    fn deliver(&self, frame: Bytes) {
        self.counters
            .bytes_received
            .fetch_add(frame.len() as u64, Ordering::Relaxed);
        self.mark_alive();
        if frame.len() < P9_HEADER_SIZE {
            warn!(
                "9P client: response frame too short ({} bytes)",
                frame.len()
            );
            return;
        }
        let tag = u16::from_le_bytes([frame[5], frame[6]]);
        if let Some((_, pending)) = self.pending.remove(&tag) {
            let _ = pending.send(frame);
        } else {
            debug!("9P client: response for unknown tag {tag}");
        }
    }

    fn connection_lost(&self, reconnect: &Notify) {
        self.dead.store(true, Ordering::Release);
        self.pending.clear();
        self.writer_shutdown.notify_one();
        reconnect.notify_waiters();
    }
}

/// Owns a response slot until dispatch. Dispatched slots remain quarantined
/// until response delivery or connection teardown prevents tag aliasing.
struct PendingTag<C: std::ops::Deref<Target = Conn>> {
    conn: C,
    tag: u16,
    dispatched: bool,
}

impl<C: std::ops::Deref<Target = Conn>> PendingTag<C> {
    fn mark_dispatched(&mut self) {
        self.dispatched = true;
    }
}

impl<C: std::ops::Deref<Target = Conn>> Drop for PendingTag<C> {
    fn drop(&mut self) {
        if !self.dispatched {
            self.conn.pending.remove(&self.tag);
        }
    }
}

/// Fid replay record keyed by stable inode identity.
#[derive(Clone)]
struct FidRecord {
    inode_id: u64,
    /// Stable inode the original Tattach resolved to.
    root_inode: u64,
    n_uname: u32,
    /// Original `Tattach` username for name-derived credentials.
    uname: Vec<u8>,
    /// `Some(flags)` if open; replayed with a `Tlopen`.
    opened: Option<u32>,
}

impl FidRecord {
    fn replay_identity(&self) -> (u32, u64, Vec<u8>) {
        (self.n_uname, self.root_inode, self.uname.clone())
    }
}

#[derive(Clone)]
struct LockRecord {
    fid: u32,
    lock_type: LockType,
    start: u64,
    length: u64,
    proc_id: u32,
    client_id: Vec<u8>,
}

/// The replayable session state: enough to rebuild every fid and lock.
#[derive(Default, Clone)]
struct SessionState {
    fids: HashMap<u32, FidRecord>,
    locks: Vec<LockRecord>,
    /// Default root for the public `rebind` helper.
    default_root: Option<u64>,
}

impl SessionState {
    fn forget_fid(&mut self, fid: u32) {
        self.fids.remove(&fid);
        self.locks.retain(|lock| lock.fid != fid);
    }
}

pub struct NinePClient {
    /// Targets probed by the reconnect supervisor.
    targets: Vec<Target>,
    /// Current transport, swapped atomically by the reconnect supervisor.
    conn: ArcSwap<Conn>,
    /// False while a reconnect+replay is in progress; requests block until true.
    live: AtomicBool,
    /// Permanent replay errno; zero while replay remains possible.
    terminal_errno: AtomicU32,
    live_notify: Notify,
    reconnect_notify: Arc<Notify>,
    /// Negotiated once for the logical session. Reconnects must match it so an
    /// ambiguous mutation can be resent byte-for-byte.
    msize: u32,
    /// Suppresses repeated message-size mismatch warnings for this logical session.
    msize_mismatch_warned: Arc<AtomicBool>,
    fid_ctr: AtomicU32,
    fid_free: Mutex<Vec<u32>>,
    /// Recorded fids (by inode id) and held locks, replayed on reconnect.
    state: Mutex<SessionState>,
    /// Opened fids that could not be restored and remain allocated by the caller.
    stale_fids: DashSet<u32>,
    /// Orders stateful response settlement against snapshot/replay/install.
    session_transition: tokio::sync::Mutex<()>,
    /// Per-fid durability obligations carried across reconnects.
    unsynced: DashMap<u32, Unsynced>,
    counters: Arc<TrafficCounters>,
}

/// One fid's durability obligation. `generation` prevents an fsync from clearing
/// a concurrent write. `reported` keeps `ESTALE` visible until a replacement write.
#[derive(Default)]
struct Unsynced {
    oldest: Option<u64>,
    generation: u64,
    reported: bool,
}

impl Unsynced {
    /// Records a write, preserving the oldest unreported lineage token.
    fn note(&mut self, token: u64) {
        if self.reported || self.oldest.is_none() {
            self.oldest = Some(token);
            self.reported = false;
        }
        self.generation = self.generation.wrapping_add(1);
    }

    fn snapshot(&self) -> (Option<u64>, u64) {
        (self.oldest, self.generation)
    }

    /// Clears the obligation if no write followed the fsync snapshot.
    fn clear_if_unchanged(&mut self, generation: u64) {
        if self.generation == generation {
            self.oldest = None;
            self.reported = false;
        }
    }

    /// Marks a stale obligation reported if no write followed the snapshot.
    fn report_if_unchanged(&mut self, generation: u64) {
        if self.generation == generation {
            self.reported = true;
        }
    }
}

impl NinePClient {
    /// Connect to a 9P server over TCP and negotiate the protocol version.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect_tcp(addr: SocketAddr, requested_msize: u32) -> std::io::Result<Arc<Self>> {
        Self::connect(vec![Target::Tcp(addr)], requested_msize)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Connect to a 9P server over a Unix domain socket and negotiate the version.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect_unix(
        path: impl AsRef<Path>,
        requested_msize: u32,
    ) -> std::io::Result<Arc<Self>> {
        Self::connect(
            vec![Target::Unix(path.as_ref().to_path_buf())],
            requested_msize,
        )
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Connect to the serving leader in a target set and re-probe on reconnect.
    pub async fn connect_multi(
        targets: Vec<Target>,
        requested_msize: u32,
    ) -> std::io::Result<Arc<Self>> {
        Self::connect(targets, requested_msize)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Connect to 9P over a browser WebSocket.
    #[cfg(target_arch = "wasm32")]
    pub async fn connect_websocket(url: &str, requested_msize: u32) -> ClientResult<Arc<Self>> {
        Self::connect(vec![Target::WebSocket(url.to_string())], requested_msize).await
    }

    async fn connect(targets: Vec<Target>, requested_msize: u32) -> ClientResult<Arc<Self>> {
        let reconnect_notify = Arc::new(Notify::new());
        let counters = Arc::new(TrafficCounters::default());
        let msize_mismatch_warned = Arc::new(AtomicBool::new(false));
        let (conn, msize) = Self::probe(
            &targets,
            requested_msize,
            None,
            Arc::clone(&reconnect_notify),
            Arc::clone(&counters),
            Arc::clone(&msize_mismatch_warned),
        )
        .await?;

        let client = Arc::new(Self {
            targets,
            conn: ArcSwap::new(conn),
            live: AtomicBool::new(true),
            terminal_errno: AtomicU32::new(0),
            live_notify: Notify::new(),
            reconnect_notify,
            msize,
            msize_mismatch_warned,
            fid_ctr: AtomicU32::new(1),
            fid_free: Mutex::new(Vec::new()),
            state: Mutex::new(SessionState::default()),
            stale_fids: DashSet::new(),
            session_transition: tokio::sync::Mutex::new(()),
            unsynced: DashMap::new(),
            counters,
        });
        client.spawn_supervisor();
        Ok(client)
    }

    /// Open a fresh socket, spawn its reader/writer tasks and negotiate.
    async fn connect_once(
        target: &Target,
        requested_msize: u32,
        required_msize: Option<u32>,
        reconnect_notify: Arc<Notify>,
        counters: Arc<TrafficCounters>,
        msize_mismatch_warned: Arc<AtomicBool>,
    ) -> ClientResult<(Arc<Conn>, u32)> {
        let transport = dial(target).await?;
        let (writer_tx, writer_rx) = mpsc::channel::<Vec<u8>>(P9_CHANNEL_SIZE);
        let conn = Arc::new(Conn {
            writer_tx,
            pending: DashMap::new(),
            tag_ctr: AtomicU16::new(0),
            lineage_token: AtomicU64::new(0),
            writer_epoch: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            base: runtime::Clock::now(),
            last_alive: AtomicU64::new(0),
            probe_lock: tokio::sync::Mutex::new(()),
            writer_shutdown: Notify::new(),
            reader_shutdown: Notify::new(),
            counters,
        });

        match transport {
            #[cfg(not(target_arch = "wasm32"))]
            DialedTransport::Native { read, write } => {
                spawn_writer(
                    write,
                    writer_rx,
                    Arc::clone(&conn),
                    Arc::clone(&reconnect_notify),
                );
                spawn_reader(read, Arc::clone(&conn), reconnect_notify);
            }
            #[cfg(target_arch = "wasm32")]
            DialedTransport::WebSocket(io) => {
                web_transport::spawn(io, writer_rx, Arc::clone(&conn), reconnect_notify);
            }
        }

        match runtime::timeout(PROBE_TIMEOUT, negotiate_on(&conn, requested_msize)).await {
            Ok(Ok(msize)) => {
                if let Some(required) = required_msize
                    && msize != required
                {
                    if !msize_mismatch_warned.swap(true, Ordering::AcqRel) {
                        warn!(
                            "9P reconnect candidate negotiated msize {msize}; logical session requires {required}"
                        );
                    } else {
                        debug!(
                            "9P reconnect candidate still negotiates msize {msize}; logical session requires {required}"
                        );
                    }
                    conn.shutdown();
                    return Err(ClientError::Unexpected("version"));
                }
                Ok((conn, msize))
            }
            // Healthy socket but the handshake failed/stalled; tear down so the fd is not leaked.
            Ok(Err(e)) => {
                conn.shutdown();
                Err(e)
            }
            Err(_) => {
                conn.shutdown();
                Err(ClientError::Disconnected)
            }
        }
    }

    /// Probes all targets concurrently. Successful negotiation proves leadership.
    async fn probe(
        targets: &[Target],
        requested_msize: u32,
        required_msize: Option<u32>,
        reconnect_notify: Arc<Notify>,
        counters: Arc<TrafficCounters>,
        msize_mismatch_warned: Arc<AtomicBool>,
    ) -> ClientResult<(Arc<Conn>, u32)> {
        let mut probes = FuturesUnordered::new();
        for target in targets {
            let target = target.clone();
            let notify = Arc::clone(&reconnect_notify);
            let counters = Arc::clone(&counters);
            let warned = Arc::clone(&msize_mismatch_warned);
            probes.push(async move {
                Self::connect_once(
                    &target,
                    requested_msize,
                    required_msize,
                    notify,
                    counters,
                    warned,
                )
                .await
            });
        }

        let mut last_err = None;
        let mut winner = None;
        while let Some(res) = probes.next().await {
            match res {
                Ok(triple) => {
                    winner = Some(triple);
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }

        // Completed losing probes may own live transport tasks.
        if !probes.is_empty() {
            runtime::spawn(async move {
                while let Some(res) = probes.next().await {
                    if let Ok((conn, _)) = res {
                        conn.shutdown();
                    }
                }
            });
        }

        winner.ok_or_else(|| last_err.unwrap_or(ClientError::Disconnected))
    }

    /// Reconnects and replays indefinitely after transport loss.
    fn spawn_supervisor(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let notify = Arc::clone(&self.reconnect_notify);
        runtime::spawn(async move {
            loop {
                // Register before reading `dead` to prevent a lost notification.
                loop {
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let this = match weak.upgrade() {
                        Some(t) => t,
                        None => return,
                    };
                    if this.conn.load().dead.load(Ordering::Acquire) {
                        this.live.store(false, Ordering::Release);
                        break;
                    }
                    drop(this);
                    notified.await;
                }

                warn!("9P connection lost; reconnecting and replaying session…");
                let mut backoff = RECONNECT_BACKOFF_MIN;
                loop {
                    let this = match weak.upgrade() {
                        Some(t) => t,
                        None => return,
                    };
                    match this.reconnect_once().await {
                        Ok(()) => {
                            this.live.store(true, Ordering::Release);
                            this.live_notify.notify_waiters();
                            info!("9P session reconnected and restored");
                            break;
                        }
                        Err(e) => {
                            if this.terminal_errno.load(Ordering::Acquire) != 0 {
                                warn!("9P session replay failed permanently: {e}");
                                return;
                            }
                            debug!("9P reconnect failed ({e}); retrying in {backoff:?}");
                            drop(this);
                            runtime::sleep(backoff).await;
                            backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                        }
                    }
                }
            }
        });
    }

    /// Dials, replays, and installs one replacement connection.
    async fn reconnect_once(&self) -> ClientResult<()> {
        let msize = self.msize();
        let (conn, msize) = Self::probe(
            &self.targets,
            msize,
            Some(msize),
            Arc::clone(&self.reconnect_notify),
            Arc::clone(&self.counters),
            Arc::clone(&self.msize_mismatch_warned),
        )
        .await?;

        // Settlement and snapshot/replay/install are mutually exclusive.
        let _transition = self.session_transition.lock().await;
        debug_assert_eq!(msize, self.msize);
        match runtime::timeout(REPLAY_TIMEOUT, self.replay(&conn)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.discard_replay_candidate(&conn).await;
                return Err(e);
            }
            Err(_) => {
                self.discard_replay_candidate(&conn).await;
                warn!("9P session replay stalled past {REPLAY_TIMEOUT:?}; retrying reconnect");
                return Err(ClientError::Disconnected);
            }
        }
        let old = self.conn.swap(conn);
        old.dead.store(true, Ordering::Release);
        old.writer_shutdown.notify_one();

        Ok(())
    }

    /// Resets a failed candidate before close. The ordered `Tversion` reply
    /// confirms release of replay-created server state.
    async fn discard_replay_candidate(&self, conn: &Conn) {
        match runtime::timeout(PROBE_TIMEOUT, version_on(conn, self.msize)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                debug!("9P replay candidate reset failed ({error}); closing connection");
            }
            Err(_) => {
                debug!("9P replay candidate reset timed out; closing connection");
            }
        }
        conn.shutdown();
    }

    /// Rebuilds recorded fids and locks on `conn`. Roots precede descendants.
    async fn replay(&self, conn: &Conn) -> ClientResult<()> {
        let snapshot = self.state.lock().unwrap().clone();
        let mut gone_unopened = Vec::new();
        let mut stale_opened = Vec::new();

        let (roots, descendants): (Vec<_>, Vec<_>) = snapshot
            .fids
            .iter()
            .partition(|(_, rec)| rec.inode_id == rec.root_inode);
        for (&fid, rec) in roots.into_iter().chain(descendants) {
            let has_lock = snapshot.locks.iter().any(|lock| lock.fid == fid);
            let restored = self.replay_fid(conn, fid, rec).await?;
            if !restored {
                if has_lock {
                    return Err(self.replay_state_lost("fid with a held lock could not be rebound"));
                }
                if rec.opened.is_some() {
                    stale_opened.push(fid);
                } else {
                    gone_unopened.push(fid);
                }
                continue;
            }
            if let Some(flags) = rec.opened {
                match Self::send_raw_rpc(conn, Message::Tlopen(Tlopen { fid, flags })).await {
                    Ok(Message::Rlopen(_)) => {}
                    Ok(_) => {
                        return Err(
                            self.replay_state_lost("opened fid returned a non-Rlopen reply")
                        );
                    }
                    Err(ClientError::Errno(errno)) if Self::replay_state_lost_errno(errno) => {
                        if has_lock {
                            return Err(self.replay_state_lost("locked fid could not be reopened"));
                        }
                        match Self::send_raw_rpc(conn, Message::Tclunk(Tclunk { fid })).await {
                            Ok(Message::Rclunk(_)) => {}
                            Ok(_) => return Err(ClientError::Unexpected("replay clunk")),
                            Err(error) => return Err(error),
                        }
                        stale_opened.push(fid);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        let mut conflict_backoff = RECONNECT_BACKOFF_MIN;
        'lock_replay: loop {
            // Install the complete recorded lock set or roll back its prefix.
            let mut acquired = Vec::new();
            for lk in &snapshot.locks {
                let body = Message::Tlock(Tlock {
                    fid: lk.fid,
                    lock_type: lk.lock_type,
                    flags: 0,
                    start: lk.start,
                    length: lk.length,
                    proc_id: lk.proc_id,
                    client_id: P9String::new(lk.client_id.clone()),
                });
                match Self::send_raw_rpc(conn, body).await {
                    Ok(Message::Rlock(r)) if matches!(r.status, LockStatus::Success) => {
                        acquired.push(lk);
                    }
                    Ok(Message::Rlock(Rlock {
                        status: LockStatus::Blocked,
                    }))
                    | Err(ClientError::Errno(linux::EAGAIN)) => {
                        let rolled_back = Self::rollback_replayed_locks(conn, &acquired).await;
                        if !rolled_back {
                            return Err(ClientError::Errno(linux::EAGAIN));
                        }
                        runtime::sleep(conflict_backoff).await;
                        conflict_backoff = (conflict_backoff * 2).min(RECONNECT_BACKOFF_MAX);
                        continue 'lock_replay;
                    }
                    Ok(Message::Rlock(_)) => {
                        Self::rollback_replayed_locks(conn, &acquired).await;
                        return Err(self.replay_state_lost("recorded lock was not reacquired"));
                    }
                    Ok(_) => {
                        Self::rollback_replayed_locks(conn, &acquired).await;
                        return Err(
                            self.replay_state_lost("lock replay returned a non-Rlock reply")
                        );
                    }
                    Err(ClientError::Errno(errno)) if Self::replay_state_lost_errno(errno) => {
                        Self::rollback_replayed_locks(conn, &acquired).await;
                        return Err(self.replay_state_lost("recorded lock was refused"));
                    }
                    Err(e) => {
                        Self::rollback_replayed_locks(conn, &acquired).await;
                        return Err(e);
                    }
                }
            }
            if !gone_unopened.is_empty() || !stale_opened.is_empty() {
                let mut state = self.state.lock().unwrap();
                for fid in gone_unopened.iter().chain(&stale_opened) {
                    state.fids.remove(fid);
                }
                state.locks.retain(|lock| {
                    !gone_unopened.contains(&lock.fid) && !stale_opened.contains(&lock.fid)
                });
                drop(state);
                for fid in &stale_opened {
                    self.stale_fids.insert(*fid);
                }
            }
            return Ok(());
        }
    }

    /// Releases the lock prefix installed by this replay attempt.
    async fn rollback_replayed_locks(conn: &Conn, acquired: &[&LockRecord]) -> bool {
        if acquired.is_empty() {
            return true;
        }
        let rollback = async {
            let mut requests = FuturesUnordered::new();
            for lk in acquired.iter().rev() {
                requests.push(Self::send_raw_rpc(
                    conn,
                    Message::Tlock(Tlock {
                        fid: lk.fid,
                        lock_type: LockType::Unlock,
                        flags: 0,
                        start: lk.start,
                        length: lk.length,
                        proc_id: lk.proc_id,
                        client_id: P9String::new(lk.client_id.clone()),
                    }),
                ));
            }
            let mut acknowledged = true;
            while let Some(result) = requests.next().await {
                if !matches!(
                    result,
                    Ok(Message::Rlock(Rlock {
                        status: LockStatus::Success
                    }))
                ) {
                    debug!("9P lock replay rollback was not acknowledged");
                    acknowledged = false;
                }
            }
            acknowledged
        };
        match runtime::timeout(PROBE_TIMEOUT, rollback).await {
            Ok(acknowledged) => acknowledged,
            Err(_) => {
                warn!("9P lock replay rollback timed out; discarding candidate session");
                false
            }
        }
    }

    /// Rebinds one fid. `Ok(false)` denotes `ENOENT` or `ESTALE`.
    async fn replay_fid(&self, conn: &Conn, fid: u32, rec: &FidRecord) -> ClientResult<bool> {
        let body = Message::Trebind(Trebind {
            fid,
            inode_id: rec.inode_id,
            root_inode: rec.root_inode,
            flags: P9_REBIND_REPLAY
                | if rec.opened.is_some() {
                    P9_REBIND_OPENED
                } else {
                    0
                },
            uname: P9String::new(rec.uname.clone()),
            n_uname: rec.n_uname,
        });
        match Self::send_raw_rpc(conn, body).await {
            Ok(Message::Rrebind(_)) => Ok(true),
            Ok(_) => Err(ClientError::Unexpected("replay fid")),
            Err(ClientError::Errno(errno)) if Self::replay_state_lost_errno(errno) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn replay_state_lost_errno(errno: u32) -> bool {
        matches!(errno, linux::ENOENT | linux::ESTALE)
    }

    fn validate_fids(&self, fids: impl IntoIterator<Item = u32>) -> ClientResult<()> {
        if fids.into_iter().any(|fid| self.stale_fids.contains(&fid)) {
            return Err(ClientError::Errno(linux::ESTALE));
        }
        Ok(())
    }

    /// The message size negotiated for the logical session.
    pub fn msize(&self) -> u32 {
        self.msize
    }

    /// Whether requests currently have a live connection rather than waiting
    /// for reconnect/session replay.
    pub fn is_connected(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    /// Cumulative traffic across the lifetime of this logical client.
    pub fn traffic_stats(&self) -> TrafficStats {
        TrafficStats {
            bytes_sent: self.counters.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.counters.bytes_received.load(Ordering::Relaxed),
            operations: self.counters.operations.load(Ordering::Relaxed),
        }
    }

    /// Records an acknowledged mutation under its connection lineage.
    fn note_unsynced(&self, fid: u32, token: u64) {
        self.unsynced.entry(fid).or_default().note(token);
    }

    /// Snapshot `(oldest token, generation)` of `fid` for a verified fsync.
    fn snapshot_unsynced(&self, fid: u32) -> (Option<u64>, u64) {
        self.unsynced.get(&fid).map_or((None, 0), |u| u.snapshot())
    }

    /// Clears an unchanged obligation after verified fsync. `remove_if` rechecks
    /// under the shard lock before deleting the empty entry.
    fn clear_unsynced_if_unchanged(&self, fid: u32, generation: u64) {
        if let Some(mut u) = self.unsynced.get_mut(&fid) {
            u.clear_if_unchanged(generation);
        }
        self.unsynced.remove_if(&fid, |_, u| u.oldest.is_none());
    }

    /// Marks an unchanged obligation reported after `ESTALE`.
    fn report_unsynced_if_unchanged(&self, fid: u32, generation: u64) {
        if let Some(mut u) = self.unsynced.get_mut(&fid) {
            u.report_if_unchanged(generation);
        }
    }

    /// Removes durability state before a fid number is reused.
    fn forget_unsynced(&self, fid: u32) {
        self.unsynced.remove(&fid);
    }

    /// Maximum data a single Tread/Treaddir response (Rread/Rreaddir) can carry
    /// within the negotiated msize: `msize - header - count`.
    pub fn max_io(&self) -> u32 {
        self.msize().saturating_sub(P9_IOHDRSZ)
    }

    /// Maximum data a single Twrite *request* can carry within the negotiated
    /// msize. The Twrite header is larger than the Rread header, so this is
    /// smaller than [`Self::max_io`]; using max_io here would produce a frame a
    /// few bytes over msize that the server rejects.
    pub fn max_write_payload(&self) -> u32 {
        self.msize()
            .saturating_sub(P9_TWRITE_HDR)
            .saturating_sub(P9_OP_ENVELOPE_LEN as u32)
    }

    /// Allocate a fresh fid (reusing a freed one when possible).
    pub fn alloc_fid(&self) -> u32 {
        if let Some(fid) = self.fid_free.lock().unwrap().pop() {
            return fid;
        }
        self.fid_ctr.fetch_add(1, Ordering::Relaxed)
    }

    /// Return a fid to the free list. The caller must have clunked it already.
    pub fn free_fid(&self, fid: u32) {
        self.forget_unsynced(fid);
        self.stale_fids.remove(&fid);
        self.fid_free.lock().unwrap().push(fid);
    }

    /// Allocated fids not yet returned to the free list. Leak-accounting diagnostic.
    pub fn outstanding_fids(&self) -> usize {
        let allocated = self.fid_ctr.load(Ordering::Relaxed).saturating_sub(1) as usize;
        allocated.saturating_sub(self.fid_free.lock().unwrap().len())
    }

    /// Records the first terminal replay failure.
    fn fail_session(&self, errno: u32) {
        let _ = self
            .terminal_errno
            .compare_exchange(0, errno, Ordering::AcqRel, Ordering::Acquire);
        self.live.store(false, Ordering::Release);
        self.live_notify.notify_waiters();
    }

    fn replay_state_lost(&self, reason: &str) -> ClientError {
        warn!("9P session replay cannot preserve observed state: {reason}");
        self.fail_session(linux::ESTALE);
        ClientError::Errno(linux::ESTALE)
    }

    /// Block until the connection is live (i.e. not mid-reconnect).
    async fn wait_until_live(&self) -> ClientResult<()> {
        loop {
            let notified = self.live_notify.notified();
            tokio::pin!(notified);
            // Register the waiter *before* the check to avoid a lost wakeup.
            notified.as_mut().enable();
            let terminal_errno = self.terminal_errno.load(Ordering::Acquire);
            if terminal_errno != 0 {
                return Err(ClientError::Errno(terminal_errno));
            }
            if self.live.load(Ordering::Acquire) {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Register one response slot under an exact numeric tag. On collision the
    /// sender is returned so the allocator can try another tag.
    fn register_tag(
        conn: &Conn,
        tag: u16,
        tx: oneshot::Sender<Bytes>,
    ) -> Result<u16, oneshot::Sender<Bytes>> {
        match conn.pending.entry(tag) {
            Entry::Vacant(slot) => {
                slot.insert(tx);
                Ok(tag)
            }
            Entry::Occupied(_) => Err(tx),
        }
    }

    /// Allocates and registers a tag. Exhaustion requires connection recycling.
    fn alloc_tag(
        conn: &Conn,
        mut tx: oneshot::Sender<Bytes>,
    ) -> Result<u16, oneshot::Sender<Bytes>> {
        // Scan all 65,535 usable tags, including a cycle starting at NOTAG.
        for _ in 0..=usize::from(NOTAG) {
            let candidate = conn.tag_ctr.fetch_add(1, Ordering::Relaxed);
            if candidate == NOTAG {
                continue;
            }
            match Self::register_tag(conn, candidate, tx) {
                Ok(tag) => return Ok(tag),
                Err(returned) => tx = returned,
            }
        }
        Err(tx)
    }

    /// Sends across reconnects and returns the accepted response connection.
    async fn send_request_on_current(
        &self,
        op_id: [u8; 16],
        body: Message,
        attempt: &mut OpAttemptState,
        stateful_dispatched_conn: Option<&Mutex<Option<Arc<Conn>>>>,
    ) -> ClientResult<(Message, Arc<Conn>)> {
        'resend: loop {
            self.validate_fids(body.request_fids())?;
            await_resend_bounded(attempt, self.wait_until_live()).await??;
            let conn = self.conn.load_full();
            self.validate_fids(body.request_fids())?;

            // Reserve capacity before registering a response tag.
            let permit = match await_resend_bounded(attempt, conn.writer_tx.reserve()).await? {
                Ok(permit) => permit,
                Err(_) => {
                    runtime::yield_now().await;
                    continue;
                }
            };

            // `live` may lag `dead`; reject the stale connection before registration.
            if conn.dead.load(Ordering::Acquire) {
                drop(permit);
                self.reconnect_notify.notify_waiters();
                runtime::yield_now().await;
                continue;
            }

            let (otx, mut orx) = oneshot::channel();
            let tag = match Self::alloc_tag(&conn, otx) {
                Ok(tag) => tag,
                Err(_) => {
                    // Teardown clears live and quarantined tags.
                    drop(permit);
                    self.force_reprobe(&conn);
                    runtime::yield_now().await;
                    continue;
                }
            };
            let mut pending = PendingTag {
                conn: Arc::clone(&conn),
                tag,
                dispatched: false,
            };

            // `connection_lost` publishes `dead` before clearing registrations.
            // This second check closes the registration race.
            if conn.dead.load(Ordering::Acquire) {
                drop(permit);
                runtime::yield_now().await;
                continue;
            }

            // Frames after the first possible dispatch carry RETRY.
            let has_op_id = op_id != [0u8; 16];
            let connection_epoch = conn.writer_epoch.load(Ordering::Relaxed);
            let (op_flags, ()) =
                attempt.dispatch_frame(has_op_id, connection_epoch, |op_flags, origin_epoch| {
                    let bytes = P9Message::new_with_op_id_flags_and_origin(
                        tag,
                        op_id,
                        op_flags,
                        origin_epoch,
                        body.clone(),
                    )
                    .to_bytes_ctx(true)
                    .map_err(ClientError::Codec)?;

                    // Registration-to-enqueue has no cancellation point.
                    pending.mark_dispatched();
                    permit.send(bytes);
                    if let Some(dispatched_conn) = stateful_dispatched_conn {
                        *dispatched_conn.lock().unwrap() = Some(Arc::clone(&conn));
                    }
                    Ok(())
                })?;
            // Preserve the in-flight request while bounded liveness checks succeed.
            let mut extra_windows = 0u32;
            let frame = loop {
                match runtime::timeout(REQUEST_TIMEOUT, &mut orx).await {
                    Ok(Ok(frame)) => break frame,
                    Ok(Err(_)) => {
                        // Lost the reply to a drop: wait for reconnect and resend.
                        runtime::yield_now().await;
                        continue 'resend;
                    }
                    Err(_) => {
                        if extra_windows < MAX_LIVENESS_EXTRA_WINDOWS
                            && Self::conn_alive(&conn).await
                        {
                            extra_windows += 1;
                            continue;
                        }
                        self.force_reprobe(&conn);
                        runtime::yield_now().await;
                        continue 'resend;
                    }
                }
            };

            // Responses never carry the private mutation envelope. Retain the
            // transport frame so bulk response payloads can borrow its storage.
            let msg = P9Message::from_owned_bytes_ctx(frame, false).map_err(ClientError::Codec)?;
            if let Message::Rlerror(ref e) = msg.body
                && matches!(e.ecode, P9_ENOTLEADER | P9_ENOTLEADER_CLEAN)
            {
                if e.ecode == P9_ENOTLEADER_CLEAN {
                    attempt.proven_predispatch(op_flags);
                    if let Some(dispatched_conn) = stateful_dispatched_conn {
                        dispatched_conn.lock().unwrap().take();
                    }
                }
                self.force_reprobe(&conn);
                runtime::yield_now().await;
                continue;
            }
            // Successful mutations create per-fid durability obligations.
            if !matches!(msg.body, Message::Rlerror(_)) {
                let token = conn.lineage_token.load(Ordering::Relaxed);
                for fid in body.durability_fids() {
                    self.note_unsynced(fid, token);
                }
            }
            return Ok((msg.body, conn));
        }
    }

    /// Request path for operations that do not change replayable session state.
    async fn send_request(&self, body: Message) -> ClientResult<Message> {
        let op_id = if body.is_mutation() {
            Uuid::new_v4().into_bytes()
        } else {
            [0u8; 16]
        };
        let mut attempt = OpAttemptState::default();
        self.send_request_on_current(op_id, body, &mut attempt, None)
            .await
            .map(|(response, _)| response)
    }

    /// Claims settlement if `response_conn` is still current and live.
    async fn accept_stateful_response(
        &self,
        response_conn: &Arc<Conn>,
    ) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        let transition = self.session_transition.lock().await;
        let current = self.conn.load_full();
        if self.live.load(Ordering::Acquire)
            && !response_conn.dead.load(Ordering::Acquire)
            && Arc::ptr_eq(response_conn, &current)
        {
            Some(transition)
        } else {
            None
        }
    }

    /// Sends a session-state mutation. The returned guard covers local replay
    /// bookkeeping. Responses from replaced connections are resent.
    async fn send_stateful_request(
        &self,
        body: Message,
    ) -> ClientResult<(Message, tokio::sync::MutexGuard<'_, ()>)> {
        let op_id = if body.is_mutation() {
            Uuid::new_v4().into_bytes()
        } else {
            [0u8; 16]
        };
        let mut attempt = OpAttemptState::default();
        let dispatched_conn = Mutex::new(None);
        let mut cancellation = StatefulCancellationGuard {
            client: self,
            dispatched_conn: &dispatched_conn,
            armed: true,
        };
        let result = loop {
            let (response, response_conn) = match self
                .send_request_on_current(op_id, body.clone(), &mut attempt, Some(&dispatched_conn))
                .await
            {
                Ok(response) => response,
                Err(error) => break Err(error),
            };
            if let Some(transition) = self.accept_stateful_response(&response_conn).await {
                break Ok((response, transition));
            }
        };
        // Errors and stale op-id results leave the server transition ambiguous.
        if !matches!(
            &result,
            Err(_)
                | Ok((
                    Message::Rlerror(Rlerror {
                        ecode: P9_EOPIDSTALE
                    }),
                    _
                ))
        ) {
            cancellation.disarm();
        }
        result
    }

    /// Tests recent traffic, then performs a single-flight lease-gated probe.
    async fn conn_alive(conn: &Conn) -> bool {
        if conn.dead.load(Ordering::Acquire) {
            return false;
        }
        if conn.heard_within(LIVENESS_WINDOW) {
            return true;
        }
        let _guard = conn.probe_lock.lock().await;
        if conn.dead.load(Ordering::Acquire) {
            return false;
        }
        if conn.heard_within(LIVENESS_WINDOW) {
            return true;
        }
        matches!(
            runtime::timeout(PROBE_TIMEOUT, query_lineage_token(conn)).await,
            Ok(Ok(()))
        )
    }

    /// Tears down `conn` and wakes the reconnect supervisor.
    fn force_reprobe(&self, conn: &Arc<Conn>) {
        conn.shutdown();
        self.reconnect_notify.notify_waiters();
    }

    /// A one-shot send on a specific connection, bypassing the live-gate and
    /// state recording. Used during reconnect to replay the session.
    async fn send_raw(conn: &Conn, body: Message) -> ClientResult<Message> {
        Self::send_raw_at_tag(conn, None, body).await
    }

    /// Sends on a specific connection with an allocated or exact tag.
    async fn send_raw_at_tag(
        conn: &Conn,
        exact_tag: Option<u16>,
        body: Message,
    ) -> ClientResult<Message> {
        let permit = conn
            .writer_tx
            .reserve()
            .await
            .map_err(|_| ClientError::Disconnected)?;
        if conn.dead.load(Ordering::Acquire) {
            drop(permit);
            return Err(ClientError::Disconnected);
        }
        let (otx, orx) = oneshot::channel();
        let tag = match exact_tag {
            Some(tag) => Self::register_tag(conn, tag, otx)
                .map_err(|_| ClientError::Unexpected("raw tag already registered"))?,
            None => match Self::alloc_tag(conn, otx) {
                Ok(tag) => tag,
                Err(_) => {
                    drop(permit);
                    conn.shutdown();
                    return Err(ClientError::Disconnected);
                }
            },
        };
        let mut pending = PendingTag {
            conn,
            tag,
            dispatched: false,
        };
        if conn.dead.load(Ordering::Acquire) {
            drop(permit);
            return Err(ClientError::Disconnected);
        }
        let bytes = match P9Message::new(tag, body).to_bytes() {
            Ok(b) => b,
            Err(e) => return Err(ClientError::Codec(e)),
        };
        pending.mark_dispatched();
        permit.send(bytes);
        let frame = orx.await.map_err(|_| ClientError::Disconnected)?;
        let msg = P9Message::from_owned_bytes_ctx(frame, false).map_err(ClientError::Codec)?;
        if msg.tag != tag {
            return Err(ClientError::Unexpected("response tag"));
        }
        Ok(msg.body)
    }

    /// [`Self::send_raw`] plus the `Rlerror -> Errno` mapping, so replay can tell
    /// "this object is gone" (a server error) from a genuine protocol desync.
    async fn send_raw_rpc(conn: &Conn, body: Message) -> ClientResult<Message> {
        match Self::send_raw(conn, body).await? {
            Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
            other => Ok(other),
        }
    }

    /// Issue a request, turning a returned `Rlerror` into [`ClientError::Errno`].
    async fn rpc(&self, body: Message) -> ClientResult<Message> {
        match self.send_request(body).await? {
            Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
            other => Ok(other),
        }
    }

    /// Stateful [`Self::rpc`]; the guard covers replay-state bookkeeping.
    async fn rpc_stateful(
        &self,
        body: Message,
    ) -> ClientResult<(Message, tokio::sync::MutexGuard<'_, ()>)> {
        let (response, transition) = self.send_stateful_request(body).await?;
        match response {
            Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
            other => Ok((other, transition)),
        }
    }

    pub async fn attach(
        &self,
        fid: u32,
        afid: u32,
        uname: &str,
        aname: &str,
        n_uname: u32,
    ) -> ClientResult<Qid> {
        let (resp, _transition) = self
            .rpc_stateful(Message::Tattach(Tattach {
                fid,
                afid,
                uname: P9String::new(uname.as_bytes().to_vec()),
                aname: P9String::new(aname.as_bytes().to_vec()),
                n_uname,
            }))
            .await?;
        match resp {
            Message::Rattach(r) => {
                let mut st = self.state.lock().unwrap();
                st.fids.insert(
                    fid,
                    FidRecord {
                        inode_id: r.qid.path,
                        root_inode: r.qid.path,
                        n_uname,
                        uname: uname.as_bytes().to_vec(),
                        opened: None,
                    },
                );
                st.default_root = Some(r.qid.path);
                Ok(r.qid)
            }
            _ => Err(ClientError::Unexpected("attach")),
        }
    }

    /// Bind `fid` to an inode by id (no path walk), acting as `n_uname`. Used for
    /// per-user fids and reconnect replay.
    pub async fn rebind(&self, fid: u32, inode_id: u64, n_uname: u32) -> ClientResult<Qid> {
        let root_inode = self.state.lock().unwrap().default_root.unwrap_or(0);
        let (resp, _transition) = self
            .rpc_stateful(Message::Trebind(Trebind {
                fid,
                inode_id,
                root_inode,
                flags: 0,
                uname: P9String::new(Vec::new()),
                n_uname,
            }))
            .await?;
        match resp {
            Message::Rrebind(r) => {
                self.state.lock().unwrap().fids.insert(
                    fid,
                    FidRecord {
                        inode_id,
                        root_inode,
                        n_uname,
                        uname: Vec::new(),
                        opened: None,
                    },
                );
                Ok(r.qid)
            }
            _ => Err(ClientError::Unexpected("rebind")),
        }
    }

    pub async fn walk(&self, fid: u32, newfid: u32, names: &[&[u8]]) -> ClientResult<Vec<Qid>> {
        let wnames = names
            .iter()
            .map(|n| P9String::new(n.to_vec()))
            .collect::<Vec<_>>();
        let (resp, _transition) = self
            .rpc_stateful(Message::Twalk(Twalk {
                fid,
                newfid,
                nwname: wnames.len() as u16,
                wnames,
            }))
            .await?;
        match resp {
            Message::Rwalk(r) => {
                // Only a full walk creates `newfid` (a partial leaves it unset).
                if names.is_empty() || r.wqids.len() == names.len() {
                    let mut st = self.state.lock().unwrap();
                    let identity = st.fids.get(&fid).map(FidRecord::replay_identity);
                    let inode_id = if names.is_empty() {
                        st.fids.get(&fid).map(|rec| rec.inode_id)
                    } else {
                        r.wqids.last().map(|q| q.path)
                    };
                    if let (Some(inode_id), Some((n_uname, root_inode, uname))) =
                        (inode_id, identity)
                    {
                        st.fids.insert(
                            newfid,
                            FidRecord {
                                inode_id,
                                root_inode,
                                n_uname,
                                uname,
                                opened: None,
                            },
                        );
                    }
                }
                Ok(r.wqids)
            }
            _ => Err(ClientError::Unexpected("walk")),
        }
    }

    /// Full walk plus the final stat in one round trip. Records `newfid` like
    /// [`Self::walk`].
    pub async fn walk_getattr(
        &self,
        fid: u32,
        newfid: u32,
        names: &[&[u8]],
    ) -> ClientResult<(Vec<Qid>, Stat)> {
        let wnames = names
            .iter()
            .map(|n| P9String::new(n.to_vec()))
            .collect::<Vec<_>>();
        let (resp, _transition) = self
            .rpc_stateful(Message::Twalkgetattr(Twalkgetattr {
                fid,
                newfid,
                nwname: wnames.len() as u16,
                wnames,
            }))
            .await?;
        match resp {
            Message::Rwalkgetattr(r) => {
                {
                    let mut st = self.state.lock().unwrap();
                    let identity = st.fids.get(&fid).map(FidRecord::replay_identity);
                    let inode_id = if names.is_empty() {
                        st.fids.get(&fid).map(|rec| rec.inode_id)
                    } else {
                        r.wqids.last().map(|q| q.path)
                    };
                    if let (Some(inode_id), Some((n_uname, root_inode, uname))) =
                        (inode_id, identity)
                    {
                        st.fids.insert(
                            newfid,
                            FidRecord {
                                inode_id,
                                root_inode,
                                n_uname,
                                uname,
                                opened: None,
                            },
                        );
                    }
                }
                Ok((r.wqids, r.stat))
            }
            _ => Err(ClientError::Unexpected("walk_getattr")),
        }
    }

    pub async fn clunk(&self, fid: u32) -> ClientResult<()> {
        if self.stale_fids.contains(&fid) && self.clear_stale_fid(fid).await {
            return Ok(());
        }
        let (resp, _transition) = match self
            .send_stateful_request(Message::Tclunk(Tclunk { fid }))
            .await
        {
            Ok(result) => result,
            Err(error) => {
                if matches!(&error, ClientError::Errno(errno) if *errno == linux::ESTALE)
                    && self.clear_stale_fid(fid).await
                {
                    return Ok(());
                }
                return Err(error);
            }
        };
        // Remove local state before releasing the replay transition.
        let mut st = self.state.lock().unwrap();
        st.forget_fid(fid);
        drop(st);
        self.stale_fids.remove(&fid);
        match resp {
            Message::Rclunk(_) => Ok(()),
            Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
            _ => Err(ClientError::Unexpected("clunk")),
        }
    }

    async fn clear_stale_fid(&self, fid: u32) -> bool {
        let _transition = self.session_transition.lock().await;
        if self.terminal_errno.load(Ordering::Acquire) != 0 {
            return false;
        }
        if self.stale_fids.remove(&fid).is_none() {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        state.forget_fid(fid);
        true
    }

    pub async fn getattr(&self, fid: u32, mask: u64) -> ClientResult<Stat> {
        let resp = self
            .rpc(Message::Tgetattr(Tgetattr {
                fid,
                request_mask: mask,
            }))
            .await?;
        match resp {
            Message::Rgetattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("getattr")),
        }
    }

    pub async fn setattr(&self, ts: Tsetattr) -> ClientResult<()> {
        match self.rpc(Message::Tsetattr(ts)).await? {
            Message::Rsetattr(_) => Ok(()),
            _ => Err(ClientError::Unexpected("setattr")),
        }
    }

    /// Atomically allocate, punch, or zero a file range through the negotiated
    /// ZeroFS-private Tfallocate request.
    pub async fn fallocate(
        &self,
        fid: u32,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> ClientResult<()> {
        match self
            .rpc(Message::Tfallocate(Tfallocate {
                fid,
                offset,
                length,
                mode,
            }))
            .await?
        {
            Message::Rfallocate(_) => Ok(()),
            _ => Err(ClientError::Unexpected("fallocate")),
        }
    }

    /// Like [`Self::setattr`] but the reply carries the post-op stat.
    pub async fn setattr_attr(&self, ts: Tsetattr) -> ClientResult<Stat> {
        match self.rpc(Message::Tsetattrattr(ts)).await? {
            Message::Rsetattrattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("setattr_attr")),
        }
    }

    pub async fn lopen(&self, fid: u32, flags: u32) -> ClientResult<(Qid, u32)> {
        let (response, _transition) = self
            .rpc_stateful(Message::Tlopen(Tlopen { fid, flags }))
            .await?;
        match response {
            Message::Rlopen(r) => {
                if let Some(rec) = self.state.lock().unwrap().fids.get_mut(&fid) {
                    rec.opened = Some(flags);
                }
                Ok((r.qid, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lopen")),
        }
    }

    /// Open `fid`'s inode on a fresh `newfid` in one round trip; `fid` is
    /// untouched.
    pub async fn lopenat(&self, fid: u32, newfid: u32, flags: u32) -> ClientResult<(Qid, u32)> {
        let (resp, _transition) = self
            .rpc_stateful(Message::Tlopenat(Tlopenat { fid, newfid, flags }))
            .await?;
        match resp {
            Message::Rlopenat(r) => {
                let mut st = self.state.lock().unwrap();
                if let Some((n_uname, root_inode, uname)) =
                    st.fids.get(&fid).map(FidRecord::replay_identity)
                {
                    st.fids.insert(
                        newfid,
                        FidRecord {
                            inode_id: r.qid.path,
                            root_inode,
                            n_uname,
                            uname,
                            opened: Some(flags),
                        },
                    );
                }
                Ok((r.qid, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lopenat")),
        }
    }

    /// Open `fid`'s inode on `newfid` like [`Self::lopenat`] and prefetch up to
    /// `count` bytes from offset 0 in the same round trip. Returns the qid, the
    /// iounit, the prefetched bytes, and whether they reach EOF (the whole file fit in
    /// `count`). The inline read is best-effort: a server-side read error yields empty
    /// data with `eof = false`, and the open still succeeds.
    pub async fn lopenatread(
        &self,
        fid: u32,
        newfid: u32,
        flags: u32,
        count: u32,
    ) -> ClientResult<(Qid, u32, Bytes, bool)> {
        let (resp, _transition) = self
            .rpc_stateful(Message::Tlopenatread(Tlopenatread {
                fid,
                newfid,
                flags,
                count,
            }))
            .await?;
        match resp {
            Message::Rlopenatread(r) => {
                let mut st = self.state.lock().unwrap();
                if let Some((n_uname, root_inode, uname)) =
                    st.fids.get(&fid).map(FidRecord::replay_identity)
                {
                    st.fids.insert(
                        newfid,
                        FidRecord {
                            inode_id: r.qid.path,
                            root_inode,
                            n_uname,
                            uname,
                            opened: Some(flags),
                        },
                    );
                }
                drop(st);
                Ok((r.qid, r.iounit, r.data.0, r.eof != 0))
            }
            _ => Err(ClientError::Unexpected("lopenatread")),
        }
    }

    pub async fn lcreate(
        &self,
        fid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> ClientResult<(Qid, u32)> {
        let (resp, _transition) = self
            .rpc_stateful(Message::Tlcreate(Tlcreate {
                fid,
                name: P9String::new(name.to_vec()),
                flags,
                mode,
                gid,
            }))
            .await?;
        match resp {
            Message::Rlcreate(r) => {
                // Replay uses the created inode and strips create-only flags.
                let reopen = flags & !(linux::O_CREAT | linux::O_EXCL | linux::O_TRUNC);
                let mut st = self.state.lock().unwrap();
                if let Some(rec) = st.fids.get_mut(&fid) {
                    rec.inode_id = r.qid.path;
                    rec.opened = Some(reopen);
                }
                Ok((r.qid, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lcreate")),
        }
    }

    /// Create and open `name` under `dfid`, returning the post-op stat in one
    /// round trip (`Tlcreateattr`).
    /// Unlike [`Self::lcreate`], `dfid` is left untouched (the file opens on
    /// `newfid`).
    pub async fn lcreateattr(
        &self,
        dfid: u32,
        newfid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> ClientResult<(Stat, u32)> {
        let (resp, _transition) = self
            .rpc_stateful(Message::Tlcreateattr(Tlcreateattr {
                dfid,
                newfid,
                name: P9String::new(name.to_vec()),
                flags,
                mode,
                gid,
            }))
            .await?;
        match resp {
            Message::Rlcreateattr(r) => {
                let reopen = flags & !(linux::O_CREAT | linux::O_EXCL | linux::O_TRUNC);
                let mut st = self.state.lock().unwrap();
                if let Some((n_uname, root_inode, uname)) =
                    st.fids.get(&dfid).map(FidRecord::replay_identity)
                {
                    st.fids.insert(
                        newfid,
                        FidRecord {
                            inode_id: r.stat.qid.path,
                            root_inode,
                            n_uname,
                            uname,
                            opened: Some(reopen),
                        },
                    );
                }
                Ok((r.stat, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lcreateattr")),
        }
    }

    /// Read up to `size` bytes at `offset`, looping over multiple Tread requests
    /// when `size` exceeds the negotiated msize. Stops early on a short read (EOF).
    pub async fn read(&self, fid: u32, offset: u64, size: u32) -> ClientResult<Vec<u8>> {
        Ok(self.read_bytes(fid, offset, size).await?.into())
    }

    /// Like [`Self::read`] but returns the payload as [`Bytes`]. The Rread
    /// payload already arrives as `Bytes`, so a single-round-trip read returns
    /// it with no copy; only a multi-chunk read concatenates.
    pub async fn read_bytes(&self, fid: u32, offset: u64, size: u32) -> ClientResult<Bytes> {
        if size == 0 {
            return Ok(Bytes::new());
        }
        let size = size as usize;
        let max = self.max_io().max(1) as usize;
        let first_count = size.min(max);
        let first = self.read_once(fid, offset, first_count as u32).await?;
        if size <= max || first.len() < first_count {
            return Ok(first);
        }
        // `size` can be `u32::MAX`; cap the initial allocation at two response chunks.
        let mut out = BytesMut::with_capacity(size.min(max.saturating_mul(2)));
        let mut off = offset + first.len() as u64;
        out.extend_from_slice(&first);
        while out.len() < size {
            let want = (size - out.len()).min(max);
            let data = self.read_once(fid, off, want as u32).await?;
            let got = data.len();
            out.extend_from_slice(&data);
            off += got as u64;
            if got < want {
                break;
            }
        }
        Ok(out.freeze())
    }

    async fn read_once(&self, fid: u32, offset: u64, count: u32) -> ClientResult<Bytes> {
        let resp = self
            .rpc(Message::Tread(Tread { fid, offset, count }))
            .await?;
        match resp {
            Message::Rread(r) if r.data.len() <= count as usize => Ok(r.data.0),
            Message::Rread(_) => Err(ClientError::Unexpected("read count")),
            _ => Err(ClientError::Unexpected("read")),
        }
    }

    /// Write all of `data` at `offset`, splitting into multiple Twrite requests
    /// when it exceeds the negotiated msize. Returns the total bytes written.
    pub async fn write(&self, fid: u32, offset: u64, data: &[u8]) -> ClientResult<u64> {
        let len = data.len();
        self.write_chunks(fid, offset, len, move |start, end| {
            Bytes::copy_from_slice(&data[start..end])
        })
        .await
    }

    /// Like [`Self::write`], but takes ownership of an existing [`Bytes`] buffer.
    /// Chunk selection uses shallow `Bytes` slices, adding no staging copy
    /// before each request is serialized.
    pub async fn write_bytes(&self, fid: u32, offset: u64, data: Bytes) -> ClientResult<u64> {
        let len = data.len();
        self.write_chunks(fid, offset, len, move |start, end| data.slice(start..end))
            .await
    }

    async fn write_chunks<F>(
        &self,
        fid: u32,
        offset: u64,
        len: usize,
        mut chunk: F,
    ) -> ClientResult<u64>
    where
        F: FnMut(usize, usize) -> Bytes,
    {
        let mut written = 0usize;
        let max = self.max_write_payload().max(1) as usize;
        while written < len {
            let end = written + (len - written).min(max);
            let data = chunk(written, end);
            debug_assert_eq!(data.len(), end - written);
            let attempted = data.len();
            let n = self.write_chunk(fid, offset + written as u64, data).await? as usize;
            written += n;
            if n < attempted {
                break;
            }
        }
        Ok(written as u64)
    }

    async fn write_chunk(&self, fid: u32, offset: u64, data: Bytes) -> ClientResult<u32> {
        let attempted = u32::try_from(data.len())
            .expect("write chunks are bounded by the negotiated u32 msize");
        let resp = self
            .rpc(Message::Twrite(Twrite {
                fid,
                offset,
                count: attempted,
                data: DekuBytes::from(data),
            }))
            .await?;
        match resp {
            Message::Rwrite(r) if r.count <= attempted => Ok(r.count),
            Message::Rwrite(_) => Err(ClientError::Unexpected("write count")),
            _ => Err(ClientError::Unexpected("write")),
        }
    }

    pub async fn readdir(&self, fid: u32, offset: u64, count: u32) -> ClientResult<Vec<DirEntry>> {
        let resp = self
            .rpc(Message::Treaddir(Treaddir { fid, offset, count }))
            .await?;
        match resp {
            Message::Rreaddir(r) => r.to_entries().map_err(ClientError::Codec),
            _ => Err(ClientError::Unexpected("readdir")),
        }
    }

    /// Like [`Self::readdir`] but each entry carries its full stat
    /// (`Treaddirattr`).
    pub async fn readdirplus(
        &self,
        fid: u32,
        offset: u64,
        count: u32,
    ) -> ClientResult<Vec<DirEntryPlus>> {
        let resp = self
            .rpc(Message::Treaddirattr(Treaddirattr { fid, offset, count }))
            .await?;
        match resp {
            Message::Rreaddirattr(r) => r.to_entries().map_err(ClientError::Codec),
            _ => Err(ClientError::Unexpected("readdirplus")),
        }
    }

    pub async fn mkdir(&self, dfid: u32, name: &[u8], mode: u32, gid: u32) -> ClientResult<Qid> {
        let resp = self
            .rpc(Message::Tmkdir(Tmkdir {
                dfid,
                name: P9String::new(name.to_vec()),
                mode,
                gid,
            }))
            .await?;
        match resp {
            Message::Rmkdir(r) => Ok(r.qid),
            _ => Err(ClientError::Unexpected("mkdir")),
        }
    }

    /// Like [`Self::mkdir`] but the reply carries the new directory's full stat.
    pub async fn mkdir_attr(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        gid: u32,
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc(Message::Tmkdirattr(Tmkdir {
                dfid,
                name: P9String::new(name.to_vec()),
                mode,
                gid,
            }))
            .await?;
        match resp {
            Message::Rmkdirattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("mkdir_attr")),
        }
    }

    pub async fn symlink(
        &self,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        gid: u32,
    ) -> ClientResult<Qid> {
        let resp = self
            .rpc(Message::Tsymlink(Tsymlink {
                dfid,
                name: P9String::new(name.to_vec()),
                symtgt: P9String::new(target.to_vec()),
                gid,
            }))
            .await?;
        match resp {
            Message::Rsymlink(r) => Ok(r.qid),
            _ => Err(ClientError::Unexpected("symlink")),
        }
    }

    /// Like [`Self::symlink`] but the reply carries the new link's full stat.
    pub async fn symlink_attr(
        &self,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        gid: u32,
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc(Message::Tsymlinkattr(Tsymlink {
                dfid,
                name: P9String::new(name.to_vec()),
                symtgt: P9String::new(target.to_vec()),
                gid,
            }))
            .await?;
        match resp {
            Message::Rsymlinkattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("symlink_attr")),
        }
    }

    pub async fn mknod(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
    ) -> ClientResult<Qid> {
        let resp = self
            .rpc(Message::Tmknod(Tmknod {
                dfid,
                name: P9String::new(name.to_vec()),
                mode,
                major,
                minor,
                gid,
            }))
            .await?;
        match resp {
            Message::Rmknod(r) => Ok(r.qid),
            _ => Err(ClientError::Unexpected("mknod")),
        }
    }

    /// Like [`Self::mknod`] but the reply carries the new node's full stat.
    pub async fn mknod_attr(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc(Message::Tmknodattr(Tmknod {
                dfid,
                name: P9String::new(name.to_vec()),
                mode,
                major,
                minor,
                gid,
            }))
            .await?;
        match resp {
            Message::Rmknodattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("mknod_attr")),
        }
    }

    pub async fn readlink(&self, fid: u32) -> ClientResult<Vec<u8>> {
        match self.rpc(Message::Treadlink(Treadlink { fid })).await? {
            Message::Rreadlink(r) => Ok(r.target.data),
            _ => Err(ClientError::Unexpected("readlink")),
        }
    }

    pub async fn link(&self, dfid: u32, fid: u32, name: &[u8]) -> ClientResult<()> {
        let resp = self
            .rpc(Message::Tlink(Tlink {
                dfid,
                fid,
                name: P9String::new(name.to_vec()),
            }))
            .await?;
        match resp {
            Message::Rlink(_) => Ok(()),
            _ => Err(ClientError::Unexpected("link")),
        }
    }

    /// Like [`Self::link`] but the reply carries the linked inode's post-op stat
    /// (updated nlink).
    pub async fn link_attr(&self, dfid: u32, fid: u32, name: &[u8]) -> ClientResult<Stat> {
        let resp = self
            .rpc(Message::Tlinkattr(Tlink {
                dfid,
                fid,
                name: P9String::new(name.to_vec()),
            }))
            .await?;
        match resp {
            Message::Rlinkattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("link_attr")),
        }
    }

    pub async fn renameat(
        &self,
        olddirfid: u32,
        oldname: &[u8],
        newdirfid: u32,
        newname: &[u8],
    ) -> ClientResult<()> {
        let resp = self
            .rpc(Message::Trenameat(Trenameat {
                olddirfid,
                oldname: P9String::new(oldname.to_vec()),
                newdirfid,
                newname: P9String::new(newname.to_vec()),
            }))
            .await?;
        match resp {
            Message::Rrenameat(_) => Ok(()),
            _ => Err(ClientError::Unexpected("renameat")),
        }
    }

    pub async fn unlinkat(&self, dirfid: u32, name: &[u8], flags: u32) -> ClientResult<()> {
        let resp = self
            .rpc(Message::Tunlinkat(Tunlinkat {
                dirfid,
                name: P9String::new(name.to_vec()),
                flags,
            }))
            .await?;
        match resp {
            Message::Runlinkat(_) => Ok(()),
            _ => Err(ClientError::Unexpected("unlinkat")),
        }
    }

    /// Verifies fsync for all fids associated with one inode. `primary` carries
    /// `Tfsyncdur`; the oldest recorded lineage token determines the result.
    pub async fn fsync_inode(&self, fids: &[u32], primary: u32, datasync: u32) -> ClientResult<()> {
        self.validate_fids(fids.iter().copied())?;
        // Generations prevent the result from clearing writes concurrent with fsync.
        let mut token: Option<u64> = None;
        let mut snaps: Vec<(u32, u64)> = Vec::with_capacity(fids.len());
        for &fid in fids {
            let (oldest, generation) = self.snapshot_unsynced(fid);
            if let Some(t) = oldest {
                token = Some(token.map_or(t, |w| w.min(t)));
            }
            snaps.push((fid, generation));
        }
        match self
            .rpc(Message::Tfsyncdur(Tfsyncdur {
                fid: primary,
                datasync,
                token: token.unwrap_or(0),
            }))
            .await
        {
            Ok(Message::Rfsync(_)) => {
                for (fid, generation) in snaps {
                    self.clear_unsynced_if_unchanged(fid, generation);
                }
                Ok(())
            }
            Ok(_) => Err(ClientError::Unexpected("fsync")),
            Err(ClientError::Errno(e)) if e == linux::ESTALE => {
                // Preserve stale obligations until replacement writes arrive.
                for (fid, generation) in snaps {
                    self.report_unsynced_if_unchanged(fid, generation);
                }
                Err(ClientError::Errno(e))
            }
            Err(e) => Err(e),
        }
    }

    /// Verified fsync for one fid. Multi-fid inode users call [`Self::fsync_inode`].
    pub async fn fsync(&self, fid: u32, datasync: u32) -> ClientResult<()> {
        self.fsync_inode(&[fid], fid, datasync).await
    }

    /// Filesystem-wide barrier for this client's outstanding durability obligations.
    pub async fn fsync_all(&self, primary: u32, datasync: u32) -> ClientResult<()> {
        let mut fids = vec![primary];
        for entry in &self.unsynced {
            let fid = *entry.key();
            if fid != primary {
                fids.push(fid);
            }
        }
        self.fsync_inode(&fids, primary, datasync).await
    }

    pub async fn statfs(&self, fid: u32) -> ClientResult<Rstatfs> {
        match self.rpc(Message::Tstatfs(Tstatfs { fid })).await? {
            Message::Rstatfs(r) => Ok(r),
            _ => Err(ClientError::Unexpected("statfs")),
        }
    }

    /// Acquire or release a POSIX record lock. Returns the lock status; note
    /// that a non-blocking conflict surfaces as `Err(ClientError::Errno(EAGAIN))`
    /// (the server replies `Rlerror`), whereas a blocking request that cannot be
    /// granted returns `Ok(LockStatus::Blocked)`.
    #[allow(clippy::too_many_arguments)]
    pub async fn lock(
        &self,
        fid: u32,
        lock_type: LockType,
        flags: u32,
        start: u64,
        length: u64,
        proc_id: u32,
        client_id: &[u8],
    ) -> ClientResult<LockStatus> {
        let (resp, _transition) = self
            .rpc_stateful(Message::Tlock(Tlock {
                fid,
                lock_type,
                flags,
                start,
                length,
                proc_id,
                client_id: P9String::new(client_id.to_vec()),
            }))
            .await?;
        match resp {
            Message::Rlock(r) => {
                let mut st = self.state.lock().unwrap();
                match lock_type {
                    LockType::Unlock if matches!(r.status, LockStatus::Success) => {
                        unlock_recorded_range(&mut st.locks, fid, start, length);
                    }
                    _ if matches!(r.status, LockStatus::Success) => {
                        replace_recorded_lock(
                            &mut st.locks,
                            LockRecord {
                                fid,
                                lock_type,
                                start,
                                length,
                                proc_id,
                                client_id: client_id.to_vec(),
                            },
                        );
                    }
                    _ => {}
                }
                Ok(r.status)
            }
            _ => Err(ClientError::Unexpected("lock")),
        }
    }

    /// Test for a conflicting POSIX record lock.
    pub async fn getlock(
        &self,
        fid: u32,
        lock_type: LockType,
        start: u64,
        length: u64,
        proc_id: u32,
        client_id: &[u8],
    ) -> ClientResult<Rgetlock> {
        let resp = self
            .rpc(Message::Tgetlock(Tgetlock {
                fid,
                lock_type,
                start,
                length,
                proc_id,
                client_id: P9String::new(client_id.to_vec()),
            }))
            .await?;
        match resp {
            Message::Rgetlock(r) => Ok(r),
            _ => Err(ClientError::Unexpected("getlock")),
        }
    }
}

impl Drop for NinePClient {
    fn drop(&mut self) {
        // Reader and writer tasks retain `Arc<Conn>` until explicit shutdown.
        self.conn.load().shutdown();
        self.reconnect_notify.notify_waiters();
    }
}

/// Applies an unlock to a recorded lock. Zero length extends to EOF.
fn subtract_lock_record(
    held: LockRecord,
    unlock_start: u64,
    unlock_length: u64,
) -> Vec<LockRecord> {
    ninep_proto::subtract_lock_range(held.start, held.length, unlock_start, unlock_length)
        .into_iter()
        .map(|(start, length)| LockRecord {
            start,
            length,
            ..held.clone()
        })
        .collect()
}

fn unlock_recorded_range(locks: &mut Vec<LockRecord>, fid: u32, start: u64, length: u64) {
    let prior = std::mem::take(locks);
    for held in prior {
        if held.fid == fid {
            locks.extend(subtract_lock_record(held, start, length));
        } else {
            locks.push(held);
        }
    }
}

/// Applies POSIX replacement semantics to this fid's recorded locks.
fn replace_recorded_lock(locks: &mut Vec<LockRecord>, replacement: LockRecord) {
    unlock_recorded_range(
        locks,
        replacement.fid,
        replacement.start,
        replacement.length,
    );
    locks.push(replacement);
}

enum DialedTransport {
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        read: Box<dyn AsyncRead + Unpin + Send>,
        write: Box<dyn AsyncWrite + Unpin + Send>,
    },
    #[cfg(target_arch = "wasm32")]
    WebSocket(web_transport::WebSocketIo),
}

/// Open a connection to the target. Native sockets are byte streams and are
/// framed by the reader; browser WebSockets already carry complete frames.
async fn dial(target: &Target) -> ClientResult<DialedTransport> {
    match target {
        #[cfg(not(target_arch = "wasm32"))]
        Target::Tcp(addr) => {
            let stream = runtime::timeout(PROBE_TIMEOUT, TcpStream::connect(addr))
                .await
                .map_err(|_| ClientError::Disconnected)?
                .map_err(|_| ClientError::Disconnected)?;
            configure_tcp(stream)
        }
        #[cfg(not(target_arch = "wasm32"))]
        Target::TcpHost(endpoint) => {
            // Resolution is per probe and isolated to this target.
            let stream = runtime::timeout(PROBE_TIMEOUT, TcpStream::connect(endpoint.as_str()))
                .await
                .map_err(|_| ClientError::Disconnected)?
                .map_err(|_| ClientError::Disconnected)?;
            configure_tcp(stream)
        }
        #[cfg(not(target_arch = "wasm32"))]
        Target::Unix(path) => {
            let stream = runtime::timeout(PROBE_TIMEOUT, UnixStream::connect(path))
                .await
                .map_err(|_| ClientError::Disconnected)?
                .map_err(|_| ClientError::Disconnected)?;
            let (r, w) = stream.into_split();
            Ok(DialedTransport::Native {
                read: Box::new(r),
                write: Box::new(w),
            })
        }
        #[cfg(target_arch = "wasm32")]
        Target::WebSocket(url) => web_transport::connect(url)
            .await
            .map(DialedTransport::WebSocket),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn configure_tcp(stream: TcpStream) -> ClientResult<DialedTransport> {
    stream.set_nodelay(true).ok();
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(45))
        .with_interval(Duration::from_secs(15))
        .with_retries(4);
    let _ = socket2::SockRef::from(&stream).set_tcp_keepalive(&keepalive);
    let (r, w) = stream.into_split();
    Ok(DialedTransport::Native {
        read: Box::new(r),
        write: Box::new(w),
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod target_dial_tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    async fn recv_message(stream: &mut TcpStream) -> P9Message {
        let mut size = [0u8; P9_SIZE_FIELD_LEN];
        stream.read_exact(&mut size).await.unwrap();
        let frame_len = u32::from_le_bytes(size) as usize;
        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(&size);
        frame.resize(frame_len, 0);
        stream
            .read_exact(&mut frame[P9_SIZE_FIELD_LEN..])
            .await
            .unwrap();
        P9Message::from_bytes((&frame, 0)).unwrap().1
    }

    async fn send_message(stream: &mut TcpStream, tag: u16, body: Message) {
        stream
            .write_all(&P9Message::new(tag, body).to_bytes().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn hostname_lookup_failure_is_isolated_from_healthy_target_dials() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let healthy_addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

        let targets = [
            Target::TcpHost("not a valid hostname:5564".to_string()),
            Target::Tcp(healthy_addr),
        ];
        let mut dials = FuturesUnordered::new();
        for target in targets {
            dials.push(async move { dial(&target).await });
        }

        let mut successes = 0;
        let mut failures = 0;
        while let Some(result) = dials.next().await {
            match result {
                Ok(transport) => {
                    successes += 1;
                    drop(transport);
                }
                Err(_) => failures += 1,
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(failures, 1);
        accept.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_rejects_a_smaller_negotiated_msize() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let version = recv_message(&mut stream).await;
            assert!(matches!(version.body, Message::Tversion(_)));
            send_message(
                &mut stream,
                version.tag,
                Message::Rversion(Rversion {
                    msize: 4096,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            )
            .await;

            let lineage = recv_message(&mut stream).await;
            assert!(matches!(lineage.body, Message::Tgetlineage(_)));
            send_message(
                &mut stream,
                lineage.tag,
                Message::Rgetlineage(Rgetlineage {
                    token: 1,
                    writer_epoch: 1,
                }),
            )
            .await;
        });

        let result = NinePClient::connect_once(
            &Target::Tcp(addr),
            8192,
            Some(8192),
            Arc::new(Notify::new()),
            Arc::new(TrafficCounters::default()),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        assert!(matches!(result, Err(ClientError::Unexpected("version"))));
        server.await.unwrap();
    }
}

/// Negotiates the required private dialect and verifies serving authority.
async fn negotiate_on(conn: &Conn, requested: u32) -> ClientResult<u32> {
    let negotiated = version_on(conn, requested).await?;
    query_lineage_token(conn).await?;
    debug!("ZeroFS 9P dialect negotiated, msize={negotiated}");
    Ok(negotiated)
}

/// Performs the session-resetting `Tversion` exchange without a lineage query.
async fn version_on(conn: &Conn, requested: u32) -> ClientResult<u32> {
    // 9P requires NOTAG for Tversion.
    match NinePClient::send_raw_at_tag(
        conn,
        Some(NOTAG),
        Message::Tversion(Tversion {
            msize: requested,
            version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
        }),
    )
    .await?
    {
        Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
        Message::Rversion(rv) => {
            if rv.version.data != VERSION_9P2000L_ZEROFS {
                warn!(
                    "server did not accept the required ZeroFS dialect: {:?}",
                    rv.version.as_str().unwrap_or("<non-UTF-8>")
                );
                return Err(ClientError::Unexpected("version"));
            }
            // Linux v9fs requires msize >= 4096.
            let negotiated = rv.msize.min(requested);
            if negotiated < 4096 {
                warn!("server negotiated msize {negotiated} below minimum 4096");
                return Err(ClientError::Unexpected("version"));
            }
            Ok(negotiated)
        }
        _ => Err(ClientError::Unexpected("version")),
    }
}

/// Records the lease-gated durability lineage and writer epoch.
async fn query_lineage_token(conn: &Conn) -> ClientResult<()> {
    match NinePClient::send_raw(conn, Message::Tgetlineage(Tgetlineage)).await? {
        Message::Rgetlineage(r) => {
            conn.lineage_token.store(r.token, Ordering::Relaxed);
            conn.writer_epoch.store(r.writer_epoch, Ordering::Relaxed);
            Ok(())
        }
        Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
        _ => Err(ClientError::Unexpected("getlineage")),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_writer(
    write: Box<dyn AsyncWrite + Unpin + Send>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    conn: Arc<Conn>,
    reconnect: Arc<Notify>,
) {
    runtime::spawn(async move {
        let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, write);
        loop {
            tokio::select! {
                biased;
                // Reader shutdown covers an idle writer.
                _ = conn.writer_shutdown.notified() => break,
                maybe = rx.recv() => {
                    let Some(frame) = maybe else { break };
                    conn.counters.bytes_sent.fetch_add(frame.len() as u64, Ordering::Relaxed);
                    conn.counters.operations.fetch_add(1, Ordering::Relaxed);
                    if writer.write_all(&frame).await.is_err() {
                        break;
                    }
                    let mut failed = false;
                    while let Ok(more) = rx.try_recv() {
                        conn.counters.bytes_sent.fetch_add(more.len() as u64, Ordering::Relaxed);
                        conn.counters.operations.fetch_add(1, Ordering::Relaxed);
                        if writer.write_all(&more).await.is_err() {
                            failed = true;
                            break;
                        }
                    }
                    if failed || writer.flush().await.is_err() {
                        break;
                    }
                }
            }
        }
        conn.dead.store(true, Ordering::Release);
        reconnect.notify_waiters();
    });
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_reader(read: Box<dyn AsyncRead + Unpin + Send>, conn: Arc<Conn>, reconnect: Arc<Notify>) {
    runtime::spawn(async move {
        let mut framed = LengthDelimitedCodec::builder()
            .little_endian()
            .length_field_offset(0)
            .length_field_length(P9_SIZE_FIELD_LEN)
            .length_adjustment(0)
            .num_skip(0)
            .max_frame_length(P9_MAX_MSIZE as usize)
            .new_read(read);

        loop {
            let next = tokio::select! {
                biased;
                // `shutdown` may discard a healthy connection.
                _ = conn.reader_shutdown.notified() => break,
                next = framed.next() => next,
            };
            let frame = match next {
                Some(Ok(buf)) => buf.freeze(),
                Some(Err(e)) => {
                    warn!("9P client read failed: {e}");
                    break;
                }
                None => break,
            };
            conn.deliver(frame);
        }

        conn.connection_lost(&reconnect);
    });
}

#[cfg(test)]
mod lock_range_tests {
    use super::*;

    fn held(start: u64, length: u64) -> LockRecord {
        LockRecord {
            fid: 9,
            lock_type: LockType::WriteLock,
            start,
            length,
            proc_id: 42,
            client_id: b"client".to_vec(),
        }
    }

    #[test]
    fn lock_record_subtraction_preserves_metadata() {
        let survivors = subtract_lock_record(held(10, 90), 30, 20);
        assert_eq!(
            survivors
                .iter()
                .map(|record| (record.start, record.length))
                .collect::<Vec<_>>(),
            vec![(10, 20), (50, 50)]
        );
        assert!(survivors.iter().all(|record| {
            record.fid == 9
                && matches!(record.lock_type, LockType::WriteLock)
                && record.proc_id == 42
                && record.client_id == b"client"
        }));
    }

    #[test]
    fn subrange_relock_preserves_tails_for_replay() {
        let mut locks = vec![held(0, 100)];
        replace_recorded_lock(
            &mut locks,
            LockRecord {
                lock_type: LockType::ReadLock,
                start: 20,
                length: 30,
                ..held(0, 0)
            },
        );
        locks.sort_by_key(|lock| lock.start);
        let shape: Vec<_> = locks
            .iter()
            .map(|lock| {
                (
                    lock.start,
                    lock.length,
                    match lock.lock_type {
                        LockType::ReadLock => "read",
                        LockType::WriteLock => "write",
                        LockType::Unlock => "unlock",
                    },
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![(0, 20, "write"), (20, 30, "read"), (50, 50, "write")]
        );
    }
}

#[cfg(test)]
mod durability_tracking_tests {
    use super::Unsynced;

    #[test]
    fn fsync_clears_the_obligation_when_quiescent() {
        let mut u = Unsynced::default();
        u.note(7);
        let (oldest, generation) = u.snapshot();
        assert_eq!(oldest, Some(7));
        u.clear_if_unchanged(generation);
        assert_eq!(u.snapshot().0, None);
    }

    #[test]
    fn a_repeated_fsync_on_one_fid_keeps_failing_until_a_redo() {
        let mut u = Unsynced::default();
        u.note(1);
        let (oldest, generation) = u.snapshot();
        assert_eq!(oldest, Some(1));
        u.report_if_unchanged(generation);
        assert_eq!(
            u.snapshot().0,
            Some(1),
            "a reported loss must persist so a repeated fsync still fails"
        );
        u.note(2);
        assert_eq!(
            u.snapshot().0,
            Some(2),
            "a redo after a report advances the obligation (no livelock)"
        );
        let (_, gen2) = u.snapshot();
        u.clear_if_unchanged(gen2);
        assert_eq!(u.snapshot().0, None);
    }

    #[test]
    fn an_estale_and_redo_on_one_fid_does_not_discharge_a_sibling_fid() {
        use std::collections::HashMap;
        let mut map: HashMap<u32, Unsynced> = HashMap::new();
        map.entry(10).or_default().note(1);
        map.entry(20).or_default().note(1);
        let (_, g) = map.get(&10).unwrap().snapshot();
        map.get_mut(&10).unwrap().report_if_unchanged(g);
        map.get_mut(&10).unwrap().note(2);
        let (_, g2) = map.get(&10).unwrap().snapshot();
        map.get_mut(&10).unwrap().clear_if_unchanged(g2);
        if map.get(&10).unwrap().snapshot().0.is_none() {
            map.remove(&10);
        }
        assert_eq!(
            map.get(&20).unwrap().snapshot().0,
            Some(1),
            "fid 10's ESTALE+redo+success cycle must not touch fid 20"
        );
    }

    #[test]
    fn fsync_does_not_erase_a_write_from_its_window() {
        let mut u = Unsynced::default();
        u.note(7);
        let (_, generation) = u.snapshot();
        u.note(7);
        u.clear_if_unchanged(generation);
        assert_eq!(
            u.snapshot().0,
            Some(7),
            "a write racing the fsync must stay tracked"
        );
    }

    #[test]
    fn oldest_token_is_kept_across_a_lineage_change() {
        let mut u = Unsynced::default();
        u.note(5);
        u.note(9);
        assert_eq!(u.snapshot().0, Some(5), "the oldest (riskiest) token wins");
    }

    #[test]
    fn nothing_tracked_reports_none() {
        let u = Unsynced::default();
        assert_eq!(u.snapshot(), (None, 0));
    }
}

#[cfg(test)]
fn test_conn_with_receiver() -> (Arc<Conn>, mpsc::Receiver<Vec<u8>>) {
    let (writer_tx, rx) = mpsc::channel(1);
    let conn = Arc::new(Conn {
        writer_tx,
        pending: DashMap::new(),
        tag_ctr: AtomicU16::new(0),
        lineage_token: AtomicU64::new(0),
        writer_epoch: AtomicU64::new(0),
        dead: AtomicBool::new(false),
        base: runtime::Clock::now(),
        last_alive: AtomicU64::new(0),
        probe_lock: tokio::sync::Mutex::new(()),
        writer_shutdown: Notify::new(),
        reader_shutdown: Notify::new(),
        counters: Arc::new(TrafficCounters::default()),
    });
    (conn, rx)
}

#[cfg(test)]
mod session_transition_tests {
    use super::*;

    type TestRequests = mpsc::Receiver<Vec<u8>>;
    const REPLAY: u8 = P9_REBIND_REPLAY;
    const OPEN_REPLAY: u8 = REPLAY | P9_REBIND_OPENED;

    fn test_conn() -> Arc<Conn> {
        test_conn_with_receiver().0
    }

    fn test_client(conn: Arc<Conn>) -> Arc<NinePClient> {
        Arc::new(NinePClient {
            targets: Vec::new(),
            conn: ArcSwap::new(conn),
            live: AtomicBool::new(true),
            terminal_errno: AtomicU32::new(0),
            live_notify: Notify::new(),
            reconnect_notify: Arc::new(Notify::new()),
            msize: 8192,
            msize_mismatch_warned: Arc::new(AtomicBool::new(false)),
            fid_ctr: AtomicU32::new(1),
            fid_free: Mutex::new(Vec::new()),
            state: Mutex::new(SessionState::default()),
            stale_fids: DashSet::new(),
            session_transition: tokio::sync::Mutex::new(()),
            unsynced: DashMap::new(),
            counters: Arc::new(TrafficCounters::default()),
        })
    }

    async fn next_request(requests: &mut TestRequests) -> Option<P9Message> {
        requests
            .recv()
            .await
            .map(|frame| P9Message::from_bytes((&frame, 0)).unwrap().1)
    }

    async fn recv_request(requests: &mut TestRequests, description: &str) -> P9Message {
        next_request(requests).await.expect(description)
    }

    async fn recv_op_request(requests: &mut TestRequests, description: &str) -> P9Message {
        let frame = requests.recv().await.expect(description);
        P9Message::from_bytes_ctx(&frame, true).unwrap()
    }

    fn reply(conn: &Conn, tag: u16, body: Message) {
        conn.deliver(Bytes::from(P9Message::new(tag, body).to_bytes().unwrap()));
    }

    fn qid(path: u64) -> Qid {
        Qid {
            type_: 0,
            version: 0,
            path,
        }
    }

    fn inode_fid(inode_id: u64, root_inode: u64, n_uname: u32, opened: Option<u32>) -> FidRecord {
        FidRecord {
            inode_id,
            root_inode,
            n_uname,
            uname: Vec::new(),
            opened,
        }
    }

    fn attach_fid(root_inode: u64, n_uname: u32) -> FidRecord {
        FidRecord {
            inode_id: root_inode,
            root_inode,
            n_uname,
            uname: Vec::new(),
            opened: None,
        }
    }

    fn write_lock(fid: u32, start: u64, length: u64) -> LockRecord {
        LockRecord {
            fid,
            lock_type: LockType::WriteLock,
            start,
            length,
            proc_id: 1,
            client_id: b"owner".to_vec(),
        }
    }

    #[test]
    fn forgetting_a_fid_removes_only_its_locks() {
        let mut state = SessionState::default();
        state.fids.insert(7, inode_fid(70, 0, 0, None));
        state.fids.insert(8, inode_fid(80, 0, 0, None));
        state.locks.extend([
            write_lock(7, 0, 10),
            write_lock(8, 0, 10),
            write_lock(7, 20, 10),
        ]);

        state.forget_fid(7);

        assert!(!state.fids.contains_key(&7));
        assert!(state.fids.contains_key(&8));
        assert_eq!(state.locks.len(), 1);
        assert_eq!(state.locks[0].fid, 8);
    }

    async fn replay_rebind_error(
        client: &NinePClient,
        conn: &Arc<Conn>,
        mut requests: TestRequests,
        expected: (u32, u64, u64, u8),
        ecode: u32,
    ) -> ClientResult<()> {
        let responder_conn = Arc::clone(conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "rebind request").await;
            let Message::Trebind(rebind) = request.body else {
                panic!("expected Trebind");
            };
            let actual = (rebind.fid, rebind.inode_id, rebind.root_inode, rebind.flags);
            assert_eq!(actual, expected);
            reply(
                &responder_conn,
                request.tag,
                Message::Rlerror(Rlerror { ecode }),
            );
        });
        let result = client.replay(conn).await;
        responder.await.unwrap();
        result
    }

    #[tokio::test]
    async fn reconnect_winner_rejects_an_old_stateful_response() {
        let old = test_conn();
        let client = test_client(Arc::clone(&old));
        let waiter_client = Arc::clone(&client);
        let waiter_old = Arc::clone(&old);

        let reconnect = client.session_transition.lock().await;
        let waiter = tokio::spawn(async move {
            waiter_client
                .accept_stateful_response(&waiter_old)
                .await
                .is_some()
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "response settlement must wait for reconnect's transition"
        );

        old.dead.store(true, Ordering::Release);
        client.live.store(false, Ordering::Release);
        client.conn.store(test_conn());
        client.live.store(true, Ordering::Release);
        drop(reconnect);

        assert!(
            !waiter.await.unwrap(),
            "a response from the replaced connection must be resent, not settled"
        );
    }

    #[tokio::test]
    async fn settled_state_is_visible_to_the_next_replay_snapshot() {
        let conn = test_conn();
        let client = test_client(Arc::clone(&conn));
        let settlement = client
            .accept_stateful_response(&conn)
            .await
            .expect("current response should settle");

        let replay_client = Arc::clone(&client);
        let snapshot = tokio::spawn(async move {
            let _reconnect = replay_client.session_transition.lock().await;
            replay_client.state.lock().unwrap().fids.contains_key(&7)
        });
        tokio::task::yield_now().await;
        assert!(
            !snapshot.is_finished(),
            "replay must wait until response bookkeeping completes"
        );

        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(70, 0, 0, Some(0)));
        drop(settlement);

        assert!(
            snapshot.await.unwrap(),
            "the replay snapshot must include the settled fid transition"
        );
    }

    #[tokio::test]
    async fn permanent_replay_loss_wakes_waiters_with_estale() {
        let client = test_client(test_conn());
        client.live.store(false, Ordering::Release);
        let waiter_client = Arc::clone(&client);
        let waiter = tokio::spawn(async move { waiter_client.wait_until_live().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        client.fail_session(linux::ESTALE);
        assert!(matches!(
            waiter.await.unwrap(),
            Err(ClientError::Errno(errno)) if errno == linux::ESTALE
        ));
        assert!(matches!(
            client.wait_until_live().await,
            Err(ClientError::Errno(errno)) if errno == linux::ESTALE
        ));
    }

    #[tokio::test]
    async fn discarded_replay_candidate_resets_session_before_shutdown() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "candidate reset").await;
            assert_eq!(request.tag, NOTAG);
            assert!(matches!(request.body, Message::Tversion(_)));
            reply(
                &responder_conn,
                request.tag,
                Message::Rversion(Rversion {
                    msize: 8192,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            );
            requests
        });

        client.discard_replay_candidate(&conn).await;
        let mut requests = responder.await.unwrap();
        assert!(
            requests.try_recv().is_err(),
            "candidate reset must not issue a throwaway lineage query"
        );
        assert!(conn.dead.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn replay_establishes_each_attach_root_before_its_explicit_descendants() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        {
            let mut state = client.state.lock().unwrap();
            state.fids.insert(1, attach_fid(10, 1000));
            state.fids.insert(2, attach_fid(20, 2000));
            state.fids.insert(3, inode_fid(11, 10, 1000, None));
            state.fids.insert(4, inode_fid(21, 20, 2000, None));
        }

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let mut roots = std::collections::BTreeSet::new();
            let mut descendants = std::collections::BTreeSet::new();
            for index in 0..4 {
                let request = recv_request(&mut requests, "replay request").await;
                let Message::Trebind(rebind) = request.body else {
                    panic!("expected Trebind");
                };
                assert_eq!(
                    rebind.flags, P9_REBIND_REPLAY,
                    "every automatic fid replay is marked independently of open state"
                );
                if index < 2 {
                    assert_eq!(rebind.inode_id, rebind.root_inode);
                    roots.insert(rebind.root_inode);
                } else {
                    assert_ne!(rebind.inode_id, rebind.root_inode);
                    descendants.insert((rebind.inode_id, rebind.root_inode));
                }
                reply(
                    &responder_conn,
                    request.tag,
                    Message::Rrebind(Rrebind {
                        qid: qid(rebind.inode_id),
                    }),
                );
            }
            assert_eq!(roots, [10, 20].into_iter().collect());
            assert_eq!(descendants, [(11, 10), (21, 20)].into_iter().collect());
        });

        client.replay(&conn).await.unwrap();
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn name_derived_credentials_survive_descendant_replay() {
        let (old_conn, mut old_requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&old_conn));
        let responder_conn = Arc::clone(&old_conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut old_requests, "attach request").await;
            let Message::Tattach(attach) = request.body else {
                panic!("expected Tattach");
            };
            assert_eq!(attach.uname.as_str().unwrap(), "root");
            assert_eq!(attach.n_uname, u32::MAX);
            reply(
                &responder_conn,
                request.tag,
                Message::Rattach(Rattach {
                    qid: Qid {
                        type_: 0x80,
                        ..qid(10)
                    },
                }),
            );

            let request = recv_request(&mut old_requests, "walk request").await;
            assert!(matches!(request.body, Message::Twalk(_)));
            reply(
                &responder_conn,
                request.tag,
                Message::Rwalk(Rwalk {
                    nwqid: 1,
                    wqids: vec![qid(11)],
                }),
            );
        });

        client.attach(1, NOFID, "root", "", u32::MAX).await.unwrap();
        client.walk(1, 2, &[b"child"]).await.unwrap();
        responder.await.unwrap();

        client.state.lock().unwrap().fids.remove(&1);

        let (replay_conn, mut replay_requests) = test_conn_with_receiver();
        let responder_conn = Arc::clone(&replay_conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut replay_requests, "descendant rebind").await;
            let Message::Trebind(rebind) = request.body else {
                panic!("expected Trebind");
            };
            assert_eq!(rebind.fid, 2);
            assert_eq!((rebind.inode_id, rebind.root_inode), (11, 10));
            assert_eq!(rebind.uname.as_str().unwrap(), "root");
            assert_eq!(rebind.n_uname, u32::MAX);
            reply(
                &responder_conn,
                request.tag,
                Message::Rrebind(Rrebind { qid: qid(11) }),
            );
        });

        client.replay(&replay_conn).await.unwrap();
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn rooted_descendant_replays_after_its_attach_fid_was_clunked() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(70, 10, 1000, None));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "descendant rebind").await;
            let Message::Trebind(rebind) = request.body else {
                panic!("expected Trebind");
            };
            assert_eq!((rebind.inode_id, rebind.root_inode), (70, 10));
            reply(
                &responder_conn,
                request.tag,
                Message::Rrebind(Rrebind { qid: qid(70) }),
            );
        });

        client.replay(&conn).await.unwrap();
        responder.await.unwrap();
        assert!(client.state.lock().unwrap().fids.contains_key(&7));
    }

    #[tokio::test]
    async fn missing_opened_fid_is_quarantined_without_terminating_the_session() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(70, 0, 0, Some(0)));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "opened fid rebind").await;
            assert!(matches!(
                request.body,
                Message::Trebind(Trebind {
                    fid: 7,
                    inode_id: 70,
                    flags: OPEN_REPLAY,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlerror(Rlerror {
                    ecode: linux::ESTALE,
                }),
            );

            let request = recv_request(&mut requests, "unrelated read").await;
            assert!(matches!(
                request.body,
                Message::Tread(Tread {
                    fid: 8,
                    count: 1,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rread(Rread {
                    count: 1,
                    data: DekuBytes::from(vec![b'x']),
                }),
            );
        });

        client.replay(&conn).await.unwrap();
        assert!(!client.state.lock().unwrap().fids.contains_key(&7));
        assert!(client.stale_fids.contains(&7));
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
        assert!(matches!(
            client.read(7, 0, 1).await,
            Err(ClientError::Errno(errno)) if errno == linux::ESTALE
        ));
        assert_eq!(client.read(8, 0, 1).await.unwrap(), vec![b'x']);
        tokio::time::timeout(Duration::from_secs(1), client.clunk(7))
            .await
            .expect("stale clunk must not reach the server")
            .unwrap();
        assert!(!client.stale_fids.contains(&7));
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn read_bytes_retains_the_response_frame_allocation() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        let request_client = Arc::clone(&client);
        let read = tokio::spawn(async move { request_client.read_bytes(7, 0, 7).await });

        let request = recv_request(&mut requests, "read request").await;
        assert!(matches!(request.body, Message::Tread(_)));
        let frame = Bytes::from(
            P9Message::new(
                request.tag,
                Message::Rread(Rread {
                    count: 7,
                    data: DekuBytes::from(Bytes::from_static(b"payload")),
                }),
            )
            .to_bytes()
            .unwrap(),
        );
        let expected_payload_ptr = frame.as_ptr() as usize + P9_IOHDRSZ as usize;
        conn.deliver(frame);

        let payload = read.await.unwrap().unwrap();
        assert_eq!(payload.as_ref(), b"payload");
        assert_eq!(payload.as_ptr() as usize, expected_payload_ptr);
    }

    #[test]
    fn freeing_a_fid_clears_its_stale_tombstone() {
        let client = test_client(test_conn());
        client.stale_fids.insert(7);
        client.free_fid(7);
        assert!(!client.stale_fids.contains(&7));
    }

    #[test]
    fn stale_tombstones_cover_source_destination_and_clunk_fids() {
        let client = test_client(test_conn());
        client.stale_fids.insert(7);

        for request in [
            Message::Tlink(Tlink {
                dfid: 8,
                fid: 7,
                name: P9String::new(b"link".to_vec()),
            }),
            Message::Twalk(Twalk {
                fid: 8,
                newfid: 7,
                nwname: 0,
                wnames: Vec::new(),
            }),
            Message::Tclunk(Tclunk { fid: 7 }),
        ] {
            assert!(matches!(
                client.validate_fids(request.request_fids()),
                Err(ClientError::Errno(errno)) if errno == linux::ESTALE
            ));
        }
        assert!(
            client
                .validate_fids(
                    Message::Tread(Tread {
                        fid: 8,
                        offset: 0,
                        count: 1,
                    })
                    .request_fids()
                )
                .is_ok()
        );
    }

    #[tokio::test]
    async fn clunk_does_not_mask_a_terminal_session_estale() {
        let client = test_client(test_conn());
        client.stale_fids.insert(7);
        client.fail_session(linux::ESTALE);

        assert!(matches!(
            client.clunk(7).await,
            Err(ClientError::Errno(errno)) if errno == linux::ESTALE
        ));
        assert!(client.stale_fids.contains(&7));
    }

    #[tokio::test]
    async fn failed_reopen_clunks_the_provisional_rebind() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(70, 0, 0, Some(0)));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "opened fid rebind").await;
            assert!(matches!(
                request.body,
                Message::Trebind(Trebind { fid: 7, .. })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rrebind(Rrebind { qid: qid(70) }),
            );

            let request = recv_request(&mut requests, "fid reopen").await;
            assert!(matches!(
                request.body,
                Message::Tlopen(Tlopen { fid: 7, .. })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlerror(Rlerror {
                    ecode: linux::ESTALE,
                }),
            );

            let request = recv_request(&mut requests, "provisional fid clunk").await;
            assert!(matches!(request.body, Message::Tclunk(Tclunk { fid: 7 })));
            reply(&responder_conn, request.tag, Message::Rclunk(Rclunk));
        });

        client.replay(&conn).await.unwrap();
        responder.await.unwrap();
        let state = client.state.lock().unwrap();
        assert!(!state.fids.contains_key(&7));
        drop(state);
        assert!(client.stale_fids.contains(&7));
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn missing_locked_fid_is_a_terminal_replay_error() {
        let (conn, requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        {
            let mut state = client.state.lock().unwrap();
            state.fids.insert(7, inode_fid(70, 0, 0, None));
            state.locks.push(write_lock(7, 0, 0));
        }

        assert!(matches!(
            replay_rebind_error(&client, &conn, requests, (7, 70, 0, REPLAY), linux::ENOENT)
                .await,
            Err(ClientError::Errno(errno)) if errno == linux::ESTALE
        ));
        {
            let state = client.state.lock().unwrap();
            assert!(state.fids.contains_key(&7));
            assert_eq!(state.locks.len(), 1);
        }
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), linux::ESTALE);
    }

    #[tokio::test]
    async fn missing_unopened_rebind_is_dropped() {
        let (conn, requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(70, 0, 0, None));

        replay_rebind_error(&client, &conn, requests, (7, 70, 0, REPLAY), linux::ENOENT)
            .await
            .unwrap();
        assert!(!client.state.lock().unwrap().fids.contains_key(&7));
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn operational_rebind_error_is_transient_and_preserves_fid() {
        let (conn, requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(70, 0, 0, None));

        assert!(matches!(
            replay_rebind_error(
                &client,
                &conn,
                requests,
                (7, 70, 0, REPLAY),
                linux::EIO as u32,
            )
            .await,
            Err(ClientError::Errno(errno)) if errno == linux::EIO as u32
        ));
        assert!(client.state.lock().unwrap().fids.contains_key(&7));
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn missing_unopened_attach_root_is_dropped_while_open_child_replays() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        {
            let mut state = client.state.lock().unwrap();
            state.fids.insert(1, attach_fid(1, 0));
            state.fids.insert(2, inode_fid(70, 1, 0, Some(0)));
        }

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "root rebind request").await;
            assert!(matches!(
                request.body,
                Message::Trebind(Trebind {
                    inode_id: 1,
                    root_inode: 1,
                    flags: P9_REBIND_REPLAY,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlerror(Rlerror {
                    ecode: linux::ENOENT,
                }),
            );

            let request = recv_request(&mut requests, "opened child rebind").await;
            assert!(matches!(
                request.body,
                Message::Trebind(Trebind {
                    inode_id: 70,
                    root_inode: 1,
                    flags: P9_REBIND_KNOWN_FLAGS,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rrebind(Rrebind { qid: qid(70) }),
            );

            let request = recv_request(&mut requests, "opened child reopen").await;
            assert!(matches!(
                request.body,
                Message::Tlopen(Tlopen { fid: 2, flags: 0 })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlopen(Rlopen {
                    qid: qid(70),
                    iounit: 0,
                }),
            );
        });

        client.replay(&conn).await.unwrap();
        responder.await.unwrap();
        let state = client.state.lock().unwrap();
        assert!(!state.fids.contains_key(&1));
        assert!(state.fids.contains_key(&2));
        drop(state);
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn operational_attach_error_is_transient() {
        let (conn, requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(1, attach_fid(2, 0));

        assert!(matches!(
            replay_rebind_error(&client, &conn, requests, (1, 2, 2, REPLAY), linux::EIO as u32)
            .await,
            Err(ClientError::Errno(errno)) if errno == linux::EIO as u32
        ));
        assert!(client.state.lock().unwrap().fids.contains_key(&1));
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn persistent_lock_conflict_remains_retryable() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client.state.lock().unwrap().locks.push(write_lock(7, 0, 0));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            while let Some(request) = next_request(&mut requests).await {
                assert!(matches!(request.body, Message::Tlock(_)));
                reply(
                    &responder_conn,
                    request.tag,
                    Message::Rlock(Rlock {
                        status: LockStatus::Blocked,
                    }),
                );
            }
        });

        assert!(
            runtime::timeout(Duration::from_secs(1), client.replay(&conn))
                .await
                .is_err(),
            "persistent lock conflicts must remain a retryable replay wait"
        );
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
        responder.abort();
        let _ = responder.await;
    }

    #[tokio::test]
    async fn refused_lock_replay_terminates_the_session() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client.state.lock().unwrap().locks.push(write_lock(7, 0, 0));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "lock replay").await;
            assert!(matches!(request.body, Message::Tlock(_)));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlock(Rlock {
                    status: LockStatus::LockError,
                }),
            );
        });

        assert!(matches!(
            client.replay(&conn).await,
            Err(ClientError::Errno(errno)) if errno == linux::ESTALE
        ));
        responder.await.unwrap();
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), linux::ESTALE);
    }

    #[tokio::test]
    async fn lock_replay_retries_through_old_session_teardown() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client.state.lock().unwrap().locks.push(write_lock(7, 0, 0));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "blocked lock acquisition").await;
            assert!(matches!(request.body, Message::Tlock(_)));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlock(Rlock {
                    status: LockStatus::Blocked,
                }),
            );

            let request = recv_request(&mut requests, "retried lock acquisition").await;
            assert!(matches!(
                request.body,
                Message::Tlock(Tlock {
                    lock_type: LockType::WriteLock,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlock(Rlock {
                    status: LockStatus::Success,
                }),
            );
        });

        client.replay(&conn).await.unwrap();
        responder.await.unwrap();
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn raced_lock_conflict_rolls_back_the_acquired_prefix() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .locks
            .extend([write_lock(7, 0, 10), write_lock(8, 20, 10)]);

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let request = recv_request(&mut requests, "first lock acquisition").await;
            assert!(matches!(
                request.body,
                Message::Tlock(Tlock {
                    fid: 7,
                    lock_type: LockType::WriteLock,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlock(Rlock {
                    status: LockStatus::Success,
                }),
            );

            let request = recv_request(&mut requests, "raced lock acquisition").await;
            assert!(matches!(
                request.body,
                Message::Tlock(Tlock {
                    fid: 8,
                    lock_type: LockType::WriteLock,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlerror(Rlerror {
                    ecode: linux::EAGAIN,
                }),
            );

            let request = recv_request(&mut requests, "prefix rollback").await;
            assert!(matches!(
                request.body,
                Message::Tlock(Tlock {
                    fid: 7,
                    lock_type: LockType::Unlock,
                    ..
                })
            ));
            reply(
                &responder_conn,
                request.tag,
                Message::Rlock(Rlock {
                    status: LockStatus::Success,
                }),
            );

            for expected_fid in [7, 8] {
                let request = recv_request(&mut requests, "retry lock acquisition").await;
                let Message::Tlock(lock) = request.body else {
                    panic!("expected Tlock");
                };
                assert_eq!(lock.fid, expected_fid);
                assert!(matches!(lock.lock_type, LockType::WriteLock));
                reply(
                    &responder_conn,
                    request.tag,
                    Message::Rlock(Rlock {
                        status: LockStatus::Success,
                    }),
                );
            }
        });

        client.replay(&conn).await.unwrap();
        responder.await.unwrap();
        assert_eq!(client.terminal_errno.load(Ordering::Acquire), 0);
    }

    #[test]
    fn clean_rejection_of_first_frame_restarts_the_retry_horizon() {
        let mut attempt = OpAttemptState::default();
        let (first_flags, first_origin) = attempt
            .dispatch_frame(true, 7, |_, origin| Ok(origin))
            .unwrap();
        assert_eq!((first_flags, first_origin), (0, 7));
        assert!(attempt.started.is_some());
        attempt.proven_predispatch(first_flags);
        assert!(attempt.started.is_none());
        let (next_flags, next_origin) = attempt
            .dispatch_frame(true, 8, |_, origin| Ok(origin))
            .unwrap();
        assert_eq!(
            (next_flags, next_origin),
            (0, 8),
            "a definitive rejection of the sole FIRST may be routed as FIRST again"
        );
    }

    #[test]
    fn clean_rejection_of_retry_does_not_erase_older_ambiguity() {
        let mut attempt = OpAttemptState::default();
        let (first_flags, first_origin) = attempt
            .dispatch_frame(true, 7, |_, origin| Ok(origin))
            .unwrap();
        assert_eq!((first_flags, first_origin), (0, 7));

        let (retry_flags, retry_origin) = attempt
            .dispatch_frame(true, 8, |_, origin| Ok(origin))
            .unwrap();
        assert_eq!((retry_flags, retry_origin), (P9_OP_FLAG_RETRY, 7));
        assert!(attempt.started.is_some());
        attempt.proven_predispatch(retry_flags);
        assert!(attempt.started.is_some());
        let (next_flags, next_origin) = attempt
            .dispatch_frame(true, 9, |_, origin| Ok(origin))
            .unwrap();
        assert_eq!(
            (next_flags, next_origin),
            (P9_OP_FLAG_RETRY, 7),
            "a rejected RETRY cannot make the older lost FIRST unambiguous"
        );
    }

    async fn assert_notleader_reroute(ecode: u32, expected_flags: u8, expected_epoch: u64) {
        let (old_conn, mut old_requests) = test_conn_with_receiver();
        old_conn.writer_epoch.store(7, Ordering::Relaxed);
        let client = test_client(Arc::clone(&old_conn));

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.write(7, 0, b"x").await });
        let first_frame = tokio::time::timeout(Duration::from_secs(1), old_requests.recv())
            .await
            .expect("FIRST request was not queued")
            .expect("old request channel closed");
        let first = P9Message::from_bytes_ctx(&first_frame, true).unwrap();
        let op_id = first.op_id;
        assert_ne!(op_id, [0u8; 16]);
        assert_eq!(first.op_flags, 0);
        assert_eq!(first.op_origin_epoch, 7);
        assert!(matches!(first.body, Message::Twrite(_)));

        client.live.store(false, Ordering::Release);
        reply(&old_conn, first.tag, Message::Rlerror(Rlerror { ecode }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !old_conn.dead.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("not-leader response did not force a re-probe");

        let (new_conn, mut new_requests) = test_conn_with_receiver();
        new_conn.writer_epoch.store(8, Ordering::Relaxed);
        client.conn.store(Arc::clone(&new_conn));
        client.live.store(true, Ordering::Release);
        client.live_notify.notify_waiters();

        let rerouted_frame = tokio::time::timeout(Duration::from_secs(1), new_requests.recv())
            .await
            .expect("request was not rerouted")
            .expect("replacement request channel closed");
        let rerouted = P9Message::from_bytes_ctx(&rerouted_frame, true).unwrap();
        assert_eq!(rerouted.op_id, op_id);
        assert_eq!(
            rerouted.op_flags, expected_flags,
            "CLEAN must restore FIRST; an ambiguous rejection must retain RETRY"
        );
        assert_eq!(
            rerouted.op_origin_epoch, expected_epoch,
            "FIRST adopts the successor epoch; RETRY retains its origin epoch"
        );
        assert!(matches!(rerouted.body, Message::Twrite(_)));

        reply(
            &new_conn,
            rerouted.tag,
            Message::Rwrite(Rwrite { count: 1 }),
        );
        assert_eq!(request.await.unwrap().unwrap(), 1);
    }

    #[tokio::test]
    async fn clean_notleader_after_first_reroutes_same_op_id_as_first() {
        assert_notleader_reroute(P9_ENOTLEADER_CLEAN, 0, 8).await;
    }

    #[tokio::test]
    async fn generic_notleader_after_first_reroutes_same_op_id_as_retry() {
        assert_notleader_reroute(P9_ENOTLEADER, P9_OP_FLAG_RETRY, 7).await;
    }

    #[tokio::test]
    async fn write_bytes_chunks_at_the_msize_boundary() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        let max = client.max_write_payload();
        assert_eq!(
            max,
            client.msize() - P9_TWRITE_HDR - P9_OP_ENVELOPE_LEN as u32
        );
        let offset = 37;
        let mut payload = vec![b'x'; max as usize + 1];
        payload[max as usize] = b'y';
        let payload = Bytes::from(payload);

        let request_client = Arc::clone(&client);
        let request =
            tokio::spawn(async move { request_client.write_bytes(7, offset, payload).await });

        let first = recv_op_request(&mut requests, "first write request").await;
        let Message::Twrite(first_write) = first.body else {
            panic!("expected Twrite");
        };
        assert_eq!(first_write.offset, offset);
        assert_eq!(first_write.count, max);
        assert_eq!(first_write.data.len(), max as usize);
        assert!(first_write.data.iter().all(|byte| *byte == b'x'));
        reply(&conn, first.tag, Message::Rwrite(Rwrite { count: max }));

        let second = recv_op_request(&mut requests, "second write request").await;
        let Message::Twrite(second_write) = second.body else {
            panic!("expected Twrite");
        };
        assert_eq!(second_write.offset, offset + u64::from(max));
        assert_eq!(second_write.count, 1);
        assert_eq!(second_write.data.as_ref(), b"y");
        reply(&conn, second.tag, Message::Rwrite(Rwrite { count: 1 }));

        assert_eq!(request.await.unwrap().unwrap(), u64::from(max) + 1);
    }

    #[tokio::test]
    async fn read_once_rejects_a_payload_larger_than_requested() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.read_once(7, 0, 1).await });
        let sent = recv_request(&mut requests, "read request").await;
        assert!(matches!(sent.body, Message::Tread(Tread { count: 1, .. })));
        reply(
            &conn,
            sent.tag,
            Message::Rread(Rread {
                count: 2,
                data: DekuBytes::from(vec![b'x', b'y']),
            }),
        );

        assert!(matches!(
            request.await.unwrap(),
            Err(ClientError::Unexpected("read count"))
        ));
    }

    #[tokio::test]
    async fn write_chunk_rejects_a_count_larger_than_attempted() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move {
            request_client
                .write_chunk(7, 0, Bytes::from_static(b"x"))
                .await
        });
        let sent = recv_op_request(&mut requests, "write request").await;
        assert!(matches!(
            sent.body,
            Message::Twrite(Twrite { count: 1, .. })
        ));
        reply(&conn, sent.tag, Message::Rwrite(Rwrite { count: 2 }));

        assert!(matches!(
            request.await.unwrap(),
            Err(ClientError::Unexpected("write count"))
        ));
    }

    #[tokio::test]
    async fn ambiguous_reply_loss_marks_the_next_frame_as_retry() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));

        let responder_conn = Arc::clone(&conn);
        let responder = tokio::spawn(async move {
            let first = recv_op_request(&mut requests, "initial write request").await;
            let op_id = first.op_id;
            assert_ne!(op_id, [0u8; 16]);
            assert_eq!(first.op_flags, 0);

            let (_, pending) = responder_conn
                .pending
                .remove(&first.tag)
                .expect("initial response slot");
            drop(pending);

            let retry = recv_op_request(&mut requests, "retried write request").await;
            assert_eq!(retry.op_id, op_id);
            assert_eq!(retry.op_flags, P9_OP_FLAG_RETRY);
            reply(
                &responder_conn,
                retry.tag,
                Message::Rlerror(Rlerror {
                    ecode: P9_EOPIDSTALE,
                }),
            );
        });

        assert!(matches!(
            client.write(7, 0, b"x").await,
            Err(ClientError::Errno(P9_EOPIDSTALE))
        ));
        responder.await.unwrap();
        assert!(conn.pending.is_empty());
    }

    #[tokio::test]
    async fn cancelled_stateful_create_retires_its_session() {
        let (old_conn, mut old_requests) = test_conn_with_receiver();
        old_conn.writer_epoch.store(7, Ordering::Relaxed);
        let client = test_client(Arc::clone(&old_conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(1, 1, 0, None));
        let first_client = Arc::clone(&client);
        let first_task = tokio::spawn(async move {
            first_client
                .lcreate(7, b"child", linux::O_CREAT, 0o644, 0)
                .await
        });
        let first = recv_op_request(&mut old_requests, "FIRST create request").await;
        assert_eq!(first.op_flags, 0);
        assert_eq!(first.op_origin_epoch, 7);
        assert!(matches!(first.body, Message::Tlcreate(_)));

        first_task.abort();
        let _ = first_task.await;
        assert!(
            old_conn.dead.load(Ordering::Acquire),
            "an unobserved stateful transition must retire its connection"
        );
        assert_eq!(client.state.lock().unwrap().fids[&7].inode_id, 1);
        old_conn.connection_lost(&client.reconnect_notify);
    }

    #[tokio::test]
    async fn cancelled_stateful_request_does_not_retire_an_unseen_successor() {
        let (old_conn, mut old_requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&old_conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(1, 1, 0, None));

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move {
            request_client
                .lcreate(7, b"child", linux::O_CREAT, 0o644, 0)
                .await
        });
        let sent = recv_op_request(&mut old_requests, "create request").await;
        assert!(matches!(sent.body, Message::Tlcreate(_)));

        client.live.store(false, Ordering::Release);
        old_conn.connection_lost(&client.reconnect_notify);
        let (successor, _successor_requests) = test_conn_with_receiver();
        client.conn.store(Arc::clone(&successor));

        request.abort();
        let _ = request.await;
        assert!(old_conn.dead.load(Ordering::Acquire));
        assert!(
            !successor.dead.load(Ordering::Acquire),
            "cancellation must not retire a connection that did not receive the request"
        );
    }

    #[tokio::test]
    async fn cancelled_zero_id_lopen_retires_its_session() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        client
            .state
            .lock()
            .unwrap()
            .fids
            .insert(7, inode_fid(42, 1, 0, None));

        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.lopen(7, 0).await });
        let sent = recv_request(&mut requests, "Tlopen request").await;
        assert!(matches!(sent.body, Message::Tlopen(Tlopen { fid: 7, .. })));

        request.abort();
        let _ = request.await;
        assert!(
            conn.dead.load(Ordering::Acquire),
            "zero-id stateful cancellation must normalize the session"
        );
        assert_eq!(client.state.lock().unwrap().fids[&7].opened, None);
    }

    #[tokio::test]
    async fn cancelling_stateful_create_before_enqueue_keeps_connection_live() {
        let (conn, _requests) = test_conn_with_receiver();
        conn.writer_tx
            .send(vec![0])
            .await
            .expect("test writer queue should accept its first frame");
        let client = test_client(Arc::clone(&conn));
        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move {
            request_client
                .lcreate(7, b"child", linux::O_CREAT, 0o644, 0)
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(!request.is_finished());
        assert!(conn.pending.is_empty());

        request.abort();
        let _ = request.await;
        assert!(
            !conn.dead.load(Ordering::Acquire),
            "pre-enqueue cancellation must not churn the live connection"
        );
    }

    #[tokio::test]
    async fn cancelling_a_dispatched_request_quarantines_its_tag() {
        let (conn, mut requests) = test_conn_with_receiver();
        let client = test_client(Arc::clone(&conn));
        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.write(7, 0, b"x").await });

        let first_frame = tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .expect("request was not queued")
            .expect("request channel closed");
        let first = P9Message::from_bytes_ctx(&first_frame, true).unwrap();
        assert_eq!(conn.pending.len(), 1);
        request.abort();
        let _ = request.await;
        assert_eq!(
            conn.pending.len(),
            1,
            "a dispatched request's tag must remain quarantined"
        );

        conn.tag_ctr.store(first.tag, Ordering::Relaxed);
        let second_client = Arc::clone(&client);
        let second_request = tokio::spawn(async move { second_client.write(7, 0, b"y").await });
        let second_frame = tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .expect("second request was not queued")
            .expect("request channel closed");
        let second = P9Message::from_bytes_ctx(&second_frame, true).unwrap();
        assert_ne!(second.tag, first.tag);

        reply(&conn, first.tag, Message::Rwrite(Rwrite { count: 1 }));
        tokio::task::yield_now().await;
        assert!(
            !second_request.is_finished(),
            "the late response must not complete the new request"
        );

        reply(&conn, second.tag, Message::Rwrite(Rwrite { count: 1 }));
        assert_eq!(second_request.await.unwrap().unwrap(), 1);
        assert!(conn.pending.is_empty());
    }

    #[tokio::test]
    async fn cancelling_before_writer_capacity_does_not_register_a_tag() {
        let (conn, _requests) = test_conn_with_receiver();
        conn.writer_tx
            .send(vec![0])
            .await
            .expect("test writer queue should accept its first frame");

        let client = test_client(Arc::clone(&conn));
        let request_client = Arc::clone(&client);
        let request = tokio::spawn(async move { request_client.write(7, 0, b"x").await });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(!request.is_finished());
        assert!(
            conn.pending.is_empty(),
            "waiting for writer capacity must happen before tag registration"
        );
        request.abort();
        let _ = request.await;
        assert!(conn.pending.is_empty());
    }

    #[test]
    fn undispatched_guard_releases_its_tag() {
        let conn = test_conn();

        let (tx, _rx) = oneshot::channel();
        let tag = NinePClient::alloc_tag(&conn, tx).expect("tag allocation failed");
        let guard = PendingTag {
            conn: Arc::clone(&conn),
            tag,
            dispatched: false,
        };
        assert!(conn.pending.contains_key(&tag));
        drop(guard);
        assert!(conn.pending.is_empty());
    }

    #[test]
    fn allocator_checks_every_wire_tag_after_skipping_notag() {
        let conn = test_conn();

        for tag in 0..(NOTAG - 1) {
            let (tx, _rx) = oneshot::channel();
            assert!(NinePClient::register_tag(&conn, tag, tx).is_ok());
        }
        conn.tag_ctr.store(NOTAG, Ordering::Relaxed);

        let (tx, _rx) = oneshot::channel();
        let tag = NinePClient::alloc_tag(&conn, tx).expect("allocator missed the last usable tag");
        assert_eq!(tag, NOTAG - 1);
    }
}

#[cfg(test)]
mod liveness_tests {
    use super::*;

    fn test_conn() -> Arc<Conn> {
        test_conn_with_receiver().0
    }

    #[test]
    fn within_is_strict_and_saturates() {
        let w = Duration::from_millis(300);
        assert!(Conn::within(1000, 800, w), "200ms ago is within 300ms");
        assert!(
            !Conn::within(1000, 700, w),
            "exactly at the window is NOT within (strict <)"
        );
        assert!(!Conn::within(1000, 500, w), "500ms ago is past 300ms");
        assert!(Conn::within(500, 1000, w));
    }

    #[test]
    fn a_just_marked_conn_is_heard() {
        let conn = test_conn();
        assert!(conn.heard_within(Duration::from_secs(60)));
        conn.mark_alive();
        assert!(conn.heard_within(Duration::from_secs(60)));
        assert!(!conn.heard_within(Duration::from_millis(0)));
    }
}
