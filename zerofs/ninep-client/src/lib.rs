//! Asynchronous 9P2000.L client.
//!
//! # Reconnection
//!
//! 9P sessions are stateful: every fid and byte-range lock lives on the
//! connection, so a dropped socket invalidates all of it. This client records,
//! per fid, the stable inode id it points at (plus open flags) and which locks
//! it holds, in [`SessionState`]; a supervisor task reconnects (indefinite
//! backoff) and replays that state onto the fresh session. Requests block
//! through the reconnect and are resent. A request in flight at the drop is
//! ambiguous; under `.zerofs3` every non-idempotent mutation carries a stable
//! op-id (including the fid-opening create, Tlcreate), so the server deduplicates
//! the resend instead of double-applying it.

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use deku::prelude::*;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use ninep_proto::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, info, warn};

/// The 9P "no tag" sentinel. We never allocate it for a normal request.
const NOTAG: u16 = 0xFFFF;
/// The 9P "no fid" sentinel, used as the `afid` in attach when not authenticating.
pub const NOFID: u32 = 0xFFFF_FFFF;

/// A fresh random idempotency op-id, stamped on each plain mutating call so a
/// resend-on-reply-loss is deduplicated rather than double-applied. A higher
/// layer retrying the same logical op across a failover passes its own stable id
/// via the `_op_id` methods.
fn new_op_id() -> [u8; 16] {
    use rand::RngCore;
    let mut id = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut id);
    id
}

const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(50);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);
/// Per-target probe budget (dial + negotiate, or the lease check). Bounds the
/// probe race so a partitioned node can neither stall the winner nor linger.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// Cap on awaiting one reply before treating the leader as hung (frozen or wedged
/// with its connection still open, so a clean-crash reconnect never fires): tear
/// it down, reprobe, and resend. The stable op-id makes the resend exactly-once.
/// Exceeds the leader's 5s ship timeout so a healthy write awaiting a slow ship is
/// not falsely re-routed.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// How recently the reader must have decoded any frame for the connection to
/// count as alive without an explicit probe. Under a write storm other replies
/// keep flowing, so a merely-slow op takes this cheap path and never triggers a
/// teardown; short enough that a genuine freeze (no frames at all) falls through
/// to the probe within roughly one [`REQUEST_TIMEOUT`] window.
const LIVENESS_WINDOW: Duration = Duration::from_secs(3);
/// Cap on how many extra [`REQUEST_TIMEOUT`] windows one request may wait while
/// the connection keeps proving itself alive. Bounds worst-case failover latency
/// when the leader answers liveness checks but a wedged write-path never lands
/// this op (reads fine, writes stuck): after the cap we tear down and re-probe
/// regardless. 7 extra windows plus the initial one is about a minute.
const MAX_LIVENESS_EXTENSIONS: u32 = 7;
/// Cap on replaying the whole recorded session (re-attach/rebind every fid,
/// re-open files, re-acquire locks) onto a fresh connection during reconnect.
/// `send_raw` has no per-reply timeout, so without this a server that accepts
/// the connection and negotiates but then stalls on a replayed request (e.g. its
/// store is not ready yet right after a restart) would wedge the supervisor
/// forever, leaving every waiting op parked in `wait_until_live`. On elapse the
/// half-built connection is torn down and the supervisor retries with backoff.
/// Generous so a large session over a slow link still replays in one pass.
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
            _ => libc::EIO,
        }
    }
}

pub type ClientResult<T> = Result<T, ClientError>;

mod ops;
pub use ops::{DirEntryCookie, ReaddirState, SetattrBuilder, SetattrTime};

/// A 9P endpoint to dial. A client may hold several (an HA node set) and probes
/// for whichever is the serving leader, re-probing on reconnect to follow a
/// failover.
#[derive(Clone)]
pub enum Target {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

/// A single live transport (one socket + its reader/writer tasks). Replaced
/// wholesale on reconnect. Requests load the current one through the `ArcSwap`.
struct Conn {
    writer_tx: mpsc::Sender<Vec<u8>>,
    pending: DashMap<u16, oneshot::Sender<Bytes>>,
    tag_ctr: AtomicU16,
    /// `.zerofs4` durability lineage token of the instance this connection reached,
    /// learned via Tgetlineage during negotiation (`0` if not `.zerofs4`). A write
    /// that returns on this connection is recorded as un-fsync'd under this token.
    lineage_token: AtomicU64,
    /// Set by whichever of the reader/writer tasks first sees the socket fail.
    dead: AtomicBool,
    /// Monotonic base for [`Conn::last_alive`].
    base: std::time::Instant,
    /// Millis (since `base`) at which the reader last decoded a frame: proof the
    /// server is answering. Read by the timeout path to tell a slow leader from a
    /// hung one without always paying for an explicit probe.
    last_alive: AtomicU64,
    /// Serialises explicit liveness probes so concurrent timed-out waiters issue
    /// at most one (they share the single reserved probe fid).
    probe_lock: tokio::sync::Mutex<()>,
    /// Signals the (possibly idle) writer task to stop when the reader exits.
    writer_shutdown: Notify,
    /// Signals the reader to stop while the socket is still healthy: it otherwise
    /// only exits on socket death, so a connection discarded before install
    /// (negotiate/replay failed) would leak its reader + fd. Reader and writer
    /// each hold an `Arc<Conn>`, so the cycle never breaks on Drop; teardown is
    /// explicit via [`Conn::shutdown`].
    reader_shutdown: Notify,
}

impl Conn {
    /// Tear down a connection discarded before install (negotiate/replay failed):
    /// wake reader and writer so they exit, drop their `Arc<Conn>` and release the
    /// fd. A socket-failed connection tears itself down; this is the discard path.
    fn shutdown(&self) {
        self.dead.store(true, Ordering::Release);
        self.reader_shutdown.notify_one();
        self.writer_shutdown.notify_one();
    }

    /// Record that the server just answered (any decoded frame). Called by the reader.
    fn mark_alive(&self) {
        self.last_alive
            .store(self.base.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// `now_ms - last_ms < window`, saturating (a `last` ahead of `now` counts as within).
    fn within(now_ms: u64, last_ms: u64, window: Duration) -> bool {
        now_ms.saturating_sub(last_ms) < window.as_millis() as u64
    }

    /// Did the reader decode a frame within `window`?
    fn heard_within(&self, window: Duration) -> bool {
        Self::within(
            self.base.elapsed().as_millis() as u64,
            self.last_alive.load(Ordering::Relaxed),
            window,
        )
    }
}

/// How a fid is re-established on a fresh session. No path/lineage: the attach
/// root is re-attached and every other fid is rebound directly to its (stable,
/// never-reused) inode id, so renames/unlinks are irrelevant.
#[derive(Clone)]
enum FidKind {
    Attach {
        afid: u32,
        uname: String,
        aname: String,
        n_uname: u32,
        /// Inode the attach resolved to (the `Rattach` qid path); clones rebind here.
        root_inode: u64,
    },
    /// Bound to an inode via `Trebind`, acting as `n_uname`.
    Inode { inode_id: u64, n_uname: u32 },
}

#[derive(Clone)]
struct FidRecord {
    kind: FidKind,
    /// `Some(flags)` if open; replayed with a `Tlopen`.
    opened: Option<u32>,
}

impl FidRecord {
    fn inode_id(&self) -> u64 {
        match self.kind {
            FidKind::Attach { root_inode, .. } => root_inode,
            FidKind::Inode { inode_id, .. } => inode_id,
        }
    }

    fn n_uname(&self) -> u32 {
        match self.kind {
            FidKind::Attach { n_uname, .. } | FidKind::Inode { n_uname, .. } => n_uname,
        }
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
}

pub struct NinePClient {
    /// HA node set to dial; single-element for a plain connection. The supervisor
    /// probes the whole list to find the serving leader.
    targets: Vec<Target>,
    requested_msize: u32,
    /// Current transport, swapped atomically by the reconnect supervisor.
    conn: ArcSwap<Conn>,
    /// False while a reconnect+replay is in progress; requests block until true.
    live: AtomicBool,
    live_notify: Notify,
    reconnect_notify: Arc<Notify>,
    msize: AtomicU32,
    /// Negotiated cumulative extension level, re-negotiated on reconnect.
    extensions: AtomicU8,
    fid_ctr: AtomicU32,
    fid_free: Mutex<Vec<u32>>,
    /// Recorded fids (by inode id) and held locks, replayed on reconnect.
    state: Mutex<SessionState>,
    /// `.zerofs4` durability tracking, keyed by the fid each mutation targets and
    /// carried across reconnect (an un-fsync'd write's obligation outlives the
    /// connection it was made on). Per-fid because `fsync` is per-fd POSIX: a verified
    /// fsync of one fid accounts only for that fid's writes. See [`Unsynced`].
    unsynced: DashMap<u32, Unsynced>,
}

/// One fid's outstanding un-fsync'd writes for `.zerofs4` verified fsync. Holds the
/// lineage token of the OLDEST un-fsync'd write on the fid (the one most likely to
/// predate a broken lineage), a `generation` bumped on every write to the fid, and a
/// `reported` flag.
///
/// `generation` gates the fsync completion: a write that lands between an fsync's
/// snapshot and its reply leaves the obligation in place rather than being cleared by
/// a result that did not cover it. `reported` survives a repeated fsync: an ESTALE
/// leaves the writes lost but still tracked (not cleared), so a later fsync of the fid
/// keeps returning ESTALE until the app redoes them; the first redo to the fid
/// supersedes the obligation.
#[derive(Default)]
struct Unsynced {
    oldest: Option<u64>,
    generation: u64,
    reported: bool,
}

impl Unsynced {
    /// Record a write under `token`: start a fresh obligation, or supersede a
    /// reported-lost one (this write is the app's redo). An existing un-reported
    /// obligation is kept, since its older token is the one that matters.
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

    /// On a successful fsync: the whole db was flushed, so every prior write is
    /// durable. Clear the obligation iff no write raced the in-flight fsync.
    fn clear_if_unchanged(&mut self, generation: u64) {
        if self.generation == generation {
            self.oldest = None;
            self.reported = false;
        }
    }

    /// On an ESTALE fsync: the writes are lost but reported, NOT resolved. Keep the
    /// obligation (so a later fsync still surfaces the loss) and flag it reported, so
    /// the app's next write supersedes it. Skipped if a write raced the fsync.
    fn report_if_unchanged(&mut self, generation: u64) {
        if self.generation == generation {
            self.reported = true;
        }
    }
}

impl NinePClient {
    /// Connect to a 9P server over TCP and negotiate the protocol version.
    pub async fn connect_tcp(addr: SocketAddr, requested_msize: u32) -> std::io::Result<Arc<Self>> {
        Self::connect(vec![Target::Tcp(addr)], requested_msize)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Connect to a 9P server over a Unix domain socket and negotiate the version.
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

    /// Connect across an HA node set: dials the serving leader and re-probes the
    /// set on reconnect to follow a failover. A single target is exactly
    /// [`Self::connect_tcp`]/[`Self::connect_unix`].
    pub async fn connect_multi(
        targets: Vec<Target>,
        requested_msize: u32,
    ) -> std::io::Result<Arc<Self>> {
        Self::connect(targets, requested_msize)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    async fn connect(targets: Vec<Target>, requested_msize: u32) -> ClientResult<Arc<Self>> {
        let reconnect_notify = Arc::new(Notify::new());
        let (conn, msize, extensions) =
            Self::probe(&targets, requested_msize, Arc::clone(&reconnect_notify)).await?;

        let client = Arc::new(Self {
            targets,
            requested_msize,
            conn: ArcSwap::new(conn),
            live: AtomicBool::new(true),
            live_notify: Notify::new(),
            reconnect_notify,
            msize: AtomicU32::new(msize),
            extensions: AtomicU8::new(extensions),
            fid_ctr: AtomicU32::new(1),
            fid_free: Mutex::new(Vec::new()),
            state: Mutex::new(SessionState::default()),
            unsynced: DashMap::new(),
        });
        client.spawn_supervisor();
        Ok(client)
    }

    /// Open a fresh socket, spawn its reader/writer tasks and negotiate.
    async fn connect_once(
        target: &Target,
        requested_msize: u32,
        reconnect_notify: Arc<Notify>,
    ) -> ClientResult<(Arc<Conn>, u32, u8)> {
        let (read, write) = dial(target).await?;
        let (writer_tx, writer_rx) = mpsc::channel::<Vec<u8>>(P9_CHANNEL_SIZE);
        let conn = Arc::new(Conn {
            writer_tx,
            pending: DashMap::new(),
            tag_ctr: AtomicU16::new(0),
            lineage_token: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            base: std::time::Instant::now(),
            last_alive: AtomicU64::new(0),
            probe_lock: tokio::sync::Mutex::new(()),
            writer_shutdown: Notify::new(),
            reader_shutdown: Notify::new(),
        });

        spawn_writer(
            write,
            writer_rx,
            Arc::clone(&conn),
            Arc::clone(&reconnect_notify),
        );
        spawn_reader(read, Arc::clone(&conn), reconnect_notify);

        match tokio::time::timeout(PROBE_TIMEOUT, negotiate_on(&conn, requested_msize)).await {
            Ok(Ok((msize, extensions))) => Ok((conn, msize, extensions)),
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

    /// Race all targets concurrently to find the serving leader: the first to
    /// pass the lease-gated [`Self::leader_check`] wins, the rest are torn down.
    /// A standby is not listening and a deposed leader fails the check, so only
    /// the real leader is adopted; concurrency stops a partitioned node stalling.
    async fn probe(
        targets: &[Target],
        requested_msize: u32,
        reconnect_notify: Arc<Notify>,
    ) -> ClientResult<(Arc<Conn>, u32, u8)> {
        let mut probes = FuturesUnordered::new();
        for target in targets {
            let target = target.clone();
            let notify = Arc::clone(&reconnect_notify);
            probes.push(async move {
                let (conn, msize, extensions) =
                    Self::connect_once(&target, requested_msize, notify).await?;
                // A fenced/lapsed leader still accepts connections; confirm the lease before adopting.
                match tokio::time::timeout(PROBE_TIMEOUT, Self::leader_check(&conn)).await {
                    Ok(Ok(())) => Ok((conn, msize, extensions)),
                    Ok(Err(e)) => {
                        conn.shutdown();
                        Err(e)
                    }
                    Err(_) => {
                        conn.shutdown();
                        Err(ClientError::Disconnected)
                    }
                }
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

        // Tear down the still-running losers (each bounded by PROBE_TIMEOUT) so their tasks don't leak.
        if !probes.is_empty() {
            tokio::spawn(async move {
                while let Some(res) = probes.next().await {
                    if let Ok((conn, _, _)) = res {
                        conn.shutdown();
                    }
                }
            });
        }

        winner.ok_or_else(|| last_err.unwrap_or(ClientError::Disconnected))
    }

    /// Throwaway lease-gated round trip confirming a node is the serving leader:
    /// attach the root, stat it (a lease-gated read), clunk. Any error means it
    /// is not serving. Uses a reserved fid the session never allocates.
    async fn leader_check(conn: &Conn) -> ClientResult<()> {
        const PROBE_FID: u32 = 0xFFFF_FFFE;
        let n_uname = unsafe { libc::geteuid() };
        match Self::send_raw_rpc(
            conn,
            Message::Tattach(Tattach {
                fid: PROBE_FID,
                afid: NOFID,
                uname: P9String::new(Vec::new()),
                aname: P9String::new(Vec::new()),
                n_uname,
            }),
        )
        .await
        {
            Ok(Message::Rattach(_)) => {}
            Ok(_) => return Err(ClientError::Unexpected("probe attach")),
            Err(e) => return Err(e),
        }
        let stat = Self::send_raw_rpc(
            conn,
            Message::Tgetattr(Tgetattr {
                fid: PROBE_FID,
                request_mask: GETATTR_ALL,
            }),
        )
        .await;
        // Best-effort release: keeps a winning connection clean; a loser is torn down anyway.
        let _ = Self::send_raw_rpc(conn, Message::Tclunk(Tclunk { fid: PROBE_FID })).await;
        match stat {
            Ok(Message::Rgetattr(_)) => Ok(()),
            Ok(_) => Err(ClientError::Unexpected("probe getattr")),
            Err(e) => Err(e),
        }
    }

    /// The reconnect supervisor: waits for the live connection to die, then
    /// reconnects and replays the session, retrying indefinitely with backoff.
    fn spawn_supervisor(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let notify = Arc::clone(&self.reconnect_notify);
        tokio::spawn(async move {
            loop {
                // Enable the waiter before reading `dead` so a set-dead-then-notify isn't lost.
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
                            debug!("9P reconnect failed ({e}); retrying in {backoff:?}");
                            drop(this);
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                        }
                    }
                }
            }
        });
    }

    /// One reconnect attempt: dial, replay state, then swap the connection in.
    async fn reconnect_once(&self) -> ClientResult<()> {
        let (conn, msize, extensions) = Self::probe(
            &self.targets,
            self.requested_msize,
            Arc::clone(&self.reconnect_notify),
        )
        .await?;

        self.msize.store(msize, Ordering::Relaxed);
        self.extensions.store(extensions, Ordering::Relaxed);
        // Replay failure, or a stall past REPLAY_TIMEOUT, discards this connection:
        // tear it down so its fd is not leaked and the supervisor reconnects afresh.
        match tokio::time::timeout(REPLAY_TIMEOUT, self.replay(&conn)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                conn.shutdown();
                return Err(e);
            }
            Err(_) => {
                conn.shutdown();
                warn!("9P session replay stalled past {REPLAY_TIMEOUT:?}; retrying reconnect");
                return Err(ClientError::Disconnected);
            }
        }
        let old = self.conn.swap(conn);
        old.dead.store(true, Ordering::Release);
        old.writer_shutdown.notify_one();

        Ok(())
    }

    /// Rebuild the recorded session onto `conn`, then re-acquire locks. Attach
    /// fids must replay first: re-attaching restores the aname subtree root the
    /// subsequent `Trebind`s validate against. A transport failure aborts (caller
    /// reconnects afresh); a server error for a fid means its inode is gone, so it
    /// is dropped.
    async fn replay(&self, conn: &Conn) -> ClientResult<()> {
        let snapshot = self.state.lock().unwrap().clone();

        let (attaches, rebinds): (Vec<_>, Vec<_>) = snapshot
            .fids
            .iter()
            .partition(|(_, rec)| matches!(rec.kind, FidKind::Attach { .. }));
        for (&fid, rec) in attaches.into_iter().chain(rebinds) {
            let restored = Self::replay_fid(conn, fid, rec).await?;
            if restored && let Some(flags) = rec.opened {
                match Self::send_raw_rpc(conn, Message::Tlopen(Tlopen { fid, flags })).await {
                    Ok(Message::Rlopen(_)) => {}
                    Ok(_) => return Err(ClientError::Unexpected("replay lopen")),
                    Err(ClientError::Errno(_)) => {} // reopen failed; leave it bound
                    Err(e) => return Err(e),
                }
            }
        }

        // Re-acquire locks, best-effort (gone fids and conflicts are ignored).
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
                Ok(_) | Err(ClientError::Errno(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Re-establish one fid on `conn`: `Ok(true)` restored, `Ok(false)` gone
    /// (skip it), `Err` on transport failure.
    ///
    /// A confined (non-empty aname) attach that fails to re-resolve is a hard
    /// error, not a gone fid: it establishes the confinement root the `Trebind`s
    /// validate against, so dropping it would silently widen the session to the
    /// whole filesystem. An empty-aname attach has no confinement, so it follows
    /// the gone-fid path.
    async fn replay_fid(conn: &Conn, fid: u32, rec: &FidRecord) -> ClientResult<bool> {
        let confined_attach =
            matches!(&rec.kind, FidKind::Attach { aname, .. } if !aname.is_empty());
        let body = match &rec.kind {
            FidKind::Attach {
                afid,
                uname,
                aname,
                n_uname,
                ..
            } => Message::Tattach(Tattach {
                fid,
                afid: *afid,
                uname: P9String::new(uname.clone().into_bytes()),
                aname: P9String::new(aname.clone().into_bytes()),
                n_uname: *n_uname,
            }),
            FidKind::Inode { inode_id, n_uname } => Message::Trebind(Trebind {
                fid,
                inode_id: *inode_id,
                n_uname: *n_uname,
            }),
        };
        match Self::send_raw_rpc(conn, body).await {
            Ok(Message::Rattach(_)) | Ok(Message::Rrebind(_)) => Ok(true),
            Ok(_) => Err(ClientError::Unexpected("replay fid")),
            Err(e @ ClientError::Errno(_)) if confined_attach => Err(e),
            Err(ClientError::Errno(_)) => Ok(false), // inode gone -> drop this fid
            Err(e) => Err(e),
        }
    }

    /// The negotiated message size.
    pub fn msize(&self) -> u32 {
        self.msize.load(Ordering::Relaxed)
    }

    /// `.zerofs` fast paths (`walk_getattr`/`readdirplus`) negotiated.
    pub fn extensions_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 1
    }

    /// `.zerofs2` negotiated: compound create/open and stat-carrying replies.
    pub fn extensions_v2_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 2
    }

    /// `.zerofs3` negotiated: covered requests carry a frame op-id for dedup.
    fn op_id_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 3
    }

    /// `.zerofs4` negotiated: this connection uses durability-verified fsync.
    fn durability_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 4
    }

    /// `.zerofs5` negotiated: a file open can fold in its first read (Tlopenatread).
    pub fn extensions_v5_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 5
    }

    /// `.zerofs6` negotiated: the atomic ZeroFS-private Tfallocate request.
    pub fn fallocate_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 6
    }

    /// Record a just-acked mutating write on `fid` as un-fsync'd under `token` (the
    /// lineage token of the connection it returned on).
    fn note_unsynced(&self, fid: u32, token: u64) {
        self.unsynced.entry(fid).or_default().note(token);
    }

    /// Snapshot `(oldest token, generation)` of `fid` for a verified fsync.
    fn snapshot_unsynced(&self, fid: u32) -> (Option<u64>, u64) {
        self.unsynced.get(&fid).map_or((None, 0), |u| u.snapshot())
    }

    /// On a successful (Rfsync) verified fsync of `fid`: clear its obligation, unless a
    /// write to it raced the in-flight fsync. The drop of the `get_mut` guard releases
    /// the shard lock before `remove_if`, which re-checks under the lock, so a note that
    /// races between the clear and the remove keeps the entry rather than being dropped.
    fn clear_unsynced_if_unchanged(&self, fid: u32, generation: u64) {
        if let Some(mut u) = self.unsynced.get_mut(&fid) {
            u.clear_if_unchanged(generation);
        }
        self.unsynced.remove_if(&fid, |_, u| u.oldest.is_none());
    }

    /// On an ESTALE verified fsync of `fid`: keep its obligation but flag it reported,
    /// so a later fsync of the fid still surfaces the loss and the app's next write to
    /// it supersedes it. Skipped if a write to the fid raced the fsync.
    fn report_unsynced_if_unchanged(&self, fid: u32, generation: u64) {
        if let Some(mut u) = self.unsynced.get_mut(&fid) {
            u.report_if_unchanged(generation);
        }
    }

    /// Drop a fid's durability tracking when the fid is clunked/recycled, so a reused
    /// fid never inherits a stale obligation (closing a fid without fsync makes no
    /// durability promise, per POSIX).
    fn forget_unsynced(&self, fid: u32) {
        self.unsynced.remove(&fid);
    }

    /// The lineage token of `fid`'s oldest un-fsync'd write, if any. Used to CARRY a
    /// failover-aware handle's obligation across a re-open onto a new connection.
    pub fn unsynced_oldest(&self, fid: u32) -> Option<u64> {
        self.snapshot_unsynced(fid).0
    }

    /// Seed `fid`'s obligation with a token carried from a prior connection (a failover
    /// re-open), so a verified fsync after the re-route still verifies the carried
    /// un-fsync'd write instead of treating the fresh handle as clean.
    pub fn seed_unsynced(&self, fid: u32, token: u64) {
        self.note_unsynced(fid, token);
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
        self.msize().saturating_sub(P9_TWRITE_HDR)
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
        self.fid_free.lock().unwrap().push(fid);
    }

    /// Allocated fids not yet returned to the free list. Leak-accounting diagnostic.
    pub fn outstanding_fids(&self) -> usize {
        let allocated = self.fid_ctr.load(Ordering::Relaxed).saturating_sub(1) as usize;
        allocated.saturating_sub(self.fid_free.lock().unwrap().len())
    }

    /// Block until the connection is live (i.e. not mid-reconnect).
    async fn wait_until_live(&self) {
        loop {
            let notified = self.live_notify.notified();
            tokio::pin!(notified);
            // Register the waiter *before* the check to avoid a lost wakeup.
            notified.as_mut().enable();
            if self.live.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Allocate a tag on `conn` and register the response slot.
    fn alloc_tag(conn: &Conn, otx: oneshot::Sender<Bytes>) -> u16 {
        let mut otx = Some(otx);
        loop {
            let candidate = conn.tag_ctr.fetch_add(1, Ordering::Relaxed);
            if candidate == NOTAG {
                continue;
            }
            match conn.pending.entry(candidate) {
                Entry::Vacant(slot) => {
                    slot.insert(otx.take().unwrap());
                    return candidate;
                }
                Entry::Occupied(_) => continue,
            }
        }
    }

    /// Send a request, blocking through any reconnect and resending across one
    /// (see the module docs for the in-flight double-apply caveat).
    async fn send_request(&self, op_id: [u8; 16], body: Message) -> ClientResult<Message> {
        'resend: loop {
            self.wait_until_live().await;
            let conn = self.conn.load_full();

            let (otx, mut orx) = oneshot::channel();
            let tag = Self::alloc_tag(&conn, otx);

            // `live` can briefly lag a conn's death, so we may have loaded the dead
            // one. The reader sets `dead` BEFORE it drains `pending` on exit, so a
            // slot registered after we observe `dead` here would never complete
            // (hanging `orx.await` forever). Drop it, nudge the supervisor, retry.
            if conn.dead.load(Ordering::Acquire) {
                conn.pending.remove(&tag);
                self.reconnect_notify.notify_waiters();
                tokio::task::yield_now().await;
                continue;
            }

            // The op-id is stable across this loop's resends, so the
            // resend-on-reply-loss below is deduplicated rather than double-applied.
            let bytes = match P9Message::new_with_op_id(tag, op_id, body.clone())
                .to_bytes_ctx(self.op_id_enabled())
            {
                Ok(b) => b,
                Err(e) => {
                    conn.pending.remove(&tag);
                    return Err(ClientError::Codec(e));
                }
            };
            if conn.writer_tx.send(bytes).await.is_err() {
                // Not sent: safe to retry after reconnect.
                conn.pending.remove(&tag);
                tokio::task::yield_now().await;
                continue;
            }

            // Await the reply. A missed REQUEST_TIMEOUT is not by itself proof the
            // leader is dead: under write backpressure a healthy leader is simply
            // slow. Grant another window for as long as the connection keeps proving
            // itself alive (see `conn_alive`), bounded by MAX_LIVENESS_EXTENSIONS so a
            // wedged leader still fails over. Only a hung/silent connection is torn
            // down. We re-await the SAME `orx` across extensions, so the request stays
            // in flight (no duplicate send) and the stable op-id keeps any eventual
            // resend exactly-once.
            let mut extensions = 0u32;
            let frame = loop {
                match tokio::time::timeout(REQUEST_TIMEOUT, &mut orx).await {
                    Ok(Ok(frame)) => break frame,
                    Ok(Err(_)) => {
                        // Lost the reply to a drop: wait for reconnect and resend.
                        conn.pending.remove(&tag);
                        tokio::task::yield_now().await;
                        continue 'resend;
                    }
                    Err(_) => {
                        if extensions < MAX_LIVENESS_EXTENSIONS && Self::conn_alive(&conn).await {
                            extensions += 1;
                            continue;
                        }
                        // Hung, or out of patience: tear it down, reprobe, resend.
                        conn.pending.remove(&tag);
                        self.force_reprobe(&conn);
                        tokio::task::yield_now().await;
                        continue 'resend;
                    }
                }
            };

            // Parse here, not on the reader task, to keep the reader unblocked.
            let (_, msg) = P9Message::from_bytes((&frame, 0)).map_err(ClientError::Codec)?;
            // Bound node is no longer the leader (lease lapsed or fenced): re-probe
            // and resend. The stable op-id keeps the resend exactly-once even if
            // leadership moves again before it lands.
            if let Message::Rlerror(ref e) = msg.body
                && e.ecode == P9_ENOTLEADER
            {
                self.force_reprobe(&conn);
                tokio::task::yield_now().await;
                continue;
            }
            // A mutating op that just succeeded is durable-but-un-fsync'd under this
            // connection's lineage token; track it on every fid a later verified fsync
            // checks (one for most ops, both directories for a renameat). A failed
            // mutation (Rlerror) changed nothing.
            if self.durability_enabled() && !matches!(msg.body, Message::Rlerror(_)) {
                let token = conn.lineage_token.load(Ordering::Relaxed);
                for fid in body.durability_fids() {
                    self.note_unsynced(fid, token);
                }
            }
            return Ok(msg.body);
        }
    }

    /// Decide whether a connection that just missed a reply deadline is still
    /// serving (merely slow, e.g. under write backpressure) rather than hung.
    ///
    /// Cheap path: if the reader decoded any frame within [`LIVENESS_WINDOW`] the
    /// socket is provably live, so no probe is sent (the common case under load,
    /// where other replies keep arriving). A silent connection gets one explicit,
    /// lease-gated round trip on the reserved probe fid; this also catches a quietly
    /// deposed leader (the lease check fails), routing us to the new one. The probe
    /// is single-flighted so concurrent waiters do not collide on the probe fid, and
    /// a re-check after taking the lock lets those queued behind a successful probe
    /// skip their own.
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
            tokio::time::timeout(PROBE_TIMEOUT, Self::leader_check(conn)).await,
            Ok(Ok(()))
        )
    }

    /// Tear down `conn` and wake the supervisor to re-probe. Used on
    /// [`P9_ENOTLEADER`]: the socket is still up, so nothing else would reconnect.
    fn force_reprobe(&self, conn: &Arc<Conn>) {
        conn.shutdown();
        self.reconnect_notify.notify_waiters();
    }

    /// A one-shot send on a specific connection, bypassing the live-gate and
    /// state recording. Used during reconnect to replay the session.
    async fn send_raw(conn: &Conn, body: Message) -> ClientResult<Message> {
        let (otx, orx) = oneshot::channel();
        let tag = Self::alloc_tag(conn, otx);
        let bytes = match P9Message::new(tag, body).to_bytes() {
            Ok(b) => b,
            Err(e) => {
                conn.pending.remove(&tag);
                return Err(ClientError::Codec(e));
            }
        };
        if conn.writer_tx.send(bytes).await.is_err() {
            conn.pending.remove(&tag);
            return Err(ClientError::Disconnected);
        }
        let frame = orx.await.map_err(|_| ClientError::Disconnected)?;
        let (_, msg) = P9Message::from_bytes((&frame, 0)).map_err(ClientError::Codec)?;
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
        self.rpc_with_op_id([0u8; 16], body).await
    }

    /// Like [`Self::rpc`] but tags the request with an idempotency op-id (on the
    /// wire only for the protocol's `carries_op_id` types).
    async fn rpc_with_op_id(&self, op_id: [u8; 16], body: Message) -> ClientResult<Message> {
        match self.send_request(op_id, body).await? {
            Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
            other => Ok(other),
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
        let resp = self
            .rpc(Message::Tattach(Tattach {
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
                        kind: FidKind::Attach {
                            afid,
                            uname: uname.to_string(),
                            aname: aname.to_string(),
                            n_uname,
                            root_inode: r.qid.path,
                        },
                        opened: None,
                    },
                );
                Ok(r.qid)
            }
            _ => Err(ClientError::Unexpected("attach")),
        }
    }

    /// Bind `fid` to an inode by id (no path walk), acting as `n_uname`. Used for
    /// per-user fids and reconnect replay.
    pub async fn rebind(&self, fid: u32, inode_id: u64, n_uname: u32) -> ClientResult<Qid> {
        let resp = self
            .rpc(Message::Trebind(Trebind {
                fid,
                inode_id,
                n_uname,
            }))
            .await?;
        match resp {
            Message::Rrebind(r) => {
                self.state.lock().unwrap().fids.insert(
                    fid,
                    FidRecord {
                        kind: FidKind::Inode { inode_id, n_uname },
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
        let resp = self
            .rpc(Message::Twalk(Twalk {
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
                    let n_uname = st.fids.get(&fid).map(FidRecord::n_uname);
                    let inode_id = if names.is_empty() {
                        st.fids.get(&fid).map(FidRecord::inode_id)
                    } else {
                        r.wqids.last().map(|q| q.path)
                    };
                    if let (Some(inode_id), Some(n_uname)) = (inode_id, n_uname) {
                        st.fids.insert(
                            newfid,
                            FidRecord {
                                kind: FidKind::Inode { inode_id, n_uname },
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

    /// Full walk plus the final stat in one round trip (Twalkgetattr fast path).
    /// Records `newfid` like `walk`. Only valid with [`Self::extensions_enabled`].
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
        let resp = self
            .rpc(Message::Twalkgetattr(Twalkgetattr {
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
                    let n_uname = st.fids.get(&fid).map(FidRecord::n_uname);
                    let inode_id = if names.is_empty() {
                        st.fids.get(&fid).map(FidRecord::inode_id)
                    } else {
                        r.wqids.last().map(|q| q.path)
                    };
                    if let (Some(inode_id), Some(n_uname)) = (inode_id, n_uname) {
                        st.fids.insert(
                            newfid,
                            FidRecord {
                                kind: FidKind::Inode { inode_id, n_uname },
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
        let resp = self.rpc(Message::Tclunk(Tclunk { fid })).await;
        // The fid is gone regardless of the reply.
        {
            let mut st = self.state.lock().unwrap();
            st.fids.remove(&fid);
            st.locks.retain(|l| l.fid != fid);
        }
        match resp? {
            Message::Rclunk(_) => Ok(()),
            _ => Err(ClientError::Unexpected("clunk")),
        }
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
        if !self.fallocate_enabled() {
            return Err(ClientError::Errno(libc::EOPNOTSUPP as u32));
        }
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

    /// Like [`Self::setattr`] but the reply carries the post-op stat. Only valid
    /// with [`Self::extensions_v2_enabled`].
    pub async fn setattr_attr(&self, ts: Tsetattr) -> ClientResult<Stat> {
        match self.rpc(Message::Tsetattrattr(ts)).await? {
            Message::Rsetattrattr(r) => Ok(r.stat),
            _ => Err(ClientError::Unexpected("setattr_attr")),
        }
    }

    pub async fn lopen(&self, fid: u32, flags: u32) -> ClientResult<(Qid, u32)> {
        match self.rpc(Message::Tlopen(Tlopen { fid, flags })).await? {
            Message::Rlopen(r) => {
                if let Some(rec) = self.state.lock().unwrap().fids.get_mut(&fid) {
                    rec.opened = Some(flags);
                }
                Ok((r.qid, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lopen")),
        }
    }

    /// Open `fid`'s inode on a fresh `newfid` in one round trip (Tlopenat fast
    /// path = Twalk(clone) + Tlopen); `fid` untouched. Only valid with
    /// [`Self::extensions_v2_enabled`].
    pub async fn lopenat(&self, fid: u32, newfid: u32, flags: u32) -> ClientResult<(Qid, u32)> {
        let resp = self
            .rpc(Message::Tlopenat(Tlopenat { fid, newfid, flags }))
            .await?;
        match resp {
            Message::Rlopenat(r) => {
                let mut st = self.state.lock().unwrap();
                if let Some(n_uname) = st.fids.get(&fid).map(FidRecord::n_uname) {
                    st.fids.insert(
                        newfid,
                        FidRecord {
                            kind: FidKind::Inode {
                                inode_id: r.qid.path,
                                n_uname,
                            },
                            opened: Some(flags),
                        },
                    );
                }
                Ok((r.qid, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lopenat")),
        }
    }

    /// `.zerofs5`: open `fid`'s inode on `newfid` like [`Self::lopenat`] AND prefetch
    /// up to `count` bytes from offset 0 in the same round trip. Returns the qid, the
    /// iounit, the prefetched bytes, and whether they reach EOF (the whole file fit in
    /// `count`). The inline read is best-effort: a server-side read error yields empty
    /// data with `eof = false`, and the open still succeeds. Only valid with
    /// [`Self::extensions_v5_enabled`].
    pub async fn lopenatread(
        &self,
        fid: u32,
        newfid: u32,
        flags: u32,
        count: u32,
    ) -> ClientResult<(Qid, u32, Bytes, bool)> {
        let resp = self
            .rpc(Message::Tlopenatread(Tlopenatread {
                fid,
                newfid,
                flags,
                count,
            }))
            .await?;
        match resp {
            Message::Rlopenatread(r) => {
                // Same fid bookkeeping as lopenat: bind newfid to the opened inode.
                let mut st = self.state.lock().unwrap();
                if let Some(n_uname) = st.fids.get(&fid).map(FidRecord::n_uname) {
                    st.fids.insert(
                        newfid,
                        FidRecord {
                            kind: FidKind::Inode {
                                inode_id: r.qid.path,
                                n_uname,
                            },
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
        self.lcreate_op_id(fid, name, flags, mode, gid, new_op_id())
            .await
    }

    /// [`Self::lcreate`] with an idempotency op-id (all-zero to opt out).
    pub async fn lcreate_op_id(
        &self,
        fid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<(Qid, u32)> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tlcreate(Tlcreate {
                    fid,
                    name: P9String::new(name.to_vec()),
                    flags,
                    mode,
                    gid,
                }),
            )
            .await?;
        match resp {
            Message::Rlcreate(r) => {
                // `fid` now names the created file: record its inode and the reopen
                // flags (create-only bits stripped) so replay rebinds and reopens it.
                let reopen = flags & !((libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC) as u32);
                let mut st = self.state.lock().unwrap();
                if let Some(rec) = st.fids.get_mut(&fid) {
                    let n_uname = rec.n_uname();
                    rec.kind = FidKind::Inode {
                        inode_id: r.qid.path,
                        n_uname,
                    };
                    rec.opened = Some(reopen);
                }
                Ok((r.qid, r.iounit))
            }
            _ => Err(ClientError::Unexpected("lcreate")),
        }
    }

    /// Create and open `name` under `dfid`, returning the post-op stat in one
    /// round trip (Tlcreateattr fast path = Twalk(clone) + Tlcreate + Tgetattr).
    /// Unlike [`Self::lcreate`], `dfid` is left untouched (the file opens on
    /// `newfid`). Only valid with [`Self::extensions_v2_enabled`].
    pub async fn lcreateattr(
        &self,
        dfid: u32,
        newfid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> ClientResult<(Stat, u32)> {
        self.lcreateattr_op_id(dfid, newfid, name, flags, mode, gid, new_op_id())
            .await
    }

    /// [`Self::lcreateattr`] with an idempotency op-id (all-zero to opt out).
    #[allow(clippy::too_many_arguments)]
    pub async fn lcreateattr_op_id(
        &self,
        dfid: u32,
        newfid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<(Stat, u32)> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tlcreateattr(Tlcreateattr {
                    dfid,
                    newfid,
                    name: P9String::new(name.to_vec()),
                    flags,
                    mode,
                    gid,
                }),
            )
            .await?;
        match resp {
            Message::Rlcreateattr(r) => {
                let reopen = flags & !((libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC) as u32);
                let mut st = self.state.lock().unwrap();
                if let Some(n_uname) = st.fids.get(&dfid).map(FidRecord::n_uname) {
                    st.fids.insert(
                        newfid,
                        FidRecord {
                            kind: FidKind::Inode {
                                inode_id: r.stat.qid.path,
                                n_uname,
                            },
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
        let max = self.max_io().max(1);
        let first = self.read_once(fid, offset, size.min(max)).await?;
        if size <= max || (first.len() as u32) < size.min(max) {
            return Ok(first);
        }
        // Spans multiple chunks. `size` is caller-controlled and may be far
        // larger than the data, so reserve a bounded amount and grow as it fills.
        let mut out = BytesMut::with_capacity(size.min(max.saturating_mul(2)) as usize);
        let mut off = offset + first.len() as u64;
        out.extend_from_slice(&first);
        while (out.len() as u32) < size {
            // Re-read the chunk cap each iteration: a reconnect can renegotiate a
            // smaller msize, and a chunk short of the *current* cap is the only
            // reliable end-of-file signal.
            let max = self.max_io().max(1);
            let want = (size - out.len() as u32).min(max);
            let data = self.read_once(fid, off, want).await?;
            let got = data.len() as u32;
            out.extend_from_slice(&data);
            off += got as u64;
            if got < want {
                break; // short read => end of file
            }
        }
        Ok(out.freeze())
    }

    async fn read_once(&self, fid: u32, offset: u64, count: u32) -> ClientResult<Bytes> {
        let resp = self
            .rpc(Message::Tread(Tread { fid, offset, count }))
            .await?;
        match resp {
            Message::Rread(r) => Ok(r.data.0),
            _ => Err(ClientError::Unexpected("read")),
        }
    }

    /// Write all of `data` at `offset`, splitting into multiple Twrite requests
    /// when it exceeds the negotiated msize. Returns the total bytes written.
    pub async fn write(&self, fid: u32, offset: u64, data: &[u8]) -> ClientResult<u64> {
        let mut written = 0usize;
        while written < data.len() {
            // Re-read the cap each iteration: a reconnect can renegotiate a
            // smaller msize, and the remaining chunks must respect the new one.
            let max = self.max_write_payload().max(1) as usize;
            let end = (written + max).min(data.len());
            let chunk = &data[written..end];
            let n = self.write_once(fid, offset + written as u64, chunk).await?;
            if n == 0 {
                break;
            }
            written += n as usize;
            if (n as usize) < chunk.len() {
                break; // short write
            }
        }
        Ok(written as u64)
    }

    async fn write_once(&self, fid: u32, offset: u64, data: &[u8]) -> ClientResult<u32> {
        let resp = self
            .rpc(Message::Twrite(Twrite {
                fid,
                offset,
                count: data.len() as u32,
                data: DekuBytes::from(data.to_vec()),
            }))
            .await?;
        match resp {
            Message::Rwrite(r) => Ok(r.count),
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

    /// Like [`Self::readdir`] but each entry carries its full stat (Treaddirattr
    /// fast path). Only valid with [`Self::extensions_enabled`].
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
        self.mkdir_op_id(dfid, name, mode, gid, new_op_id()).await
    }

    /// [`Self::mkdir`] with an idempotency op-id (all-zero to opt out).
    pub async fn mkdir_op_id(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<Qid> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tmkdir(Tmkdir {
                    dfid,
                    name: P9String::new(name.to_vec()),
                    mode,
                    gid,
                }),
            )
            .await?;
        match resp {
            Message::Rmkdir(r) => Ok(r.qid),
            _ => Err(ClientError::Unexpected("mkdir")),
        }
    }

    /// Like [`Self::mkdir`] but the reply carries the new directory's full stat.
    /// Only valid with [`Self::extensions_v2_enabled`].
    pub async fn mkdir_attr(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        gid: u32,
    ) -> ClientResult<Stat> {
        self.mkdir_attr_op_id(dfid, name, mode, gid, [0u8; 16])
            .await
    }

    /// [`Self::mkdir_attr`] with an idempotency op-id (all-zero to opt out).
    pub async fn mkdir_attr_op_id(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tmkdirattr(Tmkdir {
                    dfid,
                    name: P9String::new(name.to_vec()),
                    mode,
                    gid,
                }),
            )
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
        self.symlink_op_id(dfid, name, target, gid, new_op_id())
            .await
    }

    /// [`Self::symlink`] with an idempotency op-id (all-zero to opt out).
    pub async fn symlink_op_id(
        &self,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<Qid> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tsymlink(Tsymlink {
                    dfid,
                    name: P9String::new(name.to_vec()),
                    symtgt: P9String::new(target.to_vec()),
                    gid,
                }),
            )
            .await?;
        match resp {
            Message::Rsymlink(r) => Ok(r.qid),
            _ => Err(ClientError::Unexpected("symlink")),
        }
    }

    /// Like [`Self::symlink`] but the reply carries the new link's full stat.
    /// Only valid with [`Self::extensions_v2_enabled`].
    pub async fn symlink_attr(
        &self,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        gid: u32,
    ) -> ClientResult<Stat> {
        self.symlink_attr_op_id(dfid, name, target, gid, [0u8; 16])
            .await
    }

    /// [`Self::symlink_attr`] with an idempotency op-id (all-zero to opt out).
    pub async fn symlink_attr_op_id(
        &self,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tsymlinkattr(Tsymlink {
                    dfid,
                    name: P9String::new(name.to_vec()),
                    symtgt: P9String::new(target.to_vec()),
                    gid,
                }),
            )
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
        self.mknod_op_id(dfid, name, mode, major, minor, gid, new_op_id())
            .await
    }

    /// [`Self::mknod`] with an idempotency op-id (all-zero to opt out).
    #[allow(clippy::too_many_arguments)]
    pub async fn mknod_op_id(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<Qid> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tmknod(Tmknod {
                    dfid,
                    name: P9String::new(name.to_vec()),
                    mode,
                    major,
                    minor,
                    gid,
                }),
            )
            .await?;
        match resp {
            Message::Rmknod(r) => Ok(r.qid),
            _ => Err(ClientError::Unexpected("mknod")),
        }
    }

    /// Like [`Self::mknod`] but the reply carries the new node's full stat.
    /// Only valid with [`Self::extensions_v2_enabled`].
    pub async fn mknod_attr(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
    ) -> ClientResult<Stat> {
        self.mknod_attr_op_id(dfid, name, mode, major, minor, gid, [0u8; 16])
            .await
    }

    /// [`Self::mknod_attr`] with an idempotency op-id (all-zero to opt out).
    #[allow(clippy::too_many_arguments)]
    pub async fn mknod_attr_op_id(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
        op_id: [u8; 16],
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tmknodattr(Tmknod {
                    dfid,
                    name: P9String::new(name.to_vec()),
                    mode,
                    major,
                    minor,
                    gid,
                }),
            )
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
        self.link_op_id(dfid, fid, name, new_op_id()).await
    }

    /// [`Self::link`] with an idempotency op-id (all-zero to opt out).
    pub async fn link_op_id(
        &self,
        dfid: u32,
        fid: u32,
        name: &[u8],
        op_id: [u8; 16],
    ) -> ClientResult<()> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tlink(Tlink {
                    dfid,
                    fid,
                    name: P9String::new(name.to_vec()),
                }),
            )
            .await?;
        match resp {
            Message::Rlink(_) => Ok(()),
            _ => Err(ClientError::Unexpected("link")),
        }
    }

    /// Like [`Self::link`] but the reply carries the linked inode's post-op stat
    /// (updated nlink). Only valid with [`Self::extensions_v2_enabled`].
    pub async fn link_attr(&self, dfid: u32, fid: u32, name: &[u8]) -> ClientResult<Stat> {
        self.link_attr_op_id(dfid, fid, name, [0u8; 16]).await
    }

    /// [`Self::link_attr`] with an idempotency op-id (all-zero to opt out).
    pub async fn link_attr_op_id(
        &self,
        dfid: u32,
        fid: u32,
        name: &[u8],
        op_id: [u8; 16],
    ) -> ClientResult<Stat> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tlinkattr(Tlink {
                    dfid,
                    fid,
                    name: P9String::new(name.to_vec()),
                }),
            )
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
        self.renameat_op_id(olddirfid, oldname, newdirfid, newname, new_op_id())
            .await
    }

    /// [`Self::renameat`] with an idempotency op-id (all-zero to opt out).
    pub async fn renameat_op_id(
        &self,
        olddirfid: u32,
        oldname: &[u8],
        newdirfid: u32,
        newname: &[u8],
        op_id: [u8; 16],
    ) -> ClientResult<()> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Trenameat(Trenameat {
                    olddirfid,
                    oldname: P9String::new(oldname.to_vec()),
                    newdirfid,
                    newname: P9String::new(newname.to_vec()),
                }),
            )
            .await?;
        match resp {
            Message::Rrenameat(_) => Ok(()),
            _ => Err(ClientError::Unexpected("renameat")),
        }
    }

    pub async fn unlinkat(&self, dirfid: u32, name: &[u8], flags: u32) -> ClientResult<()> {
        self.unlinkat_op_id(dirfid, name, flags, new_op_id()).await
    }

    /// [`Self::unlinkat`] with an idempotency op-id (all-zero to opt out).
    pub async fn unlinkat_op_id(
        &self,
        dirfid: u32,
        name: &[u8],
        flags: u32,
        op_id: [u8; 16],
    ) -> ClientResult<()> {
        let resp = self
            .rpc_with_op_id(
                op_id,
                Message::Tunlinkat(Tunlinkat {
                    dirfid,
                    name: P9String::new(name.to_vec()),
                    flags,
                }),
            )
            .await?;
        match resp {
            Message::Runlinkat(_) => Ok(()),
            _ => Err(ClientError::Unexpected("unlinkat")),
        }
    }

    /// Verified fsync over an inode's whole fid set. A POSIX `fsync(fd)` persists the
    /// whole file, but the FUSE mount spreads one inode's mutations across fids: data
    /// writes ride the open handle, setattr and directory-entry ops ride the per-user
    /// inode fid. So the fsync presents the oldest outstanding token across all of `fids`
    /// and verifies them together; a broken lineage on any of them ESTALEs the whole
    /// call, and only this inode's fids are presented. `primary` carries the Tfsyncdur;
    /// single-fid callers use [`Self::fsync`].
    pub async fn fsync_inode(&self, fids: &[u32], primary: u32, datasync: u32) -> ClientResult<()> {
        if self.durability_enabled() {
            // Present the oldest token across the fids: a broken lineage is an older
            // token than the current one, so the min ESTALEs the whole fsync. Each
            // generation gates that fid's completion against a write racing the RPC.
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
                    // The whole db was flushed, so every covered fid's writes are durable.
                    for (fid, generation) in snaps {
                        self.clear_unsynced_if_unchanged(fid, generation);
                    }
                    Ok(())
                }
                Ok(_) => Err(ClientError::Unexpected("fsync")),
                Err(ClientError::Errno(e)) if e == libc::ESTALE as u32 => {
                    // A covered fid's lineage broke: its writes are lost. Keep each
                    // obligation (not cleared, so a later fsync of the inode still
                    // surfaces the loss), flagged reported so the app's redo supersedes
                    // it. Only this inode's fids are touched.
                    for (fid, generation) in snaps {
                        self.report_unsynced_if_unchanged(fid, generation);
                    }
                    Err(ClientError::Errno(e))
                }
                Err(e) => Err(e),
            }
        } else if fids
            .iter()
            .any(|&fid| self.snapshot_unsynced(fid).0.is_some())
        {
            // Fail-closed: we hold un-fsync'd writes from an earlier `.zerofs4` session
            // but are now on a pre-`.zerofs4` connection that cannot verify durability.
            // A plain unchecked fsync could succeed over a lost write, so refuse.
            Err(ClientError::Errno(libc::ESTALE as u32))
        } else {
            match self
                .rpc(Message::Tfsync(Tfsync {
                    fid: primary,
                    datasync,
                }))
                .await?
            {
                Message::Rfsync(_) => Ok(()),
                _ => Err(ClientError::Unexpected("fsync")),
            }
        }
    }

    /// Verified fsync of a single fid (its own writes only). The library `File` API
    /// keeps one fid per open handle, so a per-fid fsync is exact; the FUSE mount,
    /// which fans an inode across fids, uses [`Self::fsync_inode`].
    pub async fn fsync(&self, fid: u32, datasync: u32) -> ClientResult<()> {
        self.fsync_inode(&[fid], fid, datasync).await
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
        let resp = self
            .rpc(Message::Tlock(Tlock {
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
                    LockType::Unlock => st.locks.retain(|l| {
                        !(l.fid == fid && ranges_overlap(l.start, l.length, start, length))
                    }),
                    _ if matches!(r.status, LockStatus::Success) => {
                        st.locks
                            .retain(|l| !(l.fid == fid && l.start == start && l.length == length));
                        st.locks.push(LockRecord {
                            fid,
                            lock_type,
                            start,
                            length,
                            proc_id,
                            client_id: client_id.to_vec(),
                        });
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
        // Reader and writer each hold an `Arc<Conn>`, so the cycle never breaks on
        // its own; `shutdown` wakes both to release the fd. Otherwise the fd and
        // both tasks leak.
        self.conn.load().shutdown();
        self.reconnect_notify.notify_waiters();
    }
}

/// Two byte ranges overlap (length 0 means "to EOF").
fn ranges_overlap(a_start: u64, a_len: u64, b_start: u64, b_len: u64) -> bool {
    let a_end = if a_len == 0 {
        u64::MAX
    } else {
        a_start.saturating_add(a_len)
    };
    let b_end = if b_len == 0 {
        u64::MAX
    } else {
        b_start.saturating_add(b_len)
    };
    a_start < b_end && b_start < a_end
}

/// Open a socket to the target, returning boxed read/write halves so the
/// supervisor can redial either transport uniformly.
async fn dial(
    target: &Target,
) -> ClientResult<(
    Box<dyn AsyncRead + Unpin + Send>,
    Box<dyn AsyncWrite + Unpin + Send>,
)> {
    match target {
        Target::Tcp(addr) => {
            let stream = tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(addr))
                .await
                .map_err(|_| ClientError::Disconnected)?
                .map_err(|_| ClientError::Disconnected)?;
            stream.set_nodelay(true).ok();
            let keepalive = socket2::TcpKeepalive::new()
                .with_time(Duration::from_secs(45))
                .with_interval(Duration::from_secs(15))
                .with_retries(4);
            let _ = socket2::SockRef::from(&stream).set_tcp_keepalive(&keepalive);
            let (r, w) = stream.into_split();
            Ok((Box::new(r), Box::new(w)))
        }
        Target::Unix(path) => {
            let stream = tokio::time::timeout(PROBE_TIMEOUT, UnixStream::connect(path))
                .await
                .map_err(|_| ClientError::Disconnected)?
                .map_err(|_| ClientError::Disconnected)?;
            let (r, w) = stream.into_split();
            Ok((Box::new(r), Box::new(w)))
        }
    }
}

/// Run the Tversion handshake on a freshly opened connection, returning the
/// negotiated msize and extension level.
async fn negotiate_on(conn: &Conn, requested: u32) -> ClientResult<(u32, u8)> {
    // Tversion must carry NOTAG per spec. Proposing the newest extension lets an
    // older/foreign server substring-match down to the highest it supports.
    let (otx, orx) = oneshot::channel();
    conn.pending.insert(NOTAG, otx);
    // Propose the newest extension down through `.zerofs3` so the server's substring
    // match lands on the highest it offers and degrades cleanly: a pre-`.zerofs6`
    // server picks `.zerofs5`, while a pre-`.zerofs4` server picks `.zerofs3`
    // (keeping the op-id) rather than dropping all the way to `.zerofs`.
    let body = Message::Tversion(Tversion {
        msize: requested,
        version: P9String::new(b"9P2000.L.zerofs6.zerofs5.zerofs4.zerofs3".to_vec()),
    });
    let bytes = match P9Message::new(NOTAG, body).to_bytes() {
        Ok(b) => b,
        Err(e) => {
            conn.pending.remove(&NOTAG);
            return Err(ClientError::Codec(e));
        }
    };
    if conn.writer_tx.send(bytes).await.is_err() {
        conn.pending.remove(&NOTAG);
        return Err(ClientError::Disconnected);
    }
    let frame = orx.await.map_err(|_| ClientError::Disconnected)?;
    let (_, msg) = P9Message::from_bytes((&frame, 0)).map_err(ClientError::Codec)?;
    match msg.body {
        Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
        Message::Rversion(rv) => {
            let vstr = rv.version.as_str().unwrap_or("");
            if !vstr.contains("9P2000.L") {
                warn!("server negotiated unsupported version: {:?}", vstr);
                return Err(ClientError::Unexpected("version"));
            }
            // The server echoes the highest suffix it supports; plain `9P2000.L` means none.
            let extensions = if vstr.contains(".zerofs6") {
                6
            } else if vstr.contains(".zerofs5") {
                5
            } else if vstr.contains(".zerofs4") {
                4
            } else if vstr.contains(".zerofs3") {
                3
            } else if vstr.contains(".zerofs2") {
                2
            } else if vstr.contains(".zerofs") {
                1
            } else {
                0
            };
            // v9fs requires msize >= 4096; reject a degenerate value.
            let negotiated = rv.msize.min(requested);
            if negotiated < 4096 {
                warn!("server negotiated msize {negotiated} below minimum 4096");
                return Err(ClientError::Unexpected("version"));
            }
            // `.zerofs4`: learn this instance's durability lineage token now, before
            // the connection serves writes, so each write can be tracked under it.
            if extensions >= 4 {
                query_lineage_token(conn).await?;
            }
            debug!("9P version negotiated, msize={negotiated}, extensions={extensions}");
            Ok((negotiated, extensions))
        }
        _ => Err(ClientError::Unexpected("version")),
    }
}

/// `.zerofs4`: ask the freshly-negotiated connection for its durability lineage
/// token and record it on the `Conn`. Runs during negotiation (before the
/// connection serves any request), so a plain non-NOTAG tag is collision-free.
async fn query_lineage_token(conn: &Conn) -> ClientResult<()> {
    let (otx, orx) = oneshot::channel();
    let tag = loop {
        let t = conn.tag_ctr.fetch_add(1, Ordering::Relaxed);
        if t != NOTAG {
            break t;
        }
    };
    conn.pending.insert(tag, otx);
    let bytes = match P9Message::new(tag, Message::Tgetlineage(Tgetlineage)).to_bytes() {
        Ok(b) => b,
        Err(e) => {
            conn.pending.remove(&tag);
            return Err(ClientError::Codec(e));
        }
    };
    if conn.writer_tx.send(bytes).await.is_err() {
        conn.pending.remove(&tag);
        return Err(ClientError::Disconnected);
    }
    let frame = orx.await.map_err(|_| ClientError::Disconnected)?;
    let (_, msg) = P9Message::from_bytes((&frame, 0)).map_err(ClientError::Codec)?;
    match msg.body {
        Message::Rgetlineage(r) => {
            conn.lineage_token.store(r.token, Ordering::Relaxed);
            Ok(())
        }
        Message::Rlerror(e) => Err(ClientError::Errno(e.ecode)),
        _ => Err(ClientError::Unexpected("getlineage")),
    }
}

fn spawn_writer(
    write: Box<dyn AsyncWrite + Unpin + Send>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    conn: Arc<Conn>,
    reconnect: Arc<Notify>,
) {
    tokio::spawn(async move {
        let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, write);
        loop {
            tokio::select! {
                biased;
                // The reader signals us here when the socket dies while we are
                // idle (an idle writer never notices the broken pipe itself).
                _ = conn.writer_shutdown.notified() => break,
                maybe = rx.recv() => {
                    let Some(frame) = maybe else { break };
                    if writer.write_all(&frame).await.is_err() {
                        break;
                    }
                    let mut failed = false;
                    while let Ok(more) = rx.try_recv() {
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

fn spawn_reader(read: Box<dyn AsyncRead + Unpin + Send>, conn: Arc<Conn>, reconnect: Arc<Notify>) {
    tokio::spawn(async move {
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
                // Discard signal for a connection torn down while healthy (see `Conn::shutdown`).
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
            // Any frame is proof of life; the timeout path reads this to tell a
            // slow leader from a hung one without paying for an explicit probe.
            conn.mark_alive();
            if frame.len() < P9_HEADER_SIZE {
                warn!(
                    "9P client: response frame too short ({} bytes)",
                    frame.len()
                );
                continue;
            }
            let tag = u16::from_le_bytes([frame[5], frame[6]]);
            if let Some((_, tx)) = conn.pending.remove(&tag) {
                let _ = tx.send(frame);
            } else {
                debug!("9P client: response for unknown tag {tag}");
            }
        }

        // Connection gone: fail in-flight requests, wake the writer, reconnect.
        conn.dead.store(true, Ordering::Release);
        conn.pending.clear();
        conn.writer_shutdown.notify_one();
        reconnect.notify_waiters();
    });
}

#[cfg(test)]
mod durability_tracking_tests {
    use super::Unsynced;

    #[test]
    fn fsync_clears_the_obligation_when_quiescent() {
        let mut u = Unsynced::default();
        u.note(7); // a write under lineage token 7
        let (oldest, generation) = u.snapshot();
        assert_eq!(oldest, Some(7));
        // No write lands during the fsync RPC, so the clear fires.
        u.clear_if_unchanged(generation);
        assert_eq!(u.snapshot().0, None);
    }

    #[test]
    fn a_repeated_fsync_on_one_fid_keeps_failing_until_a_redo() {
        // Per-fid reported flag: an ESTALE fsync on a fid must NOT clear its
        // obligation, so a second fsync on the SAME fid (no redo) still surfaces the
        // loss; only the app's redo to that fid supersedes it (no livelock).
        let mut u = Unsynced::default();
        u.note(1); // a write on this fid under lineage 1
        let (oldest, generation) = u.snapshot();
        assert_eq!(oldest, Some(1));
        // First fsync ESTALEs (lineage broke): report, do NOT clear.
        u.report_if_unchanged(generation);
        // A second fsync on this fid, no redo: still sees the loss.
        assert_eq!(
            u.snapshot().0,
            Some(1),
            "a reported loss must persist so a repeated fsync still fails"
        );
        // The app's redo under the new lineage supersedes the reported-lost write.
        u.note(2);
        assert_eq!(
            u.snapshot().0,
            Some(2),
            "a redo after a report advances the obligation (no livelock)"
        );
        let (_, gen2) = u.snapshot();
        u.clear_if_unchanged(gen2); // that redo's fsync succeeds
        assert_eq!(u.snapshot().0, None);
    }

    #[test]
    fn an_estale_and_redo_on_one_fid_does_not_discharge_a_sibling_fid() {
        // Obligations are per-fid: a full ESTALE -> redo -> success cycle on fid X
        // leaves fid Y's still-lost obligation intact, so a later fsync(Y) ESTALEs.
        use std::collections::HashMap;
        let mut map: HashMap<u32, Unsynced> = HashMap::new();
        map.entry(10).or_default().note(1); // fid 10 (file X) wrote under lineage 1
        map.entry(20).or_default().note(1); // fid 20 (file Y) wrote under lineage 1
        // fsync(fid 10) ESTALEs; report only fid 10.
        let (_, g) = map.get(&10).unwrap().snapshot();
        map.get_mut(&10).unwrap().report_if_unchanged(g);
        // App redoes file X under lineage 2 (supersede fid 10), then fsync(10) succeeds.
        map.get_mut(&10).unwrap().note(2);
        let (_, g2) = map.get(&10).unwrap().snapshot();
        map.get_mut(&10).unwrap().clear_if_unchanged(g2);
        if map.get(&10).unwrap().snapshot().0.is_none() {
            map.remove(&10);
        }
        // fid 20 (file Y) is untouched: its lost write is still tracked under L1.
        assert_eq!(
            map.get(&20).unwrap().snapshot().0,
            Some(1),
            "fid 10's ESTALE+redo+success cycle must not touch fid 20"
        );
    }

    #[test]
    fn fsync_does_not_erase_a_write_from_its_window() {
        // The generation guard: a write that lands between the fsync snapshot and its
        // clear must survive, so a later fsync still verifies it.
        let mut u = Unsynced::default();
        u.note(7);
        let (_, generation) = u.snapshot(); // fsync snapshots here...
        u.note(7); // ...a concurrent write lands during the in-flight RPC...
        u.clear_if_unchanged(generation); // ...so the clear must be skipped.
        assert_eq!(
            u.snapshot().0,
            Some(7),
            "a write racing the fsync must stay tracked"
        );
    }

    #[test]
    fn oldest_token_is_kept_across_a_lineage_change() {
        // After a reconnect to a new lineage, an older un-fsync'd write's token is
        // the one most likely to be stale, so it must drive the verdict.
        let mut u = Unsynced::default();
        u.note(5); // oldest, under the original lineage
        u.note(9); // later, under a new lineage after a reconnect
        assert_eq!(u.snapshot().0, Some(5), "the oldest (riskiest) token wins");
    }

    #[test]
    fn nothing_tracked_reports_none() {
        let u = Unsynced::default();
        assert_eq!(u.snapshot(), (None, 0));
    }
}

#[cfg(test)]
mod liveness_tests {
    use super::*;

    fn test_conn() -> Conn {
        let (writer_tx, _rx) = mpsc::channel(1);
        Conn {
            writer_tx,
            pending: DashMap::new(),
            tag_ctr: AtomicU16::new(0),
            lineage_token: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            base: std::time::Instant::now(),
            last_alive: AtomicU64::new(0),
            probe_lock: tokio::sync::Mutex::new(()),
            writer_shutdown: Notify::new(),
            reader_shutdown: Notify::new(),
        }
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
        // A `last` ahead of `now` cannot arise from one Instant base, but the
        // saturating subtraction must still treat it as within rather than wrap.
        assert!(Conn::within(500, 1000, w));
    }

    #[test]
    fn a_just_marked_conn_is_heard() {
        let conn = test_conn();
        // Never marked: last_alive is 0 but base is ~now, so a long window still holds.
        assert!(conn.heard_within(Duration::from_secs(60)));
        conn.mark_alive();
        assert!(conn.heard_within(Duration::from_secs(60)));
        // A zero window is never satisfied (now - last == 0, not < 0).
        assert!(!conn.heard_within(Duration::from_millis(0)));
    }
}
