//! Asynchronous 9P2000.L client.
//!
//! This is the network counterpart to the ZeroFS 9P server: it speaks the exact
//! same wire protocol but from the client side.
//!
//! # Reconnection
//!
//! 9P sessions are stateful: every fid (the attach root, the per-inode "path"
//! fids and open file handles) and every byte-range lock lives on the
//! connection. A dropped socket therefore invalidates all of it, which is why
//! the in-kernel v9fs client simply wedges the mount on disconnect.
//!
//! Instead, this client records, per fid, the stable **inode id** it points at
//! (plus open flags) and which locks it holds, in [`SessionState`]. When the
//! connection drops, a supervisor task reconnects (retrying indefinitely with
//! backoff) and *replays* that state onto the fresh session.
//!
//! While a reconnect is in progress every request blocks (the mount "hangs"
//! rather than failing) and is resent once the session is restored. The one
//! caveat is a request in flight at the instant of the drop: we can't know
//! whether the server applied it, so resending a non-idempotent op
//! (mkdir/create/rename/unlink) could apply it twice.

use arc_swap::ArcSwap;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use deku::prelude::*;
use futures::StreamExt;
use ninep_proto::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, Ordering};
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

const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(50);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);

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

type ClientResult<T> = Result<T, ClientError>;

#[derive(Clone)]
enum Target {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

/// A single live transport (one socket + its reader/writer tasks). Replaced
/// wholesale on reconnect. Requests load the current one through the `ArcSwap`.
struct Conn {
    writer_tx: mpsc::Sender<Vec<u8>>,
    pending: DashMap<u16, oneshot::Sender<Bytes>>,
    tag_ctr: AtomicU16,
    /// Set by whichever of the reader/writer tasks first sees the socket fail.
    dead: AtomicBool,
    /// Signals the (possibly idle) writer task to stop when the reader exits.
    writer_shutdown: Notify,
    /// Signals the reader task to stop even while the socket is still healthy.
    /// The reader otherwise only exits when the socket dies, so a freshly built
    /// connection that is discarded before being installed (negotiate or replay
    /// failed) would leak its reader task and socket fd. The reader and writer
    /// each hold an `Arc<Conn>`, so `Drop` can't break that cycle — teardown
    /// must be signalled explicitly via [`Conn::shutdown`].
    reader_shutdown: Notify,
}

impl Conn {
    /// Tear down a connection that is being discarded without ever being
    /// installed (e.g. `connect_once` negotiated but `replay` then failed):
    /// wake both the reader and the writer so they exit and drop their
    /// `Arc<Conn>`, releasing the socket fd. A normally-dying connection
    /// (socket failed) tears itself down; this is only for the discard path.
    fn shutdown(&self) {
        self.dead.store(true, Ordering::Release);
        self.reader_shutdown.notify_one();
        self.writer_shutdown.notify_one();
    }
}

/// How a fid is re-established on a fresh session. There is no path/lineage and
/// no inter-fid dependency: the attach root is re-attached, and every other fid
/// is rebound directly to its (stable, never-reused) inode id. Rename, unlink of
/// another hard link, etc. are irrelevant because the id never changes.
#[derive(Clone)]
enum FidKind {
    /// The attach root, re-established with `Tattach`.
    Attach {
        afid: u32,
        uname: String,
        aname: String,
        n_uname: u32,
        /// The inode the attach resolved to (the `Rattach` qid's path): inode 0
        /// for an empty aname, the subtree root otherwise. Clones of this fid
        /// are rebound here.
        root_inode: u64,
    },
    /// A fid bound to a specific inode, re-established with `Trebind` as the
    /// user (`n_uname`) that owns it.
    Inode { inode_id: u64, n_uname: u32 },
}

#[derive(Clone)]
struct FidRecord {
    kind: FidKind,
    /// `Some(flags)` if the fid is open; replayed with a `Tlopen`.
    opened: Option<u32>,
}

impl FidRecord {
    fn inode_id(&self) -> u64 {
        match self.kind {
            FidKind::Attach { root_inode, .. } => root_inode,
            FidKind::Inode { inode_id, .. } => inode_id,
        }
    }

    /// The uid this fid acts as, replayed via `Tattach`/`Trebind`.
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
    target: Target,
    requested_msize: u32,
    /// The current transport. Swapped atomically by the reconnect supervisor.
    conn: ArcSwap<Conn>,
    /// False while a reconnect+replay is in progress; requests block until true.
    live: AtomicBool,
    live_notify: Notify,
    /// Pinged by a connection's reader/writer when the socket dies.
    reconnect_notify: Arc<Notify>,
    /// Negotiated message size (the smaller of what we asked for and what the
    /// server can handle).
    msize: AtomicU32,
    /// Negotiated ZeroFS extension level, re-negotiated on reconnect:
    /// 0 = plain 9P2000.L, 1 = `.zerofs` (Twalkgetattr/Treaddirattr),
    /// 2 = `.zerofs2` (additionally the compound create/open messages and the
    /// stat-carrying create-family/setattr replies).
    extensions: AtomicU8,
    /// Monotonic fid allocator, with a free list for reuse.
    fid_ctr: AtomicU32,
    fid_free: Mutex<Vec<u32>>,
    /// Recorded fids (by inode id) and held locks, replayed on reconnect.
    state: Mutex<SessionState>,
}

impl NinePClient {
    /// Connect to a 9P server over TCP and negotiate the protocol version.
    pub async fn connect_tcp(addr: SocketAddr, requested_msize: u32) -> std::io::Result<Arc<Self>> {
        Self::connect(Target::Tcp(addr), requested_msize)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Connect to a 9P server over a Unix domain socket and negotiate the version.
    pub async fn connect_unix(
        path: impl AsRef<Path>,
        requested_msize: u32,
    ) -> std::io::Result<Arc<Self>> {
        Self::connect(Target::Unix(path.as_ref().to_path_buf()), requested_msize)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    async fn connect(target: Target, requested_msize: u32) -> ClientResult<Arc<Self>> {
        let reconnect_notify = Arc::new(Notify::new());
        let (conn, msize, extensions) =
            Self::connect_once(&target, requested_msize, Arc::clone(&reconnect_notify)).await?;

        let client = Arc::new(Self {
            target,
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
        });
        client.spawn_supervisor();
        Ok(client)
    }

    /// Open a fresh socket, spawn its reader/writer tasks and negotiate the
    /// version.
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
            dead: AtomicBool::new(false),
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

        match negotiate_on(&conn, requested_msize).await {
            Ok((msize, extensions)) => Ok((conn, msize, extensions)),
            // The socket is healthy but unusable; tear the tasks down so the fd
            // is not leaked.
            Err(e) => {
                conn.shutdown();
                Err(e)
            }
        }
    }

    /// The reconnect supervisor: waits for the live connection to die, then
    /// reconnects and replays the session, retrying indefinitely with backoff.
    fn spawn_supervisor(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let notify = Arc::clone(&self.reconnect_notify);
        tokio::spawn(async move {
            loop {
                // Enable the waiter before reading `dead`, so a set-dead-then-notify
                // can't be lost; re-reading the current conn ignores stale notifies.
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
        let (conn, msize, extensions) = Self::connect_once(
            &self.target,
            self.requested_msize,
            Arc::clone(&self.reconnect_notify),
        )
        .await?;

        self.msize.store(msize, Ordering::Relaxed);
        self.extensions.store(extensions, Ordering::Relaxed);
        // A replay failure discards this freshly built connection (the caller
        // retries with a new one); tear it down so its fd is not leaked.
        if let Err(e) = self.replay(&conn).await {
            conn.shutdown();
            return Err(e);
        }
        let old = self.conn.swap(conn);
        old.dead.store(true, Ordering::Release);
        old.writer_shutdown.notify_one();

        Ok(())
    }

    /// Rebuild the recorded session onto `conn`, then re-acquire locks. Attach
    /// fids replay first — re-attaching restores the session's aname subtree
    /// root on the server, which the subsequent `Trebind`s are validated
    /// against — and every other fid is then rebound to its inode id (no
    /// lineage, no parent ordering). A *transport* failure aborts (the caller
    /// reconnects afresh); a *server* error for a fid means the inode is gone
    /// (or has left the subtree), so that fid is dropped.
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

    /// Re-establish one fid on `conn`. Returns `Ok(true)` if restored, `Ok(false)`
    /// if the server says it's gone (skip it), `Err` on a transport failure.
    ///
    /// An *aname* attach (a confined, subtree-rooted session) that fails to
    /// re-resolve is treated as a transport-level failure, not a gone fid: the
    /// attach establishes the server-side confinement root that every subsequent
    /// `Trebind` is validated against, so dropping it would leave the inode fids
    /// to rebind onto an unconfined (whole-filesystem) session. Returning `Err`
    /// aborts the reconnect; the supervisor retries afresh (and surfaces the
    /// error if the aname is gone for good) rather than silently widening the
    /// session's reach. A whole-filesystem attach (empty aname) has no
    /// confinement to lose, so it follows the gone-fid path.
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
            // A confined aname attach that no longer resolves must abort the
            // whole reconnect (fail closed); a gone inode fid is just dropped.
            Err(e @ ClientError::Errno(_)) if confined_attach => Err(e),
            Err(ClientError::Errno(_)) => Ok(false), // inode gone -> drop this fid
            Err(e) => Err(e),
        }
    }

    /// The negotiated message size.
    pub fn msize(&self) -> u32 {
        self.msize.load(Ordering::Relaxed)
    }

    /// Whether the server negotiated the ZeroFS fast-path extensions
    /// (`walk_getattr`/`readdirplus`).
    pub fn extensions_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 1
    }

    /// Whether the server negotiated the second-generation extensions: the
    /// compound create/open messages (`lopenat`/`lcreateattr`) and the
    /// stat-carrying replies (`mkdir_attr`/`symlink_attr`/`mknod_attr`/
    /// `link_attr`/`setattr_attr`).
    pub fn extensions_v2_enabled(&self) -> bool {
        self.extensions.load(Ordering::Relaxed) >= 2
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
        self.fid_free.lock().unwrap().push(fid);
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

    /// Send a request, blocking through any in-progress reconnect and resending
    /// across one (see the module docs for the in-flight double-apply caveat).
    async fn send_request(&self, body: Message) -> ClientResult<Message> {
        loop {
            self.wait_until_live().await;
            let conn = self.conn.load_full();

            let (otx, orx) = oneshot::channel();
            let tag = Self::alloc_tag(&conn, otx);
            let bytes = match P9Message::new(tag, body.clone()).to_bytes() {
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
            // Parse here, not on the reader task, to keep the reader unblocked.
            match orx.await {
                Ok(frame) => {
                    let (_, msg) =
                        P9Message::from_bytes((&frame, 0)).map_err(ClientError::Codec)?;
                    return Ok(msg.body);
                }
                Err(_) => {
                    // Lost the reply to a drop: wait for reconnect and resend.
                    conn.pending.remove(&tag);
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }
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
        match self.send_request(body).await? {
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

    /// Bind `fid` to an existing inode by id, acting as `n_uname`. Obtains a fresh
    /// fid for an inode without re-walking a path — used for per-user fids and for
    /// reconnect replay.
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
                // No await between the reply and this insert, so a reconnect
                // snapshot can't catch a half-recorded fid. Only a full walk
                // creates `newfid` (a partial one leaves it unset, per spec).
                if names.is_empty() || r.wqids.len() == names.len() {
                    let mut st = self.state.lock().unwrap();
                    // The new fid is reachable from `fid`, so it acts as the same
                    // user; a clone (empty names) also shares its inode.
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

    /// Full walk that also returns the final inode's stat in one round trip
    /// (the server's Twalkgetattr fast path). Records `newfid` exactly like
    /// `walk`, so reconnect replay via `Trebind` is unchanged. Only valid when
    /// the server negotiated extensions ([`Self::extensions_enabled`]).
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
        // The fid is gone regardless of the reply, so stop tracking it.
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

    /// Like [`Self::setattr`] but the reply carries the post-op stat, sparing
    /// the follow-up getattr. Only valid when the server negotiated the v2
    /// extensions ([`Self::extensions_v2_enabled`]).
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

    /// Open `fid`'s inode on a fresh `newfid` in one round trip (the server's
    /// Tlopenat fast path), replacing the Twalk(clone) + Tlopen pair. `fid` is
    /// left untouched. Records `newfid` exactly as that pair would, so reconnect
    /// replay (rebind + reopen) is unchanged. Only valid when the server
    /// negotiated the v2 extensions ([`Self::extensions_v2_enabled`]).
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

    pub async fn lcreate(
        &self,
        fid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> ClientResult<(Qid, u32)> {
        let resp = self
            .rpc(Message::Tlcreate(Tlcreate {
                fid,
                name: P9String::new(name.to_vec()),
                flags,
                mode,
                gid,
            }))
            .await?;
        match resp {
            Message::Rlcreate(r) => {
                // `fid` now names the created file: record its inode and the open
                // flags (minus create-only) so replay rebinds and reopens it.
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

    /// Create and open `name` under `dfid` and return its full post-op stat, all
    /// in one round trip (the server's Tlcreateattr fast path), replacing the
    /// Twalk(clone) + Tlcreate + Tgetattr sequence. Unlike [`Self::lcreate`],
    /// `dfid` is left untouched: the created file is opened on `newfid`. Only
    /// valid when the server negotiated the v2 extensions
    /// ([`Self::extensions_v2_enabled`]).
    pub async fn lcreateattr(
        &self,
        dfid: u32,
        newfid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> ClientResult<(Stat, u32)> {
        let resp = self
            .rpc(Message::Tlcreateattr(Tlcreateattr {
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
                // Record `newfid` for replay like `lcreate`: rebind to the new
                // inode and reopen with the create-only bits stripped.
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
        let max = self.max_io().max(1);
        let mut out: Vec<u8> = Vec::with_capacity(size.min(max) as usize);
        let mut off = offset;
        while (out.len() as u32) < size {
            let want = (size - out.len() as u32).min(max);
            let data = self.read_once(fid, off, want).await?;
            let got = data.len() as u32;
            out.extend_from_slice(&data);
            off += got as u64;
            if got < want {
                break; // short read => EOF
            }
        }
        Ok(out)
    }

    async fn read_once(&self, fid: u32, offset: u64, count: u32) -> ClientResult<Vec<u8>> {
        let resp = self
            .rpc(Message::Tread(Tread { fid, offset, count }))
            .await?;
        match resp {
            Message::Rread(r) => Ok(r.data.0.to_vec()),
            _ => Err(ClientError::Unexpected("read")),
        }
    }

    /// Write all of `data` at `offset`, splitting into multiple Twrite requests
    /// when it exceeds the negotiated msize. Returns the total bytes written.
    pub async fn write(&self, fid: u32, offset: u64, data: &[u8]) -> ClientResult<u32> {
        let max = self.max_write_payload().max(1) as usize;
        let mut written = 0usize;
        while written < data.len() {
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
        Ok(written as u32)
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

    /// Like [`Self::readdir`] but each entry carries its full stat (the server's
    /// Treaddirattr fast path). Only valid when the server negotiated extensions
    /// ([`Self::extensions_enabled`]).
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

    /// Like [`Self::mkdir`] but the reply carries the new directory's full stat,
    /// sparing the follow-up walk + getattr. Only valid when the server
    /// negotiated the v2 extensions ([`Self::extensions_v2_enabled`]).
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
    /// Only valid when the server negotiated the v2 extensions
    /// ([`Self::extensions_v2_enabled`]).
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
    /// Only valid when the server negotiated the v2 extensions
    /// ([`Self::extensions_v2_enabled`]).
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

    /// Like [`Self::link`] but the reply carries the linked inode's post-op
    /// stat (with its updated nlink). Only valid when the server negotiated the
    /// v2 extensions ([`Self::extensions_v2_enabled`]).
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

    pub async fn fsync(&self, fid: u32, datasync: u32) -> ClientResult<()> {
        match self.rpc(Message::Tfsync(Tfsync { fid, datasync })).await? {
            Message::Rfsync(_) => Ok(()),
            _ => Err(ClientError::Unexpected("fsync")),
        }
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
        // Wake the supervisor so it observes the dropped client and exits.
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
            let stream = TcpStream::connect(addr)
                .await
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
            let stream = UnixStream::connect(path)
                .await
                .map_err(|_| ClientError::Disconnected)?;
            let (r, w) = stream.into_split();
            Ok((Box::new(r), Box::new(w)))
        }
    }
}

/// Run the Tversion handshake on a freshly opened connection, returning the
/// negotiated msize and extension level.
async fn negotiate_on(conn: &Conn, requested: u32) -> ClientResult<(u32, u8)> {
    // Tversion must carry NOTAG (0xFFFF) per the spec and v9fs. Propose the
    // newest ZeroFS extension version; an older ZeroFS server matches the
    // `.zerofs` substring and replies `9P2000.L.zerofs` (first-generation
    // extensions only), and a foreign server replies plain `9P2000.L`, which
    // disables the fast paths entirely.
    let (otx, orx) = oneshot::channel();
    conn.pending.insert(NOTAG, otx);
    let body = Message::Tversion(Tversion {
        msize: requested,
        version: P9String::new(VERSION_9P2000L_ZEROFS2.to_vec()),
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
            // The server echoes the highest extension suffix it supports
            // (`.zerofs2` ⊃ `.zerofs`); a plain `9P2000.L` reply means none.
            let extensions = if vstr.contains(".zerofs2") {
                2
            } else if vstr.contains(".zerofs") {
                1
            } else {
                0
            };
            // Take the smaller of the two msizes, and reject a degenerate value:
            // v9fs requires msize >= 4096, below which I/O would degrade to tiny
            // per-message transfers.
            let negotiated = rv.msize.min(requested);
            if negotiated < 4096 {
                warn!("server negotiated msize {negotiated} below minimum 4096");
                return Err(ClientError::Unexpected("version"));
            }
            debug!("9P version negotiated, msize={negotiated}, extensions={extensions}");
            Ok((negotiated, extensions))
        }
        _ => Err(ClientError::Unexpected("version")),
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
                // Discard signal for a connection torn down while its socket is
                // still healthy (see `Conn::shutdown`). Never fires for a live
                // connection, so steady-state behavior is unchanged.
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
