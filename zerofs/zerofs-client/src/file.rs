use crate::error::{ClientResultExt, ZeroFsError};
use crate::failover::{
    FailoverClient, MAX_ATTEMPTS, OP_TIMEOUT, RETRY_DELAY, is_transport_failure,
};
use crate::session::{FidGuard, Session};
use crate::types::{Metadata, OpenOptions, SetAttrs};
use arc_swap::ArcSwap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The (session, fid) an open handle is currently bound to. A failover-aware
/// handle swaps this atomically when it re-opens on a new leader; the old `Bound`
/// drops once no in-flight op still holds it, clunking the old fid.
struct Bound {
    session: Arc<Session>,
    guard: FidGuard,
}

/// What a failover-aware handle needs to re-bind after a leader change.
struct Reconnect {
    fc: Arc<FailoverClient>,
    /// The path to RE-open on the new leader (walkable, unlike the display string).
    path: PathBuf,
    /// How to re-open: the original access, but never create/truncate. The file
    /// already exists, so re-truncating would discard durable data.
    opts: OpenOptions,
    /// Serializes re-opens so concurrent failed ops produce exactly one.
    relock: tokio::sync::Mutex<()>,
}

/// An open file. All I/O is positioned (pread/pwrite); there is no shared
/// cursor, so an `Arc<File>` is safe to use from many tasks at once.
///
/// A handle from [`crate::FailoverClient`] is failover-aware: each op is bounded
/// by a timeout and, on a transport failure or a hang against a dead/frozen
/// leader, the handle re-opens the path on the re-probed leader and retries. A
/// handle from a plain [`crate::Client`] is bound to its one connection.
pub struct File {
    bound: ArcSwap<Bound>,
    /// `None` for a plain single-connection handle; `Some` for a failover-aware one.
    reconnect: Option<Reconnect>,
    closed: AtomicBool,
    path: String,
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("File")
            .field("path", &self.path)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("failover", &self.reconnect.is_some())
            .finish_non_exhaustive()
    }
}

impl File {
    /// A plain handle bound to one connection (no failover).
    pub(crate) fn new(session: Arc<Session>, guard: FidGuard, path: String) -> Arc<Self> {
        Arc::new(Self {
            bound: ArcSwap::from_pointee(Bound { session, guard }),
            reconnect: None,
            closed: AtomicBool::new(false),
            path,
        })
    }

    /// A failover-aware handle: on a transport failure it re-opens `path` (with
    /// `reopen_opts`, which must not create/truncate) on the re-probed leader.
    pub(crate) fn new_failover(
        fc: Arc<FailoverClient>,
        session: Arc<Session>,
        guard: FidGuard,
        path: PathBuf,
        reopen_opts: OpenOptions,
    ) -> Arc<Self> {
        let display = crate::path::display(&path);
        Arc::new(Self {
            bound: ArcSwap::from_pointee(Bound { session, guard }),
            reconnect: Some(Reconnect {
                fc,
                path,
                opts: reopen_opts,
                relock: tokio::sync::Mutex::new(()),
            }),
            closed: AtomicBool::new(false),
            path: display,
        })
    }

    /// Run `op` against the current (session, fid). A plain handle does one shot.
    /// A failover-aware handle bounds each try by [`OP_TIMEOUT`] and, on a transport
    /// failure or a hang, re-opens on the re-probed leader and retries. FS-semantic
    /// errors (NotFound, etc.) surface immediately.
    async fn with_binding<T, F, Fut>(&self, op: F) -> Result<T, ZeroFsError>
    where
        F: Fn(Arc<Session>, u32) -> Fut,
        Fut: Future<Output = Result<T, ZeroFsError>>,
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(ZeroFsError::Closed);
        }
        let Some(rc) = &self.reconnect else {
            let b = self.bound.load_full();
            return op(Arc::clone(&b.session), b.guard.fid()).await;
        };
        let mut last: Option<ZeroFsError> = None;
        for _ in 0..MAX_ATTEMPTS {
            if self.closed.load(Ordering::Acquire) {
                return Err(ZeroFsError::Closed);
            }
            let b = self.bound.load_full();
            match tokio::time::timeout(OP_TIMEOUT, op(Arc::clone(&b.session), b.guard.fid())).await
            {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) if is_transport_failure(&e) => {
                    last = Some(e);
                    self.rebind(rc, &b).await?;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    last = Some(ZeroFsError::ConnectFailed {
                        message: format!("{}: file op timed out; leader unreachable", self.path),
                    });
                    self.rebind(rc, &b).await?;
                }
            }
        }
        Err(last.unwrap_or(ZeroFsError::ConnectFailed {
            message: format!("{}: file failover attempts exhausted", self.path),
        }))
    }

    /// Re-open the path on a freshly probed leader and swap the binding, unless a
    /// concurrent op already re-bound past this failure (single-flight via `relock`
    /// + pointer identity of the failed `Bound`).
    async fn rebind(&self, rc: &Reconnect, failed: &Arc<Bound>) -> Result<(), ZeroFsError> {
        let _g = rc.relock.lock().await;
        if !Arc::ptr_eq(&self.bound.load_full(), failed) {
            return Ok(());
        }
        let (session, guard) = rc.fc.reopen_handle(&rc.path, &rc.opts).await?;
        // Carry this handle's un-fsync'd `.zerofs4` durability obligation from the
        // failed connection's fid onto the freshly re-opened one, so a verified fsync
        // after the re-route still verifies the write (and ESTALEs if its lineage broke)
        // instead of treating the fresh handle as clean.
        if let Some(token) = failed.session.client.unsynced_oldest(failed.guard.fid()) {
            session.client.seed_unsynced(guard.fid(), token);
        }
        self.bound.store(Arc::new(Bound { session, guard }));
        Ok(())
    }

    /// Largest payload a single `read_at` round trip can return; used by the
    /// cursor to keep one `poll_read` to one round trip.
    #[cfg(feature = "tokio-io")]
    pub(crate) fn max_read_chunk(&self) -> u32 {
        self.bound.load().session.client.max_io()
    }

    /// Read up to `len` bytes at `offset`; a shorter result means EOF. Returns
    /// [`bytes::Bytes`]: a read served by one round trip comes back with no copy.
    pub async fn read_at(&self, offset: u64, len: u32) -> Result<bytes::Bytes, ZeroFsError> {
        let path = self.path.clone();
        self.with_binding(move |session, fid| {
            let path = path.clone();
            async move { session.client.read_bytes(fid, offset, len).await.ctx(&path) }
        })
        .await
    }

    /// Write all of `data` at `offset` (any size, chunked internally); errors
    /// on a short write.
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), ZeroFsError> {
        let data = data.to_vec();
        let path = self.path.clone();
        self.with_binding(move |session, fid| {
            let data = data.clone();
            let path = path.clone();
            async move { session.write_all(fid, offset, &data, &path).await }
        })
        .await
    }

    /// Current metadata of this open file (fstat).
    pub async fn metadata(&self) -> Result<Metadata, ZeroFsError> {
        let path = self.path.clone();
        let stat = self
            .with_binding(move |session, fid| {
                let path = path.clone();
                async move { session.stat_fid(fid, &path).await }
            })
            .await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Truncate or extend to `size` bytes.
    pub async fn set_len(&self, size: u64) -> Result<(), ZeroFsError> {
        self.set_attr(SetAttrs {
            size: Some(size),
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    /// Apply metadata changes through this handle.
    pub async fn set_attr(&self, attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        let path = self.path.clone();
        let stat = self
            .with_binding(move |session, fid| {
                let path = path.clone();
                async move { session.setattr_fid(fid, &attrs, &path).await }
            })
            .await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Flush data and metadata to durable (S3-backed) storage.
    ///
    /// Against a `.zerofs4` server this is a verified fsync: success means every write
    /// acked on this handle before it is durable and survives any failover. If the
    /// durability lineage broke (a failover the surviving node could not prove it
    /// inherited), it returns [`ZeroFsError::Stale`] rather than report success: treat
    /// the prior writes as lost, redo them, and call `sync_all` again. A pre-`.zerofs4`
    /// server gives a plain, unverified flush.
    pub async fn sync_all(&self) -> Result<(), ZeroFsError> {
        let path = self.path.clone();
        self.with_binding(move |session, fid| {
            let path = path.clone();
            async move { session.client.fsync(fid, 0).await.ctx(&path) }
        })
        .await
    }

    /// Flush file data only. Like [`Self::sync_all`] this is verified against a
    /// `.zerofs4` server: it returns [`ZeroFsError::Stale`] if the prior writes' lineage
    /// broke, rather than report success.
    pub async fn sync_data(&self) -> Result<(), ZeroFsError> {
        let path = self.path.clone();
        self.with_binding(move |session, fid| {
            let path = path.clone();
            async move { session.client.fsync(fid, 1).await.ctx(&path) }
        })
        .await
    }

    /// Mark the handle closed (later calls return `Closed`). The fid is clunked
    /// and its number recycled when the handle is dropped (for scope-bound use,
    /// right after this call). Always succeeds; idempotent; never blocks.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// An independent `AsyncRead + AsyncWrite + AsyncSeek` cursor over this
    /// file, starting at offset 0. Multiple cursors over one `File` are safe:
    /// each carries its own position and the underlying I/O is positioned.
    /// Rust-only sugar (`tokio-io` feature); never crosses FFI.
    #[cfg(feature = "tokio-io")]
    pub fn cursor(self: &Arc<File>) -> crate::io::FileCursor {
        crate::io::FileCursor::new(Arc::clone(self))
    }
}
