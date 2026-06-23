//! A failover client: given all HA node addresses, connects to the current
//! leader and transparently re-routes across a failover.
//!
//! The leader is found by probing for the node whose 9P both accepts a
//! connection and answers a sanity op (a standby is not serving; a fenced or
//! lapsed leader fails the lease gate).
//!
//! Retry safety (a prior design had a silent-duplication bug here). A
//! retry-across-failover is safe two ways: naturally idempotent ops (`read`,
//! and `write` = create+truncate), and idempotency op-ids for the non-idempotent
//! mutating ops. Each op-id is generated ONCE per logical op (here, before the
//! retry loop) and reused on every attempt, so the server recognizes a resend as
//! already-applied instead of double-applying or spuriously EEXIST/ENOENT-ing.

use crate::file::File;
use crate::session::{FidGuard, Session};
use crate::types::OpenOptions;
use crate::{Client, Metadata, NodeKind, ZeroFsError};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// A fresh op-id, generated ONCE per logical mutating op (before its retry loop)
/// and reused on every retry. Regenerating per retry would break cross-failover
/// dedup: each retry would mint an id the new leader has never seen.
fn new_op_id() -> [u8; 16] {
    use rand::RngCore;
    let mut id = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut id);
    id
}

pub(crate) const MAX_ATTEMPTS: usize = 60;
pub(crate) const RETRY_DELAY: Duration = Duration::from_millis(500);
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_OP_TIMEOUT: Duration = Duration::from_secs(3);
/// Per-op cap before treating the leader as dead and re-routing: a killed
/// leader's cached connection otherwise hangs forever. Must exceed the leader's
/// ship timeout so a healthy (shipping) write is not falsely re-routed. Also
/// bounds each op on an open [`crate::File`] handle, which re-opens on failover.
pub(crate) const OP_TIMEOUT: Duration = Duration::from_secs(8);

/// A client that connects to the current HA leader and transparently re-routes
/// across a failover. See the module docs for retry-safety.
pub struct FailoverClient {
    targets: Vec<String>,
    /// Current leader connection, loaded lock-free per op, swapped on failover.
    /// `None` means disconnected (re-probe needed).
    current: ArcSwapOption<Client>,
}

/// Errors meaning "this node is no longer the leader": re-routing and retrying an
/// idempotent op is safe. FS-semantic errors (NotFound, etc.) must surface.
pub(crate) fn is_transport_failure(e: &ZeroFsError) -> bool {
    match e {
        // NotLeader is the explicit "this node was deposed/fenced" signal
        // (P9_ENOTLEADER); a dropped connection / failed negotiation are the others.
        ZeroFsError::NotLeader { .. }
        | ZeroFsError::ConnectFailed { .. }
        | ZeroFsError::Closed
        | ZeroFsError::Protocol { .. } => true,
        // The per-op db gate (a lapse racing an already-dispatched op) flattens to EIO.
        ZeroFsError::Io { errno, .. } => *errno == libc::EIO,
        _ => false,
    }
}

impl FailoverClient {
    /// Connect to the serving leader among `targets`, retrying while none serves
    /// yet (bring-up or a failover gap).
    pub async fn connect(targets: Vec<String>) -> Result<Arc<Self>, ZeroFsError> {
        if targets.is_empty() {
            return Err(ZeroFsError::ConnectFailed {
                message: "no targets given".into(),
            });
        }
        let fc = Arc::new(Self {
            targets,
            current: ArcSwapOption::empty(),
        });
        for _ in 0..MAX_ATTEMPTS {
            if fc.obtain().await.is_some() {
                return Ok(fc);
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
        Err(ZeroFsError::ConnectFailed {
            message: "no serving leader found among targets".into(),
        })
    }

    fn cached(&self) -> Option<Arc<Client>> {
        self.current.load_full()
    }

    fn invalidate(&self) {
        self.current.store(None);
    }

    /// The cached connection, else a freshly probed one (cached on success).
    async fn obtain(&self) -> Option<Arc<Client>> {
        if let Some(c) = self.cached() {
            return Some(c);
        }
        let client = self.probe().await?;
        self.current.store(Some(client.clone()));
        Some(client)
    }

    /// Probe the targets for the serving leader (connectable AND answers a sanity op).
    async fn probe(&self) -> Option<Arc<Client>> {
        for target in &self.targets {
            let connected =
                tokio::time::timeout(PROBE_CONNECT_TIMEOUT, Client::connect(target)).await;
            let Ok(Ok(client)) = connected else {
                continue;
            };
            let healthy = matches!(
                tokio::time::timeout(PROBE_OP_TIMEOUT, client.stat("/")).await,
                Ok(Ok(_))
            );
            if healthy {
                return Some(client);
            }
            client.close().await;
        }
        None
    }

    /// Run `op` against the current leader, re-routing across a failover. `op`
    /// must be SAFE to replay (naturally idempotent, or carrying a stable op-id
    /// the caller generated once outside this helper). A transport failure or a
    /// hang past [`OP_TIMEOUT`] drops the cached connection and retries on the
    /// re-probed leader; any other error surfaces immediately.
    async fn with_failover<T, F, Fut>(&self, op: F) -> Result<T, ZeroFsError>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T, ZeroFsError>>,
    {
        let mut last = None;
        for _ in 0..MAX_ATTEMPTS {
            let Some(client) = self.obtain().await else {
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            };
            match tokio::time::timeout(OP_TIMEOUT, op(client)).await {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) if is_transport_failure(&e) => {
                    self.invalidate();
                    last = Some(e);
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    // Hung op: leader likely dead. Drop it and re-route.
                    self.invalidate();
                    last = Some(ZeroFsError::ConnectFailed {
                        message: "op timed out; leader unreachable".into(),
                    });
                }
            }
        }
        Err(last.unwrap_or(ZeroFsError::ConnectFailed {
            message: "failover attempts exhausted".into(),
        }))
    }

    /// Re-open `path` on a freshly probed leader, returning a new (session, fid)
    /// binding for a failover-aware [`File`] to swap in after a leader change.
    /// Invalidates the stale cached leader first. A plain open is idempotent, so
    /// `with_failover`'s retry is safe.
    pub(crate) async fn reopen_handle(
        &self,
        path: &Path,
        opts: &OpenOptions,
    ) -> Result<(Arc<Session>, FidGuard), ZeroFsError> {
        self.invalidate();
        let path = path.to_path_buf();
        let opts = *opts;
        self.with_failover(move |c| {
            let path = path.clone();
            async move { c.open_guard(&path, &opts).await }
        })
        .await
    }

    /// Read a file, re-routing across a failover. Idempotent.
    pub async fn read(&self, path: impl AsRef<Path>) -> Result<Bytes, ZeroFsError> {
        let path = path.as_ref().to_path_buf();
        self.with_failover(move |c| {
            let path = path.clone();
            async move { c.read(path).await }
        })
        .await
    }

    /// Write a file (create + truncate = last-write-wins), re-routing across a
    /// failover. Idempotent.
    pub async fn write(&self, path: impl AsRef<Path>, data: &[u8]) -> Result<(), ZeroFsError> {
        let path = path.as_ref().to_path_buf();
        let data = data.to_vec();
        self.with_failover(move |c| {
            let path = path.clone();
            let data = data.clone();
            async move { c.write(path, &data).await }
        })
        .await
    }

    /// Create a directory, re-routing across a failover. Retry-safe via a stable op-id.
    pub async fn create_dir(
        &self,
        path: impl AsRef<Path>,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let path = path.clone();
            async move { c.create_dir_op_id(path, mode, op_id).await }
        })
        .await
    }

    /// Create-or-truncate a file (mode 0o644) and return a failover-aware handle:
    /// its I/O is bounded by a per-op timeout and, on a leader failover, the handle
    /// re-opens the path on the re-probed leader and retries. Retry-safe via a
    /// stable op-id.
    pub async fn create(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
    ) -> Result<Arc<File>, ZeroFsError> {
        let orig = path.as_ref().to_path_buf();
        let op_id = new_op_id();
        let create_opts = OpenOptions::read_write().create(true).truncate(true);
        let (session, guard) = {
            let orig = orig.clone();
            self.with_failover(move |c| {
                let orig = orig.clone();
                async move { c.open_guard_op_id(&orig, &create_opts, op_id).await }
            })
            .await?
        };
        // Re-open after a failover with the same access but NO create/truncate:
        // the file already exists, so re-truncating would discard durable data.
        let reopen_opts = OpenOptions::read_write();
        Ok(File::new_failover(
            Arc::clone(self),
            session,
            guard,
            orig,
            reopen_opts,
        ))
    }

    /// Remove a file, symlink, or device node, re-routing across a failover.
    /// Retry-safe via a stable op-id.
    pub async fn remove_file(&self, path: impl AsRef<Path>) -> Result<(), ZeroFsError> {
        let path = path.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let path = path.clone();
            async move { c.remove_file_op_id(path, op_id).await }
        })
        .await
    }

    /// Remove an empty directory, re-routing across a failover. Retry-safe via a stable op-id.
    pub async fn remove_dir(&self, path: impl AsRef<Path>) -> Result<(), ZeroFsError> {
        let path = path.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let path = path.clone();
            async move { c.remove_dir_op_id(path, op_id).await }
        })
        .await
    }

    /// Atomically rename/move, re-routing across a failover. Retry-safe via a stable op-id.
    pub async fn rename(
        &self,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> Result<(), ZeroFsError> {
        let from = from.as_ref().to_path_buf();
        let to = to.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let from = from.clone();
            let to = to.clone();
            async move { c.rename_op_id(from, to, op_id).await }
        })
        .await
    }

    /// Create a hard link, re-routing across a failover. Retry-safe via a stable op-id.
    pub async fn hard_link(
        &self,
        original: impl AsRef<Path>,
        link: impl AsRef<Path>,
    ) -> Result<Metadata, ZeroFsError> {
        let original = original.as_ref().to_path_buf();
        let link = link.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let original = original.clone();
            let link = link.clone();
            async move { c.hard_link_op_id(original, link, op_id).await }
        })
        .await
    }

    /// Create a symlink, re-routing across a failover. Retry-safe via a stable op-id.
    pub async fn symlink(
        &self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
    ) -> Result<Metadata, ZeroFsError> {
        let target = target.as_ref().to_path_buf();
        let link_path = link_path.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let target = target.clone();
            let link_path = link_path.clone();
            async move { c.symlink_op_id(target, link_path, op_id).await }
        })
        .await
    }

    /// Create a fifo, socket, or device node, re-routing across a failover.
    /// Retry-safe via a stable op-id.
    pub async fn mknod(
        &self,
        path: impl AsRef<Path>,
        kind: NodeKind,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref().to_path_buf();
        let op_id = new_op_id();
        self.with_failover(move |c| {
            let path = path.clone();
            async move { c.mknod_op_id(path, kind, mode, op_id).await }
        })
        .await
    }
}
