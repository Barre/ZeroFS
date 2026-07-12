use crate::dir::Dir;
use crate::error::{ClientResultExt, ZeroFsError};
use crate::file::File;
use crate::path::{components, display, display_path, path_bytes, path_from_bytes, split_parent};
use crate::session::{FidGuard, Session};
use crate::types::{
    Capabilities, ConnectOptions, DirEntry, FileType, Metadata, NodeKind, OpenOptions, SetAttrs,
    SetTime, StatFs,
};
use bytes::{Bytes, BytesMut};
use ninep_client::{NOFID, NinePClient};
use ninep_proto::Stat;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_9P_PORT: u16 = 5564;

/// Symlink resolution cap, mirroring Linux's SYMLOOP_MAX headroom.
const MAX_SYMLINK_HOPS: u32 = 40;

/// One ZeroFS session, one identity. Share via `Arc`; every method takes
/// `&self` and is safe to call concurrently. The underlying connection
/// reconnects transparently, blocking calls through outages; bound waits with
/// your async runtime's timeout facilities (every future is cancel-safe).
///
/// Paths are bytes, as on POSIX and the 9P wire: every path parameter is
/// `impl AsRef<Path>`, so `&str`, `PathBuf`, and `OsStr::from_bytes(..)` for
/// non-UTF-8 names all work.
pub struct Client {
    session: Arc<Session>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("closed", &self.session.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Connect with defaults. Native targets: `"unix:/sock"`,
    /// `"tcp://host:port"`, `"host:port"`, `"host"` (port 5564), or a bare
    /// filesystem path (unix socket). Browser WASM targets are `"ws://..."`
    /// and `"wss://..."`.
    pub async fn connect(target: &str) -> Result<Arc<Client>, ZeroFsError> {
        Self::connect_with(target, ConnectOptions::default()).await
    }

    /// Connect with explicit identity, timeout, and tuning.
    pub async fn connect_with(
        target: &str,
        opts: ConnectOptions,
    ) -> Result<Arc<Client>, ZeroFsError> {
        let fut = Self::establish(target, &opts);
        match opts.connect_timeout_ms {
            Some(ms) => {
                match crate::runtime::timeout(Duration::from_millis(ms as u64), fut).await {
                    Ok(result) => result,
                    Err(_) => Err(ZeroFsError::ConnectFailed {
                        message: format!("connecting to {target}: timed out after {ms} ms"),
                    }),
                }
            }
            None => fut.await,
        }
    }

    async fn establish(target: &str, opts: &ConnectOptions) -> Result<Arc<Client>, ZeroFsError> {
        let client = dial(target, opts.msize).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let uid = opts.uid.unwrap_or_else(|| unsafe { libc::geteuid() });
        #[cfg(target_arch = "wasm32")]
        let uid = opts.uid.unwrap_or(0);
        #[cfg(not(target_arch = "wasm32"))]
        let gid = opts.gid.unwrap_or_else(|| unsafe { libc::getegid() });
        #[cfg(target_arch = "wasm32")]
        let gid = opts.gid.unwrap_or(0);
        let uname = match &opts.uname {
            Some(u) => u.clone(),
            None => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    std::env::var("USER").unwrap_or_else(|_| uid.to_string())
                }
                #[cfg(target_arch = "wasm32")]
                {
                    uid.to_string()
                }
            }
        };
        let root_fid = client.alloc_fid();
        client
            .attach(root_fid, NOFID, &uname, &opts.aname, uid)
            .await
            .map_err(|e| ZeroFsError::ConnectFailed {
                message: format!("attach to {target} failed: {e}"),
            })?;
        let session = Session::new(client, root_fid, gid);
        Ok(Arc::new(Client { session }))
    }

    /// Snapshot of currently negotiated session properties (may change across
    /// transparent reconnects).
    pub fn capabilities(&self) -> Capabilities {
        let c = &self.session.client;
        Capabilities {
            extensions_v1: c.extensions_enabled(),
            extensions_v2: c.extensions_v2_enabled(),
            msize: c.msize(),
            max_read_chunk: c.max_io(),
            max_write_chunk: c.max_write_payload(),
        }
    }

    /// Whether the underlying session is currently live. During transparent
    /// reconnect this is false while operations wait for replay to finish.
    pub fn is_connected(&self) -> bool {
        self.session.client.is_connected()
    }

    /// Cumulative 9P wire traffic for this client, including reconnects.
    pub fn traffic_stats(&self) -> ninep_client::TrafficStats {
        self.session.client.traffic_stats()
    }

    /// Number of server fids this client currently holds: the root, open
    /// `File`/`Dir` handles, and any in-flight operations. A diagnostic hook for
    /// leak tests, not part of the stable surface.
    #[doc(hidden)]
    pub fn outstanding_fids(&self) -> usize {
        self.session.client.outstanding_fids()
    }

    /// Mark the client closed (later calls return `Closed`), then hand the root
    /// fid to the janitor for a background clunk + recycle. Always succeeds,
    /// idempotent, and never blocks (no await), so a cancelled close still
    /// reclaims the root fid. Outstanding `File`/`Dir` handles keep working until
    /// individually closed.
    pub async fn close(&self) {
        if self.session.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.session.enqueue_clunk(self.session.root_fid);
    }

    /// Read the entire file into memory. Returns [`Bytes`]: a whole file that
    /// fits in one round trip comes back with no copy.
    pub async fn read(&self, path: impl AsRef<Path>) -> Result<Bytes, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let (guard, stat) = self.open_read(path, &pd).await?;
        let max = self.session.client.max_io().max(1);
        // A short first chunk means the whole file fit; hand it back uncopied.
        let first = self
            .session
            .client
            .read_bytes(guard.fid(), 0, max)
            .await
            .ctx(&pd)?;
        if (first.len() as u32) < max {
            return Ok(first);
        }
        // Cap the up-front reservation: `stat.size` is server-reported and may
        // be wildly large (or hostile); the loop grows `out` as it fills.
        let cap = stat
            .as_ref()
            .map_or(0, |s| s.size as usize)
            .min((max as usize).saturating_mul(2));
        let mut out = BytesMut::with_capacity(cap);
        out.extend_from_slice(&first);
        loop {
            let data = self
                .session
                .client
                .read_bytes(guard.fid(), out.len() as u64, max)
                .await
                .ctx(&pd)?;
            let got = data.len();
            out.extend_from_slice(&data);
            if (got as u32) < max {
                return Ok(out.freeze());
            }
        }
    }

    /// Read up to `len` bytes at `offset`; a shorter result means EOF.
    pub async fn read_range(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        len: u32,
    ) -> Result<Bytes, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let (guard, _) = self.open_read(path, &pd).await?;
        self.session
            .client
            .read_bytes(guard.fid(), offset, len)
            .await
            .ctx(&pd)
    }

    /// Create-or-truncate `path` (mode 0o644) and write all of `data`
    /// (composed client-side: open/create + truncate + write).
    pub async fn write(&self, path: impl AsRef<Path>, data: &[u8]) -> Result<(), ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let opts = OpenOptions::write_only().create(true).truncate(true);
        let guard = self.open_relative_path(path, &pd, &opts).await?;
        self.session.write_all(guard.fid(), 0, data, &pd).await
    }

    /// Append `data` at end-of-file (open-or-create + fstat + positioned
    /// write, composed client-side); returns the offset where the data landed.
    /// Last-writer-wins under concurrent appenders.
    pub async fn append(&self, path: impl AsRef<Path>, data: &[u8]) -> Result<u64, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let opts = OpenOptions::write_only().create(true);
        let guard = self.open_relative_path(path, &pd, &opts).await?;
        let stat = self.session.stat_fid(guard.fid(), &pd).await?;
        self.session
            .write_all(guard.fid(), stat.size, data, &pd)
            .await?;
        Ok(stat.size)
    }

    /// Shared namespace-op preamble: closed check, then walk to the parent of
    /// `path`, returning the parent's guard and the final name component.
    async fn parent_of<'a>(
        &self,
        path: &'a Path,
        pd: &str,
    ) -> Result<(FidGuard, &'a [u8]), ZeroFsError> {
        self.session.check_open()?;
        let names = components(path)?;
        let (parents, name) = split_parent(&names, pd)?;
        let (guard, _) = self.session.walk(parents, pd).await?;
        Ok((guard, name))
    }

    /// Walk to `path` and open it read-only, returning the opened guard plus
    /// the stat when the walk made it free.
    async fn open_read(
        &self,
        path: &Path,
        pd: &str,
    ) -> Result<(FidGuard, Option<Stat>), ZeroFsError> {
        self.session.check_open()?;
        let names = components(path)?;
        let (guard, stat) = self.session.walk(&names, pd).await?;
        self.session
            .lopen(guard.fid(), crate::linux::O_RDONLY, pd)
            .await?;
        Ok((guard, stat))
    }

    /// Walk to the parent and open/create the final component with `opts`.
    async fn open_relative_path(
        &self,
        path: &Path,
        pd: &str,
        opts: &OpenOptions,
    ) -> Result<FidGuard, ZeroFsError> {
        self.open_relative_path_op_id(path, pd, opts, [0u8; 16])
            .await
    }

    /// [`Self::open_relative_path`] threading an op-id to the create step (only
    /// relevant when `opts` creates the file).
    async fn open_relative_path_op_id(
        &self,
        path: &Path,
        pd: &str,
        opts: &OpenOptions,
        op_id: [u8; 16],
    ) -> Result<FidGuard, ZeroFsError> {
        let (dir_guard, name) = self.parent_of(path, pd).await?;
        self.session
            .open_relative_op_id(dir_guard.fid(), name, opts, pd, op_id)
            .await
    }

    /// Report the entry at `path` without following symlinks; anywhere: a
    /// path THROUGH a symlinked directory fails `NotADirectory` (9P walks are
    /// literal; this applies to every path-taking method). [`Self::metadata`]
    /// and [`Self::canonicalize`] are the only resolvers.
    pub async fn stat(&self, path: impl AsRef<Path>) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let names = components(path)?;
        let (_guard, stat) = self
            .session
            .walk_stat_from(self.session.root_fid, &names, &pd)
            .await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Like [`Self::stat`] but resolves symlinks (final AND intermediate
    /// components) client-side (readlink + re-walk), capped at 40 hops.
    pub async fn metadata(&self, path: impl AsRef<Path>) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let (_, stat) = self.resolve(path, &pd).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Resolve every symlink in `path` (40-hop cap) and return the canonical
    /// path, for use with any other method. Lossless: paths are bytes, so a
    /// non-UTF-8 component survives the round trip.
    pub async fn canonicalize(&self, path: impl AsRef<Path>) -> Result<PathBuf, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let (stack, _) = self.resolve(path, &pd).await?;
        let mut buf = Vec::new();
        for comp in &stack {
            buf.push(b'/');
            buf.extend_from_slice(comp);
        }
        if buf.is_empty() {
            buf.push(b'/');
        }
        path_from_bytes(buf)
    }

    /// True if the path exists (any file type, no symlink following);
    /// `NotFound` becomes `false`.
    pub async fn exists(&self, path: impl AsRef<Path>) -> Result<bool, ZeroFsError> {
        match self.stat(path).await {
            Ok(_) => Ok(true),
            Err(ZeroFsError::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Apply any combination of metadata changes; returns post-change metadata.
    pub async fn set_attr(
        &self,
        path: impl AsRef<Path>,
        attrs: SetAttrs,
    ) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let names = components(path)?;
        let (guard, _) = self.session.walk(&names, &pd).await?;
        let stat = self.session.setattr_fid(guard.fid(), &attrs, &pd).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Change permission bits.
    pub async fn chmod(&self, path: impl AsRef<Path>, mode: u32) -> Result<Metadata, ZeroFsError> {
        self.set_attr(
            path,
            SetAttrs {
                mode: Some(mode),
                ..Default::default()
            },
        )
        .await
    }

    /// Change owner and/or group (`None` leaves a field untouched).
    pub async fn chown(
        &self,
        path: impl AsRef<Path>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<Metadata, ZeroFsError> {
        self.set_attr(
            path,
            SetAttrs {
                uid,
                gid,
                ..Default::default()
            },
        )
        .await
    }

    /// Truncate or extend a file to `size` bytes.
    pub async fn truncate(
        &self,
        path: impl AsRef<Path>,
        size: u64,
    ) -> Result<Metadata, ZeroFsError> {
        self.set_attr(
            path,
            SetAttrs {
                size: Some(size),
                ..Default::default()
            },
        )
        .await
    }

    /// Set access/modification times (utimens; `None` leaves a field untouched).
    pub async fn set_times(
        &self,
        path: impl AsRef<Path>,
        atime: Option<SetTime>,
        mtime: Option<SetTime>,
    ) -> Result<Metadata, ZeroFsError> {
        self.set_attr(
            path,
            SetAttrs {
                atime,
                mtime,
                ..Default::default()
            },
        )
        .await
    }

    /// Filesystem-wide usage and limits.
    pub async fn statfs(&self) -> Result<StatFs, ZeroFsError> {
        self.session.check_open()?;
        let r = self
            .session
            .client
            .statfs(self.session.root_fid)
            .await
            .ctx("/")?;
        Ok(StatFs::from_wire(&r))
    }

    /// Flush to durable (S3-backed) storage. On ZeroFS the server-side flush
    /// is filesystem-global, so this is the durability endpoint for
    /// write→rename sequences.
    pub async fn sync(&self) -> Result<(), ZeroFsError> {
        self.session.check_open()?;
        self.session
            .client
            .fsync_all(self.session.root_fid, 0)
            .await
            .ctx("/")
    }

    /// Create a directory; the parent must exist.
    pub async fn create_dir(
        &self,
        path: impl AsRef<Path>,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        self.create_dir_op_id(path, mode, [0u8; 16]).await
    }

    /// [`Self::create_dir`] with a caller-supplied idempotency op-id (all-zero to
    /// opt out), passed through to the create step. Generated by the failover
    /// layer, never here (so it stays stable across retries).
    pub async fn create_dir_op_id(
        &self,
        path: impl AsRef<Path>,
        mode: u32,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let (dir_guard, name) = self.parent_of(path, &pd).await?;
        self.session
            .mkdir_at_op_id(dir_guard.fid(), name, mode, &pd, op_id)
            .await
    }

    /// Create a directory and any missing ancestors.
    pub async fn create_dir_all(
        &self,
        path: impl AsRef<Path>,
        mode: u32,
    ) -> Result<(), ZeroFsError> {
        let path = path.as_ref();
        self.session.check_open()?;
        let names = components(path)?;
        for depth in 1..=names.len() {
            let prefix = &names[..depth];
            let pd = display_path(prefix);
            let (parents, name) = split_parent(prefix, &pd)?;
            let (dir_guard, _) = self.session.walk(parents, &pd).await?;
            match self
                .session
                .mkdir_at(dir_guard.fid(), name, mode, &pd)
                .await
            {
                Ok(_) | Err(ZeroFsError::AlreadyExists { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        // `AlreadyExists` was tolerated along the way; the call only succeeds if
        // the final path resolves to a directory. Resolve symlinks (metadata,
        // not stat) so an existing symlink-to-directory counts, as `std::fs` does.
        if !names.is_empty() {
            let meta = self.metadata(path).await?;
            if !meta.is_dir() {
                return Err(ZeroFsError::NotADirectory {
                    path: display(path),
                });
            }
        }
        Ok(())
    }

    /// Remove a file, symlink, or device node.
    pub async fn remove_file(&self, path: impl AsRef<Path>) -> Result<(), ZeroFsError> {
        self.remove_file_op_id(path, [0u8; 16]).await
    }

    /// [`Self::remove_file`] with a caller-supplied idempotency op-id (all-zero to
    /// opt out). See [`Self::create_dir_op_id`].
    pub async fn remove_file_op_id(
        &self,
        path: impl AsRef<Path>,
        op_id: [u8; 16],
    ) -> Result<(), ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let (dir_guard, name) = self.parent_of(path, &pd).await?;
        self.session
            .client
            .unlinkat_op_id(dir_guard.fid(), name, 0, op_id)
            .await
            .ctx(&pd)
    }

    /// Remove an empty directory.
    pub async fn remove_dir(&self, path: impl AsRef<Path>) -> Result<(), ZeroFsError> {
        self.remove_dir_op_id(path, [0u8; 16]).await
    }

    /// [`Self::remove_dir`] with a caller-supplied idempotency op-id (all-zero to
    /// opt out). See [`Self::create_dir_op_id`].
    pub async fn remove_dir_op_id(
        &self,
        path: impl AsRef<Path>,
        op_id: [u8; 16],
    ) -> Result<(), ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let (dir_guard, name) = self.parent_of(path, &pd).await?;
        self.session
            .client
            .unlinkat_op_id(dir_guard.fid(), name, crate::linux::AT_REMOVEDIR, op_id)
            .await
            .ctx(&pd)
    }

    /// Remove a directory and all its contents, recursively (client-side
    /// walk, not atomic).
    pub async fn remove_dir_all(&self, path: impl AsRef<Path>) -> Result<(), ZeroFsError> {
        let path = path.as_ref();
        self.session.check_open()?;
        if components(path)?.is_empty() {
            return Err(ZeroFsError::InvalidArgument {
                message: "refusing to remove the attach root".to_string(),
            });
        }
        let dir = self.open_dir(path).await?;
        let result = remove_dir_contents(&dir).await;
        dir.close().await;
        result?;
        self.remove_dir(path).await
    }

    /// Atomically rename/move within the filesystem; replaces an existing
    /// target.
    pub async fn rename(
        &self,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> Result<(), ZeroFsError> {
        self.rename_op_id(from, to, [0u8; 16]).await
    }

    /// [`Self::rename`] with a caller-supplied idempotency op-id (all-zero to opt
    /// out). See [`Self::create_dir_op_id`].
    pub async fn rename_op_id(
        &self,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
        op_id: [u8; 16],
    ) -> Result<(), ZeroFsError> {
        let (from, to) = (from.as_ref(), to.as_ref());
        let (fd, td) = (display(from), display(to));
        let (from_guard, from_name) = self.parent_of(from, &fd).await?;
        let (to_guard, to_name) = self.parent_of(to, &td).await?;
        self.session
            .client
            .renameat_op_id(from_guard.fid(), from_name, to_guard.fid(), to_name, op_id)
            .await
            .ctx(&fd)
    }

    /// Create a hard link at `link` pointing to the inode of `original`.
    pub async fn hard_link(
        &self,
        original: impl AsRef<Path>,
        link: impl AsRef<Path>,
    ) -> Result<Metadata, ZeroFsError> {
        self.hard_link_op_id(original, link, [0u8; 16]).await
    }

    /// [`Self::hard_link`] with a caller-supplied idempotency op-id (all-zero to
    /// opt out). See [`Self::create_dir_op_id`].
    pub async fn hard_link_op_id(
        &self,
        original: impl AsRef<Path>,
        link: impl AsRef<Path>,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let (original, link) = (original.as_ref(), link.as_ref());
        let (od, ld) = (display(original), display(link));
        let (dir_guard, link_name) = self.parent_of(link, &ld).await?;
        let orig_names = components(original)?;
        let (orig_guard, _) = self.session.walk(&orig_names, &od).await?;
        self.session
            .link_at_op_id(dir_guard.fid(), orig_guard.fid(), link_name, &ld, op_id)
            .await
    }

    /// Create a symlink at `link_path` containing `target` (stored verbatim,
    /// bytes included).
    pub async fn symlink(
        &self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
    ) -> Result<Metadata, ZeroFsError> {
        self.symlink_op_id(target, link_path, [0u8; 16]).await
    }

    /// [`Self::symlink`] with a caller-supplied idempotency op-id (all-zero to opt
    /// out). See [`Self::create_dir_op_id`].
    pub async fn symlink_op_id(
        &self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let link_path = link_path.as_ref();
        let ld = display(link_path);
        let (dir_guard, name) = self.parent_of(link_path, &ld).await?;
        self.session
            .symlink_at_op_id(
                dir_guard.fid(),
                name,
                path_bytes(target.as_ref())?,
                &ld,
                op_id,
            )
            .await
    }

    /// Read a symlink target. Lossless: the target is returned byte-for-byte.
    pub async fn read_link(&self, path: impl AsRef<Path>) -> Result<PathBuf, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let names = components(path)?;
        let (guard, _) = self.session.walk(&names, &pd).await?;
        let target = self.session.client.readlink(guard.fid()).await.ctx(&pd)?;
        path_from_bytes(target)
    }

    /// Create a fifo, socket, or device node; `mode` carries permission bits
    /// only; the type (and device numbers) come from `kind`.
    pub async fn mknod(
        &self,
        path: impl AsRef<Path>,
        kind: NodeKind,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        self.mknod_op_id(path, kind, mode, [0u8; 16]).await
    }

    /// [`Self::mknod`] with a caller-supplied idempotency op-id (all-zero to opt
    /// out). See [`Self::create_dir_op_id`].
    pub async fn mknod_op_id(
        &self,
        path: impl AsRef<Path>,
        kind: NodeKind,
        mode: u32,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        let (dir_guard, name) = self.parent_of(path, &pd).await?;
        self.session
            .mknod_at_op_id(dir_guard.fid(), name, kind, mode, &pd, op_id)
            .await
    }

    /// List a whole directory (`.`/`..` excluded); metadata inline when the
    /// server supports readdirplus.
    pub async fn read_dir(&self, path: impl AsRef<Path>) -> Result<Vec<DirEntry>, ZeroFsError> {
        let dir = self.open_dir(path).await?;
        let mut out = Vec::new();
        let result = loop {
            match dir.next_batch(None).await {
                Ok(batch) if batch.is_empty() => break Ok(out),
                Ok(batch) => out.extend(batch),
                Err(e) => break Err(e),
            }
        };
        dir.close().await;
        result
    }

    /// Open a directory for incremental listing and byte-exact child
    /// operations.
    pub async fn open_dir(&self, path: impl AsRef<Path>) -> Result<Arc<Dir>, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let names = components(path)?;
        let (guard, stat) = self.session.walk(&names, &pd).await?;
        if let Some(stat) = &stat
            && FileType::from_mode(stat.mode) != FileType::Dir
        {
            return Err(ZeroFsError::NotADirectory { path: pd });
        }
        Ok(Dir::new(
            Arc::clone(&self.session),
            guard,
            display_path(&names),
        ))
    }

    /// Open (and optionally create) a file for positioned I/O.
    pub async fn open(
        &self,
        path: impl AsRef<Path>,
        opts: OpenOptions,
    ) -> Result<Arc<File>, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let guard = self.open_relative_path(path, &pd, &opts).await?;
        Ok(File::new(Arc::clone(&self.session), guard, pd))
    }

    /// Shorthand: open read-write with create+truncate, mode 0o644.
    pub async fn create(&self, path: impl AsRef<Path>) -> Result<Arc<File>, ZeroFsError> {
        self.create_op_id(path, [0u8; 16]).await
    }

    /// [`Self::create`] with a caller-supplied idempotency op-id (all-zero to opt
    /// out), passed through to the create step. See [`Self::create_dir_op_id`].
    pub async fn create_op_id(
        &self,
        path: impl AsRef<Path>,
        op_id: [u8; 16],
    ) -> Result<Arc<File>, ZeroFsError> {
        let path = path.as_ref();
        let pd = display(path);
        self.session.check_open()?;
        let opts = OpenOptions::read_write().create(true).truncate(true);
        let guard = self
            .open_relative_path_op_id(path, &pd, &opts, op_id)
            .await?;
        Ok(File::new(Arc::clone(&self.session), guard, pd))
    }

    /// Open `path` with `opts` and return the raw (session, fid) binding instead
    /// of a [`File`] wrapper, so a failover-aware handle can re-bind to a freshly
    /// probed leader after a failover.
    pub(crate) async fn open_guard(
        &self,
        path: &Path,
        opts: &OpenOptions,
    ) -> Result<(Arc<Session>, FidGuard), ZeroFsError> {
        self.open_guard_op_id(path, opts, [0u8; 16]).await
    }

    /// [`Self::open_guard`] threading an op-id to the create step.
    pub(crate) async fn open_guard_op_id(
        &self,
        path: &Path,
        opts: &OpenOptions,
        op_id: [u8; 16],
    ) -> Result<(Arc<Session>, FidGuard), ZeroFsError> {
        let pd = display(path);
        self.session.check_open()?;
        let guard = self
            .open_relative_path_op_id(path, &pd, opts, op_id)
            .await?;
        Ok((Arc::clone(&self.session), guard))
    }

    /// Resolve symlinks in `path` (final and intermediate), returning the
    /// canonical components and the final stat. Relative targets resolve
    /// against the link's parent, absolute targets against the attach root
    /// (the client cannot see outside its attach).
    async fn resolve(&self, path: &Path, pd: &str) -> Result<(Vec<Vec<u8>>, Stat), ZeroFsError> {
        let session = &self.session;

        // Fast path: the literal walk succeeds and the final node is not a
        // symlink; done in one round trip. Any failure falls back to the
        // component-wise resolver to find the offending symlink.
        let literal: Vec<&[u8]> = components(path)?;
        if let Ok((_guard, stat)) = session.walk_stat_from(session.root_fid, &literal, pd).await
            && FileType::from_mode(stat.mode) != FileType::Symlink
        {
            return Ok((literal.iter().map(|c| c.to_vec()).collect(), stat));
        }

        let mut todo: VecDeque<Vec<u8>> = literal.iter().map(|c| c.to_vec()).collect();
        let mut stack: Vec<Vec<u8>> = Vec::new();
        let mut hops = 0u32;
        // A fid pinned at the directory `stack` denotes, advanced one
        // component at a time.
        let (mut cur, _) = session.walk(&[], pd).await?;
        let mut cur_stat = session.stat_fid(cur.fid(), pd).await?;

        while let Some(name) = todo.pop_front() {
            if name == b".." {
                // Only reachable via a symlink target; resolve against the
                // canonical stack ("/.." stays at the root, like POSIX).
                stack.pop();
                let refs: Vec<&[u8]> = stack.iter().map(|c| c.as_slice()).collect();
                let (guard, stat) = session.walk_stat_from(session.root_fid, &refs, pd).await?;
                cur = guard;
                cur_stat = stat;
                continue;
            }

            let (guard, stat) = session
                .walk_stat_from(cur.fid(), &[name.as_slice()], pd)
                .await?;
            if FileType::from_mode(stat.mode) == FileType::Symlink {
                hops += 1;
                if hops > MAX_SYMLINK_HOPS {
                    return Err(ZeroFsError::TooManySymlinks {
                        path: pd.to_string(),
                    });
                }
                let target = session.client.readlink(guard.fid()).await.ctx(pd)?;
                if target.first() == Some(&b'/') {
                    stack.clear();
                    let (root_clone, _) = session.walk(&[], pd).await?;
                    cur = root_clone;
                    cur_stat = session.stat_fid(cur.fid(), pd).await?;
                }
                // Prepend the target's components ahead of the remaining path.
                for comp in target
                    .split(|&b| b == b'/')
                    .filter(|c| !c.is_empty() && *c != b".")
                    .rev()
                {
                    todo.push_front(comp.to_vec());
                }
            } else {
                stack.push(name);
                cur = guard;
                cur_stat = stat;
            }
        }

        Ok((stack, cur_stat))
    }
}

/// Empty a directory recursively: lists in rounds (rewinding between them so
/// deletion never races the cursor) until nothing is left.
#[cfg(not(target_arch = "wasm32"))]
type RemoveDirFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<(), ZeroFsError>> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type RemoveDirFuture<'a> = std::pin::Pin<Box<dyn Future<Output = Result<(), ZeroFsError>> + 'a>>;

fn remove_dir_contents<'a>(dir: &'a Dir) -> RemoveDirFuture<'a> {
    Box::pin(async move {
        loop {
            dir.rewind().await?;
            let batch = dir.next_batch(None).await?;
            if batch.is_empty() {
                return Ok(());
            }
            for entry in batch {
                if entry.file_type == FileType::Dir {
                    let child = dir.open_dir_at(&entry.name_bytes).await?;
                    let result = remove_dir_contents(&child).await;
                    child.close().await;
                    result?;
                    dir.remove_dir_at(&entry.name_bytes).await?;
                } else {
                    dir.remove_file_at(&entry.name_bytes).await?;
                }
            }
        }
    })
}

/// Open a transport to `target`. Mirrors the FUSE mount's target grammar.
async fn dial(target: &str, msize: u32) -> Result<Arc<NinePClient>, ZeroFsError> {
    let connect_failed = |message: String| ZeroFsError::ConnectFailed { message };

    #[cfg(target_arch = "wasm32")]
    {
        if target.starts_with("ws://") || target.starts_with("wss://") {
            return NinePClient::connect_websocket(target, msize)
                .await
                .map_err(|e| connect_failed(format!("9P WebSocket {target}: {e}")));
        }
        Err(connect_failed(format!(
            "browser clients require a ws:// or wss:// target, got {target:?}"
        )))
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        if let Some(rest) = target.strip_prefix("unix:") {
            let path = rest.strip_prefix("//").unwrap_or(rest);
            return NinePClient::connect_unix(path, msize)
                .await
                .map_err(|e| connect_failed(format!("9P unix socket {path}: {e}")));
        }

        let hostport = target.strip_prefix("tcp://").unwrap_or(target);

        // A path-like target without a scheme is treated as a unix socket.
        if hostport.starts_with('/') || hostport.starts_with('.') {
            return NinePClient::connect_unix(hostport, msize)
                .await
                .map_err(|e| connect_failed(format!("9P unix socket {hostport}: {e}")));
        }

        let addr = resolve_addr(hostport).await?;
        NinePClient::connect_tcp(addr, msize)
            .await
            .map_err(|e| connect_failed(format!("9P server {addr}: {e}")))
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn resolve_addr(s: &str) -> Result<std::net::SocketAddr, ZeroFsError> {
    if let Ok(addr) = s.parse::<std::net::SocketAddr>() {
        return Ok(addr);
    }
    let with_port = if s.contains(':') {
        s.to_string()
    } else {
        format!("{s}:{DEFAULT_9P_PORT}")
    };
    tokio::net::lookup_host(&with_port)
        .await
        .map_err(|e| ZeroFsError::ConnectFailed {
            message: format!("resolving {with_port}: {e}"),
        })?
        .next()
        .ok_or_else(|| ZeroFsError::ConnectFailed {
            message: format!("no addresses resolved for {with_port}"),
        })
}
