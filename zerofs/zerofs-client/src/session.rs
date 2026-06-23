//! The shared session: one attached 9P connection, the fid-guard machinery
//! that makes every public future cancel-safe, and the walk/open helpers the
//! path layer is built from.

use crate::error::{ClientResultExt, ZeroFsError};
use crate::types::{Metadata, NodeKind, OpenOptions, SetAttrs, SetTime};
use ninep_client::{NinePClient, SetattrBuilder, SetattrTime};
use ninep_proto::{GETATTR_ALL, Stat};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// Most 9P implementations cap Twalk at 16 name components per message;
/// longer walks are chained transparently.
const MAX_WELEM: usize = 16;

pub(crate) struct Session {
    pub(crate) client: Arc<NinePClient>,
    pub(crate) root_fid: u32,
    /// Group assigned to everything created through this session.
    pub(crate) gid: u32,
    pub(crate) closed: AtomicBool,
    clunk_tx: mpsc::UnboundedSender<u32>,
}

impl Session {
    /// Wrap an attached connection and spawn the clunk janitor, which cleans
    /// up fids whose owners were dropped (cancelled futures, dropped handles)
    /// and runs until the session and every guard are gone.
    pub(crate) fn new(client: Arc<NinePClient>, root_fid: u32, gid: u32) -> Arc<Self> {
        let (clunk_tx, mut rx) = mpsc::unbounded_channel::<u32>();
        let janitor_client = Arc::clone(&client);
        tokio::spawn(async move {
            while let Some(fid) = rx.recv().await {
                // The fid is gone server-side whatever the reply says; only
                // return it to the allocator once the clunk has been answered,
                // so an in-flight duplicate clunk can never hit a reused fid.
                let _ = janitor_client.clunk(fid).await;
                janitor_client.free_fid(fid);
            }
        });
        Arc::new(Self {
            client,
            root_fid,
            gid,
            closed: AtomicBool::new(false),
            clunk_tx,
        })
    }

    pub(crate) fn check_open(&self) -> Result<(), ZeroFsError> {
        if self.closed.load(Ordering::Acquire) {
            Err(ZeroFsError::Closed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ext_v1(&self) -> bool {
        self.client.extensions_enabled()
    }

    /// Hand a fid to the janitor for a background clunk and recycle.
    pub(crate) fn enqueue_clunk(&self, fid: u32) {
        let _ = self.clunk_tx.send(fid);
    }

    /// Allocate a fid already wrapped in its guard, so a fid never exists
    /// unguarded across an await: a future cancelled mid-op drops the guard and
    /// the janitor clunks whatever the server kept.
    pub(crate) fn alloc_guard(self: &Arc<Self>) -> FidGuard {
        FidGuard::one_shot(self, self.client.alloc_fid())
    }
}

/// Owns one server-side fid and recycles its number. Dropping the guard (a
/// cancelled future, or a closed/dropped handle) clunks the fid and returns its
/// number to the allocator. The number recycles only at Drop, which runs only
/// once no op can reference the fid (every handle method borrows `&self`, so the
/// `Arc<File>`/`Arc<Dir>` outlives its operation futures), so a recycled number
/// never aliases a live op.
pub(crate) struct FidGuard {
    session: Arc<Session>,
    fid: u32,
    state: GuardState,
}

enum GuardState {
    /// Live: Drop clunks (via the janitor) and recycles the number.
    Armed,
    /// Never created server-side (a failed op): Drop only recycles, no clunk.
    Discarded,
}

impl FidGuard {
    /// Guard a freshly allocated fid.
    pub(crate) fn one_shot(session: &Arc<Session>, fid: u32) -> Self {
        Self {
            session: Arc::clone(session),
            fid,
            state: GuardState::Armed,
        }
    }

    pub(crate) fn fid(&self) -> u32 {
        self.fid
    }

    /// The guarded fid is not live server-side (a failed alloc-then-walk): return
    /// the number to the allocator with no clunk.
    pub(crate) fn discard(mut self) {
        self.state = GuardState::Discarded;
    }
}

impl Drop for FidGuard {
    fn drop(&mut self) {
        match self.state {
            // Sync Drop cannot await the clunk, so the janitor clunks then
            // recycles the number after the reply.
            GuardState::Armed => self.session.enqueue_clunk(self.fid),
            // Never created server-side: the number is free to reuse now, and
            // Drop runs only with no op in flight.
            GuardState::Discarded => self.session.client.free_fid(self.fid),
        }
    }
}

impl Session {
    /// Walk from `from` through `names` (chained in ≤16-component messages),
    /// returning a guard for the new fid plus the final stat when the v1
    /// fast path made it free. A partial walk (the server's way of reporting a
    /// missing intermediate) maps to `NotFound`.
    pub(crate) async fn walk_from(
        self: &Arc<Self>,
        from: u32,
        names: &[&[u8]],
        display: &str,
    ) -> Result<(FidGuard, Option<Stat>), ZeroFsError> {
        // An empty walk clones the source fid; Twalkgetattr is full-walk-only,
        // so the clone goes through the plain path.
        if names.is_empty() {
            let guard = self.alloc_guard();
            return match self.client.walk(from, guard.fid(), &[]).await {
                Ok(_) => Ok((guard, None)),
                Err(e) => {
                    // A failed walk never created the fid server-side.
                    guard.discard();
                    Err(ZeroFsError::from_client(&e, display))
                }
            };
        }

        let mut cur: Option<FidGuard> = None;
        let mut idx = 0;
        loop {
            let src = cur.as_ref().map_or(from, FidGuard::fid);
            let chunk_end = (idx + MAX_WELEM).min(names.len());
            let chunk = &names[idx..chunk_end];
            let is_last = chunk_end == names.len();
            let guard = self.alloc_guard();
            let newfid = guard.fid();

            if is_last && self.ext_v1() {
                // One round trip for walk + stat; full-walk-only, so a missing
                // component is a server error rather than a partial reply.
                match self.client.walk_getattr(src, newfid, chunk).await {
                    Ok((_, stat)) => return Ok((guard, Some(stat))),
                    Err(e) => {
                        guard.discard();
                        return Err(ZeroFsError::from_client(&e, display));
                    }
                }
            }

            match self.client.walk(src, newfid, chunk).await {
                Ok(qids) if qids.len() == chunk.len() => {
                    if is_last {
                        return Ok((guard, None));
                    }
                    // The previous intermediate guard drops here → janitor clunk.
                    cur = Some(guard);
                    idx = chunk_end;
                }
                Ok(_) => {
                    // Partial walk: newfid was not created, the path is missing.
                    guard.discard();
                    return Err(ZeroFsError::NotFound {
                        path: display.to_string(),
                    });
                }
                Err(e) => {
                    guard.discard();
                    return Err(ZeroFsError::from_client(&e, display));
                }
            }
        }
    }

    /// [`Self::walk_from`] rooted at the attach point.
    pub(crate) async fn walk(
        self: &Arc<Self>,
        names: &[&[u8]],
        display: &str,
    ) -> Result<(FidGuard, Option<Stat>), ZeroFsError> {
        self.walk_from(self.root_fid, names, display).await
    }

    pub(crate) async fn stat_fid(&self, fid: u32, display: &str) -> Result<Stat, ZeroFsError> {
        self.client.getattr(fid, GETATTR_ALL).await.ctx(display)
    }

    /// Walk + stat regardless of extension level: chained walks for anything
    /// past the per-message component limit, then the shared `walk_stat` op
    /// (one round trip on v1) for the tail.
    pub(crate) async fn walk_stat_from(
        self: &Arc<Self>,
        from: u32,
        names: &[&[u8]],
        display: &str,
    ) -> Result<(FidGuard, Stat), ZeroFsError> {
        let (head, tail) = names.split_at(names.len().saturating_sub(MAX_WELEM));
        let head_guard = if head.is_empty() {
            None
        } else {
            Some(self.walk_from(from, head, display).await?.0)
        };
        let src = head_guard.as_ref().map_or(from, FidGuard::fid);
        let guard = self.alloc_guard();
        match self.client.walk_stat(src, guard.fid(), tail).await {
            Ok((_fid, stat)) => Ok((guard, stat)),
            Err(e) => {
                guard.discard();
                Err(ZeroFsError::from_client(&e, display))
            }
        }
    }

    pub(crate) async fn lopen(
        &self,
        fid: u32,
        flags: u32,
        display: &str,
    ) -> Result<(), ZeroFsError> {
        self.client.lopen(fid, flags).await.map(|_| ()).ctx(display)
    }

    /// Apply a setattr and return the post-op stat (one round trip on v2,
    /// setattr + getattr otherwise).
    pub(crate) async fn setattr_fid(
        &self,
        fid: u32,
        attrs: &SetAttrs,
        display: &str,
    ) -> Result<Stat, ZeroFsError> {
        // The 9P setattr wire encodes seconds as unsigned, so pre-epoch instants
        // are not representable; reject them rather than wrapping to a huge value.
        for t in [attrs.atime, attrs.mtime].into_iter().flatten() {
            if let SetTime::At { time } = t {
                if time.secs < 0 {
                    return Err(ZeroFsError::InvalidArgument {
                        message: format!(
                            "{display}: timestamps before the UNIX epoch are not representable"
                        ),
                    });
                }
            }
        }
        let ts = SetattrBuilder::new(fid)
            .mode(attrs.mode.map(|m| m & 0o7777))
            .uid(attrs.uid)
            .gid(attrs.gid)
            .size(attrs.size)
            .atime(attrs.atime.map(ops_time))
            .mtime(attrs.mtime.map(ops_time))
            .build();
        self.client.setattr_stat(ts).await.ctx(display)
    }

    /// Write all of `data` at `offset` (the transport chunks to msize); a
    /// short write is an error, never a silent partial.
    pub(crate) async fn write_all(
        &self,
        fid: u32,
        offset: u64,
        data: &[u8],
        display: &str,
    ) -> Result<(), ZeroFsError> {
        let written = self.client.write(fid, offset, data).await.ctx(display)?;
        if written != data.len() as u64 {
            return Err(ZeroFsError::Io {
                errno: libc::EIO,
                path: display.to_string(),
                message: format!("short write: {written} of {} bytes", data.len()),
            });
        }
        Ok(())
    }

    /// Open (and optionally create) `name` under the directory fid `dfid`,
    /// returning an opened fid guard. This is the composition documented in
    /// the design: `create_new` maps to the server's natively exclusive
    /// create; `create` is an open→exclusive-create→retry loop; `truncate` on
    /// a pre-existing file is an explicit setattr after the open.
    pub(crate) async fn open_relative(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        opts: &OpenOptions,
        display: &str,
    ) -> Result<FidGuard, ZeroFsError> {
        self.open_relative_op_id(dfid, name, opts, display, [0u8; 16])
            .await
    }

    /// [`Self::open_relative`] with an op-id (all-zero to opt out), threaded to the create step.
    pub(crate) async fn open_relative_op_id(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        opts: &OpenOptions,
        display: &str,
        op_id: [u8; 16],
    ) -> Result<FidGuard, ZeroFsError> {
        let acc = match (opts.read, opts.write) {
            (true, true) => libc::O_RDWR,
            (false, true) => libc::O_WRONLY,
            (true, false) => libc::O_RDONLY,
            (false, false) => {
                return Err(ZeroFsError::InvalidArgument {
                    message: format!("{display}: open requires read and/or write access"),
                });
            }
        } as u32;

        // Bounded retries around the open/create race; each iteration observes
        // a state another client may have changed in between.
        for _ in 0..4 {
            if !opts.create_new {
                match self.walk_from(dfid, &[name], display).await {
                    Ok((guard, stat)) => {
                        if opts.write
                            && let Some(stat) = &stat
                            && crate::types::FileType::from_mode(stat.mode)
                                == crate::types::FileType::Dir
                        {
                            return Err(ZeroFsError::IsADirectory {
                                path: display.to_string(),
                            });
                        }
                        self.lopen(guard.fid(), acc, display).await?;
                        if opts.truncate && opts.write {
                            let truncate = SetAttrs {
                                size: Some(0),
                                ..Default::default()
                            };
                            self.setattr_fid(guard.fid(), &truncate, display).await?;
                        }
                        return Ok(guard);
                    }
                    Err(ZeroFsError::NotFound { .. }) if opts.create => {}
                    Err(e) => return Err(e),
                }
            }

            // Create path: natively exclusive on the server (Tlcreateattr on
            // v2, clone + lcreate otherwise; the shared op handles both).
            let mode = libc::S_IFREG as u32 | (opts.mode & 0o7777);
            let flags = acc | libc::O_CREAT as u32;
            let guard = self.alloc_guard();
            match self
                .client
                .create_open_op_id(dfid, guard.fid(), name, flags, mode, self.gid, op_id)
                .await
            {
                Ok((_fid, _stat, _iounit)) => return Ok(guard),
                Err(e) => {
                    guard.discard();
                    let mapped = ZeroFsError::from_client(&e, display);
                    if matches!(mapped, ZeroFsError::AlreadyExists { .. }) && !opts.create_new {
                        continue; // lost the race to another creator; retry the open
                    }
                    return Err(mapped);
                }
            }
        }

        Err(ZeroFsError::Io {
            errno: libc::EAGAIN,
            path: display.to_string(),
            message: "open/create raced with concurrent create+unlink; retries exhausted"
                .to_string(),
        })
    }
}

impl Session {
    /// Common tail of the create-family ops. The op ran on a guarded `newfid`:
    /// on the fallback path that fid is the surplus walked child (drop the guard
    /// to clunk + recycle it); on the v2 path it was never used (discard to
    /// recycle just the number); on error nothing is left server-side (discard).
    /// On cancellation the guard drops and the janitor reclaims whatever the
    /// server kept, so no fid leaks.
    fn finish_create(
        &self,
        guard: FidGuard,
        res: ninep_client::ClientResult<(Option<u32>, Stat)>,
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        match res.ctx(display) {
            Ok((walked, stat)) => {
                if walked.is_some() {
                    drop(guard);
                } else {
                    guard.discard();
                }
                Ok(Metadata::from_stat(&stat))
            }
            Err(e) => {
                guard.discard();
                Err(e)
            }
        }
    }

    /// mkdir under `dfid`, returning the new directory's metadata.
    pub(crate) async fn mkdir_at(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        mode: u32,
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        self.mkdir_at_op_id(dfid, name, mode, display, [0u8; 16])
            .await
    }

    /// [`Self::mkdir_at`] with an idempotency op-id (all-zero to opt out).
    pub(crate) async fn mkdir_at_op_id(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        mode: u32,
        display: &str,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let mode = libc::S_IFDIR as u32 | (mode & 0o7777);
        let guard = self.alloc_guard();
        let res = self
            .client
            .mkdir_stat_op_id(dfid, Some(guard.fid()), name, mode, self.gid, op_id)
            .await;
        self.finish_create(guard, res, display)
    }

    /// symlink under `dfid`, returning the new link's metadata.
    pub(crate) async fn symlink_at(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        self.symlink_at_op_id(dfid, name, target, display, [0u8; 16])
            .await
    }

    /// [`Self::symlink_at`] with an idempotency op-id (all-zero to opt out).
    pub(crate) async fn symlink_at_op_id(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        display: &str,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let guard = self.alloc_guard();
        let res = self
            .client
            .symlink_stat_op_id(dfid, Some(guard.fid()), name, target, self.gid, op_id)
            .await;
        self.finish_create(guard, res, display)
    }

    /// mknod under `dfid`, returning the new node's metadata.
    pub(crate) async fn mknod_at(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        kind: NodeKind,
        mode: u32,
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        self.mknod_at_op_id(dfid, name, kind, mode, display, [0u8; 16])
            .await
    }

    /// [`Self::mknod_at`] with an idempotency op-id (all-zero to opt out).
    pub(crate) async fn mknod_at_op_id(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        kind: NodeKind,
        mode: u32,
        display: &str,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let (type_bits, major, minor) = match kind {
            NodeKind::Fifo => (libc::S_IFIFO, 0, 0),
            NodeKind::Socket => (libc::S_IFSOCK, 0, 0),
            NodeKind::BlockDevice { major, minor } => (libc::S_IFBLK, major, minor),
            NodeKind::CharDevice { major, minor } => (libc::S_IFCHR, major, minor),
        };
        let mode = type_bits as u32 | (mode & 0o7777);
        let guard = self.alloc_guard();
        let res = self
            .client
            .mknod_stat_op_id(
                dfid,
                Some(guard.fid()),
                name,
                mode,
                major,
                minor,
                self.gid,
                op_id,
            )
            .await;
        self.finish_create(guard, res, display)
    }

    /// Hard-link the inode at `target_fid` as `name` under `dfid`, returning
    /// the linked inode's post-op metadata.
    pub(crate) async fn link_at(
        self: &Arc<Self>,
        dfid: u32,
        target_fid: u32,
        name: &[u8],
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        self.link_at_op_id(dfid, target_fid, name, display, [0u8; 16])
            .await
    }

    /// [`Self::link_at`] with an idempotency op-id (all-zero to opt out).
    pub(crate) async fn link_at_op_id(
        self: &Arc<Self>,
        dfid: u32,
        target_fid: u32,
        name: &[u8],
        display: &str,
        op_id: [u8; 16],
    ) -> Result<Metadata, ZeroFsError> {
        let guard = self.alloc_guard();
        let res = self
            .client
            .link_stat_op_id(dfid, Some(guard.fid()), target_fid, name, op_id)
            .await;
        self.finish_create(guard, res, display)
    }
}

/// Map the public `SetTime` onto the shared setattr builder's time spec.
fn ops_time(t: SetTime) -> SetattrTime {
    match t {
        SetTime::Now => SetattrTime::Now,
        SetTime::At { time } => SetattrTime::At {
            sec: time.secs as u64,
            nsec: time.nanos as u64,
        },
    }
}
