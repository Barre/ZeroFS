//! Attached 9P session, fid guards, and path-operation primitives.

use crate::error::{ClientResultExt, ZeroFsError};
use crate::types::{Metadata, NodeKind, OpenOptions, SetAttrs, SetTime};
use ninep_client::{NinePClient, SetattrBuilder, SetattrTime};
use ninep_proto::{GETATTR_ALL, Stat};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// Maximum `Twalk` components per request.
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
    /// Wraps an attached connection and starts background fid cleanup.
    pub(crate) fn new(client: Arc<NinePClient>, root_fid: u32, gid: u32) -> Arc<Self> {
        let (clunk_tx, mut rx) = mpsc::unbounded_channel::<u32>();
        let janitor_client = Arc::clone(&client);
        crate::runtime::spawn(async move {
            while let Some(fid) = rx.recv().await {
                // Recycle only after clunk settlement prevents aliasing.
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

    /// Hand a fid to the janitor for a background clunk and recycle.
    pub(crate) fn enqueue_clunk(&self, fid: u32) {
        let _ = self.clunk_tx.send(fid);
    }

    /// Allocates a fid with cancellation cleanup installed before any await.
    pub(crate) fn alloc_guard(self: &Arc<Self>) -> FidGuard {
        FidGuard::one_shot(self, self.client.alloc_fid())
    }
}

/// Owns one fid. Drop schedules server clunk and number recycling.
pub(crate) struct FidGuard {
    session: Arc<Session>,
    fid: u32,
    state: GuardState,
}

enum GuardState {
    /// Drop schedules clunk and recycle.
    Armed,
    /// Drop recycles without clunk.
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

    /// Marks the fid as absent server-side.
    pub(crate) fn discard(mut self) {
        self.state = GuardState::Discarded;
    }
}

impl Drop for FidGuard {
    fn drop(&mut self) {
        match self.state {
            GuardState::Armed => self.session.enqueue_clunk(self.fid),
            GuardState::Discarded => self.session.client.free_fid(self.fid),
        }
    }
}

impl Session {
    /// Walks from `from` in chunks of at most 16 components and returns stat.
    /// Partial walks map to `NotFound`.
    pub(crate) async fn walk_from(
        self: &Arc<Self>,
        from: u32,
        names: &[&[u8]],
        display: &str,
    ) -> Result<(FidGuard, Stat), ZeroFsError> {
        let mut cur: Option<FidGuard> = None;
        let mut idx = 0;
        loop {
            let src = cur.as_ref().map_or(from, FidGuard::fid);
            let chunk_end = (idx + MAX_WELEM).min(names.len());
            let chunk = &names[idx..chunk_end];
            let is_last = chunk_end == names.len();
            let guard = self.alloc_guard();
            let newfid = guard.fid();

            if is_last {
                // The compound final walk also returns stat.
                match self.client.walk_getattr(src, newfid, chunk).await {
                    Ok((_, stat)) => return Ok((guard, stat)),
                    Err(e) => {
                        guard.discard();
                        return Err(ZeroFsError::from_client(&e, display));
                    }
                }
            }

            match self.client.walk(src, newfid, chunk).await {
                Ok(qids) if qids.len() == chunk.len() => {
                    cur = Some(guard);
                    idx = chunk_end;
                }
                Ok(_) => {
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
    ) -> Result<(FidGuard, Stat), ZeroFsError> {
        self.walk_from(self.root_fid, names, display).await
    }

    pub(crate) async fn stat_fid(&self, fid: u32, display: &str) -> Result<Stat, ZeroFsError> {
        self.client.getattr(fid, GETATTR_ALL).await.ctx(display)
    }

    pub(crate) async fn lopen(
        &self,
        fid: u32,
        flags: u32,
        display: &str,
    ) -> Result<(), ZeroFsError> {
        self.client.lopen(fid, flags).await.map(|_| ()).ctx(display)
    }

    /// Apply a setattr and return the post-op stat in one round trip.
    pub(crate) async fn setattr_fid(
        &self,
        fid: u32,
        attrs: &SetAttrs,
        display: &str,
    ) -> Result<Stat, ZeroFsError> {
        // The wire uses unsigned seconds and cannot encode pre-epoch instants.
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
        self.client.setattr_attr(ts).await.ctx(display)
    }

    /// Writes all data at `offset`; short writes return an error.
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
                errno: crate::linux::EIO,
                path: display.to_string(),
                message: format!("short write: {written} of {} bytes", data.len()),
            });
        }
        Ok(())
    }

    /// Opens or creates `name` under `dfid`. `create_new` uses exclusive create;
    /// existing-file truncation is a separate setattr request.
    pub(crate) async fn open_relative(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        opts: &OpenOptions,
        display: &str,
    ) -> Result<FidGuard, ZeroFsError> {
        let acc = match (opts.read, opts.write) {
            (true, true) => crate::linux::O_RDWR,
            (false, true) => crate::linux::O_WRONLY,
            (true, false) => crate::linux::O_RDONLY,
            (false, false) => {
                return Err(ZeroFsError::InvalidArgument {
                    message: format!("{display}: open requires read and/or write access"),
                });
            }
        };

        // Each EEXIST retry starts a new logical operation with a new op-id.
        for _ in 0..4 {
            if !opts.create_new {
                match self.walk_from(dfid, &[name], display).await {
                    Ok((guard, stat)) => {
                        if opts.write
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

            let mode = crate::linux::S_IFREG | (opts.mode & 0o7777);
            let flags = acc | crate::linux::O_CREAT;
            let guard = self.alloc_guard();
            let create = self
                .client
                .lcreateattr(dfid, guard.fid(), name, flags, mode, self.gid)
                .await;
            match create {
                Ok((_stat, _iounit)) => return Ok(guard),
                Err(e) => {
                    guard.discard();
                    let mapped = ZeroFsError::from_client(&e, display);
                    if matches!(mapped, ZeroFsError::AlreadyExists { .. }) && !opts.create_new {
                        continue;
                    }
                    return Err(mapped);
                }
            }
        }

        Err(ZeroFsError::Io {
            errno: crate::linux::EAGAIN,
            path: display.to_string(),
            message: "open/create raced with concurrent create+unlink; retries exhausted"
                .to_string(),
        })
    }
}

impl Session {
    /// mkdir under `dfid`, returning the new directory's metadata.
    pub(crate) async fn mkdir_at(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        mode: u32,
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        let mode = crate::linux::S_IFDIR | (mode & 0o7777);
        let stat = self
            .client
            .mkdir_attr(dfid, name, mode, self.gid)
            .await
            .ctx(display)?;
        Ok(Metadata::from_stat(&stat))
    }

    /// symlink under `dfid`, returning the new link's metadata.
    pub(crate) async fn symlink_at(
        self: &Arc<Self>,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        display: &str,
    ) -> Result<Metadata, ZeroFsError> {
        let stat = self
            .client
            .symlink_attr(dfid, name, target, self.gid)
            .await
            .ctx(display)?;
        Ok(Metadata::from_stat(&stat))
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
        let (type_bits, major, minor) = match kind {
            NodeKind::Fifo => (crate::linux::S_IFIFO, 0, 0),
            NodeKind::Socket => (crate::linux::S_IFSOCK, 0, 0),
            NodeKind::BlockDevice { major, minor } => (crate::linux::S_IFBLK, major, minor),
            NodeKind::CharDevice { major, minor } => (crate::linux::S_IFCHR, major, minor),
        };
        let mode = type_bits | (mode & 0o7777);
        let stat = self
            .client
            .mknod_attr(dfid, name, mode, major, minor, self.gid)
            .await
            .ctx(display)?;
        Ok(Metadata::from_stat(&stat))
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
        let stat = self
            .client
            .link_attr(dfid, target_fid, name)
            .await
            .ctx(display)?;
        Ok(Metadata::from_stat(&stat))
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
