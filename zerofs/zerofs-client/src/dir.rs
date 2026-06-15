use crate::error::{ClientResultExt, ZeroFsError};
use crate::file::File;
use crate::path::validate_name;
use crate::session::{FidGuard, Session};
use crate::types::{DirEntry, FileType, Metadata, NodeKind, OpenOptions, SetAttrs};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// An open directory: a pull-based listing cursor plus at-style child
/// operations taking ONE byte-exact name component (no `/` or NUL). The `*_at`
/// suite is the FFI-clean escape hatch for non-UTF-8 names discovered via
/// [`DirEntry::name_bytes`]; chain [`Dir::open_dir_at`] to reach arbitrary
/// depth.
///
/// Internally a `Dir` holds two fids, because the server rejects creates on an
/// already-opened fid: an unopened fid serves the `*_at` operations, and a
/// lazily opened sibling serves the listing cursor.
pub struct Dir {
    session: Arc<Session>,
    guard: FidGuard,
    closed: AtomicBool,
    list: tokio::sync::Mutex<ListState>,
    path: String,
}

struct ListState {
    guard: Option<FidGuard>,
    /// 9P cookie for the next readdir.
    cookie: u64,
    eof: bool,
    /// Entries fetched but not yet handed out (a `max_entries` smaller than a
    /// server batch leaves a remainder here).
    buf: VecDeque<DirEntry>,
}

impl std::fmt::Debug for Dir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dir")
            .field("path", &self.path)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Dir {
    pub(crate) fn new(session: Arc<Session>, guard: FidGuard, path: String) -> Arc<Self> {
        Arc::new(Self {
            session,
            guard,
            closed: AtomicBool::new(false),
            list: tokio::sync::Mutex::new(ListState {
                guard: None,
                cookie: 0,
                eof: false,
                buf: VecDeque::new(),
            }),
            path,
        })
    }

    fn check(&self) -> Result<u32, ZeroFsError> {
        if self.closed.load(Ordering::Acquire) {
            Err(ZeroFsError::Closed)
        } else {
            Ok(self.guard.fid())
        }
    }

    fn child_display(&self, name: &[u8]) -> String {
        let base = self.path.trim_end_matches('/');
        format!("{base}/{}", String::from_utf8_lossy(name))
    }

    /// Shared `*_at` preamble: closed check, name validation, display path.
    fn at(&self, name: &[u8]) -> Result<(u32, String), ZeroFsError> {
        let fid = self.check()?;
        let display = self.child_display(name);
        validate_name(name, &display)?;
        Ok((fid, display))
    }

    /// Next batch of entries in directory order; `None` max returns one server
    /// batch. An empty Vec means end of directory.
    pub async fn next_batch(&self, max_entries: Option<u32>) -> Result<Vec<DirEntry>, ZeroFsError> {
        let fid = self.check()?;
        let mut st = self.list.lock().await;

        if st.guard.is_none() {
            // Open the listing sibling on a fresh fid (shared op: Tlopenat on
            // v2, clone + lopen otherwise), leaving the ops fid unopened.
            let flags = (libc::O_RDONLY | libc::O_DIRECTORY) as u32;
            let g = self.session.alloc_guard();
            let guard = match self
                .session
                .client
                .open_clone(fid, g.fid(), flags, None)
                .await
            {
                Ok((_list_fid, _, _)) => g,
                Err(e) => {
                    g.discard();
                    return Err(ZeroFsError::from_client(&e, &self.path));
                }
            };
            st.guard = Some(guard);
        }
        let list_fid = st.guard.as_ref().expect("listing fid just ensured").fid();

        // Fetch until at least one entry survives the `.`/`..` filter or EOF.
        while st.buf.is_empty() && !st.eof {
            let count = self.session.client.max_io();
            if self.session.ext_v1() {
                let entries = self
                    .session
                    .client
                    .readdirplus(list_fid, st.cookie, count)
                    .await
                    .ctx(&self.path)?;
                match entries.last() {
                    Some(last) => st.cookie = last.offset,
                    None => st.eof = true,
                }
                st.buf.extend(
                    entries
                        .iter()
                        .filter(|e| e.name.data != b"." && e.name.data != b"..")
                        .map(DirEntry::from_plus),
                );
            } else {
                let entries = self
                    .session
                    .client
                    .readdir(list_fid, st.cookie, count)
                    .await
                    .ctx(&self.path)?;
                match entries.last() {
                    Some(last) => st.cookie = last.offset,
                    None => st.eof = true,
                }
                st.buf.extend(
                    entries
                        .iter()
                        .filter(|e| e.name.data != b"." && e.name.data != b"..")
                        .map(DirEntry::from_plain),
                );
            }
        }

        let want = max_entries.map_or(usize::MAX, |n| n as usize);
        let take = want.min(st.buf.len());
        Ok(st.buf.drain(..take).collect())
    }

    /// A [`futures_core::Stream`] of this directory's entries, yielding them one
    /// at a time (fetched a server batch at a time). Shares this `Dir`'s listing
    /// cursor, so consuming the stream advances the same position as
    /// [`Self::next_batch`]. Rust-only sugar (`stream` feature); never crosses FFI.
    #[cfg(feature = "stream")]
    pub fn entries(self: &Arc<Dir>) -> crate::stream::DirStream {
        crate::stream::DirStream::new(Arc::clone(self))
    }

    /// Restart iteration from the first entry.
    pub async fn rewind(&self) -> Result<(), ZeroFsError> {
        self.check()?;
        let mut st = self.list.lock().await;
        st.cookie = 0;
        st.eof = false;
        st.buf.clear();
        Ok(())
    }

    /// Metadata for the directory itself.
    pub async fn metadata(&self) -> Result<Metadata, ZeroFsError> {
        let fid = self.check()?;
        let stat = self.session.stat_fid(fid, &self.path).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Apply metadata changes to the directory itself (chmod/chown/utimens).
    pub async fn set_attr(&self, attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        let fid = self.check()?;
        let stat = self.session.setattr_fid(fid, &attrs, &self.path).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// openat(2)-alike: open (and optionally create) a child file.
    pub async fn open_at(&self, name: &[u8], opts: OpenOptions) -> Result<Arc<File>, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        let guard = self
            .session
            .open_relative(fid, name, &opts, &display)
            .await?;
        Ok(File::new(Arc::clone(&self.session), guard, display))
    }

    /// Open a child directory (descend without UTF-8).
    pub async fn open_dir_at(&self, name: &[u8]) -> Result<Arc<Dir>, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        let (guard, stat) = self.session.walk_from(fid, &[name], &display).await?;
        if let Some(stat) = &stat
            && FileType::from_mode(stat.mode) != FileType::Dir
        {
            return Err(ZeroFsError::NotADirectory { path: display });
        }
        Ok(Dir::new(Arc::clone(&self.session), guard, display))
    }

    /// fstatat(2)-alike; never follows symlinks.
    pub async fn metadata_at(&self, name: &[u8]) -> Result<Metadata, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        let (_guard, stat) = self.session.walk_stat_from(fid, &[name], &display).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Apply metadata changes to a child without opening it (works on
    /// symlinks, fifos, and non-UTF-8 names).
    pub async fn set_attr_at(&self, name: &[u8], attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        let (guard, _) = self.session.walk_from(fid, &[name], &display).await?;
        let stat = self
            .session
            .setattr_fid(guard.fid(), &attrs, &display)
            .await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// mkdirat(2) with explicit mode; returns the new directory's metadata.
    pub async fn create_dir_at(&self, name: &[u8], mode: u32) -> Result<Metadata, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        self.session.mkdir_at(fid, name, mode, &display).await
    }

    /// symlinkat(2): create child `name` containing raw byte `target` verbatim.
    pub async fn symlink_at(&self, name: &[u8], target: &[u8]) -> Result<Metadata, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        self.session.symlink_at(fid, name, target, &display).await
    }

    /// linkat(2): hard-link `original_dir`/`original_name` (any file type) as
    /// `self`/`new_name`; returns metadata with the updated nlink.
    pub async fn link_at(
        &self,
        original_dir: &Dir,
        original_name: &[u8],
        new_name: &[u8],
    ) -> Result<Metadata, ZeroFsError> {
        let (fid, display) = self.at(new_name)?;
        let (original_fid, original_display) = original_dir.at(original_name)?;

        let (target_guard, _) = self
            .session
            .walk_from(original_fid, &[original_name], &original_display)
            .await?;
        self.session
            .link_at(fid, target_guard.fid(), new_name, &display)
            .await
    }

    /// mknodat(2): create a fifo, socket, or device node child.
    pub async fn mknod_at(
        &self,
        name: &[u8],
        kind: NodeKind,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        self.session.mknod_at(fid, name, kind, mode, &display).await
    }

    /// unlinkat(2).
    pub async fn remove_file_at(&self, name: &[u8]) -> Result<(), ZeroFsError> {
        let (fid, display) = self.at(name)?;
        self.session
            .client
            .unlinkat(fid, name, 0)
            .await
            .ctx(&display)
    }

    /// unlinkat(2) with AT_REMOVEDIR.
    pub async fn remove_dir_at(&self, name: &[u8]) -> Result<(), ZeroFsError> {
        let (fid, display) = self.at(name)?;
        self.session
            .client
            .unlinkat(fid, name, libc::AT_REMOVEDIR as u32)
            .await
            .ctx(&display)
    }

    /// renameat(2) across two open directories (`new_dir` may be `self`).
    pub async fn rename_at(
        &self,
        old_name: &[u8],
        new_dir: &Dir,
        new_name: &[u8],
    ) -> Result<(), ZeroFsError> {
        let (fid, old_display) = self.at(old_name)?;
        let (new_fid, _) = new_dir.at(new_name)?;
        self.session
            .client
            .renameat(fid, old_name, new_fid, new_name)
            .await
            .ctx(&old_display)
    }

    /// readlinkat(2): raw target bytes.
    pub async fn read_link_at(&self, name: &[u8]) -> Result<Vec<u8>, ZeroFsError> {
        let (fid, display) = self.at(name)?;
        let (guard, _) = self.session.walk_from(fid, &[name], &display).await?;
        self.session
            .client
            .readlink(guard.fid())
            .await
            .ctx(&display)
    }

    /// Mark the handle closed (later calls return `Closed`). Both fids are
    /// clunked and their numbers recycled when the handle is dropped. Always
    /// succeeds; idempotent; never blocks.
    pub async fn close(&self) {
        // Mark closed so later calls return `Closed`. Both fids (ops + listing)
        // are clunked and their numbers recycled when the handle is dropped.
        self.closed.store(true, Ordering::Release);
    }
}
