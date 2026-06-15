use crate::error::{ClientResultExt, ZeroFsError};
use crate::session::{FidGuard, Session};
use crate::types::{Metadata, SetAttrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// An open file. All I/O is positioned (pread/pwrite); there is no shared
/// cursor, so an `Arc<File>` is safe to use from many tasks at once.
pub struct File {
    session: Arc<Session>,
    guard: FidGuard,
    closed: AtomicBool,
    path: String,
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("File")
            .field("path", &self.path)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl File {
    pub(crate) fn new(session: Arc<Session>, guard: FidGuard, path: String) -> Arc<Self> {
        Arc::new(Self {
            session,
            guard,
            closed: AtomicBool::new(false),
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

    /// Largest payload a single `read_at` round trip can return; used by the
    /// cursor to keep one `poll_read` to one round trip.
    #[cfg(feature = "tokio-io")]
    pub(crate) fn max_read_chunk(&self) -> u32 {
        self.session.client.max_io()
    }

    /// Read up to `len` bytes at `offset`; a shorter result means EOF. Returns
    /// [`bytes::Bytes`]: a read served by one round trip comes back with no copy.
    pub async fn read_at(&self, offset: u64, len: u32) -> Result<bytes::Bytes, ZeroFsError> {
        let fid = self.check()?;
        self.session
            .client
            .read_bytes(fid, offset, len)
            .await
            .ctx(&self.path)
    }

    /// Write all of `data` at `offset` (any size, chunked internally); errors
    /// on a short write.
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), ZeroFsError> {
        let fid = self.check()?;
        self.session.write_all(fid, offset, data, &self.path).await
    }

    /// Current metadata of this open file (fstat).
    pub async fn metadata(&self) -> Result<Metadata, ZeroFsError> {
        let fid = self.check()?;
        let stat = self.session.stat_fid(fid, &self.path).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Truncate or extend to `size` bytes.
    pub async fn set_len(&self, size: u64) -> Result<(), ZeroFsError> {
        let fid = self.check()?;
        let attrs = SetAttrs {
            size: Some(size),
            ..Default::default()
        };
        self.session.setattr_fid(fid, &attrs, &self.path).await?;
        Ok(())
    }

    /// Apply metadata changes through this handle.
    pub async fn set_attr(&self, attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        let fid = self.check()?;
        let stat = self.session.setattr_fid(fid, &attrs, &self.path).await?;
        Ok(Metadata::from_stat(&stat))
    }

    /// Flush data and metadata to durable (S3-backed) storage.
    pub async fn sync_all(&self) -> Result<(), ZeroFsError> {
        let fid = self.check()?;
        self.session.client.fsync(fid, 0).await.ctx(&self.path)
    }

    /// Flush file data only.
    pub async fn sync_data(&self) -> Result<(), ZeroFsError> {
        let fid = self.check()?;
        self.session.client.fsync(fid, 1).await.ctx(&self.path)
    }

    /// Mark the handle closed (later calls return `Closed`). The fid is clunked
    /// and its number recycled when the handle is dropped (for scope-bound use,
    /// right after this call). Always succeeds; idempotent; never blocks.
    pub async fn close(&self) {
        // Mark closed so later calls (here or on any clone) return `Closed`. The
        // fid is clunked and its number recycled when the handle is dropped,
        // which for scope-bound / context-manager use is immediately after.
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
