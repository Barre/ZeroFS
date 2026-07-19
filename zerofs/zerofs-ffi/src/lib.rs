//! UniFFI bindings for `zerofs-client`.
//!
//! Paths use `String`; byte-exact `Dir` child names and payloads use `Vec<u8>`;
//! handles use `Arc`; errors use an exhaustive enum. Rust-only adapters and
//! feature traits are not exposed.

uniffi::setup_scaffolding!();

use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;

mod dir;
mod error;
mod file;
mod types;

pub use dir::Dir;
pub use error::ZeroFsError;
pub use file::File;
pub use types::{
    Capabilities, ConnectOptions, DirEntry, FileType, Metadata, NodeKind, OpenOptions, SetAttrs,
    SetTime, StatFs,
};

/// One concurrent ZeroFS session and identity. Open-unlinked handles produce
/// `ESTALE` after connection loss.
#[derive(uniffi::Object)]
pub struct Client {
    inner: Arc<zerofs_client::Client>,
}

#[uniffi::export]
impl Client {
    /// Negotiated session properties, fixed for this logical session.
    pub fn capabilities(&self) -> Capabilities {
        self.inner.capabilities().into()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    /// Connect with defaults. Targets: `unix:/sock`, `tcp://host:port`,
    /// `host:port`, `host` (port 5564), or a bare filesystem path. A
    /// comma-separated string forms an HA target set.
    #[uniffi::constructor]
    pub async fn connect(target: String) -> Result<Arc<Client>, ZeroFsError> {
        let inner = zerofs_client::Client::connect(&target).await?;
        Ok(Arc::new(Client { inner }))
    }

    /// Connect with explicit identity, attach target, timeout, and tuning.
    #[uniffi::constructor]
    pub async fn connect_with(
        target: String,
        opts: ConnectOptions,
    ) -> Result<Arc<Client>, ZeroFsError> {
        let inner = zerofs_client::Client::connect_with(&target, opts.into()).await?;
        Ok(Arc::new(Client { inner }))
    }

    /// Mark the client closed and release the session best-effort.
    pub async fn close(&self) {
        self.inner.close().await;
    }

    /// Read the entire file into memory.
    pub async fn read(&self, path: String) -> Result<Vec<u8>, ZeroFsError> {
        Ok(self.inner.read(path).await?.to_vec())
    }

    /// Read up to `len` bytes at `offset`; a shorter result means EOF.
    pub async fn read_range(
        &self,
        path: String,
        offset: u64,
        len: u32,
    ) -> Result<Vec<u8>, ZeroFsError> {
        Ok(self.inner.read_range(path, offset, len).await?.to_vec())
    }

    /// Create-or-truncate `path` and write all of `data`.
    pub async fn write(&self, path: String, data: Vec<u8>) -> Result<(), ZeroFsError> {
        self.inner.write(path, &data).await?;
        Ok(())
    }

    /// Append `data` at end-of-file; returns the offset where it landed.
    pub async fn append(&self, path: String, data: Vec<u8>) -> Result<u64, ZeroFsError> {
        Ok(self.inner.append(path, &data).await?)
    }

    /// Report the entry at `path` without following symlinks.
    pub async fn stat(&self, path: String) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.stat(path).await?.into())
    }

    /// Like `stat` but resolves symlinks (final and intermediate), 40-hop cap.
    pub async fn metadata(&self, path: String) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.metadata(path).await?.into())
    }

    /// Resolve every symlink in `path` and return the canonical path bytes.
    pub async fn canonicalize(&self, path: String) -> Result<Vec<u8>, ZeroFsError> {
        let p = self.inner.canonicalize(path).await?;
        Ok(p.as_os_str().as_bytes().to_vec())
    }

    /// True if the path exists (any file type, no symlink following).
    pub async fn exists(&self, path: String) -> Result<bool, ZeroFsError> {
        Ok(self.inner.exists(path).await?)
    }

    /// Apply any combination of metadata changes; returns post-change metadata.
    pub async fn set_attr(&self, path: String, attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.set_attr(path, attrs.into()).await?.into())
    }

    /// Change permission bits.
    pub async fn chmod(&self, path: String, mode: u32) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.chmod(path, mode).await?.into())
    }

    /// Change owner and/or group (`None` leaves a field untouched).
    pub async fn chown(
        &self,
        path: String,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.chown(path, uid, gid).await?.into())
    }

    /// Truncate or extend a file to `size` bytes.
    pub async fn truncate(&self, path: String, size: u64) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.truncate(path, size).await?.into())
    }

    /// Set access/modification times (`None` leaves a field untouched).
    pub async fn set_times(
        &self,
        path: String,
        atime: Option<SetTime>,
        mtime: Option<SetTime>,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self
            .inner
            .set_times(path, atime.map(Into::into), mtime.map(Into::into))
            .await?
            .into())
    }

    /// Filesystem-wide usage and limits.
    pub async fn statfs(&self) -> Result<StatFs, ZeroFsError> {
        Ok(self.inner.statfs().await?.into())
    }

    /// Flush to durable (S3-backed) storage (filesystem-global on ZeroFS).
    pub async fn sync(&self) -> Result<(), ZeroFsError> {
        self.inner.sync().await?;
        Ok(())
    }

    /// Create a directory; the parent must exist.
    pub async fn create_dir(&self, path: String, mode: u32) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.create_dir(path, mode).await?.into())
    }

    /// Create a directory and any missing ancestors.
    pub async fn create_dir_all(&self, path: String, mode: u32) -> Result<(), ZeroFsError> {
        self.inner.create_dir_all(path, mode).await?;
        Ok(())
    }

    /// Remove a file, symlink, or device node.
    pub async fn remove_file(&self, path: String) -> Result<(), ZeroFsError> {
        self.inner.remove_file(path).await?;
        Ok(())
    }

    /// Remove an empty directory.
    pub async fn remove_dir(&self, path: String) -> Result<(), ZeroFsError> {
        self.inner.remove_dir(path).await?;
        Ok(())
    }

    /// Remove a directory and all its contents, recursively (not atomic).
    pub async fn remove_dir_all(&self, path: String) -> Result<(), ZeroFsError> {
        self.inner.remove_dir_all(path).await?;
        Ok(())
    }

    /// Atomically rename/move within the filesystem; replaces an existing target.
    pub async fn rename(&self, from: String, to: String) -> Result<(), ZeroFsError> {
        self.inner.rename(from, to).await?;
        Ok(())
    }

    /// Create a hard link at `link` pointing to the inode of `original`.
    pub async fn hard_link(&self, original: String, link: String) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.hard_link(original, link).await?.into())
    }

    /// Create a symlink at `link_path` containing `target` (stored verbatim).
    pub async fn symlink(
        &self,
        target: String,
        link_path: String,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.symlink(target, link_path).await?.into())
    }

    /// Read a symlink target as raw bytes (lossless).
    pub async fn read_link(&self, path: String) -> Result<Vec<u8>, ZeroFsError> {
        let p = self.inner.read_link(path).await?;
        Ok(p.as_os_str().as_bytes().to_vec())
    }

    /// Create a fifo, socket, or device node.
    pub async fn mknod(
        &self,
        path: String,
        kind: NodeKind,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.mknod(path, kind.into(), mode).await?.into())
    }

    /// List a whole directory (`.`/`..` excluded).
    pub async fn read_dir(&self, path: String) -> Result<Vec<DirEntry>, ZeroFsError> {
        let entries = self.inner.read_dir(path).await?;
        Ok(entries.into_iter().map(Into::into).collect())
    }

    /// Open a directory for incremental listing and byte-exact child operations.
    pub async fn open_dir(&self, path: String) -> Result<Arc<Dir>, ZeroFsError> {
        Ok(Dir::new(self.inner.open_dir(path).await?))
    }

    /// Open (and optionally create) a file for positioned I/O.
    pub async fn open(&self, path: String, opts: OpenOptions) -> Result<Arc<File>, ZeroFsError> {
        Ok(File::new(self.inner.open(path, opts.into()).await?))
    }

    /// Shorthand: open read-write with create+truncate, mode 0o644.
    pub async fn create(&self, path: String) -> Result<Arc<File>, ZeroFsError> {
        Ok(File::new(self.inner.create(path).await?))
    }
}
