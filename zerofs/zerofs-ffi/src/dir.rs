//! The `Dir` handle object: incremental listing plus the byte-exact `*_at`
//! child operations (names cross as `Vec<u8>`, so non-UTF-8 entries work).

use crate::error::ZeroFsError;
use crate::file::File;
use crate::types::{DirEntry, Metadata, NodeKind, OpenOptions, SetAttrs};
use std::sync::Arc;

/// An open directory: a pull-based listing cursor plus `openat`-style child ops.
#[derive(uniffi::Object)]
pub struct Dir {
    pub(crate) inner: Arc<zerofs_client::Dir>,
}

impl Dir {
    pub(crate) fn new(inner: Arc<zerofs_client::Dir>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Dir {
    /// Next batch of entries in directory order; `None` max returns one server
    /// batch. An empty list means end of directory.
    pub async fn next_batch(&self, max_entries: Option<u32>) -> Result<Vec<DirEntry>, ZeroFsError> {
        let batch = self.inner.next_batch(max_entries).await?;
        Ok(batch.into_iter().map(Into::into).collect())
    }

    /// Restart iteration from the first entry.
    pub async fn rewind(&self) -> Result<(), ZeroFsError> {
        self.inner.rewind().await?;
        Ok(())
    }

    /// Metadata for the directory itself.
    pub async fn metadata(&self) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.metadata().await?.into())
    }

    /// Apply metadata changes to the directory itself.
    pub async fn set_attr(&self, attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.set_attr(attrs.into()).await?.into())
    }

    /// openat(2)-alike: open (and optionally create) a child file.
    pub async fn open_at(
        &self,
        name: Vec<u8>,
        opts: OpenOptions,
    ) -> Result<Arc<File>, ZeroFsError> {
        let f = self.inner.open_at(&name, opts.into()).await?;
        Ok(File::new(f))
    }

    /// Open a child directory (descend without UTF-8).
    pub async fn open_dir_at(&self, name: Vec<u8>) -> Result<Arc<Dir>, ZeroFsError> {
        let d = self.inner.open_dir_at(&name).await?;
        Ok(Dir::new(d))
    }

    /// fstatat(2)-alike; never follows symlinks.
    pub async fn metadata_at(&self, name: Vec<u8>) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.metadata_at(&name).await?.into())
    }

    /// Apply metadata changes to a child without opening it.
    pub async fn set_attr_at(
        &self,
        name: Vec<u8>,
        attrs: SetAttrs,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.set_attr_at(&name, attrs.into()).await?.into())
    }

    /// mkdirat(2) with explicit mode; returns the new directory's metadata.
    pub async fn create_dir_at(&self, name: Vec<u8>, mode: u32) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.create_dir_at(&name, mode).await?.into())
    }

    /// symlinkat(2): create child `name` containing raw byte `target` verbatim.
    pub async fn symlink_at(
        &self,
        name: Vec<u8>,
        target: Vec<u8>,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.symlink_at(&name, &target).await?.into())
    }

    /// linkat(2): hard-link `original_dir`/`original_name` as `self`/`new_name`.
    /// Both directories must belong to the same client.
    pub async fn link_at(
        &self,
        original_dir: Arc<Dir>,
        original_name: Vec<u8>,
        new_name: Vec<u8>,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self
            .inner
            .link_at(&original_dir.inner, &original_name, &new_name)
            .await?
            .into())
    }

    /// mknodat(2): create a fifo, socket, or device node child.
    pub async fn mknod_at(
        &self,
        name: Vec<u8>,
        kind: NodeKind,
        mode: u32,
    ) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.mknod_at(&name, kind.into(), mode).await?.into())
    }

    /// unlinkat(2).
    pub async fn remove_file_at(&self, name: Vec<u8>) -> Result<(), ZeroFsError> {
        self.inner.remove_file_at(&name).await?;
        Ok(())
    }

    /// unlinkat(2) with AT_REMOVEDIR.
    pub async fn remove_dir_at(&self, name: Vec<u8>) -> Result<(), ZeroFsError> {
        self.inner.remove_dir_at(&name).await?;
        Ok(())
    }

    /// renameat(2) across two open directories (`new_dir` may be `self`). Both
    /// directories must belong to the same client.
    pub async fn rename_at(
        &self,
        old_name: Vec<u8>,
        new_dir: Arc<Dir>,
        new_name: Vec<u8>,
    ) -> Result<(), ZeroFsError> {
        self.inner
            .rename_at(&old_name, &new_dir.inner, &new_name)
            .await?;
        Ok(())
    }

    /// readlinkat(2): raw target bytes.
    pub async fn read_link_at(&self, name: Vec<u8>) -> Result<Vec<u8>, ZeroFsError> {
        Ok(self.inner.read_link_at(&name).await?)
    }

    /// Mark the handle closed, then clunk best-effort. Idempotent; never hangs.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}
