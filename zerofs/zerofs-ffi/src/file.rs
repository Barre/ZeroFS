//! The `File` handle object.

use crate::error::ZeroFsError;
use crate::types::{Metadata, SetAttrs};
use std::sync::Arc;

/// An open file. All I/O is positioned; safe to use from many tasks at once.
#[derive(uniffi::Object)]
pub struct File {
    pub(crate) inner: Arc<zerofs_client::File>,
}

impl File {
    pub(crate) fn new(inner: Arc<zerofs_client::File>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl File {
    /// Read up to `len` bytes at `offset`; a shorter result means EOF.
    pub async fn read_at(&self, offset: u64, len: u32) -> Result<Vec<u8>, ZeroFsError> {
        Ok(self.inner.read_at(offset, len).await?.to_vec())
    }

    /// Write all of `data` at `offset` (any size, chunked internally).
    pub async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<(), ZeroFsError> {
        self.inner.write_at(offset, &data).await?;
        Ok(())
    }

    /// Current metadata of this open file (fstat).
    pub async fn metadata(&self) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.metadata().await?.into())
    }

    /// Truncate or extend to `size` bytes.
    pub async fn set_len(&self, size: u64) -> Result<(), ZeroFsError> {
        self.inner.set_len(size).await?;
        Ok(())
    }

    /// Apply metadata changes through this handle.
    pub async fn set_attr(&self, attrs: SetAttrs) -> Result<Metadata, ZeroFsError> {
        Ok(self.inner.set_attr(attrs.into()).await?.into())
    }

    /// Flush data and metadata to durable (S3-backed) storage.
    pub async fn sync_all(&self) -> Result<(), ZeroFsError> {
        self.inner.sync_all().await?;
        Ok(())
    }

    /// Flush file data only.
    pub async fn sync_data(&self) -> Result<(), ZeroFsError> {
        self.inner.sync_data().await?;
        Ok(())
    }

    /// Mark the handle closed, then clunk best-effort. Idempotent; never hangs.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}
