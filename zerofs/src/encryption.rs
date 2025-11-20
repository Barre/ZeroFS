use crate::fs::errors::FsError;
use crate::fs::key_codec::PREFIX_CHUNK;
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use futures::stream::Stream;
use hkdf::Hkdf;
use rand::{RngCore, thread_rng};
use sha2::Sha256;
use slatedb::{
    DbReader, WriteBatch,
    config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions},
};
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

const NONCE_SIZE: usize = 24;

pub struct EncryptionManager {
    cipher: XChaCha20Poly1305,
}

impl EncryptionManager {
    pub fn new(master_key: &[u8; 32]) -> Self {
        let hk = Hkdf::<Sha256>::new(None, master_key);

        let mut encryption_key = [0u8; 32];

        hk.expand(b"zerofs-v1-encryption", &mut encryption_key)
            .expect("valid length");

        Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(&encryption_key)),
        }
    }

    pub fn encrypt(&self, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        // Check if this is a chunk key to decide on compression
        let data = if !key.is_empty() && key[0] == PREFIX_CHUNK {
            lz4_flex::compress_prepend_size(plaintext)
        } else {
            plaintext.to_vec()
        };

        let ciphertext = self
            .cipher
            .encrypt(nonce, data.as_ref())
            .map_err(|e| anyhow::anyhow!("Encryption failed: {}", e))?;

        // Format: [nonce][ciphertext]
        let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    pub fn decrypt(&self, key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < NONCE_SIZE {
            return Err(anyhow::anyhow!("Invalid ciphertext: too short"));
        }

        // Extract nonce and ciphertext
        let (nonce_bytes, ciphertext) = data.split_at(NONCE_SIZE);
        let nonce = XNonce::from_slice(nonce_bytes);

        // Decrypt
        let decrypted = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow::anyhow!("Decryption failed: {}", e))?;

        // Decompress chunks
        if !key.is_empty() && key[0] == PREFIX_CHUNK {
            lz4_flex::decompress_size_prepended(&decrypted)
                .map_err(|e| anyhow::anyhow!("Decompression failed: {}", e))
        } else {
            Ok(decrypted)
        }
    }
}

// Encrypted WriteBatch wrapper
pub struct EncryptedWriteBatch {
    inner: WriteBatch,
    encryptor: Arc<EncryptionManager>,
    // Queue of cache operations to apply after successful write
    cache_ops: Vec<(Bytes, Option<Bytes>)>, // (key, Some(value) for put, None for delete)
    pub(crate) pending_operations: Vec<(Bytes, Bytes)>,
}

impl EncryptedWriteBatch {
    pub fn new(encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: WriteBatch::new(),
            encryptor,
            cache_ops: Vec::new(),
            pending_operations: Vec::new(),
        }
    }

    pub fn put_bytes(&mut self, key: &bytes::Bytes, value: Bytes) {
        // Queue cache operation if this is a chunk
        if !key.is_empty() && key[0] == PREFIX_CHUNK {
            self.cache_ops.push((key.clone(), Some(value.clone())));
        }

        self.pending_operations.push((key.clone(), value));
    }

    pub fn delete_bytes(&mut self, key: &bytes::Bytes) {
        if !key.is_empty() && key[0] == PREFIX_CHUNK {
            self.cache_ops.push((key.clone(), None));
        }

        self.inner.delete(key);
    }

    #[allow(clippy::type_complexity)]
    pub async fn into_inner(self) -> Result<(WriteBatch, Vec<(Bytes, Option<Bytes>)>)> {
        let mut inner = self.inner;

        if !self.pending_operations.is_empty() {
            let operations = self.pending_operations;
            let encryptor = self.encryptor.clone();

            let encrypted_operations: Result<Vec<_>, _> = operations
                .into_iter()
                .map(|(key, value)| {
                    let encrypted = encryptor.encrypt(&key, &value)?;
                    Ok::<(Bytes, Vec<u8>), anyhow::Error>((key, encrypted))
                })
                .collect();

            for (key, encrypted) in encrypted_operations? {
                inner.put(&key, &encrypted);
            }
        }

        Ok((inner, self.cache_ops))
    }
}

// Wrapper for SlateDB handle that can be either read-write or read-only
pub enum SlateDbHandle {
    ReadWrite(Arc<slatedb::Db>),
    ReadOnly(ArcSwap<DbReader>),
}

impl SlateDbHandle {
    pub fn is_read_only(&self) -> bool {
        matches!(self, SlateDbHandle::ReadOnly(_))
    }
}

// Encrypted DB wrapper
pub struct EncryptedDb {
    inner: Arc<RwLock<SlateDbHandle>>,  // Use Arc<RwLock> for hot-swapping
    encryptor: Arc<EncryptionManager>,
    wal_streamer: Arc<RwLock<Option<Arc<crate::ha::WALStreamer>>>>,
}

impl EncryptedDb {
    pub fn new(db: Arc<slatedb::Db>, encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SlateDbHandle::ReadWrite(db))),
            encryptor,
            wal_streamer: Arc::new(RwLock::new(None)),
        }
    }

    pub fn new_read_only(db_reader: ArcSwap<DbReader>, encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SlateDbHandle::ReadOnly(db_reader))),
            encryptor,
            wal_streamer: Arc::new(RwLock::new(None)),
        }
    }

    /// Hot-swap from read-only to read-write handle (promotion)
    pub async fn swap_to_readwrite(&self, db: Arc<slatedb::Db>) -> Result<()> {
        let mut inner = self.inner.write().await;
        
        // Close old read-only handle if present
        if let SlateDbHandle::ReadOnly(reader_swap) = &*inner {
            let reader = reader_swap.load();
            if let Err(e) = reader.close().await {
                tracing::warn!("Error closing read-only handle during promotion: {}", e);
            }
        }
        
        *inner = SlateDbHandle::ReadWrite(db);
        tracing::info!("✓ Swapped to read-write handle");
        Ok(())
    }

    /// Hot-swap from read-write to read-only handle (demotion)
    pub async fn swap_to_readonly(&self, reader: ArcSwap<DbReader>) -> Result<()> {
        let mut inner = self.inner.write().await;
        
        // Close old read-write handle if present
        if let SlateDbHandle::ReadWrite(db) = &*inner {
            // Flush before closing
            if let Err(e) = db.flush().await {
                tracing::warn!("Error flushing during demotion: {}", e);
            }
            if let Err(e) = db.close().await {
                tracing::warn!("Error closing read-write handle during demotion: {}", e);
            }
        }
        
        *inner = SlateDbHandle::ReadOnly(reader);
        tracing::info!("✓ Swapped to read-only handle");
        Ok(())
    }
    
    /// Attach WAL streamer for replication (active node only)
    /// NOTE: This method is deprecated - use attach_wal_streamer() instead
    pub fn with_wal_replication(self, streamer: Arc<crate::ha::WALStreamer>) -> Self {
        // This is only used during initialization, so we can use blocking_write
        // but it should be called from a non-async context
        *self.wal_streamer.blocking_write() = Some(streamer);
        self
    }

    /// Attach WAL streamer after construction (for HA mode switching)
    pub async fn attach_wal_streamer(&self, streamer: Arc<crate::ha::WALStreamer>) {
        *self.wal_streamer.write().await = Some(streamer);
        tracing::info!("✓ WAL streamer attached to write path");
    }

    /// Detach WAL streamer (for demotion from active to standby)
    pub async fn detach_wal_streamer(&self) {
        *self.wal_streamer.write().await = None;
        tracing::info!("✓ WAL streamer detached from write path");
    }

    pub async fn is_read_only(&self) -> bool {
        self.inner.read().await.is_read_only()
    }

    pub async fn swap_reader(&self, new_reader: Arc<DbReader>) -> Result<()> {
        let inner = self.inner.read().await;
        match &*inner {
            SlateDbHandle::ReadOnly(reader_swap) => {
                reader_swap.store(new_reader);
                Ok(())
            }
            SlateDbHandle::ReadWrite(_) => Err(anyhow::anyhow!(
                "Cannot swap reader on a read-write database"
            )),
        }
    }

    pub async fn get_bytes(&self, key: &bytes::Bytes) -> Result<Option<bytes::Bytes>> {
        let read_options = ReadOptions {
            durability_filter: DurabilityLevel::Memory,
            ..Default::default()
        };

        let encrypted = {
            let inner = self.inner.read().await;
            match &*inner {
                SlateDbHandle::ReadWrite(db) => db.get_with_options(key, &read_options).await?,
                SlateDbHandle::ReadOnly(reader_swap) => {
                    let reader = reader_swap.load();
                    reader.get_with_options(key, &read_options).await?
                }
            }
        };

        match encrypted {
            Some(encrypted) => {
                let decrypted = self.encryptor.decrypt(key, &encrypted)?;
                Ok(Some(bytes::Bytes::from(decrypted)))
            }
            None => Ok(None),
        }
    }

    pub async fn scan<R: RangeBounds<Bytes> + Clone + Send + Sync + 'static>(
        &self,
        range: R,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send + '_>>> {
        let encryptor = self.encryptor.clone();
        let scan_options = ScanOptions {
            durability_filter: DurabilityLevel::Memory,
            read_ahead_bytes: 8 * 1024 * 1024, // 8MB read-ahead
            max_fetch_tasks: 8,
            ..Default::default()
        };
        let iter = {
            let inner = self.inner.read().await;
            match &*inner {
                SlateDbHandle::ReadWrite(db) => db.scan_with_options(range, &scan_options).await?,
                SlateDbHandle::ReadOnly(reader_swap) => {
                    let reader = reader_swap.load();
                    reader.scan_with_options(range, &scan_options).await?
                }
            }
        };

        Ok(Box::pin(futures::stream::unfold(
            (iter, encryptor),
            |(mut iter, encryptor)| async move {
                match iter.next().await {
                    Ok(Some(kv)) => {
                        let key = kv.key;
                        let encrypted_value = kv.value;
                        match encryptor.decrypt(&key, &encrypted_value) {
                            Ok(decrypted) => {
                                Some((Ok((key, Bytes::from(decrypted))), (iter, encryptor)))
                            }
                            Err(e) => Some((
                                Err(anyhow::anyhow!(
                                    "Decryption failed for key {:?}: {}",
                                    key,
                                    e
                                )),
                                (iter, encryptor),
                            )),
                        }
                    }
                    Ok(None) => None,
                    Err(e) => Some((
                        Err(anyhow::anyhow!("Iterator error: {}", e)),
                        (iter, encryptor),
                    )),
                }
            },
        )))
    }

    pub async fn write_with_options(
        &self,
        batch: EncryptedWriteBatch,
        options: &WriteOptions,
    ) -> Result<()> {
        let is_read_only = self.is_read_only().await;
        
        // Check if we have a streamer (quick read lock check)
        let has_streamer = self.wal_streamer.read().await.is_some();

        // Capture batch data for replication BEFORE consuming the batch
        let batch_data_for_replication = if has_streamer {
            // Serialize the pending operations before they're consumed
            Some(bincode::serialize(&batch.pending_operations)?)
        } else {
            None
        };

        let (inner_batch, _cache_ops) = batch.into_inner().await?;

        // STEP 1: Write to local disk (only if NOT read-only)
        if !is_read_only {
            let inner = self.inner.read().await;
            match &*inner {
                SlateDbHandle::ReadWrite(db) => db.write_with_options(inner_batch, options).await?,
                SlateDbHandle::ReadOnly(_) => unreachable!("is_read_only() returned false but handle is ReadOnly"),
            }
        } else {
            // In read-only mode, we skip local write but still allow replication
            // This allows standby nodes that haven't been promoted yet to still replicate
            tracing::debug!("Skipping local write in read-only mode, replication will still occur if streamer attached");
        }

        // STEP 2: Replicate to standbys (works regardless of local read-only state)
        // This is critical: even if THIS node is read-only, if it has a streamer attached,
        // it means it's about to be promoted or is in transition, and replication should work
        if let Some(batch_data) = batch_data_for_replication {
            let streamer_guard = self.wal_streamer.read().await;
            if let Some(streamer) = streamer_guard.as_ref() {
                // Fire-and-forget replication (Protocol A default)
                // This doesn't block the return
                if let Err(e) = streamer.replicate(bytes::Bytes::from(batch_data)).await {
                    tracing::warn!("WAL replication warning: {}", e);
                    // Continue - local write succeeded (if not read-only)
                }
            }
        }

        // Return error only if we're read-only AND no replication happened
        // (i.e., this was a genuine write attempt on a standby without promotion)
        if is_read_only && !has_streamer {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        Ok(())
    }
    
    /// Write with explicit replication protocol
    pub async fn write_with_protocol(
        &self,
        batch: EncryptedWriteBatch,
        options: &WriteOptions,
        protocol: crate::ha::ReplicationProtocol,
    ) -> Result<()> {
        if self.is_read_only().await {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        // Capture batch data for replication
        let batch_data = bincode::serialize(&batch.pending_operations)?;

        let (inner_batch, _cache_ops) = batch.into_inner().await?;

        // Write to local disk
        {
            let inner = self.inner.read().await;
            match &*inner {
                SlateDbHandle::ReadWrite(db) => db.write_with_options(inner_batch, options).await?,
                SlateDbHandle::ReadOnly(_) => unreachable!(),
            }
        }

        // Replicate with specific protocol
        let streamer_guard = self.wal_streamer.read().await;
        if let Some(streamer) = streamer_guard.as_ref() {
            streamer
                .replicate_with_protocol(bytes::Bytes::from(batch_data), protocol)
                .await?;
        }

        Ok(())
    }

    pub async fn new_write_batch(&self) -> Result<EncryptedWriteBatch> {
        if self.is_read_only().await {
            return Err(FsError::ReadOnlyFilesystem.into());
        }
        Ok(EncryptedWriteBatch::new(self.encryptor.clone()))
    }

    pub async fn put_with_options(
        &self,
        key: &bytes::Bytes,
        value: &[u8],
        put_options: &slatedb::config::PutOptions,
        write_options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only().await {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        let encrypted = self.encryptor.encrypt(key, value)?;
        {
            let inner = self.inner.read().await;
            match &*inner {
                SlateDbHandle::ReadWrite(db) => {
                    db.put_with_options(key, &encrypted, put_options, write_options)
                        .await?
                }
                SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
            }
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if self.is_read_only().await {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        let inner = self.inner.read().await;
        match &*inner {
            SlateDbHandle::ReadWrite(db) => db.flush().await?,
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let inner = self.inner.read().await;
        match &*inner {
            SlateDbHandle::ReadWrite(db) => db.close().await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.close().await?
            }
        }
        Ok(())
    }
}
