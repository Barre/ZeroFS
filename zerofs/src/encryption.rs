use crate::fs::errors::FsError;
use crate::fs::key_codec::KeyPrefix;
use crate::task::spawn_blocking_named;
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

const NONCE_SIZE: usize = 24;

/// Fatal handler for SlateDB write errors.
/// After a write failure, the database state is unknown - exit and let
/// the eventual orchestrator restart the service to rebuild from a known-good state.
pub fn exit_on_write_error(err: impl std::fmt::Display) -> ! {
    tracing::error!("Fatal write error, exiting: {}", err);
    std::process::exit(1)
}

#[derive(Clone)]
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
        let data =
            if key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk) {
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
        if key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk) {
            lz4_flex::decompress_size_prepended(&decrypted)
                .map_err(|e| anyhow::anyhow!("Decompression failed: {}", e))
        } else {
            Ok(decrypted)
        }
    }
}

pub struct EncryptedTransaction {
    inner: WriteBatch,
    encryptor: Arc<EncryptionManager>,
    cache_ops: Vec<(Bytes, Option<Bytes>)>,
    pending_operations: Vec<(Bytes, Bytes)>,
}

impl EncryptedTransaction {
    pub fn new(encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: WriteBatch::new(),
            encryptor,
            cache_ops: Vec::new(),
            pending_operations: Vec::new(),
        }
    }

    pub fn put_bytes(&mut self, key: &bytes::Bytes, value: Bytes) {
        if key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk) {
            self.cache_ops.push((key.clone(), Some(value.clone())));
        }
        self.pending_operations.push((key.clone(), value));
    }

    pub fn delete_bytes(&mut self, key: &bytes::Bytes) {
        if key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk) {
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

            let encrypted_operations = spawn_blocking_named("encrypt-batch", move || {
                operations
                    .into_iter()
                    .map(|(key, value)| {
                        let encrypted = encryptor.encrypt(&key, &value)?;
                        Ok::<(Bytes, Vec<u8>), anyhow::Error>((key, encrypted))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

            for (key, encrypted) in encrypted_operations {
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

impl Clone for SlateDbHandle {
    fn clone(&self) -> Self {
        match self {
            SlateDbHandle::ReadWrite(db) => SlateDbHandle::ReadWrite(db.clone()),
            SlateDbHandle::ReadOnly(reader) => {
                SlateDbHandle::ReadOnly(ArcSwap::new(reader.load_full()))
            }
        }
    }
}

impl SlateDbHandle {
    pub fn is_read_only(&self) -> bool {
        matches!(self, SlateDbHandle::ReadOnly(_))
    }
}

// Encrypted DB wrapper
pub struct EncryptedDb {
    inner: SlateDbHandle,
    encryptor: Arc<EncryptionManager>,
}

impl EncryptedDb {
    pub fn new(db: Arc<slatedb::Db>, encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: SlateDbHandle::ReadWrite(db),
            encryptor,
        }
    }

    pub fn new_read_only(db_reader: ArcSwap<DbReader>, encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: SlateDbHandle::ReadOnly(db_reader),
            encryptor,
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    pub fn swap_reader(&self, new_reader: Arc<DbReader>) -> Result<()> {
        match &self.inner {
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
            cache_blocks: true,
            ..Default::default()
        };

        let encrypted = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.get_with_options(key, &read_options).await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.get_with_options(key, &read_options).await?
            }
        };

        match encrypted {
            Some(encrypted) => {
                let encryptor = self.encryptor.clone();
                let key = key.clone();
                let decrypted =
                    spawn_blocking_named("decrypt", move || encryptor.decrypt(&key, &encrypted))
                        .await
                        .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;
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
            read_ahead_bytes: 1024 * 1024,
            cache_blocks: true,
            max_fetch_tasks: 8,
            ..Default::default()
        };
        let iter = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.scan_with_options(range, &scan_options).await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.scan_with_options(range, &scan_options).await?
            }
        };

        Ok(Box::pin(futures::stream::unfold(
            (iter, encryptor),
            |(mut iter, encryptor)| async move {
                match iter.next().await {
                    Ok(Some(kv)) => {
                        let key = kv.key;
                        let encrypted_value = kv.value;

                        // Skip decryption for system keys that use different encryption
                        // (wrapped_encryption_key uses password-derived encryption)
                        if key.as_ref() == crate::fs::key_codec::SYSTEM_WRAPPED_ENCRYPTION_KEY {
                            return Some((Ok((key, encrypted_value)), (iter, encryptor)));
                        }

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
        txn: EncryptedTransaction,
        options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        let (inner_batch, _cache_ops) = txn.into_inner().await?;

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.write_with_options(inner_batch, options).await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }

        Ok(())
    }

    pub(crate) async fn write_raw_batch(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.write_with_options(batch, options).await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }
        Ok(())
    }

    pub fn new_transaction(&self) -> Result<EncryptedTransaction, FsError> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem);
        }
        Ok(EncryptedTransaction::new(self.encryptor.clone()))
    }

    pub async fn put_with_options(
        &self,
        key: &bytes::Bytes,
        value: &[u8],
        put_options: &slatedb::config::PutOptions,
        write_options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        let encryptor = self.encryptor.clone();
        let key_clone = key.clone();
        let value = value.to_vec();
        let encrypted =
            spawn_blocking_named("encrypt", move || encryptor.encrypt(&key_clone, &value))
                .await
                .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db
                    .put_with_options(key, &encrypted, put_options, write_options)
                    .await
                {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.flush().await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.close().await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.close().await?
            }
        }
        Ok(())
    }
}
