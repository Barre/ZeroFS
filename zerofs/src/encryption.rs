use crate::config::CompressionConfig;
use crate::fs::CHUNK_SIZE;
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
use tokio_stream::Stream;

const NONCE_SIZE: usize = 24;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

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
    compression: CompressionConfig,
}

impl EncryptionManager {
    pub fn new(master_key: &[u8; 32], compression: CompressionConfig) -> Self {
        let hk = Hkdf::<Sha256>::new(None, master_key);

        let mut encryption_key = [0u8; 32];

        hk.expand(b"zerofs-v1-encryption", &mut encryption_key)
            .expect("valid length");

        Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(&encryption_key)),
            compression,
        }
    }

    pub fn encrypt(&self, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        // Check if this is a chunk key to decide on compression
        let data =
            if key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk) {
                match self.compression {
                    CompressionConfig::Lz4 => lz4_flex::compress_prepend_size(plaintext),
                    CompressionConfig::Zstd(level) => zstd::bulk::compress(plaintext, level)
                        .map_err(|e| anyhow::anyhow!("Zstd compression failed: {}", e))?,
                }
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

        let (nonce_bytes, ciphertext) = data.split_at(NONCE_SIZE);
        let nonce = XNonce::from_slice(nonce_bytes);

        let decrypted = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow::anyhow!("Decryption failed: {}", e))?;

        if key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk) {
            if decrypted.len() >= 4 && decrypted[..4] == ZSTD_MAGIC {
                zstd::bulk::decompress(&decrypted, CHUNK_SIZE)
                    .map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e))
            } else {
                lz4_flex::decompress_size_prepended(&decrypted)
                    .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))
            }
        } else {
            Ok(decrypted)
        }
    }
}

pub struct EncryptedTransaction {
    inner: WriteBatch,
    encryptor: Arc<EncryptionManager>,
    pending_operations: Vec<(Bytes, Bytes)>,
}

impl EncryptedTransaction {
    pub fn new(encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: WriteBatch::new(),
            encryptor,

            pending_operations: Vec::new(),
        }
    }

    pub fn put_bytes(&mut self, key: &bytes::Bytes, value: Bytes) {
        self.pending_operations.push((key.clone(), value));
    }

    pub fn delete_bytes(&mut self, key: &bytes::Bytes) {
        self.inner.delete(key);
    }

    #[allow(clippy::type_complexity)]
    pub async fn into_inner(self) -> Result<WriteBatch> {
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

        Ok(inner)
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
                let is_chunk = key.first().and_then(|&b| KeyPrefix::try_from(b).ok())
                    == Some(KeyPrefix::Chunk);

                let decrypted = if is_chunk {
                    let encryptor = self.encryptor.clone();
                    let key = key.clone();
                    spawn_blocking_named("decrypt", move || encryptor.decrypt(&key, &encrypted))
                        .await
                        .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??
                } else {
                    self.encryptor.decrypt(key, &encrypted)?
                };
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

        let (tx_in, mut rx_in) = tokio::sync::mpsc::channel::<(Bytes, Bytes)>(32);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel::<Result<(Bytes, Bytes)>>(32);

        spawn_blocking_named("scan-decrypt", move || {
            while let Some((key, encrypted)) = rx_in.blocking_recv() {
                let result = if key.as_ref() == crate::fs::key_codec::SYSTEM_WRAPPED_ENCRYPTION_KEY
                {
                    Ok((key, encrypted))
                } else {
                    encryptor
                        .decrypt(&key, &encrypted)
                        .map(|dec| (key, Bytes::from(dec)))
                };
                if tx_out.blocking_send(result).is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let mut iter = iter;
            while let Ok(Some(kv)) = iter.next().await {
                if tx_in.send((kv.key, kv.value)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(
            rx_out,
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

        let inner_batch = txn.into_inner().await?;

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

        let is_chunk =
            key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) == Some(KeyPrefix::Chunk);

        let encrypted = if is_chunk {
            let encryptor = self.encryptor.clone();
            let key_clone = key.clone();
            let value = value.to_vec();

            spawn_blocking_named("encrypt", move || encryptor.encrypt(&key_clone, &value))
                .await
                .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??
        } else {
            self.encryptor.encrypt(key, value)?
        };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::key_codec::KeyCodec;

    fn chunk_key() -> Vec<u8> {
        KeyCodec::chunk_key(1, 0).to_vec()
    }

    fn non_chunk_key() -> Vec<u8> {
        KeyCodec::inode_key(1).to_vec()
    }

    #[test]
    fn test_lz4_compress_decompress() {
        let manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Lz4);
        let plaintext = vec![0u8; 1024];
        let key = chunk_key();

        let encrypted = manager.encrypt(&key, &plaintext).unwrap();
        let decrypted = manager.decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_zstd_compress_decompress() {
        let manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Zstd(3));
        let plaintext = vec![0u8; 1024];
        let key = chunk_key();

        let encrypted = manager.encrypt(&key, &plaintext).unwrap();
        let decrypted = manager.decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_zstd_high_level_compress_decompress() {
        let manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Zstd(19));
        let plaintext = vec![42u8; 8192];
        let key = chunk_key();

        let encrypted = manager.encrypt(&key, &plaintext).unwrap();
        let decrypted = manager.decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_cross_algorithm_lz4_written_zstd_configured() {
        // Write with lz4
        let lz4_manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Lz4);
        let plaintext = vec![1u8; 2048];
        let key = chunk_key();

        let encrypted = lz4_manager.encrypt(&key, &plaintext).unwrap();

        // Read with zstd configured - should auto-detect lz4
        let zstd_manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Zstd(3));
        let decrypted = zstd_manager.decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_cross_algorithm_zstd_written_lz4_configured() {
        // Write with zstd
        let zstd_manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Zstd(5));
        let plaintext = vec![2u8; 2048];
        let key = chunk_key();

        let encrypted = zstd_manager.encrypt(&key, &plaintext).unwrap();

        // Read with lz4 configured - should auto-detect zstd
        let lz4_manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Lz4);
        let decrypted = lz4_manager.decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_non_chunk_data_not_compressed() {
        let manager = EncryptionManager::new(&[0u8; 32], CompressionConfig::Zstd(3));
        let plaintext = b"metadata content".to_vec();
        let key = non_chunk_key();

        let encrypted = manager.encrypt(&key, &plaintext).unwrap();
        let decrypted = manager.decrypt(&key, &encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_zstd_magic_detection() {
        // Verify zstd compressed data starts with magic bytes
        let data = vec![0u8; 1024];
        let compressed = zstd::bulk::compress(&data, 3).unwrap();

        assert!(compressed.starts_with(&ZSTD_MAGIC));
    }

    #[test]
    fn test_lz4_no_zstd_magic() {
        // Verify lz4 compressed data does NOT start with zstd magic
        let data = vec![0u8; 1024];
        let compressed = lz4_flex::compress_prepend_size(&data);

        assert!(!compressed.starts_with(&ZSTD_MAGIC));
    }
}
