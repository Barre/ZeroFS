use crate::fs::cache::UnifiedCache;
use crate::fs::key_codec::PREFIX_CHUNK;
use anyhow::Result;
use bytes::Bytes;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use futures::stream::Stream;
use hkdf::Hkdf;
use rand::{RngCore, thread_rng};
use sha2::Sha256;
use slatedb::{
    WriteBatch,
    config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions},
};
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;

const NONCE_SIZE: usize = 12;

pub struct EncryptionManager {
    cipher: ChaCha20Poly1305,
}

impl EncryptionManager {
    pub fn new(master_key: &[u8; 32]) -> Self {
        let hk = Hkdf::<Sha256>::new(None, master_key);

        let mut encryption_key = [0u8; 32];

        hk.expand(b"zerofs-v1-encryption", &mut encryption_key)
            .expect("valid length");

        Self {
            cipher: ChaCha20Poly1305::new(Key::from_slice(&encryption_key)),
        }
    }

    pub fn encrypt(&self, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

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
        let nonce = Nonce::from_slice(nonce_bytes);

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
    cache_ops: Vec<(Bytes, Option<Vec<u8>>)>, // (key, Some(value) for put, None for delete)
    pending_operations: Vec<(Bytes, Vec<u8>)>,
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

    pub fn put_bytes(&mut self, key: &bytes::Bytes, value: &[u8]) {
        // Queue cache operation if this is a chunk
        if !key.is_empty() && key[0] == PREFIX_CHUNK {
            self.cache_ops.push((key.clone(), Some(value.to_vec())));
        }

        self.pending_operations.push((key.clone(), value.to_vec()));
    }

    pub fn delete_bytes(&mut self, key: &bytes::Bytes) {
        if !key.is_empty() && key[0] == PREFIX_CHUNK {
            self.cache_ops.push((key.clone(), None));
        }

        self.inner.delete(key);
    }

    #[allow(clippy::type_complexity)]
    pub async fn into_inner(self) -> Result<(WriteBatch, Vec<(Bytes, Option<Vec<u8>>)>)> {
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

// Encrypted DB wrapper
pub struct EncryptedDb {
    inner: Arc<slatedb::Db>,
    encryptor: Arc<EncryptionManager>,
    cache: Option<Arc<UnifiedCache>>,
}

impl EncryptedDb {
    pub fn new(db: Arc<slatedb::Db>, encryptor: Arc<EncryptionManager>) -> Self {
        Self {
            inner: db,
            encryptor,
            cache: None,
        }
    }

    pub fn with_cache(mut self, cache: Arc<UnifiedCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub async fn get_bytes(&self, key: &bytes::Bytes) -> Result<Option<bytes::Bytes>> {
        let read_options = ReadOptions {
            durability_filter: DurabilityLevel::Memory,
            ..Default::default()
        };

        match self.inner.get_with_options(key, &read_options).await? {
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
        let iter = self.inner.scan_with_options(range, &scan_options).await?;

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
        let (inner_batch, _cache_ops) = batch.into_inner().await?;

        self.inner.write_with_options(inner_batch, options).await?;

        Ok(())
    }

    pub fn new_write_batch(&self) -> EncryptedWriteBatch {
        EncryptedWriteBatch::new(self.encryptor.clone())
    }

    pub async fn put_with_options(
        &self,
        key: &bytes::Bytes,
        value: &[u8],
        put_options: &slatedb::config::PutOptions,
        write_options: &WriteOptions,
    ) -> Result<()> {
        let encrypted = self.encryptor.encrypt(key, value)?;
        self.inner
            .put_with_options(key, &encrypted, put_options, write_options)
            .await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.inner.close().await?;
        Ok(())
    }
}
