use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::key_codec::KeyCodec;
use crate::fs::key_codec::KeyPrefix;
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

    pub fn save_inode(
        &mut self,
        id: InodeId,
        inode: &Inode,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let key = KeyCodec::inode_key(id);
        let data = bincode::serialize(inode)?;
        self.put_bytes(&key, Bytes::from(data));
        Ok(())
    }

    pub fn delete_inode(&mut self, id: InodeId) {
        let key = KeyCodec::inode_key(id);
        self.delete_bytes(&key);
    }

    pub fn add_dir_entry(&mut self, dir_id: InodeId, name: &[u8], entry_id: InodeId) {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        self.put_bytes(&entry_key, KeyCodec::encode_dir_entry(entry_id));

        let scan_key = KeyCodec::dir_scan_key(dir_id, entry_id, name);
        self.put_bytes(&scan_key, KeyCodec::encode_dir_entry(entry_id));
    }

    pub fn remove_dir_entry(&mut self, dir_id: InodeId, name: &[u8], entry_id: InodeId) {
        let entry_key = KeyCodec::dir_entry_key(dir_id, name);
        self.delete_bytes(&entry_key);

        let scan_key = KeyCodec::dir_scan_key(dir_id, entry_id, name);
        self.delete_bytes(&scan_key);
    }

    pub fn delete_chunk(&mut self, inode_id: InodeId, chunk_idx: u64) {
        let key = KeyCodec::chunk_key(inode_id, chunk_idx);
        self.delete_bytes(&key);
    }

    pub fn save_inode_counter(&mut self, next_id: u64) {
        let key = KeyCodec::system_counter_key();
        self.put_bytes(&key, KeyCodec::encode_counter(next_id));
    }

    pub fn add_tombstone(&mut self, inode_id: InodeId, size: u64) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let key = KeyCodec::tombstone_key(timestamp, inode_id);
        self.put_bytes(&key, KeyCodec::encode_tombstone_size(size));
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
            SlateDbHandle::ReadWrite(db) => db.write_with_options(inner_batch, options).await?,
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
            SlateDbHandle::ReadWrite(db) => db.write_with_options(batch, options).await?,
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }
        Ok(())
    }

    pub fn new_transaction(&self) -> Result<EncryptedTransaction> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
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

        let encrypted = self.encryptor.encrypt(key, value)?;
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                db.put_with_options(key, &encrypted, put_options, write_options)
                    .await?
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
            SlateDbHandle::ReadWrite(db) => db.flush().await?,
            SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.close().await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.close().await?
            }
        }
        Ok(())
    }
}
