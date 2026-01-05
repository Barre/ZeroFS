use async_trait::async_trait;
use bytes::Bytes;
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use hkdf::Hkdf;
use rand::{RngCore, thread_rng};
use sha2::Sha256;
use slatedb::BlockTransformer;
use std::sync::Arc;

use crate::config::CompressionConfig;
use crate::task::spawn_blocking_named;

const NONCE_SIZE: usize = 24;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Block transformer that handles compression and encryption for SlateDB.
///
/// This implements SlateDB's `BlockTransformer` trait to provide transparent
/// compression and encryption at the SST block level. The transformation
/// pipeline is:
///
/// - Write path: compress -> encrypt
/// - Read path: decrypt -> decompress
///
/// Format: `[nonce (24 bytes)][compressed + encrypted data + AEAD tag]`
pub struct ZeroFsBlockTransformer {
    inner: Arc<TransformerInner>,
}

struct TransformerInner {
    cipher: XChaCha20Poly1305,
    compression: CompressionConfig,
}

impl ZeroFsBlockTransformer {
    /// Create a new block transformer with the given master key and compression config.
    ///
    /// The encryption key is derived from the master key using HKDF-SHA256 with
    /// the info string "zerofs-v1-encryption".
    pub fn new(master_key: &[u8; 32], compression: CompressionConfig) -> Self {
        let hk = Hkdf::<Sha256>::new(None, master_key);

        let mut encryption_key = [0u8; 32];
        hk.expand(b"zerofs-v1-encryption", &mut encryption_key)
            .expect("valid length");

        Self {
            inner: Arc::new(TransformerInner {
                cipher: XChaCha20Poly1305::new(Key::from_slice(&encryption_key)),
                compression,
            }),
        }
    }

    /// Create a shareable Arc-wrapped transformer.
    pub fn new_arc(master_key: &[u8; 32], compression: CompressionConfig) -> Arc<Self> {
        Arc::new(Self::new(master_key, compression))
    }
}

impl TransformerInner {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, slatedb::Error> {
        match self.compression {
            CompressionConfig::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            CompressionConfig::Zstd(level) => {
                let compressed = zstd::bulk::compress(data, level)
                    .map_err(|e| slatedb::Error::data(format!("Zstd compression failed: {}", e)))?;
                // Prepend original size as little-endian u32 for decompression
                let size = data.len() as u32;
                let mut result = Vec::with_capacity(4 + compressed.len());
                result.extend_from_slice(&size.to_le_bytes());
                result.extend_from_slice(&compressed);
                Ok(result)
            }
        }
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, slatedb::Error> {
        // Auto-detect compression algorithm based on magic bytes
        // Zstd format: [u32 size][zstd data with magic at offset 4]
        if data.len() >= 8 && data[4..8] == ZSTD_MAGIC {
            let size = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            zstd::bulk::decompress(&data[4..], size)
                .map_err(|e| slatedb::Error::data(format!("Zstd decompression failed: {}", e)))
        } else {
            // LZ4 compressed (also has size prepended by lz4_flex)
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| slatedb::Error::data(format!("LZ4 decompression failed: {}", e)))
        }
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, slatedb::Error> {
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, data)
            .map_err(|e| slatedb::Error::data(format!("Encryption failed: {}", e)))?;

        // Format: [nonce][ciphertext]
        let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, slatedb::Error> {
        if data.len() < NONCE_SIZE {
            return Err(slatedb::Error::data(
                "Invalid ciphertext: too short".to_string(),
            ));
        }

        let (nonce_bytes, ciphertext) = data.split_at(NONCE_SIZE);
        let nonce = XNonce::from_slice(nonce_bytes);

        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| slatedb::Error::data(format!("Decryption failed: {}", e)))
    }
}

#[async_trait]
impl BlockTransformer for ZeroFsBlockTransformer {
    /// Encode a block: compress then encrypt.
    async fn encode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        let inner = Arc::clone(&self.inner);
        spawn_blocking_named("block-encode", move || {
            let compressed = inner.compress(&data)?;
            let encrypted = inner.encrypt(&compressed)?;
            Ok(Bytes::from(encrypted))
        })
        .await
        .map_err(|e| slatedb::Error::data(format!("Task join error: {}", e)))?
    }

    /// Decode a block: decrypt then decompress.
    async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        let inner = Arc::clone(&self.inner);
        spawn_blocking_named("block-decode", move || {
            let decrypted = inner.decrypt(&data)?;
            let decompressed = inner.decompress(&decrypted)?;
            Ok(Bytes::from(decompressed))
        })
        .await
        .map_err(|e| slatedb::Error::data(format!("Task join error: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0u8; 32]
    }

    #[tokio::test]
    async fn test_roundtrip_lz4() {
        let transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Lz4);
        let data = Bytes::from(vec![42u8; 4096]);

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_roundtrip_zstd() {
        let transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Zstd(3));
        let data = Bytes::from(vec![42u8; 4096]);

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_cross_algorithm_lz4_to_zstd() {
        // Encode with LZ4, decode with Zstd configured (should auto-detect LZ4)
        let lz4_transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Lz4);
        let zstd_transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Zstd(3));

        let data = Bytes::from(vec![1u8; 2048]);
        let encoded = lz4_transformer.encode(data.clone()).await.unwrap();
        let decoded = zstd_transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_cross_algorithm_zstd_to_lz4() {
        // Encode with Zstd, decode with LZ4 configured (should auto-detect Zstd)
        let zstd_transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Zstd(5));
        let lz4_transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Lz4);

        let data = Bytes::from(vec![2u8; 2048]);
        let encoded = zstd_transformer.encode(data.clone()).await.unwrap();
        let decoded = lz4_transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_different_keys_fail_decrypt() {
        let transformer1 = ZeroFsBlockTransformer::new(&[1u8; 32], CompressionConfig::Lz4);
        let transformer2 = ZeroFsBlockTransformer::new(&[2u8; 32], CompressionConfig::Lz4);

        let data = Bytes::from(vec![42u8; 1024]);
        let encoded = transformer1.encode(data).await.unwrap();

        // Should fail to decrypt with wrong key
        assert!(transformer2.decode(encoded).await.is_err());
    }

    #[tokio::test]
    async fn test_empty_data() {
        let transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Lz4);
        let data = Bytes::new();

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_large_data() {
        let transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Zstd(3));
        let data = Bytes::from(vec![0xABu8; 1024 * 1024]);

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_truncated_ciphertext_fails() {
        let transformer = ZeroFsBlockTransformer::new(&test_key(), CompressionConfig::Lz4);

        // Less than nonce size
        let short_data = Bytes::from(vec![0u8; 10]);
        assert!(transformer.decode(short_data).await.is_err());
    }
}
