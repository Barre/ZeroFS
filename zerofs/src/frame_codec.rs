//! Shared compress-then-encrypt frame codec.
//!
//! Used by the SlateDB [`BlockTransformer`](crate::block_transformer) (with an
//! empty AAD, preserving the historical on-disk SST block format) and by the
//! data-plane segment writer (with a per-frame AAD that binds a frame to its
//! segment, slot, and logical block so it cannot be lifted elsewhere).
//!
//! Sealed frame wire format:
//!
//! ```text
//! [nonce: 24][ ciphertext( compress(plain) ) + tag: 16 ]
//! ```
//!
//! Compression is auto-detected on `open` from the (decrypted) payload, so a
//! codec configured for one algorithm decodes frames written by the other.

use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, AeadInPlace, KeyInit, Payload},
};
use hkdf::Hkdf;
use rand::{RngCore, thread_rng};
use sha2::Sha256;

use crate::config::CompressionConfig;

const NONCE_SIZE: usize = 24;
const TAG_SIZE: usize = 16;
pub(crate) const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("compression failed: {0}")]
    Compress(String),
    #[error("decompression failed: {0}")]
    Decompress(String),
    #[error("encryption failed")]
    Encrypt,
    #[error("frame too short: {0} bytes")]
    TooShort(usize),
    #[error("decryption failed (wrong key, AAD mismatch, or corrupt frame)")]
    Decrypt,
}

/// Compress-then-encrypt primitive over a single frame.
///
/// Holds a derived XChaCha20-Poly1305 subkey and a compression config. Cheap to
/// `seal`/`open`; the async / `spawn_blocking` policy lives in the callers.
pub struct FrameCodec {
    cipher: XChaCha20Poly1305,
    compression: CompressionConfig,
}

impl FrameCodec {
    /// Derive a subkey from `master_key` via HKDF-SHA256 with `info` and build a codec.
    pub fn new(master_key: &[u8; 32], info: &[u8], compression: CompressionConfig) -> Self {
        let hk = Hkdf::<Sha256>::new(None, master_key);
        let mut subkey = [0u8; 32];
        hk.expand(info, &mut subkey)
            .expect("valid HKDF output length");
        Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(&subkey)),
            compression,
        }
    }

    /// Whether encoding is cheap enough to run inline rather than on a blocking thread.
    pub fn encode_is_cheap(&self) -> bool {
        match self.compression {
            CompressionConfig::Lz4 => true,
            CompressionConfig::Zstd(level) => level <= 12,
        }
    }

    /// Compress then encrypt `plain`, binding `aad`. Returns `[nonce][ct+tag]`.
    pub fn seal(&self, plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        let compressed = self.compress(plain)?;
        let mut out = Vec::with_capacity(NONCE_SIZE + compressed.len() + TAG_SIZE);
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        thread_rng().fill_bytes(&mut nonce_bytes);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&compressed);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let tag = self
            .cipher
            .encrypt_in_place_detached(nonce, aad, &mut out[NONCE_SIZE..])
            .map_err(|_| CodecError::Encrypt)?;
        out.extend_from_slice(tag.as_slice());
        Ok(out)
    }

    /// Decrypt (verifying `aad`) then decompress a frame produced by [`Self::seal`].
    pub fn open(&self, frame: &[u8], aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        if frame.len() < NONCE_SIZE + TAG_SIZE {
            return Err(CodecError::TooShort(frame.len()));
        }
        let (nonce_bytes, ciphertext) = frame.split_at(NONCE_SIZE);
        let nonce = XNonce::from_slice(nonce_bytes);
        let compressed = self
            .cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| CodecError::Decrypt)?;
        self.decompress(&compressed)
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        match self.compression {
            CompressionConfig::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            CompressionConfig::Zstd(level) => {
                let compressed = zstd::bulk::compress(data, level)
                    .map_err(|e| CodecError::Compress(e.to_string()))?;
                // Prepend original size (LE u32) for decompression.
                let mut result = Vec::with_capacity(4 + compressed.len());
                result.extend_from_slice(&(data.len() as u32).to_le_bytes());
                result.extend_from_slice(&compressed);
                Ok(result)
            }
        }
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        // Zstd frames carry the magic at offset 4 (after our LE size prefix);
        // everything else is treated as lz4 (also size-prefixed by lz4_flex).
        if data.len() >= 8 && data[4..8] == ZSTD_MAGIC {
            let size = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            zstd::bulk::decompress(&data[4..], size)
                .map_err(|e| CodecError::Decompress(e.to_string()))
        } else {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| CodecError::Decompress(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec() -> FrameCodec {
        FrameCodec::new(&[7u8; 32], b"frame-codec-test", CompressionConfig::Lz4)
    }

    #[test]
    fn seal_open_roundtrips_with_aad() {
        let c = codec();
        let plain = vec![3u8; 1000];
        let sealed = c.seal(&plain, b"aad-1").unwrap();
        assert_eq!(c.open(&sealed, b"aad-1").unwrap(), plain);
    }

    #[test]
    fn aad_mismatch_fails() {
        let c = codec();
        let sealed = c.seal(b"payload", b"aad-1").unwrap();
        assert!(matches!(
            c.open(&sealed, b"aad-2"),
            Err(CodecError::Decrypt)
        ));
        assert!(matches!(c.open(&sealed, b""), Err(CodecError::Decrypt)));
    }

    #[test]
    fn wrong_subkey_fails() {
        let a = FrameCodec::new(&[1u8; 32], b"info-a", CompressionConfig::Lz4);
        let b = FrameCodec::new(&[1u8; 32], b"info-b", CompressionConfig::Lz4);
        let sealed = a.seal(b"payload", b"aad").unwrap();
        // Same master key, different HKDF label -> different subkey -> must not open.
        assert!(matches!(b.open(&sealed, b"aad"), Err(CodecError::Decrypt)));
    }

    #[test]
    fn too_short_frame_fails() {
        let c = codec();
        assert!(matches!(
            c.open(&[0u8; 10], b""),
            Err(CodecError::TooShort(10))
        ));
    }

    #[test]
    fn empty_payload_roundtrips() {
        let c = codec();
        let sealed = c.seal(b"", b"aad").unwrap();
        assert!(c.open(&sealed, b"aad").unwrap().is_empty());
    }
}
