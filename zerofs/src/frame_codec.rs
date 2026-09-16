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

use bytes::{Bytes, BytesMut};
use chacha20poly1305::{
    Key, Tag, XChaCha20Poly1305, XNonce,
    aead::{AeadInPlace, KeyInit},
};
use hkdf::Hkdf;
use rand::{RngCore, thread_rng};
use sha2::Sha256;

use crate::config::CompressionConfig;
use crate::secrets::SecretBytes;

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
    #[error("failed to protect encryption key in memory: {0}")]
    LockedMemory(String),
}

/// Compressed payload for [`FrameCodec::seal_compressed`], kept distinct from
/// plaintext. Includes nonce space and spare tag capacity for sealing in place.
pub struct Compressed(Vec<u8>);

impl Compressed {
    /// Compressed payload size.
    #[allow(clippy::len_without_is_empty)] // a compressed payload is never empty
    pub fn len(&self) -> usize {
        self.0.len() - NONCE_SIZE
    }
}

/// Compress-then-encrypt primitive over a single frame.
///
/// Holds a derived XChaCha20-Poly1305 subkey and a compression config. Cheap to
/// `seal`/`open`; the async / `spawn_blocking` policy lives in the callers.
pub struct FrameCodec {
    subkey: SecretBytes<32>,
    compression: CompressionConfig,
}

impl FrameCodec {
    /// Derive a subkey from `master_key` via HKDF-SHA256 with `info` and build a codec.
    pub fn try_new(
        master_key: &[u8; 32],
        info: &[u8],
        compression: CompressionConfig,
    ) -> Result<Self, CodecError> {
        let hk = Hkdf::<Sha256>::new(None, master_key);
        let mut subkey = SecretBytes::<32>::zeroed()
            .map_err(|error| CodecError::LockedMemory(error.to_string()))?;
        hk.expand(info, subkey.expose_secret_mut())
            .expect("valid HKDF output length");
        Ok(Self {
            subkey,
            compression,
        })
    }

    /// Whether encoding is cheap enough to run inline rather than on a blocking thread.
    pub fn encode_is_cheap(&self) -> bool {
        match self.compression {
            CompressionConfig::Lz4 => true,
            CompressionConfig::Zstd(level) => level <= 12,
        }
    }

    /// Maximum sealed size for a plaintext payload under this codec.
    pub(crate) fn max_sealed_size(&self, payload_size: usize) -> u64 {
        let compressed = match self.compression {
            CompressionConfig::Lz4 => lz4_flex::block::get_maximum_output_size(payload_size)
                .saturating_add(std::mem::size_of::<u32>()),
            CompressionConfig::Zstd(_) => zstd::zstd_safe::compress_bound(payload_size)
                .saturating_add(std::mem::size_of::<u32>()),
        };
        u64::try_from(compressed)
            .unwrap_or(u64::MAX)
            .saturating_add((NONCE_SIZE + TAG_SIZE) as u64)
    }

    /// Compress then encrypt `plain`, binding `aad`. Returns `[nonce][ct+tag]`.
    pub fn seal(&self, plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.seal_compressed(self.compress(plain)?, aad)
    }

    /// Encrypt an already-[`Self::compress`]ed payload, binding `aad`.
    /// `seal(plain, aad)` is exactly `seal_compressed(compress(plain), aad)`;
    /// the split exists so callers can run the compression outside a lock whose
    /// critical section must assign the identifiers the AAD binds, and so a
    /// relocation can rebind a frame's AAD from [`Self::open_compressed`]'s
    /// output without a decompress/recompress round trip.
    pub fn seal_compressed(
        &self,
        Compressed(mut out): Compressed,
        aad: &[u8],
    ) -> Result<Vec<u8>, CodecError> {
        let (nonce, compressed) = out.split_at_mut(NONCE_SIZE);
        thread_rng().fill_bytes(nonce);
        // The long-lived subkey stays in locked memory. The RustCrypto cipher
        // is an operation-local copy whose destructor zeroizes its key.
        let cipher = XChaCha20Poly1305::new(Key::from_slice(self.subkey.expose_secret()));
        let tag = cipher
            .encrypt_in_place_detached(XNonce::from_slice(nonce), aad, compressed)
            .map_err(|_| CodecError::Encrypt)?;
        out.extend_from_slice(tag.as_slice());
        Ok(out)
    }

    /// Decrypt (verifying `aad`) then decompress a frame produced by [`Self::seal`].
    pub fn open(&self, frame: &[u8], aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        let Compressed(compressed) = self.open_compressed(frame, aad)?;
        self.decompress(&compressed[NONCE_SIZE..])
    }

    /// Decode a frame, reusing its allocation when possible.
    pub(crate) fn open_owned(&self, frame: Bytes, aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut frame = BytesMut::from(frame);
        self.decompress(self.decrypt_in_place(&mut frame, aad)?)
    }

    /// Decrypt (verifying `aad`) a frame, returning its still-compressed payload.
    /// Relocation can rebind it to new AAD without decompressing or recompressing.
    pub fn open_compressed(&self, frame: &[u8], aad: &[u8]) -> Result<Compressed, CodecError> {
        let mut out = frame.to_vec();
        self.decrypt_in_place(&mut out, aad)?;
        out.truncate(out.len() - TAG_SIZE);
        Ok(Compressed(out))
    }

    fn decrypt_in_place<'a>(
        &self,
        frame: &'a mut [u8],
        aad: &[u8],
    ) -> Result<&'a mut [u8], CodecError> {
        if frame.len() < NONCE_SIZE + TAG_SIZE {
            return Err(CodecError::TooShort(frame.len()));
        }
        let (nonce, body) = frame.split_at_mut(NONCE_SIZE);
        let (ciphertext, tag) = body.split_at_mut(body.len() - TAG_SIZE);
        // See `seal_compressed`: only the persistent key is required to remain
        // in locked memory; this operation-local cipher zeroizes on drop.
        let cipher = XChaCha20Poly1305::new(Key::from_slice(self.subkey.expose_secret()));
        cipher
            .decrypt_in_place_detached(
                XNonce::from_slice(nonce),
                aad,
                ciphertext,
                Tag::from_slice(tag),
            )
            .map_err(|_| CodecError::Decrypt)?;
        Ok(ciphertext)
    }

    /// Compress `data` with the configured codec. Pure: the output depends only
    /// on the bytes and the configured level, never on shared state, so it can
    /// run on any thread, outside any lock, and feed [`Self::seal_compressed`].
    pub fn compress(&self, data: &[u8]) -> Result<Compressed, CodecError> {
        let bound = self.max_sealed_size(data.len()) as usize;
        let mut out = Vec::with_capacity(bound);
        out.resize(NONCE_SIZE, 0);
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        match self.compression {
            CompressionConfig::Lz4 => {
                out.resize(bound - TAG_SIZE, 0);
                let len = lz4_flex::block::compress_into(data, &mut out[NONCE_SIZE + 4..])
                    .map_err(|e| CodecError::Compress(e.to_string()))?;
                out.truncate(NONCE_SIZE + 4 + len);
            }
            CompressionConfig::Zstd(level) => {
                let mut dest = std::io::Cursor::new(&mut out);
                dest.set_position((NONCE_SIZE + 4) as u64);
                zstd_compress(data, level, &mut dest)?;
            }
        }
        // Release excess capacity, keeping room for the authentication tag.
        out.shrink_to(out.len() + TAG_SIZE);
        Ok(Compressed(out))
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

/// Compress via a thread-local reused zstd context: creating a context per
/// 32 KiB frame costs more than low-level compression itself. Keyed by level so
/// codecs at different levels (or a config change) rebuild it; the output is
/// byte-identical to one-shot `zstd::bulk::compress` at the same level.
fn zstd_compress(
    data: &[u8],
    level: i32,
    dest: &mut impl zstd::zstd_safe::WriteBuf,
) -> Result<usize, CodecError> {
    use std::cell::RefCell;
    thread_local! {
        static ZSTD: RefCell<Option<(i32, zstd::bulk::Compressor<'static>)>> =
            const { RefCell::new(None) };
    }
    ZSTD.with(|slot| {
        let mut slot = slot.borrow_mut();
        if !matches!(&*slot, Some((l, _)) if *l == level) {
            let ctx = zstd::bulk::Compressor::new(level)
                .map_err(|e| CodecError::Compress(e.to_string()))?;
            *slot = Some((level, ctx));
        }
        slot.as_mut()
            .expect("compressor installed above")
            .1
            .compress_to_buffer(data, dest)
            .map_err(|e| CodecError::Compress(e.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn codec() -> FrameCodec {
        FrameCodec::try_new(&[7u8; 32], b"frame-codec-test", CompressionConfig::Lz4)
            .expect("test key should be lockable")
    }

    #[test]
    fn owned_and_shared_frames_preserve_authentication_and_cached_ciphertext() {
        for compression in [CompressionConfig::Lz4, CompressionConfig::Zstd(3)] {
            let c = FrameCodec::try_new(&[7; 32], b"decode-ownership", compression).unwrap();
            let plain = vec![0x5a; 32768];
            let sealed = c.seal(&plain, b"correct aad").unwrap();
            assert_eq!(
                c.open_owned(Bytes::from(sealed.clone()), b"correct aad")
                    .unwrap(),
                plain
            );
            let shared = Bytes::from(sealed.clone());
            assert_eq!(c.open_owned(shared.clone(), b"correct aad").unwrap(), plain);
            assert_eq!(shared.as_ref(), sealed);
            assert!(matches!(
                c.open_owned(shared.clone(), b"wrong aad"),
                Err(CodecError::Decrypt)
            ));
            assert_eq!(shared.as_ref(), sealed);
            let mut corrupt = sealed;
            *corrupt.last_mut().unwrap() ^= 1;
            assert!(matches!(
                c.open_owned(Bytes::from(corrupt), b"correct aad"),
                Err(CodecError::Decrypt)
            ));
        }
    }

    // seal() must remain exactly seal_compressed(compress(..)): the write path
    // splits the phases around a lock and both halves must roundtrip together.
    #[test]
    fn split_seal_roundtrips_like_fused_seal() {
        for c in [
            codec(),
            FrameCodec::try_new(&[7u8; 32], b"frame-codec-test", CompressionConfig::Zstd(3))
                .expect("test key should be lockable"),
        ] {
            let mut random = vec![0; 40_000];
            rand::rngs::StdRng::seed_from_u64(7).fill_bytes(&mut random);
            for plain in [vec![], vec![9u8; 40_000], random] {
                let expected = match c.compression {
                    CompressionConfig::Lz4 => lz4_flex::compress_prepend_size(&plain),
                    CompressionConfig::Zstd(level) => {
                        let mut bytes = (plain.len() as u32).to_le_bytes().to_vec();
                        bytes.extend(zstd::bulk::compress(&plain, level).unwrap());
                        bytes
                    }
                };
                let compressed = c.compress(&plain).unwrap();
                assert_eq!(&compressed.0[NONCE_SIZE..], expected);
                assert_eq!(compressed.len(), expected.len());
                assert_eq!(
                    compressed.0.capacity(),
                    NONCE_SIZE + expected.len() + TAG_SIZE
                );
                let allocation = compressed.0.as_ptr();
                let sealed = c.seal_compressed(compressed, b"aad").unwrap();
                assert_eq!(sealed.as_ptr(), allocation);
                assert!(sealed.len() as u64 <= c.max_sealed_size(plain.len()));
                assert_eq!(c.open(&sealed, b"aad").unwrap(), plain);
                assert!(matches!(
                    c.open(&sealed, b"other"),
                    Err(CodecError::Decrypt)
                ));
                let compressed = c.open_compressed(&sealed, b"aad").unwrap();
                assert_eq!(&compressed.0[NONCE_SIZE..], expected);
                let allocation = compressed.0.as_ptr();
                let resealed = c.seal_compressed(compressed, b"relocated").unwrap();
                assert_eq!(resealed.as_ptr(), allocation);
                assert_eq!(c.open(&resealed, b"relocated").unwrap(), plain);
            }
        }
    }

    // The reused zstd context must rebuild when the level changes and keep
    // producing output the (level-agnostic) decompressor accepts.
    #[test]
    fn zstd_context_reuse_survives_level_changes() {
        let a = FrameCodec::try_new(&[7u8; 32], b"t", CompressionConfig::Zstd(1))
            .expect("test key should be lockable");
        let b = FrameCodec::try_new(&[7u8; 32], b"t", CompressionConfig::Zstd(19))
            .expect("test key should be lockable");
        let plain = vec![5u8; 10_000];
        for c in [&a, &b, &a] {
            let sealed = c.seal(&plain, b"aad").unwrap();
            assert_eq!(c.open(&sealed, b"aad").unwrap(), plain);
        }
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
        let a = FrameCodec::try_new(&[1u8; 32], b"info-a", CompressionConfig::Lz4)
            .expect("test key should be lockable");
        let b = FrameCodec::try_new(&[1u8; 32], b"info-b", CompressionConfig::Lz4)
            .expect("test key should be lockable");
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
