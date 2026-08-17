use async_trait::async_trait;
use bytes::Bytes;
use slatedb::BlockTransformer;
use std::sync::Arc;

use crate::config::CompressionConfig;
use crate::frame_codec::{CodecError, FrameCodec};
use crate::task::spawn_blocking_named;

/// HKDF info label for the SST block-encryption subkey. Preserves the historical
/// key derivation so existing data still decodes.
const ENCRYPTION_INFO: &[u8] = b"zerofs-v1-encryption";

/// Payloads at or below this size are transformed inline on the runtime thread:
/// a spawn_blocking handoff costs ~25us p50 (~30-37us added per op), while the
/// transform of a 32KiB block runs 1.4-64us for the cheap compression configs.
const INLINE_MAX_LEN: usize = 64 * 1024;

/// Block transformer that handles compression and encryption for SlateDB.
///
/// Implements SlateDB's `BlockTransformer` to provide transparent compression
/// and encryption at the SST block level, delegating to [`FrameCodec`] with an
/// empty AAD (the historical wire format):
///
/// - Write path: compress -> encrypt
/// - Read path: decrypt -> decompress
///
/// Format: `[nonce (24 bytes)][compressed + encrypted data + AEAD tag]`
pub struct ZeroFsBlockTransformer {
    codec: Arc<FrameCodec>,
}

impl ZeroFsBlockTransformer {
    /// Create a block transformer, returning an error if its key cannot be locked.
    pub fn try_new(
        master_key: &[u8; 32],
        compression: CompressionConfig,
    ) -> Result<Self, CodecError> {
        Ok(Self {
            codec: Arc::new(FrameCodec::try_new(
                master_key,
                ENCRYPTION_INFO,
                compression,
            )?),
        })
    }

    /// Create a shareable transformer, returning an error if its key cannot be locked.
    pub fn try_new_arc(
        master_key: &[u8; 32],
        compression: CompressionConfig,
    ) -> Result<Arc<Self>, CodecError> {
        Ok(Arc::new(Self::try_new(master_key, compression)?))
    }
}

fn to_slatedb_err(e: CodecError) -> slatedb::Error {
    slatedb::Error::data(e.to_string())
}

#[async_trait]
impl BlockTransformer for ZeroFsBlockTransformer {
    /// Encode a block: compress then encrypt.
    async fn encode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        if data.len() <= INLINE_MAX_LEN && self.codec.encode_is_cheap() {
            return Ok(Bytes::from(
                self.codec
                    .seal(data.as_ref(), b"")
                    .map_err(to_slatedb_err)?,
            ));
        }
        let codec = Arc::clone(&self.codec);
        spawn_blocking_named("block-encode", move || {
            Ok(Bytes::from(
                codec.seal(data.as_ref(), b"").map_err(to_slatedb_err)?,
            ))
        })
        .map_err(|e| slatedb::Error::data(format!("Failed to spawn block-encode task: {}", e)))?
        .await
        .map_err(|e| slatedb::Error::data(format!("Task join error: {}", e)))?
    }

    /// Decode a block: decrypt then decompress.
    async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        if data.len() <= INLINE_MAX_LEN {
            return Ok(Bytes::from(
                self.codec
                    .open(data.as_ref(), b"")
                    .map_err(to_slatedb_err)?,
            ));
        }
        let codec = Arc::clone(&self.codec);
        spawn_blocking_named("block-decode", move || {
            Ok(Bytes::from(
                codec.open(data.as_ref(), b"").map_err(to_slatedb_err)?,
            ))
        })
        .map_err(|e| slatedb::Error::data(format!("Failed to spawn block-decode task: {}", e)))?
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
        let transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Lz4)
            .expect("test key should be lockable");
        let data = Bytes::from(vec![42u8; 4096]);

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_roundtrip_zstd() {
        let transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Zstd(3))
            .expect("test key should be lockable");
        let data = Bytes::from(vec![42u8; 4096]);

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_cross_algorithm_lz4_to_zstd() {
        // Encode with LZ4, decode with Zstd configured (should auto-detect LZ4)
        let lz4_transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Lz4)
            .expect("test key should be lockable");
        let zstd_transformer =
            ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Zstd(3))
                .expect("test key should be lockable");

        let data = Bytes::from(vec![1u8; 2048]);
        let encoded = lz4_transformer.encode(data.clone()).await.unwrap();
        let decoded = zstd_transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_cross_algorithm_zstd_to_lz4() {
        // Encode with Zstd, decode with LZ4 configured (should auto-detect Zstd)
        let zstd_transformer =
            ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Zstd(5))
                .expect("test key should be lockable");
        let lz4_transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Lz4)
            .expect("test key should be lockable");

        let data = Bytes::from(vec![2u8; 2048]);
        let encoded = zstd_transformer.encode(data.clone()).await.unwrap();
        let decoded = lz4_transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_different_keys_fail_decrypt() {
        let transformer1 = ZeroFsBlockTransformer::try_new(&[1u8; 32], CompressionConfig::Lz4)
            .expect("test key should be lockable");
        let transformer2 = ZeroFsBlockTransformer::try_new(&[2u8; 32], CompressionConfig::Lz4)
            .expect("test key should be lockable");

        let data = Bytes::from(vec![42u8; 1024]);
        let encoded = transformer1.encode(data).await.unwrap();

        // Should fail to decrypt with wrong key
        assert!(transformer2.decode(encoded).await.is_err());
    }

    #[tokio::test]
    async fn test_empty_data() {
        let transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Lz4)
            .expect("test key should be lockable");
        let data = Bytes::new();

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_large_data() {
        let transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Zstd(3))
            .expect("test key should be lockable");
        let data = Bytes::from(vec![0xABu8; 1024 * 1024]);

        let encoded = transformer.encode(data.clone()).await.unwrap();
        let decoded = transformer.decode(encoded).await.unwrap();

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_truncated_ciphertext_fails() {
        let transformer = ZeroFsBlockTransformer::try_new(&test_key(), CompressionConfig::Lz4)
            .expect("test key should be lockable");

        // Less than nonce size
        let short_data = Bytes::from(vec![0u8; 10]);
        assert!(transformer.decode(short_data).await.is_err());
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::frame_codec::ZSTD_MAGIC;
    use proptest::prelude::*;

    thread_local! {
        static RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        RT.with(|rt| rt.block_on(f))
    }

    fn compression() -> impl Strategy<Value = CompressionConfig> {
        prop_oneof![
            Just(CompressionConfig::Lz4),
            (1i32..=19).prop_map(CompressionConfig::Zstd),
        ]
    }

    // Payloads straddling the 64KiB inline/spawn_blocking boundary, sometimes
    // carrying the zstd magic inline so decode's algorithm auto-detection is
    // exercised against data that could be mistaken for a zstd frame.
    fn payload() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            prop::collection::vec(any::<u8>(), 0..70_000),
            prop::collection::vec(0u8..=3, 0..70_000),
            (0usize..64, prop::collection::vec(any::<u8>(), 0..512)).prop_map(|(off, mut v)| {
                if v.len() >= off + 4 {
                    v[off..off + 4].copy_from_slice(&ZSTD_MAGIC);
                }
                v
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // encode then decode on the same transformer must return the input exactly,
        // for any payload: decode auto-detects the algorithm from the decrypted
        // bytes, so a payload that looks like the other format must not misroute.
        #[test]
        fn encode_decode_roundtrips(data in payload(), cfg in compression()) {
            let key = [7u8; 32];
            let out = block_on(async {
                let t = ZeroFsBlockTransformer::try_new(&key, cfg).expect("test key should be lockable");
                let enc = t.encode(Bytes::from(data.clone())).await?;
                t.decode(enc).await
            });
            match out {
                Ok(dec) => prop_assert_eq!(dec.as_ref(), data.as_slice()),
                Err(e) => prop_assert!(
                    false,
                    "roundtrip failed ({} bytes, {:?}): {}",
                    data.len(),
                    cfg,
                    e
                ),
            }
        }

        // A block encoded under one algorithm decodes under a transformer configured
        // for the other: decode keys off the payload, not its own config.
        #[test]
        fn decode_is_algorithm_agnostic(data in payload(), level in 1i32..=19) {
            let key = [9u8; 32];
            let out = block_on(async {
                let enc = ZeroFsBlockTransformer::try_new(&key, CompressionConfig::Lz4).expect("test key should be lockable")
                    .encode(Bytes::from(data.clone()))
                    .await?;
                ZeroFsBlockTransformer::try_new(&key, CompressionConfig::Zstd(level)).expect("test key should be lockable")
                    .decode(enc)
                    .await
            });
            match out {
                Ok(dec) => prop_assert_eq!(dec.as_ref(), data.as_slice()),
                Err(e) => prop_assert!(false, "cross-decode failed ({} bytes): {}", data.len(), e),
            }
        }

        // AEAD integrity: a block never decodes under a different key.
        #[test]
        fn wrong_key_never_decodes(
            data in prop::collection::vec(any::<u8>(), 1..4096),
            k1 in prop::array::uniform32(any::<u8>()),
            k2 in prop::array::uniform32(any::<u8>()),
        ) {
            prop_assume!(k1 != k2);
            let out = block_on(async {
                let enc = ZeroFsBlockTransformer::try_new(&k1, CompressionConfig::Lz4).expect("test key should be lockable")
                    .encode(Bytes::from(data.clone()))
                    .await
                    .unwrap();
                ZeroFsBlockTransformer::try_new(&k2, CompressionConfig::Lz4).expect("test key should be lockable")
                    .decode(enc)
                    .await
            });
            prop_assert!(out.is_err(), "a block decoded under the wrong key");
        }
    }
}
