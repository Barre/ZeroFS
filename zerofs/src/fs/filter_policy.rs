//! All ZeroFS keys begin with `[1-byte kind] + [8-byte BE id]`. For directory
//! kinds (`DirEntry`, `DirScan`), grouping the bloom on that 9-byte prefix
//! lets a single SST be skipped for an entire directory on `readdir`.
//!
//! Chunks are deliberately NOT extracted: `scan_prefix(chunk_inode_prefix)`
//! would expand the SST set considered (every SST holding any chunk of the
//! inode), losing more on iterator init than the filter can save. The narrow
//! `scan(chunk(id, start)..chunk(id, end+1))` is already optimal.

use slatedb::filter_policy::{BloomFilterPolicy, FilterPolicy};
use slatedb::prefix_extractor::{PrefixExtractor, PrefixTarget};
use std::sync::Arc;

use super::key_codec::KeyPrefix;

/// 1-byte kind tag + 8-byte BE id.
const ENTITY_PREFIX_LEN: usize = 9;

/// Versioned name so that future tweaks to which kinds are extracted produce a
/// distinct policy name and old SSTs aren't silently misread.
const EXTRACTOR_NAME: &str = "zerofs_v1";

/// Returns `Some(9)` for keys whose first byte is a kind that benefits from
/// inode/dir-scoped filtering on prefix scans, when at least the 9-byte
/// `(kind, id)` is present. Other kinds fall through to whole-key filtering.
pub struct ZerofsPrefixExtractor;

impl ZerofsPrefixExtractor {
    fn extracts_kind(byte: u8) -> bool {
        matches!(
            KeyPrefix::try_from(byte),
            Ok(KeyPrefix::DirEntry | KeyPrefix::DirScan)
        )
    }
}

impl PrefixExtractor for ZerofsPrefixExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
        };
        if bytes.len() < ENTITY_PREFIX_LEN {
            return None;
        }
        if !Self::extracts_kind(bytes[0]) {
            return None;
        }
        Some(ENTITY_PREFIX_LEN)
    }
}

/// Both the legacy whole-key bloom (which matches the
/// SlateDB default and any pre-upgrade SSTs) and the new prefix-extracted
/// bloom are configured. On new DBs, the legacy policy can be dropped, but it would
/// break backwards compatibility and it wouldn't really bring meaningful benefits.
pub fn filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![
        Arc::new(BloomFilterPolicy::new(10)),
        Arc::new(BloomFilterPolicy::new(10).with_prefix_extractor(Arc::new(ZerofsPrefixExtractor))),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::key_codec::KeyCodec;
    use bytes::Bytes;

    fn point(b: Bytes) -> PrefixTarget {
        PrefixTarget::Point(b)
    }
    fn prefix(b: Bytes) -> PrefixTarget {
        PrefixTarget::Prefix(b)
    }

    #[test]
    fn chunk_keys_fall_through() {
        // Chunks are intentionally not prefix-extracted; see module docs.
        let key = KeyCodec::chunk_key(42, 7);
        assert_eq!(ZerofsPrefixExtractor.prefix_len(&point(key)), None);
    }

    #[test]
    fn dir_entry_keys_extract_dir_prefix() {
        let key = KeyCodec::dir_entry_key(7, b"a-very-long-filename");
        assert_eq!(ZerofsPrefixExtractor.prefix_len(&point(key)), Some(9));
    }

    #[test]
    fn dir_scan_keys_extract_dir_prefix() {
        let key = KeyCodec::dir_scan_key(7, 100);
        assert_eq!(ZerofsPrefixExtractor.prefix_len(&point(key)), Some(9));
    }

    #[test]
    fn inode_and_other_kinds_fall_through() {
        // Inode keys are exactly 9 bytes, extracting a 9-byte prefix would be
        // identical to the whole-key hash, so we skip prefix extraction.
        // All non-extracted kinds return None.
        for key in [
            KeyCodec::inode_key(1),
            KeyCodec::dir_cookie_counter_key(1),
            KeyCodec::stats_shard_key(0),
            KeyCodec::system_counter_key(),
            KeyCodec::tombstone_key(123, 1),
        ] {
            assert_eq!(ZerofsPrefixExtractor.prefix_len(&point(key)), None);
        }
    }

    #[test]
    fn prefix_shorter_than_nine_bytes_returns_none() {
        // A scan prefix that hasn't accumulated the full 9-byte (kind, id)
        // span cannot be safely probed — we'd get false negatives. Total
        // length 1..=8 (kind byte + 0..=7 id bytes).
        for kind in [KeyPrefix::DirEntry, KeyPrefix::DirScan] {
            for short_len in 0..8 {
                let mut buf = vec![u8::from(kind); 1];
                buf.extend(std::iter::repeat_n(0u8, short_len));
                assert_eq!(
                    ZerofsPrefixExtractor.prefix_len(&prefix(Bytes::from(buf.clone()))),
                    None,
                    "kind={kind:?} len={}",
                    buf.len()
                );
            }
        }
    }

    #[test]
    fn scan_prefix_dir_scoped_returns_nine() {
        // The exact prefix shape used by dir scans: kind + dir_id_be8.
        let mut buf = vec![u8::from(KeyPrefix::DirScan)];
        buf.extend_from_slice(&42u64.to_be_bytes());
        assert_eq!(
            ZerofsPrefixExtractor.prefix_len(&prefix(Bytes::from(buf))),
            Some(9)
        );
    }

    #[test]
    fn name_is_versioned() {
        // Changing extracted kinds must change the name so SlateDB doesn't
        // confuse old and new filter contents.
        assert_eq!(ZerofsPrefixExtractor.name(), "zerofs_v1");
    }
}
