//! ZeroFS keys come in two on-disk layouts (see `fs::key_codec`):
//!
//!   v1 (legacy):    `[kind: 1] + [id: 8]` + ...
//!   v2 (segmented): `[b"meta" | b"chunk"] + [kind: 1] + [id: 8]` + ...
//!
//! Both extractors are configured side-by-side so each SST is filterable
//! regardless of which layout produced it. At write time each extractor
//! ignores keys from the other layout (returning `None`), so the unused
//! filter for that SST stays empty.
//!
//! For directory kinds (`DirEntry`, `DirScan`), grouping the bloom on the
//! `[kind, id]` portion lets a single SST be skipped for an entire
//! directory on `readdir`.
//!
//! Chunks are deliberately NOT extracted: `scan_prefix(chunk_inode_prefix)`
//! would expand the SST set considered (every SST holding any chunk of the
//! inode), losing more on iterator init than the filter can save. The
//! narrow `scan(chunk(id, start)..chunk(id, end+1))` is already optimal.

use slatedb::filter_policy::{BloomFilterPolicy, FilterPolicy};
use slatedb::prefix_extractor::{PrefixExtractor, PrefixTarget};
use std::sync::Arc;

use super::key_codec::{KeyPrefix, META_DOMAIN};

/// 1-byte kind tag + 8-byte BE id.
const ENTITY_PREFIX_LEN_V1: usize = 9;
/// v2 layout adds the leading `b"meta"` (or `b"chunk"`) domain bytes ahead
/// of the kind byte. Only metadata kinds are extracted, so the relevant
/// length is `META_DOMAIN.len() + 1 + 8`.
const ENTITY_PREFIX_LEN_V2: usize = META_DOMAIN.len() + ENTITY_PREFIX_LEN_V1;

/// Versioned name so that future tweaks to which kinds are extracted produce a
/// distinct policy name and old SSTs aren't silently misread.
const EXTRACTOR_NAME_V1: &str = "zerofs_v1";
const EXTRACTOR_NAME_V2: &str = "zerofs_v2";

fn extracts_kind(byte: u8) -> bool {
    matches!(
        KeyPrefix::try_from(byte),
        Ok(KeyPrefix::DirEntry | KeyPrefix::DirScan)
    )
}

/// Filter prefix extractor for the v1 key layout.
pub struct ZerofsPrefixExtractor;

impl PrefixExtractor for ZerofsPrefixExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME_V1
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
        };
        if bytes.len() < ENTITY_PREFIX_LEN_V1 {
            return None;
        }
        if !extracts_kind(bytes[0]) {
            return None;
        }
        Some(ENTITY_PREFIX_LEN_V1)
    }
}

/// Filter prefix extractor for the v2 (segmented) key layout. Keys start
/// with `b"meta"` or `b"chunk"`; only the metadata directory kinds get
/// prefix-extracted, so the chunk domain is ignored entirely.
pub struct ZerofsPrefixExtractorV2;

impl PrefixExtractor for ZerofsPrefixExtractorV2 {
    fn name(&self) -> &str {
        EXTRACTOR_NAME_V2
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
        };
        if bytes.len() < ENTITY_PREFIX_LEN_V2 {
            return None;
        }
        // Chunk domain isn't prefix-extracted (per the module comment), so
        // we only branch on `meta`.
        if !bytes.starts_with(META_DOMAIN) {
            return None;
        }
        let kind_byte = bytes[META_DOMAIN.len()];
        if !extracts_kind(kind_byte) {
            return None;
        }
        Some(ENTITY_PREFIX_LEN_V2)
    }
}

/// Bloom filter policies for a given on-disk key layout.
///
/// Each volume is either v1 or v2 for its entire lifetime (decided at
/// creation time, see `should_enable_segments`), so only the matching
/// prefix extractor will ever match its keys — registering the other
/// would just build empty filter blobs on every SST.
///
/// The legacy whole-key bloom (the SlateDB default) is always registered
/// alongside, since it accelerates point lookups regardless of layout.
pub fn filter_policies(use_segment_layout: bool) -> Vec<Arc<dyn FilterPolicy>> {
    let prefix_extractor: Arc<dyn PrefixExtractor> = if use_segment_layout {
        Arc::new(ZerofsPrefixExtractorV2)
    } else {
        Arc::new(ZerofsPrefixExtractor)
    };
    vec![
        Arc::new(BloomFilterPolicy::new(10)),
        Arc::new(BloomFilterPolicy::new(10).with_prefix_extractor(prefix_extractor)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::key_codec::KeyCodec;
    use bytes::Bytes;

    fn v1() -> KeyCodec {
        KeyCodec::new(false)
    }
    fn v2() -> KeyCodec {
        KeyCodec::new(true)
    }

    fn extract(p: &dyn PrefixExtractor, key: Bytes) -> Option<usize> {
        p.prefix_len(&PrefixTarget::Point(key))
    }

    #[test]
    fn v1_extracts_dir_kinds() {
        let p = ZerofsPrefixExtractor;
        let codec = v1();
        assert_eq!(
            extract(&p, codec.dir_entry_key(7, b"a-very-long-filename")),
            Some(ENTITY_PREFIX_LEN_V1)
        );
        assert_eq!(
            extract(&p, codec.dir_scan_key(7, 100)),
            Some(ENTITY_PREFIX_LEN_V1)
        );
    }

    #[test]
    fn v1_skips_non_dir_kinds() {
        let p = ZerofsPrefixExtractor;
        let codec = v1();
        for k in [
            codec.inode_key(1),
            codec.dir_cookie_counter_key(1),
            codec.stats_shard_key(0),
            codec.system_counter_key(),
            codec.tombstone_key(123, 1),
            codec.chunk_key(42, 7),
        ] {
            assert_eq!(extract(&p, k), None);
        }
    }

    #[test]
    fn v2_extracts_dir_kinds() {
        let p = ZerofsPrefixExtractorV2;
        let codec = v2();
        assert_eq!(
            extract(&p, codec.dir_entry_key(7, b"a-very-long-filename")),
            Some(ENTITY_PREFIX_LEN_V2)
        );
        assert_eq!(
            extract(&p, codec.dir_scan_key(7, 100)),
            Some(ENTITY_PREFIX_LEN_V2)
        );
    }

    #[test]
    fn v2_skips_other_kinds_and_chunks() {
        let p = ZerofsPrefixExtractorV2;
        let codec = v2();
        for k in [
            codec.inode_key(1),
            codec.dir_cookie_counter_key(1),
            codec.stats_shard_key(0),
            codec.system_counter_key(),
            codec.tombstone_key(123, 1),
            codec.chunk_key(42, 7),
        ] {
            assert_eq!(extract(&p, k), None);
        }
    }

    #[test]
    fn cross_layout_extractors_skip_each_other() {
        let v1_p = ZerofsPrefixExtractor;
        let v2_p = ZerofsPrefixExtractorV2;
        let v1_dir = v1().dir_entry_key(7, b"name");
        let v2_dir = v2().dir_entry_key(7, b"name");

        // v1 extractor sees v2-layout keys starting with `m` (0x6D), not a
        // KeyPrefix value, and returns None.
        assert_eq!(extract(&v1_p, v2_dir.clone()), None);
        // v2 extractor sees v1-layout keys that don't start with b"meta"
        // and returns None.
        assert_eq!(extract(&v2_p, v1_dir.clone()), None);
    }
}
