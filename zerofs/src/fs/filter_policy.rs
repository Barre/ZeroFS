//! ZeroFS keys are `[b"meta" | b"extent"] + [kind: 1] + [id: 8] + ...` (see
//! `fs::key_codec`). This registers the bloom filter policies for that layout.
//!
//! For directory kinds (`DirEntry`, `DirScan`), grouping the bloom on the
//! `[domain, kind, id]` portion lets a single SST be skipped for an entire
//! directory on `readdir`.
//!
//! Extents are deliberately not extracted: `scan_prefix(extent_inode_prefix)`
//! would expand the SST set considered (every SST holding any extent of the
//! inode), losing more on iterator init than the filter can save. The narrow
//! `scan(extent(id, start)..extent(id, end+1))` is already optimal.

use slatedb::filter_policy::{BloomFilterPolicy, FilterPolicy};
use slatedb::prefix_extractor::{PrefixExtractor, PrefixTarget};
use std::sync::Arc;

use super::key_codec::{KeyPrefix, META_DOMAIN};

/// Leading `b"meta"` domain + 1-byte kind + 8-byte BE id. Only metadata
/// directory kinds are extracted, so this is anchored on the meta domain.
const ENTITY_PREFIX_LEN: usize = META_DOMAIN.len() + 1 + 8;

/// Versioned name so future tweaks to which kinds are extracted produce a
/// distinct policy name and old SSTs aren't silently misread.
const EXTRACTOR_NAME: &str = "zerofs_v2";

fn extracts_kind(byte: u8) -> bool {
    matches!(
        KeyPrefix::try_from(byte),
        Ok(KeyPrefix::DirEntry | KeyPrefix::DirScan)
    )
}

/// Filter prefix extractor for the segmented key layout. Keys start with
/// `b"meta"` or `b"extent"`; only the metadata directory kinds get
/// prefix-extracted, so the extent domain is ignored entirely.
pub struct ZerofsPrefixExtractor;

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
        // The extent domain isn't prefix-extracted (per the module comment).
        if !bytes.starts_with(META_DOMAIN) {
            return None;
        }
        let kind_byte = bytes[META_DOMAIN.len()];
        if !extracts_kind(kind_byte) {
            return None;
        }
        Some(ENTITY_PREFIX_LEN)
    }
}

/// Bloom filter policies for the on-disk key layout: the whole-key bloom (the
/// SlateDB default, accelerates point lookups) plus a prefix bloom on
/// `[domain, kind, id]` for directory-kind SST skipping.
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

    fn extract(p: &dyn PrefixExtractor, key: Bytes) -> Option<usize> {
        p.prefix_len(&PrefixTarget::Point(key))
    }

    #[test]
    fn extracts_dir_kinds() {
        let p = ZerofsPrefixExtractor;
        let codec = KeyCodec::new();
        assert_eq!(
            extract(&p, codec.dir_entry_key(7, b"a-very-long-filename")),
            Some(ENTITY_PREFIX_LEN)
        );
        assert_eq!(
            extract(&p, codec.dir_scan_key(7, 100)),
            Some(ENTITY_PREFIX_LEN)
        );
    }

    #[test]
    fn skips_other_kinds_and_extents() {
        let p = ZerofsPrefixExtractor;
        let codec = KeyCodec::new();
        for k in [
            codec.inode_key(1),
            codec.dir_cookie_counter_key(1),
            codec.stats_shard_key(0),
            codec.system_counter_key(),
            codec.tombstone_key(123, 1),
            codec.extent_key(42, 7),
        ] {
            assert_eq!(extract(&p, k), None);
        }
    }
}
