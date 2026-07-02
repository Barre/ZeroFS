use slatedb::{PrefixExtractor, PrefixTarget};

use crate::fs::key_codec::{EXTENT_DOMAIN, META_DOMAIN};

/// Routes keys into two slatedb segments by the leading domain bytes
/// `KeyCodec` prepends: `b"meta"` for any metadata kind, `b"extent"` for bulk
/// data. The kind byte sits inside the domain segment, preserving keyspace
/// ordering within each domain. The two prefixes are disjoint with no
/// proper-prefix relation, satisfying SlateDB's antichain invariant.
pub struct ZeroFsSegmentExtractor;

/// Persisted name. Stamped onto the manifest at first creation; checked on
/// every reopen.
pub const EXTRACTOR_NAME: &str = "zerofs-meta-extent-v1";

impl PrefixExtractor for ZeroFsSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes = match target {
            PrefixTarget::Point(k) => k,
            PrefixTarget::Prefix(p) => p,
        };
        if bytes.starts_with(META_DOMAIN) {
            Some(META_DOMAIN.len())
        } else if bytes.starts_with(EXTENT_DOMAIN) {
            Some(EXTENT_DOMAIN.len())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::key_codec::KeyCodec;
    use bytes::Bytes;

    #[test]
    fn extractor_name_is_stable() {
        // The name is persisted into the manifest and checked on every reopen, so a
        // change here would refuse to open existing volumes.
        assert_eq!(ZeroFsSegmentExtractor.name(), EXTRACTOR_NAME);
        assert_eq!(EXTRACTOR_NAME, "zerofs-meta-extent-v1");
    }

    #[test]
    fn extractor_routes_meta_and_extent_domains() {
        let ex = ZeroFsSegmentExtractor;
        let meta = Bytes::from_static(b"metaXYZ");
        let extent = Bytes::from_static(b"extentXYZ");

        // Point and Prefix targets route identically.
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(meta.clone())),
            Some(META_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Prefix(meta)),
            Some(META_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(extent.clone())),
            Some(EXTENT_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Prefix(extent)),
            Some(EXTENT_DOMAIN.len())
        );

        // A key in neither domain routes nowhere.
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(Bytes::from_static(b"\x01other"))),
            None
        );
        assert_eq!(ex.prefix_len(&PrefixTarget::Point(Bytes::new())), None);
    }

    // The extractor must agree with the KeyCodec: real metadata keys land in the
    // meta segment and real extent keys in the extent segment, the isolation the whole
    // segmented layout depends on.
    #[test]
    fn extractor_matches_real_keys() {
        let ex = ZeroFsSegmentExtractor;
        let codec = KeyCodec::new();
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(codec.inode_key(1))),
            Some(META_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(codec.tombstone_key(0, 1))),
            Some(META_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(codec.extent_key(1, 0))),
            Some(EXTENT_DOMAIN.len())
        );
    }
}
