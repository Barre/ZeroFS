use anyhow::Result;
use slatedb::admin::AdminBuilder;
use slatedb::object_store::{ObjectStore, path::Path};
use slatedb::{PrefixExtractor, PrefixTarget};
use std::sync::Arc;

use crate::fs::key_codec::{CHUNK_DOMAIN, META_DOMAIN};

/// Segment extractor that routes keys into two slatedb segments:
/// `b"meta"` for any metadata kind (`KeyPrefix::Inode..=Tombstone`) and
/// `b"chunk"` for bulk data.
///
/// Routing is determined by the leading domain bytes that `KeyCodec`
/// prepends in the v2 layout see `fs::key_codec`. The kind byte sits
/// inside the domain segment so the original keyspace ordering within
/// each domain is preserved.
///
/// `b"meta"` and `b"chunk"` are disjoint with no proper-prefix relation,
/// so the antichain invariant SlateDB requires on segment prefixes holds.
pub struct ZeroFsSegmentExtractor;

/// Persisted name. Stamped onto the manifest at first creation; checked on
/// every reopen.
pub const EXTRACTOR_NAME: &str = "zerofs-meta-chunk-v1";

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
        } else if bytes.starts_with(CHUNK_DOMAIN) {
            Some(CHUNK_DOMAIN.len())
        } else {
            None
        }
    }
}

/// Decide whether to pass the extractor to `DbBuilder` for this open.
///
/// Returns `true` when the volume is fresh (no manifest yet) or when the
/// existing manifest was already created with a segment extractor; in the
/// latter case slatedb's own name-match check will reject a mismatch.
/// Returns `false` for pre-segmentation legacy volumes so they keep opening
/// unchanged.
pub async fn should_enable_segments(
    main_object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    wal_object_store: Option<&Arc<dyn ObjectStore>>,
) -> Result<bool> {
    let mut admin_builder = AdminBuilder::new(db_path.clone(), main_object_store.clone());
    if let Some(wal) = wal_object_store {
        admin_builder = admin_builder.with_wal_object_store(wal.clone());
    }
    let admin = admin_builder.build();
    match admin
        .read_manifest(None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read manifest: {}", e))?
    {
        None => Ok(true),
        Some(manifest) => Ok(manifest.segment_extractor_name().is_some()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::key_codec::KeyCodec;
    use bytes::Bytes;
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;

    #[test]
    fn extractor_name_is_stable() {
        // The name is persisted into the manifest and checked on every reopen, so a
        // change here would refuse to open existing volumes.
        assert_eq!(ZeroFsSegmentExtractor.name(), EXTRACTOR_NAME);
        assert_eq!(EXTRACTOR_NAME, "zerofs-meta-chunk-v1");
    }

    #[test]
    fn extractor_routes_meta_and_chunk_domains() {
        let ex = ZeroFsSegmentExtractor;
        let meta = Bytes::from_static(b"metaXYZ");
        let chunk = Bytes::from_static(b"chunkXYZ");

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
            ex.prefix_len(&PrefixTarget::Point(chunk.clone())),
            Some(CHUNK_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Prefix(chunk)),
            Some(CHUNK_DOMAIN.len())
        );

        // A key in neither domain (a v1/legacy key, or empty) routes nowhere.
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(Bytes::from_static(b"\x01legacy"))),
            None
        );
        assert_eq!(ex.prefix_len(&PrefixTarget::Point(Bytes::new())), None);
    }

    // The extractor must agree with the v2 KeyCodec: real metadata keys land in the
    // meta segment and real chunk keys in the chunk segment, the isolation the whole
    // segmented layout depends on.
    #[test]
    fn extractor_matches_real_v2_keys() {
        let ex = ZeroFsSegmentExtractor;
        let codec = KeyCodec::new(true);
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(codec.inode_key(1))),
            Some(META_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(codec.tombstone_key(0, 1))),
            Some(META_DOMAIN.len())
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(codec.chunk_key(1, 0))),
            Some(CHUNK_DOMAIN.len())
        );
    }

    #[tokio::test]
    async fn should_enable_segments_is_true_for_a_fresh_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            should_enable_segments(&store, &Path::from("fresh"), None)
                .await
                .unwrap(),
            "a store with no manifest yet must enable segments"
        );
    }

    #[tokio::test]
    async fn should_enable_segments_is_true_for_a_db_built_with_the_extractor() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("segmented");
        let db = DbBuilder::new(path.clone(), store.clone())
            .with_segment_extractor(Arc::new(ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        db.close().await.unwrap();

        assert!(should_enable_segments(&store, &path, None).await.unwrap());
    }

    #[tokio::test]
    async fn should_enable_segments_is_false_for_a_legacy_db() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("legacy");
        let db = DbBuilder::new(path.clone(), store.clone())
            .build()
            .await
            .unwrap();
        db.close().await.unwrap();

        assert!(
            !should_enable_segments(&store, &path, None).await.unwrap(),
            "a pre-segmentation volume must keep opening without segments"
        );
    }
}
