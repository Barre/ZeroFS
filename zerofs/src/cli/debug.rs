use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::Settings;
use crate::db::SlateDbHandle;
use crate::fs::CacheConfig;
use crate::fs::key_codec::{EXTENT_DOMAIN, KeyCodec, KeyPrefix, META_DOMAIN, ParsedKey};
use crate::key_management;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use slatedb::BlockTransformer;
use slatedb::config::{DurabilityLevel, ScanOptions};
use slatedb::object_store::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

const U64_SIZE: usize = std::mem::size_of::<u64>();

/// Classify one raw db key the same way the codec routes it: leading
/// `META_DOMAIN`/`EXTENT_DOMAIN` prefix first, then the kind byte. Returns
/// the kind plus a rendered payload, or `None` for keys with no recognized
/// domain + kind prefix (kind Extent is only valid in the extent domain,
/// every other kind only in the meta domain).
fn describe_key(codec: &KeyCodec, key: &[u8]) -> Option<(KeyPrefix, String)> {
    let (in_extent_domain, rest) = if let Some(rest) = key.strip_prefix(EXTENT_DOMAIN) {
        (true, rest)
    } else if let Some(rest) = key.strip_prefix(META_DOMAIN) {
        (false, rest)
    } else {
        return None;
    };
    let prefix = KeyPrefix::try_from(*rest.first()?).ok()?;
    if (prefix == KeyPrefix::Extent) != in_extent_domain {
        return None;
    }
    let detail = decode_payload(codec, prefix, key).unwrap_or_else(|| format!("raw={:?}", key));
    Some((prefix, detail))
}

/// Render the id portion of a classified key. `None` means the domain + kind
/// prefix is valid but the payload is malformed for that kind, and the caller
/// falls back to printing the raw bytes.
fn decode_payload(codec: &KeyCodec, prefix: KeyPrefix, key: &[u8]) -> Option<String> {
    let id_off = codec.id_offset(prefix);
    let id_u64 = |off: usize| {
        Some(u64::from_be_bytes(
            key.get(off..off + U64_SIZE)?.try_into().ok()?,
        ))
    };
    match prefix {
        KeyPrefix::Inode if key.len() == codec.inode_key_size() => {
            Some(format!("inode_id={}", id_u64(id_off)?))
        }
        KeyPrefix::Extent => codec
            .parse_extent_key_full(key)
            .map(|(inode_id, extent_index)| {
                format!("inode_id={}, extent_index={}", inode_id, extent_index)
            }),
        KeyPrefix::DirEntry if key.len() > id_off + U64_SIZE => {
            let name = String::from_utf8_lossy(&key[id_off + U64_SIZE..]);
            Some(format!("dir_id={}, name=\"{}\"", id_u64(id_off)?, name))
        }
        KeyPrefix::DirScan => match codec.parse_key(key) {
            ParsedKey::DirScan { cookie } => {
                Some(format!("dir_id={}, cookie={}", id_u64(id_off)?, cookie))
            }
            _ => None,
        },
        KeyPrefix::DirCookie if key.len() == id_off + U64_SIZE => {
            Some(format!("dir_id={}", id_u64(id_off)?))
        }
        KeyPrefix::Tombstone => match codec.parse_key(key) {
            ParsedKey::Tombstone { inode_id } => Some(format!(
                "timestamp={}, inode_id={}",
                id_u64(id_off)?,
                inode_id
            )),
            _ => None,
        },
        KeyPrefix::Orphan => match codec.parse_key(key) {
            ParsedKey::Orphan { inode_id } => Some(format!("inode_id={}", inode_id)),
            _ => None,
        },
        KeyPrefix::Stats if key.len() == id_off + U64_SIZE => {
            Some(format!("shard_id={}", id_u64(id_off)?))
        }
        KeyPrefix::System => Some(format!(
            "subtype=0x{:02x}",
            key.get(id_off).copied().unwrap_or(0)
        )),
        KeyPrefix::SegCount => codec
            .parse_segcount_key(key)
            .map(|(epoch, counter)| format!("epoch={}, counter={}", epoch, counter)),
        _ => None,
    }
}

pub async fn list_keys(config_path: PathBuf) -> Result<()> {
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();

    let cache_config = CacheConfig {
        root_folder: settings.cache.dir.clone(),
        max_cache_size_gb: settings.cache.disk_size_gb,
        memory_cache_size_gb: settings.cache.memory_size_gb,
    };

    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(&url.parse()?, env_vars)?;

    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );

    let actual_db_path = path_from_url.to_string();

    let bucket =
        crate::bucket_identity::BucketIdentity::get_or_create(&object_store, &actual_db_path)
            .await?;

    let cache_config = CacheConfig {
        root_folder: cache_config.root_folder.join(bucket.cache_directory_name()),
        ..cache_config
    };

    let password = settings.storage.encryption_password.clone();

    crate::cli::password::validate_password(&password)
        .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

    let db_path = Path::from(actual_db_path.clone());
    let encryption_key =
        key_management::load_or_init_encryption_key(&object_store, &db_path, &password, false)
            .await?;

    let block_transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&encryption_key, settings.compression());

    let wal_object_store: Option<Arc<dyn object_store::ObjectStore>> =
        if let Some(wal_config) = &settings.wal {
            Some(super::server::parse_wal_object_store(wal_config)?)
        } else {
            None
        };

    let opened = super::server::build_slatedb(
        object_store,
        &cache_config,
        actual_db_path,
        super::server::DatabaseMode::ReadWrite,
        settings.lsm,
        block_transformer,
        wal_object_store,
        None, // debug command never participates in replication
    )
    .await?;

    let db = match opened.data {
        SlateDbHandle::ReadWrite(db) => db,
        SlateDbHandle::ReadOnly(_) => {
            return Err(anyhow::anyhow!(
                "Expected read-write mode for debug command"
            ));
        }
    };

    println!("Scanning all keys in the database...\n");

    let scan_options = ScanOptions {
        durability_filter: DurabilityLevel::Memory,
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: false,
        max_fetch_tasks: 4,
        ..Default::default()
    };

    let mut iter = db.scan_with_options(.., &scan_options).await?;

    let codec = KeyCodec::new();
    let mut count = 0;
    let mut count_by_prefix: std::collections::HashMap<KeyPrefix, usize> =
        std::collections::HashMap::new();

    loop {
        let kv = match iter.next().await {
            Ok(Some(kv)) => kv,
            Ok(None) => break,
            Err(e) => anyhow::bail!("dump scan failed after {count} keys: {e}"),
        };
        let key = kv.key;

        let (prefix, detail) = match describe_key(&codec, &key) {
            Some(described) => described,
            None => {
                if key.is_empty() {
                    println!("Empty key found");
                } else {
                    println!("Unknown key: {:?}", key);
                }
                continue;
            }
        };

        *count_by_prefix.entry(prefix).or_insert(0) += 1;

        println!("[{}] {}", prefix.as_str(), detail);

        count += 1;
    }

    println!("\n=== Summary ===");
    println!("Total keys: {}", count);
    println!("\nKeys by type:");

    let mut prefix_counts: Vec<_> = count_by_prefix.iter().collect();
    prefix_counts.sort_by_key(|(prefix, _)| u8::from(**prefix));

    for (prefix, count) in prefix_counts {
        println!("  {}: {}", prefix.as_str(), count);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every key kind the filesystem writes must classify under its KeyPrefix
    // and decode its id payload; keys are built via KeyCodec so this breaks
    // if the parser ever drifts from the codec's segmented layout again.
    #[test]
    fn test_describe_key_decodes_every_kind() {
        let codec = KeyCodec::new();
        let cases: Vec<(bytes::Bytes, KeyPrefix, &str)> = vec![
            (codec.inode_key(42), KeyPrefix::Inode, "inode_id=42"),
            (
                codec.extent_key(7, 99),
                KeyPrefix::Extent,
                "inode_id=7, extent_index=99",
            ),
            (
                codec.dir_entry_key(3, b"hello.txt"),
                KeyPrefix::DirEntry,
                "dir_id=3, name=\"hello.txt\"",
            ),
            (
                codec.dir_scan_key(3, 12),
                KeyPrefix::DirScan,
                "dir_id=3, cookie=12",
            ),
            (
                codec.dir_cookie_counter_key(3),
                KeyPrefix::DirCookie,
                "dir_id=3",
            ),
            (
                codec.tombstone_key(1111, 5),
                KeyPrefix::Tombstone,
                "timestamp=1111, inode_id=5",
            ),
            (codec.orphan_key(8), KeyPrefix::Orphan, "inode_id=8"),
            (codec.stats_shard_key(2), KeyPrefix::Stats, "shard_id=2"),
            (
                codec.system_counter_key(),
                KeyPrefix::System,
                "subtype=0x01",
            ),
            (
                codec.segcount_key(4, 17),
                KeyPrefix::SegCount,
                "epoch=4, counter=17",
            ),
        ];
        for (key, want_prefix, want_detail) in cases {
            let (prefix, detail) = describe_key(&codec, &key)
                .unwrap_or_else(|| panic!("key {:?} not recognized", key));
            assert_eq!(prefix, want_prefix, "kind for {:?}", key);
            assert_eq!(detail, want_detail, "payload for {:?}", key);
        }
    }

    #[test]
    fn test_describe_key_rejects_undecodable_keys() {
        let codec = KeyCodec::new();
        // Empty, no domain prefix, and the pre-segment layout (bare kind byte).
        assert!(describe_key(&codec, b"").is_none());
        assert!(describe_key(&codec, b"garbage").is_none());
        assert!(describe_key(&codec, &[0x01, 0, 0, 0, 0, 0, 0, 0, 42]).is_none());
        // An extent kind byte under the meta domain is misrouted, not a kind.
        let mut misrouted = META_DOMAIN.to_vec();
        misrouted.push(u8::from(KeyPrefix::Extent));
        misrouted.extend_from_slice(&[0; U64_SIZE * 2]);
        assert!(describe_key(&codec, &misrouted).is_none());
        // A recognized kind with a truncated payload still classifies (and
        // counts) but falls back to the raw rendering instead of misdecoding.
        let inode_key = codec.inode_key(1);
        let truncated = &inode_key[..inode_key.len() - 1];
        let (prefix, detail) = describe_key(&codec, truncated).unwrap();
        assert_eq!(prefix, KeyPrefix::Inode);
        assert!(detail.starts_with("raw="));
    }
}
