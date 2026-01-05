use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::Settings;
use crate::db::SlateDbHandle;
use crate::fs::CacheConfig;
use crate::fs::key_codec::KeyPrefix;
use crate::key_management;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use slatedb::BlockTransformer;
use slatedb::config::{DurabilityLevel, ScanOptions};
use slatedb::object_store::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

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
    let (object_store, path_from_url) = parse_url_opts(&url.parse()?, env_vars.into_iter())?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);

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

    let (slatedb, _, _) = super::server::build_slatedb(
        object_store,
        &cache_config,
        actual_db_path,
        super::server::DatabaseMode::ReadWrite,
        settings.lsm,
        false, // don't disable compactor
        block_transformer,
    )
    .await?;

    let db = match slatedb {
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
        cache_blocks: true,
        max_fetch_tasks: 8,
        ..Default::default()
    };

    let mut iter = db.scan_with_options::<&[u8], _>(.., &scan_options).await?;

    let mut count = 0;
    let mut count_by_prefix: std::collections::HashMap<KeyPrefix, usize> =
        std::collections::HashMap::new();

    while let Ok(Some(kv)) = iter.next().await {
        let key = kv.key;

        let prefix = match key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) {
            Some(p) => p,
            None => {
                if key.is_empty() {
                    println!("Empty key found");
                } else {
                    println!("Unknown prefix: 0x{:02x} - Key: {:?}", key[0], key);
                }
                continue;
            }
        };

        *count_by_prefix.entry(prefix).or_insert(0) += 1;

        print!("[{}] ", prefix.as_str());

        match prefix {
            KeyPrefix::Inode if key.len() == 9 => {
                let inode_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                println!("inode_id={}", inode_id);
            }
            KeyPrefix::Chunk if key.len() == 17 => {
                let inode_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                let chunk_index = u64::from_be_bytes(key[9..17].try_into().unwrap());
                println!("inode_id={}, chunk_index={}", inode_id, chunk_index);
            }
            KeyPrefix::DirEntry if key.len() > 9 => {
                let dir_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                let name = String::from_utf8_lossy(&key[9..]);
                println!("dir_id={}, name=\"{}\"", dir_id, name);
            }
            KeyPrefix::DirScan if key.len() > 17 => {
                let dir_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                let entry_id = u64::from_be_bytes(key[9..17].try_into().unwrap());
                let name = String::from_utf8_lossy(&key[17..]);
                println!(
                    "dir_id={}, entry_id={}, name=\"{}\"",
                    dir_id, entry_id, name
                );
            }
            KeyPrefix::Tombstone if key.len() == 17 => {
                let timestamp = u64::from_be_bytes(key[1..9].try_into().unwrap());
                let inode_id = u64::from_be_bytes(key[9..17].try_into().unwrap());
                println!("timestamp={}, inode_id={}", timestamp, inode_id);
            }
            KeyPrefix::Stats if key.len() == 9 => {
                let shard_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                println!("shard_id={}", shard_id);
            }
            KeyPrefix::System => {
                println!("subtype=0x{:02x}", key.get(1).unwrap_or(&0));
            }
            _ => {
                println!("raw={:?}", key);
            }
        }

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
