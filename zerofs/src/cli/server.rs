use crate::bucket_identity;
use crate::config::{NbdConfig, NfsConfig, NinePConfig, Settings};
use crate::encryption::SlateDbHandle;
use crate::fs::permissions::Credentials;
use crate::fs::types::SetAttributes;
use crate::fs::{CacheConfig, ZeroFS};
use crate::ha::{start_health_server, LeaseCoordinator, HAState};
use crate::key_management;
use crate::nbd::NBDServer;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use slatedb::DbBuilder;
use slatedb::DbReader;
use slatedb::admin::AdminBuilder;
use slatedb::config::CheckpointOptions;
use slatedb::config::DbReaderOptions;
use slatedb::config::GarbageCollectorDirectoryOptions;
use slatedb::config::{GarbageCollectorOptions, ObjectStoreCacheOptions};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::object_store::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const CHECKPOINT_REFRESH_INTERVAL_SECS: u64 = 10;

async fn start_wal_replication(
    _settings: &Settings,
    ha_config: &crate::config::HAConfig,
    wal_config: &crate::config::WALReplicationConfig,
    _fs: Arc<ZeroFS>,
    _read_only: bool,
) -> Result<(Option<JoinHandle<Result<(), std::io::Error>>>, Option<Arc<crate::ha::WALStreamer>>)> {
    use crate::ha::{
        ReplicationProtocol, WALReceiver, WALReceiverConfig, WALStreamer, WALStreamerConfig,
    };
    use std::time::Duration;

    // ALWAYS start WAL receiver on all nodes (they all need to listen for incoming WAL entries)
    info!("🔄 Starting WAL receiver on {}", wal_config.listen);

    let receiver_config = WALReceiverConfig {
        node_id: ha_config.node_id.clone(),
        listen_address: wal_config.listen.clone(),
        wal_dir: PathBuf::from(format!("/tmp/zerofs-wal-{}", ha_config.node_id)),
    };

    let receiver = WALReceiver::new(receiver_config).await?;
    let receiver_handle = tokio::spawn(async move {
        if let Err(e) = receiver.start().await {
            tracing::error!("WAL receiver error: {}", e);
            return Err(std::io::Error::other(e.to_string()));
        }
        Ok(())
    });

    // Create WAL streamer ONLY if standby_nodes are configured (this node might become active)
    // NOTE: We create it but DON'T attach it yet - attachment happens during promotion to active
    let wal_streamer = if !wal_config.standby_nodes.is_empty() {
        info!(
            "🔄 Creating WAL streamer for {} standby node(s)",
            wal_config.standby_nodes.len()
        );

        let protocol = match wal_config.default_protocol.as_str() {
            "sync" => ReplicationProtocol::Sync,
            _ => ReplicationProtocol::Async,
        };

        let ack_policy = match wal_config.sync_ack_policy.as_str() {
            "majority" => crate::ha::AckPolicy::Majority,
            "all" => crate::ha::AckPolicy::All,
            _ => crate::ha::AckPolicy::One,
        };

        let streamer_config = WALStreamerConfig {
            node_id: ha_config.node_id.clone(),
            standby_addresses: wal_config.standby_nodes.clone(),
            default_protocol: protocol,
            sync_ack_policy: ack_policy,
            sync_timeout: Duration::from_millis(wal_config.sync_timeout_ms),
            tcp_nodelay: wal_config.tcp_nodelay,
            reconnect_interval: Duration::from_secs(wal_config.reconnect_interval_secs),
        };

        let streamer = WALStreamer::new(streamer_config).await?;

        info!(
            "✓ WAL streamer ready (protocol: {}, standbys: {})",
            protocol,
            streamer.connected_standbys().await
        );
        info!("⏸️  WAL streamer will be attached during promotion to ACTIVE");

        Some(streamer)
    } else {
        None
    };

    Ok((Some(receiver_handle), wal_streamer))
}

async fn start_nfs_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NfsConfig>,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    for addr in &config.addresses {
        info!("Starting NFS server on {}", addr);
        let fs_clone = Arc::clone(&fs);
        let addr = *addr;
        handles.push(tokio::spawn(async move {
            match crate::nfs::start_nfs_server_with_config(fs_clone, addr).await {
                Ok(()) => Ok(()),
                Err(e) => Err(std::io::Error::other(e.to_string())),
            }
        }));
    }

    handles
}

async fn start_ninep_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NinePConfig>,
) -> Result<Vec<JoinHandle<Result<(), std::io::Error>>>> {
    let config = match config {
        Some(c) => c,
        None => return Ok(Vec::new()),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!("Starting 9P server on {}", addr);
            let ninep_tcp_server = crate::ninep::NinePServer::new(Arc::clone(&fs), *addr);
            handles.push(tokio::spawn(async move { ninep_tcp_server.start().await }));
        }
    }

    if let Some(socket_path) = config.unix_socket.as_ref() {
        info!(
            "Starting 9P server on Unix socket: {}",
            socket_path.display()
        );
        let ninep_unix_fs = Arc::clone(&fs);
        let ninep_unix_server =
            crate::ninep::NinePServer::new_unix(ninep_unix_fs, socket_path.clone());
        handles.push(tokio::spawn(async move { ninep_unix_server.start().await }));
    }

    Ok(handles)
}

async fn ensure_nbd_directory(fs: &Arc<ZeroFS>) -> Result<()> {
    let creds = Credentials {
        uid: 0,
        gid: 0,
        groups: [0; 16],
        groups_count: 1,
    };
    let nbd_name = b".nbd";

    match fs.process_lookup(&creds, 0, nbd_name).await {
        Ok(_) => info!(".nbd directory already exists"),
        Err(_) => {
            let attr = SetAttributes {
                mode: crate::fs::types::SetMode::Set(0o755),
                uid: crate::fs::types::SetUid::Set(0),
                gid: crate::fs::types::SetGid::Set(0),
                ..Default::default()
            };
            fs.process_mkdir(&creds, 0, nbd_name, &attr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create .nbd directory: {e:?}"))?;
            info!("Created .nbd directory for NBD device management");
        }
    }
    Ok(())
}

async fn start_nbd_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NbdConfig>,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!(
                "Starting NBD server on {} (devices dynamically discovered from .nbd/)",
                addr
            );
            let nbd_tcp_server = NBDServer::new_tcp(Arc::clone(&fs), *addr);
            handles.push(tokio::spawn(async move {
                if let Err(e) = nbd_tcp_server.start().await {
                    Err(e)
                } else {
                    Ok(())
                }
            }));
        }
    }

    if let Some(socket_path) = config.unix_socket.as_ref() {
        info!(
            "Starting NBD server on Unix socket {} (devices dynamically discovered from .nbd/)",
            socket_path.display()
        );
        let nbd_unix_server =
            NBDServer::new_unix(Arc::clone(&fs), socket_path.to_str().unwrap().to_string());
        handles.push(tokio::spawn(async move {
            if let Err(e) = nbd_unix_server.start().await {
                Err(e)
            } else {
                Ok(())
            }
        }));
    }

    handles
}

fn start_garbage_collection(fs: Arc<ZeroFS>) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting garbage collection task (runs continuously)");
        loop {
            if let Err(e) = fs.run_garbage_collection().await {
                tracing::error!("Garbage collection failed: {:?}", e);
            }
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    })
}

fn start_stats_reporting(fs: Arc<ZeroFS>) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting stats reporting task (reports to debug every 5 seconds)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            fs.stats.output_report_debug();
        }
    })
}

pub struct CheckpointRefreshParams {
    pub db_path: String,
    pub object_store: Arc<dyn object_store::ObjectStore>,
}

fn start_checkpoint_refresh(
    params: CheckpointRefreshParams,
    encrypted_db: Arc<crate::encryption::EncryptedDb>,
) -> JoinHandle<()> {
    let db_path = params.db_path;
    let object_store = params.object_store;
    tokio::spawn(async move {
        info!("Starting checkpoint refresh task",);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            CHECKPOINT_REFRESH_INTERVAL_SECS,
        ));

        let db_path = Path::from(db_path);
        let admin = AdminBuilder::new(db_path.clone(), object_store.clone()).build();

        loop {
            interval.tick().await;

            match admin
                .create_detached_checkpoint(&CheckpointOptions {
                    lifetime: Some(std::time::Duration::from_secs(
                        CHECKPOINT_REFRESH_INTERVAL_SECS * 10,
                    )),
                    ..Default::default()
                })
                .await
            {
                Ok(checkpoint_result) => {
                    debug!("Created new checkpoint with ID: {}", checkpoint_result.id);

                    match DbReader::open(
                        db_path.clone(),
                        object_store.clone(),
                        Some(checkpoint_result.id),
                        DbReaderOptions::default(),
                    )
                    .await
                    {
                        Ok(new_reader) => {
                            if let Err(e) = encrypted_db.swap_reader(Arc::new(new_reader)).await {
                                tracing::error!("Failed to swap reader: {:?}", e);
                                continue;
                            }

                            debug!("Successfully refreshed reader");
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to create new DbReader with checkpoint: {:?}",
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to create checkpoint: {:?}", e);
                }
            }
        }
    })
}

pub async fn build_slatedb(
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_config: &CacheConfig,
    db_path: String,
    read_only: bool,
    lsm_config: Option<crate::config::LsmConfig>,
) -> Result<(SlateDbHandle, Option<CheckpointRefreshParams>)> {
    let total_disk_cache_gb = cache_config.max_cache_size_gb;
    let total_memory_cache_gb = cache_config.memory_cache_size_gb.unwrap_or(0.25);

    info!(
        "Cache allocation - Disk: {:.2}GB, Memory: {:.2}GB",
        total_disk_cache_gb, total_memory_cache_gb,
    );

    let slatedb_object_cache_bytes = (total_disk_cache_gb * 1_000_000_000.0) as usize;
    let slatedb_memory_cache_bytes = (total_memory_cache_gb * 1_000_000_000.0) as u64;

    info!(
        "SlateDB in-memory block cache: {} MB",
        slatedb_memory_cache_bytes / 1_000_000
    );
    let slatedb_cache_dir = format!("{}/slatedb", cache_config.root_folder);

    let l0_max_ssts = lsm_config
        .map(|c| c.l0_max_ssts())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_L0_MAX_SSTS);
    let max_unflushed_bytes = lsm_config
        .map(|c| c.max_unflushed_bytes())
        .unwrap_or_else(|| {
            (crate::config::LsmConfig::DEFAULT_MAX_UNFLUSHED_GB * 1_000_000_000.0) as usize
        });
    let max_concurrent_compactions = lsm_config
        .map(|c| c.max_concurrent_compactions())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS);

    let settings = slatedb::config::Settings {
        l0_max_ssts,
        object_store_cache_options: ObjectStoreCacheOptions {
            root_folder: Some(slatedb_cache_dir.clone().into()),
            max_cache_size_bytes: Some(slatedb_object_cache_bytes),
            cache_puts: true,
            ..Default::default()
        },
        flush_interval: Some(std::time::Duration::from_secs(30)),
        max_unflushed_bytes,
        compactor_options: Some(slatedb::config::CompactorOptions {
            max_concurrent_compactions,
            max_sst_size: 256 * 1024 * 1024,
            ..Default::default()
        }),
        compression_codec: None, // Disable compression - we handle it in encryption layer
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(60 * 60)),
                min_age: Duration::from_secs(60 * 60),
            }),
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(60 * 60)),
                min_age: Duration::from_secs(60 * 60),
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(60 * 60)),
                min_age: Duration::from_secs(60 * 60),
            }),
        }),
        ..Default::default()
    };

    let cache = Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: slatedb_memory_cache_bytes,
        time_to_idle: None,
        time_to_live: None,
    }));

    let db_path = Path::from(db_path);

    // This may look weird, but this is required to not drop the runtime handle from the async context
    let (runtime_handle, _runtime_keeper) = tokio::task::spawn_blocking(|| {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();

        let runtime_keeper = std::thread::spawn(move || {
            runtime.block_on(async { std::future::pending::<()>().await });
        });

        (handle, runtime_keeper)
    })
    .await?;

    if read_only {
        info!("Opening database in read-only mode");

        let admin = AdminBuilder::new(db_path.clone(), object_store.clone()).build();
        let checkpoint_result = admin
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(std::time::Duration::from_secs(
                    CHECKPOINT_REFRESH_INTERVAL_SECS * 10,
                )),
                ..Default::default()
            })
            .await?;

        info!(
            "Created initial checkpoint with ID: {}",
            checkpoint_result.id
        );

        let db_path_str = db_path.to_string();
        let reader = Arc::new(
            DbReader::open(
                db_path,
                object_store.clone(),
                Some(checkpoint_result.id),
                DbReaderOptions::default(),
            )
            .await?,
        );

        let checkpoint_params = CheckpointRefreshParams {
            db_path: db_path_str,
            object_store,
        };

        Ok((
            SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
            Some(checkpoint_params),
        ))
    } else {
        let slatedb = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_gc_runtime(runtime_handle.clone())
                .with_compaction_runtime(runtime_handle.clone())
                .with_memory_cache(cache)
                .build()
                .await?,
        );

        Ok((SlateDbHandle::ReadWrite(slatedb), None))
    }
}

async fn initialize_filesystem(
    settings: &Settings,
    read_only: bool,
) -> Result<(Arc<ZeroFS>, Option<CheckpointRefreshParams>)> {
    let url = settings.storage.url.clone();

    let cache_config = CacheConfig {
        root_folder: settings.cache.dir.to_str().unwrap().to_string(),
        max_cache_size_gb: settings.cache.disk_size_gb,
        memory_cache_size_gb: settings.cache.memory_size_gb,
    };

    let mut env_vars = Vec::new();
    if let Some(aws) = &settings.aws {
        for (k, v) in &aws.0 {
            env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
        }
    }
    if let Some(azure) = &settings.azure {
        for (k, v) in &azure.0 {
            env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
        }
    }
    if let Some(gcp) = &settings.gcp {
        for (k, v) in &gcp.0 {
            env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
        }
    }

    let (object_store, path_from_url) = parse_url_opts(&url.parse()?, env_vars.into_iter())?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);

    let actual_db_path = path_from_url.to_string();

    info!("Starting ZeroFS server with {} backend", object_store);
    info!("DB Path: {}", actual_db_path);
    info!("Base Cache Directory: {}", cache_config.root_folder);
    info!("Cache Size: {} GB", cache_config.max_cache_size_gb);

    info!("Checking bucket identity...");
    let bucket =
        bucket_identity::BucketIdentity::get_or_create(&object_store, &actual_db_path).await?;

    let original_cache_root = cache_config.root_folder.clone();
    let cache_config = CacheConfig {
        root_folder: format!("{}/{}", original_cache_root, bucket.cache_directory_name()),
        ..cache_config
    };

    info!(
        "Bucket ID: {}, Cache directory: {}",
        bucket.id(),
        cache_config.root_folder
    );

    if !read_only {
        // TEMPORARILY DISABLED: Wasabi doesn't support conditional writes
        // In production, you MUST use a storage provider that supports conditional writes
        // (AWS S3, MinIO, Cloudflare R2, etc.) for safe HA operation
        tracing::warn!("⚠️  Storage compatibility check DISABLED - HA may not be safe without conditional write support!");
        // crate::storage_compatibility::check_if_match_support(&object_store, &actual_db_path)
        //     .await?;
    }

    let password = settings.storage.encryption_password.clone();

    super::password::validate_password(&password)
        .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

    info!("Loading or initializing encryption key");

    let (slatedb, checkpoint_params) = build_slatedb(
        object_store,
        &cache_config,
        actual_db_path,
        read_only,
        settings.lsm,
    )
    .await?;

    let encryption_key = key_management::load_or_init_encryption_key(&slatedb, &password).await?;

    let max_bytes = settings
        .filesystem
        .as_ref()
        .map(|fs_config| fs_config.max_bytes())
        .unwrap_or(crate::config::FilesystemConfig::DEFAULT_MAX_BYTES);

    let fs = ZeroFS::new_with_slatedb(slatedb, encryption_key, max_bytes).await?;

    Ok((Arc::new(fs), checkpoint_params))
}

pub async fn run_server(config_path: PathBuf, read_only: bool) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let settings = Settings::from_file(config_path.to_str().unwrap())
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    // If HA is enabled, start in read-only mode initially to prevent write conflicts
    // The lease coordinator will promote to read-write when this node acquires the lease
    let effective_read_only = if let Some(ha_config) = &settings.servers.ha {
        if ha_config.enabled {
            info!("🔒 HA enabled - starting in read-only mode (will promote when lease acquired)");
            true // Force read-only for HA nodes initially
        } else {
            read_only
        }
    } else {
        read_only
    };

    let (fs, checkpoint_params) = initialize_filesystem(&settings, effective_read_only).await?;

    // Start WAL replication if configured and HA is enabled
    let (wal_receiver_handle, wal_streamer) = if let Some(ha_config) = &settings.servers.ha {
        if ha_config.enabled {
            if let Some(wal_config) = &ha_config.wal_replication {
                if wal_config.enabled {
                    start_wal_replication(
                        &settings,
                        ha_config,
                        wal_config,
                        Arc::clone(&fs),
                        read_only,
                    ).await?
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    if !read_only && settings.servers.nbd.is_some() {
        ensure_nbd_directory(&fs).await?;
    }

    let nfs_handles = start_nfs_servers(Arc::clone(&fs), settings.servers.nfs.as_ref()).await;

    let ninep_handles =
        start_ninep_servers(Arc::clone(&fs), settings.servers.ninep.as_ref()).await?;

    let nbd_handles = start_nbd_servers(Arc::clone(&fs), settings.servers.nbd.as_ref()).await;

    let gc_handle = start_garbage_collection(Arc::clone(&fs));
    let stats_handle = start_stats_reporting(Arc::clone(&fs));

    let checkpoint_handle =
        checkpoint_params.map(|params| start_checkpoint_refresh(params, Arc::clone(&fs.db)));

    // Start HA health server if configured
    let ha_health_handle = if let Some(ha_config) = &settings.servers.ha {
        if ha_config.enabled {
            let ha_state = Arc::new(HAState::new());
            
            // Start health server
            let health_port = ha_config.health_port;
            let ha_state_for_health = Arc::clone(&ha_state);
            let health_handle = tokio::spawn(async move {
                if let Err(e) = start_health_server(ha_state_for_health, health_port).await {
                    tracing::error!("Health server error: {}", e);
                }
            });

            // Start lease coordinator if using lease strategy
            if ha_config.strategy == "lease" {
                let mut env_vars = Vec::new();
                if let Some(aws) = &settings.aws {
                    for (k, v) in &aws.0 {
                        env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
                    }
                }
                if let Some(azure) = &settings.azure {
                    for (k, v) in &azure.0 {
                        env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
                    }
                }
                if let Some(gcp) = &settings.gcp {
                    for (k, v) in &gcp.0 {
                        env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
                    }
                }
                let (object_store, path_from_url) = parse_url_opts(
                    &settings.storage.url.parse()?,
                    env_vars.into_iter(),
                )?;
                let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
                let db_path = path_from_url.to_string();

                let lease_config = crate::ha::lease_coordinator::LeaseCoordinatorConfig {
                    node_id: ha_config.node_id.clone(),
                    lease_ttl_secs: ha_config.lease_ttl_secs,
                    renew_interval_secs: ha_config.renew_interval_secs,
                };

                let lease_coordinator = Arc::new(LeaseCoordinator::new(
                    lease_config,
                    object_store.clone(),
                    db_path.clone(),
                    (*ha_state).clone(),
                ));

                // Set up promotion/demotion callbacks
                let fs_clone = Arc::clone(&fs);
                let object_store_for_promotion = object_store.clone();
                let db_path_for_promotion = db_path.clone();
                let settings_for_promotion = settings.clone();
                let wal_streamer_for_promotion = wal_streamer.clone();
                
                // Get cache and runtime handle for promotion
                // We need to recreate these from settings since they're not stored
                let cache_config = CacheConfig {
                    root_folder: settings.cache.dir.to_str().unwrap().to_string(),
                    max_cache_size_gb: settings.cache.disk_size_gb,
                    memory_cache_size_gb: settings.cache.memory_size_gb,
                };
                
                // Create callback for promotion/demotion
                let promotion_callback: crate::ha::lease_coordinator::PromotionCallback = Arc::new(move |mode| {
                    let fs = Arc::clone(&fs_clone);
                    let object_store = Arc::clone(&object_store_for_promotion);
                    let db_path = db_path_for_promotion.clone();
                    let settings = settings_for_promotion.clone();
                    let cache_config = cache_config.clone();
                    let wal_streamer = wal_streamer_for_promotion.clone();
                    
                    Box::pin(async move {
                        match mode {
                            crate::ha::ServerMode::Active => {
                                info!("🔄 Promotion callback: Promoting to ACTIVE");
                                
                                // Build Settings from LsmConfig (similar to build_slatedb)
                                let l0_max_ssts = settings.lsm
                                    .as_ref()
                                    .map(|c| c.l0_max_ssts())
                                    .unwrap_or(crate::config::LsmConfig::DEFAULT_L0_MAX_SSTS);
                                let max_unflushed_bytes = settings.lsm
                                    .as_ref()
                                    .map(|c| c.max_unflushed_bytes())
                                    .unwrap_or_else(|| {
                                        (crate::config::LsmConfig::DEFAULT_MAX_UNFLUSHED_GB * 1_000_000_000.0) as usize
                                    });
                                let max_concurrent_compactions = settings.lsm
                                    .as_ref()
                                    .map(|c| c.max_concurrent_compactions())
                                    .unwrap_or(crate::config::LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS);
                                
                                let slatedb_settings = slatedb::config::Settings {
                                    l0_max_ssts,
                                    object_store_cache_options: ObjectStoreCacheOptions {
                                        root_folder: Some(format!("{}/slatedb", cache_config.root_folder).into()),
                                        max_cache_size_bytes: Some((cache_config.max_cache_size_gb * 1_000_000_000.0) as usize),
                                        cache_puts: true,
                                        ..Default::default()
                                    },
                                    flush_interval: Some(std::time::Duration::from_secs(30)),
                                    max_unflushed_bytes,
                                    compactor_options: Some(slatedb::config::CompactorOptions {
                                        max_concurrent_compactions,
                                        max_sst_size: 256 * 1024 * 1024,
                                        ..Default::default()
                                    }),
                                    compression_codec: None,
                                    garbage_collector_options: Some(GarbageCollectorOptions {
                                        wal_options: Some(GarbageCollectorDirectoryOptions {
                                            interval: Some(Duration::from_secs(60 * 60)),
                                            min_age: Duration::from_secs(60 * 60),
                                        }),
                                        manifest_options: Some(GarbageCollectorDirectoryOptions {
                                            interval: Some(Duration::from_secs(60 * 60)),
                                            min_age: Duration::from_secs(60 * 60),
                                        }),
                                        compacted_options: Some(GarbageCollectorDirectoryOptions {
                                            interval: Some(Duration::from_secs(60 * 60)),
                                            min_age: Duration::from_secs(60 * 60),
                                        }),
                                    }),
                                    ..Default::default()
                                };
                                
                                // Recreate cache and runtime for read-write mode
                                let memory_cache_bytes = (cache_config.memory_cache_size_gb.unwrap_or(0.25) * 1_000_000_000.0) as u64;
                                let cache = Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
                                    max_capacity: memory_cache_bytes,
                                    time_to_idle: None,
                                    time_to_live: None,
                                }));
                                
                                let (runtime_handle, _runtime_keeper) = tokio::task::spawn_blocking(|| {
                                    let runtime = Runtime::new().unwrap();
                                    let handle = runtime.handle().clone();
                                    let runtime_keeper = std::thread::spawn(move || {
                                        runtime.block_on(async { std::future::pending::<()>().await });
                                    });
                                    (handle, runtime_keeper)
                                })
                                .await?;
                                
                                // Pass WAL streamer if available (from WAL replication setup)
                                fs.promote_to_active(
                                    object_store,
                                    db_path,
                                    &slatedb_settings,
                                    cache,
                                    runtime_handle,
                                    wal_streamer.clone(),
                                ).await?;
                                
                                Ok(())
                            }
                            crate::ha::ServerMode::Standby => {
                                warn!("🔄 Demotion callback: Demoting to STANDBY");
                                fs.demote_to_standby(object_store, db_path).await?;
                                Ok(())
                            }
                            _ => {
                                // For other modes (Fenced, Initializing), don't change DB mode
                                Ok(())
                            }
                        }
                    })
                });
                
                lease_coordinator.set_promotion_callback(promotion_callback).await;

                // Start lease coordinator in background
                let coordinator = Arc::clone(&lease_coordinator);
                tokio::spawn(async move {
                    tracing::info!("Spawning lease coordinator task...");
                    match coordinator.start().await {
                        Ok(()) => {
                            tracing::error!("Lease coordinator task completed unexpectedly (should run forever)");
                        }
                        Err(e) => {
                            tracing::error!("Lease coordinator error: {} (this will prevent HA from working)", e);
                        }
                    }
                });
            }

            Some(health_handle)
        } else {
            None
        }
    } else {
        None
    };

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let mut server_handles = Vec::new();
    server_handles.extend(nfs_handles);
    server_handles.extend(ninep_handles);
    server_handles.extend(nbd_handles);

    if server_handles.is_empty() {
        return Err(anyhow::anyhow!(
            "No servers configured. At least one server (NFS, 9P, or NBD) must be enabled."
        ));
    }

    tokio::select! {
        result = futures::future::select_all(server_handles) => {
            let (result, _, _) = result;
            result??;
        }
        _ = gc_handle, if !read_only => {
            unreachable!("GC task should never complete");
        }
        _ = stats_handle => {
            unreachable!("Stats task should never complete");
        }
        _ = async { checkpoint_handle.unwrap().await }, if checkpoint_handle.is_some() => {
            unreachable!("Checkpoint refresh task should never complete");
        }
        _ = async { ha_health_handle.unwrap().await }, if ha_health_handle.is_some() => {
            unreachable!("Health server should never complete");
        }
        _ = async { wal_receiver_handle.unwrap().await }, if wal_receiver_handle.is_some() => {
            unreachable!("WAL receiver should never complete");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, shutting down gracefully...");
            if !read_only {
                fs.flush().await?;
            }
            fs.db.close().await?;
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down gracefully...");
            if !read_only {
                fs.flush().await?;
            }
            fs.db.close().await?;
        }
    }

    Ok(())
}
