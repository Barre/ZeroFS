use crate::block_transformer::ZeroFsBlockTransformer;
use crate::bucket_identity;
use crate::cache::FoyerCache;
use crate::checkpoint_manager::CheckpointManager;
use crate::config::{NbdConfig, NfsConfig, NinePConfig, RpcConfig, Settings};
use crate::db::SlateDbHandle;
use crate::fs::permissions::Credentials;
use crate::fs::tracing::AccessTracer;
use crate::fs::types::SetAttributes;
use crate::fs::{CacheConfig, GarbageCollector, ZeroFS};
use crate::key_management;
use crate::nbd::NBDServer;
use crate::parse_object_store::parse_url_opts;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use slatedb::admin::AdminBuilder;
use slatedb::config::{
    CheckpointOptions, DbReaderOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
    ObjectStoreCacheOptions, SizeTieredCompactionSchedulerOptions,
};
use slatedb::object_store::path::Path;
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use slatedb::{BlockTransformer, DbBuilder, DbReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

const CHECKPOINT_REFRESH_INTERVAL_SECS: u64 = 10;

#[derive(Debug, Clone, Copy)]
pub enum DatabaseMode {
    ReadWrite,
    ReadOnly,
    Checkpoint(uuid::Uuid),
}

impl DatabaseMode {
    pub fn is_read_only(&self) -> bool {
        !matches!(self, DatabaseMode::ReadWrite)
    }
}

async fn resolve_checkpoint_name(settings: &Settings, name: &str) -> Result<uuid::Uuid> {
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) =
        parse_url_opts(&settings.storage.url.parse()?, env_vars.into_iter())?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let db_path = Path::from(path_from_url.to_string());

    let admin = AdminBuilder::new(db_path, object_store).build();

    let checkpoints = admin
        .list_checkpoints(Some(name))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list checkpoints: {}", e))?;

    checkpoints
        .into_iter()
        .find(|cp| cp.name.as_deref() == Some(name))
        .map(|cp| cp.id)
        .ok_or_else(|| anyhow::anyhow!("Checkpoint '{}' not found", name))
}

async fn start_nfs_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NfsConfig>,
    shutdown: CancellationToken,
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
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nfs-server", async move {
            match crate::nfs::start_nfs_server_with_config(fs_clone, addr, shutdown_clone).await {
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
    shutdown: CancellationToken,
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
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("9p-server", async move {
                ninep_tcp_server.start(shutdown_clone).await
            }));
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
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("9p-unix-server", async move {
            ninep_unix_server.start(shutdown_clone).await
        }));
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

    match fs.lookup(&creds, 0, nbd_name).await {
        Ok(_) => info!(".nbd directory already exists"),
        Err(e) => {
            debug!(".nbd directory lookup returned: {:?}, will create it", e);
            let attr = SetAttributes {
                mode: crate::fs::types::SetMode::Set(0o755),
                uid: crate::fs::types::SetUid::Set(0),
                gid: crate::fs::types::SetGid::Set(0),
                ..Default::default()
            };
            fs.mkdir(&creds, 0, nbd_name, &attr)
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
    shutdown: CancellationToken,
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
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nbd-server", async move {
                if let Err(e) = nbd_tcp_server.start(shutdown_clone).await {
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
        let nbd_unix_server = NBDServer::new_unix(Arc::clone(&fs), socket_path);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nbd-unix-server", async move {
            if let Err(e) = nbd_unix_server.start(shutdown_clone).await {
                Err(e)
            } else {
                Ok(())
            }
        }));
    }

    handles
}

async fn start_rpc_servers(
    config: Option<&RpcConfig>,
    checkpoint_manager: Arc<CheckpointManager>,
    tracer: AccessTracer,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };

    let service = crate::rpc::server::AdminRpcServer::new(checkpoint_manager, tracer);
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for &addr in addresses {
            info!("Starting RPC server on {}", addr);
            let service = service.clone();
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("rpc-server", async move {
                crate::rpc::server::serve_tcp(addr, service, shutdown_clone)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
            }));
        }
    }

    if let Some(socket_path) = &config.unix_socket {
        info!(
            "Starting RPC server on Unix socket: {}",
            socket_path.display()
        );
        let socket_path = socket_path.clone();
        let service = service.clone();
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("rpc-unix-server", async move {
            crate::rpc::server::serve_unix(socket_path, service, shutdown_clone)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }));
    }

    handles
}

fn start_stats_reporting(fs: Arc<ZeroFS>, shutdown: CancellationToken) -> JoinHandle<()> {
    spawn_named("stats-reporting", async move {
        info!("Starting stats reporting task (reports to debug every 5 seconds)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Stats reporting task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    fs.stats.output_report_debug();
                }
            }
        }
    })
}

fn start_periodic_flush(
    fs: Arc<ZeroFS>,
    interval_secs: u64,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_named("periodic-flush", async move {
        info!(
            "Starting periodic flush task (flushes every {} seconds)",
            interval_secs
        );
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Periodic flush task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = fs.flush_coordinator.flush().await {
                        tracing::error!("Periodic flush failed: {:?}", e);
                    }
                }
            }
        }
    })
}

pub struct CheckpointRefreshParams {
    pub db_path: String,
    pub object_store: Arc<dyn object_store::ObjectStore>,
}

fn start_checkpoint_refresh(
    params: CheckpointRefreshParams,
    db: Arc<crate::db::Db>,
    block_transformer: Arc<dyn slatedb::BlockTransformer>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let db_path = params.db_path;
    let object_store = params.object_store;
    spawn_named("checkpoint-refresh", async move {
        info!("Starting checkpoint refresh task",);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            CHECKPOINT_REFRESH_INTERVAL_SECS,
        ));

        let db_path = Path::from(db_path);
        let admin = AdminBuilder::new(db_path.clone(), object_store.clone()).build();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Checkpoint refresh task shutting down");
                    break;
                }
                _ = interval.tick() => {
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
                                DbReaderOptions {
                                    block_transformer: Some(block_transformer.clone()),
                                    ..Default::default()
                                },
                            )
                            .await
                            {
                                Ok(new_reader) => {
                                    if let Err(e) = db.swap_reader(Arc::new(new_reader)) {
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
            }
        }
    })
}

pub async fn build_slatedb(
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_config: &CacheConfig,
    db_path: String,
    db_mode: DatabaseMode,
    lsm_config: Option<crate::config::LsmConfig>,
    disable_compactor: bool,
    block_transformer: Arc<dyn BlockTransformer>,
) -> Result<(
    SlateDbHandle,
    Option<CheckpointRefreshParams>,
    Option<tokio::runtime::Handle>,
)> {
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

    let compactor_options = if disable_compactor {
        None
    } else {
        Some(slatedb::config::CompactorOptions {
            max_concurrent_compactions,
            max_sst_size: 1024 * 1024 * 1024,
            ..Default::default()
        })
    };

    let settings = slatedb::config::Settings {
        wal_enabled: false,
        l0_max_ssts,
        l0_sst_size_bytes: 128 * 1024 * 1024,
        filter_bits_per_key: 10,
        object_store_cache_options: ObjectStoreCacheOptions {
            root_folder: Some(cache_config.root_folder.clone()),
            max_cache_size_bytes: Some(slatedb_object_cache_bytes),
            cache_puts: true,
            ..Default::default()
        },
        flush_interval: Some(std::time::Duration::from_secs(30)),
        max_unflushed_bytes,
        compactor_options,
        compression_codec: None, // Disable compression - we handle it in encryption layer
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
            }),
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
            }),
            compactions_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
            }),
        }),
        ..Default::default()
    };

    let cache = Arc::new(FoyerCache::new(slatedb_memory_cache_bytes as usize));

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

    match db_mode {
        DatabaseMode::ReadWrite => {
            if disable_compactor {
                info!("Opening database in read-write mode (compactor disabled)");
            } else {
                info!("Opening database in read-write mode");
            }

            let mut builder = DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .with_gc_runtime(runtime_handle.clone())
                .with_sst_block_size(slatedb::SstBlockSize::Block4Kib)
                .with_memory_cache(cache)
                .with_block_transformer(block_transformer);

            if !disable_compactor {
                builder = builder
                    .with_compaction_runtime(runtime_handle.clone())
                    .with_compaction_scheduler_supplier(Arc::new(
                        SizeTieredCompactionSchedulerSupplier::new(
                            SizeTieredCompactionSchedulerOptions {
                                max_compaction_sources: 32,
                                include_size_threshold: 4.0,
                                ..Default::default()
                            },
                        ),
                    ));
            }

            let slatedb = Arc::new(builder.build().await?);

            Ok((
                SlateDbHandle::ReadWrite(slatedb),
                None,
                Some(runtime_handle),
            ))
        }
        DatabaseMode::ReadOnly => {
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
            let reader_options = DbReaderOptions {
                block_transformer: Some(block_transformer),
                ..Default::default()
            };
            let reader = Arc::new(
                DbReader::open(
                    db_path,
                    object_store.clone(),
                    Some(checkpoint_result.id),
                    reader_options,
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
                None,
            ))
        }
        DatabaseMode::Checkpoint(checkpoint_id) => {
            info!("Opening database from checkpoint ID: {}", checkpoint_id);

            let reader_options = DbReaderOptions {
                block_transformer: Some(block_transformer),
                ..Default::default()
            };
            let reader = Arc::new(
                DbReader::open(db_path, object_store, Some(checkpoint_id), reader_options).await?,
            );

            Ok((SlateDbHandle::ReadOnly(ArcSwap::new(reader)), None, None))
        }
    }
}

pub struct InitResult {
    pub fs: Arc<ZeroFS>,
    pub checkpoint_params: Option<CheckpointRefreshParams>,
    pub object_store: Arc<dyn object_store::ObjectStore>,
    pub db_path: String,
    pub db_handle: SlateDbHandle,
    pub maintenance_runtime: Option<tokio::runtime::Handle>,
    pub block_transformer: Arc<dyn BlockTransformer>,
}

async fn initialize_filesystem(
    settings: &Settings,
    db_mode: DatabaseMode,
    disable_compactor: bool,
) -> Result<InitResult> {
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

    info!("Starting ZeroFS server with {} backend", object_store);
    info!("DB Path: {}", actual_db_path);
    info!(
        "Base Cache Directory: {}",
        cache_config.root_folder.display()
    );
    info!("Cache Size: {} GB", cache_config.max_cache_size_gb);

    info!("Checking bucket identity...");
    let bucket =
        bucket_identity::BucketIdentity::get_or_create(&object_store, &actual_db_path).await?;

    let cache_config = CacheConfig {
        root_folder: cache_config.root_folder.join(bucket.cache_directory_name()),
        ..cache_config
    };

    info!(
        "Bucket ID: {}, Cache directory: {}",
        bucket.id(),
        cache_config.root_folder.display()
    );

    if !db_mode.is_read_only() {
        crate::storage_compatibility::check_if_match_support(&object_store, &actual_db_path)
            .await?;
    }

    let password = settings.storage.encryption_password.clone();

    super::password::validate_password(&password)
        .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

    info!("Loading or initializing encryption key from object store");

    let db_path = Path::from(actual_db_path.clone());
    let encryption_key = key_management::load_or_init_encryption_key(
        &object_store,
        &db_path,
        &password,
        db_mode.is_read_only(),
    )
    .await?;

    let block_transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&encryption_key, settings.compression());

    let (slatedb, checkpoint_params, maintenance_runtime) = build_slatedb(
        object_store.clone(),
        &cache_config,
        actual_db_path.clone(),
        db_mode,
        settings.lsm,
        disable_compactor,
        block_transformer.clone(),
    )
    .await?;

    let db_handle = slatedb.clone();
    let fs = ZeroFS::new_with_slatedb(slatedb, settings.max_bytes()).await?;

    Ok(InitResult {
        fs: Arc::new(fs),
        checkpoint_params,
        object_store,
        db_path: actual_db_path,
        db_handle,
        maintenance_runtime,
        block_transformer,
    })
}

pub async fn run_server(
    config_path: PathBuf,
    read_only: bool,
    checkpoint_name: Option<String>,
    no_compactor: bool,
) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));

    #[cfg(feature = "tokio-console")]
    {
        use tracing_subscriber::prelude::*;
        let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(console_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .init();
    }

    #[cfg(not(feature = "tokio-console"))]
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let db_mode = match (read_only, &checkpoint_name) {
        (false, None) => DatabaseMode::ReadWrite,
        (true, None) => DatabaseMode::ReadOnly,
        (false, Some(name)) => {
            let uuid = resolve_checkpoint_name(&settings, name)
                .await
                .with_context(|| format!("Failed to resolve checkpoint '{}'", name))?;
            DatabaseMode::Checkpoint(uuid)
        }
        (true, Some(_)) => {
            return Err(anyhow::anyhow!(
                "Cannot specify both --read-only and --checkpoint flags"
            ));
        }
    };

    let init_result = initialize_filesystem(&settings, db_mode, no_compactor).await?;
    let fs = init_result.fs;
    let checkpoint_params = init_result.checkpoint_params;

    if !db_mode.is_read_only() && settings.servers.nbd.is_some() {
        ensure_nbd_directory(&fs).await?;
    }

    let shutdown = CancellationToken::new();

    let nfs_handles = start_nfs_servers(
        Arc::clone(&fs),
        settings.servers.nfs.as_ref(),
        shutdown.clone(),
    )
    .await;

    let ninep_handles = start_ninep_servers(
        Arc::clone(&fs),
        settings.servers.ninep.as_ref(),
        shutdown.clone(),
    )
    .await?;

    let nbd_handles = start_nbd_servers(
        Arc::clone(&fs),
        settings.servers.nbd.as_ref(),
        shutdown.clone(),
    )
    .await;

    let checkpoint_manager = Arc::new(CheckpointManager::new(
        init_result.db_handle,
        slatedb::object_store::path::Path::from(init_result.db_path),
        init_result.object_store,
    ));
    let rpc_handles = start_rpc_servers(
        settings.servers.rpc.as_ref(),
        checkpoint_manager,
        fs.tracer.clone(),
        shutdown.clone(),
    )
    .await;

    let gc_handle = if !db_mode.is_read_only() {
        let gc = Arc::new(GarbageCollector::new(
            Arc::clone(&fs.db),
            fs.tombstone_store.clone(),
            fs.chunk_store.clone(),
            Arc::clone(&fs.stats),
        ));
        Some(gc.start(shutdown.clone(), init_result.maintenance_runtime.clone()))
    } else {
        None
    };
    let stats_handle = start_stats_reporting(Arc::clone(&fs), shutdown.clone());
    let flush_handle = if !db_mode.is_read_only() {
        let flush_interval_secs = settings
            .lsm
            .map(|c| c.flush_interval_secs())
            .unwrap_or(crate::config::LsmConfig::DEFAULT_FLUSH_INTERVAL_SECS);
        Some(start_periodic_flush(
            Arc::clone(&fs),
            flush_interval_secs,
            shutdown.clone(),
        ))
    } else {
        None
    };

    let checkpoint_handle = checkpoint_params.map(|params| {
        start_checkpoint_refresh(
            params,
            Arc::clone(&fs.db),
            init_result.block_transformer.clone(),
            shutdown.clone(),
        )
    });

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let mut server_handles = Vec::new();
    server_handles.extend(nfs_handles);
    server_handles.extend(ninep_handles);
    server_handles.extend(nbd_handles);
    server_handles.extend(rpc_handles);

    if server_handles.is_empty() {
        return Err(anyhow::anyhow!(
            "No servers configured. At least one server (NFS, 9P, NBD, or RPC) must be enabled."
        ));
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating graceful shutdown...");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, initiating graceful shutdown...");
        }
    }

    info!("Cancelling all servers and background tasks...");
    shutdown.cancel();

    info!("Waiting for servers to exit...");
    for handle in server_handles {
        let _ = handle.await;
    }

    info!("Waiting for background tasks to exit...");
    if let Some(gc_handle) = gc_handle {
        let _ = gc_handle.await;
    }
    let _ = stats_handle.await;
    if let Some(flush_handle) = flush_handle {
        let _ = flush_handle.await;
    }
    if let Some(checkpoint_handle) = checkpoint_handle {
        let _ = checkpoint_handle.await;
    }

    info!("Performing final flush and closing database...");
    if !db_mode.is_read_only()
        && let Err(e) = fs.flush_coordinator.flush().await
    {
        tracing::error!("Final flush failed: {:?}", e);
    }

    if let Err(e) = fs.db.close().await {
        tracing::error!("Database close failed: {:?}", e);
        return Err(e);
    }

    info!("Shutdown complete");
    Ok(())
}
