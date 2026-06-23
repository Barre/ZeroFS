use crate::checkpoint_manager::CheckpointManager;
use crate::config::{NbdConfig, NfsConfig, NinePConfig, RpcConfig, Settings};
use crate::db::SlateDbHandle;
use crate::fs::permissions::Credentials;
use crate::fs::types::SetAttributes;
use crate::fs::{CacheConfig, GarbageCollector, ZeroFS};
use crate::length_checked_object_store::LengthCheckedObjectStore;
use crate::nbd::NBDServer;
use crate::object_store_prefetch::PrefetchingObjectStore;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    S3FifoConfig, Spawner,
};
use slatedb::admin::AdminBuilder;
use slatedb::config::GarbageCollectorDirectoryOptions;
use slatedb::config::GarbageCollectorOptions;
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::object_store::path::Path;
use slatedb::{BlockTransformer, CompactorBuilder, DbBuilder, DbReader};
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Parse a WAL config into an object store rooted at the full URL path.
pub(crate) fn parse_wal_object_store(
    wal_config: &crate::config::WalConfig,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    let env_vars = wal_config.cloud_provider_env_vars();
    let (store, path) = parse_url_opts(&wal_config.url.parse()?, env_vars)?;
    let path_str: &str = path.as_ref();
    let store: Arc<dyn object_store::ObjectStore> = if path_str.is_empty() {
        Arc::from(store)
    } else {
        Arc::new(object_store::prefix::PrefixStore::new(store, path))
    };
    Ok(with_storage_class(
        store,
        wal_config.storage_class.as_deref(),
    ))
}

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
    let (object_store, path_from_url) = parse_url_opts(&settings.storage.url.parse()?, env_vars)?;
    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );
    let db_path = Path::from(path_from_url.to_string());

    let mut admin_builder = AdminBuilder::new(db_path, object_store);
    if let Some(wal_config) = &settings.wal {
        admin_builder = admin_builder.with_wal_object_store(parse_wal_object_store(wal_config)?);
    }
    let admin = admin_builder.build();

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

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!("Starting NFS server on {}", addr);
            let fs_clone = Arc::clone(&fs);
            let addr = *addr;
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nfs-server", async move {
                match crate::nfs::start_nfs_server_with_config(fs_clone, addr, shutdown_clone).await
                {
                    Ok(()) => Ok(()),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
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
    fs: Arc<ZeroFS>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };

    let service = crate::rpc::server::AdminRpcServer::new(checkpoint_manager, fs, shutdown.clone());
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

pub(crate) async fn build_parts_hybrid(
    cache_root: &std::path::Path,
    disk_bytes: usize,
    foyer_handle: &tokio::runtime::Handle,
) -> Result<foyer::HybridCache<crate::object_store_prefetch::PartKey, bytes::Bytes>> {
    use crate::object_store_prefetch::PartKey;
    use bytes::Bytes;

    const PARTS_MEMORY_BYTES: usize = 128 * 1024 * 1024;

    let parts_root = cache_root.join("parts_cache");
    tokio::fs::create_dir_all(&parts_root)
        .await
        .with_context(|| format!("creating parts cache dir at {}", parts_root.display()))?;
    HybridCacheBuilder::new()
        .with_name("zerofs-object-prefetch-parts")
        .memory(PARTS_MEMORY_BYTES)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|_: &PartKey, v: &Bytes| v.len())
        .storage()
        .with_spawner(Spawner::from(foyer_handle.clone()))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(&parts_root)
                    .with_capacity(disk_bytes)
                    .build()
                    .map_err(|e| anyhow::anyhow!("parts foyer device build failed: {e}"))?,
            )
            .with_block_size(64 * 1024 * 1024),
        )
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("parts foyer hybrid build failed: {e}"))
}

/// Compute (parts_disk_bytes, decoded_blocks_disk_bytes) from a total disk
/// budget. Parts get 10% of the budget, clamped to [256 MiB, 10 GiB]; the
/// decoded-blocks hybrid takes the rest. Floors at 256 MiB so neither side
/// collapses to zero on tiny configurations.
pub(crate) fn split_disk_budget(total_disk_bytes: usize) -> (usize, usize) {
    const MIN_PARTS_BYTES: usize = 1024 * 1024 * 1024;
    const MAX_PARTS_BYTES: usize = 10_usize
        .saturating_mul(1024)
        .saturating_mul(1024)
        .saturating_mul(1024);

    let parts = (total_disk_bytes / 10).clamp(MIN_PARTS_BYTES, MAX_PARTS_BYTES);
    let parts = parts.min(total_disk_bytes.saturating_sub(MIN_PARTS_BYTES));
    let decoded = total_disk_bytes.saturating_sub(parts).max(MIN_PARTS_BYTES);
    (parts, decoded)
}

/// Result of opening the ZeroFS database.
pub struct SlateDbOpen {
    pub data: SlateDbHandle,
    pub maintenance_runtime: Option<tokio::runtime::Handle>,
    pub metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
}

#[allow(clippy::too_many_arguments)]
pub async fn build_slatedb(
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_config: &CacheConfig,
    db_path: String,
    db_mode: DatabaseMode,
    lsm_config: Option<crate::config::LsmConfig>,
    disable_compactor: bool,
    block_transformer: Arc<dyn BlockTransformer>,
    wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    segments_enabled: bool,
    replication: Option<&crate::replication::ReplicationParams>,
) -> Result<SlateDbOpen> {
    let total_disk_cache_gb = cache_config.max_cache_size_gb;
    let total_memory_cache_gb = cache_config.memory_cache_size_gb.unwrap_or(0.25);

    let total_disk_bytes = (total_disk_cache_gb * 1_000_000_000.0) as usize;
    let (parts_disk_bytes, hybrid_disk_bytes) = split_disk_budget(total_disk_bytes);
    let hybrid_memory_bytes = (total_memory_cache_gb * 1_000_000_000.0) as usize;

    info!(
        "Cache allocation - Disk: {:.2}GB total ({} MB decoded-blocks + {} MB raw-parts), \
         Memory: {:.2}GB",
        total_disk_cache_gb,
        hybrid_disk_bytes / 1_000_000,
        parts_disk_bytes / 1_000_000,
        total_memory_cache_gb,
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

    // Replication needs the writer path: reject read-only / checkpoint, and
    // reject reaching here as a standby (a standby opens the data db as writer
    // only on promotion; doing so here would fence the live leader).
    if let Some(repl) = replication {
        if db_mode.is_read_only() {
            anyhow::bail!(
                "[replication] is incompatible with read-only / checkpoint database modes; \
                 node {} must open the data database as a writer",
                repl.node_id
            );
        }
        if !repl.is_leader() {
            anyhow::bail!(
                "internal error: build_slatedb reached as a standby (node {}); a standby must \
                 complete failover and be promoted to leader before opening the data database",
                repl.node_id
            );
        }
    }

    let wal_enabled = lsm_config.map(|c| c.wal_enabled()).unwrap_or(false);

    let settings = slatedb::config::Settings {
        wal_enabled,
        l0_max_ssts,
        l0_max_ssts_per_key: l0_max_ssts,
        l0_sst_size_bytes: 64 * 1024 * 1024,
        compactor_options: None,
        flush_interval: Some(std::time::Duration::from_secs(30)),
        max_unflushed_bytes,
        compression_codec: None, // Disable compression as we handle it in encryption layer
        l0_flush_parallelism: 16,
        min_filter_keys: 10,
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
            detach_options: None,
        }),
        ..Default::default()
    };

    // Dedicated runtime for maintenance work (foyer cache I/O, slatedb GC,
    // compactions, ZeroFS GC) so it doesn't compete with the serving runtime.
    // The runtime is moved onto a parked thread and lives for the rest of the
    // process; dropping it here would panic inside an async context.
    let maintenance_runtime = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("zerofs-maintenance")
            .build()
            .expect("failed to build maintenance runtime");
        let handle = rt.handle().clone();
        std::thread::spawn(move || {
            rt.block_on(std::future::pending::<()>());
        });
        handle
    };

    let hybrid_cache_root = cache_config.root_folder.join("hybrid_cache");
    tokio::fs::create_dir_all(&hybrid_cache_root)
        .await
        .with_context(|| {
            format!(
                "creating foyer hybrid cache dir at {}",
                hybrid_cache_root.display()
            )
        })?;

    let hybrid = HybridCacheBuilder::new()
        .with_name("zerofs-slatedb-hybrid")
        .memory(hybrid_memory_bytes)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|_, v: &slatedb::db_cache::CachedEntry| v.size())
        .storage()
        .with_spawner(Spawner::from(maintenance_runtime.clone()))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(&hybrid_cache_root)
                    .with_capacity(hybrid_disk_bytes)
                    .build()
                    .map_err(|e| anyhow::anyhow!("foyer device build failed: {e}"))?,
            )
            .with_block_size(64 * 1024 * 1024),
        )
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("foyer hybrid build failed: {e}"))?;
    let cache = Arc::new(FoyerHybridCache::new_with_cache(hybrid));

    let parts_cache = build_parts_hybrid(
        &cache_config.root_folder,
        parts_disk_bytes,
        &maintenance_runtime,
    )
    .await?;

    // Length-check the store before the data-db prefetch wrapper is layered on;
    // the compactor uses the length-checked store directly (no prefetch cache).
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LengthCheckedObjectStore::new(object_store));
    let compactor_object_store = object_store.clone();
    let wal_object_store = wal_object_store
        .map(|s| Arc::new(LengthCheckedObjectStore::new(s)) as Arc<dyn object_store::ObjectStore>);
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(PrefetchingObjectStore::new(object_store, parts_cache));

    let db_path = Path::from(db_path);

    match db_mode {
        DatabaseMode::ReadWrite => {
            if disable_compactor {
                info!("Opening database in read-write mode (compactor disabled)");
            } else {
                info!("Opening database in read-write mode");
            }

            let metrics_recorder = Arc::new(DefaultMetricsRecorder::new());

            let mut builder = DbBuilder::new(db_path.clone(), object_store.clone())
                .with_settings(settings)
                .with_gc_runtime(maintenance_runtime.clone())
                .with_sst_block_size(slatedb::SstBlockSize::Block32Kib)
                .with_db_cache(cache)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies(segments_enabled))
                .with_metrics_recorder(metrics_recorder.clone());

            if segments_enabled {
                builder = builder.with_segment_extractor(Arc::new(
                    crate::segment_extractor::ZeroFsSegmentExtractor,
                ));
            }

            if let Some(wal_store) = wal_object_store {
                builder = builder.with_wal_object_store(wal_store);
            }

            if !disable_compactor {
                let scheduler_options: std::collections::HashMap<String, String> =
                    slatedb::config::SizeTieredCompactionSchedulerOptions {
                        max_compaction_sources: 16,
                        ..Default::default()
                    }
                    .into();
                let compactor = CompactorBuilder::new(db_path, compactor_object_store)
                    .with_runtime(maintenance_runtime.clone())
                    .with_filter_policies(crate::fs::filter_policy::filter_policies(
                        segments_enabled,
                    ))
                    .with_options(slatedb::config::CompactorOptions {
                        poll_interval: std::time::Duration::from_secs(1),
                        max_concurrent_compactions,
                        max_sst_size: 1024 * 1024 * 1024,
                        max_fetch_tasks: 4,
                        scheduler_options,
                        ..Default::default()
                    });

                builder = builder.with_compactor_builder(compactor);
            }

            let slatedb = Arc::new(
                builder
                    .build()
                    .await
                    .context("Failed to build SlateDB instance")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadWrite(slatedb),
                maintenance_runtime: Some(maintenance_runtime),
                metrics_recorder: Some(metrics_recorder),
            })
        }
        DatabaseMode::ReadOnly => {
            info!("Opening database in read-only mode");

            let mut reader_builder = DbReader::builder(db_path, object_store)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies(segments_enabled));
            if let Some(wal_store) = wal_object_store {
                reader_builder = reader_builder.with_wal_object_store(wal_store);
            }
            let reader = Arc::new(
                reader_builder
                    .build()
                    .await
                    .context("Failed to open database in read-only mode")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
                maintenance_runtime: None,
                metrics_recorder: None,
            })
        }
        DatabaseMode::Checkpoint(checkpoint_id) => {
            info!("Opening database from checkpoint ID: {}", checkpoint_id);

            let mut reader_builder = DbReader::builder(db_path, object_store)
                .with_checkpoint_id(checkpoint_id)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies(segments_enabled));
            if let Some(wal_store) = wal_object_store {
                reader_builder = reader_builder.with_wal_object_store(wal_store);
            }
            let reader = Arc::new(
                reader_builder
                    .build()
                    .await
                    .context("Failed to open database from checkpoint")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
                maintenance_runtime: None,
                metrics_recorder: None,
            })
        }
    }
}

pub struct InitResult {
    pub fs: Arc<ZeroFS>,
    pub object_store: Arc<dyn object_store::ObjectStore>,
    pub wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    pub db_path: String,
    pub db_handle: SlateDbHandle,
    pub maintenance_runtime: Option<tokio::runtime::Handle>,
    /// Keeps the HA heartbeat loop alive; dropping it (or sending `true`) stops
    /// the loop. `None` in single-node mode.
    pub heartbeat_shutdown: Option<tokio::sync::watch::Sender<bool>>,
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

    info!("ZeroFS v{}", env!("CARGO_PKG_VERSION"));

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

    crate::telemetry::send_startup_event(&settings);

    let init_result =
        crate::cli::init::initialize_filesystem(&settings, db_mode, no_compactor).await?;
    let fs = init_result.fs;
    let heartbeat_shutdown = init_result.heartbeat_shutdown;

    if !db_mode.is_read_only() && settings.servers.nbd.is_some() {
        ensure_nbd_directory(&fs).await?;
    }

    let shutdown = CancellationToken::new();

    // Graceful shutdown stops the heartbeat loop so the lease lapses and a
    // standby can take over promptly.
    if let Some(hb_shutdown) = heartbeat_shutdown {
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            shutdown_clone.cancelled().await;
            let _ = hb_shutdown.send(true);
        });
    }

    let telemetry_handle = crate::telemetry::start_periodic_reporting(
        &settings,
        Arc::clone(&fs.global_stats),
        shutdown.clone(),
    );

    let prometheus_handles = if let Some(ref prometheus_config) = settings.prometheus {
        let slatedb_registry = fs.db.slatedb_metrics();
        crate::prometheus::start(
            prometheus_config,
            Arc::clone(&fs.stats),
            Arc::clone(&fs.global_stats),
            slatedb_registry,
            shutdown.clone(),
        )
    } else {
        Vec::new()
    };

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
        init_result.wal_object_store.clone(),
    ));
    #[cfg(feature = "webui")]
    let checkpoint_manager_for_webui = Arc::clone(&checkpoint_manager);
    let rpc_handles = start_rpc_servers(
        settings.servers.rpc.as_ref(),
        checkpoint_manager,
        Arc::clone(&fs),
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

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    #[cfg(feature = "webui")]
    let webui_handles = if let Some(ref webui_config) = settings.servers.webui {
        let webui_rpc_service = crate::rpc::server::AdminRpcServer::new(
            checkpoint_manager_for_webui,
            Arc::clone(&fs),
            shutdown.clone(),
        );
        let webui_lock_manager = Arc::new(crate::ninep::lock_manager::FileLockManager::new());
        crate::webui::start(
            webui_config,
            Arc::clone(&fs),
            webui_lock_manager,
            webui_rpc_service,
            shutdown.clone(),
        )
    } else {
        Vec::new()
    };

    let mut server_handles = Vec::new();
    server_handles.extend(nfs_handles);
    server_handles.extend(ninep_handles);
    server_handles.extend(nbd_handles);
    server_handles.extend(rpc_handles);
    #[cfg(feature = "webui")]
    server_handles.extend(webui_handles);

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
    if let Some(handle) = telemetry_handle {
        let _ = handle.await;
    }
    for handle in prometheus_handles {
        let _ = handle.await;
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
