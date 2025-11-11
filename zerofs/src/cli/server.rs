use crate::bucket_identity;
use crate::config::{NbdConfig, NfsConfig, NinePConfig, Settings};
use crate::fs::permissions::Credentials;
use crate::fs::types::SetAttributes;
use crate::fs::{CacheConfig, ZeroFS};
use crate::key_management;
use crate::nbd::NBDServer;
use crate::parse_object_store::parse_url_opts;
use anyhow::{Context, Result};
use slatedb::config::GarbageCollectorDirectoryOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::info;

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

    for addr in &config.addresses {
        info!("Starting 9P server on {}", addr);
        let ninep_tcp_server = crate::ninep::NinePServer::new(Arc::clone(&fs), *addr);
        handles.push(tokio::spawn(async move { ninep_tcp_server.start().await }));
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

    for addr in &config.addresses {
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

pub async fn build_slatedb(
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_config: &CacheConfig,
    db_path: String,
) -> Result<Arc<slatedb::Db>> {
    use slatedb::DbBuilder;
    use slatedb::config::{GarbageCollectorOptions, ObjectStoreCacheOptions};
    use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
    use slatedb::object_store::path::Path;
    use tokio::runtime::Runtime;

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

    let settings = slatedb::config::Settings {
        l0_max_ssts: 16,
        object_store_cache_options: ObjectStoreCacheOptions {
            root_folder: Some(slatedb_cache_dir.clone().into()),
            max_cache_size_bytes: Some(slatedb_object_cache_bytes),
            cache_puts: true,
            ..Default::default()
        },
        flush_interval: Some(std::time::Duration::from_secs(30)),
        max_unflushed_bytes: 1024 * 1024 * 1024,
        compactor_options: Some(slatedb::config::CompactorOptions {
            max_concurrent_compactions: 8,
            max_sst_size: 256 * 1024 * 1024,
            ..Default::default()
        }),
        compression_codec: None, // Disable compression - we handle it in encryption layer
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(60)),
                min_age: Duration::from_secs(60),
            }),
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(60)),
                min_age: Duration::from_secs(60),
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(60)),
                min_age: Duration::from_secs(60),
            }),
        }),
        ..Default::default()
    };

    let cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: slatedb_memory_cache_bytes,
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

    let slatedb = Arc::new(
        DbBuilder::new(db_path, object_store)
            .with_settings(settings)
            .with_gc_runtime(runtime_handle.clone())
            .with_compaction_runtime(runtime_handle.clone())
            .with_memory_cache(cache)
            .build()
            .await?,
    );

    Ok(slatedb)
}

async fn initialize_filesystem(settings: &Settings) -> Result<Arc<ZeroFS>> {
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

    crate::storage_compatibility::check_if_match_support(&object_store, &actual_db_path).await?;

    let password = settings.storage.encryption_password.clone();

    super::password::validate_password(&password)
        .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

    info!("Loading or initializing encryption key");

    let slatedb = build_slatedb(object_store, &cache_config, actual_db_path).await?;

    let encryption_key = key_management::load_or_init_encryption_key(&slatedb, &password).await?;

    let fs = ZeroFS::new_with_slatedb(slatedb, encryption_key).await?;

    Ok(Arc::new(fs))
}

pub async fn run_server(config_path: PathBuf) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let settings = Settings::from_file(config_path.to_str().unwrap())
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let fs = initialize_filesystem(&settings).await?;

    if settings.servers.nbd.is_some() {
        ensure_nbd_directory(&fs).await?;
    }

    let nfs_handles = start_nfs_servers(Arc::clone(&fs), settings.servers.nfs.as_ref()).await;

    let ninep_handles =
        start_ninep_servers(Arc::clone(&fs), settings.servers.ninep.as_ref()).await?;

    let nbd_handles = start_nbd_servers(Arc::clone(&fs), settings.servers.nbd.as_ref()).await;

    let gc_handle = start_garbage_collection(Arc::clone(&fs));
    let stats_handle = start_stats_reporting(Arc::clone(&fs));

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
        _ = gc_handle => {
            unreachable!("GC task should never complete");
        }
        _ = stats_handle => {
            unreachable!("Stats task should never complete");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, shutting down gracefully...");
            fs.flush().await?;
            fs.db.close().await?;
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down gracefully...");
            fs.flush().await?;
            fs.db.close().await?;
        }
    }

    Ok(())
}
