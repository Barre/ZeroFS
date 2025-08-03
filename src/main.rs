mod bucket_identity;
mod cache;
mod encryption;
mod filesystem;
mod filesystem_stats;
mod inode;
mod key_management;
mod lock_manager;
mod nbd;
mod nfs;
mod ninep;
mod operations;
mod permissions;
mod stats;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod posix_tests;

use crate::filesystem::{CacheConfig, SlateDbFs};
use crate::nbd::NBDServer;
use std::sync::Arc;
use tracing::info;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_NFS_HOST: &str = "127.0.0.1";
const DEFAULT_NFS_PORT: u32 = 2049;
const DEFAULT_9P_HOST: &str = "127.0.0.1";
const DEFAULT_9P_PORT: u16 = 5564;

// ANSI color codes
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const CYAN: &str = "\x1b[36m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

fn validate_environment() -> Result<(), Box<dyn std::error::Error>> {
    let mut missing_vars = Vec::new();

    if std::env::var("SLATEDB_CACHE_DIR").is_err() {
        missing_vars.push("SLATEDB_CACHE_DIR");
    }

    if std::env::var("SLATEDB_CACHE_SIZE_GB").is_err() {
        missing_vars.push("SLATEDB_CACHE_SIZE_GB");
    }

    if std::env::var("ZEROFS_ENCRYPTION_PASSWORD").is_err() {
        missing_vars.push("ZEROFS_ENCRYPTION_PASSWORD");
    }

    if !missing_vars.is_empty() {
        eprintln!(
            "{}{}Error:{} Missing required environment variables: {}{}{}",
            BOLD,
            RED,
            RESET,
            RED,
            missing_vars.join(", "),
            RESET
        );
        eprintln!();
        eprintln!("{BOLD}{CYAN}ZeroFS Configuration Guide{RESET}");
        eprintln!("{CYAN}==========================={RESET}");
        eprintln!();
        eprintln!("{YELLOW}Required Environment Variables:{RESET}");
        eprintln!(
            "  {BOLD}SLATEDB_CACHE_DIR{RESET}              - Local directory for caching data"
        );
        eprintln!(
            "  {BOLD}SLATEDB_CACHE_SIZE_GB{RESET}          - Maximum cache size in GB (e.g., 10)"
        );
        eprintln!("  {BOLD}ZEROFS_ENCRYPTION_PASSWORD{RESET}     - Password for data encryption");
        eprintln!();
        eprintln!("{YELLOW}Storage Backend Configuration:{RESET}");
        eprintln!();
        eprintln!("{CYAN}AWS S3 Backend (s3://bucket/path):{RESET}");
        eprintln!("  {BLUE}AWS_ACCESS_KEY_ID{RESET}              - AWS access key (required)");
        eprintln!("  {BLUE}AWS_SECRET_ACCESS_KEY{RESET}          - AWS secret key (required)");
        eprintln!(
            "  {BLUE}AWS_ENDPOINT{RESET}                   - S3 endpoint URL (optional, for S3-compatible services)"
        );
        eprintln!(
            "  {BLUE}AWS_DEFAULT_REGION{RESET}             - AWS region (optional, default: us-east-1)"
        );
        eprintln!(
            "  {BLUE}AWS_ALLOW_HTTP{RESET}                 - Allow HTTP endpoints (optional, default: false)"
        );
        eprintln!();
        eprintln!("{CYAN}Azure Storage Backend (azure://bucket/path):{RESET}");
        eprintln!(
            "  {BLUE}AZURE_STORAGE_ACCOUNT_NAME{RESET}     - Azure storage account name (required)"
        );
        eprintln!(
            "  {BLUE}AZURE_STORAGE_ACCOUNT_KEY{RESET}      - Azure storage account key (required)"
        );
        eprintln!();
        eprintln!("{CYAN}Local Filesystem Backend (file:///path/to/storage):{RESET}");
        eprintln!("  No additional configuration required");
        eprintln!();
        eprintln!("{YELLOW}Optional ZeroFS Configuration:{RESET}");
        eprintln!("  {BLUE}ZEROFS_MEMORY_CACHE_SIZE_GB{RESET}    - Memory cache size in GB");
        eprintln!(
            "  {BLUE}ZEROFS_NBD_PORTS{RESET}               - Comma-separated NBD server ports"
        );
        eprintln!(
            "  {BLUE}ZEROFS_NBD_DEVICE_SIZES_GB{RESET}     - Comma-separated NBD device sizes in GB"
        );
        eprintln!(
            "  {BLUE}ZEROFS_NBD_HOST{RESET}                - NBD server bind address (default: 127.0.0.1)"
        );
        eprintln!(
            "  {BLUE}ZEROFS_NEW_PASSWORD{RESET}            - New password when changing encryption"
        );
        eprintln!(
            "  {BLUE}ZEROFS_NFS_HOST{RESET}                - NFS server bind address (default: 127.0.0.1)"
        );
        eprintln!(
            "  {BLUE}ZEROFS_NFS_HOST_PORT{RESET}           - NFS server port (default: 2049)"
        );
        eprintln!(
            "  {BLUE}ZEROFS_9P_HOST{RESET}                 - 9P server bind address (default: 127.0.0.1)"
        );
        eprintln!("  {BLUE}ZEROFS_9P_PORT{RESET}                 - 9P server port (default: 5564)");
        eprintln!();
        eprintln!("{YELLOW}Logging Configuration:{RESET}");
        eprintln!(
            "  {BLUE}RUST_LOG{RESET}                       - Log level (default: error,zerofs=info)"
        );
        eprintln!();
        eprintln!("{GREEN}Usage Examples:{RESET}");
        eprintln!("{GREEN}---------------{RESET}");
        eprintln!();
        eprintln!("{CYAN}Basic usage:{RESET}");
        eprintln!("  export SLATEDB_CACHE_DIR=/tmp/zerofs-cache");
        eprintln!("  export SLATEDB_CACHE_SIZE_GB=10");
        eprintln!("  export ZEROFS_ENCRYPTION_PASSWORD='your-secure-password'");
        eprintln!("  zerofs [url]");
        eprintln!();
        eprintln!("{CYAN}With S3 backend:{RESET}");
        eprintln!("  export AWS_ACCESS_KEY_ID='your-access-key'");
        eprintln!("  export AWS_SECRET_ACCESS_KEY='your-secret-key'");
        eprintln!("  # ... other required vars ...");
        eprintln!("  zerofs s3://my-bucket/prefix");
        eprintln!();
        eprintln!("{CYAN}With Azure backend:{RESET}");
        eprintln!("  export AZURE_STORAGE_ACCOUNT_NAME='your-account'");
        eprintln!("  export AZURE_STORAGE_ACCOUNT_KEY='your-key'");
        eprintln!("  # ... other required vars ...");
        eprintln!("  zerofs azure://my-container/prefix");
        eprintln!();
        eprintln!("{CYAN}With local filesystem backend:{RESET}");
        eprintln!("  # ... required vars ...");
        eprintln!("  zerofs file:///path/to/storage");
        eprintln!();
        eprintln!("{CYAN}Change encryption password:{RESET}");
        eprintln!("  export ZEROFS_NEW_PASSWORD='new-password'");
        eprintln!("  # ... other required vars ...");
        eprintln!("  zerofs [url]");

        std::process::exit(1);
    }

    if let Ok(size_str) = std::env::var("SLATEDB_CACHE_SIZE_GB") {
        if size_str.parse::<f64>().is_err() {
            eprintln!("{BOLD}{RED}Error:{RESET} SLATEDB_CACHE_SIZE_GB must be a valid number");
            eprintln!("Current value: {RED}'{size_str}'{RESET}");
            eprintln!("Example: {GREEN}SLATEDB_CACHE_SIZE_GB=10{RESET}");
            std::process::exit(1);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::EnvFilter;

    validate_environment()?;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().collect();

    let url = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "s3://slatedb".to_string());

    let cache_config = CacheConfig {
        root_folder: std::env::var("SLATEDB_CACHE_DIR")
            .expect("SLATEDB_CACHE_DIR environment variable is required"),
        max_cache_size_gb: std::env::var("SLATEDB_CACHE_SIZE_GB")
            .expect("SLATEDB_CACHE_SIZE_GB environment variable is required")
            .parse::<f64>()
            .expect("SLATEDB_CACHE_SIZE_GB must be a valid number"),
        memory_cache_size_gb: std::env::var("ZEROFS_MEMORY_CACHE_SIZE_GB")
            .ok()
            .and_then(|s| s.parse::<f64>().ok()),
    };

    if cache_config.max_cache_size_gb <= 0.0 {
        return Err("SLATEDB_CACHE_SIZE_GB must be a positive number".into());
    }

    let (object_store, path_from_url) = object_store::parse_url_opts(
        &url.parse()?,
        std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v)),
    )?;
    let object_store = Arc::from(object_store);

    let actual_db_path = path_from_url.to_string();

    info!("Starting ZeroFS NFS server with {} backend", object_store);
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

    let password = std::env::var("ZEROFS_ENCRYPTION_PASSWORD")
        .expect("ZEROFS_ENCRYPTION_PASSWORD should be validated");

    info!("Loading or initializing encryption key");

    let temp_fs = SlateDbFs::dangerous_new_with_object_store_unencrypted_for_key_management_only(
        object_store.clone(),
        cache_config.clone(),
        actual_db_path.clone(),
    )
    .await?;

    if let Ok(new_password) = std::env::var("ZEROFS_NEW_PASSWORD") {
        info!("Password change requested - changing encryption password");

        match key_management::change_encryption_password(&temp_fs.db, &password, &new_password)
            .await
        {
            Ok(()) => {
                info!("✓ Encryption password changed successfully!");
                info!("");
                info!("IMPORTANT: Please restart the program with:");
                info!("  - ZEROFS_ENCRYPTION_PASSWORD set to your new password");
                info!("  - ZEROFS_NEW_PASSWORD environment variable removed");
                info!("");
                info!("Example:");
                info!("  unset ZEROFS_NEW_PASSWORD");
                info!(
                    "  ZEROFS_ENCRYPTION_PASSWORD='{}' zerofs {}",
                    new_password, url
                );
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("✗ Failed to change encryption password: {e}");
                eprintln!(
                    "  This usually means the database was not initialized yet: cannot change a password that does not exists."
                );
                std::process::exit(1);
            }
        }
    }

    let encryption_key =
        key_management::load_or_init_encryption_key(&temp_fs.db, &password).await?;

    temp_fs.db.flush().await?;
    temp_fs.db.close().await?;
    drop(temp_fs);

    info!("Encryption key loaded successfully");

    let fs = SlateDbFs::new_with_object_store(
        object_store,
        cache_config,
        actual_db_path,
        encryption_key,
    )
    .await?;

    // Parse NBD device configuration from environment
    let nbd_ports = std::env::var("ZEROFS_NBD_PORTS").unwrap_or_else(|_| "".to_string());
    let nbd_device_sizes =
        std::env::var("ZEROFS_NBD_DEVICE_SIZES_GB").unwrap_or_else(|_| "".to_string());
    let nbd_host = std::env::var("ZEROFS_NBD_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

    let fs_arc = Arc::new(fs);

    // Start NFS server
    let zerofs_nfs_host =
        std::env::var("ZEROFS_NFS_HOST").unwrap_or_else(|_| DEFAULT_NFS_HOST.to_string());

    let zerofs_nfs_host_port = std::env::var("ZEROFS_NFS_HOST_PORT")
        .map_or_else(|_| Ok(DEFAULT_NFS_PORT), |port| port.parse::<u32>())
        .expect("ZEROFS_NFS_HOST_PORT must be a valid port number");

    let nfs_fs = Arc::clone(&fs_arc);
    let nfs_handle = tokio::spawn(async move {
        match crate::nfs::start_nfs_server_with_config(
            (*nfs_fs).clone(),
            &zerofs_nfs_host,
            zerofs_nfs_host_port,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(e) => Err(std::io::Error::other(e.to_string())),
        }
    });

    // Start NBD servers if configured
    let mut nbd_handles = Vec::new();
    if !nbd_ports.is_empty() {
        // Create .nbd directory once before starting any NBD servers
        {
            use zerofs_nfsserve::nfs::{nfsstring, sattr3, set_mode3};
            use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

            let auth = AuthContext {
                uid: 0,
                gid: 0,
                gids: vec![],
            };
            let nbd_name = nfsstring(b".nbd".to_vec());

            match fs_arc.lookup(&auth, 0, &nbd_name).await {
                Ok(_) => info!(".nbd directory already exists"),
                Err(_) => {
                    let attr = sattr3 {
                        mode: set_mode3::mode(0o755),
                        uid: zerofs_nfsserve::nfs::set_uid3::uid(0),
                        gid: zerofs_nfsserve::nfs::set_gid3::gid(0),
                        size: zerofs_nfsserve::nfs::set_size3::Void,
                        atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                        mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                    };
                    fs_arc
                        .mkdir(&auth, 0, &nbd_name, &attr)
                        .await
                        .map_err(|e| format!("Failed to create .nbd directory: {e:?}"))?;
                    info!("Created .nbd directory");
                }
            }
        }
        let ports: Vec<u16> = nbd_ports
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        let sizes: Vec<u64> = nbd_device_sizes
            .split(',')
            .filter_map(|s| s.trim().parse::<f64>().ok())
            .map(|gb| (gb * 1024.0 * 1024.0 * 1024.0) as u64)
            .collect();

        if ports.len() != sizes.len() {
            return Err("ZEROFS_NBD_PORTS and ZEROFS_NBD_DEVICE_SIZES_GB must have the same number of entries".into());
        }

        for (&port, &size) in ports.iter().zip(sizes.iter()) {
            let mut nbd_server = NBDServer::new(Arc::clone(&fs_arc), nbd_host.clone(), port);
            nbd_server.add_device(format!("device_{port}"), size);

            info!(
                "Starting NBD server on {}:{} with device size {} GB",
                nbd_host,
                port,
                size as f64 / (1024.0 * 1024.0 * 1024.0)
            );

            let nbd_handle = tokio::spawn(async move {
                if let Err(e) = nbd_server.start().await {
                    if e.kind() == std::io::ErrorKind::InvalidInput
                        && e.to_string().contains("size mismatch")
                    {
                        eprintln!("NBD Device Size Error: {e}");
                        eprintln!();
                        eprintln!("To fix this issue:");
                        eprintln!("   • Use the same device size as before, OR");
                        eprintln!("   • Delete the existing device file via NFS and restart");
                        eprintln!("   • Example: rm /mnt/zerofs/.nbd/device_{port}");
                        std::process::exit(1);
                    }
                    Err(e)
                } else {
                    Ok(())
                }
            });
            nbd_handles.push(nbd_handle);
        }
    }

    let gc_fs = Arc::clone(&fs_arc);
    let gc_handle = tokio::spawn(async move {
        info!("Starting garbage collection task (runs continuously)");
        loop {
            if let Err(e) = gc_fs.run_garbage_collection().await {
                tracing::error!("Garbage collection failed: {:?}", e);
            }
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    });

    let stats_fs = Arc::clone(&fs_arc);
    let stats_handle = tokio::spawn(async move {
        info!("Starting stats reporting task (reports every 5 seconds)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            info!("\n{}", stats_fs.stats.report());
        }
    });

    // Start 9P server
    let ninep_host =
        std::env::var("ZEROFS_9P_HOST").unwrap_or_else(|_| DEFAULT_9P_HOST.to_string());
    let ninep_port = std::env::var("ZEROFS_9P_PORT")
        .map_or_else(|_| Ok(DEFAULT_9P_PORT), |port| port.parse::<u16>())
        .expect("ZEROFS_9P_PORT must be a valid port number");

    let ninep_fs = Arc::clone(&fs_arc);
    let ninep_addr = format!("{ninep_host}:{ninep_port}")
        .parse()
        .expect("Invalid 9P server address");
    let ninep_server = crate::ninep::NinePServer::new(ninep_fs, ninep_addr);
    let ninep_handle = tokio::spawn(async move { ninep_server.start().await });

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    if nbd_handles.is_empty() {
        tokio::select! {
            result = nfs_handle => {
                result??;
            }
            result = ninep_handle => {
                result??;
            }
            _ = gc_handle => {
                // GC task should never complete
            }
            _ = stats_handle => {
                // Stats task should never complete
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT, shutting down gracefully...");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down gracefully...");
            }
        }
    } else {
        tokio::select! {
            result = nfs_handle => {
                result??;
            }
            result = ninep_handle => {
                result??;
            }
            result = futures::future::select_all(nbd_handles) => {
                match result {
                    (Ok(Ok(())), _, _) => {
                        info!("NBD server completed successfully");
                    }
                    (Ok(Err(e)), _, _) => {
                        return Err(e.into());
                    }
                    (Err(e), _, _) => {
                        return Err(e.into());
                    }
                }
            }
            _ = gc_handle => {
            }
            _ = stats_handle => {
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT, shutting down gracefully...");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down gracefully...");
            }
        }
    }

    Ok(())
}
