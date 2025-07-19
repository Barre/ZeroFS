mod cache;
mod encryption;
mod filesystem;
mod inode;
mod key_management;
mod lock_manager;
mod nbd;
mod nfs;
mod operations;
mod permissions;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod posix_tests;

use crate::filesystem::{CacheConfig, S3Config, SlateDbFs};
use crate::nbd::NBDServer;
use std::sync::Arc;
use tracing::info;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const HOSTPORT: u32 = 2049;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // Default: info for zerofs, error for slatedb to reduce noise
        EnvFilter::new("error,zerofs=info")
    });

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().collect();

    let s3_config = S3Config {
        endpoint: std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "".to_string()),
        bucket_name: std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "slatedb".to_string()),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "".to_string()),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
            .unwrap_or_else(|_| "".to_string()),
        region: std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        allow_http: std::env::var("AWS_ALLOW_HTTP").unwrap_or_else(|_| "false".to_string())
            == "true",
    };

    let db_path = args.get(1).cloned().unwrap_or_else(|| "test".to_string());

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

    info!("Starting SlateDB NFS server with S3 backend");

    if !s3_config.endpoint.is_empty() {
        info!("S3 Endpoint: {}", s3_config.endpoint);
    }
    info!("S3 Bucket: {}", s3_config.bucket_name);
    info!("S3 Region: {}", s3_config.region);
    info!("S3 Path: {}", db_path);
    info!("Cache Directory: {}", cache_config.root_folder);
    info!("Cache Size: {} GB", cache_config.max_cache_size_gb);

    let password = match std::env::var("ZEROFS_ENCRYPTION_PASSWORD") {
        Ok(pwd) => pwd,
        Err(_) => {
            eprintln!("Error: ZEROFS_ENCRYPTION_PASSWORD environment variable is required");
            eprintln!();
            eprintln!("Usage:");
            eprintln!("  ZEROFS_ENCRYPTION_PASSWORD='your-password' zerofs <path>");
            eprintln!();
            eprintln!("To change password:");
            eprintln!(
                "  ZEROFS_ENCRYPTION_PASSWORD='current-password' ZEROFS_NEW_PASSWORD='new-password' zerofs <path>"
            );
            std::process::exit(1);
        }
    };

    info!("Loading or initializing encryption key");

    let temp_fs = SlateDbFs::dangerous_new_with_s3_unencrypted_for_key_management_only(
        s3_config.clone(),
        cache_config.clone(),
        db_path.clone(),
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
                    new_password, db_path
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

    // Flush and properly close the temporary database to avoid fencing issues
    temp_fs.db.flush().await?;
    // Give background tasks a moment to complete after flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    temp_fs.db.close().await?;
    drop(temp_fs);

    info!("Encryption key loaded successfully");

    let fs = SlateDbFs::new_with_s3(s3_config, cache_config, db_path, encryption_key).await?;

    // Parse NBD device configuration from environment
    let nbd_ports = std::env::var("ZEROFS_NBD_PORTS").unwrap_or_else(|_| "".to_string());
    let nbd_device_sizes =
        std::env::var("ZEROFS_NBD_DEVICE_SIZES_GB").unwrap_or_else(|_| "".to_string());

    let fs_arc = Arc::new(fs);

    // Start NFS server
    let nfs_fs = Arc::clone(&fs_arc);
    let nfs_handle = tokio::spawn(async move {
        match crate::nfs::start_nfs_server((*nfs_fs).clone(), HOSTPORT).await {
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
            let mut nbd_server = NBDServer::new(Arc::clone(&fs_arc), port);
            nbd_server.add_device(format!("device_{port}"), size);

            info!(
                "Starting NBD server on port {} with device size {} GB",
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

    // Wait for either NFS or any NBD server to complete (or error)
    if nbd_handles.is_empty() {
        nfs_handle.await??;
    } else {
        tokio::select! {
            result = nfs_handle => {
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
        }
    }

    Ok(())
}
