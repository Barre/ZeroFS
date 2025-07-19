mod cache;
mod encryption;
mod filesystem;
mod inode;
mod key_management;
mod lock_manager;
mod nbd;
mod operations;
mod permissions;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod posix_tests;

use crate::cache::{CacheKey, CacheValue};
use crate::filesystem::{CacheConfig, S3Config, SlateDbFs};
use crate::inode::Inode;
use crate::nbd::NBDServer;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};
use zerofs_nfsserve::nfs::{ftype3, *};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem, ReadDirResult, VFSCapabilities};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[async_trait]
impl NFSFileSystem for SlateDbFs {
    fn root_dir(&self) -> fileid3 {
        0
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    async fn lookup(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let filename_str = String::from_utf8_lossy(filename);
        debug!("lookup called: dirid={}, filename={}", dirid, filename_str);

        // Acquire read lock on the directory for the entire lookup operation
        let _guard = self.lock_manager.acquire_read(dirid).await;

        let dir_inode = self.load_inode(dirid).await?;

        match dir_inode {
            Inode::Directory(ref _dir) => {
                use crate::permissions::{AccessMode, Credentials, check_access};
                let creds = Credentials::from_auth_context(auth);
                check_access(&dir_inode, &creds, AccessMode::Execute)?;
                let name = filename_str.to_string();

                let cache_key = CacheKey::DirEntry {
                    dir_id: dirid,
                    name: name.clone(),
                };
                if let Some(CacheValue::DirEntry(inode_id)) =
                    self.dir_entry_cache.get(cache_key).await
                {
                    debug!("lookup cache hit: {} -> inode {}", name, inode_id);
                    return Ok(inode_id);
                }

                let entry_key = SlateDbFs::dir_entry_key(dirid, &name);

                match self
                    .db
                    .get_bytes(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                {
                    Some(entry_data) => {
                        let mut bytes = [0u8; 8];
                        bytes.copy_from_slice(&entry_data[..8]);
                        let inode_id = u64::from_le_bytes(bytes);
                        debug!("lookup found: {} -> inode {}", name, inode_id);

                        let cache_key = crate::cache::CacheKey::DirEntry {
                            dir_id: dirid,
                            name: name.clone(),
                        };
                        let cache_value = crate::cache::CacheValue::DirEntry(inode_id);
                        self.dir_entry_cache.insert(cache_key, cache_value, false);

                        Ok(inode_id)
                    }
                    None => {
                        debug!("lookup not found: {} in directory", name);
                        Err(nfsstat3::NFS3ERR_NOENT)
                    }
                }
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    async fn getattr(&self, _auth: &AuthContext, id: fileid3) -> Result<fattr3, nfsstat3> {
        debug!("getattr called: id={}", id);
        // Acquire read lock for consistent view of metadata
        let _guard = self.lock_manager.acquire_read(id).await;
        let inode = self.load_inode(id).await?;
        Ok(inode.to_fattr3(id))
    }

    async fn read(
        &self,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        debug!("read called: id={}, offset={}, count={}", id, offset, count);
        self.process_read_file(auth, id, offset, count).await
    }

    async fn write(
        &self,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3> {
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        self.process_write(auth, id, offset, data).await
    }

    async fn create(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let filename_str = String::from_utf8_lossy(filename);
        debug!("create called: dirid={}, filename={}", dirid, filename_str);

        self.process_create(auth, dirid, filename, attr).await
    }

    async fn create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        debug!(
            "create_exclusive called: dirid={}, filename={:?}",
            dirid, filename
        );

        self.process_create_exclusive(auth, dirid, filename).await
    }

    async fn mkdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        dirname: &filename3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let dirname_str = String::from_utf8_lossy(dirname);
        debug!("mkdir called: dirid={}, dirname={}", dirid, dirname_str);

        self.process_mkdir(auth, dirid, dirname, attr).await
    }

    async fn remove(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!("remove called: dirid={}, filename={:?}", dirid, filename);

        self.process_remove(auth, dirid, filename).await
    }

    async fn rename(
        &self,
        auth: &AuthContext,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!(
            "rename called: from_dirid={}, to_dirid={}",
            from_dirid, to_dirid
        );

        self.process_rename(auth, from_dirid, from_filename, to_dirid, to_filename)
            .await
    }

    async fn readdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        debug!(
            "readdir called: dirid={}, start_after={}, max_entries={}",
            dirid, start_after, max_entries
        );
        self.process_readdir(auth, dirid, start_after, max_entries)
            .await
    }

    async fn setattr(
        &self,
        auth: &AuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        debug!("setattr called: id={}, setattr={:?}", id, setattr);

        self.process_setattr(auth, id, setattr).await
    }

    async fn symlink(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "symlink called: dirid={}, linkname={:?}, target={:?}",
            dirid, linkname, symlink
        );

        self.process_symlink(auth, dirid, &linkname.0, &symlink.0, *attr)
            .await
    }

    async fn readlink(&self, _auth: &AuthContext, id: fileid3) -> Result<nfspath3, nfsstat3> {
        debug!("readlink called: id={}", id);

        let _guard = self.lock_manager.acquire_read(id).await;
        let inode = self.load_inode(id).await?;
        match inode {
            Inode::Symlink(symlink) => Ok(nfspath3 { 0: symlink.target }),
            _ => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    async fn mknod(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
        ftype: ftype3,
        attr: &sattr3,
        spec: Option<&specdata3>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "mknod called: dirid={}, filename={:?}, ftype={:?}",
            dirid, filename, ftype
        );

        // Extract device numbers if this is a device file
        let rdev = match ftype {
            ftype3::NF3CHR | ftype3::NF3BLK => {
                // Extract device numbers from specdata3
                spec.map(|s| (s.specdata1, s.specdata2))
            }
            _ => None,
        };

        self.process_mknod(auth, dirid, &filename.0, ftype, attr, rdev)
            .await
    }

    async fn link(
        &self,
        auth: &AuthContext,
        fileid: fileid3,
        linkdirid: fileid3,
        linkname: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!(
            "link called: fileid={}, linkdirid={}, linkname={:?}",
            fileid, linkdirid, linkname
        );
        self.process_link(auth, fileid, linkdirid, &linkname.0)
            .await
    }

    async fn commit(
        &self,
        _auth: &AuthContext,
        fileid: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<writeverf3, nfsstat3> {
        debug!(
            "commit called: fileid={}, offset={}, count={}",
            fileid, offset, count
        );

        match self.db.flush().await {
            Ok(_) => {
                debug!("commit successful for file {}", fileid);
                Ok(self.get_write_verf())
            }
            Err(e) => {
                tracing::error!("commit failed for file {}: {}", fileid, e);
                Err(nfsstat3::NFS3ERR_IO)
            }
        }
    }
}

const DEFAULT_NFS_IP: &'static str = "127.0.0.1";
const DEFAULT_NFS_PORT: u32 = 2049;

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
    let zerofs_nfs_host_ip = std::env::var("ZEROFS_NFS_HOST_IP")
        .unwrap_or_else(|_| DEFAULT_NFS_IP.to_string());

    let zerofs_nfs_host_port = std::env::var("ZEROFS_NFS_HOST_PORT")
        .map_or_else(|_| Ok(DEFAULT_NFS_PORT), |port| port.parse::<u32>())
        .expect("ZEROFS_NFS_HOST_PORT must be a valid port number");

    let nfs_fs = Arc::clone(&fs_arc);
    let nfs_handle = tokio::spawn(async move {
        let listener =
            NFSTcpListener::bind(&format!("{zerofs_nfs_host_ip}:{zerofs_nfs_host_port}"), (*nfs_fs).clone()).await?;
        info!("NFS server listening on {zerofs_nfs_host_ip}:{zerofs_nfs_host_port}");
        listener.handle_forever().await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::SlateDbFs;
    use crate::test_helpers::test_helpers_mod::{filename, test_auth};
    use zerofs_nfsserve::nfs::{set_atime, set_mtime};

    #[tokio::test]
    async fn test_nfs_filesystem_trait() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        assert_eq!(fs.root_dir(), 0);
        assert!(matches!(fs.capabilities(), VFSCapabilities::ReadWrite));
    }

    #[tokio::test]
    async fn test_lookup() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"test.txt"))
            .await
            .unwrap();
        assert_eq!(found_id, file_id);

        let result = fs
            .lookup(&test_auth(), 0, &filename(b"nonexistent.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_getattr() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let fattr = fs.getattr(&test_auth(), 0).await.unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));
        assert_eq!(fattr.fileid, 0);
    }

    #[tokio::test]
    async fn test_read_write() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let data = b"Hello, NFS!";
        let fattr = fs.write(&test_auth(), file_id, 0, data).await.unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();
        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_create_exclusive() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let file_id = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await
            .unwrap();
        assert!(file_id > 0);

        let result = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_EXIST)));
    }

    #[tokio::test]
    async fn test_mkdir_and_readdir() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .mkdir(&test_auth(), 0, &filename(b"mydir"), &sattr3::default())
            .await
            .unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));

        let (_file_id, _) = fs
            .create(
                &test_auth(),
                dir_id,
                &filename(b"file_in_dir.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        let result = fs.readdir(&test_auth(), dir_id, 0, 10).await.unwrap();
        assert!(result.end);

        let names: Vec<&[u8]> = result.entries.iter().map(|e| e.name.0.as_ref()).collect();
        assert!(names.contains(&b".".as_ref()));
        assert!(names.contains(&b"..".as_ref()));
        assert!(names.contains(&b"file_in_dir.txt".as_ref()));
    }

    #[tokio::test]
    async fn test_rename() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"original.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"test data")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            0,
            &filename(b"original.txt"),
            0,
            &filename(b"renamed.txt"),
        )
        .await
        .unwrap();

        let result = fs.lookup(&test_auth(), 0, &filename(b"original.txt")).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"renamed.txt"))
            .await
            .unwrap();
        assert_eq!(found_id, file_id);
    }

    #[tokio::test]
    async fn test_remove() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"to_remove.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.remove(&test_auth(), 0, &filename(b"to_remove.txt"))
            .await
            .unwrap();

        let result = fs
            .lookup(&test_auth(), 0, &filename(b"to_remove.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));

        let result = fs.getattr(&test_auth(), file_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_setattr() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, initial_fattr) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        // Test changing mode (which any owner can do)
        let setattr_mode = sattr3 {
            mode: set_mode3::mode(0o755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, setattr_mode)
            .await
            .unwrap();
        assert_eq!(fattr.mode, 0o755);

        // Test that uid/gid remain unchanged when not specified
        assert_eq!(fattr.uid, initial_fattr.uid);
        assert_eq!(fattr.gid, initial_fattr.gid);

        // Test changing size (truncate)
        let setattr_size = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(1024),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, setattr_size)
            .await
            .unwrap();
        assert_eq!(fattr.size, 1024);
    }

    #[tokio::test]
    async fn test_symlink_and_readlink() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let target = nfspath3 {
            0: b"/path/to/target".to_vec(),
        };
        let attr = sattr3::default();

        let (link_id, fattr) = fs
            .symlink(&test_auth(), 0, &filename(b"mylink"), &target, &attr)
            .await
            .unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3LNK));

        let read_target = fs.readlink(&test_auth(), link_id).await.unwrap();
        assert_eq!(read_target.0, target.0);
    }

    #[tokio::test]
    async fn test_complex_filesystem_operations() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (docs_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"documents"), &sattr3::default())
            .await
            .unwrap();
        let (images_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"images"), &sattr3::default())
            .await
            .unwrap();

        let (file1_id, _) = fs
            .create(
                &test_auth(),
                docs_dir,
                &filename(b"readme.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();
        let (file2_id, _) = fs
            .create(
                &test_auth(),
                docs_dir,
                &filename(b"notes.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();
        let (file3_id, _) = fs
            .create(
                &test_auth(),
                images_dir,
                &filename(b"photo.jpg"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file1_id, 0, b"This is the readme")
            .await
            .unwrap();
        fs.write(&test_auth(), file2_id, 0, b"These are my notes")
            .await
            .unwrap();
        fs.write(&test_auth(), file3_id, 0, b"JPEG data...")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            docs_dir,
            &filename(b"readme.txt"),
            images_dir,
            &filename(b"readme.txt"),
        )
        .await
        .unwrap();

        let docs_entries = fs.readdir(&test_auth(), docs_dir, 0, 10).await.unwrap();
        assert_eq!(docs_entries.entries.len(), 3);

        let images_entries = fs.readdir(&test_auth(), images_dir, 0, 10).await.unwrap();
        assert_eq!(images_entries.entries.len(), 4);

        let (data, _) = fs.read(&test_auth(), file1_id, 0, 100).await.unwrap();
        assert_eq!(data, b"This is the readme");
    }
}
