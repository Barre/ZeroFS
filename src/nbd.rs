use std::sync::Arc;
use tokio_nbd::device::NbdDriver;
use tokio_nbd::errors::{OptionReplyError, ProtocolError};
use tokio_nbd::flags::{CommandFlags, ServerFeatures};
use tracing::{debug, error};

use crate::filesystem::SlateDbFs;
use zerofs_nfsserve::nfs::nfsstat3;

pub struct ZeroFSDevice {
    filesystem: Arc<SlateDbFs>,
    name: String,
    size: u64,
    device_inode: u64,
}

fn map_err(e: nfsstat3) -> ProtocolError {
    match e {
        nfsstat3::NFS3ERR_IO => ProtocolError::IO,
        nfsstat3::NFS3ERR_NOSPC => ProtocolError::NoSpaceLeft,
        nfsstat3::NFS3ERR_PERM => ProtocolError::CommandNotPermitted,
        nfsstat3::NFS3ERR_ACCES => ProtocolError::CommandNotPermitted,
        nfsstat3::NFS3ERR_FBIG => ProtocolError::ValueTooLarge,
        nfsstat3::NFS3ERR_NOTSUPP => ProtocolError::CommandNotSupported,
        nfsstat3::NFS3ERR_SERVERFAULT => ProtocolError::IO,
        nfsstat3::NFS3ERR_ROFS => ProtocolError::CommandNotPermitted,
        _ => ProtocolError::InvalidArgument,
    }
}

// Initialize device and return the inode for the device

impl NbdDriver for ZeroFSDevice {
    fn get_features(&self) -> tokio_nbd::flags::ServerFeatures {
        ServerFeatures::SEND_FUA
            | ServerFeatures::SEND_TRIM
            | ServerFeatures::SEND_FLUSH
            | ServerFeatures::SEND_WRITE_ZEROES
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    async fn get_read_only(&self) -> std::result::Result<bool, OptionReplyError> {
        Ok(false)
    }

    async fn get_block_size(&self) -> std::result::Result<(u32, u32, u32), OptionReplyError> {
        // Default block size is 512 bytes, which is common for NBD
        Ok((512, 1024, 4096)) // (minimum, preferred, maximum)
    }

    async fn get_canonical_name(&self) -> std::result::Result<String, OptionReplyError> {
        Ok(self.name.clone())
    }

    async fn get_description(&self) -> std::result::Result<String, OptionReplyError> {
        Ok(format!("NBD device: {}", self.name))
    }

    async fn get_device_size(&self) -> std::result::Result<u64, OptionReplyError> {
        Ok(self.size)
    }

    async fn read(
        &self,
        _flags: tokio_nbd::flags::CommandFlags,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Vec<u8>, ProtocolError> {
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        // Handle zero-length read
        // TODO Consider having the driver handle this edge case
        if length == 0 {
            return Ok(vec![]);
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        let (data, _eof) = self
            .filesystem
            .read(&auth, self.device_inode, offset, length)
            .await
            .map_err(|e| {
                error!("NBD read failed: {:?}", e);
                map_err(e)
            })?;

        Ok(data)
    }

    async fn write(
        &self,
        flags: tokio_nbd::flags::CommandFlags,
        offset: u64,
        data: Vec<u8>,
    ) -> std::result::Result<(), ProtocolError> {
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        // Handle zero-length write
        // TODO Consider having the driver handle this edge case
        if data.is_empty() {
            return Ok(());
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        self.filesystem
            .write(&auth, self.device_inode, offset, &data)
            .await
            .map_err(|e| {
                error!("NBD write failed: {:?}", e);
                map_err(e)
            })?;

        if flags.contains(CommandFlags::FUA) {
            // If FUA is set, we should flush the filesystem to ensure data is written
            self.filesystem.db.flush().await.map_err(|e| {
                error!("Failed to flush database after write: {}", e);
                ProtocolError::IO
            })?;
        }

        Ok(())
    }

    async fn flush(
        &self,
        _flags: tokio_nbd::flags::CommandFlags,
    ) -> std::result::Result<(), ProtocolError> {
        self.filesystem.db.flush().await.map_err(|e| {
            error!("Failed to flush database: {}", e);
            ProtocolError::IO
        })
    }

    async fn trim(
        &self,
        flags: tokio_nbd::flags::CommandFlags,
        offset: u64,
        length: u32,
    ) -> std::result::Result<(), ProtocolError> {
        use zerofs_nfsserve::vfs::AuthContext;

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        self.filesystem
            .trim(&auth, self.device_inode, offset, length as u64)
            .await
            .map_err(|e| {
                error!("NBD trim failed: {:?}", e);
                map_err(e)
            })?;

        if flags.contains(CommandFlags::FUA) {
            // If FUA is set, we should flush the filesystem to ensure data is written
            self.filesystem.db.flush().await.map_err(|e| {
                error!("Failed to flush database after trim: {}", e);
                ProtocolError::IO
            })?;
        }

        Ok(())
    }

    async fn write_zeroes(
        &self,
        flags: tokio_nbd::flags::CommandFlags,
        offset: u64,
        length: u32,
    ) -> std::result::Result<(), ProtocolError> {
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        // Handle zero-length write_zeroes
        if length == 0 {
            return Ok(());
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let zero_data = vec![0u8; length as usize];

        self.filesystem
            .write(&auth, self.device_inode, offset, &zero_data)
            .await
            .map_err(|e| {
                error!("NBD write_zeroes failed: {:?}", e);
                map_err(e)
            })?;

        if flags.contains(CommandFlags::FUA) {
            // If FUA is set, we should flush the filesystem to ensure data is written
            self.filesystem.db.flush().await.map_err(|e| {
                error!("Failed to flush database after write_zeroes: {}", e);
                ProtocolError::IO
            })?;
        }

        Ok(())
    }

    async fn disconnect(
        &self,
        _flags: tokio_nbd::flags::CommandFlags,
    ) -> std::result::Result<(), ProtocolError> {
        // Flush the filesystem to ensure all writes are committed
        self.filesystem.db.flush().await.map_err(|e| {
            error!("Failed to flush database on disconnect: {}", e);
            ProtocolError::IO
        })?;

        Ok(())
    }
}

pub(crate) struct DeviceManager {
    pub(crate) filesystem: Arc<SlateDbFs>,
    pub(crate) devices: Vec<ZeroFSDevice>,
}

impl DeviceManager {
    pub fn new(filesystem: Arc<SlateDbFs>) -> Self {
        Self {
            filesystem,
            devices: vec![],
        }
    }

    pub(crate) async fn add_device(&mut self, name: String, size: u64) -> std::io::Result<()> {
        // Initialize the device file
        let device_inode = self
            .initialize_device(&self.filesystem, &name, size)
            .await?;

        debug!(
            "Initialized NBD device {} with inode {}",
            name, device_inode
        );
        // Store the full device information
        let device = ZeroFSDevice {
            filesystem: Arc::clone(&self.filesystem),
            name: name.clone(),
            size,
            device_inode,
        };
        self.devices.push(device);

        Ok(())
    }

    async fn initialize_device(
        &self,
        filesystem: &SlateDbFs,
        device_name: &str,
        device_size: u64,
    ) -> std::io::Result<u64> {
        use zerofs_nfsserve::nfs::{nfsstring, sattr3, set_mode3};
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let nbd_name = nfsstring(b".nbd".to_vec());
        let device_name = nfsstring(device_name.as_bytes().to_vec());

        // Check if device file exists, create it if not
        let nbd_dir_inode = filesystem.lookup(&auth, 0, &nbd_name).await.map_err(|e| {
            std::io::Error::other(format!("Failed to lookup .nbd directory: {e:?}"))
        })?;

        match filesystem.lookup(&auth, nbd_dir_inode, &device_name).await {
            Ok(device_inode) => {
                let existing_attr = filesystem.getattr(&auth, device_inode).await.map_err(|e| {
                    std::io::Error::other(format!("Failed to get device attributes: {e:?}"))
                })?;

                if existing_attr.size != device_size {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "NBD device {} size mismatch: existing size is {} bytes, requested size is {} bytes. Cannot resize existing devices.",
                            device_name, existing_attr.size, device_size
                        ),
                    ));
                }

                debug!(
                    "NBD device file {} already exists with correct size {}",
                    device_name, device_size
                );
                Ok(device_inode)
            }
            Err(_) => {
                debug!(
                    "Creating NBD device file {} with size {}",
                    device_name, device_size
                );
                let attr = sattr3 {
                    mode: set_mode3::mode(0o600),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(0),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(0),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                };

                let (device_inode, _) = filesystem
                    .create(&auth, nbd_dir_inode, &device_name, attr)
                    .await
                    .map_err(|e| {
                        std::io::Error::other(format!("Failed to create device file: {e:?}"))
                    })?;

                // Set the file size by writing a zero byte at the end
                if device_size > 0 {
                    let data = vec![0u8; 1];
                    filesystem
                        .write(&auth, device_inode, device_size - 1, &data)
                        .await
                        .map_err(|e| {
                            std::io::Error::other(format!("Failed to set device size: {e:?}"))
                        })?;
                }

                Ok(device_inode)
            }
        }
    }
}
