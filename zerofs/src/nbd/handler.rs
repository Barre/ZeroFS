use super::error::{CommandError, CommandResult, NBDError, Result};
use super::protocol::{
    NBD_INFO_EXPORT, NBD_READDIR_DEFAULT_LIMIT, NBD_REP_ACK, NBD_REP_ERR_INVALID,
    NBD_REP_ERR_UNKNOWN, NBD_REP_INFO, NBD_REP_SERVER, NBD_ZERO_CHUNK_SIZE, NBDInfoExport,
    TRANSMISSION_FLAGS,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::tracing::FileOperation;
use crate::fs::types::AuthContext;
use bytes::Bytes;
use deku::DekuContainerWrite;
use std::sync::Arc;
use tracing::debug;

/// Response to send back for an option
pub struct OptionReply {
    pub reply_type: u32,
    pub data: Vec<u8>,
}

impl OptionReply {
    pub fn new(reply_type: u32, data: Vec<u8>) -> Self {
        Self { reply_type, data }
    }

    pub fn ack() -> Self {
        Self::new(NBD_REP_ACK, Vec::new())
    }

    pub fn error(reply_type: u32) -> Self {
        Self::new(reply_type, Vec::new())
    }
}

/// Result of processing an option - may return device if negotiation complete
pub enum OptionResult {
    /// Continue negotiation, send these replies
    Continue(Vec<OptionReply>),
    /// Negotiation complete, use this device
    Done(NBDDevice, Vec<OptionReply>),
    /// Error during processing
    Error(NBDError, Vec<OptionReply>),
}

/// NBD device descriptor
#[derive(Clone)]
pub struct NBDDevice {
    pub name: Vec<u8>,
    pub size: u64,
    pub inode: u64,
}

impl NBDDevice {
    pub fn info_export(&self) -> NBDInfoExport {
        NBDInfoExport {
            info_type: NBD_INFO_EXPORT,
            size: self.size,
            transmission_flags: TRANSMISSION_FLAGS,
        }
    }
}

/// Handler for NBD protocol operations
pub struct NBDHandler {
    filesystem: Arc<ZeroFS>,
}

impl NBDHandler {
    pub fn new(filesystem: Arc<ZeroFS>) -> Self {
        Self { filesystem }
    }

    /// Get the .nbd directory inode
    async fn nbd_dir_inode(&self) -> Result<u64> {
        self.filesystem
            .directory_store
            .get(0, b".nbd")
            .await
            .map_err(NBDError::from)
    }

    /// List all available NBD devices
    pub async fn list_devices(&self) -> Result<Vec<NBDDevice>> {
        let auth = AuthContext::default();
        let nbd_dir_inode = self.nbd_dir_inode().await?;

        let entries = self
            .filesystem
            .readdir(&auth, nbd_dir_inode, 0, NBD_READDIR_DEFAULT_LIMIT)
            .await?;

        let mut devices = Vec::new();
        for entry in &entries.entries {
            let name = &entry.name;
            if name == b"." || name == b".." {
                continue;
            }

            let inode = self.filesystem.inode_store.get(entry.fileid).await?;

            if let Inode::File(file_inode) = inode {
                devices.push(NBDDevice {
                    name: name.to_vec(),
                    size: file_inode.size,
                    inode: entry.fileid,
                });
            }
        }

        Ok(devices)
    }

    /// Rreturns list of all devices
    pub async fn list(&self) -> OptionResult {
        match self.list_devices().await {
            Ok(devices) => {
                let mut replies = Vec::new();
                for device in devices {
                    let mut reply_data = Vec::new();
                    reply_data.extend_from_slice(&(device.name.len() as u32).to_be_bytes());
                    reply_data.extend_from_slice(&device.name);
                    replies.push(OptionReply::new(NBD_REP_SERVER, reply_data));
                }
                replies.push(OptionReply::ack());
                OptionResult::Continue(replies)
            }
            Err(e) => OptionResult::Error(e, vec![]),
        }
    }

    /// Returns device info without completing negotiation
    pub async fn info(&self, data: &[u8]) -> OptionResult {
        if data.len() < 4 {
            return OptionResult::Continue(vec![OptionReply::error(NBD_REP_ERR_INVALID)]);
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            return OptionResult::Error(
                NBDError::Protocol("Invalid INFO option length".to_string()),
                vec![OptionReply::error(NBD_REP_ERR_INVALID)],
            );
        }

        let name = &data[4..4 + name_len];
        debug!(
            "INFO option: requested export name '{}' (name_len: {})",
            String::from_utf8_lossy(name),
            name_len
        );

        match self.get_device(name).await {
            Ok(device) => match device.info_export().to_bytes() {
                Ok(info_bytes) => OptionResult::Continue(vec![
                    OptionReply::new(NBD_REP_INFO, info_bytes),
                    OptionReply::ack(),
                ]),
                Err(e) => OptionResult::Error(
                    NBDError::Protocol(format!("Failed to serialize info: {:?}", e)),
                    vec![],
                ),
            },
            Err(e) => {
                debug!(
                    "INFO option: device '{}' not found: {:?}",
                    String::from_utf8_lossy(name),
                    e
                );
                OptionResult::Continue(vec![OptionReply::error(NBD_REP_ERR_UNKNOWN)])
            }
        }
    }

    /// Returns device info and completes negotiation
    pub async fn go(&self, data: &[u8]) -> OptionResult {
        if data.len() < 4 {
            return OptionResult::Error(
                NBDError::Protocol("Invalid GO option".to_string()),
                vec![OptionReply::error(NBD_REP_ERR_INVALID)],
            );
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            return OptionResult::Error(
                NBDError::Protocol("Invalid GO option length".to_string()),
                vec![OptionReply::error(NBD_REP_ERR_INVALID)],
            );
        }

        let name = &data[4..4 + name_len];
        debug!(
            "GO option: requested export name '{}' (name_len: {})",
            String::from_utf8_lossy(name),
            name_len
        );
        debug!(
            "GO option data length: {}, expected minimum: {}",
            data.len(),
            4 + name_len + 2
        );

        match self.get_device(name).await {
            Ok(device) => match device.info_export().to_bytes() {
                Ok(info_bytes) => OptionResult::Done(
                    device,
                    vec![
                        OptionReply::new(NBD_REP_INFO, info_bytes),
                        OptionReply::ack(),
                    ],
                ),
                Err(e) => OptionResult::Error(
                    NBDError::Protocol(format!("Failed to serialize info: {:?}", e)),
                    vec![],
                ),
            },
            Err(e) => {
                debug!(
                    "GO option: device '{}' not found: {:?}",
                    String::from_utf8_lossy(name),
                    e
                );
                OptionResult::Error(
                    NBDError::DeviceNotFound(name.to_vec()),
                    vec![OptionReply::error(NBD_REP_ERR_UNKNOWN)],
                )
            }
        }
    }

    /// Get a specific NBD device by name
    pub async fn get_device(&self, name: &[u8]) -> Result<NBDDevice> {
        let nbd_dir_inode = self.nbd_dir_inode().await?;

        let device_inode = self
            .filesystem
            .directory_store
            .get(nbd_dir_inode, name)
            .await
            .map_err(|e| match e {
                FsError::NotFound => NBDError::DeviceNotFound(name.to_vec()),
                e => NBDError::Filesystem(e),
            })?;

        let inode = self.filesystem.inode_store.get(device_inode).await?;

        match inode {
            Inode::File(file_inode) => Ok(NBDDevice {
                name: name.to_vec(),
                size: file_inode.size,
                inode: device_inode,
            }),
            _ => Err(NBDError::Protocol(format!(
                "NBD device '{}' is not a regular file",
                String::from_utf8_lossy(name)
            ))),
        }
    }

    pub async fn read(
        &self,
        inode: u64,
        offset: u64,
        length: u32,
        device_size: u64,
    ) -> CommandResult<Bytes> {
        if offset + length as u64 > device_size {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(Bytes::new());
        }

        let auth = AuthContext::default();
        let (data, _) = self
            .filesystem
            .read_file(&auth, inode, offset, length)
            .await?;
        Ok(data)
    }

    pub async fn write(
        &self,
        inode: u64,
        offset: u64,
        data: &Bytes,
        fua: bool,
    ) -> CommandResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let auth = AuthContext::default();
        self.filesystem.write(&auth, inode, offset, data).await?;

        if fua {
            self.flush(inode).await?;
        }

        Ok(())
    }

    pub async fn trim(
        &self,
        inode: u64,
        offset: u64,
        length: u32,
        fua: bool,
        device_size: u64,
    ) -> CommandResult<()> {
        if offset + length as u64 > device_size {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let auth = AuthContext::default();
        self.filesystem
            .trim(&auth, inode, offset, length as u64)
            .await?;

        if fua {
            self.flush(inode).await?;
        }

        Ok(())
    }

    pub async fn write_zeroes(
        &self,
        inode: u64,
        offset: u64,
        length: u32,
        fua: bool,
        device_size: u64,
    ) -> CommandResult<()> {
        if offset + length as u64 > device_size {
            return Err(CommandError::NoSpace);
        }

        if length == 0 {
            return Ok(());
        }

        let auth = AuthContext::default();
        let zero_chunk = Bytes::from(vec![0u8; NBD_ZERO_CHUNK_SIZE.min(length as usize)]);

        // Write zeros in chunks to avoid huge allocations
        let mut remaining = length as usize;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_size = remaining.min(NBD_ZERO_CHUNK_SIZE);
            let chunk_data = if chunk_size == zero_chunk.len() {
                &zero_chunk
            } else {
                &zero_chunk.slice(..chunk_size)
            };

            self.filesystem
                .write(&auth, inode, current_offset, chunk_data)
                .await?;

            remaining -= chunk_size;
            current_offset += chunk_size as u64;
        }

        if fua {
            self.flush(inode).await?;
        }

        Ok(())
    }

    pub async fn cache(&self, offset: u64, length: u32, device_size: u64) -> CommandResult<()> {
        if offset + length as u64 > device_size {
            return Err(CommandError::InvalidArgument);
        }
        Ok(())
    }

    pub async fn flush(&self, inode: u64) -> CommandResult<()> {
        self.filesystem
            .flush_coordinator
            .flush()
            .await
            .map_err(|_| CommandError::IoError)?;

        self.filesystem
            .tracer
            .emit(
                || self.filesystem.resolve_path_lossy(inode),
                FileOperation::Fsync,
            )
            .await;

        Ok(())
    }
}
