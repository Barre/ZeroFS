use deku::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use super::error::{NBDError, Result};
use super::protocol::*;
use crate::filesystem::{EncodedFileId, SlateDbFs};

#[derive(Clone)]
pub struct NBDDevice {
    pub name: String,
    pub size: u64,
}

pub struct NBDServer {
    filesystem: Arc<SlateDbFs>,
    devices: HashMap<String, NBDDevice>,
    host: String,
    port: u16,
}

impl NBDServer {
    pub fn new(filesystem: Arc<SlateDbFs>, host: String, port: u16) -> Self {
        Self {
            filesystem,
            devices: HashMap::new(),
            host,
            port,
        }
    }

    pub fn add_device(&mut self, name: String, size: u64) {
        let device = NBDDevice {
            name: name.clone(),
            size,
        };
        self.devices.insert(name, device);
    }

    pub async fn start(&self) -> std::io::Result<()> {
        // Initialize device files (.nbd directory created in main)
        for device in self.devices.values() {
            self.initialize_device(device).await?;
        }

        let listener = TcpListener::bind(format!("{}:{}", self.host, self.port)).await?;
        info!("NBD server listening on {}:{}", self.host, self.port);

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("NBD client connected from {}", addr);

            let filesystem = Arc::clone(&self.filesystem);
            let devices = self.devices.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, filesystem, devices).await {
                    error!("Error handling NBD client {}: {}", addr, e);
                }
            });
        }
    }

    async fn initialize_device(&self, device: &NBDDevice) -> std::io::Result<()> {
        use zerofs_nfsserve::nfs::{nfsstring, sattr3, set_mode3};
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let nbd_name = nfsstring(b".nbd".to_vec());
        let device_name = nfsstring(device.name.as_bytes().to_vec());

        // Check if device file exists, create it if not
        let nbd_dir_inode = self
            .filesystem
            .lookup(&auth, 0, &nbd_name)
            .await
            .map_err(|e| {
                std::io::Error::other(format!("Failed to lookup .nbd directory: {e:?}"))
            })?;

        match self
            .filesystem
            .lookup(&auth, nbd_dir_inode, &device_name)
            .await
        {
            Ok(device_inode) => {
                let existing_attr =
                    self.filesystem
                        .getattr(&auth, device_inode)
                        .await
                        .map_err(|e| {
                            std::io::Error::other(format!("Failed to get device attributes: {e:?}"))
                        })?;

                if existing_attr.size != device.size {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "NBD device {} size mismatch: existing size is {} bytes, requested size is {} bytes. Cannot resize existing devices.",
                            device.name, existing_attr.size, device.size
                        ),
                    ));
                }

                debug!(
                    "NBD device file {} already exists with correct size {}",
                    device.name, device.size
                );
                Ok(())
            }
            Err(_) => {
                debug!(
                    "Creating NBD device file {} with size {}",
                    device.name, device.size
                );
                let attr = sattr3 {
                    mode: set_mode3::mode(0o600),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(0),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(0),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                };

                let (device_inode, _) = self
                    .filesystem
                    .create(&auth, nbd_dir_inode, &device_name, attr)
                    .await
                    .map_err(|e| {
                        std::io::Error::other(format!("Failed to create device file: {e:?}"))
                    })?;

                // Set the file size by writing a zero byte at the end
                if device.size > 0 {
                    let data = vec![0u8; 1];
                    self.filesystem
                        .write(&auth, device_inode, device.size - 1, &data)
                        .await
                        .map_err(|e| {
                            std::io::Error::other(format!("Failed to set device size: {e:?}"))
                        })?;
                }

                Ok(())
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    filesystem: Arc<SlateDbFs>,
    devices: HashMap<String, NBDDevice>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let reader = BufReader::new(reader);
    let writer = BufWriter::new(writer);

    let mut session = NBDSession::new(reader, writer, filesystem, devices);
    session.perform_handshake().await?;

    match session.negotiate_options().await {
        Ok(device) => {
            info!("Client selected device: {}", device.name);
            session.handle_transmission(device).await?;
        }
        Err(NBDError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Client disconnected cleanly after option negotiation");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

struct NBDSession<R, W> {
    reader: R,
    writer: W,
    filesystem: Arc<SlateDbFs>,
    devices: HashMap<String, NBDDevice>,
    client_no_zeroes: bool,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> NBDSession<R, W> {
    fn new(
        reader: R,
        writer: W,
        filesystem: Arc<SlateDbFs>,
        devices: HashMap<String, NBDDevice>,
    ) -> Self {
        Self {
            reader,
            writer,
            filesystem,
            devices,
            client_no_zeroes: false,
        }
    }

    async fn perform_handshake(&mut self) -> Result<()> {
        let handshake = NBDServerHandshake::new(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);
        let handshake_bytes = handshake.to_bytes()?;
        self.writer.write_all(&handshake_bytes).await?;
        self.writer.flush().await?;

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;
        let client_flags = NBDClientFlags::from_bytes((&buf, 0))?.1;

        debug!("Client flags: 0x{:x}", client_flags.flags);

        if (client_flags.flags & NBD_FLAG_C_FIXED_NEWSTYLE) == 0 {
            return Err(NBDError::IncompatibleClient);
        }

        self.client_no_zeroes = (client_flags.flags & NBD_FLAG_C_NO_ZEROES) != 0;

        Ok(())
    }

    async fn negotiate_options(&mut self) -> Result<NBDDevice> {
        loop {
            let mut header_buf = [0u8; 16];
            match self.reader.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Client disconnected, this is normal after LIST
                    debug!("Client disconnected during option negotiation");
                    return Err(NBDError::Io(e));
                }
                Err(e) => return Err(NBDError::Io(e)),
            }
            let header = NBDOptionHeader::from_bytes((&header_buf, 0))
                .map_err(|e| {
                    debug!("Raw header bytes: {:02x?}", header_buf);
                    NBDError::Protocol(format!("Invalid option header: {e}"))
                })?
                .1;

            debug!(
                "Received option: {} (length: {})",
                header.option, header.length
            );
            debug!(
                "NBDOption values - List: {}, Info: {}, Go: {}",
                NBDOption::List as u32,
                NBDOption::Info as u32,
                NBDOption::Go as u32
            );

            match header.option {
                3 => {
                    debug!("Handling LIST option");
                    self.handle_list_option(header.length).await?;
                }
                1 => {
                    debug!("Handling EXPORT_NAME option");
                    return self.handle_export_name_option(header.length).await;
                }
                6 => {
                    debug!("Handling INFO option");
                    self.handle_info_option(header.length).await?;
                }
                7 => {
                    // NBD_OPT_GO
                    return self.handle_go_option(header.length).await;
                }
                8 => {
                    debug!("Handling STRUCTURED_REPLY option");
                    self.handle_structured_reply_option(header.length).await?;
                }
                2 => {
                    debug!("Handling ABORT option");
                    self.send_option_reply(header.option, NBD_REP_ACK, &[])
                        .await?;
                    return Err(NBDError::Protocol("Client aborted".to_string()));
                }
                _ => {
                    debug!("Unknown option: {}", header.option);
                    if header.length > 0 {
                        let mut buf = vec![0u8; header.length as usize];
                        self.reader.read_exact(&mut buf).await?;
                    }
                    self.send_option_reply(header.option, NBD_REP_ERR_UNSUP, &[])
                        .await?;
                }
            }
        }
    }

    async fn handle_list_option(&mut self, length: u32) -> Result<()> {
        // Skip any data
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }

        // Send device list
        let devices: Vec<_> = self.devices.values().cloned().collect();
        for device in devices {
            let name_bytes = device.name.as_bytes();
            let mut reply_data = Vec::new();
            reply_data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            reply_data.extend_from_slice(name_bytes);

            self.send_option_reply(NBDOption::List as u32, NBD_REP_SERVER, &reply_data)
                .await?;
        }

        // Send final ACK
        self.send_option_reply(NBDOption::List as u32, NBD_REP_ACK, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn handle_export_name_option(&mut self, length: u32) -> Result<NBDDevice> {
        let mut name_buf = vec![0u8; length as usize];
        self.reader.read_exact(&mut name_buf).await?;
        let name = String::from_utf8_lossy(&name_buf);

        debug!("Client requested export: '{}' (length: {})", name, length);
        debug!(
            "Available devices: {:?}",
            self.devices.keys().collect::<Vec<_>>()
        );

        let device = self
            .devices
            .get(name.as_ref())
            .ok_or_else(|| NBDError::DeviceNotFound(name.to_string()))?
            .clone();

        // Write export info
        self.writer.write_all(&device.size.to_be_bytes()).await?;
        self.writer
            .write_all(&get_transmission_flags().to_be_bytes())
            .await?;

        // Only send 124 bytes of zeroes if client didn't set NBD_FLAG_C_NO_ZEROES
        if !self.client_no_zeroes {
            let zeros = vec![0u8; 124];
            self.writer.write_all(&zeros).await?;
        }

        self.writer.flush().await?;

        Ok(device)
    }

    async fn handle_info_option(&mut self, length: u32) -> Result<()> {
        if length < 4 {
            self.send_option_reply(NBDOption::Info as u32, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }

        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            self.send_option_reply(NBDOption::Info as u32, NBD_REP_ERR_INVALID, &[])
                .await?;
            return Err(NBDError::Protocol("Invalid INFO option length".to_string()));
        }

        let name = String::from_utf8_lossy(&data[4..4 + name_len]);
        debug!(
            "INFO option: requested export name '{}' (name_len: {})",
            name, name_len
        );

        match self.devices.get(name.as_ref()) {
            Some(device) => {
                let info = NBDInfoExport {
                    info_type: NBD_INFO_EXPORT,
                    size: device.size,
                    transmission_flags: get_transmission_flags(),
                };
                let info_bytes = info.to_bytes()?;
                self.send_option_reply(NBDOption::Info as u32, NBD_REP_INFO, &info_bytes)
                    .await?;
                self.send_option_reply(NBDOption::Info as u32, NBD_REP_ACK, &[])
                    .await?;
                self.writer.flush().await?;
                Ok(())
            }
            None => {
                self.send_option_reply(NBDOption::Info as u32, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                Err(NBDError::DeviceNotFound(name.to_string()))
            }
        }
    }

    async fn handle_go_option(&mut self, length: u32) -> Result<NBDDevice> {
        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;

        if data.len() < 4 {
            self.send_option_reply(NBDOption::Go as u32, NBD_REP_ERR_INVALID, &[])
                .await?;
            return Err(NBDError::Protocol("Invalid GO option".to_string()));
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            self.send_option_reply(NBDOption::Go as u32, NBD_REP_ERR_INVALID, &[])
                .await?;
            return Err(NBDError::Protocol("Invalid GO option length".to_string()));
        }

        let name = String::from_utf8_lossy(&data[4..4 + name_len]);
        debug!(
            "GO option: requested export name '{}' (name_len: {})",
            name, name_len
        );
        debug!(
            "GO option data length: {}, expected minimum: {}",
            data.len(),
            4 + name_len + 2
        );

        match self.devices.get(name.as_ref()) {
            Some(device) => {
                let device = device.clone();
                let info = NBDInfoExport {
                    info_type: NBD_INFO_EXPORT,
                    size: device.size,
                    transmission_flags: get_transmission_flags(),
                };
                let info_bytes = info.to_bytes()?;
                self.send_option_reply(NBDOption::Go as u32, NBD_REP_INFO, &info_bytes)
                    .await?;
                self.send_option_reply(NBDOption::Go as u32, NBD_REP_ACK, &[])
                    .await?;
                self.writer.flush().await?;
                Ok(device)
            }
            None => {
                self.send_option_reply(NBDOption::Go as u32, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                Err(NBDError::DeviceNotFound(name.to_string()))
            }
        }
    }

    async fn handle_structured_reply_option(&mut self, length: u32) -> Result<()> {
        // Skip any data
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }

        // We don't support structured replies for now
        self.send_option_reply(NBDOption::StructuredReply as u32, NBD_REP_ERR_UNSUP, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn send_option_reply(&mut self, option: u32, reply_type: u32, data: &[u8]) -> Result<()> {
        let reply = NBDOptionReply::new(option, reply_type, data.len() as u32);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        // Note: No flush here - caller should flush when appropriate
        Ok(())
    }

    async fn handle_transmission(&mut self, device: NBDDevice) -> Result<()> {
        use zerofs_nfsserve::nfs::nfsstring;
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let nbd_name = nfsstring(b".nbd".to_vec());
        let device_name = nfsstring(device.name.as_bytes().to_vec());

        // Get device inode
        let nbd_dir_inode = self
            .filesystem
            .lookup(&auth, 0, &nbd_name)
            .await
            .map_err(|e| NBDError::Filesystem(format!("Failed to lookup .nbd directory: {e:?}")))?;

        let device_inode = self
            .filesystem
            .lookup(&auth, nbd_dir_inode, &device_name)
            .await
            .map_err(|e| NBDError::Filesystem(format!("Failed to lookup device file: {e:?}")))?;

        loop {
            let mut request_buf = [0u8; 28];
            self.reader.read_exact(&mut request_buf).await?;
            let request = NBDRequest::from_bytes((&request_buf, 0))
                .map_err(|e| NBDError::Protocol(format!("Invalid request: {e}")))?
                .1;

            debug!(
                "NBD command: {:?}, offset={}, length={}",
                request.cmd_type, request.offset, request.length
            );

            let error = match request.cmd_type {
                NBDCommand::Read => {
                    self.handle_read(
                        device_inode,
                        request.cookie,
                        request.offset,
                        request.length,
                        device.size,
                    )
                    .await
                }
                NBDCommand::Write => {
                    self.handle_write(
                        device_inode,
                        request.cookie,
                        request.offset,
                        request.length,
                        request.flags,
                        device.size,
                    )
                    .await
                }
                NBDCommand::Disconnect => {
                    info!("Client disconnecting");
                    return Ok(());
                }
                NBDCommand::Flush => self.handle_flush(request.cookie).await,
                NBDCommand::Trim => {
                    let device_inode_raw = EncodedFileId::from(device_inode).inode_id();
                    self.handle_trim(
                        device_inode_raw,
                        request.cookie,
                        request.offset,
                        request.length,
                        request.flags,
                        device.size,
                    )
                    .await
                }
                NBDCommand::WriteZeroes => {
                    self.handle_write_zeroes(
                        device_inode,
                        request.cookie,
                        request.offset,
                        request.length,
                        request.flags,
                        device.size,
                    )
                    .await
                }
                NBDCommand::Cache => {
                    self.handle_cache(request.cookie, request.offset, request.length, device.size)
                        .await
                }
                NBDCommand::Unknown(cmd) => {
                    warn!("Unknown NBD command: {}", cmd);
                    let _ = self
                        .send_simple_reply(request.cookie, NBD_EINVAL, &[])
                        .await;
                    NBD_EINVAL
                }
            };

            if error != 0 {
                warn!("NBD command failed with error: {}", error);
            }
        }
    }

    async fn handle_read(
        &mut self,
        inode: u64,
        cookie: u64,
        offset: u64,
        length: u32,
        device_size: u64,
    ) -> u32 {
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        // Check for out-of-bounds read
        if offset + length as u64 > device_size {
            let _ = self.send_simple_reply(cookie, NBD_EINVAL, &[]).await;
            return NBD_EINVAL;
        }

        // Handle zero-length read
        if length == 0 {
            // Spec says behavior is unspecified but server SHOULD NOT disconnect
            if self
                .send_simple_reply(cookie, NBD_SUCCESS, &[])
                .await
                .is_err()
            {
                return NBD_EIO;
            }
            return NBD_SUCCESS;
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        match self.filesystem.read(&auth, inode, offset, length).await {
            Ok((data, _)) => {
                if self
                    .send_simple_reply(cookie, NBD_SUCCESS, &data)
                    .await
                    .is_err()
                {
                    return NBD_EIO;
                }
                NBD_SUCCESS
            }
            Err(_) => {
                let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                NBD_EIO
            }
        }
    }

    async fn handle_write(
        &mut self,
        inode: u64,
        cookie: u64,
        offset: u64,
        length: u32,
        flags: u16,
        device_size: u64,
    ) -> u32 {
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        // Check for out-of-bounds write
        if offset + length as u64 > device_size {
            // Must read and discard the data before sending error
            let mut data = vec![0u8; length as usize];
            let _ = self.reader.read_exact(&mut data).await;
            let _ = self.send_simple_reply(cookie, NBD_ENOSPC, &[]).await;
            return NBD_ENOSPC;
        }

        // Handle zero-length write
        if length == 0 {
            if self
                .send_simple_reply(cookie, NBD_SUCCESS, &[])
                .await
                .is_err()
            {
                return NBD_EIO;
            }
            return NBD_SUCCESS;
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        let mut data = vec![0u8; length as usize];
        if self.reader.read_exact(&mut data).await.is_err() {
            let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
            return NBD_EIO;
        }

        match self.filesystem.write(&auth, inode, offset, &data).await {
            Ok(_) => {
                if (flags & NBD_CMD_FLAG_FUA) != 0 {
                    if let Err(e) = self.filesystem.db.flush().await {
                        error!("NBD write FUA flush failed: {}", e);
                        let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                        return NBD_EIO;
                    }
                }

                if self
                    .send_simple_reply(cookie, NBD_SUCCESS, &[])
                    .await
                    .is_err()
                {
                    return NBD_EIO;
                }
                NBD_SUCCESS
            }
            Err(_) => {
                let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                NBD_EIO
            }
        }
    }

    async fn handle_flush(&mut self, cookie: u64) -> u32 {
        match self.filesystem.db.flush().await {
            Ok(_) => {
                if self
                    .send_simple_reply(cookie, NBD_SUCCESS, &[])
                    .await
                    .is_err()
                {
                    return NBD_EIO;
                }
                NBD_SUCCESS
            }
            Err(e) => {
                error!("NBD flush failed: {}", e);
                let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                NBD_EIO
            }
        }
    }

    async fn handle_trim(
        &mut self,
        inode: u64,
        cookie: u64,
        offset: u64,
        length: u32,
        flags: u16,
        device_size: u64,
    ) -> u32 {
        use zerofs_nfsserve::vfs::AuthContext;

        // Check for out-of-bounds trim
        if offset + length as u64 > device_size {
            let _ = self.send_simple_reply(cookie, NBD_EINVAL, &[]).await;
            return NBD_EINVAL;
        }

        // Handle zero-length trim
        if length == 0 {
            // Spec says behavior is unspecified but server SHOULD NOT disconnect
            if self
                .send_simple_reply(cookie, NBD_SUCCESS, &[])
                .await
                .is_err()
            {
                return NBD_EIO;
            }
            return NBD_SUCCESS;
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        match self
            .filesystem
            .trim(&auth, inode, offset, length as u64)
            .await
        {
            Ok(_) => {
                if (flags & NBD_CMD_FLAG_FUA) != 0 {
                    if let Err(e) = self.filesystem.db.flush().await {
                        error!("NBD trim FUA flush failed: {}", e);
                        let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                        return NBD_EIO;
                    }
                }

                if self
                    .send_simple_reply(cookie, NBD_SUCCESS, &[])
                    .await
                    .is_err()
                {
                    return NBD_EIO;
                }
                NBD_SUCCESS
            }
            Err(e) => {
                error!("NBD trim failed: {:?}", e);
                let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                NBD_EIO
            }
        }
    }

    async fn handle_write_zeroes(
        &mut self,
        inode: u64,
        cookie: u64,
        offset: u64,
        length: u32,
        flags: u16,
        device_size: u64,
    ) -> u32 {
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        if offset + length as u64 > device_size {
            let _ = self.send_simple_reply(cookie, NBD_ENOSPC, &[]).await;
            return NBD_ENOSPC;
        }

        // Handle zero-length write_zeroes
        if length == 0 {
            if self
                .send_simple_reply(cookie, NBD_SUCCESS, &[])
                .await
                .is_err()
            {
                return NBD_EIO;
            }
            return NBD_SUCCESS;
        }

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let zero_data = vec![0u8; length as usize];

        match self
            .filesystem
            .write(&auth, inode, offset, &zero_data)
            .await
        {
            Ok(_) => {
                // Handle FUA flag - force unit access (flush after write_zeroes)
                if (flags & NBD_CMD_FLAG_FUA) != 0 {
                    if let Err(e) = self.filesystem.db.flush().await {
                        error!("NBD write_zeroes FUA flush failed: {}", e);
                        let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                        return NBD_EIO;
                    }
                }

                if self
                    .send_simple_reply(cookie, NBD_SUCCESS, &[])
                    .await
                    .is_err()
                {
                    return NBD_EIO;
                }
                NBD_SUCCESS
            }
            Err(_) => {
                let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                NBD_EIO
            }
        }
    }

    async fn handle_cache(
        &mut self,
        cookie: u64,
        offset: u64,
        length: u32,
        device_size: u64,
    ) -> u32 {
        if offset + length as u64 > device_size {
            let _ = self.send_simple_reply(cookie, NBD_EINVAL, &[]).await;
            return NBD_EINVAL;
        }

        if length == 0 {
            if self
                .send_simple_reply(cookie, NBD_SUCCESS, &[])
                .await
                .is_err()
            {
                return NBD_EIO;
            }
            return NBD_SUCCESS;
        }

        if self
            .send_simple_reply(cookie, NBD_SUCCESS, &[])
            .await
            .is_err()
        {
            return NBD_EIO;
        }
        NBD_SUCCESS
    }

    async fn send_simple_reply(&mut self, cookie: u64, error: u32, data: &[u8]) -> Result<()> {
        let reply = NBDSimpleReply::new(cookie, error);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        self.writer.flush().await?;
        Ok(())
    }
}
