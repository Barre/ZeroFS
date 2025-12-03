use super::error::{NBDError, Result};
use super::protocol::{
    NBD_CMD_FLAG_FUA, NBD_EINVAL, NBD_EIO, NBD_ENOSPC, NBD_EXPORT_NAME_PADDING,
    NBD_FLAG_C_FIXED_NEWSTYLE, NBD_FLAG_C_NO_ZEROES, NBD_FLAG_FIXED_NEWSTYLE, NBD_FLAG_NO_ZEROES,
    NBD_INFO_EXPORT, NBD_OPT_ABORT, NBD_OPT_EXPORT_NAME, NBD_OPT_GO, NBD_OPT_INFO, NBD_OPT_LIST,
    NBD_OPT_STRUCTURED_REPLY, NBD_OPTION_HEADER_SIZE, NBD_READDIR_DEFAULT_LIMIT, NBD_REP_ACK,
    NBD_REP_ERR_INVALID, NBD_REP_ERR_UNKNOWN, NBD_REP_ERR_UNSUP, NBD_REP_INFO, NBD_REP_SERVER,
    NBD_REQUEST_HEADER_SIZE, NBD_SUCCESS, NBD_ZERO_CHUNK_SIZE, NBDClientFlags, NBDCommand,
    NBDInfoExport, NBDOptionHeader, NBDOptionReply, NBDRequest, NBDServerHandshake, NBDSimpleReply,
    TRANSMISSION_FLAGS,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::types::AuthContext;
use bytes::BytesMut;
use deku::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct NBDDevice {
    pub name: Vec<u8>,
    pub size: u64,
}

pub enum Transport {
    Tcp(SocketAddr),
    Unix(String),
}

pub struct NBDServer {
    filesystem: Arc<ZeroFS>,
    transport: Transport,
}

impl NBDServer {
    pub fn new_tcp(filesystem: Arc<ZeroFS>, socket: SocketAddr) -> Self {
        Self {
            filesystem,
            transport: Transport::Tcp(socket),
        }
    }

    pub fn new_unix(filesystem: Arc<ZeroFS>, socket_path: String) -> Self {
        Self {
            filesystem,
            transport: Transport::Unix(socket_path),
        }
    }

    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        match &self.transport {
            Transport::Tcp(socket) => {
                let listener = TcpListener::bind(socket).await?;
                info!("NBD server listening on {}", socket);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("NBD TCP server shutting down on {}", socket);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, addr) = result?;
                            info!("NBD client connected from {}", addr);

                            stream.set_nodelay(true)?;

                            let filesystem = Arc::clone(&self.filesystem);
                            let client_shutdown = shutdown.child_token();

                            tokio::spawn(async move {
                                if let Err(e) = handle_client_stream(stream, filesystem, client_shutdown).await {
                                    error!("Error handling NBD client {}: {}", addr, e);
                                }
                            });
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                // Remove existing socket file if it exists
                let _ = std::fs::remove_file(path);

                let listener = UnixListener::bind(path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to bind NBD Unix socket at {:?}: {}", path, e),
                    )
                })?;
                info!("NBD server listening on Unix socket {:?}", path);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("NBD Unix socket server shutting down at {:?}", path);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, _) = result?;
                            info!("NBD client connected via Unix socket");

                            let filesystem = Arc::clone(&self.filesystem);
                            let client_shutdown = shutdown.child_token();

                            tokio::spawn(async move {
                                if let Err(e) = handle_client_stream(stream, filesystem, client_shutdown).await {
                                    error!("Error handling NBD Unix client: {}", e);
                                }
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_client_stream<S>(
    stream: S,
    filesystem: Arc<ZeroFS>,
    shutdown: CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let reader = BufReader::new(reader);
    let writer = BufWriter::new(writer);

    let mut session = NBDSession::new(reader, writer, filesystem, shutdown);
    session.perform_handshake().await?;

    match session.negotiate_options().await {
        Ok(device) => {
            info!(
                "Client selected device: {}",
                String::from_utf8_lossy(&device.name)
            );
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
    filesystem: Arc<ZeroFS>,
    client_no_zeroes: bool,
    shutdown: CancellationToken,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> NBDSession<R, W> {
    fn new(reader: R, writer: W, filesystem: Arc<ZeroFS>, shutdown: CancellationToken) -> Self {
        Self {
            reader,
            writer,
            filesystem,
            client_no_zeroes: false,
            shutdown,
        }
    }

    async fn get_available_devices(&self) -> Result<Vec<NBDDevice>> {
        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        // Look up .nbd directory
        let nbd_dir_inode = self
            .filesystem
            .directory_store
            .get(0, b".nbd")
            .await
            .map_err(|e| {
                NBDError::Io(std::io::Error::other(format!(
                    "Failed to lookup .nbd directory: {e:?}"
                )))
            })?;

        let entries = self
            .filesystem
            .readdir(&auth, nbd_dir_inode, 0, NBD_READDIR_DEFAULT_LIMIT)
            .await
            .map_err(|e| {
                NBDError::Io(std::io::Error::other(format!(
                    "Failed to read .nbd directory: {e:?}"
                )))
            })?;

        let mut devices = Vec::new();
        for entry in &entries.entries {
            // Skip . and ..
            let name = &entry.name;
            if name == b"." || name == b".." {
                continue;
            }

            let inode = self
                .filesystem
                .inode_store
                .get(entry.fileid)
                .await
                .map_err(|e| {
                    NBDError::Io(std::io::Error::other(format!(
                        "Failed to load inode for {}: {e:?}",
                        String::from_utf8_lossy(name)
                    )))
                })?;

            if let Inode::File(file_inode) = inode {
                devices.push(NBDDevice {
                    name: name.to_vec(),
                    size: file_inode.size,
                });
            }
        }

        Ok(devices)
    }

    async fn get_device_by_name(&self, name: &[u8]) -> Result<NBDDevice> {
        let nbd_dir_inode = self
            .filesystem
            .directory_store
            .get(0, b".nbd")
            .await
            .map_err(|e| {
                NBDError::Io(std::io::Error::other(format!(
                    "Failed to lookup .nbd directory: {e:?}"
                )))
            })?;

        let device_inode = match self
            .filesystem
            .directory_store
            .get(nbd_dir_inode, name)
            .await
        {
            Ok(inode) => inode,
            Err(FsError::NotFound) => {
                return Err(NBDError::DeviceNotFound(name.to_vec()));
            }
            Err(e) => {
                return Err(NBDError::Io(std::io::Error::other(format!(
                    "Failed to lookup device: {e:?}"
                ))));
            }
        };

        let inode = self
            .filesystem
            .inode_store
            .get(device_inode)
            .await
            .map_err(|e| {
                NBDError::Io(std::io::Error::other(format!(
                    "Failed to load inode for {}: {e:?}",
                    String::from_utf8_lossy(name)
                )))
            })?;

        match inode {
            Inode::File(file_inode) => Ok(NBDDevice {
                name: name.to_vec(),
                size: file_inode.size,
            }),
            _ => Err(NBDError::Io(std::io::Error::other(format!(
                "NBD device '{}' is not a regular file",
                String::from_utf8_lossy(name)
            )))),
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
            let mut header_buf = [0u8; NBD_OPTION_HEADER_SIZE];
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

            match header.option {
                NBD_OPT_LIST => {
                    debug!("Handling LIST option");
                    self.handle_list_option(header.length).await?;
                }
                NBD_OPT_EXPORT_NAME => {
                    debug!("Handling EXPORT_NAME option");
                    return self.handle_export_name_option(header.length).await;
                }
                NBD_OPT_INFO => {
                    debug!("Handling INFO option");
                    self.handle_info_option(header.length).await?;
                }
                NBD_OPT_GO => {
                    match self.handle_go_option(header.length).await {
                        Ok(device) => return Ok(device),
                        Err(NBDError::DeviceNotFound(_)) => {
                            // Device not found - stay in negotiation loop
                            // Error reply already sent by handle_go_option
                        }
                        Err(e) => return Err(e),
                    }
                }
                NBD_OPT_STRUCTURED_REPLY => {
                    debug!("Handling STRUCTURED_REPLY option");
                    self.handle_structured_reply_option(header.length).await?;
                }
                NBD_OPT_ABORT => {
                    debug!("Handling ABORT option");
                    self.send_option_reply(header.option, NBD_REP_ACK, &[])
                        .await?;
                    self.writer.flush().await?;
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
                    self.writer.flush().await?;
                }
            }
        }
    }

    async fn handle_list_option(&mut self, length: u32) -> Result<()> {
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }

        let devices = self.get_available_devices().await?;
        for device in devices {
            let name_bytes = device.name;
            let mut reply_data = Vec::new();
            reply_data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            reply_data.extend_from_slice(&name_bytes);

            self.send_option_reply(NBD_OPT_LIST, NBD_REP_SERVER, &reply_data)
                .await?;
        }

        self.send_option_reply(NBD_OPT_LIST, NBD_REP_ACK, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn handle_export_name_option(&mut self, length: u32) -> Result<NBDDevice> {
        let mut name_buf = vec![0u8; length as usize];
        self.reader.read_exact(&mut name_buf).await?;

        debug!(
            "Client requested export: '{}' (length: {})",
            String::from_utf8_lossy(&name_buf),
            length
        );

        // For NBD_OPT_EXPORT_NAME, we can't send an error reply
        // We must either send the export info or close the connection
        let device = match self.get_device_by_name(&name_buf).await {
            Ok(device) => device,
            Err(_) => {
                error!(
                    "Export '{}' not found, closing connection",
                    String::from_utf8_lossy(&name_buf)
                );
                return Err(NBDError::DeviceNotFound(name_buf));
            }
        };

        self.writer.write_all(&device.size.to_be_bytes()).await?;
        self.writer
            .write_all(&TRANSMISSION_FLAGS.to_be_bytes())
            .await?;

        // Only send padding bytes if client didn't set NBD_FLAG_C_NO_ZEROES
        if !self.client_no_zeroes {
            let zeros = vec![0u8; NBD_EXPORT_NAME_PADDING];
            self.writer.write_all(&zeros).await?;
        }

        self.writer.flush().await?;

        Ok(device)
    }

    async fn handle_info_option(&mut self, length: u32) -> Result<()> {
        if length < 4 {
            self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }

        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::Protocol("Invalid INFO option length".to_string()));
        }

        let name = &data[4..4 + name_len];
        debug!(
            "INFO option: requested export name '{}' (name_len: {})",
            String::from_utf8_lossy(name),
            name_len
        );

        match self.get_device_by_name(name).await {
            Ok(device) => {
                let info = NBDInfoExport {
                    info_type: NBD_INFO_EXPORT,
                    size: device.size,
                    transmission_flags: TRANSMISSION_FLAGS,
                };
                let info_bytes = info.to_bytes()?;
                self.send_option_reply(NBD_OPT_INFO, NBD_REP_INFO, &info_bytes)
                    .await?;
                self.send_option_reply(NBD_OPT_INFO, NBD_REP_ACK, &[])
                    .await?;
                self.writer.flush().await?;
                Ok(())
            }
            Err(_) => {
                self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                Ok(())
            }
        }
    }

    async fn handle_go_option(&mut self, length: u32) -> Result<NBDDevice> {
        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;

        if data.len() < 4 {
            self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::Protocol("Invalid GO option".to_string()));
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::Protocol("Invalid GO option length".to_string()));
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

        match self.get_device_by_name(name).await {
            Ok(device) => {
                let info = NBDInfoExport {
                    info_type: NBD_INFO_EXPORT,
                    size: device.size,
                    transmission_flags: TRANSMISSION_FLAGS,
                };
                let info_bytes = info.to_bytes()?;
                self.send_option_reply(NBD_OPT_GO, NBD_REP_INFO, &info_bytes)
                    .await?;
                self.send_option_reply(NBD_OPT_GO, NBD_REP_ACK, &[]).await?;
                self.writer.flush().await?;
                Ok(device)
            }
            Err(_) => {
                self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                Err(NBDError::DeviceNotFound(name.to_vec()))
            }
        }
    }

    async fn handle_structured_reply_option(&mut self, length: u32) -> Result<()> {
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }

        // We don't support structured replies for now
        self.send_option_reply(NBD_OPT_STRUCTURED_REPLY, NBD_REP_ERR_UNSUP, &[])
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
        let nbd_dir_inode = self
            .filesystem
            .directory_store
            .get(0, b".nbd")
            .await
            .map_err(|e| NBDError::Filesystem(format!("Failed to lookup .nbd directory: {e:?}")))?;

        let device_inode = self
            .filesystem
            .directory_store
            .get(nbd_dir_inode, &device.name)
            .await
            .map_err(|e| NBDError::Filesystem(format!("Failed to lookup device file: {e:?}")))?;

        loop {
            let mut request_buf = [0u8; NBD_REQUEST_HEADER_SIZE];

            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    debug!("NBD client handler shutting down");
                    return Ok(());
                }
                result = self.reader.read_exact(&mut request_buf) => {
                    result?;
                }
            }

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
                    self.handle_trim(
                        device_inode,
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

        let auth = crate::fs::types::AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        match self
            .filesystem
            .read_file(&auth, inode, offset, length)
            .await
        {
            Ok((data, _)) => {
                if self
                    .send_simple_reply(cookie, NBD_SUCCESS, data.as_ref())
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
        // Check for out-of-bounds write
        if offset + length as u64 > device_size {
            // Must read and discard the data before sending error
            let mut data = BytesMut::zeroed(length as usize);
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

        let auth = crate::fs::types::AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        let mut data = BytesMut::zeroed(length as usize);
        if self.reader.read_exact(&mut data).await.is_err() {
            let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
            return NBD_EIO;
        }

        let data = data.freeze();
        match self.filesystem.write(&auth, inode, offset, &data).await {
            Ok(_) => {
                if (flags & NBD_CMD_FLAG_FUA) != 0
                    && let Err(e) = self.filesystem.flush_coordinator.flush().await
                {
                    error!("NBD write FUA flush failed: {:?}", e);
                    let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                    return NBD_EIO;
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
        match self.filesystem.flush_coordinator.flush().await {
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
                error!("NBD flush failed: {:?}", e);
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

        let auth = crate::fs::types::AuthContext {
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
                if (flags & NBD_CMD_FLAG_FUA) != 0
                    && let Err(e) = self.filesystem.flush_coordinator.flush().await
                {
                    error!("NBD trim FUA flush failed: {:?}", e);
                    let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                    return NBD_EIO;
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

        let auth = crate::fs::types::AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };

        let zero_chunk = bytes::Bytes::from(vec![0u8; NBD_ZERO_CHUNK_SIZE.min(length as usize)]);

        // Write zeros in chunks to avoid huge allocations
        let mut remaining = length as usize;
        let mut current_offset = offset;
        let mut write_succeeded = true;

        while remaining > 0 && write_succeeded {
            let chunk_size = remaining.min(NBD_ZERO_CHUNK_SIZE);
            let chunk_data = if chunk_size == zero_chunk.len() {
                &zero_chunk
            } else {
                &zero_chunk.slice(..chunk_size)
            };

            if self
                .filesystem
                .write(&auth, inode, current_offset, chunk_data)
                .await
                .is_err()
            {
                write_succeeded = false;
                break;
            }

            remaining -= chunk_size;
            current_offset += chunk_size as u64;
        }

        if write_succeeded {
            // Handle FUA flag - force unit access (flush after write_zeroes)
            if (flags & NBD_CMD_FLAG_FUA) != 0
                && let Err(e) = self.filesystem.flush_coordinator.flush().await
            {
                error!("NBD write_zeroes FUA flush failed: {:?}", e);
                let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
                return NBD_EIO;
            }

            if self
                .send_simple_reply(cookie, NBD_SUCCESS, &[])
                .await
                .is_err()
            {
                return NBD_EIO;
            }
            NBD_SUCCESS
        } else {
            let _ = self.send_simple_reply(cookie, NBD_EIO, &[]).await;
            NBD_EIO
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
