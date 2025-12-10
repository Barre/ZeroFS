use super::error::{NBDError, Result};
use super::handler::{NBDDevice, NBDHandler, OptionReply, OptionResult};
use super::protocol::*;
use crate::fs::ZeroFS;
use bytes::BytesMut;
use deku::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub enum Transport {
    Tcp(SocketAddr),
    Unix(std::path::PathBuf),
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

    pub fn new_unix(filesystem: Arc<ZeroFS>, socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            filesystem,
            transport: Transport::Unix(socket_path.into()),
        }
    }

    fn spawn_client_handler<S>(&self, stream: S, shutdown: &CancellationToken, client_name: String)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let filesystem = Arc::clone(&self.filesystem);
        let client_shutdown = shutdown.child_token();

        tokio::spawn(async move {
            if let Err(e) = handle_client_stream(stream, filesystem, client_shutdown).await {
                error!("Error handling NBD client {}: {}", client_name, e);
            }
        });
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
                            self.spawn_client_handler(stream, &shutdown, addr.to_string());
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
                            self.spawn_client_handler(stream, &shutdown, "unix".to_string());
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
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
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
    handler: NBDHandler,
    client_no_zeroes: bool,
    shutdown: CancellationToken,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> NBDSession<R, W> {
    fn new(reader: R, writer: W, filesystem: Arc<ZeroFS>, shutdown: CancellationToken) -> Self {
        Self {
            reader,
            writer,
            handler: NBDHandler::new(filesystem),
            client_no_zeroes: false,
            shutdown,
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
                    self.drain_option_data(header.length).await?;
                    self.send_option_reply(header.option, NBD_REP_ERR_UNSUP, &[])
                        .await?;
                    self.writer.flush().await?;
                }
            }
        }
    }

    async fn handle_list_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;
        let result = self.handler.list().await;
        self.process_option_result(NBD_OPT_LIST, result).await?;
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
        let device = self.handler.get_device(&name_buf).await.map_err(|e| {
            error!(
                "Export '{}' not found, closing connection: {:?}",
                String::from_utf8_lossy(&name_buf),
                e
            );
            NBDError::DeviceNotFound(name_buf.clone())
        })?;

        self.writer.write_all(&device.size.to_be_bytes()).await?;
        self.writer
            .write_all(&TRANSMISSION_FLAGS.to_be_bytes())
            .await?;

        if !self.client_no_zeroes {
            self.writer
                .write_all(&[0u8; NBD_EXPORT_NAME_PADDING])
                .await?;
        }

        self.writer.flush().await?;
        Ok(device)
    }

    async fn handle_info_option(&mut self, length: u32) -> Result<()> {
        let data = self.read_option_data(length).await?;
        let result = self.handler.info(&data).await;
        self.process_option_result(NBD_OPT_INFO, result).await?;
        Ok(())
    }

    async fn handle_go_option(&mut self, length: u32) -> Result<NBDDevice> {
        let data = self.read_option_data(length).await?;
        let result = self.handler.go(&data).await;
        match self.process_option_result(NBD_OPT_GO, result).await? {
            Some(device) => Ok(device),
            None => Err(NBDError::DeviceNotFound(Vec::new())),
        }
    }

    async fn handle_structured_reply_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;
        self.send_option_reply(NBD_OPT_STRUCTURED_REPLY, NBD_REP_ERR_UNSUP, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Read option data from the stream
    async fn read_option_data(&mut self, length: u32) -> Result<Vec<u8>> {
        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;
        Ok(data)
    }

    /// Drain any remaining option data from the reader
    async fn drain_option_data(&mut self, length: u32) -> Result<()> {
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }
        Ok(())
    }

    /// Process option result from handler - send replies and return device if done
    async fn process_option_result(
        &mut self,
        option: u32,
        result: OptionResult,
    ) -> Result<Option<NBDDevice>> {
        match result {
            OptionResult::Continue(replies) => {
                self.send_option_replies(option, &replies).await?;
                Ok(None)
            }
            OptionResult::Done(device, replies) => {
                self.send_option_replies(option, &replies).await?;
                Ok(Some(device))
            }
            OptionResult::Error(err, replies) => {
                self.send_option_replies(option, &replies).await?;
                Err(err)
            }
        }
    }

    /// Send multiple option replies and flush
    async fn send_option_replies(&mut self, option: u32, replies: &[OptionReply]) -> Result<()> {
        for reply in replies {
            self.send_option_reply(option, reply.reply_type, &reply.data)
                .await?;
        }
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
        Ok(())
    }

    async fn handle_transmission(&mut self, device: NBDDevice) -> Result<()> {
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

            let fua = (request.flags & NBD_CMD_FLAG_FUA) != 0;

            match request.cmd_type {
                NBDCommand::Read => {
                    let result = self
                        .handler
                        .read(device.inode, request.offset, request.length, device.size)
                        .await;
                    self.send_read_result(request.cookie, result).await;
                }
                NBDCommand::Write => {
                    let result = self
                        .read_write_data(
                            device.inode,
                            request.offset,
                            request.length,
                            fua,
                            device.size,
                        )
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Disconnect => {
                    info!("Client disconnecting");
                    return Ok(());
                }
                NBDCommand::Flush => {
                    let result = self.handler.flush().await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Trim => {
                    let result = self
                        .handler
                        .trim(
                            device.inode,
                            request.offset,
                            request.length,
                            fua,
                            device.size,
                        )
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::WriteZeroes => {
                    let result = self
                        .handler
                        .write_zeroes(
                            device.inode,
                            request.offset,
                            request.length,
                            fua,
                            device.size,
                        )
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Cache => {
                    let result = self
                        .handler
                        .cache(request.offset, request.length, device.size)
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Unknown(cmd) => {
                    warn!("Unknown NBD command: {}", cmd);
                    self.send_unit_result(
                        request.cookie,
                        Err(super::error::CommandError::InvalidArgument),
                    )
                    .await;
                }
            }
        }
    }

    /// Read write data from stream and delegate to handler
    async fn read_write_data(
        &mut self,
        inode: u64,
        offset: u64,
        length: u32,
        fua: bool,
        device_size: u64,
    ) -> super::error::CommandResult<()> {
        use super::error::CommandError;

        // Check for out-of-bounds write - must read and discard data first
        if offset + length as u64 > device_size {
            let mut data = BytesMut::zeroed(length as usize);
            let _ = self.reader.read_exact(&mut data).await;
            return Err(CommandError::NoSpace);
        }

        if length == 0 {
            return Ok(());
        }

        let mut data = BytesMut::zeroed(length as usize);
        self.reader
            .read_exact(&mut data)
            .await
            .map_err(|_| CommandError::IoError)?;

        let data = data.freeze();
        self.handler.write(inode, offset, &data, fua).await
    }

    /// Send read result (with data) as NBD reply
    async fn send_read_result(
        &mut self,
        cookie: u64,
        result: super::error::CommandResult<bytes::Bytes>,
    ) {
        match result {
            Ok(data) => {
                if let Err(e) = self.send_simple_reply(cookie, NBD_SUCCESS, &data).await {
                    debug!("Failed to send reply: {:?}", e);
                }
            }
            Err(e) => {
                let _ = self.send_simple_reply(cookie, e.to_errno(), &[]).await;
            }
        }
    }

    /// Send unit result (no data) as NBD reply
    async fn send_unit_result(&mut self, cookie: u64, result: super::error::CommandResult<()>) {
        match result {
            Ok(()) => {
                if let Err(e) = self.send_simple_reply(cookie, NBD_SUCCESS, &[]).await {
                    debug!("Failed to send reply: {:?}", e);
                }
            }
            Err(e) => {
                let _ = self.send_simple_reply(cookie, e.to_errno(), &[]).await;
            }
        }
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
