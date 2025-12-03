use super::handler::NinePHandler;
use super::lock_manager::FileLockManager;
use super::protocol::{
    P9_CHANNEL_SIZE, P9_DEBUG_BUFFER_SIZE, P9_MIN_MESSAGE_SIZE, P9_SIZE_FIELD_LEN, P9Message,
};
use crate::fs::ZeroFS;
use crate::ninep::handler::DEFAULT_MSIZE;
use bytes::BytesMut;
use deku::prelude::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub enum Transport {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

pub struct NinePServer {
    filesystem: Arc<ZeroFS>,
    transport: Transport,
    lock_manager: Arc<FileLockManager>,
}

impl NinePServer {
    pub fn new(filesystem: Arc<ZeroFS>, addr: SocketAddr) -> Self {
        Self {
            filesystem,
            transport: Transport::Tcp(addr),
            lock_manager: Arc::new(FileLockManager::new()),
        }
    }

    pub fn new_unix(filesystem: Arc<ZeroFS>, path: PathBuf) -> Self {
        Self {
            filesystem,
            transport: Transport::Unix(path),
            lock_manager: Arc::new(FileLockManager::new()),
        }
    }

    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        match &self.transport {
            Transport::Tcp(addr) => {
                let listener = TcpListener::bind(addr).await?;
                info!("9P server listening on TCP {}", addr);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("9P TCP server shutting down on {}", addr);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, peer_addr) = result?;
                            info!("9P client connected from {}", peer_addr);

                            stream.set_nodelay(true)?;

                            let filesystem = Arc::clone(&self.filesystem);
                            let lock_manager = Arc::clone(&self.lock_manager);
                            let client_shutdown = shutdown.child_token();

                            tokio::spawn(async move {
                                if let Err(e) = handle_client_stream(stream, filesystem, lock_manager, client_shutdown).await
                                {
                                    error!("Error handling 9P client {}: {}", peer_addr, e);
                                }
                            });
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                let _ = std::fs::remove_file(path);

                let listener = UnixListener::bind(path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to bind Unix socket at {:?}: {}", path, e),
                    )
                })?;
                info!("9P server listening on Unix socket {:?}", path);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("9P Unix socket server shutting down at {:?}", path);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, _) = result?;
                            info!("9P client connected via Unix socket");

                            let filesystem = Arc::clone(&self.filesystem);
                            let lock_manager = Arc::clone(&self.lock_manager);
                            let client_shutdown = shutdown.child_token();

                            tokio::spawn(async move {
                                if let Err(e) = handle_client_stream(stream, filesystem, lock_manager, client_shutdown).await
                                {
                                    error!("Error handling 9P Unix client: {}", e);
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
    lock_manager: Arc<FileLockManager>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let handler = Arc::new(NinePHandler::new(filesystem, lock_manager.clone()));
    let handler_id = handler.handler_id();

    let (mut read_stream, mut write_stream) = tokio::io::split(stream);

    let (tx, mut rx) = mpsc::channel::<(u16, Vec<u8>)>(P9_CHANNEL_SIZE);

    let writer_task = tokio::spawn(async move {
        while let Some((tag, response_bytes)) = rx.recv().await {
            if let Err(e) = write_stream.write_all(&response_bytes).await {
                error!("Failed to write response for tag {}: {}", tag, e);
                break;
            }
        }
    });

    let result = handle_client_loop(handler, &mut read_stream, tx, shutdown).await;

    lock_manager.release_session_locks(handler_id).await;

    let _ = writer_task.await;

    result
}

async fn handle_client_loop<R>(
    handler: Arc<NinePHandler>,
    read_stream: &mut R,
    tx: mpsc::Sender<(u16, Vec<u8>)>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    loop {
        let mut size_buf = [0u8; P9_SIZE_FIELD_LEN];

        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!("9P client handler shutting down");
                return Ok(());
            }
            result = read_stream.read_exact(&mut size_buf) => {
                match result {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        debug!("Client disconnected");
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }

        let size = u32::from_le_bytes(size_buf);
        if !(P9_MIN_MESSAGE_SIZE..=DEFAULT_MSIZE).contains(&size) {
            error!("Invalid message size: {}", size);
            return Err(anyhow::anyhow!("Invalid message size"));
        }

        let mut full_buf = BytesMut::with_capacity(size as usize);
        full_buf.extend_from_slice(&size_buf);
        full_buf.resize(size as usize, 0);

        read_stream
            .read_exact(&mut full_buf[P9_SIZE_FIELD_LEN..])
            .await?;

        match P9Message::from_bytes((&full_buf, 0)) {
            Ok((_, parsed)) => {
                debug!(
                    "Received message type {} tag {}: {:?}",
                    parsed.type_, parsed.tag, parsed.body
                );

                let tag = parsed.tag;
                let body = parsed.body;
                let handler = Arc::clone(&handler);
                let tx = tx.clone();

                tokio::spawn(async move {
                    let response = handler.handle_message(tag, body).await;
                    match response.to_bytes() {
                        Ok(response_bytes) => {
                            if let Err(e) = tx.send((tag, response_bytes)).await {
                                warn!("Failed to send response for tag {}: {}", tag, e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to serialize response for tag {}: {:?}", tag, e);
                        }
                    }
                });
            }
            Err(e) => {
                // Failed to parse message - check if we can at least get the tag
                if full_buf.len() >= P9_MIN_MESSAGE_SIZE as usize {
                    let tag = u16::from_le_bytes([full_buf[5], full_buf[6]]);
                    let msg_type = full_buf[4];
                    debug!(
                        "Failed to parse message type {} (0x{:02x}) tag {}: {:?}",
                        msg_type, msg_type, tag, e
                    );
                    debug!(
                        "Message size: {}, buffer (first {} bytes): {:?}",
                        size,
                        P9_DEBUG_BUFFER_SIZE,
                        &full_buf[0..std::cmp::min(P9_DEBUG_BUFFER_SIZE, full_buf.len())]
                    );
                    let error_msg = P9Message::error(tag, libc::ENOSYS as u32);
                    let response_bytes = error_msg.to_bytes().map_err(|e| {
                        anyhow::anyhow!("Failed to serialize error response: {e:?}")
                    })?;

                    if let Err(e) = tx.send((tag, response_bytes)).await {
                        error!("Failed to send error response: {}", e);
                        return Err(e.into());
                    }
                } else {
                    debug!("Message too short to parse: {:?}", e);
                    return Err(anyhow::anyhow!("Failed to parse message: {e:?}"));
                }
            }
        };
    }
}
