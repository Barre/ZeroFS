use deku::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::handler::NinePHandler;
use super::protocol::P9Message;
use crate::filesystem::SlateDbFs;
use crate::ninep::handler::DEFAULT_MSIZE;

pub struct NinePServer {
    filesystem: Arc<SlateDbFs>,
    addr: SocketAddr,
}

impl NinePServer {
    pub fn new(filesystem: Arc<SlateDbFs>, addr: SocketAddr) -> Self {
        Self { filesystem, addr }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("9P server listening on {}", self.addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("9P client connected from {}", addr);

            // Set TCP_NODELAY for better latency
            stream.set_nodelay(true)?;

            let filesystem = Arc::clone(&self.filesystem);
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, filesystem).await {
                    error!("Error handling 9P client {}: {}", addr, e);
                }
            });
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    filesystem: Arc<SlateDbFs>,
) -> Result<(), Box<dyn std::error::Error>> {
    let handler = Arc::new(NinePHandler::new(filesystem));

    let (mut read_stream, mut write_stream) = stream.into_split();

    let (tx, mut rx) = mpsc::channel::<(u16, Vec<u8>)>(100);

    let writer_task = tokio::spawn(async move {
        while let Some((tag, response_bytes)) = rx.recv().await {
            if let Err(e) = write_stream.write_all(&response_bytes).await {
                error!("Failed to write response for tag {}: {}", tag, e);
                break;
            }
        }
    });

    loop {
        let mut size_buf = [0u8; 4];
        match read_stream.read_exact(&mut size_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Client disconnected");
                drop(tx);
                writer_task.await?;
                return Ok(());
            }
            Err(e) => {
                drop(tx);
                writer_task.await?;
                return Err(e.into());
            }
        }

        let size = u32::from_le_bytes(size_buf);
        if !(7..=DEFAULT_MSIZE).contains(&size) {
            error!("Invalid message size: {}", size);
            drop(tx);
            writer_task.await?;
            return Err("Invalid message size".into());
        }

        let mut msg_buf = vec![0u8; (size - 4) as usize];
        read_stream.read_exact(&mut msg_buf).await?;

        let mut full_buf = Vec::with_capacity(size as usize);
        full_buf.extend_from_slice(&size_buf);
        full_buf.extend_from_slice(&msg_buf);

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
                if full_buf.len() >= 7 {
                    let tag = u16::from_le_bytes([full_buf[5], full_buf[6]]);
                    let msg_type = full_buf[4];
                    debug!(
                        "Failed to parse message type {} (0x{:02x}) tag {}: {:?}",
                        msg_type, msg_type, tag, e
                    );
                    debug!(
                        "Message size: {}, buffer (first 40 bytes): {:?}",
                        size,
                        &full_buf[0..std::cmp::min(40, full_buf.len())]
                    );
                    let error_msg = P9Message::error(tag, libc::ENOSYS as u32);
                    let response_bytes = error_msg
                        .to_bytes()
                        .map_err(|e| format!("Failed to serialize error response: {e:?}"))?;

                    if let Err(e) = tx.send((tag, response_bytes)).await {
                        error!("Failed to send error response: {}", e);
                        drop(tx);
                        writer_task.await?;
                        return Err(e.into());
                    }
                } else {
                    debug!("Message too short to parse: {:?}", e);
                    drop(tx);
                    writer_task.await?;
                    return Err(format!("Failed to parse message: {e:?}").into());
                }
            }
        };
    }
}
