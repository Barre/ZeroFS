use deku::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

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
    mut stream: TcpStream,
    filesystem: Arc<SlateDbFs>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = NinePHandler::new(filesystem);

    loop {
        let mut size_buf = [0u8; 4];
        match stream.read_exact(&mut size_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Client disconnected");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let size = u32::from_le_bytes(size_buf);
        if !(7..=DEFAULT_MSIZE).contains(&size) {
            error!("Invalid message size: {}", size);
            return Err("Invalid message size".into());
        }

        let mut msg_buf = vec![0u8; (size - 4) as usize];
        stream.read_exact(&mut msg_buf).await?;

        let mut full_buf = Vec::with_capacity(size as usize);
        full_buf.extend_from_slice(&size_buf);
        full_buf.extend_from_slice(&msg_buf);

        let response = match P9Message::from_bytes((&full_buf, 0)) {
            Ok((_, parsed)) => {
                debug!(
                    "Received message type {} tag {}: {:?}",
                    parsed.type_, parsed.tag, parsed.body
                );
                handler.handle_message(parsed.tag, parsed.body).await
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
                    P9Message::error(tag, libc::ENOSYS as u32)
                } else {
                    debug!("Message too short to parse: {:?}", e);
                    return Err(format!("Failed to parse message: {e:?}").into());
                }
            }
        };

        let response_bytes = response
            .to_bytes()
            .map_err(|e| format!("Failed to serialize response: {e:?}"))?;

        stream.write_all(&response_bytes).await?;
    }
}
