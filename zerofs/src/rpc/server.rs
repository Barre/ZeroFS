use crate::checkpoint_manager::{CheckpointInfo, CheckpointManager};
use crate::rpc::ZeroFsService;
use anyhow::{Context, Result};
use futures::prelude::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tarpc::server::{self, Channel};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct ZeroFsServiceImpl {
    checkpoint_manager: Arc<CheckpointManager>,
}

impl ZeroFsServiceImpl {
    pub fn new(checkpoint_manager: Arc<CheckpointManager>) -> Self {
        Self { checkpoint_manager }
    }
}

impl ZeroFsService for ZeroFsServiceImpl {
    async fn create_checkpoint(
        self,
        _context: tarpc::context::Context,
        name: String,
    ) -> Result<CheckpointInfo, String> {
        self.checkpoint_manager
            .create_checkpoint(&name)
            .await
            .map_err(|e| format!("Failed to create checkpoint: {}", e))
    }

    async fn list_checkpoints(
        self,
        _context: tarpc::context::Context,
    ) -> Result<Vec<CheckpointInfo>, String> {
        self.checkpoint_manager
            .list_checkpoints()
            .await
            .map_err(|e| format!("Failed to list checkpoints: {}", e))
    }

    async fn delete_checkpoint(
        self,
        _context: tarpc::context::Context,
        name: String,
    ) -> Result<(), String> {
        self.checkpoint_manager
            .delete_checkpoint(&name)
            .await
            .map_err(|e| format!("Failed to delete checkpoint: {}", e))
    }

    async fn get_checkpoint_info(
        self,
        _context: tarpc::context::Context,
        name: String,
    ) -> Result<Option<CheckpointInfo>, String> {
        self.checkpoint_manager
            .get_checkpoint_info(&name)
            .await
            .map_err(|e| format!("Failed to get checkpoint info: {}", e))
    }
}

pub async fn serve_tcp(
    addr: SocketAddr,
    service: ZeroFsServiceImpl,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind RPC TCP server to {}", addr))?;

    info!("RPC server listening on {}", addr);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("RPC TCP server shutting down on {}", addr);
                break;
            }
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to accept RPC TCP connection: {}", e);
                        continue;
                    }
                };

                let service = service.clone();
                let client_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let framed = Framed::new(stream, LengthDelimitedCodec::new());
                    let transport = tarpc::serde_transport::new(
                        framed,
                        tarpc::tokio_serde::formats::Bincode::default(),
                    );

                    let channel = server::BaseChannel::new(server::Config::default(), transport);

                    info!("RPC client connected from {}", peer_addr);

                    tokio::select! {
                        _ = client_shutdown.cancelled() => {
                            debug!("RPC client handler shutting down");
                        }
                        _ = channel
                            .execute(service.serve())
                            .for_each(|response| async move {
                                tokio::spawn(response);
                            }) => {
                            info!("RPC client disconnected from {}", peer_addr);
                        }
                    }
                });
            }
        }
    }

    Ok(())
}

pub async fn serve_unix(
    socket_path: PathBuf,
    service: ZeroFsServiceImpl,
    shutdown: CancellationToken,
) -> Result<()> {
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)
            .with_context(|| format!("Failed to remove existing socket file: {:?}", socket_path))?;
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("Failed to bind RPC Unix socket to {:?}", socket_path))?;

    info!("RPC server listening on Unix socket: {:?}", socket_path);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("RPC Unix socket server shutting down at {:?}", socket_path);
                break;
            }
            result = listener.accept() => {
                let (stream, _peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to accept RPC Unix socket connection: {}", e);
                        continue;
                    }
                };

                let service = service.clone();
                let client_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let framed = Framed::new(stream, LengthDelimitedCodec::new());
                    let transport = tarpc::serde_transport::new(
                        framed,
                        tarpc::tokio_serde::formats::Bincode::default(),
                    );

                    let channel = server::BaseChannel::new(server::Config::default(), transport);

                    info!("RPC client connected via Unix socket");
                    tokio::select! {
                        _ = client_shutdown.cancelled() => {
                            debug!("RPC client handler shutting down");
                        }
                        _ = channel
                            .execute(service.serve())
                            .for_each(|response| async move {
                                tokio::spawn(response);
                            }) => {
                            info!("RPC client disconnected");
                        }
                    }
                });
            }
        }
    }

    Ok(())
}
