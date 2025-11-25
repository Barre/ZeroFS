use crate::checkpoint_manager::CheckpointInfo;
use crate::config::RpcConfig;
use crate::rpc::ZeroFsServiceClient;
use anyhow::{Context, Result, anyhow};
use std::net::SocketAddr;
use std::path::PathBuf;
use tarpc::client;
use tarpc::tokio_serde::formats::Bincode;
use tokio::net::{TcpStream, UnixStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct RpcClient {
    client: ZeroFsServiceClient,
}

impl RpcClient {
    pub async fn connect_tcp(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("Failed to connect to RPC server at {}", addr))?;

        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let transport = tarpc::serde_transport::new(framed, Bincode::default());

        let client = ZeroFsServiceClient::new(client::Config::default(), transport).spawn();

        Ok(Self { client })
    }

    pub async fn connect_unix(socket_path: PathBuf) -> Result<Self> {
        let stream = UnixStream::connect(&socket_path)
            .await
            .with_context(|| format!("Failed to connect to RPC server at {:?}", socket_path))?;

        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let transport = tarpc::serde_transport::new(framed, Bincode::default());

        let client = ZeroFsServiceClient::new(client::Config::default(), transport).spawn();

        Ok(Self { client })
    }

    /// Connect to RPC server using config (tries Unix socket first, then TCP)
    pub async fn connect_from_config(config: &RpcConfig) -> Result<Self> {
        if let Some(socket_path) = &config.unix_socket
            && socket_path.exists()
        {
            match Self::connect_unix(socket_path.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    tracing::warn!("Failed to connect via Unix socket: {}", e);
                }
            }
        }

        if let Some(addresses) = &config.addresses {
            for &addr in addresses {
                match Self::connect_tcp(addr).await {
                    Ok(client) => return Ok(client),
                    Err(e) => {
                        tracing::warn!("Failed to connect to {}: {}", addr, e);
                    }
                }
            }
        }

        Err(anyhow!("Failed to connect to RPC server"))
    }

    pub async fn create_checkpoint(&self, name: &str) -> Result<CheckpointInfo> {
        self.client
            .create_checkpoint(tarpc::context::current(), name.to_string())
            .await
            .context("RPC call failed")?
            .map_err(|e| anyhow!("{}", e))
    }

    pub async fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        self.client
            .list_checkpoints(tarpc::context::current())
            .await
            .context("RPC call failed")?
            .map_err(|e| anyhow!("{}", e))
    }

    pub async fn delete_checkpoint(&self, name: &str) -> Result<()> {
        self.client
            .delete_checkpoint(tarpc::context::current(), name.to_string())
            .await
            .context("RPC call failed")?
            .map_err(|e| anyhow!("{}", e))
    }

    pub async fn get_checkpoint_info(&self, name: &str) -> Result<Option<CheckpointInfo>> {
        self.client
            .get_checkpoint_info(tarpc::context::current(), name.to_string())
            .await
            .context("RPC call failed")?
            .map_err(|e| anyhow!("{}", e))
    }
}
