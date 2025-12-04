use crate::checkpoint_manager::CheckpointInfo;
use crate::config::RpcConfig;
use crate::rpc::proto::{self, admin_service_client::AdminServiceClient};
use anyhow::{Context, Result, anyhow};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tonic::Code;
use tonic::Streaming;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub struct RpcClient {
    client: AdminServiceClient<Channel>,
}

impl RpcClient {
    pub async fn connect_tcp(addr: SocketAddr) -> Result<Self> {
        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)
            .context("Invalid endpoint")?
            .connect()
            .await
            .with_context(|| format!("Failed to connect to RPC server at {}", addr))?;

        Ok(Self {
            client: AdminServiceClient::new(channel),
        })
    }

    pub async fn connect_unix(socket_path: PathBuf) -> Result<Self> {
        let socket_path_clone = socket_path.clone();

        // Endpoint requires a URI, but our connector ignores it and uses the socket path
        let channel = Endpoint::try_from("http://localhost")
            .context("Invalid endpoint")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = socket_path_clone.clone();
                async move {
                    let stream = UnixStream::connect(&path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await
            .with_context(|| format!("Failed to connect to RPC server at {:?}", socket_path))?;

        Ok(Self {
            client: AdminServiceClient::new(channel),
        })
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
        let request = proto::CreateCheckpointRequest {
            name: name.to_string(),
        };

        let response = self
            .client
            .clone()
            .create_checkpoint(request)
            .await
            .map_err(|s| anyhow!("{}", s.message()))?
            .into_inner();

        response
            .checkpoint
            .ok_or_else(|| anyhow!("Empty response from server"))?
            .try_into()
            .map_err(|e| anyhow!("Invalid UUID: {}", e))
    }

    pub async fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        let request = proto::ListCheckpointsRequest {};

        let response = self
            .client
            .clone()
            .list_checkpoints(request)
            .await
            .map_err(|s| anyhow!("{}", s.message()))?
            .into_inner();

        response
            .checkpoints
            .into_iter()
            .map(|c| c.try_into().map_err(|e| anyhow!("Invalid UUID: {}", e)))
            .collect()
    }

    pub async fn delete_checkpoint(&self, name: &str) -> Result<()> {
        let request = proto::DeleteCheckpointRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_checkpoint(request)
            .await
            .map_err(|s| anyhow!("{}", s.message()))?;

        Ok(())
    }

    pub async fn get_checkpoint_info(&self, name: &str) -> Result<Option<CheckpointInfo>> {
        let request = proto::GetCheckpointInfoRequest {
            name: name.to_string(),
        };

        let result = self.client.clone().get_checkpoint_info(request).await;

        match result {
            Ok(response) => {
                let info = response
                    .into_inner()
                    .checkpoint
                    .ok_or_else(|| anyhow!("Empty response from server"))?;
                Ok(Some(
                    info.try_into()
                        .map_err(|e| anyhow!("Invalid UUID: {}", e))?,
                ))
            }
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) => Err(anyhow!("RPC call failed: {}", status.message())),
        }
    }

    pub async fn watch_file_access(&self) -> Result<Streaming<proto::FileAccessEvent>> {
        let request = proto::WatchFileAccessRequest {};

        let response = self
            .client
            .clone()
            .watch_file_access(request)
            .await
            .map_err(|s| anyhow!("Failed to start file access stream: {}", s.message()))?;

        Ok(response.into_inner())
    }
}
