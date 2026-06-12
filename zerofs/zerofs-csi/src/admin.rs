//! Client for the ZeroFS gateway admin RPC surface (CreateDirectory /
//! RemoveDirectory). One short-lived connection per CSI RPC keeps the
//! controller stateless across gateway restarts.

use crate::proto::admin::{
    CreateDirectoryRequest, RemoveDirectoryRequest, admin_service_client::AdminServiceClient,
};
use anyhow::{Context, Result};
use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub struct AdminClient {
    client: AdminServiceClient<Channel>,
}

impl AdminClient {
    /// Connect to a gateway admin endpoint. Accepted forms:
    /// `unix:///path/to.sock` (or `unix:/path`), `http://host:port`, or a
    /// bare `host:port` (treated as http).
    pub async fn connect(endpoint: &str) -> Result<Self> {
        if let Some(path) = endpoint
            .strip_prefix("unix://")
            .or_else(|| endpoint.strip_prefix("unix:"))
        {
            return Self::connect_unix(PathBuf::from(path)).await;
        }

        let url = if endpoint.contains("://") {
            endpoint.to_string()
        } else {
            format!("http://{}", endpoint)
        };
        let channel = Channel::from_shared(url.clone())
            .with_context(|| format!("Invalid admin endpoint: {}", endpoint))?
            .connect()
            .await
            .with_context(|| format!("Failed to connect to admin endpoint {}", url))?;

        Ok(Self {
            client: AdminServiceClient::new(channel),
        })
    }

    async fn connect_unix(socket_path: PathBuf) -> Result<Self> {
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
            .with_context(|| format!("Failed to connect to admin endpoint {:?}", socket_path))?;

        Ok(Self {
            client: AdminServiceClient::new(channel),
        })
    }

    /// mkdir -p `path` on the gateway. Returns true when this call created
    /// the leaf directory, false when it already existed.
    pub async fn create_directory(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<bool, tonic::Status> {
        let response = self
            .client
            .clone()
            .create_directory(CreateDirectoryRequest {
                path: path.to_string(),
                mode,
                uid,
                gid,
            })
            .await?;
        Ok(response.into_inner().created)
    }

    /// Remove `path` and everything below it on the gateway. Idempotent;
    /// deletion completes in the background on the gateway.
    pub async fn remove_directory(&self, path: &str) -> Result<(), tonic::Status> {
        self.client
            .clone()
            .remove_directory(RemoveDirectoryRequest {
                path: path.to_string(),
            })
            .await?;
        Ok(())
    }
}
