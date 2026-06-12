//! gRPC serving over a unix domain socket, the only transport the CSI spec
//! requires from a plugin.

use crate::controller::ControllerService;
use crate::identity::IdentityService;
use crate::node::NodeService;
use crate::proto::csi::v1::controller_server::ControllerServer;
use crate::proto::csi::v1::identity_server::IdentityServer;
use crate::proto::csi::v1::node_server::NodeServer;
use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::info;

/// Which CSI services this process runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    Controller,
    Node,
    All,
}

impl Mode {
    pub fn controller(self) -> bool {
        matches!(self, Mode::Controller | Mode::All)
    }

    pub fn node(self) -> bool {
        matches!(self, Mode::Node | Mode::All)
    }
}

/// Turn a CSI endpoint (`unix:///csi/csi.sock`, `unix:/csi/csi.sock`, or a
/// bare absolute path) into a socket path.
pub fn socket_path(endpoint: &str) -> Result<PathBuf> {
    let path = endpoint
        .strip_prefix("unix://")
        .or_else(|| endpoint.strip_prefix("unix:"))
        .unwrap_or(endpoint);
    if !path.starts_with('/') {
        bail!(
            "endpoint {:?} is not a unix domain socket (expected unix:///path or an absolute path)",
            endpoint
        );
    }
    Ok(PathBuf::from(path))
}

/// Serve the configured services on the endpoint's unix socket until
/// `shutdown` resolves. A stale socket file from a previous run is removed
/// before binding.
pub async fn serve(
    endpoint: &str,
    identity: IdentityService,
    controller: Option<ControllerService>,
    node: Option<NodeService>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let path = socket_path(endpoint)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create socket directory {:?}", parent))?;
    }
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("Failed to remove existing socket file: {:?}", path))?;
    }

    let listener = UnixListener::bind(&path)
        .with_context(|| format!("Failed to bind CSI socket to {:?}", path))?;
    info!("CSI server listening on {:?}", path);

    tonic::transport::Server::builder()
        .add_service(IdentityServer::new(identity))
        .add_optional_service(controller.map(ControllerServer::new))
        .add_optional_service(node.map(NodeServer::new))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
        .with_context(|| format!("Failed to run CSI server on {:?}", path))?;

    info!("CSI server shut down");
    Ok(())
}
