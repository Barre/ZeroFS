use anyhow::{Context, Result};
use clap::Parser;
use zerofs_csi::controller::ControllerService;
use zerofs_csi::identity::IdentityService;
use zerofs_csi::node::{MountAccess, NodeService};
use zerofs_csi::server::{Mode, serve};

#[derive(Parser)]
#[command(
    name = "zerofs-csi",
    version,
    about = "ZeroFS CSI driver (csi.zerofs.net)"
)]
struct Cli {
    /// CSI gRPC endpoint (unix domain socket)
    #[arg(long, default_value = "unix:///csi/csi.sock")]
    endpoint: String,

    /// Node name reported by NodeGetInfo; required when running the node
    /// service
    #[arg(long, env = "NODE_ID")]
    node_id: Option<String>,

    /// Which CSI services to run
    #[arg(long, value_enum, default_value_t = Mode::All)]
    mode: Mode,

    /// zerofs binary used to mount volumes in node mode
    #[arg(long, default_value = "zerofs")]
    zerofs_bin: String,

    /// Who may access the mounts (zerofs mount --access). `all` is what pods
    /// need; it requires root or user_allow_other in /etc/fuse.conf
    #[arg(long, value_enum, default_value_t = MountAccess::All)]
    mount_access: MountAccess,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let controller = cli.mode.controller().then(ControllerService::new);
    let node = if cli.mode.node() {
        let node_id = cli
            .node_id
            .clone()
            .context("--node-id (or NODE_ID) is required when running the node service")?;
        Some(NodeService::new(
            node_id,
            cli.zerofs_bin.clone(),
            cli.mount_access,
        ))
    } else {
        None
    };
    let identity = IdentityService::new(cli.mode.controller());

    serve(&cli.endpoint, identity, controller, node, shutdown_signal()).await
}

/// Resolves on SIGTERM (how Kubernetes stops the container) or Ctrl-C.
async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
