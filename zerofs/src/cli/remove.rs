use crate::config::Settings;
use crate::rpc::client::RpcClient;
use anyhow::{Context, Result};
use std::path::Path;

async fn connect_rpc_client(config_path: &Path) -> Result<RpcClient> {
    let settings = Settings::from_file(config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let rpc_config = settings
        .servers
        .rpc
        .as_ref()
        .context("RPC server not configured in config file")?;

    RpcClient::connect_from_config(rpc_config)
        .await
        .context("Failed to connect to RPC server. Is the server running?")
}

pub async fn remove_file(config_path: &Path, path: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    client.remove_file(path).await?;

    println!("✓ File removed successfully");
    println!("  Path: {}", path);

    Ok(())
}

