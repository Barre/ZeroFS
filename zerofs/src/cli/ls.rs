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

pub async fn list_files(config_path: &Path, prefix: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let entries = client.list_files(prefix).await?;

    if entries.is_empty() {
        println!("(empty)");
        return Ok(());
    }

    println!("{:<6} {:>12} {}", "TYPE", "SIZE", "PATH");
    for entry in entries {
        let kind = if entry.is_dir { "DIR" } else { "FILE" };
        println!("{:<6} {:>12} {}", kind, entry.size, entry.path);
    }

    Ok(())
}

