use crate::config::Settings;
use crate::rpc::client::RpcClient;
use anyhow::{Context, Result};
use comfy_table::{Table, presets::UTF8_FULL};
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

pub async fn create_checkpoint(config_path: &Path, name: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoint = client.create_checkpoint(name).await?;

    println!("✓ Checkpoint created successfully!");
    println!("  Name: {}", checkpoint.name);
    println!("  ID: {}", checkpoint.id);
    println!("  Created at: {}", format_timestamp(checkpoint.created_at));

    Ok(())
}

pub async fn list_checkpoints(config_path: &Path) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoints = client.list_checkpoints().await?;

    if checkpoints.is_empty() {
        println!("No checkpoints found.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec!["Name", "ID", "Created At"]);

    for checkpoint in checkpoints {
        table.add_row(vec![
            checkpoint.name,
            checkpoint.id.to_string(),
            format_timestamp(checkpoint.created_at),
        ]);
    }

    println!("{table}");
    Ok(())
}

pub async fn delete_checkpoint(config_path: &Path, name: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    client.delete_checkpoint(name).await?;

    println!("✓ Checkpoint '{}' deleted successfully!", name);
    Ok(())
}

pub async fn get_checkpoint_info(config_path: &Path, name: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoint = client.get_checkpoint_info(name).await?;

    match checkpoint {
        Some(info) => {
            println!("Checkpoint Information:");
            println!("  Name: {}", info.name);
            println!("  ID: {}", info.id);
            println!("  Created at: {}", format_timestamp(info.created_at));
        }
        None => {
            println!("Checkpoint '{}' not found.", name);
        }
    }

    Ok(())
}

fn format_timestamp(timestamp: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let time = UNIX_EPOCH + Duration::from_secs(timestamp);
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
