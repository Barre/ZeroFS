use super::connect_rpc_client;
use anyhow::Result;
use std::path::Path;

pub async fn flush(config_path: &Path) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    client.flush().await?;

    println!("Flush completed successfully");
    Ok(())
}
