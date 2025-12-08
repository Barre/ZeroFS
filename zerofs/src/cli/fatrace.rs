use crate::config::Settings;
use crate::rpc::client::RpcClient;
use crate::rpc::proto::{FileAccessEvent, FileOperation, OperationParams};
use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio_stream::StreamExt;

pub async fn run_fatrace(config_path: PathBuf) -> Result<()> {
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let rpc_config = settings
        .servers
        .rpc
        .as_ref()
        .context("RPC server not configured in config file")?;

    let client = RpcClient::connect_from_config(rpc_config).await?;
    let mut stream = client.watch_file_access().await?;

    println!("Tracing file access (Ctrl+C to stop)...");

    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => print_event(&event),
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn print_event(event: &FileAccessEvent) {
    let op = FileOperation::try_from(event.operation)
        .map(|o| format!("{}", o))
        .unwrap_or_else(|_| "??".to_string());
    let params = format_params(event.params.as_ref(), event.operation);
    println!("{} | {}{}", op, event.path, params);
}

fn format_params(params: Option<&OperationParams>, op: i32) -> String {
    let (params, op) = match (params, FileOperation::try_from(op).ok()) {
        (Some(p), Some(o)) => (p, o),
        _ => return String::new(),
    };

    match op {
        FileOperation::Read | FileOperation::Write | FileOperation::Trim => {
            format!(
                " offset={} len={}",
                params.offset.unwrap_or(0),
                params.length.unwrap_or(0)
            )
        }
        FileOperation::Create
        | FileOperation::Mkdir
        | FileOperation::Setattr
        | FileOperation::Mknod => params
            .mode
            .map(|m| format!(" mode={:04o}", m))
            .unwrap_or_default(),
        FileOperation::Rename | FileOperation::Link => params
            .new_path
            .as_ref()
            .map(|p| format!(" -> {}", p))
            .unwrap_or_default(),
        FileOperation::Symlink => params
            .link_target
            .as_ref()
            .map(|t| format!(" -> {}", t))
            .unwrap_or_default(),
        FileOperation::Lookup => params
            .filename
            .as_ref()
            .map(|f| format!(" name={}", f))
            .unwrap_or_default(),
        _ => String::new(),
    }
}
