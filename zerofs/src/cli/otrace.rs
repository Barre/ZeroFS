use crate::cli::monitor::format_bytes_human;
use crate::config::Settings;
use crate::rpc::client::RpcClient;
use crate::rpc::proto::{ObjectAccessEvent, ObjectOperation, ObjectParams};
use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio_stream::StreamExt;

pub async fn run_otrace(config_path: PathBuf) -> Result<()> {
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let rpc_config = settings
        .servers
        .rpc
        .as_ref()
        .context("RPC server not configured in config file")?;

    let client = RpcClient::connect_from_config(rpc_config).await?;
    let mut stream = client.watch_object_access().await?;

    println!("Tracing object store requests (Ctrl+C to stop)...");

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

fn print_event(event: &ObjectAccessEvent) {
    let op = ObjectOperation::try_from(event.operation)
        .map(|o| format!("{}", o))
        .unwrap_or_else(|_| "??".to_string());
    let params = format_params(event.params.as_ref(), event.operation);
    let timing = event
        .duration_us
        .map(|us| format!("  {}", format_duration(us)))
        .unwrap_or_default();
    let err = if event.error { "  ERR" } else { "" };
    // Only tag the non-default store: with no separate WAL configured every
    // request hits "data", so the label would just repeat on every line.
    let store = if event.store == "data" {
        String::new()
    } else {
        format!("{} ", event.store)
    };
    println!(
        "{} | {}{}{}{}{}",
        op, store, event.path, params, timing, err
    );
}

fn format_params(params: Option<&ObjectParams>, op: i32) -> String {
    let (params, op) = match (params, ObjectOperation::try_from(op).ok()) {
        (Some(p), Some(o)) => (p, o),
        _ => return String::new(),
    };

    match op {
        ObjectOperation::ObjGet => {
            // Offset is a position, kept exact; length is a byte count, humanized.
            let off = params
                .offset
                .map(|o| format!(" off={}", o))
                .unwrap_or_default();
            let len = params
                .length
                .map(|l| format!(" len={}", format_bytes_human(l)))
                .unwrap_or_default();
            format!("{}{}", off, len)
        }
        ObjectOperation::ObjPut => params
            .size
            .map(|s| format!(" size={}", format_bytes_human(s)))
            .unwrap_or_default(),
        ObjectOperation::ObjCopy | ObjectOperation::ObjRename => params
            .target_path
            .as_ref()
            .map(|t| format!(" -> {}", t))
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn format_duration(us: u64) -> String {
    if us < 1000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}
