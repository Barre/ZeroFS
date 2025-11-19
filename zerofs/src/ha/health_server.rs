/// Simple HTTP health check server for ZeroFS HA
/// 
/// Provides endpoints:
/// - GET /health - Always returns 200 if server is running
/// - GET /ready - Returns 200 only if node is Active, 503 if Standby/Fenced

use super::server_mode::HAState;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

pub async fn start_health_server(ha_state: Arc<HAState>, port: u16) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Health check server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let ha_state_clone = Arc::clone(&ha_state);
                tokio::spawn(async move {
                    if let Err(e) = handle_health_request(stream, ha_state_clone).await {
                        error!("Error handling health check from {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting health check connection: {}", e);
            }
        }
    }
}

async fn handle_health_request(mut stream: TcpStream, ha_state: Arc<HAState>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(&mut stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return send_response(&mut stream, 400, "Bad Request").await;
    }

    let method = parts[0];
    let path = parts[1];

    if method != "GET" {
        return send_response(&mut stream, 405, "Method Not Allowed").await;
    }

    match path {
        "/health" => {
            // Always return 200 if server is running
            send_response(&mut stream, 200, "OK").await
        }
        "/ready" => {
            // Return 200 only if Active, 503 if Standby/Fenced
            let mode = ha_state.get_mode().await;
            let (status, body) = match mode {
                super::server_mode::ServerMode::Active => (200, "OK"),
                super::server_mode::ServerMode::Standby => (503, "Service Unavailable - Standby"),
                super::server_mode::ServerMode::Fenced => (503, "Service Unavailable - Fenced"),
                super::server_mode::ServerMode::Initializing => (503, "Service Unavailable - Initializing"),
            };
            send_response(&mut stream, status, body).await
        }
        _ => {
            send_response(&mut stream, 404, "Not Found").await
        }
    }
}

async fn send_response(stream: &mut TcpStream, status: u16, body: &str) -> anyhow::Result<()> {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status, status_text, body.len(), body
    );

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

