use crate::config::WebUIConfig;
use crate::fs::ZeroFS;
use crate::ninep::handler::{NinePHandler, SessionReleaseGuard};
use crate::ninep::lock_manager::FileLockManager;
use crate::ninep::server::{InflightRegistry, dispatch_9p_frame, response_may_be_emitted};
use crate::rpc::proto;
use crate::rpc::server::AdminRpcServer;
use crate::task::spawn_named;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use ninep_proto::P9_CHANNEL_SIZE;
use rust_embed::Embed;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker, TaskTrackerToken};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, error, info, warn};

#[derive(Embed)]
#[folder = "../webui/dist"]
struct WebUIAssets;

#[derive(Clone)]
struct AppState {
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    uid: u32,
    gid: u32,
    shutdown: CancellationToken,
    ws_drain: TaskTracker,
}

const WS_DRAIN_TIMEOUT: std::time::Duration = crate::replication::RESPONSE_DRAIN_TIMEOUT;

async fn ws_9p_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    // Register the session before returning the upgrade response.
    let drain_guard = state.ws_drain.token();
    ws.on_upgrade(move |socket| handle_9p_ws(socket, state, drain_guard))
}

async fn handle_9p_ws(socket: WebSocket, state: AppState, _drain_guard: TaskTrackerToken) {
    let response_db = Arc::clone(&state.filesystem.db);
    let handler = Arc::new(
        NinePHandler::new(
            Arc::clone(&state.filesystem),
            Arc::clone(&state.lock_manager),
        )
        .with_credential_override(state.uid, state.gid),
    );
    let mut release_guard = SessionReleaseGuard::new(Arc::clone(&handler));
    let inflight = InflightRegistry::default();

    let (tx, mut rx) = mpsc::channel::<(u16, Vec<u8>)>(P9_CHANNEL_SIZE);

    // Writer task: sends response bytes as WS binary messages
    let (mut ws_tx, mut ws_rx) = socket.split();
    let response_authority_lost = CancellationToken::new();
    let writer_authority_lost = response_authority_lost.clone();

    // Abort on early exit; normal teardown drains responses for a bounded interval.
    let mut writer = AbortOnDropHandle::new(spawn_named("9p-ws-writer", async move {
        use futures::SinkExt;
        while let Some((tag, response_bytes)) = rx.recv().await {
            if !response_may_be_emitted(&response_db, &response_bytes) {
                warn!(
                    "Dropping successful WebSocket 9P response for tag {tag} after serving \
                     authority was lost; closing the connection"
                );
                writer_authority_lost.cancel();
                break;
            }
            if ws_tx
                .send(WsMessage::Binary(response_bytes.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }));

    use futures::StreamExt;
    loop {
        let next = tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => {
                debug!("9P WebSocket handler shutting down");
                break;
            }
            _ = response_authority_lost.cancelled() => {
                debug!("9P WebSocket handler closing after serving authority loss");
                break;
            }
            next = ws_rx.next() => next,
        };
        match next {
            Some(Ok(WsMessage::Binary(data))) => {
                if let Err(e) = dispatch_9p_frame(data, &handler, &tx, &inflight) {
                    error!("9P WebSocket dispatch error: {}", e);
                    break;
                }
            }
            Some(Ok(WsMessage::Close(_))) | None => {
                debug!("9P WebSocket client disconnected");
                break;
            }
            Some(Err(e)) => {
                debug!("9P WebSocket read error: {}", e);
                break;
            }
            _ => {} // ping/pong/text ignored
        }
    }

    // Disconnect before awaiting request tasks to block late resource installs.
    release_guard.release();
    drop(tx);

    // `SinkExt::send` flushes queued CLEAN responses to the WebSocket transport.
    // Bound the drain for unrelated stalled handlers.
    if tokio::time::timeout(WS_DRAIN_TIMEOUT, &mut writer)
        .await
        .is_err()
    {
        writer.abort();
        let _ = writer.await;
        tracing::warn!("timed out draining 9P WebSocket responses during shutdown");
    }
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("wasm") => "application/wasm",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        _ => "application/octet-stream",
    }
}

fn cache_control(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

async fn serve_spa(axum::extract::Path(path): axum::extract::Path<String>) -> impl IntoResponse {
    serve_asset(&path)
}

async fn serve_index() -> impl IntoResponse {
    serve_asset("index.html")
}

fn serve_asset(path: &str) -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::Response;

    if let Some(file) = WebUIAssets::get(path) {
        Response::builder()
            .header(header::CONTENT_TYPE, content_type(path))
            .header(header::CACHE_CONTROL, cache_control(path))
            .body(axum::body::Body::from(file.data.to_vec()))
            .unwrap()
    } else if let Some(index) = WebUIAssets::get("index.html") {
        Response::builder()
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(axum::body::Body::from(index.data.to_vec()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Web UI not available"))
            .unwrap()
    }
}

pub fn start(
    config: &WebUIConfig,
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    rpc_service: AdminRpcServer,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let ws_drain = TaskTracker::new();
    let state = AppState {
        filesystem,
        lock_manager,
        uid: config.uid,
        gid: config.gid,
        shutdown: shutdown.clone(),
        ws_drain: ws_drain.clone(),
    };

    // gRPC-web: wrap tonic service with GrpcWebService + CORS
    let grpc_service = proto::admin_service_server::AdminServiceServer::new(rpc_service);
    let grpc_web_service = tower::ServiceBuilder::new()
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::mirror_request())
                .allow_headers(tower_http::cors::Any)
                .expose_headers(tower_http::cors::Any),
        )
        .layer(GrpcWebLayer::new())
        .service(grpc_service);

    let app = Router::new()
        // 9P over WebSocket
        .route("/ws/9p", get(ws_9p_upgrade))
        // gRPC-web
        .route_service("/zerofs.admin.AdminService/{method}", grpc_web_service)
        // Static assets and SPA fallback
        .route("/{*path}", get(serve_spa))
        .route("/", get(serve_index))
        .with_state(state);

    let mut handles = Vec::new();
    for &addr in &config.addresses {
        let app = app.clone();
        let shutdown = shutdown.clone();
        let ws_drain = ws_drain.clone();
        handles.push(spawn_named("webui-http", async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to bind Web UI server to {}: {}", addr, e);
                    return Ok(());
                }
            };
            info!("Web UI server listening on http://{}", addr);
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()));
            ws_drain.close();
            if tokio::time::timeout(WS_DRAIN_TIMEOUT, ws_drain.wait())
                .await
                .is_err()
            {
                tracing::warn!("timed out waiting for 9P WebSocket sessions to drain");
            }
            result
        }));
    }
    handles
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::routing::post;
    use std::process::Stdio;

    #[derive(Clone)]
    struct SmokeState {
        app: AppState,
        connections: Arc<std::sync::Mutex<CancellationToken>>,
        connection_closed: Arc<tokio::sync::Notify>,
    }

    async fn smoke_ws_upgrade(
        ws: WebSocketUpgrade,
        State(state): State<SmokeState>,
    ) -> impl IntoResponse {
        let connection_shutdown = state.connections.lock().unwrap().clone();
        let connection_closed = state.connection_closed.clone();
        let drain_guard = state.app.ws_drain.token();
        ws.on_upgrade(move |socket| async move {
            tokio::select! {
                _ = handle_9p_ws(socket, state.app, drain_guard) => {}
                _ = connection_shutdown.cancelled() => {}
            }
            connection_closed.notify_one();
        })
    }

    async fn drop_smoke_connections(State(state): State<SmokeState>) -> StatusCode {
        let closed = state.connection_closed.notified();
        tokio::pin!(closed);
        closed.as_mut().enable();
        {
            let mut shutdown = state.connections.lock().unwrap();
            shutdown.cancel();
            *shutdown = CancellationToken::new();
        }
        match tokio::time::timeout(std::time::Duration::from_secs(1), closed).await {
            Ok(()) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Browser-runtime smoke test requiring generated wasm output and Node 22.
    #[tokio::test]
    #[ignore]
    async fn wasm_client_smoke() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.expect("in-memory filesystem"));
        let state = SmokeState {
            app: AppState {
                filesystem,
                lock_manager: Arc::new(FileLockManager::new()),
                uid: 0,
                gid: 0,
                shutdown: CancellationToken::new(),
                ws_drain: TaskTracker::new(),
            },
            connections: Arc::new(std::sync::Mutex::new(CancellationToken::new())),
            connection_closed: Arc::new(tokio::sync::Notify::new()),
        };
        let app = Router::new()
            .route("/ws/9p", get(smoke_ws_upgrade))
            .route("/drop-connections", post(drop_smoke_connections))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind smoke server");
        let address = listener.local_addr().expect("smoke server address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve smoke endpoint");
        });

        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../webui/scripts/smoke-wasm-client.mjs");
        let output = tokio::process::Command::new("node")
            .arg(script)
            .arg(format!("ws://{address}/ws/9p"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .expect("run Node WASM smoke client");
        server.abort();

        assert!(
            output.status.success(),
            "WASM client failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
