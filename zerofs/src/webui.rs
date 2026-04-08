use crate::config::WebUIConfig;
use crate::fs::ZeroFS;
use crate::ninep::handler::NinePHandler;
use crate::ninep::lock_manager::FileLockManager;
use crate::ninep::protocol::P9_CHANNEL_SIZE;
use crate::ninep::server::dispatch_9p_frame;
use crate::rpc::proto;
use crate::rpc::server::AdminRpcServer;
use crate::task::spawn_named;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use dashmap::DashMap;
use rust_embed::Embed;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, error, info};

#[derive(Embed)]
#[folder = "../webui/dist"]
struct WebUIAssets;

#[derive(Clone)]
struct AppState {
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    uid: u32,
    gid: u32,
}

async fn ws_9p_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_9p_ws(socket, state))
}

async fn handle_9p_ws(socket: WebSocket, state: AppState) {
    let handler = Arc::new(
        NinePHandler::new(state.filesystem, state.lock_manager.clone())
            .with_credential_override(state.uid, state.gid),
    );
    let handler_id = handler.handler_id();
    let inflight: Arc<DashMap<u16, Arc<tokio::sync::Notify>>> = Arc::new(DashMap::new());
    let pending_flushes: Arc<DashMap<u16, u16>> = Arc::new(DashMap::new());

    let (tx, mut rx) = mpsc::channel::<(u16, Vec<u8>)>(P9_CHANNEL_SIZE);

    // Writer task: sends response bytes as WS binary messages
    let (mut ws_tx, mut ws_rx) = socket.split();

    let writer = spawn_named("9p-ws-writer", async move {
        use futures::SinkExt;
        while let Some((_tag, response_bytes)) = rx.recv().await {
            if ws_tx
                .send(WsMessage::Binary(response_bytes.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    use futures::StreamExt;
    loop {
        match ws_rx.next().await {
            Some(Ok(WsMessage::Binary(data))) => {
                if let Err(e) = dispatch_9p_frame(&data, &handler, &tx, &inflight, &pending_flushes)
                {
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

    drop(tx);
    let _ = writer.await;
    state.lock_manager.release_session_locks(handler_id).await;
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
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
    let state = AppState {
        filesystem,
        lock_manager,
        uid: config.uid,
        gid: config.gid,
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
        info!("Web UI server listening on http://{}", addr);
        let app = app.clone();
        let shutdown = shutdown.clone();
        handles.push(spawn_named("webui-http", async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to bind Web UI server to {}: {}", addr, e);
                    return Ok(());
                }
            };
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }));
    }
    handles
}
