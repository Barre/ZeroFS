use crate::checkpoint_manager::CheckpointManager;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::metrics::FileSystemStats;
use crate::fs::stats::FileSystemGlobalStats;
use crate::fs::tracing::AccessTracer;
use crate::rpc::proto::{self, admin_service_server::AdminService};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::net::UnixListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream, UnixListenerStream};

/// Wrap a tokio_stream Stream so it ends when the CancellationToken fires.
fn take_until_cancelled<S: tokio_stream::Stream + Unpin + Send + 'static>(
    mut stream: S,
    shutdown: CancellationToken,
) -> Pin<Box<dyn tokio_stream::Stream<Item = S::Item> + Send>> {
    let output = futures::stream::poll_fn(move |cx| {
        if shutdown.is_cancelled() {
            return std::task::Poll::Ready(None);
        }
        Pin::new(&mut stream).poll_next(cx)
    });
    Box::pin(output)
}
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Clone)]
pub struct AdminRpcServer {
    checkpoint_manager: Arc<CheckpointManager>,
    flush_coordinator: FlushCoordinator,
    tracer: AccessTracer,
    fs_stats: Arc<FileSystemStats>,
    global_stats: Arc<FileSystemGlobalStats>,
    max_bytes: u64,
    shutdown: CancellationToken,
}

impl AdminRpcServer {
    pub fn new(
        checkpoint_manager: Arc<CheckpointManager>,
        flush_coordinator: FlushCoordinator,
        tracer: AccessTracer,
        fs_stats: Arc<FileSystemStats>,
        global_stats: Arc<FileSystemGlobalStats>,
        max_bytes: u64,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            checkpoint_manager,
            flush_coordinator,
            tracer,
            fs_stats,
            global_stats,
            max_bytes,
            shutdown,
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminRpcServer {
    type WatchFileAccessStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::FileAccessEvent, Status>> + Send>>;

    type StreamStatsStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::StatsSnapshot, Status>> + Send>>;

    async fn create_checkpoint(
        &self,
        request: Request<proto::CreateCheckpointRequest>,
    ) -> Result<Response<proto::CreateCheckpointResponse>, Status> {
        let name = request.into_inner().name;

        let info = self
            .checkpoint_manager
            .create_checkpoint(&name)
            .await
            .map_err(|e| Status::internal(format!("Failed to create checkpoint: {}", e)))?;

        Ok(Response::new(proto::CreateCheckpointResponse {
            checkpoint: Some(info.into()),
        }))
    }

    async fn list_checkpoints(
        &self,
        _request: Request<proto::ListCheckpointsRequest>,
    ) -> Result<Response<proto::ListCheckpointsResponse>, Status> {
        let checkpoints = self
            .checkpoint_manager
            .list_checkpoints()
            .await
            .map_err(|e| Status::internal(format!("Failed to list checkpoints: {}", e)))?;

        Ok(Response::new(proto::ListCheckpointsResponse {
            checkpoints: checkpoints.into_iter().map(|c| c.into()).collect(),
        }))
    }

    async fn delete_checkpoint(
        &self,
        request: Request<proto::DeleteCheckpointRequest>,
    ) -> Result<Response<proto::DeleteCheckpointResponse>, Status> {
        let name = request.into_inner().name;

        self.checkpoint_manager
            .delete_checkpoint(&name)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete checkpoint: {}", e)))?;

        Ok(Response::new(proto::DeleteCheckpointResponse {}))
    }

    async fn get_checkpoint_info(
        &self,
        request: Request<proto::GetCheckpointInfoRequest>,
    ) -> Result<Response<proto::GetCheckpointInfoResponse>, Status> {
        let name = request.into_inner().name;

        let info = self
            .checkpoint_manager
            .get_checkpoint_info(&name)
            .await
            .map_err(|e| Status::internal(format!("Failed to get checkpoint info: {}", e)))?;

        match info {
            Some(checkpoint) => Ok(Response::new(proto::GetCheckpointInfoResponse {
                checkpoint: Some(checkpoint.into()),
            })),
            None => Err(Status::not_found(format!(
                "Checkpoint '{}' not found",
                name
            ))),
        }
    }

    async fn watch_file_access(
        &self,
        _request: Request<proto::WatchFileAccessRequest>,
    ) -> Result<Response<Self::WatchFileAccessStream>, Status> {
        let receiver = self.tracer.subscribe();

        let stream = BroadcastStream::new(receiver)
            .filter_map(|result| result.ok())
            .map(|event| Ok(event.into()));

        Ok(Response::new(take_until_cancelled(stream, self.shutdown.clone())))
    }

    async fn flush(
        &self,
        _request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushResponse>, Status> {
        self.flush_coordinator
            .flush()
            .await
            .map_err(|e| Status::internal(format!("Flush failed: {:?}", e)))?;

        Ok(Response::new(proto::FlushResponse {}))
    }

    async fn stream_stats(
        &self,
        request: Request<proto::StreamStatsRequest>,
    ) -> Result<Response<Self::StreamStatsStream>, Status> {
        let interval_ms = request.into_inner().interval_ms.max(250) as u64;
        let fs_stats = Arc::clone(&self.fs_stats);
        let global_stats = Arc::clone(&self.global_stats);
        let max_bytes = self.max_bytes;

        let stream = IntervalStream::new(tokio::time::interval(
            std::time::Duration::from_millis(interval_ms),
        ))
        .map(move |_| {
            let (used_bytes, used_inodes) = global_stats.get_totals();
            Ok(proto::StatsSnapshot {
                timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                files_created: fs_stats.files_created.load(Ordering::Relaxed),
                files_deleted: fs_stats.files_deleted.load(Ordering::Relaxed),
                files_renamed: fs_stats.files_renamed.load(Ordering::Relaxed),
                directories_created: fs_stats.directories_created.load(Ordering::Relaxed),
                directories_deleted: fs_stats.directories_deleted.load(Ordering::Relaxed),
                directories_renamed: fs_stats.directories_renamed.load(Ordering::Relaxed),
                links_created: fs_stats.links_created.load(Ordering::Relaxed),
                links_deleted: fs_stats.links_deleted.load(Ordering::Relaxed),
                links_renamed: fs_stats.links_renamed.load(Ordering::Relaxed),
                read_operations: fs_stats.read_operations.load(Ordering::Relaxed),
                write_operations: fs_stats.write_operations.load(Ordering::Relaxed),
                bytes_read: fs_stats.bytes_read.load(Ordering::Relaxed),
                bytes_written: fs_stats.bytes_written.load(Ordering::Relaxed),
                tombstones_created: fs_stats.tombstones_created.load(Ordering::Relaxed),
                tombstones_processed: fs_stats.tombstones_processed.load(Ordering::Relaxed),
                gc_chunks_deleted: fs_stats.gc_chunks_deleted.load(Ordering::Relaxed),
                gc_runs: fs_stats.gc_runs.load(Ordering::Relaxed),
                total_operations: fs_stats.total_operations.load(Ordering::Relaxed),
                used_bytes,
                used_inodes,
                max_bytes,
            })
        });

        Ok(Response::new(take_until_cancelled(stream, self.shutdown.clone())))
    }
}

/// Serve gRPC over TCP
pub async fn serve_tcp(
    addr: SocketAddr,
    service: AdminRpcServer,
    shutdown: CancellationToken,
) -> Result<()> {
    info!("RPC server listening on {}", addr);

    let grpc_service = proto::admin_service_server::AdminServiceServer::new(service);

    tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(addr, shutdown.cancelled_owned())
        .await
        .with_context(|| format!("Failed to run RPC TCP server on {}", addr))?;

    info!("RPC TCP server shutting down on {}", addr);
    Ok(())
}

/// Serve gRPC over Unix socket
pub async fn serve_unix(
    socket_path: PathBuf,
    service: AdminRpcServer,
    shutdown: CancellationToken,
) -> Result<()> {
    // Remove existing socket file if present
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)
            .with_context(|| format!("Failed to remove existing socket file: {:?}", socket_path))?;
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("Failed to bind RPC Unix socket to {:?}", socket_path))?;

    info!("RPC server listening on Unix socket: {:?}", socket_path);

    let uds_stream = UnixListenerStream::new(listener);

    let grpc_service = proto::admin_service_server::AdminServiceServer::new(service);

    tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_incoming_shutdown(uds_stream, shutdown.cancelled_owned())
        .await
        .with_context(|| format!("Failed to run RPC Unix socket server on {:?}", socket_path))?;

    info!("RPC Unix socket server shutting down at {:?}", socket_path);
    Ok(())
}
