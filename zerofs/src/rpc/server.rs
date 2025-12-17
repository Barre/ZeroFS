use crate::checkpoint_manager::CheckpointManager;
use crate::fs::errors::FsError;
use crate::fs::permissions::Credentials;
use crate::fs::tracing::AccessTracer;
use crate::fs::types::{AuthContext, SetAttributes, SetSize};
use crate::fs::ZeroFS;
use crate::rpc::proto::{self, admin_service_server::AdminService};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Clone)]
pub struct AdminRpcServer {
    checkpoint_manager: Arc<CheckpointManager>,
    tracer: AccessTracer,
    filesystem: Arc<ZeroFS>,
}

impl AdminRpcServer {
    pub fn new(
        checkpoint_manager: Arc<CheckpointManager>,
        tracer: AccessTracer,
        filesystem: Arc<ZeroFS>,
    ) -> Self {
        Self {
            checkpoint_manager,
            tracer,
            filesystem,
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminRpcServer {
    type WatchFileAccessStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::FileAccessEvent, Status>> + Send>>;

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

        Ok(Response::new(Box::pin(stream)))
    }

    async fn truncate_file(
        &self,
        request: Request<proto::TruncateFileRequest>,
    ) -> Result<Response<proto::TruncateFileResponse>, Status> {
        let proto::TruncateFileRequest { path, size } = request.into_inner();
        let (dir_components, filename) = parse_path(&path)?;
        let auth = AuthContext::default();
        let creds = Credentials::from_auth_context(&auth);

        let parent_dir = ensure_directory_path(
            &self.filesystem,
            &auth,
            &dir_components,
            /*create_missing=*/ true,
        )
        .await?;

        let file_id = match self
            .filesystem
            .lookup(&creds, parent_dir, &filename)
            .await
        {
            Ok(id) => {
                let inode = self
                    .filesystem
                    .inode_store
                    .get(id)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to load inode: {e}")))?;
                if !inode.is_file() {
                    return Err(Status::failed_precondition(format!(
                        "Path '{}' exists but is not a file",
                        path
                    )));
                }
                id
            }
            Err(FsError::NotFound) => {
                let (id, _) = self
                    .filesystem
                    .create(&creds, parent_dir, &filename, &SetAttributes::default())
                    .await
                    .map_err(|e| fs_error_to_status("create file", e, path.clone()))?;
                id
            }
            Err(e) => return Err(fs_error_to_status("lookup", e, path.clone())),
        };

        let setattr = SetAttributes {
            size: SetSize::Set(size),
            ..Default::default()
        };

        self.filesystem
            .setattr(&creds, file_id, &setattr)
            .await
            .map_err(|e| fs_error_to_status("truncate file", e, path.clone()))?;

        Ok(Response::new(proto::TruncateFileResponse {}))
    }

    async fn remove_file(
        &self,
        request: Request<proto::RemoveFileRequest>,
    ) -> Result<Response<proto::RemoveFileResponse>, Status> {
        let proto::RemoveFileRequest { path } = request.into_inner();
        let (dir_components, filename) = parse_path(&path)?;
        let auth = AuthContext::default();

        let parent_dir = ensure_directory_path(
            &self.filesystem,
            &auth,
            &dir_components,
            /*create_missing=*/ false,
        )
        .await?;

        self.filesystem
            .remove(&auth, parent_dir, &filename)
            .await
            .map_err(|e| fs_error_to_status("remove file", e, path.clone()))?;

        Ok(Response::new(proto::RemoveFileResponse {}))
    }

    async fn list_files(
        &self,
        request: Request<proto::ListFilesRequest>,
    ) -> Result<Response<proto::ListFilesResponse>, Status> {
        let proto::ListFilesRequest { path } = request.into_inner();
        let auth = AuthContext::default();
        let dir_id = resolve_directory(&self.filesystem, &auth, &path).await?;
        let read_dir = self
            .filesystem
            .readdir(&auth, dir_id, 0, usize::MAX)
            .await
            .map_err(|e| fs_error_to_status("readdir", e, path.clone()))?;

        let mut entries = Vec::with_capacity(read_dir.entries.len());
        for entry in read_dir.entries {
            if entry.name == b"." || entry.name == b".." {
                continue;
            }
            let inode = self
                .filesystem
                .inode_store
                .get(entry.fileid)
                .await
                .map_err(|e| Status::internal(format!("Failed to load inode: {e}")))?;
            let name = String::from_utf8_lossy(&entry.name).to_string();
            entries.push(proto::FileEntry {
                path: format_path(&path, &name),
                size: inode.size(),
                is_dir: inode.is_directory(),
            });
        }

        Ok(Response::new(proto::ListFilesResponse { entries }))
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

/// Parse a path string into (parent_components, filename).
fn parse_path(path: &str) -> Result<(Vec<Vec<u8>>, Vec<u8>), Status> {
    let components: Vec<Vec<u8>> = path
        .split('/')
        .filter(|c| !c.is_empty())
        .map(|c| c.as_bytes().to_vec())
        .collect();

    if components.is_empty() {
        return Err(Status::invalid_argument("Path must not be empty or root"));
    }

    let (dirs, file) = components.split_at(components.len() - 1);
    Ok((dirs.to_vec(), file[0].clone()))
}

/// Ensure the directory path exists. Optionally creates missing components.
async fn ensure_directory_path(
    fs: &ZeroFS,
    auth: &AuthContext,
    components: &[Vec<u8>],
    create_missing: bool,
) -> Result<u64, Status> {
    let creds = Credentials::from_auth_context(auth);
    let mut current = 0;

    for comp in components {
        match fs.lookup(&creds, current, comp).await {
            Ok(next) => {
                let inode = fs
                    .inode_store
                    .get(next)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to load inode: {e}")))?;
                if !inode.is_directory() {
                    return Err(Status::failed_precondition(format!(
                        "Path component '{}' is not a directory",
                        String::from_utf8_lossy(comp)
                    )));
                }
                current = next;
            }
            Err(FsError::NotFound) if create_missing => {
                let (_id, _) = fs
                    .mkdir(&creds, current, comp, &SetAttributes::default())
                    .await
                    .map_err(|e| fs_error_to_status("mkdir", e, String::from_utf8_lossy(comp).into()))?;
                let next = fs
                    .lookup(&creds, current, comp)
                    .await
                    .map_err(|e| fs_error_to_status("lookup", e, String::from_utf8_lossy(comp).into()))?;
                current = next;
            }
            Err(e) => {
                return Err(fs_error_to_status(
                    "lookup",
                    e,
                    String::from_utf8_lossy(comp).into(),
                ))
            }
        }
    }

    Ok(current)
}

async fn resolve_directory(fs: &ZeroFS, auth: &AuthContext, path: &str) -> Result<u64, Status> {
    if path.is_empty() || path == "/" {
        return Ok(0);
    }

    let components: Vec<Vec<u8>> = path
        .split('/')
        .filter(|c| !c.is_empty())
        .map(|c| c.as_bytes().to_vec())
        .collect();

    ensure_directory_path(fs, auth, &components, false).await
}

fn format_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() || prefix == "/" {
        format!("/{}", name)
    } else if prefix.ends_with('/') {
        format!("{}{}", prefix, name)
    } else {
        format!("{}/{}", prefix, name)
    }
}

fn fs_error_to_status(op: &str, err: FsError, path: String) -> Status {
    match err {
        FsError::NotFound => Status::not_found(format!("{}: '{}' not found", op, path)),
        FsError::Exists => Status::already_exists(format!("{}: '{}' already exists", op, path)),
        FsError::NoSpace => Status::resource_exhausted(format!("{}: no space left", op)),
        FsError::IsDirectory => {
            Status::failed_precondition(format!("{}: '{}' is a directory", op, path))
        }
        FsError::NotDirectory => {
            Status::failed_precondition(format!("{}: '{}' is not a directory", op, path))
        }
        FsError::InvalidArgument => {
            Status::invalid_argument(format!("{}: invalid argument for '{}'", op, path))
        }
        other => Status::internal(format!("{} on '{}': {}", op, path, other)),
    }
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
