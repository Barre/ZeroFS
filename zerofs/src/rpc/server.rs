use crate::checkpoint_manager::CheckpointManager;
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, FileType, InodeId, SetAttributes, SetGid, SetMode, SetUid};
use crate::rpc::proto::{self, admin_service_server::AdminService};
use crate::task::spawn_named;
use anyhow::{Context, Result};
use std::future::Future;
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
use tikv_jemalloc_ctl::{epoch as jemalloc_epoch, stats as jemalloc_stats};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

/// Snapshot of jemalloc memory statistics.
#[derive(Clone, Copy, Default)]
pub struct JemallocMemStats {
    /// Bytes actively allocated by the application.
    pub allocated: u64,
    /// Bytes in physically resident pages mapped by the allocator.
    pub resident: u64,
    /// Bytes in active pages mapped by the allocator.
    pub mapped: u64,
    /// Bytes in virtual memory mappings retained for future reuse.
    pub retained: u64,
    /// Bytes dedicated to allocator metadata.
    pub metadata: u64,
}

impl JemallocMemStats {
    /// Read current jemalloc stats. Advances the epoch first so the values
    /// are fresh.
    pub fn read() -> Self {
        // Advance the epoch to refresh cached stats
        if jemalloc_epoch::mib().and_then(|e| e.advance()).is_err() {
            return Self::default();
        }

        Self {
            allocated: jemalloc_stats::allocated::read().unwrap_or(0) as u64,
            resident: jemalloc_stats::resident::read().unwrap_or(0) as u64,
            mapped: jemalloc_stats::mapped::read().unwrap_or(0) as u64,
            retained: jemalloc_stats::retained::read().unwrap_or(0) as u64,
            metadata: jemalloc_stats::metadata::read().unwrap_or(0) as u64,
        }
    }
}

/// Directory at the filesystem root that RemoveDirectory renames victims
/// into; a background task deletes its contents (precedent for hidden root
/// directories: .nbd).
const TRASH_DIR_NAME: &[u8] = b".zerofs_trash";

/// Root credentials for admin operations: the admin RPC surface is trusted
/// and operates with full filesystem rights.
fn root_auth() -> AuthContext {
    AuthContext {
        uid: 0,
        gid: 0,
        gids: Vec::new(),
    }
}

/// Split a slash-separated path into its components. Empty components are
/// dropped (so a leading slash is optional); "." and ".." are rejected so a
/// path can't dodge the trash-prefix refusal in RemoveDirectory.
fn path_components(path: &str) -> Result<Vec<&[u8]>, Status> {
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" => {}
            "." | ".." => {
                return Err(Status::invalid_argument(format!(
                    "path {path:?} contains a {component:?} component"
                )));
            }
            c => components.push(c.as_bytes()),
        }
    }
    Ok(components)
}

/// Serializes background trash sweeps so concurrent RemoveDirectory calls
/// don't race each other over the same `/.zerofs_trash` entries. Process-global
/// (not per `AdminRpcServer`) so the `webui`-feature build's second server
/// instance shares the one lock instead of sweeping the same trash in parallel.
static TRASH_SWEEP_LOCK: std::sync::OnceLock<Arc<tokio::sync::Mutex<()>>> =
    std::sync::OnceLock::new();

fn trash_sweep_lock() -> Arc<tokio::sync::Mutex<()>> {
    Arc::clone(TRASH_SWEEP_LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(()))))
}

#[derive(Clone)]
pub struct AdminRpcServer {
    checkpoint_manager: Arc<CheckpointManager>,
    fs: Arc<ZeroFS>,
    shutdown: CancellationToken,
}

impl AdminRpcServer {
    pub fn new(
        checkpoint_manager: Arc<CheckpointManager>,
        fs: Arc<ZeroFS>,
        shutdown: CancellationToken,
    ) -> Self {
        let server = Self {
            checkpoint_manager,
            fs,
            shutdown,
        };
        // Drain anything a previous run left in the trash (a crash or
        // shutdown between RemoveDirectory and its sweep finishing);
        // otherwise the leftovers sit there until the next RemoveDirectory.
        server.spawn_trash_sweep();
        server
    }

    /// Sweep /.zerofs_trash in the background. Spawned at construction and
    /// after every RemoveDirectory; runs are serialized by
    /// `trash_sweep_lock`.
    fn spawn_trash_sweep(&self) {
        let fs = Arc::clone(&self.fs);
        let sweep_lock = trash_sweep_lock();
        let shutdown = self.shutdown.clone();
        spawn_named("trash-sweep", async move {
            sweep_trash(fs, sweep_lock, shutdown).await;
        });
    }

    /// Look up /.zerofs_trash, creating it on first use.
    async fn ensure_trash_directory(&self, creds: &Credentials) -> Result<InodeId, Status> {
        match self.fs.lookup(creds, 0, TRASH_DIR_NAME).await {
            Ok(id) => Ok(id),
            Err(FsError::NotFound) => {
                let attr = SetAttributes {
                    mode: SetMode::Set(0o700),
                    uid: SetUid::Set(0),
                    gid: SetGid::Set(0),
                    ..Default::default()
                };
                match self.fs.mkdir(creds, 0, TRASH_DIR_NAME, &attr).await {
                    Ok((id, _)) => Ok(id),
                    // Lost a creation race; the directory exists now.
                    Err(FsError::Exists) => {
                        self.fs.lookup(creds, 0, TRASH_DIR_NAME).await.map_err(|e| {
                            Status::internal(format!("failed to look up trash directory: {e}"))
                        })
                    }
                    Err(e) => Err(Status::internal(format!(
                        "failed to create trash directory: {e}"
                    ))),
                }
            }
            Err(e) => Err(Status::internal(format!(
                "failed to look up trash directory: {e}"
            ))),
        }
    }
}

/// Drain everything under /.zerofs_trash. Runs are serialized by
/// `sweep_lock`; each run re-lists until the trash is empty, so a sweep
/// that's already working picks up entries trashed after it started.
async fn sweep_trash(
    fs: Arc<ZeroFS>,
    sweep_lock: Arc<tokio::sync::Mutex<()>>,
    shutdown: CancellationToken,
) {
    let _guard = sweep_lock.lock().await;
    let auth = root_auth();
    let creds = Credentials::from_auth_context(&auth);
    let trash_id = match fs.lookup(&creds, 0, TRASH_DIR_NAME).await {
        Ok(id) => id,
        Err(FsError::NotFound) => return,
        Err(e) => {
            warn!("trash sweep: failed to look up trash directory: {e}");
            return;
        }
    };
    if let Err(e) = delete_dir_contents(&fs, &auth, trash_id, &shutdown).await {
        // Leftovers stay in the trash; the next RemoveDirectory retries.
        warn!("trash sweep: incomplete: {e}");
    }
}

/// Recursively delete everything inside `dir_id` via the regular fs
/// operations. Entries deleted concurrently (NotFound) are skipped. Bails
/// out when `shutdown` fires, leaving the remainder in the trash.
fn delete_dir_contents<'a>(
    fs: &'a ZeroFS,
    auth: &'a AuthContext,
    dir_id: InodeId,
    shutdown: &'a CancellationToken,
) -> Pin<Box<dyn Future<Output = Result<(), FsError>> + Send + 'a>> {
    const BATCH_SIZE: usize = 256;

    Box::pin(async move {
        let creds = Credentials::from_auth_context(auth);
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            // Entries shift as we delete, so re-list from the start each pass.
            let batch = fs.readdir(auth, dir_id, 0, BATCH_SIZE).await?;
            let entries: Vec<_> = batch
                .entries
                .into_iter()
                .filter(|e| e.name != b"." && e.name != b"..")
                .collect();
            if entries.is_empty() {
                return Ok(());
            }
            for entry in entries {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                if entry.attr.file_type == FileType::Directory {
                    // Even root needs the execute bit to traverse a
                    // directory; restore a usable mode on directories that
                    // would otherwise be undeletable (e.g. mode 0o000).
                    if entry.attr.mode & 0o700 != 0o700 {
                        let attr = SetAttributes {
                            mode: SetMode::Set(0o700),
                            ..Default::default()
                        };
                        match fs.setattr(&creds, entry.fileid, &attr).await {
                            Ok(_) | Err(FsError::NotFound) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    delete_dir_contents(fs, auth, entry.fileid, shutdown).await?;
                    if shutdown.is_cancelled() {
                        return Ok(());
                    }
                }
                match fs.remove(auth, dir_id, &entry.name).await {
                    Ok(()) | Err(FsError::NotFound) => {}
                    Err(e) => return Err(e),
                }
            }
        }
    })
}

#[tonic::async_trait]
impl AdminService for AdminRpcServer {
    type WatchFileAccessStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::FileAccessEvent, Status>> + Send>>;

    type WatchObjectAccessStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::ObjectAccessEvent, Status>> + Send>>;

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
        let receiver = self.fs.tracer.subscribe();

        let stream = BroadcastStream::new(receiver)
            .filter_map(|result| result.ok())
            .map(|event| Ok(event.into()));

        Ok(Response::new(take_until_cancelled(
            stream,
            self.shutdown.clone(),
        )))
    }

    async fn watch_object_access(
        &self,
        _request: Request<proto::WatchObjectAccessRequest>,
    ) -> Result<Response<Self::WatchObjectAccessStream>, Status> {
        let receiver = self.fs.object_tracer.subscribe();

        let stream = BroadcastStream::new(receiver)
            .filter_map(|result| result.ok())
            .map(|event| Ok(event.into()));

        Ok(Response::new(take_until_cancelled(
            stream,
            self.shutdown.clone(),
        )))
    }

    async fn flush(
        &self,
        _request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushResponse>, Status> {
        self.fs
            .flush_coordinator
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
        let fs_stats = Arc::clone(&self.fs.stats);
        let global_stats = Arc::clone(&self.fs.global_stats);
        let seg_stats = self.fs.extent_store.segment_gc_stats();
        let extent_store = self.fs.extent_store.clone();
        let max_bytes = self.fs.max_bytes;
        let stream = IntervalStream::new(tokio::time::interval(std::time::Duration::from_millis(
            interval_ms,
        )))
        .map(move |_| {
            let (used_bytes, used_inodes) = global_stats.get_totals();
            let mem = JemallocMemStats::read();
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
                gc_extents_deleted: fs_stats.gc_extents_deleted.load(Ordering::Relaxed),
                gc_runs: fs_stats.gc_runs.load(Ordering::Relaxed),
                total_operations: fs_stats.total_operations.load(Ordering::Relaxed),
                used_bytes,
                used_inodes,
                max_bytes,
                jemalloc_allocated: mem.allocated,
                jemalloc_resident: mem.resident,
                jemalloc_mapped: mem.mapped,
                jemalloc_retained: mem.retained,
                jemalloc_metadata: mem.metadata,
                segment_gc: Some(proto::SegmentGcStatus {
                    has_run: seg_stats.has_run.load(Ordering::Relaxed),
                    segment_count: seg_stats.segment_count.load(Ordering::Relaxed),
                    appended_bytes: seg_stats.appended_bytes.load(Ordering::Relaxed),
                    live_bytes: seg_stats.live_bytes.load(Ordering::Relaxed),
                    reclaimable_bytes: seg_stats.reclaimable_bytes.load(Ordering::Relaxed),
                    awaiting_delete: seg_stats.awaiting_delete.load(Ordering::Relaxed),
                    awaiting_delete_bytes: seg_stats.awaiting_delete_bytes.load(Ordering::Relaxed),
                    candidate_backlog: seg_stats.candidate_backlog.load(Ordering::Relaxed),
                    chains_deferred: seg_stats.chains_deferred.load(Ordering::Relaxed),
                    saturated: seg_stats.saturated.load(Ordering::Relaxed) != 0,
                    last_deleted: seg_stats.last_deleted.load(Ordering::Relaxed),
                    last_deleted_bytes: seg_stats.last_deleted_bytes.load(Ordering::Relaxed),
                    last_frames_relocated: seg_stats.last_frames_relocated.load(Ordering::Relaxed),
                    last_chains_packed: seg_stats.last_chains_packed.load(Ordering::Relaxed),
                    last_chains_assembled: seg_stats.last_chains_assembled.load(Ordering::Relaxed),
                    last_hot_seams: seg_stats.last_hot_seams.load(Ordering::Relaxed),
                    pinned: seg_stats.pinned.load(Ordering::Relaxed),
                    tier: seg_stats.tier.load(Ordering::Relaxed) as u32,
                    read_directed: seg_stats.read_directed.load(Ordering::Relaxed),
                    reason: seg_stats
                        .reason
                        .lock()
                        .map(|r| r.clone())
                        .unwrap_or_default(),
                    unflushed_bytes: extent_store.unflushed_bytes(),
                }),
            })
        });

        Ok(Response::new(take_until_cancelled(
            stream,
            self.shutdown.clone(),
        )))
    }

    async fn create_directory(
        &self,
        request: Request<proto::CreateDirectoryRequest>,
    ) -> Result<Response<proto::CreateDirectoryResponse>, Status> {
        let req = request.into_inner();
        let components = path_components(&req.path)?;

        // Anything under /.zerofs_trash would be deleted by the next sweep
        // with no error returned to anyone; refuse upfront, mirroring the
        // RemoveDirectory guard.
        if components.first() == Some(&TRASH_DIR_NAME) {
            return Err(Status::invalid_argument(format!(
                "refusing to create {:?}: /{} is reserved",
                req.path,
                String::from_utf8_lossy(TRASH_DIR_NAME)
            )));
        }

        let auth = root_auth();
        let creds = Credentials::from_auth_context(&auth);
        let attr = SetAttributes {
            mode: SetMode::Set(req.mode & 0o7777),
            uid: SetUid::Set(req.uid),
            gid: SetGid::Set(req.gid),
            ..Default::default()
        };

        // Walk from the root, creating missing components (mkdir -p). The
        // root itself ("/") always exists, so an empty path reports
        // created=false.
        let mut dir_id: InodeId = 0;
        let mut created = false;
        for &name in &components {
            let display = String::from_utf8_lossy(name);
            let existing = match self.fs.lookup(&creds, dir_id, name).await {
                Ok(id) => Some(id),
                Err(FsError::NotFound) => None,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "CreateDirectory {:?}: lookup of {display:?} failed: {e}",
                        req.path
                    )));
                }
            };
            let (id, was_created) = match existing {
                Some(id) => (id, false),
                None => match self.fs.mkdir(&creds, dir_id, name, &attr).await {
                    Ok((id, _)) => (id, true),
                    // Lost a race against a concurrent create; use the winner.
                    Err(FsError::Exists) => match self.fs.lookup(&creds, dir_id, name).await {
                        Ok(id) => (id, false),
                        Err(e) => {
                            return Err(Status::internal(format!(
                                "CreateDirectory {:?}: lookup of {display:?} failed: {e}",
                                req.path
                            )));
                        }
                    },
                    Err(e) => {
                        return Err(Status::internal(format!(
                            "CreateDirectory {:?}: mkdir {display:?} failed: {e}",
                            req.path
                        )));
                    }
                },
            };
            if !was_created {
                let inode = self.fs.inode_store.get(id).await.map_err(|e| {
                    Status::internal(format!(
                        "CreateDirectory {:?}: failed to load inode for {display:?}: {e}",
                        req.path
                    ))
                })?;
                if !matches!(inode, Inode::Directory(_)) {
                    return Err(Status::failed_precondition(format!(
                        "CreateDirectory {:?}: {display:?} exists and is not a directory",
                        req.path
                    )));
                }
            }
            dir_id = id;
            created = was_created;
        }

        Ok(Response::new(proto::CreateDirectoryResponse { created }))
    }

    async fn remove_directory(
        &self,
        request: Request<proto::RemoveDirectoryRequest>,
    ) -> Result<Response<proto::RemoveDirectoryResponse>, Status> {
        let path = request.into_inner().path;
        let components = path_components(&path)?;

        let Some((&name, parents)) = components.split_last() else {
            return Err(Status::invalid_argument(
                "refusing to remove the filesystem root",
            ));
        };
        if components[0] == TRASH_DIR_NAME {
            return Err(Status::invalid_argument(format!(
                "refusing to remove {path:?}: /{} is reserved",
                String::from_utf8_lossy(TRASH_DIR_NAME)
            )));
        }

        let auth = root_auth();
        let creds = Credentials::from_auth_context(&auth);

        // Resolve the parent chain. A missing or non-directory component
        // means there is no directory at `path`, which counts as already
        // removed (the RPC is idempotent).
        let mut dir_id: InodeId = 0;
        for &component in parents {
            match self.fs.lookup(&creds, dir_id, component).await {
                Ok(id) => dir_id = id,
                Err(FsError::NotFound | FsError::NotDirectory) => {
                    return Ok(Response::new(proto::RemoveDirectoryResponse {}));
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "RemoveDirectory {path:?}: lookup of {:?} failed: {e}",
                        String::from_utf8_lossy(component)
                    )));
                }
            }
        }

        let target_id = match self.fs.lookup(&creds, dir_id, name).await {
            Ok(id) => id,
            Err(FsError::NotFound | FsError::NotDirectory) => {
                return Ok(Response::new(proto::RemoveDirectoryResponse {}));
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "RemoveDirectory {path:?}: lookup failed: {e}"
                )));
            }
        };
        let inode = self.fs.inode_store.get(target_id).await.map_err(|e| {
            Status::internal(format!(
                "RemoveDirectory {path:?}: failed to load inode: {e}"
            ))
        })?;
        if !matches!(inode, Inode::Directory(_)) {
            return Err(Status::failed_precondition(format!(
                "RemoveDirectory {path:?}: not a directory"
            )));
        }

        let trash_id = self.ensure_trash_directory(&creds).await?;

        // Move the directory into the trash under a unique name so the RPC
        // returns immediately; the actual deletion happens in the background.
        let trash_name = uuid::Uuid::new_v4().to_string();
        match self
            .fs
            .rename(&auth, dir_id, name, trash_id, trash_name.as_bytes())
            .await
        {
            Ok(()) => {}
            // A concurrent removal won the race; nothing left to do.
            Err(FsError::NotFound) => {
                return Ok(Response::new(proto::RemoveDirectoryResponse {}));
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "RemoveDirectory {path:?}: rename into trash failed: {e}"
                )));
            }
        }
        info!(
            "RemoveDirectory: moved {path:?} to /{}/{trash_name}",
            String::from_utf8_lossy(TRASH_DIR_NAME)
        );

        self.spawn_trash_sweep();

        Ok(Response::new(proto::RemoveDirectoryResponse {}))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::db::SlateDbHandle;
    use crate::rpc::client::RpcClient;
    use slatedb::DbBuilder;
    use slatedb::object_store::path::Path as DbPath;
    use std::time::Duration;

    /// Build an in-memory ZeroFS plus the CheckpointManager the admin server
    /// needs. The slatedb handle is constructed here (instead of via
    /// ZeroFS::new_in_memory) because the CheckpointManager needs it too.
    async fn make_fs() -> (Arc<ZeroFS>, Arc<CheckpointManager>) {
        let test_key = [0u8; 32];
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let block_transformer: Arc<dyn slatedb::BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());
        let db_path = DbPath::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path.clone(), Arc::clone(&object_store))
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        let db_handle = SlateDbHandle::ReadWrite(slatedb);
        let fs = Arc::new(
            ZeroFS::new_with_slatedb(
                db_handle.clone(),
                u64::MAX,
                None,
                false,
                Arc::clone(&object_store),
                crate::frame_codec::FrameCodec::new(
                    &test_key,
                    crate::segment::SEGMENT_INFO,
                    CompressionConfig::default(),
                ),
            )
            .await
            .unwrap(),
        );
        fs.start_reclaim_drainer();
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            db_handle,
            db_path,
            object_store,
            None,
        ));
        {
            let fc = fs.flush_coordinator.clone();
            checkpoint_manager.set_pre_flush(Arc::new(move || {
                let fc = fc.clone();
                Box::pin(async move {
                    fc.flush()
                        .await
                        .map_err(|e| anyhow::anyhow!("seal+flush failed: {:?}", e))
                })
            }));
        }
        (fs, checkpoint_manager)
    }

    /// Build an in-memory ZeroFS plus a real admin RPC server on a unix
    /// socket, and connect a real client to it. Mirrors the in-process test
    /// pattern from mount.rs (no mocks).
    async fn setup() -> (Arc<ZeroFS>, RpcClient, CancellationToken, tempfile::TempDir) {
        let (fs, checkpoint_manager) = make_fs().await;

        let shutdown = CancellationToken::new();
        let service = AdminRpcServer::new(checkpoint_manager, Arc::clone(&fs), shutdown.clone());

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("admin.sock");
        {
            let sock = sock.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let _ = serve_unix(sock, service, shutdown).await;
            });
        }
        let client = connect_with_retry(&sock).await;
        (fs, client, shutdown, dir)
    }

    async fn connect_with_retry(sock: &std::path::Path) -> RpcClient {
        for _ in 0..100 {
            if let Ok(c) = RpcClient::connect_unix(sock.to_path_buf()).await {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("admin RPC client failed to connect");
    }

    fn root_creds() -> Credentials {
        Credentials::from_auth_context(&root_auth())
    }

    async fn resolve_path(fs: &Arc<ZeroFS>, path: &str) -> Result<InodeId, FsError> {
        let creds = root_creds();
        let mut id = 0;
        for component in path.split('/').filter(|c| !c.is_empty()) {
            id = fs.lookup(&creds, id, component.as_bytes()).await?;
        }
        Ok(id)
    }

    async fn trash_entry_count(fs: &Arc<ZeroFS>) -> usize {
        let trash_id = match fs.lookup(&root_creds(), 0, TRASH_DIR_NAME).await {
            Ok(id) => id,
            Err(_) => return 0,
        };
        fs.readdir(&root_auth(), trash_id, 0, 64)
            .await
            .unwrap()
            .entries
            .iter()
            .filter(|e| e.name != b"." && e.name != b"..")
            .count()
    }

    async fn wait_for_trash_drained(fs: &Arc<ZeroFS>) {
        for _ in 0..500 {
            if trash_entry_count(fs).await == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("trash directory was not drained");
    }

    #[tokio::test]
    async fn create_directory_creates_parents_and_is_idempotent() {
        let (fs, client, shutdown, _dir) = setup().await;

        let created = client
            .create_directory("/volumes/pvc-1", 0o770, 1000, 1000)
            .await
            .unwrap();
        assert!(created);

        // The directory (and the parent created on the way) exist with the
        // requested attributes.
        let id = resolve_path(&fs, "/volumes/pvc-1").await.unwrap();
        match fs.inode_store.get(id).await.unwrap() {
            Inode::Directory(d) => {
                assert_eq!(d.mode, 0o770);
                assert_eq!(d.uid, 1000);
                assert_eq!(d.gid, 1000);
            }
            other => panic!("expected a directory, got {other:?}"),
        }

        // Same path again: success, nothing created.
        let created = client
            .create_directory("/volumes/pvc-1", 0o770, 1000, 1000)
            .await
            .unwrap();
        assert!(!created);

        // An intermediate that exists also reports created=false.
        let created = client
            .create_directory("/volumes", 0o770, 1000, 1000)
            .await
            .unwrap();
        assert!(!created);

        // The root always exists.
        let created = client.create_directory("/", 0o777, 0, 0).await.unwrap();
        assert!(!created);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn create_directory_refuses_non_directory_component() {
        let (fs, client, shutdown, _dir) = setup().await;
        fs.create(&root_creds(), 0, b"blocker", &SetAttributes::default())
            .await
            .unwrap();

        let err = client
            .create_directory("/blocker", 0o777, 0, 0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not a directory"),
            "unexpected error: {err}"
        );

        let err = client
            .create_directory("/blocker/sub", 0o777, 0, 0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not a directory"),
            "unexpected error: {err}"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn remove_directory_moves_to_trash_and_background_deletes() {
        let (fs, client, shutdown, _dir) = setup().await;
        let auth = root_auth();
        let creds = root_creds();

        client
            .create_directory("/volumes/pvc-2", 0o777, 0, 0)
            .await
            .unwrap();
        let volumes_id = resolve_path(&fs, "/volumes").await.unwrap();
        let volume_id = resolve_path(&fs, "/volumes/pvc-2").await.unwrap();

        // Populate a nested tree, including a directory whose mode lacks the
        // execute bit (the sweeper has to chmod it before it can delete the
        // contents).
        let (file_id, _) = fs
            .create(&creds, volume_id, b"data.bin", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &bytes::Bytes::from(vec![7u8; 100_000]))
            .await
            .unwrap();
        let (sub_id, _) = fs
            .mkdir(&creds, volume_id, b"sub", &SetAttributes::default())
            .await
            .unwrap();
        let (nested_id, _) = fs
            .create(&creds, sub_id, b"nested.txt", &SetAttributes::default())
            .await
            .unwrap();
        let no_exec = SetAttributes {
            mode: SetMode::Set(0o600),
            ..Default::default()
        };
        fs.setattr(&creds, sub_id, &no_exec).await.unwrap();

        client.remove_directory("/volumes/pvc-2").await.unwrap();

        // Gone from its original location as soon as the RPC returns.
        assert_eq!(
            fs.lookup(&creds, volumes_id, b"pvc-2").await.unwrap_err(),
            FsError::NotFound
        );

        wait_for_trash_drained(&fs).await;

        // The whole tree is actually deleted, not just unlinked.
        for id in [volume_id, file_id, sub_id, nested_id] {
            assert_eq!(
                fs.inode_store.get(id).await.unwrap_err(),
                FsError::NotFound,
                "inode {id} survived the trash sweep"
            );
        }
        // The trash directory itself stays for reuse.
        fs.lookup(&creds, 0, TRASH_DIR_NAME).await.unwrap();

        // Idempotent: the path is gone now, and missing parents are fine too.
        client.remove_directory("/volumes/pvc-2").await.unwrap();
        client.remove_directory("/no/such/path").await.unwrap();

        shutdown.cancel();
    }

    #[tokio::test]
    async fn startup_sweep_drains_trash_left_by_previous_run() {
        let (fs, checkpoint_manager) = make_fs().await;
        let creds = root_creds();

        // Simulate a previous run that crashed between RemoveDirectory and
        // the sweep: a populated directory sitting in the trash.
        let (trash_id, _) = fs
            .mkdir(&creds, 0, TRASH_DIR_NAME, &SetAttributes::default())
            .await
            .unwrap();
        let (leftover_id, _) = fs
            .mkdir(&creds, trash_id, b"leftover", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&creds, leftover_id, b"data.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Constructing the server spawns the startup sweep.
        let shutdown = CancellationToken::new();
        let _service = AdminRpcServer::new(checkpoint_manager, Arc::clone(&fs), shutdown.clone());

        wait_for_trash_drained(&fs).await;
        assert_eq!(
            fs.inode_store.get(leftover_id).await.unwrap_err(),
            FsError::NotFound
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn create_directory_refuses_trash_paths() {
        let (_fs, client, shutdown, _dir) = setup().await;

        for path in ["/.zerofs_trash", "/.zerofs_trash/foo", ".zerofs_trash/a/b"] {
            let err = client
                .create_directory(path, 0o777, 0, 0)
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("reserved"),
                "expected refusal for {path:?}, got: {err}"
            );
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn remove_directory_refuses_root_and_trash_paths() {
        let (_fs, client, shutdown, _dir) = setup().await;

        for path in ["/", "", "/.zerofs_trash", "/.zerofs_trash/foo"] {
            let err = client.remove_directory(path).await.unwrap_err();
            assert!(
                err.to_string().contains("refusing"),
                "expected refusal for {path:?}, got: {err}"
            );
        }

        // ".." could sidestep the trash-prefix refusal, so it's rejected.
        let err = client
            .remove_directory("/volumes/../.zerofs_trash")
            .await
            .unwrap_err();
        assert!(err.to_string().contains(".."), "unexpected error: {err}");

        shutdown.cancel();
    }

    #[tokio::test]
    async fn remove_directory_refuses_non_directory_target() {
        let (fs, client, shutdown, _dir) = setup().await;
        fs.create(&root_creds(), 0, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        let err = client.remove_directory("/file.txt").await.unwrap_err();
        assert!(
            err.to_string().contains("not a directory"),
            "unexpected error: {err}"
        );

        shutdown.cancel();
    }
}
