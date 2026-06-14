//! CSI Node service. There is no staging step: NodePublishVolume mounts the
//! volume's gateway subdirectory directly at the target path by spawning a
//! `zerofs mount <gateway> <target> --aname <volumesRoot>/<volume_id>` child.
//! The 9P client inside that child reconnects with fid replay, so a gateway
//! restart does not break published volumes. The children are however tied to
//! this process: if the node plugin container restarts, its FUSE mounts die
//! with it (documented v1 limitation; dedicated mount pods are the v2 plan).

use crate::controller::{
    PARAM_GATEWAY, PARAM_VOLUMES_ROOT, validate_volume_capability, volume_path,
};
use crate::proto::csi::v1::node_server::Node;
use crate::proto::csi::v1::{
    NodeExpandVolumeRequest, NodeExpandVolumeResponse, NodeGetCapabilitiesRequest,
    NodeGetCapabilitiesResponse, NodeGetInfoRequest, NodeGetInfoResponse,
    NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse, NodePublishVolumeRequest,
    NodePublishVolumeResponse, NodeServiceCapability, NodeStageVolumeRequest,
    NodeStageVolumeResponse, NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse,
    NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, VolumeUsage, node_service_capability,
    volume_capability, volume_usage,
};
use std::collections::HashMap;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

/// Default 9P port, matching `zerofs mount`'s own default, used when the
/// gateway address omits one.
const DEFAULT_9P_PORT: u16 = 5564;

/// How long NodePublishVolume waits for the mount to come up before killing
/// the child and failing.
const MOUNT_TIMEOUT: Duration = Duration::from_secs(15);

/// How long the gateway-reachability probe may take. The probe is a
/// cancellable async connect to the gateway's 9P address; on timeout the
/// future is dropped without parking any thread.
const GATEWAY_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Deadline for the one statfs the stats path still issues (only after the
/// gateway probe has confirmed the gateway is reachable, so it should
/// answer). A pure safety net for the gateway dying between the probe and
/// the syscall; on timeout the blocking thread is released once statfs
/// eventually returns.
const STATFS_TIMEOUT: Duration = Duration::from_secs(5);

/// How many trailing stderr lines from the mount child are kept for error
/// reporting.
const STDERR_KEEP_LINES: usize = 50;

/// Who may access the mounts, passed through as `zerofs mount --access`.
/// Pods run as arbitrary users, so the production default is `all` (which
/// needs root or user_allow_other in /etc/fuse.conf).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum MountAccess {
    Owner,
    Root,
    #[default]
    All,
}

impl MountAccess {
    fn as_arg(self) -> &'static str {
        match self {
            MountAccess::Owner => "owner",
            MountAccess::Root => "root",
            MountAccess::All => "all",
        }
    }
}

pub struct NodeService {
    node_id: String,
    /// Binary spawned to mount volumes; "zerofs" from PATH by default.
    zerofs_bin: String,
    mount_access: MountAccess,
    state: Arc<NodeState>,
}

/// A `zerofs mount` child the node plugin is serving a target with.
#[derive(Clone)]
struct Child {
    pid: u32,
    /// The 9P address this mount connects to, kept so unpublish and stats —
    /// which never see the volume's `volume_context` — can probe whether the
    /// gateway is reachable without going through the (possibly wedged) mount.
    gateway: String,
}

struct NodeState {
    /// target path -> the `zerofs mount` child serving it. Entries are removed
    /// by the per-child reaper task when the child exits.
    children: StdMutex<HashMap<PathBuf, Child>>,
    /// Per-target locks serializing publish/unpublish on the same path.
    target_locks: StdMutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
}

impl NodeState {
    fn target_lock(&self, target: &Path) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.target_locks.lock().unwrap();
        Arc::clone(locks.entry(target.to_path_buf()).or_default())
    }

    /// Drop the lock entry for `target` when no one else holds it, so the
    /// map doesn't grow with every pod that ever mounted a volume here.
    /// Called after unpublish, with the caller's own clone already dropped.
    fn release_target_lock(&self, target: &Path) {
        let mut locks = self.target_locks.lock().unwrap();
        // The map itself holds one reference; with the map locked no one can
        // clone it concurrently, so a count of 1 means idle.
        if let Some(entry) = locks.get(target)
            && Arc::strong_count(entry) == 1
        {
            locks.remove(target);
        }
    }
}

/// The state of a publish target, determined without a syscall that can hang
/// on a wedged mount: the mount table says whether it is a FUSE mount, the
/// serving pid's liveness tells a live mount from a corpse, and a cancellable
/// gateway probe tells a healthy mount from one blocked on a dead gateway.
enum TargetState {
    /// A live FUSE mount whose gateway is reachable.
    FuseMounted,
    /// A FUSE mount whose serving child is gone; needs a lazy unmount before
    /// remounting (a statfs here would return ENOTCONN).
    BrokenFuse,
    /// A FUSE mount whose child is alive but whose gateway is unreachable, so
    /// it is blocked behind the 9P client's reconnect loop. Left in place to
    /// reconnect on its own.
    Unresponsive,
    /// Not a FUSE mount: a plain directory or a path that does not exist yet.
    /// Publish creates it if needed and mounts fresh.
    NotMounted,
}

fn statfs(path: &Path) -> std::io::Result<libc::statfs> {
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: c_path is a valid NUL-terminated string and st is a valid
    // out-pointer for the call's duration.
    let rc = unsafe { libc::statfs(c_path.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(st)
}

/// statfs without hanging the caller: the syscall runs on a blocking thread
/// with a deadline. A FUSE mount whose gateway is unreachable blocks statfs
/// until the 9P reconnect succeeds; that shows up here as a TimedOut error
/// (the blocking thread is released back to the pool once the syscall
/// eventually returns).
async fn statfs_async(path: &Path) -> std::io::Result<libc::statfs> {
    let buf = path.to_path_buf();
    match tokio::time::timeout(
        STATFS_TIMEOUT,
        tokio::task::spawn_blocking(move || statfs(&buf)),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(join_err)) => Err(std::io::Error::other(join_err)),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "statfs {} did not answer within {:?}",
                path.display(),
                STATFS_TIMEOUT
            ),
        )),
    }
}

/// Whether `pid` names a live process. kill(pid, 0) probes existence without
/// sending a signal: 0 means alive, EPERM means alive but not ours, ESRCH
/// means gone.
fn process_alive(pid: u32) -> bool {
    // SAFETY: signal 0 sends nothing; it only reports whether the pid exists.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Octal-unescape a /proc/self/mountinfo field (space, tab, newline and
/// backslash are encoded as \040 \011 \012 \134). Returns raw bytes for an
/// exact, encoding-agnostic comparison against a path.
fn unescape_mountinfo(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\'
            && i + 3 < b.len()
            && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c))
        {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}

/// The filesystem type of the mount at `target`, or None if `target` is not a
/// mountpoint. Read from /proc/self/mountinfo, which the kernel renders from
/// the mount table without ever touching the FUSE daemon, so it cannot hang on
/// a wedged mount the way statfs would.
fn mount_fstype(target: &Path) -> std::io::Result<Option<String>> {
    let want = target.as_os_str().as_bytes();
    let content = std::fs::read_to_string("/proc/self/mountinfo")?;
    for line in content.lines() {
        // Optional fields end at " - "; the mount point is field 4 of the part
        // before it, the fstype is the first field after.
        let Some((pre, post)) = line.split_once(" - ") else {
            continue;
        };
        let Some(mount_point) = pre.split(' ').nth(4) else {
            continue;
        };
        if unescape_mountinfo(mount_point) == want {
            return Ok(Some(post.split(' ').next().unwrap_or("").to_string()));
        }
    }
    Ok(None)
}

/// Whether `target` is currently a FUSE mountpoint.
fn is_fuse_mount(target: &Path) -> std::io::Result<bool> {
    Ok(mount_fstype(target)?.is_some_and(|ft| ft.starts_with("fuse")))
}

/// Whether the gateway's 9P endpoint accepts a connection within
/// [`GATEWAY_PROBE_TIMEOUT`]. The connect is cancellable async I/O, so a dead
/// gateway fails (or times out) here without parking a blocking thread the way
/// probing a wedged mount with statfs would. Address parsing mirrors
/// `zerofs mount`'s own.
async fn gateway_reachable(gateway: &str) -> bool {
    let connect = async {
        if let Some(rest) = gateway.strip_prefix("unix:") {
            let path = rest.strip_prefix("//").unwrap_or(rest);
            return tokio::net::UnixStream::connect(path).await.map(drop);
        }
        let hostport = gateway.strip_prefix("tcp://").unwrap_or(gateway);
        if hostport.starts_with('/') || hostport.starts_with('.') {
            return tokio::net::UnixStream::connect(hostport).await.map(drop);
        }
        let with_port = if hostport.contains(':') {
            hostport.to_string()
        } else {
            format!("{hostport}:{DEFAULT_9P_PORT}")
        };
        tokio::net::TcpStream::connect(with_port).await.map(drop)
    };
    matches!(
        tokio::time::timeout(GATEWAY_PROBE_TIMEOUT, connect).await,
        Ok(Ok(()))
    )
}

/// Classify a publish target without issuing a syscall that can hang on a
/// wedged mount. `pid` is the serving child's pid (None if none is tracked)
/// and `gateway` its 9P address.
async fn target_state(
    target: &Path,
    pid: Option<u32>,
    gateway: Option<&str>,
) -> Result<TargetState, Status> {
    if !is_fuse_mount(target).map_err(|e| {
        Status::internal(format!(
            "reading mount table for {}: {}",
            target.display(),
            e
        ))
    })? {
        return Ok(TargetState::NotMounted);
    }
    // It is a FUSE mount: live, or a corpse left by a dead serving child.
    if !pid.is_some_and(process_alive) {
        return Ok(TargetState::BrokenFuse);
    }
    // The child is alive: healthy, or blocked behind an unreachable gateway.
    match gateway {
        Some(gw) if !gateway_reachable(gw).await => Ok(TargetState::Unresponsive),
        _ => Ok(TargetState::FuseMounted),
    }
}

/// Unmount a FUSE mountpoint, trying the fusermount helpers first and lazy
/// umount as the last resort. `lazy` is used for broken mounts where a
/// regular unmount may block or fail.
async fn unmount(target: &Path, lazy: bool) -> Result<(), String> {
    let attempts: &[(&str, &[&str])] = if lazy {
        &[
            ("fusermount3", &["-u", "-z"]),
            ("fusermount", &["-u", "-z"]),
            ("umount", &["-l"]),
        ]
    } else {
        &[
            ("fusermount3", &["-u"]),
            ("fusermount", &["-u"]),
            ("umount", &["-l"]),
        ]
    };

    let mut errors = Vec::new();
    for (bin, args) in attempts {
        match Command::new(bin).args(*args).arg(target).output().await {
            Ok(out) if out.status.success() => return Ok(()),
            Ok(out) => errors.push(format!(
                "{} {} {}: {}",
                bin,
                args.join(" "),
                target.display(),
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => errors.push(format!("{}: {}", bin, e)),
        }
    }
    Err(errors.join("; "))
}

impl NodeService {
    pub fn new(node_id: String, zerofs_bin: String, mount_access: MountAccess) -> Self {
        Self {
            node_id,
            zerofs_bin,
            mount_access,
            state: Arc::new(NodeState {
                children: StdMutex::new(HashMap::new()),
                target_locks: StdMutex::new(HashMap::new()),
            }),
        }
    }

    /// Spawn the `zerofs mount` child for `target` and wait until the
    /// mountpoint is live. On failure the child is killed and its stderr is
    /// surfaced in the returned status.
    async fn spawn_mount(
        &self,
        gateway: &str,
        target: &Path,
        aname: &str,
        read_only: bool,
        writeback: bool,
    ) -> Result<(), Status> {
        let mut cmd = Command::new(&self.zerofs_bin);
        cmd.arg("mount")
            .arg(gateway)
            .arg(target)
            .arg("--aname")
            .arg(aname)
            .arg("--access")
            .arg(self.mount_access.as_arg())
            .arg("--writeback")
            .arg(if writeback { "true" } else { "false" })
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            // A child dropped before it is handed to the reaper task (any
            // early error return below) must not keep running untracked.
            .kill_on_drop(true);
        if read_only {
            cmd.arg("--read-only");
        }

        let mut child = cmd.spawn().map_err(|e| {
            Status::internal(format!("failed to spawn {} mount: {}", self.zerofs_bin, e))
        })?;

        // Collect the child's stderr: logged live, and the tail is attached
        // to the gRPC error if the mount fails.
        let stderr_tail: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        if let Some(stderr) = child.stderr.take() {
            let tail = Arc::clone(&stderr_tail);
            let target_str = target.display().to_string();
            tokio::spawn(async move {
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    info!(mount = %target_str, "zerofs mount: {}", line);
                    let mut tail = tail.lock().unwrap();
                    if tail.len() >= STDERR_KEEP_LINES {
                        tail.remove(0);
                    }
                    tail.push(line);
                }
            });
        }

        let fail = |reason: String, stderr_tail: &StdMutex<Vec<String>>| {
            let tail = stderr_tail.lock().unwrap().join("\n");
            if tail.is_empty() {
                Status::internal(reason)
            } else {
                Status::internal(format!("{}; stderr:\n{}", reason, tail))
            }
        };

        let deadline = tokio::time::Instant::now() + MOUNT_TIMEOUT;
        loop {
            // A child that exits before the mount is live has failed.
            if let Some(status) = child
                .try_wait()
                .map_err(|e| Status::internal(format!("waiting on zerofs mount: {}", e)))?
            {
                // Give the stderr task a beat to drain the pipe.
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Err(fail(
                    format!(
                        "zerofs mount exited with {} before the mount came up",
                        status
                    ),
                    &stderr_tail,
                ));
            }

            // We hold the live child handle (try_wait above), so the mount
            // table appearing is enough; no need for the full classifier.
            if is_fuse_mount(target)
                .map_err(|e| Status::internal(format!("reading mount table: {}", e)))?
            {
                break;
            }

            if tokio::time::Instant::now() >= deadline {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(fail(
                    format!(
                        "mount of {} at {} did not come up within {:?}",
                        gateway,
                        target.display(),
                        MOUNT_TIMEOUT
                    ),
                    &stderr_tail,
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Mount is live: track the child and reap it in the background so it
        // never lingers as a zombie.
        let pid = child.id().ok_or_else(|| {
            Status::internal("zerofs mount child has no pid after successful mount")
        })?;
        self.state.children.lock().unwrap().insert(
            target.to_path_buf(),
            Child {
                pid,
                gateway: gateway.to_string(),
            },
        );

        let state = Arc::clone(&self.state);
        let target_buf = target.to_path_buf();
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => {
                    info!(mount = %target_buf.display(), pid, "zerofs mount exited with {}", status)
                }
                Err(e) => {
                    warn!(mount = %target_buf.display(), pid, "failed to reap zerofs mount: {}", e)
                }
            }
            let mut children = state.children.lock().unwrap();
            // Only drop the entry if it still belongs to this child; a
            // remount may have replaced it.
            if children.get(&target_buf).map(|c| c.pid) == Some(pid) {
                children.remove(&target_buf);
            }
        });

        info!(mount = %target.display(), pid, gateway, aname, read_only, writeback, "mounted volume");
        Ok(())
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }
        if req.target_path.is_empty() {
            return Err(Status::invalid_argument("target_path is required"));
        }
        let cap = req
            .volume_capability
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("volume_capability is required"))?;
        validate_volume_capability(cap).map_err(Status::invalid_argument)?;

        let gateway = req.volume_context.get(PARAM_GATEWAY).ok_or_else(|| {
            Status::invalid_argument(format!("volume_context is missing {}", PARAM_GATEWAY))
        })?;
        let volumes_root = req
            .volume_context
            .get(PARAM_VOLUMES_ROOT)
            .map(String::as_str)
            .unwrap_or(crate::DEFAULT_VOLUMES_ROOT);
        let aname = volume_path(volumes_root, &req.volume_id);

        use volume_capability::access_mode::Mode;
        let mode = cap.access_mode.as_ref().map(|m| m.mode).unwrap_or(0);
        let read_only = req.readonly
            || matches!(
                Mode::try_from(mode),
                Ok(Mode::SingleNodeReaderOnly | Mode::MultiNodeReaderOnly)
            );
        // A multi-writer volume can be written from another node's client, so
        // mount it write-through. The writeback cache pins pages across opens
        // (FOPEN_KEEP_CACHE) and only drops them on explicit invalidation, so a
        // peer's overwrite is served stale indefinitely. Write-through instead
        // revalidates against the mount's ~1s attribute TTL, so a peer's writes
        // become visible within about a second even on an already-open file
        // (bounded staleness, stronger than close-to-open). Single-writer and
        // read-only volumes have no peer writer, so they keep the faster
        // writeback default.
        let writeback = !matches!(Mode::try_from(mode), Ok(Mode::MultiNodeMultiWriter));

        let target = PathBuf::from(&req.target_path);
        let lock = self.state.target_lock(&target);
        let _guard = lock.lock().await;

        let pid = self
            .state
            .children
            .lock()
            .unwrap()
            .get(&target)
            .map(|c| c.pid);
        match target_state(&target, pid, Some(gateway)).await? {
            // Already a live FUSE mount: assume it is ours and succeed.
            TargetState::FuseMounted => return Ok(Response::new(NodePublishVolumeResponse {})),
            // Alive but blocked on its gateway: the 9P client reconnects on
            // its own, so leave the mount in place and let the CO retry.
            TargetState::Unresponsive => {
                return Err(Status::unavailable(format!(
                    "mount at {} is up but gateway {} is unreachable; \
                     leaving it to reconnect",
                    target.display(),
                    gateway
                )));
            }
            TargetState::BrokenFuse => {
                warn!(mount = %target.display(), "broken FUSE mount, lazily unmounting before remount");
                unmount(&target, true).await.map_err(|e| {
                    Status::internal(format!(
                        "failed to clear broken mount at {}: {}",
                        target.display(),
                        e
                    ))
                })?;
            }
            // A plain directory or a missing path: create it (idempotent) and
            // mount fresh.
            TargetState::NotMounted => {
                tokio::fs::create_dir_all(&target).await.map_err(|e| {
                    Status::internal(format!("mkdir -p {}: {}", target.display(), e))
                })?;
            }
        }

        self.spawn_mount(gateway, &target, &aname, read_only, writeback)
            .await?;
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }
        if req.target_path.is_empty() {
            return Err(Status::invalid_argument("target_path is required"));
        }

        let target = PathBuf::from(&req.target_path);
        let lock = self.state.target_lock(&target);
        let _guard = lock.lock().await;

        let child = self.state.children.lock().unwrap().get(&target).cloned();
        let pid = child.as_ref().map(|c| c.pid);
        let gateway = child.as_ref().map(|c| c.gateway.as_str());
        match target_state(&target, pid, gateway).await? {
            TargetState::FuseMounted => {
                unmount(&target, false).await.map_err(Status::internal)?;
            }
            // Broken or blocked on its gateway: the pod is going away either
            // way, so detach lazily rather than wait for a reconnect.
            TargetState::BrokenFuse | TargetState::Unresponsive => {
                unmount(&target, true).await.map_err(Status::internal)?;
            }
            TargetState::NotMounted => {}
        }

        // Unmounting ends the FUSE session and the child exits on its own;
        // the SIGTERM covers a child that never finished mounting.
        if let Some(pid) = pid {
            // SAFETY: plain signal send; the worst case is a stale pid that
            // the reaper has not yet removed, in which case ESRCH is ignored.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }

        match tokio::fs::remove_dir(&target).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(Status::internal(format!(
                    "rmdir {}: {}",
                    target.display(),
                    e
                )));
            }
        }

        info!(mount = %target.display(), volume = %req.volume_id, "unpublished volume");
        drop(_guard);
        drop(lock);
        self.state.release_target_lock(&target);
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let req = request.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }
        if req.volume_path.is_empty() {
            return Err(Status::invalid_argument("volume_path is required"));
        }

        let volume_path = Path::new(&req.volume_path);

        // statfs on a FUSE mount blocks until the 9P client answers, which
        // never happens if the gateway is gone — and the blocking thread it
        // parks cannot be cancelled. So for a mount we track, probe the gateway
        // first with a cancellable connect: if it is unreachable, report the
        // volume unavailable without ever issuing the statfs. Only when the
        // gateway answers (so statfs will too) do we go through with it.
        let gateway = self
            .state
            .children
            .lock()
            .unwrap()
            .get(volume_path)
            .map(|c| c.gateway.clone());
        if let Some(gateway) = &gateway
            && !gateway_reachable(gateway).await
        {
            return Err(Status::unavailable(format!(
                "gateway {} for {} is unreachable; volume stats unavailable",
                gateway, req.volume_path
            )));
        }

        // Whole-filesystem numbers: the gateway reports one filesystem shared
        // by every volume. The STATFS_TIMEOUT is a safety net for the gateway
        // dying between the probe and the syscall.
        let st = statfs_async(volume_path).await.map_err(|e| {
            if e.raw_os_error() == Some(libc::ENOENT) {
                Status::not_found(format!("{}: {}", req.volume_path, e))
            } else if e.kind() == std::io::ErrorKind::TimedOut {
                Status::unavailable(format!("{}", e))
            } else {
                Status::internal(format!("statfs {}: {}", req.volume_path, e))
            }
        })?;

        // The gateway advertises an effectively unbounded filesystem, so the
        // block-count * block-size products can exceed i64; do the math in
        // i128 and clamp to the proto's int64 fields.
        let clamp = |v: i128| -> i64 { v.clamp(0, i64::MAX as i128) as i64 };
        let bsize = st.f_bsize as i128;
        let usage = vec![
            VolumeUsage {
                available: clamp(st.f_bavail as i128 * bsize),
                total: clamp(st.f_blocks as i128 * bsize),
                used: clamp((st.f_blocks as i128 - st.f_bfree as i128) * bsize),
                unit: volume_usage::Unit::Bytes as i32,
            },
            VolumeUsage {
                available: clamp(st.f_ffree as i128),
                total: clamp(st.f_files as i128),
                used: clamp(st.f_files as i128 - st.f_ffree as i128),
                unit: volume_usage::Unit::Inodes as i32,
            },
        ];

        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage,
            volume_condition: None,
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        let capabilities = vec![NodeServiceCapability {
            r#type: Some(node_service_capability::Type::Rpc(
                node_service_capability::Rpc {
                    r#type: node_service_capability::rpc::Type::GetVolumeStats as i32,
                },
            )),
        }];
        Ok(Response::new(NodeGetCapabilitiesResponse { capabilities }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }

    async fn node_stage_volume(
        &self,
        _request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeStageVolume"))
    }

    async fn node_unstage_volume(
        &self,
        _request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeUnstageVolume"))
    }

    async fn node_expand_volume(
        &self,
        _request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeExpandVolume"))
    }
}
