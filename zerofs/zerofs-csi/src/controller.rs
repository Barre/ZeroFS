//! CSI Controller service. Volumes are directories under `volumesRoot` on
//! the gateway filesystem, created and removed through the gateway's admin
//! RPC. The controller is stateless: everything it needs arrives in the
//! request (StorageClass parameters for CreateVolume, the provisioner secret
//! for DeleteVolume, which carries no parameters per the CSI spec).

use crate::admin::AdminClient;
use crate::proto::csi::v1::controller_server::Controller;
use crate::proto::csi::v1::{
    ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
    ControllerGetVolumeRequest, ControllerGetVolumeResponse, ControllerModifyVolumeRequest,
    ControllerModifyVolumeResponse, ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse, ControllerServiceCapability, ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse, CreateSnapshotRequest, CreateSnapshotResponse,
    CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest, GetCapacityResponse,
    GetSnapshotRequest, GetSnapshotResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    ListVolumesRequest, ListVolumesResponse, ValidateVolumeCapabilitiesRequest,
    ValidateVolumeCapabilitiesResponse, Volume, VolumeCapability, controller_service_capability,
    validate_volume_capabilities_response, volume_capability,
};
use std::collections::HashMap;
use tonic::{Code, Request, Response, Status};
use tracing::info;

/// StorageClass parameter / provisioner-secret key: gateway admin RPC URL
/// (e.g. http://zerofs-gateway.zerofs.svc:7000).
pub const PARAM_ADMIN_ENDPOINT: &str = "adminEndpoint";
/// StorageClass parameter: 9P TCP address nodes mount from
/// (e.g. zerofs-gateway.zerofs.svc:5564).
pub const PARAM_GATEWAY: &str = "gateway";
/// StorageClass parameter: directory on the gateway filesystem under which
/// volume directories are created. Defaults to /volumes.
pub const PARAM_VOLUMES_ROOT: &str = "volumesRoot";

pub struct ControllerService;

impl ControllerService {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ControllerService {
    fn default() -> Self {
        Self::new()
    }
}

/// Check that a capability is a mount volume (no block support) with one of
/// the supported access modes.
pub fn validate_volume_capability(cap: &VolumeCapability) -> Result<(), String> {
    match &cap.access_type {
        Some(volume_capability::AccessType::Mount(m)) => {
            // The FUSE mount takes no pass-through options; refuse
            // StorageClass mountOptions rather than silently dropping them.
            if !m.mount_flags.is_empty() {
                return Err(format!(
                    "mount_flags are not supported: {:?}",
                    m.mount_flags
                ));
            }
        }
        Some(volume_capability::AccessType::Block(_)) => {
            return Err("block volumes are not supported".to_string());
        }
        None => return Err("volume capability has no access_type".to_string()),
    }

    use volume_capability::access_mode::Mode;
    let mode = cap.access_mode.as_ref().map(|m| m.mode).unwrap_or(0);
    match Mode::try_from(mode) {
        Ok(
            Mode::SingleNodeWriter
            | Mode::SingleNodeReaderOnly
            | Mode::MultiNodeReaderOnly
            | Mode::MultiNodeMultiWriter,
        ) => Ok(()),
        Ok(other) => Err(format!("unsupported access mode {:?}", other)),
        Err(_) => Err(format!("unknown access mode {}", mode)),
    }
}

/// `volumesRoot` + `/` + `volume_id`, the directory backing a volume. Also
/// used verbatim as the 9P attach aname on nodes.
pub fn volume_path(volumes_root: &str, volume_id: &str) -> String {
    format!("{}/{}", volumes_root.trim_end_matches('/'), volume_id)
}

/// Volume IDs become single path components on the gateway; refuse anything
/// that would walk elsewhere.
fn validate_volume_id(volume_id: &str) -> Result<(), Status> {
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume_id is required"));
    }
    if volume_id.contains('/') || volume_id == "." || volume_id == ".." {
        return Err(Status::invalid_argument(format!(
            "volume_id {:?} is not a valid single path component",
            volume_id
        )));
    }
    Ok(())
}

/// Whether an admin RPC failure from one endpoint means "not the leader / not
/// reachable" and we should try the next endpoint, rather than surface it. A
/// fresh standby is not listening (connect refused, surfaced before the op); a
/// deposed-but-alive old leader returns its `LeaderLeaseExpired` as `Internal`.
/// An authoritative status (the leader rejecting the request on its merits,
/// e.g. a non-directory in the way) is returned as-is.
fn admin_error_is_retriable(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::Internal
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::Cancelled
    )
}

/// `adminEndpoint` may be a comma-separated HA node set. Only the leader can
/// write, so create the directory against the first endpoint that answers as
/// the leader: connect failures and not-leader/opaque errors fall through to
/// the next endpoint (CreateDirectory is idempotent and a non-leader never
/// applies it, so retrying is safe); an authoritative status returns
/// immediately. If no endpoint serves (both down, or a failover gap) the last
/// error is returned as UNAVAILABLE so the CO retries.
async fn create_dir_on_leader(
    endpoints: &str,
    path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<bool, Status> {
    let segments = crate::targets::split(endpoints);
    if segments.is_empty() {
        return Err(Status::invalid_argument(format!(
            "empty {}",
            PARAM_ADMIN_ENDPOINT
        )));
    }
    let mut last = None;
    for ep in segments {
        let client = match AdminClient::connect(ep).await {
            Ok(c) => c,
            Err(e) => {
                last = Some(Status::unavailable(format!("admin {}: {:#}", ep, e)));
                continue;
            }
        };
        match client.create_directory(path, mode, uid, gid).await {
            Ok(created) => return Ok(created),
            Err(s) if admin_error_is_retriable(s.code()) => {
                last = Some(Status::unavailable(format!(
                    "admin {}: {}",
                    ep,
                    s.message()
                )));
            }
            // A non-directory in the way: same name, incompatible volume.
            Err(s) if s.code() == Code::FailedPrecondition => {
                return Err(Status::already_exists(format!(
                    "CreateDirectory {}: {}",
                    path,
                    s.message()
                )));
            }
            Err(s) if s.code() == Code::InvalidArgument => {
                return Err(Status::invalid_argument(format!(
                    "CreateDirectory {}: {}",
                    path,
                    s.message()
                )));
            }
            Err(s) => {
                return Err(Status::internal(format!(
                    "CreateDirectory {}: {}",
                    path,
                    s.message()
                )));
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        Status::unavailable(format!("no admin endpoint among {} answered", endpoints))
    }))
}

/// Remove the directory against the leader among the comma-separated
/// `adminEndpoint` set. Idempotent on the gateway side, so the same
/// fall-through-on-transport-error rule as [`create_dir_on_leader`] applies.
async fn remove_dir_on_leader(endpoints: &str, path: &str) -> Result<(), Status> {
    let segments = crate::targets::split(endpoints);
    if segments.is_empty() {
        return Err(Status::invalid_argument(format!(
            "empty {}",
            PARAM_ADMIN_ENDPOINT
        )));
    }
    let mut last = None;
    for ep in segments {
        let client = match AdminClient::connect(ep).await {
            Ok(c) => c,
            Err(e) => {
                last = Some(Status::unavailable(format!("admin {}: {:#}", ep, e)));
                continue;
            }
        };
        match client.remove_directory(path).await {
            Ok(()) => return Ok(()),
            Err(s) if admin_error_is_retriable(s.code()) => {
                last = Some(Status::unavailable(format!(
                    "admin {}: {}",
                    ep,
                    s.message()
                )));
            }
            Err(s) => {
                return Err(Status::internal(format!(
                    "RemoveDirectory {}: {}",
                    path,
                    s.message()
                )));
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        Status::unavailable(format!("no admin endpoint among {} answered", endpoints))
    }))
}

/// Check whether a volume directory exists on the gateway by performing a 9P
/// attach with the volume's aname — the same operation nodes perform to mount
/// it, and side-effect free. `gateway` may be an HA node set: `connect_multi`
/// probes the targets and follows the serving leader. ENOENT/ENOTDIR mean the
/// volume does not exist; connect failures and timeouts map to UNAVAILABLE so
/// the CO retries (the 9P client blocks requests while reconnecting, hence the
/// timeout).
async fn volume_exists_on_gateway(gateway: &str, aname: &str) -> Result<bool, Status> {
    const MSIZE: u32 = 64 * 1024;
    const ATTACH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    let targets = crate::targets::parse_9p_targets(gateway)
        .map_err(|e| Status::unavailable(format!("gateway {}: {}", gateway, e)))?;
    let client = ninep_client::NinePClient::connect_multi(targets, MSIZE)
        .await
        .map_err(|e| Status::unavailable(format!("connecting to gateway {}: {}", gateway, e)))?;

    let attach = client.attach(0, ninep_client::NOFID, "root", aname, 0);
    match tokio::time::timeout(ATTACH_TIMEOUT, attach).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) if e.to_errno() == libc::ENOENT || e.to_errno() == libc::ENOTDIR => Ok(false),
        Ok(Err(e)) => Err(Status::unavailable(format!(
            "gateway attach {}: {}",
            aname, e
        ))),
        Err(_) => Err(Status::unavailable(format!(
            "gateway attach {} timed out",
            aname
        ))),
    }
}

#[tonic::async_trait]
impl Controller for ControllerService {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        // The CO-provided name doubles as the volume id and the directory
        // name under volumesRoot.
        validate_volume_id(&req.name)?;
        if req.volume_capabilities.is_empty() {
            return Err(Status::invalid_argument("volume_capabilities is required"));
        }
        for cap in &req.volume_capabilities {
            validate_volume_capability(cap).map_err(Status::invalid_argument)?;
        }
        // No snapshot or clone support: a volume that must be pre-populated
        // from a source cannot be provisioned here, so fail instead of
        // returning an empty directory.
        if req.volume_content_source.is_some() {
            return Err(Status::invalid_argument(
                "volume_content_source is not supported (snapshots and clones are not implemented)",
            ));
        }

        // StorageClass parameters, with the provisioner secret as fallback
        // (DeleteVolume only receives the secret, so installs are expected to
        // duplicate adminEndpoint there; accepting it here too keeps the two
        // paths symmetric).
        let lookup = |key: &str| {
            req.parameters
                .get(key)
                .or_else(|| req.secrets.get(key))
                .map(String::as_str)
        };
        let admin_endpoint = lookup(PARAM_ADMIN_ENDPOINT).ok_or_else(|| {
            Status::invalid_argument(format!("missing parameter {}", PARAM_ADMIN_ENDPOINT))
        })?;
        let gateway = lookup(PARAM_GATEWAY).ok_or_else(|| {
            Status::invalid_argument(format!("missing parameter {}", PARAM_GATEWAY))
        })?;
        let volumes_root = lookup(PARAM_VOLUMES_ROOT).unwrap_or(crate::DEFAULT_VOLUMES_ROOT);

        let path = volume_path(volumes_root, &req.name);
        // Permissions are wide open: aname is namespacing, not a security
        // boundary, and pods of any uid must be able to use the volume.
        let created = create_dir_on_leader(admin_endpoint, &path, 0o777, 0, 0).await?;
        info!(volume = %req.name, %path, created, "CreateVolume");

        // Capacity is recorded but not enforced; the gateway filesystem is
        // shared by all volumes.
        let capacity_bytes = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes)
            .unwrap_or(0);

        let mut volume_context = HashMap::new();
        volume_context.insert(PARAM_GATEWAY.to_string(), gateway.to_string());
        volume_context.insert(PARAM_VOLUMES_ROOT.to_string(), volumes_root.to_string());

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                capacity_bytes,
                volume_id: req.name,
                volume_context,
                content_source: None,
                accessible_topology: Vec::new(),
            }),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        validate_volume_id(&req.volume_id)?;

        // DeleteVolumeRequest carries no parameters or volume context, only
        // secrets. The StorageClass must reference a provisioner secret
        // (csi.storage.k8s.io/provisioner-secret-name) holding adminEndpoint.
        let admin_endpoint = req.secrets.get(PARAM_ADMIN_ENDPOINT).ok_or_else(|| {
            Status::invalid_argument(format!(
                "missing {} in secrets; the StorageClass must reference a provisioner \
                 secret carrying it (DeleteVolume receives no parameters)",
                PARAM_ADMIN_ENDPOINT
            ))
        })?;
        let volumes_root = req
            .secrets
            .get(PARAM_VOLUMES_ROOT)
            .map(String::as_str)
            .unwrap_or(crate::DEFAULT_VOLUMES_ROOT);

        let path = volume_path(volumes_root, &req.volume_id);
        // Idempotent on the gateway side: a missing directory is a success.
        remove_dir_on_leader(admin_endpoint, &path).await?;
        info!(volume = %req.volume_id, %path, "DeleteVolume");

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        validate_volume_id(&req.volume_id)?;
        if req.volume_capabilities.is_empty() {
            return Err(Status::invalid_argument("volume_capabilities is required"));
        }

        // The spec wants NOT_FOUND for a nonexistent volume_id. The
        // volume_context returned by CreateVolume (recorded on the PV)
        // carries gateway + volumesRoot; fall back to parameters/secrets for
        // pre-provisioned volumes. Without any gateway information the check
        // is impossible and capabilities are confirmed for any well-formed
        // volume_id (Kubernetes' external-provisioner never calls this RPC).
        let gateway = req
            .volume_context
            .get(PARAM_GATEWAY)
            .or_else(|| req.parameters.get(PARAM_GATEWAY))
            .or_else(|| req.secrets.get(PARAM_GATEWAY));
        if let Some(gateway) = gateway {
            let volumes_root = req
                .volume_context
                .get(PARAM_VOLUMES_ROOT)
                .or_else(|| req.parameters.get(PARAM_VOLUMES_ROOT))
                .or_else(|| req.secrets.get(PARAM_VOLUMES_ROOT))
                .map(String::as_str)
                .unwrap_or(crate::DEFAULT_VOLUMES_ROOT);
            let aname = volume_path(volumes_root, &req.volume_id);
            if !volume_exists_on_gateway(gateway, &aname).await? {
                return Err(Status::not_found(format!(
                    "volume {} not found at {} on the gateway",
                    req.volume_id, aname
                )));
            }
        }

        let unsupported = req
            .volume_capabilities
            .iter()
            .find_map(|cap| validate_volume_capability(cap).err());
        let response = match unsupported {
            None => ValidateVolumeCapabilitiesResponse {
                confirmed: Some(validate_volume_capabilities_response::Confirmed {
                    volume_context: req.volume_context,
                    volume_capabilities: req.volume_capabilities,
                    parameters: req.parameters,
                    mutable_parameters: req.mutable_parameters,
                }),
                message: String::new(),
            },
            Some(reason) => ValidateVolumeCapabilitiesResponse {
                confirmed: None,
                message: reason,
            },
        };
        Ok(Response::new(response))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        let capabilities = vec![ControllerServiceCapability {
            r#type: Some(controller_service_capability::Type::Rpc(
                controller_service_capability::Rpc {
                    r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                },
            )),
        }];
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn controller_publish_volume(
        &self,
        _request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerPublishVolume"))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerUnpublishVolume"))
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented("ListVolumes"))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(Status::unimplemented("GetCapacity"))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("CreateSnapshot"))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("DeleteSnapshot"))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("ListSnapshots"))
    }

    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("GetSnapshot"))
    }

    async fn controller_expand_volume(
        &self,
        _request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerExpandVolume"))
    }

    async fn controller_get_volume(
        &self,
        _request: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerGetVolume"))
    }

    async fn controller_modify_volume(
        &self,
        _request: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerModifyVolume"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retriable_admin_codes_fall_through_authoritative_ones_surface() {
        // Transport / not-leader: try the next endpoint.
        for c in [
            Code::Unavailable,
            Code::Internal,
            Code::Unknown,
            Code::DeadlineExceeded,
            Code::Cancelled,
        ] {
            assert!(admin_error_is_retriable(c), "{c:?} should be retriable");
        }
        // The leader rejecting the request on its merits: surface it.
        for c in [
            Code::FailedPrecondition,
            Code::InvalidArgument,
            Code::AlreadyExists,
            Code::NotFound,
            Code::PermissionDenied,
        ] {
            assert!(
                !admin_error_is_retriable(c),
                "{c:?} should be authoritative"
            );
        }
    }
}
