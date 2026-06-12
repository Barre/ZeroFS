//! CSI Identity service.

use crate::proto::csi::v1::identity_server::Identity;
use crate::proto::csi::v1::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, PluginCapability, ProbeRequest, ProbeResponse, plugin_capability,
};
use tonic::{Request, Response, Status};

pub struct IdentityService {
    /// Whether this process runs the controller service (mode controller or
    /// all). Advertised via GetPluginCapabilities so the CO knows whether to
    /// route controller RPCs here.
    serve_controller: bool,
}

impl IdentityService {
    pub fn new(serve_controller: bool) -> Self {
        Self { serve_controller }
    }
}

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: crate::DRIVER_NAME.to_string(),
            vendor_version: crate::DRIVER_VERSION.to_string(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        let mut capabilities = Vec::new();
        if self.serve_controller {
            capabilities.push(PluginCapability {
                r#type: Some(plugin_capability::Type::Service(
                    plugin_capability::Service {
                        r#type: plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            });
        }
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}
