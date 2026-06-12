pub mod admin;
pub mod controller;
pub mod identity;
pub mod node;
pub mod proto;
pub mod server;

/// CSI driver name, as registered with the CO and referenced by
/// StorageClass.provisioner.
pub const DRIVER_NAME: &str = "csi.zerofs.net";

/// Reported as vendor_version in GetPluginInfo.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default root directory for volumes on the gateway filesystem. One
/// subdirectory per volume, e.g. /volumes/pvc-<uuid>.
pub const DEFAULT_VOLUMES_ROOT: &str = "/volumes";
