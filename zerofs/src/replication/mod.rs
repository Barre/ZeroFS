//! High-availability replication.
//!
//! Durable marker ownership precedes every data-database writer open. An Active
//! owner serves under a bounded exact-marker validation or epoch-bound standby
//! acknowledgement. A claimant stops acknowledgements, updates the marker, and
//! waits for prior grants to expire before opening the next writer.

pub mod failover;
pub mod leader_record;
pub mod lease;
mod replay;
pub mod replicator;
pub mod tail;
pub mod transport;
pub(crate) mod types;
#[cfg(test)]
mod zombie;

pub use failover::watch_heartbeats_until_takeover_hint;
pub use lease::{AuthoritySupervisor, Lease, activate_lease_from_marker};
#[doc(hidden)]
pub use replay::{LineageProof, PromotionRetryGraceProof, ReconcileOutcome};
#[allow(unused_imports)]
pub use replicator::{Replicator, ShipOutcome};
#[allow(unused_imports)]
pub use tail::{ReplOp, TailBuffer};

use crate::config::ReplicationRole;
use std::time::Duration;

/// Maximum age of an exact-marker validation or standby acknowledgement,
/// measured from request start.
pub const AUTHORITY_TTL: Duration = Duration::from_secs(3);
/// Exact-marker validation interval for degraded and Solo owners.
pub const MARKER_VALIDATION_INTERVAL: Duration = Duration::from_secs(1);
/// Maximum time allowed for one ordinary exact-marker read. Serving remains
/// independently bounded by [`AUTHORITY_TTL`].
pub const MARKER_VALIDATION_TIMEOUT: Duration = Duration::from_secs(3);
/// Coverage/liveness beacon cadence.
pub const COVERAGE_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
/// Age after which standby acknowledgements no longer suppress marker validation.
pub const HEARTBEAT_ACK_FRESH_FOR: Duration = COVERAGE_HEARTBEAT_INTERVAL.saturating_mul(3);
/// Heartbeat gap before attempting a durable claim.
pub const TAKEOVER_HINT_AFTER: Duration = Duration::from_secs(2);
/// Timeout for the final exact-marker validation after authority expiry.
pub const FINAL_MARKER_RECOVERY_TIMEOUT: Duration = Duration::from_secs(3);
/// Bound for protocol and serving shutdown after authority loss.
pub const RESPONSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Runtime replication endpoints and recovery policy.
#[derive(Debug, Clone)]
pub struct ReplicationParams {
    pub node_id: String,
    pub role: ReplicationRole,
    /// Standby replication endpoint used by a leader.
    pub peers: Vec<String>,
    /// Local replication listener address.
    pub replication_listen: Option<String>,
    /// One-shot operator authorization to skip stale-handoff observation.
    pub force_recovery: bool,
}

impl ReplicationParams {
    pub fn from_config(cfg: &crate::config::ReplicationConfig) -> Self {
        Self {
            node_id: cfg.node_id.clone(),
            role: cfg.role,
            peers: cfg.peers.clone(),
            replication_listen: cfg.replication_listen.clone(),
            force_recovery: cfg.force_recovery,
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, ReplicationRole::Leader)
    }
}
