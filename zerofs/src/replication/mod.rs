//! High-availability replication.
//!
//! The single-writer invariant lives on the DATA db's writer epoch (object-store
//! CAS on open): opening as writer bumps the epoch and fences any prior writer,
//! whose next flush then returns Fenced. The standby follows the leader's
//! heartbeat liveness and, when it stops, takes over by opening the data db as
//! writer (fencing the old leader). No separate coordination database.

pub mod failover;
pub mod leader_record;
pub mod lease;
pub mod replicator;
pub mod tail;
pub mod transport;
#[cfg(test)]
mod zombie;

pub use failover::watch_heartbeats_until_takeover;
#[allow(unused_imports)]
pub use lease::{Lease, run_lease_from_status};
#[allow(unused_imports)]
pub use replicator::Replicator;
#[allow(unused_imports)]
pub use tail::{ReplOp, TailBuffer};

use crate::config::ReplicationRole;

/// HA failover timing. Hardcoded (not user-configurable): a gRPC heartbeat is
/// cheap, so there's no reason to run it slowly, and these are internal mechanism
/// tuning. The hard "at most one writer" guarantee is the writer-epoch CAS on the
/// data db and is independent of these values; the timers only trade detection
/// speed against tolerance for a transient (non-fatal) stall.
/// Invariant: HEARTBEAT_INTERVAL < LEASE_TTL < TAKEOVER_TTL.
pub const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
pub const LEASE_TTL: std::time::Duration = std::time::Duration::from_secs(1);
pub const TAKEOVER_TTL: std::time::Duration = std::time::Duration::from_secs(2);

/// Enforce the timing invariant at compile time: a standby that takes over before
/// the deposed leader's lease can lapse is split-brain, so a future edit that
/// breaks HEARTBEAT_INTERVAL < LEASE_TTL < TAKEOVER_TTL must fail the build.
const _: () = assert!(
    HEARTBEAT_INTERVAL.as_nanos() < LEASE_TTL.as_nanos()
        && LEASE_TTL.as_nanos() < TAKEOVER_TTL.as_nanos(),
    "HA timing invariant violated: need HEARTBEAT_INTERVAL < LEASE_TTL < TAKEOVER_TTL"
);

/// Runtime parameters from [`crate::config::ReplicationConfig`]; the failover
/// timing is hardcoded (see the constants above).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ReplicationParams {
    pub node_id: String,
    pub role: ReplicationRole,
    pub heartbeat_interval: std::time::Duration,
    pub lease_ttl: std::time::Duration,
    pub takeover_ttl: std::time::Duration,
    /// Peer replication-listen address(es): on a leader, where the standby receives.
    pub peers: Vec<String>,
    /// This node's replication-listen address (a standby receives here).
    pub replication_listen: Option<String>,
}

impl ReplicationParams {
    pub fn from_config(cfg: &crate::config::ReplicationConfig) -> Self {
        Self {
            node_id: cfg.node_id.clone(),
            role: cfg.role,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            lease_ttl: LEASE_TTL,
            takeover_ttl: TAKEOVER_TTL,
            peers: cfg.peers.clone(),
            replication_listen: cfg.replication_listen.clone(),
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, ReplicationRole::Leader)
    }
}
