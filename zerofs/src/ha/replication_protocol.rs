/// WAL Replication protocols inspired by DRBD
///
/// Protocol A: Asynchronous replication (default)
/// - Write completes after local disk write
/// - Replication happens in background
/// - 0ms overhead, small data loss window (~5-10ms)
///
/// Protocol C: Synchronous replication (optional)
/// - Write completes after remote ACK received
/// - Zero data loss on failover
/// - ~5-10ms overhead per write

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationProtocol {
    /// Protocol A: Asynchronous replication (fire-and-forget)
    Async,
    
    /// Protocol C: Synchronous replication (wait for ACK)
    Sync,
}

impl Default for ReplicationProtocol {
    fn default() -> Self {
        ReplicationProtocol::Async
    }
}

impl std::fmt::Display for ReplicationProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationProtocol::Async => write!(f, "async"),
            ReplicationProtocol::Sync => write!(f, "sync"),
        }
    }
}

/// How many ACKs are required for Protocol C
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckPolicy {
    /// Wait for at least one standby
    One,
    
    /// Wait for majority of standbys (quorum)
    Majority,
    
    /// Wait for all standbys
    All,
}

impl Default for AckPolicy {
    fn default() -> Self {
        AckPolicy::One
    }
}

impl std::fmt::Display for AckPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AckPolicy::One => write!(f, "one"),
            AckPolicy::Majority => write!(f, "majority"),
            AckPolicy::All => write!(f, "all"),
        }
    }
}

