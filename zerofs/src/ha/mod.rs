// Raft implementation requires additional dependencies (raft crate, slog)
// For now, we'll focus on the lease-based coordinator which has no external deps
// #[cfg(feature = "raft")]
// pub mod raft_cluster;

pub mod health_server;
pub mod lease_coordinator;
pub mod server_mode;

// WAL Replication
pub mod replication_protocol;
pub mod wal_entry;
pub mod wal_receiver;
pub mod wal_streamer;

// #[cfg(feature = "raft")]
// pub use raft_cluster::RaftCluster;
pub use health_server::start_health_server;
pub use lease_coordinator::LeaseCoordinator;
pub use replication_protocol::{AckPolicy, ReplicationProtocol};
pub use server_mode::HAState;
pub use wal_receiver::{WALReceiver, WALReceiverConfig};
pub use wal_streamer::{WALStreamer, WALStreamerConfig};
#[allow(unused_imports)]
pub use server_mode::ServerMode;

