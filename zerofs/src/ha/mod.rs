// Raft implementation requires additional dependencies (raft crate, slog)
// For now, we'll focus on the lease-based coordinator which has no external deps
// #[cfg(feature = "raft")]
// pub mod raft_cluster;

pub mod health_server;
pub mod lease_coordinator;
pub mod server_mode;

// #[cfg(feature = "raft")]
// pub use raft_cluster::RaftCluster;
pub use health_server::start_health_server;
pub use lease_coordinator::LeaseCoordinator;
pub use server_mode::HAState;
#[allow(unused_imports)]
pub use server_mode::ServerMode;

