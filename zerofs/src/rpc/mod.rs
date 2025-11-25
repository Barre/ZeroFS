pub mod client;
pub mod server;

use crate::checkpoint_manager::CheckpointInfo;

/// RPC service for ZeroFS operations
#[tarpc::service]
pub trait ZeroFsService {
    // ========== Checkpoint Operations ==========

    /// Create a new checkpoint with the given name
    async fn create_checkpoint(name: String) -> Result<CheckpointInfo, String>;

    /// List all checkpoints
    async fn list_checkpoints() -> Result<Vec<CheckpointInfo>, String>;

    /// Delete a checkpoint by name
    async fn delete_checkpoint(name: String) -> Result<(), String>;

    /// Get checkpoint info by name
    async fn get_checkpoint_info(name: String) -> Result<Option<CheckpointInfo>, String>;
}
