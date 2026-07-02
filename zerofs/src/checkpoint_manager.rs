use crate::db::SlateDbHandle;
use anyhow::{Result, anyhow};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use slatedb::admin::Admin;
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::path::Path;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

/// Seal the data-plane open segment and flush the metadata memtable under the
/// flush barrier, so a subsequent durable-scope checkpoint captures only
/// already-sealed state (never a FrameLoc whose segment is still in RAM).
pub type PreCheckpointFlush =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub id: Uuid,
    pub name: String,
    pub created_at: u64,
}

pub struct CheckpointManager {
    db_handle: SlateDbHandle,
    admin: Admin,
    /// Set once at bring-up (see [`PreCheckpointFlush`]); unset in tests, which
    /// checkpoint durable state as-is.
    pre_flush: Arc<OnceLock<PreCheckpointFlush>>,
}

impl CheckpointManager {
    pub fn new(
        db_handle: SlateDbHandle,
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Self {
        let mut admin_builder = slatedb::admin::AdminBuilder::new(path, object_store);
        if let Some(wal_store) = wal_object_store {
            admin_builder = admin_builder.with_wal_object_store(wal_store);
        }
        let admin = admin_builder.build();
        Self {
            db_handle,
            admin,
            pre_flush: Arc::new(OnceLock::new()),
        }
    }

    /// Install the pre-checkpoint seal+flush hook (first call wins). Wired at
    /// bring-up to the filesystem's flush coordinator.
    pub fn set_pre_flush(&self, hook: PreCheckpointFlush) {
        let _ = self.pre_flush.set(hook);
    }

    pub async fn create_checkpoint(&self, name: &str) -> Result<CheckpointInfo> {
        let db = match &self.db_handle {
            SlateDbHandle::ReadWrite(db) => db,
            SlateDbHandle::ReadOnly(_) => {
                return Err(anyhow!(
                    "Cannot create checkpoints in read-only mode. Start the server without --read-only or --checkpoint flags."
                ));
            }
        };

        let name = name.trim();

        if name.is_empty() {
            return Err(anyhow!("Checkpoint name cannot be empty"));
        }

        let existing = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        if !existing.is_empty() {
            return Err(anyhow!("A checkpoint with name '{}' already exists", name));
        }

        // Seal + flush under the barrier, then checkpoint `Durable` scope,
        // which captures only the already-durable manifest. `Scope::All` would
        // freeze the memtable itself and durably publish FrameLocs whose
        // segment is still the RAM open buffer — dangling pointers the
        // checkpoint would pin forever.
        if let Some(pre_flush) = self.pre_flush.get() {
            pre_flush()
                .await
                .map_err(|e| anyhow!("Failed to seal+flush before checkpoint: {}", e))?;
        }

        let result = db
            .create_checkpoint(
                CheckpointScope::Durable,
                &CheckpointOptions {
                    lifetime: None,
                    source: None,
                    name: Some(name.to_string()),
                },
            )
            .await
            .map_err(|e| anyhow!("Failed to create checkpoint: {}", e))?;

        let checkpoints = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to get checkpoint info: {}", e))?;

        let checkpoint = checkpoints
            .into_iter()
            .find(|cp| cp.id == result.id)
            .ok_or_else(|| anyhow!("Created checkpoint not found"))?;

        Ok(CheckpointInfo {
            id: checkpoint.id,
            name: name.to_string(),
            created_at: checkpoint.create_time.timestamp() as u64,
        })
    }

    pub async fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        let checkpoints = self
            .admin
            .list_checkpoints(None)
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        Ok(checkpoints
            .into_iter()
            .filter_map(|cp| {
                let name = cp.name.as_ref()?;
                if name.is_empty() {
                    return None;
                }
                Some(CheckpointInfo {
                    id: cp.id,
                    name: name.clone(),
                    created_at: cp.create_time.timestamp() as u64,
                })
            })
            .collect())
    }

    pub async fn delete_checkpoint(&self, name: &str) -> Result<()> {
        let name = name.trim();

        let checkpoints = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        let checkpoint = checkpoints
            .into_iter()
            .find(|cp| cp.name.as_deref() == Some(name))
            .ok_or_else(|| anyhow!("Checkpoint '{}' not found", name))?;

        self.admin
            .delete_checkpoint(checkpoint.id)
            .await
            .map_err(|e| anyhow!("Failed to delete checkpoint: {}", e))?;

        Ok(())
    }

    pub async fn get_checkpoint_info(&self, name: &str) -> Result<Option<CheckpointInfo>> {
        let name = name.trim();

        let checkpoints = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        Ok(checkpoints
            .into_iter()
            .find(|cp| cp.name.as_deref() == Some(name))
            .map(|cp| CheckpointInfo {
                id: cp.id,
                name: name.to_string(),
                created_at: cp.create_time.timestamp() as u64,
            }))
    }
}
