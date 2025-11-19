/// S3-based lease coordination for ZeroFS HA
/// 
/// This provides simple leader election using S3 conditional writes (if-match).
/// Benefits:
/// - No external dependencies (uses existing S3 storage)
/// - Simple implementation
/// - Works across any cloud/region
/// - Prevents split-brain with S3 consistency guarantees
/// 
/// How it works:
/// 1. Nodes try to write a lease file with their ID
/// 2. Lease has a TTL and must be renewed periodically
/// 3. Only one node can hold the lease (enforced by S3 conditional writes)
/// 4. If leader fails to renew, lease expires and others can acquire it

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::server_mode::{HAState, ServerMode};

const LEASE_FILE_NAME: &str = ".zerofs_ha_lease";
const DEFAULT_LEASE_TTL_SECS: u64 = 30;
const DEFAULT_RENEW_INTERVAL_SECS: u64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseData {
    /// Node ID holding the lease
    node_id: String,
    /// When the lease was acquired
    acquired_at: DateTime<Utc>,
    /// When the lease expires
    expires_at: DateTime<Utc>,
    /// Monotonically increasing version for fencing
    version: u64,
}

#[derive(Debug, Clone)]
pub struct LeaseCoordinatorConfig {
    /// Unique identifier for this node
    pub node_id: String,
    /// How long a lease is valid (seconds)
    pub lease_ttl_secs: u64,
    /// How often to renew the lease (seconds)
    pub renew_interval_secs: u64,
}

impl Default for LeaseCoordinatorConfig {
    fn default() -> Self {
        Self {
            node_id: format!("node-{}", uuid::Uuid::new_v4()),
            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
            renew_interval_secs: DEFAULT_RENEW_INTERVAL_SECS,
        }
    }
}

pub struct LeaseCoordinator {
    config: LeaseCoordinatorConfig,
    object_store: Arc<dyn ObjectStore>,
    db_path: String,
    ha_state: HAState,
    current_lease: Arc<tokio::sync::RwLock<Option<LeaseData>>>,
}

impl LeaseCoordinator {
    pub fn new(
        config: LeaseCoordinatorConfig,
        object_store: Arc<dyn ObjectStore>,
        db_path: String,
        ha_state: HAState,
    ) -> Self {
        info!(
            "Creating lease coordinator for node '{}' with TTL {}s",
            config.node_id, config.lease_ttl_secs
        );

        Self {
            config,
            object_store,
            db_path,
            ha_state,
            current_lease: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Start the lease coordination loop
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting lease coordinator for node '{}'", self.config.node_id);

        let mut renew_interval = tokio::time::interval(Duration::from_secs(
            self.config.renew_interval_secs,
        ));
        renew_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            renew_interval.tick().await;

            match self.try_acquire_or_renew_lease().await {
                Ok(true) => {
                    // We hold the lease
                    if !self.ha_state.is_active().await {
                        info!("✓ Acquired lease - promoting to ACTIVE");
                        // TODO: In full implementation, this would signal a separate
                        // HA coordinator to handle DB promotion. For now, go directly active.
                        self.ha_state.set_mode(ServerMode::Active).await;
                        self.ha_state
                            .set_leader(Some(self.config.node_id.clone()))
                            .await;
                    }
                }
                Ok(false) => {
                    // Someone else holds the lease
                    if self.ha_state.is_active().await {
                        warn!("Lost lease - should demote to standby");
                        self.ha_state.set_mode(ServerMode::Fenced).await;
                    } else if !self.ha_state.is_standby().await {
                        self.ha_state.set_mode(ServerMode::Standby).await;
                    }
                }
                Err(e) => {
                    error!("Lease coordination error: {}", e);
                    // On error, be conservative - go to standby
                    if self.ha_state.is_active().await {
                        warn!("Lease error while active - demoting to standby");
                        self.ha_state.set_mode(ServerMode::Fenced).await;
                    }
                }
            }
        }
    }

    /// Try to acquire or renew the lease
    /// Returns Ok(true) if we hold the lease, Ok(false) if someone else does
    async fn try_acquire_or_renew_lease(&self) -> Result<bool> {
        let lease_path = Path::from(format!("{}/{}", self.db_path, LEASE_FILE_NAME));

        // First, try to read the existing lease
        let existing_lease = self.read_lease(&lease_path).await?;

        let now = Utc::now();

        match existing_lease {
            Some((lease_data, etag)) => {
                // Check if lease is expired
                if lease_data.expires_at < now {
                    debug!("Lease expired, attempting to acquire");
                    self.try_acquire_lease(&lease_path, Some(lease_data.version))
                        .await
                } else if lease_data.node_id == self.config.node_id {
                    // We hold the lease, try to renew it
                    debug!("Renewing lease");
                    self.try_renew_lease(&lease_path, lease_data, etag).await
                } else {
                    // Someone else holds a valid lease
                    debug!(
                        "Node '{}' holds the lease (expires in {} seconds)",
                        lease_data.node_id,
                        (lease_data.expires_at - now).num_seconds()
                    );
                    self.ha_state
                        .set_leader(Some(lease_data.node_id.clone()))
                        .await;
                    Ok(false)
                }
            }
            None => {
                // No lease exists, try to create one
                debug!("No lease exists, attempting to acquire");
                self.try_acquire_lease(&lease_path, None).await
            }
        }
    }

    /// Read the current lease from S3
    async fn read_lease(&self, path: &Path) -> Result<Option<(LeaseData, String)>> {
        match self.object_store.get(path).await {
            Ok(result) => {
                // Extract etag before consuming result
                let etag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                let bytes = result.bytes().await?;
                let lease_data: LeaseData = serde_json::from_slice(&bytes)?;
                Ok(Some((lease_data, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow!("Failed to read lease: {}", e)),
        }
    }

    /// Try to acquire a new lease
    async fn try_acquire_lease(&self, path: &Path, last_version: Option<u64>) -> Result<bool> {
        let now = Utc::now();
        let new_lease = LeaseData {
            node_id: self.config.node_id.clone(),
            acquired_at: now,
            expires_at: now + chrono::Duration::seconds(self.config.lease_ttl_secs as i64),
            version: last_version.map(|v| v + 1).unwrap_or(1),
        };

        let data = serde_json::to_vec(&new_lease)?;

        // Use put_opts with if-match to ensure atomic acquisition
        let put_opts = object_store::PutOptions {
            mode: object_store::PutMode::Create, // Only succeed if file doesn't exist
            ..Default::default()
        };

        match self.object_store.put_opts(path, data.into(), put_opts).await {
            Ok(_) => {
                info!(
                    "Successfully acquired lease (version {})",
                    new_lease.version
                );
                *self.current_lease.write().await = Some(new_lease);
                Ok(true)
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                debug!("Failed to acquire lease - already exists");
                Ok(false)
            }
            Err(object_store::Error::Precondition { .. }) => {
                debug!("Failed to acquire lease - precondition failed");
                Ok(false)
            }
            Err(e) => Err(anyhow!("Failed to acquire lease: {}", e)),
        }
    }

    /// Try to renew an existing lease we hold
    async fn try_renew_lease(
        &self,
        path: &Path,
        current_lease: LeaseData,
        etag: String,
    ) -> Result<bool> {
        let now = Utc::now();
        let renewed_lease = LeaseData {
            node_id: self.config.node_id.clone(),
            acquired_at: current_lease.acquired_at,
            expires_at: now + chrono::Duration::seconds(self.config.lease_ttl_secs as i64),
            version: current_lease.version + 1,
        };

        let data = serde_json::to_vec(&renewed_lease)?;

        // Use conditional update to ensure we still own the lease
        let put_opts = object_store::PutOptions {
            mode: object_store::PutMode::Update(object_store::UpdateVersion {
                e_tag: Some(etag.clone()),
                version: None,
            }),
            ..Default::default()
        };

        match self.object_store.put_opts(path, data.into(), put_opts).await {
            Ok(_) => {
                debug!("Successfully renewed lease (version {})", renewed_lease.version);
                *self.current_lease.write().await = Some(renewed_lease);
                Ok(true)
            }
            Err(object_store::Error::Precondition { .. }) => {
                warn!("Failed to renew lease - precondition failed (someone else acquired it)");
                *self.current_lease.write().await = None;
                Ok(false)
            }
            Err(e) => Err(anyhow!("Failed to renew lease: {}", e)),
        }
    }

    /// Voluntarily release the lease
    #[allow(dead_code)]
    pub async fn release_lease(&self) -> Result<()> {
        let lease_path = Path::from(format!("{}/{}", self.db_path, LEASE_FILE_NAME));
        let current = self.current_lease.read().await;

        if let Some(lease) = current.as_ref() {
            if lease.node_id == self.config.node_id {
                info!("Releasing lease");
                // Delete the lease file
                match self.object_store.delete(&lease_path).await {
                    Ok(_) => {
                        drop(current);
                        *self.current_lease.write().await = None;
                        Ok(())
                    }
                    Err(e) => Err(anyhow!("Failed to release lease: {}", e)),
                }
            } else {
                Ok(()) // We don't hold the lease
            }
        } else {
            Ok(()) // No lease to release
        }
    }

    /// Check if we currently hold the lease
    #[allow(dead_code)]
    pub async fn holds_lease(&self) -> bool {
        let lease = self.current_lease.read().await;
        lease
            .as_ref()
            .map(|l| l.node_id == self.config.node_id && l.expires_at > Utc::now())
            .unwrap_or(false)
    }

    /// Get the current lease holder
    #[allow(dead_code)]
    pub async fn get_lease_holder(&self) -> Option<String> {
        let lease = self.current_lease.read().await;
        lease.as_ref().map(|l| l.node_id.clone())
    }
}

impl Drop for LeaseCoordinator {
    fn drop(&mut self) {
        // Best effort to release lease on drop
        // Note: This is sync, so we can't use async release_lease()
        // In production, you'd want a shutdown handler
        debug!("LeaseCoordinator dropped for node '{}'", self.config.node_id);
    }
}

