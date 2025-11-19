use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ServerMode {
    /// Node is the active writer
    Active,
    /// Node is a standby reader
    Standby,
    /// Node is transitioning between modes (reject all requests)
    Fenced,
    /// Node is starting up
    Initializing,
}

/// Shared HA state accessible by protocol handlers
#[derive(Clone)]
pub struct HAState {
    mode: Arc<RwLock<ServerMode>>,
    current_leader: Arc<RwLock<Option<String>>>,
}

impl HAState {
    pub fn new() -> Self {
        Self {
            mode: Arc::new(RwLock::new(ServerMode::Initializing)),
            current_leader: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn get_mode(&self) -> ServerMode {
        *self.mode.read().await
    }

    pub async fn set_mode(&self, mode: ServerMode) {
        *self.mode.write().await = mode;
    }

    pub async fn is_active(&self) -> bool {
        matches!(*self.mode.read().await, ServerMode::Active)
    }

    pub async fn is_standby(&self) -> bool {
        matches!(*self.mode.read().await, ServerMode::Standby)
    }

    #[allow(dead_code)]
    pub async fn can_accept_writes(&self) -> bool {
        matches!(*self.mode.read().await, ServerMode::Active)
    }

    #[allow(dead_code)]
    pub async fn can_accept_reads(&self) -> bool {
        matches!(
            *self.mode.read().await,
            ServerMode::Active | ServerMode::Standby
        )
    }

    pub async fn set_leader(&self, leader: Option<String>) {
        *self.current_leader.write().await = leader;
    }

    #[allow(dead_code)]
    pub async fn get_leader(&self) -> Option<String> {
        self.current_leader.read().await.clone()
    }
}

impl Default for HAState {
    fn default() -> Self {
        Self::new()
    }
}

