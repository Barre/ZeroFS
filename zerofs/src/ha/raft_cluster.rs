/// Embedded Raft-based coordination for ZeroFS HA cluster
/// 
/// This provides:
/// - Automatic leader election
/// - Consensus on who can write
/// - No external dependencies
/// - Split-brain prevention
/// 
/// NOTE: This requires the "raft" feature to be enabled.
/// For now, use the lease-based coordinator which has no external dependencies.

#[cfg(feature = "raft")]
use anyhow::{anyhow, Result};
#[cfg(feature = "raft")]
use raft::{prelude::*, storage::MemStorage, Config as RaftConfig};
#[cfg(feature = "raft")]
use slog::{o, Drain, Logger};
#[cfg(feature = "raft")]
use std::collections::HashMap;
#[cfg(feature = "raft")]
use std::net::SocketAddr;
#[cfg(feature = "raft")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "raft")]
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "raft")]
use tokio::sync::mpsc;
#[cfg(feature = "raft")]
use tracing::{debug, error, info, warn};

#[cfg(feature = "raft")]
use super::server_mode::{HAState, ServerMode};

/// Raft message sent between nodes
#[cfg(feature = "raft")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum RaftMessage {
    /// Standard Raft message
    Raft(Message),
    /// Request current leader info
    LeaderRequest,
    /// Response with leader info
    LeaderResponse { leader_id: Option<u64> },
}

#[cfg(feature = "raft")]
/// Configuration for Raft cluster
#[derive(Debug, Clone)]
pub struct RaftClusterConfig {
    /// This node's unique ID (1-based)
    pub node_id: u64,
    /// Address this node listens on for Raft messages
    pub listen_addr: SocketAddr,
    /// All peers in the cluster (including self)
    pub peers: HashMap<u64, SocketAddr>,
    /// Raft election timeout in ticks (default: 10)
    pub election_tick: usize,
    /// Raft heartbeat interval in ticks (default: 3)
    pub heartbeat_tick: usize,
}

#[cfg(feature = "raft")]
pub struct RaftCluster {
    config: RaftClusterConfig,
    node: Arc<Mutex<RawNode<MemStorage>>>,
    ha_state: HAState,
    message_tx: mpsc::UnboundedSender<RaftMessage>,
    message_rx: Arc<Mutex<mpsc::UnboundedReceiver<RaftMessage>>>,
    logger: Logger,
}

#[cfg(feature = "raft")]
impl RaftCluster {
    pub fn new(config: RaftClusterConfig, ha_state: HAState) -> Result<Self> {
        // Setup slog logger for raft
        let decorator = slog_term::PlainDecorator::new(std::io::stderr());
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let logger = Logger::root(drain, o!("node_id" => config.node_id));

        // Create Raft configuration
        let raft_config = RaftConfig {
            id: config.node_id,
            election_tick: config.election_tick,
            heartbeat_tick: config.heartbeat_tick,
            max_size_per_msg: 1024 * 1024, // 1MB
            max_inflight_msgs: 256,
            ..Default::default()
        };

        // Validate configuration
        raft_config.validate()?;

        // Create storage
        let storage = MemStorage::new_with_conf_state(ConfState::from((
            config.peers.keys().copied().collect::<Vec<_>>(),
            vec![],
        )));

        // Create Raft node
        let node = RawNode::new(&raft_config, storage, &logger)?;

        let (message_tx, message_rx) = mpsc::unbounded_channel();

        Ok(Self {
            config,
            node: Arc::new(Mutex::new(node)),
            ha_state,
            message_tx,
            message_rx: Arc::new(Mutex::new(message_rx)),
            logger,
        })
    }

    /// Start the Raft cluster
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!(
            "Starting Raft cluster node {} on {}",
            self.config.node_id, self.config.listen_addr
        );

        // Start network listener
        let listener_handle = {
            let cluster = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = cluster.run_listener().await {
                    error!("Raft listener error: {}", e);
                }
            })
        };

        // Start Raft tick loop
        let tick_handle = {
            let cluster = Arc::clone(&self);
            tokio::spawn(async move {
                cluster.run_tick_loop().await;
            })
        };

        // Start message processor
        let processor_handle = {
            let cluster = Arc::clone(&self);
            tokio::spawn(async move {
                cluster.run_message_processor().await;
            })
        };

        // Wait for all tasks
        tokio::try_join!(listener_handle, tick_handle, processor_handle)?;

        Ok(())
    }

    /// Run the network listener for incoming Raft messages
    async fn run_listener(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.listen_addr).await?;
        info!("Raft listening on {}", self.config.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!("Accepted Raft connection from {}", peer_addr);
                    let tx = self.message_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, tx).await {
                            error!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }
    }

    /// Handle incoming connection
    async fn handle_connection(
        stream: TcpStream,
        tx: mpsc::UnboundedSender<RaftMessage>,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;

        let mut stream = stream;
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer

        loop {
            // Read message length (4 bytes)
            let mut len_bytes = [0u8; 4];
            match stream.read_exact(&mut len_bytes).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Connection closed
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            let len = u32::from_be_bytes(len_bytes) as usize;
            if len > buffer.len() {
                buffer.resize(len, 0);
            }

            // Read message payload
            stream.read_exact(&mut buffer[..len]).await?;

            // Deserialize message
            let message: RaftMessage = bincode::deserialize(&buffer[..len])?;
            tx.send(message)?;
        }

        Ok(())
    }

    /// Main Raft tick loop
    async fn run_tick_loop(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

        loop {
            interval.tick().await;

            let mut node = self.node.lock().unwrap();
            node.tick();

            // Check if we're the leader
            let is_leader = node.raft.state == StateRole::Leader;

            // Update HA state based on leadership
            let current_mode = self.ha_state.get_mode().await;
            match (is_leader, current_mode) {
                (true, ServerMode::Standby | ServerMode::Initializing) => {
                    info!("Became Raft leader - promoting to Active mode");
                    self.ha_state.set_mode(ServerMode::Fenced).await;
                    // Actual promotion happens in the main HA coordinator
                    self.ha_state.set_leader(Some(self.config.node_id.to_string())).await;
                }
                (false, ServerMode::Active) => {
                    warn!("Lost Raft leadership - demoting to Standby mode");
                    self.ha_state.set_mode(ServerMode::Fenced).await;
                    // Actual demotion happens in the main HA coordinator
                }
                _ => {}
            }

            // Process ready states
            if !node.has_ready() {
                continue;
            }

            let mut ready = node.ready();

            // Send messages to peers
            for msg in ready.take_messages() {
                if let Err(e) = self.send_to_peer(msg.to, RaftMessage::Raft(msg)).await {
                    error!("Failed to send message to peer {}: {}", msg.to, e);
                }
            }

            // Apply committed entries (not needed for leader election, but for completeness)
            if let Some(committed_entries) = ready.committed_entries.take() {
                for entry in committed_entries {
                    if entry.data.is_empty() {
                        // Empty entry from new leader
                        continue;
                    }
                    // Handle committed entries if needed
                }
            }

            // Persist Raft state if needed
            if let Some(hs) = ready.hs() {
                // In a real implementation, persist hard state
                debug!("Would persist hard state: {:?}", hs);
            }

            // Advance Raft
            let mut light_rd = node.advance(ready);
            
            // Send out messages again
            for msg in light_rd.take_messages() {
                if let Err(e) = self.send_to_peer(msg.to, RaftMessage::Raft(msg)).await {
                    error!("Failed to send message to peer {}: {}", msg.to, e);
                }
            }

            // Apply committed entries from light ready
            if let Some(committed_entries) = light_rd.take_committed_entries() {
                for entry in committed_entries {
                    if entry.data.is_empty() {
                        continue;
                    }
                }
            }

            node.advance_apply();
        }
    }

    /// Process incoming Raft messages
    async fn run_message_processor(&self) {
        let mut rx = self.message_rx.lock().unwrap();

        while let Some(msg) = rx.recv().await {
            match msg {
                RaftMessage::Raft(raft_msg) => {
                    let mut node = self.node.lock().unwrap();
                    if let Err(e) = node.step(raft_msg) {
                        error!("Raft step error: {}", e);
                    }
                }
                RaftMessage::LeaderRequest => {
                    // Respond with current leader
                    let leader_id = {
                        let node = self.node.lock().unwrap();
                        let leader = node.raft.leader_id;
                        if leader == 0 {
                            None
                        } else {
                            Some(leader)
                        }
                    };
                    // Send response (implementation depends on who asked)
                    debug!("Leader request - current leader: {:?}", leader_id);
                }
                RaftMessage::LeaderResponse { leader_id } => {
                    debug!("Received leader info: {:?}", leader_id);
                    if let Some(id) = leader_id {
                        self.ha_state.set_leader(Some(id.to_string())).await;
                    }
                }
            }
        }
    }

    /// Send message to a peer
    async fn send_to_peer(&self, peer_id: u64, message: RaftMessage) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let peer_addr = self
            .config
            .peers
            .get(&peer_id)
            .ok_or_else(|| anyhow!("Unknown peer: {}", peer_id))?;

        // Connect to peer
        let mut stream = TcpStream::connect(peer_addr).await?;

        // Serialize message
        let data = bincode::serialize(&message)?;
        let len = data.len() as u32;

        // Send length + data
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&data).await?;
        stream.flush().await?;

        Ok(())
    }

    /// Get current leader ID
    pub fn get_leader_id(&self) -> Option<u64> {
        let node = self.node.lock().unwrap();
        let leader = node.raft.leader_id;
        if leader == 0 {
            None
        } else {
            Some(leader)
        }
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        let node = self.node.lock().unwrap();
        node.raft.state == StateRole::Leader
    }

    /// Propose becoming the leader (campaign)
    pub fn campaign(&self) -> Result<()> {
        let mut node = self.node.lock().unwrap();
        node.campaign().map_err(|e| anyhow!("Campaign failed: {}", e))
    }
}

