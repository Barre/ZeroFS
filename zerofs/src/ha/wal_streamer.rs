/// WAL Streamer - Sends WAL entries from active node to standby nodes

use super::replication_protocol::{AckPolicy, ReplicationProtocol};
use super::wal_entry::{WALAck, WALEntry, WireMessage};
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex, RwLock};
use tracing::{debug, error, info, warn};

pub struct WALStreamerConfig {
    pub node_id: String,
    pub standby_addresses: Vec<String>,
    pub default_protocol: ReplicationProtocol,
    pub sync_ack_policy: AckPolicy,
    pub sync_timeout: Duration,
    pub tcp_nodelay: bool,
    pub reconnect_interval: Duration,
}

pub struct WALStreamer {
    config: WALStreamerConfig,
    sequence: Arc<AtomicU64>,
    connections: Arc<RwLock<Vec<StandbyConnection>>>,
}

struct StandbyConnection {
    address: String,
    writer: Arc<Mutex<BufWriter<tokio::io::WriteHalf<TcpStream>>>>,
    pending_acks: Arc<Mutex<HashMap<u64, oneshot::Sender<WALAck>>>>,
}

impl WALStreamer {
    pub async fn new(config: WALStreamerConfig) -> Result<Arc<Self>> {
        let streamer = Arc::new(Self {
            config,
            sequence: Arc::new(AtomicU64::new(0)),
            connections: Arc::new(RwLock::new(Vec::new())),
        });
        
        // Connect to all standbys
        streamer.connect_to_standbys().await;
        
        // Start reconnection task
        let streamer_clone = Arc::clone(&streamer);
        tokio::spawn(async move {
            streamer_clone.reconnection_loop().await;
        });
        
        Ok(streamer)
    }
    
    async fn connect_to_standbys(&self) {
        let mut connections = self.connections.write().await;
        
        for addr in &self.config.standby_addresses {
            match self.connect_to_standby(addr).await {
                Ok(conn) => {
                    info!("✓ Connected to standby: {}", addr);
                    connections.push(conn);
                }
                Err(e) => {
                    warn!("Failed to connect to standby {}: {}", addr, e);
                }
            }
        }
        
        if connections.is_empty() {
            warn!("No standby nodes connected - WAL replication disabled");
        }
    }
    
    async fn connect_to_standby(&self, addr: &str) -> Result<StandbyConnection> {
        let stream = TcpStream::connect(addr).await?;
        
        if self.config.tcp_nodelay {
            stream.set_nodelay(true)?;
        }
        
        let (reader, writer) = tokio::io::split(stream);
        let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
        let pending_acks = Arc::new(Mutex::new(HashMap::new()));
        
        // Start ACK receiver task
        let pending_acks_clone = Arc::clone(&pending_acks);
        let addr_clone = addr.to_string();
        tokio::spawn(async move {
            if let Err(e) = Self::ack_receiver_loop(reader, pending_acks_clone).await {
                debug!("ACK receiver for {} stopped: {}", addr_clone, e);
            }
        });
        
        Ok(StandbyConnection {
            address: addr.to_string(),
            writer,
            pending_acks,
        })
    }
    
    async fn ack_receiver_loop(
        mut reader: tokio::io::ReadHalf<TcpStream>,
        pending_acks: Arc<Mutex<HashMap<u64, oneshot::Sender<WALAck>>>>,
    ) -> Result<()> {
        loop {
            match WireMessage::decode(&mut reader).await {
                Ok(WireMessage::Ack(ack)) => {
                    let mut pending = pending_acks.lock().await;
                    if let Some(sender) = pending.remove(&ack.sequence) {
                        let _ = sender.send(ack);
                    } else {
                        debug!("Received ACK for sequence {} but no pending sender", ack.sequence);
                    }
                }
                Ok(WireMessage::Pong) => {
                    // Heartbeat response - ignore
                }
                Ok(_) => {
                    warn!("Unexpected message type from standby");
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }
    
    /// Replicate a WAL entry using the default protocol
    pub async fn replicate(&self, batch_data: Bytes) -> Result<()> {
        self.replicate_with_protocol(batch_data, self.config.default_protocol)
            .await
    }
    
    /// Replicate with explicit protocol
    pub async fn replicate_with_protocol(
        &self,
        batch_data: Bytes,
        protocol: ReplicationProtocol,
    ) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        
        let entry = WALEntry {
            sequence,
            node_id: self.config.node_id.clone(),
            timestamp_millis: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
            batch_data,
            protocol,
        };
        
        match protocol {
            ReplicationProtocol::Async => self.replicate_async(entry).await,
            ReplicationProtocol::Sync => self.replicate_sync(entry).await,
        }
    }
    
    /// Protocol A: Asynchronous replication (fire-and-forget)
    async fn replicate_async(&self, entry: WALEntry) -> Result<()> {
        let connections = self.connections.read().await;
        
        if connections.is_empty() {
            // No standbys - just log and continue
            debug!("No standbys available for replication");
            return Ok(());
        }
        
        for conn in connections.iter() {
            let entry = entry.clone();
            let writer = Arc::clone(&conn.writer);
            let addr = conn.address.clone();
            
            // Spawn task to send (non-blocking)
            tokio::spawn(async move {
                if let Err(e) = Self::send_entry(writer, entry).await {
                    debug!("Failed to send WAL entry to {}: {}", addr, e);
                }
            });
        }
        
        Ok(())
    }
    
    /// Protocol C: Synchronous replication (wait for ACKs)
    async fn replicate_sync(&self, entry: WALEntry) -> Result<()> {
        let connections = self.connections.read().await;
        
        if connections.is_empty() {
            return Err(anyhow::anyhow!("No standby nodes available for sync replication"));
        }
        
        let mut ack_receivers = Vec::new();
        
        // Send to all standbys and collect ACK receivers
        for conn in connections.iter() {
            let (tx, rx) = oneshot::channel();
            
            // Register pending ACK
            conn.pending_acks.lock().await.insert(entry.sequence, tx);
            
            // Send entry
            let entry = entry.clone();
            let writer = Arc::clone(&conn.writer);
            let addr = conn.address.clone();
            
            tokio::spawn(async move {
                if let Err(e) = Self::send_entry(writer, entry).await {
                    error!("Failed to send WAL entry to {}: {}", addr, e);
                }
            });
            
            ack_receivers.push(rx);
        }
        
        // Wait for ACKs based on policy
        let required_acks = match self.config.sync_ack_policy {
            AckPolicy::One => 1,
            AckPolicy::Majority => (connections.len() / 2) + 1,
            AckPolicy::All => connections.len(),
        };
        
        drop(connections); // Release read lock
        
        let timeout = tokio::time::sleep(self.config.sync_timeout);
        tokio::pin!(timeout);
        
        let mut successful_acks = 0;
        let mut pending_futures = ack_receivers;
        
        loop {
            if pending_futures.is_empty() {
                break;
            }
            
            tokio::select! {
                _ = &mut timeout => {
                    return Err(anyhow::anyhow!(
                        "Timeout waiting for ACKs (got {}/{} required)",
                        successful_acks,
                        required_acks
                    ));
                }
                (result, _index, remaining) = futures::future::select_all(pending_futures) => {
                    pending_futures = remaining;
                    
                    if let Ok(ack) = result {
                        if ack.success {
                            successful_acks += 1;
                            if successful_acks >= required_acks {
                                debug!("Received {}/{} required ACKs", successful_acks, required_acks);
                                return Ok(());
                            }
                        } else {
                            warn!("Received failed ACK from {}: {:?}", ack.node_id, ack.error);
                        }
                    }
                    
                    // Check if we can still reach quorum
                    let max_possible = successful_acks + pending_futures.len();
                    if max_possible < required_acks {
                        return Err(anyhow::anyhow!(
                            "Cannot reach quorum: have {}, need {}, {} pending",
                            successful_acks,
                            required_acks,
                            pending_futures.len()
                        ));
                    }
                }
            }
        }
        
        // Should not reach here if required_acks > 0
        Ok(())
    }
    
    async fn send_entry(
        writer: Arc<Mutex<BufWriter<tokio::io::WriteHalf<TcpStream>>>>,
        entry: WALEntry,
    ) -> Result<()> {
        let msg = WireMessage::Entry(entry);
        let encoded = msg.encode()?;
        
        let mut writer = writer.lock().await;
        writer.write_all(&encoded).await?;
        writer.flush().await?;
        
        Ok(())
    }
    
    async fn reconnection_loop(&self) {
        let mut interval = tokio::time::interval(self.config.reconnect_interval);
        
        loop {
            interval.tick().await;
            
            // Check for disconnected standbys and reconnect
            let current_count = self.connections.read().await.len();
            let expected_count = self.config.standby_addresses.len();
            
            if current_count < expected_count {
                debug!(
                    "Attempting to reconnect standbys ({}/{})",
                    current_count, expected_count
                );
                
                for addr in &self.config.standby_addresses {
                    let connections = self.connections.read().await;
                    let already_connected = connections.iter().any(|c| c.address == *addr);
                    drop(connections);
                    
                    if !already_connected {
                        match self.connect_to_standby(addr).await {
                            Ok(conn) => {
                                info!("✓ Reconnected to standby: {}", addr);
                                self.connections.write().await.push(conn);
                            }
                            Err(_) => {
                                // Don't log error - already logged in connect_to_standby
                            }
                        }
                    }
                }
            }
        }
    }
    
    /// Get current sequence number
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
    
    /// Get number of connected standbys
    pub async fn connected_standbys(&self) -> usize {
        self.connections.read().await.len()
    }
}

