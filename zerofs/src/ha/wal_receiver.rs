/// WAL Receiver - Receives WAL entries on standby nodes

use super::wal_entry::{WALAck, WALEntry, WireMessage};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

pub struct WALReceiverConfig {
    pub node_id: String,
    pub listen_address: String,
    pub wal_dir: PathBuf,
}

pub struct WALReceiver {
    config: WALReceiverConfig,
    last_applied_sequence: Arc<AtomicU64>,
}

impl WALReceiver {
    pub async fn new(config: WALReceiverConfig) -> Result<Arc<Self>> {
        // Ensure WAL directory exists
        tokio::fs::create_dir_all(&config.wal_dir).await?;
        
        Ok(Arc::new(Self {
            config,
            last_applied_sequence: Arc::new(AtomicU64::new(0)),
        }))
    }
    
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address).await?;
        info!("✓ WAL receiver listening on {}", self.config.listen_address);
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted WAL connection from: {}", addr);
                    
                    let receiver = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(e) = receiver.handle_connection(stream).await {
                            debug!("Connection handler error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }
    
    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let (reader, writer) = tokio::io::split(stream);
        let mut reader = reader;
        let writer = Arc::new(tokio::sync::Mutex::new(BufWriter::new(writer)));
        
        loop {
            match WireMessage::decode(&mut reader).await {
                Ok(WireMessage::Entry(entry)) => {
                    let ack = self.handle_wal_entry(entry).await;
                    self.send_ack(Arc::clone(&writer), ack).await?;
                }
                Ok(WireMessage::Ping) => {
                    self.send_pong(Arc::clone(&writer)).await?;
                }
                Ok(_) => {
                    warn!("Unexpected message type");
                }
                Err(e) => {
                    debug!("Connection closed: {}", e);
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    async fn handle_wal_entry(&self, entry: WALEntry) -> WALAck {
        // Validate sequence ordering
        let expected = self.last_applied_sequence.load(Ordering::SeqCst);
        
        if entry.sequence <= expected {
            debug!(
                "Duplicate or old WAL entry: {} (expected > {})",
                entry.sequence, expected
            );
            return WALAck {
                sequence: entry.sequence,
                node_id: self.config.node_id.clone(),
                success: true, // Already applied
                error: None,
            };
        }
        
        if entry.sequence != expected + 1 {
            error!(
                "Sequence gap: got {}, expected {}",
                entry.sequence,
                expected + 1
            );
            return WALAck {
                sequence: entry.sequence,
                node_id: self.config.node_id.clone(),
                success: false,
                error: Some(format!(
                    "Sequence mismatch: expected {}, got {}",
                    expected + 1,
                    entry.sequence
                )),
            };
        }
        
        // Write WAL entry to local disk
        let wal_file = self
            .config
            .wal_dir
            .join(format!("{:020}.wal", entry.sequence));
        
        match tokio::fs::write(&wal_file, &entry.batch_data).await {
            Ok(()) => {
                self.last_applied_sequence
                    .store(entry.sequence, Ordering::SeqCst);
                
                debug!(
                    "✓ Replicated WAL entry {} from {} (protocol: {})",
                    entry.sequence, entry.node_id, entry.protocol
                );
                
                WALAck {
                    sequence: entry.sequence,
                    node_id: self.config.node_id.clone(),
                    success: true,
                    error: None,
                }
            }
            Err(e) => {
                error!("Failed to write WAL file {}: {}", wal_file.display(), e);
                WALAck {
                    sequence: entry.sequence,
                    node_id: self.config.node_id.clone(),
                    success: false,
                    error: Some(e.to_string()),
                }
            }
        }
    }
    
    async fn send_ack(
        &self,
        writer: Arc<tokio::sync::Mutex<BufWriter<tokio::io::WriteHalf<TcpStream>>>>,
        ack: WALAck,
    ) -> Result<()> {
        let msg = WireMessage::Ack(ack);
        let encoded = msg.encode()?;
        
        let mut writer = writer.lock().await;
        writer.write_all(&encoded).await?;
        writer.flush().await?;
        
        Ok(())
    }
    
    async fn send_pong(
        &self,
        writer: Arc<tokio::sync::Mutex<BufWriter<tokio::io::WriteHalf<TcpStream>>>>,
    ) -> Result<()> {
        let msg = WireMessage::Pong;
        let encoded = msg.encode()?;
        
        let mut writer = writer.lock().await;
        writer.write_all(&encoded).await?;
        writer.flush().await?;
        
        Ok(())
    }
    
    /// Get last applied sequence number
    pub fn last_sequence(&self) -> u64 {
        self.last_applied_sequence.load(Ordering::SeqCst)
    }
}

