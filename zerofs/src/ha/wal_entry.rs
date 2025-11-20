/// WAL entry format for replication

use super::replication_protocol::ReplicationProtocol;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// WAL entry sent over the network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALEntry {
    /// Sequence number (monotonically increasing)
    pub sequence: u64,
    
    /// Node ID that generated this entry
    pub node_id: String,
    
    /// Timestamp when entry was created (milliseconds since epoch)
    pub timestamp_millis: u64,
    
    /// Serialized WriteBatch data
    pub batch_data: Bytes,
    
    /// Protocol used for this entry
    pub protocol: ReplicationProtocol,
}

/// Acknowledgment sent by standby nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALAck {
    /// Sequence number being acknowledged
    pub sequence: u64,
    
    /// Node ID of standby sending ACK
    pub node_id: String,
    
    /// Success or error
    pub success: bool,
    
    /// Error message if any
    pub error: Option<String>,
}

/// Wire protocol: Simple length-prefixed messages
/// Format: [4 bytes: message type][4 bytes: length][N bytes: payload]
#[derive(Debug)]
pub enum WireMessage {
    Entry(WALEntry),
    Ack(WALAck),
    Ping,
    Pong,
}

impl WireMessage {
    const MSG_TYPE_ENTRY: u32 = 1;
    const MSG_TYPE_ACK: u32 = 2;
    const MSG_TYPE_PING: u32 = 3;
    const MSG_TYPE_PONG: u32 = 4;
    
    /// Encode message to bytes
    pub fn encode(&self) -> Result<Bytes> {
        let (msg_type, payload) = match self {
            WireMessage::Entry(e) => (Self::MSG_TYPE_ENTRY, bincode::serialize(e)?),
            WireMessage::Ack(a) => (Self::MSG_TYPE_ACK, bincode::serialize(a)?),
            WireMessage::Ping => (Self::MSG_TYPE_PING, vec![]),
            WireMessage::Pong => (Self::MSG_TYPE_PONG, vec![]),
        };
        
        let len = payload.len() as u32;
        let mut buf = Vec::with_capacity(8 + payload.len());
        buf.extend_from_slice(&msg_type.to_be_bytes());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&payload);
        
        Ok(Bytes::from(buf))
    }
    
    /// Decode message from reader
    pub async fn decode<R: tokio::io::AsyncReadExt + Unpin>(
        reader: &mut R,
    ) -> Result<Self> {
        use tokio::io::AsyncReadExt;
        
        let msg_type = reader.read_u32().await?;
        let len = reader.read_u32().await?;
        
        let mut payload = vec![0u8; len as usize];
        reader.read_exact(&mut payload).await?;
        
        match msg_type {
            Self::MSG_TYPE_ENTRY => Ok(WireMessage::Entry(bincode::deserialize(&payload)?)),
            Self::MSG_TYPE_ACK => Ok(WireMessage::Ack(bincode::deserialize(&payload)?)),
            Self::MSG_TYPE_PING => Ok(WireMessage::Ping),
            Self::MSG_TYPE_PONG => Ok(WireMessage::Pong),
            _ => Err(anyhow::anyhow!("Unknown message type: {}", msg_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_wire_message_encode_decode() {
        let entry = WALEntry {
            sequence: 42,
            node_id: "node-1".to_string(),
            timestamp_millis: 1234567890,
            batch_data: Bytes::from(vec![1, 2, 3, 4]),
            protocol: ReplicationProtocol::Async,
        };
        
        let msg = WireMessage::Entry(entry.clone());
        let encoded = msg.encode().unwrap();
        
        // Should be able to decode
        assert!(encoded.len() > 8);
    }
    
    #[test]
    fn test_ack_message() {
        let ack = WALAck {
            sequence: 42,
            node_id: "node-2".to_string(),
            success: true,
            error: None,
        };
        
        let msg = WireMessage::Ack(ack);
        let encoded = msg.encode().unwrap();
        
        assert!(encoded.len() > 8);
    }
}

