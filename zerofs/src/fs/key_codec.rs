use super::errors::FsError;
use super::inode::InodeId;
use bytes::Bytes;

pub const PREFIX_INODE: u8 = 0x01;
pub const PREFIX_CHUNK: u8 = 0x02;
pub const PREFIX_DIR_ENTRY: u8 = 0x03;
pub const PREFIX_DIR_SCAN: u8 = 0x04;
pub const PREFIX_TOMBSTONE: u8 = 0x05;
pub const PREFIX_STATS: u8 = 0x06;
pub const PREFIX_SYSTEM: u8 = 0x07;

const SYSTEM_COUNTER_SUBTYPE: u8 = 0x01;

#[derive(Debug, Clone)]
pub enum ParsedKey {
    DirScan { entry_id: InodeId, name: String },
    Tombstone { inode_id: InodeId },
    Unknown,
}

pub struct KeyCodec;

impl KeyCodec {
    pub fn inode_key(inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(9);
        key.push(PREFIX_INODE);
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn chunk_key(inode_id: InodeId, chunk_index: u64) -> Bytes {
        let mut key = Vec::with_capacity(17);
        key.push(PREFIX_CHUNK);
        key.extend_from_slice(&inode_id.to_be_bytes());
        key.extend_from_slice(&chunk_index.to_be_bytes());
        Bytes::from(key)
    }

    pub fn dir_entry_key(dir_id: InodeId, name: &str) -> Bytes {
        let mut key = Vec::with_capacity(9 + name.len());
        key.push(PREFIX_DIR_ENTRY);
        key.extend_from_slice(&dir_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        Bytes::from(key)
    }

    pub fn dir_scan_key(dir_id: InodeId, entry_id: InodeId, name: &str) -> Bytes {
        let mut key = Vec::with_capacity(17 + name.len());
        key.push(PREFIX_DIR_SCAN);
        key.extend_from_slice(&dir_id.to_be_bytes());
        key.extend_from_slice(&entry_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        Bytes::from(key)
    }

    pub fn dir_scan_prefix(dir_id: InodeId) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(9);
        prefix.push(PREFIX_DIR_SCAN);
        prefix.extend_from_slice(&dir_id.to_be_bytes());
        prefix
    }

    // Build a key for resuming dir scan from a specific entry
    pub fn dir_scan_resume_key(dir_id: InodeId, resume_from: InodeId) -> Bytes {
        let mut key = Self::dir_scan_prefix(dir_id);
        key.extend_from_slice(&resume_from.to_be_bytes());
        Bytes::from(key)
    }

    // Build the end key for a directory scan range (next directory)
    pub fn dir_scan_end_key(dir_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(9);
        key.push(PREFIX_DIR_SCAN);
        key.extend_from_slice(&(dir_id + 1).to_be_bytes());
        Bytes::from(key)
    }

    pub fn tombstone_key(timestamp: u64, inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(17);
        key.push(PREFIX_TOMBSTONE);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn stats_shard_key(shard_id: usize) -> Bytes {
        let mut key = Vec::with_capacity(9);
        key.push(PREFIX_STATS);
        key.extend_from_slice(&(shard_id as u64).to_be_bytes());
        Bytes::from(key)
    }

    pub fn system_counter_key() -> Bytes {
        Bytes::from(vec![PREFIX_SYSTEM, SYSTEM_COUNTER_SUBTYPE])
    }

    pub fn parse_key(key: &[u8]) -> ParsedKey {
        if key.is_empty() {
            return ParsedKey::Unknown;
        }

        match key[0] {
            PREFIX_DIR_SCAN if key.len() > 17 => {
                if let Ok(entry_bytes) = key[9..17].try_into() {
                    let entry_id = u64::from_be_bytes(entry_bytes);
                    if let Ok(name) = String::from_utf8(key[17..].to_vec()) {
                        ParsedKey::DirScan { entry_id, name }
                    } else {
                        ParsedKey::Unknown
                    }
                } else {
                    ParsedKey::Unknown
                }
            }
            PREFIX_TOMBSTONE if key.len() == 17 => {
                if let Ok(id_bytes) = key[9..17].try_into() {
                    ParsedKey::Tombstone {
                        inode_id: u64::from_be_bytes(id_bytes),
                    }
                } else {
                    ParsedKey::Unknown
                }
            }
            _ => ParsedKey::Unknown,
        }
    }

    pub fn encode_counter(value: u64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    pub fn decode_counter(data: &[u8]) -> Result<u64, FsError> {
        if data.len() != 8 {
            return Err(FsError::InvalidData);
        }
        let bytes: [u8; 8] = data.try_into().map_err(|_| FsError::InvalidData)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn encode_dir_entry(inode_id: InodeId) -> Vec<u8> {
        inode_id.to_le_bytes().to_vec()
    }

    pub fn decode_dir_entry(data: &[u8]) -> Result<InodeId, FsError> {
        if data.len() < 8 {
            return Err(FsError::InvalidData);
        }
        let bytes: [u8; 8] = data[..8].try_into().map_err(|_| FsError::InvalidData)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn encode_tombstone_size(size: u64) -> Vec<u8> {
        size.to_le_bytes().to_vec()
    }

    pub fn decode_tombstone_size(data: &[u8]) -> Result<u64, FsError> {
        if data.len() != 8 {
            return Err(FsError::InvalidData);
        }
        let bytes: [u8; 8] = data.try_into().map_err(|_| FsError::InvalidData)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn prefix_range(prefix: u8) -> (Bytes, Bytes) {
        let start = Bytes::from(vec![prefix]);
        let end = Bytes::from(vec![prefix + 1]);
        (start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_scan_parsing() {
        let dir_id = 10u64;
        let entry_id = 20u64;
        let name = "test_file.txt";
        let key = KeyCodec::dir_scan_key(dir_id, entry_id, name);

        match KeyCodec::parse_key(&key) {
            ParsedKey::DirScan {
                entry_id: parsed_entry,
                name: parsed_name,
            } => {
                assert_eq!(parsed_entry, entry_id);
                assert_eq!(parsed_name, name);
            }
            _ => panic!("Failed to parse dir scan key"),
        }
    }

    #[test]
    fn test_tombstone_parsing() {
        let timestamp = 123456u64;
        let inode_id = 789u64;
        let key = KeyCodec::tombstone_key(timestamp, inode_id);

        match KeyCodec::parse_key(&key) {
            ParsedKey::Tombstone {
                inode_id: parsed_id,
            } => {
                assert_eq!(parsed_id, inode_id);
            }
            _ => panic!("Failed to parse tombstone key"),
        }
    }

    #[test]
    fn test_value_encoding() {
        // Test counter encoding
        let counter = 12345u64;
        let encoded = KeyCodec::encode_counter(counter);
        let decoded = KeyCodec::decode_counter(&encoded).unwrap();
        assert_eq!(decoded, counter);

        // Test dir entry encoding
        let inode_id = 999u64;
        let encoded = KeyCodec::encode_dir_entry(inode_id);
        let decoded = KeyCodec::decode_dir_entry(&encoded).unwrap();
        assert_eq!(decoded, inode_id);

        // Test tombstone size encoding
        let size = 1024u64;
        let encoded = KeyCodec::encode_tombstone_size(size);
        let decoded = KeyCodec::decode_tombstone_size(&encoded).unwrap();
        assert_eq!(decoded, size);
    }

    #[test]
    fn test_invalid_key_parsing() {
        assert!(matches!(KeyCodec::parse_key(&[]), ParsedKey::Unknown));
        assert!(matches!(KeyCodec::parse_key(&[0xFF]), ParsedKey::Unknown));
        assert!(matches!(
            KeyCodec::parse_key(&[PREFIX_INODE]),
            ParsedKey::Unknown
        ));
        let inode_key = KeyCodec::inode_key(1);
        assert!(matches!(
            KeyCodec::parse_key(&inode_key),
            ParsedKey::Unknown
        ));
    }
}
