use super::errors::FsError;
use super::inode::InodeId;
use bytes::Bytes;

// Key prefix design for LSM tree optimization with size-tiered compaction.
//
// Prefixes are ordered to optimize S3 request patterns during scans. Since SlateDB
// stores each SST as a separate S3 object, keys that are scanned together should be
// adjacent in keyspace to minimize the number of SSTs (and thus S3 GETs) touched.
//
// Layout:
//   0x01-0x05: Hot metadata (frequently accessed together)
//     - INODE + DIR_ENTRY are adjacent for lookup() operations
//     - DIR_ENTRY + DIR_SCAN + DIR_COOKIE are adjacent for directory operations
//     - STATS
//   0x06-0x07: Cold metadata
//     - SYSTEM: rarely accessed configuration
//     - TOMBSTONE: only scanned during background GC
//   0xFE: Bulk data
//     - CHUNK: large data that dominates storage; isolated to prevent metadata
//       scans from touching chunk-heavy SSTs

const PREFIX_INODE: u8 = 0x01;
const PREFIX_DIR_ENTRY: u8 = 0x02;
const PREFIX_DIR_SCAN: u8 = 0x03;
const PREFIX_DIR_COOKIE: u8 = 0x04;
const PREFIX_STATS: u8 = 0x05;
const PREFIX_SYSTEM: u8 = 0x06;
const PREFIX_TOMBSTONE: u8 = 0x07;
const PREFIX_CHUNK: u8 = 0xFE;

const SYSTEM_COUNTER_SUBTYPE: u8 = 0x01;

pub const SYSTEM_WRAPPED_ENCRYPTION_KEY: &[u8] = b"system:wrapped_encryption_key";

const U64_SIZE: usize = std::mem::size_of::<u64>();
const KEY_INODE_SIZE: usize = 1 + U64_SIZE;
const KEY_CHUNK_SIZE: usize = 17;
const KEY_TOMBSTONE_SIZE: usize = 17;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPrefix {
    Inode,
    Chunk,
    DirEntry,
    DirScan,
    Tombstone,
    Stats,
    System,
    DirCookie,
}

impl TryFrom<u8> for KeyPrefix {
    type Error = ();

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            PREFIX_INODE => Ok(Self::Inode),
            PREFIX_CHUNK => Ok(Self::Chunk),
            PREFIX_DIR_ENTRY => Ok(Self::DirEntry),
            PREFIX_DIR_SCAN => Ok(Self::DirScan),
            PREFIX_TOMBSTONE => Ok(Self::Tombstone),
            PREFIX_STATS => Ok(Self::Stats),
            PREFIX_SYSTEM => Ok(Self::System),
            PREFIX_DIR_COOKIE => Ok(Self::DirCookie),
            _ => Err(()),
        }
    }
}

impl From<KeyPrefix> for u8 {
    fn from(prefix: KeyPrefix) -> Self {
        match prefix {
            KeyPrefix::Inode => PREFIX_INODE,
            KeyPrefix::Chunk => PREFIX_CHUNK,
            KeyPrefix::DirEntry => PREFIX_DIR_ENTRY,
            KeyPrefix::DirScan => PREFIX_DIR_SCAN,
            KeyPrefix::Tombstone => PREFIX_TOMBSTONE,
            KeyPrefix::Stats => PREFIX_STATS,
            KeyPrefix::System => PREFIX_SYSTEM,
            KeyPrefix::DirCookie => PREFIX_DIR_COOKIE,
        }
    }
}

impl KeyPrefix {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inode => "INODE",
            Self::Chunk => "CHUNK",
            Self::DirEntry => "DIR_ENTRY",
            Self::DirScan => "DIR_SCAN",
            Self::Tombstone => "TOMBSTONE",
            Self::Stats => "STATS",
            Self::System => "SYSTEM",
            Self::DirCookie => "DIR_COOKIE",
        }
    }
}

#[derive(Debug, Clone)]
pub enum ParsedKey {
    DirScan { cookie: u64 },
    Tombstone { inode_id: InodeId },
    Unknown,
}

pub struct KeyCodec;

impl KeyCodec {
    pub fn inode_key(inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(KEY_INODE_SIZE);
        key.push(u8::from(KeyPrefix::Inode));
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn chunk_key(inode_id: InodeId, chunk_index: u64) -> Bytes {
        let mut key = Vec::with_capacity(KEY_CHUNK_SIZE);
        key.push(u8::from(KeyPrefix::Chunk));
        key.extend_from_slice(&inode_id.to_be_bytes());
        key.extend_from_slice(&chunk_index.to_be_bytes());
        Bytes::from(key)
    }

    pub fn parse_chunk_key(key: &[u8]) -> Option<u64> {
        if key.len() != KEY_CHUNK_SIZE || key[0] != PREFIX_CHUNK {
            return None;
        }
        let chunk_bytes: [u8; U64_SIZE] = key[KEY_INODE_SIZE..KEY_CHUNK_SIZE].try_into().ok()?;
        Some(u64::from_be_bytes(chunk_bytes))
    }

    pub fn dir_entry_key(dir_id: InodeId, name: &[u8]) -> Bytes {
        let mut key = Vec::with_capacity(KEY_INODE_SIZE + name.len());
        key.push(u8::from(KeyPrefix::DirEntry));
        key.extend_from_slice(&dir_id.to_be_bytes());
        key.extend_from_slice(name);
        Bytes::from(key)
    }

    pub fn dir_scan_key(dir_id: InodeId, cookie: u64) -> Bytes {
        let mut key = Vec::with_capacity(KEY_CHUNK_SIZE);
        key.push(u8::from(KeyPrefix::DirScan));
        key.extend_from_slice(&dir_id.to_be_bytes());
        key.extend_from_slice(&cookie.to_be_bytes());
        Bytes::from(key)
    }

    pub fn dir_scan_prefix(dir_id: InodeId) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(KEY_INODE_SIZE);
        prefix.push(u8::from(KeyPrefix::DirScan));
        prefix.extend_from_slice(&dir_id.to_be_bytes());
        prefix
    }

    /// Build a key for resuming dir scan from a specific cookie
    pub fn dir_scan_resume_key(dir_id: InodeId, resume_after_cookie: u64) -> Bytes {
        let mut key = Self::dir_scan_prefix(dir_id);
        key.extend_from_slice(&(resume_after_cookie + 1).to_be_bytes());
        Bytes::from(key)
    }

    /// Build the end key for a directory scan range (next directory)
    pub fn dir_scan_end_key(dir_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(KEY_INODE_SIZE);
        key.push(u8::from(KeyPrefix::DirScan));
        key.extend_from_slice(&(dir_id + 1).to_be_bytes());
        Bytes::from(key)
    }

    /// Key for storing next cookie counter per directory
    pub fn dir_cookie_counter_key(dir_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(KEY_INODE_SIZE);
        key.push(u8::from(KeyPrefix::DirCookie));
        key.extend_from_slice(&dir_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn tombstone_key(timestamp: u64, inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(KEY_TOMBSTONE_SIZE);
        key.push(u8::from(KeyPrefix::Tombstone));
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn stats_shard_key(shard_id: usize) -> Bytes {
        let mut key = Vec::with_capacity(KEY_INODE_SIZE);
        key.push(u8::from(KeyPrefix::Stats));
        key.extend_from_slice(&(shard_id as u64).to_be_bytes());
        Bytes::from(key)
    }

    pub fn system_counter_key() -> Bytes {
        Bytes::from(vec![u8::from(KeyPrefix::System), SYSTEM_COUNTER_SUBTYPE])
    }

    pub fn parse_key(key: &[u8]) -> ParsedKey {
        let prefix = match key.first().and_then(|&b| KeyPrefix::try_from(b).ok()) {
            Some(p) => p,
            None => return ParsedKey::Unknown,
        };

        match prefix {
            KeyPrefix::DirScan if key.len() == KEY_CHUNK_SIZE => {
                if let Ok(cookie_bytes) = key[KEY_INODE_SIZE..KEY_CHUNK_SIZE].try_into() {
                    let cookie = u64::from_be_bytes(cookie_bytes);
                    ParsedKey::DirScan { cookie }
                } else {
                    ParsedKey::Unknown
                }
            }
            KeyPrefix::Tombstone if key.len() == KEY_TOMBSTONE_SIZE => {
                if let Ok(id_bytes) = key[KEY_INODE_SIZE..KEY_TOMBSTONE_SIZE].try_into() {
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

    pub fn encode_counter(value: u64) -> Bytes {
        Bytes::copy_from_slice(&value.to_le_bytes())
    }

    pub fn decode_counter(data: &[u8]) -> Result<u64, FsError> {
        if data.len() != U64_SIZE {
            return Err(FsError::InvalidData);
        }
        let bytes: [u8; U64_SIZE] = data.try_into().map_err(|_| FsError::InvalidData)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn encode_dir_entry(inode_id: InodeId, cookie: u64) -> Bytes {
        let mut value = Vec::with_capacity(U64_SIZE * 2);
        value.extend_from_slice(&inode_id.to_le_bytes());
        value.extend_from_slice(&cookie.to_le_bytes());
        Bytes::from(value)
    }

    pub fn decode_dir_entry(data: &[u8]) -> Result<(InodeId, u64), FsError> {
        if data.len() < U64_SIZE * 2 {
            return Err(FsError::InvalidData);
        }
        let inode_bytes: [u8; U64_SIZE] = data[..U64_SIZE]
            .try_into()
            .map_err(|_| FsError::InvalidData)?;
        let cookie_bytes: [u8; U64_SIZE] = data[U64_SIZE..U64_SIZE * 2]
            .try_into()
            .map_err(|_| FsError::InvalidData)?;
        Ok((
            u64::from_le_bytes(inode_bytes),
            u64::from_le_bytes(cookie_bytes),
        ))
    }

    pub fn encode_tombstone_size(size: u64) -> Bytes {
        Bytes::copy_from_slice(&size.to_le_bytes())
    }

    pub fn decode_tombstone_size(data: &[u8]) -> Result<u64, FsError> {
        if data.len() != U64_SIZE {
            return Err(FsError::InvalidData);
        }
        let bytes: [u8; U64_SIZE] = data.try_into().map_err(|_| FsError::InvalidData)?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn prefix_range(prefix: KeyPrefix) -> (Bytes, Bytes) {
        let prefix_byte = u8::from(prefix);
        let start = Bytes::from(vec![prefix_byte]);
        let end = Bytes::from(vec![prefix_byte + 1]);
        (start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_scan_parsing() {
        let dir_id = 10u64;
        let cookie = 42u64;
        let key = KeyCodec::dir_scan_key(dir_id, cookie);

        match KeyCodec::parse_key(&key) {
            ParsedKey::DirScan {
                cookie: parsed_cookie,
            } => {
                assert_eq!(parsed_cookie, cookie);
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
        let cookie = 42u64;
        let encoded = KeyCodec::encode_dir_entry(inode_id, cookie);
        let (decoded_id, decoded_cookie) = KeyCodec::decode_dir_entry(&encoded).unwrap();
        assert_eq!(decoded_id, inode_id);
        assert_eq!(decoded_cookie, cookie);

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
            KeyCodec::parse_key(&[u8::from(KeyPrefix::Inode)]),
            ParsedKey::Unknown
        ));
        let inode_key = KeyCodec::inode_key(1);
        assert!(matches!(
            KeyCodec::parse_key(&inode_key),
            ParsedKey::Unknown
        ));
    }
}
