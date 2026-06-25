use super::errors::FsError;
use super::inode::InodeId;
use bytes::Bytes;

// Key layout for the underlying LSM.
//
// Two on-disk layouts coexist:
//   v1 (legacy, pre-segments): [kind: 1] + [id: 8] + ...
//   v2 (segmented):            [b"meta" | b"chunk"] + [kind: 1] + [id: 8] + ...
//
// V2 prepends a domain prefix that slatedb's segment extractor matches on
// (RFC-0024). All metadata kinds route into the `b"meta"` segment; chunks
// route into `b"chunk"`. Each segment is an independent LSM tree, so
// metadata churn and bulk-data compaction never share an L0 list or
// compaction lifecycle. Metadata/chunk isolation is therefore structural
// in v2, not lexicographic.
//
// Within a single segment, kind-byte values still determine block-level
// adjacency. A `lookup()` touches both INODE and DIR_ENTRY: with kind
// bytes 0x01/0x02 adjacent, their entries land in neighbouring blocks of
// the same meta-segment SST, so the read can reuse the same block-cache
// index/filter entries. Similarly, DIR_ENTRY/DIR_SCAN/DIR_COOKIE (0x02
// /0x03/0x04) are kept adjacent for directory operations.
//
// Kind byte assignments (one byte each, both layouts):
//   0x01 INODE         hot metadata, point-keyed by inode_id
//   0x02 DIR_ENTRY     hot metadata, lookup by (dir_id, name)
//   0x03 DIR_SCAN      hot metadata, ordered scan by (dir_id, cookie)
//   0x04 DIR_COOKIE    per-directory cookie counter
//   0x05 STATS         shard-keyed fs-wide counters
//   0x06 SYSTEM        rare config (e.g. next-inode counter)
//   0x07 TOMBSTONE     deferred-deletion entries, scanned only by GC
//   0x08 ORPHAN        open-unlinked inodes pending reclaim, drained at startup
//   0xFE CHUNK         bulk file data — the only kind in the chunk segment
//
// V1 keeps the same kind bytes but no domain prefix, so every kind shares
// a single LSM tree. The 0xFE/0x01..0x07 gap that used to isolate chunks
// from metadata via lexicographic distance is vestigial under v2: chunks
// are now in their own segment regardless of byte value.

const PREFIX_INODE: u8 = 0x01;
const PREFIX_DIR_ENTRY: u8 = 0x02;
const PREFIX_DIR_SCAN: u8 = 0x03;
const PREFIX_DIR_COOKIE: u8 = 0x04;
const PREFIX_STATS: u8 = 0x05;
const PREFIX_SYSTEM: u8 = 0x06;
const PREFIX_TOMBSTONE: u8 = 0x07;
const PREFIX_ORPHAN: u8 = 0x08;
const PREFIX_CHUNK: u8 = 0xFE;

const SYSTEM_COUNTER_SUBTYPE: u8 = 0x01;
// HA: the highest shipped replication batch seqno (with its writer epoch) that
// has been flushed into this data db. Written atomically with each shipped
// batch so a promoted standby can prune its tail to exactly what the db already
// holds (see write_coordinator + takeover replay).
const SYSTEM_HA_SEQNO_SUBTYPE: u8 = 0x02;
// Durability lineage token (see fsync-honesty / ZeroFS::lineage_token). A single
// u64 identifying the current unbroken durable lineage. Regenerated at a cold
// bootstrap or a Solo-tainted takeover; carried forward unchanged at an untainted
// takeover (so a clean failover keeps a client's fsync transparent).
const SYSTEM_LINEAGE_SUBTYPE: u8 = 0x03;
// Solo taint: set to the lineage token that was live when the leader first
// downgraded to Solo replication. A takeover reads it to decide keep-vs-regenerate
// the lineage token (taint == stored lineage => the lineage may be missing acked
// Solo writes => regenerate, so those writes' fsync fails instead of reporting success).
const SYSTEM_TAINT_SUBTYPE: u8 = 0x04;

const U64_SIZE: usize = std::mem::size_of::<u64>();

/// v2 domain prefix for any metadata kind.
pub const META_DOMAIN: &[u8] = b"meta";
/// v2 domain prefix for bulk chunk data.
pub const CHUNK_DOMAIN: &[u8] = b"chunk";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPrefix {
    Inode,
    Chunk,
    DirEntry,
    DirScan,
    Tombstone,
    Orphan,
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
            PREFIX_ORPHAN => Ok(Self::Orphan),
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
            KeyPrefix::Orphan => PREFIX_ORPHAN,
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
            Self::Orphan => "ORPHAN",
            Self::Stats => "STATS",
            Self::System => "SYSTEM",
            Self::DirCookie => "DIR_COOKIE",
        }
    }

    fn domain(self) -> &'static [u8] {
        match self {
            KeyPrefix::Chunk => CHUNK_DOMAIN,
            _ => META_DOMAIN,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ParsedKey {
    DirScan { cookie: u64 },
    Tombstone { inode_id: InodeId },
    Orphan { inode_id: InodeId },
    Unknown,
}

/// Per-volume key encoder/decoder. The `use_segment_layout` flag is set
/// once at DB open time based on whether the volume was created with
/// segment-oriented compaction enabled (RFC-0024); it never changes for
/// the life of a DB handle.
#[derive(Debug, Clone)]
pub struct KeyCodec {
    use_segment_layout: bool,
}

impl KeyCodec {
    pub fn new(use_segment_layout: bool) -> Self {
        Self { use_segment_layout }
    }

    /// Number of bytes the domain prefix contributes for `prefix`.
    pub fn domain_len(&self, prefix: KeyPrefix) -> usize {
        if self.use_segment_layout {
            prefix.domain().len()
        } else {
            0
        }
    }

    /// Byte offset where the kind byte lives for `prefix`.
    pub fn kind_offset(&self, prefix: KeyPrefix) -> usize {
        self.domain_len(prefix)
    }

    /// Byte offset where the id portion lives for `prefix`. Used by raw
    /// key consumers (tests, verifiers) that need to slice key bytes
    /// without going through a typed parse_*.
    pub fn id_offset(&self, prefix: KeyPrefix) -> usize {
        self.kind_offset(prefix) + 1
    }

    /// Push the domain prefix (if any) plus the kind byte onto `key`.
    fn push_prefix(&self, key: &mut Vec<u8>, prefix: KeyPrefix) {
        if self.use_segment_layout {
            key.extend_from_slice(prefix.domain());
        }
        key.push(u8::from(prefix));
    }

    /// Total bytes in a complete inode key.
    pub fn inode_key_size(&self) -> usize {
        self.id_offset(KeyPrefix::Inode) + U64_SIZE
    }

    /// Total bytes in a complete chunk key.
    pub fn chunk_key_size(&self) -> usize {
        self.id_offset(KeyPrefix::Chunk) + U64_SIZE * 2
    }

    /// Total bytes in a complete tombstone key.
    pub fn tombstone_key_size(&self) -> usize {
        self.id_offset(KeyPrefix::Tombstone) + U64_SIZE * 2
    }

    /// Total bytes in a complete orphan key.
    pub fn orphan_key_size(&self) -> usize {
        self.id_offset(KeyPrefix::Orphan) + U64_SIZE
    }

    pub fn inode_key(&self, inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(self.inode_key_size());
        self.push_prefix(&mut key, KeyPrefix::Inode);
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn chunk_key(&self, inode_id: InodeId, chunk_index: u64) -> Bytes {
        let mut key = Vec::with_capacity(self.chunk_key_size());
        self.push_prefix(&mut key, KeyPrefix::Chunk);
        key.extend_from_slice(&inode_id.to_be_bytes());
        key.extend_from_slice(&chunk_index.to_be_bytes());
        Bytes::from(key)
    }

    pub fn parse_chunk_key(&self, key: &[u8]) -> Option<u64> {
        let expected = self.chunk_key_size();
        if key.len() != expected {
            return None;
        }
        let kind_off = self.kind_offset(KeyPrefix::Chunk);
        if self.use_segment_layout && !key.starts_with(CHUNK_DOMAIN) {
            return None;
        }
        if key[kind_off] != PREFIX_CHUNK {
            return None;
        }
        let chunk_off = self.id_offset(KeyPrefix::Chunk) + U64_SIZE;
        let chunk_bytes: [u8; U64_SIZE] = key[chunk_off..expected].try_into().ok()?;
        Some(u64::from_be_bytes(chunk_bytes))
    }

    pub fn dir_entry_key(&self, dir_id: InodeId, name: &[u8]) -> Bytes {
        let mut key =
            Vec::with_capacity(self.id_offset(KeyPrefix::DirEntry) + U64_SIZE + name.len());
        self.push_prefix(&mut key, KeyPrefix::DirEntry);
        key.extend_from_slice(&dir_id.to_be_bytes());
        key.extend_from_slice(name);
        Bytes::from(key)
    }

    pub fn dir_scan_key(&self, dir_id: InodeId, cookie: u64) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::DirScan) + U64_SIZE * 2);
        self.push_prefix(&mut key, KeyPrefix::DirScan);
        key.extend_from_slice(&dir_id.to_be_bytes());
        key.extend_from_slice(&cookie.to_be_bytes());
        Bytes::from(key)
    }

    pub fn dir_scan_prefix(&self, dir_id: InodeId) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(self.id_offset(KeyPrefix::DirScan) + U64_SIZE);
        self.push_prefix(&mut prefix, KeyPrefix::DirScan);
        prefix.extend_from_slice(&dir_id.to_be_bytes());
        prefix
    }

    /// Build a key for resuming dir scan from a specific cookie
    pub fn dir_scan_resume_key(&self, dir_id: InodeId, resume_after_cookie: u64) -> Bytes {
        let mut key = self.dir_scan_prefix(dir_id);
        key.extend_from_slice(&(resume_after_cookie + 1).to_be_bytes());
        Bytes::from(key)
    }

    /// Key for storing next cookie counter per directory
    pub fn dir_cookie_counter_key(&self, dir_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::DirCookie) + U64_SIZE);
        self.push_prefix(&mut key, KeyPrefix::DirCookie);
        key.extend_from_slice(&dir_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn tombstone_key(&self, timestamp: u64, inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(self.tombstone_key_size());
        self.push_prefix(&mut key, KeyPrefix::Tombstone);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    /// Key for an orphan-set entry. The inode_id alone keys the entry (no
    /// timestamp, unlike tombstones): presence signals "open-unlinked, pending
    /// reclaim", so it must be a point key the reclaim path can delete by id.
    pub fn orphan_key(&self, inode_id: InodeId) -> Bytes {
        let mut key = Vec::with_capacity(self.orphan_key_size());
        self.push_prefix(&mut key, KeyPrefix::Orphan);
        key.extend_from_slice(&inode_id.to_be_bytes());
        Bytes::from(key)
    }

    pub fn stats_shard_key(&self, shard_id: usize) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::Stats) + U64_SIZE);
        self.push_prefix(&mut key, KeyPrefix::Stats);
        key.extend_from_slice(&(shard_id as u64).to_be_bytes());
        Bytes::from(key)
    }

    pub fn system_counter_key(&self) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::System) + 1);
        self.push_prefix(&mut key, KeyPrefix::System);
        key.push(SYSTEM_COUNTER_SUBTYPE);
        Bytes::from(key)
    }

    /// Key for the HA tail watermark: the highest shipped replication batch seqno
    /// (and the writer epoch that shipped it) flushed into this data db.
    pub fn ha_seqno_key(&self) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::System) + 1);
        self.push_prefix(&mut key, KeyPrefix::System);
        key.push(SYSTEM_HA_SEQNO_SUBTYPE);
        Bytes::from(key)
    }

    pub fn encode_ha_seqno(writer_epoch: u64, seqno: u64) -> Bytes {
        let mut v = Vec::with_capacity(U64_SIZE * 2);
        v.extend_from_slice(&writer_epoch.to_le_bytes());
        v.extend_from_slice(&seqno.to_le_bytes());
        Bytes::from(v)
    }

    /// Key for the durability lineage token (a single u64).
    pub fn lineage_key(&self) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::System) + 1);
        self.push_prefix(&mut key, KeyPrefix::System);
        key.push(SYSTEM_LINEAGE_SUBTYPE);
        Bytes::from(key)
    }

    /// Key for the Solo taint (the lineage token that went Solo).
    pub fn taint_key(&self) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::System) + 1);
        self.push_prefix(&mut key, KeyPrefix::System);
        key.push(SYSTEM_TAINT_SUBTYPE);
        Bytes::from(key)
    }

    pub fn encode_u64(value: u64) -> Bytes {
        Bytes::copy_from_slice(&value.to_le_bytes())
    }

    pub fn decode_u64(data: &[u8]) -> Option<u64> {
        if data.len() != U64_SIZE {
            return None;
        }
        Some(u64::from_le_bytes(data[..U64_SIZE].try_into().ok()?))
    }

    /// Returns `(writer_epoch, seqno)`.
    pub fn decode_ha_seqno(data: &[u8]) -> Option<(u64, u64)> {
        if data.len() != U64_SIZE * 2 {
            return None;
        }
        let epoch = u64::from_le_bytes(data[..U64_SIZE].try_into().ok()?);
        let seqno = u64::from_le_bytes(data[U64_SIZE..].try_into().ok()?);
        Some((epoch, seqno))
    }

    pub fn parse_key(&self, key: &[u8]) -> ParsedKey {
        let kind = match self.peek_kind(key) {
            Some(k) => k,
            None => return ParsedKey::Unknown,
        };
        let id_off = self.id_offset(kind);

        match kind {
            KeyPrefix::DirScan => {
                let expected = id_off + U64_SIZE * 2;
                if key.len() != expected {
                    return ParsedKey::Unknown;
                }
                if let Ok(cookie_bytes) = key[id_off + U64_SIZE..expected].try_into() {
                    let cookie = u64::from_be_bytes(cookie_bytes);
                    ParsedKey::DirScan { cookie }
                } else {
                    ParsedKey::Unknown
                }
            }
            KeyPrefix::Tombstone => {
                let expected = self.tombstone_key_size();
                if key.len() != expected {
                    return ParsedKey::Unknown;
                }
                if let Ok(id_bytes) = key[id_off + U64_SIZE..expected].try_into() {
                    ParsedKey::Tombstone {
                        inode_id: u64::from_be_bytes(id_bytes),
                    }
                } else {
                    ParsedKey::Unknown
                }
            }
            KeyPrefix::Orphan => {
                let expected = self.orphan_key_size();
                if key.len() != expected {
                    return ParsedKey::Unknown;
                }
                if let Ok(id_bytes) = key[id_off..expected].try_into() {
                    ParsedKey::Orphan {
                        inode_id: u64::from_be_bytes(id_bytes),
                    }
                } else {
                    ParsedKey::Unknown
                }
            }
            _ => ParsedKey::Unknown,
        }
    }

    /// Decode the kind byte from a stored key. Returns `None` if the key
    /// is too short, lacks the expected domain prefix (v2), or carries a
    /// kind byte we don't recognize.
    fn peek_kind(&self, key: &[u8]) -> Option<KeyPrefix> {
        if self.use_segment_layout {
            // v2: dispatch on the leading domain prefix to determine which
            // kind byte to read.
            if let Some(rest) = key.strip_prefix(CHUNK_DOMAIN) {
                let kind = KeyPrefix::try_from(*rest.first()?).ok()?;
                return (kind == KeyPrefix::Chunk).then_some(kind);
            }
            if let Some(rest) = key.strip_prefix(META_DOMAIN) {
                let kind = KeyPrefix::try_from(*rest.first()?).ok()?;
                return (kind != KeyPrefix::Chunk).then_some(kind);
            }
            None
        } else {
            KeyPrefix::try_from(*key.first()?).ok()
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

    /// Half-open `[start, end)` range covering every key of `prefix`.
    /// In v2, `end` is the prefix bytes followed by the kind-byte successor,
    /// so the range stays within the domain segment.
    pub fn prefix_range(&self, prefix: KeyPrefix) -> (Bytes, Bytes) {
        let mut start = Vec::with_capacity(self.id_offset(prefix));
        self.push_prefix(&mut start, prefix);
        let mut end = start.clone();
        // The kind byte we just pushed is at `start.len() - 1`. The end of
        // the range is the same bytes with that kind byte incremented by 1.
        let last_idx = end.len() - 1;
        end[last_idx] += 1;
        (Bytes::from(start), Bytes::from(end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v1() -> KeyCodec {
        KeyCodec::new(false)
    }
    fn v2() -> KeyCodec {
        KeyCodec::new(true)
    }

    #[test]
    fn test_dir_scan_parsing() {
        for codec in [v1(), v2()] {
            let dir_id = 10u64;
            let cookie = 42u64;
            let key = codec.dir_scan_key(dir_id, cookie);

            match codec.parse_key(&key) {
                ParsedKey::DirScan {
                    cookie: parsed_cookie,
                } => {
                    assert_eq!(parsed_cookie, cookie);
                }
                _ => panic!(
                    "Failed to parse dir scan key (segment_layout={})",
                    codec.use_segment_layout
                ),
            }
        }
    }

    #[test]
    fn test_tombstone_parsing() {
        for codec in [v1(), v2()] {
            let timestamp = 123456u64;
            let inode_id = 789u64;
            let key = codec.tombstone_key(timestamp, inode_id);

            match codec.parse_key(&key) {
                ParsedKey::Tombstone {
                    inode_id: parsed_id,
                } => {
                    assert_eq!(parsed_id, inode_id);
                }
                _ => panic!(
                    "Failed to parse tombstone key (segment_layout={})",
                    codec.use_segment_layout
                ),
            }
        }
    }

    #[test]
    fn test_chunk_parsing() {
        for codec in [v1(), v2()] {
            let inode_id = 7u64;
            let chunk_index = 99u64;
            let key = codec.chunk_key(inode_id, chunk_index);
            assert_eq!(codec.parse_chunk_key(&key), Some(chunk_index));
        }
    }

    #[test]
    fn test_v2_layout_routing() {
        let codec = v2();
        let inode_key = codec.inode_key(0);
        assert!(inode_key.starts_with(META_DOMAIN));
        assert_eq!(inode_key[META_DOMAIN.len()], PREFIX_INODE);

        let chunk_key = codec.chunk_key(0, 0);
        assert!(chunk_key.starts_with(CHUNK_DOMAIN));
        assert_eq!(chunk_key[CHUNK_DOMAIN.len()], PREFIX_CHUNK);

        let tombstone = codec.tombstone_key(0, 0);
        assert!(tombstone.starts_with(META_DOMAIN));

        // No metadata key should be misrouted into the chunk domain.
        assert!(!inode_key.starts_with(CHUNK_DOMAIN));
        assert!(!tombstone.starts_with(CHUNK_DOMAIN));
    }

    #[test]
    fn test_value_encoding() {
        let counter = 12345u64;
        let encoded = KeyCodec::encode_counter(counter);
        let decoded = KeyCodec::decode_counter(&encoded).unwrap();
        assert_eq!(decoded, counter);

        let inode_id = 999u64;
        let cookie = 42u64;
        let encoded = KeyCodec::encode_dir_entry(inode_id, cookie);
        let (decoded_id, decoded_cookie) = KeyCodec::decode_dir_entry(&encoded).unwrap();
        assert_eq!(decoded_id, inode_id);
        assert_eq!(decoded_cookie, cookie);

        let size = 1024u64;
        let encoded = KeyCodec::encode_tombstone_size(size);
        let decoded = KeyCodec::decode_tombstone_size(&encoded).unwrap();
        assert_eq!(decoded, size);
    }

    #[test]
    fn test_ha_seqno_encoding() {
        let (epoch, seqno) = (7u64, 123456u64);
        let encoded = KeyCodec::encode_ha_seqno(epoch, seqno);
        assert_eq!(KeyCodec::decode_ha_seqno(&encoded), Some((epoch, seqno)));
        // Wrong length is rejected, not silently misread.
        assert_eq!(KeyCodec::decode_ha_seqno(&encoded[..8]), None);
        assert_eq!(KeyCodec::decode_ha_seqno(&[]), None);

        // The HA-seqno key is distinct from the inode counter (both System-prefixed).
        for v2 in [false, true] {
            let codec = KeyCodec::new(v2);
            assert_ne!(codec.ha_seqno_key(), codec.system_counter_key());
        }
    }

    #[test]
    fn test_invalid_key_parsing() {
        let codec = v1();
        assert!(matches!(codec.parse_key(&[]), ParsedKey::Unknown));
        assert!(matches!(codec.parse_key(&[0xFF]), ParsedKey::Unknown));
        assert!(matches!(
            codec.parse_key(&[u8::from(KeyPrefix::Inode)]),
            ParsedKey::Unknown
        ));
        let inode_key = codec.inode_key(1);
        assert!(matches!(codec.parse_key(&inode_key), ParsedKey::Unknown));
    }
}
