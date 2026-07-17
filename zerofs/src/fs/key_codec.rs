use super::errors::FsError;
use super::inode::InodeId;
use crate::replication::types::{HaStamp, ShipSeqno, SoloHistory, WriterEpoch};
use bytes::Bytes;

// Key layout for the underlying LSM.
//
// Every key is [b"meta" | b"extent"] + [kind: 1] + [id: 8] + ...
//
// The leading domain prefix is what slatedb's segment extractor routes on:
// all metadata kinds land in the `b"meta"` segment, bulk extent pointers in
// `b"extent"`. Each segment is an independent LSM tree, so metadata churn and
// bulk-data compaction never share an L0 list or compaction lifecycle.
// Metadata/extent isolation is structural, not lexicographic.
//
// Within the meta segment, kind-byte values determine block-level adjacency.
// A `lookup()` touches both INODE and DIR_ENTRY: with kind bytes 0x01/0x02
// adjacent, their entries land in neighbouring blocks of the same meta-segment
// SST, so the read can reuse the same block-cache index/filter entries.
// Similarly, DIR_ENTRY/DIR_SCAN/DIR_COOKIE (0x02/0x03/0x04) are kept adjacent
// for directory operations.
//
// Kind byte assignments (one byte each):
//   0x01 INODE         hot metadata, point-keyed by inode_id
//   0x02 DIR_ENTRY     hot metadata, lookup by (dir_id, name)
//   0x03 DIR_SCAN      hot metadata, ordered scan by (dir_id, cookie)
//   0x04 DIR_COOKIE    per-directory cookie counter
//   0x05 STATS         shard-keyed fs-wide counters
//   0x06 SYSTEM        rare config (e.g. next-inode counter)
//   0x07 TOMBSTONE     deferred-deletion entries, scanned only by GC
//   0x08 ORPHAN        open-unlinked inodes pending reclaim, drained at startup
//   0x09 SEGCOUNT      per-segment (live, total) byte counters, segid-keyed; drives segment GC reclaim
//   0xFE EXTENT        bulk file data — the only kind in the extent segment

const PREFIX_INODE: u8 = 0x01;
const PREFIX_DIR_ENTRY: u8 = 0x02;
const PREFIX_DIR_SCAN: u8 = 0x03;
const PREFIX_DIR_COOKIE: u8 = 0x04;
const PREFIX_STATS: u8 = 0x05;
const PREFIX_SYSTEM: u8 = 0x06;
const PREFIX_TOMBSTONE: u8 = 0x07;
const PREFIX_ORPHAN: u8 = 0x08;
const PREFIX_SEGCOUNT: u8 = 0x09;
const PREFIX_EXTENT: u8 = 0xFE;

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
// Wall-clock epoch-seconds (u64 LE via encode_u64) of the last completed slow
// orphan sweep. Persisted (not process-uptime) so the daily sweep cadence holds
// across restarts (see gc.rs maybe_sweep_orphans).
const SYSTEM_ORPHAN_SWEEP_SUBTYPE: u8 = 0x05;

const U64_SIZE: usize = std::mem::size_of::<u64>();

/// Domain prefix for any metadata kind.
pub const META_DOMAIN: &[u8] = b"meta";
/// Domain prefix for bulk extent data.
pub const EXTENT_DOMAIN: &[u8] = b"extent";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPrefix {
    Inode,
    Extent,
    DirEntry,
    DirScan,
    Tombstone,
    Orphan,
    Stats,
    System,
    DirCookie,
    SegCount,
}

impl TryFrom<u8> for KeyPrefix {
    type Error = ();

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            PREFIX_INODE => Ok(Self::Inode),
            PREFIX_EXTENT => Ok(Self::Extent),
            PREFIX_DIR_ENTRY => Ok(Self::DirEntry),
            PREFIX_DIR_SCAN => Ok(Self::DirScan),
            PREFIX_TOMBSTONE => Ok(Self::Tombstone),
            PREFIX_ORPHAN => Ok(Self::Orphan),
            PREFIX_STATS => Ok(Self::Stats),
            PREFIX_SYSTEM => Ok(Self::System),
            PREFIX_DIR_COOKIE => Ok(Self::DirCookie),
            PREFIX_SEGCOUNT => Ok(Self::SegCount),
            _ => Err(()),
        }
    }
}

impl From<KeyPrefix> for u8 {
    fn from(prefix: KeyPrefix) -> Self {
        match prefix {
            KeyPrefix::Inode => PREFIX_INODE,
            KeyPrefix::Extent => PREFIX_EXTENT,
            KeyPrefix::DirEntry => PREFIX_DIR_ENTRY,
            KeyPrefix::DirScan => PREFIX_DIR_SCAN,
            KeyPrefix::Tombstone => PREFIX_TOMBSTONE,
            KeyPrefix::Orphan => PREFIX_ORPHAN,
            KeyPrefix::Stats => PREFIX_STATS,
            KeyPrefix::System => PREFIX_SYSTEM,
            KeyPrefix::DirCookie => PREFIX_DIR_COOKIE,
            KeyPrefix::SegCount => PREFIX_SEGCOUNT,
        }
    }
}

impl KeyPrefix {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inode => "INODE",
            Self::Extent => "EXTENT",
            Self::DirEntry => "DIR_ENTRY",
            Self::DirScan => "DIR_SCAN",
            Self::Tombstone => "TOMBSTONE",
            Self::Orphan => "ORPHAN",
            Self::Stats => "STATS",
            Self::System => "SYSTEM",
            Self::DirCookie => "DIR_COOKIE",
            Self::SegCount => "SEGCOUNT",
        }
    }

    fn domain(self) -> &'static [u8] {
        match self {
            KeyPrefix::Extent => EXTENT_DOMAIN,
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

/// Per-volume key encoder/decoder. Stateless: every volume uses the segmented
/// layout, a `b"meta"`/`b"extent"` domain prefix that the slatedb segment
/// extractor routes on.
#[derive(Debug, Clone, Default)]
pub struct KeyCodec;

impl KeyCodec {
    pub fn new() -> Self {
        Self
    }

    /// Number of bytes the domain prefix contributes for `prefix`.
    pub fn domain_len(&self, prefix: KeyPrefix) -> usize {
        prefix.domain().len()
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
        key.extend_from_slice(prefix.domain());
        key.push(u8::from(prefix));
    }

    /// Total bytes in a complete inode key.
    pub fn inode_key_size(&self) -> usize {
        self.id_offset(KeyPrefix::Inode) + U64_SIZE
    }

    /// Total bytes in a complete extent key.
    pub fn extent_key_size(&self) -> usize {
        self.id_offset(KeyPrefix::Extent) + U64_SIZE * 2
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

    pub fn extent_key(&self, inode_id: InodeId, extent_index: u64) -> Bytes {
        let mut key = Vec::with_capacity(self.extent_key_size());
        self.push_prefix(&mut key, KeyPrefix::Extent);
        key.extend_from_slice(&inode_id.to_be_bytes());
        key.extend_from_slice(&extent_index.to_be_bytes());
        Bytes::from(key)
    }

    pub fn parse_extent_key(&self, key: &[u8]) -> Option<u64> {
        let expected = self.extent_key_size();
        if key.len() != expected {
            return None;
        }
        let kind_off = self.kind_offset(KeyPrefix::Extent);
        if !key.starts_with(EXTENT_DOMAIN) {
            return None;
        }
        if key[kind_off] != PREFIX_EXTENT {
            return None;
        }
        let extent_off = self.id_offset(KeyPrefix::Extent) + U64_SIZE;
        let extent_bytes: [u8; U64_SIZE] = key[extent_off..expected].try_into().ok()?;
        Some(u64::from_be_bytes(extent_bytes))
    }

    /// `(inode_id, extent_index)` for an extent key, for the HA standby rebuilding a
    /// shipped segment's directory on takeover.
    pub fn parse_extent_key_full(&self, key: &[u8]) -> Option<(InodeId, u64)> {
        let expected = self.extent_key_size();
        if key.len() != expected {
            return None;
        }
        let kind_off = self.kind_offset(KeyPrefix::Extent);
        if !key.starts_with(EXTENT_DOMAIN) {
            return None;
        }
        if key[kind_off] != PREFIX_EXTENT {
            return None;
        }
        let id_off = self.id_offset(KeyPrefix::Extent);
        let inode = u64::from_be_bytes(key[id_off..id_off + U64_SIZE].try_into().ok()?);
        let extent = u64::from_be_bytes(key[id_off + U64_SIZE..expected].try_into().ok()?);
        Some((inode, extent))
    }

    /// Key for a segment's live-byte counter, keyed by its `(epoch, counter)` segid.
    /// Big-endian id so a prefix scan visits segments in creation order (like every
    /// other id key); the value is `(live, total)` via [`Self::encode_segcount`].
    pub fn segcount_key(&self, epoch: u64, counter: u64) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::SegCount) + U64_SIZE * 2);
        self.push_prefix(&mut key, KeyPrefix::SegCount);
        key.extend_from_slice(&epoch.to_be_bytes());
        key.extend_from_slice(&counter.to_be_bytes());
        Bytes::from(key)
    }

    /// `(epoch, counter)` of a segcount key, for the reclaim / rebuild keyspace scan.
    pub fn parse_segcount_key(&self, key: &[u8]) -> Option<(u64, u64)> {
        let id_off = self.id_offset(KeyPrefix::SegCount);
        if key.len() != id_off + U64_SIZE * 2
            || !key.starts_with(META_DOMAIN)
            || key[self.kind_offset(KeyPrefix::SegCount)] != PREFIX_SEGCOUNT
        {
            return None;
        }
        let epoch = u64::from_be_bytes(key[id_off..id_off + U64_SIZE].try_into().ok()?);
        let counter = u64::from_be_bytes(key[id_off + U64_SIZE..].try_into().ok()?);
        Some((epoch, counter))
    }

    /// Half-open `[start, end)` covering every segcount key, for a full scan.
    pub fn segcount_prefix_range(&self) -> (Bytes, Bytes) {
        self.prefix_range(KeyPrefix::SegCount)
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

    /// Key for HA provenance flushed atomically with each replicated leader
    /// batch. The stamp records the writer epoch, acknowledged ship, Solo
    /// history, and highest locally applied ship attempt; takeover validates the
    /// volatile tail and exact-result ledger against it.
    pub fn ha_seqno_key(&self) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::System) + 1);
        self.push_prefix(&mut key, KeyPrefix::System);
        key.push(SYSTEM_HA_SEQNO_SUBTYPE);
        Bytes::from(key)
    }

    pub(crate) fn encode_ha_stamp(stamp: &HaStamp) -> Bytes {
        let mut v = Vec::with_capacity(U64_SIZE * 5);
        v.extend_from_slice(&stamp.writer_epoch().get().to_le_bytes());
        v.extend_from_slice(&stamp.last_shipped().map_or(0, ShipSeqno::get).to_le_bytes());
        v.extend_from_slice(&stamp.solo_history().commits_since_last_ship().to_le_bytes());
        v.extend_from_slice(
            &stamp
                .applied_through()
                .map_or(0, ShipSeqno::get)
                .to_le_bytes(),
        );
        v.extend_from_slice(&u64::from(stamp.solo_history().ran_solo()).to_le_bytes());
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

    /// Key for the last-orphan-sweep wall-clock timestamp (epoch seconds, a u64 via
    /// [`Self::encode_u64`]).
    pub fn last_orphan_sweep_key(&self) -> Bytes {
        let mut key = Vec::with_capacity(self.id_offset(KeyPrefix::System) + 1);
        self.push_prefix(&mut key, KeyPrefix::System);
        key.push(SYSTEM_ORPHAN_SWEEP_SUBTYPE);
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

    /// Encode a segment counter value: `live` bytes (decrements as frames die) and
    /// `total` bytes (cumulative frame bytes ever appended, monotonic). The GC reads
    /// `live/total` as the segment's live fraction straight from this value, so the
    /// fast reclaim path never has to list the object to get its size.
    pub fn encode_segcount(live: u64, total: u64) -> Bytes {
        let mut b = [0u8; U64_SIZE * 2];
        b[..U64_SIZE].copy_from_slice(&live.to_le_bytes());
        b[U64_SIZE..].copy_from_slice(&total.to_le_bytes());
        Bytes::copy_from_slice(&b)
    }

    /// Decode a segment counter value. A legacy 8-byte value (live only, pre-`total`)
    /// decodes as `(live, live)`: it treats the whole segment as live until its
    /// counter is next rewritten, which is the safe (over-count) direction.
    pub fn decode_segcount(data: &[u8]) -> Option<(u64, u64)> {
        if data.len() == U64_SIZE * 2 {
            let live = u64::from_le_bytes(data[..U64_SIZE].try_into().ok()?);
            let total = u64::from_le_bytes(data[U64_SIZE..].try_into().ok()?);
            Some((live, total))
        } else if data.len() == U64_SIZE {
            let live = u64::from_le_bytes(data[..U64_SIZE].try_into().ok()?);
            Some((live, live))
        } else {
            None
        }
    }

    /// Decode and validate a durable HA provenance stamp.
    ///
    /// The 16- and 24-byte layouts predate the applied frontier and Solo-history
    /// latch. They are durable data and can survive a coordinated binary upgrade,
    /// so decode them conservatively: the acknowledged ship is known applied, but
    /// the term cannot prove it never ran Solo and therefore cannot authorize
    /// promotion retry grace.
    pub(crate) fn decode_ha_stamp(data: &[u8]) -> Option<HaStamp> {
        if data.len() != U64_SIZE * 2 && data.len() != U64_SIZE * 3 && data.len() != U64_SIZE * 5 {
            return None;
        }
        let writer_epoch = WriterEpoch::new(u64::from_le_bytes(data[..U64_SIZE].try_into().ok()?))?;
        let last_shipped = ShipSeqno::new(u64::from_le_bytes(
            data[U64_SIZE..U64_SIZE * 2].try_into().ok()?,
        ));
        if data.len() == U64_SIZE * 2 {
            return HaStamp::new(
                writer_epoch,
                last_shipped,
                SoloHistory::ever(0),
                last_shipped,
            )
            .ok();
        }
        let solo = u64::from_le_bytes(data[U64_SIZE * 2..U64_SIZE * 3].try_into().ok()?);
        if data.len() == U64_SIZE * 3 {
            return HaStamp::new(
                writer_epoch,
                last_shipped,
                SoloHistory::ever(solo),
                last_shipped,
            )
            .ok();
        }
        let applied_through = ShipSeqno::new(u64::from_le_bytes(
            data[U64_SIZE * 3..U64_SIZE * 4].try_into().ok()?,
        ));
        let solo_ever = u64::from_le_bytes(data[U64_SIZE * 4..].try_into().ok()?);
        let ran_solo = match solo_ever {
            0 => false,
            1 => true,
            _ => return None,
        };
        let solo_history = SoloHistory::from_parts(solo, ran_solo)?;
        HaStamp::new(writer_epoch, last_shipped, solo_history, applied_through).ok()
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
    /// is too short, lacks the expected domain prefix, or carries a kind
    /// byte we don't recognize.
    fn peek_kind(&self, key: &[u8]) -> Option<KeyPrefix> {
        // Dispatch on the leading domain prefix to pick which kind byte to read.
        if let Some(rest) = key.strip_prefix(EXTENT_DOMAIN) {
            let kind = KeyPrefix::try_from(*rest.first()?).ok()?;
            return (kind == KeyPrefix::Extent).then_some(kind);
        }
        if let Some(rest) = key.strip_prefix(META_DOMAIN) {
            let kind = KeyPrefix::try_from(*rest.first()?).ok()?;
            return (kind != KeyPrefix::Extent).then_some(kind);
        }
        None
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
    /// `end` is the prefix bytes followed by the kind-byte successor, so the
    /// range stays within the domain segment.
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

    #[test]
    fn test_dir_scan_parsing() {
        let codec = KeyCodec::new();
        let dir_id = 10u64;
        let cookie = 42u64;
        let key = codec.dir_scan_key(dir_id, cookie);

        match codec.parse_key(&key) {
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
        let codec = KeyCodec::new();
        let timestamp = 123456u64;
        let inode_id = 789u64;
        let key = codec.tombstone_key(timestamp, inode_id);

        match codec.parse_key(&key) {
            ParsedKey::Tombstone {
                inode_id: parsed_id,
            } => {
                assert_eq!(parsed_id, inode_id);
            }
            _ => panic!("Failed to parse tombstone key"),
        }
    }

    #[test]
    fn test_extent_parsing() {
        let codec = KeyCodec::new();
        let inode_id = 7u64;
        let extent_index = 99u64;
        let key = codec.extent_key(inode_id, extent_index);
        assert_eq!(codec.parse_extent_key(&key), Some(extent_index));
    }

    #[test]
    fn test_layout_routing() {
        let codec = KeyCodec::new();
        let inode_key = codec.inode_key(0);
        assert!(inode_key.starts_with(META_DOMAIN));
        assert_eq!(inode_key[META_DOMAIN.len()], PREFIX_INODE);

        let extent_key = codec.extent_key(0, 0);
        assert!(extent_key.starts_with(EXTENT_DOMAIN));
        assert_eq!(extent_key[EXTENT_DOMAIN.len()], PREFIX_EXTENT);

        let tombstone = codec.tombstone_key(0, 0);
        assert!(tombstone.starts_with(META_DOMAIN));

        // No metadata key should be misrouted into the extent domain.
        assert!(!inode_key.starts_with(EXTENT_DOMAIN));
        assert!(!tombstone.starts_with(EXTENT_DOMAIN));
    }

    #[test]
    fn segcount_key_roundtrips_and_orders() {
        let codec = KeyCodec::new();
        for (e, c) in [(0u64, 0u64), (5, 0x23b), (7, 255), (u64::MAX, u64::MAX)] {
            let k = codec.segcount_key(e, c);
            assert_eq!(k.len(), META_DOMAIN.len() + 1 + 16);
            assert!(k.starts_with(META_DOMAIN));
            assert_eq!(k[META_DOMAIN.len()], PREFIX_SEGCOUNT);
            assert_eq!(codec.parse_segcount_key(&k), Some((e, c)));
        }
        // Big-endian id => creation-order scan.
        assert!(codec.segcount_key(5, 10).as_ref() < codec.segcount_key(5, 11).as_ref());
        assert!(codec.segcount_key(5, u64::MAX).as_ref() < codec.segcount_key(6, 0).as_ref());
        // The prefix range brackets every segcount key and excludes other kinds.
        let (start, end) = codec.segcount_prefix_range();
        let sc = codec.segcount_key(9, 9);
        assert!(sc.as_ref() >= start.as_ref() && sc.as_ref() < end.as_ref());
        let ino = codec.inode_key(9);
        assert!(!(ino.as_ref() >= start.as_ref() && ino.as_ref() < end.as_ref()));
        assert_eq!(codec.parse_segcount_key(&ino), None);
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
    fn test_ha_stamp_encoding() {
        let (epoch, seqno, solo, applied_through) = (7u64, 123456u64, 3u64, 123458u64);
        let stamp = HaStamp::new(
            WriterEpoch::new(epoch).unwrap(),
            ShipSeqno::new(seqno),
            SoloHistory::ever(solo),
            ShipSeqno::new(applied_through),
        )
        .unwrap();
        let encoded = KeyCodec::encode_ha_stamp(&stamp);
        assert_eq!(KeyCodec::decode_ha_stamp(&encoded), Some(stamp));
        // A transitional four-field layout never existed and remains invalid.
        assert_eq!(KeyCodec::decode_ha_stamp(&encoded[..32]), None);
        // Durable legacy layouts migrate conservatively: the shipped seqno is
        // known applied, but missing Solo history disables retry grace.
        let legacy_three_field = HaStamp::new(
            WriterEpoch::new(epoch).unwrap(),
            ShipSeqno::new(seqno),
            SoloHistory::ever(solo),
            ShipSeqno::new(seqno),
        )
        .unwrap();
        assert_eq!(
            KeyCodec::decode_ha_stamp(&encoded[..24]),
            Some(legacy_three_field)
        );
        let legacy_two_field = HaStamp::new(
            WriterEpoch::new(epoch).unwrap(),
            ShipSeqno::new(seqno),
            SoloHistory::ever(0),
            ShipSeqno::new(seqno),
        )
        .unwrap();
        assert_eq!(
            KeyCodec::decode_ha_stamp(&encoded[..16]),
            Some(legacy_two_field)
        );
        let mut legacy_solo_from_birth = Vec::with_capacity(U64_SIZE * 3);
        legacy_solo_from_birth.extend_from_slice(&epoch.to_le_bytes());
        legacy_solo_from_birth.extend_from_slice(&0u64.to_le_bytes());
        legacy_solo_from_birth.extend_from_slice(&2u64.to_le_bytes());
        let legacy_solo_from_birth_stamp = HaStamp::new(
            WriterEpoch::new(epoch).unwrap(),
            None,
            SoloHistory::ever(2),
            None,
        )
        .unwrap();
        assert_eq!(
            KeyCodec::decode_ha_stamp(&legacy_solo_from_birth),
            Some(legacy_solo_from_birth_stamp),
            "a pre-upgrade writer may have committed Solo before its first ship"
        );
        // Wrong length is rejected, not silently misread.
        assert_eq!(KeyCodec::decode_ha_stamp(&encoded[..8]), None);
        assert_eq!(KeyCodec::decode_ha_stamp(&[]), None);

        let mut zero_epoch = encoded.to_vec();
        zero_epoch[..8].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(
            KeyCodec::decode_ha_stamp(&zero_epoch),
            None,
            "zero is the absence sentinel, not a valid HA writer epoch"
        );

        let mut regressed = encoded.to_vec();
        regressed[24..32].copy_from_slice(&(seqno - 1).to_le_bytes());
        assert_eq!(
            KeyCodec::decode_ha_stamp(&regressed),
            None,
            "an applied frontier behind an acknowledged ship is invalid"
        );

        let mut invalid_solo_ever = encoded.to_vec();
        invalid_solo_ever[32..].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(
            KeyCodec::decode_ha_stamp(&invalid_solo_ever),
            None,
            "the durable Solo-history bit accepts only canonical boolean values"
        );

        let never_solo = HaStamp::new(
            WriterEpoch::new(epoch).unwrap(),
            ShipSeqno::new(seqno),
            SoloHistory::never(),
            ShipSeqno::new(seqno),
        )
        .unwrap();
        let inconsistent_solo_history = KeyCodec::encode_ha_stamp(&never_solo);
        let mut inconsistent_solo_history = inconsistent_solo_history.to_vec();
        inconsistent_solo_history[16..24].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(
            KeyCodec::decode_ha_stamp(&inconsistent_solo_history),
            None,
            "a positive current Solo count cannot claim the term never ran Solo"
        );

        // The HA-stamp key is distinct from the inode counter (both System-prefixed).
        let codec = KeyCodec::new();
        assert_ne!(codec.ha_seqno_key(), codec.system_counter_key());
    }

    #[test]
    fn last_orphan_sweep_key_is_distinct_system_key() {
        let codec = KeyCodec::new();
        let k = codec.last_orphan_sweep_key();
        assert!(k.starts_with(META_DOMAIN));
        assert_eq!(k[codec.kind_offset(KeyPrefix::System)], PREFIX_SYSTEM);
        // Must not alias any other System-subtyped key.
        for other in [
            codec.system_counter_key(),
            codec.ha_seqno_key(),
            codec.lineage_key(),
            codec.taint_key(),
        ] {
            assert_ne!(k, other);
        }
    }

    #[test]
    fn test_invalid_key_parsing() {
        let codec = KeyCodec::new();
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

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        // Big-endian id encoding is the reason lexicographic key order matches
        // numeric order; every range scan (extent reads, dir listing, GC) leans on
        // it. The prose comments assert this layout; nothing tested it until now.
        #[test]
        fn extent_key_roundtrips_and_orders(
            a in (any::<u64>(), any::<u64>()),
            b in (any::<u64>(), any::<u64>()),
        ) {
            let codec = KeyCodec::new();
            let ka = codec.extent_key(a.0, a.1);
            let kb = codec.extent_key(b.0, b.1);
            prop_assert_eq!(codec.parse_extent_key(&ka), Some(a.1));
            prop_assert_eq!(codec.parse_extent_key(&kb), Some(b.1));
            prop_assert_eq!(ka.as_ref().cmp(kb.as_ref()), a.cmp(&b));
        }

        #[test]
        fn tombstone_key_roundtrips_and_orders(
            a in (any::<u64>(), any::<u64>()),
            b in (any::<u64>(), any::<u64>()),
        ) {
            let codec = KeyCodec::new();
            let ka = codec.tombstone_key(a.0, a.1);
            let kb = codec.tombstone_key(b.0, b.1);
            match codec.parse_key(&ka) {
                ParsedKey::Tombstone { inode_id } => prop_assert_eq!(inode_id, a.1),
                other => prop_assert!(false, "expected Tombstone, got {:?}", other),
            }
            // Ordered by (timestamp, inode_id): GC scans tombstones in time order.
            prop_assert_eq!(ka.as_ref().cmp(kb.as_ref()), a.cmp(&b));
        }

        #[test]
        fn dir_scan_key_roundtrips_and_orders(
            a in (any::<u64>(), any::<u64>()),
            b in (any::<u64>(), any::<u64>()),
        ) {
            let codec = KeyCodec::new();
            let ka = codec.dir_scan_key(a.0, a.1);
            let kb = codec.dir_scan_key(b.0, b.1);
            match codec.parse_key(&ka) {
                ParsedKey::DirScan { cookie } => prop_assert_eq!(cookie, a.1),
                other => prop_assert!(false, "expected DirScan, got {:?}", other),
            }
            prop_assert_eq!(ka.as_ref().cmp(kb.as_ref()), a.cmp(&b));
        }

        // A resume key must land exactly on the next cookie, so a paged scan
        // continues strictly after `cookie` with no skipped or repeated entry.
        #[test]
        fn dir_scan_resume_is_next_cookie(dir in any::<u64>(), cookie in 0u64..u64::MAX) {
            let codec = KeyCodec::new();
            let resume = codec.dir_scan_resume_key(dir, cookie);
            prop_assert_eq!(&resume, &codec.dir_scan_key(dir, cookie + 1));
            prop_assert!(resume.as_ref() > codec.dir_scan_key(dir, cookie).as_ref());
        }

        #[test]
        fn orphan_key_roundtrips(ino in any::<u64>()) {
            let codec = KeyCodec::new();
            match codec.parse_key(&codec.orphan_key(ino)) {
                ParsedKey::Orphan { inode_id } => prop_assert_eq!(inode_id, ino),
                other => prop_assert!(false, "expected Orphan, got {:?}", other),
            }
        }

        // parse_extent_key must reject every non-extent key, including any that
        // happens to match the extent key's byte length (only the domain/kind
        // bytes distinguish them).
        #[test]
        fn non_extent_keys_never_parse_as_extent(
            ino in any::<u64>(),
            x in any::<u64>(),
            name in prop::collection::vec(any::<u8>(), 0..40),
        ) {
            let codec = KeyCodec::new();
            prop_assert_eq!(codec.parse_extent_key(&codec.inode_key(ino)), None);
            prop_assert_eq!(codec.parse_extent_key(&codec.tombstone_key(x, ino)), None);
            prop_assert_eq!(codec.parse_extent_key(&codec.orphan_key(ino)), None);
            prop_assert_eq!(codec.parse_extent_key(&codec.dir_scan_key(ino, x)), None);
            prop_assert_eq!(codec.parse_extent_key(&codec.dir_entry_key(ino, &name)), None);
        }

        #[test]
        fn value_codecs_roundtrip(x in 1u64..=u64::MAX, y in any::<u64>()) {
            prop_assert_eq!(KeyCodec::decode_u64(&KeyCodec::encode_u64(x)), Some(x));
            prop_assert_eq!(KeyCodec::decode_counter(&KeyCodec::encode_counter(x)).unwrap(), x);
            prop_assert_eq!(
                KeyCodec::decode_tombstone_size(&KeyCodec::encode_tombstone_size(x)).unwrap(),
                x
            );
            let last_shipped = ShipSeqno::new(y);
            let stamp = HaStamp::new(
                WriterEpoch::new(x).unwrap(),
                last_shipped,
                SoloHistory::ever(x),
                last_shipped,
            ).unwrap();
            prop_assert_eq!(
                KeyCodec::decode_ha_stamp(&KeyCodec::encode_ha_stamp(&stamp)),
                Some(stamp)
            );
            prop_assert_eq!(
                KeyCodec::decode_dir_entry(&KeyCodec::encode_dir_entry(x, y)).unwrap(),
                (x, y)
            );
        }

        // The half-open prefix range must contain every extent key and no metadata
        // key, so an extent-domain scan can never read (or GC) metadata.
        #[test]
        fn prefix_range_isolates_extents(ino in any::<u64>(), idx in any::<u64>(), x in any::<u64>()) {
            let codec = KeyCodec::new();
            let (start, end) = codec.prefix_range(KeyPrefix::Extent);
            let extent = codec.extent_key(ino, idx);
            prop_assert!(extent.as_ref() >= start.as_ref() && extent.as_ref() < end.as_ref());

            let meta = codec.tombstone_key(x, ino);
            prop_assert!(
                !(meta.as_ref() >= start.as_ref() && meta.as_ref() < end.as_ref()),
                "a metadata key leaked into the extent prefix range"
            );
        }
    }
}
