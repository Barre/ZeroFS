use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

/// Compression algorithm configuration for extent data.
/// Supports lz4 and zstd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionConfig {
    /// LZ4 compression: fast with moderate compression ratio
    Lz4,
    /// Zstd compression with configurable level (1-22)
    /// Level 1 is fastest, level 22 is maximum compression
    Zstd(i32),
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig::Zstd(3)
    }
}

impl Serialize for CompressionConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionConfig::Lz4 => serializer.serialize_str("lz4"),
            CompressionConfig::Zstd(level) => serializer.serialize_str(&format!("zstd-{}", level)),
        }
    }
}

impl<'de> Deserialize<'de> for CompressionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CompressionConfigVisitor;

        impl de::Visitor<'_> for CompressionConfigVisitor {
            type Value = CompressionConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("'zstd-{level}' where level is 1-22, or 'lz4'")
            }

            fn visit_str<E>(self, value: &str) -> Result<CompressionConfig, E>
            where
                E: de::Error,
            {
                if value == "lz4" {
                    return Ok(CompressionConfig::Lz4);
                }

                if let Some(level_str) = value.strip_prefix("zstd-") {
                    let level: i32 = level_str.parse().map_err(|_| {
                        de::Error::invalid_value(
                            de::Unexpected::Str(value),
                            &"'zstd-{level}' where level is a number 1-22",
                        )
                    })?;

                    if !(1..=22).contains(&level) {
                        return Err(de::Error::invalid_value(
                            de::Unexpected::Signed(level as i64),
                            &"zstd level must be between 1 and 22",
                        ));
                    }

                    return Ok(CompressionConfig::Zstd(level));
                }

                Err(de::Error::invalid_value(
                    de::Unexpected::Str(value),
                    &"'zstd-{level}' where level is 1-22, or 'lz4'",
                ))
            }
        }

        deserializer.deserialize_str(CompressionConfigVisitor)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    /// Object storage class/tier for WAL writes.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_string"
    )]
    pub storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcsConfig>,
}

impl WalConfig {
    pub fn cloud_provider_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = Vec::new();
        if let Some(aws) = &self.aws {
            for (k, v) in &aws.0 {
                env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(azure) = &self.azure {
            for (k, v) in &azure.0 {
                env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(gcp) = &self.gcp {
            for (k, v) in &gcp.0 {
                env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
            }
        }
        env_vars
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub cache: CacheConfig,
    pub storage: StorageConfig,
    pub servers: ServerConfig,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub filesystem: Option<FilesystemConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub lsm: Option<LsmConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub gc: Option<GcConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcsConfig>,
    /// Location of a pre-2.0 volume's separate WAL store.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub wal: Option<WalConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "default_telemetry")]
    pub telemetry: Option<TelemetryConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub prometheus: Option<PrometheusConfig>,
    /// HA replication. Absent means single-node (non-replicated behavior).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub replication: Option<ReplicationConfig>,
}

/// Node role within an HA pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// Bootstraps as the active leader, opening the data db as writer.
    Leader,
    /// Watches the leader's heartbeats; takes over (opens the data db as writer)
    /// when they stop.
    Standby,
}

impl Serialize for ReplicationRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            ReplicationRole::Leader => "leader",
            ReplicationRole::Standby => "standby",
        })
    }
}

impl<'de> Deserialize<'de> for ReplicationRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "leader" => Ok(ReplicationRole::Leader),
            "standby" => Ok(ReplicationRole::Standby),
            other => Err(de::Error::invalid_value(
                de::Unexpected::Str(other),
                &"\"leader\" or \"standby\"",
            )),
        }
    }
}

/// HA replication: one node of a leader/standby pair over a single object-store
/// endpoint. Leadership is the data db's writer epoch; the standby takes over
/// when heartbeats stop. Read-only / checkpoint modes are rejected (a node must
/// open the data db as a writer).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    /// This node's stable identity within the pair.
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub node_id: String,
    /// Role: "leader" or "standby".
    pub role: ReplicationRole,
    /// Peer address(es). On a leader, the standby's `replication_listen`.
    #[serde(default, deserialize_with = "deserialize_expandable_string_vec")]
    pub peers: Vec<String>,
    /// Address this node listens on for the replication stream (a standby
    /// receives the leader's ships here).
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_string",
        default
    )]
    pub replication_listen: Option<String>,
}

impl ReplicationConfig {
    pub fn validate(&self) -> Result<()> {
        if self.node_id.trim().is_empty() {
            anyhow::bail!("[replication] node_id must not be empty");
        }

        // A standby must be reachable to receive the leader's ships and to watch
        // its heartbeats; with no listen it can do neither (it would fail at startup
        // with "standby has no replication_listen; cannot watch heartbeats").
        if self.role == ReplicationRole::Standby && self.replication_listen.is_none() {
            anyhow::bail!(
                "[replication] role = \"standby\" requires replication_listen (the address it \
                 receives the leader's ships on)"
            );
        }

        // The receiver binds replication_listen verbatim, so it must be a socket
        // address (host:port); catch a typo here rather than at bind time.
        if let Some(listen) = &self.replication_listen {
            listen.parse::<SocketAddr>().with_context(|| {
                format!(
                    "[replication] replication_listen {listen:?} is not a valid socket address \
                     (expected host:port)"
                )
            })?;
        }

        for peer in &self.peers {
            if peer.trim().is_empty() {
                anyhow::bail!("[replication] peers must not contain an empty entry");
            }
            // A node replicating to its own listen address is always a mistake.
            if let Some(listen) = &self.replication_listen {
                let bare = peer
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                if bare == listen {
                    anyhow::bail!(
                        "[replication] peer {peer:?} is this node's own replication_listen; a \
                         node must not replicate to itself"
                    );
                }
            }
        }

        // Asymmetric HA (only one of peers/listen set) works for the initial roles
        // but not a role swap: dynamic election and a promoted standby both need
        // peers AND a listen. Warn rather than fail: a deliberately one-directional
        // setup is still usable, and a solo node (neither set) is fine.
        let (has_peers, has_listen) = (!self.peers.is_empty(), self.replication_listen.is_some());
        if has_peers != has_listen {
            let detail = if has_peers {
                "has peers but no replication_listen, so it cannot receive ships if demoted to a follower"
            } else {
                "has replication_listen but no peers, so if promoted to leader it has nowhere to ship"
            };
            tracing::warn!(
                "[replication] node {:?} {detail}; HA will be degraded across a role swap \
                 (configure both peers and replication_listen for a balanced pair)",
                self.node_id
            );
        }

        Ok(())
    }
}

/// What slatedb block-cache content to warm for the metadata segment at startup.
///
/// Every filesystem op is a point lookup gated by per-SST bloom filters and the
/// SST index, so warming those removes the cold-cache latency cliff a fresh node
/// or restart otherwise pays on its first reads. The bulk extent segment is never
/// warmed.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum WarmMetadata {
    /// Don't warm anything on startup.
    Off,
    /// Warm the metadata SST filters and indexes only (small, bounded). Default.
    #[default]
    FiltersIndex,
    /// Also warm the metadata data blocks (larger; for metadata-heavy workloads
    /// whose working set fits the cache).
    Full,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub dir: PathBuf,
    pub disk_size_gb: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_size_gb: Option<f64>,
    #[serde(default)]
    pub warm_metadata: WarmMetadata,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub encryption_password: String,
    /// Object storage class/tier for data writes, passed through verbatim as the
    /// per-backend tiering header.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_string"
    )]
    pub storage_class: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FilesystemConfig {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_size_gb: Option<f64>,
    /// Compression algorithm for extent data: "zstd-{level}" (default: "zstd-3", level 1-22) or "lz4"
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Treat `fsync` as a no-op: a client fsync/COMMIT returns without forcing a
    /// flush to object storage. Intended for HA, where semi-sync replication
    /// already holds the write on the standby, making the per-fsync flush
    /// redundant. Trades object-store durability for latency: un-flushed writes are
    /// lost if both nodes (or a standalone node) die before a background flush.
    #[serde(default)]
    pub ignore_fsync: bool,
}

impl FilesystemConfig {
    pub fn max_bytes(&self) -> u64 {
        self.max_size_gb
            .filter(|&gb| gb.is_finite() && gb > 0.0)
            .map(|gb| (gb * 1_000_000_000.0) as u64)
            .unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub struct LsmConfig {
    /// Maximum number of SST files in level 0 before triggering compaction
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub l0_max_ssts: Option<usize>,
    /// Maximum number of concurrent compactions
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_concurrent_compactions: Option<usize>,
    /// Interval in seconds between periodic flushes
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flush_interval_secs: Option<u64>,
    /// When true, every committed write is durably flushed to object storage
    /// before returning success. Trades per-op latency for zero unflushed data
    /// in case of a crash. Expensive: the WAL is off, so each write forces a
    /// full seal + memtable flush.
    ///
    /// This does NOT change POSIX semantics: with `sync_writes = false` (the
    /// default), explicit fsync from clients is still honored and
    /// waits for durable persistence. The flag only changes what happens to
    /// writes between fsync calls, making them durable on return rather than
    /// buffered until the next flush.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sync_writes: Option<bool>,
    /// Deprecated, ignored: the WAL is permanently off (sealing correctness
    /// requires it). Accepted so pre-2.0 configs still parse; never re-emitted.
    #[serde(default, skip_serializing)]
    pub wal_enabled: Option<bool>,
    /// Deprecated, ignored: unflushed-data budgeting went away with the WAL.
    /// Accepted so pre-2.0 configs still parse; never re-emitted.
    #[serde(default, skip_serializing)]
    pub max_unflushed_gb: Option<f64>,
}

impl LsmConfig {
    /// Default l0_max_ssts: 256. With the WAL off every `db.flush()` (periodic
    /// and each fsync) freezes a fresh L0 SST, so L0 must hold a deep backlog
    /// or flushes stall on compaction. The SSTs are small, bloom-filtered
    /// metadata, so the point-lookup cost is negligible. Applies to
    /// `l0_max_ssts_per_key` too.
    pub const DEFAULT_L0_MAX_SSTS: usize = 256;
    /// Default max_concurrent_compactions
    pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: usize = 2;
    /// Default flush_interval_sec
    pub const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 30;

    /// Minimum l0_max_ssts to maintain reasonable performance
    pub const MIN_L0_MAX_SSTS: usize = 4;
    /// Minimum max_concurrent_compactions: 1
    pub const MIN_MAX_CONCURRENT_COMPACTIONS: usize = 1;
    /// Minimum flush_interval_secs: 5 seconds
    pub const MIN_FLUSH_INTERVAL_SECS: u64 = 5;

    pub fn l0_max_ssts(&self) -> usize {
        self.l0_max_ssts
            .unwrap_or(Self::DEFAULT_L0_MAX_SSTS)
            .max(Self::MIN_L0_MAX_SSTS)
    }

    pub fn max_concurrent_compactions(&self) -> usize {
        self.max_concurrent_compactions
            .unwrap_or(Self::DEFAULT_MAX_CONCURRENT_COMPACTIONS)
            .max(Self::MIN_MAX_CONCURRENT_COMPACTIONS)
    }

    pub fn flush_interval_secs(&self) -> u64 {
        self.flush_interval_secs
            .unwrap_or(Self::DEFAULT_FLUSH_INTERVAL_SECS)
            .max(Self::MIN_FLUSH_INTERVAL_SECS)
    }

    pub fn sync_writes(&self) -> bool {
        self.sync_writes.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    /// Seconds between segment-GC passes while the filesystem is active. The
    /// ceiling of the adaptive cadence: the loop never sleeps longer. Values
    /// below the ~30 s flush cadence make a busy pass's barrier seal real
    /// sub-1-MiB segments — each itself future GC work.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub interval_secs: Option<u64>,
    /// Seconds between passes while a saturated backlog meets an idle store
    /// (no reads, no writes since the previous pass). A fast pass performs a
    /// full reclamation round plus roughly two small bookkeeping PUTs of
    /// fixed overhead (the previous pass's own commits flushing); total
    /// fast-mode work is bounded by the backlog. Setting it equal to
    /// interval_secs disables adaptation.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub idle_interval_secs: Option<u64>,
    /// Whether reads steer compaction (nominations, seam heat, chain repacks).
    /// The counter-driven policy and the tail scrub are unaffected.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub read_directed: Option<bool>,
    /// Tail-scrub floor: a write-cold segment more than this percent dead —
    /// but not dead enough for normal compaction candidacy — is repacked with
    /// leftover pass budget. The space-amplification dial: worst-case overhead
    /// on write-cold data is 1/(1 - floor/100) of live bytes, bought at up to
    /// (100 - floor)/floor bytes rewritten per byte reclaimed. 0 disables the
    /// scrub; 50 empties the band, same effect.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tail_scrub_min_dead_percent: Option<u64>,
}

impl GcConfig {
    /// Default interval_secs: 60 seconds, the historical fixed cadence.
    pub const DEFAULT_INTERVAL_SECS: u64 = 60;
    /// Default idle_interval_secs: 5 seconds (~12x drain while idle).
    pub const DEFAULT_IDLE_INTERVAL_SECS: u64 = 5;
    /// Default read_directed: true.
    pub const DEFAULT_READ_DIRECTED: bool = true;
    /// Default tail_scrub_min_dead_percent: 5 (1.053x worst-case space
    /// amplification at up to 19x rewrite per reclaimed byte; the request-cost
    /// break-even on S3 is near 1.5%, so 5 is the write-amplification choice).
    pub const DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT: u64 = 5;

    /// Minimum interval_secs: each pass runs a flush barrier, so the LSM
    /// flush floor applies.
    pub const MIN_INTERVAL_SECS: u64 = LsmConfig::MIN_FLUSH_INTERVAL_SECS;
    /// Minimum idle_interval_secs: below ~1 s the pass's barrier round-trips
    /// stop amortizing.
    pub const MIN_IDLE_INTERVAL_SECS: u64 = 1;

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
            .unwrap_or(Self::DEFAULT_INTERVAL_SECS)
            .max(Self::MIN_INTERVAL_SECS)
    }

    pub fn idle_interval_secs(&self) -> u64 {
        self.idle_interval_secs
            .unwrap_or(Self::DEFAULT_IDLE_INTERVAL_SECS)
            .max(Self::MIN_IDLE_INTERVAL_SECS)
            .min(self.interval_secs())
    }

    pub fn read_directed(&self) -> bool {
        self.read_directed.unwrap_or(Self::DEFAULT_READ_DIRECTED)
    }

    /// `None` = scrub disabled (configured 0).
    pub fn tail_scrub_min_dead_percent(&self) -> Option<u64> {
        match self.tail_scrub_min_dead_percent {
            Some(0) => None,
            v => Some(
                v.unwrap_or(Self::DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT)
                    .clamp(1, 50),
            ),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NfsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ninep: Option<NinePConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbd: Option<NbdConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RpcConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webui: Option<WebUIConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WebUIConfig {
    #[serde(
        default = "default_webui_addresses",
        deserialize_with = "deserialize_expandable_socket_addrs"
    )]
    pub addresses: HashSet<SocketAddr>,
    pub uid: u32,
    pub gid: u32,
}

fn default_webui_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    ));
    set
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NfsConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NinePConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

fn default_telemetry() -> Option<TelemetryConfig> {
    Some(TelemetryConfig { enabled: true })
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    #[serde(
        default = "default_prometheus_addresses",
        deserialize_with = "deserialize_expandable_socket_addrs"
    )]
    pub addresses: HashSet<SocketAddr>,
}

#[derive(Debug, Serialize, Clone)]
pub struct AwsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AwsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AwsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct AzureConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AzureConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AzureConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct GcsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for GcsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(GcsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

fn default_nfs_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        2049,
    ));
    set
}

fn default_9p_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        5564,
    ));
    set
}

fn default_nbd_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        10809,
    ));
    set
}

fn default_prometheus_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        9091,
    ));
    set
}

fn default_rpc_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        7000,
    ));
    set
}

fn deserialize_expandable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_string_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let items = Vec::<String>::deserialize(deserializer)?;
    items
        .into_iter()
        .map(|s| match shellexpand::env(&s) {
            Ok(expanded) => Ok(expanded.into_owned()),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

/// Expand `${VAR}` in each entry, then parse to a `SocketAddr`. Bind addresses
/// deserialize straight to `SocketAddr`, so without this a templated value like
/// `"${POD_IP}:2049"` would fail to parse before it could be expanded.
fn expand_socket_addrs<E>(raw: Vec<String>) -> Result<HashSet<SocketAddr>, E>
where
    E: de::Error,
{
    let mut set = HashSet::with_capacity(raw.len());
    for s in raw {
        let expanded = shellexpand::env(&s).map_err(|e| {
            de::Error::custom(format!("Failed to expand environment variable: {}", e))
        })?;
        let addr = expanded.parse::<SocketAddr>().map_err(|e| {
            de::Error::custom(format!(
                "invalid socket address {expanded:?} (expected host:port): {e}"
            ))
        })?;
        set.insert(addr);
    }
    Ok(set)
}

fn deserialize_expandable_socket_addrs<'de, D>(
    deserializer: D,
) -> Result<HashSet<SocketAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    expand_socket_addrs(Vec::<String>::deserialize(deserializer)?)
}

fn deserialize_optional_expandable_socket_addrs<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<SocketAddr>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer)?
        .map(expand_socket_addrs)
        .transpose()
}

fn deserialize_expandable_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_path<'de, D>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_hashmap<'de, D>(
    deserializer: D,
) -> Result<std::collections::HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
    map.into_iter()
        .map(|(k, v)| match shellexpand::env(&v) {
            Ok(expanded) => Ok((k, expanded.into_owned())),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

impl Settings {
    pub fn max_bytes(&self) -> u64 {
        self.filesystem
            .as_ref()
            .map(|fs| fs.max_bytes())
            .unwrap_or(u64::MAX)
    }

    pub fn compression(&self) -> CompressionConfig {
        self.filesystem
            .as_ref()
            .map(|fs| fs.compression)
            .unwrap_or_default()
    }

    pub fn from_file(config_path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = config_path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let settings: Settings = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        settings.validate()?;

        Ok(settings)
    }

    /// Cross-section validation applied after deserialization.
    pub fn validate(&self) -> Result<()> {
        if let Some(replication) = &self.replication {
            replication
                .validate()
                .context("Invalid [replication] configuration")?;
        }

        if let Some(fs) = &self.filesystem
            && fs.ignore_fsync
            && self.lsm.as_ref().map(|l| l.sync_writes()).unwrap_or(false)
        {
            anyhow::bail!(
                "[filesystem] ignore_fsync and [lsm] sync_writes are contradictory: \
                 sync_writes flushes after every write, ignore_fsync skips the fsync flush"
            );
        }

        // Deprecated [lsm] keys still parse (an upgrade must not brick startup
        // on an old config) but no longer do anything; nudge the operator to
        // drop them.
        if let Some(lsm) = &self.lsm {
            if lsm.wal_enabled.is_some() {
                tracing::warn!(
                    "[lsm] wal_enabled is deprecated and ignored (the WAL is permanently off); \
                     remove it from the config"
                );
            }
            if lsm.max_unflushed_gb.is_some() {
                tracing::warn!(
                    "[lsm] max_unflushed_gb is deprecated and ignored (it tuned the removed WAL); \
                     remove it from the config"
                );
            }
        }
        Ok(())
    }

    pub fn cloud_provider_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = Vec::new();
        if let Some(aws) = &self.aws {
            for (k, v) in &aws.0 {
                env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(azure) = &self.azure {
            for (k, v) in &azure.0 {
                env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(gcp) = &self.gcp {
            for (k, v) in &gcp.0 {
                env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
            }
        }
        env_vars
    }

    pub fn generate_default() -> Self {
        let mut aws_config = std::collections::HashMap::new();
        aws_config.insert(
            "access_key_id".to_string(),
            "${AWS_ACCESS_KEY_ID}".to_string(),
        );
        aws_config.insert(
            "secret_access_key".to_string(),
            "${AWS_SECRET_ACCESS_KEY}".to_string(),
        );

        Settings {
            cache: CacheConfig {
                dir: PathBuf::from("${HOME}/.cache/zerofs"),
                disk_size_gb: 10.0,
                memory_size_gb: Some(1.0),
                warm_metadata: WarmMetadata::default(),
            },
            storage: StorageConfig {
                url: "s3://your-bucket/zerofs-data".to_string(),
                encryption_password: "${ZEROFS_PASSWORD}".to_string(),
                storage_class: None,
            },
            servers: ServerConfig {
                nfs: Some(NfsConfig {
                    addresses: Some(default_nfs_addresses()),
                }),
                ninep: Some(NinePConfig {
                    addresses: Some(default_9p_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.9p.sock")),
                }),
                nbd: Some(NbdConfig {
                    addresses: Some(default_nbd_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.nbd.sock")),
                }),
                rpc: Some(RpcConfig {
                    addresses: Some(default_rpc_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.rpc.sock")),
                }),
                webui: Some(WebUIConfig {
                    addresses: default_webui_addresses(),
                    uid: 1000,
                    gid: 1000,
                }),
            },
            filesystem: None,
            lsm: None,
            gc: None,
            aws: Some(AwsConfig(aws_config)),
            azure: None,
            gcp: None,
            wal: None,
            telemetry: None,
            prometheus: None,
            replication: None,
        }
    }

    pub fn render_default_config() -> Result<String> {
        let default = Self::generate_default();
        let mut toml_string = toml::to_string_pretty(&default)?;

        // Inject a commented storage_class hint into the [storage] section. It
        // can't be appended like the others below because [storage] is not the
        // last table in the serialized output.
        toml_string = toml_string.replace(
            "encryption_password = \"${ZEROFS_PASSWORD}\"\n",
            "encryption_password = \"${ZEROFS_PASSWORD}\"\n\
             # storage_class = \"...\"   # Optional object storage class/tier for all writes (provider-specific value).\n"
        );

        // Document warm_metadata in place (the [cache] table is not last, so the
        // hint can't be appended like the sections below).
        toml_string = toml_string.replace(
            "warm_metadata = \"filters_index\"\n",
            "# Keep the metadata block cache warm so reads don't pay cold object-store latency\n\
             # on startup and right after each compaction. Values:\n\
             #   \"filters_index\" (default)  warm metadata SST filters + indexes (small, bounded)\n\
             #   \"full\"                      also warm the metadata data blocks (metadata-heavy working sets)\n\
             #   \"off\"                       disable warming\n\
             warm_metadata = \"filters_index\"\n",
        );

        toml_string.push_str("\n# Optional AWS S3 settings (uncomment to use):\n");
        toml_string.push_str(
            "# endpoint = \"https://s3.us-east-1.amazonaws.com\"  # For S3-compatible services\n",
        );
        toml_string.push_str("# default_region = \"us-east-1\"\n");
        toml_string.push_str("# allow_http = \"true\"  # For non-HTTPS endpoints\n");
        toml_string.push_str("# conditional_put = \"redis://localhost:6379\"  # For S3-compatible stores without conditional put support\n");

        toml_string.push_str("\n# Optional filesystem configuration\n");
        toml_string
            .push_str("# Limit the maximum size of the filesystem to prevent unlimited growth\n");
        toml_string
            .push_str("# If not specified, defaults to 16 EiB, the maximum filesystem size\n");
        toml_string.push_str("#\n");
        toml_string.push_str("# Compression algorithm for extent data:\n");
        toml_string.push_str(
            "#   - \"zstd-{level}\" (default: \"zstd-3\"): Configurable compression (level 1-22)\n",
        );
        toml_string.push_str("#     Level 1 is fastest, level 22 is maximum compression\n");
        toml_string.push_str("#   - \"lz4\": Faster compression, lower ratio (prefer for write-throughput-bound workloads)\n");
        toml_string.push_str("#\n");
        toml_string
            .push_str("# Note: Compression can be changed at any time. Existing data remains\n");
        toml_string
            .push_str("# readable regardless of compression setting (auto-detected on read).\n");
        toml_string.push_str("\n# [filesystem]\n");
        toml_string.push_str("# max_size_gb = 100.0     # Limit filesystem to 100 GB\n");
        toml_string.push_str("# compression = \"zstd-3\"  # or \"lz4\", \"zstd-19\", etc.\n");
        toml_string.push_str("# ignore_fsync = false    # HA: make fsync a no-op, relying on the standby for durability (see [replication])\n");

        toml_string.push_str("\n# Optional LSM tree tuning parameters\n");
        toml_string
            .push_str("# Advanced performance tuning for the underlying LSM tree storage engine\n");
        toml_string.push_str("# Only modify these if you understand LSM tree behavior\n");
        toml_string.push_str("\n# [lsm]\n");
        toml_string.push_str("# l0_max_ssts = 256                # Max SST files in L0 before compaction (default: 256, min: 4)\n");
        toml_string.push_str("# max_concurrent_compactions = 2   # Max concurrent compaction operations (default: 2, min: 1)\n");
        toml_string.push_str("# flush_interval_secs = 30         # Interval between periodic flushes in seconds (default: 30, min: 5)\n");
        toml_string.push_str("# sync_writes = false              # Flush every write to object storage before returning success (default: false).\n");
        toml_string.push_str("                                   # Does NOT affect POSIX fsync semantics: explicit fsync from clients\n");
        toml_string.push_str("                                   # is always honored. This flag only governs writes between fsync calls. When on,\n");
        toml_string.push_str("                                   # they become durable on return instead of buffered until the next periodic flush.\n");
        toml_string.push_str("                                   # Expensive: the WAL is off, so each write forces a full seal + memtable flush.\n");

        toml_string
            .push_str("\n# Optional segment garbage-collection tuning. Governs the segment\n");
        toml_string.push_str("# reclamation loop.\n");
        toml_string.push_str("\n# [gc]\n");
        toml_string.push_str("# interval_secs = 60               # Pass interval while the store is active (default: 60, min: 5).\n");
        toml_string.push_str("#                                  # Below the ~30 s flush cadence, busy passes seal sub-1-MiB segments.\n");
        toml_string.push_str("# idle_interval_secs = 5           # Pass interval while a saturated backlog meets an idle store\n");
        toml_string.push_str("#                                  # (default: 5, min: 1, capped at interval_secs; equal = adaptation off).\n");
        toml_string.push_str("#                                  # A fast pass runs a full reclamation round plus ~2 small PUTs of\n");
        toml_string.push_str("#                                  # overhead; total fast-mode work is bounded by the backlog.\n");
        toml_string.push_str("# read_directed = true             # Reads steer compaction: nominations, seam heat, chain repacks\n");
        toml_string.push_str("#                                  # (default: true). Counter-driven reclamation is unaffected.\n");
        toml_string.push_str("# tail_scrub_min_dead_percent = 5  # Repack write-cold segments more than this % dead that normal\n");
        toml_string.push_str("#                                  # candidacy would strand forever (default: 5, range 1-50; 0 disables\n");
        toml_string.push_str("#                                  # the scrub).\n");
        toml_string.push_str("#                                  # Space overhead on write-cold data is capped at 1/(1 - floor/100)\n");
        toml_string.push_str("#                                  # of live bytes, paid with up to (100-floor)/floor bytes rewritten\n");
        toml_string.push_str("#                                  # per byte reclaimed, using leftover pass budget only.\n");

        toml_string.push_str(
            "\n# Optional HA replication: a leader + standby pair over one object store, with\n",
        );
        toml_string
            .push_str("# automatic failover. Each node has its OWN config (node_id, role,\n");
        toml_string.push_str(
            "# replication_listen, and peers differ per node). See the High Availability docs.\n",
        );
        toml_string.push_str("\n# [replication]\n");
        toml_string.push_str("# node_id = \"node-a\"\n");
        toml_string.push_str("# role = \"leader\"                       # or \"standby\"\n");
        toml_string.push_str("# replication_listen = \"10.0.0.1:9000\"  # this node receives ships + heartbeats here\n");
        toml_string.push_str(
            "# peers = [\"10.0.0.2:9000\"]             # the other node's replication_listen\n",
        );

        toml_string.push_str("\n# Optional Prometheus metrics endpoint\n");
        toml_string.push_str("# Exposes filesystem, LSM, and cache metrics in Prometheus format\n");
        toml_string.push_str("\n# [prometheus]\n");
        toml_string.push_str("# addresses = [\"127.0.0.1:9091\"]\n");

        toml_string.push_str("\n# Optional Azure settings can be added to [azure] section\n");

        // Add commented-out Azure section
        toml_string.push_str("\n# [azure]\n");
        toml_string.push_str("# storage_account_name = \"${AZURE_STORAGE_ACCOUNT_NAME}\"\n");
        toml_string.push_str("# storage_account_key = \"${AZURE_STORAGE_ACCOUNT_KEY}\"\n");

        toml_string.push_str("\n# Optional GCS (Google Cloud Storage) settings\n");
        toml_string.push_str("# Use gs:// URLs with the [gcp] section\n");

        // Add commented-out GCS section
        toml_string.push_str("\n# [gcp]\n");
        toml_string.push_str(
            "# service_account = \"${GCS_SERVICE_ACCOUNT}\"  # Path to service account JSON file\n",
        );
        toml_string
            .push_str("# Or use application_credentials = \"${GOOGLE_APPLICATION_CREDENTIALS}\"\n");

        toml_string.push_str("\n# Anonymous telemetry (enabled by default)\n");
        toml_string.push_str(
            "# Shares anonymous usage data (version, OS, backend type, filesystem size) to help improve ZeroFS\n",
        );
        toml_string.push_str(
            "# No file contents, paths, or personally identifiable information is collected\n",
        );
        toml_string.push_str("\n# [telemetry]\n");
        toml_string.push_str("# enabled = false  # Set to false to disable\n");

        let commented = format!(
            "# ZeroFS Configuration File\n\
             # Generated by ZeroFS v{}\n\
             #\n\
             # ============================================================================\n\
             # ENVIRONMENT VARIABLE SUBSTITUTION\n\
             # ============================================================================\n\
             # This config file supports environment variable substitution.\n\
             # \n\
             # Supported syntax:\n\
             #   - ${{VAR}} or $VAR  : Environment variable substitution\n\
             # \n\
             # Examples:\n\
             #   encryption_password = \"${{ZEROFS_PASSWORD}}\"\n\
             #   dir = \"${{HOME}}/.cache/zerofs\"\n\
             #   access_key_id = \"${{AWS_ACCESS_KEY_ID}}\"\n\
             #   peers = [\"${{PEER_A_ADDR}}\", \"${{PEER_B_ADDR}}\"]\n\
             # \n\
             # In array values (e.g. [replication] peers) each entry is expanded on\n\
             # its own; a single variable is not split into multiple entries.\n\
             #\n\
             # All referenced environment variables must be set, or the config will fail to load.\n\
             #\n\
             # ============================================================================\n\
             # SERVER CONFIGURATION\n\
             # ============================================================================\n\
             # - To disable a server, remove or comment out its entire section\n\
             # - Unix sockets are optional for 9P and NBD servers\n\
             # - NFS only supports TCP connections\n\
             # - Each protocol supports multiple bind addresses\n\
             # \n\
             # Examples:\n\
             #   addresses = [\"127.0.0.1:2049\"]                  # IPv4 localhost only\n\
             #   addresses = [\"0.0.0.0:2049\"]                    # All IPv4 interfaces\n\
             #   addresses = [\"[::]:2049\"]                       # All IPv6 interfaces\n\
             #   addresses = [\"127.0.0.1:2049\", \"[::1]:2049\"]  # Both IPv4 and IPv6 localhost\n\
             #   addresses = [\"${{POD_IP}}:2049\"]                # env vars are expanded before the address is parsed,\n\
             #                                                  # so the host, the port, or the whole value may come from one\n\
             #\n\
             # ============================================================================\n\
             # CLOUD STORAGE\n\
             # ============================================================================\n\
             # - For S3: Configure [aws] section with your credentials\n\
             # - For Azure: Configure [azure] section with your credentials\n\
             # - For GCS: Configure [gcp] section or set GOOGLE_APPLICATION_CREDENTIALS env var\n\
             # - For local storage: Use file:// URLs (no cloud config needed)\n\
             # ============================================================================\n\
             \n{}",
            env!("CARGO_PKG_VERSION"),
            toml_string
        );

        Ok(commented)
    }

    pub fn write_default_config(path: impl AsRef<std::path::Path>) -> Result<()> {
        fs::write(path, Self::render_default_config()?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::NamedTempFile;

    #[test]
    fn test_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_PASSWORD", "secret123");
            env::set_var("ZEROFS_TEST_BUCKET", "my-bucket");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://${ZEROFS_TEST_BUCKET}/data"
encryption_password = "${ZEROFS_TEST_PASSWORD}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.storage.url, "s3://my-bucket/data");
        assert_eq!(settings.storage.encryption_password, "secret123");
    }

    #[test]
    fn test_home_env_var() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME", home_dir.to_str().unwrap());
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME}/test-cache"
disk_size_gb = 1.0

[storage]
url = "file://${ZEROFS_TEST_HOME}/data"
encryption_password = "test"

[servers]

[servers.ninep]
unix_socket = "${ZEROFS_TEST_HOME}/zerofs.sock"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(settings.cache.dir, home_dir.join("test-cache"));
        assert_eq!(
            settings.storage.url,
            format!("file://{}/data", home_dir.display())
        );
        if let Some(ninep) = settings.servers.ninep {
            assert_eq!(ninep.unix_socket.unwrap(), home_dir.join("zerofs.sock"));
        } else {
            panic!("Expected 9P config");
        }
    }

    #[test]
    fn test_undefined_env_var_error() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "${ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
        let error = format!("{:#}", result.unwrap_err());
        assert!(
            error.contains("ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST"),
            "Error was: {}",
            error
        );
    }

    #[test]
    fn test_mixed_expansion() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME_MIX", home_dir.to_str().unwrap());
            env::set_var("ZEROFS_TEST_DIR_MIX", "mydir");
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME_MIX}/${ZEROFS_TEST_DIR_MIX}/cache"
disk_size_gb = 1.0

[storage]
url = "file:///data"
encryption_password = "test"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.cache.dir, home_dir.join("mydir/cache"));
    }

    #[test]
    fn test_aws_azure_config_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_AWS_KEY", "aws123");
            env::set_var("ZEROFS_TEST_AWS_SECRET", "aws_secret");
            env::set_var("ZEROFS_TEST_AZURE_KEY", "azure456");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "${ZEROFS_TEST_AWS_KEY}"
secret_access_key = "${ZEROFS_TEST_AWS_SECRET}"

[azure]
storage_account_key = "${ZEROFS_TEST_AZURE_KEY}"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        let aws = settings.aws.unwrap();
        assert_eq!(aws.0.get("access_key_id").unwrap(), "aws123");
        assert_eq!(aws.0.get("secret_access_key").unwrap(), "aws_secret");

        let azure = settings.azure.unwrap();
        assert_eq!(azure.0.get("storage_account_key").unwrap(), "azure456");
    }

    #[test]
    fn test_aws_bool_values() {
        let config_with_bool = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "key"
allow_http = true
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_with_bool).unwrap();

        // This should fail because we can't deserialize a bool into a String
        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());

        // Now test with string "true"
        let config_with_string = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "key"
allow_http = "true"
"#;

        std::fs::write(temp_file.path(), config_with_string).unwrap();
        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_ok());
        let settings = result.unwrap();
        assert_eq!(settings.aws.unwrap().0.get("allow_http").unwrap(), "true");
    }

    fn base_config_with_replication(replication: &str) -> String {
        format!(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

{replication}
"#
        )
    }

    fn write_and_load(content: &str) -> Result<Settings> {
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), content).unwrap();
        Settings::from_file(temp_file.path().to_str().unwrap())
    }

    #[test]
    fn test_no_replication_is_single_node() {
        let content = base_config_with_replication("");
        let settings = write_and_load(&content).unwrap();
        assert!(settings.replication.is_none());
    }

    #[test]
    fn test_replication_defaults_validate() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader""#,
        );
        let settings = write_and_load(&content).unwrap();
        let repl = settings.replication.unwrap();
        assert_eq!(repl.node_id, "n1");
        assert_eq!(repl.role, ReplicationRole::Leader);
    }

    #[test]
    fn test_replication_standby_role() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n2"
role = "standby"
replication_listen = "127.0.0.1:5599""#,
        );
        let settings = write_and_load(&content).unwrap();
        assert_eq!(settings.replication.unwrap().role, ReplicationRole::Standby);
    }

    #[test]
    fn test_replication_standby_without_listen_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n2"
role = "standby""#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("replication_listen"), "got: {err}");
    }

    #[test]
    fn test_replication_invalid_listen_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "standby"
replication_listen = "not-an-address""#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("socket address"), "got: {err}");
    }

    #[test]
    fn test_replication_self_peer_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5599"]"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("must not replicate to itself"), "got: {err}");
    }

    #[test]
    fn test_replication_balanced_pair_validates() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5600"]"#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert_eq!(repl.peers, vec!["127.0.0.1:5600".to_string()]);
    }

    #[test]
    fn test_replication_bad_role_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "witness""#,
        );
        assert!(write_and_load(&content).is_err());
    }

    #[test]
    fn test_replication_empty_node_id_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = ""
role = "leader""#,
        );
        assert!(write_and_load(&content).is_err());
    }

    #[test]
    fn test_ignore_fsync_parses() {
        let content = base_config_with_replication(
            r#"[filesystem]
ignore_fsync = true"#,
        );
        let settings = write_and_load(&content).unwrap();
        assert!(settings.filesystem.unwrap().ignore_fsync);
    }

    #[test]
    fn gc_section_parses_defaults_and_clamps() {
        // Absent section: every accessor returns the historical behavior.
        let gc: GcConfig = toml::from_str("").unwrap();
        assert_eq!(gc.interval_secs(), GcConfig::DEFAULT_INTERVAL_SECS);
        assert_eq!(
            gc.idle_interval_secs(),
            GcConfig::DEFAULT_IDLE_INTERVAL_SECS
        );
        assert!(gc.read_directed());
        assert_eq!(
            gc.tail_scrub_min_dead_percent(),
            Some(GcConfig::DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT)
        );

        // Out-of-range values clamp silently
        let gc: GcConfig = toml::from_str(
            "interval_secs = 2\nidle_interval_secs = 30\n\
             tail_scrub_min_dead_percent = 90\nread_directed = false\n",
        )
        .unwrap();
        assert_eq!(gc.interval_secs(), GcConfig::MIN_INTERVAL_SECS);
        assert_eq!(gc.idle_interval_secs(), GcConfig::MIN_INTERVAL_SECS);
        assert_eq!(gc.tail_scrub_min_dead_percent(), Some(50));
        assert!(!gc.read_directed());

        // 0 is off, not a clamp to the most aggressive floor.
        let gc: GcConfig = toml::from_str("tail_scrub_min_dead_percent = 0").unwrap();
        assert_eq!(gc.tail_scrub_min_dead_percent(), None);

        // A [gc] table parses as part of Settings.
        let content = base_config_with_replication(
            r#"[gc]
interval_secs = 120
tail_scrub_min_dead_percent = 10"#,
        );
        let settings = write_and_load(&content).unwrap();
        let gc = settings.gc.unwrap();
        assert_eq!(gc.interval_secs(), 120);
        assert_eq!(gc.tail_scrub_min_dead_percent(), Some(10));
    }

    // Pre-2.0 configs set the removed [lsm] wal_enabled / max_unflushed_gb
    // keys; they must still parse (ignored, with a warning) so an upgrade
    // doesn't brick startup, and must be dropped on re-serialization.
    #[test]
    fn test_deprecated_lsm_keys_parse_and_round_trip() {
        let content = base_config_with_replication(
            r#"[lsm]
wal_enabled = true
max_unflushed_gb = 2.0
flush_interval_secs = 30"#,
        );
        let settings = write_and_load(&content).unwrap();
        let lsm = settings.lsm.as_ref().unwrap();
        assert_eq!(lsm.wal_enabled, Some(true));
        assert_eq!(lsm.max_unflushed_gb, Some(2.0));
        assert_eq!(lsm.flush_interval_secs, Some(30));

        // Round-trip: the deprecated keys are never re-emitted, and the
        // result still parses.
        let serialized = toml::to_string(&settings).unwrap();
        assert!(!serialized.contains("wal_enabled"), "got: {serialized}");
        assert!(
            !serialized.contains("max_unflushed_gb"),
            "got: {serialized}"
        );
        let reparsed: Settings = toml::from_str(&serialized).unwrap();
        let lsm = reparsed.lsm.unwrap();
        assert!(lsm.wal_enabled.is_none());
        assert!(lsm.max_unflushed_gb.is_none());
        assert_eq!(lsm.flush_interval_secs, Some(30));
    }

    // deny_unknown_fields still catches typos: only the two deprecated keys
    // get a pass.
    #[test]
    fn test_unknown_lsm_key_still_rejected() {
        let content = base_config_with_replication(
            r#"[lsm]
wal_enable = true"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("unknown field"), "got: {err}");
    }

    // The generated config must not resurrect the deprecated keys, nor
    // advertise the [wal] section (accepted only for upgraded 1.x volumes;
    // new volumes never write a WAL).
    #[test]
    fn test_generated_config_omits_deprecated_lsm_keys() {
        let rendered = Settings::render_default_config().unwrap();
        assert!(!rendered.contains("wal_enabled"));
        assert!(!rendered.contains("max_unflushed_gb"));
        assert!(!rendered.contains("[wal]"));
    }

    // A pre-2.0 config with a custom [wal] location must keep parsing: the
    // upgraded volume needs it to open (WAL replay / checkpoint references).
    #[test]
    fn test_wal_section_still_parses() {
        let content = base_config_with_replication(
            r#"[wal]
url = "file:///mnt/nvme/zerofs-wal""#,
        );
        let settings = write_and_load(&content).unwrap();
        assert_eq!(
            settings.wal.as_ref().map(|w| w.url.as_str()),
            Some("file:///mnt/nvme/zerofs-wal")
        );
    }

    #[test]
    fn test_ignore_fsync_with_sync_writes_rejected() {
        let content = base_config_with_replication(
            r#"[filesystem]
ignore_fsync = true

[lsm]
sync_writes = true"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("contradictory"), "got: {err}");
    }

    #[test]
    fn compression_config_serializes_to_canonical_strings() {
        assert_eq!(
            serde_json::to_string(&CompressionConfig::Lz4).unwrap(),
            "\"lz4\""
        );
        assert_eq!(
            serde_json::to_string(&CompressionConfig::Zstd(7)).unwrap(),
            "\"zstd-7\""
        );
    }

    #[test]
    fn compression_config_parses_valid_strings() {
        let parse = |s: &str| serde_json::from_str::<CompressionConfig>(s).unwrap();
        assert_eq!(parse("\"lz4\""), CompressionConfig::Lz4);
        assert_eq!(parse("\"zstd-1\""), CompressionConfig::Zstd(1));
        assert_eq!(parse("\"zstd-22\""), CompressionConfig::Zstd(22));
    }

    #[test]
    fn compression_config_round_trips() {
        for cfg in [
            CompressionConfig::Lz4,
            CompressionConfig::Zstd(1),
            CompressionConfig::Zstd(3),
            CompressionConfig::Zstd(22),
        ] {
            let s = serde_json::to_string(&cfg).unwrap();
            assert_eq!(serde_json::from_str::<CompressionConfig>(&s).unwrap(), cfg);
        }
    }

    #[test]
    fn compression_config_rejects_bad_strings() {
        for bad in [
            "\"zstd-0\"",   // below the 1..=22 range
            "\"zstd-23\"",  // above the range
            "\"zstd-abc\"", // non-numeric level
            "\"zstd-\"",    // missing level
            "\"gzip\"",     // unknown algorithm
            "\"\"",         // empty
        ] {
            assert!(
                serde_json::from_str::<CompressionConfig>(bad).is_err(),
                "expected {bad} to be rejected"
            );
        }
    }

    #[test]
    fn compression_config_default_is_zstd_3() {
        assert_eq!(CompressionConfig::default(), CompressionConfig::Zstd(3));
    }

    #[test]
    fn replication_role_round_trips() {
        for role in [ReplicationRole::Leader, ReplicationRole::Standby] {
            let s = serde_json::to_string(&role).unwrap();
            assert_eq!(serde_json::from_str::<ReplicationRole>(&s).unwrap(), role);
        }
        assert_eq!(
            serde_json::to_string(&ReplicationRole::Standby).unwrap(),
            "\"standby\""
        );
    }

    // Asymmetric (peers set, listen unset) is usable for the initial role, so it
    // validates with only a degraded-HA warning rather than an error.
    #[test]
    fn replication_leader_with_peers_but_no_listen_validates() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
peers = ["127.0.0.1:5600"]"#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert!(repl.replication_listen.is_none());
        assert_eq!(repl.peers, vec!["127.0.0.1:5600".to_string()]);
    }

    #[test]
    fn replication_empty_peer_entry_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5600", ""]"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("empty entry"), "got: {err}");
    }

    #[test]
    fn test_replication_node_id_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_NODE_ID", "my-node");
        }

        let content = base_config_with_replication(
            r#"[replication]
node_id = "${ZEROFS_TEST_NODE_ID}"
role = "leader""#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert_eq!(repl.node_id, "my-node");
    }

    #[test]
    fn test_replication_listen_and_peers_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_LISTEN", "10.0.0.1:9000");
            env::set_var("ZEROFS_TEST_PEER", "10.0.0.2:9000");
        }

        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "${ZEROFS_TEST_LISTEN}"
peers = ["${ZEROFS_TEST_PEER}"]"#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert_eq!(repl.replication_listen.as_deref(), Some("10.0.0.1:9000"));
        assert_eq!(repl.peers, vec!["10.0.0.2:9000".to_string()]);
    }

    #[test]
    fn test_storage_class_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_STORAGE_CLASS", "INTELLIGENT_TIERING");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"
storage_class = "${ZEROFS_TEST_STORAGE_CLASS}"

[servers]
"#;
        let settings = write_and_load(content).unwrap();
        assert_eq!(
            settings.storage.storage_class.as_deref(),
            Some("INTELLIGENT_TIERING")
        );
    }

    #[test]
    fn test_server_addresses_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_NFS_ADDR", "0.0.0.0:2049");
            env::set_var("ZEROFS_TEST_PROM_ADDR", "0.0.0.0:9091");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["${ZEROFS_TEST_NFS_ADDR}"]

[prometheus]
addresses = ["${ZEROFS_TEST_PROM_ADDR}"]
"#;
        let settings = write_and_load(content).unwrap();
        let nfs = settings.servers.nfs.unwrap().addresses.unwrap();
        assert!(nfs.contains(&"0.0.0.0:2049".parse().unwrap()));
        let prom = settings.prometheus.unwrap().addresses;
        assert!(prom.contains(&"0.0.0.0:9091".parse().unwrap()));
    }

    // Expansion runs on the whole string before it is parsed, so a variable can
    // supply just the host (or just the port) with the rest written literally.
    #[test]
    fn test_address_partial_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_HOST", "10.0.0.7");
            env::set_var("ZEROFS_TEST_PORT", "2049");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["${ZEROFS_TEST_HOST}:2049", "127.0.0.1:${ZEROFS_TEST_PORT}"]
"#;
        let nfs = write_and_load(content)
            .unwrap()
            .servers
            .nfs
            .unwrap()
            .addresses
            .unwrap();
        assert!(nfs.contains(&"10.0.0.7:2049".parse().unwrap()));
        assert!(nfs.contains(&"127.0.0.1:2049".parse().unwrap()));
    }

    #[test]
    fn test_address_invalid_after_expansion_rejected() {
        unsafe {
            env::set_var("ZEROFS_TEST_BAD_ADDR", "not-an-address");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["${ZEROFS_TEST_BAD_ADDR}"]
"#;
        let err = format!("{:#}", write_and_load(content).unwrap_err());
        assert!(err.contains("socket address"), "got: {err}");
    }

    // The generated config serializes its default addresses as SocketAddr
    // strings; they must re-parse through the expandable-address deserializer.
    #[test]
    fn test_generated_config_round_trips() {
        unsafe {
            env::set_var("ZEROFS_PASSWORD", "pw");
            env::set_var("AWS_ACCESS_KEY_ID", "key");
            env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
        }
        let rendered = Settings::render_default_config().unwrap();
        let settings = write_and_load(&rendered).unwrap();
        assert!(
            settings
                .servers
                .nfs
                .unwrap()
                .addresses
                .unwrap()
                .contains(&"127.0.0.1:2049".parse().unwrap())
        );
    }
}
