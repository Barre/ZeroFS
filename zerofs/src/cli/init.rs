//! Type-state filesystem bring-up: each phase consumes the previous, so the
//! ordering is compile-time enforced. Receiver listening before any Hello
//! (`ReceiverUp`); db opened only from `Writer`, so a follower can't open it and
//! fence a live leader; tail replayed only from `DbOpen`; fs assembled only from
//! `Replayed`, so no acked write is served before its tail is restored.
//!
//! `ha: None` is single-node mode; the HA steps are then no-ops.

use crate::block_transformer::ZeroFsBlockTransformer;
use crate::bucket_identity;
use crate::cli::server::{
    DatabaseMode, InitResult, SlateDbOpen, build_slatedb, parse_wal_object_store,
};
use crate::config::Settings;
use crate::db::SlateDbHandle;
use crate::fs::key_codec::KeyCodec;
use crate::fs::{CacheConfig, ZeroFS};
use crate::key_management;
use crate::parse_object_store::parse_url_opts;
use crate::replication::{ReplicationParams, TailBuffer};
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use slatedb::BlockTransformer;
use slatedb::object_store::path::Path;
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify, watch};
use tracing::info;

/// Storage, crypto, and parsed config in hand; no db or receiver yet.
struct Prepared {
    object_store: Arc<dyn object_store::ObjectStore>,
    wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    actual_db_path: String,
    block_transformer: Arc<dyn BlockTransformer>,
    cache_config: CacheConfig,
    segments_enabled: bool,
    dedup: Arc<crate::dedup::DedupCache>,
    replication_params: Option<ReplicationParams>,
    db_mode: DatabaseMode,
    disable_compactor: bool,
}

/// Present only with a `replication_listen` address. `leading` answers a peer's
/// Hello; `takeover_trigger` fires an immediate takeover when a deposed leader
/// checks in; `liveness`/`highest_epoch`/`tail` feed the watch and the replay.
struct HaReceiver {
    leading: Arc<AtomicBool>,
    takeover_trigger: Arc<Notify>,
    liveness: Option<watch::Receiver<u64>>,
    highest_epoch: Option<Arc<AtomicU64>>,
    tail: Option<Arc<Mutex<TailBuffer>>>,
}

/// The receiver is listening, so a peer's concurrent Hello is answered before we
/// decide our role.
struct ReceiverUp {
    prepared: Prepared,
    ha: Option<HaReceiver>,
}

/// The role is resolved and this node will write: a config leader with no active
/// peer, or a standby whose leader stopped.
struct Writer {
    prepared: Prepared,
    ha: Option<HaReceiver>,
}

/// The data db is open as the writer.
struct DbOpen {
    prepared: Prepared,
    ha: Option<HaReceiver>,
    slatedb: SlateDbHandle,
    db_handle: SlateDbHandle,
    maintenance_runtime: Option<tokio::runtime::Handle>,
    metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    sync_writes: bool,
    ignore_fsync: bool,
}

/// The buffered tail (if any) has been replayed into the open db.
struct Replayed(DbOpen);

impl Prepared {
    async fn prepare(
        settings: &Settings,
        db_mode: DatabaseMode,
        disable_compactor: bool,
    ) -> Result<Self> {
        let url = settings.storage.url.clone();

        let cache_config = CacheConfig {
            root_folder: settings.cache.dir.clone(),
            max_cache_size_gb: settings.cache.disk_size_gb,
            memory_cache_size_gb: settings.cache.memory_size_gb,
        };

        let env_vars = settings.cloud_provider_env_vars();

        let (object_store, path_from_url) = parse_url_opts(
            &url.parse().context("Failed to parse storage URL")?,
            env_vars,
        )
        .context("Failed to connect to storage backend")?;
        let object_store = with_storage_class(
            Arc::from(object_store),
            settings.storage.storage_class.as_deref(),
        );

        let actual_db_path = path_from_url.to_string();

        info!("Starting ZeroFS server with {} backend", object_store);
        info!("DB Path: {}", actual_db_path);
        info!(
            "Base Cache Directory: {}",
            cache_config.root_folder.display()
        );
        info!("Cache Size: {} GB", cache_config.max_cache_size_gb);

        info!("Checking bucket identity...");
        let bucket = bucket_identity::BucketIdentity::get_or_create(&object_store, &actual_db_path)
            .await
            .context("Failed to resolve bucket identity")?;

        let cache_config = CacheConfig {
            root_folder: cache_config.root_folder.join(bucket.cache_directory_name()),
            ..cache_config
        };

        info!(
            "Bucket ID: {}, Cache directory: {}",
            bucket.id(),
            cache_config.root_folder.display()
        );

        if !db_mode.is_read_only() {
            crate::storage_compatibility::check_if_match_support(&object_store, &actual_db_path)
                .await?;
        }

        let password = settings.storage.encryption_password.clone();
        crate::cli::password::validate_password(&password).context("Password validation failed")?;

        info!("Loading or initializing encryption key from object store");
        let db_path = Path::from(actual_db_path.clone());
        let encryption_key = key_management::load_or_init_encryption_key(
            &object_store,
            &db_path,
            &password,
            db_mode.is_read_only(),
        )
        .await
        .context("Failed to load or initialize encryption key")?;

        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&encryption_key, settings.compression());

        let wal_object_store: Option<Arc<dyn object_store::ObjectStore>> =
            if let Some(wal_config) = &settings.wal {
                info!("Using separate WAL object store: {}", wal_config.url);
                Some(
                    parse_wal_object_store(wal_config)
                        .context("Failed to connect to WAL object store")?,
                )
            } else {
                None
            };

        let segments_enabled = crate::segment_extractor::should_enable_segments(
            &object_store,
            &db_path,
            wal_object_store.as_ref(),
        )
        .await
        .context("Failed to determine segment compaction mode")?;
        if segments_enabled {
            info!("Segment-oriented compaction enabled (RFC-0024); using v2 key layout");
        } else {
            info!("Segment-oriented compaction disabled; using legacy v1 key layout");
        }

        let replication_params = settings
            .replication
            .as_ref()
            .map(crate::replication::ReplicationParams::from_config);

        // Shared with the receiver, which records shipped op-ids to keep the
        // cache warm for after a takeover.
        let dedup = Arc::new(crate::dedup::DedupCache::new(65_536));

        Ok(Self {
            object_store,
            wal_object_store,
            actual_db_path,
            block_transformer,
            cache_config,
            segments_enabled,
            dedup,
            replication_params,
            db_mode,
            disable_compactor,
        })
    }

    /// Answers Hello (so a peer learns our state) and buffers the leader's stream
    /// (the un-flushed tail, for replay on takeover). Started before the role
    /// decision so a peer's concurrent Hello is answered.
    fn start_receiver(self) -> Result<ReceiverUp> {
        let listen = self
            .replication_params
            .as_ref()
            .filter(|_| !self.db_mode.is_read_only())
            .and_then(|p| p.replication_listen.clone());

        let ha = if let Some(listen) = listen {
            let leading = Arc::new(AtomicBool::new(false));
            let takeover_trigger = Arc::new(Notify::new());
            let node_id = self.replication_params.as_ref().unwrap().node_id.clone();
            let addr: std::net::SocketAddr = listen
                .parse()
                .with_context(|| format!("invalid replication_listen address {listen:?}"))?;
            let buffer = Arc::new(Mutex::new(TailBuffer::new()));
            let receiver = crate::replication::transport::ReplicationReceiver::new(
                buffer.clone(),
                self.dedup.clone(),
                leading.clone(),
                Some(takeover_trigger.clone()),
            );
            let liveness = Some(receiver.liveness());
            let highest_epoch = Some(receiver.highest_epoch());
            let server = receiver.into_server();
            info!("HA {node_id}: replication receiver listening on {addr}");
            tokio::spawn(async move {
                if let Err(e) = tonic::transport::Server::builder()
                    .add_service(server)
                    .serve(addr)
                    .await
                {
                    tracing::error!("HA replication receiver exited: {e:#}");
                }
            });
            Some(HaReceiver {
                leading,
                takeover_trigger,
                liveness,
                highest_epoch,
                tail: Some(buffer),
            })
        } else {
            None
        };

        Ok(ReceiverUp { prepared: self, ha })
    }
}

impl ReceiverUp {
    /// Resolve the role by asking peers (Hello) and not the static config: a failover
    /// makes the config-standby the live leader, so a (re)starting node must learn
    /// the situation. Defer to an active peer; else lead if configured leader, or
    /// if configured standby with every peer confirmed down. A follower then
    /// watches heartbeats and promotes once they stop. Resolving to writer here,
    /// before `open_db`, is what keeps a follower from opening the db and fencing a
    /// live leader; the writer-epoch CAS is the hard single-writer guarantee.
    async fn become_writer(mut self) -> Result<Writer> {
        let db_mode = self.prepared.db_mode;

        if let Some(params) = self.prepared.replication_params.as_mut()
            && !db_mode.is_read_only()
            && !params.peers.is_empty()
            && params.replication_listen.is_some()
        {
            let peers = params.peers.clone();
            let config_leader = params.is_leader();
            let mut lead = config_leader;
            for attempt in 0..6u32 {
                let mut any_active = false;
                let mut any_reachable = false;
                for peer in &peers {
                    let endpoint = if peer.starts_with("http") {
                        peer.clone()
                    } else {
                        format!("http://{peer}")
                    };
                    match crate::replication::transport::hello_peer(endpoint).await {
                        Ok(true) => any_active = true,
                        Ok(false) => any_reachable = true,
                        Err(e) => tracing::debug!("HA: Hello to peer {peer} failed: {e:#}"),
                    }
                }
                if any_active {
                    lead = false; // a peer serves and will preserve the tail: defer
                    break;
                }
                if config_leader {
                    lead = true; // configured leader, no active peer: lead
                    break;
                }
                if any_reachable {
                    lead = false; // standby, peer up but not yet leading: watch it
                    break;
                }
                // Standby whose peer is unreachable: re-take so a promoted standby
                // is not stuck when its leader is gone, but retry first so a booting
                // configured leader can appear before we invert roles.
                lead = true;
                if attempt < 5 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
            params.role = if lead {
                crate::config::ReplicationRole::Leader
            } else {
                if config_leader {
                    info!(
                        "HA {}: a peer is active; deferring to it instead of opening as writer",
                        params.node_id
                    );
                }
                crate::config::ReplicationRole::Standby
            };
        }

        // A standby opens no db while it waits; it promotes only once the leader's
        // heartbeats stop.
        let need_watch = self
            .prepared
            .replication_params
            .as_ref()
            .map(|p| !db_mode.is_read_only() && !p.is_leader())
            .unwrap_or(false);
        if need_watch {
            let (node_id, takeover_ttl) = {
                let params = self.prepared.replication_params.as_ref().unwrap();
                (params.node_id.clone(), params.takeover_ttl)
            };
            info!(
                "HA standby {node_id}: watching leader heartbeats; will take over when they stop"
            );
            let liveness = self
                .ha
                .as_mut()
                .and_then(|h| h.liveness.take())
                .ok_or_else(|| {
                    anyhow::anyhow!("HA standby has no replication_listen; cannot watch heartbeats")
                })?;
            let takeover_trigger = self.ha.as_ref().unwrap().takeover_trigger.clone();
            let (_watch_tx, watch_rx) = tokio::sync::watch::channel(false);
            let took_over = crate::replication::watch_heartbeats_until_takeover(
                liveness,
                takeover_ttl,
                watch_rx,
                takeover_trigger,
            )
            .await
            .context("HA standby heartbeat watch failed")?;
            if !took_over {
                anyhow::bail!("HA standby was shut down before it could take over");
            }
            info!("HA standby {node_id}: leader heartbeats stopped, taking over as leader");
            self.prepared.replication_params.as_mut().unwrap().role =
                crate::config::ReplicationRole::Leader;
        }

        Ok(Writer {
            prepared: self.prepared,
            ha: self.ha,
        })
    }
}

impl Writer {
    async fn open_db(self, settings: &Settings) -> Result<DbOpen> {
        let opened = build_slatedb(
            self.prepared.object_store.clone(),
            &self.prepared.cache_config,
            self.prepared.actual_db_path.clone(),
            self.prepared.db_mode,
            settings.lsm,
            self.prepared.disable_compactor,
            self.prepared.block_transformer.clone(),
            self.prepared.wal_object_store.clone(),
            self.prepared.segments_enabled,
            self.prepared.replication_params.as_ref(),
        )
        .await
        .context("Failed to open database")?;

        let SlateDbOpen {
            data: slatedb,
            maintenance_runtime,
            metrics_recorder,
        } = opened;

        let db_handle = slatedb.clone();
        // Now the writer; the Hello handler reports `leading` so a (re)starting
        // peer defers instead of re-taking.
        if matches!(slatedb, SlateDbHandle::ReadWrite(_))
            && let Some(ha) = &self.ha
        {
            ha.leading.store(true, Ordering::Release);
        }

        let sync_writes = settings.lsm.map(|c| c.sync_writes()).unwrap_or(false);
        let ignore_fsync = settings
            .filesystem
            .as_ref()
            .map(|f| f.ignore_fsync)
            .unwrap_or(false);
        if ignore_fsync {
            tracing::warn!(
                "[filesystem] ignore_fsync is set: fsync/COMMIT will NOT force a flush to \
                 object storage; durability of un-flushed writes then relies on replication, \
                 and without it un-flushed writes are lost on any crash"
            );
        }

        Ok(DbOpen {
            prepared: self.prepared,
            ha: self.ha,
            slatedb,
            db_handle,
            maintenance_runtime,
            metrics_recorder,
            sync_writes,
            ignore_fsync,
        })
    }
}

impl DbOpen {
    /// A promoted standby replays its buffered tail into the freshly-opened data
    /// db (idempotent, seqno order) and flushes BEFORE serving, so no acknowledged
    /// write is lost.
    async fn replay_tail(self) -> Result<Replayed> {
        if let (Some(ha), SlateDbHandle::ReadWrite(raw_db)) = (&self.ha, &self.slatedb)
            && let Some(buffer) = ha.tail.as_ref()
        {
            let mut buf = buffer.lock().await;
            // Prune the tail to exactly what this db already holds. The leader
            // stamps each shipped batch with its seqno (`ha_seqno_key`), flushed
            // ATOMICALLY with the batch, so the db's durable value is the highest
            // shipped batch flushed here. A deposed leader that kept flushing Solo
            // (a partition) advances it past the whole tail, so we drop those stale
            // batches instead of replaying them (a replay would regress monotonic
            // counters and double-count stats against the newer db). A crashed
            // leader (kill-leader) didn't flush its last shipped batches, so they
            // stay > the watermark and are replayed (un-fsynced recovery). Trust it
            // only when written by this same term, since the seqno restarts per term.
            {
                let codec = KeyCodec::new(self.prepared.segments_enabled);
                match raw_db.get(&codec.ha_seqno_key()).await {
                    Ok(Some(v)) => {
                        if let Some((epoch, seqno)) = KeyCodec::decode_ha_seqno(&v) {
                            let term = ha
                                .highest_epoch
                                .as_ref()
                                .map(|e| e.load(Ordering::Acquire))
                                .unwrap_or(0);
                            if epoch == term {
                                let before = buf.len();
                                buf.prune(seqno);
                                info!(
                                    "HA takeover: db holds shipped batches through seqno {} \
                                     (epoch {}); pruned {} stale of {} buffered batch(es)",
                                    seqno,
                                    epoch,
                                    before - buf.len(),
                                    before
                                );
                            } else {
                                info!(
                                    "HA takeover: db HA watermark epoch {} != current term {}; \
                                     replaying full tail",
                                    epoch, term
                                );
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!("HA takeover: reading HA tail watermark failed: {e}")
                    }
                }
            }
            if !buf.is_empty() {
                info!(
                    "HA takeover: replaying {} buffered batch(es) into the data db",
                    buf.len()
                );
                for (_seqno, ops) in buf.batches_in_order() {
                    let mut batch = slatedb::WriteBatch::new();
                    for op in ops {
                        match op {
                            crate::replication::ReplOp::Put(k, v) => {
                                batch.put_bytes(k.clone(), v.clone())
                            }
                            crate::replication::ReplOp::Delete(k) => batch.delete(k.clone()),
                        }
                    }
                    raw_db
                        .write_with_options(
                            batch,
                            &slatedb::config::WriteOptions {
                                await_durable: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .context("HA takeover tail replay failed")?;
                }
                raw_db
                    .flush()
                    .await
                    .context("HA takeover replay flush failed")?;
            }
        }

        Ok(Replayed(self))
    }
}

impl Replayed {
    async fn into_filesystem(self, settings: &Settings) -> Result<InitResult> {
        let DbOpen {
            prepared,
            ha: _,
            slatedb,
            db_handle,
            maintenance_runtime,
            metrics_recorder,
            sync_writes,
            ignore_fsync,
        } = self.0;

        // Leader replicator: ships to the standby when connected, runs solo
        // otherwise, reconnecting when it reappears. Never blocks startup or writes.
        let replicator = match prepared.replication_params.as_ref() {
            Some(params) if params.is_leader() && !params.peers.is_empty() => {
                let peer = params.peers[0].clone();
                let endpoint = if peer.starts_with("http://") || peer.starts_with("https://") {
                    peer
                } else {
                    format!("http://{peer}")
                };
                // Tag every ship with this leader's data-db writer epoch, so a
                // deposed leader ships a lower epoch and the standby rejects it.
                let writer_epoch = match &slatedb {
                    SlateDbHandle::ReadWrite(db) => {
                        db.subscribe().borrow().current_manifest.writer_epoch()
                    }
                    SlateDbHandle::ReadOnly(_) => 0,
                };
                info!(
                    "HA leader: replicating to standby peer {} (writer epoch {})",
                    endpoint, writer_epoch
                );
                // Stream heartbeats so the standby detects this leader's death
                // (near-instant on a crash via the broken stream; periodic beats
                // cover an idle leader), reconnecting on break.
                let hb_endpoint = endpoint.clone();
                let hb_interval = params.heartbeat_interval;
                tokio::spawn(async move {
                    loop {
                        if let Err(e) = crate::replication::transport::run_heartbeat_sender(
                            hb_endpoint.clone(),
                            writer_epoch,
                            hb_interval,
                        )
                        .await
                        {
                            tracing::debug!(
                                "HA: heartbeat stream to standby ended ({e:#}); reconnecting"
                            );
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                });
                let replicator = crate::replication::Replicator::new(endpoint, writer_epoch);
                tokio::spawn(crate::replication::replicator::run_reconnect(
                    Arc::downgrade(&replicator),
                ));
                // Drive the standby's prune watermark from the data db's durable_seq.
                if let SlateDbHandle::ReadWrite(raw_db) = &slatedb {
                    tokio::spawn(crate::replication::replicator::run_watermark(
                        replicator.clone(),
                        raw_db.subscribe(),
                    ));
                }
                Some(replicator)
            }
            _ => None,
        };

        // Leader lease, driven by the data db's own status: renewed while open,
        // revoked the instant SlateDB closes it (CloseReason::Fenced on a takeover).
        let (heartbeat_shutdown, lease) = match prepared.replication_params.as_ref() {
            Some(params) if !prepared.db_mode.is_read_only() => {
                let lease = crate::replication::Lease::new();
                lease.renew(params.lease_ttl);
                let (tx, rx) = tokio::sync::watch::channel(false);
                if let SlateDbHandle::ReadWrite(raw_db) = &slatedb {
                    tokio::spawn(crate::replication::run_lease_from_status(
                        lease.clone(),
                        raw_db.subscribe(),
                        params.heartbeat_interval,
                        params.lease_ttl,
                        rx,
                    ));
                }
                (Some(tx), Some(lease))
            }
            _ => (None, None),
        };

        let fs = ZeroFS::new_with_slatedb_and_lease(
            slatedb,
            settings.max_bytes(),
            metrics_recorder,
            sync_writes,
            ignore_fsync,
            prepared.segments_enabled,
            lease,
            replicator,
            prepared.dedup,
        )
        .await
        .context("Failed to initialize filesystem")?;

        let fs = Arc::new(fs);
        // Reclaims open-unlinked inodes once their last open handle is dropped.
        fs.start_reclaim_drainer();

        Ok(InitResult {
            fs,
            object_store: prepared.object_store,
            wal_object_store: prepared.wal_object_store,
            db_path: prepared.actual_db_path,
            db_handle,
            maintenance_runtime,
            heartbeat_shutdown,
        })
    }
}

/// Bring the filesystem up through the type-state phases; the chain is the whole
/// control flow.
pub async fn initialize_filesystem(
    settings: &Settings,
    db_mode: DatabaseMode,
    disable_compactor: bool,
) -> Result<InitResult> {
    Prepared::prepare(settings, db_mode, disable_compactor)
        .await?
        .start_receiver()?
        .become_writer()
        .await?
        .open_db(settings)
        .await?
        .replay_tail()
        .await?
        .into_filesystem(settings)
        .await
}
