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
use crate::object_trace::{ObjectTracer, TracingObjectStore};
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
    /// `object_store` behind the shared retry wrapper, for I/O ZeroFS issues
    /// directly (segments, GC/checkpoint listings, the HA leader record).
    /// SlateDB traffic stays on `object_store`: it brings its own retry layer.
    retrying_object_store: Arc<dyn object_store::ObjectStore>,
    wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    /// Shared by the data and WAL `TracingObjectStore` wrappers and handed to
    /// the filesystem so the RPC server can stream backend requests (`otrace`).
    object_tracer: ObjectTracer,
    actual_db_path: String,
    block_transformer: Arc<dyn BlockTransformer>,
    segment_codec: crate::frame_codec::FrameCodec,
    cache_config: CacheConfig,
    dedup: Arc<crate::dedup::DedupCache>,
    replication_params: Option<ReplicationParams>,
    db_mode: DatabaseMode,
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
    /// Prefetch-wrapped object store for the segment data plane (cold read-ahead).
    segment_object_store: Arc<dyn object_store::ObjectStore>,
    /// Warms the parts cache with a just-sealed segment (multipart uploads bypass
    /// the prefetcher's own write-through). Applies the same db-path prefix as
    /// `segment_object_store` so the cache key matches the read path.
    segment_warm: Option<crate::segment_store::SegmentWarmHook>,
}

/// The buffered tail (if any) has been replayed into the open db.
struct Replayed(DbOpen);

impl Prepared {
    async fn prepare(settings: &Settings, db_mode: DatabaseMode) -> Result<Self> {
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

        let replication_params = settings
            .replication
            .as_ref()
            .map(crate::replication::ReplicationParams::from_config);

        // Shared with the receiver, which records shipped op-ids to keep the
        // cache warm for after a takeover.
        let dedup = Arc::new(crate::dedup::DedupCache::new(65_536));

        // Trace at the bottom of the stack so otrace sees the requests that
        // actually leave the process. Everything above (length-check, prefetch,
        // compactor) reads through these wrappers; cache hits make no backend
        // request and so produce no event.
        let object_tracer = ObjectTracer::new();
        let object_store = Arc::new(TracingObjectStore::new(
            object_store,
            object_tracer.clone(),
            "data",
        )) as Arc<dyn object_store::ObjectStore>;
        let wal_object_store = wal_object_store.map(|s| {
            Arc::new(TracingObjectStore::new(s, object_tracer.clone(), "wal"))
                as Arc<dyn object_store::ObjectStore>
        });

        Ok(Self {
            retrying_object_store: Arc::new(
                crate::retrying_object_store::RetryingObjectStore::new(object_store.clone()),
            ),
            object_store,
            wal_object_store,
            object_tracer,
            actual_db_path,
            block_transformer,
            segment_codec: crate::frame_codec::FrameCodec::new(
                &encryption_key,
                crate::segment::SEGMENT_INFO,
                settings.compression(),
            ),
            cache_config,
            dedup,
            replication_params,
            db_mode,
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
                node_id.clone(),
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
    /// the situation. Defer to an active peer. On silence, the recorded latest
    /// writer and any configured standby block until a peer answers (see
    /// [`crate::replication::leader_record`]); only a configured leader the record
    /// does not name leads through it (fresh db, or recovery after losing both
    /// nodes). A follower then watches heartbeats and promotes once they stop.
    /// Resolving to writer here, before `open_db`, is what keeps a follower from
    /// opening the db and fencing a live leader; the writer-epoch CAS is the hard
    /// single-writer guarantee.
    async fn become_writer(mut self) -> Result<Writer> {
        let db_mode = self.prepared.db_mode;

        if let Some(params) = self.prepared.replication_params.as_mut()
            && !db_mode.is_read_only()
            && !params.peers.is_empty()
            && params.replication_listen.is_some()
        {
            let was_latest = crate::replication::leader_record::read(
                &self.prepared.retrying_object_store,
                &self.prepared.actual_db_path,
            )
            .await
            .context("HA: cannot resolve a boot role without the latest-leader record")?
            .is_some_and(|(_, node)| node == params.node_id);

            let peers = params.peers.clone();
            let config_leader = params.is_leader();
            let lead;
            let mut silent_rounds = 0u32;
            loop {
                let mut any_active = false;
                let mut any_reachable = false;
                let mut all_answered = true;
                for peer in &peers {
                    let endpoint = if peer.starts_with("http") {
                        peer.clone()
                    } else {
                        format!("http://{peer}")
                    };
                    match crate::replication::transport::hello_peer(endpoint).await {
                        Ok(answer) => {
                            // Identity keys the latest-leader record; a duplicated
                            // node_id is a config error.
                            if !answer.node_id.is_empty() && answer.node_id == params.node_id {
                                anyhow::bail!(
                                    "HA: peer {peer} reports the same node_id {:?} as this \
                                     node; node_id must be unique within the pair",
                                    params.node_id
                                );
                            }
                            if answer.peer_active {
                                any_active = true;
                            } else {
                                any_reachable = true;
                            }
                        }
                        Err(e) => {
                            all_answered = false;
                            tracing::debug!("HA: Hello to peer {peer} failed: {e:#}");
                        }
                    }
                }
                if any_active {
                    lead = false; // a peer serves and will preserve the tail: defer
                    break;
                }
                if was_latest && !all_answered {
                    // This node was the last writer and a peer is silent. That peer
                    // may be live behind a partition, holding this node's acked
                    // writes in its RAM tail or already promoted; electing would
                    // abandon the tail or fence the live writer.
                    if silent_rounds.is_multiple_of(20) {
                        tracing::warn!(
                            "HA {}: this node was the latest writer and a peer does not \
                             answer Hello; blocking startup until it does (if the peer is \
                             permanently gone, remove it from replication.peers, and set \
                             role = \"leader\" if this node was the standby)",
                            params.node_id
                        );
                    }
                    silent_rounds += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                if config_leader {
                    lead = true; // configured leader, no active peer: lead
                    break;
                }
                if any_reachable {
                    lead = false; // standby, peer up but not yet leading: watch it
                    break;
                }
                // Configured standby, every peer silent: wait. The configured
                // leader decides the roles when it appears. A timeout here could
                // not tell a dead peer from a partition whose live leader this
                // election would fence, so a permanently gone peer is an operator
                // call (promote this node by config).
                if silent_rounds.is_multiple_of(20) {
                    tracing::warn!(
                        "HA {}: configured standby and no peer answers Hello; waiting \
                         for one (if the peer is permanently gone, set role = \"leader\" \
                         and remove it from replication.peers to promote this node)",
                        params.node_id
                    );
                }
                silent_rounds += 1;
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
            self.prepared.block_transformer.clone(),
            self.prepared.wal_object_store.clone(),
            self.prepared.replication_params.as_ref(),
        )
        .await
        .context("Failed to open database")?;

        let SlateDbOpen {
            data: slatedb,
            maintenance_runtime,
            metrics_recorder,
            parts_cache,
        } = opened;

        // Segment reads share SlateDB's prefetch parts cache (one budget, keyed
        // by path); the seal-cache still serves same-process read-after-write.
        //
        // Segments are namespaced under the db path, so databases sharing one
        // bucket don't collide on a global `segments/` keyspace. The prefix
        // sits outside the prefetcher so the cache keys are db-distinct too.
        //
        // Retries sit under the prefetcher, so a single-flight window GET rides
        // out a transient error before failing every waiting reader, and above
        // the tracing layer, so each attempt is visible to otrace.
        let prefetch = Arc::new(crate::object_store_prefetch::PrefetchingObjectStore::new(
            self.prepared.retrying_object_store.clone(),
            parts_cache,
        ));
        let db_prefix = Path::from(self.prepared.actual_db_path.clone());
        let segment_object_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::prefix::PrefixStore::new(
                Arc::clone(&prefetch) as Arc<dyn object_store::ObjectStore>,
                db_prefix.clone(),
            ));
        // Warm the parts cache at seal time. `put_segment` passes the unprefixed
        // object path, so prepend the db prefix exactly as PrefixStore would, giving
        // the same cache key the read path derives.
        let segment_warm: Option<crate::segment_store::SegmentWarmHook> =
            Some(Arc::new(move |loc: &Path, bytes: bytes::Bytes| {
                let full: Path = db_prefix.parts().chain(loc.parts()).collect();
                prefetch.warm_object(&full, bytes);
            }));

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
            segment_object_store,
            segment_warm,
        })
    }
}

impl DbOpen {
    /// A promoted standby replays its buffered tail into the freshly-opened data
    /// db (idempotent, seqno order) and flushes before serving, so no acknowledged
    /// write is lost.
    async fn replay_tail(self) -> Result<Replayed> {
        if let (Some(ha), SlateDbHandle::ReadWrite(raw_db)) = (&self.ha, &self.slatedb)
            && let Some(buffer) = ha.tail.as_ref()
        {
            let mut buf = buffer.lock().await;
            // Replay must only extend durable history: validate the tail against
            // the db's provenance stamp (see `replay_decision`). On an unreadable
            // or undecodable stamp, drop the tail: that forfeits at most
            // un-fsynced writes (the honest designed-in loss), while replaying
            // blind could regress durable state.
            if !buf.is_empty() {
                use crate::replication::tail::{ReplayDecision, replay_decision};
                let codec = KeyCodec::new();
                let decision = match raw_db.get(&codec.ha_seqno_key()).await {
                    Ok(None) => replay_decision(None, buf.epoch()),
                    Ok(Some(v)) => match KeyCodec::decode_ha_stamp(&v) {
                        Some(stamp) => replay_decision(Some(stamp), buf.epoch()),
                        None => {
                            tracing::warn!("HA takeover: undecodable HA stamp; dropping the tail");
                            ReplayDecision::Discard
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            "HA takeover: reading the HA stamp failed ({e}); dropping the \
                             tail rather than replaying over unseen durable state"
                        );
                        ReplayDecision::Discard
                    }
                };
                match decision {
                    ReplayDecision::ReplayAll => {
                        info!(
                            "HA takeover: nothing of the tail's term (epoch {}) is durable; \
                             replaying the full tail",
                            buf.epoch()
                        );
                    }
                    ReplayDecision::PruneTo(seqno) => {
                        let before = buf.len();
                        buf.prune(seqno);
                        info!(
                            "HA takeover: db holds shipped batches through seqno {} \
                             (epoch {}); pruned {} durable of {} buffered batch(es)",
                            seqno,
                            buf.epoch(),
                            before - buf.len(),
                            before
                        );
                    }
                    ReplayDecision::Discard => {
                        info!(
                            "HA takeover: the durable head supersedes the buffered tail \
                             (epoch {}, {} batch(es)); dropping it instead of regressing \
                             newer state",
                            buf.epoch(),
                            buf.len()
                        );
                        buf.discard();
                    }
                }
            }
            if !buf.is_empty() {
                info!(
                    "HA takeover: replaying {} buffered batch(es) into the data db",
                    buf.len()
                );
                let key_codec = KeyCodec::new();
                // Un-PUT segments referenced by shipped extent writes, rebuilt
                // on the shared store so the replayed FrameLocs resolve.
                let mut recon: std::collections::HashMap<
                    crate::segment::Segid,
                    Vec<crate::segment_store::ReconFrame>,
                > = std::collections::HashMap::new();
                for (_seqno, ops) in buf.batches_in_order() {
                    let mut batch = slatedb::WriteBatch::new();
                    for op in ops {
                        match op {
                            crate::replication::ReplOp::Put(k, v) => {
                                batch.put_bytes(k.clone(), v.clone())
                            }
                            crate::replication::ReplOp::Delete(k) => batch.delete(k.clone()),
                            crate::replication::ReplOp::PutFrame(k, v, frame) => {
                                batch.put_bytes(k.clone(), v.clone());
                                if let Some((inode, extent)) = key_codec.parse_extent_key_full(k)
                                    && let Some(loc) = crate::segment::FrameLoc::decode(v)
                                {
                                    recon.entry(loc.segid).or_default().push(
                                        crate::segment_store::ReconFrame {
                                            frame_index: loc.frame_index,
                                            byte_offset: loc.byte_offset,
                                            byte_len: loc.byte_len,
                                            inode,
                                            extent,
                                            bytes: frame.clone(),
                                        },
                                    );
                                }
                            }
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
                // Materialize the un-PUT segments before serving; HEAD-guarded
                // so a leader-sealed object is never overwritten with a
                // (possibly partial) reconstruction.
                let mut materialized = 0usize;
                for (segid, frames) in &recon {
                    // A failure here can't be swallowed: the next step flushes
                    // these FrameLocs durably, and serving without the segment
                    // object would leave acked writes permanently dangling.
                    // Retry a few times, then fail the takeover.
                    const MATERIALIZE_ATTEMPTS: u32 = 5;
                    let mut attempt = 0;
                    loop {
                        match crate::segment_store::materialize_segment_if_absent(
                            &self.segment_object_store,
                            &self.prepared.segment_codec,
                            *segid,
                            frames,
                        )
                        .await
                        {
                            Ok(true) => {
                                materialized += 1;
                                break;
                            }
                            Ok(false) => break,
                            Err(e) => {
                                attempt += 1;
                                if attempt >= MATERIALIZE_ATTEMPTS {
                                    return Err(anyhow::anyhow!(
                                        "HA takeover: reconstructing un-PUT segment {:?} failed \
                                         after {} attempts: {}. Aborting takeover rather than \
                                         durably committing a dangling FrameLoc for an acked write.",
                                        segid,
                                        MATERIALIZE_ATTEMPTS,
                                        e
                                    ));
                                }
                                tracing::warn!(
                                    "HA takeover: reconstructing segment {:?} failed \
                                     (attempt {}/{}): {}; retrying",
                                    segid,
                                    attempt,
                                    MATERIALIZE_ATTEMPTS,
                                    e
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    200 * attempt as u64,
                                ))
                                .await;
                            }
                        }
                    }
                }
                if materialized > 0 {
                    info!(
                        "HA takeover: materialized {} un-PUT segment(s) from the replayed tail",
                        materialized
                    );
                }
                raw_db
                    .flush()
                    .await
                    .context("HA takeover replay flush failed")?;
            }
        }

        // Every HA writer names itself in the latest-leader record before it
        // serves, so its next boot blocks on a silent peer instead of electing.
        // After the writer-epoch CAS, so concurrent openers are serialized.
        if let Some(params) = self.prepared.replication_params.as_ref()
            && !self.prepared.db_mode.is_read_only()
            && let SlateDbHandle::ReadWrite(raw_db) = &self.slatedb
        {
            let epoch = raw_db.subscribe().borrow().current_manifest.writer_epoch();
            crate::replication::leader_record::write(
                &self.prepared.retrying_object_store,
                &self.prepared.actual_db_path,
                epoch,
                &params.node_id,
            )
            .await
            .context("HA: writing the latest-leader record failed")?;
        }

        Ok(Replayed(self))
    }
}

impl Replayed {
    async fn into_filesystem(self, settings: &Settings) -> Result<InitResult> {
        let DbOpen {
            prepared,
            ha,
            slatedb,
            db_handle,
            maintenance_runtime,
            metrics_recorder,
            sync_writes,
            ignore_fsync,
            segment_object_store,
            segment_warm,
        } = self.0;

        // A live-standby takeover iff we observed a live leader's epoch on the
        // replication stream (ships/heartbeats) before promoting. A cold bootstrap /
        // config leader / single node observed none (highest_epoch stays 0), so it
        // regenerates the durability lineage rather than carrying a stale one forward.
        let is_live_takeover = ha
            .as_ref()
            .and_then(|h| h.highest_epoch.as_ref())
            .is_some_and(|e| e.load(Ordering::Relaxed) > 0);

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
            lease,
            replicator,
            prepared.dedup,
            is_live_takeover,
            prepared.object_tracer.clone(),
            segment_object_store,
            prepared.segment_codec,
            segment_warm,
        )
        .await
        .context("Failed to initialize filesystem")?;

        let fs = Arc::new(fs);
        // Reclaims open-unlinked inodes once their last open handle is dropped.
        fs.start_reclaim_drainer();

        Ok(InitResult {
            fs,
            // Retry-wrapped for the consumers downstream (the GC's checkpoint-gate
            // admin and the checkpoint manager), whose listings would otherwise
            // fail on one transient backend error.
            object_store: prepared.retrying_object_store,
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
) -> Result<InitResult> {
    Prepared::prepare(settings, db_mode)
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
