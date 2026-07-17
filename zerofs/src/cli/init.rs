//! Filesystem startup and HA role election.
//!
//! The replication receiver starts before role election. Marker capabilities
//! enforce the claim, open, and activation order. A promoted writer reconciles
//! its frozen tail before activation.
//!
//! `ha: None` is single-node mode; the HA steps are then no-ops.

use crate::block_transformer::ZeroFsBlockTransformer;
use crate::bucket_identity;
use crate::cli::server::{
    DatabaseMode, InitResult, SlateDbOpen, build_slatedb, parse_wal_object_store,
};
use crate::config::Settings;
use crate::db::SlateDbHandle;
use crate::fs::{CacheConfig, ZeroFS};
use crate::key_management;
use crate::object_trace::{ObjectTracer, TracingObjectStore};
use crate::parse_object_store::parse_url_opts;
use crate::replication::transport::{PromotionSnapshot, ReceiverControl};
use crate::replication::{LineageProof, PromotionRetryGraceProof, ReplicationParams};
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use slatedb::BlockTransformer;
use slatedb::object_store::path::Path;
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::sync::Arc;
use tokio::sync::{Notify, watch};
use tracing::info;

/// State retained across role-election and writer-open retries.
struct StartupContext {
    object_store: Arc<dyn object_store::ObjectStore>,
    /// Retrying store for direct ZeroFS I/O. SlateDB retries its own store I/O.
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
    /// Configured role. `replication_params.role` holds the elected role.
    configured_replication_role: Option<crate::config::ReplicationRole>,
    db_mode: DatabaseMode,
    ha: Option<HaReceiver>,
    /// Whether this startup observed predecessor failure.
    took_over_from_standby: bool,
    /// Use the recovery-only claim for an observed Claiming or Opening marker.
    recovering_handoff: bool,
    /// Opening capability retained across writer-open retries.
    opening: Option<crate::replication::leader_record::OpeningToken>,
}

/// Receiver handles carried through role election and takeover reconciliation.
struct HaReceiver {
    control: ReceiverControl,
    takeover_trigger: Arc<Notify>,
    /// Cloned for each standby watch; the receiver remains bound across retries.
    liveness: watch::Receiver<u64>,
}

/// The data db is open as the writer.
struct DbOpen {
    promotion: Option<PromotionSnapshot>,
    slatedb: SlateDbHandle,
    metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    /// Prefetch-wrapped object store for the segment data plane (cold read-ahead).
    segment_object_store: Arc<dyn object_store::ObjectStore>,
    /// Warms the parts cache with a just-sealed segment (multipart uploads bypass
    /// the prefetcher's own write-through). Applies the same db-path prefix as
    /// `segment_object_store` so the cache key matches the read path.
    segment_warm: Option<crate::segment_store::SegmentWarmHook>,
}

/// Open database with a reconciled replication tail.
struct ReconciledDb {
    open: DbOpen,
    lineage_proof: Option<LineageProof>,
    retry_grace_proof: Option<PromotionRetryGraceProof>,
}

enum ClaimOutcome {
    Claimed(Option<crate::replication::leader_record::OpeningToken>),
    RetryRole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ImmediateRoleDecision {
    RecoverHandoff,
    FollowActivePeer,
}

/// Resolve marker recovery before advisory peer state.
fn immediate_role_decision(
    unresolved_handoff: bool,
    any_active: bool,
) -> Option<ImmediateRoleDecision> {
    if unresolved_handoff {
        Some(ImmediateRoleDecision::RecoverHandoff)
    } else if any_active {
        Some(ImmediateRoleDecision::FollowActivePeer)
    } else {
        None
    }
}

enum OpenOutcome {
    Opened(DbOpen),
    RetryRole,
    RetryWriter,
}

enum ReconcileOutcome {
    Reconciled(Box<ReconciledDb>),
    RetryRole,
    RetryWriter,
}

impl StartupContext {
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
        let configured_replication_role = settings.replication.as_ref().map(|cfg| cfg.role);

        // Shared by request handling and takeover reconciliation.
        let dedup = Arc::new(crate::dedup::DedupCache::new());
        dedup.start_expiry_reaper();

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
            configured_replication_role,
            db_mode,
            ha: None,
            took_over_from_standby: false,
            recovering_handoff: false,
            opening: None,
        })
    }

    /// Answers Hello (so a peer learns our state) and buffers the leader's stream
    /// (the un-flushed tail, for reconciliation on takeover). Started before the role
    /// decision so a peer's concurrent Hello is answered.
    fn start_receiver(mut self) -> Result<Self> {
        let listen = self
            .replication_params
            .as_ref()
            .filter(|_| !self.db_mode.is_read_only())
            .and_then(|p| p.replication_listen.clone());

        let ha = if let Some(listen) = listen {
            let takeover_trigger = Arc::new(Notify::new());
            let node_id = self.replication_params.as_ref().unwrap().node_id.clone();
            let addr: std::net::SocketAddr = listen
                .parse()
                .with_context(|| format!("invalid replication_listen address {listen:?}"))?;
            let receiver = crate::replication::transport::ReplicationReceiver::new(
                self.dedup.clone(),
                Some(takeover_trigger.clone()),
                node_id.clone(),
            );
            let control = receiver.control();
            let liveness = receiver.liveness();
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
                control,
                takeover_trigger,
                liveness,
            })
        } else {
            None
        };

        self.ha = ha;
        Ok(self)
    }

    /// Elect a runtime role from the durable marker and peer Hello responses.
    /// The recorded latest writer and configured standby wait on ambiguous peer
    /// silence. Only a writer elected here may open the database.
    async fn become_writer(&mut self) -> Result<()> {
        debug_assert!(self.opening.is_none());
        self.recovering_handoff = false;
        // Acknowledgements remain enabled during role election and are quiesced
        // before a durable claim.
        if let Some(receiver) = &self.ha {
            receiver.control.resume_heartbeat_acks().await;
        }
        let db_mode = self.db_mode;
        let mut recovered_handoff = false;

        if let Some(params) = self.replication_params.as_mut()
            && !db_mode.is_read_only()
            && !params.peers.is_empty()
        {
            let marker = crate::replication::leader_record::inspect(
                &self.object_store,
                &self.actual_db_path,
            )
            .await
            .context("HA: cannot resolve a boot role without the leader record")?;
            let was_latest = marker
                .latest_writer()
                .is_some_and(|(_, node)| node == params.node_id);
            let unresolved_handoff = matches!(
                marker.phase(),
                Some(
                    crate::replication::leader_record::LeaderRecordPhase::Claiming
                        | crate::replication::leader_record::LeaderRecordPhase::Opening
                )
            );
            let handoff_blockers = marker.startup_blockers();

            let peers = params.peers.clone();
            let config_leader = matches!(
                self.configured_replication_role,
                Some(crate::config::ReplicationRole::Leader)
            );
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
                            if answer.node_id == params.node_id {
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
                            if crate::replication::transport::hello_protocol_incompatible(&e) {
                                return Err(e).with_context(|| {
                                    format!(
                                        "HA: peer {peer} answered Hello with an incompatible \
                                         protocol; refusing role election"
                                    )
                                });
                            }
                            all_answered = false;
                            tracing::debug!("HA: Hello to peer {peer} failed: {e:#}");
                        }
                    }
                }
                match immediate_role_decision(unresolved_handoff, any_active) {
                    Some(ImmediateRoleDecision::RecoverHandoff) => {
                        // Claiming and Opening liveness is determined from the
                        // durable marker, not a predecessor's Hello response.
                        lead = true;
                        recovered_handoff = true;
                        break;
                    }
                    Some(ImmediateRoleDecision::FollowActivePeer) => {
                        lead = false;
                        break;
                    }
                    None => {}
                }
                if was_latest && !all_answered {
                    // This node was the last writer and a peer is silent. That peer
                    // may be live behind a partition, holding this node's acked
                    // writes in its RAM tail or already promoted; electing would
                    // abandon the tail or fence the live writer.
                    if silent_rounds.is_multiple_of(20) {
                        tracing::warn!(
                            "HA {}: the durable leader record makes peer silence ambiguous \
                             (startup blockers: {:?}); blocking startup until every peer \
                             answers (if the recorded process is permanently gone, remove it \
                             from replication.peers and set role = \"leader\")",
                            params.node_id,
                            handoff_blockers
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
                         for one (if the peer is permanently gone, set role = \"leader\" and \
                         remove it from replication.peers)",
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
            .replication_params
            .as_ref()
            .is_some_and(|p| !db_mode.is_read_only() && !p.is_leader());
        if need_watch {
            let node_id = self.replication_params.as_ref().unwrap().node_id.clone();
            info!(
                "HA standby {node_id}: watching leader heartbeats; silence will trigger a durable claim attempt"
            );
            let liveness = self
                .ha
                .as_ref()
                .map(|h| h.liveness.clone())
                .ok_or_else(|| {
                    anyhow::anyhow!("HA standby has no replication_listen; cannot watch heartbeats")
                })?;
            let takeover_trigger = self.ha.as_ref().unwrap().takeover_trigger.clone();
            let (_watch_tx, watch_rx) = tokio::sync::watch::channel(false);
            let should_attempt_takeover = crate::replication::watch_heartbeats_until_takeover_hint(
                liveness,
                crate::replication::TAKEOVER_HINT_AFTER,
                watch_rx,
                takeover_trigger,
            )
            .await
            .context("HA standby heartbeat watch failed")?;
            if !should_attempt_takeover {
                anyhow::bail!("HA standby was shut down before its durable claim attempt");
            }
            info!(
                "HA standby {node_id}: leader heartbeats stopped; attempting the durable takeover claim"
            );
            self.replication_params.as_mut().unwrap().role = crate::config::ReplicationRole::Leader;
        }

        self.recovering_handoff = recovered_handoff;
        self.took_over_from_standby |= recovered_handoff || need_watch;
        Ok(())
    }

    /// Acquire Claiming and advance it to an Opening capability.
    async fn claim_opening(&self) -> Result<ClaimOutcome> {
        let claim_request = self
            .replication_params
            .as_ref()
            .filter(|_| !self.db_mode.is_read_only())
            .map(|params| {
                (
                    params.node_id.clone(),
                    params.force_recovery,
                    self.recovering_handoff,
                )
            });

        let Some((node_id, force, recovering_handoff)) = claim_request else {
            return Ok(ClaimOutcome::Claimed(None));
        };

        // Heartbeat admission uses the same receiver phase lock. No new
        // acknowledgement can linearize between this fence and the claim CAS.
        if let Some(receiver) = &self.ha {
            receiver.control.quiesce_heartbeat_acks().await;
        }
        let claim_result = if recovering_handoff && !force {
            crate::replication::leader_record::recover_handoff(
                &self.object_store,
                &self.actual_db_path,
                &node_id,
            )
            .await
        } else {
            crate::replication::leader_record::claim(
                &self.object_store,
                &self.actual_db_path,
                &node_id,
                force,
            )
            .await
        };
        let claim = match claim_result {
            Ok(claim) => claim,
            Err(error)
                if error
                    .downcast_ref::<crate::replication::leader_record::ClaimRejected>()
                    .is_some() =>
            {
                if force {
                    return Err(error).context(
                        "HA: forced solo recovery raced another initializer; verify no \
                         other process is using this database before retrying",
                    );
                }
                tracing::warn!(
                    "HA: another initializer owns the durable pre-open marker \
                     ({error}); returning to role election"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return Ok(ClaimOutcome::RetryRole);
            }
            Err(error) => {
                return Err(error).context("HA: acquiring durable writer claim failed");
            }
        };

        let opening = match crate::replication::leader_record::begin_open(claim).await {
            Ok(opening) => opening,
            Err(error)
                if error
                    .downcast_ref::<crate::replication::leader_record::OwnershipLost>()
                    .is_some() =>
            {
                tracing::warn!(
                    "HA: durable Claiming ownership was superseded before database open; \
                     returning to role election"
                );
                return Ok(ClaimOutcome::RetryRole);
            }
            Err(error) => {
                return Err(error).context("HA: writer claim lost before database open");
            }
        };
        Ok(ClaimOutcome::Claimed(Some(opening)))
    }

    /// Close a superseded writer and revalidate its Opening capability.
    async fn close_writer_for_retry(
        &mut self,
        slatedb: &SlateDbHandle,
        validation_context: &'static str,
    ) -> Result<bool> {
        if let SlateDbHandle::ReadWrite(raw_db) = slatedb
            && let Err(error) = raw_db.close().await
        {
            // Fenced and already-closed handles are dropped after this check.
            tracing::warn!("HA: closing a superseded writer returned: {error}");
        }
        let opening = self
            .opening
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("HA writer retry lost its Opening capability"))?;
        let still_owns_opening = crate::replication::leader_record::validate_opening(opening)
            .await
            .context(validation_context)?;
        if !still_owns_opening {
            tracing::warn!(
                "HA: durable Opening ownership was superseded while closing a stale writer; \
                 returning to role election"
            );
            self.opening.take();
        }
        Ok(still_owns_opening)
    }

    async fn open_db(&mut self, settings: &Settings) -> Result<OpenOutcome> {
        if let Some(opening) = self.opening.as_ref() {
            let owns_opening = crate::replication::leader_record::validate_opening(opening)
                .await
                .context("HA: validating Opening before database open failed")?;
            if !owns_opening {
                tracing::warn!(
                    "HA: durable Opening ownership was superseded before database open; \
                     returning to role election"
                );
                self.opening.take();
                return Ok(OpenOutcome::RetryRole);
            }
        }
        let opened = build_slatedb(
            self.object_store.clone(),
            &self.cache_config,
            self.actual_db_path.clone(),
            self.db_mode,
            settings.lsm,
            self.block_transformer.clone(),
            self.wal_object_store.clone(),
            self.replication_params.as_ref(),
        )
        .await
        .context("Failed to open database")?;

        let SlateDbOpen {
            data: slatedb,
            metrics_recorder,
            parts_cache,
        } = opened;

        if let Some(opening) = self.opening.as_ref() {
            let owns_opening = crate::replication::leader_record::validate_opening(opening)
                .await
                .context("HA: validating Opening after database open failed")?;
            if !owns_opening {
                if let SlateDbHandle::ReadWrite(raw_db) = &slatedb
                    && let Err(error) = raw_db.close().await
                {
                    tracing::warn!(
                        "HA: closing a writer whose Opening ownership was superseded returned: \
                         {error}"
                    );
                }
                tracing::warn!(
                    "HA: durable Opening ownership was superseded during database open; \
                     returning to role election"
                );
                self.opening.take();
                return Ok(OpenOutcome::RetryRole);
            }
        }

        // Atomically fence receiver admission and take ownership of the standby
        // tail. Later status manifests may belong to a writer that fenced us.
        let promotion =
            if let (SlateDbHandle::ReadWrite(raw_db), Some(receiver)) = (&slatedb, &self.ha) {
                let writer_epoch = raw_db.subscribe().borrow().current_manifest.writer_epoch();
                match receiver.control.begin_promotion(writer_epoch).await {
                    Ok(promotion) => Some(promotion),
                    Err(error)
                        if error
                            .downcast_ref::<crate::replication::transport::PromotionSuperseded>()
                            .is_some() =>
                    {
                        let superseded = error
                            .downcast_ref::<crate::replication::transport::PromotionSuperseded>()
                            .expect("guard checked the error type");
                        tracing::warn!(
                            "HA: local writer epoch {} is behind observed peer epoch {}; \
                         closing it and retrying under the same durable Opening capability",
                            superseded.writer_epoch,
                            superseded.observed_epoch
                        );
                        let still_owns_opening = self
                            .close_writer_for_retry(
                                &slatedb,
                                "HA: revalidating Opening before writer retry failed",
                            )
                            .await?;
                        return Ok(if still_owns_opening {
                            OpenOutcome::RetryWriter
                        } else {
                            OpenOutcome::RetryRole
                        });
                    }
                    Err(error) => {
                        return Err(error)
                            .context("HA: freezing the standby tail for promotion failed");
                    }
                }
            } else {
                None
            };

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
            self.retrying_object_store.clone(),
            parts_cache,
        ));
        let db_prefix = Path::from(self.actual_db_path.clone());
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

        Ok(OpenOutcome::Opened(DbOpen {
            promotion,
            slatedb,
            metrics_recorder,
            segment_object_store,
            segment_warm,
        }))
    }
}

impl DbOpen {
    /// Reconcile a standby's frozen tail before serving.
    async fn reconcile_tail(mut self, startup: &mut StartupContext) -> Result<ReconcileOutcome> {
        let (lineage_proof, retry_grace_proof) = match self.promotion.take() {
            Some(promotion) => {
                let SlateDbHandle::ReadWrite(raw_db) = &self.slatedb else {
                    anyhow::bail!("HA promotion requires a writable data db");
                };
                match promotion
                    .reconcile_into(raw_db, &self.segment_object_store, &startup.segment_codec)
                    .await?
                {
                    crate::replication::ReconcileOutcome::Promoted {
                        lineage_proof,
                        retry_grace_proof,
                    } => {
                        // Retry grace applies only after standby takeover.
                        let retry_grace_proof = if startup.took_over_from_standby {
                            retry_grace_proof
                        } else {
                            None
                        };
                        (lineage_proof, retry_grace_proof)
                    }
                    crate::replication::ReconcileOutcome::RetryRole {
                        writer_epoch,
                        observed_epoch,
                    } => {
                        tracing::warn!(
                            "HA: promotion at writer epoch {writer_epoch} lost to observed \
                             epoch {observed_epoch}; closing the stale writer and retrying \
                             under the same durable Opening capability"
                        );
                        let still_owns_opening = startup
                            .close_writer_for_retry(
                                &self.slatedb,
                                "HA: revalidating Opening after reconciliation retry failed",
                            )
                            .await?;
                        return Ok(if still_owns_opening {
                            ReconcileOutcome::RetryWriter
                        } else {
                            ReconcileOutcome::RetryRole
                        });
                    }
                }
            }
            None => (None, None),
        };

        Ok(ReconcileOutcome::Reconciled(Box::new(ReconciledDb {
            open: self,
            lineage_proof,
            retry_grace_proof,
        })))
    }
}

impl ReconciledDb {
    async fn into_filesystem(
        self,
        mut startup: StartupContext,
        settings: &Settings,
    ) -> Result<InitResult> {
        let ReconciledDb {
            open,
            lineage_proof,
            retry_grace_proof,
        } = self;
        let DbOpen {
            promotion: _,
            slatedb,
            metrics_recorder,
            segment_object_store,
            segment_warm,
        } = open;
        // Activation requires the Opening token and a reconciled tail.
        let ownership = match startup.opening.take() {
            Some(opening) => {
                let writer_epoch = match &slatedb {
                    SlateDbHandle::ReadWrite(db) => {
                        db.subscribe().borrow().current_manifest.writer_epoch()
                    }
                    SlateDbHandle::ReadOnly(_) => {
                        anyhow::bail!("HA marker activation requires a writable data db")
                    }
                };
                Some(
                    crate::replication::leader_record::activate(opening, writer_epoch)
                        .await
                        .context("HA: activating durable writer ownership failed")?,
                )
            }
            None => None,
        };
        let StartupContext {
            object_store,
            retrying_object_store,
            wal_object_store,
            object_tracer,
            actual_db_path,
            block_transformer: _,
            segment_codec,
            cache_config: _,
            dedup,
            replication_params,
            configured_replication_role: _,
            db_mode,
            ha,
            took_over_from_standby: _,
            recovering_handoff: _,
            opening: _,
        } = startup;

        let serving_writer_epoch = match &slatedb {
            SlateDbHandle::ReadWrite(db) => db.subscribe().borrow().current_manifest.writer_epoch(),
            SlateDbHandle::ReadOnly(_) => 0,
        };
        // Initial authority comes from an exact Active-marker read. Current-epoch
        // standby acknowledgements provide the steady-state authority signal.
        let pending_authority = match (replication_params.as_ref(), ownership) {
            (Some(_), Some(ownership)) if !db_mode.is_read_only() => {
                let lease = crate::replication::Lease::new();
                crate::replication::activate_lease_from_marker(
                    &lease,
                    &object_store,
                    &actual_db_path,
                    &ownership,
                )
                .await
                .context("HA: fresh Active-marker validation failed")?;

                let status = match &slatedb {
                    SlateDbHandle::ReadWrite(raw_db) => raw_db.subscribe(),
                    SlateDbHandle::ReadOnly(_) => {
                        anyhow::bail!("HA Active ownership requires a writable data db")
                    }
                };
                Some((lease, ownership, status))
            }
            (Some(_), None) if !db_mode.is_read_only() => {
                anyhow::bail!("HA writer reached serving assembly without Active ownership")
            }
            (None, Some(_)) => {
                anyhow::bail!("non-HA startup unexpectedly acquired HA marker ownership")
            }
            _ => None,
        };

        // Leader replicator: ships to the standby when connected, runs solo
        // otherwise, reconnecting when it reappears. Never blocks startup or writes.
        let (replicator, replication_control) = match replication_params.as_ref() {
            Some(params)
                if !db_mode.is_read_only() && params.is_leader() && !params.peers.is_empty() =>
            {
                let peer = params.peers[0].clone();
                let endpoint = if peer.starts_with("http://") || peer.starts_with("https://") {
                    peer
                } else {
                    format!("http://{peer}")
                };
                // Tag every ship with this leader's data-db writer epoch, so a
                // deposed leader ships a lower epoch and the standby rejects it.
                let writer_epoch = serving_writer_epoch;
                let typed_writer_epoch = crate::replication::types::WriterEpoch::new(writer_epoch)
                    .context("HA writer opened with an invalid zero writer epoch")?;
                info!(
                    "HA leader: replicating to standby peer {} (writer epoch {})",
                    endpoint, writer_epoch
                );
                let (replicator, control) =
                    crate::replication::Replicator::new(endpoint.clone(), typed_writer_epoch);
                // Heartbeat acknowledgements require coverage of the local
                // applied frontier for this writer epoch.
                let hb_endpoint = endpoint.clone();
                let heartbeat_control = control.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = heartbeat_control.deposed() => {
                                tracing::warn!(
                                    "HA: stopping outbound heartbeats for deposed epoch \
                                     {writer_epoch}"
                                );
                                return;
                            }
                            exit = crate::replication::transport::run_heartbeat_sender(
                                hb_endpoint.clone(),
                                typed_writer_epoch,
                                crate::replication::COVERAGE_HEARTBEAT_INTERVAL,
                                heartbeat_control.clone(),
                            ) => {
                                match exit {
                                    crate::replication::transport::HeartbeatExit::BaseNotCovered(status) => {
                                        tracing::warn!(
                                            "HA: standby heartbeat rejected an uncovered applied \
                                             replication base; repairing durability ({status})"
                                        );
                                        // Base repair is serialized after outstanding ship
                                        // and apply permits.
                                        if let Err(error) = heartbeat_control.repair_base().await {
                                            tracing::error!(
                                                "HA: receiver-base repair could not reach the commit \
                                                 sequencer: {error:#}"
                                            );
                                            return;
                                        }
                                        continue;
                                    }
                                    crate::replication::transport::HeartbeatExit::ProtocolIncompatible(status) => {
                                        tracing::error!(
                                            "HA: heartbeat sender stopped after a terminal peer error \
                                             ({status})"
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                });
                tokio::spawn(crate::replication::replicator::run_reconnect(
                    Arc::downgrade(&control),
                ));
                // Drive the standby's prune watermark from the data db's durable_seq.
                if let SlateDbHandle::ReadWrite(raw_db) = &slatedb {
                    tokio::spawn(crate::replication::replicator::run_watermark(
                        control.clone(),
                        raw_db.subscribe(),
                    ));
                }
                (Some(replicator), Some(control))
            }
            _ => (None, None),
        };

        if let (Some(receiver), Some((lease, ..))) = (&ha, &pending_authority) {
            receiver
                .control
                .attach_serving_lease(serving_writer_epoch, lease);
        }

        // Configuration permits one acknowledging standby.
        let peer_acks = replication_control
            .as_ref()
            .map(|control| control.heartbeat_acks());

        let authority = match pending_authority {
            Some((lease, ownership, status)) => Some(
                crate::replication::AuthoritySupervisor::start(
                    lease,
                    object_store.clone(),
                    actual_db_path.clone(),
                    ownership,
                    status,
                    replication_control,
                    peer_acks,
                )
                .context("HA: starting serving authority supervisor failed")?,
            ),
            None => None,
        };
        let lease = authority.as_ref().map(|authority| authority.lease());

        let sync_writes = settings.lsm.map(|c| c.sync_writes()).unwrap_or(false);
        let ignore_fsync = settings
            .filesystem
            .as_ref()
            .is_some_and(|filesystem| filesystem.ignore_fsync);
        if ignore_fsync {
            tracing::warn!(
                "[filesystem] ignore_fsync is set: fsync/COMMIT will NOT force a flush to \
                 object storage; durability of un-flushed writes then relies on replication, \
                 and without it un-flushed writes are lost on any crash"
            );
        }

        let db_handle = slatedb.clone();
        let fs = ZeroFS::new_with_slatedb_and_lease(
            slatedb,
            settings.max_bytes(),
            metrics_recorder,
            sync_writes,
            ignore_fsync,
            lease,
            replicator,
            dedup,
            lineage_proof,
            object_tracer.clone(),
            segment_object_store,
            segment_codec,
            segment_warm,
            None,
        )
        .await
        .context("Failed to initialize filesystem")?;

        let fs = Arc::new(fs);
        // Reclaims open-unlinked inodes once their last open handle is dropped.
        fs.start_reclaim_drainer();

        // Arm retry grace only after successful filesystem construction.
        if let Some(proof) = retry_grace_proof
            && !proof.arm()
        {
            tracing::info!(
                "HA takeover retry grace elapsed during filesystem initialization; unseen \
                 predecessor retries remain stale"
            );
        }

        Ok(InitResult {
            fs,
            // Retry-wrapped for the consumers downstream (the GC's checkpoint-gate
            // admin and the checkpoint manager), whose listings would otherwise
            // fail on one transient backend error.
            object_store: retrying_object_store,
            wal_object_store,
            db_path: actual_db_path,
            db_handle,
            authority,
        })
    }
}

/// Run role election, database open, reconciliation, and activation.
pub async fn initialize_filesystem(
    settings: &Settings,
    db_mode: DatabaseMode,
) -> Result<InitResult> {
    let mut startup = StartupContext::prepare(settings, db_mode)
        .await?
        .start_receiver()?;
    'role_election: loop {
        startup.become_writer().await?;
        startup.opening = match startup.claim_opening().await? {
            ClaimOutcome::Claimed(opening) => opening,
            ClaimOutcome::RetryRole => continue,
        };
        loop {
            let db = match startup.open_db(settings).await? {
                OpenOutcome::Opened(db) => db,
                OpenOutcome::RetryWriter => continue,
                OpenOutcome::RetryRole => continue 'role_election,
            };
            match db.reconcile_tail(&mut startup).await? {
                ReconcileOutcome::Reconciled(db) => {
                    return (*db).into_filesystem(startup, settings).await;
                }
                ReconcileOutcome::RetryRole => continue 'role_election,
                ReconcileOutcome::RetryWriter => continue,
            }
        }
    }
}

#[cfg(test)]
mod role_decision_tests {
    use super::{ImmediateRoleDecision, immediate_role_decision};

    #[test]
    fn unresolved_handoff_uses_recovery_even_if_peer_still_answers_active() {
        assert_eq!(
            immediate_role_decision(true, true),
            Some(ImmediateRoleDecision::RecoverHandoff)
        );
        assert_eq!(
            immediate_role_decision(false, true),
            Some(ImmediateRoleDecision::FollowActivePeer)
        );
        assert_eq!(immediate_role_decision(false, false), None);
    }
}
