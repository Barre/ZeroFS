//! World orchestration: build the filesystem over the simulated store, run
//! concurrent rounds, and drive the clean or crash-recovery lifecycle.

use crate::actor::{ActorContext, FloorSlot, Floors};
use crate::checks::Checks;
use crate::consistency::{verify_consistency_sparse, verify_consistency_sparse_online};
use crate::data::{FileOp, FileOpMix, FileSnapshot, FileState, Region, pattern};
use crate::digest::Digest;
#[cfg(feature = "failpoints")]
use crate::fp_crash;
use crate::gc::gc_round;
use crate::namespace::{NamespaceSnapshot, NsModel};
use crate::sim::{SimClock, SimStore};
use crate::{FILES, Scale, auth, creds, env_or, scale_for};
use bytes::Bytes;
use chrono::Utc;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::DbBuilder;
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use zerofs::db::SlateDbHandle;
use zerofs::fs::inode::InodeId;
use zerofs::fs::types::{SetAttributes, SetSize};
use zerofs::fs::{EXTENT_SIZE, ZeroFS};

struct WorldConfig {
    seed: u64,
    failpoint_crashes: bool,
    rounds: usize,
    ops_per_file: usize,
    gc_passes: usize,
    crash_pct: usize,
    fault_ppm: u32,
    scale: Scale,
    file_mix: FileOpMix,
}

impl WorldConfig {
    fn from_env(seed: u64, failpoint_crashes: bool, rng: &mut StdRng) -> Self {
        let fault_ppm =
            [0u32, 0, 2_000, 10_000][StdRng::seed_from_u64(seed ^ 0xFA17).gen_range(0..4)];
        let write = 30 + rng.gen_range(0..30u32);
        let mutation = rng.gen_range(3..20u32);
        let read = 10 + rng.gen_range(0..20u32);
        let fsync = rng.gen_range(1..12u32);
        Self {
            seed,
            failpoint_crashes,
            rounds: env_or("DST_ROUNDS", 4),
            ops_per_file: env_or("DST_OPS", 50),
            gc_passes: env_or("DST_GC", 6),
            crash_pct: env_or("DST_CRASH_PCT", 70).min(100),
            fault_ppm,
            scale: scale_for(seed),
            file_mix: FileOpMix::new(write, mutation, read, fsync),
        }
    }

    fn round_seed(&self, round: usize) -> u64 {
        self.seed ^ ((round as u64) << 8)
    }

    fn file_seed(&self, round: usize, index: usize) -> u64 {
        self.round_seed(round) ^ ((index as u64) << 4) ^ 1
    }

    fn gc_seed(&self, round: usize) -> u64 {
        self.round_seed(round) ^ 2
    }

    fn namespace_seed(&self, round: usize) -> u64 {
        self.round_seed(round) ^ 3
    }

    fn region_seed(&self, round: usize, index: usize) -> u64 {
        self.round_seed(round) ^ ((index as u64) << 4) ^ 4
    }

    fn reopen_seed(&self, round: usize) -> u64 {
        self.seed ^ (round as u64 + 1)
    }
}

enum Incarnation {
    Live { sim: Arc<SimStore>, fs: Arc<ZeroFS> },
    Reopening,
}

/// The durable backing store and the currently live filesystem incarnation.
struct Storage {
    backing: Arc<dyn ObjectStore>,
    clock: Arc<SimClock>,
    incarnation: Incarnation,
}

impl Storage {
    async fn new(config: &WorldConfig, digest: &Digest) -> Self {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let clock = Arc::new(SimClock::new());
        let sim = Arc::new(SimStore::new(
            backing.clone(),
            config.seed ^ 0x5157,
            config.fault_ppm,
            digest.clone(),
        ));
        let fs = Self::open(sim.clone(), clock.clone(), config.seed, config.scale).await;
        Self {
            backing,
            clock,
            incarnation: Incarnation::Live { sim, fs },
        }
    }

    async fn open(
        sim: Arc<SimStore>,
        clock: Arc<SimClock>,
        seed: u64,
        scale: Scale,
    ) -> Arc<ZeroFS> {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(zerofs::retrying_object_store::RetryingObjectStore::new(sim));
        let settings = slatedb::config::Settings {
            wal_enabled: false,
            l0_sst_size_bytes: usize::MAX,
            l0_max_ssts: 256,
            l0_max_ssts_per_key: 256,
            ..Default::default()
        };
        let slatedb = Arc::new(
            DbBuilder::new(ObjPath::from("slatedb"), Arc::clone(&object_store))
                .with_settings(settings)
                .with_seed(seed)
                .with_system_clock(clock)
                .with_db_cache_disabled()
                .with_filter_policies(zerofs::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(zerofs::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .expect("slatedb open"),
        );
        let fs = Arc::new(
            ZeroFS::new_with_slatedb_and_lease(
                SlateDbHandle::ReadWrite(slatedb),
                u64::MAX,
                None,
                false,
                false,
                None,
                None,
                Arc::new(zerofs::dedup::DedupCache::new()),
                None,
                zerofs::object_trace::ObjectTracer::new(),
                object_store,
                zerofs::frame_codec::FrameCodec::new(
                    &[7u8; 32],
                    zerofs::segment::SEGMENT_INFO,
                    zerofs::config::CompressionConfig::default(),
                ),
                None,
                Some(scale.seal_threshold),
            )
            .await
            .expect("zerofs open"),
        );
        fs.start_reclaim_drainer();
        fs.extent_store.enable_nominations();
        fs
    }

    fn fs(&self) -> &Arc<ZeroFS> {
        match &self.incarnation {
            Incarnation::Live { fs, .. } => fs,
            Incarnation::Reopening => panic!("filesystem accessed while reopening"),
        }
    }

    fn sim(&self) -> &Arc<SimStore> {
        match &self.incarnation {
            Incarnation::Live { sim, .. } => sim,
            Incarnation::Reopening => panic!("sim store accessed while reopening"),
        }
    }

    async fn crash_and_reopen(&mut self, config: &WorldConfig, digest: &Digest, round: usize) {
        let old = std::mem::replace(&mut self.incarnation, Incarnation::Reopening);
        let Incarnation::Live { sim, fs } = old else {
            panic!("attempted to crash while already reopening");
        };

        // Isolation is permanent for this wrapper. The next incarnation gets
        // a fresh wrapper over the same durable backing store.
        sim.hooks.isolated.store(true, Relaxed);
        drop(fs);
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        // Virtual time: give abandoned tasks time to wake, observe the dead
        // store, and exit before the next incarnation starts.
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(sim);

        let reopen_seed = config.reopen_seed(round);
        let sim = Arc::new(SimStore::new(
            self.backing.clone(),
            config.seed ^ 0x5157 ^ (round as u64 + 1),
            config.fault_ppm,
            digest.clone(),
        ));
        let fs = Self::open(sim.clone(), self.clock.clone(), reopen_seed, config.scale).await;
        self.incarnation = Incarnation::Live { sim, fs };
    }

    /// Reopen the cleanly closed filesystem over the same backing store.
    async fn reopen_after_close(&mut self, config: &WorldConfig, digest: &Digest) {
        let old = std::mem::replace(&mut self.incarnation, Incarnation::Reopening);
        let Incarnation::Live { sim, fs } = old else {
            panic!("attempted to reopen while already reopening");
        };
        drop(fs);
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        drop(sim);
        let sim = Arc::new(SimStore::new(
            self.backing.clone(),
            config.seed ^ 0x5157 ^ 0xC105E,
            config.fault_ppm,
            digest.clone(),
        ));
        let fs = Self::open(
            sim.clone(),
            self.clock.clone(),
            config.seed ^ 1,
            config.scale,
        )
        .await;
        self.incarnation = Incarnation::Live { sim, fs };
    }
}

struct WorldModel {
    files: Vec<FileState>,
    namespace: NsModel,
    regions: Vec<Region>,
    shared_id: InodeId,
}

impl WorldModel {
    async fn create(fs: &ZeroFS, config: &WorldConfig) -> Self {
        let creds = creds();
        let auth = auth();
        let mut files = Vec::with_capacity(FILES);
        for index in 0..FILES {
            let name = format!("f{index}").into_bytes();
            let (id, _) = fs
                .create(&creds, 0, &name, &SetAttributes::default())
                .await
                .expect("create");
            let tag = config.seed ^ index as u64;
            let op = FileOp::Write {
                offset: 0,
                len: 1024,
                tag,
            };
            fs.write(&auth, id, 0, &Bytes::from(pattern(tag, 1024)))
                .await
                .expect("seed write");
            files.push(FileState::with_initial_op(id, name, op));
        }

        // Namespace mutations stay in a subtree untouched by data writers.
        let (namespace_root, _) = fs
            .mkdir(&creds, 0, b"ns", &SetAttributes::default())
            .await
            .expect("ns root mkdir");

        // Two writers own disjoint halves of one size-pinned file.
        let cap = config.scale.file_cap;
        let half = (cap / 2 / EXTENT_SIZE) * EXTENT_SIZE;
        let (shared_id, _) = fs
            .create(&creds, 0, b"shared", &SetAttributes::default())
            .await
            .expect("shared create");
        fs.setattr(
            &creds,
            shared_id,
            &SetAttributes {
                size: SetSize::Set(cap as u64),
                ..Default::default()
            },
        )
        .await
        .expect("shared truncate");
        fs.client_fsync().await.expect("setup fsync");
        for file in &mut files {
            file.fsynced = file.ops.len();
        }

        let regions = [(0usize, half), (half, cap - half)]
            .into_iter()
            .map(|(base, len)| Region::new(shared_id, base, len, cap))
            .collect();
        Self {
            files,
            namespace: NsModel::new(namespace_root),
            regions,
            shared_id,
        }
    }
}

/// Named access to the fs-wide durability floors shared by every actor.
struct FloorRegistry {
    inner: Arc<Floors>,
    namespace_slot: FloorSlot,
    region_slot_start: usize,
}

impl FloorRegistry {
    fn new(model: &WorldModel) -> Self {
        let namespace_index = model.files.len();
        let region_slot_start = namespace_index + 1;
        let inner = Arc::new(Floors::new(
            model
                .files
                .iter()
                .map(|file| (file.acked, file.fsynced))
                .chain(std::iter::once((
                    model.namespace.acked,
                    model.namespace.fsynced,
                )))
                .chain(
                    model
                        .regions
                        .iter()
                        .map(|region| (region.acked, region.fsynced)),
                ),
        ));
        Self {
            inner,
            namespace_slot: FloorSlot::new(namespace_index),
            region_slot_start,
        }
    }

    fn handle(&self) -> Arc<Floors> {
        self.inner.clone()
    }

    fn file_slot(&self, index: usize) -> FloorSlot {
        FloorSlot::new(index)
    }

    fn namespace_slot(&self) -> FloorSlot {
        self.namespace_slot
    }

    fn region_slot(&self, index: usize) -> FloorSlot {
        FloorSlot::new(self.region_slot_start + index)
    }

    fn fold_fsyncs_into(&self, model: &mut WorldModel) {
        for (index, file) in model.files.iter_mut().enumerate() {
            file.fsynced = file.fsynced.max(self.inner.floor(self.file_slot(index)));
        }
        model.namespace.fsynced = model
            .namespace
            .fsynced
            .max(self.inner.floor(self.namespace_slot));
        for (index, region) in model.regions.iter_mut().enumerate() {
            region.fsynced = region
                .fsynced
                .max(self.inner.floor(self.region_slot(index)));
        }
    }

    fn reanchor(&self, model: &WorldModel) {
        for (index, file) in model.files.iter().enumerate() {
            self.inner
                .reanchor(self.file_slot(index), file.acked, file.fsynced);
        }
        self.inner.reanchor(
            self.namespace_slot,
            model.namespace.acked,
            model.namespace.fsynced,
        );
        for (index, region) in model.regions.iter().enumerate() {
            self.inner
                .reanchor(self.region_slot(index), region.acked, region.fsynced);
        }
    }
}

/// One round's crash signal, timer, and optional failpoint arm.
struct CrashTrigger {
    sender: tokio::sync::watch::Sender<bool>,
    receiver: tokio::sync::watch::Receiver<bool>,
    timer: Option<tokio::task::JoinHandle<()>>,
    failpoint_mode: bool,
}

impl CrashTrigger {
    fn arm(config: &WorldConfig, rng: &mut StdRng, sim: &SimStore) -> Self {
        #[cfg(not(feature = "failpoints"))]
        let _ = sim;
        // The sender must outlive the actors even when no killer is armed: a
        // closed watch channel would otherwise look like a crash cancellation.
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let timer = if config.failpoint_crashes {
            #[cfg(feature = "failpoints")]
            {
                let point = fp_crash::POINTS[rng.gen_range(0..fp_crash::POINTS.len())];
                let hits_left = rng.gen_range(1..=6);
                let isolated = sim.hooks.isolated.clone();
                let tx = sender.clone();
                *fp_crash::ARMED.lock().unwrap() = Some(fp_crash::Armed {
                    point,
                    thread: std::thread::current().id(),
                    hits_left,
                    fire: Box::new(move || {
                        isolated.store(true, Relaxed);
                        let _ = tx.send(true);
                    }),
                });

                let widen_ms = [0u64, 0, 10, 30][rng.gen_range(0..4)];
                let widen_point =
                    fp_crash::WIDEN_POINTS[rng.gen_range(0..fp_crash::WIDEN_POINTS.len())];
                *zerofs::failpoints::WIDEN.lock().unwrap() =
                    (widen_ms > 0).then(|| (widen_point, std::thread::current().id(), widen_ms));
            }
            None
        } else if rng.gen_bool(config.crash_pct as f64 / 100.0) {
            let delay = Duration::from_millis(rng.gen_range(3..=250));
            let tx = sender.clone();
            Some(tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx.send(true);
            }))
        } else {
            None
        };
        Self {
            sender,
            receiver,
            timer,
            failpoint_mode: config.failpoint_crashes,
        }
    }

    fn receiver(&self) -> tokio::sync::watch::Receiver<bool> {
        self.receiver.clone()
    }

    fn finish(mut self) -> bool {
        let crashed = *self.receiver.borrow();
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        if self.failpoint_mode {
            #[cfg(feature = "failpoints")]
            {
                *fp_crash::ARMED.lock().unwrap() = None;
                *zerofs::failpoints::WIDEN.lock().unwrap() = None;
            }
        }
        drop(self.sender);
        crashed
    }
}

struct RoundOutcome {
    model: WorldModel,
    crashed: bool,
}

struct WorldHarness {
    config: WorldConfig,
    digest: Digest,
    storage: Storage,
    floors: FloorRegistry,
    rng: StdRng,
}

impl WorldHarness {
    async fn new(seed: u64, failpoint_crashes: bool) -> (Self, WorldModel) {
        let mut rng = StdRng::seed_from_u64(seed ^ 0xD57);
        let config = WorldConfig::from_env(seed, failpoint_crashes, &mut rng);
        let digest = Digest::default();
        let storage = Storage::new(&config, &digest).await;
        let model = WorldModel::create(storage.fs().as_ref(), &config).await;
        let floors = FloorRegistry::new(&model);
        (
            Self {
                config,
                digest,
                storage,
                floors,
                rng,
            },
            model,
        )
    }

    async fn run(mut self, mut model: WorldModel) -> (u64, Vec<String>) {
        for round in 0..self.config.rounds {
            model = self.run_round(round, model).await;
        }
        self.digest.finish()
    }

    fn actor_context(
        &self,
        fs: &Arc<ZeroFS>,
        slot: FloorSlot,
        trigger: &CrashTrigger,
    ) -> ActorContext {
        ActorContext::new(
            fs.clone(),
            self.digest.clone(),
            self.floors.handle(),
            slot,
            trigger.receiver(),
        )
    }

    async fn run_round(&mut self, round: usize, model: WorldModel) -> WorldModel {
        let RoundOutcome { mut model, crashed } = self.run_actors(round, model).await;
        self.floors.fold_fsyncs_into(&mut model);
        if crashed {
            self.recover(round, model).await
        } else {
            self.verify_quiescent(round, &model).await;
            model
        }
    }

    async fn run_actors(&mut self, round: usize, model: WorldModel) -> RoundOutcome {
        let WorldModel {
            files,
            namespace,
            regions,
            shared_id,
        } = model;
        let trigger = CrashTrigger::arm(&self.config, &mut self.rng, self.storage.sim());
        let fs = self.storage.fs().clone();

        let mut file_handles = Vec::with_capacity(files.len());
        for (index, file) in files.into_iter().enumerate() {
            file_handles.push(tokio::spawn(file.run_round(
                StdRng::seed_from_u64(self.config.file_seed(round, index)),
                self.config.ops_per_file,
                self.config.file_mix,
                self.config.scale.file_cap,
                self.actor_context(&fs, self.floors.file_slot(index), &trigger),
            )));
        }
        let gc_handle = tokio::spawn(gc_round(
            fs.clone(),
            StdRng::seed_from_u64(self.config.gc_seed(round)),
            self.config.gc_passes,
            self.digest.clone(),
            trigger.receiver(),
        ));
        let namespace_handle = tokio::spawn(namespace.run_round(
            StdRng::seed_from_u64(self.config.namespace_seed(round)),
            self.config.ops_per_file,
            self.actor_context(&fs, self.floors.namespace_slot(), &trigger),
        ));
        let mut region_handles = Vec::with_capacity(regions.len());
        for (index, region) in regions.into_iter().enumerate() {
            region_handles.push(tokio::spawn(region.run_round(
                StdRng::seed_from_u64(self.config.region_seed(round, index)),
                self.config.ops_per_file,
                self.actor_context(&fs, self.floors.region_slot(index), &trigger),
            )));
        }

        let mut files = Vec::with_capacity(file_handles.len());
        for handle in file_handles {
            files.push(handle.await.expect("writer actor"));
        }
        gc_handle.await.expect("gc actor");
        let namespace = namespace_handle.await.expect("ns actor");
        let mut regions = Vec::with_capacity(region_handles.len());
        for handle in region_handles {
            regions.push(handle.await.expect("region actor"));
        }

        RoundOutcome {
            model: WorldModel {
                files,
                namespace,
                regions,
                shared_id,
            },
            crashed: trigger.finish(),
        }
    }

    async fn verify_quiescent(&self, round: usize, model: &WorldModel) {
        let fs = self.storage.fs();
        for file in &model.files {
            assert_eq!(
                file.ops.len(),
                file.acked,
                "clean round left an attempted op dangling (file {})",
                file.id
            );
            let observed = FileSnapshot::read(fs.as_ref(), file.id).await;
            assert_eq!(
                observed.as_bytes(),
                file.cur.as_slice(),
                "quiesced content mismatch (seed {}, round {round}, file {})",
                self.config.seed,
                file.id
            );
            file.verify_extent_keys(fs.as_ref(), self.config.seed, round)
                .await;
            self.digest.event(("q", file.id, observed.len()));
        }
        Checks::new(fs.as_ref(), self.config.seed, round)
            .verify()
            .await;

        let report = verify_consistency_sparse_online(fs)
            .await
            .expect("quiesced consistency scan");
        assert!(
            report.is_consistent(),
            "consistency errors at quiesce (seed {}, round {round}):\n{report}",
            self.config.seed
        );

        let observed_namespace = NamespaceSnapshot::read(fs, model.namespace.root).await;
        model
            .namespace
            .verify_quiescent(&observed_namespace, self.config.seed, round);
        self.digest.event(("nsq", observed_namespace.len()));

        for region in &model.regions {
            assert_eq!(
                region.ops.len(),
                region.acked,
                "clean round left an attempted region op dangling (inode {} [{}, {}))",
                region.id,
                region.base,
                region.base + region.len
            );
        }
        let shared = FileSnapshot::read(fs.as_ref(), model.shared_id).await;
        assert_eq!(
            shared.len(),
            self.config.scale.file_cap,
            "shared file size drifted (seed {}, round {round}): {} != {}",
            self.config.seed,
            shared.len(),
            self.config.scale.file_cap
        );
        for region in &model.regions {
            assert_eq!(
                &shared.as_bytes()[region.base..region.base + region.len],
                &region.cur[region.base..region.base + region.len],
                "quiesced region mismatch (seed {}, round {round}, inode {} [{}, {}))",
                self.config.seed,
                region.id,
                region.base,
                region.base + region.len
            );
        }
        self.digest.event(("rq", model.shared_id, shared.len()));
    }

    async fn recover(&mut self, round: usize, mut model: WorldModel) -> WorldModel {
        self.storage
            .crash_and_reopen(&self.config, &self.digest, round)
            .await;
        self.digest.event(("crashed", round));
        let fs = self.storage.fs().clone();

        fs.extent_store
            .sweep_orphans(Utc::now() + chrono::Duration::days(1))
            .await
            .expect("orphan sweep");
        let report = verify_consistency_sparse(&fs)
            .await
            .expect("consistency scan");
        assert!(
            report.is_consistent(),
            "consistency errors after crash (seed {}, round {round}):\n{report}",
            self.config.seed
        );

        let mut files = Vec::with_capacity(model.files.len());
        for file in model.files {
            let observed = FileSnapshot::read(fs.as_ref(), file.id).await;
            self.digest.event(("c", file.id, observed.len()));
            files.push(file.reconcile_after_crash(observed, self.config.seed, round));
        }
        model.files = files;

        let observed_namespace = NamespaceSnapshot::read(&fs, model.namespace.root).await;
        self.digest.event(("nsc", observed_namespace.len()));
        model
            .namespace
            .reconcile_after_crash(&observed_namespace, self.config.seed, round);

        let shared = FileSnapshot::read(fs.as_ref(), model.shared_id).await;
        assert_eq!(
            shared.len(),
            self.config.scale.file_cap,
            "shared file size drifted after crash (seed {}, round {round}): {} != {}",
            self.config.seed,
            shared.len(),
            self.config.scale.file_cap
        );
        self.digest.event(("rc", model.shared_id, shared.len()));
        model.regions = model
            .regions
            .into_iter()
            .map(|region| region.reconcile_after_crash(&shared, self.config.seed, round))
            .collect();

        self.floors.reanchor(&model);
        for file in &model.files {
            file.verify_extent_keys(fs.as_ref(), self.config.seed, round)
                .await;
        }
        Checks::new(fs.as_ref(), self.config.seed, round)
            .verify()
            .await;
        model
    }
}

pub(crate) fn run_seed(seed: u64) -> (u64, Vec<String>) {
    run_seed_mode(seed, false)
}

/// Cover an acknowledged write after the last ordinary flush, then race a write
/// against close. An acknowledged write must survive; a rejected one must not.
pub(crate) fn run_graceful_close_case(seed: u64) {
    zerofs::fs::DST_FIXED_TIME.store(true, Relaxed);
    zerofs::db::DST_PANIC_ON_WRITE_ERROR.store(true, Relaxed);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()))
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let digest = Digest::default();
        let config = WorldConfig {
            seed,
            failpoint_crashes: false,
            rounds: 0,
            ops_per_file: 0,
            gc_passes: 0,
            crash_pct: 0,
            fault_ppm: 0,
            scale: scale_for(seed),
            file_mix: FileOpMix::new(1, 1, 1, 1),
        };
        let mut storage = Storage::new(&config, &digest).await;
        let creds = creds();
        let auth = auth();

        let fs = storage.fs().clone();
        let (id, _) = fs
            .create(&creds, 0, b"graceful", &SetAttributes::default())
            .await
            .expect("create");
        let flushed = pattern(seed, EXTENT_SIZE);
        fs.write(&auth, id, 0, &Bytes::from(flushed.clone()))
            .await
            .expect("flushed write");
        fs.client_fsync().await.expect("flush");
        let late = pattern(seed ^ 1, EXTENT_SIZE);
        fs.write(&auth, id, EXTENT_SIZE as u64, &Bytes::from(late.clone()))
            .await
            .expect("late write");
        fs.flush_coordinator.close().await.expect("close");
        drop(fs);

        storage.reopen_after_close(&config, &digest).await;
        let fs = storage.fs();
        let (bytes, _) = fs
            .read_file(&auth, id, 0, 2 * EXTENT_SIZE as u32)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "read after graceful close failed (seed {seed}): {e:?} — \
                     a dangling extent pointer survived the close"
                )
            });
        let expected = [flushed.as_slice(), late.as_slice()].concat();
        assert_eq!(
            bytes.as_ref(),
            expected,
            "contents after close (seed {seed})"
        );
        let report = verify_consistency_sparse(fs)
            .await
            .expect("consistency scan");
        assert!(
            report.is_consistent(),
            "consistency errors after graceful close (seed {seed}):\n{report}"
        );

        // A racing write may win or be rejected, but may never leave a dangling
        // extent or disappear after being acknowledged.
        let fs = storage.fs().clone();
        let racer = pattern(seed ^ 2, EXTENT_SIZE);
        let racing_write = {
            let fs = fs.clone();
            let racer = Bytes::from(racer.clone());
            let auth = auth.clone();
            tokio::spawn(async move { fs.write(&auth, id, 2 * EXTENT_SIZE as u64, &racer).await })
        };
        fs.flush_coordinator.close().await.expect("second close");
        let raced = racing_write.await.expect("racing write task");
        drop(fs);

        storage.reopen_after_close(&config, &digest).await;
        let fs = storage.fs();
        let (bytes, _) = fs
            .read_file(&auth, id, 0, 3 * EXTENT_SIZE as u32)
            .await
            .unwrap_or_else(|e| panic!("read after racing close failed (seed {seed}): {e:?}"));
        let expected = match raced {
            Ok(_) => [flushed.as_slice(), late.as_slice(), racer.as_slice()].concat(),
            Err(_) => [flushed.as_slice(), late.as_slice()].concat(),
        };
        assert_eq!(
            bytes.as_ref(),
            expected,
            "racing close contents (seed {seed})"
        );
    });
}

pub(crate) fn run_seed_mode(seed: u64, failpoint_crashes: bool) -> (u64, Vec<String>) {
    zerofs::fs::DST_FIXED_TIME.store(true, Relaxed);
    zerofs::db::DST_PANIC_ON_WRITE_ERROR.store(true, Relaxed);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()))
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (harness, model) = WorldHarness::new(seed, failpoint_crashes).await;
        harness.run(model).await
    })
}
