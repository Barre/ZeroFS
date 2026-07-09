//! Deterministic simulation testing for the extent-store data plane.
//!
//! One seed = one world: a current-thread runtime with paused (virtual) time,
//! a seeded latency-injecting object store over a persistent `InMemory`, and
//! seeded writer/GC actors driving the real `ZeroFS` stack in its production
//! durability shape (WAL off, durable state only via the seal-gated flush).
//! Under `start_paused` every sleep is virtual, so the store's seeded per-op
//! latencies decide task wake order: one seed is one schedule, and the same
//! seed replays the same run (asserted by `same_seed_same_digest`).
//!
//! Each round runs concurrent writers (write/truncate/read/fsync per file)
//! against a GC actor doing full reclaim passes, then quiesces and may crash:
//! drop the fs without flushing, reopen over the surviving store, and check
//! the oracle: post-crash content must equal the reference model at some
//! op-prefix at least as new as the last acked fsync, every extent must still
//! resolve (full re-read), and the metadata invariants must hold.
//!
//! Env knobs: `DST_SEEDS=3,17` (explicit seeds, for reproduction),
//! `DST_WALL_CLOCK_SECS` (soak: fresh random seeds until the budget elapses;
//! without it, 8 fresh seeds; fanned across all cores), `DST_ROUNDS`,
//! `DST_OPS` (per file per round). All harness sleeps are whole
//! milliseconds; see `Hooks::latency` for why.

#[path = "../failpoints/consistency.rs"]
mod consistency;

use bytes::Bytes;
use chrono::Utc;
use consistency::verify_consistency_sparse;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::DbBuilder;
use slatedb::object_store::memory::InMemory;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zerofs::db::SlateDbHandle;
use zerofs::fs::inode::InodeId;
use zerofs::fs::permissions::Credentials;
use zerofs::fs::types::{AuthContext, SetAttributes, SetSize};
use zerofs::fs::{EXTENT_SIZE, ZeroFS};

/// Logical file size cap: enough extents that reclaim/compaction have real
/// material, small enough that a run stays cheap.
const FILE_CAP: usize = 16 * EXTENT_SIZE;
const FILES: usize = 3;
/// Small segments so the background-seal and reclaim paths run at test scale.
const SEAL_THRESHOLD: usize = 96 * 1024;

/// World scale. Most seeds run small and fast; every fourth runs at chain
/// scale, with segments above `SMALL_SEGMENT_BYTES` (1 MiB) so they are not
/// unconditional compaction candidates and the dense-segment paths (pair
/// heat, chain assembly) are reachable.
#[derive(Clone, Copy)]
struct Scale {
    file_cap: usize,
    seal_threshold: usize,
}

fn scale_for(seed: u64) -> Scale {
    if seed % 4 == 3 {
        Scale {
            file_cap: 64 * EXTENT_SIZE,
            seal_threshold: 3 * 1024 * 1024 / 2,
        }
    } else {
        Scale {
            file_cap: FILE_CAP,
            seal_threshold: SEAL_THRESHOLD,
        }
    }
}

fn env_or(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(v) => v
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer, got {v:?}")),
        Err(_) => default,
    }
}

/// The per-op instrumentation, split from the store so the wrapped delete and
/// list streams can carry it with a 'static lifetime.
#[derive(Clone)]
struct Hooks {
    rng: Arc<Mutex<StdRng>>,
    /// Latency ceiling in whole milliseconds (the timer-wheel grid; see
    /// `latency`); 0 degrades to yield-only (no timers).
    lat_max: u64,
    /// Transient-fault rate in parts per million, seed-varied per world.
    /// Under the production retry layer a transient never surfaces to the
    /// data plane; what it buys is retry-machinery coverage, effect-kept
    /// error-reported puts and deletes (CAS conflicts on retry, ObjectAbsent
    /// handling), and schedule perturbation.
    fault_ppm: u32,
    /// Crash fidelity: while set, every op fails fast. A real crash kills
    /// in-flight internal work (a background seal's PUT, a mid-stream delete);
    /// without this a dropped instance finishes its writes and no write-back
    /// state is ever lost. Set at crash time and never cleared: the next
    /// incarnation runs on a fresh wrapper, so a zombie task waking after any
    /// grace period still hits its own dead store.
    isolated: Arc<std::sync::atomic::AtomicBool>,
    /// Store ops are digest events too, so replay comparison covers the IO layer.
    digest: Digest,
    /// First-seen ordinals for generated path tokens (SST ULIDs), so traces
    /// compare structurally rather than by generated name.
    names: Arc<Mutex<std::collections::HashMap<String, usize>>>,
}

impl Hooks {
    fn normalize(&self, path: &ObjPath) -> String {
        let mut names = self.names.lock().unwrap();
        path.as_ref()
            .split('/')
            .map(|tok| {
                // ULID-shaped stem (26 alnum chars, extension aside): replace
                // with a first-seen index.
                let (stem, ext) = tok.split_once('.').unwrap_or((tok, ""));
                if stem.len() >= 20 && stem.chars().all(|c| c.is_ascii_alphanumeric()) {
                    let n = names.len();
                    let id = *names.entry(stem.to_string()).or_insert(n);
                    format!("#{id}.{ext}")
                } else {
                    tok.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    fn dead() -> object_store::Error {
        object_store::Error::Generic {
            store: "SimStore",
            source: "simulated crash: instance is dead".into(),
        }
    }

    fn transient() -> object_store::Error {
        object_store::Error::Generic {
            store: "SimStore",
            source: "injected transient fault".into(),
        }
    }

    /// Seeded transient-fault draw; the event makes injections visible in the
    /// trace and part of the replay proof.
    fn draw_fault(&self, kind: &'static str, op: &'static str, path: &ObjPath) -> bool {
        if self.fault_ppm == 0 {
            return false;
        }
        let hit = self
            .rng
            .lock()
            .unwrap()
            .gen_ratio(self.fault_ppm, 1_000_000);
        if hit {
            self.digest.event((kind, op, self.normalize(path)));
        }
        hit
    }

    fn check_isolated(&self) -> object_store::Result<()> {
        if self.isolated.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Self::dead());
        }
        Ok(())
    }

    async fn latency(&self, op: &'static str, path: &ObjPath) -> object_store::Result<()> {
        // Whole milliseconds only: tokio's timer wheel buckets deadlines into
        // absolute ~1ms slots anchored at the runtime's start instant, so a
        // sub-ms deadline pair may or may not share a slot depending on that
        // real start phase, flipping their firing order between runs of the
        // same seed. On the whole-ms grid every deadline hits a slot boundary
        // exactly and ordering is phase-independent.
        let millis = if self.lat_max == 0 {
            0
        } else {
            self.rng.lock().unwrap().gen_range(1..=self.lat_max)
        };
        self.digest.event(("os", op, self.normalize(path), millis));
        if millis == 0 {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(Duration::from_millis(millis)).await;
        }
        self.check_isolated()?;
        // Fail-before: the request never reached the store.
        if self.draw_fault("fault", op, path) {
            return Err(Self::transient());
        }
        Ok(())
    }
}

struct SimStore {
    inner: Arc<dyn ObjectStore>,
    hooks: Hooks,
}

impl SimStore {
    fn new(inner: Arc<dyn ObjectStore>, seed: u64, fault_ppm: u32, digest: Digest) -> Self {
        Self {
            inner,
            hooks: Hooks {
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
                lat_max: env_or("DST_LAT_MAX_MS", 20) as u64,
                fault_ppm,
                isolated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                digest,
                names: Arc::new(Mutex::new(std::collections::HashMap::new())),
            },
        }
    }

    async fn latency(&self, op: &'static str, path: &ObjPath) -> object_store::Result<()> {
        self.hooks.latency(op, path).await
    }
}

impl std::fmt::Display for SimStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimStore({})", self.inner)
    }
}

impl std::fmt::Debug for SimStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimStore({:?})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SimStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.latency("put", location).await?;
        let result = self.inner.put_opts(location, payload, opts).await?;
        // Fail-after: the put landed but the response was lost. A retry then
        // faces its own already-applied effect (a conditional put conflicts).
        if self.hooks.draw_fault("fault-after", "put", location) {
            return Err(Hooks::transient());
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.latency("putm", location).await?;
        // A pass-through MultipartUpload would bypass latency, digest, and the
        // crash isolation for every part. Nothing in the DST world reaches the
        // multipart threshold today; fail loudly rather than simulate it wrong.
        Err(object_store::Error::Generic {
            store: "SimStore",
            source: "multipart is not simulated; wrap MultipartUpload before \
                     raising segment sizes past SEAL_PART_SIZE"
                .into(),
        })
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.latency("get", location).await?;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        use futures::StreamExt;
        // object_store's default `delete` routes through here, so this is the
        // path every irreversible segment delete takes: each one must draw
        // latency, land in the digest, and see the isolation check per item.
        let hooks = self.hooks.clone();
        let gated = locations
            .then(move |loc| {
                let hooks = hooks.clone();
                async move {
                    let loc = loc?;
                    hooks.latency("del", &loc).await?;
                    Ok(loc)
                }
            })
            .boxed();
        let hooks = self.hooks.clone();
        self.inner
            .delete_stream(gated)
            .map(move |res| {
                // Fail-after: the object is gone but the caller sees an error
                // (GC must fail closed, then find ObjectAbsent next pass).
                let loc = res?;
                if hooks.draw_fault("fault-after", "del", &loc) {
                    return Err(Hooks::transient());
                }
                Ok(loc)
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::StreamExt;
        let hooks = self.hooks.clone();
        self.inner
            .list(prefix)
            .then(move |item| {
                let hooks = hooks.clone();
                async move {
                    let meta = item?;
                    hooks.latency("list", &meta.location).await?;
                    Ok(meta)
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.latency(
            "lwd",
            &prefix.cloned().unwrap_or_else(|| ObjPath::from("_root")),
        )
        .await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.latency("copy", from).await?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct SimClock {
    base: tokio::time::Instant,
}

impl SimClock {
    fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
        }
    }
}

impl slatedb_common::SystemClock for SimClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        chrono::DateTime::<Utc>::UNIX_EPOCH
            + chrono::Duration::from_std(self.base.elapsed()).expect("virtual elapsed fits")
    }

    fn sleep<'a>(
        &'a self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn ticker<'a>(&'a self, duration: Duration) -> slatedb_common::SystemClockTicker<'a> {
        slatedb_common::SystemClockTicker::new(self, duration)
    }
}

#[derive(Clone, Debug, Hash)]
enum FileOp {
    Write { offset: usize, len: usize, tag: u64 },
    Truncate { size: usize },
}

/// Deterministic byte pattern for a write, reconstructible from its tag.
fn pattern(tag: u64, len: usize) -> Vec<u8> {
    let mut s = tag ^ 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

fn apply(model: &mut Vec<u8>, op: &FileOp) {
    match op {
        FileOp::Write { offset, len, tag } => {
            let end = offset + len;
            if model.len() < end {
                model.resize(end, 0);
            }
            model[*offset..end].copy_from_slice(&pattern(*tag, *len));
        }
        FileOp::Truncate { size } => model.resize(*size, 0),
    }
}

fn replay(ops: &[FileOp]) -> Vec<u8> {
    let mut m = Vec::new();
    for op in ops {
        apply(&mut m, op);
    }
    m
}

struct FileState {
    id: InodeId,
    name: Vec<u8>,
    /// Attempted ops, pushed before the call is awaited. At most one op past
    /// `acked` (the in-flight one when a mid-round crash cancelled the writer);
    /// whether it applied is decided post-crash by the prefix search.
    ops: Vec<FileOp>,
    /// Ops whose ack was observed. `cur` replays exactly these.
    acked: usize,
    /// Ops index covered by the last acked fsync: the crash-survival floor.
    fsynced: usize,
    /// Replay of `ops[..acked]`, maintained incrementally.
    cur: Vec<u8>,
    /// Every op ever attempted, crash-discarded ones included. Diagnostics
    /// only: fingerprinting which op mystery bytes came from.
    history: Vec<FileOp>,
}

#[derive(Clone, Default)]
struct Digest {
    hasher: Arc<Mutex<DefaultHasher>>,
    trace: Arc<Mutex<Vec<String>>>,
    /// Virtual-time base for trace timestamps (diagnostic only; the hash
    /// covers the events themselves). Set once per world.
    base: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl Digest {
    fn event(&self, e: impl Hash + std::fmt::Debug) {
        e.hash(&mut *self.hasher.lock().unwrap());
        let mut base = self.base.lock().unwrap();
        let base = base.get_or_insert_with(tokio::time::Instant::now);
        let t = base.elapsed().as_millis();
        self.trace.lock().unwrap().push(format!("[{t}] {e:?}"));
    }
    fn finish(&self) -> (u64, Vec<String>) {
        (
            self.hasher.lock().unwrap().clone().finish(),
            std::mem::take(&mut *self.trace.lock().unwrap()),
        )
    }
}

fn creds() -> Credentials {
    Credentials {
        uid: 1000,
        gid: 1000,
        groups: [1000; 16],
        groups_count: 1,
    }
}

fn auth() -> AuthContext {
    AuthContext {
        uid: 1000,
        gid: 1000,
        gids: vec![1000],
    }
}

/// Build a ZeroFS over the (persistent) simulated store.
async fn build_fs(
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
        ZeroFS::new_with_slatedb(
            SlateDbHandle::ReadWrite(slatedb),
            u64::MAX,
            None,
            false,
            object_store,
            zerofs::frame_codec::FrameCodec::new(
                &[7u8; 32],
                zerofs::segment::SEGMENT_INFO,
                zerofs::config::CompressionConfig::default(),
            ),
        )
        .await
        .expect("zerofs open"),
    );
    fs.extent_store.set_seal_threshold(scale.seal_threshold);
    fs.extent_store.enable_nominations();
    fs
}

/// Crash: drop every handle without flushing, then let the coordinator tasks
/// observe channel closure and exit before the next incarnation opens (their
/// queues are empty, all actors joined first, so nothing applies late).
async fn crash(fs: Arc<ZeroFS>, store: &SimStore) {
    // Cut the store first: in-flight internal work (background seals, the
    // flush coordinator) must fail like the process died, not complete
    // gracefully after the actors stopped. Isolation is permanent; the next
    // incarnation runs on a fresh wrapper over the same backing store, so a
    // zombie task whose timer outlives any grace period still wakes into its
    // own dead store instead of the successor's.
    store
        .hooks
        .isolated
        .store(true, std::sync::atomic::Ordering::Relaxed);
    drop(fs);
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    // Let most of the dropped incarnation's tasks wake, observe the dead
    // store, and exit before the next incarnation starts (virtual, so free).
    tokio::time::sleep(Duration::from_secs(10)).await;
}

/// One writer owns one file for a round: seeded writes, truncates, verified
/// reads, and occasional fsyncs. Reads must match the model exactly (single
/// writer per file). A mid-round crash signal cancels the in-flight op at its
/// current await point, leaving it attempted-but-unacked in the model.
/// Returns the updated state.
#[allow(clippy::too_many_arguments)]
async fn writer_round(
    fs: Arc<ZeroFS>,
    mut f: FileState,
    mut rng: StdRng,
    ops: usize,
    digest: Digest,
    // Cumulative op-mix thresholds out of `mix[3]` total: write, truncate,
    // read, rest = fsync. Seed-varied so different seeds explore different
    // regimes (fsync-starved, truncate-heavy, ...).
    mix: [u32; 4],
    cap: usize,
    slot: usize,
    floors: Arc<Floors>,
    mut crashed: tokio::sync::watch::Receiver<bool>,
) -> FileState {
    let auth = auth();
    let creds = creds();
    // Half the reads concentrate on one window so the same cross-segment
    // seams are re-read repeatedly, which is what accrues pair heat.
    let hot_base = rng.gen_range(0..cap - 3 * EXTENT_SIZE);
    for op_no in 0..ops {
        if *crashed.borrow() {
            break;
        }
        let dice = rng.gen_range(0..mix[3]);
        if dice < mix[0] {
            // Write: anywhere in the cap, sized to cross extent boundaries.
            let offset = rng.gen_range(0..cap - 1);
            let len = rng.gen_range(1..=EXTENT_SIZE * 5 / 2).min(cap - offset);
            let tag = rng.r#gen::<u64>();
            let data = Bytes::from(pattern(tag, len));
            let op = FileOp::Write { offset, len, tag };
            f.ops.push(op.clone());
            f.history.push(op.clone());
            tokio::select! {
                biased;
                r = fs.write(&auth, f.id, offset as u64, &data) => {
                    match r {
                        Ok(_) => {
                            apply(&mut f.cur, &op);
                            f.acked = f.ops.len();
                            floors.acked[slot]
                                .store(f.acked, std::sync::atomic::Ordering::Relaxed);
                            digest.event(("w", f.id, op_no, offset, len, tag));
                        }
                        // A failpoint crash isolates the store before the kill
                        // signal lands, so an in-flight op can error first; it
                        // stays attempted-unacked.
                        Err(_) if *crashed.borrow() => return f,
                        Err(e) => panic!("write({}, {offset}, {len}): {e:?}", f.id),
                    }
                }
                _ = crashed.changed() => return f,
            }
        } else if dice < mix[1] {
            let size = rng.gen_range(0..=cap);
            let op = FileOp::Truncate { size };
            f.ops.push(op.clone());
            f.history.push(op.clone());
            let setattr = SetAttributes {
                size: SetSize::Set(size as u64),
                ..Default::default()
            };
            tokio::select! {
                biased;
                r = fs.setattr(&creds, f.id, &setattr) => {
                    match r {
                        Ok(_) => {
                            apply(&mut f.cur, &op);
                            f.acked = f.ops.len();
                            floors.acked[slot]
                                .store(f.acked, std::sync::atomic::Ordering::Relaxed);
                            digest.event(("t", f.id, op_no, size));
                        }
                        Err(_) if *crashed.borrow() => return f,
                        Err(e) => panic!("truncate({}, {size}): {e:?}", f.id),
                    }
                }
                _ = crashed.changed() => return f,
            }
        } else if dice < mix[2] {
            // Verified read: a sequential whole-file scan (crosses every
            // segment seam, heating pairs for chain compaction) or a point
            // read, uniform or clustered on the hot window.
            if rng.gen_bool(0.3) {
                // One call per pass over the whole file: seams are detected
                // per read call, so chunked scans would miss most of them.
                let chunk = f.cur.len().max(EXTENT_SIZE);
                let mut at = 0usize;
                let mut interrupted = false;
                while at < f.cur.len() {
                    tokio::select! {
                        biased;
                        r = fs.read_file(&auth, f.id, at as u64, chunk as u32) => {
                            let (got, _eof) = match r {
                                Ok(v) => v,
                                Err(_) if *crashed.borrow() => {
                                    interrupted = true;
                                    break;
                                }
                                Err(e) => panic!("scan({}, {at}): {e:?}", f.id),
                            };
                            let end = (at + chunk).min(f.cur.len());
                            assert_eq!(
                                got.as_ref(),
                                &f.cur[at..end],
                                "scan mismatch: file {} at {at}",
                                f.id
                            );
                            at = end;
                        }
                        _ = crashed.changed() => { interrupted = true; break; }
                    }
                }
                if interrupted {
                    // No digest event on the cancellation path: the crash
                    // signal wakes every waiter at the same instant and their
                    // relative poll order is not a defined ordering, so an
                    // emission here would put an order-free race into the
                    // trace the replay check requires to be total.
                    return f;
                }
                digest.event(("s", f.id, op_no, at));
                let pause = rng.gen_range(1..=3);
                tokio::time::sleep(Duration::from_millis(pause)).await;
                continue;
            }
            let offset = if rng.gen_bool(0.5) {
                hot_base + rng.gen_range(0..EXTENT_SIZE)
            } else {
                rng.gen_range(0..cap)
            };
            let count = rng.gen_range(1..=EXTENT_SIZE * 3);
            tokio::select! {
                biased;
                r = fs.read_file(&auth, f.id, offset as u64, count as u32) => {
                    let (got, _eof) = match r {
                        Ok(v) => v,
                        Err(_) if *crashed.borrow() => return f,
                        Err(e) => panic!("read({}, {offset}, {count}): {e:?}", f.id),
                    };
                    let end = (offset + count).min(f.cur.len());
                    let expected: &[u8] = if offset >= f.cur.len() {
                        &[]
                    } else {
                        &f.cur[offset..end]
                    };
                    assert_eq!(
                        got.as_ref(),
                        expected,
                        "read mismatch: file {} offset {offset} count {count}",
                        f.id
                    );
                    digest.event(("r", f.id, op_no, offset, count, got.len()));
                }
                _ = crashed.changed() => return f,
            }
        } else {
            // Snapshot every file's acked count first: anything acked before
            // the flush starts is covered by it, so the ack below may raise
            // every file's floor, not just this one's.
            let snap: Vec<usize> = floors
                .acked
                .iter()
                .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
                .collect();
            tokio::select! {
                biased;
                r = fs.client_fsync() => {
                    match r {
                        Ok(()) => {
                            for (floor, snapped) in floors.floor.iter().zip(&snap) {
                                floor.fetch_max(*snapped, std::sync::atomic::Ordering::Relaxed);
                            }
                            f.fsynced = f.acked;
                            digest.event(("f", f.id, op_no));
                        }
                        // A failed fsync raises no floor: the safe direction.
                        Err(_) if *crashed.borrow() => return f,
                        Err(e) => panic!("fsync: {e:?}"),
                    }
                }
                // A cancelled fsync that actually flushed just leaves the
                // floor lower than it could be.
                _ = crashed.changed() => return f,
            }
        }
        let pause = rng.gen_range(1..=3);
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }
    f
}

/// The GC actor: full reclaim passes (barrier + durable scan + delete +
/// compact) racing the writers, with an always-past delete horizon so a dead
/// segment is deleted the pass it is seen. A mid-round crash cancels the pass
/// wherever it is, including between an object delete and its counter-drop
/// commit.
///
/// Pass tuning is seeded per pass so the sweep also reaches the wrapper's
/// blind spots: quiescent_after zero opens the write-cold gates (tail scrub,
/// chain assembly), an occasional pinned gate exercises the
/// checkpoint-protected pass, small round budgets exercise the gather caps
/// and deferrals, and multi-batch drains run the keep_going loop.
async fn gc_round(
    fs: Arc<ZeroFS>,
    mut rng: StdRng,
    passes: usize,
    digest: Digest,
    mut crashed: tokio::sync::watch::Receiver<bool>,
) {
    for pass in 0..passes {
        if *crashed.borrow() {
            return;
        }
        let pause = rng.gen_range(1..=20);
        tokio::time::sleep(Duration::from_millis(pause)).await;
        let horizon = Utc::now() - chrono::Duration::days(1);
        // 1 in 8: a persistent checkpoint pins the view (nothing classified).
        let protect_before = rng
            .gen_bool(0.125)
            .then(|| Utc::now() + chrono::Duration::days(1));
        // Half the passes see a write-cold store (quiescence gate open); the
        // other half never do (an hour of real Instant time can't elapse).
        let quiescent_after = if rng.gen_bool(0.5) {
            Duration::ZERO
        } else {
            Duration::from_secs(3600)
        };
        let tail_scrub = rng.gen_bool(0.8).then(|| rng.gen_range(5..=60u64));
        let round_bytes = rng.gen_range(256 * 1024..=4 * 1024 * 1024u64);
        let max_batches = rng.gen_range(1..=3usize);
        tokio::select! {
            biased;
            r = fs.extent_store.reclaim_segments_gated(
                move || std::future::ready(Ok(Some((horizon, protect_before)))),
                tail_scrub,
                quiescent_after,
                move |batches| batches < max_batches,
                round_bytes,
            ) => {
                let out = match r {
                    Ok(out) => out,
                    Err(_) if *crashed.borrow() => return,
                    Err(e) => panic!("reclaim pass {pass}: {e:?}"),
                };
                if std::env::var_os("DST_GC_LOG").is_some() {
                    eprintln!("dst: gc pass {pass}: {out:?}");
                }
                digest.event((
                    "gc",
                    pass,
                    out.deleted,
                    out.relocated,
                    format!("{:?}", out.status),
                    out.chains.assembled,
                    out.chains.packed,
                    out.chains.deferred,
                ));
            }
            _ = crashed.changed() => return,
        }
    }
}

/// Cross-file fsync floors. client_fsync flushes the whole filesystem, so one
/// writer's acked fsync makes every file's previously-acked ops durable, not
/// just its own. Writers publish acked counts here; an fsync ack raises every
/// file's floor to the count it had when the fsync started.
struct Floors {
    acked: Vec<std::sync::atomic::AtomicUsize>,
    floor: Vec<std::sync::atomic::AtomicUsize>,
}

/// The extent-key set an inode holds, any durability level.
async fn extent_keys(fs: &ZeroFS, id: InodeId) -> std::collections::BTreeSet<u64> {
    let codec = zerofs::fs::key_codec::KeyCodec::new();
    let start = codec.extent_key(id, 0);
    let end = codec.extent_key(id, u64::MAX);
    let mut out = std::collections::BTreeSet::new();
    use futures::StreamExt;
    let mut stream = fs.db.scan(start..end).await.expect("extent key scan");
    while let Some(item) = stream.next().await {
        let (key, _) = item.expect("extent key scan item");
        if let Some(idx) = codec.parse_extent_key(&key) {
            out.insert(idx);
        }
    }
    out
}

/// Key-level invariants against the model: every extent with nonzero content
/// must have a key (a hole reads as zeros, so the content oracle alone cannot
/// tell a legitimate hole from a wrongly deleted key), and no key may exist at
/// or beyond EOF. A key over all-zero content is left alone: elision decides
/// that case and it cannot hide data loss.
async fn check_extent_keys(fs: &ZeroFS, f: &FileState, seed: u64, round: usize) {
    let keys = extent_keys(fs, f.id).await;
    let nonzero: std::collections::BTreeSet<u64> = f
        .cur
        .chunks(EXTENT_SIZE)
        .enumerate()
        .filter(|(_, c)| c.iter().any(|b| *b != 0))
        .map(|(i, _)| i as u64)
        .collect();
    let missing: Vec<u64> = nonzero.difference(&keys).copied().collect();
    assert!(
        missing.is_empty(),
        "extent keys missing for nonzero content (seed {seed}, round {round}, file {}): {missing:?}",
        f.id
    );
    let eof_extents = (f.cur.len() as u64).div_ceil(EXTENT_SIZE as u64);
    let beyond: Vec<u64> = keys.range(eof_extents..).copied().collect();
    assert!(
        beyond.is_empty(),
        "extent keys at or beyond EOF (seed {seed}, round {round}, file {}, eof {eof_extents}): {beyond:?}",
        f.id
    );
}

/// Read the whole file through the store (chunked, fresh caches post-crash, so
/// every referenced frame is actually fetched and decoded).
async fn read_all(fs: &ZeroFS, id: InodeId) -> Vec<u8> {
    let auth = auth();
    let mut out = Vec::new();
    let chunk = 2 * EXTENT_SIZE as u32;
    loop {
        let (got, eof) = fs
            .read_file(&auth, id, out.len() as u64, chunk)
            .await
            .unwrap_or_else(|e| panic!("post-crash read of inode {id} at {}: {e:?}", out.len()));
        out.extend_from_slice(&got);
        if eof || got.is_empty() {
            return out;
        }
    }
}

/// Post-crash check for one file: observed content must equal the replay of
/// ops[..k] for some k in [fsynced, len]; the range spans an attempted op a
/// mid-round crash left with an unknown fate. Truncates the model to the
/// surviving prefix (now durable) and returns the state for the next round.
fn reconcile_crash(mut f: FileState, observed: Vec<u8>, seed: u64, round: usize) -> FileState {
    for k in f.fsynced..=f.ops.len() {
        if replay(&f.ops[..k]) == observed {
            eprintln!(
                "dst: reconcile file {} round {round}: k={k} of {} ops ({} acked, floor {})",
                f.id,
                f.ops.len(),
                f.acked,
                f.fsynced
            );
            f.ops.truncate(k);
            f.acked = k;
            f.fsynced = k;
            f.cur = observed;
            return f;
        }
    }
    let mut detail = String::new();
    let mut diff_at = None;
    for k in f.fsynced..=f.ops.len() {
        let expect = replay(&f.ops[..k]);
        let diff = expect
            .iter()
            .zip(observed.iter())
            .position(|(a, b)| a != b)
            .map(|o| {
                diff_at = Some(o);
                format!("first content diff at {o} (extent {})", o / EXTENT_SIZE)
            })
            .unwrap_or_else(|| "contents equal over common length".into());
        detail.push_str(&format!(
            "\n  k={k} ({:?}): expected len {}, observed len {}, {diff}",
            f.ops.get(k.wrapping_sub(1)),
            expect.len(),
            observed.len(),
        ));
    }
    // Fingerprint the observed window against every op that ever covered the
    // diff offset, crash-discarded ones included, to name the bytes' origin.
    if let Some(o) = diff_at {
        let window = &observed[o..(o + 16).min(observed.len())];
        detail.push_str(&format!(
            "\n  observed[{o}..+16] = {window:02x?}, candidates:"
        ));
        for (i, op) in f.history.iter().enumerate() {
            if let FileOp::Write { offset, len, tag } = op
                && o >= *offset
                && o < offset + len
            {
                let pat = pattern(*tag, *len);
                let w = &pat[o - offset..(o - offset + 16).min(pat.len())];
                detail.push_str(&format!(
                    "\n    history[{i}] Write{{offset {offset}, len {len}}}: {}",
                    if w == window { "MATCHES" } else { "differs" }
                ));
            }
        }
    }
    panic!(
        "DST ORACLE VIOLATION (seed {seed}, round {round}): file {} (inode {}) post-crash \
         content ({} bytes) matches no op-prefix >= the fsync floor ({} of {} ops, {} acked). \
         An fsync-acked or prefix-consistent state was lost.{detail}",
        String::from_utf8_lossy(&f.name),
        f.id,
        observed.len(),
        f.fsynced,
        f.ops.len(),
        f.acked,
    )
}

/// Accounting reconciliation: every segment counter's `live` must equal the
/// byte sum of the FrameLocs actually pointing at it, `total` must bound
/// `live`, and a referenced segment must have a counter row (its credit rides
/// the same commit as the pointer). Catches leak-shaped drift the content
/// oracle is blind to: an over-counted segment is never reclaimed, an
/// under-counted one is a delete candidate.
async fn check_segcounts(fs: &ZeroFS, files: &[FileState], seed: u64, round: usize) {
    use futures::StreamExt;
    let codec = zerofs::fs::key_codec::KeyCodec::new();
    let mut computed: std::collections::BTreeMap<zerofs::segment::Segid, u64> =
        std::collections::BTreeMap::new();
    for f in files {
        let start = codec.extent_key(f.id, 0);
        let end = codec.extent_key(f.id, u64::MAX);
        let mut stream = fs.db.scan(start..end).await.expect("extent scan");
        while let Some(item) = stream.next().await {
            let (_, value) = item.expect("extent scan item");
            let loc = zerofs::segment::FrameLoc::decode(&value).expect("FrameLoc decode");
            *computed.entry(loc.segid).or_insert(0) += loc.byte_len as u64;
        }
    }
    let (sc_start, sc_end) = codec.segcount_prefix_range();
    let mut stream = fs.db.scan(sc_start..sc_end).await.expect("segcount scan");
    while let Some(item) = stream.next().await {
        let (key, value) = item.expect("segcount scan item");
        let Some((epoch, counter)) = codec.parse_segcount_key(&key) else {
            continue;
        };
        let segid = zerofs::segment::Segid::new(epoch, counter);
        let (live, total) =
            zerofs::fs::key_codec::KeyCodec::decode_segcount(&value).expect("segcount decode");
        let expected = computed.remove(&segid).unwrap_or(0);
        assert_eq!(
            live, expected,
            "segcount live mismatch (seed {seed}, round {round}, {segid:?}): \
             counter says {live}, FrameLocs sum to {expected}"
        );
        assert!(
            total >= live,
            "segcount total < live (seed {seed}, round {round}, {segid:?}): {total} < {live}"
        );
    }
    assert!(
        computed.is_empty(),
        "segments referenced by FrameLocs but missing counter rows \
         (seed {seed}, round {round}): {computed:?}"
    );
}

/// The incremental footprint gauges must agree with an authoritative scan at
/// any quiesced moment (they are maintained off the commit path and never
/// rescan on their own).
async fn check_footprint_gauges(fs: &ZeroFS, seed: u64, round: usize) {
    use std::sync::atomic::Ordering::Relaxed;
    let scan = fs
        .extent_store
        .sample_footprint()
        .await
        .expect("footprint scan");
    let gauges = fs.extent_store.segment_gc_stats();
    assert_eq!(
        (scan.segment_count, scan.appended_bytes, scan.live_bytes),
        (
            gauges.segment_count.load(Relaxed),
            gauges.appended_bytes.load(Relaxed),
            gauges.live_bytes.load(Relaxed),
        ),
        "footprint gauges diverged from the authoritative scan \
         (seed {seed}, round {round})"
    );
}

// ---------------------------------------------------------------------------
// Failpoint-placed crashes: the timer killer can only land at instants with
// virtual width, so awaitless windows (post-barrier pre-scan, between an
// object delete and its counter drop, between a pack's per-inode repoints)
// are unreachable by it. An armed failpoint callback isolates the store and
// fires the kill signal exactly there instead.
// ---------------------------------------------------------------------------

#[cfg(feature = "failpoints")]
mod fp_crash {
    use std::sync::Mutex;

    pub const POINTS: &[&str] = &[
        zerofs::failpoints::FLUSH_AFTER_SEAL_BEFORE_MANIFEST,
        zerofs::failpoints::COMPACT_AFTER_SEAL_BEFORE_REPOINT,
        zerofs::failpoints::COMPACT_BETWEEN_REPOINTS,
        zerofs::failpoints::RECLAIM_AFTER_BARRIER_BEFORE_SCAN,
        zerofs::failpoints::RECLAIM_AFTER_SEGMENT_DELETE,
    ];

    /// Sites that await a configured delay, stretching a read-decide-act gap
    /// so concurrent commits can land inside it (see zerofs::failpoints::widen).
    pub const WIDEN_POINTS: &[&str] = &[
        zerofs::failpoints::RECLAIM_AFTER_BARRIER_BEFORE_SCAN,
        zerofs::failpoints::RECLAIM_AFTER_VERIFY_BEFORE_DELETE,
        zerofs::failpoints::COMPACT_BETWEEN_REPOINTS,
        zerofs::failpoints::READ_AFTER_RESOLVE_BEFORE_FETCH,
    ];

    pub struct Armed {
        pub point: &'static str,
        /// Worlds are single-threaded, so the thread id scopes the hook to
        /// the armed world; parallel tests in this binary cannot cross-fire.
        pub thread: std::thread::ThreadId,
        pub hits_left: u32,
        pub fire: Box<dyn Fn() + Send>,
    }

    pub static ARMED: Mutex<Option<Armed>> = Mutex::new(None);

    /// Register the pass-through callbacks once per process; arming happens
    /// per round via `ARMED`.
    pub fn register_callbacks() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            for point in POINTS {
                fail::cfg_callback(*point, move || trip(point)).expect("cfg_callback");
            }
        });
    }

    fn trip(point: &'static str) {
        let mut slot = ARMED.lock().unwrap();
        let Some(armed) = slot.as_mut() else {
            return;
        };
        if armed.point != point || armed.thread != std::thread::current().id() {
            return;
        }
        armed.hits_left -= 1;
        if armed.hits_left == 0 {
            (armed.fire)();
            *slot = None;
        }
    }
}

// ---------------------------------------------------------------------------
// The world
// ---------------------------------------------------------------------------

async fn world(seed: u64, fp_crash: bool) -> (u64, Vec<String>) {
    // Used only under the failpoints feature; see crash_points.
    let _ = fp_crash;
    let rounds = env_or("DST_ROUNDS", 4);
    let ops_per_file = env_or("DST_OPS", 50);
    let gc_passes = env_or("DST_GC", 6);
    let crash_pct = env_or("DST_CRASH_PCT", 70).min(100);
    let digest = Digest::default();

    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // Seed-varied transient-fault rate; half the worlds run fault-free.
    let fault_ppm = [0u32, 0, 2_000, 10_000][StdRng::seed_from_u64(seed ^ 0xFA17).gen_range(0..4)];
    let mut sim = Arc::new(SimStore::new(
        backing.clone(),
        seed ^ 0x5157,
        fault_ppm,
        digest.clone(),
    ));
    let clock = Arc::new(SimClock::new());
    let scale = scale_for(seed);
    let mut fs = build_fs(sim.clone(), clock.clone(), seed, scale).await;

    // Setup: create the files, one small fsynced write each, so every later
    // incarnation finds them.
    let creds = creds();
    let auth = auth();
    let mut files: Vec<FileState> = Vec::new();
    for i in 0..FILES {
        let name = format!("f{i}").into_bytes();
        let (id, _) = fs
            .create(&creds, 0, &name, &SetAttributes::default())
            .await
            .expect("create");
        let tag = seed ^ (i as u64);
        let op = FileOp::Write {
            offset: 0,
            len: 1024,
            tag,
        };
        fs.write(&auth, id, 0, &Bytes::from(pattern(tag, 1024)))
            .await
            .expect("seed write");
        let mut f = FileState {
            id,
            name,
            ops: vec![op.clone()],
            acked: 1,
            fsynced: 0,
            cur: Vec::new(),
            history: vec![op],
        };
        apply(&mut f.cur, &f.ops[0]);
        files.push(f);
    }
    fs.client_fsync().await.expect("setup fsync");
    for f in &mut files {
        f.fsynced = f.ops.len();
    }
    let floors = Arc::new(Floors {
        acked: files
            .iter()
            .map(|f| std::sync::atomic::AtomicUsize::new(f.acked))
            .collect(),
        floor: files
            .iter()
            .map(|f| std::sync::atomic::AtomicUsize::new(f.fsynced))
            .collect(),
    });

    let mut world_rng = StdRng::seed_from_u64(seed ^ 0xD57);
    // Seed-varied op mix (see writer_round): cumulative write/truncate/read
    // thresholds; the remainder is fsync weight (can approach zero, leaving
    // nearly everything unflushed, the regime reclaim's durable-level reads
    // exist for).
    let mix = {
        let w_write = 30 + world_rng.gen_range(0..30u32);
        let w_trunc = world_rng.gen_range(3..20u32);
        let w_read = 10 + world_rng.gen_range(0..20u32);
        let w_fsync = world_rng.gen_range(1..12u32);
        [
            w_write,
            w_write + w_trunc,
            w_write + w_trunc + w_read,
            w_write + w_trunc + w_read + w_fsync,
        ]
    };
    for round in 0..rounds {
        // Concurrent phase: one writer per file plus the GC actor, plus a
        // seeded killer whose timer, when it fires inside the round, crashes
        // the world mid-flight, cancelling every actor at whatever await
        // point it happens to be in.
        // crash_tx must outlive the round even when no killer runs: a dropped
        // watch sender completes changed() with Err, which would read as a
        // crash signal and cancel writers mid-op on a clean round.
        let (crash_tx, crash_rx) = tokio::sync::watch::channel(false);
        let killer = if fp_crash {
            // Arm one window with a seeded hit count instead of a timer; the
            // round crashes exactly there, or completes cleanly if the window
            // is never reached that often.
            #[cfg(feature = "failpoints")]
            {
                let point = fp_crash::POINTS[world_rng.gen_range(0..fp_crash::POINTS.len())];
                let hits_left = world_rng.gen_range(1..=6);
                let isolated = sim.hooks.isolated.clone();
                let tx = crash_tx.clone();
                *fp_crash::ARMED.lock().unwrap() = Some(fp_crash::Armed {
                    point,
                    thread: std::thread::current().id(),
                    hits_left,
                    fire: Box::new(move || {
                        isolated.store(true, std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(true);
                    }),
                });
                // Half the rounds also widen one read-decide-act gap, so
                // commits can land between a decision's inputs and the act;
                // the armed crash then tests recovery from state built on
                // that in-window view.
                let widen_ms = [0u64, 0, 10, 30][world_rng.gen_range(0..4)];
                let widen_point =
                    fp_crash::WIDEN_POINTS[world_rng.gen_range(0..fp_crash::WIDEN_POINTS.len())];
                *zerofs::failpoints::WIDEN.lock().unwrap() =
                    (widen_ms > 0).then(|| (widen_point, std::thread::current().id(), widen_ms));
            }
            None
        } else if world_rng.gen_bool(crash_pct as f64 / 100.0) {
            let delay = Duration::from_millis(world_rng.gen_range(3..=250));
            let tx = crash_tx.clone();
            Some(tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx.send(true);
            }))
        } else {
            None
        };
        let mut handles = Vec::new();
        for (i, f) in files.drain(..).enumerate() {
            let rng = StdRng::seed_from_u64(seed ^ (round as u64) << 8 ^ (i as u64) << 4 ^ 1);
            handles.push(tokio::spawn(writer_round(
                fs.clone(),
                f,
                rng,
                ops_per_file,
                digest.clone(),
                mix,
                scale.file_cap,
                i,
                floors.clone(),
                crash_rx.clone(),
            )));
        }
        let gc = tokio::spawn(gc_round(
            fs.clone(),
            StdRng::seed_from_u64(seed ^ (round as u64) << 8 ^ 2),
            gc_passes,
            digest.clone(),
            crash_rx.clone(),
        ));
        for h in handles {
            files.push(h.await.expect("writer actor"));
        }
        gc.await.expect("gc actor");
        let crashing = *crash_rx.borrow();
        if let Some(k) = killer {
            k.abort();
        }
        #[cfg(feature = "failpoints")]
        if fp_crash {
            *fp_crash::ARMED.lock().unwrap() = None;
            *zerofs::failpoints::WIDEN.lock().unwrap() = None;
        }
        drop(crash_tx);
        // Fold the fs-wide floors in: any fsync acked this round covered every
        // file's ops acked before it started, not only the caller's.
        for (i, f) in files.iter_mut().enumerate() {
            f.fsynced = f
                .fsynced
                .max(floors.floor[i].load(std::sync::atomic::Ordering::Relaxed));
        }

        if !crashing {
            // Quiesced: every ack has landed, so live state must equal the
            // model exactly. (Skipped on a crash round: an attempted op's
            // fate is only decided by the post-crash prefix search.)
            for f in &files {
                assert_eq!(
                    f.ops.len(),
                    f.acked,
                    "clean round left an attempted op dangling (file {})",
                    f.id
                );
                let observed = read_all(&fs, f.id).await;
                assert_eq!(
                    observed, f.cur,
                    "quiesced content mismatch (seed {seed}, round {round}, file {})",
                    f.id
                );
                check_extent_keys(&fs, f, seed, round).await;
                digest.event(("q", f.id, observed.len()));
            }
            check_segcounts(&fs, &files, seed, round).await;
            check_footprint_gauges(&fs, seed, round).await;
            // Full metadata invariants (stats counters, directory
            // structure, nlink) hold at any quiesced moment, not only after
            // a crash.
            let report = verify_consistency_sparse(&fs)
                .await
                .expect("quiesced consistency scan");
            assert!(
                report.is_consistent(),
                "consistency errors at quiesce (seed {seed}, round {round}):\n{report}"
            );
        } else {
            crash(fs, &sim).await;
            sim = Arc::new(SimStore::new(
                backing.clone(),
                seed ^ 0x5157 ^ (round as u64 + 1),
                fault_ppm,
                digest.clone(),
            ));
            fs = build_fs(sim.clone(), clock.clone(), seed ^ (round as u64 + 1), scale).await;
            digest.event(("crashed", round));
            // A fresh quiesced incarnation: reclaim crash orphans (min-age
            // deliberately bypassed; nothing is in flight), then check every
            // invariant survived.
            fs.extent_store
                .sweep_orphans(Utc::now() + chrono::Duration::days(1))
                .await
                .expect("orphan sweep");
            let report = verify_consistency_sparse(&fs)
                .await
                .expect("consistency scan");
            assert!(
                report.is_consistent(),
                "consistency errors after crash (seed {seed}, round {round}):\n{report}"
            );
            files = {
                let mut next = Vec::new();
                for f in files {
                    let observed = read_all(&fs, f.id).await;
                    digest.event(("c", f.id, observed.len()));
                    next.push(reconcile_crash(f, observed, seed, round));
                }
                next
            };
            // The reconciled prefixes are the new ground truth: re-anchor the
            // floor registry and check the key set against each model.
            for (i, f) in files.iter().enumerate() {
                floors.acked[i].store(f.acked, std::sync::atomic::Ordering::Relaxed);
                floors.floor[i].store(f.fsynced, std::sync::atomic::Ordering::Relaxed);
                check_extent_keys(&fs, f, seed, round).await;
            }
            check_segcounts(&fs, &files, seed, round).await;
        }
    }
    digest.finish()
}

fn run_seed(seed: u64) -> (u64, Vec<String>) {
    run_seed_mode(seed, false)
}

fn run_seed_mode(seed: u64, fp_crash: bool) -> (u64, Vec<String>) {
    zerofs::fs::DST_FIXED_TIME.store(true, std::sync::atomic::Ordering::Relaxed);
    zerofs::db::DST_PANIC_ON_WRITE_ERROR.store(true, std::sync::atomic::Ordering::Relaxed);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    rt.block_on(world(seed, fp_crash))
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Where the sweep's seeds come from: an explicit reproduction list, or fresh
/// OS entropy bounded by a count or a real-time budget (new seeds stop when
/// the budget elapses; in-flight ones finish).
enum SeedSource {
    List(Mutex<std::collections::VecDeque<u64>>),
    Random {
        deadline: std::time::Instant,
        remaining: std::sync::atomic::AtomicUsize,
    },
}

impl SeedSource {
    fn next(&self) -> Option<u64> {
        match self {
            SeedSource::List(queue) => queue.lock().unwrap().pop_front(),
            SeedSource::Random {
                deadline,
                remaining,
            } => {
                if std::time::Instant::now() >= *deadline {
                    return None;
                }
                remaining
                    .fetch_update(
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                        |n| n.checked_sub(1),
                    )
                    .ok()?;
                Some(rand::thread_rng().r#gen())
            }
        }
    }
}

#[test]
fn seeds() {
    // DST_SEEDS runs an explicit list (reproduction). Otherwise seeds are
    // drawn fresh from OS entropy: DST_WALL_CLOCK_SECS keeps drawing new ones
    // until the real-time budget elapses (a soak), the default is 8. Every
    // seed is printed and a failure names its seed, so the repro is always
    // DST_SEEDS=<seed>.
    let source = match std::env::var("DST_SEEDS") {
        Ok(s) => SeedSource::List(Mutex::new(
            s.split(',')
                .map(|t| t.trim().parse().expect("DST_SEEDS: u64 list"))
                .collect(),
        )),
        Err(_) => {
            let budget = env_or("DST_WALL_CLOCK_SECS", 0);
            SeedSource::Random {
                deadline: std::time::Instant::now() + Duration::from_secs(budget.max(1) as u64),
                remaining: std::sync::atomic::AtomicUsize::new(if budget == 0 {
                    8
                } else {
                    usize::MAX
                }),
            }
        }
    };
    // Worlds are isolated (own runtime, own store), so seeds fan out across
    // all cores; per-seed determinism is unaffected by CPU contention since
    // every schedule runs on virtual time.
    let jobs = std::thread::available_parallelism().map_or(1, |n| n.get());
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..jobs)
            .map(|_| {
                scope.spawn(|| {
                    while let Some(seed) = source.next() {
                        eprintln!("dst: seed {seed}");
                        let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            run_seed(seed)
                        }));
                        if run.is_err() {
                            // Parallel workers interleave output; restate the
                            // failing seed unambiguously.
                            panic!("dst: seed {seed} failed (repro: DST_SEEDS={seed})");
                        }
                    }
                })
            })
            .collect();
        for worker in workers {
            if worker.join().is_err() {
                panic!("a seed failed; its panic message names the seed above");
            }
        }
    });
}

/// Seeded crashes placed inside awaitless windows via failpoints. Sequential:
/// the fail crate's registry is process-global (the per-thread guard in
/// `fp_crash` additionally protects against parallel tests in this binary).
#[cfg(feature = "failpoints")]
#[test]
fn crash_points() {
    let scenario = fail::FailScenario::setup();
    fp_crash::register_callbacks();
    let seeds: Vec<u64> = match std::env::var("DST_SEEDS") {
        Ok(s) => s
            .split(',')
            .map(|t| t.trim().parse().expect("DST_SEEDS: u64 list"))
            .collect(),
        Err(_) => {
            let mut rng = rand::thread_rng();
            (0..6).map(|_| rng.r#gen()).collect()
        }
    };
    for seed in seeds {
        eprintln!("dst: crash-point seed {seed}");
        let _ = run_seed_mode(seed, true);
    }
    scenario.teardown();
}

/// A failing seed is only a repro if the same seed is the same run. The seed
/// under replay is drawn fresh each run (or DST_SEEDS' first entry, so a
/// reproduction exercises the same one) and printed.
#[test]
fn same_seed_same_digest() {
    let seed: u64 = std::env::var("DST_SEEDS")
        .ok()
        .and_then(|s| s.split(',').next()?.trim().parse().ok())
        .unwrap_or_else(|| rand::thread_rng().r#gen());
    eprintln!("dst: replay check seed {seed}");
    let (a, ta) = run_seed(seed);
    let (b, tb) = run_seed(seed);
    if a != b {
        let dir = std::env::temp_dir();
        std::fs::write(dir.join("dst_trace_a.log"), ta.join("\n")).ok();
        std::fs::write(dir.join("dst_trace_b.log"), tb.join("\n")).ok();
        eprintln!(
            "dst: full traces dumped to {}/dst_trace_{{a,b}}.log",
            dir.display()
        );
        let n = ta.len().min(tb.len());
        for i in 0..n {
            if ta[i] != tb[i] {
                let lo = i.saturating_sub(40);
                let hi_a = (i + 3).min(ta.len());
                let hi_b = (i + 3).min(tb.len());
                panic!(
                    "same seed diverged at event {i} of {}/{}:\n  run A: ...{}\n  run B: ...{}",
                    ta.len(),
                    tb.len(),
                    ta[lo..hi_a].join("\n         "),
                    tb[lo..hi_b].join("\n         "),
                );
            }
        }
        panic!(
            "same seed diverged: traces equal for {n} events but lengths {} vs {}",
            ta.len(),
            tb.len()
        );
    }
}
