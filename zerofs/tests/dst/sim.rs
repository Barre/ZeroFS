//! The simulated object store and clock: seeded latency, transient faults, and
//! crash isolation over a persistent in-memory backing store.

use crate::digest::Digest;
use crate::env_or;
use chrono::Utc;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The per-op instrumentation, split from the store so the wrapped delete and
/// list streams can carry it with a 'static lifetime.
#[derive(Clone)]
pub(crate) struct Hooks {
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
    pub(crate) isolated: Arc<AtomicBool>,
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
        if self.isolated.load(Relaxed) {
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

pub(crate) struct SimStore {
    inner: Arc<dyn ObjectStore>,
    pub(crate) hooks: Hooks,
}

impl SimStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        seed: u64,
        fault_ppm: u32,
        digest: Digest,
    ) -> Self {
        Self {
            inner,
            hooks: Hooks {
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
                lat_max: env_or("DST_LAT_MAX_MS", 20) as u64,
                fault_ppm,
                isolated: Arc::new(AtomicBool::new(false)),
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
pub(crate) struct SimClock {
    base: tokio::time::Instant,
}

impl SimClock {
    pub(crate) fn new() -> Self {
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
