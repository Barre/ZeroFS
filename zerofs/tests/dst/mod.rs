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
//! Each round races ordinary file writers, two disjoint-region writers on one
//! shared inode, a namespace mutation actor, and full GC/reclaim passes. A
//! round then quiesces or drops the filesystem without flushing and reopens it
//! over the surviving store. Recovery must match the data and namespace models
//! at operation prefixes no older than the last acknowledged fsync; every
//! extent must remain readable, segment counters and footprint gauges must
//! reconcile with authoritative scans, and the metadata invariants must hold.
//!
//! Env knobs: `DST_SEEDS=3,17` (explicit seeds, for reproduction),
//! `DST_WALL_CLOCK_SECS` (soak: fresh random seeds until the budget elapses;
//! without it, 8 fresh seeds; fanned across all cores), `DST_ROUNDS`,
//! `DST_OPS` (per file per round). All harness sleeps are whole
//! milliseconds; see `Hooks::latency` for why.
//!
//! This target runs with SlateDB's `cfg(dst)` branches and Tokio's seeded
//! scheduler. Keep its dedicated CI invocation's `RUSTFLAGS` when reproducing
//! locally; an ordinary `cargo test` intentionally compiles this as an empty
//! target, matching SlateDB's own DST tests.

#![cfg(dst)]

#[path = "../failpoints/consistency.rs"]
mod consistency;

mod actor;
mod checks;
mod data;
mod digest;
#[cfg(feature = "failpoints")]
mod fp_crash;
mod gc;
mod namespace;
mod sim;
mod world;

use crate::world::run_seed;
#[cfg(feature = "failpoints")]
use crate::world::run_seed_mode;
use rand::Rng;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use zerofs::fs::EXTENT_SIZE;
use zerofs::fs::permissions::Credentials;
use zerofs::fs::types::AuthContext;

/// Logical file size cap: enough extents that reclaim/compaction have real
/// material, small enough that a run stays cheap.
const FILE_CAP: usize = 16 * EXTENT_SIZE;
pub(crate) const FILES: usize = 3;
/// Small segments so the background-seal and reclaim paths run at test scale.
const SEAL_THRESHOLD: usize = 96 * 1024;

/// World scale. Most seeds run small and fast; every fourth runs at chain
/// scale, with segments above `SMALL_SEGMENT_BYTES` (1 MiB) so they are not
/// unconditional compaction candidates and the dense-segment paths (pair
/// heat, chain assembly) are reachable.
#[derive(Clone, Copy)]
pub(crate) struct Scale {
    pub(crate) file_cap: usize,
    pub(crate) seal_threshold: usize,
}

pub(crate) fn scale_for(seed: u64) -> Scale {
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

pub(crate) fn env_or(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(v) => v
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer, got {v:?}")),
        Err(_) => default,
    }
}

pub(crate) fn creds() -> Credentials {
    Credentials {
        uid: 1000,
        gid: 1000,
        groups: [1000; 16],
        groups_count: 1,
    }
}

pub(crate) fn auth() -> AuthContext {
    AuthContext {
        uid: 1000,
        gid: 1000,
        gids: vec![1000],
    }
}

/// Where the sweep's seeds come from: an explicit reproduction list, or fresh
/// OS entropy bounded by a count or a real-time budget (new seeds stop when
/// the budget elapses; in-flight ones finish).
enum SeedSource {
    List(Mutex<VecDeque<u64>>),
    Count(AtomicUsize),
    Until(Instant),
}

impl SeedSource {
    fn next(&self) -> Option<u64> {
        match self {
            SeedSource::List(queue) => queue.lock().unwrap().pop_front(),
            SeedSource::Count(remaining) => {
                remaining
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                    .ok()?;
                Some(rand::thread_rng().r#gen())
            }
            SeedSource::Until(deadline) => {
                if Instant::now() >= *deadline {
                    return None;
                }
                Some(rand::thread_rng().r#gen())
            }
        }
    }
}

fn dst_seeds() -> Option<Vec<u64>> {
    let value = match std::env::var("DST_SEEDS") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => panic!("DST_SEEDS must be valid UTF-8"),
    };
    Some(
        value
            .split(',')
            .map(|seed| seed.trim().parse().expect("DST_SEEDS: u64 list"))
            .collect(),
    )
}

#[test]
fn seeds() {
    // DST_SEEDS runs an explicit list (reproduction). Otherwise seeds are
    // drawn fresh from OS entropy: DST_WALL_CLOCK_SECS keeps drawing new ones
    // until the real-time budget elapses (a soak), the default is 8. Every
    // seed is printed and a failure names its seed, so the repro is always
    // DST_SEEDS=<seed>.
    let source = match dst_seeds() {
        Some(seeds) => SeedSource::List(Mutex::new(seeds.into_iter().collect())),
        None => {
            let budget = env_or("DST_WALL_CLOCK_SECS", 0);
            if budget == 0 {
                SeedSource::Count(AtomicUsize::new(8))
            } else {
                SeedSource::Until(Instant::now() + Duration::from_secs(budget as u64))
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
                        let run = catch_unwind(AssertUnwindSafe(|| run_seed(seed)));
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
    let seeds = match dst_seeds() {
        Some(seeds) => seeds,
        None => {
            let mut rng = rand::thread_rng();
            (0..6).map(|_| rng.r#gen()).collect()
        }
    };
    for seed in seeds {
        eprintln!("dst: crash-point seed {seed}");
        run_seed_mode(seed, true);
    }
    scenario.teardown();
}

/// Every write acknowledged before a graceful close must survive reopen.
#[test]
fn graceful_close_durability() {
    let seed = dst_seeds()
        .and_then(|seeds| seeds.into_iter().next())
        .unwrap_or_else(|| rand::thread_rng().r#gen());
    eprintln!("dst: graceful close seed {seed}");
    crate::world::run_graceful_close_case(seed);
}

/// A failing seed is only a repro if the same seed is the same run. The seed
/// under replay is drawn fresh each run (or DST_SEEDS' first entry, so a
/// reproduction exercises the same one) and printed.
#[test]
fn same_seed_same_digest() {
    let seed = dst_seeds()
        .and_then(|seeds| seeds.into_iter().next())
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
