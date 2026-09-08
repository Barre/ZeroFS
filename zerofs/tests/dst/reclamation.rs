//! Segment-reclamation cycles racing the writers and namespace actor.

use crate::digest::Digest;
use rand::Rng;
use rand::rngs::StdRng;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use zerofs::fs::store::extent::reclaim::cycle::{self, CyclePolicy, SegmentProtection};
use zerofs::fs::{TombstoneCleaner, ZeroFS};

/// Scan tuning is seeded independently: occasional indefinite protection exercises
/// the checkpoint-protected path, small job budgets exercise gather limits and
/// deferrals, and repeated jobs exercise continuous scheduling.
pub(crate) async fn reclamation_scans(
    fs: Arc<ZeroFS>,
    mut rng: StdRng,
    scan_count: usize,
    digest: Digest,
    mut crashed: tokio::sync::watch::Receiver<bool>,
) {
    for scan in 0..scan_count {
        if *crashed.borrow() {
            return;
        }
        let pause = rng.gen_range(1..=20);
        tokio::time::sleep(Duration::from_millis(pause)).await;
        // 1 in 8: simulate a persistent checkpoint, skipping deletion and repacking.
        let protection = if rng.gen_bool(0.125) {
            SegmentProtection::Indefinite
        } else {
            SegmentProtection::Until(tokio::time::Instant::now())
        };
        let policy = CyclePolicy {
            repack_min_dead_percent: rng.gen_range(5..=60u64),
            job_bytes: rng.gen_range(256 * 1024..=4 * 1024 * 1024u64),
            max_concurrent_repacks: rng.gen_range(1..=3usize),
        };
        let never_cancel = CancellationToken::new();
        tokio::select! {
            biased;
            r = cycle::run(
                &fs.extent_store,
                move || std::future::ready(Ok(protection)),
                policy,
                &never_cancel,
            ) => {
                let out = match r {
                    Ok(out) => out,
                    Err(_) if *crashed.borrow() => return,
                    Err(e) => panic!("reclaim scan {scan}: {e:?}"),
                };
                if std::env::var_os("DST_RECLAIM_LOG").is_some() {
                    eprintln!("dst: reclamation scan {scan}: {out:?}");
                }
                digest.event((
                    "reclaim",
                    scan,
                    out.deleted,
                    out.relocated,
                ));
            }
            _ = crashed.changed() => return,
        }
        if scan % 3 == 0 {
            tokio::select! {
                biased;
                result = cycle::sweep_orphans(&fs.extent_store, &never_cancel) => {
                    match result {
                        Ok(out) => digest.event(("online-orphans", scan, out.deleted())),
                        Err(_) if *crashed.borrow() => return,
                        Err(error) => panic!("online orphan sweep {scan}: {error:?}"),
                    }
                }
                _ = crashed.changed() => return,
            }
        }
    }
}

pub(crate) fn cleaner(fs: &ZeroFS) -> TombstoneCleaner {
    TombstoneCleaner::new(
        fs.tombstone_store.clone(),
        fs.extent_store.clone(),
        fs.stats.clone(),
    )
}

pub(crate) async fn tombstone_cleanup(
    fs: Arc<ZeroFS>,
    mut rng: StdRng,
    digest: Digest,
    mut crashed: tokio::sync::watch::Receiver<bool>,
    done: CancellationToken,
) {
    let cleaner = cleaner(&fs);
    loop {
        if *crashed.borrow() {
            return;
        }
        tokio::select! {
            biased;
            _ = done.cancelled() => return,
            _ = crashed.changed() => return,
            result = cleaner.run() => {
                match result {
                    Ok(()) => digest.event((
                        "tombstone-cleanup",
                        fs.stats.tombstone_cleanup_extents_deleted.load(Relaxed),
                    )),
                    Err(_) if *crashed.borrow() => return,
                    Err(error) => panic!("tombstone cleanup: {error:?}"),
                }
            }
        }
        tokio::select! {
            _ = done.cancelled() => return,
            _ = crashed.changed() => return,
            _ = tokio::time::sleep(Duration::from_millis(rng.gen_range(10..=50))) => {}
        }
    }
}
