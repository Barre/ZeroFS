//! The GC actor: full reclaim passes racing the writers and namespace actor.

use crate::digest::Digest;
use chrono::Utc;
use rand::Rng;
use rand::rngs::StdRng;
use std::sync::Arc;
use std::time::Duration;
use zerofs::fs::ZeroFS;

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
pub(crate) async fn gc_round(
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
