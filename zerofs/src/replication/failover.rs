//! Standby failover: watch the leader's heartbeat liveness and decide when to
//! take over.

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

/// `liveness` ticks on each heartbeat accepted from the current-or-newer writer
/// epoch. A standby NEVER bootstraps: it waits to observe the leader at least
/// once before the staleness timer runs, so an absent leader is never elected.
/// Once seen, no beat for `takeover_ttl` (crash or hang) means take over
/// (`Ok(true)`); `Ok(false)` on shutdown.
pub async fn watch_heartbeats_until_takeover(
    mut liveness: tokio::sync::watch::Receiver<u64>,
    takeover_ttl: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    takeover_trigger: Arc<tokio::sync::Notify>,
) -> Result<bool> {
    // Phase 1: wait until the leader is observed at least once. No timeout: never
    // take over a leader never seen (no split brain on bring-up).
    while *liveness.borrow() == 0 {
        tokio::select! {
            // Told (via Hello) to take over now: only fired for a node holding a
            // tail, which has therefore observed the leader.
            _ = takeover_trigger.notified() => return Ok(true),
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() { return Ok(false); }
            }
            res = liveness.changed() => {
                // A closed channel pre-takeover means teardown, so stand down.
                if res.is_err() { return Ok(false); }
            }
        }
    }
    // Phase 2: take over when the observed leader's heartbeats stop.
    loop {
        tokio::select! {
            _ = takeover_trigger.notified() => return Ok(true),
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() { return Ok(false); }
            }
            res = tokio::time::timeout(takeover_ttl, liveness.changed()) => {
                match res {
                    Ok(Ok(())) => {}               // a beat: the leader is alive
                    Ok(Err(_)) => return Ok(true), // liveness channel closed
                    Err(_) => return Ok(true),     // no beat for takeover_ttl
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn takes_over_only_after_observed_then_silent() {
        let (live_tx, live_rx) = watch::channel(0u64);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_heartbeats_until_takeover(
            live_rx,
            Duration::from_millis(200),
            sd_rx,
            Arc::new(tokio::sync::Notify::new()),
        ));

        // No beat ever observed: must not take over even well past the ttl.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            !watcher.is_finished(),
            "must not take over a leader that was never seen"
        );

        // Observe the leader, then keep beating: still no takeover.
        live_tx.send_modify(|c| *c += 1);
        tokio::time::sleep(Duration::from_millis(90)).await;
        live_tx.send_modify(|c| *c += 1);
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert!(
            !watcher.is_finished(),
            "must not take over while beats continue"
        );

        let took_over = tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("the watch should finish after beats stop")
            .unwrap()
            .unwrap();
        assert!(took_over, "take over after the leader's beats go silent");
    }

    // A shutdown before any takeover stands the standby down (`Ok(false)`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_stands_down() {
        let (_live_tx, live_rx) = watch::channel(0u64);
        let (sd_tx, sd_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_heartbeats_until_takeover(
            live_rx,
            Duration::from_millis(200),
            sd_rx,
            Arc::new(tokio::sync::Notify::new()),
        ));
        sd_tx.send(true).unwrap();
        let took_over = tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("the watch should finish on shutdown")
            .unwrap()
            .unwrap();
        assert!(!took_over, "shutdown means stand down, not take over");
    }
}
