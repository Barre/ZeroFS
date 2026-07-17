//! Standby heartbeat monitoring.

use crate::replication::transport::INCOMPATIBLE_HEARTBEAT_LIVENESS;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

fn require_compatible_heartbeat_protocol(liveness: u64) -> Result<()> {
    anyhow::ensure!(
        liveness != INCOMPATIBLE_HEARTBEAT_LIVENESS,
        "incompatible heartbeat protocol; upgrade both peers and restart the receiver"
    );
    Ok(())
}

/// Waits for a fresh accepted heartbeat, then reports lost heartbeat liveness.
/// A preexisting counter does not arm the timer. An incompatible-protocol latch
/// is terminal. The result authorizes only a durable marker claim attempt.
/// Returns `Ok(true)` for a claim attempt and `Ok(false)` on shutdown.
pub async fn watch_heartbeats_until_takeover_hint(
    mut liveness: tokio::sync::watch::Receiver<u64>,
    takeover_hint_after: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    takeover_trigger: Arc<tokio::sync::Notify>,
) -> Result<bool> {
    // A preexisting counter does not arm this invocation's timer.
    let baseline = *liveness.borrow_and_update();
    require_compatible_heartbeat_protocol(baseline)?;

    // Arm the timer on a fresh heartbeat only.
    loop {
        tokio::select! {
            // Hello triggers only for a node holding a replicated tail.
            _ = takeover_trigger.notified() => {
                require_compatible_heartbeat_protocol(*liveness.borrow())?;
                return Ok(true);
            }
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() { return Ok(false); }
            }
            res = liveness.changed() => {
                // A closed channel before observation is shutdown.
                if res.is_err() { return Ok(false); }
                let observed = *liveness.borrow_and_update();
                require_compatible_heartbeat_protocol(observed)?;
                if observed != 0 && observed != baseline {
                    break;
                }
            }
        }
    }
    // The durable marker claim determines authority.
    loop {
        require_compatible_heartbeat_protocol(*liveness.borrow())?;
        tokio::select! {
            _ = takeover_trigger.notified() => {
                require_compatible_heartbeat_protocol(*liveness.borrow())?;
                return Ok(true);
            }
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() { return Ok(false); }
            }
            res = tokio::time::timeout(takeover_hint_after, liveness.changed()) => {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        // Source teardown after observation is lost liveness.
                        require_compatible_heartbeat_protocol(*liveness.borrow())?;
                        return Ok(true);
                    }
                    Err(_) => {
                        require_compatible_heartbeat_protocol(*liveness.borrow())?;
                        return Ok(true);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    #[tokio::test]
    async fn hints_only_after_observed_then_silent() {
        let (live_tx, live_rx) = watch::channel(0u64);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_heartbeats_until_takeover_hint(
            live_rx,
            Duration::from_millis(200),
            sd_rx,
            Arc::new(tokio::sync::Notify::new()),
        ));

        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(!watcher.is_finished(), "no claim hint before a heartbeat");

        live_tx.send_modify(|c| *c += 1);
        tokio::time::sleep(Duration::from_millis(90)).await;
        live_tx.send_modify(|c| *c += 1);
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert!(
            !watcher.is_finished(),
            "no claim hint while heartbeats continue"
        );

        let should_attempt = tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("the watch should finish after beats stop")
            .unwrap()
            .unwrap();
        assert!(should_attempt, "claim hint after heartbeat timeout");
    }

    #[tokio::test]
    async fn a_second_watch_pass_requires_a_fresh_heartbeat() {
        let (live_tx, live_rx) = watch::channel(0u64);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let takeover_trigger = Arc::new(tokio::sync::Notify::new());

        let first = tokio::spawn(watch_heartbeats_until_takeover_hint(
            live_rx,
            Duration::from_millis(50),
            sd_rx.clone(),
            takeover_trigger.clone(),
        ));
        tokio::task::yield_now().await;
        live_tx.send(1).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), first)
                .await
                .expect("the first pass should hint after its fresh heartbeat goes silent")
                .unwrap()
                .unwrap()
        );

        let second = tokio::spawn(watch_heartbeats_until_takeover_hint(
            live_tx.subscribe(),
            Duration::from_millis(50),
            sd_rx,
            takeover_trigger,
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !second.is_finished(),
            "preexisting counter armed a new watch"
        );

        live_tx.send(2).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), second)
                .await
                .expect("the second pass should hint after a fresh heartbeat goes silent")
                .unwrap()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn shutdown_stands_down() {
        let (_live_tx, live_rx) = watch::channel(0u64);
        let (sd_tx, sd_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_heartbeats_until_takeover_hint(
            live_rx,
            Duration::from_millis(200),
            sd_rx,
            Arc::new(tokio::sync::Notify::new()),
        ));
        sd_tx.send(true).unwrap();
        let should_attempt = tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("the watch should finish on shutdown")
            .unwrap()
            .unwrap();
        assert!(!should_attempt, "shutdown produced a claim hint");
    }

    #[tokio::test]
    async fn incompatible_heartbeat_blocks_an_already_armed_takeover() {
        let (live_tx, live_rx) = watch::channel(1u64);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let watcher = tokio::spawn(watch_heartbeats_until_takeover_hint(
            live_rx,
            Duration::from_millis(50),
            sd_rx,
            Arc::new(tokio::sync::Notify::new()),
        ));

        tokio::task::yield_now().await;
        live_tx.send(2).unwrap();
        tokio::task::yield_now().await;
        live_tx.send(INCOMPATIBLE_HEARTBEAT_LIVENESS).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("the watcher must fail closed immediately")
            .unwrap();

        let error = result.expect_err("an incompatible live peer must never fund takeover");
        assert!(
            error
                .to_string()
                .contains("incompatible heartbeat protocol")
        );
    }
}
