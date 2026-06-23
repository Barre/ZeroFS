//! The leader lease: a soft gate that stops a deposed leader serving stale reads
//! before its next write fails. Driven by the data db's own status (see
//! [`run_lease_from_status`]). The writer-epoch fence and `exit_on_write_error`
//! are the hard guarantees.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Monotonic-clock validity checked on the read and write path. `valid_until_ms`
/// is milliseconds from `base`; 0 means invalid.
pub struct Lease {
    base: Instant,
    valid_until_ms: AtomicU64,
}

impl Lease {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            valid_until_ms: AtomicU64::new(0),
        })
    }

    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    /// Valid for `ttl` from now. Never stores 0 (reserved for "invalid").
    pub fn renew(&self, ttl: Duration) {
        let until = self.now_ms().saturating_add(ttl.as_millis() as u64).max(1);
        self.valid_until_ms.store(until, Ordering::Release);
    }

    pub fn invalidate(&self) {
        self.valid_until_ms.store(0, Ordering::Release);
    }

    pub fn is_valid(&self) -> bool {
        let until = self.valid_until_ms.load(Ordering::Acquire);
        until != 0 && self.now_ms() < until
    }
}

/// Renew the lease while the data db is open, revoke it the instant the db is
/// closed. SlateDB closes with `CloseReason::Fenced` when its manifest poll sees
/// a newer writer (a takeover), so a deposed leader stops serving with no stale
/// reads, ahead of its next write failing.
pub async fn run_lease_from_status(
    lease: Arc<Lease>,
    mut status: tokio::sync::watch::Receiver<slatedb::DbStatus>,
    renew_interval: Duration,
    lease_ttl: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if status.borrow().close_reason.is_some() {
            tracing::error!(
                "HA: data db closed (fenced by a takeover / deposed); revoking the lease \
                 and stepping down."
            );
            lease.invalidate();
            return;
        }
        lease.renew(lease_ttl);
        tokio::select! {
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() { return; }
            }
            res = status.changed() => {
                // Channel closed: the data db was dropped, so stop serving.
                if res.is_err() { lease.invalidate(); return; }
            }
            _ = tokio::time::sleep(renew_interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_expires_renews_and_revokes() {
        let lease = Lease::new();
        assert!(!lease.is_valid(), "a fresh lease is invalid until renewed");

        lease.renew(Duration::from_millis(60));
        assert!(lease.is_valid());

        std::thread::sleep(Duration::from_millis(90));
        assert!(!lease.is_valid(), "the lease lapses after its ttl");

        lease.renew(Duration::from_millis(60));
        assert!(lease.is_valid());

        lease.invalidate();
        assert!(!lease.is_valid(), "invalidate revokes immediately");
    }
}
