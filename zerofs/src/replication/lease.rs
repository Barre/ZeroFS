//! Bounded serving authority from an exact Active marker or epoch-bound standby
//! acknowledgement. Expiry suspends serving for one final marker validation.
//! Ownership loss, validation failure, and database fencing are terminal.
#![cfg_attr(not(test), allow(dead_code))] // The CLI compiles this module separately.

use crate::replication::leader_record::{self, ActiveOwnership};
use crate::replication::replicator::{HeartbeatAck, ReplicationControl};
use slatedb::object_store::ObjectStore;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const UNINITIALIZED: u64 = 0;
const REVOKED: u64 = 1;
const CLOSED_CLEANLY: u64 = 2;
const SUSPENDED: u64 = 3;
const DEADLINE_BIAS: u64 = 4;

trait WallClock: Send + Sync {
    fn now(&self) -> SystemTime;
}

struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Atomic serving-authority state. Values 0 through 3 represent uninitialized,
/// revoked, cleanly closed, and suspended. Larger values encode a nanosecond
/// deadline relative to `base`. Only final marker recovery may replace
/// `SUSPENDED` with a deadline.
pub struct Lease {
    base: Instant,
    /// Secondary deadline anchor for hosts whose monotonic clock stops during
    /// suspension. Either clock may expire the lease. A backward wall-clock
    /// adjustment leaves the monotonic deadline authoritative.
    wall_base: SystemTime,
    wall_clock: Arc<dyn WallClock>,
    state: AtomicU64,
    /// Notification for terminal invalidation; `state` is authoritative.
    revoked: CancellationToken,
    /// Broadcast notification for serving-gate state changes.
    state_changed: Notify,
}

impl Lease {
    pub fn new() -> Arc<Self> {
        Self::new_with_wall_clock(Arc::new(SystemWallClock))
    }

    fn new_with_wall_clock(wall_clock: Arc<dyn WallClock>) -> Arc<Self> {
        let base = Instant::now();
        let wall_base = wall_clock.now();
        Arc::new(Self {
            base,
            wall_base,
            wall_clock,
            state: AtomicU64::new(UNINITIALIZED),
            revoked: CancellationToken::new(),
            state_changed: Notify::new(),
        })
    }

    fn offset_nanos(&self, instant: Instant) -> Option<u64> {
        let elapsed = instant.checked_duration_since(self.base)?;
        Some(elapsed.as_nanos().min((u64::MAX - DEADLINE_BIAS) as u128) as u64)
    }

    fn encoded_deadline(&self, start: Instant, ttl: Duration) -> Option<u64> {
        let deadline = start.checked_add(ttl)?;
        Some(self.offset_nanos(deadline)?.saturating_add(DEADLINE_BIAS))
    }

    fn now_nanos(&self) -> u64 {
        self.offset_nanos(Instant::now()).unwrap_or(0)
    }

    fn deadline_nanos(state: u64) -> u64 {
        state - DEADLINE_BIAS
    }

    fn deadline_instant(&self, state: u64) -> Instant {
        self.base + Duration::from_nanos(Self::deadline_nanos(state))
    }

    fn deadline_expired(&self, state: u64) -> bool {
        let offset = Duration::from_nanos(Self::deadline_nanos(state));
        if self.now_nanos() >= Self::deadline_nanos(state) {
            return true;
        }
        let Some(wall_deadline) = self.wall_base.checked_add(offset) else {
            return true;
        };
        self.wall_clock.now().duration_since(wall_deadline).is_ok()
    }

    /// Atomically suspends serving after the deadline.
    fn suspend_expiry(&self) -> bool {
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                SUSPENDED => return true,
                UNINITIALIZED | REVOKED | CLOSED_CLEANLY => return false,
                _ if !self.deadline_expired(state) => return false,
                _ => {
                    if self
                        .state
                        .compare_exchange(state, SUSPENDED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.state_changed.notify_waiters();
                        return true;
                    }
                }
            }
        }
    }

    /// Suspends serving after an observed authority gap, including across a
    /// concurrent renewal.
    fn suspend_after_gap(&self) {
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                UNINITIALIZED | REVOKED | CLOSED_CLEANLY | SUSPENDED => return,
                _ => {
                    if self
                        .state
                        .compare_exchange(state, SUSPENDED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.state_changed.notify_waiters();
                        return;
                    }
                }
            }
        }
    }

    /// Activates an uninitialized lease from the authority request start time.
    pub fn activate_from(&self, start: Instant, ttl: Duration) -> bool {
        if self.state.load(Ordering::Acquire) != UNINITIALIZED {
            return false;
        }

        let Some(deadline) = self.encoded_deadline(start, ttl) else {
            if self
                .state
                .compare_exchange(UNINITIALIZED, REVOKED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.revoked.cancel();
            }
            return false;
        };
        if self.deadline_expired(deadline) {
            if self
                .state
                .compare_exchange(UNINITIALIZED, REVOKED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.revoked.cancel();
            }
            return false;
        }

        let activated = self
            .state
            .compare_exchange(UNINITIALIZED, deadline, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if activated && self.deadline_expired(deadline) {
            // Do not publish a grant if the deadline crossed during the CAS.
            self.revoke();
            return false;
        }
        activated
    }

    /// Renews an active lease from the authority request start time. A response
    /// received after expiry suspends serving.
    pub fn renew_from(&self, start: Instant, ttl: Duration) -> bool {
        let proposed = self.encoded_deadline(start, ttl);
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                UNINITIALIZED | REVOKED | CLOSED_CLEANLY | SUSPENDED => return false,
                _ if self.deadline_expired(state) => {
                    if self
                        .state
                        .compare_exchange(state, SUSPENDED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.state_changed.notify_waiters();
                        return false;
                    }
                }
                _ => {
                    let Some(proposed) = proposed else {
                        return false;
                    };
                    // An out-of-order response cannot shorten the deadline.
                    if proposed <= state {
                        return true;
                    }
                    if self
                        .state
                        .compare_exchange(state, proposed, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        // Suspension wins if the previous deadline crossed during the CAS.
                        if self.deadline_expired(state) {
                            self.suspend_after_gap();
                            return false;
                        }
                        return true;
                    }
                }
            }
        }
    }

    /// Replaces `SUSPENDED` with a deadline after final marker validation.
    fn recover_from(&self, start: Instant, ttl: Duration) -> bool {
        let Some(deadline) = self.encoded_deadline(start, ttl) else {
            return false;
        };
        if self.deadline_expired(deadline) {
            return false;
        }
        if self
            .state
            .compare_exchange(SUSPENDED, deadline, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.state_changed.notify_waiters();
        if self.deadline_expired(deadline) {
            // Do not publish a grant if the deadline crossed during the CAS.
            self.revoke();
            return false;
        }
        true
    }

    /// Test helper for activating or renewing from the current instant.
    #[cfg(test)]
    pub fn renew(&self, ttl: Duration) {
        let start = Instant::now();
        if self.state.load(Ordering::Acquire) == UNINITIALIZED {
            let _ = self.activate_from(start, ttl);
        } else {
            let _ = self.renew_from(start, ttl);
        }
    }

    /// Permanently revokes the lease. The first terminal transition wins.
    pub fn revoke(&self) {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if matches!(state, REVOKED | CLOSED_CLEANLY) {
                return;
            }
            if self
                .state
                .compare_exchange(state, REVOKED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.state_changed.notify_waiters();
                self.revoked.cancel();
                return;
            }
        }
    }

    /// Records an orderly database close unless revocation already won.
    fn close_cleanly(&self) -> bool {
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                REVOKED => return false,
                CLOSED_CLEANLY => return true,
                _ if state == SUSPENDED
                    || (state >= DEADLINE_BIAS && self.deadline_expired(state)) =>
                {
                    if self
                        .state
                        .compare_exchange(state, REVOKED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.state_changed.notify_waiters();
                        self.revoked.cancel();
                        return false;
                    }
                }
                _ => {
                    if self
                        .state
                        .compare_exchange(
                            state,
                            CLOSED_CLEANLY,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.state_changed.notify_waiters();
                        self.revoked.cancel();
                        return true;
                    }
                }
            }
        }
    }

    fn was_closed_cleanly(&self) -> bool {
        self.state.load(Ordering::Acquire) == CLOSED_CLEANLY
    }

    fn is_uninitialized(&self) -> bool {
        self.state.load(Ordering::Acquire) == UNINITIALIZED
    }

    /// Resolves on terminal invalidation, not recoverable suspension.
    pub async fn revoked(&self) {
        self.revoked.cancelled().await;
    }

    /// Resolves after atomically suspending an expired lease.
    async fn suspended(&self) {
        loop {
            if self.suspend_expiry() {
                return;
            }
            let notified = self.state_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let state = self.state.load(Ordering::Acquire);
            match state {
                SUSPENDED => return,
                REVOKED | CLOSED_CLEANLY | UNINITIALIZED => {
                    notified.await;
                }
                _ => {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(self.deadline_instant(state)) => {}
                    }
                }
            }
        }
    }

    /// Waits through recoverable suspension for already-admitted internal work.
    /// Terminal and uninitialized states return false. Protocol admission and
    /// responses remain gated by [`Self::is_valid`].
    pub(crate) async fn wait_until_internal_work_is_permitted(&self) -> bool {
        loop {
            let notified = self.state_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let state = self.state.load(Ordering::Acquire);
            match state {
                UNINITIALIZED | REVOKED | CLOSED_CLEANLY => return false,
                SUSPENDED => notified.await,
                _ if self.deadline_expired(state) => {
                    let _ = self.suspend_expiry();
                }
                _ => return true,
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        if state < DEADLINE_BIAS {
            return false;
        }
        if !self.deadline_expired(state) {
            return true;
        }
        let _ = self.suspend_expiry();
        false
    }

    fn is_suspended(&self) -> bool {
        self.state.load(Ordering::Acquire) == SUSPENDED
    }

    #[cfg(test)]
    pub(crate) fn suspend_for_tests(&self) {
        self.suspend_after_gap();
    }

    #[cfg(test)]
    pub(crate) fn recover_for_tests(&self, ttl: Duration) -> bool {
        self.recover_from(Instant::now(), ttl)
    }

    fn is_terminal(&self) -> bool {
        matches!(self.state.load(Ordering::Acquire), REVOKED | CLOSED_CLEANLY)
    }
}

/// Validates newly published Active ownership before listener startup. The
/// grant begins at marker-read start time.
pub async fn activate_lease_from_marker(
    lease: &Lease,
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    ownership: &ActiveOwnership,
) -> anyhow::Result<()> {
    activate_lease_until_valid(lease, crate::replication::AUTHORITY_TTL, || {
        leader_record::validate_active(object_store, db_path, ownership)
    })
    .await
}

async fn activate_lease_until_valid<F, Fut>(
    lease: &Lease,
    authority_ttl: Duration,
    mut validate: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<bool>>,
{
    let mut failures = 0u64;
    loop {
        match activate_lease_with_validator(lease, authority_ttl, &mut validate).await {
            Ok(()) => return Ok(()),
            Err(error) if lease.is_uninitialized() => {
                if failures == 0 {
                    tracing::warn!(
                        "initial HA Active marker validation failed; listeners closed: {error:#}"
                    );
                } else {
                    tracing::debug!(
                        "initial HA Active marker validation still failing; listeners closed: \
                         {error:#}"
                    );
                }
                failures = failures.saturating_add(1);
                tokio::time::sleep(crate::replication::MARKER_VALIDATION_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn activate_lease_with_validator<F, Fut>(
    lease: &Lease,
    authority_ttl: Duration,
    validate: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<bool>>,
{
    anyhow::ensure!(
        !authority_ttl.is_zero(),
        "HA authority TTL must be greater than zero"
    );
    let validation_started = Instant::now();
    let exact = tokio::time::timeout(authority_ttl, validate())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timed out after {authority_ttl:?} validating the active HA leader record"
            )
        })??;
    if !exact {
        lease.revoke();
        anyhow::bail!("the active HA leader record no longer names this writer");
    }
    let minimum_headroom = authority_ttl / 3;
    let remaining = validation_started
        .checked_add(authority_ttl)
        .map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        });
    if remaining < minimum_headroom {
        anyhow::bail!(
            "the exact HA leader record validation left only {remaining:?} of its \
             request-start authority; retrying before listeners are exposed"
        );
    }
    if !lease.activate_from(validation_started, authority_ttl) {
        anyhow::bail!(
            "the HA leader record validation did not activate a fresh serving lease before its \
             deadline"
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct AuthorityTiming {
    marker_validation_interval: Duration,
    marker_validation_timeout: Duration,
    authority_ttl: Duration,
}

/// Owns lease renewal and retirement.
pub struct AuthoritySupervisor {
    lease: Arc<Lease>,
    loss: CancellationToken,
    deposal: Option<CancellationToken>,
    task: Option<JoinHandle<()>>,
}

impl AuthoritySupervisor {
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn start(
        lease: Arc<Lease>,
        object_store: Arc<dyn ObjectStore>,
        db_path: String,
        ownership: ActiveOwnership,
        status: watch::Receiver<slatedb::DbStatus>,
        replication: Option<Arc<ReplicationControl>>,
        peer_acks: Option<watch::Receiver<Option<HeartbeatAck>>>,
    ) -> anyhow::Result<Self> {
        Self::start_with_validator(
            lease,
            AuthorityTiming {
                marker_validation_interval: crate::replication::MARKER_VALIDATION_INTERVAL,
                marker_validation_timeout: crate::replication::MARKER_VALIDATION_TIMEOUT,
                authority_ttl: crate::replication::AUTHORITY_TTL,
            },
            status,
            replication,
            peer_acks,
            move || {
                let object_store = Arc::clone(&object_store);
                let db_path = db_path.clone();
                let ownership = ownership.clone();
                async move { leader_record::validate_active(&object_store, &db_path, &ownership).await }
            },
        )
    }

    fn start_with_validator<F, Fut>(
        lease: Arc<Lease>,
        timing: AuthorityTiming,
        status: watch::Receiver<slatedb::DbStatus>,
        replication: Option<Arc<ReplicationControl>>,
        peer_acks: Option<watch::Receiver<Option<HeartbeatAck>>>,
        validate: F,
    ) -> anyhow::Result<Self>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<bool>> + Send + 'static,
    {
        let loss = CancellationToken::new();
        let deposal = replication.as_ref().map(|control| control.deposal_token());
        let startup_error = if timing.marker_validation_interval.is_zero()
            || timing.marker_validation_timeout.is_zero()
            || timing.authority_ttl.is_zero()
        {
            Some("HA authority intervals must be greater than zero")
        } else if !lease.is_valid() {
            Some("HA authority supervisor requires an active serving lease")
        } else if status.borrow().close_reason.is_some() || status.has_changed().is_err() {
            Some("HA authority supervisor cannot start after the data db has closed")
        } else {
            None
        };
        if let Some(error) = startup_error {
            lease.revoke();
            deposal.iter().for_each(CancellationToken::cancel);
            anyhow::bail!(error);
        }
        let guard = AuthorityGuard::new(Arc::clone(&lease), loss.clone(), deposal.clone());
        let task = tokio::spawn(run_authority(
            guard,
            status,
            replication,
            peer_acks,
            timing,
            validate,
        ));
        Ok(Self {
            lease,
            loss,
            deposal,
            task: Some(task),
        })
    }

    pub fn lease(&self) -> Arc<Lease> {
        Arc::clone(&self.lease)
    }

    pub fn loss_token(&self) -> CancellationToken {
        self.loss.clone()
    }

    /// Finishes after serving drains and `Db::close` succeeds.
    pub(crate) async fn finish_after_close(mut self) {
        if let Some(deposal) = &self.deposal {
            deposal.cancel();
        }
        let _ = self.lease.close_cleanly();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for AuthoritySupervisor {
    fn drop(&mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        if let Some(deposal) = &self.deposal {
            deposal.cancel();
        }
        self.lease.revoke();
        if !self.lease.was_closed_cleanly() {
            self.loss.cancel();
        }
    }
}

/// Revokes authority if the supervisor task exits without disarming the guard.
struct AuthorityGuard {
    lease: Arc<Lease>,
    loss: CancellationToken,
    deposal: Option<CancellationToken>,
    armed: bool,
}

impl AuthorityGuard {
    fn new(lease: Arc<Lease>, loss: CancellationToken, deposal: Option<CancellationToken>) -> Self {
        Self {
            lease,
            loss,
            deposal,
            armed: true,
        }
    }

    fn finish(&mut self, lost: bool) {
        if lost {
            self.lease.revoke();
            if !self.lease.was_closed_cleanly() {
                self.loss.cancel();
            }
        }
        if let Some(deposal) = &self.deposal {
            deposal.cancel();
        }
        self.armed = false;
    }
}

impl Drop for AuthorityGuard {
    fn drop(&mut self) {
        if self.armed {
            self.finish(true);
        }
    }
}

enum AuthorityEvent {
    Revoked,
    Suspended,
    DbClosed(Option<slatedb::CloseReason>),
    PeerAck(HeartbeatAck),
    PeerAckChannelClosed,
    PeerAckStale,
    Validated(
        Instant,
        Result<anyhow::Result<bool>, tokio::time::error::Elapsed>,
    ),
}

enum FinalRecoveryEvent {
    Revoked,
    DbClosed(Option<slatedb::CloseReason>),
    Validated(
        Instant,
        Result<anyhow::Result<bool>, tokio::time::error::Elapsed>,
    ),
}

async fn wait_for_db_close(
    status: &mut watch::Receiver<slatedb::DbStatus>,
) -> Option<slatedb::CloseReason> {
    match status
        .wait_for(|status| status.close_reason.is_some())
        .await
    {
        Ok(status) => status.close_reason,
        Err(_) => None,
    }
}

async fn finish_authority(
    guard: &mut AuthorityGuard,
    lost: bool,
    replication: Option<&Arc<ReplicationControl>>,
) {
    guard.finish(lost);
    if let Some(replication) = replication {
        replication.depose().await;
    }
}

async fn next_peer_ack(
    peer_acks: &mut Option<watch::Receiver<Option<HeartbeatAck>>>,
) -> Option<HeartbeatAck> {
    let Some(peer_acks) = peer_acks else {
        return std::future::pending().await;
    };
    if peer_acks.changed().await.is_err() {
        return None;
    }
    *peer_acks.borrow_and_update()
}

/// Runs the single marker recovery attempt after lease suspension. Heartbeat
/// acknowledgements cannot recover a suspended lease.
async fn final_marker_recovery<F, Fut>(
    lease: &Lease,
    status: &mut watch::Receiver<slatedb::DbStatus>,
    authority_ttl: Duration,
    validate: &mut F,
) -> FinalRecoveryEvent
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<bool>>,
{
    let started = Instant::now();
    let timeout = authority_ttl.min(crate::replication::FINAL_MARKER_RECOVERY_TIMEOUT);
    let db_close = wait_for_db_close(status);
    let validation = tokio::time::timeout(timeout, validate());
    tokio::pin!(db_close, validation);
    tokio::select! {
        biased;
        _ = lease.revoked() => FinalRecoveryEvent::Revoked,
        reason = &mut db_close => FinalRecoveryEvent::DbClosed(reason),
        result = &mut validation => FinalRecoveryEvent::Validated(started, result),
    }
}

/// Applies terminal database status. Returns false for a winning clean close.
fn db_lost(lease: &Lease, reason: Option<slatedb::CloseReason>) -> bool {
    match reason {
        Some(slatedb::CloseReason::Clean) => {
            tracing::info!("HA metadata store closed cleanly");
            !lease.close_cleanly()
        }
        Some(slatedb::CloseReason::Fenced) => {
            tracing::warn!("HA metadata store fenced; stepping down");
            true
        }
        Some(other) => {
            tracing::error!("HA metadata store closed; reason={other:?}; stepping down");
            true
        }
        None => {
            tracing::error!("HA metadata status channel closed; stepping down");
            true
        }
    }
}

async fn run_authority<F, Fut>(
    mut guard: AuthorityGuard,
    mut status: watch::Receiver<slatedb::DbStatus>,
    replication: Option<Arc<ReplicationControl>>,
    mut peer_acks: Option<watch::Receiver<Option<HeartbeatAck>>>,
    timing: AuthorityTiming,
    mut validate: F,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<bool>> + Send + 'static,
{
    let AuthorityTiming {
        marker_validation_interval,
        marker_validation_timeout,
        authority_ttl,
    } = timing;
    let mut ack_fresh_until = None;
    if let Some(ack) = peer_acks
        .as_mut()
        .and_then(|acks| *acks.borrow_and_update())
        && guard.lease.renew_from(ack.started, authority_ttl)
    {
        ack_fresh_until = ack
            .started
            .checked_add(crate::replication::HEARTBEAT_ACK_FRESH_FOR)
            .filter(|deadline| *deadline > Instant::now());
    }
    let mut next_validation = ack_fresh_until.unwrap_or_else(Instant::now);
    let mut failures = 0u64;
    loop {
        let ack_is_fresh = ack_fresh_until.is_some_and(|deadline| deadline > Instant::now());
        let event = {
            let db_close = wait_for_db_close(&mut status);
            let peer_ack = next_peer_ack(&mut peer_acks);
            let stale = async move {
                if let Some(deadline) = ack_fresh_until.filter(|_| ack_is_fresh) {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let validation = async {
                if ack_is_fresh {
                    return std::future::pending().await;
                }
                tokio::time::sleep_until(next_validation).await;
                let started = Instant::now();
                (
                    started,
                    tokio::time::timeout(marker_validation_timeout, validate()).await,
                )
            };
            tokio::pin!(db_close, peer_ack, stale, validation);
            tokio::select! {
                biased;
                _ = guard.lease.revoked() => AuthorityEvent::Revoked,
                reason = &mut db_close => AuthorityEvent::DbClosed(reason),
                _ = guard.lease.suspended() => AuthorityEvent::Suspended,
                (started, result) = &mut validation => AuthorityEvent::Validated(started, result),
                ack = &mut peer_ack => match ack {
                    Some(ack) => AuthorityEvent::PeerAck(ack),
                    None => AuthorityEvent::PeerAckChannelClosed,
                },
                _ = &mut stale => AuthorityEvent::PeerAckStale,
            }
        };

        match event {
            AuthorityEvent::Revoked => {
                let lost = !guard.lease.was_closed_cleanly();
                if lost {
                    tracing::error!("HA serving authority revoked; stepping down");
                }
                finish_authority(&mut guard, lost, replication.as_ref()).await;
                return;
            }
            AuthorityEvent::DbClosed(reason) => {
                let lost = db_lost(&guard.lease, reason);
                finish_authority(&mut guard, lost, replication.as_ref()).await;
                return;
            }
            AuthorityEvent::Suspended => {
                tracing::warn!(
                    "HA serving authority expired; responses suspended during final marker \
                     validation"
                );
                match final_marker_recovery(&guard.lease, &mut status, authority_ttl, &mut validate)
                    .await
                {
                    FinalRecoveryEvent::Revoked => {
                        finish_authority(&mut guard, true, replication.as_ref()).await;
                        return;
                    }
                    FinalRecoveryEvent::DbClosed(reason) => {
                        let lost = db_lost(&guard.lease, reason);
                        finish_authority(&mut guard, lost, replication.as_ref()).await;
                        return;
                    }
                    FinalRecoveryEvent::Validated(started, result) => {
                        // Terminal database state takes precedence over recovery.
                        let close_reason = status.borrow().close_reason;
                        if let Some(reason) = close_reason {
                            let lost = db_lost(&guard.lease, Some(reason));
                            finish_authority(&mut guard, lost, replication.as_ref()).await;
                            return;
                        }
                        if status.has_changed().is_err() {
                            finish_authority(&mut guard, true, replication.as_ref()).await;
                            return;
                        }
                        match result {
                            Ok(Ok(true)) if guard.lease.recover_from(started, authority_ttl) => {
                                tracing::info!(
                                    "HA serving authority recovered from exact Active marker"
                                );
                                ack_fresh_until = None;
                                next_validation = started + marker_validation_interval;
                                failures = 0;
                            }
                            Ok(Ok(true)) => {
                                tracing::error!(
                                    "HA final marker validation exceeded recovery deadline; \
                                     stepping down"
                                );
                                finish_authority(&mut guard, true, replication.as_ref()).await;
                                return;
                            }
                            Ok(Ok(false)) => {
                                tracing::error!(
                                    "HA Active marker changed during final recovery; stepping down"
                                );
                                finish_authority(&mut guard, true, replication.as_ref()).await;
                                return;
                            }
                            Ok(Err(error)) => {
                                tracing::error!(
                                    "HA final Active marker validation failed; stepping down: \
                                     {error:#}"
                                );
                                finish_authority(&mut guard, true, replication.as_ref()).await;
                                return;
                            }
                            Err(_) => {
                                tracing::error!(
                                    "HA final Active marker validation timed out; stepping down"
                                );
                                finish_authority(&mut guard, true, replication.as_ref()).await;
                                return;
                            }
                        }
                    }
                }
            }
            AuthorityEvent::PeerAck(ack) => {
                if !guard.lease.renew_from(ack.started, authority_ttl) {
                    if guard.lease.is_suspended() {
                        ack_fresh_until = None;
                        next_validation = Instant::now();
                        continue;
                    }
                    finish_authority(&mut guard, true, replication.as_ref()).await;
                    return;
                }
                let fresh_until = ack
                    .started
                    .checked_add(crate::replication::HEARTBEAT_ACK_FRESH_FOR)
                    .filter(|deadline| *deadline > Instant::now());
                if let Some(fresh_until) = fresh_until {
                    ack_fresh_until = Some(fresh_until);
                    next_validation = fresh_until;
                    failures = 0;
                }
            }
            AuthorityEvent::PeerAckChannelClosed => {
                peer_acks = None;
                ack_fresh_until = None;
                next_validation = Instant::now();
            }
            AuthorityEvent::PeerAckStale => {
                ack_fresh_until = None;
                next_validation = Instant::now();
            }
            AuthorityEvent::Validated(started, result) => {
                next_validation = started + marker_validation_interval;
                // Terminal state takes precedence over renewal.
                if guard.lease.is_terminal() {
                    finish_authority(&mut guard, true, replication.as_ref()).await;
                    return;
                }
                let close_reason = status.borrow().close_reason;
                if let Some(reason) = close_reason {
                    let lost = db_lost(&guard.lease, Some(reason));
                    finish_authority(&mut guard, lost, replication.as_ref()).await;
                    return;
                }
                if status.has_changed().is_err() {
                    finish_authority(&mut guard, true, replication.as_ref()).await;
                    return;
                }
                match result {
                    Ok(Ok(true)) if guard.lease.renew_from(started, authority_ttl) => failures = 0,
                    Ok(Ok(true))
                        if guard.lease.is_suspended()
                            && guard.lease.recover_from(started, authority_ttl) =>
                    {
                        tracing::info!("HA serving authority recovered from exact Active marker");
                        ack_fresh_until = None;
                        next_validation = started + marker_validation_interval;
                        failures = 0;
                    }
                    Ok(Ok(true)) => {
                        tracing::error!("HA marker validation completed after authority expiry");
                        finish_authority(&mut guard, true, replication.as_ref()).await;
                        return;
                    }
                    Ok(Ok(false)) => {
                        tracing::error!("HA Active marker changed; stepping down");
                        finish_authority(&mut guard, true, replication.as_ref()).await;
                        return;
                    }
                    Ok(Err(error)) => {
                        if failures == 0 {
                            tracing::warn!(
                                "HA Active marker validation failed; lease not renewed: {error:#}"
                            );
                        } else {
                            tracing::debug!("HA Active marker validation still failing: {error:#}");
                        }
                        failures = failures.saturating_add(1);
                    }
                    Err(_) => {
                        if failures == 0 {
                            tracing::warn!(
                                "HA Active marker validation timed out; timeout=\
                                 {marker_validation_timeout:?}; lease not renewed"
                            );
                        } else {
                            tracing::debug!("HA Active marker validation still timing out");
                        }
                        failures = failures.saturating_add(1);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestWallClock {
        nanos: AtomicU64,
    }

    impl TestWallClock {
        fn advance(&self, duration: Duration) {
            let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
            self.nanos.fetch_add(nanos, Ordering::SeqCst);
        }
    }

    impl WallClock for TestWallClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
        }
    }

    fn test_lease() -> Arc<Lease> {
        Lease::new_with_wall_clock(Arc::new(TestWallClock::default()))
    }

    fn lease_with_wall_clock() -> (Arc<Lease>, Arc<TestWallClock>) {
        let wall_clock = Arc::new(TestWallClock::default());
        let lease = Lease::new_with_wall_clock(wall_clock.clone());
        (lease, wall_clock)
    }

    fn active(ttl: Duration) -> Arc<Lease> {
        let lease = test_lease();
        assert!(lease.activate_from(Instant::now(), ttl));
        lease
    }

    fn replication() -> (crate::replication::Replicator, Arc<ReplicationControl>) {
        crate::replication::Replicator::new(
            "unused".into(),
            crate::replication::types::WriterEpoch::new(1).unwrap(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn injected_wall_clock_expires_without_monotonic_progress() {
        let (lease, wall_clock) = lease_with_wall_clock();
        assert!(lease.activate_from(Instant::now(), Duration::from_secs(1)));

        wall_clock.advance(Duration::from_millis(999));
        assert!(lease.is_valid());

        wall_clock.advance(Duration::from_millis(1));
        assert!(!lease.is_valid());
        assert!(!lease.renew_from(Instant::now(), Duration::from_secs(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_uses_request_time_and_never_shortens_a_newer_grant() {
        let lease = active(Duration::from_secs(1));
        tokio::time::advance(Duration::from_millis(100)).await;
        let started = Instant::now();
        tokio::time::advance(Duration::from_millis(200)).await;
        assert!(lease.renew_from(started, Duration::from_secs(1)));
        tokio::time::advance(Duration::from_millis(801)).await;
        assert!(!lease.is_valid(), "response time must not extend the grant");

        let lease = active(Duration::from_millis(100));
        let old = Instant::now();
        tokio::time::advance(Duration::from_millis(80)).await;
        assert!(lease.renew_from(Instant::now(), Duration::from_millis(100)));
        assert!(lease.renew_from(old, Duration::from_millis(100)));
        tokio::time::advance(Duration::from_millis(21)).await;
        assert!(
            lease.is_valid(),
            "an old response shortened the newer grant"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn initial_marker_grant_is_anchored_at_request_start() {
        let lease = test_lease();
        let activating = tokio::spawn({
            let lease = lease.clone();
            async move {
                activate_lease_until_valid(&lease, Duration::from_millis(100), || async {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    Ok(true)
                })
                .await
            }
        });
        settle().await;
        tokio::time::advance(Duration::from_millis(40)).await;
        activating.await.unwrap().unwrap();
        tokio::time::advance(Duration::from_millis(61)).await;
        assert!(!lease.is_valid(), "response latency must consume the grant");
    }

    #[tokio::test(start_paused = true)]
    async fn slow_initial_marker_result_retries_before_exposing_a_short_grant() {
        let ttl = Duration::from_millis(300);
        let lease = test_lease();
        let validating = tokio::spawn({
            let lease = lease.clone();
            async move {
                activate_lease_with_validator(&lease, ttl, || async {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    Ok(true)
                })
                .await
            }
        });
        settle().await;
        tokio::time::advance(Duration::from_millis(250)).await;
        assert!(validating.await.unwrap().is_err());
        assert!(
            lease.is_uninitialized(),
            "listeners must not inherit a nearly exhausted startup grant"
        );

        activate_lease_with_validator(&lease, ttl, || async { Ok(true) })
            .await
            .unwrap();
        assert!(lease.is_valid());
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_peer_acks_short_circuit_marker_validation() {
        let (db, _tx, status) = status_channel("lease-ack-fast-path").await;
        let lease = active(Duration::from_secs(1));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_tx, ack_rx) = watch::channel(None);
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            Duration::from_secs(1),
            None,
            Some(ack_rx),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Ok(true) }
                }
            },
        );

        for _ in 1..=10 {
            ack_tx
                .send(Some(HeartbeatAck {
                    started: Instant::now(),
                }))
                .unwrap();
            settle().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        settle().await;
        assert!(lease.is_valid());
        assert_eq!(attempts.load(Ordering::SeqCst), 0);

        drop(authority);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_peer_ack_resumes_marker_validation() {
        let (db, _tx, status) = status_channel("lease-ack-stale").await;
        let lease = active(Duration::from_secs(1));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_tx, ack_rx) = watch::channel(None);
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            Duration::from_secs(1),
            None,
            Some(ack_rx),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Ok(true) }
                }
            },
        );
        ack_tx
            .send(Some(HeartbeatAck {
                started: Instant::now(),
            }))
            .unwrap();
        settle().await;
        tokio::time::advance(crate::replication::HEARTBEAT_ACK_FRESH_FOR).await;
        settle().await;

        assert!(attempts.load(Ordering::SeqCst) >= 1);
        assert!(
            lease.is_valid(),
            "the exact marker preserves Solo authority"
        );
        drop(authority);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_ack_does_not_suppress_the_marker_path() {
        let (db, _tx, status) = status_channel("lease-delayed-ack").await;
        let lease = active(Duration::from_secs(1));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Instant::now();
        tokio::time::advance(crate::replication::HEARTBEAT_ACK_FRESH_FOR).await;
        let (_ack_tx, ack_rx) = watch::channel(Some(HeartbeatAck { started }));
        let authority = test_authority(
            lease,
            status,
            Duration::from_millis(100),
            Duration::from_secs(1),
            None,
            Some(ack_rx),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Ok(true) }
                }
            },
        );
        settle().await;
        tokio::time::advance(Duration::from_millis(101)).await;
        settle().await;
        assert!(attempts.load(Ordering::SeqCst) >= 1);
        drop(authority);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn marker_mismatch_is_terminal_and_a_later_ack_cannot_revive_it() {
        let (db, _tx, status) = status_channel("lease-marker-mismatch").await;
        let lease = active(Duration::from_secs(1));
        let (ack_tx, ack_rx) = watch::channel(None);
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            Duration::from_secs(1),
            None,
            Some(ack_rx),
            || async { Ok(false) },
        );
        let loss = authority.loss_token();
        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert!(loss.is_cancelled());
        assert!(!lease.is_valid());
        let _ = ack_tx.send(Some(HeartbeatAck {
            started: Instant::now(),
        }));
        assert!(!lease.is_valid());
        drop(authority);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_gates_serving_until_one_exact_marker_recovery_succeeds() {
        let (db, _tx, status) = status_channel("lease-final-recovery").await;
        let ttl = Duration::from_millis(300);
        let lease = active(ttl);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let final_release = Arc::new(tokio::sync::Notify::new());
        let (ack_tx, ack_rx) = watch::channel(None);
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            ttl,
            None,
            Some(ack_rx),
            {
                let attempts = attempts.clone();
                let final_release = final_release.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let final_release = final_release.clone();
                    async move {
                        if attempt == 0 {
                            std::future::pending::<()>().await;
                        }
                        final_release.notified().await;
                        Ok(true)
                    }
                }
            },
        );
        let loss = authority.loss_token();

        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(lease.is_valid());

        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        assert!(!lease.is_valid(), "the expired gate must remain closed");
        assert!(
            !loss.is_cancelled(),
            "the final marker attempt is recoverable"
        );

        ack_tx
            .send(Some(HeartbeatAck {
                started: Instant::now(),
            }))
            .unwrap();
        settle().await;
        assert!(
            !lease.is_valid(),
            "a heartbeat ACK must not recover suspended authority"
        );

        tokio::time::advance(Duration::from_millis(200)).await;
        settle().await;
        assert!(!lease.is_valid(), "serving stays gated for the whole read");
        final_release.notify_one();
        settle().await;
        assert!(
            lease.is_valid(),
            "the one exact marker result should reopen the gate"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            5,
            "a slow recovery leaves little request-start authority, so the next degraded read \
             must start immediately"
        );
        assert!(!loss.is_cancelled());

        drop(authority);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn final_marker_timeout_is_once_and_terminal_despite_a_later_ack() {
        let (db, _tx, status) = status_channel("lease-final-timeout").await;
        let ttl = Duration::from_millis(300);
        let lease = active(ttl);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ack_tx, ack_rx) = watch::channel(None);
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            ttl,
            None,
            Some(ack_rx),
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<anyhow::Result<bool>>()
                }
            },
        );
        let loss = authority.loss_token();

        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(lease.is_valid());

        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(lease.is_valid());

        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        assert!(!lease.is_valid());
        assert!(!loss.is_cancelled());

        tokio::time::advance(ttl).await;
        settle().await;
        assert!(loss.is_cancelled());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            4,
            "three interval-bounded reads plus one post-expiry recovery are allowed"
        );
        let _ = ack_tx.send(Some(HeartbeatAck {
            started: Instant::now(),
        }));
        assert!(!lease.is_valid(), "terminal loss cannot be resurrected");

        drop(authority);
        db.close().await.unwrap();
    }

    #[derive(Clone, Copy)]
    enum FinalMarkerFailure {
        Mismatch,
        Error,
    }

    #[tokio::test(start_paused = true)]
    async fn final_marker_mismatch_and_error_are_terminal() {
        for (name, failure) in [
            ("lease-final-mismatch", FinalMarkerFailure::Mismatch),
            ("lease-final-error", FinalMarkerFailure::Error),
        ] {
            let (db, _tx, status) = status_channel(name).await;
            let ttl = Duration::from_millis(300);
            let lease = active(ttl);
            let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let authority = test_authority(lease.clone(), status, ttl, ttl, None, None, {
                let attempts = attempts.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            std::future::pending::<()>().await;
                        }
                        match failure {
                            FinalMarkerFailure::Mismatch => Ok(false),
                            FinalMarkerFailure::Error => {
                                Err(anyhow::anyhow!("injected marker read failure"))
                            }
                        }
                    }
                }
            });
            let loss = authority.loss_token();

            settle().await;
            tokio::time::advance(Duration::from_millis(100)).await;
            settle().await;
            tokio::time::advance(Duration::from_millis(200)).await;
            settle().await;

            assert!(loss.is_cancelled());
            assert!(!lease.is_valid());
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                2,
                "the definitive final result must not trigger another retry"
            );

            drop(authority);
            db.close().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn degraded_validation_resumes_immediately_after_slow_recovery() {
        let (db, _tx, status) = status_channel("lease-post-recovery-validation").await;
        let ttl = Duration::from_millis(300);
        let lease = active(ttl);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recovery_release = Arc::new(tokio::sync::Notify::new());
        let later_release = Arc::new(tokio::sync::Notify::new());
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            ttl,
            None,
            None,
            {
                let attempts = attempts.clone();
                let recovery_release = recovery_release.clone();
                let later_release = later_release.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let recovery_release = recovery_release.clone();
                    let later_release = later_release.clone();
                    async move {
                        match attempt {
                            0..=2 => std::future::pending::<anyhow::Result<bool>>().await,
                            3 => {
                                recovery_release.notified().await;
                                Ok(true)
                            }
                            _ => {
                                later_release.notified().await;
                                Ok(false)
                            }
                        }
                    }
                }
            },
        );
        let loss = authority.loss_token();

        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 4);

        tokio::time::advance(Duration::from_millis(200)).await;
        recovery_release.notify_one();
        settle().await;
        assert!(lease.is_valid());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            5,
            "the next read should start immediately when its request-start schedule is past"
        );

        later_release.notify_one();
        settle().await;
        assert!(loss.is_cancelled());
        assert!(!lease.is_valid());

        drop(authority);
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn suspended_recovery_is_request_start_anchored_and_revoke_wins() {
        let ttl = Duration::from_millis(100);
        let lease = active(ttl);
        tokio::time::advance(ttl).await;
        assert!(!lease.is_valid());
        assert!(lease.is_suspended());
        assert!(
            !lease.renew_from(Instant::now(), ttl),
            "ordinary renewal must not recover a suspended gate"
        );

        let started = Instant::now();
        tokio::time::advance(Duration::from_millis(40)).await;
        assert!(lease.recover_from(started, ttl));
        tokio::time::advance(Duration::from_millis(61)).await;
        assert!(
            !lease.is_valid(),
            "response latency must consume the recovered grant"
        );

        lease.revoke();
        assert!(!lease.recover_from(Instant::now(), ttl));
        assert!(!lease.is_valid());

        let late = active(ttl);
        tokio::time::advance(ttl).await;
        assert!(!late.is_valid());
        let late_started = Instant::now();
        tokio::time::advance(ttl + Duration::from_millis(1)).await;
        assert!(
            !late.recover_from(late_started, ttl),
            "a result received after its own deadline cannot recover authority"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn suspended_internal_work_waits_for_recovery_and_wakes_every_waiter() {
        let ttl = Duration::from_millis(100);
        let lease = active(ttl);
        lease.suspend_for_tests();

        let first = tokio::spawn({
            let lease = lease.clone();
            async move { lease.wait_until_internal_work_is_permitted().await }
        });
        let second = tokio::spawn({
            let lease = lease.clone();
            async move { lease.wait_until_internal_work_is_permitted().await }
        });
        settle().await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());

        assert!(lease.recover_for_tests(ttl));
        assert!(first.await.unwrap());
        assert!(second.await.unwrap());

        lease.suspend_for_tests();
        let terminal = tokio::spawn({
            let lease = lease.clone();
            async move { lease.wait_until_internal_work_is_permitted().await }
        });
        settle().await;
        assert!(!terminal.is_finished());
        lease.revoke();
        assert!(!terminal.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn fenced_db_wins_a_race_with_successful_final_recovery() {
        let (db, status_tx, status) = status_channel("lease-final-fence-race").await;
        let ttl = Duration::from_millis(300);
        let lease = active(ttl);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let final_release = Arc::new(tokio::sync::Notify::new());
        let authority = test_authority(lease.clone(), status, ttl, ttl, None, None, {
            let attempts = attempts.clone();
            let final_release = final_release.clone();
            move || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let final_release = final_release.clone();
                async move {
                    if attempt == 0 {
                        std::future::pending::<()>().await;
                    }
                    final_release.notified().await;
                    Ok(true)
                }
            }
        });
        let loss = authority.loss_token();

        settle().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
        tokio::time::advance(Duration::from_millis(200)).await;
        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(!lease.is_valid());

        status_tx.send_modify(|status| {
            status.close_reason = Some(slatedb::CloseReason::Fenced);
        });
        final_release.notify_one();
        settle().await;

        assert!(loss.is_cancelled(), "the database fence must win the race");
        assert!(
            !lease.is_valid(),
            "a final exact result cannot revive a fenced database"
        );

        drop(authority);
        db.close().await.unwrap();
    }

    async fn status_channel(
        name: &str,
    ) -> (
        slatedb::Db,
        watch::Sender<slatedb::DbStatus>,
        watch::Receiver<slatedb::DbStatus>,
    ) {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::DbBuilder::new(object_store::path::Path::from(name), store)
            .build()
            .await
            .unwrap();
        let initial = db.subscribe().borrow().clone();
        let (tx, rx) = watch::channel(initial);
        (db, tx, rx)
    }

    fn test_authority<F, Fut>(
        lease: Arc<Lease>,
        status: watch::Receiver<slatedb::DbStatus>,
        interval: Duration,
        ttl: Duration,
        replication: Option<Arc<ReplicationControl>>,
        peer_acks: Option<watch::Receiver<Option<HeartbeatAck>>>,
        validate: F,
    ) -> AuthoritySupervisor
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<bool>> + Send + 'static,
    {
        test_authority_with_timeout(
            lease,
            status,
            interval,
            interval,
            ttl,
            replication,
            peer_acks,
            validate,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn test_authority_with_timeout<F, Fut>(
        lease: Arc<Lease>,
        status: watch::Receiver<slatedb::DbStatus>,
        interval: Duration,
        validation_timeout: Duration,
        ttl: Duration,
        replication: Option<Arc<ReplicationControl>>,
        peer_acks: Option<watch::Receiver<Option<HeartbeatAck>>>,
        validate: F,
    ) -> AuthoritySupervisor
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<bool>> + Send + 'static,
    {
        AuthoritySupervisor::start_with_validator(
            lease,
            AuthorityTiming {
                marker_validation_interval: interval,
                marker_validation_timeout: validation_timeout,
                authority_ttl: ttl,
            },
            status,
            replication,
            peer_acks,
            validate,
        )
        .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn ordinary_marker_read_may_outlive_the_poll_cadence() {
        let (db, _tx, status) = status_channel("lease-slow-ordinary-validation").await;
        let ttl = Duration::from_millis(300);
        let lease = active(ttl);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let completions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let authority = test_authority_with_timeout(
            lease.clone(),
            status,
            Duration::from_millis(100),
            ttl,
            ttl,
            None,
            None,
            {
                let attempts = attempts.clone();
                let completions = completions.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let completions = completions.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(120)).await;
                        completions.fetch_add(1, Ordering::SeqCst);
                        Ok(true)
                    }
                }
            },
        );
        let loss = authority.loss_token();

        settle().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        for completed in 1..=5 {
            tokio::time::advance(Duration::from_millis(120)).await;
            settle().await;
            assert_eq!(completions.load(Ordering::SeqCst), completed);
            assert!(
                lease.is_valid(),
                "slow successful reads must renew authority"
            );
            assert!(!loss.is_cancelled());
        }

        drop(authority);
        db.close().await.unwrap();
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn status_churn_and_child_shutdown_preserve_authority_until_clean_close() {
        let (db, status_tx, status) = status_channel("lease-status-churn").await;
        let lease = active(Duration::from_millis(200));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (_replicator, control) = replication();
        let authority = test_authority(
            lease.clone(),
            status,
            Duration::from_millis(100),
            Duration::from_millis(200),
            Some(control.clone()),
            None,
            {
                let attempts = attempts.clone();
                let release = release.clone();
                move || {
                    let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    let release = release.clone();
                    async move {
                        if first {
                            release.notified().await;
                        }
                        Ok(true)
                    }
                }
            },
        );
        let parent = authority.loss_token();
        let child = parent.child_token();

        settle().await;
        tokio::time::advance(Duration::from_millis(99)).await;
        settle().await;
        for _ in 0..5 {
            status_tx.send_modify(|status| status.durable_seq += 1);
            settle().await;
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        child.cancel();
        assert!(lease.is_valid());
        assert!(!parent.is_cancelled());
        release.notify_one();
        settle().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        tokio::time::advance(Duration::from_millis(101)).await;
        settle().await;
        assert!(lease.is_valid(), "renewal must survive the old deadline");

        status_tx.send_modify(|status| status.close_reason = Some(slatedb::CloseReason::Clean));
        authority.finish_after_close().await;
        assert!(lease.was_closed_cleanly());
        assert!(!parent.is_cancelled());
        assert!(control.is_deposed_for_tests());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn revoke_and_nonclean_db_termination_beat_shutdown() {
        for termination in [
            Ok(()),
            Err(Some(slatedb::CloseReason::Fenced)),
            Err(Some(slatedb::CloseReason::Panic)),
            Err(None),
        ] {
            let (db, status_tx, status) = status_channel("lease-terminal").await;
            let lease = test_lease();
            assert!(lease.activate_from(Instant::now(), Duration::from_secs(30)));
            let authority = test_authority(
                lease.clone(),
                status,
                Duration::from_secs(30),
                Duration::from_secs(30),
                None,
                None,
                || async { Ok(true) },
            );
            let loss = authority.loss_token();
            match termination {
                Ok(()) => lease.revoke(),
                Err(Some(reason)) => {
                    status_tx.send_modify(|status| status.close_reason = Some(reason))
                }
                Err(None) => drop(status_tx),
            }
            settle().await;
            assert!(loss.is_cancelled());
            drop(authority);
            db.close().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_reason_is_first_wins_and_expiry_is_not_clean() {
        let lease = test_lease();
        assert!(lease.activate_from(Instant::now(), Duration::from_secs(30)));
        lease.revoke();
        assert!(!lease.close_cleanly());
        assert!(!lease.was_closed_cleanly());
        assert!(!lease.renew_from(Instant::now(), Duration::from_secs(30)));
        assert!(!lease.activate_from(Instant::now(), Duration::from_secs(30)));

        let clean = test_lease();
        assert!(clean.activate_from(Instant::now(), Duration::from_secs(30)));
        assert!(clean.close_cleanly());
        let loss = CancellationToken::new();
        AuthorityGuard::new(clean.clone(), loss.clone(), None).finish(true);
        assert!(clean.was_closed_cleanly());
        assert!(!loss.is_cancelled());

        let expired = test_lease();
        assert!(expired.activate_from(Instant::now(), Duration::from_millis(100)));
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(!expired.close_cleanly());
        assert!(!expired.was_closed_cleanly());

        let suspended = test_lease();
        assert!(suspended.activate_from(Instant::now(), Duration::from_millis(100)));
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(!suspended.is_valid());
        assert!(suspended.is_suspended());
        assert!(!suspended.close_cleanly());
        assert!(!suspended.was_closed_cleanly());
    }

    #[tokio::test]
    async fn preclosed_status_is_rejected_synchronously() {
        let (db, status_tx, status) = status_channel("lease-preclosed").await;
        status_tx.send_modify(|status| status.close_reason = Some(slatedb::CloseReason::Fenced));
        let lease = test_lease();
        assert!(lease.activate_from(Instant::now(), Duration::from_secs(30)));
        assert!(
            AuthoritySupervisor::start_with_validator(
                lease.clone(),
                AuthorityTiming {
                    marker_validation_interval: Duration::from_secs(1),
                    marker_validation_timeout: Duration::from_secs(3),
                    authority_ttl: Duration::from_secs(30),
                },
                status,
                None,
                None,
                || async { Ok(true) },
            )
            .is_err()
        );
        assert!(!lease.is_valid());
        db.close().await.unwrap();
    }

    #[derive(Clone, Copy, PartialEq)]
    enum TaskFailure {
        OwnerDrop,
        Abort,
        Panic,
    }

    #[tokio::test(start_paused = true)]
    async fn owner_drop_abort_and_panic_all_fail_closed() {
        for failure in [
            TaskFailure::OwnerDrop,
            TaskFailure::Abort,
            TaskFailure::Panic,
        ] {
            let (db, _status_tx, status) = status_channel("lease-task-failure").await;
            let lease = active(Duration::from_secs(30));
            let (_replicator, control) = replication();
            let mut authority = Some(test_authority(
                lease.clone(),
                status,
                Duration::from_millis(1),
                Duration::from_secs(30),
                Some(control.clone()),
                None,
                move || async move {
                    assert!(failure != TaskFailure::Panic, "injected supervisor panic");
                    Ok(true)
                },
            ));
            let loss = authority.as_ref().unwrap().loss_token();
            match failure {
                TaskFailure::OwnerDrop => drop(authority.take()),
                TaskFailure::Abort => authority.as_ref().unwrap().task.as_ref().unwrap().abort(),
                TaskFailure::Panic => {
                    settle().await;
                    tokio::time::advance(Duration::from_millis(1)).await;
                }
            }
            settle().await;
            assert!(!lease.is_valid());
            assert!(loss.is_cancelled());
            assert!(control.is_deposed_for_tests());
            drop(authority);
            db.close().await.unwrap();
        }
    }
}
