//! Idempotency ledger for non-idempotent mutations and HA replay.
//!
//! Completed results outlive the client retry horizon. An unseen retry after
//! expiry is stale; only an initial frame may allocate a new operation ID.

use crate::fs::inode::InodeId;
use crate::fs::types::FileAttributes;
use bincode::Options;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Notify, mpsc};
use tokio_util::time::DelayQueue;

/// Retention period for completed operation IDs.
pub const OP_ID_RETENTION: Duration = ninep_proto::retry::MUTATION_RESULT_RETENTION;

/// Client-generated op id. All-zero means "no id supplied": no deduplication.
pub type OpId = [u8; 16];

const DEDUP_RESULT_WIRE_VERSION: u8 = 1;

/// Result retained for mutation replay.
///
/// Namespace results retain their operation type. Create-like results retain
/// the returned object identity and attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DedupResult {
    Applied,
    Create {
        inode_id: InodeId,
        attrs: FileAttributes,
    },
    Mkdir {
        inode_id: InodeId,
        attrs: FileAttributes,
    },
    Mknod {
        inode_id: InodeId,
        attrs: FileAttributes,
    },
    Symlink {
        inode_id: InodeId,
        attrs: FileAttributes,
    },
    Link {
        inode_id: InodeId,
        attrs: FileAttributes,
    },
    Setattr {
        attrs: FileAttributes,
    },
    Write {
        attrs: FileAttributes,
    },
    Fallocate {
        attrs: FileAttributes,
    },
    /// Terminal protocol error with no mutation effect.
    Error {
        errno: u32,
    },
    /// A completed unlink or rmdir operation.
    Remove,
    /// A completed rename or renameat operation.
    Rename,
}

/// One completed mutation and the result associated with its client op id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub op_id: OpId,
    pub result: DedupResult,
}

#[derive(Debug, thiserror::Error)]
pub enum DedupWireError {
    #[error("dedup result payload is empty")]
    EmptyResult,
    #[error("unsupported dedup result wire version {0}")]
    UnsupportedVersion(u8),
    #[error("dedup op id must be exactly 16 bytes, got {0}")]
    InvalidOpIdLength(usize),
    #[error("dedup op id must not be all zero")]
    MissingOpId,
    #[error("invalid dedup result payload: {0}")]
    InvalidResult(#[source] bincode::Error),
}

fn wire_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

impl DedupResult {
    pub(crate) fn into_create(self) -> Option<(InodeId, FileAttributes)> {
        match self {
            Self::Create { inode_id, attrs } => Some((inode_id, attrs)),
            _ => None,
        }
    }

    pub(crate) fn into_mkdir(self) -> Option<(InodeId, FileAttributes)> {
        match self {
            Self::Mkdir { inode_id, attrs } => Some((inode_id, attrs)),
            _ => None,
        }
    }

    pub(crate) fn into_mknod(self) -> Option<(InodeId, FileAttributes)> {
        match self {
            Self::Mknod { inode_id, attrs } => Some((inode_id, attrs)),
            _ => None,
        }
    }

    pub(crate) fn into_symlink(self) -> Option<(InodeId, FileAttributes)> {
        match self {
            Self::Symlink { inode_id, attrs } => Some((inode_id, attrs)),
            _ => None,
        }
    }

    pub(crate) fn into_link(self) -> Option<(InodeId, FileAttributes)> {
        match self {
            Self::Link { inode_id, attrs } => Some((inode_id, attrs)),
            _ => None,
        }
    }

    pub(crate) fn into_setattr(self) -> Option<FileAttributes> {
        match self {
            Self::Setattr { attrs } => Some(attrs),
            _ => None,
        }
    }

    pub(crate) fn into_write(self) -> Option<FileAttributes> {
        match self {
            Self::Write { attrs } => Some(attrs),
            _ => None,
        }
    }

    pub(crate) fn into_fallocate(self) -> Option<FileAttributes> {
        match self {
            Self::Fallocate { attrs } => Some(attrs),
            _ => None,
        }
    }

    pub(crate) fn into_remove(self) -> Option<()> {
        matches!(self, Self::Remove).then_some(())
    }

    pub(crate) fn into_rename(self) -> Option<()> {
        matches!(self, Self::Rename).then_some(())
    }

    /// Inode pinned while this result can be replayed.
    fn replay_inode(&self) -> Option<InodeId> {
        match self {
            Self::Create { inode_id, .. } => Some(*inode_id),
            _ => None,
        }
    }

    /// Encode a versioned replication result.
    pub fn encode_wire(&self) -> Result<Vec<u8>, DedupWireError> {
        let encoded = wire_options()
            .serialize(self)
            .map_err(DedupWireError::InvalidResult)?;
        let mut out = Vec::with_capacity(1 + encoded.len());
        out.push(DEDUP_RESULT_WIRE_VERSION);
        out.extend_from_slice(&encoded);
        Ok(out)
    }

    /// Decode a versioned replication result.
    pub fn decode_wire(raw: &[u8]) -> Result<Self, DedupWireError> {
        let (&version, encoded) = raw.split_first().ok_or(DedupWireError::EmptyResult)?;
        if version != DEDUP_RESULT_WIRE_VERSION {
            return Err(DedupWireError::UnsupportedVersion(version));
        }
        wire_options()
            .deserialize(encoded)
            .map_err(DedupWireError::InvalidResult)
    }
}

impl DedupEntry {
    pub fn from_wire(op_id: &[u8], result: &[u8]) -> Result<Self, DedupWireError> {
        let op_id: OpId = op_id
            .try_into()
            .map_err(|_| DedupWireError::InvalidOpIdLength(op_id.len()))?;
        if !has_op_id(&op_id) {
            return Err(DedupWireError::MissingOpId);
        }
        Ok(Self {
            op_id,
            result: DedupResult::decode_wire(result)?,
        })
    }

    pub fn to_wire_parts(&self) -> Result<(Vec<u8>, Vec<u8>), DedupWireError> {
        if !has_op_id(&self.op_id) {
            return Err(DedupWireError::MissingOpId);
        }
        Ok((self.op_id.to_vec(), self.result.encode_wire()?))
    }
}

/// Whether an op id was actually supplied (non-zero).
pub fn has_op_id(op_id: &OpId) -> bool {
    op_id.iter().any(|&b| b != 0)
}

/// Time-bounded map from op id to the completed mutation's original result.
pub struct DedupCache {
    inner: Mutex<Inner>,
    retention: Duration,
    now: Arc<dyn Fn() -> Instant + Send + Sync>,
    /// Publication channel for the single-owner expiration scheduler.
    expiry_tx: mpsc::UnboundedSender<ScheduledExpiry>,
    expiry_rx: Mutex<Option<mpsc::UnboundedReceiver<ScheduledExpiry>>>,
    /// Weak sender to the filesystem orphan-reclaim drainer.
    reclaim_tx: Mutex<Option<mpsc::WeakUnboundedSender<InodeId>>>,
}

/// Expiry event keyed by both operation ID and generation deadline.
#[derive(Debug, Clone, Copy)]
struct ScheduledExpiry {
    op_id: OpId,
    expires_at: Instant,
}

/// Maximum expiry events processed per ledger lock acquisition.
const EXPIRY_REAP_BATCH: usize = 256;

/// Point-in-time occupancy of the retry ledger.
#[derive(Debug, Clone, Copy)]
pub struct DedupStats {
    /// Completed outcomes retained for exact replay.
    pub retained_results: usize,
    /// Distinct operation IDs currently being applied.
    pub inflight_ids: usize,
    /// Retained outcomes currently pinned while a reply is reconstructed.
    pub replay_pinned_results: usize,
}

struct Inner {
    /// One lifecycle state per operation ID.
    entries: HashMap<OpId, OpState>,
    /// Counters updated under the `entries` mutex.
    inflight_ids: usize,
    replay_pinned_results: usize,
    /// Reference counts for inodes pinned by replayable create results.
    protected_inodes: HashMap<InodeId, InodeProtection>,
    /// Orphans scheduled when their final replay protection expires.
    reclaim_on_unprotect: HashSet<InodeId>,
    /// Temporary unseen-retry admission after a coverage-proven promotion.
    promotion_retry_grace: Option<PromotionRetryGrace>,
}

struct TimedResult {
    value: DedupResult,
    expires_at: Instant,
    /// Active replay guards that extend this result past expiry.
    replay_pins: usize,
}

struct InodeProtection {
    /// Maximum retained deadline among references to this inode.
    until: Instant,
    refs: usize,
}

enum OpState {
    Applying(Arc<Notify>),
    Complete(TimedResult),
}

#[derive(Clone, Copy)]
struct PromotionRetryGrace {
    allowed_origin_epoch: u64,
    deadline: Instant,
}

/// Mutation-envelope admission failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DedupAdmissionError {
    #[error("retry op id is no longer present in the dedup ledger")]
    UnseenRetry,
}

enum Admission {
    Admitted(DedupGuard),
    InFlight(std::pin::Pin<Box<OwnedNotified>>),
    UnseenRetry,
}

impl Inner {
    fn protect_result_inode(&mut self, result: &DedupResult, expires_at: Instant) {
        if let Some(inode_id) = result.replay_inode() {
            let protection = self
                .protected_inodes
                .entry(inode_id)
                .or_insert(InodeProtection {
                    until: expires_at,
                    refs: 0,
                });
            protection.until = protection.until.max(expires_at);
            protection.refs += 1;
        }
    }

    fn remove_result(&mut self, op_id: &OpId) -> Option<InodeId> {
        let Some(OpState::Complete(entry)) = self.entries.remove(op_id) else {
            unreachable!("remove_result is only called for a completed entry")
        };
        let inode_id = entry.value.replay_inode()?;
        let protection = self.protected_inodes.get_mut(&inode_id).unwrap();
        protection.refs -= 1;
        if protection.refs == 0 {
            self.protected_inodes.remove(&inode_id);
            return self
                .reclaim_on_unprotect
                .remove(&inode_id)
                .then_some(inode_id);
        }
        None
    }

    /// Enforce logical expiry independently of reaper scheduling.
    fn remove_target_if_expired_and_unpinned(
        &mut self,
        op_id: &OpId,
        now: Instant,
    ) -> Option<InodeId> {
        let removable = matches!(
            self.entries.get(op_id),
            Some(OpState::Complete(entry))
                if entry.expires_at <= now && entry.replay_pins == 0
        );
        if removable {
            self.remove_result(op_id)
        } else {
            None
        }
    }
}

async fn run_expiry_reaper(
    cache: Weak<DedupCache>,
    mut scheduled: mpsc::UnboundedReceiver<ScheduledExpiry>,
) {
    let mut expirations = DelayQueue::new();
    loop {
        if expirations.is_empty() {
            let Some(expiry) = scheduled.recv().await else {
                return;
            };
            expirations.insert_at(expiry, tokio::time::Instant::from_std(expiry.expires_at));
            continue;
        }

        tokio::select! {
            expiry = scheduled.recv() => {
                let Some(expiry) = expiry else {
                    return;
                };
                expirations.insert_at(
                    expiry,
                    tokio::time::Instant::from_std(expiry.expires_at),
                );
            }
            expired = expirations.next() => {
                let Some(expired) = expired else {
                    continue;
                };
                let mut batch = Vec::with_capacity(EXPIRY_REAP_BATCH);
                batch.push(expired.into_inner());
                while batch.len() < EXPIRY_REAP_BATCH {
                    let Some(expired) = expirations.next().now_or_never().flatten() else {
                        break;
                    };
                    batch.push(expired.into_inner());
                }

                let Some(cache) = cache.upgrade() else {
                    return;
                };
                cache.reap_expired_batch(&batch);
                drop(cache);
                tokio::task::yield_now().await;
            }
        }
    }
}

impl DedupCache {
    /// Create an empty ledger with the protocol retention period.
    pub fn new() -> Self {
        Self::new_with_clock(OP_ID_RETENTION, Arc::new(Instant::now))
    }

    fn new_with_clock(retention: Duration, now: Arc<dyn Fn() -> Instant + Send + Sync>) -> Self {
        let (expiry_tx, expiry_rx) = mpsc::unbounded_channel();
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                inflight_ids: 0,
                replay_pinned_results: 0,
                protected_inodes: HashMap::new(),
                reclaim_on_unprotect: HashSet::new(),
                promotion_retry_grace: None,
            }),
            retention,
            now,
            expiry_tx,
            expiry_rx: Mutex::new(Some(expiry_rx)),
            reclaim_tx: Mutex::new(None),
        }
    }

    /// Register the filesystem orphan-reclaim drainer.
    pub(crate) fn set_reclaim_sender(&self, sender: &mpsc::UnboundedSender<InodeId>) {
        *self.reclaim_tx.lock().unwrap() = Some(sender.downgrade());
    }

    fn notify_reclaim(&self, inode_id: Option<InodeId>) {
        let Some(inode_id) = inode_id else {
            return;
        };
        let sender = self
            .reclaim_tx
            .lock()
            .unwrap()
            .as_ref()
            .and_then(mpsc::WeakUnboundedSender::upgrade);
        if let Some(sender) = sender {
            let _ = sender.send(inode_id);
        }
    }

    /// Starts the expiry reaper once.
    pub fn start_expiry_reaper(self: &Arc<Self>) {
        let Some(receiver) = self.expiry_rx.lock().unwrap().take() else {
            return;
        };
        crate::task::spawn_named(
            "dedup-expiry-reaper",
            run_expiry_reaper(Arc::downgrade(self), receiver),
        );
    }

    fn reap_expired_batch(&self, batch: &[ScheduledExpiry]) {
        let mut inner = self.inner.lock().unwrap();
        let mut reclaim = Vec::new();
        for expiry in batch {
            let removable = matches!(
                inner.entries.get(&expiry.op_id),
                Some(OpState::Complete(entry))
                    if entry.expires_at == expiry.expires_at && entry.replay_pins == 0
            );
            if removable {
                reclaim.extend(inner.remove_result(&expiry.op_id));
            }
        }
        drop(inner);
        for inode_id in reclaim {
            self.notify_reclaim(Some(inode_id));
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        retention: Duration,
        now: impl Fn() -> Instant + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_clock(retention, Arc::new(now))
    }

    /// Admit a test mutation as an initial frame or retry.
    #[cfg(test)]
    async fn begin(
        self: &Arc<Self>,
        op_id: OpId,
        is_retry: bool,
    ) -> Result<Option<DedupGuard>, DedupAdmissionError> {
        self.begin_attempt_from_origin(op_id, is_retry, 0).await
    }

    /// Apply the locked admission transition for a non-zero operation ID.
    fn admit_nonzero(self: &Arc<Self>, op_id: OpId, retry_origin: Option<u64>) -> Admission {
        debug_assert!(has_op_id(&op_id));

        let mut inner = self.inner.lock().unwrap();
        let now = (self.now)();
        let reclaim = inner.remove_target_if_expired_and_unpinned(&op_id, now);
        self.notify_reclaim(reclaim);

        match inner.entries.get_mut(&op_id) {
            Some(OpState::Complete(result)) => {
                if result.expires_at <= now {
                    return Admission::UnseenRetry;
                }
                let first_pin = result.replay_pins == 0;
                result.replay_pins += 1;
                if first_pin {
                    inner.replay_pinned_results += 1;
                }
                return Admission::Admitted(DedupGuard {
                    op_id,
                    claim: None,
                    cache: Arc::clone(self),
                });
            }
            Some(OpState::Applying(notify)) => {
                // Register under the state lock to avoid a lost notification.
                let mut notified = Box::pin(Arc::clone(notify).notified_owned());
                notified.as_mut().enable();
                return Admission::InFlight(notified);
            }
            None => {}
        }

        if let Some(origin_epoch) = retry_origin {
            let promoted_retry_is_fresh = inner.promotion_retry_grace.is_some_and(|grace| {
                origin_epoch != 0
                    && origin_epoch == grace.allowed_origin_epoch
                    && now < grace.deadline
            });
            if !promoted_retry_is_fresh {
                return Admission::UnseenRetry;
            }
        }

        let claim = Arc::new(Notify::new());
        inner
            .entries
            .insert(op_id, OpState::Applying(Arc::clone(&claim)));
        inner.inflight_ids += 1;
        Admission::Admitted(DedupGuard {
            op_id,
            claim: Some(claim),
            cache: Arc::clone(self),
        })
    }

    /// Admit a mutation using its initial HA writer epoch.
    ///
    /// An unexpired result is pinned for replay. A new initial frame reserves an
    /// apply slot. Unseen retries require promotion grace for their origin epoch.
    pub async fn begin_attempt_from_origin(
        self: &Arc<Self>,
        op_id: OpId,
        is_retry: bool,
        op_origin_epoch: u64,
    ) -> Result<Option<DedupGuard>, DedupAdmissionError> {
        if !has_op_id(&op_id) {
            return if is_retry {
                Err(DedupAdmissionError::UnseenRetry)
            } else {
                Ok(None)
            };
        }
        let retry_origin = is_retry.then_some(op_origin_epoch);
        loop {
            match self.admit_nonzero(op_id, retry_origin) {
                Admission::Admitted(guard) => return Ok(Some(guard)),
                Admission::InFlight(notified) => notified.await,
                Admission::UnseenRetry => return Err(DedupAdmissionError::UnseenRetry),
            }
        }
    }

    /// Reserves an initial attempt before asynchronous dispatch.
    // The library target does not compile the 9P receive path that calls this.
    #[allow(dead_code)]
    pub(crate) fn reserve_initial(self: &Arc<Self>, op_id: OpId) -> Option<DedupGuard> {
        if !has_op_id(&op_id) {
            return None;
        }
        match self.admit_nonzero(op_id, None) {
            Admission::Admitted(guard) => Some(guard),
            Admission::InFlight(_) | Admission::UnseenRetry => None,
        }
    }

    /// Return a live or replay-pinned result.
    pub fn get(&self, op_id: &OpId) -> Option<DedupResult> {
        if !has_op_id(op_id) {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        let reclaim = inner.remove_target_if_expired_and_unpinned(op_id, (self.now)());
        self.notify_reclaim(reclaim);
        match inner.entries.get(op_id) {
            Some(OpState::Complete(entry)) => Some(entry.value.clone()),
            _ => None,
        }
    }

    /// Return physical ledger occupancy.
    pub fn stats(&self) -> DedupStats {
        let inner = self.inner.lock().unwrap();
        DedupStats {
            retained_results: inner.entries.len() - inner.inflight_ids,
            inflight_ids: inner.inflight_ids,
            replay_pinned_results: inner.replay_pinned_results,
        }
    }

    /// Publish a committed result for tests. The first result wins.
    #[cfg(test)]
    fn record_result(&self, op_id: OpId, result: DedupResult) {
        let _ = self.record_entry_with_expiry(DedupEntry { op_id, result });
    }

    /// Publish a committed entry and return its retention deadline.
    /// Duplicate publication does not extend the original deadline.
    pub(crate) fn record_entry_with_expiry(&self, entry: DedupEntry) -> Option<Instant> {
        let DedupEntry { op_id, result } = entry;
        if !has_op_id(&op_id) {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        let now = (self.now)();
        let reclaim = inner.remove_target_if_expired_and_unpinned(&op_id, now);
        self.notify_reclaim(reclaim);
        if let Some(OpState::Complete(existing)) = inner.entries.get(&op_id) {
            return Some(existing.expires_at);
        }

        let expires_at = now + self.retention;
        inner.protect_result_inode(&result, expires_at);
        let previous = inner.entries.insert(
            op_id,
            OpState::Complete(TimedResult {
                value: result,
                expires_at,
                replay_pins: 0,
            }),
        );
        self.expiry_tx
            .send(ScheduledExpiry { op_id, expires_at })
            .expect("dedup expiry receiver must live as long as its cache");
        if let Some(OpState::Applying(notify)) = previous {
            inner.inflight_ids -= 1;
            notify.notify_waiters();
        }
        Some(expires_at)
    }

    pub fn record_entry(&self, entry: DedupEntry) {
        let _ = self.record_entry_with_expiry(entry);
    }

    /// Clear promotion retry grace.
    pub(crate) fn clear_promotion_retry_grace(&self) {
        self.inner.lock().unwrap().promotion_retry_grace = None;
    }

    /// Maximum deadline for new promotion retry grace.
    pub(crate) fn promotion_retry_deadline(&self) -> Instant {
        (self.now)() + self.retention
    }

    /// Whether a proof deadline is still live.
    pub(crate) fn promotion_retry_deadline_is_live(&self, deadline: Instant) -> bool {
        (self.now)() < deadline
    }

    /// Arm unseen-retry admission for one origin epoch until `deadline`.
    /// The caller must prove complete publication of surviving results.
    pub(crate) fn arm_promotion_retry_grace_until(
        &self,
        allowed_origin_epoch: u64,
        deadline: Instant,
    ) -> bool {
        debug_assert_ne!(allowed_origin_epoch, 0);
        let now = (self.now)();
        let mut inner = self.inner.lock().unwrap();
        inner.promotion_retry_grace = None;
        if allowed_origin_epoch == 0 || now >= deadline {
            return false;
        }
        // Grace cannot exceed one local retention period.
        let deadline = deadline.min(now + self.retention);
        inner.promotion_retry_grace = Some(PromotionRetryGrace {
            allowed_origin_epoch,
            deadline,
        });
        true
    }

    #[cfg(test)]
    pub(crate) fn arm_promotion_retry_grace(&self, allowed_origin_epoch: u64) {
        let deadline = self.promotion_retry_deadline();
        assert!(self.arm_promotion_retry_grace_until(allowed_origin_epoch, deadline));
    }

    /// Maximum replay-protection deadline for an inode.
    pub(crate) fn replay_inode_protected_until(&self, inode_id: InodeId) -> Option<Instant> {
        let inner = self.inner.lock().unwrap();
        inner
            .protected_inodes
            .get(&inode_id)
            .map(|protection| protection.until)
    }

    pub(crate) fn protects_replay_inode(&self, inode_id: InodeId) -> bool {
        self.replay_inode_protected_until(inode_id).is_some()
    }

    /// Schedule reclaim when the inode's final replay protection expires.
    /// Returns whether protection currently exists.
    pub(crate) fn reclaim_when_unprotected(&self, inode_id: InodeId) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.protected_inodes.contains_key(&inode_id) {
            return false;
        }
        inner.reclaim_on_unprotect.insert(inode_id);
        true
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.stats().retained_results == 0
    }
}

impl Default for DedupCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Holds either an apply reservation or a completed-result replay pin for the
/// duration of one handler call.
pub struct DedupGuard {
    op_id: OpId,
    claim: Option<Arc<Notify>>,
    cache: Arc<DedupCache>,
}

impl Drop for DedupGuard {
    fn drop(&mut self) {
        let now = (self.cache.now)();
        let mut inner = self.cache.inner.lock().unwrap();
        let mut reclaim = None;
        if let Some(claim) = &self.claim {
            let owns_claim = inner.entries.get(&self.op_id).is_some_and(
                |state| matches!(state, OpState::Applying(current) if Arc::ptr_eq(current, claim)),
            );
            if owns_claim && let Some(OpState::Applying(notify)) = inner.entries.remove(&self.op_id)
            {
                inner.inflight_ids -= 1;
                notify.notify_waiters();
            }
        } else {
            let (last_pin, remove_expired) = match inner.entries.get_mut(&self.op_id) {
                Some(OpState::Complete(result)) if result.replay_pins != 0 => {
                    result.replay_pins -= 1;
                    let last_pin = result.replay_pins == 0;
                    (last_pin, last_pin && result.expires_at <= now)
                }
                _ => (false, false),
            };
            if last_pin {
                inner.replay_pinned_results -= 1;
            }
            if remove_expired {
                reclaim = inner.remove_result(&self.op_id);
            }
        }
        drop(inner);
        self.cache.notify_reclaim(reclaim);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn id(n: u8) -> OpId {
        let mut x = [0u8; 16];
        x[0] = n;
        x
    }

    fn numbered_id(n: usize) -> OpId {
        let mut x = [0u8; 16];
        x[..8].copy_from_slice(&(n as u64).to_le_bytes());
        x
    }

    fn manual_cache() -> (Arc<DedupCache>, Arc<AtomicU64>) {
        let start = Instant::now();
        let elapsed_secs = Arc::new(AtomicU64::new(0));
        let clock = Arc::clone(&elapsed_secs);
        let cache = DedupCache::new_for_test(Duration::from_secs(10), move || {
            start + Duration::from_secs(clock.load(Ordering::SeqCst))
        });
        (Arc::new(cache), elapsed_secs)
    }

    fn delay_queue_cache(retention: Duration, start: bool) -> Arc<DedupCache> {
        let cache = Arc::new(DedupCache::new_for_test(retention, || {
            tokio::time::Instant::now().into_std()
        }));
        if start {
            cache.start_expiry_reaper();
        }
        cache
    }

    async fn settle_reaper() {
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_retained_results(cache: &DedupCache, expected: usize) {
        for _ in 0..10_000 {
            if cache.stats().retained_results == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(cache.stats().retained_results, expected);
    }

    async fn assert_unseen(cache: &Arc<DedupCache>, origin_epoch: u64) {
        assert!(matches!(
            cache
                .begin_attempt_from_origin(id(1), true, origin_epoch)
                .await,
            Err(DedupAdmissionError::UnseenRetry)
        ));
    }

    fn assert_stats(cache: &DedupCache, expected: (usize, usize, usize)) {
        let stats = cache.stats();
        assert_eq!(
            (
                stats.retained_results,
                stats.inflight_ids,
                stats.replay_pinned_results
            ),
            expected
        );
    }

    async fn pin_applied(cache: &Arc<DedupCache>, clock: &AtomicU64) -> DedupGuard {
        cache.record_result(id(1), DedupResult::Applied);
        clock.store(9, Ordering::SeqCst);
        let replay = cache.begin(id(1), true).await.unwrap().unwrap();
        clock.store(10, Ordering::SeqCst);
        assert_stats(cache, (1, 0, 1));
        replay
    }

    #[test]
    fn records_and_returns_the_cached_reply() {
        let cache = DedupCache::new();
        assert!(cache.get(&id(1)).is_none(), "unseen op is not deduplicated");
        cache.record_result(id(1), DedupResult::Applied);
        assert!(
            matches!(cache.get(&id(1)), Some(DedupResult::Applied)),
            "a seen op returns its cached result"
        );
        assert!(cache.get(&id(2)).is_none());
    }

    #[test]
    fn duplicate_publication_keeps_first_result_and_expiry() {
        let (cache, clock) = manual_cache();
        let first_deadline = cache
            .record_entry_with_expiry(DedupEntry {
                op_id: id(1),
                result: DedupResult::Applied,
            })
            .unwrap();

        clock.store(9, Ordering::SeqCst);
        let duplicate_deadline = cache
            .record_entry_with_expiry(DedupEntry {
                op_id: id(1),
                result: DedupResult::Error { errno: 5 },
            })
            .unwrap();
        assert_eq!(duplicate_deadline, first_deadline);
        assert!(matches!(cache.get(&id(1)), Some(DedupResult::Applied)));

        clock.store(10, Ordering::SeqCst);
        assert!(
            cache.get(&id(1)).is_none(),
            "a duplicate publication must not refresh retention"
        );
    }

    #[test]
    fn absent_op_id_is_never_deduplicated() {
        let cache = DedupCache::new();
        let absent = [0u8; 16];
        cache.record_result(absent, DedupResult::Applied);
        assert!(
            cache.get(&absent).is_none(),
            "the all-zero id is excluded from deduplication"
        );
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn unexpired_results_do_not_block_new_admission() {
        let (cache, _clock) = manual_cache();
        for n in 1..=3 {
            cache.record_result(id(n), DedupResult::Applied);
        }
        assert!((1..=3).all(|n| cache.get(&id(n)).is_some()));
        assert_eq!(cache.stats().retained_results, 3);
        let guard = cache
            .begin(id(4), false)
            .await
            .unwrap()
            .expect("retained results must not cap new mutation throughput");
        drop(guard);
    }

    #[tokio::test]
    async fn expired_results_are_reclaimed_on_target_access() {
        let (cache, clock) = manual_cache();
        cache.record_result(id(1), DedupResult::Applied);
        cache.record_result(id(2), DedupResult::Applied);
        clock.store(10, Ordering::SeqCst);
        let guard = cache
            .begin(id(3), false)
            .await
            .unwrap()
            .expect("the new id gets a reservation");
        assert!(cache.get(&id(1)).is_none());
        assert!(cache.get(&id(2)).is_none());
        assert!(cache.is_empty());
        drop(guard);
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_drains_large_expiry_cohorts_in_batches() {
        let retention = Duration::from_secs(10);
        let cache = delay_queue_cache(retention, true);
        cache.start_expiry_reaper();
        cache.record_result(
            numbered_id(1),
            DedupResult::Create {
                inode_id: 44,
                attrs: FileAttributes::default(),
            },
        );
        for n in 2..=(EXPIRY_REAP_BATCH * 2 + 17) {
            cache.record_result(numbered_id(n), DedupResult::Applied);
        }
        assert_eq!(cache.stats().retained_results, EXPIRY_REAP_BATCH * 2 + 17);
        assert!(cache.protects_replay_inode(44));

        tokio::time::advance(retention).await;
        wait_for_retained_results(&cache, 0).await;
        assert!(!cache.protects_replay_inode(44));
    }

    #[tokio::test(start_paused = true)]
    async fn stale_delay_queue_event_cannot_remove_a_reused_op_id() {
        let retention = Duration::from_secs(10);
        let cache = delay_queue_cache(retention, false);
        cache.record_result(id(1), DedupResult::Applied);

        tokio::time::advance(retention).await;
        assert!(cache.get(&id(1)).is_none());
        cache.record_result(id(1), DedupResult::Error { errno: 5 });

        // Expiry events match both operation ID and generation deadline.
        cache.start_expiry_reaper();
        settle_reaper().await;
        assert!(matches!(
            cache.get(&id(1)),
            Some(DedupResult::Error { errno: 5 })
        ));

        tokio::time::advance(retention).await;
        wait_for_retained_results(&cache, 0).await;
    }

    #[tokio::test(start_paused = true)]
    async fn delay_queue_expiry_defers_to_an_admitted_replay_pin() {
        let retention = Duration::from_secs(10);
        let cache = delay_queue_cache(retention, true);
        cache.record_result(id(1), DedupResult::Applied);
        tokio::time::advance(retention - Duration::from_secs(1)).await;
        let replay = cache.begin(id(1), true).await.unwrap().unwrap();

        tokio::time::advance(Duration::from_secs(1)).await;
        settle_reaper().await;
        assert!(matches!(cache.get(&id(1)), Some(DedupResult::Applied)));

        drop(replay);
        assert!(cache.get(&id(1)).is_none());
        assert_stats(&cache, (0, 0, 0));
    }

    #[tokio::test]
    async fn promotion_grace_can_reuse_an_expired_unpinned_id() {
        let (cache, clock) = manual_cache();
        cache.record_result(id(1), DedupResult::Applied);
        clock.store(5, Ordering::SeqCst);
        cache.arm_promotion_retry_grace(7);

        clock.store(10, Ordering::SeqCst);
        let fresh = cache
            .begin_attempt_from_origin(id(1), true, 7)
            .await
            .expect("live promotion grace admits the logically expired id")
            .expect("the expired result becomes a fresh apply reservation");
        assert!(cache.get(&id(1)).is_none());
        drop(fresh);
    }

    #[tokio::test]
    async fn retry_after_result_expiry_fails_closed() {
        let (cache, clock) = manual_cache();
        cache.record_result(id(1), DedupResult::Applied);

        clock.store(10, Ordering::SeqCst);
        assert!(matches!(
            cache.begin(id(1), true).await,
            Err(DedupAdmissionError::UnseenRetry)
        ));
        assert!(cache.get(&id(1)).is_none());

        assert!(cache.begin(id(2), false).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn ordinary_cache_rejects_an_unseen_retry() {
        let (cache, _clock) = manual_cache();
        assert_unseen(&cache, 0).await;
    }

    #[tokio::test]
    async fn promotion_grace_admits_an_unseen_retry_as_fresh() {
        let (cache, clock) = manual_cache();
        cache.arm_promotion_retry_grace(7);
        clock.store(9, Ordering::SeqCst);

        let fresh = cache
            .begin_attempt_from_origin(id(1), true, 7)
            .await
            .expect("coverage-proven promotion admits the resend")
            .expect("unseen resend receives an apply reservation");
        assert!(
            cache.get(&id(1)).is_none(),
            "the resend is fresh, not replayed"
        );
        cache.record_result(id(1), DedupResult::Applied);
        drop(fresh);
        assert!(matches!(cache.get(&id(1)), Some(DedupResult::Applied)));
    }

    #[tokio::test]
    async fn promotion_grace_expires_at_deadline() {
        let (cache, clock) = manual_cache();
        cache.arm_promotion_retry_grace(7);
        clock.store(10, Ordering::SeqCst);

        assert_unseen(&cache, 7).await;
    }

    #[tokio::test]
    async fn absolute_promotion_deadline_is_not_reset_when_armed() {
        let (cache, clock) = manual_cache();
        let deadline = cache.promotion_retry_deadline();

        clock.store(9, Ordering::SeqCst);
        assert!(cache.arm_promotion_retry_grace_until(7, deadline));
        clock.store(10, Ordering::SeqCst);
        assert_unseen(&cache, 7).await;
    }

    #[tokio::test]
    async fn clearing_promotion_grace_rejects_an_unseen_retry() {
        let (cache, _clock) = manual_cache();
        cache.arm_promotion_retry_grace(7);
        cache.clear_promotion_retry_grace();
        assert_unseen(&cache, 7).await;
    }

    #[tokio::test]
    async fn promotion_grace_rejects_invalid_origins() {
        let (cache, _clock) = manual_cache();
        cache.arm_promotion_retry_grace(7);

        for origin in [0, 6, 8] {
            assert_unseen(&cache, origin).await;
        }
    }

    #[tokio::test]
    async fn admitted_replay_pins_result_across_expiry() {
        let (cache, clock) = manual_cache();
        let replay = pin_applied(&cache, &clock).await;
        clock.store(20, Ordering::SeqCst);
        assert!(
            matches!(cache.get(&id(1)), Some(DedupResult::Applied)),
            "the handler's second lookup must see the admitted exact result"
        );

        drop(replay);
        assert!(
            cache.get(&id(1)).is_none(),
            "the expired result becomes reclaimable after reply reconstruction"
        );
        assert_stats(&cache, (0, 0, 0));
    }

    #[tokio::test]
    async fn expired_pinned_result_cannot_issue_another_replay_pin() {
        let (cache, clock) = manual_cache();
        let replay = pin_applied(&cache, &clock).await;

        for is_retry in [true, false] {
            assert!(matches!(
                cache.begin(id(1), is_retry).await,
                Err(DedupAdmissionError::UnseenRetry)
            ));
        }
        assert!(cache.reserve_initial(id(1)).is_none());
        assert!(
            cache.get(&id(1)).is_some(),
            "the original replay stays pinned"
        );

        drop(replay);
        assert!(cache.is_empty());
    }

    #[test]
    fn receive_time_first_pins_a_cached_result_across_expiry() {
        let (cache, clock) = manual_cache();
        cache.record_result(id(1), DedupResult::Applied);

        clock.store(9, Ordering::SeqCst);
        let received = cache
            .reserve_initial(id(1))
            .expect("a cached result receives a replay pin");
        clock.store(20, Ordering::SeqCst);
        assert!(
            matches!(cache.get(&id(1)), Some(DedupResult::Applied)),
            "receive-time admission pins the result until dispatch"
        );

        drop(received);
        assert!(
            cache.get(&id(1)).is_none(),
            "the expired result becomes reclaimable after the received frame finishes"
        );
    }

    #[tokio::test]
    async fn pinned_oldest_result_does_not_block_later_expiry() {
        let (cache, clock) = manual_cache();
        cache.record_result(id(1), DedupResult::Applied);
        cache.record_result(id(2), DedupResult::Applied);

        clock.store(9, Ordering::SeqCst);
        let oldest_replay = cache.begin(id(1), true).await.unwrap().unwrap();
        clock.store(10, Ordering::SeqCst);

        assert!(
            cache.get(&id(2)).is_none(),
            "later expired result is pruned"
        );
        assert!(cache.get(&id(1)).is_some(), "admitted replay stays pinned");
        assert_eq!(cache.stats().retained_results, 1);

        drop(oldest_replay);
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn create_inode_protection_tracks_replay_pin() {
        let (cache, clock) = manual_cache();
        cache.record_result(
            id(1),
            DedupResult::Create {
                inode_id: 44,
                attrs: FileAttributes::default(),
            },
        );
        assert!(cache.protects_replay_inode(44));

        clock.store(9, Ordering::SeqCst);
        let replay = cache.begin(id(1), true).await.unwrap().unwrap();
        clock.store(20, Ordering::SeqCst);
        assert!(
            cache.protects_replay_inode(44),
            "admitted create replay keeps the inode live past nominal expiry"
        );

        drop(replay);
        assert!(!cache.protects_replay_inode(44));
    }

    #[tokio::test]
    async fn final_create_unprotect_notifies_reclaimer_once() {
        let (cache, clock) = manual_cache();
        let (reclaim_tx, mut reclaim_rx) = mpsc::unbounded_channel();
        cache.set_reclaim_sender(&reclaim_tx);
        cache.record_result(
            id(1),
            DedupResult::Create {
                inode_id: 44,
                attrs: FileAttributes::default(),
            },
        );

        assert!(cache.reclaim_when_unprotected(44));
        assert!(reclaim_rx.try_recv().is_err());
        clock.store(10, Ordering::SeqCst);
        assert!(cache.get(&id(1)).is_none());
        assert_eq!(reclaim_rx.try_recv().unwrap(), 44);
        assert!(reclaim_rx.try_recv().is_err());
        assert!(!cache.reclaim_when_unprotected(44));
    }

    #[tokio::test]
    async fn shared_inode_protection_handles_reverse_expiry() {
        let (cache, clock) = manual_cache();
        let first_deadline = cache
            .record_entry_with_expiry(DedupEntry {
                op_id: id(1),
                result: DedupResult::Create {
                    inode_id: 44,
                    attrs: FileAttributes::default(),
                },
            })
            .unwrap();

        clock.store(5, Ordering::SeqCst);
        let second_deadline = cache
            .record_entry_with_expiry(DedupEntry {
                op_id: id(2),
                result: DedupResult::Create {
                    inode_id: 44,
                    attrs: FileAttributes::default(),
                },
            })
            .unwrap();
        assert!(second_deadline > first_deadline);

        // Duplicate replicated inode references may expire out of insertion order.
        clock.store(9, Ordering::SeqCst);
        let first_replay = cache.begin(id(1), true).await.unwrap().unwrap();
        clock.store(15, Ordering::SeqCst);
        assert!(cache.get(&id(2)).is_none());
        assert_eq!(
            cache.replay_inode_protected_until(44),
            Some(second_deadline),
            "the stale maximum deadline remains conservative while a reference survives"
        );

        drop(first_replay);
        assert!(!cache.protects_replay_inode(44));
    }

    #[tokio::test]
    async fn distinct_inflight_reservations_are_independent() {
        let (cache, _clock) = manual_cache();
        let first = cache.begin(id(1), false).await.unwrap().unwrap();
        let second = cache.begin(id(2), false).await.unwrap().unwrap();
        assert_stats(&cache, (0, 2, 0));
        cache.record_result(id(1), DedupResult::Applied);
        assert_stats(&cache, (1, 1, 0));
        drop(first);
        assert_stats(&cache, (1, 1, 0));
        drop(second);
        assert_stats(&cache, (1, 0, 0));
    }

    #[test]
    fn rerecord_keeps_first_result_without_growth() {
        let cache = DedupCache::new();
        cache.record_result(
            id(1),
            DedupResult::Create {
                inode_id: 11,
                attrs: FileAttributes::default(),
            },
        );
        cache.record_result(id(1), DedupResult::Applied);
        assert_eq!(cache.stats().retained_results, 1);
        assert!(matches!(
            cache.get(&id(1)),
            Some(DedupResult::Create { inode_id: 11, .. })
        ));
    }

    #[test]
    fn wire_result_roundtrips_and_rejects_bad_encodings() {
        let attrs = FileAttributes {
            fileid: 42,
            mode: 0o640,
            ..Default::default()
        };
        let entry = DedupEntry {
            op_id: id(9),
            result: DedupResult::Create {
                inode_id: 42,
                attrs,
            },
        };
        let (raw_id, raw_result) = entry.to_wire_parts().unwrap();
        let decoded = DedupEntry::from_wire(&raw_id, &raw_result).unwrap();
        assert_eq!(decoded.op_id, id(9));
        match decoded.result {
            DedupResult::Create { inode_id, attrs } => {
                assert_eq!(inode_id, 42);
                assert_eq!(attrs.fileid, 42);
                assert_eq!(attrs.mode, 0o640);
            }
            _ => panic!("decoded the wrong result variant"),
        }
        assert!(matches!(
            DedupResult::decode_wire(&DedupResult::Remove.encode_wire().unwrap()).unwrap(),
            DedupResult::Remove
        ));
        assert!(matches!(
            DedupResult::decode_wire(&DedupResult::Rename.encode_wire().unwrap()).unwrap(),
            DedupResult::Rename
        ));

        let mut unknown_version = raw_result.clone();
        unknown_version[0] = DEDUP_RESULT_WIRE_VERSION + 1;
        assert!(matches!(
            DedupResult::decode_wire(&unknown_version),
            Err(DedupWireError::UnsupportedVersion(_))
        ));

        let mut trailing = raw_result;
        trailing.push(0xff);
        assert!(matches!(
            DedupResult::decode_wire(&trailing),
            Err(DedupWireError::InvalidResult(_))
        ));
        assert!(matches!(
            DedupEntry::from_wire(&[1u8; 15], &[DEDUP_RESULT_WIRE_VERSION]),
            Err(DedupWireError::InvalidOpIdLength(15))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_serializes_concurrent_ops() {
        let cache = Arc::new(DedupCache::new());
        let g1 = cache.begin(id(1), false).await.unwrap().unwrap();
        // A concurrent caller for the same id must block until the first finishes.
        let c2 = cache.clone();
        let waiter = tokio::spawn(async move { c2.begin(id(1), false).await.unwrap().is_some() });
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!waiter.is_finished());
        cache.record_result(id(1), DedupResult::Applied);
        drop(g1);
        assert!(waiter.await.unwrap());
        assert!(cache.begin(id(2), false).await.unwrap().is_some());
    }
}
