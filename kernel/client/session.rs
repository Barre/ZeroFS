use core::{
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

use kernel::{
    alloc::{KBox, KVVec, KVec, flags::GFP_KERNEL},
    bindings,
    bitmap::BitmapVec,
    error::code::ERESTARTSYS,
    ffi, new_condvar, new_mutex,
    prelude::*,
    sync::{Arc, CondVar, CondVarTimeoutResult, Mutex},
    time::msecs_to_jiffies,
};

use crate::{protocol::Rgetlineage, transport::SocketTransport};

use super::durability::UnsyncedEntry;
use super::endpoint::Endpoint;
use super::errors::{is_internal_restart_status, not_connected_errno};
use super::receive::ReceiveState;
use super::reconnect::ProbedCandidate;
use super::registry::{CredentialSlot, FidSlot, LockRecord};
use super::signals::sleep_uninterruptible_tick;
use super::slots::{PendingSlot, PendingState};
use super::{
    CLIENT_ID_LEN, FIRST_NORMAL_TAG, INITIAL_PENDING_TAGS, LIVENESS_WINDOW_MS, MAX_REPLY_WAITERS,
    NORMAL_TAG_COUNT, RECEIVE_BATCH_BYTES, ROOT_FID, SLOT_SHARDS, SMALL_REPLY_BUFFERS,
    SMALL_REPLY_BYTES, elapsed_ms, jiffies_for_ms, monotonic_ns,
};

#[pin_data]
pub(super) struct Session {
    pub(super) msize: u32,
    pub(super) timeout_jiffies: usize,
    /// Longest a request blocks waiting for reconnect and replay.
    pub(super) grace_ms: u64,
    /// Age of a decoded frame still accepted as proof a peer is alive. This is
    /// shorter than a reply deadline so only recent traffic extends that wait.
    liveness_window_ms: u64,
    /// Lock owner identity sent with every `Tlock` and `Tgetlock`.
    ///
    /// Fixed for the mount because replay has to resend the same bytes, and
    /// held here rather than per record because there is only ever one value.
    client_id: [u8; CLIENT_ID_LEN],
    /// Index in the endpoint's target set that the last successful probe won
    /// on, and where the next probe round starts.
    ///
    /// The steady state is therefore one dial, and a failover is typically two.
    /// A target only becomes the starting point by proving it serves, so a peer
    /// that refused or lost leadership never holds the rotation.
    pub(super) preferred_target: AtomicU32,
    /// Latches the first msize-mismatch report. A permanently misconfigured
    /// peer is re-probed on every reconnect round, so an unthrottled warning
    /// would be one log line per round forever.
    pub(super) msize_mismatch_warned: AtomicBool,
    /// Monotonic nanoseconds when a frame was last decoded on any connection.
    pub(super) last_frame_ns: AtomicU64,
    /// Set while an in-band liveness probe is active.
    pub(super) probe_in_flight: AtomicBool,
    /// Lock-free mirror of `SessionState::connection_epoch`.
    ///
    /// A receiver checks this before and after taking a tag shard. Retirement
    /// publishes the new epoch before sweeping the shards, so a reply either
    /// wins its tag lock and is published, or observes that its stream has
    /// already been retired.
    pub(super) active_epoch: AtomicU64,
    /// Frames written to the current connection whose reply is not published
    /// yet. Kept outside `state` because the receiver updates it for every
    /// frame; Tflush is the only other reader or writer.
    pub(super) sent_count: AtomicUsize,
    /// Outstanding registered reads whose payload cannot fit the accumulator.
    ///
    /// Nonzero means the next frame is likely a bulk read, so an empty
    /// accumulator is filled with just a header rather than with as much as the
    /// socket will give. Anything already buffered has to be copied into the
    /// caller's folios; not buffering it is the only way to avoid that copy.
    /// Maintained from `SlotDestination::limit` at both ends, so it cannot
    /// drift, and a wrong value only costs an extra receive.
    pub(super) bulk_reads: AtomicU32,
    #[pin]
    pub(super) send_lock: Mutex<()>,
    #[pin]
    pub(super) receive: Mutex<ReceiveState>,
    /// Reusable frames for allocation-free metadata replies.
    #[pin]
    pub(super) reply_pool: Mutex<KVVec<KVVec<u8>>>,
    /// Resident pending request slots distributed by `tag % slot_shards.len()`.
    ///
    /// Reply publication and the matching waiter touch only one shard. Session
    /// admission serializes bitmap allocation and geometric table growth
    /// through `state`.
    pub(super) slot_shards: KVVec<Pin<KBox<Mutex<KVVec<PendingSlot>>>>>,
    #[pin]
    pub(super) state: Mutex<SessionState>,
    /// Redial descriptor for the connection task.
    ///
    /// This follows `state` so its active network-namespace reference is
    /// released only after the current and candidate sockets have been
    /// dropped.
    pub(super) endpoint: Endpoint,
    #[pin]
    pub(super) changed: CondVar,
    /// Signalled on every `SessionStatus` transition so callers can wait for a
    /// replacement and reconnect backoff can be interrupted.
    #[pin]
    pub(super) live_changed: CondVar,
    pub(super) reply_waiters: KVVec<Pin<KBox<CondVar>>>,
}

pub(super) struct SessionState {
    pub(super) status: SessionStatus,
    /// The transport every sender and receiver currently uses.
    ///
    /// Senders read it inside their `send_lock` section and receivers inside
    /// their `receive` section; `install_connection` holds both, so a frame can
    /// never be split across two sockets.
    pub(super) transport: Arc<SocketTransport>,
    /// A replacement the connection task is negotiating or replaying on,
    /// published so termination can shut it down instead of waiting it out.
    pub(super) candidate: Option<Arc<SocketTransport>>,
    /// Bumped whenever the tag namespace is invalidated, which is exactly when
    /// a connection is retired. A reply, a failure or a Tflush that names an
    /// older epoch belongs to a connection nobody can answer on any more.
    pub(super) connection_epoch: u64,
    /// Durability lineage of the current connection.
    lineage: Rgetlineage,
    /// Ordinary tag ownership, indexed independently of the four low control
    /// tags. A bit stays set through reply consumption until any direct-read
    /// destination has also been released.
    pub(super) normal_tags: BitmapVec,
    /// Prefix of `normal_tags` whose backing slots are resident.
    ///
    /// The high-water mark only grows. It is published after every shard has
    /// been extended, so a resident numeric tag is always addressable.
    pub(super) resident_normal_tags: usize,
    /// Next ordinary bitmap index considered by the cyclic allocator.
    pub(super) next_tag: usize,
    pub(super) next_fid: u32,
    pub(super) recycled_fids: KVec<u32>,
    /// Replay records indexed by fid.
    ///
    /// A slot is reserved before its number is issued, so recording a fid the
    /// server has already committed to cannot fail.
    pub(super) records: KVVec<FidSlot>,
    /// Interned identities referenced by `records`.
    pub(super) credentials: KVVec<Option<CredentialSlot>>,
    /// Granted byte ranges, one per slot, grown like `credentials` rather than
    /// compacted: a table of free slots is what lets a lock reserve its
    /// worst-case space before the request and store the result infallibly.
    pub(super) locks: KVVec<Option<LockRecord>>,
    /// Free slots promised to locks that are on the wire and not stored yet.
    /// Counted separately so two concurrent locks cannot be handed the same
    /// free slot.
    pub(super) lock_slots_claimed: usize,
    /// Durability obligations, one entry per remote inode holding an
    /// acknowledged mutation no fsync has verified. Capacity grows before a
    /// mutation is dispatched and retains its high-water allocation.
    pub(super) unsynced: KVVec<UnsyncedEntry>,
    /// Spare entries promised to mutations that may already be on the wire.
    pub(super) unsynced_slots_claimed: usize,
    /// Monotonic mutation counter. Every note takes the next value and stamps
    /// it into its entry, so `entry.generation <= snapshot` says exactly "no
    /// mutation touched this inode after that snapshot", including for an inode
    /// that had no entry at snapshot time.
    ///
    /// The userspace client keeps a counter per obligation. That cannot work
    /// here: discharged entries are removed, and recreating one would restart
    /// its counter, allowing a stale fsync to clear a live obligation carrying
    /// the same value.
    pub(super) mutation_stamp: u64,
    /// Set if `mutation_stamp` ever exhausts u64. Past that point a stamp no
    /// longer separates two windows, so nothing is ever discharged again.
    pub(super) mutation_stamp_exhausted: bool,
}

/// Durability obligations covered by one verified fsync.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum FsyncScope {
    Inode(u64),
    All,
}

/// Two distinct failure levels.
///
/// A transport failure is a property of the socket, not of the mount, and only
/// retires one connection. A replay that cannot rebuild observed state is the
/// one thing that ends the logical session.
#[derive(Clone, Copy)]
pub(super) enum SessionStatus {
    Connected,
    /// This connection is gone and the connection task is rebuilding it.
    Lost,
    /// Permanent. The first cause wins and is replayed to every later caller.
    Dead(ffi::c_int),
}

/// The connection one dispatch attempt is bound to.
pub(super) struct Dispatch {
    pub(super) transport: Arc<SocketTransport>,
    pub(super) epoch: u64,
    /// Lineage recorded against a mutation this connection acknowledges.
    pub(super) token: u64,
    /// Writer epoch stamped into a first-dispatch mutation envelope.
    pub(super) writer_epoch: u64,
}

/// The transport one receive pass is bound to.
pub(super) struct ReceiveLink {
    pub(super) transport: Arc<SocketTransport>,
    pub(super) epoch: u64,
}

/// The connection a status transition orphaned, as `shutdown_transports` takes
/// it: the published transport plus any candidate the connection task was
/// dialing.
pub(super) type OrphanedTransports = (Arc<SocketTransport>, Option<Arc<SocketTransport>>);

impl Session {
    pub(super) fn new(endpoint: Endpoint, candidate: ProbedCandidate) -> Result<Pin<KBox<Self>>> {
        let timeout_ms = endpoint.timeout_ms;
        let ProbedCandidate {
            transport,
            negotiated_msize: msize,
            lineage,
            target,
        } = candidate;
        let slot_count = INITIAL_PENDING_TAGS
            .checked_add(FIRST_NORMAL_TAG)
            .ok_or_else(|| EOVERFLOW)?;
        let slot_shards = vacant_slot_shards(slot_count)?;
        let reply_waiters = reply_waiter_queues(slot_count)?;
        let normal_tags = BitmapVec::new(NORMAL_TAG_COUNT, GFP_KERNEL)?;

        let timeout_jiffies = msecs_to_jiffies(timeout_ms) as usize;
        // Endpoint::validate already rejected a zero timeout, but a small
        // nonzero one still rounds to zero jiffies on a low-HZ kernel, and a
        // zero-jiffy wait expires without ever waiting.
        if timeout_jiffies == 0 {
            return Err(EINVAL);
        }
        let receive_capacity = (msize as usize).min(RECEIVE_BATCH_BYTES);
        let receive_buffer = KVVec::from_elem(0u8, receive_capacity, GFP_KERNEL)?;
        let reply_pool = small_reply_pool()?;

        KBox::pin_init(
            pin_init!(Self {
                endpoint,
                msize,
                timeout_jiffies,
                grace_ms: endpoint.grace_ms as u64,
                // A liveness proof older than the wait it justifies extending
                // would let a dead peer keep earning windows.
                liveness_window_ms: LIVENESS_WINDOW_MS.min((timeout_ms as u64 / 2).max(1)),
                client_id: generate_client_id(),
                preferred_target: AtomicU32::new(target as u32),
                msize_mismatch_warned: AtomicBool::new(false),
                last_frame_ns: AtomicU64::new(monotonic_ns()),
                probe_in_flight: AtomicBool::new(false),
                active_epoch: AtomicU64::new(0),
                sent_count: AtomicUsize::new(0),
                bulk_reads: AtomicU32::new(0),
                send_lock <- new_mutex!(()),
                receive <- new_mutex!(ReceiveState {
                    buffer: receive_buffer,
                    buffered: 0,
                }),
                reply_pool <- new_mutex!(reply_pool),
                slot_shards,
                state <- new_mutex!(SessionState {
                    status: SessionStatus::Connected,
                    transport,
                    candidate: None,
                    connection_epoch: 0,
                    lineage,
                    normal_tags,
                    resident_normal_tags: INITIAL_PENDING_TAGS,
                    next_tag: 0,
                    next_fid: ROOT_FID + 1,
                    recycled_fids: KVec::new(),
                    // Client::connect reserves the root slot and records it.
                    records: KVVec::new(),
                    credentials: KVVec::new(),
                    locks: KVVec::new(),
                    lock_slots_claimed: 0,
                    unsynced: KVVec::new(),
                    unsynced_slots_claimed: 0,
                    mutation_stamp: 0,
                    mutation_stamp_exhausted: false,
                }),
                changed <- new_condvar!(),
                live_changed <- new_condvar!(),
                reply_waiters,
            }),
            GFP_KERNEL,
        )
    }

    pub(super) fn client_id(&self) -> &[u8] {
        &self.client_id
    }

    /// Wait for a live connection and snapshot it under one acquisition.
    ///
    /// Taking the gate and the snapshot separately left a window in which the
    /// connection could be retired between them, costing the caller a whole
    /// extra pass. Holding the lock across both closes that window, and the
    /// already-connected case now costs one acquisition per request instead of
    /// two.
    ///
    /// The connection task changes `status` under this same mutex, so there is
    /// no window between the check and the sleep for a wakeup to be lost. The
    /// wait is interruptible because a caller may hold `i_rwsem`, and bounded
    /// because a netfslib worker running on a shared workqueue must not park
    /// there indefinitely.
    pub(super) fn dispatch_or_wait(
        &self,
        budget_jiffies: usize,
        resolve_ambiguity: bool,
    ) -> Result<Dispatch> {
        let mut remaining = budget_jiffies.min(self.grace_jiffies());
        loop {
            let mut state = self.state.lock();
            match state.status {
                SessionStatus::Dead(status) => return Err(Error::from_errno(status)),
                SessionStatus::Connected => {
                    return Ok(Dispatch {
                        transport: state.transport.clone(),
                        epoch: state.connection_epoch,
                        token: state.lineage.token,
                        writer_epoch: state.lineage.writer_epoch,
                    });
                }
                SessionStatus::Lost => {}
            }
            match self
                .live_changed
                .wait_interruptible_timeout(&mut state, remaining)
            {
                CondVarTimeoutResult::Woken { jiffies } => remaining = jiffies,
                CondVarTimeoutResult::Signal { jiffies } => {
                    remaining = jiffies;
                    if !resolve_ambiguity {
                        // Nothing from this logical operation is ambiguous.
                        // Preserve normal restart semantics for a signal that
                        // interrupts its initial dispatch gate.
                        return Err(ERESTARTSYS);
                    }
                    // A prior attempt of this mutation may already have been
                    // applied. Letting ERESTARTSYS escape would mint a new
                    // operation ID on transparent syscall restart, so pace the
                    // pending signal and keep resolving the original ID.
                    drop(state);
                    if !sleep_uninterruptible_tick(&mut remaining) {
                        return Err(ETIMEDOUT);
                    }
                }
                CondVarTimeoutResult::Timeout => return Err(ETIMEDOUT),
            }
        }
    }

    /// The reconnect budget in jiffies, never zero: `Endpoint::validate`
    /// refuses a zero grace, but a small nonzero one still rounds to zero
    /// jiffies on a low-HZ kernel, and a zero-jiffy wait expires without ever
    /// waiting.
    fn grace_jiffies(&self) -> usize {
        jiffies_for_ms(self.grace_ms).max(1)
    }

    /// Whether a frame was decoded recently enough to prove a peer is alive.
    pub(super) fn heard_recently(&self) -> bool {
        elapsed_ms(self.last_frame_ns.load(Ordering::Relaxed), monotonic_ns())
            < self.liveness_window_ms
    }

    /// Retire the connection identified by `epoch`, leaving the session alive.
    ///
    /// The epoch witness is what stops a failure observed on an already
    /// replaced connection from tearing down its successor.
    pub(super) fn retire_connection(&self, error: Error, epoch: u64) {
        let transport = {
            let mut state = self.state.lock();
            if state.connection_epoch != epoch {
                return;
            }
            if !self.retire_locked(&mut state, None) {
                return;
            }
            state.transport.clone()
        };
        pr_warn!(
            "zerofs: connection lost (errno={}); reconnecting and replaying\n",
            error.to_errno()
        );
        self.notify_status_change();
        transport.shutdown();
    }

    /// Retire whatever connection is current, for a caller with no epoch
    /// witness of its own.
    pub(super) fn retire_current(&self, error: Error) {
        let epoch = self.state.lock().connection_epoch;
        self.retire_connection(error, epoch);
    }

    /// End the logical session permanently.
    ///
    /// Only a replay that cannot rebuild observed state, or a protocol desync,
    /// may reach this. The first cause wins and is replayed to every later
    /// caller.
    pub(super) fn terminate(&self, error: Error) {
        let transports = {
            let mut state = self.state.lock();
            if !self.retire_locked(&mut state, Some(error.to_errno())) {
                return;
            }
            (state.transport.clone(), state.candidate.clone())
        };
        pr_err!("zerofs: session ended: errno={}\n", error.to_errno());
        self.notify_status_change();
        shutdown_transports(transports);
    }

    /// Take the session out of `Connected`, sweeping the tag table.
    ///
    /// `terminal` distinguishes retiring one connection from ending the
    /// session. Returns whether this call performed the transition: `Dead` is
    /// final and a second retirement is a no-op, so the first cause wins in
    /// both directions.
    ///
    /// The sweep is what a per-connection pending map gives the userspace
    /// client for free. Leaving a slot `Sent` across the swap would let a reply
    /// on the replacement match a tag reserved for the retired connection.
    pub(super) fn retire_locked(
        &self,
        state: &mut SessionState,
        terminal: Option<ffi::c_int>,
    ) -> bool {
        match (state.status, terminal) {
            (SessionStatus::Dead(_), _) => return false,
            (SessionStatus::Lost, None) => return false,
            _ => {}
        }
        let status = match terminal {
            Some(status) => {
                // EINTR and ERESTART* are meaningful only while returning to
                // the task whose signal interrupted an operation. A terminal
                // status is retained and replayed to unrelated future callers,
                // so persist ENOTCONN instead of making every later operation
                // look freshly interrupted.
                let status = if status == EINTR.to_errno() || is_internal_restart_status(status) {
                    not_connected_errno().to_errno()
                } else {
                    status
                };
                state.status = SessionStatus::Dead(status);
                status
            }
            None => {
                state.status = SessionStatus::Lost;
                // Never returned to a caller: a swept slot means resend.
                not_connected_errno().to_errno()
            }
        };
        state.connection_epoch = state.connection_epoch.wrapping_add(1);
        self.active_epoch
            .store(state.connection_epoch, Ordering::Release);
        self.sent_count.store(0, Ordering::Relaxed);
        for shard in self.slot_shards.iter() {
            let mut slots = shard.as_ref().get_ref().lock();
            for slot in slots.iter_mut() {
                if matches!(slot.state, PendingState::Reserved | PendingState::Sent) {
                    slot.state = PendingState::Failed(status);
                }
            }
        }
        true
    }

    pub(super) fn decrement_sent_count(&self) {
        // Retirement may reset this advisory count while a reply that already
        // won its tag lock finishes.
        let _ = self
            .sent_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_sub(1))
            });
    }

    /// Wake every wait that observes `SessionStatus`.
    pub(super) fn notify_status_change(&self) {
        self.changed.notify_all();
        self.live_changed.notify_all();
        self.wake_all_reply_waiters();
    }

    /// Publish a fully replayed candidate as the session's connection.
    ///
    /// Ordering is send_lock -> receive -> state, which is the only order any
    /// path in this file uses. Holding the first two is what proves no sender
    /// is mid-frame and no receiver is mid-frame on the transport being
    /// replaced, including one filling a caller's registered iterator: a
    /// destination is only ever claimed while `receive` is held.
    pub(super) fn install_connection(
        &self,
        transport: &Arc<SocketTransport>,
        lineage: Rgetlineage,
    ) -> Result<()> {
        // Replay still needs bounded receives. Once this transport is visible
        // to the session, request waiters own response timeouts and retire it
        // by shutdown, so the sole receiver may block here while idle.
        transport.set_blocking_receive()?;
        let _send = self.send_lock.lock();
        let mut receive = self.receive.lock();
        let mut state = self.state.lock();
        if let SessionStatus::Dead(status) = state.status {
            return Err(Error::from_errno(status));
        }
        // A completed Rflush is a stream-consumption barrier owned by an
        // interrupted caller. Vacating it here would make that caller's
        // completion path see a slot it never consumed, so let the owner
        // finish and dial again.
        if self.flush_reply_pending() {
            return Err(EAGAIN);
        }
        // The retired stream's bytes went with its socket.
        receive.buffered = 0;
        state.transport = transport.clone();
        state.candidate = None;
        state.lineage = lineage;
        state.status = SessionStatus::Connected;
        state.next_tag = 0;
        self.last_frame_ns.store(monotonic_ns(), Ordering::Relaxed);
        drop(state);
        drop(receive);

        self.notify_status_change();
        Ok(())
    }
}

/// Mint this mount's lock owner identity.
///
/// v9fs sends `utsname()->nodename` here, which the generated bindings cannot
/// reach: `struct new_utsname` is absent from them and `uts_namespace` is
/// opaque. Nothing reads the value back in either reference implementation, so
/// what the wire actually requires of it is only that it stays fixed for the
/// mount, and a random identity is unique across hosts where a nodename is not.
fn generate_client_id() -> [u8; CLIENT_ID_LEN] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    // generate_random_uuid() writes a fixed 16 bytes, so the identity sized for
    // its hex form must have room for exactly that many pairs.
    const _: () = assert!(CLIENT_ID_LEN == 7 + 2 * 16);
    let mut uuid = [0u8; 16];
    // SAFETY: `uuid` is a writable 16-byte buffer, exactly the contract
    // required by generate_random_uuid().
    unsafe {
        bindings::generate_random_uuid(uuid.as_mut_ptr());
    }
    let mut client_id = [b'-'; CLIENT_ID_LEN];
    client_id[..7].copy_from_slice(b"zerofs-");
    for (index, byte) in uuid.iter().enumerate() {
        client_id[7 + 2 * index] = HEX[(byte >> 4) as usize];
        client_id[8 + 2 * index] = HEX[(byte & 0xf) as usize];
    }
    client_id
}

/// The tag table split into a bounded number of permanently pinned shards.
fn vacant_slot_shards(count: usize) -> Result<KVVec<Pin<KBox<Mutex<KVVec<PendingSlot>>>>>> {
    let shard_count = count.min(SLOT_SHARDS);
    let mut shards = KVVec::with_capacity(shard_count, GFP_KERNEL)?;
    for shard_index in 0..shard_count {
        let local_count = count.saturating_sub(shard_index).div_ceil(shard_count);
        let mut slots = KVVec::with_capacity(local_count, GFP_KERNEL)?;
        for _ in 0..local_count {
            slots
                .push_within_capacity(PendingSlot::vacant())
                .map_err(|_| ENOMEM)?;
        }
        let shard = KBox::pin_init(new_mutex!(slots), GFP_KERNEL)?;
        shards.push_within_capacity(shard).map_err(|_| ENOMEM)?;
    }
    Ok(shards)
}

/// Allocate a bounded set of empty small-frame buffers.
fn small_reply_pool() -> Result<KVVec<KVVec<u8>>> {
    let mut pool = KVVec::with_capacity(SMALL_REPLY_BUFFERS, GFP_KERNEL)?;
    for _ in 0..SMALL_REPLY_BUFFERS {
        let buffer = KVVec::with_capacity(SMALL_REPLY_BYTES, GFP_KERNEL)?;
        pool.push_within_capacity(buffer).map_err(|_| ENOMEM)?;
    }
    Ok(pool)
}

/// Reply waitqueues, one per tag until `MAX_REPLY_WAITERS`, then shared.
///
/// `Session::wake_reply_waiter` is what handles the shared case.
fn reply_waiter_queues(slot_count: usize) -> Result<KVVec<Pin<KBox<CondVar>>>> {
    let count = slot_count.min(MAX_REPLY_WAITERS);
    let mut waiters = KVVec::with_capacity(count, GFP_KERNEL)?;
    for _ in 0..count {
        let waiter = KBox::pin_init(new_condvar!(), GFP_KERNEL)?;
        waiters.push_within_capacity(waiter).map_err(|_| ENOMEM)?;
    }
    Ok(waiters)
}

/// Wake every socket-side waiter on the transports a transition retired.
///
/// The exact handles are captured under `state`, so this can never reach a
/// successor connection, and it needs no lock: `kernel_sock_shutdown` is
/// designed to race socket I/O, and a frame cut in half no longer matters
/// because a retired stream is never read or written again and its sender
/// resends the whole frame. Taking `send_lock` here instead would invert the
/// send_lock -> receive -> state order `install_connection` relies on, because
/// a receiver retires its connection while holding `receive`.
pub(super) fn shutdown_transports((transport, candidate): OrphanedTransports) {
    transport.shutdown();
    // A dial or replay in progress must not outlive the session, or the join
    // in Client::drop waits it out.
    if let Some(candidate) = candidate {
        candidate.shutdown();
    }
}
