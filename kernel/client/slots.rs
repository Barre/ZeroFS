use core::sync::atomic::Ordering;

use kernel::{
    alloc::{KVVec, flags::GFP_NOWAIT},
    error::code::ERESTARTSYS,
    ffi,
    prelude::*,
    sync::{CondVarTimeoutResult, Mutex},
};

use crate::{
    protocol::{self, GETATTR_ALL, HEADER_SIZE, Request, Response},
    transport::{CrossTaskDestination, PayloadIter, SendResult},
};

use super::RECEIVE_BATCH_BYTES;
use super::errors::{
    is_interrupted_error, message_size_errno, not_connected_errno, protocol_errno,
};
use super::registry::is_tombstoned_locked;
use super::session::{Dispatch, Session, SessionState, SessionStatus};
use super::signals::{SendSignalMask, resume_interrupted_send, sleep_uninterruptible_tick};
use super::tag_space::{
    FIRST_NORMAL_TAG, NORMAL_TAG_COUNT, next_free_resident_tag, next_normal_index,
    next_resident_count, normal_tag_index, normal_wire_tag,
};

/// Outcome of one transmission on the session's current transport.
pub(super) enum FrameSend {
    Sent,
    /// The frame never entered the stream and its tag is still `Reserved`.
    Rejected(Error),
    /// A signal interrupted the send before its first byte entered the stream.
    Interrupted(Error),
    /// Part of the frame may have entered the stream; its tag is `Sent`.
    Broken(Error),
}

pub(super) struct PendingSlot {
    pub(super) state: PendingState,
    pub(super) expected: Option<ExpectedResponse>,
    /// Largest frame accepted from the peer for protocol validation.
    pub(super) maximum_frame: usize,
    /// Storage allocated before this request can enter the wire.
    ///
    /// Buffered replies reserve `maximum_frame` bytes. A registered `Rread`
    /// delivers its payload into netfs-owned folios and reserves only enough
    /// for the alternative `Rlerror`. The receiver temporarily takes this
    /// value while filling it and publishes that same allocation as the
    /// completed frame.
    pub(super) reply_buffer: Option<KVVec<u8>>,
    /// A caller's claim on this tag for one direct `Rread` delivery.
    ///
    /// Only the owning [`DestinationGuard`] clears it, and `reserve_slot`
    /// skips a slot that still carries one, so a tag cannot be recycled under
    /// a receiver that is still writing into the registered iterator.
    pub(super) destination: Option<SlotDestination>,
}

impl PendingSlot {
    pub(super) fn vacant() -> Self {
        Self {
            state: PendingState::Vacant,
            expected: None,
            maximum_frame: 0,
            reply_buffer: None,
            destination: None,
        }
    }
}

pub(super) struct SlotDestination {
    /// Capability borrowed from the caller for this transaction.
    pub(super) iterator: CrossTaskDestination,
    /// Largest payload the owner registered room for.
    pub(super) limit: usize,
    /// A receiver is writing into `iterator` right now.
    pub(super) in_use: bool,
    /// Payload bytes a receiver placed in `iterator`.
    pub(super) delivered: Option<usize>,
}

impl SlotDestination {
    pub(super) fn from_registration(
        registration: &super::ops::ReplyDestinationRegistration<'_>,
    ) -> Self {
        let (iterator, limit) = registration.slot_parts();
        Self {
            iterator,
            limit,
            in_use: false,
            delivered: None,
        }
    }
}

/// Holds a tag's direct-delivery registration for one whole transaction.
///
/// Dropping it is the only way to retire the registration, and it waits out a
/// receiver that is mid-write, so a caller cannot return to netfslib, and let
/// the subrequest be terminated, while its folios are still being filled.
pub(super) struct DestinationGuard<'a> {
    pub(super) session: &'a Session,
    pub(super) tag: usize,
}

impl Drop for DestinationGuard<'_> {
    fn drop(&mut self) {
        self.session.release_destination(self.tag);
    }
}

pub(super) enum PendingState {
    Vacant,
    Reserved,
    Sent,
    Completed(KVVec<u8>),
    Consuming,
    Failed(ffi::c_int),
}

/// Result of claiming request admission.
///
/// A connection-retirement sweep may overtake the allocation phase after the
/// tag was claimed. That is a resend decision, not a pre-dispatch error that
/// may escape to the VFS caller.
pub(super) enum SlotReservation {
    Reserved(usize),
    Retry(Error),
}

impl Session {
    pub(super) fn slot_count(&self) -> usize {
        NORMAL_TAG_COUNT
    }

    /// Locate a wire tag in the interleaved shard table.
    ///
    /// The numeric range is fixed by the protocol. A high ordinary tag whose
    /// slot has not become resident yet maps to a valid shard but no element;
    /// callers reject that through their subsequent `get`.
    pub(super) fn slot_shard(&self, tag: usize) -> Result<(&Mutex<KVVec<PendingSlot>>, usize)> {
        let shard_count = self.slot_shards.len();
        if shard_count == 0 || tag >= protocol::NOTAG as usize {
            return Err(protocol_errno());
        }
        let shard = self
            .slot_shards
            .get(tag % shard_count)
            .map(|shard| shard.as_ref().get_ref())
            .ok_or_else(protocol_errno)?;
        Ok((shard, tag / shard_count))
    }

    /// Claim a tag for one dispatch, waiting for admission capacity.
    ///
    /// `admission_jiffies` clips the tag wait to what its operation has left.
    /// The caller rechecks that budget after response allocation and before
    /// dispatch, so a mutation cannot cross its retry horizon here.
    pub(super) fn reserve_slot(
        &self,
        expected: ExpectedResponse,
        maximum_frame: usize,
        destination: Option<SlotDestination>,
        admission_jiffies: usize,
        resolve_ambiguity: bool,
        guarded_fids: &[u32],
    ) -> Result<SlotReservation> {
        let direct_read =
            destination.is_some() && matches!(expected, ExpectedResponse::Read { .. });
        let reply_capacity = if direct_read {
            HEADER_SIZE + core::mem::size_of::<u32>()
        } else {
            maximum_frame
        };

        // Admission is local tag congestion, not a peer liveness test. The
        // caller already clipped this budget to reconnect grace and, for
        // mutations, to the dedup retry horizon. Never let local congestion
        // extend a first attempt beyond its ordinary request timeout.
        // Liveness probes pass one jiffy explicitly so they never queue behind
        // the work they diagnose.
        let mut remaining = self.timeout_jiffies.min(admission_jiffies).max(1);
        loop {
            let mut state = self.state.lock();
            match state.status {
                SessionStatus::Dead(status) => return Err(Error::from_errno(status)),
                SessionStatus::Lost => {
                    return Ok(SlotReservation::Retry(not_connected_errno()));
                }
                SessionStatus::Connected => {}
            }
            // A replay that completes while this caller allocates or waits for
            // a slot tombstones under the same mutex, so rechecking each pass
            // cannot admit a fid that just died.
            if guarded_fids
                .iter()
                .any(|fid| is_tombstoned_locked(&state, *fid))
            {
                return Err(errno!(ESTALE));
            }

            let resident = state.resident_normal_tags;
            let cursor = state.next_tag;
            if let Some(index) = next_free_resident_tag(resident, cursor, |start| {
                state.normal_tags.next_zero_bit(start)
            }) {
                let candidate = normal_wire_tag(index).ok_or_else(protocol_errno)?;
                let (shard, local_tag) = self.slot_shard(candidate)?;
                let mut slots = shard.lock();
                let slot = slots.get_mut(local_tag).ok_or_else(protocol_errno)?;
                // The bitmap is the allocation authority. Disagreement means
                // a tag could be handed to two requests, so fail closed rather
                // than silently repairing either side.
                if !matches!(slot.state, PendingState::Vacant)
                    || slot.reply_buffer.is_some()
                    || slot.destination.is_some()
                {
                    return Err(protocol_errno());
                }

                state.normal_tags.set_bit(index);
                slot.state = PendingState::Reserved;
                slot.expected = Some(expected);
                slot.maximum_frame = maximum_frame;
                state.next_tag = next_normal_index(index, state.resident_normal_tags);
                drop(slots);
                drop(state);

                // Claim the tag first, so at most the finite tag space can be
                // holding maximum-sized reply allocations. The request stays
                // `Reserved` and cannot receive anything until mark_sent(), so
                // allocating outside the locks is safe.
                let reply_buffer = match self.reply_buffer(reply_capacity) {
                    Ok(buffer) => buffer,
                    Err(error) => {
                        self.release_slot(candidate);
                        return Err(error);
                    }
                };

                // Retirement may fail this reserved slot, and replay may
                // tombstone one of its fids, while allocation sleeps. Attach
                // the buffer and direct-read registration only if the request
                // is still admissible.
                let state = self.state.lock();
                let status = match state.status {
                    SessionStatus::Dead(status) => Some((Error::from_errno(status), false)),
                    SessionStatus::Lost => Some((not_connected_errno(), true)),
                    _ if guarded_fids
                        .iter()
                        .any(|fid| is_tombstoned_locked(&state, *fid)) =>
                    {
                        Some((errno!(ESTALE), false))
                    }
                    _ => None,
                };
                let Ok((shard, local_tag)) = self.slot_shard(candidate) else {
                    drop(state);
                    self.recycle_reply_buffer(reply_buffer);
                    self.release_slot(candidate);
                    return Err(protocol_errno());
                };
                let mut slots = shard.lock();
                let Some(slot) = slots.get_mut(local_tag) else {
                    drop(slots);
                    drop(state);
                    self.recycle_reply_buffer(reply_buffer);
                    self.release_slot(candidate);
                    return Err(protocol_errno());
                };
                let status = status.or_else(|| match slot.state {
                    PendingState::Reserved
                        if slot.reply_buffer.is_none() && slot.destination.is_none() =>
                    {
                        None
                    }
                    // Retirement deliberately sweeps every claimed tag to
                    // Failed. Even if a replacement connection was installed
                    // while allocation slept, this request was never sent and
                    // belongs on the resend path.
                    PendingState::Failed(status) => Some((Error::from_errno(status), true)),
                    _ => Some((protocol_errno(), false)),
                });
                if let Some((error, retry)) = status {
                    drop(slots);
                    drop(state);
                    self.recycle_reply_buffer(reply_buffer);
                    self.release_slot(candidate);
                    return if retry {
                        Ok(SlotReservation::Retry(error))
                    } else {
                        Err(error)
                    };
                }

                slot.reply_buffer = Some(reply_buffer);
                if destination
                    .as_ref()
                    .is_some_and(|slot| slot.limit > RECEIVE_BATCH_BYTES)
                {
                    self.bulk_reads.fetch_add(1, Ordering::Relaxed);
                }
                slot.destination = destination;
                return Ok(SlotReservation::Reserved(candidate));
            }

            if self.grow_resident_slots(&mut state)? {
                continue;
            }

            match self
                .changed
                .wait_interruptible_timeout(&mut state, remaining)
            {
                CondVarTimeoutResult::Timeout => {
                    pr_warn!(
                        "zerofs: admission timed out waiting to send {}\n",
                        expected.name()
                    );
                    return Err(ETIMEDOUT);
                }
                CondVarTimeoutResult::Signal { jiffies } => {
                    remaining = jiffies;
                    if !resolve_ambiguity {
                        // No earlier frame from this logical operation is
                        // ambiguous, so a restart from the VFS entry point is
                        // still safe.
                        return Err(ERESTARTSYS);
                    }
                    // An ambiguous mutation must keep its operation ID even
                    // while waiting for a tag. Pace an unmaskable pending
                    // signal and stay inside the bounded recovery episode.
                    drop(state);
                    if !sleep_uninterruptible_tick(&mut remaining) {
                        return Err(ETIMEDOUT);
                    }
                }
                CondVarTimeoutResult::Woken { jiffies } => remaining = jiffies,
            }
        }
    }

    /// Transmit one complete frame and mark `tag` as sent.
    pub(super) fn send_frame(&self, tag: usize, frame: &[u8], dispatch: &Dispatch) -> FrameSend {
        self.transmit(tag, dispatch, |on_error| {
            dispatch.transport.send_all_interruptible(frame, on_error)
        })
    }

    /// Transmit one complete `Twrite` and mark `tag` as sent.
    ///
    /// The payload never lands in an intermediate buffer, so the prefix and
    /// the payload are two socket calls that must stay adjacent on the stream.
    /// Holding the same send lock over both keeps the record complete, and the
    /// signal policy is the one [`Self::transmit`] documents.
    pub(super) fn send_frame_with_payload(
        &self,
        tag: usize,
        prefix: &[u8],
        payload: PayloadIter<'_>,
        dispatch: &Dispatch,
    ) -> FrameSend {
        self.transmit(tag, dispatch, |on_error| {
            dispatch
                .transport
                .send_all_with_payload(prefix, payload, on_error)
        })
    }

    /// Claim `tag`, then run one transmission holding the send lock.
    ///
    /// Signals stay unblocked until the socket reports an interrupted send.
    /// Blocking them only then still keeps a started frame from being
    /// abandoned mid-stream, because the retry resumes at the exact cursor,
    /// and it saves two sigprocmask calls on every uninterrupted request. The
    /// reply wait afterward observes the pending signal and retires the
    /// request through Tflush.
    fn transmit(
        &self,
        tag: usize,
        dispatch: &Dispatch,
        send: impl FnOnce(&mut dyn FnMut(Error) -> Result<()>) -> SendResult,
    ) -> FrameSend {
        let mut signal_mask: Option<SendSignalMask> = None;
        let _guard = self.send_lock.lock();
        if let Err(error) = self.mark_sent(tag, dispatch.epoch) {
            return FrameSend::Rejected(error);
        }
        let mut on_error = resume_interrupted_send(&mut signal_mask);
        match send(&mut on_error) {
            Ok(()) => FrameSend::Sent,
            Err(failure) if !failure.started() && is_interrupted_error(failure.error()) => {
                // mark_sent accounted for this tag, but a zero-progress send is
                // still safe to release and return to the interrupted caller.
                self.decrement_sent_count();
                FrameSend::Interrupted(ERESTARTSYS)
            }
            Err(failure) => FrameSend::Broken(failure.error()),
        }
    }

    /// Claim `tag` for a frame about to be transmitted on connection `epoch`.
    ///
    /// The epoch check is what keeps a frame off a connection other than the
    /// one whose lineage its envelope names, and off a tag table that has been
    /// swept since the caller snapshotted it.
    pub(super) fn mark_sent(&self, tag: usize, epoch: u64) -> Result<()> {
        let state = self.state.lock();
        match state.status {
            SessionStatus::Dead(status) => return Err(Error::from_errno(status)),
            SessionStatus::Lost => return Err(not_connected_errno()),
            SessionStatus::Connected => {}
        }
        if state.connection_epoch != epoch {
            return Err(not_connected_errno());
        }
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut slots = shard.lock();
        let Some(slot) = slots.get_mut(local_tag) else {
            return Err(protocol_errno());
        };
        if !matches!(slot.state, PendingState::Reserved) {
            return Err(protocol_errno());
        }
        slot.state = PendingState::Sent;
        self.sent_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn release_slot(&self, tag: usize) {
        let mut state = self.state.lock();
        let Ok((shard, local_tag)) = self.slot_shard(tag) else {
            drop(state);
            self.terminate(protocol_errno());
            return;
        };
        let mut slots = shard.lock();
        let Some(slot) = slots.get_mut(local_tag) else {
            drop(slots);
            drop(state);
            self.terminate(protocol_errno());
            return;
        };
        let unused_buffer = vacate_slot(slot);
        clear_normal_tag_if_idle(&mut state, tag, slot);
        drop(slots);
        drop(state);
        if let Some(buffer) = unused_buffer {
            self.recycle_reply_buffer(buffer);
        }
        // Control tags live outside the ordinary bitmap, but releasing one can
        // still unblock reserve_flush_tag(). Waking unconditionally also keeps
        // allocation changes visible under the same release contract.
        self.changed.notify_all();
    }

    /// Vacate one tag while the caller holds the session-state mutex.
    ///
    /// Flush completion uses this to retire its control tag and the original
    /// request atomically.
    pub(super) fn vacate_tag_locked(&self, state: &mut SessionState, tag: usize) -> Result<()> {
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut slots = shard.lock();
        let slot = slots.get_mut(local_tag).ok_or_else(protocol_errno)?;
        drop(vacate_slot(slot));
        clear_normal_tag_if_idle(state, tag, slot);
        Ok(())
    }

    /// Retire a tag's direct-delivery registration once no receiver holds it.
    ///
    /// The registration is what keeps the tag claimed, so this wait is what
    /// stops a caller from returning to netfslib while a receiver is still
    /// filling its folios. It is uninterruptible on purpose: the usual way to
    /// arrive here is a signal. The first local deadline retires the connection,
    /// whose socket shutdown wakes a receiver blocked in `recvmsg`. Retiring
    /// rather than terminating keeps a slow direct read survivable.
    fn release_destination(&self, tag: usize) {
        let mut remaining = self.timeout_jiffies;
        let mut escalated = false;
        loop {
            let Ok((shard, local_tag)) = self.slot_shard(tag) else {
                self.terminate(protocol_errno());
                return;
            };
            let mut slots = shard.lock();
            let Some(slot) = slots.get_mut(local_tag) else {
                drop(slots);
                self.terminate(protocol_errno());
                return;
            };
            let Some(destination) = slot.destination.as_ref() else {
                return;
            };
            if !destination.in_use {
                if destination.limit > RECEIVE_BATCH_BYTES {
                    self.bulk_reads.fetch_sub(1, Ordering::Relaxed);
                }
                slot.destination = None;
                drop(slots);
                match self.release_normal_tag_if_idle(tag) {
                    Ok(_) => self.changed.notify_all(),
                    Err(error) => self.terminate(error),
                }
                return;
            }
            drop(slots);

            if !sleep_uninterruptible_tick(&mut remaining) {
                remaining = self.timeout_jiffies;
                if escalated {
                    pr_err!("receiver still holds the read destination for tag {tag}\n");
                } else {
                    escalated = true;
                    pr_warn!(
                        "zerofs: direct-read destination for tag {} remained busy; retiring connection\n",
                        tag
                    );
                    // A receiver holding this claim also holds the `receive`
                    // mutex, which install_connection takes, so no swap can
                    // have happened and the current connection is its own.
                    self.retire_current(ETIMEDOUT);
                }
            }
        }
    }

    /// Return an ordinary wire tag to the allocator once neither its request
    /// state nor a direct-read destination still owns it.
    ///
    /// Reply consumption vacates the slot under the shard lock alone. Taking
    /// `state` afterward and rechecking the slot preserves the global
    /// `state -> shard` order used by allocation.
    pub(super) fn release_normal_tag_if_idle(&self, tag: usize) -> Result<bool> {
        let mut state = self.state.lock();
        let (shard, local_tag) = self.slot_shard(tag)?;
        let slots = shard.lock();
        let slot = slots.get(local_tag).ok_or_else(protocol_errno)?;
        Ok(clear_normal_tag_if_idle(&mut state, tag, slot))
    }

    /// Extend every shard before publishing a larger ordinary-tag high-water
    /// mark.
    ///
    /// Capacity is reserved without reclaim in a first pass because admission
    /// may run from netfs writeback while holding the session mutex. If one
    /// reservation cannot be satisfied immediately, no shard length and no
    /// externally visible tag range changed; the caller waits for an existing
    /// request to release a tag and retries later. The second pass therefore
    /// uses only infallible-within-capacity pushes. Even an unexpected push
    /// failure leaves the high-water mark unchanged, so the partially appended
    /// tail remains unreachable and a later attempt can finish it safely.
    fn grow_resident_slots(&self, state: &mut SessionState) -> Result<bool> {
        let old = state.resident_normal_tags;
        if old >= NORMAL_TAG_COUNT {
            return Ok(false);
        }
        let new = next_resident_count(old);
        let total = FIRST_NORMAL_TAG
            .checked_add(new)
            .ok_or_else(protocol_errno)?;
        let shard_count = self.slot_shards.len();
        if shard_count == 0 {
            return Err(protocol_errno());
        }

        for (index, shard) in self.slot_shards.iter().enumerate() {
            let desired = total.saturating_sub(index).div_ceil(shard_count);
            let mut slots = shard.as_ref().get_ref().lock();
            let additional = desired.saturating_sub(slots.len());
            if additional != 0 && slots.reserve(additional, GFP_NOWAIT).is_err() {
                return Ok(false);
            }
        }

        for (index, shard) in self.slot_shards.iter().enumerate() {
            let desired = total.saturating_sub(index).div_ceil(shard_count);
            let mut slots = shard.as_ref().get_ref().lock();
            while slots.len() < desired {
                slots
                    .push_within_capacity(PendingSlot::vacant())
                    .map_err(|_| protocol_errno())?;
            }
        }

        state.resident_normal_tags = new;
        Ok(true)
    }
}

fn clear_normal_tag_if_idle(state: &mut SessionState, tag: usize, slot: &PendingSlot) -> bool {
    let Some(index) = normal_tag_index(tag) else {
        return false;
    };
    if !matches!(slot.state, PendingState::Vacant) || slot.destination.is_some() {
        return false;
    }
    state.normal_tags.clear_bit(index);
    true
}

/// Return a slot to `Vacant` and hand back any unused preallocated buffer.
///
/// `destination` survives on purpose: only the owning [`DestinationGuard`]
/// clears it, which is what keeps the tag out of `reserve_slot` while a
/// receiver may still be writing into the registered iterator.
pub(super) fn vacate_slot(slot: &mut PendingSlot) -> Option<KVVec<u8>> {
    let reply_buffer = slot.reply_buffer.take();
    slot.state = PendingState::Vacant;
    slot.expected = None;
    slot.maximum_frame = 0;
    reply_buffer
}

#[derive(Clone, Copy)]
pub(super) enum ExpectedResponse {
    Version,
    Lineage,
    Rebind,
    WalkGetattr { qids: usize },
    Getattr,
    Setattrattr,
    Fallocate,
    Openat,
    OpenatRead { maximum: usize },
    Lcreateattr,
    Mkdirattr,
    Symlinkattr,
    Mknodattr,
    Linkattr,
    Renameat,
    Unlinkat,
    Readlink,
    Flush,
    Read { maximum: usize },
    Write { maximum: usize },
    Fsync,
    Readdirattr { maximum: usize },
    Clunk,
    Statfs,
    Lock,
    Getlock,
}

impl ExpectedResponse {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Version => "Tversion",
            Self::Lineage => "Tgetlineage",
            Self::Rebind => "Trebind",
            Self::WalkGetattr { .. } => "Twalkgetattr",
            Self::Getattr => "Tgetattr",
            Self::Setattrattr => "Tsetattrattr",
            Self::Fallocate => "Tfallocate",
            Self::Openat => "Tlopenat",
            Self::OpenatRead { .. } => "Tlopenatread",
            Self::Lcreateattr => "Tlcreateattr",
            Self::Mkdirattr => "Tmkdirattr",
            Self::Symlinkattr => "Tsymlinkattr",
            Self::Mknodattr => "Tmknodattr",
            Self::Linkattr => "Tlinkattr",
            Self::Renameat => "Trenameat",
            Self::Unlinkat => "Tunlinkat",
            Self::Readlink => "Treadlink",
            Self::Flush => "Tflush",
            Self::Read { .. } => "Tread",
            Self::Write { .. } => "Twrite",
            Self::Fsync => "Tfsyncdur",
            Self::Readdirattr { .. } => "Treaddirattr",
            Self::Clunk => "Tclunk",
            Self::Statfs => "Tstatfs",
            Self::Lock => "Tlock",
            Self::Getlock => "Tgetlock",
        }
    }

    pub(super) fn for_request(request: &Request<'_>) -> Result<Self> {
        Ok(match request {
            Request::Tversion { .. } => Self::Version,
            Request::Tgetlineage => Self::Lineage,
            Request::Trebind { .. } => Self::Rebind,
            Request::Twalkgetattr { names, .. } => {
                if names.len() > u16::MAX as usize {
                    return Err(message_size_errno());
                }
                Self::WalkGetattr { qids: names.len() }
            }
            Request::Tgetattr { .. } => Self::Getattr,
            Request::Tsetattrattr { .. } => Self::Setattrattr,
            Request::Tfallocate { .. } => Self::Fallocate,
            Request::Tlopenat { .. } => Self::Openat,
            Request::Tlopenatread { count, .. } => Self::OpenatRead {
                maximum: *count as usize,
            },
            Request::Tlcreateattr { .. } => Self::Lcreateattr,
            Request::Tmkdirattr { .. } => Self::Mkdirattr,
            Request::Tsymlinkattr { .. } => Self::Symlinkattr,
            Request::Tmknodattr { .. } => Self::Mknodattr,
            Request::Tlinkattr { .. } => Self::Linkattr,
            Request::Trenameat { .. } => Self::Renameat,
            Request::Tunlinkat { .. } => Self::Unlinkat,
            Request::Treadlink { .. } => Self::Readlink,
            Request::Tflush { .. } => Self::Flush,
            Request::Tread { count, .. } => Self::Read {
                maximum: *count as usize,
            },
            Request::Twrite { data, .. } => Self::Write {
                maximum: data.len(),
            },
            Request::Tfsyncdur { .. } => Self::Fsync,
            Request::Treaddirattr { count, .. } => Self::Readdirattr {
                maximum: *count as usize,
            },
            Request::Tclunk { .. } => Self::Clunk,
            Request::Tstatfs { .. } => Self::Statfs,
            Request::Tlock { .. } => Self::Lock,
            Request::Tgetlock { .. } => Self::Getlock,
        })
    }

    pub(super) fn maximum_frame_size(self, msize: u32) -> Result<usize> {
        let normal = match self {
            Self::Version => msize as usize,
            Self::Lineage => HEADER_SIZE + 2 * 8,
            Self::Rebind => HEADER_SIZE + protocol::QID_WIRE_SIZE,
            Self::WalkGetattr { qids } => qids
                .checked_mul(protocol::QID_WIRE_SIZE)
                .and_then(|qids_size| {
                    (HEADER_SIZE + 2 + protocol::STAT_WIRE_SIZE).checked_add(qids_size)
                })
                .ok_or_else(message_size_errno)?,
            Self::Getattr => HEADER_SIZE + 8 + protocol::STAT_WIRE_SIZE,
            Self::Setattrattr
            | Self::Mkdirattr
            | Self::Symlinkattr
            | Self::Mknodattr
            | Self::Linkattr => HEADER_SIZE + protocol::STAT_WIRE_SIZE,
            Self::Fallocate | Self::Renameat | Self::Unlinkat => HEADER_SIZE,
            Self::Openat => HEADER_SIZE + protocol::QID_WIRE_SIZE + 4,
            Self::OpenatRead { maximum } => protocol::RLOPENATREAD_OVERHEAD
                .checked_add(maximum.min(protocol::max_lopenatread_payload(msize) as usize))
                .ok_or_else(message_size_errno)?,
            Self::Lcreateattr => HEADER_SIZE + 4 + protocol::STAT_WIRE_SIZE,
            Self::Readlink => {
                (msize as usize).min(HEADER_SIZE + core::mem::size_of::<u16>() + u16::MAX as usize)
            }
            Self::Flush => HEADER_SIZE,
            Self::Read { maximum } | Self::Readdirattr { maximum } => (HEADER_SIZE + 4)
                .checked_add(maximum)
                .ok_or_else(message_size_errno)?,
            Self::Write { .. } => HEADER_SIZE + 4,
            Self::Fsync => HEADER_SIZE,
            Self::Clunk => HEADER_SIZE,
            Self::Statfs => HEADER_SIZE + 2 * 4 + 6 * 8 + 4,
            Self::Lock => HEADER_SIZE + 1,
            // The server picks the reported holder's identity, so bound it by
            // the string length field rather than by our own client id.
            Self::Getlock => (msize as usize)
                .min(HEADER_SIZE + 1 + 8 + 8 + 4 + core::mem::size_of::<u16>() + u16::MAX as usize),
        };
        let maximum = normal.max(HEADER_SIZE + 4);
        if maximum > msize as usize {
            Err(message_size_errno())
        } else {
            Ok(maximum)
        }
    }

    /// The one reply type this request may be answered with.
    ///
    /// `Openat` names `Rlopenat`, so a server answering `Tlopenat` with the
    /// standard `Rlopen` that the codec also decodes is rejected here.
    fn response_type(self) -> u8 {
        match self {
            Self::Version => protocol::message_type::RVERSION,
            Self::Lineage => protocol::message_type::RGETLINEAGE,
            Self::Rebind => protocol::message_type::RREBIND,
            Self::WalkGetattr { .. } => protocol::message_type::RWALKGETATTR,
            Self::Getattr => protocol::message_type::RGETATTR,
            Self::Setattrattr => protocol::message_type::RSETATTRATTR,
            Self::Fallocate => protocol::message_type::RFALLOCATE,
            Self::Openat => protocol::message_type::RLOPENAT,
            Self::OpenatRead { .. } => protocol::message_type::RLOPENATREAD,
            Self::Lcreateattr => protocol::message_type::RLCREATEATTR,
            Self::Mkdirattr => protocol::message_type::RMKDIRATTR,
            Self::Symlinkattr => protocol::message_type::RSYMLINKATTR,
            Self::Mknodattr => protocol::message_type::RMKNODATTR,
            Self::Linkattr => protocol::message_type::RLINKATTR,
            Self::Renameat => protocol::message_type::RRENAMEAT,
            Self::Unlinkat => protocol::message_type::RUNLINKAT,
            Self::Readlink => protocol::message_type::RREADLINK,
            Self::Flush => protocol::message_type::RFLUSH,
            Self::Read { .. } => protocol::message_type::RREAD,
            Self::Write { .. } => protocol::message_type::RWRITE,
            Self::Fsync => protocol::message_type::RFSYNC,
            Self::Readdirattr { .. } => protocol::message_type::RREADDIRATTR,
            Self::Clunk => protocol::message_type::RCLUNK,
            Self::Statfs => protocol::message_type::RSTATFS,
            Self::Lock => protocol::message_type::RLOCK,
            Self::Getlock => protocol::message_type::RGETLOCK,
        }
    }

    pub(super) fn matches_type(self, type_: u8) -> bool {
        type_ == protocol::message_type::RLERROR || type_ == self.response_type()
    }

    pub(super) fn matches(self, response: &Response<'_>) -> bool {
        matches!(
            (self, response),
            (Self::Version, Response::Rversion(_))
                | (Self::Lineage, Response::Rgetlineage(_))
                | (Self::Rebind, Response::Rrebind(_))
                | (Self::Setattrattr, Response::Rsetattrattr(_))
                | (Self::Fallocate, Response::Rfallocate)
                | (Self::Openat, Response::Rlopenat(_))
                | (Self::Lcreateattr, Response::Rlcreateattr(_))
                | (Self::Mkdirattr, Response::Rmkdirattr(_))
                | (Self::Symlinkattr, Response::Rsymlinkattr(_))
                | (Self::Mknodattr, Response::Rmknodattr(_))
                | (Self::Linkattr, Response::Rlinkattr(_))
                | (Self::Renameat, Response::Rrenameat)
                | (Self::Unlinkat, Response::Runlinkat)
                | (Self::Readlink, Response::Rreadlink(_))
                | (Self::Flush, Response::Rflush)
                | (Self::Fsync, Response::Rfsync)
                | (Self::Clunk, Response::Rclunk)
                | (Self::Statfs, Response::Rstatfs(_))
                | (Self::Lock, Response::Rlock(_))
                | (Self::Getlock, Response::Rgetlock(_))
        ) || match (self, response) {
            (Self::Getattr, Response::Rgetattr(attributes)) => {
                attributes.valid & GETATTR_ALL == GETATTR_ALL
            }
            (Self::WalkGetattr { qids }, Response::Rwalkgetattr(walk)) => walk.qids.len() == qids,
            (Self::OpenatRead { maximum }, Response::Rlopenatread(open)) => {
                open.eof <= 1 && open.data.len() <= maximum
            }
            (Self::Read { maximum }, Response::Rread(read)) => read.data.len() <= maximum,
            (Self::Write { maximum }, Response::Rwrite(write)) => write.count as usize <= maximum,
            (Self::Readdirattr { maximum }, Response::Rreaddirattr(directory)) => {
                directory.data().len() <= maximum
            }
            _ => false,
        }
    }
}
