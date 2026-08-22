use core::mem;
use core::sync::atomic::Ordering;

use kernel::{
    alloc::KVVec,
    bindings,
    error::code::ERESTARTSYS,
    prelude::*,
    sync::{CondVar, CondVarTimeoutResult},
};

use crate::protocol::{self, Request, Response};

use super::errors::{codec_errno, not_connected_errno, protocol_errno};
use super::flush::FlushOutcome;
use super::receive::validate_response_frame;
use super::retry::{AttemptOutcome, OpAttempt};
use super::session::{Dispatch, Session};
use super::signals::{sleep_uninterruptible_tick, SendSignalMask};
use super::slots::{
    vacate_slot, ExpectedResponse, FrameSend, PendingSlot, PendingState, SlotReservation,
};
use super::{jiffies_for_ms, MAX_LIVENESS_EXTENSIONS, PROBE_TIMEOUT_MS};

pub(super) struct OwnedFrame<'a> {
    pub(super) bytes: ReplyBytes<'a>,
    pub(super) tag: u16,
    /// `Some(n)` means the payload never entered `bytes`: a receiver placed
    /// `n` bytes in the caller's registered iterator instead.
    pub(super) delivered: Option<usize>,
    /// Durability lineage of the connection that answered this request.
    pub(super) lineage_token: u64,
    /// Identity of that connection, so replay bookkeeping can tell whether it
    /// is still the session's.
    pub(super) connection_epoch: u64,
}

/// Reply storage that returns small allocations to the per-mount pool.
///
/// This wrapper moves intact into an [`OwnedPayload`](super::ops::OwnedPayload)
/// when a variable response outlives decoding, so recycling happens only after
/// its final consumer finishes.
pub(super) struct ReplyBytes<'a> {
    bytes: KVVec<u8>,
    session: &'a Session,
}

impl ReplyBytes<'_> {
    pub(super) fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl Drop for ReplyBytes<'_> {
    fn drop(&mut self) {
        let bytes = mem::replace(&mut self.bytes, KVVec::new());
        self.session.recycle_reply_buffer(bytes);
    }
}

impl Session {
    fn reply_waiter(&self, tag: usize) -> Result<&CondVar> {
        let waiter_count = self.reply_waiters.len();
        if waiter_count == 0 {
            // The `tag % waiter_count` below would divide by zero.
            return Err(protocol_errno());
        }
        self.reply_waiters
            .get(tag % waiter_count)
            .map(|waiter| waiter.as_ref().get_ref())
            .ok_or_else(protocol_errno)
    }

    pub(super) fn wake_reply_waiter(&self, tag: usize) -> Result<()> {
        let waiter = self.reply_waiter(tag)?;
        // The resident table may grow beyond this fixed waiter set. Wake every
        // colliding waiter so an exclusive wake cannot select a different tag
        // and leave the matching request asleep.
        waiter.notify_all();
        Ok(())
    }

    pub(super) fn wake_all_reply_waiters(&self) {
        for waiter in self.reply_waiters.iter() {
            waiter.as_ref().get_ref().notify_all();
        }
    }

    /// Wrap a reply wait as one attempt outcome.
    pub(super) fn reply_outcome<'a>(&self, result: Result<OwnedFrame<'a>>) -> AttemptOutcome<'a> {
        match result {
            Ok(frame) => AttemptOutcome::Reply(frame),
            Err(error) => AttemptOutcome::Failed(error),
        }
    }

    /// Hand a taken preallocated reply frame to its caller.
    fn own_completed_reply(
        &self,
        tag: usize,
        dispatch: &Dispatch,
        reply: CompletedReply,
    ) -> Result<OwnedFrame<'_>> {
        let frame = OwnedFrame {
            bytes: ReplyBytes {
                bytes: reply.bytes,
                session: self,
            },
            tag: tag as u16,
            delivered: reply.delivered,
            lineage_token: dispatch.token,
            connection_epoch: dispatch.epoch,
        };
        // A directly delivered frame carries no payload to decode.
        // check_incoming_header() already bound its tag, its type and its
        // declared size by the slot's maximum_frame, which is exactly the data
        // length bound decode_response would have applied.
        if frame.delivered.is_none() {
            // A frame that passed the header checks but fails to decode is a
            // protocol desync, not a lost peer.
            validate_response_frame(&frame).inspect_err(|error| self.terminate(*error))?;
        }
        Ok(frame)
    }

    pub(super) fn wait_for_reply(
        &self,
        tag: usize,
        dispatch: &Dispatch,
        attempt: &mut OpAttempt,
    ) -> Result<OwnedFrame<'_>> {
        let reply_waiter = self.reply_waiter(tag)?;
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut remaining = self.timeout_jiffies;
        // Set once cancellation has been declined. The guard blocks ordinary
        // signals; the loop paces the two unmaskable ones against the same
        // reply deadline.
        let mut uncancellable: Option<SendSignalMask> = None;
        let mut liveness_extensions = 0u32;
        loop {
            let mut slots = shard.lock();
            let Some(slot) = slots.get_mut(local_tag) else {
                return Err(protocol_errno());
            };

            match &slot.state {
                PendingState::Completed(_) => {
                    let reply = take_completed_reply(slot);
                    drop(slots);
                    self.release_normal_tag_if_idle(tag)?;
                    self.changed.notify_all();
                    let Some(reply) = reply else {
                        return Err(protocol_errno());
                    };
                    return self.own_completed_reply(tag, dispatch, reply);
                }
                PendingState::Failed(status) => {
                    let error = Error::from_errno(*status);
                    drop(slots);
                    self.release_slot(tag);
                    return Err(error);
                }
                PendingState::Reserved | PendingState::Sent => {}
                PendingState::Consuming | PendingState::Vacant => {
                    return Err(protocol_errno());
                }
            }

            if uncancellable.is_some() || attempt.is_resolving_ambiguity() {
                // SIGKILL and SIGSTOP cannot be masked. Pace their immediate
                // wakeups without abandoning the tag that still owns the
                // eventual reply.
                drop(slots);
                if sleep_uninterruptible_tick(&mut remaining) {
                    continue;
                }
                self.handle_reply_deadline(tag, dispatch, &mut liveness_extensions)?;
                remaining = self.timeout_jiffies;
                continue;
            }

            match reply_waiter.wait_interruptible_timeout(&mut slots, remaining) {
                CondVarTimeoutResult::Woken { jiffies } => {
                    remaining = jiffies;
                }
                CondVarTimeoutResult::Signal { jiffies } => {
                    remaining = jiffies;
                    // Recheck under the same lock: a response already
                    // published wins the signal race on the next iteration.
                    if slot_completed(&slots, local_tag) {
                        continue;
                    }
                    drop(slots);
                    self.try_cancel_on_signal(tag, dispatch, attempt, &mut uncancellable)?;
                }
                CondVarTimeoutResult::Timeout => {
                    drop(slots);
                    self.handle_reply_deadline(tag, dispatch, &mut liveness_extensions)?;
                    remaining = self.timeout_jiffies;
                }
            }
        }
    }

    /// Recheck an expired reply and either extend its deadline or retire it.
    fn handle_reply_deadline(
        &self,
        tag: usize,
        dispatch: &Dispatch,
        liveness_extensions: &mut u32,
    ) -> Result<()> {
        let (shard, local_tag) = self.slot_shard(tag)?;
        let slots = shard.lock();
        if slot_completed(&slots, local_tag) {
            return Ok(());
        }
        let request_name = slots
            .get(local_tag)
            .and_then(|slot| slot.expected)
            .map(ExpectedResponse::name)
            .unwrap_or("unknown request");
        // Other replies prove the connection is still alive, but only for a
        // finite number of windows: peer liveness is not proof that this tag
        // will ever receive its terminal response.
        if *liveness_extensions < MAX_LIVENESS_EXTENSIONS && self.heard_recently() {
            *liveness_extensions += 1;
            return Ok(());
        }
        drop(slots);
        // Tgetlineage remains available during a backend stall and verifies
        // that this peer still has serving authority.
        if *liveness_extensions < MAX_LIVENESS_EXTENSIONS && self.probe_peer(dispatch) {
            *liveness_extensions += 1;
            return Ok(());
        }
        pr_warn!(
            "zerofs: {} tag {} remained unresolved after liveness probe; retiring connection\n",
            request_name,
            tag
        );
        let error = ETIMEDOUT;
        self.retire_connection(error, dispatch.epoch);
        self.release_slot(tag);
        Err(error)
    }

    /// Run or coalesce an in-band liveness probe.
    fn probe_peer(&self, dispatch: &Dispatch) -> bool {
        if self.probe_in_flight.swap(true, Ordering::Acquire) {
            // The active probe will refresh liveness or retire the connection.
            return true;
        }
        let alive = self.exchange_probe(dispatch);
        self.probe_in_flight.store(false, Ordering::Release);
        alive
    }

    /// Run one bounded Tgetlineage round trip.
    fn exchange_probe(&self, dispatch: &Dispatch) -> bool {
        let request = Request::Tgetlineage;
        let Ok(expected) = ExpectedResponse::for_request(&request) else {
            return false;
        };
        let Ok(maximum_response) = expected.maximum_frame_size(self.msize) else {
            return false;
        };
        // Do not wait for capacity that only an incoming reply can free.
        let Ok(SlotReservation::Reserved(tag)) =
            self.reserve_slot(expected, maximum_response, None, 1, false, &[])
        else {
            return false;
        };
        let mut frame = [0u8; protocol::HEADER_SIZE];
        let Ok(encoded) = protocol::encode_request(&mut frame, self.msize, tag as u16, request)
        else {
            self.release_slot(tag);
            return false;
        };
        let Some(request_frame) = frame.get(..encoded) else {
            self.release_slot(tag);
            return false;
        };
        match self.send_frame(tag, request_frame, dispatch) {
            FrameSend::Sent => self.wait_for_probe_reply(tag, dispatch),
            FrameSend::Rejected(_) => {
                self.release_slot(tag);
                false
            }
            FrameSend::Interrupted(_) => {
                // The probe never entered the stream, so it says nothing about
                // peer health. Let the unresolved request retire the connection
                // once it has exhausted its actual liveness evidence.
                self.release_slot(tag);
                false
            }
            FrameSend::Broken(error) => {
                // The frame may be partially sent, so retire before reuse.
                self.retire_connection(error, dispatch.epoch);
                self.release_slot(tag);
                false
            }
        }
    }

    /// Wait uninterruptibly for a probe response within its timeout.
    ///
    /// A sent tag cannot be abandoned while its reply may still arrive.
    fn wait_for_probe_reply(&self, tag: usize, dispatch: &Dispatch) -> bool {
        let mut remaining = jiffies_for_ms(u64::from(PROBE_TIMEOUT_MS))
            .min(self.timeout_jiffies)
            .max(1);
        let mut liveness_extensions = 0u32;
        loop {
            let Ok((shard, local_tag)) = self.slot_shard(tag) else {
                self.terminate(protocol_errno());
                return false;
            };
            let mut slots = shard.lock();
            let Some(slot) = slots.get_mut(local_tag) else {
                drop(slots);
                self.terminate(protocol_errno());
                return false;
            };
            match &slot.state {
                PendingState::Completed(_) => {
                    let reply = take_completed_reply(slot);
                    drop(slots);
                    match self.release_normal_tag_if_idle(tag) {
                        Ok(_) => self.changed.notify_all(),
                        Err(error) => {
                            self.terminate(error);
                            return false;
                        }
                    }
                    let Some(reply) = reply else {
                        return false;
                    };
                    let verdict = validate_probe_response(
                        reply.bytes.as_slice(),
                        self.msize,
                        tag as u16,
                        reply.expected,
                    );
                    self.recycle_reply_buffer(reply.bytes);
                    return match verdict {
                        Ok(alive) => alive,
                        Err(error) => {
                            self.terminate(error);
                            false
                        }
                    };
                }
                PendingState::Failed(_) => {
                    drop(slots);
                    self.release_slot(tag);
                    return false;
                }
                PendingState::Sent => {}
                PendingState::Reserved | PendingState::Consuming | PendingState::Vacant => {
                    drop(slots);
                    self.terminate(protocol_errno());
                    return false;
                }
            }
            drop(slots);
            if !sleep_uninterruptible_tick(&mut remaining) {
                // The probe shares the server's response queue and this
                // connection's byte stream with ordinary replies. Under load
                // its Rgetlineage can therefore sit behind data responses
                // even while those responses prove the peer is alive. Keep
                // owning the sent probe tag and give it another quiet phase;
                // abandoning it would let its eventual reply hit a reused tag.
                if liveness_extensions < MAX_LIVENESS_EXTENSIONS && self.heard_recently() {
                    liveness_extensions += 1;
                    remaining = jiffies_for_ms(u64::from(PROBE_TIMEOUT_MS))
                        .min(self.timeout_jiffies)
                        .max(1);
                    continue;
                }
                pr_warn!(
                    "zerofs: Tgetlineage tag {} timed out without intervening traffic\n",
                    tag
                );
                // Retire before vacating so the receiver cannot publish here.
                self.retire_connection(ETIMEDOUT, dispatch.epoch);
                self.release_slot(tag);
                return false;
            }
        }
    }

    /// Act on a signal that interrupted a wait for `tag` while its slot was
    /// still unresolved.
    ///
    /// Runs with the session lock dropped, because cancellation takes it again.
    fn try_cancel_on_signal(
        &self,
        tag: usize,
        dispatch: &Dispatch,
        attempt: &mut OpAttempt,
        uncancellable: &mut Option<SendSignalMask>,
    ) -> Result<()> {
        // Keep the exact task mask alive until the cancellation outcome is
        // known. If the flush becomes ambiguous, ownership moves to OpAttempt
        // so reconnect and replay cannot be interrupted by the same signal.
        let signal_mask = match SendSignalMask::block() {
            Ok(signal_mask) => signal_mask,
            Err(error) => {
                // Do not leave the original Sent tag registered if signal-mask
                // setup itself fails. Retiring the stream is conservative but
                // preserves the tag and frame-ownership invariants.
                return match self.fail_flush_preserving_original(
                    tag,
                    None,
                    error,
                    dispatch.epoch,
                )? {
                    FlushOutcome::OriginalReady => Ok(()),
                    FlushOutcome::ConnectionLost => {
                        attempt.resolve_uncertain_flush(None);
                        Err(not_connected_errno())
                    }
                    // fail_flush_preserving_original cannot manufacture either
                    // of these outcomes without reserving and completing a
                    // flush, which did not happen on this path.
                    FlushOutcome::Cancelled | FlushOutcome::NotCancelled => Err(protocol_errno()),
                };
            }
        };
        match self.flush_interrupted_request(tag, dispatch.epoch)? {
            FlushOutcome::OriginalReady => Ok(()),
            // Rflush is the protocol cancellation boundary.
            // Signal disposition decides whether userspace ultimately sees
            // EINTR or the syscall is transparently restarted.
            FlushOutcome::Cancelled => Err(ERESTARTSYS),
            FlushOutcome::NotCancelled => {
                // Cancellation capacity is exhausted. Abandoning the tag here
                // would leak it and its preallocated reply buffer, because the
                // reply still arrives and still needs an owner, so complete
                // the operation instead of interrupting it.
                *uncancellable = Some(signal_mask);
                Ok(())
            }
            FlushOutcome::ConnectionLost => {
                attempt.resolve_uncertain_flush(Some(signal_mask));
                // The failure that made Tflush ambiguous may itself have been
                // EINTR (for example from an unmaskable signal during socket
                // I/O).
                Err(not_connected_errno())
            }
        }
    }
}

/// Decode and validate a probe response.
fn validate_probe_response(
    frame: &[u8],
    msize: u32,
    tag: u16,
    expected: ExpectedResponse,
) -> Result<bool> {
    let decoded = protocol::decode_response(frame, msize, tag).map_err(codec_errno)?;
    match decoded.body {
        // A valid Rlerror rejects the probe; invalid errnos are protocol errors.
        Response::Rlerror(error) if (1..=bindings::MAX_ERRNO).contains(&error.ecode) => Ok(false),
        Response::Rlerror(_) => Err(protocol_errno()),
        response if expected.matches(&response) => Ok(true),
        _ => Err(protocol_errno()),
    }
}

/// What a completed slot holds for its owner.
struct CompletedReply {
    bytes: KVVec<u8>,
    expected: ExpectedResponse,
    delivered: Option<usize>,
}

/// Vacate a completed slot and take the reply out of it.
///
/// `None` is a protocol fault: a slot in `Completed` must carry a frame and an
/// expectation. It is vacated in either case.
fn take_completed_reply(slot: &mut PendingSlot) -> Option<CompletedReply> {
    let previous = mem::replace(&mut slot.state, PendingState::Vacant);
    let expected = slot.expected;
    let delivered = slot
        .destination
        .as_ref()
        .and_then(|destination| destination.delivered);
    drop(vacate_slot(slot));
    let PendingState::Completed(bytes) = previous else {
        return None;
    };
    let expected = expected?;
    Some(CompletedReply {
        bytes,
        expected,
        delivered,
    })
}

fn slot_completed(slots: &KVVec<PendingSlot>, local_tag: usize) -> bool {
    slots
        .get(local_tag)
        .is_some_and(|slot| matches!(slot.state, PendingState::Completed(_)))
}
