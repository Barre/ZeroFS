use core::mem;

use kernel::{
    alloc::KVVec,
    prelude::*,
    sync::{CondVar, CondVarTimeoutResult},
};

use super::errors::protocol_errno;
use super::flush::FlushOutcome;
use super::receive::validate_response_frame;
use super::retry::AttemptOutcome;
use super::session::{Dispatch, Session};
use super::signals::{SendSignalMask, sleep_uninterruptible_tick};
use super::slots::{ExpectedResponse, PendingSlot, PendingState, vacate_slot};

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
    pub(super) _credit: ReplyCredit<'a>,
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

/// Keeps conservative reply-memory accounting charged while an owned frame is
/// queued, decoded, copied to userspace, or consumed by a directory actor.
pub(super) enum ReplyCredit<'a> {
    Multiplexed(SessionReplyCredit<'a>),
}

pub(super) struct SessionReplyCredit<'a> {
    session: &'a Session,
    bytes: usize,
}

impl Drop for SessionReplyCredit<'_> {
    fn drop(&mut self) {
        self.session.release_reply_credit(self.bytes);
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

    /// Hand a taken reply to its caller, still charged for its reply credit.
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
            _credit: ReplyCredit::Multiplexed(SessionReplyCredit {
                session: self,
                bytes: reply.reply_credit,
            }),
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

    pub(super) fn wait_for_reply(&self, tag: usize, dispatch: &Dispatch) -> Result<OwnedFrame<'_>> {
        let reply_waiter = self.reply_waiter(tag)?;
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut remaining = self.timeout_jiffies;
        // Set once cancellation has been declined. The guard blocks ordinary
        // signals; the loop paces the two unmaskable ones against the same
        // reply deadline.
        let mut uncancellable: Option<SendSignalMask> = None;
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

            if uncancellable.is_some() {
                // SIGKILL and SIGSTOP cannot be masked. Pace their immediate
                // wakeups without abandoning the tag that still owns the
                // eventual reply.
                drop(slots);
                if sleep_uninterruptible_tick(&mut remaining) {
                    continue;
                }
                self.handle_reply_deadline(tag, dispatch)?;
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
                    self.try_cancel_on_signal(tag, dispatch, &mut uncancellable)?;
                }
                CondVarTimeoutResult::Timeout => {
                    drop(slots);
                    self.handle_reply_deadline(tag, dispatch)?;
                    remaining = self.timeout_jiffies;
                }
            }
        }
    }

    /// Recheck an expired reply and either extend its deadline or retire it.
    fn handle_reply_deadline(&self, tag: usize, dispatch: &Dispatch) -> Result<()> {
        let (shard, local_tag) = self.slot_shard(tag)?;
        let slots = shard.lock();
        if slot_completed(&slots, local_tag) {
            return Ok(());
        }
        // A slow request is not a dead connection. As long as other replies
        // keep proving the peer alive, let server-side backpressure reach this
        // waiter instead of forcing an unnecessary reconnect. A silent peer
        // still expires after one full window without a decoded frame.
        if self.heard_recently() {
            return Ok(());
        }
        drop(slots);
        let error = ETIMEDOUT;
        self.retire_connection(error, dispatch.epoch);
        self.release_slot(tag);
        Err(error)
    }

    /// Act on a signal that interrupted a wait for `tag` while its slot was
    /// still unresolved.
    ///
    /// Runs with the session lock dropped, because cancellation takes it again.
    fn try_cancel_on_signal(
        &self,
        tag: usize,
        dispatch: &Dispatch,
        uncancellable: &mut Option<SendSignalMask>,
    ) -> Result<()> {
        match self.flush_interrupted_request(tag, dispatch.epoch)? {
            FlushOutcome::OriginalReady => Ok(()),
            FlushOutcome::Interrupted => Err(EINTR),
            FlushOutcome::NotCancelled => {
                // Cancellation capacity is exhausted. Abandoning the tag here
                // would leak it and its reply credit, because the reply still
                // arrives and still needs an owner, so complete the operation
                // instead of interrupting it.
                *uncancellable = Some(SendSignalMask::block()?);
                Ok(())
            }
        }
    }
}

/// What a completed slot holds for its owner.
struct CompletedReply {
    bytes: KVVec<u8>,
    expected: ExpectedResponse,
    /// Reply credit the slot was charged, now owed by the owned frame.
    reply_credit: usize,
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
    let reply_credit = vacate_slot(slot);
    let PendingState::Completed(bytes) = previous else {
        return None;
    };
    let expected = expected?;
    Some(CompletedReply {
        bytes,
        expected,
        reply_credit,
        delivered,
    })
}

fn slot_completed(slots: &KVVec<PendingSlot>, local_tag: usize) -> bool {
    slots
        .get(local_tag)
        .is_some_and(|slot| matches!(slot.state, PendingState::Completed(_)))
}
