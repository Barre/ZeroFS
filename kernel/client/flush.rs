use core::{mem, sync::atomic::Ordering};

use kernel::{alloc::KVVec, ffi, prelude::*, sync::CondVarTimeoutResult};

use crate::protocol::{self, HEADER_SIZE, Request, Response};

use super::errors::{
    codec_errno, is_protocol_error, protocol_errno, server_errno,
};
use super::session::{
    OrphanedTransports, Session, SessionState, SessionStatus, shutdown_transports,
};
use super::signals::{SendSignalMask, sleep_uninterruptible_tick};
use super::slots::{ExpectedResponse, PendingState};
use super::tag_space::{FIRST_NORMAL_TAG, FLUSH_SLOTS, normal_tag_index};

pub(super) enum FlushOutcome {
    /// The original reply was published before the flush barrier completed.
    OriginalReady,
    /// No cancellation tag became available. The original request is still
    /// outstanding and still owns its tag, so the caller must wait it out.
    NotCancelled,
    /// Rflush completed without an original reply, so the operation was
    /// retired and its syscall should report EINTR.
    Interrupted,
}

/// What claiming one of the cancellation tags produced.
enum ControlTag {
    /// The tag is `Reserved` and its reply credit charged to the caller.
    Reserved(usize),
    /// Nothing was reserved because the flush already has its answer.
    Decided(FlushOutcome),
}

/// The flushed request's state at the moment its Rflush was accepted.
#[derive(Clone, Copy)]
enum FlushedOriginalState {
    Completed,
    Sent,
    Failed(ffi::c_int),
    Invalid,
}

impl Session {
    /// Retire an interrupted request with a standard 9P flush barrier.
    ///
    /// Signals remain pending after the first interruptible wait returns, so
    /// the cancellation path polls in uninterruptible one-jiffy sleeps. The
    /// polling is bounded by the same per-phase timeout as normal admission
    /// and reply waits.
    pub(super) fn flush_interrupted_request(
        &self,
        oldtag: usize,
        epoch: u64,
    ) -> Result<FlushOutcome> {
        if normal_tag_index(oldtag).is_none() {
            let error = protocol_errno();
            self.terminate(error);
            return Err(error);
        }

        let _signal_mask = match SendSignalMask::block() {
            Ok(mask) => mask,
            Err(error) => {
                return self.fail_flush_preserving_original(oldtag, None, error, epoch);
            }
        };

        let maximum_frame = match ExpectedResponse::Flush.maximum_frame_size(self.msize) {
            Ok(size) => size,
            Err(error) => {
                return self.fail_flush_preserving_original(oldtag, None, error, epoch);
            }
        };

        let flush_tag = match self.reserve_flush_tag(oldtag, maximum_frame, epoch)? {
            ControlTag::Reserved(tag) => tag,
            ControlTag::Decided(outcome) => return Ok(outcome),
        };

        if let Err(error) = self.send_flush(oldtag, flush_tag, epoch) {
            return self.fail_flush_preserving_original(oldtag, Some(flush_tag), error, epoch);
        }

        self.wait_for_flush_reply(oldtag, flush_tag, epoch)
    }

    /// Claim a cancellation tag for a Tflush naming `oldtag`.
    ///
    /// The control tags are outside the configured normal slot range, so a
    /// completely occupied request table cannot deadlock cancellation. The
    /// caller's `SendSignalMask` already masked this task's blockable signals,
    /// so the interruptible wait below sleeps instead of returning immediately
    /// on the signal that brought us here.
    fn reserve_flush_tag(
        &self,
        oldtag: usize,
        maximum_frame: usize,
        epoch: u64,
    ) -> Result<ControlTag> {
        let mut remaining = self.timeout_jiffies;
        loop {
            let mut state = self.state.lock();
            if let Some(error) = self.flush_band_lost(&state, epoch) {
                drop(state);
                self.release_slot(oldtag);
                return Err(error);
            }
            match self.observed_slot_state(oldtag)? {
                FlushedOriginalState::Completed => {
                    return Ok(ControlTag::Decided(FlushOutcome::OriginalReady));
                }
                FlushedOriginalState::Sent => {}
                FlushedOriginalState::Failed(_) | FlushedOriginalState::Invalid => {
                    drop(state);
                    let error = protocol_errno();
                    return self
                        .fail_flush_preserving_original(oldtag, None, error, epoch)
                        .map(ControlTag::Decided);
                }
            }

            for candidate in 0..FLUSH_SLOTS {
                let (shard, local_tag) = self.slot_shard(candidate)?;
                let mut slots = shard.lock();
                let Some(slot) = slots.get_mut(local_tag) else {
                    return Err(protocol_errno());
                };
                if !matches!(slot.state, PendingState::Vacant) {
                    continue;
                }
                slot.state = PendingState::Reserved;
                slot.expected = Some(ExpectedResponse::Flush);
                slot.maximum_frame = maximum_frame;
                state.used_reply_credit = state.used_reply_credit.saturating_add(maximum_frame);
                return Ok(ControlTag::Reserved(candidate));
            }

            match self
                .changed
                .wait_interruptible_timeout(&mut state, remaining)
            {
                CondVarTimeoutResult::Woken { jiffies } => remaining = jiffies,
                CondVarTimeoutResult::Signal { .. } => remaining = 0,
                CondVarTimeoutResult::Timeout => remaining = 0,
            }
            if remaining == 0 {
                // Every control tag is busy cancelling someone else. Declining
                // to cancel is not a protocol fault: the original request is
                // still outstanding under its own tag and its reply will still
                // arrive, so report that rather than tearing down the session.
                return Ok(ControlTag::Decided(FlushOutcome::NotCancelled));
            }
        }
    }

    fn send_flush(&self, oldtag: usize, flush_tag: usize, epoch: u64) -> Result<()> {
        let mut request_frame = [0u8; HEADER_SIZE + mem::size_of::<u16>()];
        let encoded = protocol::encode_request(
            &mut request_frame,
            self.msize,
            flush_tag as u16,
            Request::Tflush {
                oldtag: oldtag as u16,
            },
        );
        if !matches!(encoded, Ok(size) if size == request_frame.len()) {
            return Err(protocol_errno());
        }

        // A Tflush names a tag on one connection, so mark_sent's epoch check is
        // what stops it from being transmitted on a replacement.
        let _guard = self.send_lock.lock();
        self.mark_sent(flush_tag, epoch)?;
        let transport = self.state.lock().transport.clone();
        transport.send_all(&request_frame)
    }

    fn wait_for_flush_reply(
        &self,
        oldtag: usize,
        flush_tag: usize,
        epoch: u64,
    ) -> Result<FlushOutcome> {
        let mut remaining = self.timeout_jiffies;
        loop {
            let state = self.state.lock();
            let original_ready = matches!(
                self.observed_slot_state(oldtag)?,
                FlushedOriginalState::Completed
            );

            if let Some(error) = self.flush_band_lost(&state, epoch) {
                drop(state);
                self.release_slot(flush_tag);
                if original_ready {
                    return Ok(FlushOutcome::OriginalReady);
                }
                self.release_slot(oldtag);
                return Err(error);
            }

            match self.observed_slot_state(flush_tag)? {
                FlushedOriginalState::Completed => {
                    // Keep the flush slot in Consuming while validating, then
                    // retire it and the old request atomically. The consuming
                    // slot is also the barrier that keeps the receiver parked.
                    let frame = self.take_flush_frame(flush_tag)?;
                    drop(state);
                    let Some(bytes) = frame else {
                        let error = protocol_errno();
                        return self.fail_flush_preserving_original(
                            oldtag,
                            Some(flush_tag),
                            error,
                            epoch,
                        );
                    };

                    if let Err(error) = validate_flush_response_frame(
                        bytes.as_slice(),
                        self.msize,
                        flush_tag as u16,
                    ) {
                        return self.fail_flush_preserving_original(
                            oldtag,
                            Some(flush_tag),
                            error,
                            epoch,
                        );
                    }

                    let outcome = self.finish_successful_flush(oldtag, flush_tag);
                    drop(bytes);
                    return outcome;
                }
                FlushedOriginalState::Sent => {
                    drop(state);
                }
                FlushedOriginalState::Failed(_) | FlushedOriginalState::Invalid => {
                    drop(state);
                    let error = protocol_errno();
                    return self.fail_flush_preserving_original(
                        oldtag,
                        Some(flush_tag),
                        error,
                        epoch,
                    );
                }
            }

            if !sleep_uninterruptible_tick(&mut remaining) {
                let error = ETIMEDOUT;
                return self.fail_flush_preserving_original(oldtag, Some(flush_tag), error, epoch);
            }
        }
    }

    /// Close an unresolved flush transaction without discarding a reply that
    /// the receiver already published for the original request.
    ///
    /// `owned_flush_tag` is `None` when failure occurred before a cancellation
    /// tag was reserved. Once this caller owns one, both tags are retired here
    /// unless the original response is already complete. The connection is
    /// always retired because a failed flush leaves tag reuse unsafe; only a
    /// protocol fault ends the session.
    fn fail_flush_preserving_original(
        &self,
        oldtag: usize,
        owned_flush_tag: Option<usize>,
        error: Error,
        epoch: u64,
    ) -> Result<FlushOutcome> {
        let terminal = is_protocol_error(error);
        let mut state = self.state.lock();
        let original_ready = matches!(
            self.observed_slot_state(oldtag)?,
            FlushedOriginalState::Completed
        );
        let transitioned = if terminal {
            self.retire_locked(&mut state, Some(error.to_errno()))
        } else if state.connection_epoch == epoch {
            self.retire_locked(&mut state, None)
        } else {
            // The connection this flush belonged to is already gone; retiring
            // its replacement would turn one cancelled syscall into a reconnect.
            false
        };
        // Read back the durable status after retire_locked() has normalized any
        // kernel-private ERESTART* value to ENOTCONN.
        let outcome_error = match state.status {
            SessionStatus::Dead(status) => Error::from_errno(status),
            // The flushed request died with its connection, and this caller was
            // already asking to cancel it.
            _ => EINTR,
        };

        let orphaned = self.vacate_flush_tags_locked(
            &mut state,
            oldtag,
            owned_flush_tag,
            original_ready,
            transitioned,
        );
        drop(state);
        self.publish_flush_release(orphaned);

        if original_ready {
            Ok(FlushOutcome::OriginalReady)
        } else {
            Err(outcome_error)
        }
    }

    fn finish_successful_flush(&self, oldtag: usize, flush_tag: usize) -> Result<FlushOutcome> {
        let mut state = self.state.lock();
        let flush_consuming = self.slot_consuming(flush_tag)?;
        let original_state = self.observed_slot_state(oldtag)?;

        let invalid = !flush_consuming
            || matches!(original_state, FlushedOriginalState::Invalid)
            || (matches!(original_state, FlushedOriginalState::Sent)
                && self.sent_count.load(Ordering::Relaxed) == 0);
        if invalid {
            let error = protocol_errno();
            let terminated = self.retire_locked(&mut state, Some(error.to_errno()));
            let original_ready = matches!(original_state, FlushedOriginalState::Completed);
            let orphaned = self.vacate_flush_tags_locked(
                &mut state,
                oldtag,
                Some(flush_tag),
                original_ready,
                terminated,
            );
            drop(state);
            self.publish_flush_release(orphaned);
            return if original_ready {
                Ok(FlushOutcome::OriginalReady)
            } else {
                Err(error)
            };
        }

        let flush_credit = self.vacate_tag_locked(&mut state, flush_tag)?;
        let (result, old_credit) = match original_state {
            FlushedOriginalState::Completed => (Ok(FlushOutcome::OriginalReady), 0),
            FlushedOriginalState::Sent => {
                let credit = self.vacate_tag_locked(&mut state, oldtag)?;
                self.decrement_sent_count();
                (Ok(FlushOutcome::Interrupted), credit)
            }
            FlushedOriginalState::Failed(status) => {
                let credit = self.vacate_tag_locked(&mut state, oldtag)?;
                (Err(Error::from_errno(status)), credit)
            }
            // Handled by the invariant-failure branch above.
            FlushedOriginalState::Invalid => (Err(protocol_errno()), 0),
        };
        state.used_reply_credit = state
            .used_reply_credit
            .saturating_sub(flush_credit)
            .saturating_sub(old_credit);
        drop(state);
        self.changed.notify_all();
        result
    }

    /// Publish the slot release `vacate_flush_tags_locked` performed, once the
    /// state lock is dropped.
    ///
    /// `orphaned` is `Some` only when this caller's retire transitioned the
    /// session. That transition has to reach everyone parked on the status and
    /// the sockets it left behind have to be shut down; a caller that only gave
    /// its tags back just wakes whoever those tags were blocking.
    fn publish_flush_release(&self, orphaned: Option<OrphanedTransports>) {
        if let Some(transports) = orphaned {
            self.notify_status_change();
            shutdown_transports(transports);
        } else {
            self.changed.notify_all();
            self.wake_all_reply_waiters();
        }
    }

    /// Whether a control tag holds an Rflush no owner has consumed yet.
    ///
    /// That reply is a stream-consumption barrier: the interrupted caller has
    /// to decide its request's outcome before anyone reads the bytes behind it,
    /// so a receiver stays idle and a reconnect declines to publish while one
    /// is outstanding.
    pub(super) fn flush_reply_pending(&self) -> bool {
        (0..FIRST_NORMAL_TAG).any(|tag| {
            let Ok((shard, local_tag)) = self.slot_shard(tag) else {
                return true;
            };
            shard.lock().get(local_tag).is_some_and(|slot| {
                matches!(
                    slot.state,
                    PendingState::Completed(_) | PendingState::Consuming
                )
            })
        })
    }

    /// The error a Tflush band must report, if its connection is gone.
    fn flush_band_lost(&self, state: &SessionState, epoch: u64) -> Option<Error> {
        if let SessionStatus::Dead(status) = state.status {
            return Some(Error::from_errno(status));
        }
        // A Tflush names a tag on one connection. Once that connection is
        // retired the flushed request died with it, and this caller was already
        // asking to cancel it.
        (state.connection_epoch != epoch).then_some(EINTR)
    }

    fn slot_consuming(&self, tag: usize) -> Result<bool> {
        let (shard, local_tag) = self.slot_shard(tag)?;
        Ok(shard
            .lock()
            .get(local_tag)
            .is_some_and(|slot| matches!(slot.state, PendingState::Consuming)))
    }

    /// Snapshot one tag once so `Sent -> Completed` cannot be misread as
    /// neither state by two separate shard acquisitions.
    fn observed_slot_state(&self, tag: usize) -> Result<FlushedOriginalState> {
        let (shard, local_tag) = self.slot_shard(tag)?;
        Ok(shard
            .lock()
            .get(local_tag)
            .map(|slot| match &slot.state {
                PendingState::Completed(_) => FlushedOriginalState::Completed,
                PendingState::Sent => FlushedOriginalState::Sent,
                PendingState::Failed(status) => FlushedOriginalState::Failed(*status),
                _ => FlushedOriginalState::Invalid,
            })
            .unwrap_or(FlushedOriginalState::Invalid))
    }

    /// Keep a completed Rflush as a stream barrier while its frame is checked.
    fn take_flush_frame(&self, tag: usize) -> Result<Option<KVVec<u8>>> {
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut slots = shard.lock();
        let slot = slots.get_mut(local_tag).ok_or_else(protocol_errno)?;
        Ok(
            match mem::replace(&mut slot.state, PendingState::Consuming) {
                PendingState::Completed(bytes) => Some(bytes),
                _ => None,
            },
        )
    }

    /// Give back this flush transaction's tags and refund their credit.
    fn vacate_flush_tags_locked(
        &self,
        state: &mut SessionState,
        oldtag: usize,
        owned_flush_tag: Option<usize>,
        original_ready: bool,
        transitioned: bool,
    ) -> Option<OrphanedTransports> {
        let flush_credit = owned_flush_tag
            .and_then(|tag| self.vacate_tag_locked(state, tag).ok())
            .unwrap_or(0);
        let old_credit = if original_ready {
            0
        } else {
            self.vacate_tag_locked(state, oldtag).unwrap_or(0)
        };
        state.used_reply_credit = state
            .used_reply_credit
            .saturating_sub(flush_credit)
            .saturating_sub(old_credit);
        transitioned.then(|| (state.transport.clone(), state.candidate.clone()))
    }
}

fn validate_flush_response_frame(frame: &[u8], msize: u32, tag: u16) -> Result<()> {
    let decoded = protocol::decode_response(frame, msize, tag).map_err(codec_errno)?;
    match decoded.body {
        Response::Rflush => Ok(()),
        Response::Rlerror(error) => Err(server_errno(error.ecode)),
        _ => Err(protocol_errno()),
    }
}
