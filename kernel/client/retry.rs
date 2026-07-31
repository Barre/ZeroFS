use kernel::{
    alloc::{KVVec, flags::GFP_KERNEL},
    bindings,
    prelude::*,
};

use crate::{
    protocol::{self, DecodedResponse, MutationEnvelope, Request, Response},
    transport::PayloadIter,
};

use super::errors::{
    codec_errno, codec_errno_without_disconnect, is_interrupted_error, is_protocol_error,
    message_size_errno, not_connected_errno, protocol_errno, server_errno,
};
use super::ops::ReplyDestinationRegistration;
use super::receive::rlerror_code;
use super::registry::request_fids;
use super::reply::OwnedFrame;
use super::session::{Dispatch, Session};
use super::slots::{DestinationGuard, ExpectedResponse, FrameSend, SlotDestination};
use super::{
    Client, MUTATION_RETRY_HORIZON_MS, STACK_REQUEST_BYTES, elapsed_ms, jiffies_for_ms,
    monotonic_ns,
};

impl Client {
    /// Run one transaction for a request the server treats as idempotent.
    ///
    /// The builder takes no envelope because these request types have no field
    /// to put one in: losing and resending them is never ambiguous, so they
    /// need no operation ID and no retry horizon. Sending a mutation this way
    /// would strip it of the identity its dedup entry is keyed on.
    pub(super) fn transact<'a, 'r>(
        &'a self,
        build: impl Fn() -> Request<'r>,
    ) -> Result<OwnedFrame<'a>> {
        self.resend_loop(&mut OpAttempt::new(false), |_| build(), None)
    }

    /// Run one transaction whose resends must carry a stable operation ID.
    pub(super) fn transact_mutation<'a, 'r>(
        &'a self,
        build: impl Fn(MutationEnvelope) -> Request<'r>,
    ) -> Result<OwnedFrame<'a>> {
        self.resend_loop(&mut OpAttempt::new(true), build, None)
    }

    /// Run one idempotent transaction, claiming its tag for direct delivery.
    pub(super) fn transact_registered<'a, 'r>(
        &'a self,
        build: impl Fn() -> Request<'r>,
        destination: Option<ReplyDestinationRegistration<'_>>,
    ) -> Result<OwnedFrame<'a>> {
        self.resend_loop(
            &mut OpAttempt::new(false),
            |_| build(),
            destination.as_ref(),
        )
    }

    /// Wait for a connection this attempt may dispatch on.
    ///
    /// The wait is bounded by whichever of the retry horizon and the reconnect
    /// grace runs out first, so an operation cannot sleep through its horizon
    /// and then put a frame on the wire the server can no longer recognise as
    /// a retry. `None` means the connection went away again between the gate
    /// and the snapshot, which is one more pass rather than an error.
    fn await_resend_bounded(&self, attempt: &OpAttempt) -> Result<Dispatch> {
        let session = self.session();
        let budget = attempt.resend_budget(session.grace_ms)?;
        match session.dispatch_or_wait(budget) {
            Ok(dispatch) => Ok(dispatch),
            Err(error) => {
                // The budget may have been the horizon rather than the grace, in
                // which case the honest report is an ambiguous disconnect. Only a
                // timeout is reclassified: a signal must still surface as one, and
                // a terminated session keeps its own cause.
                if error == ETIMEDOUT {
                    attempt.resend_budget(session.grace_ms)?;
                }
                Err(error)
            }
        }
    }

    /// Run `send` against one connection after another until it produces the
    /// caller's reply.
    ///
    /// The envelope is minted here rather than inside `send` because the same
    /// one describes the frame that goes out and decides how its outcome is
    /// classified. An error out of `send` reaches the caller unchanged, so
    /// `send` may only fail while nothing has been transmitted; anything past
    /// that has to come back as an `AttemptOutcome` for classification.
    fn drive_attempts<'a>(
        &'a self,
        attempt: &mut OpAttempt,
        send: impl Fn(&Dispatch, MutationEnvelope, &mut OpAttempt) -> Result<AttemptOutcome<'a>>,
    ) -> Result<OwnedFrame<'a>> {
        loop {
            let dispatch = self.await_resend_bounded(attempt)?;
            let envelope = attempt.envelope_for(dispatch.writer_epoch);
            let outcome = send(&dispatch, envelope, attempt)?;
            match self.classify_attempt(attempt, &dispatch, envelope, outcome)? {
                Some(frame) => return Ok(frame),
                None => continue,
            }
        }
    }

    /// Dispatch one operation, resending it across connection replacements.
    ///
    /// The request is rebuilt for every attempt because the codec consumes a
    /// `Request` by value, and the envelope comes from `attempt` rather than
    /// being captured, so a resend is marked as a retry of the same operation
    /// ID instead of repeating the first frame's flags.
    pub(super) fn resend_loop<'a, 'r>(
        &'a self,
        attempt: &mut OpAttempt,
        build: impl Fn(MutationEnvelope) -> Request<'r>,
        destination: Option<&ReplyDestinationRegistration<'_>>,
    ) -> Result<OwnedFrame<'a>> {
        let session = self.session();
        self.drive_attempts(attempt, |dispatch, envelope, attempt| {
            let request = build(envelope);
            // Recomputed after the gate so the horizon is rechecked with
            // nothing on the wire, and so the admission wait below cannot
            // carry this frame past it.
            let admission = attempt.resend_budget(session.grace_ms)?;
            Ok(self.run_transaction(dispatch, request, destination, admission, attempt, envelope))
        })
    }

    /// Turn one attempt's outcome into a reply, a resend, or a caller error.
    fn classify_attempt<'a>(
        &self,
        attempt: &mut OpAttempt,
        dispatch: &Dispatch,
        envelope: MutationEnvelope,
        outcome: AttemptOutcome<'a>,
    ) -> Result<Option<OwnedFrame<'a>>> {
        let session = self.session();
        match outcome {
            // Nothing reached the wire, so nothing about this operation is
            // ambiguous and the connection is not implicated.
            AttemptOutcome::Rejected(error) => Err(error),
            AttemptOutcome::Retry(error) => {
                // A tag that is not the one this attempt reserved is a local
                // bookkeeping fault, not a lost connection, and resending would
                // reproduce it forever.
                if is_protocol_error(error) {
                    session.terminate(error);
                    return Err(error);
                }
                Ok(None)
            }
            AttemptOutcome::Failed(error) => {
                if is_interrupted_error(error) {
                    return Err(error);
                }
                if is_protocol_error(error) {
                    session.terminate(error);
                    return Err(error);
                }
                session.retire_connection(error, dispatch.epoch);
                Ok(None)
            }
            AttemptOutcome::Reply(frame) => match self.reroute_code(&frame) {
                // A lost-leadership reply is not the caller's answer: retire
                // and resend. Plain ENOTLEADER is post-dispatch and ambiguous,
                // so the resend keeps its operation ID and its retry marking.
                Some(code) => {
                    if code == protocol::P9_ENOTLEADER_CLEAN {
                        // The server proved this frame was rejected before
                        // dispatch, so the next one must not claim to retry
                        // something the new leader never saw.
                        attempt.proven_predispatch(envelope);
                    }
                    drop(frame);
                    session.retire_connection(not_connected_errno(), dispatch.epoch);
                    Ok(None)
                }
                // wait_for_reply already stamped this connection's lineage on
                // the frame.
                None => Ok(Some(frame)),
            },
        }
    }

    /// The lost-leadership code in `frame`, if it carries one.
    fn reroute_code(&self, frame: &OwnedFrame<'_>) -> Option<u32> {
        if frame.delivered.is_some() {
            return None;
        }
        match rlerror_code(frame.bytes.as_slice()) {
            Some(code)
                if matches!(
                    code,
                    protocol::P9_ENOTLEADER | protocol::P9_ENOTLEADER_CLEAN
                ) =>
            {
                Some(code)
            }
            _ => None,
        }
    }

    /// Write one payload directly off its owner's iterator, resending it from a
    /// fresh snapshot on every attempt.
    pub(super) fn transact_write<'a>(
        &'a self,
        wire_fid: u32,
        offset: u64,
        payload: &PayloadIter<'_>,
    ) -> Result<OwnedFrame<'a>> {
        let session = self.session();
        let length = payload.len();
        let request_size = protocol::TWRITE_OVERHEAD
            .checked_add(length)
            .ok_or_else(message_size_errno)?;
        if request_size > self.negotiated_msize() as usize {
            return Err(message_size_errno());
        }
        let expected = ExpectedResponse::Write { maximum: length };
        let write = WritePlan {
            wire_fid,
            offset,
            payload,
            expected,
            maximum_response: expected.maximum_frame_size(self.negotiated_msize())?,
        };

        let mut attempt = OpAttempt::new(true);
        self.drive_attempts(&mut attempt, |dispatch, envelope, attempt| {
            session.validate_fid(wire_fid)?;

            // Recomputed after the gate so the horizon is rechecked with
            // nothing on the wire, and so the admission wait below cannot
            // carry this frame past it.
            let admission = attempt.resend_budget(session.grace_ms)?;
            Ok(self.run_write_transaction(dispatch, &write, envelope, admission, attempt))
        })
    }

    /// Reserve a tag for one `Twrite`, transmit its prefix ahead of the payload
    /// and wait out the reply.
    ///
    /// The tag claims no slot destination: an `Rwrite` carries only a count, so
    /// no part of this reply has to land in a caller-owned iterator.
    fn run_write_transaction<'a>(
        &'a self,
        dispatch: &Dispatch,
        write: &WritePlan<'_>,
        envelope: MutationEnvelope,
        admission_jiffies: usize,
        attempt: &mut OpAttempt,
    ) -> AttemptOutcome<'a> {
        let session = self.session();
        let length = write.payload.len();
        let reserved = session.reserve_slot(
            write.expected,
            write.maximum_response,
            None,
            admission_jiffies,
            &[write.wire_fid],
        );
        let tag = match reserved {
            Ok(tag) => tag,
            Err(error) => return AttemptOutcome::Rejected(error),
        };
        let mut request_prefix = [0u8; protocol::TWRITE_OVERHEAD];
        let encoded = protocol::encode_twrite_prefix(
            &mut request_prefix,
            self.negotiated_msize(),
            tag as u16,
            envelope,
            write.wire_fid,
            write.offset,
            length,
        );
        if let Err(error) = check_encoded(encoded, request_prefix.len()) {
            session.release_slot(tag);
            return AttemptOutcome::Rejected(error);
        }

        // Every attempt sends from an unconsumed snapshot: sock_sendmsg only
        // advances the msghdr's owned copy, so the payload's own cursor and the
        // subrequest iterator behind it are both untouched by a failed send.
        let sent = SendOutcome::from_send(
            session,
            tag,
            session.send_frame_with_payload(
                tag,
                &request_prefix,
                write.payload.snapshot(),
                dispatch,
            ),
        );
        self.resolve_send(tag, dispatch, attempt, envelope, sent)
    }

    /// The reply expectation, encoded request size and reply credit `request`
    /// needs.
    ///
    /// Every failure here predates the reservation, so each one is a rejection
    /// that leaves nothing on the wire and nothing to release.
    fn plan_request(&self, request: &Request<'_>) -> Result<(ExpectedResponse, usize, usize)> {
        let expected = ExpectedResponse::for_request(request)?;
        let request_size =
            protocol::encoded_request_size(request).map_err(codec_errno_without_disconnect)?;
        if request_size > self.negotiated_msize() as usize {
            return Err(message_size_errno());
        }
        let maximum_response = expected.maximum_frame_size(self.negotiated_msize())?;
        Ok((expected, request_size, maximum_response))
    }

    /// Reserve a tag for `request`, transmit it on `dispatch` and wait out its
    /// reply.
    fn run_transaction<'a>(
        &'a self,
        dispatch: &Dispatch,
        request: Request<'_>,
        destination: Option<&ReplyDestinationRegistration<'_>>,
        admission_jiffies: usize,
        attempt: &mut OpAttempt,
        envelope: MutationEnvelope,
    ) -> AttemptOutcome<'a> {
        let session = self.session();
        let (expected, request_size, maximum_response) = match self.plan_request(&request) {
            Ok(plan) => plan,
            Err(error) => return AttemptOutcome::Rejected(error),
        };

        let registered = destination.is_some();
        let destination = destination.map(SlotDestination::from_registration);
        let (fids, fid_count) = request_fids(&request);
        let reserved = session.reserve_slot(
            expected,
            maximum_response,
            destination,
            admission_jiffies,
            &fids[..fid_count],
        );
        let tag = match reserved {
            Ok(tag) => tag,
            Err(error) => return AttemptOutcome::Rejected(error),
        };
        // Every later return, including both of resolve_send's reply waits,
        // drops this before the frame reaches the caller, so deregistration
        // always precedes the caller touching its iterator again.
        let _destination = registered.then(|| DestinationGuard { session, tag });
        let sent = self.send_request(session, tag, request, request_size, dispatch);
        self.resolve_send(tag, dispatch, attempt, envelope, sent)
    }

    /// Encode one request and transmit it under `tag`.
    ///
    /// Requests up to `STACK_REQUEST_BYTES` skip the allocator. The synchronous
    /// encode and send finish before this stack buffer goes out of scope.
    #[inline(never)]
    fn send_request(
        &self,
        session: &Session,
        tag: usize,
        request: Request<'_>,
        request_size: usize,
        dispatch: &Dispatch,
    ) -> SendOutcome {
        if request_size <= STACK_REQUEST_BYTES {
            let mut request_frame = [0u8; STACK_REQUEST_BYTES];
            let Some(destination) = request_frame.get_mut(..request_size) else {
                session.release_slot(tag);
                return SendOutcome::Released(protocol_errno());
            };
            self.encode_and_send(session, tag, destination, request, dispatch)
        } else {
            let mut request_frame = match KVVec::from_elem(0u8, request_size, GFP_KERNEL) {
                Ok(frame) => frame,
                Err(error) => {
                    session.release_slot(tag);
                    return SendOutcome::Released(error.into());
                }
            };
            self.encode_and_send(
                session,
                tag,
                request_frame.as_mut_slice(),
                request,
                dispatch,
            )
        }
    }

    fn encode_and_send(
        &self,
        session: &Session,
        tag: usize,
        destination: &mut [u8],
        request: Request<'_>,
        dispatch: &Dispatch,
    ) -> SendOutcome {
        let encoded =
            protocol::encode_request(destination, self.negotiated_msize(), tag as u16, request);
        if let Err(error) = check_encoded(encoded, destination.len()) {
            session.release_slot(tag);
            return SendOutcome::Released(error);
        }

        SendOutcome::from_send(session, tag, session.send_frame(tag, destination, dispatch))
    }

    /// Turn one transmission into an attempt outcome, waiting out the reply
    /// when a frame reached the stream.
    ///
    /// `attempt` is marked as dispatched at the send and not once the reply
    /// wait ends: the retry horizon bounds the age of the frame that created
    /// the ambiguity, and the wait that follows is bounded only by liveness.
    fn resolve_send<'a>(
        &'a self,
        tag: usize,
        dispatch: &Dispatch,
        attempt: &mut OpAttempt,
        envelope: MutationEnvelope,
        sent: SendOutcome,
    ) -> AttemptOutcome<'a> {
        let session = self.session();
        match sent {
            SendOutcome::Sent => {
                attempt.note_dispatched(envelope);
                session.reply_outcome(session.wait_for_reply(tag, dispatch))
            }
            SendOutcome::Released(error) => AttemptOutcome::Rejected(error),
            SendOutcome::Stale(error) => AttemptOutcome::Retry(error),
            SendOutcome::Failed(error) => {
                // A broken send may still have put a prefix on the wire.
                attempt.note_dispatched(envelope);
                session.retire_connection(error, dispatch.epoch);
                session.reply_outcome(session.wait_for_reply(tag, dispatch))
            }
        }
    }

    pub(super) fn decode<'a>(&self, frame: &'a OwnedFrame<'_>) -> Result<DecodedResponse<'a>> {
        let decoded =
            protocol::decode_response(frame.bytes.as_slice(), self.negotiated_msize(), frame.tag)
                .map_err(codec_errno)
                .inspect_err(|error| self.session().terminate(*error))?;
        // The resend loop already consumed every lost-leadership reply, so an
        // Rlerror here is the server's answer to the caller.
        if let Response::Rlerror(response) = &decoded.body {
            return Err(server_errno(response.ecode));
        }
        Ok(decoded)
    }

    pub(super) fn invariant_failure<T>(&self) -> Result<T> {
        let error = protocol_errno();
        self.session().terminate(error);
        Err(error)
    }
}

/// The parts of one `Twrite` that every attempt reuses unchanged.
///
/// The payload is held by reference rather than snapshotted once, because each
/// attempt must take its own snapshot off the owner's untouched iterator.
struct WritePlan<'p> {
    wire_fid: u32,
    offset: u64,
    payload: &'p PayloadIter<'p>,
    expected: ExpectedResponse,
    maximum_response: usize,
}

/// Outcome of one dispatch attempt inside a resend loop.
pub(super) enum AttemptOutcome<'a> {
    /// A complete reply from the connection this attempt targeted.
    Reply(OwnedFrame<'a>),
    /// Nothing reached the wire; report this error to the caller unchanged.
    Rejected(Error),
    /// Nothing reached the wire and the target connection is gone.
    Retry(Error),
    /// A frame may have reached the wire and the transaction failed.
    Failed(Error),
}

/// FIRST/RETRY state for one public operation.
///
/// The operation ID is minted once and held across every resend: the server's
/// dedup map admits a retry only for an ID it has already seen, so a fresh ID
/// per attempt would make a resend double-apply.
pub(super) struct OpAttempt {
    op_id: [u8; protocol::OP_ID_SIZE],
    /// Writer epoch of the first dispatched frame. `Some` marks later frames as
    /// retries and stays stable across a writer change.
    origin_epoch: Option<u64>,
    /// Monotonic nanoseconds at the first dispatched frame.
    started_ns: Option<u64>,
    /// Monotonic nanoseconds when this operation entered its resend loop.
    entered_ns: u64,
}

impl OpAttempt {
    pub(super) fn new(mutating: bool) -> Self {
        // generate_random_uuid() writes a fixed UUID_SIZE bytes, so the buffer
        // the wire format sizes must match it.
        const _: () = assert!(protocol::OP_ID_SIZE == 16);
        let mut op_id = [0u8; protocol::OP_ID_SIZE];
        if mutating {
            // SAFETY: `op_id` is a writable 16-byte buffer, exactly the
            // contract required by generate_random_uuid(). A generated v4 UUID
            // is nonzero.
            unsafe {
                bindings::generate_random_uuid(op_id.as_mut_ptr());
            }
        }
        Self {
            op_id,
            origin_epoch: None,
            started_ns: None,
            entered_ns: monotonic_ns(),
        }
    }

    fn has_op_id(&self) -> bool {
        self.op_id != [0u8; protocol::OP_ID_SIZE]
    }

    /// The envelope for the next frame on a connection with `writer_epoch`.
    ///
    /// The origin epoch is pinned to the first dispatch rather than tracking
    /// the current connection, which is what lets a successor writer recognise
    /// a retry that originated under its predecessor.
    fn envelope_for(&self, writer_epoch: u64) -> MutationEnvelope {
        if !self.has_op_id() {
            return MutationEnvelope::default();
        }
        MutationEnvelope {
            op_id: self.op_id,
            flags: if self.origin_epoch.is_some() {
                protocol::OP_FLAG_RETRY
            } else {
                0
            },
            origin_writer_epoch: self.origin_epoch.unwrap_or(writer_epoch),
        }
    }

    /// Record that this frame may have reached the server.
    ///
    /// Called at the send rather than once the outcome is known, so the horizon
    /// runs from the frame that created the ambiguity. Stamping it after the
    /// reply wait would extend the effective window by that wait, which the
    /// liveness extensions can stretch well past the margin the server's
    /// result retention leaves beyond the horizon.
    fn note_dispatched(&mut self, envelope: MutationEnvelope) {
        if !self.has_op_id() {
            return;
        }
        if self.origin_epoch.is_none() {
            self.origin_epoch = Some(envelope.origin_writer_epoch);
        }
        if self.started_ns.is_none() {
            self.started_ns = Some(monotonic_ns());
        }
    }

    /// The server proved this frame was rejected before dispatch.
    ///
    /// Only a frame that was not itself a retry may erase the ambiguity: a
    /// clean rejection of a retry says nothing about the older frame it
    /// retried.
    fn proven_predispatch(&mut self, envelope: MutationEnvelope) {
        if envelope.flags & protocol::OP_FLAG_RETRY == 0 {
            self.origin_epoch = None;
            self.started_ns = None;
        }
    }

    /// Jiffies still available to a wait that precedes a dispatch.
    ///
    /// Every blocking wait between an attempt and the frame it sends is clipped
    /// to this, so nothing goes on the wire after the horizon. A dispatched
    /// operation is bounded twice over, and the horizon is checked first
    /// because its expiry is the more honest report: the operation may have
    /// applied and can no longer be safely resent, where the grace only says
    /// this caller waited long enough.
    fn resend_budget(&self, grace_ms: u64) -> Result<usize> {
        let now = monotonic_ns();
        let horizon_ms = match self.started_ns {
            // Past the horizon the server's dedup entry may be gone, so a
            // resend would be indistinguishable from a new operation. Report an
            // ambiguous disconnect rather than resending.
            Some(started) => Some(
                MUTATION_RETRY_HORIZON_MS
                    .checked_sub(elapsed_ms(started, now))
                    .filter(|remaining| *remaining != 0)
                    .ok_or_else(not_connected_errno)?,
            ),
            // An operation with nothing on the wire has no ambiguity to bound.
            None => None,
        };
        let mut remaining = grace_ms
            .checked_sub(elapsed_ms(self.entered_ns, now))
            .filter(|remaining| *remaining != 0)
            .ok_or(ETIMEDOUT)?;
        if let Some(horizon_ms) = horizon_ms {
            remaining = remaining.min(horizon_ms);
        }
        Ok(jiffies_for_ms(remaining).max(1))
    }
}

/// Outcome of one request transmission.
enum SendOutcome {
    /// The complete frame reached the stream.
    Sent,
    /// The attempt failed with the slot still registered, so the reply wait
    /// reports the session's recorded status.
    Failed(Error),
    /// The attempt failed before anything was transmitted and its slot has
    /// already been released.
    Released(Error),
    /// Nothing was transmitted because the target connection was replaced, and
    /// the slot has already been released.
    Stale(Error),
}

impl SendOutcome {
    /// Classify one transmission, releasing the slot when nothing was sent.
    ///
    /// A rejected frame left its tag `Reserved`, so this side owns the release.
    /// A broken one may have put bytes on the stream, so its tag stays `Sent`
    /// for the reply wait to retire.
    fn from_send(session: &Session, tag: usize, send: FrameSend) -> Self {
        match send {
            FrameSend::Sent => Self::Sent,
            FrameSend::Rejected(error) => {
                session.release_slot(tag);
                Self::Stale(error)
            }
            FrameSend::Broken(error) => Self::Failed(error),
        }
    }
}

/// Errno for an encode that failed or that filled a length other than the one
/// the frame was sized for.
///
/// A short encode means the size the slot was reserved for disagrees with what
/// the codec wrote, which is a local fault rather than a codec one, so it
/// reports EPROTO. Both callers reject the request without touching the
/// connection.
fn check_encoded(
    encoded: core::result::Result<usize, protocol::CodecError>,
    frame_size: usize,
) -> Result<()> {
    match encoded {
        Ok(length) if length == frame_size => Ok(()),
        Ok(_) => Err(protocol_errno()),
        Err(error) => Err(codec_errno_without_disconnect(error)),
    }
}
