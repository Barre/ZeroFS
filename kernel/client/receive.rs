use core::sync::atomic::Ordering;

use kernel::{
    alloc::{
        KVVec,
        flags::{GFP_KERNEL, GFP_NOWAIT},
    },
    bindings, ffi,
    prelude::*,
};

use crate::{
    protocol::{self, HEADER_SIZE, NOTAG},
    transport::{CrossTaskDestination, IterCursor},
};

use super::errors::{codec_errno, not_connected_errno, protocol_errno};
use super::reply::OwnedFrame;
use super::session::{ReceiveLink, Session, SessionStatus};
use super::slots::{PendingSlot, PendingState};
use super::tag_space::{is_flush_tag, normal_tag_index};
use super::{READ_PAYLOAD_OFFSET, SMALL_REPLY_BYTES, monotonic_ns};

unsafe extern "C" {
    fn zerofs_client_memalloc_nofs_save() -> ffi::c_uint;
    fn zerofs_client_memalloc_nofs_restore(flags: ffi::c_uint);
}

/// Scope allocations away from filesystem reclaim while dispatch may be
/// running under netfs writeback.
struct NoFsAllocation(ffi::c_uint);

impl NoFsAllocation {
    fn enter() -> Self {
        // SAFETY: the paired restore runs from Drop on this same task.
        Self(unsafe { zerofs_client_memalloc_nofs_save() })
    }
}

impl Drop for NoFsAllocation {
    fn drop(&mut self) {
        // SAFETY: this is the saved value returned by the matching enter.
        unsafe { zerofs_client_memalloc_nofs_restore(self.0) };
    }
}

/// The one stream accumulator of a session, held under `Session::receive`.
///
/// Bytes past the last complete frame stay buffered across receive phases.
pub(super) struct ReceiveState {
    pub(super) buffer: KVVec<u8>,
    pub(super) buffered: usize,
}

/// A receiver's exclusive window on one registered destination.
///
/// The destination's `in_use` flag stays set for this lease's lifetime, which
/// keeps its iterator and backing folios exclusively available to the fill.
struct ReadClaim<'a> {
    session: &'a Session,
    tag: usize,
    iterator: CrossTaskDestination,
    payload_len: usize,
}

impl ReadClaim<'_> {
    fn cursor(&mut self) -> Result<IterCursor<'_>> {
        // SAFETY: claim_read_destination set this slot's `in_use` flag before
        // creating the lease. Its Drop clears the flag after the cursor is gone.
        unsafe { self.iterator.cursor(self.payload_len) }
    }
}

impl Drop for ReadClaim<'_> {
    fn drop(&mut self) {
        self.session.release_read_claim(self.tag);
    }
}

/// Where a published reply's payload was delivered.
enum PublishedPayload {
    /// The complete frame, copied into client memory.
    Frame(KVVec<u8>),
    /// This many payload bytes went straight into the caller's iterator.
    Direct(usize),
}

impl Session {
    /// Take a reusable metadata frame or allocate before sending a request.
    pub(super) fn reply_buffer(&self, frame_size: usize) -> Result<KVVec<u8>> {
        if frame_size <= SMALL_REPLY_BYTES {
            if let Some(mut frame) = self.reply_pool.lock().pop() {
                frame.clear();
                return Ok(frame);
            }
            // Keep fallback allocations pool-compatible. Otherwise one flush
            // that discards an unused pooled frame would permanently shrink
            // the pool because its exact-sized replacement could not return.
            let _nofs = NoFsAllocation::enter();
            return Ok(KVVec::with_capacity(SMALL_REPLY_BYTES, GFP_KERNEL)?);
        }
        let _nofs = NoFsAllocation::enter();
        Ok(KVVec::with_capacity(frame_size, GFP_KERNEL)?)
    }

    /// Return an empty metadata frame to the bounded pool.
    pub(super) fn recycle_reply_buffer(&self, mut frame: KVVec<u8>) {
        if frame.capacity() != SMALL_REPLY_BYTES {
            return;
        }
        frame.clear();
        // The pool was allocated to this exact bound. A full pool means an
        // extra frame reached us (for example after an allocation raced an
        // outstanding pooled frame), so dropping it is the bounded behavior.
        let _ = self.reply_pool.lock().push_within_capacity(frame);
    }

    /// Select the connection task's next phase.
    ///
    /// A connected idle session deliberately passes this gate and sleeps in
    /// `recvmsg`: request waiters own response deadlines and shut the socket
    /// down when one expires. Only a completed Rflush pauses consumption,
    /// because its owner must decide the fate of the request it cancelled
    /// before any following stream bytes are interpreted.
    fn next_io_phase(&self) -> IoPhase {
        let mut state = self.state.lock();
        loop {
            if let SessionStatus::Dead(status) = state.status {
                return IoPhase::Exit(status);
            }
            // SAFETY: This is the connection kthread created by Client.
            if unsafe { bindings::kthread_should_stop() } {
                return IoPhase::Exit(0);
            }
            match state.status {
                SessionStatus::Lost => return IoPhase::Reconnect,
                SessionStatus::Connected if !self.flush_reply_pending() => {
                    return IoPhase::Receive(ReceiveLink {
                        transport: state.transport.clone(),
                        epoch: state.connection_epoch,
                    });
                }
                SessionStatus::Connected | SessionStatus::Dead(_) => {}
            }
            // The timeout exists only to recheck kthread_should_stop().
            let _ = self
                .changed
                .wait_interruptible_timeout(&mut state, self.timeout_jiffies);
        }
    }

    /// Own the installed stream and rebuild it after failure.
    ///
    /// The return value becomes the connection kthread's exit status.
    pub(super) fn io_loop(&self) -> ffi::c_int {
        loop {
            match self.next_io_phase() {
                IoPhase::Exit(status) => return status,
                IoPhase::Reconnect => {
                    if let Some(status) = self.reconnect_with_backoff() {
                        return status;
                    }
                    continue;
                }
                IoPhase::Receive(link) => {
                    // This task is both the only receiver and the only
                    // connection installer. Keep the accumulator lock across
                    // the steady receive loop rather than dropping and
                    // reacquiring it, plus two session locks, for every reply.
                    let mut receive = self.receive.lock();
                    loop {
                        let ReceiveState { buffer, buffered } = &mut *receive;
                        match self.receive_available(&link, buffer.as_mut_slice(), buffered) {
                            Ok(false) => {}
                            Ok(true) => break,
                            Err(error) => {
                                drop(receive);
                                // An earlier caller retirement has already
                                // bumped the epoch, making this a no-op.
                                self.retire_connection(error, link.epoch);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Take exclusive use of a tag's registered destination for one reply.
    ///
    /// `Ok(None)` means this is an unregistered read and the caller may use its
    /// preallocated frame. A registered read must use its direct destination
    /// because its preallocated frame covers only an error response.
    fn claim_read_destination(
        &self,
        link: &ReceiveLink,
        header: protocol::Header,
        payload_len: usize,
    ) -> Result<Option<ReadClaim<'_>>> {
        if self.active_epoch.load(Ordering::Acquire) != link.epoch {
            return Err(not_connected_errno());
        }
        let tag = incoming_tag(header)?;
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut slots = shard.lock();
        if self.active_epoch.load(Ordering::Acquire) != link.epoch {
            return Err(not_connected_errno());
        }
        let slot = slots.get_mut(local_tag).ok_or_else(protocol_errno)?;
        check_incoming_header(slot, header)?;
        let Some(destination) = slot.destination.as_mut() else {
            return Ok(None);
        };
        if destination.in_use || destination.delivered.is_some() || payload_len > destination.limit
        {
            // Registered reads reserve only an Rlerror-sized allocation. Once
            // an Rread names their tag, falling back to buffered receipt would
            // violate that memory promise; these states are also impossible
            // for a request with exactly one terminal response.
            return Err(protocol_errno());
        }
        destination.in_use = true;
        Ok(Some(ReadClaim {
            session: self,
            tag,
            iterator: destination.iterator,
            payload_len,
        }))
    }

    fn release_read_claim(&self, tag: usize) {
        let Ok((shard, local_tag)) = self.slot_shard(tag) else {
            return;
        };
        let mut slots = shard.lock();
        if let Some(destination) = slots
            .get_mut(local_tag)
            .and_then(|slot| slot.destination.as_mut())
        {
            destination.in_use = false;
        }
    }

    /// Place a claimed reply's payload in its caller's folios.
    ///
    /// The already-buffered prefix is copied first, then the remainder comes
    /// off the socket. Any failure leaves the frame partly consumed.
    fn fill_read_destination(
        &self,
        link: &ReceiveLink,
        claim: &mut ReadClaim<'_>,
        prefix: &[u8],
    ) -> Result<()> {
        let mut cursor = claim.cursor()?;
        if !prefix.is_empty() && cursor.write(prefix) != prefix.len() {
            return Err(protocol_errno());
        }
        link.transport.recv_exact_into(&mut cursor)
    }

    /// Receive one `Rread` payload straight into its caller's folios.
    ///
    /// Returns the bytes consumed from `frame`, or `Ok(None)` when this reply
    /// has no usable registration and the ordinary path should handle it.
    fn try_direct_read(
        &self,
        link: &ReceiveLink,
        header: protocol::Header,
        frame: &[u8],
    ) -> Result<Option<usize>> {
        let Some(payload_len) = (header.size as usize).checked_sub(READ_PAYLOAD_OFFSET) else {
            return Ok(None);
        };
        let count = frame
            .get(HEADER_SIZE..READ_PAYLOAD_OFFSET)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes);
        let claim = self.claim_read_destination(link, header, payload_len)?;
        // A count that does not cover exactly the rest of the frame leaves the
        // payload bounds ambiguous. An unregistered read hands the frame to
        // the ordinary path, where decode_response rejects it. A registered
        // read cannot buffer the declared payload in its small preallocated
        // frame, so reject it at the header instead.
        if count != Some(payload_len as u32) {
            return if claim.is_some() {
                Err(protocol_errno())
            } else {
                Ok(None)
            };
        }

        let buffered_payload = frame
            .len()
            .saturating_sub(READ_PAYLOAD_OFFSET)
            .min(payload_len);
        let consumed = READ_PAYLOAD_OFFSET
            .checked_add(buffered_payload)
            .ok_or_else(protocol_errno)?;
        let prefix = frame
            .get(READ_PAYLOAD_OFFSET..consumed)
            .ok_or_else(protocol_errno)?;

        let Some(mut claim) = claim else {
            return Ok(None);
        };
        let filled = self.fill_read_destination(link, &mut claim, prefix);
        // Past the first payload byte the stream is desynchronized, so no later
        // receiver could resynchronize on the next header. The caller's own
        // iterator was never advanced, because IterCursor consumes a private
        // copy, so its resend refills the same range.
        filled?;

        self.publish_incoming_frame(link, header, PublishedPayload::Direct(payload_len))?;
        // Keep the lease through publication so a timing-out caller cannot
        // clear the destination between the final socket read and recording
        // the directly delivered byte count.
        drop(claim);
        Ok(Some(consumed))
    }

    /// Receive one stream chunk and publish every complete response it holds.
    ///
    /// Small responses normally arrive in one AF_UNIX skb. Reading into a
    /// persistent accumulator consumes that skb once rather than issuing one
    /// recvmsg for its header and another for its body. Complete adjacent
    /// frames are dispatched from the same receive. Large frames copy only
    /// their buffered prefix and receive the remainder directly into their
    /// final allocation.
    fn receive_available(
        &self,
        link: &ReceiveLink,
        buffer: &mut [u8],
        buffered: &mut usize,
    ) -> Result<bool> {
        if *buffered > buffer.len() {
            return Err(protocol_errno());
        }

        let mut received = false;
        loop {
            let mut offset = 0usize;
            let mut published = false;
            let mut flush_barrier = false;
            while buffered.saturating_sub(offset) >= HEADER_SIZE {
                let header_end = offset.checked_add(HEADER_SIZE).ok_or_else(protocol_errno)?;
                let header_bytes = buffer.get(offset..header_end).ok_or_else(protocol_errno)?;
                let header =
                    protocol::decode_header(header_bytes, self.msize).map_err(codec_errno)?;
                let frame_size = header.size as usize;
                let available = buffered.saturating_sub(offset);

                // An Rread can never use a reserved cancellation tag, whose
                // slots only expect Rflush, so taking this path cannot
                // step over the Tflush consumption barrier below.
                if header.type_ == protocol::message_type::RREAD {
                    if frame_size < READ_PAYLOAD_OFFSET {
                        return Err(protocol_errno());
                    }
                    // A registered read reserved only an Rlerror-sized frame.
                    // Do not route a fragmented Rread through that allocation
                    // before its count field is present and direct delivery can
                    // claim the destination.
                    if available < READ_PAYLOAD_OFFSET {
                        break;
                    }
                    let frame = buffer.get(offset..*buffered).ok_or_else(protocol_errno)?;
                    if let Some(consumed) = self.try_direct_read(link, header, frame)? {
                        offset = offset.checked_add(consumed).ok_or_else(protocol_errno)?;
                        published = true;
                        if consumed == available {
                            break;
                        }
                        continue;
                    }
                }

                if frame_size > buffer.len() {
                    // A large declaration is bounded by negotiated msize. Take
                    // the buffer its request allocated before entering the
                    // stream, then receive the rest without any allocation.
                    let prefix = buffer.get(offset..*buffered).ok_or_else(protocol_errno)?;
                    let mut frame = self.frame_with_prefix(link, header, prefix)?;
                    let remaining = frame_size
                        .checked_sub(prefix.len())
                        .ok_or_else(protocol_errno)?;
                    let body = frame
                        .spare_capacity_mut()
                        .get_mut(..remaining)
                        .ok_or_else(protocol_errno)?;
                    // The receiver kthread has no userspace signal policy. A
                    // caller timeout retires and shuts down this transport,
                    // waking a peer that stopped partway through the body.
                    link.transport.recv_exact_uninit(body)?;
                    // SAFETY: recv_exact_uninit returned success only after
                    // initializing all `remaining` spare bytes.
                    unsafe {
                        frame.inc_len(remaining);
                    }

                    offset = *buffered;
                    self.publish_incoming_frame(link, header, PublishedPayload::Frame(frame))?;
                    published = true;
                    break;
                }

                if available < frame_size {
                    break;
                }
                let frame_end = offset.checked_add(frame_size).ok_or_else(protocol_errno)?;
                let frame_bytes = buffer.get(offset..frame_end).ok_or_else(protocol_errno)?;
                let frame = self.frame_with_prefix(link, header, frame_bytes)?;
                offset = frame_end;
                let tag =
                    self.publish_incoming_frame(link, header, PublishedPayload::Frame(frame))?;
                published = true;

                // Preserve the existing Tflush consumption barrier. Bytes
                // already received after Rflush stay buffered until its owner
                // vacates its cancellation slot.
                if is_flush_tag(tag) {
                    flush_barrier = true;
                    break;
                }
            }

            let remaining = buffered.checked_sub(offset).ok_or_else(protocol_errno)?;
            if offset != 0 && remaining != 0 {
                buffer.copy_within(offset..*buffered, 0);
            }
            *buffered = remaining;

            if published {
                return Ok(flush_barrier);
            }
            if received {
                // An incomplete buffered frame necessarily belongs to an
                // outstanding request.
                return Ok(false);
            }

            let mut destination = buffer.get_mut(*buffered..).ok_or_else(protocol_errno)?;
            if destination.is_empty() {
                return Err(protocol_errno());
            }
            // With a bulk read outstanding and nothing buffered, take only the
            // header. Whatever payload lands in the accumulator has to be
            // copied into the caller's folios, while everything left on the
            // socket goes straight there, so reading less here removes that
            // copy. The cost when the next frame is not the bulk read is one
            // extra receive.
            if *buffered == 0 && self.bulk_reads.load(Ordering::Relaxed) != 0 {
                destination = destination
                    .get_mut(..READ_PAYLOAD_OFFSET)
                    .ok_or_else(protocol_errno)?;
            }
            let count = link.transport.recv_some(destination)?;
            *buffered = buffered.checked_add(count).ok_or_else(protocol_errno)?;
            received = true;
        }
    }

    /// Take the response allocation attached to an incoming frame's tag.
    ///
    /// The slot stays `Sent` while a large body is received. Connection
    /// retirement may fail the slot in the meantime, but it first changes
    /// `active_epoch`, so publication below rejects this old frame.
    fn take_reply_buffer(&self, link: &ReceiveLink, header: protocol::Header) -> Result<KVVec<u8>> {
        if self.active_epoch.load(Ordering::Acquire) != link.epoch {
            return Err(not_connected_errno());
        }
        let tag = incoming_tag(header)?;
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut slots = shard.lock();
        if self.active_epoch.load(Ordering::Acquire) != link.epoch {
            return Err(not_connected_errno());
        }
        let slot = slots.get_mut(local_tag).ok_or_else(protocol_errno)?;
        check_incoming_header(slot, header)?;
        let buffer = slot.reply_buffer.take().ok_or_else(protocol_errno)?;
        if buffer.capacity() < header.size as usize {
            return Err(protocol_errno());
        }
        Ok(buffer)
    }

    /// Hand one complete reply to the waiter holding its tag.
    fn publish_incoming_frame(
        &self,
        link: &ReceiveLink,
        header: protocol::Header,
        payload: PublishedPayload,
    ) -> Result<usize> {
        if let PublishedPayload::Frame(frame) = &payload {
            if frame.len() != header.size as usize {
                return Err(protocol_errno());
            }
        }

        if self.active_epoch.load(Ordering::Acquire) != link.epoch {
            return Err(not_connected_errno());
        }
        let tag = incoming_tag(header)?;
        let (shard, local_tag) = self.slot_shard(tag)?;
        let mut slots = shard.lock();
        // Retirement publishes the replacement epoch before sweeping slots.
        // Rechecking under the tag lock makes the boundary exact: a reply that
        // got here first is completed, while a receiver from the retired
        // stream can no longer publish afterward.
        if self.active_epoch.load(Ordering::Acquire) != link.epoch {
            return Err(not_connected_errno());
        }
        let slot = slots.get_mut(local_tag).ok_or_else(protocol_errno)?;
        check_incoming_header(slot, header)?;
        match payload {
            PublishedPayload::Frame(frame) => {
                if slot.reply_buffer.is_some() {
                    return Err(protocol_errno());
                }
                slot.state = PendingState::Completed(frame);
            }
            PublishedPayload::Direct(delivered) => {
                let destination = slot.destination.as_mut().ok_or_else(protocol_errno)?;
                destination.delivered = Some(delivered);
                let mut frame = slot.reply_buffer.take().ok_or_else(protocol_errno)?;
                frame.clear();
                slot.state = PendingState::Completed(frame);
            }
        }
        drop(slots);

        self.decrement_sent_count();
        // Keep the clocksource read outside the tag critical section. On KVM
        // this is visible in profiles, and no waiter needs the timestamp to
        // consume a reply that is already complete.
        self.last_frame_ns.store(monotonic_ns(), Ordering::Relaxed);
        if normal_tag_index(tag).is_some() {
            self.wake_reply_waiter(tag)?;
        }
        Ok(tag)
    }

    /// Fill the preallocated frame with its accumulated prefix.
    fn frame_with_prefix(
        &self,
        link: &ReceiveLink,
        header: protocol::Header,
        prefix: &[u8],
    ) -> Result<KVVec<u8>> {
        let mut frame = self.take_reply_buffer(link, header)?;
        // reserve_slot already allocated the complete declared capacity. This
        // append therefore cannot need reclaim while the receiver holds a tag.
        frame.extend_from_slice(prefix, GFP_NOWAIT)?;
        Ok(frame)
    }
}

/// Work exclusively owned by the per-mount connection task.
enum IoPhase {
    Receive(ReceiveLink),
    Reconnect,
    Exit(ffi::c_int),
}

/// The error code of an `Rlerror` frame, read without decoding it.
///
/// An `Rlerror` body is exactly the four little-endian bytes after the header,
/// so the code is reachable directly. Everything that only needs to classify a
/// reply uses this and leaves the one complete decode to the operation that
/// consumes it.
pub(super) fn rlerror_code(frame: &[u8]) -> Option<u32> {
    if frame.get(4).copied()? != protocol::message_type::RLERROR {
        return None;
    }
    let code = frame.get(HEADER_SIZE..HEADER_SIZE.checked_add(4)?)?;
    Some(u32::from_le_bytes(<[u8; 4]>::try_from(code).ok()?))
}

/// Reject an `Rlerror` code outside the Linux errno range.
///
/// Header validation and operation decoding cover the rest of the frame. The
/// internal restart codes remain valid wire errors here and are interpreted by
/// the request that receives them.
pub(super) fn validate_response_frame(frame: &OwnedFrame<'_>) -> Result<()> {
    match rlerror_code(frame.bytes.as_slice()) {
        Some(code) if (1..=bindings::MAX_ERRNO).contains(&code) => Ok(()),
        Some(_) => Err(protocol_errno()),
        None => Ok(()),
    }
}

/// Convert a wire tag to its slot index.
fn incoming_tag(header: protocol::Header) -> Result<usize> {
    if header.tag == NOTAG {
        return Err(protocol_errno());
    }
    Ok(header.tag as usize)
}

/// Match an incoming header to the slot awaiting it.
///
/// Directly delivered replies never reach `decode_response`, so this is their
/// only tag, type, and declared-size validation.
fn check_incoming_header(slot: &PendingSlot, header: protocol::Header) -> Result<()> {
    if !matches!(slot.state, PendingState::Sent) {
        return Err(protocol_errno());
    }
    let expected = slot.expected.ok_or_else(protocol_errno)?;
    let rlerror_size = HEADER_SIZE + core::mem::size_of::<u32>();
    if header.size as usize > slot.maximum_frame
        || (header.type_ == protocol::message_type::RLERROR && header.size as usize != rlerror_size)
        || !expected.matches_type(header.type_)
    {
        return Err(protocol_errno());
    }
    Ok(())
}
