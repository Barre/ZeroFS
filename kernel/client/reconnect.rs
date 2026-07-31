use core::{
    mem,
    sync::atomic::{AtomicBool, Ordering},
};

use kernel::{
    alloc::{KVec, flags::GFP_KERNEL},
    bindings, ffi,
    prelude::*,
    sync::Arc,
};

use crate::{
    protocol::{
        self, DecodedResponse, NOTAG, Qid, Request, Response, Rgetlineage, VERSION_9P2000L_ZEROFS,
    },
    transport::SocketTransport,
};

use super::endpoint::Endpoint;
use super::errors::{
    codec_errno, codec_errno_without_disconnect, message_size_errno, not_connected_errno,
    protocol_errno, server_errno,
};
use super::registry::{
    FidRecord, FidSlot, LockRecord, RebindCredentials, forget_fid_locks_locked,
    release_credential_locked,
};
use super::session::{Session, SessionStatus};
use super::slots::ExpectedResponse;
use super::{
    CLIENT_ID_LEN, MIN_MSIZE, RECONNECT_BACKOFF_MAX_MS, RECONNECT_BACKOFF_MIN_MS,
    REPLAY_FRAME_BYTES, REPLAY_TIMEOUT_MS, ROOT_FID, ROOT_INODE_ID, jiffies_for_ms, monotonic_ns,
};

pub(super) struct BootstrappedTransport {
    pub(super) candidate: ProbedCandidate,
    pub(super) root_qid: Qid,
}

/// Rebuild the session after a lost connection.
impl Session {
    /// Probe, replay and install until one attempt succeeds.
    ///
    /// `Some` is the connection task's exit status.
    pub(super) fn reconnect_with_backoff(&self) -> Option<ffi::c_int> {
        let mut backoff_ms = RECONNECT_BACKOFF_MIN_MS;
        loop {
            // SAFETY: This is the connection kthread created by Client.
            if unsafe { bindings::kthread_should_stop() } {
                return Some(0);
            }
            match self.reconnect_once() {
                Ok(()) => {
                    pr_info!("zerofs: session reconnected and restored\n");
                    return None;
                }
                Err(error) => {
                    let mut state = self.state.lock();
                    if let SessionStatus::Dead(status) = state.status {
                        return Some(status);
                    }
                    pr_warn!(
                        "zerofs: reconnect failed (errno={}); retrying in {}ms\n",
                        error.to_errno(),
                        backoff_ms
                    );
                    // Waiting on the status condvar rather than sleeping is what
                    // lets termination break the backoff at once.
                    let _ = self
                        .live_changed
                        .wait_interruptible_timeout(&mut state, jiffies_for_ms(backoff_ms).max(1));
                    backoff_ms = backoff_ms.saturating_mul(2).min(RECONNECT_BACKOFF_MAX_MS);
                }
            }
        }
    }

    /// Probe for the serving leader, then replay and install it.
    fn reconnect_once(&self) -> Result<()> {
        let candidate = {
            let mut io = [0u8; REPLAY_FRAME_BYTES];
            probe_targets(
                &self.endpoint,
                Some(RequiredMsize {
                    msize: self.msize,
                    warned: &self.msize_mismatch_warned,
                }),
                self.preferred_target.load(Ordering::Relaxed) as usize,
                &mut io,
                Some(self),
            )?
        };
        self.preferred_target
            .store(candidate.target as u32, Ordering::Relaxed);

        let transport = candidate.transport;
        let result = self
            .replay_session(&transport)
            .and_then(|()| self.install_connection(&transport, candidate.lineage));
        if result.is_err() {
            self.discard_candidate(&transport);
        }
        self.clear_candidate();
        result
    }

    /// Publish the socket a probe or a replay is working on.
    ///
    /// Termination shuts a published candidate down instead of making
    /// `Client::drop` wait out a dial, a handshake or a whole replay.
    fn publish_candidate(&self, candidate: Arc<SocketTransport>) -> Result<()> {
        let previous = {
            let mut state = self.state.lock();
            if let SessionStatus::Dead(status) = state.status {
                return Err(Error::from_errno(status));
            }
            state.candidate.replace(candidate)
        };
        // Releasing the last reference to a socket is not work for the state
        // lock to hold.
        drop(previous);
        Ok(())
    }

    fn clear_candidate(&self) {
        // Binding before dropping is what keeps the socket's release out of the
        // critical section: as one statement the take's temporary would be
        // dropped ahead of the guard.
        let previous = self.state.lock().candidate.take();
        drop(previous);
    }

    /// The terminal errno if the logical session has ended.
    fn terminal_status(&self) -> Option<ffi::c_int> {
        match self.state.lock().status {
            SessionStatus::Dead(status) => Some(status),
            _ => None,
        }
    }

    /// Reset a rejected candidate before closing it.
    ///
    /// Tversion resets the server session, releasing every fid and open-handle
    /// pin this replay created; waiting for its ordered reply is the proof of
    /// release. A bare shutdown would leave them held until the server's own
    /// connection guard fires.
    fn discard_candidate(&self, transport: &SocketTransport) {
        let mut io = [0u8; REPLAY_FRAME_BYTES];
        if let Err(error) = transact_version(transport, &mut io, self.endpoint.requested_msize) {
            pr_warn!(
                "zerofs: replay candidate reset failed (errno={}); closing it\n",
                error.to_errno()
            );
        }
        transport.shutdown();
    }

    /// Rebuild every recorded fid on `transport`, then every recorded lock.
    ///
    /// No record can change while this runs: a record is only installed by a
    /// decoded reply and only removed by a decoded Rclunk, and every request
    /// path is parked in the live gate while the session is `Lost`. A lock
    /// granted on the connection this one replaces carries its epoch, so its
    /// store is refused rather than landing behind the pass below.
    fn replay_session(&self, transport: &SocketTransport) -> Result<()> {
        let deadline_ns =
            monotonic_ns().saturating_add(REPLAY_TIMEOUT_MS.saturating_mul(1_000_000));
        let mut io = [0u8; REPLAY_FRAME_BYTES];
        let mut gone: KVec<(u32, bool)> = KVec::new();

        // Attach roots must exist before descendants resolve against them.
        for roots in [true, false] {
            let mut from = 0usize;
            while let Some((index, record, credentials)) = self.next_replay_record(from, roots)? {
                from = index.saturating_add(1);
                if monotonic_ns() >= deadline_ns {
                    return Err(ETIMEDOUT);
                }
                let fid = index as u32;
                match self.replay_fid(transport, &mut io, fid, &record, &credentials)? {
                    ReplayOutcome::Restored => {}
                    // A fid that no longer resolves only fails its own
                    // operations, so it becomes a tombstone and the session
                    // survives. A fid that held a lock is different: the
                    // exclusion the application is relying on is gone and
                    // nothing else would ever report it.
                    ReplayOutcome::Gone if self.fid_has_recorded_lock(fid) => {
                        return Err(
                            self.replay_state_lost("fid with a held lock could not be rebound")
                        );
                    }
                    ReplayOutcome::Gone => gone.push((fid, record.opened), GFP_KERNEL)?,
                }
            }
        }

        if !gone.is_empty() {
            let mut state = self.state.lock();
            for &(fid, opened) in gone.iter() {
                // An open handle whose inode is gone must report ESTALE to its
                // owner. An unopened fid is only a path capability, so the
                // caller may legitimately rebind that number to a new inode
                // later and a tombstone would poison it.
                let replacement = if opened {
                    FidSlot::Stale
                } else {
                    FidSlot::Vacant
                };
                let Some(slot) = state.records.get_mut(fid as usize) else {
                    continue;
                };
                if let FidSlot::Live(record) = mem::replace(slot, replacement) {
                    release_credential_locked(&mut state, record.credential);
                }
                // Nothing can reinstall a lock on a fid that no longer resolves,
                // and a record left here would be replayed onto whatever this
                // number is next bound to.
                forget_fid_locks_locked(&mut state, fid);
            }
        }

        // Locks come last because every record names a fid the steps above had
        // to reinstall first; a Tlock on a fid this connection does not have
        // earns EBADF, which is neither a conflict nor a lost object, so it
        // would fail every reconnect round forever.
        self.replay_locks(transport, &mut io, deadline_ns)
    }

    /// Reinstall the complete recorded lock set on `transport`.
    ///
    /// All of it or none of it: a partial set would leave ranges held that this
    /// client does not believe it holds, denying them to everyone else while
    /// this mount reports nothing. A conflict is the expected case rather than
    /// an edge case, because the server keys conflicts on the connection and the
    /// one that just died still holds these same ranges until its guard runs, so
    /// the whole set is rolled back and retried with backoff.
    fn replay_locks(
        &self,
        transport: &SocketTransport,
        io: &mut [u8],
        deadline_ns: u64,
    ) -> Result<()> {
        // Declared out here so it keeps growing across attempts rather than
        // restarting at the minimum on every retry.
        let mut backoff_ms = RECONNECT_BACKOFF_MIN_MS;
        'attempt: loop {
            // What this pass holds, as an exclusive bound on record indices.
            let mut acquired = 0usize;
            let mut from = 0usize;
            while let Some((index, record)) = self.next_replay_lock(from) {
                from = index.saturating_add(1);
                if monotonic_ns() >= deadline_ns {
                    // The candidate is discarded on this error and its Tversion
                    // releases the prefix, which is more than a rollback past
                    // the deadline could promise.
                    return Err(ETIMEDOUT);
                }
                match self.replay_lock_step(transport, io, &record) {
                    Ok(LockReplayOutcome::Acquired) => acquired = from,
                    Ok(LockReplayOutcome::Conflict) => {
                        if !self.rollback_replayed_locks(transport, io, acquired, deadline_ns) {
                            // Ranges may still be held under a prefix this
                            // client can no longer account for. EAGAIN is
                            // non-terminal, so the candidate is discarded and
                            // its Tversion is what releases them.
                            return Err(EAGAIN);
                        }
                        self.replay_conflict_backoff(backoff_ms, deadline_ns)?;
                        backoff_ms = backoff_ms.saturating_mul(2).min(RECONNECT_BACKOFF_MAX_MS);
                        continue 'attempt;
                    }
                    // Rollback first: replay_state_lost shuts the candidate
                    // down, and these unlocks still have to reach it.
                    Ok(LockReplayOutcome::Refused(reason)) => {
                        self.rollback_replayed_locks(transport, io, acquired, deadline_ns);
                        return Err(self.replay_state_lost(reason));
                    }
                    Err(error) => {
                        self.rollback_replayed_locks(transport, io, acquired, deadline_ns);
                        return Err(error);
                    }
                }
            }
            return Ok(());
        }
    }

    /// Reacquire one recorded range, classifying what came back.
    ///
    /// Never asks to block: a blocking request parks inside the server, where
    /// this client can neither bound the wait nor cancel it, and a request
    /// parked there outlives both the replay deadline and the reconnect grace.
    /// A conflict therefore arrives as an `Rlerror` carrying `EAGAIN`, and as
    /// `LOCK_BLOCKED` from a server that answers a non-blocking request that
    /// way anyway.
    ///
    /// The conflict is read off the decoded `Rlerror` rather than off an errno,
    /// because a socket that hits `SO_RCVTIMEO` or `SO_SNDTIMEO` reports
    /// `EAGAIN` too. Treating that as a conflict would retry the whole set on a
    /// stream carrying a truncated request or an unread reply, and every later
    /// exchange would answer the previous one. A transport failure is exactly
    /// the case that has to discard the candidate.
    fn replay_lock_step(
        &self,
        transport: &SocketTransport,
        io: &mut [u8],
        record: &LockRecord,
    ) -> Result<LockReplayOutcome> {
        let request = Request::Tlock {
            fid: record.fid,
            lock_type: record.lock_type,
            flags: 0,
            start: record.start,
            length: record.length,
            proc_id: record.proc_id,
            client_id: self.client_id(),
        };
        match bootstrap_exchange(transport, io, request, REPLAY_TAG, self.msize) {
            Ok(reply) => match reply.body {
                Response::Rlock(lock) if lock.status == protocol::LOCK_SUCCESS => {
                    Ok(LockReplayOutcome::Acquired)
                }
                Response::Rlock(lock) if lock.status == protocol::LOCK_BLOCKED => {
                    Ok(LockReplayOutcome::Conflict)
                }
                Response::Rlock(_) => Ok(LockReplayOutcome::Refused(
                    "recorded lock was not reacquired",
                )),
                Response::Rlerror(error) if error.ecode == bindings::EAGAIN => {
                    Ok(LockReplayOutcome::Conflict)
                }
                Response::Rlerror(error) if is_replay_state_lost(server_errno(error.ecode)) => {
                    Ok(LockReplayOutcome::Refused("recorded lock was refused"))
                }
                // Operational rather than terminal, as in the userspace client:
                // the candidate is discarded and the next one is asked again.
                Response::Rlerror(error) => Err(server_errno(error.ecode)),
                _ => Ok(LockReplayOutcome::Refused(
                    "lock replay returned a non-Rlock reply",
                )),
            },
            Err(error) => Err(error),
        }
    }

    /// Release the lock prefix one replay attempt installed.
    ///
    /// Sequential where the userspace client fires every unlock at once: replay
    /// owns a bare socket with no tag table, so this costs one round trip per
    /// acquired lock. Reverse order matches that client; the ranges are disjoint
    /// by construction, so it is fidelity rather than a requirement.
    ///
    /// Returns whether every unlock was acknowledged. Anything else, including
    /// running out of replay budget part way, leaves ranges held that only the
    /// candidate's `Tversion` reset can clear.
    fn rollback_replayed_locks(
        &self,
        transport: &SocketTransport,
        io: &mut [u8],
        acquired: usize,
        deadline_ns: u64,
    ) -> bool {
        let mut acknowledged = true;
        let mut before = acquired;
        while let Some((index, record)) = self.previous_replay_lock(before) {
            before = index;
            if monotonic_ns() >= deadline_ns {
                return false;
            }
            let request = Request::Tlock {
                fid: record.fid,
                lock_type: protocol::LOCK_TYPE_UNLCK,
                flags: 0,
                start: record.start,
                length: record.length,
                proc_id: record.proc_id,
                client_id: self.client_id(),
            };
            let reply = bootstrap_transact(transport, io, request, REPLAY_TAG, self.msize);
            if !reply.is_ok_and(|reply| acknowledged_unlock(&reply.body)) {
                acknowledged = false;
            }
        }
        if !acknowledged {
            pr_warn!("zerofs: lock replay rollback was not acknowledged\n");
        }
        acknowledged
    }

    /// Wait out one lock conflict without outliving the replay budget.
    ///
    /// Waiting on the status condvar rather than sleeping is what lets an
    /// unmount or a termination break the retry at once; nothing can cancel a
    /// sleeping kthread, which is what the userspace client's cancellable
    /// timeout does for it.
    fn replay_conflict_backoff(&self, backoff_ms: u64, deadline_ns: u64) -> Result<()> {
        let remaining_ms = deadline_ns.saturating_sub(monotonic_ns()) / 1_000_000;
        if remaining_ms == 0 {
            return Err(ETIMEDOUT);
        }
        let mut state = self.state.lock();
        if let SessionStatus::Dead(status) = state.status {
            return Err(Error::from_errno(status));
        }
        let _ = self.live_changed.wait_interruptible_timeout(
            &mut state,
            jiffies_for_ms(backoff_ms.min(remaining_ms)).max(1),
        );
        // A session that ended while this waited has already shut the candidate
        // down, so the next Tlock would only wait out a request timeout.
        match state.status {
            SessionStatus::Dead(status) => Err(Error::from_errno(status)),
            _ => Ok(()),
        }
    }

    /// Whether any recorded lock names `fid`.
    ///
    /// A `bool` computed under the lock, because the caller goes on to
    /// `replay_state_lost`, which takes the same mutex through `terminate`.
    fn fid_has_recorded_lock(&self, fid: u32) -> bool {
        let state = self.state.lock();
        state
            .locks
            .iter()
            .any(|slot| matches!(slot, Some(record) if record.fid == fid))
    }

    /// The next recorded lock at or after `from`, with its slot index.
    ///
    /// The table is a slot array rather than a dense vector, so the walk skips
    /// holes. Slot order is arbitrary but stable while replay runs, which is all
    /// the retry needs: the records describe disjoint ranges, so no order among
    /// them grants a set the others would refuse.
    fn next_replay_lock(&self, from: usize) -> Option<(usize, LockRecord)> {
        let state = self.state.lock();
        for index in from..state.locks.len() {
            if let Some(Some(record)) = state.locks.get(index).copied() {
                return Some((index, record));
            }
        }
        None
    }

    /// The last recorded lock strictly below `before`, with its slot index.
    fn previous_replay_lock(&self, before: usize) -> Option<(usize, LockRecord)> {
        let state = self.state.lock();
        for index in (0..before.min(state.locks.len())).rev() {
            if let Some(Some(record)) = state.locks.get(index).copied() {
                return Some((index, record));
            }
        }
        None
    }

    /// The next recorded fid at or after `from`, with its identity copied out.
    fn next_replay_record(
        &self,
        from: usize,
        roots: bool,
    ) -> Result<Option<(usize, FidRecord, RebindCredentials)>> {
        let state = self.state.lock();
        for index in from..state.records.len() {
            let Some(FidSlot::Live(record)) = state.records.get(index).copied() else {
                continue;
            };
            if (record.inode_id == ROOT_INODE_ID) != roots {
                continue;
            }
            let Some(Some(slot)) = state.credentials.get(record.credential) else {
                // Refcounting keeps a live record's identity interned, so this
                // is unreachable; without it the fid cannot be replayed at all.
                drop(state);
                return Err(self.replay_state_lost("recorded fid lost its identity"));
            };
            return Ok(Some((index, record, slot.credentials.clone())));
        }
        Ok(None)
    }

    /// Rebind one recorded fid, reopening it when it was open.
    fn replay_fid(
        &self,
        transport: &SocketTransport,
        io: &mut [u8],
        fid: u32,
        record: &FidRecord,
        credentials: &RebindCredentials,
    ) -> Result<ReplayOutcome> {
        // REBIND_REPLAY marks recovery but grants no authority: the server
        // reruns the applicable namespace checks. REBIND_OPENED marks the
        // expected in-place reopen so a denied replay can be retired as stale;
        // it neither pins the inode nor skips the reopen's DAC check.
        let flags = protocol::REBIND_REPLAY
            | if record.opened {
                protocol::REBIND_OPENED
            } else {
                0
            };
        let rebind = Request::Trebind {
            fid,
            inode_id: record.inode_id,
            root_inode: ROOT_INODE_ID,
            flags,
            uname: credentials.payload(),
            n_uname: credentials.fsuid,
        };
        match self.replay_step(transport, io, rebind)? {
            ReplayOutcome::Restored => {}
            ReplayOutcome::Gone => return Ok(ReplayOutcome::Gone),
        }
        if !record.opened {
            return Ok(ReplayOutcome::Restored);
        }

        // The shared codec has no Tlopen encoder. Tlopenat with newfid == fid
        // is equivalent on the server: it skips the fid-in-use check for that
        // case and replaces the pending replay marker with an authorized open
        // handle.
        let reopen = Request::Tlopenat {
            fid,
            newfid: fid,
            flags: record.open_flags,
        };
        match self.replay_step(transport, io, reopen)? {
            ReplayOutcome::Restored => Ok(ReplayOutcome::Restored),
            ReplayOutcome::Gone => {
                // Rebind installed a fid even though reopen failed. Leaving it
                // behind would desynchronize the client and server fid tables.
                self.replay_transact(transport, io, Request::Tclunk { fid })?;
                Ok(ReplayOutcome::Gone)
            }
        }
    }

    /// One rebind or reopen step, reporting a vanished object as `Gone`.
    fn replay_step(
        &self,
        transport: &SocketTransport,
        io: &mut [u8],
        request: Request<'_>,
    ) -> Result<ReplayOutcome> {
        match self.replay_transact(transport, io, request) {
            Ok(()) => Ok(ReplayOutcome::Restored),
            // Only these two mean the object is gone. Any other failure is
            // operational, so preserving the record retries it next attempt.
            Err(error) if is_replay_state_lost(error) => Ok(ReplayOutcome::Gone),
            Err(error) => Err(error),
        }
    }

    /// One replay round trip, whose reply carries nothing replay needs.
    fn replay_transact(
        &self,
        transport: &SocketTransport,
        io: &mut [u8],
        request: Request<'_>,
    ) -> Result<()> {
        bootstrap_transact(transport, io, request, REPLAY_TAG, self.msize).map(|_| ())
    }

    /// Replay cannot rebuild state a caller has already observed.
    ///
    /// This is the one path that ends the logical session: continuing would
    /// serve a session the server no longer has.
    fn replay_state_lost(&self, reason: &str) -> Error {
        pr_err!("zerofs: session replay cannot preserve observed state: {reason}\n");
        let error = errno!(ESTALE);
        self.terminate(error);
        error
    }
}

/// Whether a replayed fid, or one step of its replay, was restored or is gone.
enum ReplayOutcome {
    Restored,
    Gone,
}

/// What one replayed lock produced.
enum LockReplayOutcome {
    Acquired,
    /// The range is still held, by the connection that just died or by a third
    /// party. The whole set is rolled back and retried.
    Conflict,
    /// The server will not give this range back. The caller rolls the attempt
    /// back and then ends the logical session, carrying this reason.
    Refused(&'static str),
}

/// Replay runs one request at a time on a connection nothing else uses.
const REPLAY_TAG: u16 = 0;

/// The staging buffer has to hold a replayed `Tlock`, whose only variable part
/// is this mount's lock owner identity: header, fid, lock type, flags, start,
/// length, proc id and the identity's length prefix. Encoding into a buffer
/// this does not fit would fail with `EMSGSIZE` on every reconnect that holds a
/// lock, and the mount would never go live again.
const _: () = assert!(
    protocol::HEADER_SIZE + 4 + 1 + 4 + 8 + 8 + 4 + 2 + CLIENT_ID_LEN <= REPLAY_FRAME_BYTES
);

/// Exactly two errnos mean "this object is gone".
fn is_replay_state_lost(error: Error) -> bool {
    error == ENOENT || error == errno!(ESTALE)
}

/// Only an explicit success releases a range; anything else leaves it held.
fn acknowledged_unlock(body: &Response<'_>) -> bool {
    matches!(body, Response::Rlock(lock) if lock.status == protocol::LOCK_SUCCESS)
}

/// A peer that proved it speaks this dialect and is serving as leader.
pub(super) struct ProbedCandidate {
    pub(super) transport: Arc<SocketTransport>,
    pub(super) negotiated_msize: u32,
    pub(super) lineage: Rgetlineage,
    /// Index in the endpoint's target set, kept as the next round's start.
    pub(super) target: usize,
}

/// The message size a reconnect candidate has to negotiate.
struct RequiredMsize<'a> {
    msize: u32,
    warned: &'a AtomicBool,
}

/// Probe targets in rotation until one proves it is serving as leader.
///
/// A standby refuses the connection until it takes over, and a deposed leader
/// keeps its listener and still completes Tversion, so neither the dial nor the
/// version exchange proves anything. Any failure costs one probe deadline and
/// the rotation moves on; only an exhausted rotation is an error, reported as
/// the last target's failure.
///
/// `session` publishes the candidate currently being negotiated, so a session
/// that ends mid-probe shuts the socket down rather than being waited out. The
/// first connect has no session yet and passes `None`. A winner stays published
/// for the caller to replay on.
fn probe_targets(
    endpoint: &Endpoint,
    required: Option<RequiredMsize<'_>>,
    preferred: usize,
    io: &mut [u8],
    session: Option<&Session>,
) -> Result<ProbedCandidate> {
    let count = endpoint.targets.len();
    if count == 0 {
        return Err(EINVAL);
    }

    let mut last_error = not_connected_errno();
    for step in 0..count {
        if let Some(status) = session.and_then(Session::terminal_status) {
            return Err(Error::from_errno(status));
        }
        let index = preferred.wrapping_add(step) % count;
        match probe_target(endpoint, index, required.as_ref(), io, session) {
            Ok(candidate) => {
                // Which peer is serving is the first thing anyone debugging a
                // failover needs, and this is one line per connection.
                pr_info!("zerofs: target {} of {} is serving\n", index, count);
                return Ok(candidate);
            }
            Err(error) => last_error = error,
        }
    }
    Err(last_error)
}

/// Dial one target and prove it is serving.
///
/// Every exit but the successful one closes the socket it opened, so a rejected
/// candidate leaves neither a file descriptor nor a server-side session behind.
fn probe_target(
    endpoint: &Endpoint,
    index: usize,
    required: Option<&RequiredMsize<'_>>,
    io: &mut [u8],
    session: Option<&Session>,
) -> Result<ProbedCandidate> {
    let target = endpoint
        .targets
        .get(index)
        .ok_or_else(|| EINVAL)?;
    let transport = Arc::new(endpoint.dial(target)?, GFP_KERNEL)?;
    let (negotiated_msize, lineage) = (|| -> Result<(u32, Rgetlineage)> {
        if let Some(session) = session {
            session.publish_candidate(transport.clone())?;
        }
        let negotiated = negotiate_candidate(endpoint, &transport, required, io)?;
        // The winner carries ordinary requests from here on, whose reply wait
        // is the session's own timeout and not the probe's.
        transport.set_io_timeout(endpoint.timeout_ms)?;
        Ok(negotiated)
    })()
    .inspect_err(|_| {
        transport.shutdown();
        if let Some(session) = session {
            session.clear_candidate();
        }
    })?;

    Ok(ProbedCandidate {
        transport,
        negotiated_msize,
        lineage,
        target: index,
    })
}

/// Negotiate the dialect on one candidate and read back its lineage.
///
/// Only Tversion is exempt from the server's lease gate, so Tgetlineage is the
/// leadership proof: a standby or a deposed leader answers it with a
/// lost-leadership error. Completing it is the cheapest available evidence that
/// this peer currently permits successful responses, which is what replay needs
/// before it installs fids.
fn negotiate_candidate(
    endpoint: &Endpoint,
    transport: &SocketTransport,
    required: Option<&RequiredMsize<'_>>,
    io: &mut [u8],
) -> Result<(u32, Rgetlineage)> {
    let requested = endpoint.requested_msize;
    let negotiated = match transact_version(transport, io, requested)?.body {
        Response::Rversion(version)
            if version.version.as_ref() == VERSION_9P2000L_ZEROFS
                && version.msize >= MIN_MSIZE
                && version.msize <= requested =>
        {
            version.msize
        }
        _ => return Err(protocol_errno()),
    };

    // An ambiguous mutation must be resendable byte for byte, and every io
    // bound this mount published was derived from the session's value, so a
    // peer that negotiates anything else is not a candidate.
    if let Some(required) = required {
        if negotiated != required.msize {
            if !required.warned.swap(true, Ordering::AcqRel) {
                pr_warn!(
                    "zerofs: candidate negotiated msize {}; session requires {}\n",
                    negotiated,
                    required.msize
                );
            }
            return Err(protocol_errno());
        }
    }

    match bootstrap_transact(transport, io, Request::Tgetlineage, 0, negotiated)?.body {
        Response::Rgetlineage(lineage) => Ok((negotiated, lineage)),
        _ => Err(protocol_errno()),
    }
}

pub(super) fn bootstrap_connection(
    endpoint: &Endpoint,
    credentials: &RebindCredentials,
) -> Result<BootstrappedTransport> {
    let mut io = [0u8; REPLAY_FRAME_BYTES];
    // The first connect defines the message size for the logical session, so it
    // imposes none.
    let candidate = probe_targets(endpoint, None, 0, &mut io, None)?;

    let reply = bootstrap_transact(
        &candidate.transport,
        &mut io,
        Request::Trebind {
            fid: ROOT_FID,
            inode_id: ROOT_INODE_ID,
            root_inode: ROOT_INODE_ID,
            flags: 0,
            uname: credentials.payload(),
            n_uname: credentials.fsuid,
        },
        1,
        candidate.negotiated_msize,
    )?;
    let Response::Rrebind(rebind) = reply.body else {
        return Err(protocol_errno());
    };

    Ok(BootstrappedTransport {
        candidate,
        root_qid: rebind.qid,
    })
}

/// Exchange Tversion at NOTAG, the one request no lease gate refuses.
///
/// The same exchange opens the dialect on a fresh candidate and resets one that
/// replay has already put state on, so both callers send it identically.
fn transact_version<'a>(
    transport: &SocketTransport,
    io: &'a mut [u8],
    requested: u32,
) -> Result<DecodedResponse<'a>> {
    bootstrap_transact(
        transport,
        io,
        Request::Tversion {
            msize: requested,
            version: VERSION_9P2000L_ZEROFS,
        },
        NOTAG,
        requested,
    )
}

/// One blocking round trip, off the tag table entirely.
///
/// Probe, negotiation and replay each own their connection, so there is no slot
/// to reserve and no receiver to route through: the reply to the request just
/// sent is the next frame on the socket. `Ok` carries the reply type the request
/// expects, which is why a caller matching one variant needs its other arm only
/// for exhaustiveness.
fn bootstrap_transact<'a>(
    transport: &SocketTransport,
    io: &'a mut [u8],
    request: Request<'_>,
    tag: u16,
    max_msize: u32,
) -> Result<DecodedResponse<'a>> {
    let expected = ExpectedResponse::for_request(&request)?;
    let decoded = bootstrap_exchange(transport, io, request, tag, max_msize)?;
    if let Response::Rlerror(error) = decoded.body {
        return Err(server_errno(error.ecode));
    }
    if expected.matches(&decoded.body) {
        Ok(decoded)
    } else {
        Err(protocol_errno())
    }
}

/// The same round trip, returning whatever well-formed body came back.
///
/// Lock replay is the one caller that has to tell a served error, and a reply
/// of the wrong type, from a codec or transport failure: the first two decide
/// between a conflict and the end of the logical session, the third is retried
/// on another candidate. Collapsing them into an errno, as the caller above
/// does, makes those distinctions unavailable, and the server's conflict errno
/// is the one a socket timeout also reports.
fn bootstrap_exchange<'a>(
    transport: &SocketTransport,
    io: &'a mut [u8],
    request: Request<'_>,
    tag: u16,
    max_msize: u32,
) -> Result<DecodedResponse<'a>> {
    let encoded = protocol::encode_request(io, max_msize, tag, request)
        .map_err(codec_errno_without_disconnect)?;
    let request_frame = io.get(..encoded).ok_or_else(protocol_errno)?;
    transport.send_all(request_frame)?;
    let prefix = io.get_mut(..4).ok_or_else(protocol_errno)?;
    transport.recv_exact(prefix)?;
    let frame_size = protocol::decode_frame_size(prefix, max_msize).map_err(codec_errno)?;
    if frame_size > io.len() {
        return Err(message_size_errno());
    }
    let remainder = io.get_mut(4..frame_size).ok_or_else(protocol_errno)?;
    transport.recv_exact(remainder)?;
    let frame = io.get(..frame_size).ok_or_else(protocol_errno)?;
    protocol::decode_response(frame, max_msize, tag).map_err(codec_errno)
}
