use core::{marker::PhantomData, mem, ptr::NonNull};

use kernel::{bindings, iov::IovIterDest, prelude::*};

use crate::{
    protocol::{
        self, GETATTR_ALL, HEADER_SIZE, Qid, Request, Response, Rgetattr, Rlcreateattr, Rlopen,
        Rrebind, Rstatfs, Stat,
    },
    transport::{CrossTaskDestination, PayloadIter},
};

use super::errors::{message_size_errno, protocol_errno};
use super::registry::{FidRecord, LockRecord, LockRecorded, LockSlotClaim, RebindCredentials};
use super::reply::{OwnedFrame, ReplyBytes, ReplyCredit};
use super::retry::OpAttempt;
use super::{Client, READ_PAYLOAD_OFFSET, ROOT_INODE_ID};

/// Decode one reply and take the caller's value out of the single body its
/// request can be answered with.
///
/// Any other body, or one that fails the optional `if` test on its fields, is
/// a server that broke the protocol rather than an operation that failed, so
/// it goes to `invariant_failure`: the session ends and the caller sees EPROTO.
/// Resending would only earn the same reply, and a body this caller cannot
/// interpret leaves it nothing to report.
///
/// `Rlerror` never reaches the match; `Client::decode` has already turned it
/// into the server's error for this caller.
macro_rules! expect_body {
    ($self:expr, $frame:expr, $body:pat $(if $valid:expr)? => $take:expr) => {
        match $self.decode($frame)?.body {
            $body $(if $valid)? => Ok($take),
            _ => $self.invariant_failure(),
        }
    };
}

/// An owned variable-length reply payload.
///
/// The frame remains alive while VFS copies data to userspace or calls a
/// directory actor; no session lock is held across either operation.
pub(crate) struct OwnedPayload<'a> {
    frame: ReplyBytes<'a>,
    offset: usize,
    length: usize,
    _credit: ReplyCredit<'a>,
}

impl OwnedPayload<'_> {
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.frame
            .as_slice()
            .get(self.offset..self.offset.saturating_add(self.length))
            .unwrap_or(&[])
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.length == 0
    }
}

/// Result of an open with a best-effort read from offset zero.
pub(crate) struct OpenReadPayload<'a> {
    pub(crate) open: Rlopen,
    pub(crate) eof: bool,
    pub(crate) payload: OwnedPayload<'a>,
}

/// Record-table space promised to one unlock that has not been sent yet.
///
/// Dropping it returns the promise, so a caller that gives up before sending
/// cannot leak capacity.
pub(crate) struct UnlockSlot<'a>(LockSlotClaim<'a>);

/// A netfslib destination iterator borrowed for one read transaction.
///
/// Its owner keeps the subrequest, and therefore the folios the iterator
/// describes, alive for `'a`.
pub(crate) struct ReplyDestination<'a> {
    iterator: NonNull<bindings::iov_iter>,
    _lifetime: PhantomData<&'a mut bindings::iov_iter>,
}

/// One synchronous transaction's cross-task destination reservation.
///
/// Its lifetime keeps the caller's iterator exclusively borrowed until the
/// transaction has removed every receiver-visible copy from its tag slot.
pub(super) struct ReplyDestinationRegistration<'a> {
    iterator: CrossTaskDestination,
    limit: usize,
    _lifetime: PhantomData<&'a mut bindings::iov_iter>,
}

impl ReplyDestinationRegistration<'_> {
    pub(super) fn slot_parts(&self) -> (CrossTaskDestination, usize) {
        (self.iterator, self.limit)
    }
}

impl<'a> ReplyDestination<'a> {
    /// # Safety
    ///
    /// `iterator` must reference a live `ITER_DEST` iterator of a kind another
    /// task may consume, valid for `'a`, that nothing else reads or advances.
    pub(crate) unsafe fn from_raw(iterator: *mut bindings::iov_iter) -> Option<Self> {
        let iterator = NonNull::new(iterator)?;
        // SAFETY: The caller guarantees that the iterator is live for this
        // immediate validation.
        let raw = unsafe { iterator.as_ref() };
        let worker_safe = raw.iter_type == bindings::iter_type_ITER_BVEC as u8
            || raw.iter_type == bindings::iter_type_ITER_FOLIOQ as u8;
        if !worker_safe || raw.data_source != (bindings::ITER_DEST != 0) {
            return None;
        }
        Some(Self {
            iterator,
            _lifetime: PhantomData,
        })
    }

    fn registration(&mut self, limit: usize) -> ReplyDestinationRegistration<'_> {
        ReplyDestinationRegistration {
            // SAFETY: `self` was validated at construction, and the returned
            // registration keeps it exclusively borrowed for the handoff.
            iterator: unsafe { CrossTaskDestination::from_exclusive(self.iterator) },
            limit,
            _lifetime: PhantomData,
        }
    }

    fn remaining(&mut self) -> usize {
        self.borrow().len()
    }

    fn advance(&mut self, bytes: usize) {
        self.borrow().advance(bytes);
    }

    fn copy(&mut self, bytes: &[u8]) -> usize {
        self.borrow().copy_to_iter(bytes)
    }

    fn borrow(&mut self) -> &mut IovIterDest<'a> {
        // SAFETY: The constructor's contract keeps this iterator live, of
        // destination direction, and exclusively ours for `'a`.
        unsafe { IovIterDest::from_raw(self.iterator.as_ptr()) }
    }
}

/// Protocol byte range; zero length extends to end of file.
#[derive(Clone, Copy)]
pub(crate) struct LockRange {
    pub(crate) start: u64,
    pub(crate) length: u64,
}

/// Lock owner and API flavor used for wire requests and local conflict tracking.
///
/// The flavors are distinct locally but share one server owner, so a fid
/// cannot hold both.
#[derive(Clone, Copy)]
pub(crate) struct LockOwner {
    pub(crate) proc_id: u32,
    pub(crate) flock: bool,
}

/// Protocol device number; both fields are zero for FIFOs and sockets.
#[derive(Clone, Copy, Default)]
pub(crate) struct DeviceNumber {
    pub(crate) major: u32,
    pub(crate) minor: u32,
}

/// A normalized protocol timestamp.
#[derive(Clone, Copy)]
pub(crate) struct WireTime {
    sec: u64,
    nsec: u64,
}

impl WireTime {
    /// Construct a timestamp, rejecting invalid nanoseconds.
    pub(crate) fn new(sec: u64, nsec: u64) -> Result<Self> {
        if nsec >= 1_000_000_000 {
            return Err(EINVAL);
        }
        Ok(Self { sec, nsec })
    }
}

/// A timestamp change modeled as one choice, excluding invalid flag pairs.
#[derive(Clone, Copy, Default)]
pub(crate) enum TimeChange {
    #[default]
    Retain,
    Now,
    Set(WireTime),
}

impl TimeChange {
    fn wire(self) -> WireTime {
        match self {
            Self::Set(time) => time,
            // Ignored unless the corresponding SET bit is present.
            Self::Retain | Self::Now => WireTime { sec: 0, nsec: 0 },
        }
    }
}

/// Optional attributes for `Tsetattrattr`; `None` and `Retain` leave fields unchanged.
#[derive(Clone, Copy, Default)]
pub(crate) struct SetAttributes {
    pub(crate) mode: Option<u32>,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) size: Option<u64>,
    pub(crate) atime: TimeChange,
    pub(crate) mtime: TimeChange,
}

impl SetAttributes {
    pub(crate) fn is_empty(&self) -> bool {
        self.valid_mask() == 0
    }

    fn valid_mask(&self) -> u32 {
        let mut valid = 0;
        if self.mode.is_some() {
            valid |= protocol::SETATTR_MODE;
        }
        if self.uid.is_some() {
            valid |= protocol::SETATTR_UID;
        }
        if self.gid.is_some() {
            valid |= protocol::SETATTR_GID;
        }
        if self.size.is_some() {
            valid |= protocol::SETATTR_SIZE;
        }
        valid |= match self.atime {
            TimeChange::Retain => 0,
            TimeChange::Now => protocol::SETATTR_ATIME,
            TimeChange::Set(_) => protocol::SETATTR_ATIME | protocol::SETATTR_ATIME_SET,
        };
        valid |= match self.mtime {
            TimeChange::Retain => 0,
            TimeChange::Now => protocol::SETATTR_MTIME,
            TimeChange::Set(_) => protocol::SETATTR_MTIME | protocol::SETATTR_MTIME_SET,
        };
        valid
    }
}

/// The conflicting lock an `Rgetlock` reported, or `LOCK_TYPE_UNLCK` when the
/// range is free.
///
/// The reply's `client_id` is dropped: it borrows the reply frame, copying it
/// out would allocate on the query path, and neither reference client looks at
/// it. `fs/9p` reports the holder as a negative pid and frees the string
/// unread.
#[derive(Clone, Copy)]
pub(crate) struct GetLock {
    pub(crate) lock_type: u8,
    pub(crate) start: u64,
    pub(crate) length: u64,
    pub(crate) proc_id: u32,
}

/// Owned result of the private compound walk/getattr operation.
pub(crate) struct WalkGetattr {
    pub(crate) qid_count: usize,
    pub(crate) final_qid: Option<Qid>,
    pub(crate) stat: Stat,
}

impl Client {
    /// Persist the server-selected atime for a VFS access.
    pub(crate) fn record_access_time(&self, fid: u32) -> Result<Stat> {
        let wire_fid = self.route_fid(fid)?;
        let mut durability = self.reserve_mutation(&[fid])?;
        let frame = self.transact_mutation(|envelope| Request::Tsetattrattr {
            envelope,
            fid: wire_fid,
            valid: protocol::SETATTR_ATIME_ACCESS | protocol::SETATTR_ATIME,
            mode: 0,
            uid: 0,
            gid: 0,
            size: 0,
            atime_sec: 0,
            atime_nsec: 0,
            mtime_sec: 0,
            mtime_nsec: 0,
        })?;
        let stat = expect_body!(self, &frame, Response::Rsetattrattr(stat) => stat)?;
        durability.record(frame.lineage_token)?;
        Ok(stat)
    }

    /// Rebind `fid` to an inode below the global root as `credentials`.
    pub(crate) fn rebind(
        &self,
        fid: u32,
        inode_id: u64,
        credentials: &RebindCredentials,
    ) -> Result<Rrebind> {
        let wire_fid = self.route_fid(fid)?;
        let credential = self.session().intern_credential(credentials)?;
        // One attempt spans every pass, so a rebind that keeps losing its
        // connection is bounded by the reconnect grace rather than restarting
        // that budget on each pass.
        let mut attempt = OpAttempt::new(false);
        (|| loop {
            let frame = self.resend_loop(
                &mut attempt,
                |_| Request::Trebind {
                    fid: wire_fid,
                    inode_id,
                    root_inode: ROOT_INODE_ID,
                    flags: 0,
                    uname: credentials.payload(),
                    n_uname: credentials.fsuid,
                },
                None,
            )?;
            let rebind = expect_body!(self, &frame, Response::Rrebind(rebind) => rebind)?;
            // The identity bound is the one sent, not the qid read back; the
            // caller validates that qid and clunks on a mismatch.
            // The replacement did not receive the fid, so retry on it.
            if self
                .session()
                .record_fid(
                    fid,
                    FidRecord {
                        inode_id,
                        credential,
                        open_flags: 0,
                        opened: false,
                    },
                    frame.connection_epoch,
                )
                .should_retry()?
            {
                continue;
            }
            return Ok(rebind);
        })()
        .inspect_err(|_| self.session().release_credential(credential))
    }

    /// Walk `names` from `fid`, installing and describing `newfid`.
    pub(crate) fn walk_getattr(
        &self,
        fid: u32,
        newfid: u32,
        names: &[&[u8]],
    ) -> Result<WalkGetattr> {
        let wire_fid = self.route_fid(fid)?;
        let wire_newfid = self.route_fid(newfid)?;
        let expected_qids = names.len();
        let mut attempt = OpAttempt::new(false);
        loop {
            let frame = self.resend_loop(
                &mut attempt,
                |_| Request::Twalkgetattr {
                    fid: wire_fid,
                    newfid: wire_newfid,
                    names,
                },
                None,
            )?;
            let walk = expect_body!(self, &frame, Response::Rwalkgetattr(walk)
                if walk.qids.len() == expected_qids => walk)?;
            let final_qid = if walk.qids.is_empty() {
                None
            } else {
                walk.qids.get(walk.qids.len().saturating_sub(1))
            };
            // Rwalkgetattr only replies for a walk that resolved every name,
            // so reaching here means newfid is installed. Replay rebinds it by
            // identity, never by repeating the walk.
            // Repeat the walk to install newfid on the replacement.
            if self
                .session()
                .record_derived_fid(
                    fid,
                    newfid,
                    final_qid.map(|qid| qid.path),
                    None,
                    frame.connection_epoch,
                )
                .should_retry()?
            {
                continue;
            }
            return Ok(WalkGetattr {
                qid_count: walk.qids.len(),
                final_qid,
                stat: walk.stat,
            });
        }
    }

    pub(crate) fn getattr(&self, fid: u32) -> Result<Rgetattr> {
        let wire_fid = self.route_fid(fid)?;
        let frame = self.transact(|| Request::Tgetattr {
            fid: wire_fid,
            request_mask: GETATTR_ALL,
        })?;
        expect_body!(self, &frame, Response::Rgetattr(attributes)
            if attributes.valid & GETATTR_ALL == GETATTR_ALL => attributes)
    }

    /// Update selected inode attributes and return the authoritative post-op stat.
    pub(crate) fn setattrattr(&self, fid: u32, attributes: &SetAttributes) -> Result<Stat> {
        let valid = attributes.valid_mask();
        let atime = attributes.atime.wire();
        let mtime = attributes.mtime.wire();

        let wire_fid = self.route_fid(fid)?;
        let mut durability = self.reserve_mutation(&[fid])?;
        let frame = self.transact_mutation(|envelope| Request::Tsetattrattr {
            envelope,
            fid: wire_fid,
            valid,
            mode: attributes.mode.unwrap_or(0),
            uid: attributes.uid.unwrap_or(0),
            gid: attributes.gid.unwrap_or(0),
            size: attributes.size.unwrap_or(0),
            atime_sec: atime.sec,
            atime_nsec: atime.nsec,
            mtime_sec: mtime.sec,
            mtime_nsec: mtime.nsec,
        })?;
        let stat = expect_body!(self, &frame, Response::Rsetattrattr(stat) => stat)?;
        durability.record(frame.lineage_token)?;
        Ok(stat)
    }

    /// Atomically allocate, punch, or zero one regular-file range.
    pub(crate) fn fallocate(&self, fid: u32, offset: u64, length: u64, mode: u32) -> Result<()> {
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(EINVAL);
        }
        if !is_supported_fallocate_mode(mode) {
            return Err(errno!(EOPNOTSUPP));
        }

        let wire_fid = self.route_fid(fid)?;
        let mut durability = self.reserve_mutation(&[fid])?;
        let frame = self.transact_mutation(|envelope| Request::Tfallocate {
            envelope,
            fid: wire_fid,
            offset,
            length,
            mode,
        })?;
        expect_body!(self, &frame, Response::Rfallocate => ())?;
        durability.record(frame.lineage_token)?;
        Ok(())
    }

    /// Open `fid` as `newfid` with the supplied protocol flags.
    pub(crate) fn openat(&self, fid: u32, newfid: u32, flags: u32) -> Result<Rlopen> {
        let wire_fid = self.route_fid(fid)?;
        let wire_newfid = self.route_fid(newfid)?;
        let mut attempt = OpAttempt::new(false);
        loop {
            let frame = self.resend_loop(
                &mut attempt,
                |_| Request::Tlopenat {
                    fid: wire_fid,
                    newfid: wire_newfid,
                    flags,
                },
                None,
            )?;
            let open = expect_body!(self, &frame, Response::Rlopenat(open) => open)?;
            // The open lands on newfid; the source fid stays unopened, so only
            // newfid gets a Tlopen on replay. The VFS supplies access-mode bits
            // only, so replay cannot reapply O_TRUNC.
            if self
                .session()
                .record_derived_fid(
                    fid,
                    newfid,
                    Some(open.qid.path),
                    Some(flags),
                    frame.connection_epoch,
                )
                .should_retry()?
            {
                continue;
            }
            return Ok(open);
        }
    }

    /// Open `fid` as `newfid` and prefetch from offset zero.
    pub(crate) fn openat_read(
        &self,
        fid: u32,
        newfid: u32,
        flags: u32,
        count: u32,
    ) -> Result<OpenReadPayload<'_>> {
        let count = count.min(protocol::max_lopenatread_payload(self.negotiated_msize()));
        if count == 0 {
            return Err(message_size_errno());
        }
        let wire_fid = self.route_fid(fid)?;
        let wire_newfid = self.route_fid(newfid)?;
        let mut attempt = OpAttempt::new(false);
        loop {
            let frame = self.resend_loop(
                &mut attempt,
                |_| Request::Tlopenatread {
                    fid: wire_fid,
                    newfid: wire_newfid,
                    flags,
                    count,
                },
                None,
            )?;
            let (open, eof, length) = expect_body!(
                self,
                &frame,
                Response::Rlopenatread(reply) if reply.eof <= 1 && reply.data.len() <= count as usize => (
                    Rlopen {
                        qid: reply.qid,
                        iounit: reply.iounit,
                    },
                    reply.eof != 0,
                    reply.data.len(),
                )
            )?;
            // Reconnect replay needs the open capability, not the disposable
            // prefetch.
            if self
                .session()
                .record_derived_fid(
                    fid,
                    newfid,
                    Some(open.qid.path),
                    Some(flags),
                    frame.connection_epoch,
                )
                .should_retry()?
            {
                continue;
            }
            return Ok(OpenReadPayload {
                open,
                eof,
                payload: payload_from_frame(frame, protocol::RLOPENATREAD_OVERHEAD, length)?,
            });
        }
    }

    /// Create and open `name` on `newfid`, leaving the directory fid unchanged.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lcreateattr(
        &self,
        dfid: u32,
        newfid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> Result<Rlcreateattr> {
        validate_name(name)?;
        let wire_dfid = self.route_fid(dfid)?;
        let wire_newfid = self.route_fid(newfid)?;
        let mut durability = self.reserve_mutation(&[dfid])?;
        // Reopening with the create flags would either re-truncate a file that
        // has since been written or fail the whole replay with EEXIST.
        let reopen = flags & !(bindings::O_CREAT | bindings::O_EXCL | bindings::O_TRUNC);
        // The attempt spans every pass, so a pass that has to create the file
        // again on a replacement carries the original operation ID and the
        // server's dedup map answers it with the first create's result.
        let mut attempt = OpAttempt::new(true);
        loop {
            let frame = self.resend_loop(
                &mut attempt,
                |envelope| Request::Tlcreateattr {
                    envelope,
                    dfid: wire_dfid,
                    newfid: wire_newfid,
                    name,
                    flags,
                    mode,
                    gid,
                },
                None,
            )?;
            let created = expect_body!(self, &frame, Response::Rlcreateattr(created) => created)?;
            if self
                .session()
                .record_derived_fid(
                    dfid,
                    newfid,
                    Some(created.stat.qid.path),
                    Some(reopen),
                    frame.connection_epoch,
                )
                .should_retry()?
            {
                continue;
            }
            // Record the mutation against the connection that returned this reply.
            durability.record(frame.lineage_token)?;
            return Ok(created);
        }
    }

    /// Create a directory and return its authoritative post-op stat.
    pub(crate) fn mkdirattr(&self, dfid: u32, name: &[u8], mode: u32, gid: u32) -> Result<Stat> {
        validate_name(name)?;
        let wire_dfid = self.route_fid(dfid)?;
        let mut durability = self.reserve_mutation(&[dfid])?;
        let frame = self.transact_mutation(|envelope| Request::Tmkdirattr {
            envelope,
            dfid: wire_dfid,
            name,
            mode,
            gid,
        })?;
        let stat = expect_body!(self, &frame, Response::Rmkdirattr(stat) => stat)?;
        durability.record(frame.lineage_token)?;
        Ok(stat)
    }

    /// Create a symlink and return its authoritative post-op stat.
    pub(crate) fn symlinkattr(
        &self,
        dfid: u32,
        name: &[u8],
        target: &[u8],
        gid: u32,
    ) -> Result<Stat> {
        validate_name(name)?;
        let wire_dfid = self.route_fid(dfid)?;
        let mut durability = self.reserve_mutation(&[dfid])?;
        let frame = self.transact_mutation(|envelope| Request::Tsymlinkattr {
            envelope,
            dfid: wire_dfid,
            name,
            target,
            gid,
        })?;
        let stat = expect_body!(self, &frame, Response::Rsymlinkattr(stat) => stat)?;
        durability.record(frame.lineage_token)?;
        Ok(stat)
    }

    /// Create a special node and return its authoritative post-op stat.
    pub(crate) fn mknodattr(
        &self,
        dfid: u32,
        name: &[u8],
        mode: u32,
        device: DeviceNumber,
        gid: u32,
    ) -> Result<Stat> {
        validate_name(name)?;
        let wire_dfid = self.route_fid(dfid)?;
        let mut durability = self.reserve_mutation(&[dfid])?;
        let frame = self.transact_mutation(|envelope| Request::Tmknodattr {
            envelope,
            dfid: wire_dfid,
            name,
            mode,
            major: device.major,
            minor: device.minor,
            gid,
        })?;
        let stat = expect_body!(self, &frame, Response::Rmknodattr(stat) => stat)?;
        durability.record(frame.lineage_token)?;
        Ok(stat)
    }

    /// Add a hard link and return the linked inode's authoritative stat.
    pub(crate) fn linkattr(&self, dfid: u32, fid: u32, name: &[u8]) -> Result<Stat> {
        validate_name(name)?;
        let wire_dfid = self.route_fid(dfid)?;
        let wire_fid = self.route_fid(fid)?;
        let mut durability = self.reserve_mutation(&[dfid])?;
        let frame = self.transact_mutation(|envelope| Request::Tlinkattr {
            envelope,
            dfid: wire_dfid,
            fid: wire_fid,
            name,
        })?;
        let stat = expect_body!(self, &frame, Response::Rlinkattr(stat) => stat)?;
        durability.record(frame.lineage_token)?;
        Ok(stat)
    }

    /// Atomically rename an entry. The wire operation has no rename flags.
    pub(crate) fn renameat(
        &self,
        olddirfid: u32,
        oldname: &[u8],
        newdirfid: u32,
        newname: &[u8],
    ) -> Result<()> {
        validate_name(oldname)?;
        validate_name(newname)?;
        let wire_olddirfid = self.route_fid(olddirfid)?;
        let wire_newdirfid = self.route_fid(newdirfid)?;
        let mut durability = self.reserve_mutation(&[newdirfid, olddirfid])?;
        let frame = self.transact_mutation(|envelope| Request::Trenameat {
            envelope,
            olddirfid: wire_olddirfid,
            oldname,
            newdirfid: wire_newdirfid,
            newname,
        })?;
        expect_body!(self, &frame, Response::Rrenameat => ())?;
        durability.record(frame.lineage_token)?;
        Ok(())
    }

    /// Remove one entry; only zero or `AT_REMOVEDIR` are valid flags.
    pub(crate) fn unlinkat(&self, dirfid: u32, name: &[u8], flags: u32) -> Result<()> {
        validate_name(name)?;
        if !matches!(flags, 0 | protocol::AT_REMOVEDIR) {
            return Err(EINVAL);
        }
        let wire_dirfid = self.route_fid(dirfid)?;
        let mut durability = self.reserve_mutation(&[dirfid])?;
        let frame = self.transact_mutation(|envelope| Request::Tunlinkat {
            envelope,
            dirfid: wire_dirfid,
            name,
            flags,
        })?;
        expect_body!(self, &frame, Response::Runlinkat => ())?;
        durability.record(frame.lineage_token)?;
        Ok(())
    }

    /// Fetch one symlink target into a response-credit-owned frame.
    pub(crate) fn readlink(&self, fid: u32) -> Result<OwnedPayload<'_>> {
        let wire_fid = self.route_fid(fid)?;
        let frame = self.transact(|| Request::Treadlink { fid: wire_fid })?;
        let length =
            expect_body!(self, &frame, Response::Rreadlink(readlink) => readlink.target.len())?;
        // Rreadlink carries header[7], length[2], target[length].
        payload_from_frame(frame, HEADER_SIZE + mem::size_of::<u16>(), length)
    }

    /// Write one payload directly off its owner's iterator and return the
    /// server-acknowledged byte count.
    pub(crate) fn write(&self, fid: u32, offset: u64, payload: PayloadIter<'_>) -> Result<usize> {
        let length = payload.len();
        if length > protocol::max_write_payload(self.negotiated_msize()) as usize {
            return Err(message_size_errno());
        }

        let wire_fid = self.route_fid(fid)?;
        let mut durability = if length == 0 {
            None
        } else {
            Some(self.reserve_mutation(&[fid])?)
        };
        let frame = self.transact_write(wire_fid, offset, &payload)?;
        let count = expect_body!(self, &frame, Response::Rwrite(write)
            if write.count as usize <= length => write.count as usize)?;
        if length != 0 && count == 0 {
            return Err(EIO);
        }
        if count != 0 {
            let Some(durability) = durability.as_mut() else {
                return self.invariant_failure();
            };
            durability.record(frame.lineage_token)?;
        }
        Ok(count)
    }

    /// Read at most `count` bytes into an owned response frame.
    pub(crate) fn read(&self, fid: u32, offset: u64, count: u32) -> Result<OwnedPayload<'_>> {
        self.validate_reply_payload_count(count)?;
        let wire_fid = self.route_fid(fid)?;
        let frame = self.transact(|| Request::Tread {
            fid: wire_fid,
            offset,
            count,
        })?;
        let length = expect_body!(self, &frame, Response::Rread(read)
            if read.data.len() <= count as usize => read.data.len())?;
        payload_from_frame(frame, READ_PAYLOAD_OFFSET, length)
    }

    /// Read at most `count` bytes straight into a netfslib destination.
    ///
    /// Returns the bytes placed in `destination`, which is advanced by exactly
    /// that many. Whichever task drains the socket writes the payload into the
    /// iterator when it can; otherwise this falls back to the owned-frame copy.
    pub(crate) fn read_into(
        &self,
        fid: u32,
        offset: u64,
        count: u32,
        destination: &mut ReplyDestination<'_>,
    ) -> Result<usize> {
        self.validate_reply_payload_count(count)?;
        let wire_fid = self.route_fid(fid)?;
        // The receiver can fill but cannot inspect or grow the registered
        // iterator, so the complete declared payload must fit.
        let registration = (destination.remaining() >= count as usize)
            .then(|| destination.registration(count as usize));
        let frame = self.transact_registered(
            || Request::Tread {
                fid: wire_fid,
                offset,
                count,
            },
            registration,
        )?;
        if let Some(delivered) = frame.delivered {
            // Leave the iterator where copy_to_iter would have, which is what
            // netfs_clear_unread() zeroes from.
            destination.advance(delivered);
            return Ok(delivered);
        }

        let length = expect_body!(self, &frame, Response::Rread(read)
            if read.data.len() <= count as usize => read.data.len())?;
        let end = READ_PAYLOAD_OFFSET
            .checked_add(length)
            .ok_or_else(protocol_errno)?;
        let payload = frame
            .bytes
            .as_slice()
            .get(READ_PAYLOAD_OFFSET..end)
            .ok_or_else(protocol_errno)?;
        if destination.copy(payload) != length {
            return Err(EFAULT);
        }
        Ok(length)
    }

    /// Read stat-bearing directory entries into an owned response frame.
    pub(crate) fn readdirattr(
        &self,
        fid: u32,
        offset: u64,
        count: u32,
    ) -> Result<OwnedPayload<'_>> {
        self.validate_reply_payload_count(count)?;
        let wire_fid = self.route_fid(fid)?;
        let frame = self.transact(|| Request::Treaddirattr {
            fid: wire_fid,
            offset,
            count,
        })?;
        let length = expect_body!(self, &frame, Response::Rreaddirattr(directory)
            if directory.data().len() <= count as usize => directory.data().len())?;
        payload_from_frame(frame, HEADER_SIZE + mem::size_of::<u32>(), length)
    }

    /// Release one caller-owned installed fid after its final use.
    ///
    /// Callers must clunk each fid exactly once. Reuse is published only after
    /// a matching, fully decoded `Rclunk`, or after the fid is known to be
    /// gone; any other failure leaves the number unavailable and its record in
    /// place, so a later replay rebinds it.
    pub(crate) fn clunk(&self, fid: u32) -> Result<()> {
        let wire_fid = self.route_fid(fid)?;
        // A tombstoned fid names an object the server no longer has. close()
        // must still succeed so the number can be recycled, and sending on it
        // would only earn EBADF.
        if self.session().clear_stale_fid(fid)? {
            return Ok(());
        }
        let mut attempt = OpAttempt::new(false);
        loop {
            let result = (|| {
                let frame =
                    self.resend_loop(&mut attempt, |_| Request::Tclunk { fid: wire_fid }, None)?;
                expect_body!(self, &frame, Response::Rclunk => frame.connection_epoch)
            })();

            match result {
                Ok(epoch) => {
                    // A replacement that replayed this fid holds it installed,
                    // and open again if it was open, so the release only counts
                    // once the connection that currently holds it clunks it.
                    if self.session().retire_clunked_fid(fid, Some(epoch)) {
                        return Ok(());
                    }
                }
                Err(error) if error == errno!(ESTALE) => {
                    // The server says this fid is gone, so releasing it locally
                    // is exactly what happened.
                    self.session().retire_clunked_fid(fid, None);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn statfs(&self, fid: u32) -> Result<Rstatfs> {
        let wire_fid = self.route_fid(fid)?;
        let frame = self.transact(|| Request::Tstatfs { fid: wire_fid })?;
        expect_body!(self, &frame, Response::Rstatfs(statfs) => statfs)
    }

    /// Acquire one byte range, recording what the server granted.
    ///
    /// Returns the raw `LOCK_*` status. The server never waits, so a blocking
    /// request that conflicts is answered `LOCK_BLOCKED` and the caller owns the
    /// wait; a non-blocking conflict is an `Rlerror`, so it arrives as
    /// `Err(EAGAIN)`. A status this client does not know is passed through
    /// rather than treated as a desync, which is what lets the caller answer
    /// `ENOLCK` for it as `fs/9p` does.
    ///
    /// A `Tlock` carries no mutation envelope: the server decodes neither an
    /// operation ID nor a retry flag for it, and it needs neither, because a
    /// resend of the same range on the same fid replaces in place and an unlock
    /// is idempotent.
    ///
    /// `flock` distinguishes a BSD lock from a record lock. The two are separate
    /// objects locally but one owner on the server, which is why a fid that
    /// holds one of them refuses the other.
    pub(crate) fn lock(
        &self,
        fid: u32,
        lock_type: u8,
        flags: u32,
        range: LockRange,
        owner: LockOwner,
    ) -> Result<u8> {
        validate_lock_type(lock_type)?;
        if lock_type == protocol::LOCK_TYPE_UNLCK {
            // Releasing goes through reserve_unlock, whose claim its caller has
            // to take before giving up any local state.
            return Err(EINVAL);
        }
        if flags & !protocol::LOCK_FLAGS_BLOCK != 0 {
            return Err(EINVAL);
        }
        // Granting this would give the fid ranges from both APIs, and a later
        // release under either one would silently cut the other's ranges down
        // on the server while the VFS still reports them held. fs/9p reaches
        // the same refusal by converting BSD locks to POSIX ones, which makes
        // the two collide locally instead.
        if self.session().fid_holds_other_flavor(fid, owner.flock) {
            return Err(errno!(ENOLCK));
        }
        // Reserved ahead of the request so that storing what the server grants
        // cannot fail: an acquire subtracts the covered range, which splits at
        // most one record, and then stores the new one.
        let claim = self.session().reserve_lock_slots(2)?;
        self.transact_lock(claim, fid, lock_type, flags, range, owner)
    }

    /// Reserve the record-table space a later [`Client::unlock`] may need.
    ///
    /// The records of one fid are disjoint, so an unlock splits at most one of
    /// them and needs one free slot. Taking it up front is what lets a caller
    /// that cannot undo its own local release find out first: the table is
    /// bounded, so this is the one part of an unlock that can be refused, and it
    /// is refused with nothing sent and no record touched.
    ///
    /// `Ok(None)` means this flavor holds nothing on the fid, so the release has
    /// no counterpart to send: the fid's ranges belong to the other API, and
    /// subtracting from them is exactly the damage [`Client::lock`] refuses to
    /// set up. The caller still releases locally.
    pub(crate) fn reserve_unlock(&self, fid: u32, flock: bool) -> Result<Option<UnlockSlot<'_>>> {
        if self.session().fid_holds_other_flavor(fid, flock) {
            return Ok(None);
        }
        self.session()
            .reserve_lock_slots(1)
            .map(|claim| Some(UnlockSlot(claim)))
    }

    /// Release one byte range, pruning what the server acknowledges.
    pub(crate) fn unlock(
        &self,
        slot: UnlockSlot<'_>,
        fid: u32,
        range: LockRange,
        owner: LockOwner,
    ) -> Result<u8> {
        self.transact_lock(slot.0, fid, protocol::LOCK_TYPE_UNLCK, 0, range, owner)
    }

    /// Send one `Tlock` against space the record set has already promised.
    fn transact_lock(
        &self,
        mut claim: LockSlotClaim<'_>,
        fid: u32,
        lock_type: u8,
        flags: u32,
        range: LockRange,
        owner: LockOwner,
    ) -> Result<u8> {
        let wire_fid = self.route_fid(fid)?;
        let client_id = self.session().client_id();
        // One attempt spans every pass, so a lock that keeps losing its
        // connection is bounded by the reconnect grace rather than restarting
        // that budget on each pass.
        let mut attempt = OpAttempt::new(false);
        loop {
            let frame = self.resend_loop(
                &mut attempt,
                |_| Request::Tlock {
                    fid: wire_fid,
                    lock_type,
                    flags,
                    start: range.start,
                    length: range.length,
                    proc_id: owner.proc_id,
                    client_id,
                },
                None,
            )?;
            let status = expect_body!(self, &frame, Response::Rlock(lock) => lock.status)?;
            // Only a granted lock changes the record set. Recording a refused
            // one would have replay acquire a range the application never held.
            if status != protocol::LOCK_SUCCESS {
                return Ok(status);
            }
            let record = LockRecord {
                fid,
                lock_type,
                start: range.start,
                length: range.length,
                proc_id: owner.proc_id,
                flock: owner.flock,
            };
            match self
                .session()
                .record_lock(&mut claim, record, frame.connection_epoch)
            {
                LockRecorded::Installed => return Ok(status),
                // Replay rebuilt the record set on the replacement without this
                // lock, so the replacement does not hold it. Asking again there
                // is the only way the record and the server agree.
                LockRecorded::Superseded => continue,
                // Unreachable: the claim above reserved the worst case. The
                // server holds a lock nothing would ever reacquire, so end the
                // session rather than report a lock this client cannot keep.
                LockRecorded::Unrecorded => {
                    let error = errno!(ENOLCK);
                    pr_err!("zerofs: granted lock could not be recorded; ending session\n");
                    self.session().terminate(error);
                    return Err(error);
                }
            }
        }
    }

    /// Report the lock that would conflict with this range, if any.
    ///
    /// A pure query, so it records nothing and takes no epoch witness: a reply
    /// from a connection since replaced is stale advice, not lost state, which
    /// is exactly why the userspace client sends it outside its session
    /// transition guard.
    pub(crate) fn getlock(
        &self,
        fid: u32,
        lock_type: u8,
        range: LockRange,
        proc_id: u32,
    ) -> Result<GetLock> {
        validate_lock_type(lock_type)?;
        let wire_fid = self.route_fid(fid)?;
        let client_id = self.session().client_id();
        let frame = self.transact(|| Request::Tgetlock {
            fid: wire_fid,
            lock_type,
            start: range.start,
            length: range.length,
            proc_id,
            client_id,
        })?;
        expect_body!(self, &frame, Response::Rgetlock(holder) => GetLock {
            lock_type: holder.lock_type,
            start: holder.start,
            length: holder.length,
            proc_id: holder.proc_id,
        })
    }

    fn validate_reply_payload_count(&self, count: u32) -> Result<()> {
        // Rread and Rreaddirattr both carry header[7], count[4], data[count].
        let maximum = self
            .negotiated_msize()
            .checked_sub((HEADER_SIZE + mem::size_of::<u32>()) as u32)
            .ok_or_else(message_size_errno)?;
        if count > maximum {
            Err(message_size_errno())
        } else {
            Ok(())
        }
    }
}

fn payload_from_frame<'a>(
    frame: OwnedFrame<'a>,
    offset: usize,
    length: usize,
) -> Result<OwnedPayload<'a>> {
    let end = offset.checked_add(length).ok_or_else(protocol_errno)?;
    if frame.bytes.as_slice().get(offset..end).is_none() {
        return Err(protocol_errno());
    }
    Ok(OwnedPayload {
        frame: frame.bytes,
        offset,
        length,
        _credit: frame._credit,
    })
}

fn validate_name(name: &[u8]) -> Result<()> {
    if name.is_empty() {
        Err(EINVAL)
    } else if name.len() > protocol::MAX_NAME_LEN {
        Err(errno!(ENAMETOOLONG))
    } else if name.contains(&b'/') || name.contains(&b'\0') {
        Err(EINVAL)
    } else {
        Ok(())
    }
}

/// Reject a lock type the wire does not define before it reaches the encoder.
fn validate_lock_type(lock_type: u8) -> Result<()> {
    if matches!(
        lock_type,
        protocol::LOCK_TYPE_RDLCK | protocol::LOCK_TYPE_WRLCK | protocol::LOCK_TYPE_UNLCK
    ) {
        Ok(())
    } else {
        Err(EINVAL)
    }
}

fn is_supported_fallocate_mode(mode: u32) -> bool {
    mode == 0
        || mode == (protocol::FALLOC_FL_PUNCH_HOLE | protocol::FALLOC_FL_KEEP_SIZE)
        || mode == protocol::FALLOC_FL_ZERO_RANGE
        || mode == (protocol::FALLOC_FL_ZERO_RANGE | protocol::FALLOC_FL_KEEP_SIZE)
}
