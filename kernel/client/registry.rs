use core::{mem, ptr};

use kernel::{
    alloc::{KVVec, flags::GFP_KERNEL},
    bindings,
    cred::Credential,
    prelude::*,
};

use crate::protocol::{self, Request};

use super::errors::not_connected_errno;
use super::session::{Session, SessionState, SessionStatus};
use super::{Client, MAX_LOCK_RECORDS, REBIND_CREDENTIAL_IDENTITY_WORDS, ROOT_FID, ROOT_INODE_ID};

/// Immutable filesystem-credential snapshot carried by a private `Trebind`.
///
/// Keeping this separate from `Client::rebind` is important: pathname
/// operations snapshot the calling task once, while open snapshots
/// `file->f_cred`. The latter remains the identity of an inherited or passed
/// file descriptor even if the task later changes fsuid or supplementary
/// groups.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RebindCredentials {
    pub(super) fsuid: u32,
    payload: [u8; protocol::REBIND_CREDENTIAL_MAX_SIZE],
    payload_len: usize,
}

impl RebindCredentials {
    /// Snapshot the calling task's subjective filesystem credentials.
    pub(crate) fn current() -> Result<Self> {
        // SAFETY: get_current() returns the calling task. Its subjective
        // credential pointer is non-null and stable for this immediate,
        // non-sleeping copy.
        let credential = unsafe { (*bindings::get_current()).cred };
        if credential.is_null() {
            return Err(EINVAL);
        }
        // SAFETY: The current task retains this immutable credential while it
        // is copied synchronously below.
        Self::from_credential(unsafe { Credential::from_ptr(credential) })
    }

    /// Snapshot a referenced, immutable Linux credential.
    pub(crate) fn from_credential(credential: &Credential) -> Result<Self> {
        let mut payload = [0u8; protocol::REBIND_CREDENTIAL_MAX_SIZE];
        payload[0] = protocol::REBIND_CREDENTIAL_SENTINEL;
        payload[1] = protocol::REBIND_CREDENTIAL_VERSION;

        let credential = credential.as_ptr();
        // SAFETY: Credential's type invariant keeps the published immutable
        // object live for this copy.
        let (fsuid, fsgid, group_info) = unsafe {
            (
                (*credential).fsuid.val,
                (*credential).fsgid.val,
                (*credential).group_info,
            )
        };
        payload[2..6].copy_from_slice(&fsgid.to_le_bytes());

        // Linux has already applied local VFS DAC. If more groups exist than
        // the private wire format can carry, mark the list incomplete so the
        // server does not reject access granted by an omitted group.
        let (group_count, groups_complete) = if group_info.is_null() {
            (0, true)
        } else {
            // SAFETY: group_info is owned by the referenced immutable
            // credential described above.
            let count = unsafe { (*group_info).ngroups };
            if count <= 0 {
                (0, true)
            } else {
                let count = count as usize;
                (
                    count.min(protocol::P9_MAX_GROUPS),
                    count <= protocol::P9_MAX_GROUPS,
                )
            }
        };
        payload[6] = group_count as u8
            | if groups_complete {
                0
            } else {
                protocol::REBIND_CREDENTIAL_GROUPS_INCOMPLETE
            };

        for index in 0..group_count {
            // SAFETY: index is below the snapshotted, clamped ngroups count.
            let gid = unsafe { ptr::read((*group_info).gid.as_ptr().add(index)) };
            let offset = protocol::REBIND_CREDENTIAL_HEADER_SIZE + index * 4;
            payload[offset..offset + 4].copy_from_slice(&gid.val.to_le_bytes());
        }

        Ok(Self {
            fsuid,
            payload,
            payload_len: protocol::REBIND_CREDENTIAL_HEADER_SIZE + group_count * 4,
        })
    }

    pub(super) fn payload(&self) -> &[u8] {
        &self.payload[..self.payload_len]
    }

    /// Fixed-width representation for exact lockless identity comparisons.
    pub(crate) fn identity_words(&self) -> ([u64; REBIND_CREDENTIAL_IDENTITY_WORDS], usize) {
        let mut bytes = [0u8; mem::size_of::<u32>() + protocol::REBIND_CREDENTIAL_MAX_SIZE];
        let length = mem::size_of::<u32>() + self.payload_len;
        bytes[..4].copy_from_slice(&self.fsuid.to_le_bytes());
        bytes[4..length].copy_from_slice(self.payload());

        let mut words = [0u64; REBIND_CREDENTIAL_IDENTITY_WORDS];
        for (word, chunk) in words.iter_mut().zip(bytes[..length].chunks(8)) {
            let mut padded = [0u8; 8];
            padded[..chunk.len()].copy_from_slice(chunk);
            *word = u64::from_le_bytes(padded);
        }
        (words, length)
    }
}

/// One interned filesystem identity, shared by every fid created under it.
///
/// [`RebindCredentials`] embeds a fixed payload array, so a copy per fid record
/// would cost far more than the record itself.
#[derive(Clone)]
pub(super) struct CredentialSlot {
    pub(super) credentials: RebindCredentials,
    /// Live records naming this slot. Zero retires it for reuse.
    references: u64,
}

/// Everything a `Trebind`, plus an optional reopen, needs to rebuild one fid
/// on a replacement connection.
///
/// The server copies a source fid's credentials into any fid derived from it,
/// so a record keeps the identity its fid was created under rather than the
/// identity of whoever uses it later.
#[derive(Clone, Copy)]
pub(super) struct FidRecord {
    pub(super) inode_id: u64,
    /// Index into `SessionState::credentials`.
    pub(super) credential: usize,
    /// Reopen flags, meaningful only while `opened`.
    pub(super) open_flags: u32,
    pub(super) opened: bool,
}

/// One entry of the fid-indexed replay table.
#[derive(Clone, Copy)]
pub(super) enum FidSlot {
    /// The number is unissued, or its fid has been clunked.
    Vacant,
    Live(FidRecord),
    /// An opened fid whose inode no longer exists.
    ///
    /// Its operations report `ESTALE` without affecting any other fid, and the
    /// number stays allocated to its caller until that caller closes it.
    Stale,
}

/// One granted byte range this session must reinstall on a replacement
/// connection.
///
/// The server hangs one lock guard off the fid slot, so a clunk or a lost
/// connection releases every lock the fid holds. A record is exactly what the
/// replacement has to be told about.
///
/// No `client_id` field: the kernel client has one identity for the whole
/// mount, held on [`Session`], so a copy per record could only ever hold the
/// same bytes. The userspace client stores it per record because its public
/// `lock()` takes one per call.
///
/// No flags field either, matching userspace: replay always asks
/// non-blocking, so recording the blocking bit would let one contended range
/// park the connection task inside the server's grant path.
///
/// One field the wire does not carry: `flock`. The server has no BSD mode, so
/// both families reach it as byte ranges on the same fid, which makes them one
/// owner there while they stay independent locks locally. The bit is what lets
/// a fid refuse to hold both at once.
#[derive(Clone, Copy)]
pub(super) struct LockRecord {
    pub(super) fid: u32,
    pub(super) lock_type: u8,
    pub(super) start: u64,
    pub(super) length: u64,
    pub(super) proc_id: u32,
    pub(super) flock: bool,
}

/// Outcome of storing a lock against the connection that granted it.
pub(super) enum LockRecorded {
    Installed,
    /// The answering connection was replaced before the store, so replay
    /// rebuilt the record set without this lock and the replacement does not
    /// hold it. The caller asks the replacement instead.
    Superseded,
    /// The reserved slot was gone by the time the reply landed, which the
    /// reservation makes unreachable. The lock is held on the server and
    /// nothing will reacquire it, so the caller must not report success.
    Unrecorded,
}

/// Outcome of storing a record against the connection that installed the fid.
pub(super) enum FidRecorded {
    /// The record is installed and the fid is usable by its caller.
    Installed,
    /// The answering connection was replaced before the store, so replay's
    /// snapshot never saw this fid and the replacement does not have it.
    Superseded,
    /// The fid cannot be rebuilt and is now a tombstone.
    Tombstoned,
}

impl FidRecorded {
    /// Return whether to retry on the replacement; tombstones fail with `ESTALE`.
    pub(super) fn should_retry(self) -> Result<bool> {
        match self {
            Self::Installed => Ok(false),
            Self::Superseded => Ok(true),
            Self::Tombstoned => Err(errno!(ESTALE)),
        }
    }
}

impl Client {
    /// Allocate a unique session-local fid.
    ///
    /// A fid is reusable only after a successful `Rclunk`. Both the recycled
    /// pool and monotonic fallback are protected by the session.
    pub(crate) fn allocate_fid(&self) -> Result<u32> {
        loop {
            match allocate_session_fid(self.session(), u32::MAX - 1)? {
                FidAllocation::Allocated(fid) => return Ok(fid),
                FidAllocation::NeedsRecord(fid) => self.session().reserve_records(fid)?,
            }
        }
    }

    /// Reject the reserved wire values before a fid reaches the encoder.
    pub(super) fn route_fid(&self, fid: u32) -> Result<u32> {
        if fid == 0 || fid == u32::MAX {
            return Err(EINVAL);
        }
        Ok(fid)
    }
}

impl Session {
    /// Reserve a replay-record slot for every fid up to and including `fid`.
    ///
    /// Reserving before the number is issued is what lets the post-reply record
    /// store be infallible: a fid the server has already installed must never
    /// end up without a record.
    fn reserve_records(&self, fid: u32) -> Result<()> {
        let minimum = (fid as usize)
            .checked_add(1)
            .ok_or_else(|| EOVERFLOW)?;
        loop {
            let length = {
                let state = self.state.lock();
                if state.records.len() >= minimum {
                    return Ok(());
                }
                state.records.len()
            };
            let replacement = KVVec::from_elem(
                FidSlot::Vacant,
                grown_table_length(length, minimum)?,
                GFP_KERNEL,
            )?;
            let mut state = self.state.lock();
            adopt_longer_table(&mut state.records, replacement);
        }
    }

    /// Reserve the free lock-record slots one lock may need.
    ///
    /// Charging before the request is what makes the post-reply store
    /// infallible, for the same reason `reserve_records` reserves before a fid
    /// number is issued: a lock the server has granted must never end up
    /// without a record, because nothing would then reacquire it.
    ///
    /// The records of one fid are disjoint by construction, so an unlock splits
    /// at most one of them and needs one free slot, and any other lock subtracts
    /// and then pushes, so it needs two.
    pub(super) fn reserve_lock_slots(&self, needed: usize) -> Result<LockSlotClaim<'_>> {
        loop {
            let (length, minimum) = {
                let mut state = self.state.lock();
                if let SessionStatus::Dead(status) = state.status {
                    return Err(Error::from_errno(status));
                }
                let free = state.locks.iter().filter(|slot| slot.is_none()).count();
                let wanted = state
                    .lock_slots_claimed
                    .checked_add(needed)
                    .ok_or_else(|| errno!(ENOLCK))?;
                if free >= wanted {
                    state.lock_slots_claimed = wanted;
                    return Ok(LockSlotClaim {
                        session: self,
                        slots: needed,
                    });
                }
                (
                    state.locks.len(),
                    state.locks.len().saturating_add(wanted - free),
                )
            };
            // Refused before anything reaches the wire, so the server keeps
            // whatever it already granted and the record set still describes it.
            if minimum > MAX_LOCK_RECORDS {
                return Err(errno!(ENOLCK));
            }
            let replacement = KVVec::from_elem(
                None,
                grown_table_length(length, minimum)?.min(MAX_LOCK_RECORDS),
                GFP_KERNEL,
            )?;
            let mut state = self.state.lock();
            adopt_longer_table(&mut state.locks, replacement);
        }
    }

    /// Whether `fid` holds ranges taken through the other locking API.
    ///
    /// One fid is one lock owner on the server, so a release under either API
    /// subtracts from every range the fid holds. `flock` and `fcntl` locks are
    /// separate objects locally, so the flavor that did not ask would lose
    /// coverage with nothing to report it. This is what keeps the two from
    /// coexisting on one fid in the first place.
    pub(super) fn fid_holds_other_flavor(&self, fid: u32, flock: bool) -> bool {
        let state = self.state.lock();
        state
            .locks
            .iter()
            .any(|slot| matches!(slot, Some(record) if record.fid == fid && record.flock != flock))
    }

    /// Store one granted lock against the connection that granted it.
    ///
    /// The epoch check is what the userspace client gets from holding its
    /// session-transition mutex across the record update. Without it a lock
    /// granted on a retired connection could land after replay rebuilt the set,
    /// leaving the client believing in a lock the live connection never granted,
    /// and an acknowledged unlock could erase a record replay had just
    /// reinstalled, leaving a lock nothing releases.
    pub(super) fn record_lock(
        &self,
        claim: &mut LockSlotClaim<'_>,
        record: LockRecord,
        epoch: u64,
    ) -> LockRecorded {
        let mut state = self.state.lock();
        if state.connection_epoch != epoch {
            return LockRecorded::Superseded;
        }
        // POSIX replacement is a removal of the covered range followed by the
        // new lock, and an unlock is the removal alone. The predicate is the fid
        // and nothing else, matching the server, which replaces in place for the
        // same (session, fid) regardless of proc_id or lock type.
        let mut stored =
            unlock_recorded_range_locked(&mut state, record.fid, record.start, record.length);
        if record.lock_type != protocol::LOCK_TYPE_UNLCK {
            stored &= install_lock_locked(&mut state, record);
        }
        if !stored {
            return LockRecorded::Unrecorded;
        }
        claim.settle_locked(&mut state);
        LockRecorded::Installed
    }

    /// Record the root capability the bootstrap `Trebind` installed.
    ///
    /// Every other record inherits its identity from a fid that descends from
    /// this one, so nothing else may be recorded before it.
    pub(super) fn record_root_fid(&self, fid: u32, credentials: &RebindCredentials) -> Result<()> {
        self.reserve_records(fid)?;
        let credential = self.intern_credential(credentials)?;
        let epoch = self.state.lock().connection_epoch;
        let record = FidRecord {
            inode_id: ROOT_INODE_ID,
            credential,
            open_flags: 0,
            opened: false,
        };
        // Nothing can retire this connection before the session's threads
        // exist, so the store cannot be superseded here.
        match self.record_fid(fid, record, epoch) {
            FidRecorded::Installed => Ok(()),
            _ => {
                self.release_credential(credential);
                Err(not_connected_errno())
            }
        }
    }

    /// Take a reference to the interned form of `credentials`.
    ///
    /// Identity is compared exactly, never by hash: a collision would replay one
    /// user's fid under another user's authority.
    pub(super) fn intern_credential(&self, credentials: &RebindCredentials) -> Result<usize> {
        loop {
            let length = {
                let mut state = self.state.lock();
                if let SessionStatus::Dead(status) = state.status {
                    return Err(Error::from_errno(status));
                }
                if let Some(index) = intern_credential_locked(&mut state, credentials) {
                    return Ok(index);
                }
                state.credentials.len()
            };
            let minimum = length
                .checked_add(1)
                .ok_or_else(|| EOVERFLOW)?;
            let replacement =
                KVVec::from_elem(None, grown_table_length(length, minimum)?, GFP_KERNEL)?;
            let mut state = self.state.lock();
            adopt_longer_table(&mut state.credentials, replacement);
        }
    }

    pub(super) fn release_credential(&self, credential: usize) {
        let mut state = self.state.lock();
        release_credential_locked(&mut state, credential);
    }

    /// Record a fid created directly under `credential`, which this call
    /// consumes on `Installed` only.
    ///
    /// A connection replaced between the reply and here means replay's snapshot
    /// never saw this fid, so the replacement does not have it. The caller runs
    /// the operation again on the replacement rather than publishing a handle
    /// no connection holds; the credential reference stays with the caller for
    /// that next attempt.
    pub(super) fn record_fid(&self, fid: u32, record: FidRecord, epoch: u64) -> FidRecorded {
        let mut state = self.state.lock();
        if state.connection_epoch != epoch {
            return FidRecorded::Superseded;
        }
        install_record_locked(&mut state, fid, record);
        FidRecorded::Installed
    }

    /// Record `newfid` as created from `source`, inheriting its identity.
    ///
    /// `inode_id` is the qid the server reported, or `None` to keep the source
    /// inode when the operation walked no names. `opened` carries the reopen
    /// flags for a fid the server also opened.
    pub(super) fn record_derived_fid(
        &self,
        source: u32,
        newfid: u32,
        inode_id: Option<u64>,
        opened: Option<u32>,
        epoch: u64,
    ) -> FidRecorded {
        let mut state = self.state.lock();
        if state.connection_epoch != epoch {
            return FidRecorded::Superseded;
        }
        // Every fid descends from the root recorded at connect, so a missing
        // source is unreachable. Tombstoning the derived fid is the safe
        // response: it fails on its own instead of quietly becoming a live
        // handle that no replay can rebuild.
        let Some(&FidSlot::Live(parent)) = state.records.get(source as usize) else {
            tombstone_locked(&mut state, newfid);
            return FidRecorded::Tombstoned;
        };
        let record = FidRecord {
            inode_id: inode_id.unwrap_or(parent.inode_id),
            credential: parent.credential,
            open_flags: opened.unwrap_or(0),
            opened: opened.is_some(),
        };
        retain_credential_locked(&mut state, record.credential);
        install_record_locked(&mut state, newfid, record);
        FidRecorded::Installed
    }

    /// Drop `fid`'s record or tombstone and return its number to the pool.
    ///
    /// Both happen in one critical section so a recycled number can never be
    /// observed carrying the previous record, and clearing the tombstone is
    /// what stops a later allocation of the number inheriting its `ESTALE`.
    ///
    /// Returns whether the record was dropped.
    pub(super) fn retire_clunked_fid(&self, fid: u32, epoch: Option<u64>) -> bool {
        let mut state = self.state.lock();
        // A connection replaced between the Rclunk and here means replay
        // rebound this fid on the replacement, where it is installed and may be
        // open again. Forgetting the record would leave it held for that
        // connection's life, so the caller clunks it there instead.
        if epoch.is_some_and(|epoch| epoch != state.connection_epoch) {
            return false;
        }
        if let Some(displaced) = replace_slot_locked(&mut state, fid, FidSlot::Vacant) {
            release_displaced_locked(&mut state, displaced);
        }
        // The server drops this fid's locks with the fid, and the number may be
        // recycled to an unrelated inode below.
        forget_fid_locks_locked(&mut state, fid);
        // The root capability is installed for the mount's lifetime, and
        // u32::MAX is reserved on the wire, so neither may re-enter the pool.
        if fid <= ROOT_FID || fid == u32::MAX {
            return true;
        }
        // A dead session's fids are released by the server's connection guard,
        // and a number this session never issued was never ours to reuse. A
        // retired connection is transient, so its numbers stay reusable.
        if matches!(state.status, SessionStatus::Dead(_)) || fid >= state.next_fid {
            return true;
        }
        // The pool is only an optimization; a dropped number is simply never
        // reused by this session.
        let _ = state.recycled_fids.push(fid, GFP_KERNEL);
        true
    }

    /// Release a tombstoned fid without touching the wire.
    ///
    /// Returns whether `fid` was tombstoned. A terminal session still reports
    /// its errno rather than pretending the release happened.
    pub(super) fn clear_stale_fid(&self, fid: u32) -> Result<bool> {
        // The scope ends before retire_clunked_fid takes the same lock.
        {
            let state = self.state.lock();
            if let SessionStatus::Dead(status) = state.status {
                return Err(Error::from_errno(status));
            }
            if !is_tombstoned_locked(&state, fid) {
                return Ok(false);
            }
        }
        self.retire_clunked_fid(fid, None);
        Ok(true)
    }

    pub(super) fn validate_fid(&self, fid: u32) -> Result<()> {
        let state = self.state.lock();
        if is_tombstoned_locked(&state, fid) {
            return Err(errno!(ESTALE));
        }
        Ok(())
    }
}

/// Free lock-record slots promised to one lock that is not stored yet.
///
/// Dropping it returns the promise, so an operation that failed, that was
/// superseded, or that was never granted cannot leak capacity. It is held
/// across every pass of one lock, so a resend after a connection replacement
/// keeps the space it already reserved.
pub(super) struct LockSlotClaim<'a> {
    session: &'a Session,
    slots: usize,
}

impl LockSlotClaim<'_> {
    /// Give the promise back once the slots it covered have been written.
    fn settle_locked(&mut self, state: &mut SessionState) {
        state.lock_slots_claimed = state.lock_slots_claimed.saturating_sub(self.slots);
        self.slots = 0;
    }
}

impl Drop for LockSlotClaim<'_> {
    fn drop(&mut self) {
        if self.slots == 0 {
            return;
        }
        let mut state = self.session.state.lock();
        state.lock_slots_claimed = state.lock_slots_claimed.saturating_sub(self.slots);
    }
}

/// Outcome of one attempt to mint a session-local fid.
enum FidAllocation {
    Allocated(u32),
    /// The replay-record table must cover this number before it is issued.
    NeedsRecord(u32),
}

fn allocate_session_fid(session: &Session, maximum: u32) -> Result<FidAllocation> {
    let mut state = session.state.lock();
    if let SessionStatus::Dead(status) = state.status {
        return Err(Error::from_errno(status));
    }
    if let Some(fid) = state.recycled_fids.pop() {
        // A recycled number was covered when it was first issued.
        return Ok(FidAllocation::Allocated(fid));
    }
    if state.next_fid > maximum {
        return Err(EOVERFLOW);
    }
    let fid = state.next_fid;
    if state.records.len() <= fid as usize {
        // Growing the table allocates, which must not happen here.
        return Ok(FidAllocation::NeedsRecord(fid));
    }
    state.next_fid = state.next_fid.saturating_add(1);
    Ok(FidAllocation::Allocated(fid))
}

/// Next length for a table that must cover at least `minimum` entries.
fn grown_table_length(current: usize, minimum: usize) -> Result<usize> {
    let target = current.saturating_mul(2).max(minimum).max(1);
    if target <= current {
        return Err(EOVERFLOW);
    }
    Ok(target)
}

/// Replace `table` with a longer one, preserving its entries.
///
/// The replacement is allocated by the caller with no session lock held: a
/// GFP_KERNEL allocation under `state` can enter direct reclaim, which can
/// enter this filesystem's writeback, which takes `state`. A replacement that
/// lost the race to a larger one is simply discarded.
fn adopt_longer_table<T: Clone>(table: &mut KVVec<T>, mut replacement: KVVec<T>) {
    if replacement.len() <= table.len() {
        return;
    }
    for (index, entry) in table.iter().enumerate() {
        if let Some(slot) = replacement.get_mut(index) {
            *slot = entry.clone();
        }
    }
    *table = replacement;
}

/// Take a reference to an existing or newly installed identity, or report that
/// the table has no free slot.
fn intern_credential_locked(
    state: &mut SessionState,
    credentials: &RebindCredentials,
) -> Option<usize> {
    let mut free = None;
    for (index, entry) in state.credentials.iter_mut().enumerate() {
        match entry {
            Some(slot) if slot.credentials == *credentials => {
                slot.references = slot.references.saturating_add(1);
                return Some(index);
            }
            None if free.is_none() => free = Some(index),
            _ => {}
        }
    }
    let index = free?;
    *state.credentials.get_mut(index)? = Some(CredentialSlot {
        credentials: credentials.clone(),
        references: 1,
    });
    Some(index)
}

/// One reference per live record, so a u64 count cannot wrap.
fn retain_credential_locked(state: &mut SessionState, credential: usize) {
    if let Some(Some(slot)) = state.credentials.get_mut(credential) {
        slot.references = slot.references.saturating_add(1);
    }
}

pub(super) fn release_credential_locked(state: &mut SessionState, credential: usize) {
    let Some(entry) = state.credentials.get_mut(credential) else {
        return;
    };
    let Some(slot) = entry.as_mut() else {
        return;
    };
    slot.references = slot.references.saturating_sub(1);
    if slot.references == 0 {
        *entry = None;
    }
}

/// Whether `fid` is a tombstone. A number with no reserved slot never is.
pub(super) fn is_tombstoned_locked(state: &SessionState, fid: u32) -> bool {
    matches!(state.records.get(fid as usize), Some(FidSlot::Stale))
}

/// Overwrite `fid`'s slot and return what it displaced, or `None` when the
/// number has no reserved slot.
fn replace_slot_locked(state: &mut SessionState, fid: u32, slot: FidSlot) -> Option<FidSlot> {
    Some(mem::replace(state.records.get_mut(fid as usize)?, slot))
}

/// Release the credential reference a displaced slot held.
fn release_displaced_locked(state: &mut SessionState, displaced: FidSlot) {
    if let FidSlot::Live(record) = displaced {
        release_credential_locked(state, record.credential);
    }
}

/// Mark `fid` as naming an object this session can no longer reach.
fn tombstone_locked(state: &mut SessionState, fid: u32) {
    if let Some(displaced) = replace_slot_locked(state, fid, FidSlot::Stale) {
        release_displaced_locked(state, displaced);
    }
    forget_fid_locks_locked(state, fid);
}

/// Store `record` in a free lock slot, reporting whether one existed.
///
/// Every caller reserved its space first, so a full table here is a
/// bookkeeping fault rather than a condition to report.
fn install_lock_locked(state: &mut SessionState, record: LockRecord) -> bool {
    for slot in state.locks.iter_mut() {
        if slot.is_none() {
            *slot = Some(record);
            return true;
        }
    }
    false
}

/// Remove `[start, length)` from every lock recorded for `fid`.
///
/// The arithmetic is [`protocol::subtract_lock_range`], the allocation-free
/// port of the same function the userspace client applies to its own record
/// list, including the re-encoding of an open-ended survivor back to a zero
/// length rather than to the distance to `u64::MAX`.
///
/// Filtering on the fid alone is deliberate and matches the server: it replaces
/// in place for the same (session, fid) whatever the requesting process or lock
/// type, so a record left behind because its `proc_id` differed would be
/// replayed onto a range the server had already given to someone else.
///
/// Returns whether every surviving fragment found a slot. A split that cannot
/// be stored would drop a range the server still holds.
fn unlock_recorded_range_locked(
    state: &mut SessionState,
    fid: u32,
    start: u64,
    length: u64,
) -> bool {
    let mut stored = true;
    for index in 0..state.locks.len() {
        let Some(Some(held)) = state.locks.get(index).copied() else {
            continue;
        };
        if held.fid != fid {
            continue;
        }
        let [left, right] = protocol::subtract_lock_range(held.start, held.length, start, length);
        if let Some(slot) = state.locks.get_mut(index) {
            *slot = left.map(|(start, length)| LockRecord {
                start,
                length,
                ..held
            });
        }
        // A right fragment begins at the end of the removed range, so a later
        // pass of this loop that revisits the slot it lands in subtracts
        // nothing from it.
        if let Some((start, length)) = right {
            stored &= install_lock_locked(
                state,
                LockRecord {
                    start,
                    length,
                    ..held
                },
            );
        }
    }
    stored
}

/// Drop every lock recorded for `fid`.
///
/// The server released them with the fid, and a record that outlives its fid is
/// replayed onto a number this session may since have recycled to an unrelated
/// inode.
pub(super) fn forget_fid_locks_locked(state: &mut SessionState, fid: u32) {
    for slot in state.locks.iter_mut() {
        if matches!(slot, Some(record) if record.fid == fid) {
            *slot = None;
        }
    }
}

/// Install `record`, consuming the credential reference it carries.
fn install_record_locked(state: &mut SessionState, fid: u32, record: FidRecord) {
    match replace_slot_locked(state, fid, FidSlot::Live(record)) {
        Some(displaced) => release_displaced_locked(state, displaced),
        // Every issued number has a reserved slot, so this is unreachable;
        // releasing the reference keeps the identity count exact if it happens.
        None => release_credential_locked(state, record.credential),
    }
}

/// The fids a request names on the wire: source, destination and clunk.
pub(super) fn request_fids(request: &Request<'_>) -> ([u32; 2], usize) {
    match *request {
        Request::Tversion { .. } | Request::Tgetlineage | Request::Tflush { .. } => ([0; 2], 0),
        Request::Trebind { fid, .. }
        | Request::Tgetattr { fid, .. }
        | Request::Tsetattrattr { fid, .. }
        | Request::Tfallocate { fid, .. }
        | Request::Treadlink { fid }
        | Request::Tread { fid, .. }
        | Request::Twrite { fid, .. }
        | Request::Tfsyncdur { fid, .. }
        | Request::Treaddirattr { fid, .. }
        | Request::Tclunk { fid }
        | Request::Tstatfs { fid }
        | Request::Tlock { fid, .. }
        | Request::Tgetlock { fid, .. } => ([fid, 0], 1),
        Request::Tmkdirattr { dfid, .. }
        | Request::Tsymlinkattr { dfid, .. }
        | Request::Tmknodattr { dfid, .. } => ([dfid, 0], 1),
        Request::Tunlinkat { dirfid, .. } => ([dirfid, 0], 1),
        Request::Twalkgetattr { fid, newfid, .. } | Request::Tlopenat { fid, newfid, .. } => {
            ([fid, newfid], 2)
        }
        Request::Tlcreateattr { dfid, newfid, .. } => ([dfid, newfid], 2),
        Request::Tlinkattr { dfid, fid, .. } => ([dfid, fid], 2),
        Request::Trenameat {
            olddirfid,
            newdirfid,
            ..
        } => ([olddirfid, newdirfid], 2),
    }
}
