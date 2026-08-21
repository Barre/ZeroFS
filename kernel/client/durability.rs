use kernel::{alloc::flags::GFP_NOWAIT, prelude::*};

use crate::protocol::{P9_FSYNC_INODE, Request, Response};

use super::Client;
use super::registry::FidSlot;
use super::session::{FsyncScope, Session, SessionState, SessionStatus};

/// One remote inode's outstanding durability obligation.
///
/// `oldest` is the lineage of the earliest acknowledged mutation on this inode
/// that no fsync has verified. `generation` is the session mutation stamp of
/// the newest mutation folded in, which is what stops an fsync discharging a
/// mutation that arrived inside its own window. `reported` keeps an `ESTALE`
/// answer visible until a replacement mutation records a lineage the current
/// writer can satisfy.
pub(super) struct UnsyncedEntry {
    inode: u64,
    oldest: u64,
    generation: u64,
    reported: bool,
}

impl UnsyncedEntry {
    /// Whether one barrier answered for this obligation.
    ///
    /// A mutation acknowledged after the snapshot took a higher stamp, so a
    /// flush it did not precede says nothing about it.
    fn answered_by(&self, scope: FsyncScope, snapshot: u64) -> bool {
        scope.covers_inode(self.inode) && self.generation <= snapshot
    }
}

impl FsyncScope {
    fn covers_inode(self, inode: u64) -> bool {
        match self {
            Self::Inode(target) => target == inode,
            Self::All => true,
        }
    }

    fn wire_flag(self) -> u32 {
        match self {
            Self::Inode(_) => P9_FSYNC_INODE,
            Self::All => 0,
        }
    }
}

impl Client {
    /// Durably flush, and verify every obligation `inode` holds.
    ///
    /// One inode is mutated through several fids: the caller's own, the
    /// capability writeback retained after the file closed, and one per
    /// credential the inode has been reached with. Obligations are filed under
    /// the inode so the barrier covers all of them, whichever fid ran the
    /// mutation.
    ///
    /// `primary` carries the request. The server resolves it to the same remote
    /// inode and limits the verified barrier to that inode.
    pub(crate) fn fsync_inode(&self, inode: u64, primary: u32) -> Result<()> {
        self.fsync_scope(FsyncScope::Inode(inode), primary)
    }

    /// Mount-wide barrier: verify every obligation this client still holds.
    ///
    /// syncfs promises the whole mount, so it must answer for the oldest
    /// outstanding lineage across all inodes and not just the root's.
    pub(crate) fn fsync_all(&self, primary: u32) -> Result<()> {
        self.fsync_scope(FsyncScope::All, primary)
    }

    /// The token sent is the lineage of the OLDEST covered mutation this client
    /// has not had verified, so after a failover the server is asked whether
    /// everything since that point is durable. A successor writer that cannot
    /// prove it answers `ESTALE`, which is this fsync's answer for the inodes
    /// it covers and not the session's.
    fn fsync_scope(&self, scope: FsyncScope, primary: u32) -> Result<()> {
        let (token, snapshot) = self.session().fsync_snapshot(scope, primary)?;
        let wire_fid = self.route_fid(primary)?;

        let outcome = (|| {
            let frame = self.transact(|| Request::Tfsyncdur {
                fid: wire_fid,
                datasync: scope.wire_flag(),
                // Token zero still performs the requested inode or mount-wide
                // barrier and states that this client has no unverified
                // lineage obligation in that scope.
                token,
            })?;
            let response = self.decode(&frame)?;
            if !matches!(response.body, Response::Rfsync) {
                return self.invariant_failure();
            }
            Ok(())
        })();

        match outcome {
            Ok(()) => {
                self.session().clear_unsynced(scope, snapshot);
                Ok(())
            }
            Err(error) => {
                if error == errno!(ESTALE) {
                    // Keep the obligations outstanding and marked reported, so
                    // every later fsync covering them keeps failing until a
                    // replacement mutation records a lineage the current writer
                    // can satisfy.
                    self.session().report_unsynced(scope, snapshot);
                }
                Err(error)
            }
        }
    }

    /// Reserve enough bookkeeping capacity before a mutation can reach the wire.
    pub(super) fn reserve_mutation(&self, fids: &[u32]) -> Result<UnsyncedClaim<'_>> {
        self.session().reserve_unsynced(fids)
    }
}

impl Session {
    /// Claim one spare entry per inode a mutation may affect.
    ///
    /// Claims include inodes already present in the table because a concurrent
    /// fsync may remove their entries before the mutation is acknowledged. The
    /// table retains grown capacity: this runs under the session mutex and may
    /// be called from netfs writeback. Failure therefore happens before anything
    /// reaches the server.
    fn reserve_unsynced(&self, fids: &[u32]) -> Result<UnsyncedClaim<'_>> {
        if fids.len() > 2 {
            return Err(EINVAL);
        }
        let mut state = self.state.lock();
        if let SessionStatus::Dead(status) = state.status {
            return Err(Error::from_errno(status));
        }

        let mut inodes = [0u64; 2];
        let mut inode_count = 0usize;
        for &fid in fids {
            let inode = match state.records.get(fid as usize) {
                Some(FidSlot::Live(record)) => record.inode_id,
                Some(FidSlot::Stale) => return Err(errno!(ESTALE)),
                _ => return Err(EBADF),
            };
            if !inodes[..inode_count].contains(&inode) {
                inodes[inode_count] = inode;
                inode_count += 1;
            }
        }

        let required_spares = state
            .unsynced_slots_claimed
            .checked_add(inode_count)
            .ok_or_else(|| EOVERFLOW)?;
        state.unsynced.reserve(required_spares, GFP_NOWAIT)?;
        state.unsynced_slots_claimed = required_spares;

        Ok(UnsyncedClaim {
            session: self,
            inodes,
            inode_count,
        })
    }

    /// Snapshot the lineage one barrier must verify and the mutation window it
    /// covers.
    ///
    /// The token is the numerically smallest covered one: that is the earliest
    /// and riskiest lineage point, and a server still matching it never broke
    /// lineage at all, so every later obligation is durable too. Zero says this
    /// client has nothing to verify, and still asks for the inode or mount-wide
    /// barrier the caller requires.
    fn fsync_snapshot(&self, scope: FsyncScope, primary: u32) -> Result<(u64, u64)> {
        let state = self.state.lock();
        if let SessionStatus::Dead(status) = state.status {
            return Err(Error::from_errno(status));
        }
        if let FsyncScope::Inode(inode) = scope {
            match state.records.get(primary as usize) {
                Some(FidSlot::Live(record)) if record.inode_id == inode => {}
                Some(FidSlot::Live(_)) => return Err(EINVAL),
                Some(FidSlot::Stale) => return Err(errno!(ESTALE)),
                _ => return Err(EBADF),
            }
        }
        let token = state
            .unsynced
            .iter()
            .filter(|entry| scope.covers_inode(entry.inode))
            .map(|entry| entry.oldest)
            .min()
            .unwrap_or(0);
        Ok((token, state.mutation_stamp))
    }

    /// Discharge every covered obligation this barrier verified.
    ///
    /// Discharging one the barrier did not answer for would make the next fsync
    /// send token zero and report success for a mutation whose lineage was never
    /// verified.
    fn clear_unsynced(&self, scope: FsyncScope, snapshot: u64) {
        let mut state = self.state.lock();
        if state.mutation_stamp_exhausted {
            return;
        }
        state
            .unsynced
            .retain(|entry| !entry.answered_by(scope, snapshot));
    }

    /// The current writer could not satisfy a covered obligation.
    ///
    /// The obligation and its token survive, marked reported, so every later
    /// fsync covering that inode keeps failing until a replacement mutation
    /// records a lineage the current writer can satisfy. The stamp guard stops
    /// a mutation that raced the barrier being marked reported, since nobody
    /// was told about that one.
    fn report_unsynced(&self, scope: FsyncScope, snapshot: u64) {
        let mut state = self.state.lock();
        if state.mutation_stamp_exhausted {
            return;
        }
        for entry in state.unsynced.iter_mut() {
            if entry.answered_by(scope, snapshot) {
                entry.reported = true;
            }
        }
    }
}

/// Capacity reserved before a mutation reaches the wire.
///
/// Dropping an unsettled claim returns its spare slots. Recording consumes the
/// claim only after every affected inode has an allocation-free table entry.
pub(super) struct UnsyncedClaim<'a> {
    session: &'a Session,
    inodes: [u64; 2],
    /// Number of affected inodes and, until `record`, claimed spare entries.
    inode_count: usize,
}

impl UnsyncedClaim<'_> {
    /// Record the lineage of the connection that acknowledged the mutation.
    pub(super) fn record(&mut self, token: u64) -> Result<()> {
        let inode_count = self.inode_count;
        let stored = {
            let mut state = self.session.state.lock();
            let mut stored = true;
            for index in 0..inode_count {
                if !note_unsynced_locked(&mut state, self.inodes[index], token) {
                    stored = false;
                    break;
                }
            }
            state.unsynced_slots_claimed = state.unsynced_slots_claimed.saturating_sub(inode_count);
            stored
        };
        self.inode_count = 0;
        if stored {
            return Ok(());
        }
        let error = EOVERFLOW;
        self.session.terminate(error);
        Err(error)
    }
}

impl Drop for UnsyncedClaim<'_> {
    fn drop(&mut self) {
        if self.inode_count == 0 {
            return;
        }
        let mut state = self.session.state.lock();
        state.unsynced_slots_claimed = state
            .unsynced_slots_claimed
            .saturating_sub(self.inode_count);
    }
}

/// Store one acknowledged mutation without allocating.
fn note_unsynced_locked(state: &mut SessionState, inode: u64, token: u64) -> bool {
    let stamp = match state.mutation_stamp.checked_add(1) {
        Some(next) => {
            state.mutation_stamp = next;
            next
        }
        None => {
            // Never turn an acknowledged remote mutation into a local error.
            // Past exhaustion nothing is discharged again.
            state.mutation_stamp_exhausted = true;
            state.mutation_stamp
        }
    };
    if let Some(entry) = state.unsynced.iter_mut().find(|entry| entry.inode == inode) {
        if entry.reported {
            // The caller of the fsync that failed already has that answer, so
            // this mutation's lineage replaces it.
            entry.oldest = token;
            entry.reported = false;
        } else {
            // Replies can settle out of connection order, so preserve the
            // numerically oldest lineage rather than the first one observed.
            entry.oldest = core::cmp::min(entry.oldest, token);
        }
        entry.generation = stamp;
        return true;
    }
    state
        .unsynced
        .push_within_capacity(UnsyncedEntry {
            inode,
            oldest: token,
            generation: stamp,
            reported: false,
        })
        .is_ok()
}
