use kernel::{prelude::*, sync::CondVarTimeoutResult};

use crate::protocol::{Request, Response};

use super::Client;
use super::registry::FidSlot;
use super::reply::OwnedFrame;
use super::session::{CompletedBarrier, Session, SessionState, SessionStatus};

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
        scope.covers(self.inode) && self.generation <= snapshot
    }
}

/// Obligations that could not be attributed to an inode.
///
/// Every fid that reaches the wire is recorded, so this is only reached if the
/// obligation table is full. Folding rather than dropping keeps the mutation
/// provable; the cost is that it is then only provable mount-wide.
pub(super) struct OrphanUnsynced {
    pub(super) oldest: Option<u64>,
    pub(super) generation: u64,
    pub(super) reported: bool,
}

/// Which durability obligations one barrier covers.
///
/// The mount-wide obligation is covered by both, so this selects only the
/// per-inode entries.
#[derive(Clone, Copy)]
enum FsyncScope {
    Inode(u64),
    All,
}

impl FsyncScope {
    fn covers(self, inode: u64) -> bool {
        match self {
            Self::Inode(target) => target == inode,
            Self::All => true,
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
    /// `primary` carries the request. `Tfsyncdur` flushes the whole server
    /// whatever fid it names, so one round trip discharges the whole set.
    pub(crate) fn fsync_inode(&self, inode: u64, primary: u32, datasync: bool) -> Result<()> {
        self.fsync_scope(FsyncScope::Inode(inode), primary, datasync)
    }

    /// Mount-wide barrier: verify every obligation this client still holds.
    ///
    /// syncfs promises the whole mount, so it must answer for the oldest
    /// outstanding lineage across all inodes and not just the root's.
    pub(crate) fn fsync_all(&self, primary: u32, datasync: bool) -> Result<()> {
        self.fsync_scope(FsyncScope::All, primary, datasync)
    }

    /// The token sent is the lineage of the OLDEST covered mutation this client
    /// has not had verified, so after a failover the server is asked whether
    /// everything since that point is durable. A successor writer that cannot
    /// prove it answers `ESTALE`, which is this fsync's answer for the inodes
    /// it covers and not the session's.
    fn fsync_scope(&self, scope: FsyncScope, primary: u32, datasync: bool) -> Result<()> {
        let (token, snapshot) = self.session().fsync_snapshot(scope)?;
        let wire_fid = self.route_fid(primary)?;
        // The connection's own lineage, which is what the server compares a
        // token against. Recording the token this caller happens to send would
        // make a later adopter with a different token wrongly see ESTALE.
        let dispatch = self.session().dispatch_or_wait(usize::MAX)?;
        let (epoch, lineage) = (dispatch.epoch, dispatch.token);

        // A barrier that already covers this snapshot on this connection makes
        // the round trip redundant: the server's flush is unconditional and
        // filesystem-wide, so the only thing left is the lineage equality the
        // server would have checked, which is checkable here.
        if let Some(lineage) = self.session().claim_barrier(epoch, snapshot)? {
            if token != 0 && token != lineage {
                self.session().report_unsynced(scope, snapshot);
                return Err(errno!(ESTALE));
            }
            self.session().clear_unsynced(scope, snapshot);
            return Ok(());
        }

        let outcome = (|| {
            let frame = self.transact(|| Request::Tfsyncdur {
                fid: wire_fid,
                datasync: u32::from(datasync),
                // Token zero still performs the server-wide flush fsync and
                // syncfs require, and correctly states that this client has no
                // unverified lineage obligation here.
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
                self.session()
                    .finish_barrier(epoch, snapshot, Some(lineage));
                self.session().clear_unsynced(scope, snapshot);
                Ok(())
            }
            Err(error) => {
                // Nothing is published, so anyone waiting on this barrier runs
                // its own rather than adopting an outcome that never happened.
                self.session().finish_barrier(epoch, snapshot, None);
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

    /// Record an acknowledged mutation against the lineage that answered it.
    ///
    /// `fids` are the fids the mutation actually changed, which for a create,
    /// unlink or rename is the DIRECTORY and never the new child: only an
    /// fsync covering the parent can verify a namespace change. `Trenameat`
    /// changes two directories and names both. Each is resolved to the inode
    /// it names, which is what the obligation is filed under.
    ///
    /// The token comes from the frame, so it is the lineage of the connection
    /// that carried the reply rather than whichever connection is current now.
    /// Recording a newer one would let a post-failover fsync match and report
    /// success for a mutation the new lineage may never have received.
    pub(super) fn note_mutation(&self, frame: &OwnedFrame<'_>, fids: &[u32]) {
        for fid in fids {
            self.session().note_unsynced(*fid, frame.lineage_token);
        }
    }
}

impl Session {
    /// Record one acknowledged mutation on `fid` against `token`, the lineage
    /// of the connection that answered it.
    ///
    /// The obligation is filed under the remote inode `fid` names rather than
    /// under the fid itself. See [`FsyncScope`] for why.
    fn note_unsynced(&self, fid: u32, token: u64) {
        let mut state = self.state.lock();
        let stamp = match state.mutation_stamp.checked_add(1) {
            Some(next) => {
                state.mutation_stamp = next;
                next
            }
            None => {
                // Never turn an acknowledged remote mutation into a local
                // error. Past exhaustion nothing is discharged again.
                state.mutation_stamp_exhausted = true;
                state.mutation_stamp
            }
        };
        let named = match state.records.get(fid as usize) {
            Some(FidSlot::Live(record)) => Some(record.inode_id),
            _ => None,
        };
        // Every fid that reaches the wire is recorded, so an unattributable
        // mutation should be unreachable. If it happens the obligation still
        // has to survive, so it becomes mount-wide rather than disappearing.
        let Some(inode) = named else {
            fold_orphan_locked(&mut state, token, stamp);
            return;
        };
        if let Some(entry) = state.unsynced.iter_mut().find(|entry| entry.inode == inode) {
            if entry.reported {
                // The caller of the fsync that failed already has that answer,
                // so this mutation's lineage replaces it. Without this an inode
                // that once lost a write could never report durable again.
                entry.oldest = token;
                entry.reported = false;
            } else {
                // Keep the OLDEST lineage point. Asking about a newer one
                // after a failover would let the server answer for a mutation
                // this client still cannot prove durable. The userspace client
                // keeps the first token rather than the smallest; here two
                // mutations on one inode can be acknowledged over different
                // connections out of lineage order, so first-wins could keep
                // the newer of the two.
                entry.oldest = core::cmp::min(entry.oldest, token);
            }
            entry.generation = stamp;
            return;
        }
        let overflowed = state
            .unsynced
            .push_within_capacity(UnsyncedEntry {
                inode,
                oldest: token,
                generation: stamp,
                reported: false,
            })
            .is_err();
        if overflowed {
            fold_orphan_locked(&mut state, token, stamp);
        }
    }

    /// Snapshot the lineage one barrier must verify and the mutation window it
    /// covers.
    ///
    /// The token is the numerically smallest covered one: that is the earliest
    /// and riskiest lineage point, and a server still matching it never broke
    /// lineage at all, so every later obligation is durable too. Zero says this
    /// client has nothing to verify, and still asks for the filesystem-wide
    /// flush that fsync and syncfs require.
    ///
    /// The mount-wide obligation is covered whatever the scope, because it
    /// exists precisely when the mutation could not be attributed to an inode.
    /// Claim the right to run a barrier, or wait for one that already covers
    /// this caller.
    ///
    /// `Ok(None)` means a completed barrier proved this snapshot durable and no
    /// round trip is needed. Anything the caller cannot prove from a completed
    /// barrier on the current connection makes it the issuer instead, so an
    /// extra flush is the worst outcome and a false success is unreachable.
    fn claim_barrier(&self, epoch: u64, stamp: u64) -> Result<Option<u64>> {
        let mut remaining = self.timeout_jiffies;
        loop {
            let mut state = self.state.lock();
            if let SessionStatus::Dead(status) = state.status {
                return Err(Error::from_errno(status));
            }
            // A flush that started after this snapshot was taken covers it.
            if let Some(done) = state.barrier_done {
                if done.epoch == epoch && done.stamp >= stamp {
                    return Ok(Some(done.lineage));
                }
            }
            match state.barrier_in_flight {
                // Someone else's barrier will cover this caller once it lands.
                Some((flight_epoch, flight_stamp))
                    if flight_epoch == epoch && flight_stamp >= stamp => {}
                // Nothing in flight covers this snapshot, so run one.
                _ => {
                    state.barrier_in_flight = Some((epoch, stamp));
                    return Ok(None);
                }
            }
            match self
                .changed
                .wait_interruptible_timeout(&mut state, remaining)
            {
                CondVarTimeoutResult::Woken { jiffies } => remaining = jiffies,
                CondVarTimeoutResult::Signal { .. } => return Err(EINTR),
                // Waiting cost more than issuing would have. Fall through to
                // running one rather than failing a barrier that may be fine.
                CondVarTimeoutResult::Timeout => {
                    // The wait returns holding the lock, so reuse that guard:
                    // re-locking here would deadlock against this task.
                    state.barrier_in_flight = Some((epoch, stamp));
                    return Ok(None);
                }
            }
        }
    }

    /// Publish a barrier's outcome and release everyone waiting on it.
    ///
    /// Only a successful flush on the connection it was claimed for is
    /// recorded. A resend can carry the request onto a replacement whose
    /// lineage differs, and publishing that under the old connection's lineage
    /// would let a later caller adopt a verdict the server never gave. A failed,
    /// abandoned or rerouted barrier leaves `barrier_done` alone, so its waiters
    /// issue their own.
    fn finish_barrier(&self, epoch: u64, stamp: u64, lineage: Option<u64>) {
        {
            let mut state = self.state.lock();
            if state.barrier_in_flight == Some((epoch, stamp)) {
                state.barrier_in_flight = None;
            }
            if let Some(lineage) = lineage.filter(|_| state.connection_epoch == epoch) {
                let newer = state
                    .barrier_done
                    .is_none_or(|done| done.epoch != epoch || done.stamp < stamp);
                if newer {
                    state.barrier_done = Some(CompletedBarrier {
                        epoch,
                        stamp,
                        lineage,
                    });
                }
            }
        }
        self.changed.notify_all();
    }

    fn fsync_snapshot(&self, scope: FsyncScope) -> Result<(u64, u64)> {
        let state = self.state.lock();
        if let SessionStatus::Dead(status) = state.status {
            return Err(Error::from_errno(status));
        }
        let oldest_per_inode = state
            .unsynced
            .iter()
            .filter(|entry| scope.covers(entry.inode))
            .map(|entry| entry.oldest)
            .min();
        let token = [state.orphan.oldest, oldest_per_inode]
            .into_iter()
            .flatten()
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
        if state.orphan.generation <= snapshot {
            state.orphan.oldest = None;
            state.orphan.reported = false;
        }
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
        if state.orphan.oldest.is_some() && state.orphan.generation <= snapshot {
            state.orphan.reported = true;
        }
    }
}

/// Merge one unattributable obligation into the mount-wide one.
///
/// A reported obligation is replaced rather than merged, for the reason
/// `note_unsynced` gives for a reported per-inode entry: otherwise nothing that
/// folded in here could ever report durable again.
fn fold_orphan_locked(state: &mut SessionState, oldest: u64, generation: u64) {
    let orphan = &mut state.orphan;
    match orphan.oldest {
        Some(current) if !orphan.reported => orphan.oldest = Some(core::cmp::min(current, oldest)),
        _ => {
            orphan.oldest = Some(oldest);
            orphan.reported = false;
        }
    }
    orphan.generation = core::cmp::max(orphan.generation, generation);
}
