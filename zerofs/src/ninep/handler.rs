use super::errors::{P9Error, P9Result};
use super::lock_manager::{FileLock, FileLockManager, LockGuard};
#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::tracing::FileOperation;
use crate::fs::types::{
    AuthContext, FallocateMode, FileAttributes, FileType, InodeWithId, SetAttributes, SetGid,
    SetMode, SetSize, SetTime, SetUid, Timestamp,
};
use crate::fs::{OpenHandle, ZeroFS};
use bytes::Bytes;
#[cfg(feature = "failpoints")]
use fp::fail_point;
use ninep_proto::*;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::debug;

pub const DEFAULT_MSIZE: u32 = 256 * 1024;

pub const AT_REMOVEDIR: u32 = 0x200;
// Linux dirent type constants
pub const DT_DIR: u8 = 4;
pub const DT_REG: u8 = 8;
pub const DT_LNK: u8 = 10;
pub const DT_CHR: u8 = 2;
pub const DT_BLK: u8 = 6;
pub const DT_FIFO: u8 = 1;
pub const DT_SOCK: u8 = 12;

// File mode type bits (S_IF* constants)
pub const S_IFREG: u32 = 0o100000; // Regular file
pub const S_IFDIR: u32 = 0o040000; // Directory
pub const S_IFLNK: u32 = 0o120000; // Symbolic link
pub const S_IFCHR: u32 = 0o020000; // Character device
pub const S_IFBLK: u32 = 0o060000; // Block device
pub const S_IFIFO: u32 = 0o010000; // FIFO
pub const S_IFSOCK: u32 = 0o140000; // Socket

// Default permissions for symbolic links
pub const SYMLINK_DEFAULT_MODE: u32 = 0o777;

// Default block size for stat
pub const DEFAULT_BLKSIZE: u64 = 4096;

// Block size for calculating block count
pub const BLOCK_SIZE: u64 = 512;

/// Classify a post-dispatch error for failover retry semantics.
fn post_dispatch_errno(error: P9Error, lease_is_valid: bool) -> u32 {
    if matches!(error, P9Error::Fs(FsError::LeaderRejectedBeforeApply)) {
        ninep_proto::P9_ENOTLEADER_CLEAN
    } else if lease_is_valid {
        error.to_errno()
    } else {
        ninep_proto::P9_ENOTLEADER
    }
}

/// Errors that conclusively completed without mutation effects.
/// I/O, corruption, and leadership-transition failures remain ambiguous.
fn is_terminal_dedup_error(error: P9Error) -> bool {
    !matches!(
        error,
        P9Error::Fs(
            FsError::IoError
                | FsError::InvalidData
                | FsError::LeaderLeaseExpired
                | FsError::LeaderRejectedBeforeApply
                | FsError::ShuttingDown,
        )
    )
}

/// Upper bound on the number of parent-pointer hops an aname membership walk
/// (`..` clamp and `Trebind` confinement check) will follow before giving up.
/// A single committed snapshot is always acyclic, but these walks read each
/// ancestor unlocked across separate `get`s, so a pathological concurrent
/// rename workload could keep re-pointing two ancestors at each other and make
/// the walk ping-pong forever. Capping it turns that liveness hole into a safe
/// clamp/deny. The bound is far above any real directory depth.
const MAX_ANCESTOR_HOPS: u32 = 100_000;

// Represents an open file handle
#[derive(Debug, Clone)]
pub struct Fid {
    pub path: Vec<bytes::Bytes>,
    pub inode_id: InodeId,
    /// The attach root this fid descends from (the inode the originating
    /// Tattach's aname resolved to; 0 for a whole-filesystem attach). Walking
    /// `..` clamps here, so the fid can never climb out of its subtree.
    pub root: InodeId,
    pub qid: Qid,
    pub opened: bool,
    pub mode: Option<u32>,
    pub creds: Credentials, // Store credentials per fid/session
}

/// A session fid-table entry: the cloneable `Fid` data, plus the guards that
/// tie its open-handle count and byte-range locks to the entry's lifetime.
/// Resource guards remain in the table when fid metadata is cloned.
#[derive(Debug)]
pub struct FidSlot {
    pub fid: Fid,
    /// Open-handle pin, including provisional replay pins before `Tlopen`.
    pub handle: Option<OpenHandle>,
    /// Present once the fid takes its first byte-range lock; releases the fid's
    /// locks on drop.
    pub lock_guard: Option<LockGuard>,
}

impl FidSlot {
    fn unopened(fid: Fid) -> Self {
        Self {
            fid,
            handle: None,
            lock_guard: None,
        }
    }
}

#[derive(Debug, Default)]
struct SessionState {
    msize: u32,
    zerofs_protocol: bool,
    fids: HashMap<u32, FidSlot>,
    /// Blocks installation of new resource-bearing fid slots.
    disconnected: bool,
}

impl From<&Tsetattr> for SetAttributes {
    fn from(ts: &Tsetattr) -> Self {
        SetAttributes {
            mode: if ts.valid & SETATTR_MODE != 0 {
                SetMode::Set(ts.mode)
            } else {
                SetMode::NoChange
            },
            uid: if ts.valid & SETATTR_UID != 0 {
                SetUid::Set(ts.uid)
            } else {
                SetUid::NoChange
            },
            gid: if ts.valid & SETATTR_GID != 0 {
                SetGid::Set(ts.gid)
            } else {
                SetGid::NoChange
            },
            size: if ts.valid & SETATTR_SIZE != 0 {
                SetSize::Set(ts.size)
            } else {
                SetSize::NoChange
            },
            atime: if ts.valid & SETATTR_ATIME_SET != 0 {
                SetTime::SetToClientTime(Timestamp {
                    seconds: ts.atime_sec,
                    nanoseconds: ts.atime_nsec as u32,
                })
            } else if ts.valid & SETATTR_ATIME != 0 {
                SetTime::SetToServerTime
            } else {
                SetTime::NoChange
            },
            mtime: if ts.valid & SETATTR_MTIME_SET != 0 {
                SetTime::SetToClientTime(Timestamp {
                    seconds: ts.mtime_sec,
                    nanoseconds: ts.mtime_nsec as u32,
                })
            } else if ts.valid & SETATTR_MTIME != 0 {
                SetTime::SetToServerTime
            } else {
                SetTime::NoChange
            },
        }
    }
}

#[derive(Clone)]
pub struct NinePHandler {
    filesystem: Arc<ZeroFS>,
    session: Arc<Mutex<SessionState>>,
    lock_manager: Arc<FileLockManager>,
    handler_id: u64,
    /// If set, overrides the client-provided credentials on attach.
    credential_override: Option<(u32, u32)>,
}

/// Retires a transport session on exit or unwind.
pub(crate) struct SessionReleaseGuard(Option<Arc<NinePHandler>>);

impl SessionReleaseGuard {
    pub(crate) fn new(handler: Arc<NinePHandler>) -> Self {
        Self(Some(handler))
    }

    pub(crate) fn release(&mut self) {
        if let Some(handler) = self.0.take() {
            handler.close_for_disconnect();
        }
    }
}

impl Drop for SessionReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl NinePHandler {
    pub fn new(filesystem: Arc<ZeroFS>, lock_manager: Arc<FileLockManager>) -> Self {
        static HANDLER_COUNTER: AtomicU64 = AtomicU64::new(1);

        let session = Arc::new(Mutex::new(SessionState {
            msize: DEFAULT_MSIZE,
            ..Default::default()
        }));

        Self {
            filesystem,
            session,
            lock_manager,
            handler_id: HANDLER_COUNTER.fetch_add(1, AtomicOrdering::SeqCst),
            credential_override: None,
        }
    }

    /// Set a (uid, gid) override that will be used for all sessions instead of
    /// trusting the client-provided credentials in Tattach.
    #[cfg(feature = "webui")]
    pub fn with_credential_override(mut self, uid: u32, gid: u32) -> Self {
        self.credential_override = Some((uid, gid));
        self
    }

    #[cfg(test)]
    pub fn handler_id(&self) -> u64 {
        self.handler_id
    }

    /// Session table used for atomic guard installation and retirement.
    fn session_state(&self) -> MutexGuard<'_, SessionState> {
        self.session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn connected_session(&self) -> P9Result<MutexGuard<'_, SessionState>> {
        let guard = self.session_state();
        if guard.disconnected {
            Err(FsError::StaleHandle.into())
        } else {
            Ok(guard)
        }
    }

    /// Publish an opened compound-operation fid under the session lock.
    fn install_open_fid(
        &self,
        state: &mut SessionState,
        newfid: u32,
        replace_fid: u32,
        fid: Fid,
    ) -> P9Result<()> {
        let inode_id = fid.inode_id;
        let slot = match state.fids.entry(newfid) {
            Entry::Vacant(entry) => entry.insert(FidSlot::unopened(fid)),
            Entry::Occupied(_) if newfid != replace_fid => return Err(P9Error::FidInUse),
            Entry::Occupied(entry) if entry.get().fid.opened => {
                return Err(P9Error::FidAlreadyOpen);
            }
            Entry::Occupied(entry) => {
                let slot = entry.into_mut();
                slot.fid = fid;
                slot
            }
        };
        slot.handle = Some(self.filesystem.new_open_handle(inode_id));
        Ok(())
    }

    fn install_walk_fid(&self, source: u32, newfid: u32, fid: Fid) -> P9Result<()> {
        let replaced = {
            let mut state = self.session_state();
            if source != newfid && state.fids.contains_key(&newfid) {
                return Err(P9Error::FidInUse);
            }
            state.fids.insert(newfid, FidSlot::unopened(fid))
        };
        drop(replaced);
        Ok(())
    }

    /// Whether this connection negotiated the private ZeroFS dialect.
    pub fn zerofs_protocol_enabled(&self) -> bool {
        self.session_state().zerofs_protocol
    }

    /// Reserve a valid FIRST mutation before task detachment.
    pub(crate) fn try_reserve_received_first(
        &self,
        op_id: crate::dedup::OpId,
        op_flags: u8,
        type_byte: u8,
    ) -> Option<crate::dedup::DedupGuard> {
        if !self.zerofs_protocol_enabled()
            || !P9Message::carries_op_id(type_byte)
            || op_flags & !ninep_proto::P9_OP_KNOWN_FLAGS != 0
            || op_flags & ninep_proto::P9_OP_FLAG_RETRY != 0
            || !self.filesystem.db.permits_successful_response()
        {
            return None;
        }
        self.filesystem.dedup.reserve_initial(op_id)
    }

    /// Commit terminal no-op and error outcomes to the mutation ledger.
    async fn ensure_terminal_dedup_result(
        &self,
        op_id: crate::dedup::OpId,
        result: crate::dedup::DedupResult,
    ) -> P9Result<()> {
        if !crate::dedup::has_op_id(&op_id) || self.filesystem.dedup.get(&op_id).is_some() {
            return Ok(());
        }
        let mut txn = self.filesystem.db.new_transaction()?;
        txn.set_dedup_result(op_id, result);
        self.filesystem.write_coordinator.commit(txn).await?;
        Ok(())
    }

    /// Returns the effective GID for a create-type operation. When a credential
    /// override is active, the server-configured GID is used instead of
    /// whatever the client sent.
    fn effective_gid(&self, client_gid: u32, fid_creds: &Credentials) -> u32 {
        if self.credential_override.is_some() {
            fid_creds.gid
        } else {
            client_gid
        }
    }

    /// Per 9P spec, iounit may be zero, in which case the client calculates
    /// the maximum I/O size based on the negotiated msize.
    fn iounit(&self) -> u32 {
        0
    }

    fn get_fid(&self, fid: u32) -> P9Result<Fid> {
        self.session_state()
            .fids
            .get(&fid)
            .ok_or(P9Error::BadFid)
            .map(|s| s.fid.clone())
    }

    /// Snapshot only the fid fields needed by attribute mutations. Unlike
    /// `get_fid`, this does not allocate and clone the fid's path.
    fn get_fid_inode_and_creds(&self, fid: u32) -> P9Result<(InodeId, Credentials)> {
        self.session_state()
            .fids
            .get(&fid)
            .ok_or(P9Error::BadFid)
            .map(|slot| (slot.fid.inode_id, slot.fid.creds))
    }

    fn clamp_count(&self, count: u32, header: u32) -> u32 {
        count.min(self.session_state().msize.saturating_sub(header))
    }

    /// Op-id-less dispatch for the unit tests; production passes the frame op-id.
    #[cfg(test)]
    pub async fn handle_message(&self, tag: u16, msg: Message) -> P9Message {
        self.handle_message_with_op_id(tag, [0u8; 16], msg).await
    }

    /// Test dispatch using initial-attempt framing.
    #[cfg(test)]
    pub async fn handle_message_with_op_id(
        &self,
        tag: u16,
        op_id: crate::dedup::OpId,
        msg: Message,
    ) -> P9Message {
        self.handle_message_with_op_envelope(tag, op_id, 0, msg)
            .await
    }

    /// Dispatch a request with its idempotency envelope.
    #[cfg(test)]
    async fn handle_message_with_op_envelope(
        &self,
        tag: u16,
        op_id: crate::dedup::OpId,
        op_flags: u8,
        msg: Message,
    ) -> P9Message {
        self.handle_message_with_op_envelope_origin(tag, op_id, op_flags, 0, msg)
            .await
    }

    #[cfg(test)]
    pub async fn handle_message_with_op_envelope_origin(
        &self,
        tag: u16,
        op_id: crate::dedup::OpId,
        op_flags: u8,
        op_origin_epoch: u64,
        msg: Message,
    ) -> P9Message {
        self.handle_message_with_received_admission(
            tag,
            op_id,
            op_flags,
            op_origin_epoch,
            msg,
            None,
        )
        .await
    }

    /// Dispatch using a FIRST reservation created by the connection reader.
    pub(crate) async fn handle_message_with_received_admission(
        &self,
        tag: u16,
        op_id: crate::dedup::OpId,
        op_flags: u8,
        op_origin_epoch: u64,
        msg: Message,
        received_guard: Option<crate::dedup::DedupGuard>,
    ) -> P9Message {
        let zerofs_protocol = self.zerofs_protocol_enabled();
        let private_request = msg.is_zerofs_private_request();
        let has_private_envelope =
            crate::dedup::has_op_id(&op_id) || op_flags != 0 || op_origin_epoch != 0;
        if !zerofs_protocol && (private_request || has_private_envelope) {
            return P9Message::new(
                tag,
                Message::Rlerror(Rlerror {
                    ecode: P9Error::NotSupported.to_errno(),
                }),
            );
        }

        if op_flags & !ninep_proto::P9_OP_KNOWN_FLAGS != 0 {
            return P9Message::new(
                tag,
                Message::Rlerror(Rlerror {
                    ecode: ninep_proto::P9_EOPIDSTALE,
                }),
            );
        }
        if has_private_envelope && !msg.is_mutation() {
            // Operation envelopes are valid only on mutation messages.
            return P9Message::new(
                tag,
                Message::Rlerror(Rlerror {
                    ecode: ninep_proto::P9_EOPIDSTALE,
                }),
            );
        }
        let is_retry = op_flags & ninep_proto::P9_OP_FLAG_RETRY != 0;

        // Pre-dispatch rejection is CLEAN; loss during dispatch is ambiguous.
        let lease_gated = !matches!(msg, Message::Tversion(_));
        if lease_gated && !self.filesystem.db.permits_successful_response() {
            return P9Message::new(
                tag,
                Message::Rlerror(Rlerror {
                    ecode: ninep_proto::P9_ENOTLEADER_CLEAN,
                }),
            );
        }
        // Dedup admission precedes mutation dispatch.
        let _dedup_guard = if let Some(guard) = received_guard {
            Some(guard)
        } else {
            match self
                .filesystem
                .dedup
                .begin_attempt_from_origin(op_id, is_retry, op_origin_epoch)
                .await
            {
                Ok(guard) => guard,
                Err(error) => {
                    // Authority loss while waiting for admission remains pre-dispatch.
                    if lease_gated && !self.filesystem.db.permits_successful_response() {
                        return P9Message::new(
                            tag,
                            Message::Rlerror(Rlerror {
                                ecode: ninep_proto::P9_ENOTLEADER_CLEAN,
                            }),
                        );
                    }
                    let ecode = match error {
                        crate::dedup::DedupAdmissionError::UnseenRetry => {
                            ninep_proto::P9_EOPIDSTALE
                        }
                    };
                    return P9Message::new(tag, Message::Rlerror(Rlerror { ecode }));
                }
            }
        };

        // Replay retained protocol errors before operation-specific dispatch.
        if let Some(crate::dedup::DedupResult::Error { errno }) = self.filesystem.dedup.get(&op_id)
        {
            let ecode = if !lease_gated || self.filesystem.db.permits_successful_response() {
                errno
            } else {
                ninep_proto::P9_ENOTLEADER
            };
            return P9Message::new(tag, Message::Rlerror(Rlerror { ecode }));
        }
        let result = match msg {
            Message::Tversion(tv) => self.version(tv).await,
            Message::Tattach(ta) => self.attach(ta).await,
            Message::Twalk(tw) => self.walk(tw).await,
            Message::Tlopen(tl) => self.lopen(tl).await,
            Message::Tlcreate(tc) => self.lcreate(tc, op_id).await,
            Message::Tread(tr) => self.read(tr).await,
            Message::Twrite(tw) => self.write(tw, op_id).await,
            Message::Tclunk(tc) => Ok(self.clunk(tc).await),
            Message::Treaddir(tr) => self.readdir(tr).await,
            Message::Tgetattr(tg) => self.getattr(tg).await,
            Message::Tsetattr(ts) => self.setattr(ts, op_id).await,
            Message::Tfallocate(tf) => self.fallocate(tf, op_id).await,
            Message::Tmkdir(tm) => self.mkdir(tm, op_id).await,
            Message::Tsymlink(ts) => self.symlink(ts, op_id).await,
            Message::Tmknod(tm) => self.mknod(tm, op_id).await,
            Message::Treadlink(tr) => self.readlink(tr).await,
            Message::Tlink(tl) => self.link(tl, op_id).await,
            Message::Trename(tr) => self.rename(tr, op_id).await,
            Message::Trenameat(tr) => self.renameat(tr, op_id).await,
            Message::Tunlinkat(tu) => self.unlinkat(tu, op_id).await,
            Message::Tfsync(tf) => self.fsync(tf).await,
            Message::Tfsyncdur(tf) => self.fsyncdur(tf).await,
            Message::Tgetlineage(_) => self.getlineage().await,
            Message::Tflush(_) => Ok(Message::Rflush(Rflush)),
            Message::Txattrwalk(_) => Err(P9Error::NotSupported),
            Message::Tstatfs(ts) => self.statfs(ts).await,
            Message::Tlock(tl) => self.lock(tl).await,
            Message::Tgetlock(tg) => self.getlock(tg).await,
            Message::Trebind(tr) => self.rebind(tr).await,
            Message::Twalkgetattr(tw) => self.walk_getattr(tw).await,
            Message::Treaddirattr(tr) => self.readdir_attr(tr).await,
            Message::Tlopenat(tl) => self.lopenat(tl).await,
            Message::Tlopenatread(tl) => self.lopenatread(tl).await,
            Message::Tlcreateattr(tc) => self.lcreateattr(tc, op_id).await,
            Message::Tmkdirattr(tm) => self.mkdir_attr(tm, op_id).await,
            Message::Tsymlinkattr(ts) => self.symlink_attr(ts, op_id).await,
            Message::Tmknodattr(tm) => self.mknod_attr(tm, op_id).await,
            Message::Tlinkattr(tl) => self.link_attr(tl, op_id).await,
            Message::Tsetattrattr(ts) => self.setattr_attr(ts, op_id).await,
            _ => Err(P9Error::NotImplemented),
        };

        // Publish terminal outcomes before releasing single-flight ownership.
        let result = match result {
            Ok(body) => match self
                .ensure_terminal_dedup_result(op_id, crate::dedup::DedupResult::Applied)
                .await
            {
                Ok(()) => Ok(body),
                Err(error) => Err(error),
            },
            Err(error)
                if self.filesystem.db.permits_successful_response()
                    && is_terminal_dedup_error(error) =>
            {
                match self
                    .ensure_terminal_dedup_result(
                        op_id,
                        crate::dedup::DedupResult::Error {
                            errno: error.to_errno(),
                        },
                    )
                    .await
                {
                    Ok(()) => Err(error),
                    Err(commit_error) => Err(commit_error),
                }
            }
            Err(error) => Err(error),
        };

        match result {
            Ok(body) if !lease_gated || self.filesystem.db.permits_successful_response() => {
                P9Message::new(tag, body)
            }
            // Authority loss after dispatch suppresses successful responses.
            Ok(_) => P9Message::new(
                tag,
                Message::Rlerror(Rlerror {
                    ecode: ninep_proto::P9_ENOTLEADER,
                }),
            ),
            Err(e) => {
                // CLEAN requires proof that neither replica applied the batch.
                let ecode =
                    post_dispatch_errno(e, self.filesystem.db.permits_successful_response());
                P9Message::new(tag, Message::Rlerror(Rlerror { ecode }))
            }
        }
    }

    async fn version(&self, tv: Tversion) -> P9Result<Message> {
        let version_str = tv.version.as_str().map_err(|e| {
            debug!("Invalid version string encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;

        debug!("Client requested version: {}", version_str);

        // Tversion implicitly clunks every fid and resets the session. Dropping
        // the fids releases their handle counts (reclaiming any now-last
        // open-unlinked inode) and their locks; the explicit lock sweep is a
        // safety net in case some path ever leaves a lock without a guard.
        let fids = {
            let mut state = self.session_state();
            state.zerofs_protocol = false;
            std::mem::take(&mut state.fids)
        };
        drop(fids);
        self.lock_manager.release_session_locks(self.handler_id);

        let requested = version_str.as_bytes();
        if requested != VERSION_9P2000L && requested != VERSION_9P2000L_ZEROFS {
            debug!("Client requested an unsupported 9P dialect, returning unknown");
            return Ok(Message::Rversion(Rversion {
                msize: tv.msize,
                version: P9String::new(b"unknown".to_vec()),
            }));
        }

        let msize = tv.msize.min(P9_MAX_MSIZE);
        let zerofs_protocol = requested == VERSION_9P2000L_ZEROFS;
        let mut state = self.session_state();
        state.msize = msize;
        state.zerofs_protocol = zerofs_protocol;

        Ok(Message::Rversion(Rversion {
            msize,
            version: P9String::new(requested.to_vec()),
        }))
    }

    /// Build per-fid credentials from a client-supplied `n_uname` (the 9P2000.L
    /// numeric uid), honoring any credential override. `username` is only
    /// consulted for the `n_uname == -1` ("unspecified") fallback.
    fn creds_for(&self, n_uname: u32, username: &str) -> Credentials {
        let (uid, gid) = if let Some((override_uid, override_gid)) = self.credential_override {
            (override_uid, override_gid)
        } else {
            // In 9P2000.L we trust the client and use UID as GID as a reasonable
            // default. n_uname = -1 (0xFFFFFFFF) means "unspecified": map by name.
            let uid = if n_uname == 0xFFFFFFFF {
                match username {
                    "root" => 0,
                    _ => P9_NOBODY_UID,
                }
            } else {
                n_uname
            };
            (uid, uid)
        };
        let mut groups = [0u32; P9_MAX_GROUPS];
        groups[0] = gid; // User is always a member of their primary group.
        Credentials {
            uid,
            gid,
            groups,
            groups_count: 1,
        }
    }

    async fn attach(&self, ta: Tattach) -> P9Result<Message> {
        let username = ta.uname.as_str().map_err(|e| {
            debug!("Invalid username encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;

        debug!(
            "attach: fid={}, afid={}, uname={}, aname={:?}, n_uname={}",
            ta.fid,
            ta.afid,
            username,
            ta.aname.as_str().ok(),
            ta.n_uname
        );

        let creds = self.creds_for(ta.n_uname, username);

        if self.session_state().fids.contains_key(&ta.fid) {
            return Err(P9Error::FidInUse);
        }

        // aname selects the subtree the session is rooted at: a path from the
        // filesystem root whose components must all be existing directories.
        // "" and "/" (no components) keep the whole-filesystem root.
        let aname = ta.aname.as_str().map_err(|e| {
            debug!("Invalid aname encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;
        let components: Vec<&str> = aname.split('/').filter(|c| !c.is_empty()).collect();

        let mut root_id: InodeId = 0;
        for name in &components {
            root_id = self
                .filesystem
                .lookup(&creds, root_id, name.as_bytes())
                .await?;
        }
        let root_inode = self.filesystem.inode_store.get(root_id).await?;
        if !matches!(root_inode, Inode::Directory(_)) {
            return Err(P9Error::NotADirectory);
        }

        let qid = inode_to_qid(&root_inode, root_id);

        let mut state = self.session_state();
        if state.fids.contains_key(&ta.fid) {
            return Err(P9Error::FidInUse);
        }
        state.fids.insert(
            ta.fid,
            FidSlot::unopened(Fid {
                // The fid's path stays absolute (rename resolves it from
                // inode 0), so it starts at the aname components.
                path: components
                    .iter()
                    .map(|c| Bytes::copy_from_slice(c.as_bytes()))
                    .collect(),
                inode_id: root_id,
                root: root_id,
                qid: qid.clone(),
                opened: false,
                mode: None,
                creds,
            }),
        );
        Ok(Message::Rattach(Rattach { qid }))
    }

    /// Bind a replay fid to a linked inode by ID.
    async fn rebind(&self, tr: Trebind) -> P9Result<Message> {
        let username = tr.uname.as_str().map_err(|e| {
            debug!("Invalid rebind username encoding: {:?}", e);
            P9Error::InvalidEncoding
        })?;
        if tr.flags & !P9_REBIND_KNOWN_FLAGS != 0
            || tr.flags & P9_REBIND_OPENED != 0 && tr.flags & P9_REBIND_REPLAY == 0
        {
            return Err(P9Error::InvalidArgument);
        }
        let replay = tr.flags & P9_REBIND_REPLAY != 0;
        let opened = tr.flags & P9_REBIND_OPENED != 0;

        // Like attach/walk, refuse to clobber a fid that's already in use.
        if self.session_state().fids.contains_key(&tr.fid) {
            return Err(P9Error::FidInUse);
        }
        // The inode lock orders validation and fid installation against unlink.
        let _inode_guard = self.filesystem.lock_manager.acquire(tr.inode_id).await;
        let inode = self.filesystem.inode_store.get(tr.inode_id).await?;

        let root_inode = tr.root_inode;
        let establishing_root = tr.inode_id == root_inode;

        // Replay does not restore connection-local open-unlinked handles.
        if inode.nlink() == 0 {
            return Err(P9Error::Fs(if replay {
                FsError::StaleHandle
            } else {
                FsError::PermissionDenied
            }));
        }

        if establishing_root {
            // Attach-root replay uses stable directory identity.
            if !matches!(inode, Inode::Directory(_)) {
                return Err(P9Error::NotADirectory);
            }
        } else {
            // Manual rebind requires current ancestry below the attach root.
            // Automatic replay may restore a previously recorded linked fid.
            if root_inode != 0 && !replay {
                let mut current_id = tr.inode_id;
                let mut current = inode.clone();
                let mut hops = 0u32;
                while current_id != root_inode {
                    if hops >= MAX_ANCESTOR_HOPS {
                        // Bound ancestry traversal under concurrent rename.
                        return Err(P9Error::Fs(FsError::PermissionDenied));
                    }
                    hops += 1;
                    if current_id == 0 {
                        // The inode is outside the attach subtree.
                        return Err(P9Error::Fs(FsError::PermissionDenied));
                    }
                    current_id = current
                        .parent()
                        .ok_or(P9Error::Fs(FsError::PermissionDenied))?;
                    current = self.filesystem.inode_store.get(current_id).await?;
                }
            }
        }

        let creds = self.creds_for(tr.n_uname, username);
        let path = self
            .filesystem
            .inode_store
            .resolve_path_components(tr.inode_id)
            .await
            .into_iter()
            .map(Bytes::from)
            .collect();
        let qid = inode_to_qid(&inode, tr.inode_id);

        // Opened replay installs one provisional pin under the inode lock;
        // `Tlopen` consumes that pin.
        let mut state = self.connected_session()?;
        if state.fids.contains_key(&tr.fid) {
            return Err(P9Error::FidInUse);
        }
        let handle = opened.then(|| self.filesystem.new_open_handle(tr.inode_id));
        state.fids.insert(
            tr.fid,
            FidSlot {
                fid: Fid {
                    path,
                    inode_id: tr.inode_id,
                    root: root_inode,
                    qid: qid.clone(),
                    opened: false,
                    mode: None,
                    creds,
                },
                handle,
                lock_guard: None,
            },
        );
        Ok(Message::Rrebind(Rrebind { qid }))
    }

    /// Resolve one walk component from `current_id`. `..` resolves through the
    /// directory's parent pointer, clamped at `root_id` (the fid's attach
    /// root): walking `..` at the subtree root yields the subtree root itself,
    /// exactly mirroring the inode-0 clamp readdir applies at the filesystem
    /// root. Everything else is a plain lookup.
    async fn walk_component(
        &self,
        creds: &Credentials,
        root_id: InodeId,
        current_id: InodeId,
        name: &[u8],
    ) -> P9Result<InodeId> {
        if name != b".." {
            return Ok(self.filesystem.lookup(creds, current_id, name).await?);
        }
        let inode = self.filesystem.inode_store.get(current_id).await?;
        let Inode::Directory(dir) = &inode else {
            return Err(P9Error::NotADirectory);
        };
        check_access(&inode, creds, AccessMode::Execute)?;
        if current_id == root_id {
            return Ok(current_id);
        }
        // On an aname-rooted fid the parent pointer alone is not enough: a
        // concurrent rename can re-point an ancestor outside the subtree, so
        // `..` is only honored while the parent chain still passes through
        // the attach root. A detached chain clamps to the root itself, the
        // same way `..` at the root does (mirrors the membership walk rebind
        // performs).
        if root_id != 0 {
            let mut ancestor = dir.parent;
            let mut hops = 0u32;
            while ancestor != root_id {
                if ancestor == 0 || hops >= MAX_ANCESTOR_HOPS {
                    // Reached the filesystem root (or an implausibly long /
                    // concurrently re-pointed chain) without passing the attach
                    // root: clamp to the root, the same safe direction as a
                    // detached chain.
                    return Ok(root_id);
                }
                hops += 1;
                let Inode::Directory(d) = self.filesystem.inode_store.get(ancestor).await? else {
                    return Ok(root_id);
                };
                ancestor = d.parent;
            }
        }
        Ok(dir.parent)
    }

    /// Keep a fid's absolute path in step with a walk: `..` pops a component
    /// (unless clamped at the attach root, where `current_id == root`), any
    /// other name appends. Shared by `walk` and `walk_getattr`.
    fn step_path(path: &mut Vec<Bytes>, name: Bytes, current_id: InodeId, root: InodeId) {
        if name.as_ref() == b".." {
            if current_id != root {
                path.pop();
            }
        } else {
            path.push(name);
        }
    }

    async fn walk(&self, tw: Twalk) -> P9Result<Message> {
        let src_fid = self.get_fid(tw.fid)?;

        let mut current_path = src_fid.path;
        let mut current_id = src_fid.inode_id;
        let mut wqids = Vec::new();
        let has_wnames = !tw.wnames.is_empty();

        for (i, wname) in tw.wnames.into_iter().enumerate() {
            let name_bytes = Bytes::from(wname.data);

            let creds = src_fid.creds;
            let child_id = match self
                .walk_component(&creds, src_fid.root, current_id, &name_bytes)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    // Per 9P spec: if first element fails, return error.
                    // If later element fails, return partial Rwalk with qids so far.
                    if i == 0 {
                        return Err(e);
                    }
                    // Partial walk - return what we have so far (newfid is NOT created)
                    return Ok(Message::Rwalk(Rwalk {
                        nwqid: wqids.len() as u16,
                        wqids,
                    }));
                }
            };

            let child_inode = match self.filesystem.inode_store.get(child_id).await {
                Ok(inode) => inode,
                Err(e) => {
                    if i == 0 {
                        return Err(e.into());
                    }
                    return Ok(Message::Rwalk(Rwalk {
                        nwqid: wqids.len() as u16,
                        wqids,
                    }));
                }
            };

            Self::step_path(&mut current_path, name_bytes, current_id, src_fid.root);
            wqids.push(inode_to_qid(&child_inode, child_id));
            current_id = child_id;
        }

        // Only create newfid if the walk fully succeeded
        if tw.newfid != tw.fid || has_wnames {
            let new_fid = Fid {
                path: current_path,
                inode_id: current_id,
                root: src_fid.root,
                qid: wqids.last().cloned().unwrap_or(src_fid.qid),
                opened: false,
                mode: None,
                creds: src_fid.creds, // Inherit credentials from source fid
            };
            // In-place replacement drops the old slot's resource guards.
            self.install_walk_fid(tw.fid, tw.newfid, new_fid)?;
        }

        Ok(Message::Rwalk(Rwalk {
            nwqid: wqids.len() as u16,
            wqids,
        }))
    }

    // ZeroFS fast path: a full walk that also returns the final inode's stat, so
    // the client's lookup costs one round trip instead of walk + getattr. Unlike
    // Twalk there is no partial result. Any miss is a hard error.
    async fn walk_getattr(&self, tw: Twalkgetattr) -> P9Result<Message> {
        let src_fid = self.get_fid(tw.fid)?;

        let mut current_path = src_fid.path;
        let mut current_id = src_fid.inode_id;
        let mut wqids = Vec::new();
        let mut last_inode = None;

        for wname in tw.wnames {
            let name_bytes = Bytes::from(wname.data);
            let child_id = self
                .walk_component(&src_fid.creds, src_fid.root, current_id, &name_bytes)
                .await?;
            let child_inode = self.filesystem.inode_store.get(child_id).await?;
            Self::step_path(&mut current_path, name_bytes, current_id, src_fid.root);
            wqids.push(inode_to_qid(&child_inode, child_id));
            current_id = child_id;
            last_inode = Some(child_inode);
        }

        // Complete fallible reads before publishing `newfid`.
        let stat_inode = match last_inode {
            Some(inode) => inode,
            None => self.filesystem.inode_store.get(current_id).await?,
        };
        self.install_walk_fid(
            tw.fid,
            tw.newfid,
            Fid {
                path: current_path,
                inode_id: current_id,
                root: src_fid.root,
                qid: wqids.last().cloned().unwrap_or_else(|| src_fid.qid.clone()),
                opened: false,
                mode: None,
                creds: src_fid.creds,
            },
        )?;

        Ok(Message::Rwalkgetattr(Rwalkgetattr {
            nwqid: wqids.len() as u16,
            wqids,
            stat: inode_to_stat(&stat_inode, current_id),
        }))
    }

    async fn lopen(&self, tl: Tlopen) -> P9Result<Message> {
        let fid_entry = self.get_fid(tl.fid)?;

        if fid_entry.opened {
            return Err(P9Error::FidAlreadyOpen);
        }

        let inode_id = fid_entry.inode_id;
        let creds = fid_entry.creds;

        debug!(
            "lopen: fid={}, inode_id={}, uid={}, gid={}, flags={:#x}",
            tl.fid, inode_id, creds.uid, creds.gid, tl.flags
        );

        // Liveness check + open + handle-guard install, all under the inode lock
        // so the increment is ordered against remove/rename's defer decision.
        let inode = {
            let _guard = self.filesystem.lock_manager.acquire(inode_id).await;
            let inode = self.filesystem.inode_store.get(inode_id).await?;
            let qid = inode_to_qid(&inode, inode_id);
            let mut state = self.connected_session()?;
            match state.fids.get_mut(&tl.fid) {
                Some(slot) => {
                    // Serialize racing opens under the session lock.
                    if slot.fid.opened {
                        return Err(P9Error::FidAlreadyOpen);
                    }
                    slot.fid.qid = qid;
                    slot.fid.opened = true;
                    slot.fid.mode = Some(tl.flags);
                    // Publish open state and its guard atomically.
                    if slot.handle.is_none() {
                        slot.handle = Some(self.filesystem.new_open_handle(inode_id));
                    }
                }
                None => return Err(P9Error::BadFid),
            }
            inode
        };

        let qid = inode_to_qid(&inode, inode_id);

        Ok(Message::Rlopen(Rlopen {
            qid,
            iounit: self.iounit(),
        }))
    }

    // ZeroFS fast path: open `fid`'s inode on a fresh `newfid`, leaving `fid`
    // untouched, so the client's open costs one round trip instead of the
    // Twalk(clone) + Tlopen pair that standard Tlopen's fid-mutating semantics
    // force on it.
    async fn lopenat(&self, tl: Tlopenat) -> P9Result<Message> {
        let src_fid = self.get_fid(tl.fid)?;

        if tl.newfid != tl.fid && self.session_state().fids.contains_key(&tl.newfid) {
            return Err(P9Error::FidInUse);
        }

        // The inode lock orders fid publication against unlink.
        let inode = {
            let _guard = self.filesystem.lock_manager.acquire(src_fid.inode_id).await;
            let inode = self.filesystem.inode_store.get(src_fid.inode_id).await?;
            let qid = inode_to_qid(&inode, src_fid.inode_id);
            let mut state = self.connected_session()?;
            let new_fid = Fid {
                path: src_fid.path.clone(),
                inode_id: src_fid.inode_id,
                root: src_fid.root,
                qid,
                opened: true,
                mode: Some(tl.flags),
                creds: src_fid.creds,
            };
            self.install_open_fid(&mut state, tl.newfid, tl.fid, new_fid)?;
            inode
        };

        let qid = inode_to_qid(&inode, src_fid.inode_id);

        Ok(Message::Rlopenat(Rlopenat {
            qid,
            iounit: self.iounit(),
        }))
    }

    // Tlopenatread = Tlopenat + a best-effort read of [0, count). The open is
    // authoritative: the inline read never fails it, so a read error returns the
    // open result with empty data (eof=0) and the client falls back to a plain Tread.
    async fn lopenatread(&self, tl: Tlopenatread) -> P9Result<Message> {
        // Reuse the open path so the fid/handle bookkeeping is identical to Tlopenat.
        let (qid, iounit) = match self
            .lopenat(Tlopenat {
                fid: tl.fid,
                newfid: tl.newfid,
                flags: tl.flags,
            })
            .await?
        {
            Message::Rlopenat(r) => (r.qid, r.iounit),
            other => return Ok(other),
        };

        // The new fid is open; prefetch from offset 0, clamped to msize. eof here
        // means the read reached end of file, i.e. (offset 0) the whole file fit in
        // `count`, which is what lets the client serve the file's reads from `data`.
        let new_fid = self.get_fid(tl.newfid)?;
        // Clamp with the Rlopenatread overhead (not P9_IOHDRSZ): its reply carries an
        // extra qid+iounit+eof, so the whole `29 + count` frame must fit the msize.
        let count = self.clamp_count(tl.count, P9_RLOPENATREAD_HDR);
        let auth = AuthContext::from(&new_fid.creds);
        let (data, eof) = match self
            .filesystem
            .read_file(&auth, new_fid.inode_id, 0, count)
            .await
        {
            Ok((d, e)) => (DekuBytes::from(d), e),
            Err(_) => (DekuBytes::default(), false),
        };

        Ok(Message::Rlopenatread(Rlopenatread {
            qid,
            iounit,
            eof: eof as u8,
            count: data.len() as u32,
            data,
        }))
    }

    async fn clunk(&self, tc: Tclunk) -> Message {
        let slot = self.session_state().fids.remove(&tc.fid);
        // Release resources outside the session lock.
        drop(slot);
        Message::Rclunk(Rclunk)
    }

    /// Implicitly clunk every fid, as required by Tversion.
    #[cfg(test)]
    fn close_all_open_handles(&self) {
        let fids = std::mem::take(&mut self.session_state().fids);
        drop(fids);
    }

    /// Retire resources while retaining fid metadata for received tasks.
    pub fn close_for_disconnect(&self) {
        let resources = {
            let mut state = self.session_state();
            if state.disconnected {
                return;
            }
            state.disconnected = true;

            state
                .fids
                .values_mut()
                .map(|slot| (slot.handle.take(), slot.lock_guard.take()))
                .collect::<Vec<_>>()
        };
        drop(resources);
        // Release locks not represented by slot guards.
        self.lock_manager.release_session_locks(self.handler_id);
    }

    /// At the attach root of an aname-rooted session, rewrite the synthesized
    /// ".." readdir entry to the root itself, mirroring the self-clamp inode 0
    /// gets, so the external parent's inode id and attributes don't leak out
    /// of the subtree.
    async fn clamp_dotdot_at_attach_root(
        &self,
        fid: &Fid,
        entries: &mut [crate::fs::types::DirEntry],
    ) -> P9Result<()> {
        if fid.root == 0 || fid.inode_id != fid.root {
            return Ok(());
        }
        if let Some(entry) = entries.iter_mut().find(|e| e.name == b"..") {
            let inode = self.filesystem.inode_store.get(fid.inode_id).await?;
            entry.fileid = fid.inode_id;
            entry.attr = InodeWithId {
                inode: &inode,
                id: fid.inode_id,
            }
            .into();
        }
        Ok(())
    }

    async fn readdir(&self, tr: Treaddir) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;

        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        let count = self.clamp_count(tr.count, P9_IOHDRSZ);

        let auth = AuthContext::from(&fid_entry.creds);

        // tr.offset is the cookie from the last entry the client received (0 for first call)
        // Pass it directly to readdir which handles . and .. with cookies 1 and 2
        let mut result = self
            .filesystem
            .readdir(&auth, fid_entry.inode_id, tr.offset, P9_READDIR_BATCH_SIZE)
            .await?;
        self.clamp_dotdot_at_attach_root(&fid_entry, &mut result.entries)
            .await?;

        let mut dir_entries = Vec::new();
        let mut total_size = 0usize;

        for entry in result.entries {
            let dirent = DirEntry {
                qid: attrs_to_qid(&entry.attr, entry.fileid),
                offset: entry.cookie, // Use cookie as offset for client to resume
                type_: filetype_to_dt(entry.attr.file_type),
                name: P9String::new(entry.name),
            };

            let entry_size = dirent.wire_size();

            if total_size + entry_size > count as usize {
                break;
            }

            total_size += entry_size;
            dir_entries.push(dirent);
        }

        Ok(Message::Rreaddir(
            Rreaddir::from_entries(dir_entries).unwrap_or(Rreaddir {
                count: 0,
                data: DekuBytes::default(),
            }),
        ))
    }

    // ZeroFS fast path (readdirplus): like readdir, but each entry carries its
    // full stat. The attributes come straight from the readdir result, so there
    // are no extra fetches; it lets the client populate the kernel's attr cache
    // and skip a getattr/lookup per entry.
    async fn readdir_attr(&self, tr: Treaddirattr) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;
        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        let count = self.clamp_count(tr.count, P9_IOHDRSZ);

        let auth = AuthContext::from(&fid_entry.creds);
        let mut result = self
            .filesystem
            .readdir(&auth, fid_entry.inode_id, tr.offset, P9_READDIR_BATCH_SIZE)
            .await?;
        self.clamp_dotdot_at_attach_root(&fid_entry, &mut result.entries)
            .await?;

        let mut entries = Vec::new();
        let mut total_size = 0usize;
        for entry in result.entries {
            let plus = DirEntryPlus {
                qid: attrs_to_qid(&entry.attr, entry.fileid),
                offset: entry.cookie,
                type_: filetype_to_dt(entry.attr.file_type),
                stat: attrs_to_stat(&entry.attr, entry.fileid),
                name: P9String::new(entry.name),
            };

            let entry_size = plus.wire_size();
            if total_size + entry_size > count as usize {
                break;
            }
            total_size += entry_size;
            entries.push(plus);
        }

        Ok(Message::Rreaddirattr(
            Rreaddirattr::from_entries(entries).unwrap_or(Rreaddirattr {
                count: 0,
                data: DekuBytes::default(),
            }),
        ))
    }

    /// Create a regular file under `parent_fid`, returning the new inode's id
    /// and post-op attributes. Shared by Tlcreate and Tlcreateattr.
    async fn create_child(
        &self,
        parent_fid: &Fid,
        name: &[u8],
        mode: u32,
        client_gid: u32,
        op_id: crate::dedup::OpId,
    ) -> P9Result<(InodeId, FileAttributes)> {
        let gid = self.effective_gid(client_gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .create_idempotent(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                name,
                &SetAttributes {
                    mode: SetMode::Set(mode),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
                op_id,
            )
            .await?)
    }

    async fn lcreate(&self, tc: Tlcreate, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let parent_fid = self.get_fid(tc.fid)?;

        if parent_fid.opened {
            return Err(P9Error::FidAlreadyOpen);
        }

        let (child_id, post_attr) = self
            .create_child(&parent_fid, &tc.name.data, tc.mode, tc.gid, op_id)
            .await?;

        let qid = attrs_to_qid(&post_attr, child_id);

        // The child lock orders fid publication against unlink.
        {
            let _guard = self.filesystem.lock_manager.acquire(child_id).await;
            match self.filesystem.inode_store.get(child_id).await {
                Ok(_) => {}
                Err(FsError::NotFound) => return Err(FsError::StaleHandle.into()),
                Err(error) => return Err(error.into()),
            }
            let mut state = self.connected_session()?;
            let slot = state.fids.get_mut(&tc.fid).ok_or(P9Error::BadFid)?;
            // Serialize racing creates under the session lock.
            if slot.fid.opened {
                return Err(P9Error::FidAlreadyOpen);
            }
            slot.fid.path.push(Bytes::from(tc.name.data));
            slot.fid.inode_id = child_id;
            slot.fid.qid = qid.clone();
            slot.fid.opened = true;
            slot.fid.mode = Some(tc.flags);
            slot.handle = Some(self.filesystem.new_open_handle(child_id));
        }

        Ok(Message::Rlcreate(Rlcreate {
            qid,
            iounit: self.iounit(),
        }))
    }

    // ZeroFS fast path: Tlcreate + the follow-up walk/getattr in one round trip.
    // Unlike Tlcreate, the directory fid is NOT mutated; the created file is
    // opened on `newfid` and the reply carries its full post-op stat.
    async fn lcreateattr(&self, tc: Tlcreateattr, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let parent_fid = self.get_fid(tc.dfid)?;

        if tc.newfid != tc.dfid && self.session_state().fids.contains_key(&tc.newfid) {
            return Err(P9Error::FidInUse);
        }

        let (child_id, post_attr) = self
            .create_child(&parent_fid, &tc.name.data, tc.mode, tc.gid, op_id)
            .await?;

        let mut path = parent_fid.path;
        path.push(Bytes::from(tc.name.data));
        // The child lock orders fid publication against unlink.
        {
            let _guard = self.filesystem.lock_manager.acquire(child_id).await;
            match self.filesystem.inode_store.get(child_id).await {
                Ok(_) => {}
                Err(FsError::NotFound) => return Err(FsError::StaleHandle.into()),
                Err(error) => return Err(error.into()),
            }
            let mut state = self.connected_session()?;
            let new_fid = Fid {
                path,
                inode_id: child_id,
                root: parent_fid.root,
                qid: attrs_to_qid(&post_attr, child_id),
                opened: true,
                mode: Some(tc.flags),
                creds: parent_fid.creds,
            };
            self.install_open_fid(&mut state, tc.newfid, tc.dfid, new_fid)?;
        }

        Ok(Message::Rlcreateattr(Rlcreateattr {
            iounit: self.iounit(),
            stat: attrs_to_stat(&post_attr, child_id),
        }))
    }

    async fn read(&self, tr: Tread) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;

        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        let count = self.clamp_count(tr.count, P9_IOHDRSZ);

        let auth = AuthContext::from(&fid_entry.creds);

        let (data, _eof) = self
            .filesystem
            .read_file(&auth, fid_entry.inode_id, tr.offset, count)
            .await?;

        Ok(Message::Rread(Rread {
            count: data.len() as u32,
            data: DekuBytes::from(data),
        }))
    }

    async fn write(&self, tw: Twrite, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let fid_entry = self.get_fid(tw.fid)?;

        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }

        debug!(
            "write: fid={}, inode_id={}, uid={}, gid={}, offset={}, data_len={}",
            tw.fid,
            fid_entry.inode_id,
            fid_entry.creds.uid,
            fid_entry.creds.gid,
            tw.offset,
            tw.data.len()
        );

        let auth = AuthContext::from(&fid_entry.creds);
        let data_len = tw.data.len();
        let data = Bytes::from(tw.data);

        self.filesystem
            .write_idempotent(&auth, fid_entry.inode_id, tw.offset, &data, op_id)
            .await
            .inspect_err(|&e| {
                debug!("write: failed with error: {:?}", e);
            })?;

        debug!("write: succeeded");
        Ok(Message::Rwrite(Rwrite {
            count: data_len as u32,
        }))
    }

    async fn getattr(&self, tg: Tgetattr) -> P9Result<Message> {
        let fid_entry = self.get_fid(tg.fid)?;

        let inode = self.filesystem.inode_store.get(fid_entry.inode_id).await?;

        Ok(Message::Rgetattr(Rgetattr {
            valid: tg.request_mask & GETATTR_ALL,
            stat: inode_to_stat(&inode, fid_entry.inode_id),
        }))
    }

    async fn setattr(&self, ts: Tsetattr, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (inode_id, creds) = self.get_fid_inode_and_creds(ts.fid)?;
        let attr = SetAttributes::from(&ts);

        self.filesystem
            .setattr_idempotent(&creds, inode_id, &attr, op_id)
            .await?;
        Ok(Message::Rsetattr(Rsetattr))
    }

    async fn fallocate(&self, tf: Tfallocate, op_id: crate::dedup::OpId) -> P9Result<Message> {
        if !self.zerofs_protocol_enabled() {
            return Err(P9Error::NotSupported);
        }
        let fid_entry = self.get_fid(tf.fid)?;
        if !fid_entry.opened {
            return Err(P9Error::FidNotOpen);
        }
        let mode = match classify_fallocate_mode(tf.mode) {
            Some(FallocateKind::Allocate) => FallocateMode::Allocate,
            Some(FallocateKind::PunchHole) => FallocateMode::PunchHole,
            Some(FallocateKind::ZeroRange { keep_size }) => FallocateMode::ZeroRange { keep_size },
            None => return Err(P9Error::NotSupported),
        };

        let auth = AuthContext::from(&fid_entry.creds);
        if crate::dedup::has_op_id(&op_id) {
            self.filesystem
                .fallocate_idempotent(&auth, fid_entry.inode_id, tf.offset, tf.length, mode, op_id)
                .await?;
        } else {
            self.filesystem
                .fallocate(&auth, fid_entry.inode_id, tf.offset, tf.length, mode)
                .await?;
        }
        Ok(Message::Rfallocate(Rfallocate))
    }

    // ZeroFS fast path: Tsetattr whose reply carries the post-op stat (which the
    // filesystem computes anyway), sparing the client its follow-up Tgetattr.
    async fn setattr_attr(&self, ts: Tsetattr, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (inode_id, creds) = self.get_fid_inode_and_creds(ts.fid)?;
        let attr = SetAttributes::from(&ts);

        let post_attr = self
            .filesystem
            .setattr_idempotent(&creds, inode_id, &attr, op_id)
            .await?;
        Ok(Message::Rsetattrattr(Rsetattrattr {
            stat: attrs_to_stat(&post_attr, inode_id),
        }))
    }

    /// The body of Tmkdir, returning the new directory's id and post-op
    /// attributes. Shared by Tmkdir and Tmkdirattr.
    async fn make_dir(
        &self,
        tm: &Tmkdir,
        op_id: crate::dedup::OpId,
    ) -> P9Result<(InodeId, FileAttributes)> {
        let parent_fid = self.get_fid(tm.dfid)?;

        debug!(
            "mkdir: parent_id={}, name={:?}, dfid={}, mode={:o}, gid={}, fid uid={}, fid gid={}",
            parent_fid.inode_id,
            &tm.name.data,
            tm.dfid,
            tm.mode,
            tm.gid,
            parent_fid.creds.uid,
            parent_fid.creds.gid
        );

        let gid = self.effective_gid(tm.gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .mkdir_idempotent(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                &tm.name.data,
                &SetAttributes {
                    mode: SetMode::Set(tm.mode),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
                op_id,
            )
            .await?)
    }

    async fn mkdir(&self, tm: Tmkdir, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_dir(&tm, op_id).await?;
        Ok(Message::Rmkdir(Rmkdir {
            qid: attrs_to_qid(&post_attr, new_id),
        }))
    }

    async fn mkdir_attr(&self, tm: Tmkdir, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_dir(&tm, op_id).await?;
        Ok(Message::Rmkdirattr(Rmkdirattr {
            stat: attrs_to_stat(&post_attr, new_id),
        }))
    }

    /// The body of Tsymlink, returning the new link's id and post-op
    /// attributes. Shared by Tsymlink and Tsymlinkattr.
    async fn make_symlink(
        &self,
        ts: &Tsymlink,
        op_id: crate::dedup::OpId,
    ) -> P9Result<(InodeId, FileAttributes)> {
        let parent_fid = self.get_fid(ts.dfid)?;

        let gid = self.effective_gid(ts.gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .symlink_idempotent(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                &ts.name.data,
                &ts.symtgt.data,
                &SetAttributes {
                    mode: SetMode::Set(SYMLINK_DEFAULT_MODE),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
                op_id,
            )
            .await?)
    }

    async fn symlink(&self, ts: Tsymlink, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_symlink(&ts, op_id).await?;
        Ok(Message::Rsymlink(Rsymlink {
            qid: attrs_to_qid(&post_attr, new_id),
        }))
    }

    async fn symlink_attr(&self, ts: Tsymlink, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (new_id, post_attr) = self.make_symlink(&ts, op_id).await?;
        Ok(Message::Rsymlinkattr(Rsymlinkattr {
            stat: attrs_to_stat(&post_attr, new_id),
        }))
    }

    /// The body of Tmknod, returning the new node's id and post-op attributes.
    /// Shared by Tmknod and Tmknodattr.
    async fn make_node(
        &self,
        tm: &Tmknod,
        op_id: crate::dedup::OpId,
    ) -> P9Result<(InodeId, FileAttributes)> {
        let parent_fid = self.get_fid(tm.dfid)?;

        let file_type = tm.mode & 0o170000; // S_IFMT
        let device_type = match file_type {
            S_IFCHR => FileType::CharDevice,
            S_IFBLK => FileType::BlockDevice,
            S_IFIFO => FileType::Fifo,
            S_IFSOCK => FileType::Socket,
            _ => return Err(P9Error::InvalidDeviceType),
        };

        let gid = self.effective_gid(tm.gid, &parent_fid.creds);
        Ok(self
            .filesystem
            .mknod_idempotent(
                &parent_fid.creds.with_gid(gid),
                parent_fid.inode_id,
                &tm.name.data,
                device_type,
                &SetAttributes {
                    mode: SetMode::Set(tm.mode & 0o7777),
                    uid: SetUid::Set(parent_fid.creds.uid),
                    gid: SetGid::Set(gid),
                    ..Default::default()
                },
                match device_type {
                    FileType::CharDevice | FileType::BlockDevice => Some((tm.major, tm.minor)),
                    _ => None,
                },
                op_id,
            )
            .await?)
    }

    async fn mknod(&self, tm: Tmknod, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (child_id, post_attr) = self.make_node(&tm, op_id).await?;
        Ok(Message::Rmknod(Rmknod {
            qid: attrs_to_qid(&post_attr, child_id),
        }))
    }

    async fn mknod_attr(&self, tm: Tmknod, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (child_id, post_attr) = self.make_node(&tm, op_id).await?;
        Ok(Message::Rmknodattr(Rmknodattr {
            stat: attrs_to_stat(&post_attr, child_id),
        }))
    }

    async fn readlink(&self, tr: Treadlink) -> P9Result<Message> {
        let fid_entry = self.get_fid(tr.fid)?;

        let inode = self.filesystem.inode_store.get(fid_entry.inode_id).await?;

        match inode {
            Inode::Symlink(s) => Ok(Message::Rreadlink(Rreadlink {
                target: P9String::new(s.target.clone()),
            })),
            _ => Err(P9Error::NotASymlink),
        }
    }

    /// Shared `Tlink` implementation returning the retained post-link result.
    async fn make_link(
        &self,
        tl: &Tlink,
        op_id: crate::dedup::OpId,
    ) -> P9Result<(InodeId, FileAttributes)> {
        let dir_fid = self.get_fid(tl.dfid)?;
        let file_fid = self.get_fid(tl.fid)?;

        let dir_id = dir_fid.inode_id;
        let file_id = file_fid.inode_id;
        let creds = dir_fid.creds;
        let name_bytes = &tl.name.data;

        debug!(
            "link: file_id={}, dir_id={}, name={:?}, uid={}, gid={}",
            file_id, dir_id, name_bytes, creds.uid, creds.gid
        );

        let auth = AuthContext::from(&creds);

        Ok(self
            .filesystem
            .link_idempotent(&auth, file_id, dir_id, name_bytes, op_id)
            .await?)
    }

    async fn link(&self, tl: Tlink, op_id: crate::dedup::OpId) -> P9Result<Message> {
        self.make_link(&tl, op_id).await?;
        Ok(Message::Rlink(Rlink))
    }

    async fn link_attr(&self, tl: Tlink, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let (file_id, attrs) = self.make_link(&tl, op_id).await?;
        Ok(Message::Rlinkattr(Rlinkattr {
            stat: attrs_to_stat(&attrs, file_id),
        }))
    }

    async fn rename(&self, tr: Trename, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let source_fid = self.get_fid(tr.fid)?;
        let dest_fid = self.get_fid(tr.dfid)?;

        if source_fid.path.is_empty() {
            return Err(P9Error::InvalidArgument);
        }

        if self
            .filesystem
            .replay_dedup_result(&op_id, crate::dedup::DedupResult::into_rename)?
            .is_some()
        {
            return Ok(Message::Rrename(Rrename));
        }

        let source_name = source_fid.path.last().unwrap();
        let source_parent_path = source_fid.path[..source_fid.path.len() - 1].to_vec();
        let dest_parent_id = dest_fid.inode_id;
        let creds = source_fid.creds;

        let mut source_parent_id = 0;
        for name in &source_parent_path {
            source_parent_id = self
                .filesystem
                .lookup(&creds, source_parent_id, name)
                .await?;
        }

        let new_name_bytes = Bytes::copy_from_slice(&tr.name.data);

        let auth = AuthContext::from(&creds);

        self.filesystem
            .rename_idempotent(
                &auth,
                source_parent_id,
                source_name,
                dest_parent_id,
                &new_name_bytes,
                op_id,
            )
            .await?;

        Ok(Message::Rrename(Rrename))
    }

    async fn renameat(&self, tr: Trenameat, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let old_dir_fid = self.get_fid(tr.olddirfid)?;
        let new_dir_fid = self.get_fid(tr.newdirfid)?;

        let auth = AuthContext::from(&old_dir_fid.creds);

        self.filesystem
            .rename_idempotent(
                &auth,
                old_dir_fid.inode_id,
                &tr.oldname.data,
                new_dir_fid.inode_id,
                &tr.newname.data,
                op_id,
            )
            .await?;

        Ok(Message::Rrenameat(Rrenameat))
    }

    async fn unlinkat(&self, tu: Tunlinkat, op_id: crate::dedup::OpId) -> P9Result<Message> {
        let dir_fid = self.get_fid(tu.dirfid)?;

        let parent_id = dir_fid.inode_id;
        let creds = dir_fid.creds;

        if self
            .filesystem
            .replay_dedup_result(&op_id, crate::dedup::DedupResult::into_remove)?
            .is_some()
        {
            return Ok(Message::Runlinkat(Runlinkat));
        }

        let child_id = self
            .filesystem
            .lookup(&creds, parent_id, &tu.name.data)
            .await?;

        let child_inode = self.filesystem.inode_store.get(child_id).await?;

        let is_dir = matches!(child_inode, Inode::Directory(_));

        // If AT_REMOVEDIR is set, we must be removing a directory
        if (tu.flags & AT_REMOVEDIR) != 0 && !is_dir {
            return Err(P9Error::NotADirectory);
        }

        // If AT_REMOVEDIR is not set, we must not be removing a directory
        if (tu.flags & AT_REMOVEDIR) == 0 && is_dir {
            return Err(P9Error::IsADirectory);
        }

        let auth = AuthContext::from(&creds);

        self.filesystem
            .remove_idempotent(&auth, parent_id, &tu.name.data, op_id)
            .await?;

        Ok(Message::Runlinkat(Runlinkat))
    }

    async fn fsync(&self, tf: Tfsync) -> P9Result<Message> {
        let fid = self.get_fid(tf.fid)?;
        let fid_path = fid.path.clone();

        self.filesystem.client_fsync().await?;

        {
            let path = if fid_path.is_empty() {
                "/".to_string()
            } else {
                format!(
                    "/{}",
                    fid_path
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect::<Vec<_>>()
                        .join("/")
                )
            };
            self.filesystem
                .tracer
                .emit_with_path(path, FileOperation::Fsync);
        }

        Ok(Message::Rfsync(Rfsync))
    }

    /// Flush and verify the client's oldest unflushed-write lineage token.
    /// Token zero means no unflushed write; a broken lineage returns `ESTALE`.
    async fn fsyncdur(&self, tf: Tfsyncdur) -> P9Result<Message> {
        let fid = self.get_fid(tf.fid)?;
        let fid_path = fid.path.clone();

        self.filesystem.client_fsync_verified(tf.token).await?;

        {
            let path = if fid_path.is_empty() {
                "/".to_string()
            } else {
                format!(
                    "/{}",
                    fid_path
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect::<Vec<_>>()
                        .join("/")
                )
            };
            self.filesystem
                .tracer
                .emit_with_path(path, FileOperation::Fsync);
        }

        Ok(Message::Rfsync(Rfsync))
    }

    /// Return the connection's durability lineage token.
    async fn getlineage(&self) -> P9Result<Message> {
        Ok(Message::Rgetlineage(Rgetlineage {
            token: self.filesystem.lineage_token,
            writer_epoch: self.filesystem.serving_writer_epoch,
        }))
    }

    async fn statfs(&self, ts: Tstatfs) -> P9Result<Message> {
        if !self.session_state().fids.contains_key(&ts.fid) {
            return Err(P9Error::BadFid);
        }

        let (used_bytes, used_inodes) = self.filesystem.global_stats.get_totals();

        const BLOCK_SIZE: u32 = 4096; // 4KB blocks

        let total_bytes = self.filesystem.max_bytes;

        let total_blocks = total_bytes.div_ceil(BLOCK_SIZE as u64);
        let used_blocks = used_bytes.div_ceil(BLOCK_SIZE as u64);
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        // Fixed, signed-safe inode capacity. A u64::MAX-based count renders as a
        // negative value under GNU `stat`/`df` (signed %d) and breaks their inode
        // accounting; `available` = capacity - in-use.
        let total_inodes = crate::fs::TOTAL_INODES;
        let available_inodes = total_inodes.saturating_sub(used_inodes);

        let statfs = Rstatfs {
            r#type: 0x5a45524f,
            bsize: BLOCK_SIZE,
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: total_inodes,
            ffree: available_inodes,
            fsid: 0,
            namelen: P9_MAX_NAME_LEN,
        };

        Ok(Message::Rstatfs(statfs))
    }

    async fn lock(&self, tl: Tlock) -> P9Result<Message> {
        let fid = self.get_fid(tl.fid)?;

        if matches!(tl.lock_type, LockType::Unlock) {
            self.lock_manager.unlock_range(
                fid.inode_id,
                tl.fid,
                tl.start,
                tl.length,
                self.handler_id,
            );

            return Ok(Message::Rlock(Rlock {
                status: LockStatus::Success,
            }));
        }

        // Publish lock state and its guard under the session lock.
        let mut state = self.connected_session()?;
        let slot = match state.fids.get_mut(&tl.fid) {
            Some(slot) => slot,
            None => return Err(P9Error::BadFid),
        };

        let new_lock = FileLock {
            lock_type: tl.lock_type,
            start: tl.start,
            length: tl.length,
            proc_id: tl.proc_id,
            client_id: tl.client_id.data.clone(),
            fid: tl.fid,
            inode_id: slot.fid.inode_id,
        };

        if !self.lock_manager.try_add_lock(self.handler_id, new_lock) {
            debug!("Lock conflict on inode {}", slot.fid.inode_id);
            if (tl.flags & P9_LOCK_FLAGS_BLOCK) != 0 {
                return Ok(Message::Rlock(Rlock {
                    status: LockStatus::Blocked,
                }));
            } else {
                return Err(P9Error::LockConflict);
            }
        }

        #[cfg(feature = "failpoints")]
        fail_point!(fp::LOCK_AFTER_REGISTER_BEFORE_GUARD);

        // One guard releases all locks for this session and fid.
        if slot.lock_guard.is_none() {
            slot.lock_guard = Some(LockGuard::new(
                self.lock_manager.clone(),
                self.handler_id,
                tl.fid,
            ));
        }

        Ok(Message::Rlock(Rlock {
            status: LockStatus::Success,
        }))
    }

    async fn getlock(&self, tg: Tgetlock) -> P9Result<Message> {
        let fid = self.get_fid(tg.fid)?;

        let test_lock = FileLock {
            lock_type: tg.lock_type,
            start: tg.start,
            length: tg.length,
            proc_id: tg.proc_id,
            client_id: tg.client_id.data.clone(),
            fid: tg.fid,
            inode_id: fid.inode_id,
        };

        if let Some(conflicting_lock) =
            self.lock_manager
                .check_would_block(fid.inode_id, &test_lock, self.handler_id)
        {
            Ok(Message::Rgetlock(Rgetlock {
                lock_type: conflicting_lock.lock_type,
                start: conflicting_lock.start,
                length: conflicting_lock.length,
                proc_id: conflicting_lock.proc_id,
                client_id: P9String::new(conflicting_lock.client_id.clone()),
            }))
        } else {
            Ok(Message::Rgetlock(Rgetlock {
                lock_type: LockType::Unlock,
                start: tg.start,
                length: tg.length,
                proc_id: 0,
                client_id: P9String::new(Vec::new()),
            }))
        }
    }
}

pub fn inode_to_qid(inode: &Inode, inode_id: u64) -> Qid {
    let type_ = match inode {
        Inode::Directory(_) => QID_TYPE_DIR,
        Inode::Symlink(_) => QID_TYPE_SYMLINK,
        _ => QID_TYPE_FILE,
    };

    Qid {
        type_,
        version: inode.mtime() as u32,
        path: inode_id,
    }
}

pub fn attrs_to_qid(attrs: &FileAttributes, fileid: u64) -> Qid {
    let type_ = match attrs.file_type {
        FileType::Directory => QID_TYPE_DIR,
        FileType::Symlink => QID_TYPE_SYMLINK,
        _ => QID_TYPE_FILE,
    };

    Qid {
        type_,
        version: attrs.mtime.seconds as u32,
        path: fileid,
    }
}

pub fn filetype_to_dt(ft: FileType) -> u8 {
    match ft {
        FileType::Directory => DT_DIR,
        FileType::Regular => DT_REG,
        FileType::Symlink => DT_LNK,
        FileType::CharDevice => DT_CHR,
        FileType::BlockDevice => DT_BLK,
        FileType::Fifo => DT_FIFO,
        FileType::Socket => DT_SOCK,
    }
}

// Build a wire Stat from the readdir-supplied attributes (already fetched, so
// readdirplus costs no extra round trips). Mirrors `inode_to_stat`.
pub fn attrs_to_stat(attrs: &FileAttributes, fileid: u64) -> Stat {
    let type_bits = match attrs.file_type {
        FileType::Regular => S_IFREG,
        FileType::Directory => S_IFDIR,
        FileType::Symlink => S_IFLNK,
        FileType::CharDevice => S_IFCHR,
        FileType::BlockDevice => S_IFBLK,
        FileType::Fifo => S_IFIFO,
        FileType::Socket => S_IFSOCK,
    };
    let rdev = attrs
        .rdev
        .map_or(0, |(maj, min)| ((maj as u64) << 8) | (min as u64));
    // FileAttributes uses the NFS-facing conventional 4 KiB directory size,
    // while the established 9P getattr representation reports directory size
    // as zero. Keep compound/readdir stats identical to `inode_to_stat`.
    let size = if attrs.file_type == FileType::Directory {
        0
    } else {
        attrs.size
    };
    Stat {
        qid: attrs_to_qid(attrs, fileid),
        mode: (attrs.mode & 0o7777) | type_bits,
        uid: attrs.uid,
        gid: attrs.gid,
        nlink: attrs.nlink as u64,
        rdev,
        size,
        blksize: DEFAULT_BLKSIZE,
        blocks: size.div_ceil(BLOCK_SIZE),
        atime_sec: attrs.atime.seconds,
        atime_nsec: attrs.atime.nanoseconds as u64,
        mtime_sec: attrs.mtime.seconds,
        mtime_nsec: attrs.mtime.nanoseconds as u64,
        ctime_sec: attrs.ctime.seconds,
        ctime_nsec: attrs.ctime.nanoseconds as u64,
        btime_sec: 0,
        btime_nsec: 0,
        r#gen: 0,
        data_version: 0,
    }
}

pub fn inode_to_stat(inode: &Inode, inode_id: u64) -> Stat {
    let (type_bits, size, rdev) = match inode {
        Inode::File(f) => (S_IFREG, f.size, 0),
        Inode::Directory(_) => (S_IFDIR, 0, 0),
        Inode::Symlink(s) => (S_IFLNK, s.target.len() as u64, 0),
        Inode::CharDevice(d) => (
            S_IFCHR,
            0,
            d.rdev
                .map_or(0, |(maj, min)| ((maj as u64) << 8) | (min as u64)),
        ),
        Inode::BlockDevice(d) => (
            S_IFBLK,
            0,
            d.rdev
                .map_or(0, |(maj, min)| ((maj as u64) << 8) | (min as u64)),
        ),
        Inode::Fifo(_) => (S_IFIFO, 0, 0),
        Inode::Socket(_) => (S_IFSOCK, 0, 0),
    };

    Stat {
        qid: inode_to_qid(inode, inode_id),
        mode: inode.mode() | type_bits,
        uid: inode.uid(),
        gid: inode.gid(),
        nlink: inode.nlink() as u64,
        rdev,
        size,
        blksize: DEFAULT_BLKSIZE,
        blocks: size.div_ceil(BLOCK_SIZE),
        atime_sec: inode.atime(),
        atime_nsec: inode.atime_nsec() as u64,
        mtime_sec: inode.mtime(),
        mtime_nsec: inode.mtime_nsec() as u64,
        ctime_sec: inode.ctime(),
        ctime_nsec: inode.ctime_nsec() as u64,
        btime_sec: 0,
        btime_nsec: 0,
        r#gen: 0,
        data_version: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::FileLockManager;
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::SetAttributes;
    use libc::O_RDONLY;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    async fn request(handler: &NinePHandler, tag: u16, body: Message) -> P9Message {
        handler.handle_message(tag, body).await
    }

    async fn negotiate(handler: &NinePHandler, msize: u32, version: &[u8]) -> P9Message {
        request(
            handler,
            0,
            Message::Tversion(Tversion {
                msize,
                version: P9String::new(version.to_vec()),
            }),
        )
        .await
    }

    async fn attach(
        handler: &NinePHandler,
        tag: u16,
        fid: u32,
        uname: &[u8],
        aname: &[u8],
        n_uname: u32,
    ) -> P9Message {
        request(
            handler,
            tag,
            Message::Tattach(Tattach {
                fid,
                afid: u32::MAX,
                uname: P9String::new(uname.to_vec()),
                aname: P9String::new(aname.to_vec()),
                n_uname,
            }),
        )
        .await
    }

    async fn start_session(handler: &NinePHandler, msize: u32, version: &[u8], aname: &[u8]) {
        assert!(matches!(
            negotiate(handler, msize, version).await.body,
            Message::Rversion(_)
        ));
        assert!(matches!(
            attach(handler, 1, 1, b"test", aname, 1000).await.body,
            Message::Rattach(_)
        ));
    }

    async fn start_plain_session(handler: &NinePHandler) {
        start_session(handler, DEFAULT_MSIZE, VERSION_9P2000L, b"").await;
    }

    async fn negotiate_zerofs(handler: &NinePHandler) {
        let response = negotiate(handler, DEFAULT_MSIZE, VERSION_9P2000L_ZEROFS).await;
        assert!(matches!(response.body, Message::Rversion(_)));
        assert!(handler.zerofs_protocol_enabled());
    }

    async fn zerofs_handler(fs: &Arc<ZeroFS>) -> NinePHandler {
        let handler = NinePHandler::new(Arc::clone(fs), Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;
        handler
    }

    async fn rebind(
        handler: &NinePHandler,
        tag: u16,
        fid: u32,
        inode_id: u64,
        root_inode: u64,
        flags: u8,
    ) -> Message {
        request(
            handler,
            tag,
            Message::Trebind(Trebind {
                fid,
                inode_id,
                root_inode,
                flags,
                uname: P9String::new(Vec::new()),
                n_uname: test_creds().uid,
            }),
        )
        .await
        .body
    }

    async fn walk_fid(
        handler: &NinePHandler,
        tag: u16,
        fid: u32,
        newfid: u32,
        names: &[&[u8]],
    ) -> P9Message {
        request(
            handler,
            tag,
            Message::Twalk(Twalk {
                fid,
                newfid,
                nwname: names.len() as u16,
                wnames: names
                    .iter()
                    .map(|name| P9String::new(name.to_vec()))
                    .collect(),
            }),
        )
        .await
    }

    async fn expect_walk(handler: &NinePHandler, tag: u16, fid: u32, newfid: u32, names: &[&[u8]]) {
        assert!(matches!(
            walk_fid(handler, tag, fid, newfid, names).await.body,
            Message::Rwalk(_)
        ));
    }

    async fn open_fid(handler: &NinePHandler, tag: u16, fid: u32, flags: u32) {
        assert!(matches!(
            request(handler, tag, Message::Tlopen(Tlopen { fid, flags }))
                .await
                .body,
            Message::Rlopen(_)
        ));
    }

    async fn create_file(handler: &NinePHandler, tag: u16, fid: u32, name: &[u8]) {
        assert!(matches!(
            request(
                handler,
                tag,
                Message::Tlcreate(Tlcreate {
                    fid,
                    name: P9String::new(name.to_vec()),
                    flags: 0x8002,
                    mode: 0o644,
                    gid: 1000,
                }),
            )
            .await
            .body,
            Message::Rlcreate(_)
        ));
    }

    #[tokio::test]
    async fn failed_empty_walk_getattr_does_not_publish_newfid() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs, Arc::new(FileLockManager::new()));
        start_session(&handler, DEFAULT_MSIZE, VERSION_9P2000L_ZEROFS, b"").await;

        let mut stale = handler.get_fid(1).unwrap();
        stale.inode_id = u64::MAX;
        handler
            .session_state()
            .fids
            .insert(2, FidSlot::unopened(stale));
        assert!(
            handler
                .walk_getattr(Twalkgetattr {
                    fid: 2,
                    newfid: 3,
                    nwname: 0,
                    wnames: Vec::new(),
                })
                .await
                .is_err()
        );
        assert!(!handler.session_state().fids.contains_key(&3));
    }

    #[tokio::test]
    async fn rebind_preserves_name_derived_credentials() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs, Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;

        let attached = handler
            .handle_message(
                1,
                Message::Tattach(Tattach {
                    fid: 1,
                    afid: u32::MAX,
                    uname: P9String::new(b"root".to_vec()),
                    aname: P9String::new(Vec::new()),
                    n_uname: u32::MAX,
                }),
            )
            .await;
        assert!(matches!(attached.body, Message::Rattach(_)));
        assert_eq!(handler.get_fid(1).unwrap().creds.uid, 0);

        let rebound = handler
            .handle_message(
                2,
                Message::Trebind(Trebind {
                    fid: 2,
                    inode_id: 0,
                    root_inode: 0,
                    flags: P9_REBIND_REPLAY,
                    uname: P9String::new(b"root".to_vec()),
                    n_uname: u32::MAX,
                }),
            )
            .await;
        assert!(matches!(rebound.body, Message::Rrebind(_)));
        assert_eq!(handler.get_fid(2).unwrap().creds.uid, 0);

        let numeric = handler
            .handle_message(
                3,
                Message::Trebind(Trebind {
                    fid: 3,
                    inode_id: 0,
                    root_inode: 0,
                    flags: P9_REBIND_REPLAY,
                    uname: P9String::new(b"root".to_vec()),
                    n_uname: 1234,
                }),
            )
            .await;
        assert!(matches!(numeric.body, Message::Rrebind(_)));
        assert_eq!(handler.get_fid(3).unwrap().creds.uid, 1234);
    }

    #[test]
    fn only_proven_unapplied_deposal_stays_clean_after_lease_loss() {
        assert_eq!(
            FsError::LeaderRejectedBeforeApply.to_errno(),
            libc::EIO as u32,
            "the private CLEAN code must not leak through generic errno consumers"
        );
        assert_eq!(
            post_dispatch_errno(P9Error::Fs(FsError::LeaderRejectedBeforeApply), false),
            ninep_proto::P9_ENOTLEADER_CLEAN
        );
        assert_eq!(
            post_dispatch_errno(P9Error::Fs(FsError::IoError), false),
            ninep_proto::P9_ENOTLEADER
        );
        assert_eq!(
            post_dispatch_errno(P9Error::Fs(FsError::IoError), true),
            libc::EIO as u32
        );
    }

    #[tokio::test]
    async fn nonmutation_op_envelope_is_rejected_without_entering_dedup() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;

        let response = handler
            .handle_message_with_op_id(1, [0x5a; 16], Message::Tgetlineage(Tgetlineage))
            .await;
        assert!(matches!(
            response.body,
            Message::Rlerror(Rlerror {
                ecode: ninep_proto::P9_EOPIDSTALE
            })
        ));
        assert!(
            fs.dedup.is_empty(),
            "a tagged read/control request must not occupy the mutation ledger"
        );
    }

    #[tokio::test]
    async fn unseen_retry_is_rejected_before_mutation_dispatch() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs, Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;

        let attach = handler
            .handle_message(
                0,
                Message::Tattach(Tattach {
                    fid: 1,
                    afid: u32::MAX,
                    uname: P9String::new(b"test".to_vec()),
                    aname: P9String::new(b"/".to_vec()),
                    n_uname: 1000,
                }),
            )
            .await;
        assert!(matches!(attach.body, Message::Rattach(_)));

        let op_id = [0x5au8; 16];
        let mkdir = Message::Tmkdir(Tmkdir {
            dfid: 1,
            name: P9String::new(b"once".to_vec()),
            mode: 0o755,
            gid: 1000,
        });
        let stale = handler
            .handle_message_with_op_envelope(1, op_id, ninep_proto::P9_OP_FLAG_RETRY, mkdir.clone())
            .await;
        assert!(matches!(
            stale.body,
            Message::Rlerror(Rlerror {
                ecode: ninep_proto::P9_EOPIDSTALE
            })
        ));

        let initial = handler.handle_message_with_op_id(2, op_id, mkdir).await;
        assert!(
            matches!(initial.body, Message::Rmkdir(_)),
            "the rejected resend must not have dispatched the mkdir: {:?}",
            initial.body
        );
    }

    #[tokio::test]
    async fn connected_bad_fid_remains_a_terminal_dedup_result() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;

        let op_id = [0x64; 16];
        let mkdir = Message::Tmkdir(Tmkdir {
            dfid: 77,
            name: P9String::new(b"must-stay-absent".to_vec()),
            mode: 0o755,
            gid: 1000,
        });
        let first = handler
            .handle_message_with_op_id(1, op_id, mkdir.clone())
            .await;
        assert!(matches!(
            first.body,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EBADF as u32
        ));
        assert!(matches!(
            fs.dedup.get(&op_id),
            Some(crate::dedup::DedupResult::Error { errno })
                if errno == libc::EBADF as u32
        ));

        // The fid becomes valid only after the recorded `EBADF` result.
        let attach = handler
            .handle_message(
                2,
                Message::Tattach(Tattach {
                    fid: 77,
                    afid: u32::MAX,
                    uname: P9String::new(b"test".to_vec()),
                    aname: P9String::new(b"/".to_vec()),
                    n_uname: 1000,
                }),
            )
            .await;
        assert!(matches!(attach.body, Message::Rattach(_)));

        let retry = handler
            .handle_message_with_op_envelope(3, op_id, ninep_proto::P9_OP_FLAG_RETRY, mkdir)
            .await;
        assert!(matches!(
            retry.body,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EBADF as u32
        ));
        assert!(matches!(
            fs.lookup(&test_creds(), 0, b"must-stay-absent").await,
            Err(FsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn trename_retry_replays_after_source_parent_is_removed() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let (source_parent, _) = fs
            .mkdir(&creds, 0, b"source-parent", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&creds, source_parent, b"source", &SetAttributes::default())
            .await
            .unwrap();

        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        start_session(&handler, DEFAULT_MSIZE, VERSION_9P2000L_ZEROFS, b"").await;
        expect_walk(&handler, 2, 1, 2, &[b"source-parent", b"source"]).await;

        let op_id = [0x65; 16];
        let rename = Message::Trename(Trename {
            fid: 2,
            dfid: 1,
            name: P9String::new(b"destination".to_vec()),
        });
        let first = handler
            .handle_message_with_op_id(3, op_id, rename.clone())
            .await;
        assert!(matches!(first.body, Message::Rrename(_)));

        fs.remove(&AuthContext::from(&creds), 0, b"source-parent")
            .await
            .unwrap();

        let retry = handler
            .handle_message_with_op_envelope(4, op_id, ninep_proto::P9_OP_FLAG_RETRY, rename)
            .await;
        assert!(matches!(retry.body, Message::Rrename(_)));
        assert!(matches!(
            fs.lookup(&creds, 0, b"source-parent").await,
            Err(FsError::NotFound)
        ));
        assert!(fs.lookup(&creds, 0, b"destination").await.is_ok());
    }

    #[tokio::test]
    async fn tunlinkat_retry_rejects_mismatched_result_type() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        start_session(&handler, DEFAULT_MSIZE, VERSION_9P2000L_ZEROFS, b"").await;

        let op_id = [0x66; 16];
        let mkdir = handler
            .handle_message_with_op_id(
                2,
                op_id,
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(b"kept".to_vec()),
                    mode: 0o755,
                    gid: 1000,
                }),
            )
            .await;
        assert!(matches!(mkdir.body, Message::Rmkdir(_)));

        let unlink = handler
            .handle_message_with_op_envelope(
                3,
                op_id,
                ninep_proto::P9_OP_FLAG_RETRY,
                Message::Tunlinkat(Tunlinkat {
                    dirfid: 1,
                    name: P9String::new(b"kept".to_vec()),
                    flags: AT_REMOVEDIR,
                }),
            )
            .await;
        assert!(matches!(
            unlink.body,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EINVAL as u32
        ));
        assert!(fs.lookup(&test_creds(), 0, b"kept").await.is_ok());
    }

    #[tokio::test]
    async fn promotion_grace_accepts_only_predecessor_retry() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        let dedup = Arc::new(crate::dedup::DedupCache::new_for_test(
            Duration::from_secs(60),
            Instant::now,
        ));
        dedup.arm_promotion_retry_grace(7);
        fs.dedup = dedup;
        let handler = NinePHandler::new(Arc::new(fs), Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;

        let attach = handler
            .handle_message(
                0,
                Message::Tattach(Tattach {
                    fid: 1,
                    afid: u32::MAX,
                    uname: P9String::new(b"test".to_vec()),
                    aname: P9String::new(b"/".to_vec()),
                    n_uname: 1000,
                }),
            )
            .await;
        assert!(matches!(attach.body, Message::Rattach(_)));

        let mkdir = |name: &'static [u8]| {
            Message::Tmkdir(Tmkdir {
                dfid: 1,
                name: P9String::new(name.to_vec()),
                mode: 0o755,
                gid: 1000,
            })
        };
        let admitted = handler
            .handle_message_with_op_envelope_origin(
                1,
                [0x71; 16],
                ninep_proto::P9_OP_FLAG_RETRY,
                7,
                mkdir(b"direct-predecessor"),
            )
            .await;
        assert!(
            matches!(admitted.body, Message::Rmkdir(_)),
            "the coverage-proven direct predecessor retry must dispatch: {:?}",
            admitted.body
        );

        for (tag, op_id, origin, name) in [
            (2, [0x72; 16], 0, b"zero-origin".as_slice()),
            (3, [0x73; 16], 6, b"older-origin".as_slice()),
            (4, [0x74; 16], 8, b"successor-origin".as_slice()),
        ] {
            let stale = handler
                .handle_message_with_op_envelope_origin(
                    tag,
                    op_id,
                    ninep_proto::P9_OP_FLAG_RETRY,
                    origin,
                    mkdir(name),
                )
                .await;
            assert!(matches!(
                stale.body,
                Message::Rlerror(Rlerror {
                    ecode: ninep_proto::P9_EOPIDSTALE
                })
            ));
        }
    }

    #[tokio::test]
    async fn unknown_op_envelope_flags_fail_closed() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs, Arc::new(FileLockManager::new()));
        negotiate_zerofs(&handler).await;
        let response = handler
            .handle_message_with_op_envelope(
                1,
                [0x33; 16],
                !ninep_proto::P9_OP_KNOWN_FLAGS,
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(b"never".to_vec()),
                    mode: 0o755,
                    gid: 1000,
                }),
            )
            .await;
        assert!(matches!(
            response.body,
            Message::Rlerror(Rlerror {
                ecode: ninep_proto::P9_EOPIDSTALE
            })
        ));
    }

    #[tokio::test]
    async fn version_negotiation_accepts_only_exact_dialects() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = NinePHandler::new(fs, Arc::new(FileLockManager::new()));

        // Standard clients retain the requested 9P2000.L dialect.
        let resp = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            )
            .await;
        match resp.body {
            Message::Rversion(rv) => assert_eq!(rv.version.data, VERSION_9P2000L),
            other => panic!("expected Rversion, got {other:?}"),
        }
        assert!(!handler.zerofs_protocol_enabled());
        let private_response = handler
            .handle_message(1, Message::Tgetlineage(Tgetlineage))
            .await;
        assert!(matches!(
            private_response.body,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EOPNOTSUPP as u32
        ));
        let enveloped_response = handler
            .handle_message_with_op_id(
                2,
                [0x11; 16],
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(b"not-dispatched".to_vec()),
                    mode: 0o755,
                    gid: 1000,
                }),
            )
            .await;
        assert!(matches!(
            enveloped_response.body,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EOPNOTSUPP as u32
        ));

        // The private dialect enables all ZeroFS extensions together.
        let resp = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            )
            .await;
        match resp.body {
            Message::Rversion(rv) => assert_eq!(rv.version.data, VERSION_9P2000L_ZEROFS),
            other => panic!("expected Rversion, got {other:?}"),
        }
        assert!(handler.zerofs_protocol_enabled());

        // Version matching is exact; failed renegotiation clears private framing.
        for unsupported in [
            b"9P2000.L.zerofs".as_slice(),
            b"9P2000.L.zerofs6".as_slice(),
            b"9P2000.L.zerofs7".as_slice(),
            b"9P2000.L.zerofs.extra".as_slice(),
            b"9P2000.L.Z2".as_slice(),
            b"prefix-9P2000.L.zerofs".as_slice(),
        ] {
            let resp = handler
                .handle_message(
                    0,
                    Message::Tversion(Tversion {
                        msize: DEFAULT_MSIZE,
                        version: P9String::new(unsupported.to_vec()),
                    }),
                )
                .await;
            match resp.body {
                Message::Rversion(rv) => assert_eq!(rv.version.data, b"unknown"),
                other => panic!("expected Rversion, got {other:?}"),
            }
            assert!(!handler.zerofs_protocol_enabled());
        }
    }

    #[tokio::test]
    async fn zerofs_dialect_enables_fallocate_with_ignore_fsync() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.ignore_fsync = true;
        let handler = NinePHandler::new(Arc::new(fs), Arc::new(FileLockManager::new()));
        let resp = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            )
            .await;
        match resp.body {
            Message::Rversion(rv) => {
                assert_eq!(rv.version.data, VERSION_9P2000L_ZEROFS);
                assert!(
                    handler.zerofs_protocol_enabled(),
                    "ignore_fsync is an explicit opt-out, not a protocol-level cap"
                );
            }
            other => panic!("expected Rversion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_statfs() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs.clone(), lock_manager);
        start_plain_session(&handler).await;

        let statfs_msg = Message::Tstatfs(Tstatfs { fid: 1 });
        let statfs_resp = handler.handle_message(2, statfs_msg).await;

        match &statfs_resp.body {
            Message::Rstatfs(rstatfs) => {
                assert_eq!(rstatfs.r#type, 0x5a45524f); // "ZERO" filesystem type
                assert_eq!(rstatfs.bsize, 4096);
                assert!(rstatfs.blocks > 0);
                assert!(rstatfs.bfree > 0);
                assert_eq!(rstatfs.bavail, rstatfs.bfree);
                assert!(rstatfs.files > 0);
                assert!(rstatfs.ffree > 0);
                assert_eq!(rstatfs.namelen, 255);

                // Verify totals match our constants
                const TOTAL_BYTES: u64 = u64::MAX;
                assert_eq!(rstatfs.blocks, TOTAL_BYTES.div_ceil(4096));
                // files = used_inodes + available_inodes, ffree = available_inodes
                // Since no files created yet (only root inode), files should equal ffree + used
                assert!(rstatfs.files > 0);
                assert!(rstatfs.ffree > 0);
            }
            _ => panic!("Expected Rstatfs, got {:?}", statfs_resp.body),
        }

        // Test statfs with invalid fid
        let invalid_statfs_msg = Message::Tstatfs(Tstatfs { fid: 999 });
        let invalid_resp = handler.handle_message(3, invalid_statfs_msg).await;

        match &invalid_resp.body {
            Message::Rlerror(rerror) => {
                assert_eq!(rerror.ecode, libc::EBADF as u32);
            }
            _ => panic!("Expected Rlerror, got {:?}", invalid_resp.body),
        }
    }

    #[tokio::test]
    async fn test_statfs_with_files() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs.clone(), lock_manager);
        start_plain_session(&handler).await;

        // Get initial statfs
        let statfs_msg = Message::Tstatfs(Tstatfs { fid: 1 });
        let initial_resp = handler.handle_message(2, statfs_msg.clone()).await;

        let (initial_free_blocks, _initial_free_inodes) = match &initial_resp.body {
            Message::Rstatfs(rstatfs) => (rstatfs.bfree, rstatfs.ffree),
            _ => panic!("Expected Rstatfs"),
        };

        expect_walk(&handler, 3, 1, 2, &[]).await;
        create_file(&handler, 4, 2, b"test.txt").await;

        // Write 10KB of data
        let data = vec![0u8; 10240];
        let write_msg = Message::Twrite(Twrite {
            fid: 2,
            offset: 0,
            count: data.len() as u32,
            data: DekuBytes::from(data),
        });
        handler.handle_message(5, write_msg).await;

        // Get statfs after write (using original fid which still points to root)
        let after_resp = handler.handle_message(6, statfs_msg).await;

        match &after_resp.body {
            Message::Rstatfs(rstatfs) => {
                // Available inodes = capacity - in-use; creating the file consumed one.
                let (_, used_inodes) = handler.filesystem.global_stats.get_totals();
                assert_eq!(rstatfs.ffree, crate::fs::TOTAL_INODES - used_inodes);

                // Should have fewer free blocks (10KB written = 3 blocks of 4KB)
                let expected_blocks_used = 10240_u64.div_ceil(4096); // Round up
                assert_eq!(rstatfs.bfree, initial_free_blocks - expected_blocks_used);
            }
            _ => panic!("Expected Rstatfs"),
        }
    }

    #[tokio::test]
    async fn test_readdir_random_pagination() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        for i in 0..10 {
            fs.create(
                &creds,
                0,
                format!("file{i:02}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);
        start_session(&handler, 8192, VERSION_9P2000L, b"/").await;
        open_fid(&handler, 200, 1, O_RDONLY as u32).await;

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 0,
            count: 8192,
        });
        let resp = handler.handle_message(201, readdir_msg).await;

        let entries_count = match &resp.body {
            Message::Rreaddir(rreaddir) => {
                let entries = rreaddir.to_entries().unwrap();
                assert!(!entries.is_empty());
                entries.len()
            }
            _ => panic!("Expected Rreaddir"),
        };

        // Should have at least . and .. plus the created files
        assert_eq!(
            entries_count, 12,
            "Expected 12 entries (. .. and 10 files), got {entries_count}"
        );

        // Test reading from random offset (skip first 5 entries)
        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 5,
            count: 8192,
        });
        let resp = handler.handle_message(202, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                // Should have fewer entries when starting from offset 5
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(entries.len(), entries_count - 5);
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_readdir_backwards_seek() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        // Create a few files
        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        for i in 0..5 {
            fs.create(
                &creds,
                0,
                format!("file{i}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);
        start_session(&handler, 8192, VERSION_9P2000L, b"/").await;
        open_fid(&handler, 20, 1, O_RDONLY as u32).await;

        // Read from offset 3
        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 3,
            count: 8192,
        });
        handler.handle_message(21, readdir_msg).await;

        // Now read from offset 1 (backwards seek)
        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 1,
            offset: 1,
            count: 8192,
        });
        let resp = handler.handle_message(22, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                // Should successfully read from offset 1
                let entries = rreaddir.to_entries().unwrap();
                assert!(!entries.is_empty());

                // Should have 6 entries from offset 1 (skipping only ".")
                assert_eq!(entries.len(), 6, "Expected 6 entries from offset 1");
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_readdir_pagination_duplicates_at_boundary() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };

        for i in 0..1002 {
            fs.create(
                &creds,
                0,
                format!("file_{:06}.txt", i).as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);
        start_plain_session(&handler).await;
        open_fid(&handler, 2, 1, O_RDONLY as u32).await;

        let mut all_names = Vec::new();
        let mut seen_offsets = std::collections::HashSet::new();
        let mut current_offset = 0u64;
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > 10 {
                panic!("Too many iterations, likely infinite loop");
            }

            println!(
                "Iteration {}: Reading from offset {}",
                iterations, current_offset
            );

            let readdir_msg = Message::Treaddir(Treaddir {
                fid: 1,
                offset: current_offset,
                count: 8192, // Typical buffer size
            });
            let resp = handler
                .handle_message(iterations as u16 + 2, readdir_msg)
                .await;

            match &resp.body {
                Message::Rreaddir(rreaddir) => {
                    let entries = rreaddir.to_entries().unwrap();
                    if entries.is_empty() {
                        println!("Got empty response, ending");
                        break;
                    }

                    // Parse entries
                    let mut batch_count = 0;

                    for entry in &entries {
                        let entry_offset = entry.offset;
                        let name = entry.name.as_str().unwrap_or("").to_string();

                        // Check for duplicate offsets
                        if !seen_offsets.insert(entry_offset) {
                            println!(
                                "WARNING: Duplicate offset {} for entry: {}",
                                entry_offset, name
                            );
                        }

                        if name != "." && name != ".." {
                            all_names.push(name.clone());
                            batch_count += 1;

                            // Debug: print entries near the boundary
                            if (998..=1004).contains(&entry_offset) {
                                println!("  Entry at offset {}: {}", entry_offset, name);
                            }
                        }

                        current_offset = entry_offset;
                    }

                    println!(
                        "Got {} entries in this batch, last offset: {}",
                        batch_count, current_offset
                    );

                    // If we got less than a reasonable amount, we might be at the end
                    if batch_count == 0 {
                        break;
                    }
                }
                _ => panic!("Expected Rreaddir"),
            };
        }

        // Check for duplicates
        let mut name_counts = std::collections::HashMap::new();
        for name in &all_names {
            *name_counts.entry(name.clone()).or_insert(0) += 1;
        }

        let mut duplicates = Vec::new();
        for (name, count) in &name_counts {
            if *count > 1 {
                duplicates.push((name.clone(), *count));
            }
        }

        if !duplicates.is_empty() {
            println!("Found {} duplicate entries:", duplicates.len());
            for (name, count) in &duplicates {
                println!("  {} appears {} times", name, count);
            }
        }

        // We should have exactly 1002 unique files
        assert_eq!(
            duplicates.len(),
            0,
            "Found duplicate entries: {:?}",
            duplicates
        );
        assert_eq!(
            all_names.len(),
            1002,
            "Expected 1002 entries, got {}",
            all_names.len()
        );
    }

    #[tokio::test]
    async fn test_readdir_empty_directory() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());

        let creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };
        let (_empty_dir_id, _) = fs
            .mkdir(&creds, 0, b"emptydir", &SetAttributes::default())
            .await
            .unwrap();

        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs, lock_manager);
        start_session(&handler, 8192, VERSION_9P2000L, b"/").await;
        expect_walk(&handler, 2, 1, 2, &[b"emptydir"]).await;
        open_fid(&handler, 3, 2, O_RDONLY as u32).await;

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 2,
            offset: 0,
            count: 8192,
        });
        let resp = handler.handle_message(4, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                // Should have . and .. entries
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(entries.len(), 2, "Expected 2 entries (. and ..)");
            }
            _ => panic!("Expected Rreaddir"),
        };

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 2,
            offset: 2,
            count: 8192,
        });
        let resp = handler.handle_message(5, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(
                    entries.len(),
                    0,
                    "Expected empty response for offset past end"
                );
            }
            _ => panic!("Expected Rreaddir"),
        };

        let readdir_msg = Message::Treaddir(Treaddir {
            fid: 2,
            offset: 2,
            count: 8192,
        });
        let resp = handler.handle_message(6, readdir_msg).await;

        match &resp.body {
            Message::Rreaddir(rreaddir) => {
                let entries = rreaddir.to_entries().unwrap();
                assert_eq!(
                    entries.len(),
                    0,
                    "Expected empty response for sequential read past end"
                );
            }
            _ => panic!("Expected Rreaddir"),
        };
    }

    #[tokio::test]
    async fn test_open_unlink_read_after_unlink_via_9p() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = NinePHandler::new(fs.clone(), lock_manager);
        start_plain_session(&handler).await;
        expect_walk(&handler, 2, 1, 2, &[]).await;
        create_file(&handler, 3, 2, b"a").await;

        let data = b"open-unlink via 9p";
        handler
            .handle_message(
                4,
                Message::Twrite(Twrite {
                    fid: 2,
                    offset: 0,
                    count: data.len() as u32,
                    data: DekuBytes::from(data.to_vec()),
                }),
            )
            .await;

        let file_id = fs.lookup(&test_creds(), 0, b"a").await.unwrap();
        assert_eq!(fs.open_handle_count(file_id), 1);

        let unlink_resp = handler
            .handle_message(
                5,
                Message::Tunlinkat(Tunlinkat {
                    dirfid: 1,
                    name: P9String::new(b"a".to_vec()),
                    flags: 0,
                }),
            )
            .await;
        assert!(matches!(unlink_resp.body, Message::Runlinkat(_)));
        assert!(matches!(
            fs.lookup(&test_creds(), 0, b"a").await,
            Err(FsError::NotFound)
        ));
        assert!(fs.inode_store.get(file_id).await.is_ok());

        let read_resp = handler
            .handle_message(
                6,
                Message::Tread(Tread {
                    fid: 2,
                    offset: 0,
                    count: data.len() as u32,
                }),
            )
            .await;
        match read_resp.body {
            Message::Rread(r) => assert_eq!(r.data.as_ref(), data),
            other => panic!("expected Rread with data, got {other:?}"),
        }

        let clunk_resp = handler
            .handle_message(7, Message::Tclunk(Tclunk { fid: 2 }))
            .await;
        assert!(matches!(clunk_resp.body, Message::Rclunk(_)));
        // Reclaim is invoked directly without a drainer.
        assert_eq!(fs.open_handle_count(file_id), 0);
        fs.reclaim_if_unreferenced(file_id).await;
        assert!(matches!(
            fs.inode_store.get(file_id).await,
            Err(FsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn disconnect_releases_open_handle_immediately() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = zerofs_handler(&fs).await;
        let creds = test_creds();
        let (file_id, _) = fs
            .create(&creds, 0, b"plain-disconnect", &SetAttributes::default())
            .await
            .unwrap();
        let inode = fs.inode_store.get(file_id).await.unwrap();
        let handle = {
            let _lock = fs.lock_manager.acquire(file_id).await;
            fs.new_open_handle(file_id)
        };
        handler.session_state().fids.insert(
            2,
            FidSlot {
                fid: Fid {
                    path: vec![Bytes::from_static(b"plain-disconnect")],
                    inode_id: file_id,
                    root: 0,
                    qid: inode_to_qid(&inode, file_id),
                    opened: true,
                    mode: Some(O_RDONLY as u32),
                    creds,
                },
                handle: Some(handle),
                lock_guard: None,
            },
        );
        assert_eq!(fs.open_handle_count(file_id), 1);

        // Reconnect restores linked identity but not old-session handle pins.
        handler.close_for_disconnect();
        {
            let state = handler.session_state();
            let slot = state
                .fids
                .get(&2)
                .expect("retired fid metadata remains available to received tasks");
            assert!(slot.handle.is_none());
            assert!(slot.lock_guard.is_none());
        }
        assert_eq!(fs.open_handle_count(file_id), 0);
    }

    #[tokio::test]
    async fn confined_rebind_uses_new_hardlink_subtree() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let (tenant_id, _) = fs
            .mkdir(&creds, 0, b"tenant", &SetAttributes::default())
            .await
            .unwrap();
        let (other_id, _) = fs
            .mkdir(&creds, 0, b"other", &SetAttributes::default())
            .await
            .unwrap();
        let (file_id, _) = fs
            .create(
                &creds,
                tenant_id,
                b"open-unlinked",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        // Relink restores the parent/name representation used by `Trebind`.
        let _handle = {
            let _lock = fs.lock_manager.acquire(file_id).await;
            fs.new_open_handle(file_id)
        };
        let auth = AuthContext::from(&creds);
        fs.remove(&auth, tenant_id, b"open-unlinked").await.unwrap();

        let stale_replay = zerofs_handler(&fs).await;
        let stale = rebind(
            &stale_replay,
            1,
            1,
            file_id,
            tenant_id,
            P9_REBIND_REPLAY | P9_REBIND_OPENED,
        )
        .await;
        assert!(matches!(
            stale,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::ESTALE as u32
        ));
        assert_eq!(fs.open_handle_count(file_id), 1);

        fs.link(&auth, file_id, tenant_id, b"restored")
            .await
            .unwrap();

        assert!(matches!(
            fs.inode_store.get(file_id).await.unwrap(),
            Inode::File(ref file)
                if file.nlink == 1
                    && file.parent == Some(tenant_id)
                    && file.name.as_deref() == Some(b"restored".as_slice())
        ));

        let reconnect = zerofs_handler(&fs).await;
        assert!(matches!(
            attach(&reconnect, 1, 1, b"test", b"/tenant", creds.uid)
                .await
                .body,
            Message::Rattach(_)
        ));
        assert!(matches!(
            rebind(&reconnect, 2, 2, file_id, tenant_id, 0).await,
            Message::Rrebind(_)
        ));

        let outsider = zerofs_handler(&fs).await;
        assert!(matches!(
            attach(&outsider, 3, 1, b"test", b"/other", creds.uid)
                .await
                .body,
            Message::Rattach(_)
        ));
        assert!(matches!(
            rebind(&outsider, 4, 2, file_id, other_id, 0).await,
            Message::Rlerror(_)
        ));
    }

    #[tokio::test]
    async fn automatic_replay_recovers_parentless_hardlink() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let auth = AuthContext::from(&creds);
        let (tenant_id, _) = fs
            .mkdir(&creds, 0, b"tenant", &SetAttributes::default())
            .await
            .unwrap();
        let (other_id, _) = fs
            .mkdir(&creds, 0, b"other", &SetAttributes::default())
            .await
            .unwrap();
        let (wrong_id, _) = fs
            .mkdir(&creds, 0, b"wrong", &SetAttributes::default())
            .await
            .unwrap();
        let (file_id, _) = fs
            .create(&creds, tenant_id, b"file", &SetAttributes::default())
            .await
            .unwrap();

        let old_pin = {
            let _lock = fs.lock_manager.acquire(file_id).await;
            fs.new_open_handle(file_id)
        };
        fs.link(&auth, file_id, other_id, b"alias").await.unwrap();
        assert!(matches!(
            fs.inode_store.get(file_id).await.unwrap(),
            Inode::File(ref file) if file.nlink == 2 && file.parent.is_none()
        ));

        // Manual binding cannot establish ancestry for a parentless hardlink.
        let live = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(&live, 1, 1, file_id, tenant_id, 0).await,
            Message::Rlerror(_)
        ));

        // Unrelated attach roots cannot establish ancestry.
        let wrong = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(&wrong, 2, 1, file_id, wrong_id, 0).await,
            Message::Rlerror(_)
        ));

        // Automatic replay restores the recorded linked fid without an open pin.
        drop(old_pin);
        assert_eq!(fs.open_handle_count(file_id), 0);
        let lock_only = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(&lock_only, 3, 1, file_id, tenant_id, P9_REBIND_REPLAY).await,
            Message::Rrebind(_)
        ));
        assert_eq!(fs.open_handle_count(file_id), 0);
        assert!(matches!(
            lock_only
                .handle_message(
                    4,
                    Message::Tlock(Tlock {
                        fid: 1,
                        lock_type: LockType::WriteLock,
                        flags: 0,
                        start: 0,
                        length: 0,
                        proc_id: 7,
                        client_id: P9String::new(b"replayed-lock".to_vec()),
                    }),
                )
                .await
                .body,
            Message::Rlock(Rlock {
                status: LockStatus::Success
            })
        ));
        assert_eq!(
            fs.open_handle_count(file_id),
            0,
            "a replayed lock-only fid remains unpinned"
        );

        // `Tlopen` consumes the provisional replay pin.
        let promoted = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(
                &promoted,
                4,
                1,
                file_id,
                tenant_id,
                P9_REBIND_REPLAY | P9_REBIND_OPENED,
            )
            .await,
            Message::Rrebind(_)
        ));
        assert_eq!(fs.open_handle_count(file_id), 1);
        assert!(matches!(
            promoted
                .handle_message(
                    5,
                    Message::Tlopen(Tlopen {
                        fid: 1,
                        flags: O_RDONLY as u32,
                    }),
                )
                .await
                .body,
            Message::Rlopen(_)
        ));
        assert_eq!(fs.open_handle_count(file_id), 1);
        assert!(matches!(
            promoted
                .handle_message(6, Message::Tclunk(Tclunk { fid: 1 }))
                .await
                .body,
            Message::Rclunk(_)
        ));
        assert_eq!(fs.open_handle_count(file_id), 0);
    }

    #[tokio::test]
    async fn moved_open_fid_rebinds_to_its_old_root_after_promotion() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let auth = AuthContext::from(&creds);
        let (old_root, _) = fs
            .mkdir(&creds, 0, b"old", &SetAttributes::default())
            .await
            .unwrap();
        let (new_root, _) = fs
            .mkdir(&creds, 0, b"new", &SetAttributes::default())
            .await
            .unwrap();
        let (wrong_root, _) = fs
            .mkdir(&creds, 0, b"wrong", &SetAttributes::default())
            .await
            .unwrap();
        let (file_id, _) = fs
            .create(&creds, old_root, b"file", &SetAttributes::default())
            .await
            .unwrap();
        let old_pin = {
            let _lock = fs.lock_manager.acquire(file_id).await;
            fs.new_open_handle(file_id)
        };
        fs.write(
            &auth,
            file_id,
            0,
            &Bytes::from_static(b"moved-open contents"),
        )
        .await
        .unwrap();

        fs.rename(&auth, old_root, b"file", new_root, b"moved")
            .await
            .unwrap();

        // Manual binding follows current ancestry after rename.
        let live = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(&live, 1, 1, file_id, old_root, 0).await,
            Message::Rlerror(_)
        ));
        let wrong = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(&wrong, 2, 1, file_id, wrong_root, 0).await,
            Message::Rlerror(_)
        ));

        // Promotion replay restores the recorded fid by identity.
        drop(old_pin);
        let promoted = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(
                &promoted,
                3,
                1,
                file_id,
                old_root,
                P9_REBIND_REPLAY | P9_REBIND_OPENED,
            )
            .await,
            Message::Rrebind(_)
        ));
        assert_eq!(promoted.get_fid(1).unwrap().root, old_root);
        assert_eq!(fs.open_handle_count(file_id), 1);
        assert!(matches!(
            promoted
                .handle_message(
                    4,
                    Message::Tlopen(Tlopen {
                        fid: 1,
                        flags: O_RDONLY as u32,
                    }),
                )
                .await
                .body,
            Message::Rlopen(_)
        ));
        assert_eq!(
            fs.open_handle_count(file_id),
            1,
            "Tlopen must consume the moved fid's provisional pin"
        );
        let read = promoted
            .handle_message(
                5,
                Message::Tread(Tread {
                    fid: 1,
                    offset: 0,
                    count: 64,
                }),
            )
            .await;
        assert!(matches!(
            read.body,
            Message::Rread(Rread { ref data, .. })
                if data.as_ref() == b"moved-open contents"
        ));
    }

    #[tokio::test]
    async fn root_self_orphan_rebind_is_always_stale() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let auth = AuthContext::from(&creds);
        let (root_id, _) = fs
            .mkdir(&creds, 0, b"orphan-root", &SetAttributes::default())
            .await
            .unwrap();
        let old_pin = {
            let _lock = fs.lock_manager.acquire(root_id).await;
            fs.new_open_handle(root_id)
        };
        fs.remove(&auth, 0, b"orphan-root").await.unwrap();
        assert_eq!(fs.open_handle_count(root_id), 1);

        let handler = zerofs_handler(&fs).await;
        for (tag, flags) in [
            (1, P9_REBIND_REPLAY),
            (2, P9_REBIND_REPLAY | P9_REBIND_OPENED),
        ] {
            let stale = rebind(&handler, tag, tag.into(), root_id, root_id, flags).await;
            assert!(matches!(
                stale,
                Message::Rlerror(Rlerror { ecode }) if ecode == libc::ESTALE as u32
            ));
        }
        assert_eq!(
            fs.open_handle_count(root_id),
            1,
            "rejected replay must not install a provisional pin"
        );

        let denied = rebind(&handler, 3, 3, root_id, root_id, 0).await;
        assert!(matches!(
            denied,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EACCES as u32
        ));

        drop(old_pin);
        assert_eq!(fs.open_handle_count(root_id), 0);
    }

    #[tokio::test]
    async fn linked_provisional_rebind_pin_releases_on_disconnect() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let (file_id, _) = fs
            .create(&creds, 0, b"file", &SetAttributes::default())
            .await
            .unwrap();
        let handler = zerofs_handler(&fs).await;

        let invalid = rebind(&handler, 1, 1, file_id, 0, P9_REBIND_OPENED).await;
        assert!(matches!(
            invalid,
            Message::Rlerror(Rlerror { ecode }) if ecode == libc::EINVAL as u32
        ));
        assert_eq!(fs.open_handle_count(file_id), 0);

        assert!(matches!(
            rebind(
                &handler,
                2,
                1,
                file_id,
                0,
                P9_REBIND_REPLAY | P9_REBIND_OPENED,
            )
            .await,
            Message::Rrebind(_)
        ));
        assert_eq!(fs.open_handle_count(file_id), 1);

        assert!(matches!(
            handler
                .handle_message(3, Message::Tclunk(Tclunk { fid: 1 }))
                .await
                .body,
            Message::Rclunk(_)
        ));
        assert_eq!(
            fs.open_handle_count(file_id),
            0,
            "clunk immediately releases an abandoned provisional pin"
        );

        let retry = zerofs_handler(&fs).await;
        assert!(matches!(
            rebind(
                &retry,
                4,
                1,
                file_id,
                0,
                P9_REBIND_REPLAY | P9_REBIND_OPENED,
            )
            .await,
            Message::Rrebind(_)
        ));
        assert_eq!(fs.open_handle_count(file_id), 1);

        retry.close_for_disconnect();
        {
            let state = retry.session_state();
            let retired = state
                .fids
                .get(&1)
                .expect("provisional replay fid metadata remains available");
            assert!(retired.handle.is_none());
            assert!(retired.lock_guard.is_none());
        }
        assert_eq!(
            fs.open_handle_count(file_id),
            0,
            "disconnect must release an incomplete Trebind/Tlopen pin immediately"
        );
    }

    #[tokio::test]
    async fn rebind_tracks_renamed_attach_root() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let auth = AuthContext::from(&creds);
        let (root_a, _) = fs
            .mkdir(&creds, 0, b"root-a", &SetAttributes::default())
            .await
            .unwrap();
        let (root_b, _) = fs
            .mkdir(&creds, 0, b"root-b", &SetAttributes::default())
            .await
            .unwrap();
        let (child_a, _) = fs
            .create(&creds, root_a, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (child_b, _) = fs
            .create(&creds, root_b, b"b", &SetAttributes::default())
            .await
            .unwrap();

        // Root-self replay restores the renamed attach root by identity.
        fs.rename(&auth, 0, b"root-a", 0, b"renamed-a")
            .await
            .unwrap();
        assert!(matches!(
            fs.lookup(&creds, 0, b"root-a").await,
            Err(FsError::NotFound)
        ));

        let handler = zerofs_handler(&fs).await;
        for (tag, fid, inode_id, root_inode) in [
            (1, 1, root_a, root_a),
            (2, 2, root_b, root_b),
            (3, 3, child_a, root_a),
            (4, 4, child_b, root_b),
        ] {
            assert!(matches!(
                rebind(&handler, tag, fid, inode_id, root_inode, P9_REBIND_REPLAY).await,
                Message::Rrebind(_)
            ));
        }
        assert_eq!(handler.get_fid(3).unwrap().root, root_a);
        assert_eq!(handler.get_fid(4).unwrap().root, root_b);

        assert!(matches!(
            rebind(&handler, 5, 5, child_a, root_b, 0).await,
            Message::Rlerror(_)
        ));
    }

    /// Last clunk reclaims an open-unlinked inode through the async drainer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_reclaim_drainer_reclaims_after_last_clunk() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        fs.start_reclaim_drainer();
        let handler = Arc::new(NinePHandler::new(
            fs.clone(),
            Arc::new(FileLockManager::new()),
        ));
        start_plain_session(&handler).await;
        expect_walk(&handler, 2, 1, 2, &[]).await;
        create_file(&handler, 3, 2, b"a").await;
        let file_id = fs.lookup(&test_creds(), 0, b"a").await.unwrap();
        // unlink (defers: fid 2 holds it open)
        handler
            .handle_message(
                4,
                Message::Tunlinkat(Tunlinkat {
                    dirfid: 1,
                    name: P9String::new(b"a".to_vec()),
                    flags: 0,
                }),
            )
            .await;
        assert!(fs.inode_store.get(file_id).await.is_ok());

        // Last clunk: the guard drop enqueues the inode; the drainer reclaims it.
        handler
            .handle_message(5, Message::Tclunk(Tclunk { fid: 2 }))
            .await;

        let mut gone = false;
        for _ in 0..250 {
            if fs.inode_store.get(file_id).await.is_err() {
                gone = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            gone,
            "reclaim drainer did not reclaim the open-unlinked inode after the last clunk"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_lopen_same_fid_counts_once() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager));
        start_plain_session(&handler).await;
        // Create "a" (fid 2 via lcreate), then clunk so it exists with 0 handles.
        expect_walk(&handler, 2, 1, 2, &[]).await;
        create_file(&handler, 3, 2, b"a").await;
        handler
            .handle_message(4, Message::Tclunk(Tclunk { fid: 2 }))
            .await;
        let file_id = fs.lookup(&test_creds(), 0, b"a").await.unwrap();
        assert_eq!(fs.open_handle_count(file_id), 0);

        for round in 0..200u32 {
            // Walk a fresh, unopened fid 3 to "a".
            expect_walk(&handler, 10, 1, 3, &[b"a"]).await;

            let h1 = handler.clone();
            let h2 = handler.clone();
            let t1 = tokio::spawn(async move {
                h1.handle_message(11, Message::Tlopen(Tlopen { fid: 3, flags: 0 }))
                    .await
            });
            let t2 = tokio::spawn(async move {
                h2.handle_message(12, Message::Tlopen(Tlopen { fid: 3, flags: 0 }))
                    .await
            });
            let r1 = t1.await.unwrap();
            let r2 = t2.await.unwrap();

            let successes = matches!(r1.body, Message::Rlopen(_)) as u8
                + matches!(r2.body, Message::Rlopen(_)) as u8;
            assert_eq!(
                successes, 1,
                "round {round}: exactly one Tlopen should win, got {:?} / {:?}",
                r1.body, r2.body
            );
            assert_eq!(
                fs.open_handle_count(file_id),
                1,
                "round {round}: handle was double-counted"
            );

            handler
                .handle_message(13, Message::Tclunk(Tclunk { fid: 3 }))
                .await;
            assert_eq!(
                fs.open_handle_count(file_id),
                0,
                "round {round}: handle not released"
            );
        }
    }

    // Teardown (close_all) racing a Tclunk of the same fid must release its
    // handle exactly once. A double release would drop the count below the true
    // live count and reclaim an inode another session still holds open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_teardown_vs_clunk_releases_handle_once() {
        for round in 0..300u32 {
            let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
            let lock_manager = Arc::new(FileLockManager::new());
            let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager));
            start_plain_session(&handler).await;
            // Create + open "a" on fid 2 (this session's handle) -> count 1.
            expect_walk(&handler, 2, 1, 2, &[]).await;
            create_file(&handler, 3, 2, b"a").await;
            let file_id = fs.lookup(&test_creds(), 0, b"a").await.unwrap();

            // Simulate a SECOND session also holding "a" open (not torn down here).
            fs.open_handle_inc(file_id);

            // `rm a` while open -> deferred (orphan); count still 2.
            handler
                .handle_message(
                    4,
                    Message::Tunlinkat(Tunlinkat {
                        dirfid: 1,
                        name: P9String::new(b"a".to_vec()),
                        flags: 0,
                    }),
                )
                .await;
            assert_eq!(fs.open_handle_count(file_id), 2);

            // RACE: this session disconnects (close_all) while a Tclunk of fid 2 is
            // in flight on the same session.
            let h1 = handler.clone();
            let h2 = handler.clone();
            let t1 = tokio::spawn(async move { h1.close_all_open_handles() });
            let t2 = tokio::spawn(async move {
                h2.handle_message(5, Message::Tclunk(Tclunk { fid: 2 }))
                    .await
            });
            t1.await.unwrap();
            t2.await.unwrap();

            // This session's single handle was released exactly once (2 -> 1); the
            // other session's handle must keep "a" alive (NOT reclaimed).
            assert_eq!(
                fs.open_handle_count(file_id),
                1,
                "round {round}: fid double-released, count dropped below live"
            );
            assert!(
                fs.inode_store.get(file_id).await.is_ok(),
                "round {round}: inode reclaimed while another session still holds it"
            );

            // The other session finally closes -> reclaim.
            fs.handle_closed(file_id).await;
            assert!(matches!(
                fs.inode_store.get(file_id).await,
                Err(FsError::NotFound)
            ));
        }
    }

    // Walking a fid in place (newfid == fid, non-empty wnames) overwrites its
    // slot; if the fid was an opened directory, the overwrite must still release
    // its handle count. Note a pure clone (empty wnames) is a no-op, so the walk
    // target has to be a real child.
    #[tokio::test]
    async fn test_walk_in_place_over_open_dir_leaks_handle() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager));
        start_plain_session(&handler).await;
        // Create a child "x" so the in-place walk has a target.
        expect_walk(&handler, 2, 1, 2, &[]).await;
        create_file(&handler, 3, 2, b"x").await;
        handler
            .handle_message(4, Message::Tclunk(Tclunk { fid: 2 }))
            .await;

        // Open a *directory* fid on the root (fid 10).
        expect_walk(&handler, 5, 1, 10, &[]).await;
        open_fid(&handler, 6, 10, 0).await;
        assert_eq!(
            fs.open_handle_count(0),
            1,
            "root dir should be open on fid 10"
        );

        // Walk fid 10 in place to child "x": overwrites the opened dir fid.
        let resp = handler
            .handle_message(
                7,
                Message::Twalk(Twalk {
                    fid: 10,
                    newfid: 10,
                    nwname: 1,
                    wnames: vec![P9String::new(b"x".to_vec())],
                }),
            )
            .await;
        assert!(
            matches!(resp.body, Message::Rwalk(_)),
            "in-place walk must succeed to exercise the overwrite, got {:?}",
            resp.body
        );

        // Clunk the (now opened:false) fid 10.
        handler
            .handle_message(8, Message::Tclunk(Tclunk { fid: 10 }))
            .await;

        assert_eq!(
            fs.open_handle_count(0),
            0,
            "N2: walk-in-place over an open dir fid stranded the open-handle count"
        );
    }

    // A fid holding a POSIX lock, then walked in place (overwriting its slot),
    // must have its lock released. ZeroFS doesn't enforce 9P's "don't walk an
    // open fid" rule, so this is reachable.
    #[tokio::test]
    async fn test_walk_in_place_over_locked_dir_releases_lock() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager.clone()));
        start_plain_session(&handler).await;
        // child "x" for the in-place walk target
        expect_walk(&handler, 2, 1, 2, &[]).await;
        create_file(&handler, 3, 2, b"x").await;
        handler
            .handle_message(4, Message::Tclunk(Tclunk { fid: 2 }))
            .await;
        // open root dir on fid 10, then write-lock it
        expect_walk(&handler, 5, 1, 10, &[]).await;
        open_fid(&handler, 6, 10, 0).await;
        handler
            .handle_message(
                7,
                Message::Tlock(Tlock {
                    fid: 10,
                    lock_type: LockType::WriteLock,
                    flags: 0,
                    start: 0,
                    length: 0,
                    proc_id: 1,
                    client_id: P9String::new(b"c".to_vec()),
                }),
            )
            .await;
        assert!(
            lock_manager.session_has_locks(handler.handler_id),
            "write lock should be held on fid 10"
        );

        // Walk fid 10 in place to "x": overwrites the locked dir fid's slot.
        let resp = handler
            .handle_message(
                8,
                Message::Twalk(Twalk {
                    fid: 10,
                    newfid: 10,
                    nwname: 1,
                    wnames: vec![P9String::new(b"x".to_vec())],
                }),
            )
            .await;
        assert!(matches!(resp.body, Message::Rwalk(_)));

        assert!(
            !lock_manager.session_has_locks(handler.handler_id),
            "walk-in-place over a locked fid leaked the byte-range lock"
        );
    }

    // Tversion resets the session, so it must release the session's byte-range
    // locks along with its fids.
    #[tokio::test]
    async fn test_tversion_releases_byte_range_locks() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager.clone()));
        start_plain_session(&handler).await;
        expect_walk(&handler, 2, 1, 10, &[]).await;
        open_fid(&handler, 3, 10, 0).await;
        handler
            .handle_message(
                4,
                Message::Tlock(Tlock {
                    fid: 10,
                    lock_type: LockType::WriteLock,
                    flags: 0,
                    start: 0,
                    length: 0,
                    proc_id: 1,
                    client_id: P9String::new(b"c".to_vec()),
                }),
            )
            .await;
        assert!(lock_manager.session_has_locks(handler.handler_id));

        // Tversion resets the session.
        handler
            .handle_message(
                5,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            )
            .await;

        assert!(
            !lock_manager.session_has_locks(handler.handler_id),
            "Tversion left the session's byte-range locks dangling"
        );
    }

    // A failpoint pauses lock() after it registers the lock but before the guard
    // is installed, then a concurrent Tversion clears the fid table. Unless the
    // register and guard install are atomic (the slot held across both), Tversion
    // drops the guardless slot and strands the lock — which, with no in-session
    // backstop, leaks for the whole connection. Run with `--features failpoints`.
    #[cfg(feature = "failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_lock_register_guard_atomic_vs_tversion() {
        let _scenario = fail::FailScenario::setup();
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager.clone()));
        handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            )
            .await;
        handler
            .handle_message(
                1,
                Message::Tattach(Tattach {
                    fid: 1,
                    afid: u32::MAX,
                    uname: P9String::new(b"t".to_vec()),
                    aname: P9String::new(Vec::new()),
                    n_uname: 1000,
                }),
            )
            .await;
        handler
            .handle_message(
                2,
                Message::Twalk(Twalk {
                    fid: 1,
                    newfid: 10,
                    nwname: 0,
                    wnames: vec![],
                }),
            )
            .await;
        handler
            .handle_message(3, Message::Tlopen(Tlopen { fid: 10, flags: 0 }))
            .await;

        // Pause lock() after it registers the lock, before installing the guard.
        fail::cfg(fp::LOCK_AFTER_REGISTER_BEFORE_GUARD, "pause").unwrap();

        let h1 = handler.clone();
        let lock_task = tokio::spawn(async move {
            h1.handle_message(
                4,
                Message::Tlock(Tlock {
                    fid: 10,
                    lock_type: LockType::WriteLock,
                    flags: 0,
                    start: 0,
                    length: 0,
                    proc_id: 1,
                    client_id: P9String::new(b"c".to_vec()),
                }),
            )
            .await
        });

        // Wait for lock registration under the session lock.
        let mut registered = false;
        for _ in 0..400 {
            if lock_manager.session_has_locks(handler.handler_id()) {
                registered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(registered, "lock task did not register/pause in time");

        // `Tversion` blocks on the session lock.
        let h2 = handler.clone();
        let version_task = tokio::spawn(async move {
            h2.handle_message(
                5,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Resume lock installation before session reset.
        fail::cfg(fp::LOCK_AFTER_REGISTER_BEFORE_GUARD, "off").unwrap();

        let _ = lock_task.await;
        let _ = version_task.await;

        assert!(
            !lock_manager.session_has_locks(handler.handler_id()),
            "Tlock racing Tversion stranded a phantom byte-range lock (register+guard not atomic)"
        );
    }

    // Teardown (close_all) racing an in-flight open that inserts a fresh fid
    // (lopenat). The late fid's handle guard must still be released — if not at
    // the clear, then when the session table drops. 300 rounds on a shared fs;
    // any stranded count survives to the final assert.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_close_all_vs_inflight_open_no_leak() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());

        // Create "a" once (persists on the shared fs across rounds).
        {
            let h = Arc::new(NinePHandler::new(fs.clone(), lock_manager.clone()));
            h.handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            )
            .await;
            h.handle_message(
                1,
                Message::Tattach(Tattach {
                    fid: 1,
                    afid: u32::MAX,
                    uname: P9String::new(b"t".to_vec()),
                    aname: P9String::new(Vec::new()),
                    n_uname: 1000,
                }),
            )
            .await;
            h.handle_message(
                2,
                Message::Twalk(Twalk {
                    fid: 1,
                    newfid: 2,
                    nwname: 0,
                    wnames: vec![],
                }),
            )
            .await;
            h.handle_message(
                3,
                Message::Tlcreate(Tlcreate {
                    fid: 2,
                    name: P9String::new(b"a".to_vec()),
                    flags: 0x8002,
                    mode: 0o644,
                    gid: 1000,
                }),
            )
            .await;
            h.handle_message(4, Message::Tclunk(Tclunk { fid: 2 }))
                .await;
        }

        for round in 0..300u16 {
            let handler = Arc::new(NinePHandler::new(fs.clone(), lock_manager.clone()));
            handler
                .handle_message(
                    0,
                    Message::Tversion(Tversion {
                        msize: DEFAULT_MSIZE,
                        version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                    }),
                )
                .await;
            handler
                .handle_message(
                    1,
                    Message::Tattach(Tattach {
                        fid: 1,
                        afid: u32::MAX,
                        uname: P9String::new(b"t".to_vec()),
                        aname: P9String::new(Vec::new()),
                        n_uname: 1000,
                    }),
                )
                .await;
            // fid 5 -> "a" (unopened): the source for the in-flight open.
            handler
                .handle_message(
                    2,
                    Message::Twalk(Twalk {
                        fid: 1,
                        newfid: 5,
                        nwname: 1,
                        wnames: vec![P9String::new(b"a".to_vec())],
                    }),
                )
                .await;

            let h1 = handler.clone();
            let h2 = handler.clone();
            // RACE: teardown vs an in-flight lopenat that inserts fid 6 (opened).
            let t1 = tokio::spawn(async move { h1.close_all_open_handles() });
            let t2 = tokio::spawn(async move {
                h2.handle_message(
                    round,
                    Message::Tlopenat(Tlopenat {
                        fid: 5,
                        newfid: 6,
                        flags: 0,
                    }),
                )
                .await
            });
            let _ = t1.await;
            let _ = t2.await;
            // Dropping the last handler ref drops the session table; a fid the
            // open inserted after the clear releases its guard here.
            drop(handler);
        }

        let leaked: Vec<(u64, u64)> = fs
            .open_handles
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect();
        assert!(
            leaked.is_empty(),
            "close_all vs in-flight open stranded handle counts: {leaked:?}"
        );
    }

    // Many concurrent sessions, each with several worker tasks, thrash the
    // fid/handle lifecycle over a tiny shared namespace. Invariant: once every
    // session has torn down, no open-handle count may remain — a leftover entry
    // is a leak, whatever path caused it. The seed is in the failure message.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_handle_lifecycle_no_leak() {
        fn xs(s: &mut u64) -> u64 {
            let mut x = *s;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *s = x;
            x
        }
        const SESSIONS: u64 = 4;
        const TASKS: u64 = 3;
        const ITERS: u64 = 120;
        let names: [&[u8]; 3] = [b"a", b"b", b"c"];

        for seed in 1..=8u64 {
            let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
            let lock_manager = Arc::new(FileLockManager::new());

            let mut sessions = Vec::new();
            for s in 0..SESSIONS {
                let fs = fs.clone();
                let lm = lock_manager.clone();
                sessions.push(tokio::spawn(async move {
                    let handler = Arc::new(NinePHandler::new(fs, lm));
                    handler
                        .handle_message(
                            0,
                            Message::Tversion(Tversion {
                                msize: DEFAULT_MSIZE,
                                version: P9String::new(VERSION_9P2000L.to_vec()),
                            }),
                        )
                        .await;
                    handler
                        .handle_message(
                            1,
                            Message::Tattach(Tattach {
                                fid: 1,
                                afid: u32::MAX,
                                uname: P9String::new(b"t".to_vec()),
                                aname: P9String::new(Vec::new()),
                                n_uname: 1000,
                            }),
                        )
                        .await;

                    let mut workers = Vec::new();
                    for t in 0..TASKS {
                        let handler = handler.clone();
                        let mut rng = (seed ^ (s << 8) ^ (t << 4)) | 1;
                        let base_fid = 10 + (t as u32) * 10;
                        workers.push(tokio::spawn(async move {
                            for i in 0..ITERS {
                                let r = xs(&mut rng);
                                let fid = base_fid + ((r >> 3) as u32) % 4;
                                let name = names[(r >> 20) as usize % names.len()].to_vec();
                                let tag = (2000 + i) as u16;
                                match r % 8 {
                                    0 => {
                                        // create + open on `fid`
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Twalk(Twalk {
                                                    fid: 1,
                                                    newfid: fid,
                                                    nwname: 0,
                                                    wnames: vec![],
                                                }),
                                            )
                                            .await;
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Tlcreate(Tlcreate {
                                                    fid,
                                                    name: P9String::new(name),
                                                    flags: 0x8002,
                                                    mode: 0o644,
                                                    gid: 1000,
                                                }),
                                            )
                                            .await;
                                    }
                                    1 => {
                                        // walk to name, then open
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Twalk(Twalk {
                                                    fid: 1,
                                                    newfid: fid,
                                                    nwname: 1,
                                                    wnames: vec![P9String::new(name)],
                                                }),
                                            )
                                            .await;
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Tlopen(Tlopen { fid, flags: 0 }),
                                            )
                                            .await;
                                    }
                                    2 => {
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Tread(Tread {
                                                    fid,
                                                    offset: 0,
                                                    count: 32,
                                                }),
                                            )
                                            .await;
                                    }
                                    3 => {
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Twrite(Twrite {
                                                    fid,
                                                    offset: 0,
                                                    count: 2,
                                                    data: DekuBytes::from(vec![1u8, 2u8]),
                                                }),
                                            )
                                            .await;
                                    }
                                    4 => {
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Tunlinkat(Tunlinkat {
                                                    dirfid: 1,
                                                    name: P9String::new(name),
                                                    flags: 0,
                                                }),
                                            )
                                            .await;
                                    }
                                    5 => {
                                        // open a directory fid (clone root, then open) so
                                        // op 6's in-place walk has an opened dir to overwrite
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Twalk(Twalk {
                                                    fid: 1,
                                                    newfid: fid,
                                                    nwname: 0,
                                                    wnames: vec![],
                                                }),
                                            )
                                            .await;
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Tlopen(Tlopen { fid, flags: 0 }),
                                            )
                                            .await;
                                    }
                                    6 => {
                                        // walk `fid` in place to a child: if it's an opened
                                        // dir this overwrites the slot, which must release
                                        // the handle (empty wnames would be a no-op)
                                        handler
                                            .handle_message(
                                                tag,
                                                Message::Twalk(Twalk {
                                                    fid,
                                                    newfid: fid,
                                                    nwname: 1,
                                                    wnames: vec![P9String::new(name)],
                                                }),
                                            )
                                            .await;
                                    }
                                    _ => {
                                        handler
                                            .handle_message(tag, Message::Tclunk(Tclunk { fid }))
                                            .await;
                                    }
                                }
                            }
                        }));
                    }
                    for w in workers {
                        let _ = w.await;
                    }
                    handler.close_all_open_handles();
                }));
            }
            for sh in sessions {
                sh.await.unwrap();
            }

            let leaked: Vec<(u64, u64)> = fs
                .open_handles
                .iter()
                .map(|e| (*e.key(), *e.value()))
                .collect();
            assert!(
                leaked.is_empty(),
                "seed {seed}: leaked open-handle counts after all sessions closed: {leaked:?}"
            );
        }
    }

    fn test_creds() -> Credentials {
        Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        }
    }
}
