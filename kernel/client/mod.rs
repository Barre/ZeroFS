//! ZeroFS protocol client.
//!
//! One TCP or AF_UNIX connection carries every request for a mount, tagged so
//! they can complete out of order. Callers transmit directly and wait on their
//! own tags; one connection task blocks on the stream, routes complete replies,
//! and wakes the matching callers.
//!
//! # Reconnection
//!
//! Fids are connection-scoped. After a transport failure the connection task
//! reconnects with backoff and restores recorded state before releasing blocked
//! requests. A failure turns everything in flight into a resend; only a replay
//! that cannot rebuild observed state, or a protocol desync, ends the logical
//! session.
//!
//! Reorder-sensitive mutations keep one operation ID across resends. Resends
//! stop at the protocol retry horizon; expiry returns a disconnect error with
//! an ambiguous outcome. The negotiated message size is fixed for the logical
//! session, so a reconnect candidate must negotiate the same value and an
//! ambiguous mutation is resendable byte for byte.
//!
//! An opened fid that no longer exists becomes an `ESTALE` tombstone; its
//! operations fail without affecting other fids. Byte-range locks are held by
//! the server against the connection, so every granted lock is recorded and
//! has to be reacquired alongside its fid.
//!
//! # Layout
//!
//! [`session`] holds connection state, [`slots`] the tag table, [`retry`] request
//! transmission, [`receive`] socket reads and reply routing, and [`reply`] each
//! caller's tag wait. [`flush`] cancels interrupted requests; [`signals`] owns
//! the task signal masks used while sending and cancelling.
//!
//! [`registry`] records fid and lock replay state and interns credentials,
//! [`durability`] tracks the obligations an `fsync` has to cover, [`retry`]
//! bounds one operation's attempts, and [`reconnect`] probes targets and
//! replays recorded state onto a replacement connection.
//!
//! [`ops`] holds the request methods, [`endpoint`] parses the mount source, and
//! [`errors`] maps status codes.

mod durability;
mod endpoint;
mod errors;
mod flush;
mod ops;
mod receive;
mod reconnect;
mod registry;
mod reply;
mod retry;
mod session;
mod signals;
mod slots;
mod tag_space;

pub(crate) use self::endpoint::Endpoint;
pub(crate) use self::ops::{
    DeviceNumber, LockOwner, LockRange, OwnedPayload, ReplyDestination, SetAttributes, TimeChange,
    WireTime,
};
pub(crate) use self::registry::RebindCredentials;

use core::{ffi::c_void, mem, pin::Pin, ptr::NonNull};

use kernel::{
    alloc::KBox, bindings, ffi, prelude::*, sync::aref::ARef, task::Task, time::msecs_to_jiffies,
};

use crate::protocol::{self, HEADER_SIZE, Qid};

use self::errors::not_connected_errno;
use self::reconnect::bootstrap_connection;
use self::session::Session;
use self::tag_space::{FIRST_NORMAL_TAG, NORMAL_TAG_COUNT};

/// Smallest useful 9P message size accepted by this client.
pub(crate) const MIN_MSIZE: u32 = 4096;

const MAX_INFLIGHT_REQUESTS: usize = 1024;
const _: () = assert!(MAX_INFLIGHT_REQUESTS < NORMAL_TAG_COUNT);

/// Fid installed for the root during synchronous bootstrap.
pub(crate) const ROOT_FID: u32 = 1;

/// Stable inode the mount's attach root resolves to.
///
/// Every `Trebind` this client sends passes it as `root_inode`, so a mount has
/// exactly one attach root and a fid record need not carry its own.
const ROOT_INODE_ID: u64 = 0;

/// Atomic words needed for an exact credential comparison in the dentry cache.
pub(crate) const REBIND_CREDENTIAL_IDENTITY_WORDS: usize =
    (mem::size_of::<u32>() + protocol::REBIND_CREDENTIAL_MAX_SIZE).div_ceil(8);

/// Byte offset of an `Rread` payload inside its frame: header plus count.
const READ_PAYLOAD_OFFSET: usize = HEADER_SIZE + mem::size_of::<u32>();

/// Persistent receive accumulator for small responses.
///
/// Larger frames take the response allocation attached to their tag after the
/// buffered prefix, so a large negotiated msize does not permanently consume
/// that much accumulator memory per mount.
const RECEIVE_BATCH_BYTES: usize = 64 * 1024;

/// Reusable allocation size for ordinary metadata replies.
///
/// This covers every fixed metadata response, including a full ZeroFS stat.
/// Variable walk, readlink, readdir and data replies use request-sized
/// allocations when their maximum frame is larger.
const SMALL_REPLY_BYTES: usize = 256;

/// Per-mount buffers kept hot for concurrently owned metadata replies.
///
/// The pool is deliberately independent of the pending-tag table. A fixed hot
/// set stays resident; concurrent requests beyond it allocate on demand and
/// return compatible buffers as the pool has room.
const SMALL_REPLY_BUFFERS: usize = 64;

/// Largest request encoded without touching the allocator.
///
/// Every fixed-layout request plus one maximum-length name fits here, which is
/// the whole hot metadata path. Rename carries two names, symlink carries a
/// target and a multi-element walk carries several names, so those keep the
/// allocating path.
const STACK_REQUEST_BYTES: usize = 320;

const _: () = assert!(
    STACK_REQUEST_BYTES
        >= HEADER_SIZE + protocol::OP_ENVELOPE_SIZE + 6 * 4 + 2 + protocol::MAX_NAME_LEN
);

/// Ordinary slots made resident when the mount is created.
///
/// This is only the allocation-free hot set, not a concurrency limit. If all
/// resident slots are occupied, the sharded table grows geometrically until it
/// covers the complete wire-tag namespace.
const INITIAL_PENDING_TAGS: usize = 1024;
const _: () = assert!(
    INITIAL_PENDING_TAGS >= 2
        && INITIAL_PENDING_TAGS <= NORMAL_TAG_COUNT
        && FIRST_NORMAL_TAG + NORMAL_TAG_COUNT == protocol::NOTAG as usize
);

/// Bound the fixed waiter set while the tag table grows.
///
/// High tags share queues by modulo; publication wakes every colliding waiter,
/// avoiding tens of thousands of individually allocated waitqueues.
const MAX_REPLY_WAITERS: usize = 4096;

/// Mutex shards backing the pending-tag table.
///
/// This is enough to keep ordinary parallel builds from contending on the
/// receiver's tag while remaining a fixed per-mount allocation.
const SLOT_SHARDS: usize = 64;

/// Past it the server's dedup entry for an operation ID may have been evicted,
/// so a resend would be indistinguishable from a new operation.
///
/// Taken from the shared crate rather than restated here: it is one half of a
/// pair with the server's result retention, not a client tuning knob.
const MUTATION_RETRY_HORIZON_MS: u64 = protocol::retry::MUTATION_RETRY_HORIZON.as_millis() as u64;

/// Sentinel stored while no receiver-owned liveness probe has a normal tag.
const NO_PROBE_TAG: u32 = u32::MAX;

/// Most server addresses one mount will rotate through.
///
/// ZeroFS HA is exactly two participants: one leader and one standby.
///
/// Probing is sequential here, so a probe round costs at most this many probe
/// deadlines. Keeping the bound small is what keeps a round well inside the
/// reconnect grace and far from the hung-task threshold, since a VFS caller may
/// be holding `i_rwsem` throughout.
const MAX_TARGETS: usize = 2;

/// Deadline for one target's dial and for each of its handshake receives.
///
/// A standby that completes the TCP handshake and then goes silent must not
/// hold up the rotation. The userspace client bounds the whole handshake with
/// one cancellable timeout; blocking kernel sockets only have per-syscall
/// timeouts, so the worst case here is a dial plus one stalled receive.
const PROBE_TIMEOUT_MS: u32 = 3_000;

const RECONNECT_BACKOFF_MIN_MS: u64 = 50;
const RECONNECT_BACKOFF_MAX_MS: u64 = 500;

/// Staging buffer for one probe, negotiation or replay round trip.
///
/// The largest frame any of them sends is a `Trebind` carrying a full
/// credential payload; every reply here is a few tens of bytes. Sizing it to
/// `msize` instead would put a 10 MiB allocation on the reconnect path, which
/// is exactly where memory is least likely to be available.
const REPLAY_FRAME_BYTES: usize = 256;

/// Byte-range locks one mount may hold at once.
///
/// The userspace client keeps an unbounded list. Pinned kernel memory driven by
/// an unprivileged `fcntl` needs a ceiling, and this one is also what bounds the
/// slot reservation a lock takes before it reaches the wire. Exhaustion is
/// reported as `ENOLCK` with nothing sent, which is the errno `fcntl(2)`
/// documents for it and keeps the record set and the server in agreement: a
/// refused lock was never taken. A release can be refused too, because removing
/// part of a recorded range can split it, which is why the reservation for an
/// unlock is taken before its caller gives up the local grant.
///
/// The value is a replay cost as much as a memory one. Every record is one
/// round trip on every reconnect, so the bound also keeps recovery work and
/// pinned state finite.
const MAX_LOCK_RECORDS: usize = 1024;

/// Bytes in the per-mount lock owner identity: `zerofs-` plus a hex UUID.
const CLIENT_ID_LEN: usize = 7 + 2 * 16;

const IO_TASK_NAME: &[u8] = b"zerofs-io\0";

/// A started kthread and the extra task reference that keeps it joinable.
struct KthreadHandle {
    task: ARef<Task>,
    session: Pin<KBox<Session>>,
}

impl KthreadHandle {
    fn start(
        session: Pin<KBox<Session>>,
        entry: Option<KthreadEntry>,
        name: &[u8],
    ) -> Result<Self> {
        let session_pointer = session.as_ref().get_ref() as *const Session as *mut c_void;

        // SAFETY: The handle retains the pinned Session until this task has
        // stopped. The static format string is NUL terminated and contains no
        // conversions.
        let task = unsafe {
            bindings::kthread_create_on_node(
                entry,
                session_pointer,
                bindings::NUMA_NO_NODE,
                name.as_ptr().cast::<ffi::c_char>(),
            )
        };
        let task = kernel::error::from_err_ptr(task)?;
        let task = NonNull::new(task).ok_or_else(|| ENOMEM)?;

        // Keep task_struct alive if the thread exits before the handle joins
        // it, then transfer that reference to ARef.
        unsafe {
            bindings::get_task_struct(task.as_ptr());
        }
        // SAFETY: get_task_struct above transferred one live reference.
        let task = unsafe { ARef::from_raw(task.cast::<Task>()) };
        // SAFETY: The retained Task is the kthread just created above.
        unsafe {
            bindings::wake_up_process(task.as_ptr());
        }
        Ok(Self { task, session })
    }

    fn session(&self) -> &Session {
        &self.session
    }
}

impl Drop for KthreadHandle {
    fn drop(&mut self) {
        // SAFETY: This handle owns both a live kthread reference and the pinned
        // Session passed as its callback data. kthread_stop takes its own
        // temporary reference; ARef releases this handle's reference before
        // the Session field is dropped.
        unsafe {
            bindings::kthread_stop(self.task.as_ptr());
        }
    }
}

/// A negotiated ZeroFS session, its tag table and its connection task.
pub(crate) struct Client {
    io_task: KthreadHandle,
    negotiated_msize: u32,
    root_qid: Qid,
}

impl Client {
    /// Connect, negotiate the private dialect, query lineage, and bind root.
    pub(crate) fn connect(endpoint: Endpoint, credentials: &RebindCredentials) -> Result<Self> {
        let endpoint = endpoint.validate()?;
        let bootstrapped = bootstrap_connection(&endpoint, credentials)?;
        let negotiated_msize = bootstrapped.candidate.negotiated_msize;
        // Bootstrap needs a finite receive deadline because no request waiter
        // can retire a silent candidate. From here the session receiver owns
        // the socket wait, while ordinary callers detect connection-wide
        // receive silence and retire the transport after a failed probe.
        bootstrapped.candidate.transport.set_blocking_receive()?;
        let session = Session::new(endpoint, bootstrapped.candidate)?;
        // Failing here drops the transport, so the server's connection guard
        // releases the root fid installed by the bootstrap Trebind.
        session.record_root_fid(ROOT_FID, credentials)?;
        let io_task = KthreadHandle::start(session, Some(io_task_main), IO_TASK_NAME)?;

        Ok(Self {
            io_task,
            negotiated_msize,
            root_qid: bootstrapped.root_qid,
        })
    }

    fn session(&self) -> &Session {
        self.io_task.session()
    }

    pub(crate) fn negotiated_msize(&self) -> u32 {
        self.negotiated_msize
    }

    pub(crate) fn pending_tag_capacity(&self) -> usize {
        self.session().slot_count().min(MAX_INFLIGHT_REQUESTS)
    }

    pub(crate) fn root_qid(&self) -> Qid {
        self.root_qid
    }

    pub(crate) fn root_fid(&self) -> u32 {
        ROOT_FID
    }

    /// Permanently abort this mount's session for a forced unmount.
    ///
    /// The owning MountState remains alive until put_super, so Client::drop
    /// performs the eventual connection-task join.
    pub(crate) fn terminate_for_unmount(&self) {
        self.session().terminate(EIO);
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Terminating first shuts down both the current transport and any
        // candidate the connection task is dialing or replaying on. Without
        // that, joining could wait out a dial plus a full replay.
        self.session().terminate(not_connected_errno());
        // KthreadHandle joins the task before dropping its Session.
    }
}

unsafe extern "C" fn io_task_main(data: *mut c_void) -> ffi::c_int {
    if data.is_null() {
        return EINVAL.to_errno();
    }
    // SAFETY: KthreadHandle owns the pinned Session and joins this thread
    // before dropping that allocation.
    let session = unsafe { &*data.cast::<Session>() };
    session.io_loop()
}

type KthreadEntry = unsafe extern "C" fn(*mut c_void) -> ffi::c_int;

/// Monotonic nanoseconds, the clock every deadline in this client uses.
fn monotonic_ns() -> u64 {
    // SAFETY: ktime_get() is always safe to call outside NMI context and
    // returns a non-negative CLOCK_MONOTONIC timestamp.
    unsafe { bindings::ktime_get() as u64 }
}

fn elapsed_ms(since_ns: u64, now_ns: u64) -> u64 {
    now_ns.saturating_sub(since_ns) / 1_000_000
}

/// Clamps rather than truncating: a bare `as u32` on an out-of-range
/// millisecond count would turn a long wait into a short one.
fn jiffies_for_ms(milliseconds: u64) -> usize {
    msecs_to_jiffies(milliseconds.min(u32::MAX as u64) as u32) as usize
}
