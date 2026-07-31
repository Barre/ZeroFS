//! POSIX and BSD file-lock callbacks.

use kernel::{
    bindings,
    error::{
        Error, Result,
        code::{EAGAIN, EINVAL, ERESTARTSYS},
        from_result,
    },
    ffi,
    time::msecs_to_jiffies,
};

use crate::protocol;

use super::{
    LOCK_RETRY_POLL_MS,
    attributes::{expire_cached_data, protocol_error},
    io::{FileLockRef, OpenFileRef},
};

// include/linux/fcntl.h spells IS_GETLK/IS_SETLK/IS_SETLKW as function-like
// macros, which bindgen does not emit. Where BITS_PER_LONG is 64, the only ABI
// this module builds for, their 32-bit arms are constant false and the *64 arms
// compare against the plain commands; F_GETLK64 and friends are dead there.
// OFD commands need no arm of their own: fcntl_setlk rewrites F_OFD_SETLK to
// F_SETLK and F_OFD_SETLKW to F_SETLKW before dispatching, and vfs_test_lock
// always passes F_GETLK.
fn is_getlk(cmd: ffi::c_int) -> bool {
    cmd == bindings::F_GETLK as ffi::c_int
}

fn is_setlk(cmd: ffi::c_int) -> bool {
    cmd == bindings::F_SETLK as ffi::c_int
}

fn is_setlkw(cmd: ffi::c_int) -> bool {
    cmd == bindings::F_SETLKW as ffi::c_int
}

/// Map a VFS lock type onto its 9P wire value.
fn wire_lock_type(flc_type: u8) -> Result<u8> {
    match flc_type as ffi::c_uint {
        bindings::F_RDLCK => Ok(protocol::LOCK_TYPE_RDLCK),
        bindings::F_WRLCK => Ok(protocol::LOCK_TYPE_WRLCK),
        bindings::F_UNLCK => Ok(protocol::LOCK_TYPE_UNLCK),
        _ => Err(EINVAL),
    }
}

/// Inverse of [`wire_lock_type`], for the holder an `Rgetlock` names.
fn vfs_lock_type(lock_type: u8) -> Result<u8> {
    match lock_type {
        protocol::LOCK_TYPE_RDLCK => Ok(bindings::F_RDLCK as u8),
        protocol::LOCK_TYPE_WRLCK => Ok(bindings::F_WRLCK as u8),
        protocol::LOCK_TYPE_UNLCK => Ok(bindings::F_UNLCK as u8),
        _ => Err(protocol_error()),
    }
}

/// Acquire a POSIX or BSD lock: the VFS's own lock state first, then the
/// server, rolling the local grant back if the server refuses.
///
/// The server arbitrates per session and exempts every lock the requesting
/// connection already holds, so two processes on this mount are excluded from
/// each other only by the local grant. Taking it first also means an
/// `F_SETLKW` waits on the VFS queue before any other client is excluded,
/// which is what keeps two mounts contending in opposite order from
/// deadlocking. Remote-first would additionally leave the VFS believing in a
/// lock the server refused.
fn zerofs_do_setlk(
    file: &OpenFileRef<'_>,
    lock: &mut FileLockRef<'_>,
    blocking: bool,
) -> Result<()> {
    let requested = wire_lock_type(lock.lock_type())?;
    if requested == protocol::LOCK_TYPE_UNLCK {
        return Err(EINVAL);
    }
    let range = lock.range_9p()?;
    let owner = lock.owner();
    // A grant can widen the request in place, so the rollback below needs the
    // range as it was asked for.
    let requested_range = lock.raw_range();
    let fid = file.state().fid();
    let state = file.inode().mount()?;

    // Upload first, so a peer that has been waiting for this range does not
    // read bytes this mount is still holding dirty.
    let mapping = file.inode().mapping()?;
    let writeback = mapping.write_and_wait_all();
    if writeback < 0 {
        return Err(Error::from_errno(writeback));
    }

    lock.local_apply(file.inode())?;
    let flags = if blocking {
        protocol::LOCK_FLAGS_BLOCK
    } else {
        0
    };
    let outcome = loop {
        match state.client.lock(fid, requested, flags, range, owner) {
            Ok(protocol::LOCK_SUCCESS) => break Ok(()),
            // The server never waits. Both answers mean another client holds
            // the range, and which one it picks depends only on the flag.
            Ok(protocol::LOCK_BLOCKED) => {}
            Err(error) if error == EAGAIN => {}
            // Grace, an explicit lock error, and any status this wire does not
            // define cannot clear by waiting. fs/9p answers ENOLCK for all
            // three; the codec deliberately passes an unknown byte through so
            // it lands here instead of ending the logical session.
            Ok(_) => break Err(errno!(ENOLCK)),
            Err(error) => break Err(error),
        }
        if !blocking {
            break Err(EAGAIN);
        }
        // SAFETY: The sleep has no caller-side precondition. No inode or
        // mapping lock is held across it; the local grant that is held is the
        // one a same-mount waiter would queue behind anyway.
        let woken_early = unsafe {
            bindings::schedule_timeout_interruptible(
                msecs_to_jiffies(LOCK_RETRY_POLL_MS).max(1) as ffi::c_long
            )
        };
        if woken_early != 0 {
            // Nothing else wakes this task, so this is a signal. Unlike v9fs,
            // which reports EAGAIN, restart the syscall: EAGAIN is not a value
            // F_SETLKW can produce and applications do not handle it.
            break Err(ERESTARTSYS);
        }
    };
    if let Err(error) = outcome {
        lock.revert_local(file.inode(), requested_range);
        return Err(error);
    }
    // The range is this mount's now, so anything cached before the grant may
    // predate a peer's writes.
    expire_cached_data(file.inode());
    Ok(())
}

/// Release a lock: local state first, then the server, and never rolled back.
///
/// Reinstating a local lock after a failed remote release would exclude this
/// mount's own processes from a range with no remote protection left. The
/// client prunes its replay record only on a granted unlock, so a failure here
/// leaves the lock recorded and reacquired on reconnect: over-holding, which
/// is safe, rather than under-holding, which is not.
fn zerofs_do_unlk(file: &OpenFileRef<'_>, lock: &mut FileLockRef<'_>) -> Result<()> {
    let range = lock.range_9p()?;
    let owner = lock.owner();
    let fid = file.state().fid();
    let state = file.inode().mount()?;

    // Upload before the range stops being ours, so a peer that acquires it
    // next does not read stale bytes. This is only an upload barrier; fsync
    // remains the ZeroFS durability barrier. A task unwinding with a fatal
    // signal pending cannot complete it and still owes the unlock, so the
    // error stays in wb_err for whoever reports it next.
    if let Ok(mapping) = file.inode().mapping() {
        let _ = mapping.write_and_wait_all();
    }

    // The record table is bounded and releasing part of a recorded range can
    // need one more slot than it frees, so this is the one part of an unlock
    // that can be refused. Claiming it before the local release is what keeps a
    // refusal consistent: nothing has been given up yet, so the caller still
    // holds the lock everywhere.
    let slot = state.client.reserve_unlock(fid, owner.flock)?;

    // As in v9fs, a failed local release stops before the wire: the VFS still
    // believes the lock is held, and dropping it on the server would leave the
    // two disagreeing in the unsafe direction.
    lock.local_apply(file.inode())?;
    let Some(slot) = slot else {
        // This flavor holds nothing on the fid, so the only thing the wire
        // release could subtract from is the other flavor's ranges.
        return Ok(());
    };
    match state.client.unlock(slot, fid, range, owner)? {
        protocol::LOCK_SUCCESS => Ok(()),
        _ => Err(errno!(ENOLCK)),
    }
}

/// Run [`zerofs_do_unlk`] and decide whether its failure is reportable.
///
/// A close-time release stays local. `locks_remove_posix` scopes its release to
/// the owner whose descriptor is closing, but the wire has no owner: a `Tlock`
/// names only the fid, and the server drops every lock that fid holds. Two
/// owners share one fid whenever they share a `struct file`, which is what a
/// fork leaves behind, so forwarding this would destroy a lock the other owner
/// still holds and still believes in. The userspace client does not forward it
/// either: the fid's own guard releases whatever is left when its last
/// reference clunks it, and a dirty-folio group holds that fid until writeback
/// completes, so no peer can be handed the range before this mount's data has
/// landed. Until then the server over-holds, which is the safe direction.
///
/// `locks_remove_posix` and `locks_remove_flock` discard this return, and the
/// application has stopped believing it holds the lock, so a close-time release
/// reports success either way. An explicit `fcntl(F_UNLCK)` must see the
/// failure.
fn zerofs_unlock_status(file: &OpenFileRef<'_>, lock: &mut FileLockRef<'_>) -> Result<()> {
    if lock.is_close() {
        let _ = lock.local_apply(file.inode());
        return Ok(());
    }
    zerofs_do_unlk(file, lock)
}

/// Report the lock that would conflict with this range.
///
/// Local test first, and the requested type is captured before
/// `posix_test_lock` overwrites it. The server reports no conflict for a lock
/// this same session holds, so asking it first would answer "free" for a range
/// another process on this mount owns. `fs/9p` reads the type after the local
/// test and consequently always sends `LOCK_TYPE_UNLCK`, which this server
/// treats as conflicting with an overlapping reader; the userspace client
/// sends the real type and is the authority here.
fn zerofs_do_getlk(file: &OpenFileRef<'_>, lock: &mut FileLockRef<'_>) -> Result<()> {
    let requested = wire_lock_type(lock.lock_type())?;
    let range = lock.range_9p()?;
    let proc_id = lock.pid() as u32;
    let fid = file.state().fid();
    let state = file.inode().mount()?;

    lock.local_test(file);
    if lock.lock_type() != bindings::F_UNLCK as u8 {
        // posix_test_lock has already described the local holder.
        return Ok(());
    }
    let holder = state.client.getlock(fid, requested, range, proc_id)?;
    if holder.lock_type == protocol::LOCK_TYPE_UNLCK {
        return Ok(());
    }
    let holder_type = vfs_lock_type(holder.lock_type)?;
    lock.set_range_9p(holder.start, holder.length)?;
    lock.set_lock_type(holder_type);
    // A pid from another node means nothing in this node's pid space. Report
    // it negated, the v9fs convention, so nothing can signal a local process
    // that happens to share the number.
    lock.set_pid((holder.proc_id as bindings::pid_t).wrapping_neg());
    Ok(())
}

pub(super) unsafe extern "C" fn zerofs_lock(
    file: *mut bindings::file,
    cmd: ffi::c_int,
    lock: *mut bindings::file_lock,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the file for the duration of this callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        // SAFETY: VFS lends this request for the duration of the callback, and
        // this task is its only mutator.
        let mut lock = unsafe { FileLockRef::from_raw(lock) }?;
        if is_getlk(cmd) {
            zerofs_do_getlk(&file, &mut lock)?;
            return Ok(0);
        }
        if !is_setlk(cmd) && !is_setlkw(cmd) {
            // F_CANCELLK is unreachable: this filesystem never returns
            // FILE_LOCK_DEFERRED, so no request is ever left outstanding to
            // cancel. fs/9p rejects it the same way.
            return Err(EINVAL);
        }
        if lock.lock_type() == bindings::F_UNLCK as u8 {
            zerofs_unlock_status(&file, &mut lock)?;
            return Ok(0);
        }
        zerofs_do_setlk(&file, &mut lock, is_setlkw(cmd))?;
        Ok(0)
    })
}

pub(super) unsafe extern "C" fn zerofs_flock(
    file: *mut bindings::file,
    cmd: ffi::c_int,
    lock: *mut bindings::file_lock,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains the file for the duration of this callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        // SAFETY: VFS lends this request for the duration of the callback, and
        // this task is its only mutator.
        let mut lock = unsafe { FileLockRef::from_raw(lock) }?;
        // The request keeps FL_FLOCK instead of being rewritten to FL_POSIX the
        // way fs/9p does. That conversion stores flock locks in flc_posix owned by
        // the file, which collides with OFD locks on the same descriptor and makes
        // a process's own flock conflict with its own fcntl lock. fs/nfs keeps
        // FL_FLOCK and lets locks_remove_flock deliver the close-time release,
        // which is what this reproduces. The wire is unaffected: both simulate an
        // flock as a whole-file byte range, and this server has no BSD mode.
        //
        // The two flavors are still one owner on the server, which keys a lock by
        // fid alone. The client refuses to let one descriptor hold both rather than
        // let a release under one of them quietly shrink the other's ranges; a fid
        // per flavor is what would support the combination.
        if !lock.is_flock() {
            return Err(errno!(ENOLCK));
        }
        // locks_remove_flock issues the close-time release as F_SETLKW/F_UNLCK.
        if lock.lock_type() == bindings::F_UNLCK as u8 {
            zerofs_unlock_status(&file, &mut lock)?;
            return Ok(0);
        }
        if !is_setlk(cmd) && !is_setlkw(cmd) {
            return Err(EINVAL);
        }
        zerofs_do_setlk(&file, &mut lock, is_setlkw(cmd))?;
        Ok(0)
    })
}
