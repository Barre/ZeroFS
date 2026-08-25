use core::ptr;

use kernel::{bindings, error::to_result, ffi, prelude::*};

use super::errors::is_interrupted_error;

/// Temporarily blocks every blockable signal for the current syscall task.
///
/// A complete 9P frame cannot be abandoned halfway through a stream send.
/// Signals remain pending while the transmit mutex is held and are observed by
/// the reply wait afterward, which keeps owning the request until its terminal
/// reply. An `OpAttempt` can also hold this guard while a request resolves
/// across reconnect.
/// Drop restores the task's exact prior mask and recalculates pending signals.
pub(super) struct SendSignalMask {
    saved: bindings::sigset_t,
}

impl SendSignalMask {
    pub(super) fn block() -> Result<Self> {
        // The in-kernel sigprocmask, unlike the userspace syscall, happily
        // blocks SIGKILL and SIGSTOP, so the caller has to exclude them.
        let unblockable = signal_bit(bindings::SIGKILL) | signal_bit(bindings::SIGSTOP);
        let mut blocked = bindings::sigset_t {
            sig: [!unblockable],
        };

        let mut saved = bindings::sigset_t::default();
        // SAFETY: This runs in the interrupted VFS caller's task context.
        // Both sigsets are initialized stack objects valid for this call.
        let status = unsafe {
            bindings::sigprocmask(bindings::SIG_BLOCK as ffi::c_int, &mut blocked, &mut saved)
        };
        to_result(status)?;
        Ok(Self { saved })
    }
}

impl Drop for SendSignalMask {
    fn drop(&mut self) {
        // SAFETY: A successful block() captured this same task's prior mask.
        // This synchronous guard never crosses into another task context.
        let status = unsafe {
            bindings::sigprocmask(
                bindings::SIG_SETMASK as ffi::c_int,
                &mut self.saved,
                ptr::null_mut(),
            )
        };
        if status < 0 {
            pr_err!("failed to restore request signal mask: errno={status}\n");
        }
    }
}

/// Send-error hook that blocks signals once and resumes the started frame.
///
/// It fires only on the first interrupted send, so a second failure, or any
/// non-interrupted one, still propagates and the hook cannot spin.
pub(super) fn resume_interrupted_send(
    signal_mask: &mut Option<SendSignalMask>,
) -> impl FnMut(Error) -> Result<()> + '_ {
    move |error| {
        if signal_mask.is_none() && is_interrupted_error(error) {
            *signal_mask = Some(SendSignalMask::block()?);
            return Ok(());
        }
        Err(error)
    }
}

/// Sleep despite a pending signal while consuming one jiffy of a bounded wait.
pub(super) fn sleep_uninterruptible_tick(remaining: &mut usize) -> bool {
    if *remaining == 0 {
        return false;
    }
    // SAFETY: Sleeping is permitted in VFS request context here; no spinlock
    // or session mutex is held. A one-jiffy poll interval bounds response
    // latency without allowing a pending signal to spin this task.
    let _ = unsafe { bindings::schedule_timeout_uninterruptible(1) };
    *remaining -= 1;
    true
}

/// Single-bit `sigset_t` mask for `signal`, as the kernel's `sigmask()` macro
/// computes it.
fn signal_bit(signal: u32) -> ffi::c_ulong {
    (1 as ffi::c_ulong) << (signal - 1)
}
