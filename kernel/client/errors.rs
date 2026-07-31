use kernel::{bindings, ffi, prelude::*};

use crate::protocol;

pub(super) fn message_size_errno() -> Error {
    errno!(EMSGSIZE)
}

pub(super) fn not_connected_errno() -> Error {
    errno!(ENOTCONN)
}

pub(super) fn protocol_errno() -> Error {
    errno!(EPROTO)
}

/// Errno for a codec failure on a request that never reached the wire.
///
/// Nothing was transmitted, so the failure says nothing about the peer and the
/// caller reports it without touching the connection.
pub(super) fn codec_errno_without_disconnect(error: protocol::CodecError) -> Error {
    match error {
        protocol::CodecError::BufferTooSmall
        | protocol::CodecError::MessageTooLarge
        | protocol::CodecError::StringTooLong
        | protocol::CodecError::TooManyNames
        | protocol::CodecError::LengthOverflow => message_size_errno(),
        _ => protocol_errno(),
    }
}

/// Errno for a codec failure on a frame already received.
///
/// The mapping is the encode-side one above. The separate name marks the
/// callers whose failure implicates the stream itself, since bytes past an
/// undecodable frame cannot be framed, so each one ends the connection rather
/// than reading on.
pub(super) fn codec_errno(error: protocol::CodecError) -> Error {
    codec_errno_without_disconnect(error)
}

/// Errno for an `Rlerror` code.
///
/// A code outside the errno range, or one of the kernel's internal restart
/// values, would let the server ask a syscall to restart, so it degrades to a
/// protocol error instead.
pub(super) fn server_errno(ecode: u32) -> Error {
    let status = -(ecode as ffi::c_int);
    if (1..=bindings::MAX_ERRNO).contains(&ecode) && !is_internal_restart_status(status) {
        Error::from_errno(status)
    } else {
        protocol_errno()
    }
}

/// Whether an error means the stream itself no longer makes sense.
///
/// A frame that passed the tag, type and size checks but fails to decode is a
/// server bug, not a lost peer, and resending would only repeat it.
pub(super) fn is_protocol_error(error: Error) -> bool {
    let status = error.to_errno();
    is_status(status, bindings::EPROTO) || is_status(status, bindings::EMSGSIZE)
}

pub(super) fn is_interrupted_error(error: Error) -> bool {
    let status = error.to_errno();
    is_status(status, bindings::EINTR) || is_internal_restart_status(status)
}

pub(super) fn is_internal_restart_status(status: ffi::c_int) -> bool {
    is_status(status, bindings::ERESTARTSYS)
        || is_status(status, bindings::ERESTARTNOINTR)
        || is_status(status, bindings::ERESTARTNOHAND)
        || is_status(status, bindings::ERESTART_RESTARTBLOCK)
}

/// `bindings` errno names are positive; a status carries the negation.
fn is_status(status: ffi::c_int, positive_errno: u32) -> bool {
    status == -(positive_errno as ffi::c_int)
}
