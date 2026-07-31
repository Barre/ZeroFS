//! Allocation-free codec shared by userspace and the native ZeroFS client.
//!
//! ZeroFS speaks the private `9P2000.L.Z` dialect. This module implements the
//! messages used by the native kernel client:
//! version and lineage negotiation, fid rebinding, compound walk/getattr,
//! getattr/setattr, open/create, positioned reads and writes, namespace
//! mutations, symlinks, fallocate, verified fsync, readdir-with-attributes,
//! clunk, statfs, and byte-range locking.
//!
//! The codec has no dependency on `std`, `alloc`, or third-party crates.
//! Requests are written into a caller-owned fixed buffer and responses borrow
//! their variable-length fields from a caller-owned frame.  A transport must
//! keep that frame alive while the decoded response is in use.
//!
//! All scalar fields are little-endian.  A frame is:
//!
//! ```text
//! size[4] type[1] tag[2] body[size - 7]
//! ```
//!
//! ZeroFS mutations carry a private envelope immediately after the tag:
//!
//! ```text
//! op_id[16] flags[1] origin_writer_epoch[8]
//! ```
//!
//! Responses and durability barriers do not carry the envelope.

#![cfg_attr(MODULE, allow(dead_code, unreachable_pub))]

#[cfg(MODULE)]
#[path = "wire_requests.rs"]
mod wire_requests;
#[cfg(MODULE)]
#[path = "wire_types.rs"]
mod wire_types;

#[cfg(not(MODULE))]
pub use crate::wire_types::*;
#[cfg(MODULE)]
pub use wire_types::*;

pub const MAX_MSIZE: u32 = P9_MAX_MSIZE;

/// Reserved tag required by `Tversion`.
pub const NOTAG: u16 = u16::MAX;

/// `Tunlinkat.flags` bit distinguishing rmdir from unlink.
pub const AT_REMOVEDIR: u32 = 0x200;

/// Maximum component length accepted by the ZeroFS server.
pub const MAX_NAME_LEN: usize = P9_MAX_NAME_LEN as usize;

/// `Tlock.lock_type`: shared read lock.
pub const LOCK_TYPE_RDLCK: u8 = 0;

/// `Tlock.lock_type`: exclusive write lock.
pub const LOCK_TYPE_WRLCK: u8 = 1;

/// `Tlock.lock_type`: release the named range. `Rgetlock` reports it when no
/// lock conflicts with the probe.
pub const LOCK_TYPE_UNLCK: u8 = 2;

/// `Tlock.flags`: report a conflict as [`LOCK_BLOCKED`] instead of `EAGAIN`.
/// The server never waits, so the caller owns the retry loop either way.
pub const LOCK_FLAGS_BLOCK: u32 = 1;

/// `Rlock.status`: the range is held by the requesting fid.
pub const LOCK_SUCCESS: u8 = LockStatus::Success.as_wire();

/// `Rlock.status`: a conflicting lock exists and the request wanted to block.
pub const LOCK_BLOCKED: u8 = LockStatus::Blocked.as_wire();

/// `Rlock.status`: the server refused the lock.
pub const LOCK_ERROR: u8 = LockStatus::LockError.as_wire();

/// `Rlock.status`: the server is in its post-restart grace period.
pub const LOCK_GRACE: u8 = LockStatus::Grace.as_wire();

/// `Trebind` restores a previously recorded fid after reconnection.
pub const REBIND_REPLAY: u8 = P9_REBIND_REPLAY;

/// `Trebind` marks a fid for a forthcoming in-place replay reopen. The flag
/// grants neither access nor an inode pin.
pub const REBIND_OPENED: u8 = P9_REBIND_OPENED;

/// All currently defined `Trebind.flags` bits.
pub const REBIND_KNOWN_FLAGS: u8 = P9_REBIND_KNOWN_FLAGS;
pub const REBIND_CREDENTIAL_SENTINEL: u8 = P9_REBIND_CREDENTIAL_SENTINEL;
pub const REBIND_CREDENTIAL_VERSION: u8 = P9_REBIND_CREDENTIAL_VERSION;
pub const REBIND_CREDENTIAL_GROUPS_INCOMPLETE: u8 = P9_REBIND_CREDENTIAL_GROUPS_INCOMPLETE;
pub const REBIND_CREDENTIAL_HEADER_SIZE: usize = P9_REBIND_CREDENTIAL_HEADER_SIZE;
pub const REBIND_CREDENTIAL_MAX_SIZE: usize = P9_REBIND_CREDENTIAL_MAX_SIZE;

/// Exclusive end of a 9P lock range. A zero length extends to EOF.
///
/// This is the single source of truth for 9P lock arithmetic.
/// `lock_range.rs` wraps it for the server's allocating API.
pub fn lock_range_end(start: u64, length: u64) -> u64 {
    if length == 0 {
        u64::MAX
    } else {
        start.saturating_add(length)
    }
}

/// Remove one range from another, returning the surviving left and right
/// fragments as `(start, length)`. A zero length extends to EOF, both on input
/// and on a right fragment that reaches it.
///
/// At most two fragments survive, so the fixed array is exact rather than a
/// bound.
pub fn subtract_lock_range(
    held_start: u64,
    held_length: u64,
    remove_start: u64,
    remove_length: u64,
) -> [Option<(u64, u64)>; 2] {
    let held_end = lock_range_end(held_start, held_length);
    let remove_end = lock_range_end(remove_start, remove_length);
    if held_start >= remove_end || remove_start >= held_end {
        return [Some((held_start, held_length)), None];
    }

    let mut left = None;
    if held_start < remove_start {
        let left_end = remove_start.min(held_end);
        if held_start < left_end {
            left = Some((held_start, left_end - held_start));
        }
    }

    let mut right = None;
    if remove_end < held_end {
        let right_start = remove_end.max(held_start);
        if right_start < held_end {
            // Re-encode an open-ended survivor as EOF rather than as the
            // distance to u64::MAX, which would bound a lock that is not.
            let right_length = if held_end == u64::MAX {
                0
            } else {
                held_end - right_start
            };
            right = Some((right_start, right_length));
        }
    }

    [left, right]
}

/// Bytes in `size[4] + type[1] + tag[2]`.
pub const HEADER_SIZE: usize = P9_HEADER_SIZE;

/// Bytes in a mutation operation identifier.
pub const OP_ID_SIZE: usize = P9_OP_ID_LEN;

/// The frame is an ambiguous resend of an operation identifier.
pub const OP_FLAG_RETRY: u8 = P9_OP_FLAG_RETRY;

/// All mutation-envelope flag bits currently defined by ZeroFS.
pub const OP_KNOWN_FLAGS: u8 = P9_OP_KNOWN_FLAGS;

/// Bytes in `op_id[16] + flags[1] + origin_writer_epoch[8]`.
pub const OP_ENVELOPE_SIZE: usize = P9_OP_ENVELOPE_LEN;

/// Fixed wire bytes in an enveloped `Twrite`, before its data payload.
pub const TWRITE_OVERHEAD: usize = HEADER_SIZE + OP_ENVELOPE_SIZE + 4 + 8 + 4;

/// Serialized size of a [`Qid`].
pub const QID_WIRE_SIZE: usize = Qid::WIRE_SIZE;

/// Serialized size of a [`Stat`].
pub const STAT_WIRE_SIZE: usize = Stat::WIRE_SIZE;

/// A bounded codec failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// The caller-provided output buffer cannot hold the request.
    BufferTooSmall,
    /// An encoded or declared frame exceeds the caller's message-size limit.
    MessageTooLarge,
    /// A 9P string exceeds the `u16` length field.
    StringTooLong,
    /// A walk contains more names than its `u16` count can represent.
    TooManyNames,
    /// An integer length computation overflowed.
    LengthOverflow,
    /// A frame or variable-length field ends before its declared length.
    Truncated,
    /// A declared frame size is smaller than the 9P header.
    InvalidFrameSize,
    /// The frame slice contains fewer or more bytes than its size field.
    FrameSizeMismatch,
    /// `Tversion` did not use `NOTAG`, or another request did use it.
    InvalidTag,
    /// The response tag does not match the outstanding request.
    TagMismatch {
        /// Tag allocated to the request.
        expected: u16,
        /// Tag received from the server.
        actual: u16,
    },
    /// The frame type is outside the response subset understood here.
    UnexpectedMessageType(u8),
    /// A decoded fixed-layout response has extra bytes.
    TrailingData,
}

/// Largest data payload an enveloped `Twrite` can carry within `msize`.
pub fn max_write_payload(msize: u32) -> u32 {
    msize.saturating_sub(TWRITE_OVERHEAD as u32)
}

/// One request supported by the native client.
#[derive(Clone, Copy)]
pub enum Request<'a> {
    /// Negotiate `9P2000.L.Z`. This request must use [`NOTAG`].
    Tversion { msize: u32, version: &'a [u8] },
    /// Query durability lineage and the active writer epoch.
    Tgetlineage,
    /// Bind a fresh fid to a known inode.
    Trebind {
        fid: u32,
        inode_id: u64,
        root_inode: u64,
        flags: u8,
        /// Legacy UTF-8 username, or the versioned binary credential payload.
        uname: &'a [u8],
        /// Numeric uid; this is fsuid when `uname` is a credential payload.
        n_uname: u32,
    },
    /// Walk all names and return attributes for the destination.
    Twalkgetattr {
        fid: u32,
        newfid: u32,
        names: &'a [&'a [u8]],
    },
    /// Fetch attributes for a fid.
    Tgetattr { fid: u32, request_mask: u64 },
    /// Atomically update selected attributes and return the post-operation stat.
    Tsetattrattr {
        envelope: MutationEnvelope,
        fid: u32,
        valid: u32,
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
        atime_sec: u64,
        atime_nsec: u64,
        mtime_sec: u64,
        mtime_nsec: u64,
    },
    /// Atomically allocate, punch, or zero a regular-file range.
    Tfallocate {
        envelope: MutationEnvelope,
        fid: u32,
        offset: u64,
        length: u64,
        mode: u32,
    },
    /// Open a copy of `fid` as `newfid`.
    Tlopenat { fid: u32, newfid: u32, flags: u32 },
    /// Create and open a regular file on `newfid`, preserving `dfid`.
    Tlcreateattr {
        envelope: MutationEnvelope,
        dfid: u32,
        newfid: u32,
        name: &'a [u8],
        flags: u32,
        mode: u32,
        gid: u32,
    },
    /// Create a directory and return its post-operation stat.
    Tmkdirattr {
        envelope: MutationEnvelope,
        dfid: u32,
        name: &'a [u8],
        mode: u32,
        gid: u32,
    },
    /// Create a symlink and return its post-operation stat.
    Tsymlinkattr {
        envelope: MutationEnvelope,
        dfid: u32,
        name: &'a [u8],
        target: &'a [u8],
        gid: u32,
    },
    /// Create a special node and return its post-operation stat.
    Tmknodattr {
        envelope: MutationEnvelope,
        dfid: u32,
        name: &'a [u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
    },
    /// Create a hard link and return the linked inode's post-operation stat.
    Tlinkattr {
        envelope: MutationEnvelope,
        dfid: u32,
        fid: u32,
        name: &'a [u8],
    },
    /// Atomically rename one directory entry, replacing a compatible target.
    Trenameat {
        envelope: MutationEnvelope,
        olddirfid: u32,
        oldname: &'a [u8],
        newdirfid: u32,
        newname: &'a [u8],
    },
    /// Remove a directory entry; `AT_REMOVEDIR` selects rmdir semantics.
    Tunlinkat {
        envelope: MutationEnvelope,
        dirfid: u32,
        name: &'a [u8],
        flags: u32,
    },
    /// Read a symlink's opaque target bytes.
    Treadlink { fid: u32 },
    /// Cancel or retire the outstanding request identified by `oldtag`.
    Tflush { oldtag: u16 },
    /// Read bytes from an open fid.
    Tread { fid: u32, offset: u64, count: u32 },
    /// Write bytes to an open fid using the ZeroFS mutation envelope.
    Twrite {
        envelope: MutationEnvelope,
        fid: u32,
        offset: u64,
        data: &'a [u8],
    },
    /// Flush and verify the lineage of previously acknowledged writes.
    Tfsyncdur { fid: u32, datasync: u32, token: u64 },
    /// Read directory entries carrying full attributes.
    Treaddirattr { fid: u32, offset: u64, count: u32 },
    /// Release a fid.
    Tclunk { fid: u32 },
    /// Fetch filesystem capacity and limits.
    Tstatfs { fid: u32 },
    /// Acquire or release a byte range. `lock_type` is one of the
    /// `LOCK_TYPE_*` values; a conflict is answered rather than waited on.
    Tlock {
        fid: u32,
        lock_type: u8,
        flags: u32,
        start: u64,
        length: u64,
        proc_id: u32,
        /// Opaque lock owner identity, echoed back by `Rgetlock`.
        client_id: &'a [u8],
    },
    /// Report the lock that would conflict with this range, if any.
    Tgetlock {
        fid: u32,
        lock_type: u8,
        start: u64,
        length: u64,
        proc_id: u32,
        client_id: &'a [u8],
    },
}

// Request encoding, type IDs, tag rules, and size arithmetic come from the
// table in `wire_requests.rs`. The borrowed enum and owned codec declare their
// fields separately; cross-codec tests keep all three representations aligned.

macro_rules! put_request_field {
    ($writer:expr, $value:expr, u8) => {
        $writer.put_u8($value)?
    };
    ($writer:expr, $value:expr, u16) => {
        $writer.put_u16($value)?
    };
    ($writer:expr, $value:expr, u32) => {
        $writer.put_u32($value)?
    };
    ($writer:expr, $value:expr, u64) => {
        $writer.put_u64($value)?
    };
    ($writer:expr, $value:expr, str) => {
        $writer.put_string($value)?
    };
    ($writer:expr, $value:expr, envelope) => {
        put_mutation_envelope(&mut $writer, $value)?
    };
    ($writer:expr, $value:expr, names) => {{
        if $value.len() > u16::MAX as usize {
            return Err(CodecError::TooManyNames);
        }
        $writer.put_u16($value.len() as u16)?;
        for name in $value {
            $writer.put_string(name)?;
        }
    }};
    ($writer:expr, $value:expr, payload) => {{
        if $value.len() > u32::MAX as usize {
            return Err(CodecError::MessageTooLarge);
        }
        $writer.put_u32($value.len() as u32)?;
        $writer.put($value)?
    }};
}

macro_rules! request_field_size {
    // Fixed-width fields contribute their width whatever they hold, but still
    // consume the binding so the expansion has no unused pattern variables.
    ($value:expr, u8) => {{
        let _ = $value;
        1
    }};
    ($value:expr, u16) => {{
        let _ = $value;
        2
    }};
    ($value:expr, u32) => {{
        let _ = $value;
        4
    }};
    ($value:expr, u64) => {{
        let _ = $value;
        8
    }};
    ($value:expr, envelope) => {{
        let _ = $value;
        OP_ENVELOPE_SIZE
    }};
    ($value:expr, str) => {
        string_size($value)?
    };
    ($value:expr, names) => {{
        if $value.len() > u16::MAX as usize {
            return Err(CodecError::TooManyNames);
        }
        let mut total = 2usize;
        for name in *$value {
            total = total
                .checked_add(string_size(name)?)
                .ok_or(CodecError::LengthOverflow)?;
        }
        total
    }};
    ($value:expr, payload) => {{
        if $value.len() > u32::MAX as usize {
            return Err(CodecError::MessageTooLarge);
        }
        checked_sum(&[4, $value.len()])?
    }};
}

macro_rules! emit_request_codec {
    ($($variant:ident, $id:ident, $tag:ident, { $($field:ident : $kind:tt),* $(,)? });* $(;)?) => {
        impl Request<'_> {
            fn type_id(&self) -> u8 {
                match self {
                    $(Request::$variant { .. } => message_type::$id,)*
                }
            }

            fn tag_is_valid(&self, tag: u16) -> bool {
                match self {
                    $(Request::$variant { .. } => emit_request_codec!(@tag $tag, tag),)*
                }
            }
        }

        /// Encode one request into `output`, returning its length in bytes.
        ///
        /// `max_msize` is the requested size for `Tversion` and the negotiated
        /// size afterward. Requests larger than it are rejected. On success,
        /// the encoded prefix is one complete frame and the rest of `output`
        /// remains untouched.
        pub fn encode_request(
            output: &mut [u8],
            max_msize: u32,
            tag: u16,
            request: Request<'_>,
        ) -> Result<usize, CodecError> {
            if !request.tag_is_valid(tag) {
                return Err(CodecError::InvalidTag);
            }

            let type_id = request.type_id();
            let mut writer = Writer::new(output);
            writer.put_u32(0)?;
            writer.put_u8(type_id)?;
            writer.put_u16(tag)?;

            match request {
                $(Request::$variant { $($field),* } => {
                    $(put_request_field!(writer, $field, $kind);)*
                })*
            }

            writer.finish(max_msize)
        }

        /// Bytes `encode_request` will write for this request, header included.
        pub fn encoded_request_size(request: &Request<'_>) -> Result<usize, CodecError> {
            let body_size = match request {
                $(Request::$variant { $($field),* } => {
                    let total = 0usize;
                    $(let total = total
                        .checked_add(request_field_size!($field, $kind))
                        .ok_or(CodecError::LengthOverflow)?;)*
                    total
                })*
            };
            body_size
                .checked_add(HEADER_SIZE)
                .ok_or(CodecError::LengthOverflow)
        }
    };
    (@tag notag, $tag:ident) => {
        $tag == NOTAG
    };
    (@tag tag, $tag:ident) => {
        $tag != NOTAG
    };
}

#[cfg(MODULE)]
use self::wire_requests::for_each_request;
#[cfg(not(MODULE))]
use crate::wire_requests::for_each_request;

for_each_request!(emit_request_codec);

/// Encode the fixed portion of an enveloped `Twrite`.
///
/// The returned bytes declare the complete frame size and payload count, but
/// deliberately omit the payload itself. A stream transport can submit this
/// prefix and the caller-owned payload as two adjacent scatter-gather
/// segments without first copying the payload into an encoded frame.
pub fn encode_twrite_prefix(
    output: &mut [u8],
    max_msize: u32,
    tag: u16,
    envelope: MutationEnvelope,
    fid: u32,
    offset: u64,
    data_length: usize,
) -> Result<usize, CodecError> {
    if tag == NOTAG {
        return Err(CodecError::InvalidTag);
    }
    if data_length > u32::MAX as usize {
        return Err(CodecError::MessageTooLarge);
    }
    let frame_size = TWRITE_OVERHEAD
        .checked_add(data_length)
        .ok_or(CodecError::LengthOverflow)?;
    if frame_size > u32::MAX as usize || frame_size > max_msize as usize {
        return Err(CodecError::MessageTooLarge);
    }

    let mut writer = Writer::new(output);
    writer.put_u32(frame_size as u32)?;
    writer.put_u8(message_type::TWRITE)?;
    writer.put_u16(tag)?;
    put_mutation_envelope(&mut writer, envelope)?;
    writer.put_u32(fid)?;
    writer.put_u64(offset)?;
    writer.put_u32(data_length as u32)?;
    Ok(writer.position)
}

/// Validate a four-byte frame prefix and return its declared size.
///
/// A socket transport can call this after reading the first four bytes, then
/// read exactly the returned size minus four into its fixed receive buffer.
pub fn decode_frame_size(prefix: &[u8], max_msize: u32) -> Result<usize, CodecError> {
    let mut reader = Reader::new(prefix);
    let size = reader.get_u32()?;
    if size < HEADER_SIZE as u32 {
        return Err(CodecError::InvalidFrameSize);
    }
    if size > max_msize {
        return Err(CodecError::MessageTooLarge);
    }
    Ok(size as usize)
}

/// Decode the seven-byte header at the start of a response frame.
///
/// The body need not be present yet. A stream receiver uses this to validate
/// the declared allocation and route the response by tag before reading the
/// remainder of the frame.
pub fn decode_header(prefix: &[u8], max_msize: u32) -> Result<Header, CodecError> {
    let declared_size = decode_frame_size(prefix, max_msize)?;
    let mut reader = Reader::new(prefix);
    let _size = reader.get_u32()?;
    let type_ = reader.get_u8()?;
    let tag = reader.get_u16()?;
    Ok(Header {
        size: declared_size as u32,
        type_,
        tag,
    })
}

/// Header common to every decoded response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub size: u32,
    pub type_: u8,
    pub tag: u16,
}

/// A borrowed, validated response frame.
#[derive(Clone, Copy, Debug)]
pub struct DecodedResponse<'a> {
    pub header: Header,
    pub body: Response<'a>,
}

/// A decoded response in the native client subset.
#[derive(Clone, Copy, Debug)]
pub enum Response<'a> {
    Rversion(Rversion<'a>),
    Rgetlineage(Rgetlineage),
    Rrebind(Rrebind),
    Rwalkgetattr(Rwalkgetattr<'a>),
    Rgetattr(Rgetattr),
    Rsetattrattr(Stat),
    Rfallocate,
    /// Standard 9P `Rlopen` (type 13), accepted for compatibility.
    Rlopen(Rlopen),
    /// ZeroFS-private response to `Tlopenat` (type 237).
    Rlopenat(Rlopen),
    Rlcreateattr(Rlcreateattr),
    Rmkdirattr(Stat),
    Rsymlinkattr(Stat),
    Rmknodattr(Stat),
    Rlinkattr(Stat),
    Rrenameat,
    Runlinkat,
    Rreadlink(Rreadlink<'a>),
    Rflush,
    Rread(Rread<'a>),
    Rwrite(Rwrite),
    Rfsync,
    Rreaddirattr(Rreaddirattr<'a>),
    Rclunk,
    Rstatfs(Rstatfs),
    Rlock(Rlock),
    Rgetlock(Rgetlock<'a>),
    Rlerror(Rlerror),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rversion<'a> {
    pub msize: u32,
    pub version: WireString<&'a [u8]>,
}

#[derive(Clone, Copy, Debug)]
pub struct Rwalkgetattr<'a> {
    pub qids: Qids<'a>,
    pub stat: Stat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rreadlink<'a> {
    pub target: WireString<&'a [u8]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rread<'a> {
    pub data: WireBytes<&'a [u8]>,
}

/// Validated `Rreaddirattr` payload.
#[derive(Clone, Copy, Debug)]
pub struct Rreaddirattr<'a> {
    data: WireBytes<&'a [u8]>,
    entry_count: usize,
}

impl<'a> Rreaddirattr<'a> {
    /// Serialized directory payload, excluding the outer `count[4]`.
    pub fn data(&self) -> &'a [u8] {
        self.data.0
    }

    /// Number of complete [`DirEntryPlus`] records in the payload.
    pub fn len(&self) -> usize {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Iterate over entries without allocating.
    pub fn entries(&self) -> DirEntryPlusIter<'a> {
        DirEntryPlusIter {
            reader: Reader::new(self.data.0),
            remaining: self.entry_count,
        }
    }
}

/// Validate a bare `Rreaddirattr.data` payload and return a borrowed view.
///
/// VFS callers use this on a request-owned reply frame, so directory actors
/// never run while holding client state.
pub fn decode_readdirattr_payload(data: &[u8]) -> Result<Rreaddirattr<'_>, CodecError> {
    let entry_count = validate_dir_entries(data)?;
    Ok(Rreaddirattr {
        data: WireBytes::from(data),
        entry_count,
    })
}

/// The lock that would conflict with a `Tgetlock` probe.
///
/// No conflict is reported as `lock_type == LOCK_TYPE_UNLCK` with an empty
/// `client_id`, so a zero-length string is the common case and not truncation.
/// `client_id` borrows the reply frame; copy what you need before releasing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rgetlock<'a> {
    pub lock_type: u8,
    pub start: u64,
    pub length: u64,
    pub proc_id: u32,
    pub client_id: WireString<&'a [u8]>,
}

/// Decode one complete response.
///
/// The decoder validates the size field, `max_msize`, tag, fixed-layout body
/// length, counted payloads, every directory entry, and trailing data.
pub fn decode_response<'a>(
    frame: &'a [u8],
    max_msize: u32,
    expected_tag: u16,
) -> Result<DecodedResponse<'a>, CodecError> {
    let declared_size = decode_frame_size(frame, max_msize)?;
    if declared_size != frame.len() {
        return Err(CodecError::FrameSizeMismatch);
    }

    let mut reader = Reader::new(frame);
    let size = reader.get_u32()?;
    let type_ = reader.get_u8()?;
    let tag = reader.get_u16()?;
    if tag != expected_tag {
        return Err(CodecError::TagMismatch {
            expected: expected_tag,
            actual: tag,
        });
    }

    let body = match type_ {
        message_type::RVERSION => {
            let msize = reader.get_u32()?;
            let version = reader.get_string()?;
            reader.require_end()?;
            Response::Rversion(Rversion {
                msize,
                version: WireString::from_storage(version),
            })
        }
        message_type::RGETLINEAGE => {
            let token = reader.get_u64()?;
            let writer_epoch = reader.get_u64()?;
            reader.require_end()?;
            Response::Rgetlineage(Rgetlineage {
                token,
                writer_epoch,
            })
        }
        message_type::RREBIND => {
            let qid = decode_qid(&mut reader)?;
            reader.require_end()?;
            Response::Rrebind(Rrebind { qid })
        }
        message_type::RWALKGETATTR => {
            let count = reader.get_u16()?;
            let qid_bytes_len = (count as usize)
                .checked_mul(QID_WIRE_SIZE)
                .ok_or(CodecError::LengthOverflow)?;
            let qid_bytes = reader.take(qid_bytes_len)?;
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rwalkgetattr(Rwalkgetattr {
                qids: Qids {
                    bytes: qid_bytes,
                    count,
                },
                stat,
            })
        }
        message_type::RGETATTR => {
            let valid = reader.get_u64()?;
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rgetattr(Rgetattr { valid, stat })
        }
        message_type::RSETATTRATTR => {
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rsetattrattr(stat)
        }
        message_type::RFALLOCATE => {
            reader.require_end()?;
            Response::Rfallocate
        }
        message_type::RLOPEN | message_type::RLOPENAT => {
            let qid = decode_qid(&mut reader)?;
            let iounit = reader.get_u32()?;
            reader.require_end()?;
            let open = Rlopen { qid, iounit };
            if type_ == message_type::RLOPEN {
                Response::Rlopen(open)
            } else {
                Response::Rlopenat(open)
            }
        }
        message_type::RLCREATEATTR => {
            let iounit = reader.get_u32()?;
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rlcreateattr(Rlcreateattr { iounit, stat })
        }
        message_type::RMKDIRATTR => {
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rmkdirattr(stat)
        }
        message_type::RSYMLINKATTR => {
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rsymlinkattr(stat)
        }
        message_type::RMKNODATTR => {
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rmknodattr(stat)
        }
        message_type::RLINKATTR => {
            let stat = decode_stat(&mut reader)?;
            reader.require_end()?;
            Response::Rlinkattr(stat)
        }
        message_type::RRENAMEAT => {
            reader.require_end()?;
            Response::Rrenameat
        }
        message_type::RUNLINKAT => {
            reader.require_end()?;
            Response::Runlinkat
        }
        message_type::RREADLINK => {
            let target = reader.get_string()?;
            reader.require_end()?;
            Response::Rreadlink(Rreadlink {
                target: WireString::from_storage(target),
            })
        }
        message_type::RFLUSH => {
            reader.require_end()?;
            Response::Rflush
        }
        message_type::RREAD => {
            let count = reader.get_u32()? as usize;
            let data = reader.take(count)?;
            reader.require_end()?;
            Response::Rread(Rread {
                data: WireBytes::from(data),
            })
        }
        message_type::RWRITE => {
            let count = reader.get_u32()?;
            reader.require_end()?;
            Response::Rwrite(Rwrite { count })
        }
        message_type::RFSYNC => {
            reader.require_end()?;
            Response::Rfsync
        }
        message_type::RREADDIRATTR => {
            let count = reader.get_u32()? as usize;
            let data = reader.take(count)?;
            reader.require_end()?;
            Response::Rreaddirattr(decode_readdirattr_payload(data)?)
        }
        message_type::RCLUNK => {
            reader.require_end()?;
            Response::Rclunk
        }
        message_type::RSTATFS => {
            let response = Rstatfs {
                r#type: reader.get_u32()?,
                bsize: reader.get_u32()?,
                blocks: reader.get_u64()?,
                bfree: reader.get_u64()?,
                bavail: reader.get_u64()?,
                files: reader.get_u64()?,
                ffree: reader.get_u64()?,
                fsid: reader.get_u64()?,
                namelen: reader.get_u32()?,
            };
            reader.require_end()?;
            Response::Rstatfs(response)
        }
        message_type::RLOCK => {
            let status = reader.get_u8()?;
            reader.require_end()?;
            Response::Rlock(Rlock { status })
        }
        message_type::RGETLOCK => {
            let lock_type = reader.get_u8()?;
            let start = reader.get_u64()?;
            let length = reader.get_u64()?;
            let proc_id = reader.get_u32()?;
            let client_id = reader.get_string()?;
            reader.require_end()?;
            Response::Rgetlock(Rgetlock {
                lock_type,
                start,
                length,
                proc_id,
                client_id: WireString::from_storage(client_id),
            })
        }
        message_type::RLERROR => {
            let ecode = reader.get_u32()?;
            reader.require_end()?;
            Response::Rlerror(Rlerror { ecode })
        }
        other => return Err(CodecError::UnexpectedMessageType(other)),
    };

    Ok(DecodedResponse {
        header: Header { size, type_, tag },
        body,
    })
}

/// Borrowed list of QIDs in `Rwalkgetattr`.
#[derive(Clone, Copy, Debug)]
pub struct Qids<'a> {
    bytes: &'a [u8],
    count: u16,
}

impl<'a> Qids<'a> {
    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Decode the QID at `index`.
    pub fn get(&self, index: usize) -> Option<Qid> {
        if index >= self.len() {
            return None;
        }
        let start = index.checked_mul(QID_WIRE_SIZE)?;
        let end = start.checked_add(QID_WIRE_SIZE)?;
        let bytes = self.bytes.get(start..end)?;
        let mut reader = Reader::new(bytes);
        decode_qid(&mut reader).ok()
    }

    pub fn iter(&self) -> QidIter<'a> {
        QidIter {
            qids: *self,
            index: 0,
        }
    }
}

/// Iterator over a borrowed [`Qids`] list.
pub struct QidIter<'a> {
    qids: Qids<'a>,
    index: usize,
}

impl Iterator for QidIter<'_> {
    type Item = Qid;

    fn next(&mut self) -> Option<Self::Item> {
        let qid = self.qids.get(self.index)?;
        self.index = self.index.saturating_add(1);
        Some(qid)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.qids.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for QidIter<'_> {}

/// One entry in an `Rreaddirattr` counted payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirEntryPlus<'a> {
    pub qid: Qid,
    /// Cookie to send in the next `Treaddirattr`.
    pub offset: u64,
    pub type_: u8,
    pub name: &'a [u8],
    pub stat: Stat,
}

/// Allocation-free iterator over validated `Rreaddirattr` entries.
pub struct DirEntryPlusIter<'a> {
    reader: Reader<'a>,
    remaining: usize,
}

impl<'a> Iterator for DirEntryPlusIter<'a> {
    type Item = Result<DirEntryPlus<'a>, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(decode_dir_entry_plus(&mut self.reader))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for DirEntryPlusIter<'_> {}

fn validate_dir_entries(data: &[u8]) -> Result<usize, CodecError> {
    let mut reader = Reader::new(data);
    let mut count = 0usize;
    while reader.remaining() != 0 {
        let _entry = decode_dir_entry_plus(&mut reader)?;
        count = count.checked_add(1).ok_or(CodecError::LengthOverflow)?;
    }
    Ok(count)
}

fn put_mutation_envelope(
    writer: &mut Writer<'_>,
    envelope: MutationEnvelope,
) -> Result<(), CodecError> {
    writer.put(&envelope.op_id)?;
    writer.put_u8(envelope.flags)?;
    writer.put_u64(envelope.origin_writer_epoch)
}

fn string_size(value: &[u8]) -> Result<usize, CodecError> {
    if value.len() > u16::MAX as usize {
        return Err(CodecError::StringTooLong);
    }
    2usize
        .checked_add(value.len())
        .ok_or(CodecError::LengthOverflow)
}

fn checked_sum(values: &[usize]) -> Result<usize, CodecError> {
    let mut total = 0usize;
    for value in values {
        total = total
            .checked_add(*value)
            .ok_or(CodecError::LengthOverflow)?;
    }
    Ok(total)
}

fn decode_dir_entry_plus<'a>(reader: &mut Reader<'a>) -> Result<DirEntryPlus<'a>, CodecError> {
    Ok(DirEntryPlus {
        qid: decode_qid(reader)?,
        offset: reader.get_u64()?,
        type_: reader.get_u8()?,
        name: reader.get_string()?,
        stat: decode_stat(reader)?,
    })
}

fn decode_qid(reader: &mut Reader<'_>) -> Result<Qid, CodecError> {
    Ok(Qid {
        type_: reader.get_u8()?,
        version: reader.get_u32()?,
        path: reader.get_u64()?,
    })
}

fn decode_stat(reader: &mut Reader<'_>) -> Result<Stat, CodecError> {
    Ok(Stat {
        qid: decode_qid(reader)?,
        mode: reader.get_u32()?,
        uid: reader.get_u32()?,
        gid: reader.get_u32()?,
        nlink: reader.get_u64()?,
        rdev: reader.get_u64()?,
        size: reader.get_u64()?,
        blksize: reader.get_u64()?,
        blocks: reader.get_u64()?,
        atime_sec: reader.get_u64()?,
        atime_nsec: reader.get_u64()?,
        mtime_sec: reader.get_u64()?,
        mtime_nsec: reader.get_u64()?,
        ctime_sec: reader.get_u64()?,
        ctime_nsec: reader.get_u64()?,
        btime_sec: reader.get_u64()?,
        btime_nsec: reader.get_u64()?,
        r#gen: reader.get_u64()?,
        data_version: reader.get_u64()?,
    })
}

struct Writer<'a> {
    output: &'a mut [u8],
    position: usize,
}

impl<'a> Writer<'a> {
    fn new(output: &'a mut [u8]) -> Self {
        Self {
            output,
            position: 0,
        }
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or(CodecError::LengthOverflow)?;
        let destination = self
            .output
            .get_mut(self.position..end)
            .ok_or(CodecError::BufferTooSmall)?;
        destination.copy_from_slice(bytes);
        self.position = end;
        Ok(())
    }

    fn put_u8(&mut self, value: u8) -> Result<(), CodecError> {
        self.put(&[value])
    }

    fn put_u16(&mut self, value: u16) -> Result<(), CodecError> {
        self.put(&value.to_le_bytes())
    }

    fn put_u32(&mut self, value: u32) -> Result<(), CodecError> {
        self.put(&value.to_le_bytes())
    }

    fn put_u64(&mut self, value: u64) -> Result<(), CodecError> {
        self.put(&value.to_le_bytes())
    }

    fn put_string(&mut self, value: &[u8]) -> Result<(), CodecError> {
        if value.len() > u16::MAX as usize {
            return Err(CodecError::StringTooLong);
        }
        self.put_u16(value.len() as u16)?;
        self.put(value)
    }

    fn finish(self, max_msize: u32) -> Result<usize, CodecError> {
        if self.position > u32::MAX as usize || self.position > max_msize as usize {
            return Err(CodecError::MessageTooLarge);
        }
        let size = (self.position as u32).to_le_bytes();
        let size_field = self.output.get_mut(..4).ok_or(CodecError::BufferTooSmall)?;
        size_field.copy_from_slice(&size);
        Ok(self.position)
    }
}

#[derive(Clone, Copy, Debug)]
struct Reader<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(count)
            .ok_or(CodecError::LengthOverflow)?;
        let bytes = self
            .input
            .get(self.position..end)
            .ok_or(CodecError::Truncated)?;
        self.position = end;
        Ok(bytes)
    }

    fn get_u8(&mut self) -> Result<u8, CodecError> {
        match self.take(1)? {
            [value] => Ok(*value),
            _ => Err(CodecError::Truncated),
        }
    }

    fn get_u16(&mut self) -> Result<u16, CodecError> {
        match self.take(2)? {
            [a, b] => Ok(u16::from_le_bytes([*a, *b])),
            _ => Err(CodecError::Truncated),
        }
    }

    fn get_u32(&mut self) -> Result<u32, CodecError> {
        match self.take(4)? {
            [a, b, c, d] => Ok(u32::from_le_bytes([*a, *b, *c, *d])),
            _ => Err(CodecError::Truncated),
        }
    }

    fn get_u64(&mut self) -> Result<u64, CodecError> {
        match self.take(8)? {
            [a, b, c, d, e, f, g, h] => Ok(u64::from_le_bytes([*a, *b, *c, *d, *e, *f, *g, *h])),
            _ => Err(CodecError::Truncated),
        }
    }

    fn get_string(&mut self) -> Result<&'a [u8], CodecError> {
        let length = self.get_u16()? as usize;
        self.take(length)
    }

    fn require_end(&self) -> Result<(), CodecError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CodecError::TrailingData)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(MODULE))]
    use std::{vec, vec::Vec};

    const TEST_MSIZE: u32 = 4096;

    fn put_header(frame: &mut [u8], type_: u8, tag: u16) {
        let size = frame.len() as u32;
        frame[0..4].copy_from_slice(&size.to_le_bytes());
        frame[4] = type_;
        frame[5..7].copy_from_slice(&tag.to_le_bytes());
    }

    fn put_qid(writer: &mut Writer<'_>, qid: Qid) {
        writer.put_u8(qid.type_).unwrap();
        writer.put_u32(qid.version).unwrap();
        writer.put_u64(qid.path).unwrap();
    }

    fn put_stat(writer: &mut Writer<'_>, stat: Stat) {
        put_qid(writer, stat.qid);
        writer.put_u32(stat.mode).unwrap();
        writer.put_u32(stat.uid).unwrap();
        writer.put_u32(stat.gid).unwrap();
        writer.put_u64(stat.nlink).unwrap();
        writer.put_u64(stat.rdev).unwrap();
        writer.put_u64(stat.size).unwrap();
        writer.put_u64(stat.blksize).unwrap();
        writer.put_u64(stat.blocks).unwrap();
        writer.put_u64(stat.atime_sec).unwrap();
        writer.put_u64(stat.atime_nsec).unwrap();
        writer.put_u64(stat.mtime_sec).unwrap();
        writer.put_u64(stat.mtime_nsec).unwrap();
        writer.put_u64(stat.ctime_sec).unwrap();
        writer.put_u64(stat.ctime_nsec).unwrap();
        writer.put_u64(stat.btime_sec).unwrap();
        writer.put_u64(stat.btime_nsec).unwrap();
        writer.put_u64(stat.r#gen).unwrap();
        writer.put_u64(stat.data_version).unwrap();
    }

    fn mutation_envelope() -> MutationEnvelope {
        MutationEnvelope {
            op_id: [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ],
            flags: OP_FLAG_RETRY,
            origin_writer_epoch: 0x0807_0605_0403_0201,
        }
    }

    fn encode_test_request(request: Request<'_>, tag: u16) -> Vec<u8> {
        let size = encoded_request_size(&request).unwrap();
        let mut frame = vec![0u8; size];
        assert_eq!(
            encode_request(&mut frame, TEST_MSIZE, tag, request),
            Ok(size)
        );
        frame
    }

    fn assert_mutation_prefix(frame: &[u8], type_: u8, tag: u16) {
        assert_eq!(frame[4], type_);
        assert_eq!(&frame[5..7], &tag.to_le_bytes());
        assert_eq!(&frame[7..23], &mutation_envelope().op_id);
        assert_eq!(frame[23], OP_FLAG_RETRY);
        assert_eq!(
            &frame[24..32],
            &mutation_envelope().origin_writer_epoch.to_le_bytes()
        );
    }

    /// Every request, with values chosen so each field kind is exercised.
    #[cfg(all(not(MODULE), feature = "owned"))]
    fn request_corpus() -> [(u16, Request<'static>); 25] {
        const ENVELOPE: MutationEnvelope = MutationEnvelope {
            op_id: [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ],
            flags: OP_FLAG_RETRY,
            origin_writer_epoch: 0x0807_0605_0403_0201,
        };
        const NAMES: &[&[u8]] = &[b"a", b"bb", b"ccc"];

        [
            (
                NOTAG,
                Request::Tversion {
                    msize: 0x0011_2233,
                    version: VERSION_9P2000L_ZEROFS,
                },
            ),
            (1, Request::Tgetlineage),
            (
                2,
                Request::Trebind {
                    fid: 0x1122_3344,
                    inode_id: 0x0102_0304_0506_0708,
                    root_inode: 0x1112_1314_1516_1718,
                    flags: REBIND_REPLAY | REBIND_OPENED,
                    uname: b"someone",
                    n_uname: 1000,
                },
            ),
            (
                3,
                Request::Twalkgetattr {
                    fid: 7,
                    newfid: 9,
                    names: NAMES,
                },
            ),
            (
                4,
                Request::Tgetattr {
                    fid: 11,
                    request_mask: GETATTR_ALL,
                },
            ),
            (
                5,
                Request::Tsetattrattr {
                    envelope: ENVELOPE,
                    fid: 13,
                    valid: SETATTR_MODE | SETATTR_CTIME,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    size: 4096,
                    atime_sec: 1,
                    atime_nsec: 2,
                    mtime_sec: 3,
                    mtime_nsec: 4,
                },
            ),
            (
                6,
                Request::Tfallocate {
                    envelope: ENVELOPE,
                    fid: 17,
                    offset: 8192,
                    length: 4096,
                    mode: FALLOC_FL_ZERO_RANGE,
                },
            ),
            (
                7,
                Request::Tlopenat {
                    fid: 19,
                    newfid: 23,
                    flags: 0o2,
                },
            ),
            (
                8,
                Request::Tlcreateattr {
                    envelope: ENVELOPE,
                    dfid: 29,
                    newfid: 31,
                    name: b"created",
                    flags: 0o101,
                    mode: 0o600,
                    gid: 1000,
                },
            ),
            (
                9,
                Request::Tmkdirattr {
                    envelope: ENVELOPE,
                    dfid: 37,
                    name: b"dir",
                    mode: 0o755,
                    gid: 1000,
                },
            ),
            (
                10,
                Request::Tsymlinkattr {
                    envelope: ENVELOPE,
                    dfid: 41,
                    name: b"link",
                    target: b"../target",
                    gid: 1000,
                },
            ),
            (
                11,
                Request::Tmknodattr {
                    envelope: ENVELOPE,
                    dfid: 43,
                    name: b"node",
                    mode: 0o020_600,
                    major: 4,
                    minor: 65,
                    gid: 1000,
                },
            ),
            (
                12,
                Request::Tlinkattr {
                    envelope: ENVELOPE,
                    dfid: 47,
                    fid: 53,
                    name: b"hardlink",
                },
            ),
            (
                13,
                Request::Trenameat {
                    envelope: ENVELOPE,
                    olddirfid: 59,
                    oldname: b"from",
                    newdirfid: 61,
                    newname: b"to",
                },
            ),
            (
                14,
                Request::Tunlinkat {
                    envelope: ENVELOPE,
                    dirfid: 67,
                    name: b"victim",
                    flags: AT_REMOVEDIR,
                },
            ),
            (15, Request::Treadlink { fid: 71 }),
            (16, Request::Tflush { oldtag: 0x1234 }),
            (
                17,
                Request::Tread {
                    fid: 73,
                    offset: 0x0102_0304_0506_0708,
                    count: 4096,
                },
            ),
            (
                18,
                Request::Twrite {
                    envelope: ENVELOPE,
                    fid: 79,
                    offset: 0x1122_3344_5566_7788,
                    data: b"payload bytes",
                },
            ),
            (
                19,
                Request::Tfsyncdur {
                    fid: 83,
                    datasync: 1,
                    token: 0xdead_beef,
                },
            ),
            (
                20,
                Request::Treaddirattr {
                    fid: 89,
                    offset: 42,
                    count: 8192,
                },
            ),
            (21, Request::Tclunk { fid: 97 }),
            (22, Request::Tstatfs { fid: 101 }),
            (
                23,
                Request::Tlock {
                    fid: 103,
                    lock_type: LOCK_TYPE_WRLCK,
                    flags: LOCK_FLAGS_BLOCK,
                    start: 1024,
                    length: 2048,
                    proc_id: 4242,
                    client_id: b"client",
                },
            ),
            (
                24,
                Request::Tgetlock {
                    fid: 107,
                    lock_type: LOCK_TYPE_RDLCK,
                    start: 0,
                    length: 0,
                    proc_id: 4243,
                    client_id: b"probe",
                },
            ),
        ]
    }

    /// Every request this codec encodes must decode in the owned Deku codec and
    /// re-encode to exactly the same bytes.
    ///
    /// The codecs are independent implementations; comparing complete frames
    /// catches differences despite their distinct field names and in-memory
    /// representations.
    #[cfg(all(not(MODULE), feature = "owned"))]
    #[test]
    fn every_request_round_trips_through_the_owned_codec() {
        use crate::protocol::P9Message;

        macro_rules! carries_envelope {
            ($($variant:ident, $id:ident, $tag:ident, { $($field:ident : $kind:tt),* $(,)? });* $(;)?) => {
                fn carries_envelope(request: &Request<'_>) -> bool {
                    match request {
                        $(Request::$variant { .. } => has_envelope!($($kind),*),)*
                    }
                }
            };
        }
        macro_rules! has_envelope {
            () => { false };
            (envelope $(, $rest:tt)*) => { true };
            ($first:tt $(, $rest:tt)*) => { has_envelope!($($rest),*) };
        }

        for_each_request!(carries_envelope);

        for (tag, request) in request_corpus() {
            let mut frame = [0u8; 512];
            let length = encode_request(&mut frame, TEST_MSIZE, tag, request).unwrap();
            let frame = &frame[..length];
            let op_id_enabled = carries_envelope(&request);

            let decoded = P9Message::from_bytes_ctx(frame, op_id_enabled)
                .expect("the owned codec must decode what this codec encodes");
            assert_eq!(decoded.type_, request.type_id(), "message type");
            assert_eq!(decoded.tag, tag, "tag");
            assert_eq!(decoded.size as usize, frame.len(), "declared size");

            let reencoded = decoded
                .to_bytes_ctx(op_id_enabled)
                .expect("a decoded message must re-encode");
            assert_eq!(
                reencoded,
                frame,
                "the two codecs disagree on the layout of type {}",
                request.type_id()
            );
        }
    }

    #[test]
    fn tversion_matches_the_9p_layout() {
        let mut output = [0u8; 64];
        let request = Request::Tversion {
            msize: TEST_MSIZE,
            version: VERSION_9P2000L_ZEROFS,
        };
        assert_eq!(encoded_request_size(&request), Ok(23));
        let length = encode_request(&mut output, TEST_MSIZE, NOTAG, request).unwrap();

        let expected = [
            23,
            0,
            0,
            0,
            message_type::TVERSION,
            0xff,
            0xff,
            0x00,
            0x10,
            0x00,
            0x00,
            10,
            0,
            b'9',
            b'P',
            b'2',
            b'0',
            b'0',
            b'0',
            b'.',
            b'L',
            b'.',
            b'Z',
        ];
        assert_eq!(&output[..length], &expected);
    }

    #[test]
    fn twrite_matches_the_canonical_mutation_layout() {
        let mut output = [0u8; TWRITE_OVERHEAD + 3];
        let envelope = MutationEnvelope {
            op_id: [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ],
            flags: OP_FLAG_RETRY,
            origin_writer_epoch: 0x0807_0605_0403_0201,
        };
        let request = Request::Twrite {
            envelope,
            fid: 0x1122_3344,
            offset: 0x0102_0304_0506_0708,
            data: b"abc",
        };
        assert_eq!(encoded_request_size(&request), Ok(TWRITE_OVERHEAD + 3));
        let length = encode_request(&mut output, TEST_MSIZE, 0x1234, request).unwrap();

        let expected = [
            51,
            0,
            0,
            0,
            message_type::TWRITE,
            0x34,
            0x12,
            0x00,
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            0x09,
            0x0a,
            0x0b,
            0x0c,
            0x0d,
            0x0e,
            0x0f,
            OP_FLAG_RETRY,
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            0x44,
            0x33,
            0x22,
            0x11,
            0x08,
            0x07,
            0x06,
            0x05,
            0x04,
            0x03,
            0x02,
            0x01,
            3,
            0,
            0,
            0,
            b'a',
            b'b',
            b'c',
        ];
        assert_eq!(length, expected.len());
        assert_eq!(output, expected);

        let mut prefix = [0u8; TWRITE_OVERHEAD];
        assert_eq!(
            encode_twrite_prefix(
                &mut prefix,
                TEST_MSIZE,
                0x1234,
                envelope,
                0x1122_3344,
                0x0102_0304_0506_0708,
                3,
            ),
            Ok(TWRITE_OVERHEAD)
        );
        assert_eq!(prefix, expected[..TWRITE_OVERHEAD]);
    }

    #[test]
    fn twrite_payload_is_bounded_by_the_enveloped_msize() {
        let maximum = max_write_payload(TEST_MSIZE) as usize;
        assert_eq!(maximum, TEST_MSIZE as usize - TWRITE_OVERHEAD);

        let payload = vec![0xa5; maximum];
        let request = Request::Twrite {
            envelope: MutationEnvelope {
                op_id: [7; OP_ID_SIZE],
                flags: 0,
                origin_writer_epoch: 42,
            },
            fid: 9,
            offset: 11,
            data: &payload,
        };
        assert_eq!(encoded_request_size(&request), Ok(TEST_MSIZE as usize));
        let mut output = vec![0; TEST_MSIZE as usize];
        assert_eq!(
            encode_request(&mut output, TEST_MSIZE, 5, request),
            Ok(TEST_MSIZE as usize)
        );
        assert_eq!(
            &output[TWRITE_OVERHEAD - 4..TWRITE_OVERHEAD],
            &(maximum as u32).to_le_bytes()
        );
        assert_eq!(&output[TWRITE_OVERHEAD..], payload.as_slice());

        let oversized = vec![0x5a; maximum + 1];
        let request = Request::Twrite {
            envelope: MutationEnvelope::default(),
            fid: 9,
            offset: 11,
            data: &oversized,
        };
        assert_eq!(encoded_request_size(&request), Ok(TEST_MSIZE as usize + 1));
        let mut output = vec![0; TEST_MSIZE as usize + 1];
        assert_eq!(
            encode_request(&mut output, TEST_MSIZE, 5, request),
            Err(CodecError::MessageTooLarge)
        );
    }

    #[test]
    fn setattrattr_and_fallocate_match_canonical_mutation_layouts() {
        let setattr = encode_test_request(
            Request::Tsetattrattr {
                envelope: mutation_envelope(),
                fid: 0x1122_3344,
                valid: SETATTR_MODE | SETATTR_SIZE | SETATTR_MTIME | SETATTR_MTIME_SET,
                mode: 0o100640,
                uid: 1000,
                gid: 1001,
                size: 0x0102_0304_0506_0708,
                atime_sec: 11,
                atime_nsec: 12,
                mtime_sec: 13,
                mtime_nsec: 14,
            },
            0x1234,
        );
        assert_eq!(setattr.len(), 92);
        assert_mutation_prefix(&setattr, message_type::TSETATTRATTR, 0x1234);
        let mut body = Reader::new(&setattr[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(0x1122_3344));
        assert_eq!(
            body.get_u32(),
            Ok(SETATTR_MODE | SETATTR_SIZE | SETATTR_MTIME | SETATTR_MTIME_SET)
        );
        assert_eq!(body.get_u32(), Ok(0o100640));
        assert_eq!(body.get_u32(), Ok(1000));
        assert_eq!(body.get_u32(), Ok(1001));
        assert_eq!(body.get_u64(), Ok(0x0102_0304_0506_0708));
        assert_eq!(body.get_u64(), Ok(11));
        assert_eq!(body.get_u64(), Ok(12));
        assert_eq!(body.get_u64(), Ok(13));
        assert_eq!(body.get_u64(), Ok(14));
        assert_eq!(body.require_end(), Ok(()));

        let fallocate = encode_test_request(
            Request::Tfallocate {
                envelope: mutation_envelope(),
                fid: 9,
                offset: 10,
                length: 11,
                mode: FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            },
            7,
        );
        assert_eq!(fallocate.len(), 56);
        assert_mutation_prefix(&fallocate, message_type::TFALLOCATE, 7);
        let mut body = Reader::new(&fallocate[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(9));
        assert_eq!(body.get_u64(), Ok(10));
        assert_eq!(body.get_u64(), Ok(11));
        assert_eq!(
            body.get_u32(),
            Ok(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)
        );
        assert_eq!(body.require_end(), Ok(()));
    }

    #[test]
    fn namespace_requests_match_canonical_compound_layouts() {
        let tag = 23;

        let create = encode_test_request(
            Request::Tlcreateattr {
                envelope: mutation_envelope(),
                dfid: 1,
                newfid: 2,
                name: b"f",
                flags: 3,
                mode: 0o100640,
                gid: 4,
            },
            tag,
        );
        assert_eq!(create.len(), 55);
        assert_mutation_prefix(&create, message_type::TLCREATEATTR, tag);
        let mut body = Reader::new(&create[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(1));
        assert_eq!(body.get_u32(), Ok(2));
        assert_eq!(body.get_string(), Ok(b"f".as_slice()));
        assert_eq!(body.get_u32(), Ok(3));
        assert_eq!(body.get_u32(), Ok(0o100640));
        assert_eq!(body.get_u32(), Ok(4));
        assert_eq!(body.require_end(), Ok(()));

        let mkdir = encode_test_request(
            Request::Tmkdirattr {
                envelope: mutation_envelope(),
                dfid: 5,
                name: b"d",
                mode: 0o40750,
                gid: 6,
            },
            tag,
        );
        assert_eq!(mkdir.len(), 47);
        assert_mutation_prefix(&mkdir, message_type::TMKDIRATTR, tag);
        let mut body = Reader::new(&mkdir[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(5));
        assert_eq!(body.get_string(), Ok(b"d".as_slice()));
        assert_eq!(body.get_u32(), Ok(0o40750));
        assert_eq!(body.get_u32(), Ok(6));
        assert_eq!(body.require_end(), Ok(()));

        let symlink = encode_test_request(
            Request::Tsymlinkattr {
                envelope: mutation_envelope(),
                dfid: 7,
                name: b"s",
                target: b"to",
                gid: 8,
            },
            tag,
        );
        assert_eq!(symlink.len(), 47);
        assert_mutation_prefix(&symlink, message_type::TSYMLINKATTR, tag);
        let mut body = Reader::new(&symlink[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(7));
        assert_eq!(body.get_string(), Ok(b"s".as_slice()));
        assert_eq!(body.get_string(), Ok(b"to".as_slice()));
        assert_eq!(body.get_u32(), Ok(8));
        assert_eq!(body.require_end(), Ok(()));

        let mknod = encode_test_request(
            Request::Tmknodattr {
                envelope: mutation_envelope(),
                dfid: 9,
                name: b"p",
                mode: 0o10640,
                major: 10,
                minor: 11,
                gid: 12,
            },
            tag,
        );
        assert_eq!(mknod.len(), 55);
        assert_mutation_prefix(&mknod, message_type::TMKNODATTR, tag);
        let mut body = Reader::new(&mknod[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(9));
        assert_eq!(body.get_string(), Ok(b"p".as_slice()));
        assert_eq!(body.get_u32(), Ok(0o10640));
        assert_eq!(body.get_u32(), Ok(10));
        assert_eq!(body.get_u32(), Ok(11));
        assert_eq!(body.get_u32(), Ok(12));
        assert_eq!(body.require_end(), Ok(()));

        let link = encode_test_request(
            Request::Tlinkattr {
                envelope: mutation_envelope(),
                dfid: 13,
                fid: 14,
                name: b"h",
            },
            tag,
        );
        assert_eq!(link.len(), 43);
        assert_mutation_prefix(&link, message_type::TLINKATTR, tag);
        let mut body = Reader::new(&link[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(13));
        assert_eq!(body.get_u32(), Ok(14));
        assert_eq!(body.get_string(), Ok(b"h".as_slice()));
        assert_eq!(body.require_end(), Ok(()));

        let rename = encode_test_request(
            Request::Trenameat {
                envelope: mutation_envelope(),
                olddirfid: 15,
                oldname: b"a",
                newdirfid: 16,
                newname: b"b",
            },
            tag,
        );
        assert_eq!(rename.len(), 46);
        assert_mutation_prefix(&rename, message_type::TRENAMEAT, tag);
        let mut body = Reader::new(&rename[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(15));
        assert_eq!(body.get_string(), Ok(b"a".as_slice()));
        assert_eq!(body.get_u32(), Ok(16));
        assert_eq!(body.get_string(), Ok(b"b".as_slice()));
        assert_eq!(body.require_end(), Ok(()));

        let unlink = encode_test_request(
            Request::Tunlinkat {
                envelope: mutation_envelope(),
                dirfid: 17,
                name: b"x",
                flags: AT_REMOVEDIR,
            },
            tag,
        );
        assert_eq!(unlink.len(), 43);
        assert_mutation_prefix(&unlink, message_type::TUNLINKAT, tag);
        let mut body = Reader::new(&unlink[HEADER_SIZE + OP_ENVELOPE_SIZE..]);
        assert_eq!(body.get_u32(), Ok(17));
        assert_eq!(body.get_string(), Ok(b"x".as_slice()));
        assert_eq!(body.get_u32(), Ok(AT_REMOVEDIR));
        assert_eq!(body.require_end(), Ok(()));

        let readlink = encode_test_request(Request::Treadlink { fid: 18 }, tag);
        assert_eq!(readlink.len(), HEADER_SIZE + 4);
        assert_eq!(readlink[4], message_type::TREADLINK);
        assert_eq!(&readlink[HEADER_SIZE..], &18u32.to_le_bytes());
    }

    #[test]
    fn fixed_and_variable_requests_have_exact_sizes() {
        let mut output = [0u8; 128];

        let walk = Request::Twalkgetattr {
            fid: 1,
            newfid: 2,
            names: &[b"dir", b"file"],
        };
        let walk_size = HEADER_SIZE + 4 + 4 + 2 + 2 + 3 + 2 + 4;
        assert_eq!(encoded_request_size(&walk), Ok(walk_size));
        let length = encode_request(&mut output, TEST_MSIZE, 5, walk).unwrap();
        assert_eq!(length, walk_size);
        assert_eq!(output[4], message_type::TWALKGETATTR);
        assert_eq!(&output[7..11], &1u32.to_le_bytes());
        assert_eq!(&output[11..15], &2u32.to_le_bytes());
        assert_eq!(&output[15..17], &2u16.to_le_bytes());
        assert_eq!(&output[17..22], &[3, 0, b'd', b'i', b'r']);
        assert_eq!(&output[22..28], &[4, 0, b'f', b'i', b'l', b'e']);

        let rebind = Request::Trebind {
            fid: 3,
            inode_id: 4,
            root_inode: 5,
            flags: 1,
            uname: b"u",
            n_uname: 1000,
        };
        let rebind_size = HEADER_SIZE + 4 + 8 + 8 + 1 + 2 + 1 + 4;
        assert_eq!(encoded_request_size(&rebind), Ok(rebind_size));
        let length = encode_request(&mut output, TEST_MSIZE, 6, rebind).unwrap();
        assert_eq!(length, rebind_size);
        assert_eq!(output[4], message_type::TREBIND);

        let cases = [
            (Request::Tgetlineage, message_type::TGETLINEAGE, HEADER_SIZE),
            (
                Request::Tgetattr {
                    fid: 1,
                    request_mask: 2,
                },
                message_type::TGETATTR,
                HEADER_SIZE + 12,
            ),
            (
                Request::Tlopenat {
                    fid: 1,
                    newfid: 2,
                    flags: 3,
                },
                message_type::TLOPENAT,
                HEADER_SIZE + 12,
            ),
            (
                Request::Tread {
                    fid: 1,
                    offset: 2,
                    count: 3,
                },
                message_type::TREAD,
                HEADER_SIZE + 16,
            ),
            (
                Request::Tfsyncdur {
                    fid: 1,
                    datasync: 1,
                    token: 2,
                },
                message_type::TFSYNCDUR,
                HEADER_SIZE + 16,
            ),
            (
                Request::Treaddirattr {
                    fid: 1,
                    offset: 2,
                    count: 3,
                },
                message_type::TREADDIRATTR,
                HEADER_SIZE + 16,
            ),
            (
                Request::Tclunk { fid: 1 },
                message_type::TCLUNK,
                HEADER_SIZE + 4,
            ),
            (
                Request::Tstatfs { fid: 1 },
                message_type::TSTATFS,
                HEADER_SIZE + 4,
            ),
        ];
        for (request, type_, expected_length) in cases {
            assert_eq!(encoded_request_size(&request), Ok(expected_length));
            let length = encode_request(&mut output, TEST_MSIZE, 7, request).unwrap();
            assert_eq!(length, expected_length);
            assert_eq!(output[4], type_);
        }
    }

    #[test]
    fn flush_matches_the_standard_9p_layout() {
        let tag = 0x1234;
        let oldtag = 0xabcd;
        let request = Request::Tflush { oldtag };
        assert_eq!(encoded_request_size(&request), Ok(HEADER_SIZE + 2));

        let frame = encode_test_request(request, tag);
        assert_eq!(frame.len(), HEADER_SIZE + 2);
        assert_eq!(frame[4], message_type::TFLUSH);
        assert_eq!(&frame[5..7], &tag.to_le_bytes());
        assert_eq!(&frame[HEADER_SIZE..], &oldtag.to_le_bytes());

        let mut response = [0u8; HEADER_SIZE];
        put_header(&mut response, message_type::RFLUSH, tag);
        assert!(matches!(
            decode_response(&response, TEST_MSIZE, tag).unwrap().body,
            Response::Rflush
        ));

        let mut trailing = [0u8; HEADER_SIZE + 1];
        put_header(&mut trailing, message_type::RFLUSH, tag);
        assert!(matches!(
            decode_response(&trailing, TEST_MSIZE, tag),
            Err(CodecError::TrailingData)
        ));
    }

    #[test]
    fn lock_requests_match_the_9p_layout() {
        // These values are also spelled as Deku literals in the owned codec,
        // so pin them: a silent divergence would be a protocol split.
        assert_eq!(
            [LOCK_TYPE_RDLCK, LOCK_TYPE_WRLCK, LOCK_TYPE_UNLCK],
            [0, 1, 2]
        );
        assert_eq!(
            [LOCK_SUCCESS, LOCK_BLOCKED, LOCK_ERROR, LOCK_GRACE],
            [0, 1, 2, 3]
        );
        assert_eq!(LOCK_FLAGS_BLOCK, 1);

        let lock = Request::Tlock {
            fid: 0x1122_3344,
            lock_type: LOCK_TYPE_WRLCK,
            flags: LOCK_FLAGS_BLOCK,
            start: 0x0102_0304_0506_0708,
            length: 4096,
            proc_id: 7,
            client_id: b"id",
        };
        assert_eq!(encoded_request_size(&lock), Ok(HEADER_SIZE + 31 + 2));
        let frame = encode_test_request(lock, 0x1234);
        let expected = [
            40,
            0,
            0,
            0,
            message_type::TLOCK,
            0x34,
            0x12,
            0x44,
            0x33,
            0x22,
            0x11,
            1,
            1,
            0,
            0,
            0,
            0x08,
            0x07,
            0x06,
            0x05,
            0x04,
            0x03,
            0x02,
            0x01,
            0x00,
            0x10,
            0,
            0,
            0,
            0,
            0,
            0,
            7,
            0,
            0,
            0,
            2,
            0,
            b'i',
            b'd',
        ];
        assert_eq!(frame.as_slice(), expected.as_slice());

        let getlock = Request::Tgetlock {
            fid: 9,
            lock_type: LOCK_TYPE_RDLCK,
            start: 10,
            length: 11,
            proc_id: 12,
            client_id: b"peer",
        };
        assert_eq!(encoded_request_size(&getlock), Ok(HEADER_SIZE + 27 + 4));
        let frame = encode_test_request(getlock, 5);
        assert_eq!(frame.len(), HEADER_SIZE + 27 + 4);
        assert_eq!(frame[4], message_type::TGETLOCK);
        assert_eq!(&frame[5..7], &5u16.to_le_bytes());
        let mut body = Reader::new(&frame[HEADER_SIZE..]);
        assert_eq!(body.get_u32(), Ok(9));
        assert_eq!(body.get_u8(), Ok(LOCK_TYPE_RDLCK));
        assert_eq!(body.get_u64(), Ok(10));
        assert_eq!(body.get_u64(), Ok(11));
        assert_eq!(body.get_u32(), Ok(12));
        assert_eq!(body.get_string(), Ok(b"peer".as_slice()));
        assert_eq!(body.require_end(), Ok(()));

        // An unnamed owner is legal, and the reserved size must still be exact.
        let anonymous = Request::Tgetlock {
            fid: 9,
            lock_type: LOCK_TYPE_UNLCK,
            start: 0,
            length: 0,
            proc_id: 0,
            client_id: b"",
        };
        assert_eq!(encoded_request_size(&anonymous), Ok(HEADER_SIZE + 27));
        assert_eq!(encode_test_request(anonymous, 5).len(), HEADER_SIZE + 27);
    }

    /// The cases carried over verbatim from `lock_range.rs`, which is the copy
    /// userspace uses. Nothing else keeps the two in step.
    #[test]
    fn lock_range_subtraction_matches_the_owned_codec() {
        assert_eq!(lock_range_end(10, 0), u64::MAX);
        assert_eq!(lock_range_end(10, 5), 15);
        assert_eq!(lock_range_end(u64::MAX, 10), u64::MAX);

        assert_eq!(
            subtract_lock_range(10, 90, 30, 20),
            [Some((10, 20)), Some((50, 50))]
        );
        // An open-ended survivor is re-encoded as EOF, not as a bounded tail.
        assert_eq!(
            subtract_lock_range(10, 0, 30, 20),
            [Some((10, 20)), Some((50, 0))]
        );
        assert_eq!(subtract_lock_range(10, 0, 30, 0), [Some((10, 20)), None]);
        // Disjoint ranges leave the held record exactly as it was.
        assert_eq!(subtract_lock_range(10, 20, 40, 10), [Some((10, 20)), None]);
        assert_eq!(subtract_lock_range(10, 20, 0, 40), [None, None]);
    }

    #[test]
    fn rebind_binary_credentials_are_encoded_verbatim() {
        let credentials = [
            REBIND_CREDENTIAL_SENTINEL,
            REBIND_CREDENTIAL_VERSION,
            0xd2,
            0x04,
            0,
            0,
            2,
            0x2e,
            0x16,
            0,
            0,
            0x34,
            0x12,
            0,
            0,
        ];
        let request = Request::Trebind {
            fid: 3,
            inode_id: 4,
            root_inode: 0,
            flags: 0,
            uname: &credentials,
            n_uname: 1000,
        };
        let mut output = [0u8; 128];
        let length = encode_request(&mut output, TEST_MSIZE, 6, request).unwrap();
        assert_eq!(
            length,
            HEADER_SIZE + 4 + 8 + 8 + 1 + 2 + credentials.len() + 4
        );

        let mut body = Reader::new(&output[HEADER_SIZE..length]);
        assert_eq!(body.get_u32(), Ok(3));
        assert_eq!(body.get_u64(), Ok(4));
        assert_eq!(body.get_u64(), Ok(0));
        assert_eq!(body.get_u8(), Ok(0));
        assert_eq!(body.get_string(), Ok(credentials.as_slice()));
        assert_eq!(body.get_u32(), Ok(1000));
        assert_eq!(body.require_end(), Ok(()));
    }

    #[test]
    fn header_can_be_routed_before_the_body_arrives() {
        let header = [11, 0, 0, 0, message_type::RLERROR, 0x34, 0x12];
        assert_eq!(
            decode_header(&header, TEST_MSIZE),
            Ok(Header {
                size: 11,
                type_: message_type::RLERROR,
                tag: 0x1234,
            })
        );
        assert_eq!(
            decode_header(&header[..HEADER_SIZE - 1], TEST_MSIZE),
            Err(CodecError::Truncated)
        );
    }

    #[test]
    fn errors_are_bounded_and_tags_are_checked() {
        let mut tiny = [0u8; HEADER_SIZE];
        assert_eq!(
            encode_request(
                &mut tiny,
                TEST_MSIZE,
                1,
                Request::Tread {
                    fid: 0,
                    offset: 0,
                    count: 0,
                },
            ),
            Err(CodecError::BufferTooSmall)
        );
        assert_eq!(
            encode_request(
                &mut [0u8; 64],
                TEST_MSIZE,
                1,
                Request::Tversion {
                    msize: TEST_MSIZE,
                    version: VERSION_9P2000L_ZEROFS,
                },
            ),
            Err(CodecError::InvalidTag)
        );
        assert_eq!(
            encode_request(
                &mut [0u8; 64],
                TEST_MSIZE,
                NOTAG,
                Request::Tclunk { fid: 1 },
            ),
            Err(CodecError::InvalidTag)
        );
    }

    #[test]
    fn decodes_version_error_read_and_open_replies() {
        let mut version = [0u8; HEADER_SIZE + 4 + 2 + 10];
        put_header(&mut version, message_type::RVERSION, NOTAG);
        version[7..11].copy_from_slice(&TEST_MSIZE.to_le_bytes());
        version[11..13].copy_from_slice(&10u16.to_le_bytes());
        version[13..].copy_from_slice(VERSION_9P2000L_ZEROFS);
        let decoded = decode_response(&version, TEST_MSIZE, NOTAG).unwrap();
        match decoded.body {
            Response::Rversion(reply) => {
                assert_eq!(reply.msize, TEST_MSIZE);
                assert_eq!(reply.version.as_ref(), VERSION_9P2000L_ZEROFS);
            }
            _ => panic!("wrong response"),
        }

        let mut error = [0u8; HEADER_SIZE + 4];
        put_header(&mut error, message_type::RLERROR, 9);
        error[7..].copy_from_slice(&5u32.to_le_bytes());
        match decode_response(&error, TEST_MSIZE, 9).unwrap().body {
            Response::Rlerror(reply) => assert_eq!(reply.ecode, 5),
            _ => panic!("wrong response"),
        }

        let mut read = [0u8; HEADER_SIZE + 4 + 3];
        put_header(&mut read, message_type::RREAD, 10);
        read[7..11].copy_from_slice(&3u32.to_le_bytes());
        read[11..].copy_from_slice(b"abc");
        match decode_response(&read, TEST_MSIZE, 10).unwrap().body {
            Response::Rread(reply) => assert_eq!(reply.data.as_ref(), b"abc"),
            _ => panic!("wrong response"),
        }

        let mut open = [0u8; HEADER_SIZE + QID_WIRE_SIZE + 4];
        put_header(&mut open, message_type::RLOPENAT, 11);
        let mut writer = Writer::new(&mut open[HEADER_SIZE..]);
        put_qid(
            &mut writer,
            Qid {
                type_: 0,
                version: 2,
                path: 3,
            },
        );
        writer.put_u32(8192).unwrap();
        match decode_response(&open, TEST_MSIZE, 11).unwrap().body {
            Response::Rlopenat(reply) => {
                assert_eq!(reply.qid.path, 3);
                assert_eq!(reply.iounit, 8192);
            }
            _ => panic!("wrong response"),
        }
    }

    #[test]
    fn decodes_compound_attribute_namespace_and_readlink_replies() {
        let stat = Stat {
            qid: Qid {
                type_: QID_TYPE_FILE,
                version: 3,
                path: 0x1122_3344_5566_7788,
            },
            mode: 0o100640,
            uid: 1000,
            gid: 1001,
            nlink: 2,
            size: 123,
            mtime_sec: 456,
            data_version: 789,
            ..Stat::default()
        };

        let mut create = [0u8; HEADER_SIZE + 4 + STAT_WIRE_SIZE];
        put_header(&mut create, message_type::RLCREATEATTR, 31);
        let mut writer = Writer::new(&mut create[HEADER_SIZE..]);
        writer.put_u32(8192).unwrap();
        put_stat(&mut writer, stat);
        match decode_response(&create, TEST_MSIZE, 31).unwrap().body {
            Response::Rlcreateattr(reply) => {
                assert_eq!(reply.iounit, 8192);
                assert_eq!(reply.stat, stat);
            }
            _ => panic!("wrong create response"),
        }

        for (type_, expected) in [
            (message_type::RMKDIRATTR, 0u8),
            (message_type::RSYMLINKATTR, 1u8),
            (message_type::RMKNODATTR, 2u8),
            (message_type::RLINKATTR, 3u8),
            (message_type::RSETATTRATTR, 4u8),
        ] {
            let mut frame = [0u8; HEADER_SIZE + STAT_WIRE_SIZE];
            put_header(&mut frame, type_, 32);
            put_stat(&mut Writer::new(&mut frame[HEADER_SIZE..]), stat);
            let decoded = decode_response(&frame, TEST_MSIZE, 32).unwrap();
            let decoded_stat = match (expected, decoded.body) {
                (0, Response::Rmkdirattr(value))
                | (1, Response::Rsymlinkattr(value))
                | (2, Response::Rmknodattr(value))
                | (3, Response::Rlinkattr(value))
                | (4, Response::Rsetattrattr(value)) => value,
                _ => panic!("wrong stat response"),
            };
            assert_eq!(decoded_stat, stat);
        }

        for (type_, expected) in [
            (message_type::RFALLOCATE, 0u8),
            (message_type::RRENAMEAT, 1u8),
            (message_type::RUNLINKAT, 2u8),
        ] {
            let mut frame = [0u8; HEADER_SIZE];
            put_header(&mut frame, type_, 33);
            let body = decode_response(&frame, TEST_MSIZE, 33).unwrap().body;
            assert!(matches!(
                (expected, body),
                (0, Response::Rfallocate) | (1, Response::Rrenameat) | (2, Response::Runlinkat)
            ));
        }

        let mut readlink = [0u8; HEADER_SIZE + 2 + 6];
        put_header(&mut readlink, message_type::RREADLINK, 34);
        readlink[HEADER_SIZE..HEADER_SIZE + 2].copy_from_slice(&6u16.to_le_bytes());
        readlink[HEADER_SIZE + 2..].copy_from_slice(b"target");
        match decode_response(&readlink, TEST_MSIZE, 34).unwrap().body {
            Response::Rreadlink(reply) => assert_eq!(reply.target.as_ref(), b"target"),
            _ => panic!("wrong readlink response"),
        }

        let truncated = &readlink[..readlink.len() - 1];
        assert!(matches!(
            decode_response(truncated, TEST_MSIZE, 34),
            Err(CodecError::FrameSizeMismatch)
        ));
    }

    #[test]
    fn tfsyncdur_and_write_replies_match_the_canonical_layout() {
        let mut request = [0u8; HEADER_SIZE + 4 + 4 + 8];
        let length = encode_request(
            &mut request,
            TEST_MSIZE,
            0x1234,
            Request::Tfsyncdur {
                fid: 0x1122_3344,
                datasync: 1,
                token: 0x0807_0605_0403_0201,
            },
        )
        .unwrap();
        let expected = [
            23,
            0,
            0,
            0,
            message_type::TFSYNCDUR,
            0x34,
            0x12,
            0x44,
            0x33,
            0x22,
            0x11,
            1,
            0,
            0,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
        ];
        assert_eq!(length, expected.len());
        assert_eq!(request, expected);

        let mut write = [0u8; HEADER_SIZE + 4];
        put_header(&mut write, message_type::RWRITE, 7);
        write[HEADER_SIZE..].copy_from_slice(&123u32.to_le_bytes());
        match decode_response(&write, TEST_MSIZE, 7).unwrap().body {
            Response::Rwrite(response) => assert_eq!(response.count, 123),
            _ => panic!("wrong response"),
        }

        let mut fsync = [0u8; HEADER_SIZE];
        put_header(&mut fsync, message_type::RFSYNC, 8);
        assert!(matches!(
            decode_response(&fsync, TEST_MSIZE, 8).unwrap().body,
            Response::Rfsync
        ));

        let truncated = &write[..write.len() - 1];
        assert!(matches!(
            decode_response(truncated, TEST_MSIZE, 7),
            Err(CodecError::FrameSizeMismatch)
        ));
    }

    #[test]
    fn decodes_lock_replies_and_rejects_truncation() {
        for status in [LOCK_SUCCESS, LOCK_BLOCKED] {
            let mut frame = [0u8; HEADER_SIZE + 1];
            put_header(&mut frame, message_type::RLOCK, 21);
            frame[HEADER_SIZE] = status;
            match decode_response(&frame, TEST_MSIZE, 21).unwrap().body {
                Response::Rlock(reply) => assert_eq!(reply.status, status),
                _ => panic!("wrong lock response"),
            }
        }

        let mut getlock = [0u8; HEADER_SIZE + 1 + 8 + 8 + 4 + 2 + 4];
        put_header(&mut getlock, message_type::RGETLOCK, 22);
        let mut writer = Writer::new(&mut getlock[HEADER_SIZE..]);
        writer.put_u8(LOCK_TYPE_WRLCK).unwrap();
        writer.put_u64(64).unwrap();
        writer.put_u64(128).unwrap();
        writer.put_u32(4242).unwrap();
        writer.put_string(b"peer").unwrap();
        match decode_response(&getlock, TEST_MSIZE, 22).unwrap().body {
            Response::Rgetlock(reply) => {
                assert_eq!(reply.lock_type, LOCK_TYPE_WRLCK);
                assert_eq!(reply.start, 64);
                assert_eq!(reply.length, 128);
                assert_eq!(reply.proc_id, 4242);
                assert_eq!(reply.client_id.as_ref(), b"peer");
                // The owner identity borrows the frame instead of copying it.
                assert!(core::ptr::eq(
                    reply.client_id.as_ref().as_ptr(),
                    getlock[HEADER_SIZE + 23..].as_ptr(),
                ));
            }
            _ => panic!("wrong getlock response"),
        }

        let mut headerless = [0u8; HEADER_SIZE];
        put_header(&mut headerless, message_type::RLOCK, 23);
        assert!(matches!(
            decode_response(&headerless, TEST_MSIZE, 23),
            Err(CodecError::Truncated)
        ));

        // The declared frame size matches, so only the string length lies.
        let mut overrun = [0u8; HEADER_SIZE + 1 + 8 + 8 + 4 + 2];
        put_header(&mut overrun, message_type::RGETLOCK, 24);
        let mut writer = Writer::new(&mut overrun[HEADER_SIZE..]);
        writer.put_u8(LOCK_TYPE_RDLCK).unwrap();
        writer.put_u64(0).unwrap();
        writer.put_u64(0).unwrap();
        writer.put_u32(0).unwrap();
        writer.put_u16(3).unwrap();
        assert!(matches!(
            decode_response(&overrun, TEST_MSIZE, 24),
            Err(CodecError::Truncated)
        ));

        let mut trailing = [0u8; HEADER_SIZE + 2];
        put_header(&mut trailing, message_type::RLOCK, 25);
        assert!(matches!(
            decode_response(&trailing, TEST_MSIZE, 25),
            Err(CodecError::TrailingData)
        ));
    }

    #[test]
    fn walk_and_readdir_views_are_allocation_free() {
        let stat = Stat {
            qid: Qid {
                type_: 0x80,
                version: 7,
                path: 8,
            },
            mode: 0o40755,
            uid: 1000,
            gid: 1001,
            nlink: 2,
            size: 99,
            data_version: 42,
            ..Stat::default()
        };
        let qid = Qid {
            type_: 0,
            version: 3,
            path: 4,
        };

        let mut walk = [0u8; HEADER_SIZE + 2 + QID_WIRE_SIZE + STAT_WIRE_SIZE];
        put_header(&mut walk, message_type::RWALKGETATTR, 12);
        let mut writer = Writer::new(&mut walk[HEADER_SIZE..]);
        writer.put_u16(1).unwrap();
        put_qid(&mut writer, qid);
        put_stat(&mut writer, stat);
        match decode_response(&walk, TEST_MSIZE, 12).unwrap().body {
            Response::Rwalkgetattr(reply) => {
                assert_eq!(reply.qids.len(), 1);
                assert_eq!(reply.qids.get(0), Some(qid));
                assert_eq!(reply.qids.get(1), None);
                assert_eq!(reply.stat, stat);
            }
            _ => panic!("wrong response"),
        }

        let entry_size = QID_WIRE_SIZE + 8 + 1 + 2 + 4 + STAT_WIRE_SIZE;
        let mut readdir = [0u8; HEADER_SIZE + 4 + QID_WIRE_SIZE + 8 + 1 + 2 + 4 + STAT_WIRE_SIZE];
        put_header(&mut readdir, message_type::RREADDIRATTR, 13);
        readdir[7..11].copy_from_slice(&(entry_size as u32).to_le_bytes());
        let mut writer = Writer::new(&mut readdir[11..]);
        put_qid(&mut writer, qid);
        writer.put_u64(55).unwrap();
        writer.put_u8(0).unwrap();
        writer.put_string(b"name").unwrap();
        put_stat(&mut writer, stat);
        match decode_response(&readdir, TEST_MSIZE, 13).unwrap().body {
            Response::Rreaddirattr(reply) => {
                assert_eq!(reply.len(), 1);
                let entries = reply.entries().collect::<Result<Vec<_>, _>>().unwrap();
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].qid, qid);
                assert_eq!(entries[0].offset, 55);
                assert_eq!(entries[0].name, b"name");
                assert_eq!(entries[0].stat, stat);
            }
            _ => panic!("wrong response"),
        }
    }

    #[test]
    fn rejects_count_size_tag_and_trailing_data_violations() {
        let mut truncated = [0u8; HEADER_SIZE + 4 + 2];
        put_header(&mut truncated, message_type::RREAD, 1);
        truncated[7..11].copy_from_slice(&3u32.to_le_bytes());
        assert!(matches!(
            decode_response(&truncated, TEST_MSIZE, 1),
            Err(CodecError::Truncated)
        ));

        let mut trailing = [0u8; HEADER_SIZE + 1];
        put_header(&mut trailing, message_type::RCLUNK, 2);
        assert!(matches!(
            decode_response(&trailing, TEST_MSIZE, 2),
            Err(CodecError::TrailingData)
        ));
        assert!(matches!(
            decode_response(&trailing, TEST_MSIZE, 3),
            Err(CodecError::TagMismatch {
                expected: 3,
                actual: 2,
            })
        ));

        let mut mismatch = trailing;
        mismatch[0..4].copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
        assert!(matches!(
            decode_response(&mismatch, TEST_MSIZE, 2),
            Err(CodecError::FrameSizeMismatch)
        ));
    }
}
