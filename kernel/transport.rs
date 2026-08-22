//! Blocking stream-socket transport for the ZeroFS kernel client.
//!
//! Each session serializes complete writes and gives its connection task sole
//! ownership of reads. Linux sockets permit the two directions to proceed
//! concurrently; shutdown may race either direction to terminate the session.

use core::{
    ffi::c_void,
    marker::PhantomData,
    mem::{MaybeUninit, size_of},
    ptr::{self, NonNull},
};

use kernel::{
    bindings,
    error::code::{EINVAL, EIO},
    error::to_result,
    ffi,
    iov::IovIterDest,
    prelude::*,
    types::ScopeGuard,
};

const IPPROTO_TCP: ffi::c_int = 6;
const TCP_NODELAY: ffi::c_int = 1;
const UNIX_PATH_MAX: usize = 108;
const SOCKADDR_UN_PATH_OFFSET: usize = size_of::<u16>();

/// A stream-send failure together with whether this frame entered the stream.
///
/// A local interruption before the first byte is harmless: the request still
/// owns no server state and its tag can be released. Once any byte was accepted,
/// the stream framing is ambiguous and the connection has to be retired.
pub(crate) struct SendFailure {
    error: Error,
    started: bool,
}

impl SendFailure {
    pub(crate) fn error(&self) -> Error {
        self.error
    }

    pub(crate) fn started(&self) -> bool {
        self.started
    }
}

pub(crate) type SendResult = core::result::Result<(), SendFailure>;

#[allow(improper_ctypes)]
unsafe extern "C" {
    fn sock_setsockopt(
        socket: *mut bindings::socket,
        level: ffi::c_int,
        option: ffi::c_int,
        value: bindings::sockptr_t,
        length: ffi::c_uint,
    ) -> ffi::c_int;
}

/// Linux's IPv4 `struct sockaddr_in`.
///
/// The generated Rust bindings do not expose this UAPI type, so keep the
/// definition local and verify its layout at compile time below.
#[repr(C)]
#[allow(dead_code)]
struct SockAddrIn {
    family: u16,
    port: u16,
    address: u32,
    zero: [u8; 8],
}

const _: () = assert!(size_of::<SockAddrIn>() == 16);

/// Linux's `struct sockaddr_un` in `sockaddr_storage`-sized backing memory.
///
/// The generated Rust kernel bindings omit this UAPI type. Linux fixes
/// `sun_path` at 108 bytes on every supported architecture. The AF_UNIX
/// implementation may append a defensive NUL just beyond a maximum-length
/// `sun_path`, relying on callers to provide `sockaddr_storage`; retain that
/// extra backing memory even though `kernel_connect` receives the shorter
/// logical address length.
#[repr(C, align(8))]
#[allow(dead_code)]
struct SockAddrUn {
    family: u16,
    path: [u8; UNIX_PATH_MAX],
    padding: [u8; 18],
}

const _: () = assert!(size_of::<SockAddrUn>() == 128);

/// A pathname or abstract AF_UNIX stream address ready for `kernel_connect`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UnixSocketAddress {
    path: [u8; UNIX_PATH_MAX],
    length: u16,
}

impl UnixSocketAddress {
    /// Convert mount-source syntax into an AF_UNIX address.
    ///
    /// Absolute paths use the filesystem namespace. A leading `@` follows the
    /// usual userspace convention for Linux's leading-NUL abstract namespace.
    pub(crate) fn from_mount_source(source: &[u8]) -> Result<Self> {
        if source.is_empty() || source.contains(&0) {
            return Err(EINVAL);
        }

        let mut path = [0u8; UNIX_PATH_MAX];
        let address_length = if source[0] == b'@' {
            let name = source.get(1..).ok_or(EINVAL)?;
            if name.is_empty() || name.len() >= UNIX_PATH_MAX {
                return Err(EINVAL);
            }
            path.get_mut(1..1 + name.len())
                .ok_or(EINVAL)?
                .copy_from_slice(name);
            SOCKADDR_UN_PATH_OFFSET
                .checked_add(1)
                .and_then(|length| length.checked_add(name.len()))
                .ok_or(EINVAL)?
        } else {
            if source[0] != b'/' || source.len() >= UNIX_PATH_MAX {
                return Err(EINVAL);
            }
            path.get_mut(..source.len())
                .ok_or(EINVAL)?
                .copy_from_slice(source);
            // Filesystem-path addresses include their terminating NUL.
            SOCKADDR_UN_PATH_OFFSET
                .checked_add(source.len())
                .and_then(|length| length.checked_add(1))
                .ok_or(EINVAL)?
        };

        Ok(Self {
            path,
            length: u16::try_from(address_length).map_err(|_| EINVAL)?,
        })
    }
}

fn iov_iter_count_mut(iterator: &mut bindings::iov_iter) -> &mut usize {
    // SAFETY: `count` overlays `__ubuf_iovec.iov_len` and is the active
    // bindgen-union member for every iterator kind Linux builds. The mutable
    // iterator reference makes the returned field reference exclusive.
    unsafe { &mut iterator.__bindgen_anon_1.__bindgen_anon_1.as_mut().count }
}

/// One request payload sent straight from its owner's source iterator.
///
/// The iterator is copied by value, the way Linux itself duplicates one for a
/// send (`netfs_reissue_write`, `__smb_send_rqst`), so transmitting never
/// disturbs the position its owner still needs. Truncating the copy up front
/// makes the declared length and the bytes actually pushed equal by
/// construction.
pub(crate) struct PayloadIter<'a> {
    message: bindings::msghdr,
    length: usize,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> PayloadIter<'a> {
    /// Snapshot at most `maximum` bytes of a source iterator.
    ///
    /// `splice` pins the iterator's pages into skbs instead of copying them.
    ///
    /// # Safety
    ///
    /// `source` must reference a live `struct iov_iter` whose `data_source` is
    /// `ITER_SOURCE`. Its segment array, its pages, and the bytes in those
    /// pages must all stay valid and unmodified for `'a`. When `splice` is set
    /// every page must additionally satisfy `sendpage_ok()`.
    pub(crate) unsafe fn from_source(
        source: *const bindings::iov_iter,
        maximum: usize,
        splice: bool,
    ) -> Self {
        let mut message = bindings::msghdr::default();
        message.msg_flags = bindings::MSG_NOSIGNAL
            | if splice {
                bindings::MSG_SPLICE_PAGES
            } else {
                0
            };
        // SAFETY: The caller guarantees `source` points at a live iov_iter.
        // The copy describes the caller's segment array rather than owning it,
        // which `'a` enforces.
        message.msg_iter = unsafe { ptr::read(source) };

        let mut payload = Self {
            message,
            length: 0,
            _lifetime: PhantomData,
        };
        let count = iov_iter_count_mut(&mut payload.message.msg_iter);
        if *count > maximum {
            *count = maximum;
        }
        payload.length = payload.remaining();
        payload
    }

    /// An unconsumed duplicate of this payload for one more send attempt.
    ///
    /// A resend after a lost connection has to push the same bytes again.
    /// `sock_sendmsg` only advances the msghdr it is given, which is the
    /// duplicate, so this snapshot and the source iterator behind it both stay
    /// where `from_source` left them. Its safety contract is inherited: the
    /// segments and pages the copy describes are the ones the constructor's
    /// caller already promised to keep valid and byte-stable for `'a`.
    pub(crate) fn snapshot(&self) -> Self {
        // SAFETY: `message` is plain data owned by value, so a bitwise copy
        // describes the same segment array without duplicating ownership of
        // anything. This is the same copy `from_source` makes of the caller's
        // iterator.
        let message = unsafe { ptr::read(&self.message) };
        Self {
            message,
            length: self.length,
            _lifetime: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    fn remaining(&mut self) -> usize {
        *iov_iter_count_mut(&mut self.message.msg_iter)
    }
}

/// A private cursor over a caller's destination iterator.
///
/// Copying `struct iov_iter` by value gives an independent position over the
/// same bvec array or folio queue, which is how Linux's own socket receivers
/// consume part of a caller's iterator (`cifs_read_iter_from_socket`). No page
/// reference is taken, so the iterator's owner stays responsible for keeping
/// those pages pinned.
pub(crate) struct IterCursor<'a> {
    message: bindings::msghdr,
    _lifetime: PhantomData<&'a mut ()>,
}

/// An exclusive destination iterator erased for a synchronous cross-task handoff.
///
/// The registration that creates this capability keeps the iterator's owner
/// borrowed until the receiving task gives the capability back. The receiver
/// additionally serializes access with its slot's `in_use` state.
#[derive(Clone, Copy)]
pub(crate) struct CrossTaskDestination(NonNull<bindings::iov_iter>);

// SAFETY: Construction requires a worker-safe destination iterator whose owner
// remains borrowed for the complete handoff. Access is permitted only to the
// receiver holding the corresponding slot claim.
unsafe impl Send for CrossTaskDestination {}

impl CrossTaskDestination {
    /// Erase the lifetime of an exclusively borrowed, worker-safe destination.
    ///
    /// # Safety
    ///
    /// `iterator` must remain live, writable, and exclusively reserved until
    /// the registration containing this capability is dropped. It must be an
    /// `ITER_DEST` BVEC or FOLIOQ iterator.
    pub(crate) unsafe fn from_exclusive(iterator: NonNull<bindings::iov_iter>) -> Self {
        Self(iterator)
    }

    /// Take a private cursor while the caller holds the registration's claim.
    ///
    /// # Safety
    ///
    /// No other task may access or advance the referenced iterator until the
    /// returned cursor is dropped.
    pub(crate) unsafe fn cursor<'a>(&'a mut self, length: usize) -> Result<IterCursor<'a>> {
        // SAFETY: The caller upholds the erased handoff's exclusivity and
        // lifetime contract.
        unsafe { IterCursor::new(self.0.as_ptr(), length) }
    }
}

impl<'a> IterCursor<'a> {
    /// Take a cursor over the next `length` bytes of `destination`.
    ///
    /// # Safety
    ///
    /// `destination` must reference a live `struct iov_iter` whose segment
    /// array and pages stay valid and writable, and which nothing else reads
    /// or advances, for `'a`.
    unsafe fn new(destination: *const bindings::iov_iter, length: usize) -> Result<Self> {
        // SAFETY: The caller guarantees `destination` points at a live
        // iov_iter. The copy describes the caller's segment array rather than
        // owning it, which `'a` enforces.
        let iterator = unsafe { ptr::read(destination) };
        // Only these kinds may be consumed from a task other than the one that
        // built them; UBUF, IOVEC and KVEC can name task-local or kmap-local
        // storage.
        let consumable = iterator.iter_type == bindings::iter_type_ITER_BVEC as u8
            || iterator.iter_type == bindings::iter_type_ITER_FOLIOQ as u8;
        if !consumable || iterator.data_source != (bindings::ITER_DEST != 0) {
            return Err(EINVAL);
        }

        let mut message = bindings::msghdr::default();
        message.msg_iter = iterator;
        let mut cursor = Self {
            message,
            _lifetime: PhantomData,
        };
        if cursor.remaining() < length {
            return Err(EINVAL);
        }
        // iov_iter_truncate(), which the bindings omit because it is a static
        // inline. The check above proves it only shortens the cursor.
        *iov_iter_count_mut(&mut cursor.message.msg_iter) = length;
        Ok(cursor)
    }

    /// Bytes still to be written at the cursor.
    pub(crate) fn remaining(&mut self) -> usize {
        *iov_iter_count_mut(&mut self.message.msg_iter)
    }

    /// Write `bytes` at the cursor, returning how many were consumed.
    pub(crate) fn write(&mut self, bytes: &[u8]) -> usize {
        // SAFETY: `msg_iter` is an owned ITER_DEST cursor, checked by `new`,
        // over pages the constructor's caller keeps pinned; nothing else
        // refers to this copy.
        let iterator = unsafe { IovIterDest::from_raw(ptr::addr_of_mut!(self.message.msg_iter)) };
        iterator.copy_to_iter(bytes)
    }
}

/// Owned kernel stream socket.
pub(crate) struct SocketTransport {
    socket: NonNull<bindings::socket>,
}

// SAFETY: `SocketTransport` uniquely owns the socket. Kernel send, receive,
// and shutdown operations support concurrent callers. The client guarantees
// one receive owner per session and serializes complete request writes. The
// last Arc cannot drop the socket while any safe method borrow remains.
unsafe impl Send for SocketTransport {}
unsafe impl Sync for SocketTransport {}

type SocketGuard = ScopeGuard<NonNull<bindings::socket>, fn(NonNull<bindings::socket>)>;

fn release_socket(socket: NonNull<bindings::socket>) {
    // SAFETY: SocketGuard owns a successfully created kernel socket.
    unsafe {
        bindings::sock_release(socket.as_ptr());
    }
}

fn create_stream_socket(
    network_namespace: *mut bindings::net,
    family: ffi::c_int,
    protocol: ffi::c_int,
    timeout_ms: u32,
    tcp_nodelay: bool,
) -> Result<SocketGuard> {
    if network_namespace.is_null() || timeout_ms == 0 {
        return Err(EINVAL);
    }
    let mut socket = ptr::null_mut();
    // SAFETY: `socket` is a valid out-pointer. The mount keeps its network
    // namespace live through this call, and the socket retains it afterward.
    to_result(unsafe {
        bindings::sock_create_kern(
            network_namespace,
            family,
            bindings::sock_type_SOCK_STREAM as ffi::c_int,
            protocol,
            &mut socket,
        )
    })?;
    let socket = NonNull::new(socket).ok_or(EIO)?;
    let guard = ScopeGuard::new_with_data(socket, release_socket as fn(NonNull<bindings::socket>));
    if tcp_nodelay {
        set_tcp_nodelay(*guard)?;
    }
    set_timeout(*guard, bindings::SO_SNDTIMEO_NEW, timeout_ms)?;
    set_timeout(*guard, bindings::SO_RCVTIMEO_NEW, timeout_ms)?;
    Ok(guard)
}

impl SocketTransport {
    /// Connect to an IPv4 address represented in network byte order.
    pub(crate) fn connect_ipv4(
        network_namespace: *mut bindings::net,
        address: [u8; 4],
        port: u16,
        timeout_ms: u32,
    ) -> Result<Self> {
        if port == 0 {
            return Err(EINVAL);
        }
        let socket = create_stream_socket(
            network_namespace,
            bindings::AF_INET as ffi::c_int,
            IPPROTO_TCP,
            timeout_ms,
            true,
        )?;

        let mut peer = SockAddrIn {
            family: bindings::AF_INET as u16,
            port: port.to_be(),
            // `address` is already a sequence of network-order octets. Reading
            // it in native order preserves those octets in memory.
            address: u32::from_ne_bytes(address),
            zero: [0; 8],
        };

        // SAFETY: `socket` is a live kernel socket and `peer` has the exact
        // layout and initialized length of `struct sockaddr_in`.
        let status = unsafe {
            bindings::kernel_connect(
                socket.as_ptr(),
                (&mut peer as *mut SockAddrIn).cast(),
                size_of::<SockAddrIn>() as ffi::c_int,
                0,
            )
        };
        to_result(status)?;

        Ok(Self {
            socket: socket.dismiss(),
        })
    }

    /// Connect to an IPv6 address represented in network byte order.
    ///
    /// Unlike the IPv4 path this needs no locally declared address type:
    /// bindgen exposes `sockaddr_in6` and `in6_addr` for this target, so the
    /// kernel's own layout is used directly.
    pub(crate) fn connect_ipv6(
        network_namespace: *mut bindings::net,
        address: [u8; 16],
        port: u16,
        timeout_ms: u32,
    ) -> Result<Self> {
        if port == 0 {
            return Err(EINVAL);
        }
        // A kernel without IPv6 fails creation rather than requiring a
        // separate capability probe.
        let socket = create_stream_socket(
            network_namespace,
            bindings::AF_INET6 as ffi::c_int,
            IPPROTO_TCP,
            timeout_ms,
            true,
        )?;

        let mut peer = bindings::sockaddr_in6 {
            sin6_family: bindings::AF_INET6 as u16,
            sin6_port: port.to_be(),
            sin6_flowinfo: 0,
            sin6_addr: bindings::in6_addr {
                in6_u: bindings::in6_addr__bindgen_ty_1 { u6_addr8: address },
            },
            // A scope id cannot be spelled in a mount source, so a link-local
            // address is unreachable rather than silently unscoped.
            sin6_scope_id: 0,
        };

        // SAFETY: `socket` is a live kernel socket and `peer` is a fully
        // initialized `struct sockaddr_in6` of exactly the length passed.
        let status = unsafe {
            bindings::kernel_connect(
                socket.as_ptr(),
                (&mut peer as *mut bindings::sockaddr_in6).cast(),
                size_of::<bindings::sockaddr_in6>() as ffi::c_int,
                0,
            )
        };
        to_result(status)?;

        Ok(Self {
            socket: socket.dismiss(),
        })
    }

    /// Connect to a filesystem-path or abstract AF_UNIX stream socket.
    pub(crate) fn connect_unix(
        network_namespace: *mut bindings::net,
        address: UnixSocketAddress,
        timeout_ms: u32,
    ) -> Result<Self> {
        let socket = create_stream_socket(
            network_namespace,
            bindings::AF_UNIX as ffi::c_int,
            0,
            timeout_ms,
            false,
        )?;

        let mut peer = SockAddrUn {
            family: bindings::AF_UNIX as u16,
            path: address.path,
            padding: [0; 18],
        };
        // SAFETY: `peer` has the exact sockaddr_un layout and `length` was
        // derived from a bounded pathname or abstract name above.
        let status = unsafe {
            bindings::kernel_connect(
                socket.as_ptr(),
                (&mut peer as *mut SockAddrUn).cast(),
                address.length as ffi::c_int,
                0,
            )
        };
        to_result(status)?;

        Ok(Self {
            socket: socket.dismiss(),
        })
    }

    /// Send the complete buffer or return the first socket error.
    pub(crate) fn send_all(&self, bytes: &[u8]) -> Result<()> {
        self.send_buffer(bytes, 0, false, &mut |error| Err(error))
            .map_err(|failure| failure.error())
    }

    /// Send the complete buffer, giving `on_error` a chance to resume it.
    ///
    /// `on_error` sees every failed send while the retry cursor is still
    /// exact. Returning `Ok(())` resends the unsent remainder, so a hook that
    /// never returns the error would spin; callers bound their retries.
    pub(crate) fn send_all_interruptible(
        &self,
        bytes: &[u8],
        on_error: &mut dyn FnMut(Error) -> Result<()>,
    ) -> SendResult {
        self.send_buffer(bytes, 0, false, on_error)
    }

    /// Send a complete request whose payload streams out of an iterator.
    ///
    /// The prefix is corked with MSG_MORE so a fixed header does not leave as
    /// its own segment on a TCP_NODELAY socket. A partial send leaves the
    /// stream desynchronized, so every error return obliges the caller to fail
    /// the session, exactly as for [`Self::send_all`].
    pub(crate) fn send_all_with_payload(
        &self,
        prefix: &[u8],
        mut payload: PayloadIter<'_>,
        on_error: &mut dyn FnMut(Error) -> Result<()>,
    ) -> SendResult {
        let mut unsent = payload.len();
        let prefix_flags = if unsent == 0 { 0 } else { bindings::MSG_MORE };
        self.send_buffer(prefix, prefix_flags, false, on_error)?;
        let mut started = !prefix.is_empty();

        while unsent != 0 {
            // SAFETY: The owned socket remains live for this blocking call.
            // `payload` owns the msghdr, and its constructor's contract keeps
            // the iterator's segments and pages valid and byte-stable;
            // sock_sendmsg only reads through the iterator and advances it by
            // what it accepted.
            let sent =
                unsafe { bindings::sock_sendmsg(self.socket.as_ptr(), &mut payload.message) };
            if sent < 0 {
                // Resuming is only exact while the iterator still holds every
                // byte the frame header already declared.
                if payload.remaining() != unsent {
                    return Err(SendFailure {
                        error: EIO,
                        started: true,
                    });
                }
                if let Err(error) = on_error(Error::from_errno(sent)) {
                    return Err(SendFailure { error, started });
                }
                continue;
            }
            if sent == 0 {
                return Err(SendFailure {
                    error: errno!(ECONNRESET),
                    started,
                });
            }

            let sent = sent as usize;
            if sent > unsent {
                return Err(SendFailure {
                    error: EIO,
                    started: true,
                });
            }
            started = true;
            unsent -= sent;
            // skb_splice_from_iter can consume iterator bytes it does not
            // report as sent, which leaves the declared length unreachable.
            if payload.remaining() != unsent {
                return Err(SendFailure {
                    error: EIO,
                    started: true,
                });
            }
        }

        Ok(())
    }

    /// Send the complete buffer with extra message flags.
    ///
    /// `kernel_sendmsg` may consume only part of the buffer. Rebuilding the
    /// kvec after every call keeps the retry cursor exact without relying on
    /// the socket layer to preserve or update the caller's vector.
    fn send_buffer(
        &self,
        mut bytes: &[u8],
        flags: u32,
        mut started: bool,
        on_error: &mut dyn FnMut(Error) -> Result<()>,
    ) -> SendResult {
        while !bytes.is_empty() {
            let mut message = bindings::msghdr::default();
            message.msg_flags = bindings::MSG_NOSIGNAL | flags;
            let mut vector = bindings::kvec {
                iov_base: bytes.as_ptr().cast_mut().cast::<c_void>(),
                iov_len: bytes.len(),
            };

            // SAFETY: The owned socket remains live, and `vector` describes an
            // immutable input slice for exactly this blocking call.
            let sent = unsafe {
                bindings::kernel_sendmsg(
                    self.socket.as_ptr(),
                    &mut message,
                    &mut vector,
                    1,
                    bytes.len(),
                )
            };
            if sent < 0 {
                if let Err(error) = on_error(Error::from_errno(sent)) {
                    return Err(SendFailure { error, started });
                }
                continue;
            }
            if sent == 0 {
                return Err(SendFailure {
                    error: errno!(ECONNRESET),
                    started,
                });
            }

            started = true;
            bytes = bytes.get(sent as usize..).ok_or(SendFailure {
                error: EIO,
                started,
            })?;
        }

        Ok(())
    }

    /// Fill the complete initialized buffer or return the first socket
    /// error/EOF.
    pub(crate) fn recv_exact(&self, bytes: &mut [u8]) -> Result<()> {
        // SAFETY: `bytes` is writable for its complete initialized extent.
        unsafe { self.recv_exact_raw(bytes.as_mut_ptr(), bytes.len()) }
    }

    /// Initialize the complete spare-capacity slice from the socket.
    ///
    /// This avoids zero-filling large read replies before the kernel
    /// immediately overwrites them with received bytes.
    pub(crate) fn recv_exact_uninit(&self, bytes: &mut [MaybeUninit<u8>]) -> Result<()> {
        // SAFETY: A MaybeUninit slice is writable for its complete extent.
        // recv_exact_raw only returns success after the socket initialized
        // every byte.
        unsafe { self.recv_exact_raw(bytes.as_mut_ptr().cast::<u8>(), bytes.len()) }
    }

    /// Receive one currently available stream chunk into `bytes`.
    ///
    /// Unlike [`Self::recv_exact`], this does not use `MSG_WAITALL`. A
    /// persistent stream accumulator can therefore collect a complete small
    /// response (and often several responses) with one socket receive instead
    /// of reading every seven-byte header separately.
    pub(crate) fn recv_some(&self, bytes: &mut [u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Err(EINVAL);
        }

        // SAFETY: An initialized byte slice is writable for its full extent.
        unsafe { self.recv_some_raw(bytes.as_mut_ptr(), bytes.len()) }
    }

    /// Receive one stream chunk into a caller-provided writable range.
    ///
    /// # Safety
    ///
    /// `destination` must remain writable for `length` bytes for this call.
    unsafe fn recv_some_raw(&self, destination: *mut u8, length: usize) -> Result<usize> {
        let mut message = bindings::msghdr::default();
        let mut vector = bindings::kvec {
            iov_base: destination.cast::<c_void>(),
            iov_len: length,
        };

        // SAFETY: The owned socket remains live, and `vector` describes the
        // complete writable output range promised by the caller.
        let received = unsafe {
            bindings::kernel_recvmsg(
                self.socket.as_ptr(),
                &mut message,
                &mut vector,
                1,
                length,
                0,
            )
        };
        if received < 0 {
            return Err(Error::from_errno(received));
        }
        if received == 0 {
            return Err(errno!(ECONNRESET));
        }

        let received = received as usize;
        if received > length {
            return Err(EIO);
        }
        Ok(received)
    }

    /// Fill exactly `length` writable bytes beginning at `destination`.
    ///
    /// # Safety
    ///
    /// `destination` must remain writable for `length` bytes for this call.
    unsafe fn recv_exact_raw(&self, mut destination: *mut u8, mut length: usize) -> Result<()> {
        while length != 0 {
            let mut message = bindings::msghdr::default();
            let mut vector = bindings::kvec {
                iov_base: destination.cast::<c_void>(),
                iov_len: length,
            };

            // SAFETY: The caller guarantees the remaining destination is
            // writable. The owned socket remains live, and `vector` describes
            // exactly that destination for this blocking call.
            let received = unsafe {
                bindings::kernel_recvmsg(
                    self.socket.as_ptr(),
                    &mut message,
                    &mut vector,
                    1,
                    length,
                    bindings::MSG_WAITALL as ffi::c_int,
                )
            };
            if received < 0 {
                return Err(Error::from_errno(received));
            }
            if received == 0 {
                return Err(errno!(ECONNRESET));
            }

            let received = received as usize;
            if received > length {
                return Err(EIO);
            }
            // SAFETY: `received <= length`, so the next destination remains
            // within the caller-provided writable allocation.
            destination = unsafe { destination.add(received) };
            length -= received;
        }

        Ok(())
    }

    /// Fill the cursor's whole remaining extent from the socket.
    ///
    /// A stream receive may return less than requested, so this loops the same
    /// way [`Self::recv_exact_raw`] does. Any error leaves the frame partly
    /// consumed, which obliges the caller to fail the session.
    pub(crate) fn recv_exact_into(&self, cursor: &mut IterCursor<'_>) -> Result<()> {
        loop {
            let expected = cursor.remaining();
            if expected == 0 {
                return Ok(());
            }

            // SAFETY: The owned socket remains live for this blocking call.
            // `cursor` owns the msghdr, and its constructor's contract keeps
            // the iterator's segments and pages writable; sock_recvmsg writes
            // only through the iterator and advances it by what it stored.
            let received = unsafe {
                bindings::sock_recvmsg(
                    self.socket.as_ptr(),
                    &mut cursor.message,
                    bindings::MSG_WAITALL as ffi::c_int,
                )
            };
            if received < 0 {
                return Err(Error::from_errno(received));
            }
            if received == 0 {
                return Err(errno!(ECONNRESET));
            }

            let received = received as usize;
            if received > expected {
                return Err(EIO);
            }
            // A copy fault inside tcp_recvmsg can advance the iterator past
            // what it reports, which would leave the rest of the frame
            // unreachable and desynchronize the stream.
            if cursor.remaining() != expected - received {
                return Err(EIO);
            }
        }
    }

    /// Re-arm the blocking send and receive timeouts.
    ///
    /// A probe dials with a short deadline so one unresponsive target cannot
    /// hold up the rotation. The winner carries ordinary requests afterwards,
    /// so it is handed the session's own timeout before it is installed.
    pub(crate) fn set_io_timeout(&self, timeout_ms: u32) -> Result<()> {
        if timeout_ms == 0 {
            return Err(EINVAL);
        }
        set_timeout(self.socket, bindings::SO_SNDTIMEO_NEW, timeout_ms)?;
        set_timeout(self.socket, bindings::SO_RCVTIMEO_NEW, timeout_ms)
    }

    /// Let the session receiver sleep on this socket without an idle timeout.
    ///
    /// Handshake and replay use a finite receive deadline because no ordinary
    /// request waiter exists to retire a silent candidate. Once installed, the
    /// one receiver remains in `recvmsg` even with no request outstanding.
    /// Ordinary callers own the response deadline and shut the socket down on
    /// expiry, which wakes that receive. A zero `SO_RCVTIMEO` is Linux's
    /// representation of an unbounded blocking receive.
    pub(crate) fn set_blocking_receive(&self) -> Result<()> {
        set_timeout(self.socket, bindings::SO_RCVTIMEO_NEW, 0)
    }

    /// Wake any blocked send or receive and make future I/O fail.
    ///
    /// `kernel_sock_shutdown` is safe to call more than once; Drop releases the
    /// socket exactly once when its final owner goes away.
    pub(crate) fn shutdown(&self) {
        // SAFETY: The constructor owns a live socket until Drop. Shutdown does
        // not release the socket and is designed to race socket I/O.
        unsafe {
            bindings::kernel_sock_shutdown(
                self.socket.as_ptr(),
                bindings::sock_shutdown_cmd_SHUT_RDWR,
            );
        }
    }
}

impl Drop for SocketTransport {
    fn drop(&mut self) {
        // SAFETY: A successful constructor gives `Self` exclusive ownership of
        // this live socket, and Drop runs exactly once. Shutdown wakes any
        // socket-side waiter before the final release; its status is irrelevant.
        self.shutdown();
        // SAFETY: Drop owns the last socket reference and runs exactly once.
        unsafe { bindings::sock_release(self.socket.as_ptr()) };
    }
}

fn set_tcp_nodelay(socket: NonNull<bindings::socket>) -> Result<()> {
    let mut enabled: ffi::c_int = 1;
    let mut value = bindings::sockptr_t::default();
    value.__bindgen_anon_1.kernel = (&mut enabled as *mut ffi::c_int).cast::<c_void>();
    value.set_is_kernel(true);

    // SAFETY: `socket` is live for this call. `value` is marked as a kernel
    // pointer and references an initialized integer of the supplied length.
    let status = unsafe {
        sock_setsockopt(
            socket.as_ptr(),
            IPPROTO_TCP,
            TCP_NODELAY,
            value,
            size_of::<ffi::c_int>() as ffi::c_uint,
        )
    };
    to_result(status)
}

fn set_timeout(socket: NonNull<bindings::socket>, option: u32, timeout_ms: u32) -> Result<()> {
    let mut timeout = bindings::__kernel_sock_timeval {
        tv_sec: (timeout_ms / 1000) as i64,
        tv_usec: ((timeout_ms % 1000) * 1000) as i64,
    };
    let mut value = bindings::sockptr_t::default();
    value.__bindgen_anon_1.kernel =
        (&mut timeout as *mut bindings::__kernel_sock_timeval).cast::<c_void>();
    value.set_is_kernel(true);

    // SAFETY: `socket` is live for this call. `value` is marked as a kernel
    // pointer and references an initialized timeval of the supplied length.
    let status = unsafe {
        sock_setsockopt(
            socket.as_ptr(),
            bindings::SOL_SOCKET as ffi::c_int,
            option as ffi::c_int,
            value,
            size_of::<bindings::__kernel_sock_timeval>() as ffi::c_uint,
        )
    };
    to_result(status)
}
