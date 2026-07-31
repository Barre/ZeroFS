use core::{
    net::{IpAddr, SocketAddr},
    ptr::NonNull,
    str::FromStr,
};

use kernel::{
    alloc::{KVec, flags::GFP_KERNEL},
    bindings,
    error::to_result,
    ffi,
    prelude::*,
};

use crate::{
    protocol,
    transport::{SocketTransport, UnixSocketAddress},
};

use super::{MAX_TARGETS, MIN_MSIZE, PROBE_TIMEOUT_MS};

const IPPROTO_TCP: ffi::c_int = 6;

/// Active reference to the network namespace used for every reconnect.
///
/// `put_net()` is a C inline over layout the canonical Rust bindings leave
/// opaque. An unconnected non-kernel socket is the kernel's existing RAII
/// owner for the same active reference: `sk_alloc(..., kern = false)` gets the
/// namespace and `sock_release()` puts it.
#[derive(Debug)]
struct NetworkNamespace {
    raw: NonNull<bindings::net>,
    anchor: NonNull<bindings::socket>,
}

// SAFETY: The anchor owns an active namespace reference and may be released
// from any task. The raw namespace pointer is only exposed while that owner is
// alive.
unsafe impl Send for NetworkNamespace {}
unsafe impl Sync for NetworkNamespace {}

impl NetworkNamespace {
    fn acquire(expected: *mut bindings::net, targets: &[EndpointAddress]) -> Result<Self> {
        let raw = NonNull::new(expected).ok_or_else(invalid_errno)?;
        let mut last_error = None;
        for target in targets {
            let (family, protocol) = target.socket_parameters();
            let mut anchor = core::ptr::null_mut();
            // SAFETY: fs_context keeps `raw` live during mount setup. Passing
            // kern=0 makes the resulting protocol socket acquire the active
            // namespace reference that sock_release later returns.
            let status = unsafe {
                bindings::__sock_create(
                    raw.as_ptr(),
                    family,
                    bindings::sock_type_SOCK_STREAM as ffi::c_int,
                    protocol,
                    &mut anchor,
                    0,
                )
            };
            if let Err(error) = to_result(status) {
                last_error = Some(error);
                continue;
            }
            let anchor = NonNull::new(anchor).ok_or_else(|| EIO)?;
            return Ok(Self { raw, anchor });
        }
        Err(last_error.unwrap_or_else(invalid_errno))
    }

    fn as_ptr(&self) -> *mut bindings::net {
        self.raw.as_ptr()
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        // SAFETY: acquire owns this unconnected socket. Releasing it performs
        // the matching active namespace put through the protocol destructor.
        unsafe { bindings::sock_release(self.anchor.as_ptr()) };
    }
}

/// Stream endpoint selected for one ZeroFS session.
#[derive(Clone, Copy, Debug)]
pub(super) enum EndpointAddress {
    TcpIpv4 { address: [u8; 4], port: u16 },
    TcpIpv6 { address: [u8; 16], port: u16 },
    Unix(UnixSocketAddress),
}

impl EndpointAddress {
    fn socket_parameters(self) -> (ffi::c_int, ffi::c_int) {
        match self {
            Self::TcpIpv4 { .. } => (bindings::AF_INET as ffi::c_int, IPPROTO_TCP),
            Self::TcpIpv6 { .. } => (bindings::AF_INET6 as ffi::c_int, IPPROTO_TCP),
            Self::Unix(_) => (bindings::AF_UNIX as ffi::c_int, 0),
        }
    }

    /// Parse one element of a mount source.
    ///
    /// Accepted forms are `unix:PATH` or `unix://PATH`, a leading `/` or `@`
    /// for AF_UNIX, and an IPv4 or IPv6 literal with an optional port behind an
    /// optional `tcp://` prefix. A name is rejected rather than dialed as
    /// something else: this client has no resolver, and the userspace client's
    /// per-probe re-resolution would need a userspace helper that may itself
    /// live on the filesystem being mounted.
    fn parse_one(spec: &[u8], default_port: u16) -> Result<Self> {
        if let Some(path) = spec.strip_prefix(b"unix:".as_slice()) {
            let path = path.strip_prefix(b"//".as_slice()).unwrap_or(path);
            return Ok(Self::Unix(UnixSocketAddress::from_mount_source(path)?));
        }
        if matches!(spec.first(), Some(b'/') | Some(b'@')) {
            return Ok(Self::Unix(UnixSocketAddress::from_mount_source(spec)?));
        }
        let spec = spec.strip_prefix(b"tcp://".as_slice()).unwrap_or(spec);
        Self::parse_ip(spec, default_port)
    }

    /// Parse an IP literal with an optional port.
    ///
    /// core's parsers are the same ones userspace tools use, so one spelling of
    /// an address cannot mean two things depending on who reads it. They also
    /// reject a multi-digit run starting with `0` rather than reading it as
    /// octal the way `inet_aton` would.
    ///
    /// An IPv6 address must be bracketed to carry a port, because `fd00::1:5564`
    /// is itself a valid literal whose last group is `5564`. Without brackets
    /// that spelling names a different host on the default port, which is the
    /// same rule every other tool applies.
    fn parse_ip(spec: &[u8], default_port: u16) -> Result<Self> {
        let text = core::str::from_utf8(spec).map_err(|_| invalid_errno())?;
        let (address, port) = match SocketAddr::from_str(text) {
            Ok(socket) => (socket.ip(), socket.port()),
            // A bracketed literal with no port is still a target. The socket
            // form required the port, so strip the brackets and take the
            // default.
            Err(_) => {
                let bare = text
                    .strip_prefix('[')
                    .and_then(|rest| rest.strip_suffix(']'))
                    .unwrap_or(text);
                (
                    IpAddr::from_str(bare).map_err(|_| invalid_errno())?,
                    default_port,
                )
            }
        };
        if port == 0 {
            return Err(invalid_errno());
        }

        Ok(match address {
            IpAddr::V4(address) => Self::TcpIpv4 {
                address: address.octets(),
                port,
            },
            // from_str accepts no scope id, so a link-local address cannot be
            // spelled here and sin6_scope_id is always zero.
            IpAddr::V6(address) => Self::TcpIpv6 {
                address: address.octets(),
                port,
            },
        })
    }
}

/// Parameters needed to establish a ZeroFS stream session.
///
/// The session keeps a counted namespace reference so reconnect remains valid
/// after the mount context that selected it has been freed.
#[derive(Debug)]
pub(crate) struct Endpoint {
    /// Network namespace selected by the VFS mount context.
    network_namespace: NetworkNamespace,
    /// Peers to probe for the serving leader, in the order they were given.
    pub(super) targets: KVec<EndpointAddress>,
    /// Per-phase socket, admission, and reply-wait timeout.
    pub(crate) timeout_ms: u32,
    /// Longest a request blocks waiting for reconnect and replay.
    pub(crate) grace_ms: u32,
    /// Upper bound for every wire frame in this session.
    pub(crate) requested_msize: u32,
}

impl Endpoint {
    /// Build a target set from addresses already in binary form, as the
    /// module-parameter mount path supplies them.
    pub(crate) fn tcp_ipv4(
        network_namespace: *mut bindings::net,
        addresses: &[([u8; 4], u16)],
        timeout_ms: u32,
        grace_ms: u32,
        requested_msize: u32,
    ) -> Result<Self> {
        let mut targets = KVec::with_capacity(addresses.len(), GFP_KERNEL)?;
        for &(address, port) in addresses {
            targets
                .push_within_capacity(EndpointAddress::TcpIpv4 { address, port })
                .map_err(|_| ENOMEM)?;
        }
        let network_namespace = NetworkNamespace::acquire(network_namespace, &targets)?;
        Ok(Self {
            network_namespace,
            targets,
            timeout_ms,
            grace_ms,
            requested_msize,
        })
    }

    /// Parse a comma-separated target set.
    ///
    /// Whitespace around an element and empty elements are ignored, matching
    /// the userspace client: config generators emit padded lists and trailing
    /// commas, and turning a trailing comma into a target would leave a
    /// permanently unreachable entry in the probe rotation. A specification
    /// that yields no target at all is rejected instead of producing a mount
    /// that can never connect.
    pub(crate) fn parse_targets(
        network_namespace: *mut bindings::net,
        spec: &[u8],
        default_port: u16,
        timeout_ms: u32,
        grace_ms: u32,
        requested_msize: u32,
    ) -> Result<Self> {
        let mut targets = KVec::new();
        for element in spec.split(|byte| *byte == b',') {
            let element = element.trim_ascii();
            if element.is_empty() {
                continue;
            }
            // Truncating instead would silently drop the element that may be
            // the one currently leading.
            if targets.len() >= MAX_TARGETS {
                return Err(invalid_errno());
            }
            targets.push(
                EndpointAddress::parse_one(element, default_port)?,
                GFP_KERNEL,
            )?;
        }
        if targets.is_empty() {
            return Err(invalid_errno());
        }
        let network_namespace = NetworkNamespace::acquire(network_namespace, &targets)?;

        Ok(Self {
            network_namespace,
            targets,
            timeout_ms,
            grace_ms,
            requested_msize,
        })
    }

    pub(super) fn validate(self) -> Result<Self> {
        if self.timeout_ms == 0
            || self.grace_ms == 0
            || self.requested_msize < MIN_MSIZE
            || self.requested_msize > protocol::MAX_MSIZE
        {
            return Err(invalid_errno());
        }
        if self.targets.is_empty() || self.targets.len() > MAX_TARGETS {
            return Err(invalid_errno());
        }
        if self.targets.iter().any(|target| {
            matches!(
                target,
                EndpointAddress::TcpIpv4 { port: 0, .. } | EndpointAddress::TcpIpv6 { port: 0, .. }
            )
        }) {
            return Err(invalid_errno());
        }

        Ok(self)
    }

    /// Dial one target with the probe deadline rather than the session's.
    pub(super) fn dial(&self, target: &EndpointAddress) -> Result<SocketTransport> {
        let timeout_ms = core::cmp::min(self.timeout_ms, PROBE_TIMEOUT_MS);
        match *target {
            EndpointAddress::TcpIpv4 { address, port } => SocketTransport::connect_ipv4(
                self.network_namespace.as_ptr(),
                address,
                port,
                timeout_ms,
            ),
            EndpointAddress::TcpIpv6 { address, port } => SocketTransport::connect_ipv6(
                self.network_namespace.as_ptr(),
                address,
                port,
                timeout_ms,
            ),
            EndpointAddress::Unix(address) => {
                SocketTransport::connect_unix(self.network_namespace.as_ptr(), address, timeout_ms)
            }
        }
    }
}

/// Errno for a target specification or session parameter this client rejects.
fn invalid_errno() -> Error {
    EINVAL
}
