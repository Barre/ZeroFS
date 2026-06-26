use std::fmt::Display;
use std::io;

/// Enrich a raw TCP bind failure so it names the listener and, for the two
/// common operator mistakes, says how to fix it. Mirrors the context the
/// Unix-socket bind paths already attach, so a port clash no longer surfaces as
/// a bare `Address already in use (os error 98)`.
pub(crate) fn tcp_bind_error(service: &str, addr: impl Display, err: &io::Error) -> io::Error {
    let hint = match err.kind() {
        io::ErrorKind::AddrInUse => {
            " (another process is already bound here; is a zerofs instance already running?)"
        }
        io::ErrorKind::PermissionDenied => {
            " (binding a port below 1024 needs root or CAP_NET_BIND_SERVICE)"
        }
        _ => "",
    };
    io::Error::new(
        err.kind(),
        format!("failed to bind {service} TCP listener on {addr}: {err}{hint}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addr_in_use_names_the_listener_and_hints_at_a_running_instance() {
        let err = io::Error::new(
            io::ErrorKind::AddrInUse,
            "Address already in use (os error 98)",
        );
        let msg = tcp_bind_error("9P", "0.0.0.0:5564", &err).to_string();
        assert!(msg.contains("9P"), "{msg}");
        assert!(msg.contains("0.0.0.0:5564"), "{msg}");
        assert!(msg.contains("Address already in use"), "{msg}");
        assert!(msg.contains("zerofs instance already running"), "{msg}");
    }

    #[test]
    fn permission_denied_hints_at_privilege() {
        let err = io::Error::from(io::ErrorKind::PermissionDenied);
        let msg = tcp_bind_error("NFS", "0.0.0.0:2049", &err).to_string();
        assert!(msg.contains("CAP_NET_BIND_SERVICE"), "{msg}");
    }

    #[test]
    fn unrelated_errors_get_no_hint_and_keep_their_kind() {
        let err = io::Error::from(io::ErrorKind::AddrNotAvailable);
        let mapped = tcp_bind_error("NBD", "10.0.0.1:10809", &err);
        assert_eq!(mapped.kind(), io::ErrorKind::AddrNotAvailable);
        let msg = mapped.to_string();
        assert!(msg.contains("NBD"), "{msg}");
        assert!(!msg.contains("zerofs instance"), "{msg}");
        assert!(!msg.contains("CAP_NET_BIND_SERVICE"), "{msg}");
    }
}
