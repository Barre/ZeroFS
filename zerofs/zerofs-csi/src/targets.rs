//! Parsing of the comma-separated `gateway` / `adminEndpoint` StorageClass
//! parameters into the per-node lists the HA-aware clients dial. An HA
//! deployment is a leader/standby pair, so the parameter carries both node
//! addresses; the client probes them and follows the serving leader. A plain
//! single address is just a one-element list, so non-HA StorageClasses are
//! unaffected.

use ninep_client::Target;

/// Default 9P port, matching `zerofs mount`, used when a 9P address omits one.
pub const DEFAULT_9P_PORT: u16 = 5564;

/// Split a comma-separated parameter into trimmed, non-empty segments. A plain
/// single address yields a one-element vec.
pub fn split(spec: &str) -> Vec<&str> {
    spec.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect()
}

/// Parse the `gateway` parameter (one or more comma-separated 9P addresses)
/// into the dial targets `NinePClient::connect_multi` probes for the leader.
/// Mirrors `zerofs mount`'s own target parsing so a value that works there
/// works here.
pub async fn parse_9p_targets(spec: &str) -> Result<Vec<Target>, String> {
    let mut targets = Vec::new();
    for seg in split(spec) {
        targets.push(parse_9p_target(seg).await?);
    }
    if targets.is_empty() {
        return Err("no 9P target given".to_string());
    }
    Ok(targets)
}

/// Parse one 9P target spec: `unix:/path`, `tcp://host:port`, a bare
/// `host:port`, or a path-like value (a Unix socket).
async fn parse_9p_target(spec: &str) -> Result<Target, String> {
    if let Some(rest) = spec.strip_prefix("unix:") {
        let path = rest.strip_prefix("//").unwrap_or(rest);
        return Ok(Target::Unix(path.into()));
    }
    let hostport = spec.strip_prefix("tcp://").unwrap_or(spec);
    if hostport.starts_with('/') || hostport.starts_with('.') {
        return Ok(Target::Unix(hostport.into()));
    }
    Ok(Target::Tcp(resolve_addr(hostport).await?))
}

async fn resolve_addr(s: &str) -> Result<std::net::SocketAddr, String> {
    if let Ok(addr) = s.parse::<std::net::SocketAddr>() {
        return Ok(addr);
    }
    let with_port = if s.contains(':') {
        s.to_string()
    } else {
        format!("{s}:{DEFAULT_9P_PORT}")
    };
    tokio::net::lookup_host(&with_port)
        .await
        .map_err(|e| format!("resolving {with_port}: {e}"))?
        .next()
        .ok_or_else(|| format!("no addresses resolved for {with_port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_trims_and_drops_empties() {
        assert_eq!(split("a:1"), vec!["a:1"]);
        assert_eq!(split(" a:1 , b:2 "), vec!["a:1", "b:2"]);
        // A trailing comma or stray whitespace yields no empty segments.
        assert_eq!(split("a:1, ,b:2,"), vec!["a:1", "b:2"]);
        assert!(split("").is_empty());
        assert!(split("  ,  ").is_empty());
    }

    #[tokio::test]
    async fn parses_each_address_form() {
        let t = parse_9p_targets("unix:/run/zerofs.sock").await.unwrap();
        assert!(matches!(t.as_slice(), [Target::Unix(p)] if p.as_os_str() == "/run/zerofs.sock"));

        // unix:// is accepted with the double slash stripped.
        let t = parse_9p_targets("unix:///run/z.sock").await.unwrap();
        assert!(matches!(t.as_slice(), [Target::Unix(p)] if p.as_os_str() == "/run/z.sock"));

        // A bare path is a Unix socket.
        let t = parse_9p_targets("/run/z.sock").await.unwrap();
        assert!(matches!(t.as_slice(), [Target::Unix(_)]));

        // An ip:port resolves without DNS; a two-node set yields two targets.
        let t = parse_9p_targets("127.0.0.1:5564,127.0.0.1:5565")
            .await
            .unwrap();
        assert!(matches!(
            t.as_slice(),
            [Target::Tcp(a), Target::Tcp(b)] if a.port() == 5564 && b.port() == 5565
        ));

        // A bare host without a port gets the default 9P port (loopback so the
        // resolver does not hit the network).
        let t = parse_9p_targets("127.0.0.1").await.unwrap();
        assert!(matches!(t.as_slice(), [Target::Tcp(a)] if a.port() == DEFAULT_9P_PORT));
    }

    #[tokio::test]
    async fn empty_spec_is_an_error() {
        assert!(parse_9p_targets("  ").await.is_err());
    }
}
