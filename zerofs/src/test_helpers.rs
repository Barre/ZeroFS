#[cfg(test)]
pub mod test_helpers_mod {
    use zerofs_nfsserve::nfs::nfsstring;
    use zerofs_nfsserve::vfs::AuthContext;

    pub fn filename(s: &[u8]) -> nfsstring {
        nfsstring(s.to_vec())
    }

    pub fn test_auth() -> AuthContext {
        AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![],
        }
    }
}
