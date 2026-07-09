//! Shared fixture for the filesystem tests.

use crate::fs::permissions::Credentials;
use crate::test_helpers::test_helpers_mod::test_auth;

pub(super) fn test_creds() -> Credentials {
    Credentials::from_auth_context(&(&test_auth()).into())
}
