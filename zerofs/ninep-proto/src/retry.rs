//! Mutation retry timings shared by 9P clients and servers.

use std::time::Duration;

/// Maximum elapsed time from the first dispatched mutation to a resend using
/// the same operation ID.
pub const MUTATION_RETRY_HORIZON: Duration = Duration::from_secs(120);

/// Retention period for a completed mutation result.
pub const MUTATION_RESULT_RETENTION: Duration = Duration::from_secs(150);

const _: () = assert!(
    MUTATION_RETRY_HORIZON.as_nanos() < MUTATION_RESULT_RETENTION.as_nanos(),
    "mutation result retention must exceed the retry horizon"
);
