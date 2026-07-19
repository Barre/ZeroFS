//! The filesystem operations, one file per family: inherent impls on
//! [`ZeroFS`](crate::fs::ZeroFS), each mutation with its idempotent
//! (op-id deduped) variant.

use crate::dedup::{DedupResult, OpId};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;

mod create;
mod io;
mod link;
mod lookup;
mod remove;
mod rename;
mod setattr;

impl ZeroFS {
    /// Decode a cached result as the operation's expected variant.
    /// An incompatible operation ID reuse returns an error.
    pub(crate) fn replay_dedup_result<T>(
        &self,
        op_id: &OpId,
        extract: fn(DedupResult) -> Option<T>,
    ) -> Result<Option<T>, FsError> {
        match self.dedup.get(op_id) {
            None => Ok(None),
            Some(result) => extract(result).map(Some).ok_or(FsError::InvalidArgument),
        }
    }
}
