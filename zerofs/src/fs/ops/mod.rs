//! The filesystem operations, one file per family: inherent impls on
//! [`ZeroFS`](crate::fs::ZeroFS), each mutation with its idempotent
//! (op-id deduped) variant.

mod create;
mod io;
mod link;
mod lookup;
mod remove;
mod rename;
mod setattr;

use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::types::{FileAttributes, InodeWithId};

impl ZeroFS {
    /// Reconstruct the `(id, attrs)` reply for an existing child of `dirid`, for
    /// the create ops' idempotency path (return the entry instead of EEXIST).
    async fn existing_child_result(
        &self,
        dirid: InodeId,
        name: &[u8],
    ) -> Result<(InodeId, FileAttributes), FsError> {
        let id = self.directory_store.get(dirid, name).await?;
        let inode = self.inode_store.get(id).await?;
        Ok((id, InodeWithId { inode: &inode, id }.into()))
    }
}
