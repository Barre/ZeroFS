use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};

pub const SMALL_FILE_TOMBSTONE_THRESHOLD: usize = 10;

pub const NAME_MAX: usize = 255;

pub fn validate_filename(filename: &[u8]) -> Result<(), FsError> {
    if filename.len() > NAME_MAX {
        Err(FsError::NameTooLong)
    } else {
        Ok(())
    }
}

impl ZeroFS {
    pub async fn is_ancestor_of(
        &self,
        ancestor_id: InodeId,
        descendant_id: InodeId,
    ) -> Result<bool, FsError> {
        if ancestor_id == descendant_id {
            return Ok(true);
        }

        let mut current_id = descendant_id;

        while current_id != 0 {
            let inode = self.inode_store.get(current_id).await?;
            let parent_id = match inode {
                Inode::Directory(d) => Some(d.parent),
                Inode::File(f) => f.parent,
                Inode::Symlink(s) => s.parent,
                Inode::Fifo(s) => s.parent,
                Inode::Socket(s) => s.parent,
                Inode::CharDevice(s) => s.parent,
                Inode::BlockDevice(s) => s.parent,
            };

            // If parent is None (file is hardlinked), can't determine ancestry
            let Some(pid) = parent_id else {
                return Ok(false);
            };

            if pid == ancestor_id {
                return Ok(true);
            }

            current_id = pid;
        }

        Ok(false)
    }

    /// Check execute permission on all parent directories leading to a file
    ///
    /// NOTE: This function has a known race condition - parent directory permissions
    /// could change after we check them but before the operation completes. This is
    /// accepted because:
    /// - The race window is extremely small
    /// - Fixing it would require complex multi-directory locking  
    /// - NFS traditionally has relaxed consistency semantics
    pub async fn check_parent_execute_permissions(
        &self,
        id: InodeId,
        creds: &Credentials,
    ) -> Result<(), FsError> {
        if id == 0 {
            return Ok(());
        }

        let inode = self.inode_store.get(id).await?;
        let parent_id = match &inode {
            Inode::Directory(d) => Some(d.parent),
            Inode::File(f) => f.parent,
            Inode::Symlink(s) => s.parent,
            Inode::Fifo(s) => s.parent,
            Inode::Socket(s) => s.parent,
            Inode::CharDevice(s) => s.parent,
            Inode::BlockDevice(s) => s.parent,
        };

        // If parent is None (file is hardlinked), skip parent permission checks
        let Some(mut current_id) = parent_id else {
            return Ok(());
        };
        while current_id != 0 {
            let parent_inode = self.inode_store.get(current_id).await?;

            check_access(&parent_inode, creds, AccessMode::Execute)?;

            current_id = match &parent_inode {
                Inode::Directory(d) => d.parent,
                _ => return Err(FsError::NotDirectory),
            };
        }

        Ok(())
    }
}
