use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};

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
            let inode = self.load_inode(current_id).await?;
            let parent_id = match inode {
                Inode::File(f) => f.parent,
                Inode::Directory(d) => d.parent,
                Inode::Symlink(s) => s.parent,
                Inode::Fifo(s) => s.parent,
                Inode::Socket(s) => s.parent,
                Inode::CharDevice(s) => s.parent,
                Inode::BlockDevice(s) => s.parent,
            };

            if parent_id == ancestor_id {
                return Ok(true);
            }

            current_id = parent_id;
        }

        Ok(false)
    }
}
