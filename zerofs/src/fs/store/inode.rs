use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::key_codec::KeyCodec;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_HARDLINKS_PER_INODE: u32 = u32::MAX;

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<Db>,
    next_id: Arc<AtomicU64>,
}

impl InodeStore {
    pub fn new(db: Arc<Db>, initial_next_id: u64) -> Self {
        Self {
            db,
            next_id: Arc::new(AtomicU64::new(initial_next_id)),
        }
    }

    pub fn allocate(&self) -> InodeId {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub async fn get(&self, id: InodeId) -> Result<Inode, FsError> {
        let key = KeyCodec::inode_key(id);

        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                tracing::error!(
                    "InodeStore::get({}): database get_bytes failed: {:?}",
                    id,
                    e
                );
                FsError::IoError
            })?
            .ok_or_else(|| {
                tracing::warn!(
                    "InodeStore::get({}): inode key not found in database (key={:?}).",
                    id,
                    key
                );
                FsError::NotFound
            })?;

        bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "InodeStore::get({}): failed to deserialize inode data (len={}): {:?}.",
                id,
                data.len(),
                e
            );
            FsError::InvalidData
        })
    }

    pub fn save(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        inode: &Inode,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let key = KeyCodec::inode_key(id);
        let data = bincode::serialize(inode)?;
        txn.put_bytes(&key, Bytes::from(data));
        Ok(())
    }

    pub fn delete(&self, txn: &mut Transaction, id: InodeId) {
        let key = KeyCodec::inode_key(id);
        txn.delete_bytes(&key);
    }

    /// Resolve inode ID to full path components by walking parent chain.
    /// Returns Vec of path components (excluding root), in order from root to target.
    pub async fn resolve_path_components(&self, id: InodeId) -> Vec<Vec<u8>> {
        const ROOT_INODE_ID: InodeId = 0;

        if id == ROOT_INODE_ID {
            return Vec::new();
        }

        let mut components = Vec::new();
        let mut current_id = id;

        while current_id != ROOT_INODE_ID {
            if let Ok(inode) = self.get(current_id).await {
                let parent_id = match inode.parent() {
                    Some(p) => p,
                    None => {
                        // Hardlinked file - use placeholder
                        components.push(format!("<inode:{}>", current_id).into_bytes());
                        break;
                    }
                };

                if let Some(name) = inode.name() {
                    components.push(name.to_vec());
                    current_id = parent_id;
                } else {
                    // Name not available (shouldn't happen for non-hardlinked files)
                    components.push(format!("<inode:{}>", current_id).into_bytes());
                    break;
                }
            } else {
                break;
            }
        }

        components.reverse();
        components
    }

    /// Resolve inode ID to full path string.
    pub async fn resolve_path_lossy(&self, id: InodeId) -> String {
        let components = self.resolve_path_components(id).await;
        if components.is_empty() {
            return "/".to_string();
        }
        format!(
            "/{}",
            components
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}
