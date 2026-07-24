use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::key_codec::KeyCodec;
use crate::fs::store::read_cache::{InvalidationGuard, MetadataCache};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_HARDLINKS_PER_INODE: u32 = u32::MAX;

const INODE_CACHE_BYTES: usize = 8 * 1024 * 1024;

type InodeCache = MetadataCache<InodeId, Inode>;

fn inode_cache_weight(_: &InodeId, inode: &Inode) -> usize {
    let allocated = match inode {
        Inode::File(inode) => inode.name.as_ref().map_or(0, Vec::len),
        Inode::Directory(inode) => inode.name.as_ref().map_or(0, Vec::len),
        Inode::Symlink(inode) => inode.target.len() + inode.name.as_ref().map_or(0, Vec::len),
        Inode::Fifo(inode)
        | Inode::Socket(inode)
        | Inode::CharDevice(inode)
        | Inode::BlockDevice(inode) => inode.name.as_ref().map_or(0, Vec::len),
    };
    std::mem::size_of::<InodeId>() + std::mem::size_of::<Inode>() + allocated
}

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    next_id: Arc<AtomicU64>,
    cache: InodeCache,
}

impl InodeStore {
    pub fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>, initial_next_id: u64) -> Self {
        let cache = InodeCache::new(
            db.clone(),
            INODE_CACHE_BYTES,
            "zerofs-inode-cache",
            inode_cache_weight,
        );
        Self {
            db,
            key_codec,
            next_id: Arc::new(AtomicU64::new(initial_next_id)),
            cache,
        }
    }

    pub fn allocate(&self) -> InodeId {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub async fn get(&self, id: InodeId) -> Result<Inode, FsError> {
        self.cache.get_or_load(id, || self.load(id)).await
    }

    async fn load(&self, id: InodeId) -> Result<Inode, FsError> {
        let key = self.key_codec.inode_key(id);

        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                let error = FsError::from_db_error(&e);
                if matches!(error, FsError::LeaderLeaseExpired | FsError::ShuttingDown) {
                    tracing::debug!("InodeStore::get({id}): serving authority lost");
                } else {
                    tracing::error!(
                        "InodeStore::get({}): database get_bytes failed: {:?}",
                        id,
                        e
                    );
                }
                error
            })?
            .ok_or_else(|| {
                // A missing inode is a normal ENOENT (a stat or deferred flush
                // racing a removal), not warning-worthy.
                tracing::debug!(
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

    pub(crate) fn invalidate_cache(
        &self,
        inode_ids: impl IntoIterator<Item = InodeId>,
    ) -> InvalidationGuard<InodeId, Inode> {
        self.cache.invalidate(inode_ids)
    }

    #[cfg(test)]
    pub(crate) fn cached_inode(&self, inode_id: InodeId) -> Option<Inode> {
        self.cache.peek(&inode_id)
    }

    #[cfg(test)]
    pub(crate) fn cache_enabled(&self) -> bool {
        self.cache.is_enabled()
    }

    pub fn save(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        inode: &Inode,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let key = self.key_codec.inode_key(id);
        let data = Bytes::from(bincode::serialize(inode)?);
        txn.put_bytes(&key, data);
        txn.invalidate_cached_inode(id);
        Ok(())
    }

    pub fn delete(&self, txn: &mut Transaction, id: InodeId) {
        let key = self.key_codec.inode_key(id);
        txn.delete_bytes(&key);
        txn.invalidate_cached_inode(id);
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
