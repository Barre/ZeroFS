pub mod errors;
pub mod flush_coordinator;
pub mod gc;
pub mod inode;
pub mod key_codec;
pub mod lock_manager;
pub mod metrics;
pub mod permissions;
pub mod stats;
pub mod store;
pub mod tracing;
pub mod types;
pub mod write_coordinator;

use std::path::PathBuf;

use self::flush_coordinator::FlushCoordinator;
use self::key_codec::KeyCodec;
use self::lock_manager::LockManager;
use self::metrics::FileSystemStats;
use self::stats::{FileSystemGlobalStats, StatsShardData};
use self::store::{ChunkStore, DirectoryStore, InodeStore, TombstoneStore};
use self::tracing::{AccessTracer, FileOperation};
use self::write_coordinator::WriteCoordinator;
use crate::config::CompressionConfig;
use crate::encryption::{EncryptedDb, EncryptedTransaction, EncryptionManager};
use slatedb::config::{PutOptions, WriteOptions};
use std::sync::Arc;

pub use self::gc::GarbageCollector;
pub use self::write_coordinator::SequenceGuard;

use self::errors::FsError;
use self::inode::{
    DirectoryInode, FileInode, Inode, InodeAttrs, InodeId, SpecialInode, SymlinkInode,
};
use self::permissions::{
    AccessMode, Credentials, can_set_times, check_access, check_ownership, check_sticky_bit_delete,
    validate_mode,
};
use self::store::inode::MAX_HARDLINKS_PER_INODE;
use self::types::{
    AuthContext, DirEntry, FileAttributes, FileType, InodeWithId, ReadDirResult, SetAttributes,
    SetGid, SetMode, SetSize, SetTime, SetUid,
};
use ::tracing::{debug, error, warn};
use bytes::Bytes;
use futures::pin_mut;
use futures::stream::{self, StreamExt};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

fn get_current_uid_gid() -> (u32, u32) {
    (0, 0)
}

pub fn get_current_time() -> (u64, u32) {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(e) => {
            warn!("System time is before UNIX epoch: {:?}", e);
            std::time::Duration::ZERO
        }
    };
    (now.as_secs(), now.subsec_nanos())
}

pub const CHUNK_SIZE: usize = 32 * 1024;
pub const STATS_SHARDS: usize = 100;
pub const SMALL_FILE_TOMBSTONE_THRESHOLD: usize = 10;
pub const NAME_MAX: usize = 255;

pub fn validate_filename(filename: &[u8]) -> Result<(), FsError> {
    if filename.len() > NAME_MAX {
        Err(FsError::NameTooLong)
    } else {
        Ok(())
    }
}
#[derive(Clone)]
pub struct ZeroFS {
    pub db: Arc<EncryptedDb>,
    pub chunk_store: ChunkStore,
    pub directory_store: DirectoryStore,
    pub inode_store: InodeStore,
    pub tombstone_store: TombstoneStore,
    pub lock_manager: Arc<LockManager>,
    pub stats: Arc<FileSystemStats>,
    pub global_stats: Arc<FileSystemGlobalStats>,
    pub flush_coordinator: FlushCoordinator,
    pub write_coordinator: Arc<WriteCoordinator>,
    pub max_bytes: u64,
    pub tracer: AccessTracer,
}

#[derive(Clone)]
pub struct CacheConfig {
    pub root_folder: PathBuf,
    pub max_cache_size_gb: f64,
    pub memory_cache_size_gb: Option<f64>,
}

impl ZeroFS {
    pub async fn new_with_slatedb(
        slatedb: crate::encryption::SlateDbHandle,
        encryption_key: [u8; 32],
        max_bytes: u64,
        compression: CompressionConfig,
    ) -> anyhow::Result<Self> {
        let encryptor = Arc::new(EncryptionManager::new(&encryption_key, compression));

        let lock_manager = Arc::new(LockManager::new());

        let db = Arc::new(match slatedb {
            crate::encryption::SlateDbHandle::ReadWrite(db) => EncryptedDb::new(db, encryptor),
            crate::encryption::SlateDbHandle::ReadOnly(reader) => {
                EncryptedDb::new_read_only(reader, encryptor)
            }
        });

        let counter_key = KeyCodec::system_counter_key();
        let next_inode_id = match db.get_bytes(&counter_key).await? {
            Some(data) => KeyCodec::decode_counter(&data)?,
            None => 1,
        };

        let root_inode_key = KeyCodec::inode_key(0);
        if db.get_bytes(&root_inode_key).await?.is_none() {
            if db.is_read_only() {
                return Err(anyhow::anyhow!(
                    "Cannot initialize filesystem in read-only mode. Root inode does not exist."
                ));
            }

            let (uid, gid) = get_current_uid_gid();
            let (now_sec, now_nsec) = get_current_time();
            let root_dir = DirectoryInode {
                mtime: now_sec,
                mtime_nsec: now_nsec,
                ctime: now_sec,
                ctime_nsec: now_nsec,
                atime: now_sec,
                atime_nsec: now_nsec,
                mode: 0o1777,
                uid,
                gid,
                entry_count: 0,
                parent: 0,
                name: None, // Root has no name
                nlink: 2,   // . and ..
            };
            let serialized = bincode::serialize(&Inode::Directory(root_dir))?;
            db.put_with_options(
                &root_inode_key,
                &serialized,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await?;
        }

        let global_stats = Arc::new(FileSystemGlobalStats::new());

        for i in 0..STATS_SHARDS {
            let shard_key = KeyCodec::stats_shard_key(i);
            if let Some(data) = db.get_bytes(&shard_key).await?
                && let Ok(shard_data) = bincode::deserialize::<StatsShardData>(&data)
            {
                global_stats.load_shard(i, &shard_data);
            }
        }

        let flush_coordinator = FlushCoordinator::new(db.clone());
        let write_coordinator = Arc::new(WriteCoordinator::new());
        let stats = Arc::new(FileSystemStats::new());
        let chunk_store = ChunkStore::new(db.clone());
        let directory_store = DirectoryStore::new(db.clone());
        let inode_store = InodeStore::new(db.clone(), next_inode_id);
        let tombstone_store = TombstoneStore::new(db.clone());

        let fs = Self {
            db: db.clone(),
            chunk_store,
            directory_store,
            inode_store,
            tombstone_store,
            lock_manager,
            stats,
            global_stats,
            flush_coordinator,
            write_coordinator,
            max_bytes,
            tracer: AccessTracer::new(),
        };

        Ok(fs)
    }

    pub async fn commit_transaction(
        &self,
        mut txn: EncryptedTransaction,
        seq_guard: &mut SequenceGuard,
    ) -> Result<(), FsError> {
        self.inode_store.save_counter(&mut txn);

        let (encrypt_result, _) = tokio::join!(txn.into_inner(), seq_guard.wait_for_predecessors());
        let (inner_batch, _cache_ops) = encrypt_result.map_err(|_| FsError::IoError)?;

        self.db
            .write_raw_batch(
                inner_batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        seq_guard.mark_committed();
        Ok(())
    }

    /// Resolve inode ID to full path components by walking parent chain
    /// Returns Vec of path components (excluding root), in order from root to target
    pub async fn resolve_path_components(&self, id: InodeId) -> Vec<Vec<u8>> {
        const ROOT_INODE_ID: InodeId = 0;

        if id == ROOT_INODE_ID {
            return Vec::new();
        }

        let mut components = Vec::new();
        let mut current_id = id;

        while current_id != ROOT_INODE_ID {
            if let Ok(inode) = self.inode_store.get(current_id).await {
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

    /// Resolve inode ID to full path string
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

    #[cfg(test)]
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        // Use a fixed test key for in-memory tests
        let test_key = [0u8; 32];
        Self::new_in_memory_with_encryption(test_key).await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_encryption(encryption_key: [u8; 32]) -> anyhow::Result<Self> {
        use slatedb::DbBuilder;
        use slatedb::object_store::path::Path;

        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(object_store);

        let settings = slatedb::config::Settings {
            compression_codec: None, // Disable compression - we handle it in encryption layer
            compactor_options: Some(slatedb::config::CompactorOptions {
                ..Default::default()
            }),
            ..Default::default()
        };

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path, object_store)
                .with_settings(settings)
                .build()
                .await?,
        );

        Self::new_with_slatedb(
            crate::encryption::SlateDbHandle::ReadWrite(slatedb),
            encryption_key,
            u64::MAX,
            CompressionConfig::default(),
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory_read_only(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        encryption_key: [u8; 32],
    ) -> anyhow::Result<Self> {
        use arc_swap::ArcSwap;
        use slatedb::DbReader;
        use slatedb::config::DbReaderOptions;
        use slatedb::object_store::path::Path;

        let db_path = Path::from("test_slatedb");
        let reader = Arc::new(
            DbReader::open(db_path, object_store, None, DbReaderOptions::default()).await?,
        );

        Self::new_with_slatedb(
            crate::encryption::SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
            encryption_key,
            u64::MAX,
            CompressionConfig::default(),
        )
        .await
    }

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
            let parent_id = inode.parent();

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
        let parent_id = inode.parent();

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

    pub async fn write(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
    ) -> Result<FileAttributes, FsError> {
        let start_time = std::time::Instant::now();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let creds = Credentials::from_auth_context(auth);

        // Check parent permissions before lock (also validates inode exists)
        self.check_parent_execute_permissions(id, &creds).await?;

        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.inode_store.get(id).await?;

        // NFS RFC 1813 section 4.4: Allow owners to write to their files regardless of permission bits
        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            _ => {}
        }

        match &mut inode {
            Inode::File(file) => {
                let old_size = file.size;
                let end_offset = offset + data.len() as u64;
                let new_size = std::cmp::max(file.size, end_offset);

                if new_size > old_size {
                    let size_increase = new_size - old_size;
                    let (used_bytes, _) = self.global_stats.get_totals();

                    if used_bytes.saturating_add(size_increase) > self.max_bytes {
                        debug!(
                            "Write would exceed quota: used={}, increase={}, max={}",
                            used_bytes, size_increase, self.max_bytes
                        );
                        return Err(FsError::NoSpace);
                    }
                }

                let mut txn = self.db.new_transaction()?;

                self.chunk_store.write(&mut txn, id, offset, data).await?;

                file.size = new_size;
                let (now_sec, now_nsec) = get_current_time();
                file.mtime = now_sec;
                file.mtime_nsec = now_nsec;
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;

                // POSIX: Clear SUID/SGID bits on write by non-owner
                if creds.uid != file.uid && creds.uid != 0 {
                    file.mode &= !0o6000;
                }

                let parent_name_for_update = file.parent.zip(file.name.clone());

                self.inode_store.save(&mut txn, id, &inode)?;

                if let Some((parent_id, name)) = parent_name_for_update {
                    self.directory_store
                        .update_inode_in_entry(&mut txn, parent_id, &name, id, &inode)
                        .await?;
                }

                let stats_update = if let Some(update) = self
                    .global_stats
                    .prepare_size_change(id, old_size, new_size)
                    .await
                {
                    self.global_stats.add_to_transaction(&update, &mut txn)?;
                    Some(update)
                } else {
                    None
                };

                let db_write_start = std::time::Instant::now();
                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;
                debug!("DB write took: {:?}", db_write_start.elapsed());

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                let elapsed = start_time.elapsed();
                debug!(
                    "Write processed successfully for inode {}, new size: {}, took: {:?}",
                    id, new_size, elapsed
                );

                self.stats
                    .bytes_written
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer
                    .emit(
                        || self.resolve_path_lossy(id),
                        FileOperation::Write {
                            offset,
                            length: data.len() as u64,
                        },
                    )
                    .await;

                Ok(InodeWithId { inode: &inode, id }.into())
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    pub async fn create(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(name)?;

        debug!(
            "create: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(name)
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.inode_store.get(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                if self.directory_store.exists(dirid, name).await? {
                    return Err(FsError::Exists);
                }

                let file_id = self.inode_store.allocate();
                debug!(
                    "Allocated inode {} for file {}",
                    file_id,
                    String::from_utf8_lossy(name)
                );

                let (now_sec, now_nsec) = get_current_time();

                let final_mode = match &attr.mode {
                    SetMode::Set(m) => validate_mode(*m),
                    SetMode::NoChange => 0o666,
                };

                let file_inode = FileInode {
                    size: 0,
                    mtime: now_sec,
                    mtime_nsec: now_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: now_sec,
                    atime_nsec: now_nsec,
                    mode: final_mode,
                    uid: match &attr.uid {
                        SetUid::Set(u) => *u,
                        SetUid::NoChange => creds.uid,
                    },
                    gid: match &attr.gid {
                        SetGid::Set(g) => *g,
                        SetGid::NoChange => creds.gid,
                    },
                    parent: Some(dirid),
                    name: Some(name.to_vec()),
                    nlink: 1,
                };

                let mut txn = self.db.new_transaction()?;
                let cookie = self
                    .directory_store
                    .allocate_cookie(dirid, &mut txn)
                    .await?;

                let file_inode_enum = Inode::File(file_inode.clone());
                self.inode_store.save(&mut txn, file_id, &file_inode_enum)?;
                self.directory_store.add(
                    &mut txn,
                    dirid,
                    name,
                    file_id,
                    cookie,
                    Some(&file_inode_enum),
                );

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                let stats_update = self.global_stats.prepare_inode_create(file_id).await;
                self.global_stats
                    .add_to_transaction(&stats_update, &mut txn)?;

                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard)
                    .await
                    .inspect_err(|e| {
                        error!("Failed to write batch: {:?}", e);
                    })?;

                self.global_stats.commit_update(&stats_update);

                self.stats.files_created.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer
                    .emit(
                        || self.resolve_path_lossy(file_id),
                        FileOperation::Create {
                            mode: file_inode.mode,
                        },
                    )
                    .await;

                let inode = Inode::File(file_inode);
                let file_attrs = InodeWithId {
                    inode: &inode,
                    id: file_id,
                }
                .into();
                Ok((file_id, file_attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        filename: &[u8],
    ) -> Result<InodeId, FsError> {
        let (id, _) = self
            .create(
                &Credentials::from_auth_context(auth),
                dirid,
                filename,
                &SetAttributes::default(),
            )
            .await?;
        Ok(id)
    }

    pub async fn read_file(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        debug!("read_file: id={}, offset={}, count={}", id, offset, count);

        let inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        check_access(&inode, &creds, AccessMode::Read)?;

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    self.tracer
                        .emit(
                            || self.resolve_path_lossy(id),
                            FileOperation::Read { offset, length: 0 },
                        )
                        .await;
                    return Ok((Bytes::new(), true));
                }

                let read_len = std::cmp::min(count as u64, file.size - offset);
                let result_bytes = self.chunk_store.read(id, offset, read_len).await?;
                let eof = offset + read_len >= file.size;

                self.stats
                    .bytes_read
                    .fetch_add(result_bytes.len() as u64, Ordering::Relaxed);
                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer
                    .emit(
                        || self.resolve_path_lossy(id),
                        FileOperation::Read {
                            offset,
                            length: read_len,
                        },
                    )
                    .await;

                Ok((result_bytes, eof))
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    pub async fn trim(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Result<(), FsError> {
        debug!(
            "Processing trim on inode {} at offset {} length {}",
            id, offset, length
        );

        let _guard = self.lock_manager.acquire_write(id).await;
        let inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            Inode::File(_) => {}
            _ => return Err(FsError::IsDirectory),
        }

        let file = match &inode {
            Inode::File(f) => f,
            _ => return Err(FsError::IsDirectory),
        };

        let mut txn = self.db.new_transaction()?;

        self.chunk_store
            .zero_range(&mut txn, id, offset, length, file.size)
            .await;

        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard)
            .await
            .inspect_err(|e| {
                error!("Failed to commit trim batch: {}", e);
            })?;

        debug!("Trim completed successfully for inode {}", id);

        self.tracer
            .emit(
                || self.resolve_path_lossy(id),
                FileOperation::Trim { offset, length },
            )
            .await;

        Ok(())
    }

    pub async fn lookup(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        filename: &[u8],
    ) -> Result<InodeId, FsError> {
        debug!(
            "lookup: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(filename)
        );

        let dir_inode = self.inode_store.get(dirid).await?;

        match dir_inode {
            Inode::Directory(_) => {
                check_access(&dir_inode, creds, AccessMode::Execute)?;

                match self.directory_store.get(dirid, filename).await {
                    Ok(inode_id) => {
                        debug!(
                            "lookup found: {} -> inode {}",
                            String::from_utf8_lossy(filename),
                            inode_id
                        );

                        self.tracer
                            .emit(
                                || self.resolve_path_lossy(inode_id),
                                FileOperation::Lookup {
                                    filename: String::from_utf8_lossy(filename).to_string(),
                                },
                            )
                            .await;

                        Ok(inode_id)
                    }
                    Err(FsError::NotFound) => {
                        debug!(
                            "lookup not found: {} in directory",
                            String::from_utf8_lossy(filename)
                        );
                        Err(FsError::NotFound)
                    }
                    Err(e) => Err(e),
                }
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn mkdir(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(name)?;

        debug!(
            "mkdir: dirid={}, dirname={}",
            dirid,
            String::from_utf8_lossy(name)
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.inode_store.get(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                if self.directory_store.exists(dirid, name).await? {
                    return Err(FsError::Exists);
                }

                let new_dir_id = self.inode_store.allocate();

                let (now_sec, now_nsec) = get_current_time();

                let mut new_mode = match &attr.mode {
                    SetMode::Set(m) => *m,
                    SetMode::NoChange => 0o777,
                };

                let parent_mode = dir.mode;
                if parent_mode & 0o2000 != 0 {
                    new_mode |= 0o2000;
                }

                let new_uid = match &attr.uid {
                    SetUid::Set(u) => *u,
                    SetUid::NoChange => creds.uid,
                };

                let new_gid = match &attr.gid {
                    SetGid::Set(g) => *g,
                    SetGid::NoChange => {
                        if parent_mode & 0o2000 != 0 {
                            dir.gid
                        } else {
                            creds.gid
                        }
                    }
                };

                let (atime_sec, atime_nsec) = match &attr.atime {
                    SetTime::SetToClientTime(ts) => (ts.seconds, ts.nanoseconds),
                    SetTime::SetToServerTime | SetTime::NoChange => (now_sec, now_nsec),
                };

                let (mtime_sec, mtime_nsec) = match &attr.mtime {
                    SetTime::SetToClientTime(ts) => (ts.seconds, ts.nanoseconds),
                    SetTime::SetToServerTime | SetTime::NoChange => (now_sec, now_nsec),
                };

                let new_dir_inode = DirectoryInode {
                    mtime: mtime_sec,
                    mtime_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: atime_sec,
                    atime_nsec,
                    mode: new_mode,
                    uid: new_uid,
                    gid: new_gid,
                    entry_count: 0,
                    parent: dirid,
                    name: Some(name.to_vec()),
                    nlink: 2,
                };

                let mut txn = self.db.new_transaction()?;
                let cookie = self
                    .directory_store
                    .allocate_cookie(dirid, &mut txn)
                    .await?;

                let new_dir_inode_enum = Inode::Directory(new_dir_inode.clone());
                self.inode_store
                    .save(&mut txn, new_dir_id, &new_dir_inode_enum)?;
                self.directory_store.add(
                    &mut txn,
                    dirid,
                    name,
                    new_dir_id,
                    cookie,
                    Some(&new_dir_inode_enum),
                );

                dir.entry_count += 1;
                if dir.nlink == u32::MAX {
                    return Err(FsError::NoSpace);
                }
                dir.nlink += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                let stats_update = self.global_stats.prepare_inode_create(new_dir_id).await;
                self.global_stats
                    .add_to_transaction(&stats_update, &mut txn)?;

                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;

                self.global_stats.commit_update(&stats_update);

                self.stats
                    .directories_created
                    .fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer
                    .emit(
                        || self.resolve_path_lossy(new_dir_id),
                        FileOperation::Mkdir {
                            mode: new_dir_inode.mode,
                        },
                    )
                    .await;

                let new_inode = Inode::Directory(new_dir_inode);
                let attrs = InodeWithId {
                    inode: &new_inode,
                    id: new_dir_id,
                }
                .into();
                Ok((new_dir_id, attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn readdir(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        start_after: InodeId,
        max_entries: usize,
    ) -> Result<ReadDirResult, FsError> {
        let dir_inode = self.inode_store.get(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Read)?;

        use crate::fs::store::directory::{COOKIE_DOT, COOKIE_DOTDOT};

        match &dir_inode {
            Inode::Directory(dir) => {
                let mut entries = Vec::new();

                // Handle . and .. based on start_after cookie
                if start_after < COOKIE_DOT {
                    entries.push(DirEntry {
                        fileid: dirid,
                        name: b".".to_vec(),
                        attr: InodeWithId {
                            inode: &dir_inode,
                            id: dirid,
                        }
                        .into(),
                        cookie: COOKIE_DOT,
                    });
                }

                if start_after < COOKIE_DOTDOT {
                    let parent_id = if dirid == 0 { 0 } else { dir.parent };
                    let parent_attr = if parent_id == dirid {
                        InodeWithId {
                            inode: &dir_inode,
                            id: dirid,
                        }
                        .into()
                    } else {
                        let parent_inode = self.inode_store.get(parent_id).await?;
                        InodeWithId {
                            inode: &parent_inode,
                            id: parent_id,
                        }
                        .into()
                    };
                    entries.push(DirEntry {
                        fileid: parent_id,
                        name: b"..".to_vec(),
                        attr: parent_attr,
                        cookie: COOKIE_DOTDOT,
                    });
                }

                // Get regular entries, starting after the given cookie
                let iter = if start_after < COOKIE_DOTDOT {
                    self.directory_store.list(dirid).await?
                } else {
                    self.directory_store.list_from(dirid, start_after).await?
                };
                pin_mut!(iter);

                let mut dir_entries: Vec<(InodeId, Vec<u8>, u64, Option<Inode>)> = Vec::new();
                let mut has_more = false;

                while let Some(result) = iter.next().await {
                    if dir_entries.len() >= max_entries - entries.len() {
                        has_more = true;
                        break;
                    }
                    let entry = result?;
                    dir_entries.push((entry.inode_id, entry.name, entry.cookie, entry.inode));
                }

                let lookup_indices: Vec<usize> = dir_entries
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, _, _, inode))| inode.is_none())
                    .map(|(i, _)| i)
                    .collect();

                if !lookup_indices.is_empty() {
                    const BUFFER_SIZE: usize = 256;

                    let lookup_entries: Vec<_> = lookup_indices
                        .iter()
                        .map(|&i| (i, dir_entries[i].0))
                        .collect();

                    let inode_futures = stream::iter(lookup_entries.into_iter()).map(
                        |(idx, inode_id)| async move {
                            match self.inode_store.get(inode_id).await {
                                Ok(inode) => Ok::<_, FsError>((idx, Some(inode))),
                                Err(FsError::NotFound) => {
                                    debug!("readdir: skipping deleted entry (inode {})", inode_id);
                                    Ok((idx, None))
                                }
                                Err(e) => {
                                    error!("readdir: failed to load inode {}: {:?}", inode_id, e);
                                    Err(e)
                                }
                            }
                        },
                    );

                    let loaded_inodes: Vec<_> = inode_futures
                        .buffered(BUFFER_SIZE)
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;

                    for (idx, inode_opt) in loaded_inodes {
                        dir_entries[idx].3 = inode_opt;
                    }
                }

                for (inode_id, name, cookie, inode_opt) in dir_entries {
                    if let Some(inode) = inode_opt {
                        entries.push(DirEntry {
                            fileid: inode_id,
                            name,
                            attr: InodeWithId {
                                inode: &inode,
                                id: inode_id,
                            }
                            .into(),
                            cookie,
                        });
                    }
                }

                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer
                    .emit(
                        || self.resolve_path_lossy(dirid),
                        FileOperation::Readdir {
                            count: entries.len() as u32,
                        },
                    )
                    .await;

                Ok(ReadDirResult {
                    entries,
                    end: !has_more,
                })
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn symlink(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        linkname: &[u8],
        target: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(linkname)?;

        debug!(
            "symlink: dirid={}, linkname={:?}, target={:?}",
            dirid,
            String::from_utf8_lossy(linkname),
            target
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.inode_store.get(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        let dir = match &mut dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(FsError::NotDirectory),
        };

        if self.directory_store.exists(dirid, linkname).await? {
            return Err(FsError::Exists);
        }

        let new_id = self.inode_store.allocate();

        let mode = match &attr.mode {
            SetMode::Set(m) => *m | 0o120000,
            SetMode::NoChange => 0o120777,
        };

        let uid = match &attr.uid {
            SetUid::Set(u) => *u,
            SetUid::NoChange => creds.uid,
        };

        let gid = match &attr.gid {
            SetGid::Set(g) => *g,
            SetGid::NoChange => creds.gid,
        };

        let (now_sec, now_nsec) = get_current_time();
        let symlink_inode = Inode::Symlink(SymlinkInode {
            target: target.to_vec(),
            mtime: now_sec,
            mtime_nsec: now_nsec,
            ctime: now_sec,
            ctime_nsec: now_nsec,
            atime: now_sec,
            atime_nsec: now_nsec,
            mode,
            uid,
            gid,
            parent: Some(dirid),
            name: Some(linkname.to_vec()),
            nlink: 1,
        });

        let mut txn = self.db.new_transaction()?;
        let cookie = self
            .directory_store
            .allocate_cookie(dirid, &mut txn)
            .await?;

        self.inode_store.save(&mut txn, new_id, &symlink_inode)?;
        self.directory_store.add(
            &mut txn,
            dirid,
            linkname,
            new_id,
            cookie,
            Some(&symlink_inode),
        );

        dir.entry_count += 1;
        dir.mtime = now_sec;
        dir.mtime_nsec = now_nsec;
        dir.ctime = now_sec;
        dir.ctime_nsec = now_nsec;

        self.inode_store.save(&mut txn, dirid, &dir_inode)?;

        let stats_update = self.global_stats.prepare_inode_create(new_id).await;
        self.global_stats
            .add_to_transaction(&stats_update, &mut txn)?;

        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard).await?;

        self.global_stats.commit_update(&stats_update);

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        self.tracer
            .emit(
                || self.resolve_path_lossy(new_id),
                FileOperation::Symlink {
                    target: String::from_utf8_lossy(target).to_string(),
                },
            )
            .await;

        Ok((
            new_id,
            InodeWithId {
                inode: &symlink_inode,
                id: new_id,
            }
            .into(),
        ))
    }

    pub async fn link(
        &self,
        auth: &AuthContext,
        fileid: InodeId,
        linkdirid: InodeId,
        linkname: &[u8],
    ) -> Result<(), FsError> {
        validate_filename(linkname)?;

        let linkname_str = String::from_utf8_lossy(linkname);
        debug!(
            "link: fileid={}, linkdirid={}, linkname={}",
            fileid, linkdirid, linkname_str
        );

        let _guards = self
            .lock_manager
            .acquire_multiple_write(vec![fileid, linkdirid])
            .await;

        let link_dir_inode = self.inode_store.get(linkdirid).await?;
        let creds = Credentials::from_auth_context(auth);

        check_access(&link_dir_inode, &creds, AccessMode::Write)?;
        check_access(&link_dir_inode, &creds, AccessMode::Execute)?;

        self.check_parent_execute_permissions(fileid, &creds)
            .await?;

        let mut link_dir = match link_dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(FsError::NotDirectory),
        };

        let mut file_inode = self.inode_store.get(fileid).await?;

        if matches!(file_inode, Inode::Directory(_)) {
            return Err(FsError::InvalidArgument);
        }

        if matches!(file_inode, Inode::Symlink(_)) {
            return Err(FsError::InvalidArgument);
        }

        if self.directory_store.exists(linkdirid, linkname).await? {
            return Err(FsError::Exists);
        }

        let original_parent_name = file_inode
            .parent()
            .zip(file_inode.name().map(|n| n.to_vec()));

        let mut txn = self.db.new_transaction()?;
        let cookie = self
            .directory_store
            .allocate_cookie(linkdirid, &mut txn)
            .await?;

        self.directory_store
            .add(&mut txn, linkdirid, linkname, fileid, cookie, None);

        if let Some((orig_parent, orig_name)) = original_parent_name {
            self.directory_store
                .convert_to_reference(&mut txn, orig_parent, &orig_name, fileid)
                .await?;
        }

        let (now_sec, now_nsec) = get_current_time();
        match &mut file_inode {
            Inode::File(file) => {
                if file.nlink == MAX_HARDLINKS_PER_INODE {
                    return Err(FsError::TooManyLinks);
                }
                file.nlink += 1;
                if file.nlink > 1 {
                    file.parent = None;
                    file.name = None;
                }
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                if special.nlink == MAX_HARDLINKS_PER_INODE {
                    return Err(FsError::TooManyLinks);
                }
                special.nlink += 1;
                if special.nlink > 1 {
                    special.parent = None;
                    special.name = None;
                }
                special.ctime = now_sec;
                special.ctime_nsec = now_nsec;
            }
            _ => unreachable!(),
        }

        self.inode_store.save(&mut txn, fileid, &file_inode)?;

        link_dir.entry_count += 1;
        link_dir.mtime = now_sec;
        link_dir.mtime_nsec = now_nsec;
        link_dir.ctime = now_sec;
        link_dir.ctime_nsec = now_nsec;

        self.inode_store
            .save(&mut txn, linkdirid, &Inode::Directory(link_dir))?;

        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard).await?;

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        // Emit event with the original file's path and new link path
        if self.tracer.has_subscribers() {
            let file_path = self.resolve_path_lossy(fileid).await;
            let dir_path = self.resolve_path_lossy(linkdirid).await;
            let new_path = format!(
                "{}/{}",
                dir_path.trim_end_matches('/'),
                String::from_utf8_lossy(linkname)
            );
            self.tracer
                .emit(|| async { file_path }, FileOperation::Link { new_path })
                .await;
        }

        Ok(())
    }

    pub async fn setattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        setattr: &SetAttributes,
    ) -> Result<FileAttributes, FsError> {
        debug!("setattr: id={}, setattr={:?}", id, setattr);
        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.inode_store.get(id).await?;

        self.check_parent_execute_permissions(id, creds).await?;

        // For chmod (mode change), must be owner
        if matches!(setattr.mode, SetMode::Set(_)) {
            check_ownership(&inode, creds)?;
        }

        // For chown/chgrp, must be root (or owner with restrictions)
        let changing_uid = matches!(&setattr.uid, SetUid::Set(_));
        let changing_gid = matches!(&setattr.gid, SetGid::Set(_));

        if (changing_uid || changing_gid) && creds.uid != 0 {
            check_ownership(&inode, creds)?;

            if let SetUid::Set(new_uid) = setattr.uid
                && new_uid != creds.uid
            {
                return Err(FsError::OperationNotPermitted);
            }

            // POSIX: Owner can change group to any group they belong to
            if let SetGid::Set(new_gid) = setattr.gid
                && !creds.is_member_of_group(new_gid)
            {
                return Err(FsError::OperationNotPermitted);
            }
        }

        match setattr.atime {
            SetTime::SetToClientTime(_) => {
                can_set_times(&inode, creds, false)?;
            }
            SetTime::SetToServerTime => {
                can_set_times(&inode, creds, true)?;
            }
            SetTime::NoChange => {}
        }
        match setattr.mtime {
            SetTime::SetToClientTime(_) => {
                can_set_times(&inode, creds, false)?;
            }
            SetTime::SetToServerTime => {
                can_set_times(&inode, creds, true)?;
            }
            SetTime::NoChange => {}
        }

        if matches!(setattr.size, SetSize::Set(_)) {
            check_access(&inode, creds, AccessMode::Write)?;
        }

        match &mut inode {
            Inode::File(file) => {
                if let SetSize::Set(new_size) = setattr.size {
                    let old_size = file.size;
                    if new_size != old_size {
                        if new_size > old_size {
                            let size_increase = new_size - old_size;
                            let (used_bytes, _) = self.global_stats.get_totals();
                            if used_bytes.saturating_add(size_increase) > self.max_bytes {
                                debug!(
                                    "Setattr size change would exceed quota: used={}, increase={}, max={}",
                                    used_bytes, size_increase, self.max_bytes
                                );
                                return Err(FsError::NoSpace);
                            }
                        }

                        file.size = new_size;
                        let (now_sec, now_nsec) = get_current_time();
                        file.mtime = now_sec;
                        file.mtime_nsec = now_nsec;
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;

                        let mut txn = self.db.new_transaction()?;

                        self.chunk_store
                            .truncate(&mut txn, id, old_size, new_size)
                            .await?;

                        let parent_name_for_update = file.parent.zip(file.name.clone());

                        self.inode_store.save(&mut txn, id, &inode)?;

                        if let Some((parent_id, name)) = parent_name_for_update {
                            self.directory_store
                                .update_inode_in_entry(&mut txn, parent_id, &name, id, &inode)
                                .await?;
                        }

                        let stats_update = if let Some(update) = self
                            .global_stats
                            .prepare_size_change(id, old_size, new_size)
                            .await
                        {
                            self.global_stats.add_to_transaction(&update, &mut txn)?;
                            Some(update)
                        } else {
                            None
                        };

                        let mut seq_guard = self.write_coordinator.allocate_sequence();
                        self.commit_transaction(txn, &mut seq_guard).await?;

                        if let Some(update) = stats_update {
                            self.global_stats.commit_update(&update);
                        }

                        self.tracer
                            .emit(
                                || self.resolve_path_lossy(id),
                                FileOperation::Setattr {
                                    mode: match setattr.mode {
                                        SetMode::Set(m) => Some(m),
                                        SetMode::NoChange => None,
                                    },
                                },
                            )
                            .await;

                        return Ok(InodeWithId { inode: &inode, id }.into());
                    }
                }

                if let SetMode::Set(mode) = setattr.mode {
                    debug!("Setting file mode from {} to {:#o}", file.mode, mode);
                    file.mode = validate_mode(mode);
                    // POSIX: If non-root user sets mode with setgid bit and doesn't belong to file's group, clear setgid
                    if creds.uid != 0
                        && (file.mode & 0o2000) != 0
                        && !creds.is_member_of_group(file.gid)
                    {
                        file.mode &= !0o2000;
                    }
                }
                if let SetUid::Set(uid) = setattr.uid {
                    file.uid = uid;
                    if creds.uid != 0 {
                        file.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    file.gid = gid;
                    // Clear SUID/SGID bits when non-root user calls chown with a gid
                    // This happens even if the gid doesn't actually change (POSIX behavior)
                    if creds.uid != 0 {
                        file.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        file.atime = t.seconds;
                        file.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        file.atime = now_sec;
                        file.atime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        file.mtime = t.seconds;
                        file.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        file.mtime = now_sec;
                        file.mtime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(setattr.size, SetSize::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    file.ctime = now_sec;
                    file.ctime_nsec = now_nsec;
                }
            }
            Inode::Directory(dir) => {
                if let SetMode::Set(mode) = setattr.mode {
                    debug!("Setting directory mode from {} to {:#o}", dir.mode, mode);
                    dir.mode = validate_mode(mode);
                    // POSIX: If non-root user sets mode with setgid bit and doesn't belong to directory's group, clear setgid
                    if creds.uid != 0
                        && (dir.mode & 0o2000) != 0
                        && !creds.is_member_of_group(dir.gid)
                    {
                        dir.mode &= !0o2000;
                    }
                }
                if let SetUid::Set(uid) = setattr.uid {
                    dir.uid = uid;
                    if creds.uid != 0 {
                        dir.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    dir.gid = gid;
                    // Clear SUID/SGID bits when non-root user calls chown with a gid
                    // This happens even if the gid doesn't actually change (POSIX behavior)
                    if creds.uid != 0 {
                        dir.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        dir.atime = t.seconds;
                        dir.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        dir.atime = now_sec;
                        dir.atime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        dir.mtime = t.seconds;
                        dir.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        dir.mtime = now_sec;
                        dir.mtime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    dir.ctime = now_sec;
                    dir.ctime_nsec = now_nsec;
                }
            }
            Inode::Symlink(symlink) => {
                if let SetMode::Set(mode) = setattr.mode {
                    symlink.mode = validate_mode(mode);
                }
                if let SetUid::Set(uid) = setattr.uid {
                    symlink.uid = uid;
                    if creds.uid != 0 {
                        symlink.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    symlink.gid = gid;
                    if creds.uid != 0 {
                        symlink.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        symlink.atime = t.seconds;
                        symlink.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.atime = now_sec;
                        symlink.atime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        symlink.mtime = t.seconds;
                        symlink.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.mtime = now_sec;
                        symlink.mtime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    symlink.ctime = now_sec;
                    symlink.ctime_nsec = now_nsec;
                }
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                if let SetMode::Set(mode) = setattr.mode {
                    special.mode = validate_mode(mode);
                }
                if let SetUid::Set(uid) = setattr.uid {
                    special.uid = uid;
                    if creds.uid != 0 {
                        special.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    special.gid = gid;
                    if creds.uid != 0 {
                        special.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        special.atime = t.seconds;
                        special.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (sec, nsec) = get_current_time();
                        special.atime = sec;
                        special.atime_nsec = nsec;
                    }
                    _ => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        special.mtime = t.seconds;
                        special.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (sec, nsec) = get_current_time();
                        special.mtime = sec;
                        special.mtime_nsec = nsec;
                    }
                    _ => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    special.ctime = now_sec;
                    special.ctime_nsec = now_nsec;
                }
            }
        }

        let mut txn = self.db.new_transaction()?;
        self.inode_store.save(&mut txn, id, &inode)?;

        if let Some(parent_id) = inode.parent()
            && let Some(name) = inode.name()
        {
            self.directory_store
                .update_inode_in_entry(&mut txn, parent_id, name, id, &inode)
                .await?;
        }

        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard).await?;

        self.tracer
            .emit(
                || self.resolve_path_lossy(id),
                FileOperation::Setattr {
                    mode: match setattr.mode {
                        SetMode::Set(m) => Some(m),
                        SetMode::NoChange => None,
                    },
                },
            )
            .await;

        Ok(InodeWithId { inode: &inode, id }.into())
    }

    pub async fn mknod(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        ftype: FileType,
        attr: &SetAttributes,
        rdev: Option<(u32, u32)>,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(name)?;

        debug!(
            "mknod: dirid={}, filename={}, ftype={:?}",
            dirid,
            String::from_utf8_lossy(name),
            ftype
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.inode_store.get(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                if self.directory_store.exists(dirid, name).await? {
                    debug!("File already exists");
                    return Err(FsError::Exists);
                }

                let special_id = self.inode_store.allocate();
                let (now_sec, now_nsec) = get_current_time();

                let base_mode = match ftype {
                    FileType::Fifo => 0o666,
                    FileType::CharDevice | FileType::BlockDevice => 0o666,
                    FileType::Socket => 0o666,
                    _ => return Err(FsError::InvalidArgument),
                };

                let final_mode = if let SetMode::Set(m) = attr.mode {
                    validate_mode(m)
                } else {
                    base_mode
                };

                let special_inode = SpecialInode {
                    mtime: now_sec,
                    mtime_nsec: now_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: now_sec,
                    atime_nsec: now_nsec,
                    mode: final_mode,
                    uid: match attr.uid {
                        SetUid::Set(u) => u,
                        _ => creds.uid,
                    },
                    gid: match attr.gid {
                        SetGid::Set(g) => g,
                        _ => creds.gid,
                    },
                    parent: Some(dirid),
                    name: Some(name.to_vec()),
                    nlink: 1,
                    rdev,
                };

                let inode = match ftype {
                    FileType::Fifo => Inode::Fifo(special_inode),
                    FileType::CharDevice => Inode::CharDevice(special_inode),
                    FileType::BlockDevice => Inode::BlockDevice(special_inode),
                    FileType::Socket => Inode::Socket(special_inode),
                    _ => return Err(FsError::InvalidArgument),
                };

                let mut txn = self.db.new_transaction()?;
                let cookie = self
                    .directory_store
                    .allocate_cookie(dirid, &mut txn)
                    .await?;

                self.inode_store.save(&mut txn, special_id, &inode)?;
                self.directory_store
                    .add(&mut txn, dirid, name, special_id, cookie, Some(&inode));

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                let stats_update = self.global_stats.prepare_inode_create(special_id).await;
                self.global_stats
                    .add_to_transaction(&stats_update, &mut txn)?;

                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;

                self.global_stats.commit_update(&stats_update);

                self.tracer
                    .emit(
                        || self.resolve_path_lossy(special_id),
                        FileOperation::Mknod { mode: final_mode },
                    )
                    .await;

                Ok((
                    special_id,
                    InodeWithId {
                        inode: &inode,
                        id: special_id,
                    }
                    .into(),
                ))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn remove(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        name: &[u8],
    ) -> Result<(), FsError> {
        validate_filename(name)?;

        let creds = Credentials::from_auth_context(auth);

        let (file_id, cookie) = self
            .directory_store
            .get_entry_with_cookie(dirid, name)
            .await?;

        let _guards = self
            .lock_manager
            .acquire_multiple_write(vec![dirid, file_id])
            .await;

        let mut dir_inode = self.inode_store.get(dirid).await?;
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let is_dir = matches!(dir_inode, Inode::Directory(_));
        if !is_dir {
            return Err(FsError::NotDirectory);
        }

        // Re-check inside lock to verify entry still points to same inode
        let (verified_id, verified_cookie) = self
            .directory_store
            .get_entry_with_cookie(dirid, name)
            .await?;
        if verified_id != file_id || verified_cookie != cookie {
            return Err(FsError::NotFound);
        }

        let mut file_inode = self.inode_store.get(file_id).await?;

        let original_nlink = match &file_inode {
            Inode::File(f) => f.nlink,
            Inode::Fifo(s) | Inode::Socket(s) | Inode::CharDevice(s) | Inode::BlockDevice(s) => {
                s.nlink
            }
            _ => 1,
        };

        check_sticky_bit_delete(&dir_inode, &file_inode, &creds)?;

        // Capture path before deletion for tracing (inode will be gone after)
        let trace_path = if self.tracer.has_subscribers() {
            Some(self.resolve_path_lossy(file_id).await)
        } else {
            None
        };

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let mut txn = self.db.new_transaction()?;
                let (now_sec, now_nsec) = get_current_time();

                match &mut file_inode {
                    Inode::File(file) => {
                        if file.nlink > 1 {
                            file.nlink -= 1;
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;

                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                        } else {
                            let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64);

                            if total_chunks as usize <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                                self.chunk_store
                                    .delete_range(&mut txn, file_id, 0, total_chunks);
                            } else {
                                self.tombstone_store.add(&mut txn, file_id, file.size);
                                self.stats
                                    .tombstones_created
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            self.inode_store.delete(&mut txn, file_id);
                            self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(FsError::NotEmpty);
                        }
                        self.inode_store.delete(&mut txn, file_id);
                        self.directory_store.delete_directory(&mut txn, file_id);
                        dir.nlink = dir.nlink.saturating_sub(1);
                        self.stats
                            .directories_deleted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Inode::Symlink(_) => {
                        self.inode_store.delete(&mut txn, file_id);
                        self.stats.links_deleted.fetch_add(1, Ordering::Relaxed);
                    }
                    Inode::Fifo(special)
                    | Inode::Socket(special)
                    | Inode::CharDevice(special)
                    | Inode::BlockDevice(special) => {
                        if special.nlink > 1 {
                            special.nlink -= 1;
                            special.ctime = now_sec;
                            special.ctime_nsec = now_nsec;

                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                        } else {
                            self.inode_store.delete(&mut txn, file_id);
                        }
                    }
                }

                self.directory_store
                    .unlink_entry(&mut txn, dirid, name, cookie);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                // For directories and symlinks: always remove from stats
                // For files and special files: only remove if this is the last link
                let (file_size, should_always_remove_stats) = match &file_inode {
                    Inode::File(f) => (Some(f.size), false),
                    Inode::Directory(_) | Inode::Symlink(_) => (None, true),
                    _ => (None, false),
                };

                let stats_update = if should_always_remove_stats || original_nlink <= 1 {
                    Some(
                        self.global_stats
                            .prepare_inode_remove(file_id, file_size)
                            .await,
                    )
                } else {
                    None
                };

                if let Some(ref update) = stats_update {
                    self.global_stats.add_to_transaction(update, &mut txn)?;
                }

                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                if let Some(path) = trace_path {
                    self.tracer
                        .emit(|| async { path }, FileOperation::Remove)
                        .await;
                }

                Ok(())
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn rename(
        &self,
        auth: &AuthContext,
        from_dirid: u64,
        from_name: &[u8],
        to_dirid: u64,
        to_name: &[u8],
    ) -> Result<(), FsError> {
        if from_name.is_empty() || to_name.is_empty() {
            return Err(FsError::InvalidArgument);
        }

        validate_filename(from_name)?;
        validate_filename(to_name)?;

        if from_name == b"." || from_name == b".." {
            return Err(FsError::InvalidArgument);
        }
        if to_name == b"." || to_name == b".." {
            return Err(FsError::Exists);
        }

        if from_dirid == to_dirid && from_name == to_name {
            return Ok(());
        }

        debug!(
            "rename: from_dir={}, from_name={}, to_dir={}, to_name={}",
            from_dirid,
            String::from_utf8_lossy(from_name),
            to_dirid,
            String::from_utf8_lossy(to_name)
        );

        let creds = Credentials::from_auth_context(auth);

        // Look up all inode IDs without holding any locks
        let (source_inode_id, source_cookie) = self
            .directory_store
            .get_entry_with_cookie(from_dirid, from_name)
            .await?;

        if to_dirid == source_inode_id {
            return Err(FsError::InvalidArgument);
        }

        let target_entry = match self
            .directory_store
            .get_entry_with_cookie(to_dirid, to_name)
            .await
        {
            Ok((id, cookie)) => Some((id, cookie)),
            Err(FsError::NotFound) => None,
            Err(e) => return Err(e),
        };
        let target_inode_id = target_entry.map(|(id, _)| id);

        let mut all_inodes_to_lock = vec![from_dirid, source_inode_id];
        if from_dirid != to_dirid {
            all_inodes_to_lock.push(to_dirid);
        }
        if let Some(target_id) = target_inode_id {
            all_inodes_to_lock.push(target_id);
        }

        let _guards = self
            .lock_manager
            .acquire_multiple_write(all_inodes_to_lock)
            .await;

        // Re-verify inside lock that entries still point to same inodes
        let (verified_source_id, verified_source_cookie) = self
            .directory_store
            .get_entry_with_cookie(from_dirid, from_name)
            .await?;
        if verified_source_id != source_inode_id || verified_source_cookie != source_cookie {
            return Err(FsError::StaleHandle);
        }

        let verified_target_entry = match self
            .directory_store
            .get_entry_with_cookie(to_dirid, to_name)
            .await
        {
            Ok((id, cookie)) => Some((id, cookie)),
            Err(FsError::NotFound) => None,
            Err(e) => return Err(e),
        };
        if verified_target_entry.map(|(id, _)| id) != target_inode_id {
            return Err(FsError::StaleHandle);
        }
        let target_cookie = verified_target_entry.map(|(_, cookie)| cookie);

        let mut source_inode = self.inode_store.get(source_inode_id).await?;
        if matches!(source_inode, Inode::Directory(_))
            && self.is_ancestor_of(source_inode_id, to_dirid).await?
        {
            return Err(FsError::InvalidArgument);
        }

        let mut from_dir = self.inode_store.get(from_dirid).await?;
        let mut to_dir = if from_dirid != to_dirid {
            Some(self.inode_store.get(to_dirid).await?)
        } else {
            None
        };

        check_access(&from_dir, &creds, AccessMode::Write)?;
        check_access(&from_dir, &creds, AccessMode::Execute)?;
        if let Some(ref to_dir) = to_dir {
            check_access(to_dir, &creds, AccessMode::Write)?;
            check_access(to_dir, &creds, AccessMode::Execute)?;
        }

        check_sticky_bit_delete(&from_dir, &source_inode, &creds)?;

        // Capture old path before rename for tracing (name will change after)
        let trace_old_path = if self.tracer.has_subscribers() {
            Some(self.resolve_path_lossy(source_inode_id).await)
        } else {
            None
        };

        // POSIX: Moving directories in sticky directories requires ownership of the moved directory
        if from_dirid != to_dirid
            && matches!(source_inode, Inode::Directory(_))
            && let Inode::Directory(from_dir_data) = &from_dir
            && from_dir_data.mode & 0o1000 != 0
        {
            let source_uid = match &source_inode {
                Inode::Directory(d) => d.uid,
                _ => unreachable!(),
            };
            if creds.uid != 0 && creds.uid != source_uid {
                return Err(FsError::PermissionDenied);
            }
        }

        let same_inode = target_inode_id == Some(source_inode_id);

        let target = if let Some(target_id) = target_inode_id {
            if same_inode {
                None
            } else {
                let inode = self.inode_store.get(target_id).await?;
                if let Inode::Directory(dir) = &inode
                    && dir.entry_count > 0
                {
                    return Err(FsError::NotEmpty);
                }

                let target_dir = if let Some(ref to_dir) = to_dir {
                    to_dir
                } else {
                    &from_dir
                };
                check_sticky_bit_delete(target_dir, &inode, &creds)?;
                Some((target_id, inode))
            }
        } else {
            None
        };

        let mut txn = self.db.new_transaction()?;

        let mut target_was_directory = false;
        let mut target_stats_update = None;
        if let Some((target_id, existing_inode)) = target {
            target_was_directory = matches!(existing_inode, Inode::Directory(_));

            let (original_nlink, original_file_size, should_always_remove_stats) =
                match &existing_inode {
                    Inode::File(f) => (f.nlink, Some(f.size), false),
                    Inode::Directory(_) | Inode::Symlink(_) => (1, None, true),
                    Inode::Fifo(s)
                    | Inode::Socket(s)
                    | Inode::CharDevice(s)
                    | Inode::BlockDevice(s) => (s.nlink, None, false),
                };

            macro_rules! handle_special_file {
                ($special:expr, $inode_variant:ident) => {
                    if $special.nlink > 1 {
                        $special.nlink -= 1;
                        let (now_sec, now_nsec) = get_current_time();
                        $special.ctime = now_sec;
                        $special.ctime_nsec = now_nsec;

                        self.inode_store.save(
                            &mut txn,
                            target_id,
                            &Inode::$inode_variant($special),
                        )?;
                    } else {
                        self.inode_store.delete(&mut txn, target_id);
                    }
                };
            }

            match existing_inode {
                Inode::File(mut file) => {
                    if file.nlink > 1 {
                        file.nlink -= 1;
                        let (now_sec, now_nsec) = get_current_time();
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;

                        self.inode_store
                            .save(&mut txn, target_id, &Inode::File(file))?;
                    } else {
                        let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64);

                        if total_chunks as usize <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                            self.chunk_store
                                .delete_range(&mut txn, target_id, 0, total_chunks);
                        } else {
                            self.tombstone_store.add(&mut txn, target_id, file.size);
                            self.stats
                                .tombstones_created
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        self.inode_store.delete(&mut txn, target_id);
                    }
                }
                Inode::Directory(_) => {
                    self.inode_store.delete(&mut txn, target_id);
                    self.directory_store.delete_directory(&mut txn, target_id);
                }
                Inode::Symlink(_) => {
                    self.inode_store.delete(&mut txn, target_id);
                }
                Inode::Fifo(mut special) => {
                    handle_special_file!(special, Fifo);
                }
                Inode::Socket(mut special) => {
                    handle_special_file!(special, Socket);
                }
                Inode::CharDevice(mut special) => {
                    handle_special_file!(special, CharDevice);
                }
                Inode::BlockDevice(mut special) => {
                    handle_special_file!(special, BlockDevice);
                }
            }

            // For directories and symlinks: always remove from stats
            // For files and special files: only remove if this is the last link
            if should_always_remove_stats || original_nlink <= 1 {
                target_stats_update = Some(
                    self.global_stats
                        .prepare_inode_remove(target_id, original_file_size)
                        .await,
                );
            }

            self.directory_store
                .unlink_entry(&mut txn, to_dirid, to_name, target_cookie.unwrap());
        } else if same_inode {
            self.directory_store
                .unlink_entry(&mut txn, to_dirid, to_name, target_cookie.unwrap());
        }

        self.directory_store
            .unlink_entry(&mut txn, from_dirid, from_name, source_cookie);

        let dir_changed = from_dirid != to_dirid;
        let (now_sec, now_nsec) = get_current_time();
        match &mut source_inode {
            Inode::Directory(d) => {
                if dir_changed {
                    d.parent = to_dirid;
                }
                d.name = Some(to_name.to_vec());
            }
            Inode::File(f) => {
                if same_inode {
                    f.nlink = f.nlink.saturating_sub(1);
                    f.ctime = now_sec;
                    f.ctime_nsec = now_nsec;
                }
                if f.nlink == 1 {
                    f.parent = Some(to_dirid);
                    f.name = Some(to_name.to_vec());
                }
            }
            Inode::Symlink(s) => {
                if s.nlink == 1 {
                    if dir_changed {
                        s.parent = Some(to_dirid);
                    }
                    s.name = Some(to_name.to_vec());
                }
            }
            Inode::Fifo(s) | Inode::Socket(s) | Inode::CharDevice(s) | Inode::BlockDevice(s) => {
                if same_inode {
                    s.nlink = s.nlink.saturating_sub(1);
                    s.ctime = now_sec;
                    s.ctime_nsec = now_nsec;
                }
                if s.nlink == 1 {
                    s.parent = Some(to_dirid);
                    s.name = Some(to_name.to_vec());
                }
            }
        }

        let new_cookie = if from_dirid == to_dirid {
            source_cookie
        } else {
            self.directory_store
                .allocate_cookie(to_dirid, &mut txn)
                .await?
        };

        let embed_inode = if source_inode.nlink() == 1 {
            Some(&source_inode)
        } else {
            None
        };
        self.directory_store.add(
            &mut txn,
            to_dirid,
            to_name,
            source_inode_id,
            new_cookie,
            embed_inode,
        );

        self.inode_store
            .save(&mut txn, source_inode_id, &source_inode)?;

        let is_moved_dir = matches!(source_inode, Inode::Directory(_));

        if let Inode::Directory(d) = &mut from_dir {
            if from_dirid != to_dirid {
                // Moving to different directory: source leaves from_dir
                d.entry_count = d.entry_count.saturating_sub(1);
                if is_moved_dir {
                    d.nlink = d.nlink.saturating_sub(1);
                }
            } else if target_inode_id.is_some() {
                // Same directory with target: only target was removed (source renamed in place)
                d.entry_count = d.entry_count.saturating_sub(1);
            }
            // If same dir without target: entry_count unchanged (rename only)
            d.mtime = now_sec;
            d.mtime_nsec = now_nsec;
            d.ctime = now_sec;
            d.ctime_nsec = now_nsec;
        }
        self.inode_store.save(&mut txn, from_dirid, &from_dir)?;

        if let Some(ref mut to_dir) = to_dir {
            if let Inode::Directory(d) = to_dir {
                if target_inode_id.is_none() {
                    d.entry_count += 1;
                }
                if is_moved_dir && (target_inode_id.is_none() || !target_was_directory) {
                    if d.nlink == u32::MAX {
                        return Err(FsError::NoSpace);
                    }
                    d.nlink += 1;
                }
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            self.inode_store.save(&mut txn, to_dirid, to_dir)?;
        }

        if let Some(ref update) = target_stats_update {
            self.global_stats.add_to_transaction(update, &mut txn)?;
        }

        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard).await?;

        if let Some(update) = target_stats_update {
            self.global_stats.commit_update(&update);
        }

        match source_inode {
            Inode::File(_) => {
                self.stats.files_renamed.fetch_add(1, Ordering::Relaxed);
            }
            Inode::Directory(_) => {
                self.stats
                    .directories_renamed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Inode::Symlink(_) => {
                self.stats.links_renamed.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        // Emit rename event with old path and new path
        if let Some(old_path) = trace_old_path {
            let to_dir_path = self.resolve_path_lossy(to_dirid).await;
            let new_path = format!(
                "{}/{}",
                to_dir_path.trim_end_matches('/'),
                String::from_utf8_lossy(to_name)
            );
            self.tracer
                .emit(|| async { old_path }, FileOperation::Rename { new_path })
                .await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::inode::FileInode;
    use crate::test_helpers::test_helpers_mod::test_auth;

    fn test_creds() -> Credentials {
        Credentials::from_auth_context(&(&test_auth()).into())
    }

    #[tokio::test]
    async fn test_create_filesystem() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match root_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.mode, 0o1777);
                let (expected_uid, expected_gid) = get_current_uid_gid();
                assert_eq!(dir.uid, expected_uid);
                assert_eq!(dir.gid, expected_gid);
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Root should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_allocate_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let inode1 = fs.inode_store.allocate();
        let inode2 = fs.inode_store.allocate();
        let inode3 = fs.inode_store.allocate();

        assert_ne!(inode1, 0);
        assert_ne!(inode2, 0);
        assert_ne!(inode3, 0);
        assert_ne!(inode1, inode2);
        assert_ne!(inode2, inode3);
        assert_ne!(inode1, inode3);
    }

    #[tokio::test]
    async fn test_save_and_load_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let file_inode = FileInode {
            size: 1024,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: Some(0),
            name: Some(b"test.txt".to_vec()),
            nlink: 1,
        };

        let inode = Inode::File(file_inode.clone());
        let inode_id = fs.inode_store.allocate();

        let mut txn = fs.db.new_transaction().unwrap();
        fs.inode_store.save(&mut txn, inode_id, &inode).unwrap();
        let mut seq_guard = fs.write_coordinator.allocate_sequence();
        fs.commit_transaction(txn, &mut seq_guard).await.unwrap();

        let loaded_inode = fs.inode_store.get(inode_id).await.unwrap();
        match loaded_inode {
            Inode::File(f) => {
                assert_eq!(f.size, file_inode.size);
                assert_eq!(f.mtime, file_inode.mtime);
                assert_eq!(f.ctime, file_inode.ctime);
                assert_eq!(f.mode, file_inode.mode);
                assert_eq!(f.uid, file_inode.uid);
                assert_eq!(f.gid, file_inode.gid);
            }
            _ => panic!("Expected File inode"),
        }
    }

    #[tokio::test]
    async fn test_inode_key_generation() {
        use crate::fs::key_codec::{KeyCodec, KeyPrefix};
        // Test binary key format: [PREFIX_INODE | inode_id(8 bytes BE)]
        let key0 = KeyCodec::inode_key(0);
        assert_eq!(key0[0], u8::from(KeyPrefix::Inode));
        assert_eq!(&key0[1..9], &0u64.to_be_bytes());

        let key42 = KeyCodec::inode_key(42);
        assert_eq!(key42[0], u8::from(KeyPrefix::Inode));
        assert_eq!(&key42[1..9], &42u64.to_be_bytes());

        let key999 = KeyCodec::inode_key(999);
        assert_eq!(key999[0], u8::from(KeyPrefix::Inode));
        assert_eq!(&key999[1..9], &999u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_chunk_key_generation() {
        use crate::fs::key_codec::{KeyCodec, KeyPrefix};
        // Test binary key format: [PREFIX_CHUNK | inode_id(8 bytes BE) | chunk_index(8 bytes BE)]
        let key = KeyCodec::chunk_key(1, 0);
        assert_eq!(key[0], u8::from(KeyPrefix::Chunk));
        assert_eq!(&key[1..9], &1u64.to_be_bytes());
        assert_eq!(&key[9..17], &0u64.to_be_bytes());

        let key = KeyCodec::chunk_key(42, 10);
        assert_eq!(key[0], u8::from(KeyPrefix::Chunk));
        assert_eq!(&key[1..9], &42u64.to_be_bytes());
        assert_eq!(&key[9..17], &10u64.to_be_bytes());

        let key = KeyCodec::chunk_key(999, 999);
        assert_eq!(key[0], u8::from(KeyPrefix::Chunk));
        assert_eq!(&key[1..9], &999u64.to_be_bytes());
        assert_eq!(&key[9..17], &999u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_load_nonexistent_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let result = fs.inode_store.get(999).await;
        match result {
            Err(FsError::NotFound) => {} // Expected
            other => panic!("Expected NFS3ERR_NOENT, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_read_only_mode_operations() {
        use slatedb::DbBuilder;
        use slatedb::object_store::path::Path;

        let object_store = slatedb::object_store::memory::InMemory::new();
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(object_store);

        let test_key = [0u8; 32];
        let settings = slatedb::config::Settings {
            compression_codec: None,
            compactor_options: Some(slatedb::config::CompactorOptions {
                ..Default::default()
            }),
            ..Default::default()
        };

        let db_path = Path::from("test_slatedb");
        let slatedb = Arc::new(
            DbBuilder::new(db_path.clone(), object_store.clone())
                .with_settings(settings)
                .build()
                .await
                .unwrap(),
        );

        let fs_rw = ZeroFS::new_with_slatedb(
            crate::encryption::SlateDbHandle::ReadWrite(slatedb),
            test_key,
            u64::MAX,
            CompressionConfig::default(),
        )
        .await
        .unwrap();

        let test_inode_id = fs_rw.inode_store.allocate();
        let file_inode = FileInode {
            size: 2048,
            mtime: 1234567890,
            mtime_nsec: 123456789,
            ctime: 1234567891,
            ctime_nsec: 234567890,
            atime: 1234567892,
            atime_nsec: 345678901,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: Some(0),
            name: Some(b"test.txt".to_vec()),
            nlink: 1,
        };
        let mut txn = fs_rw.db.new_transaction().unwrap();
        fs_rw
            .inode_store
            .save(&mut txn, test_inode_id, &Inode::File(file_inode.clone()))
            .unwrap();
        let mut seq_guard = fs_rw.write_coordinator.allocate_sequence();
        fs_rw.commit_transaction(txn, &mut seq_guard).await.unwrap();

        fs_rw.flush_coordinator.flush().await.unwrap();
        drop(fs_rw);

        let fs_ro = ZeroFS::new_in_memory_read_only(object_store, test_key)
            .await
            .unwrap();

        let root_inode = fs_ro.inode_store.get(0).await.unwrap();
        assert!(matches!(root_inode, Inode::Directory(_)));

        let loaded_inode = fs_ro.inode_store.get(test_inode_id).await.unwrap();
        match loaded_inode {
            Inode::File(f) => {
                assert_eq!(f.size, file_inode.size);
                assert_eq!(f.mode, file_inode.mode);
            }
            _ => panic!("Expected File inode"),
        }

        // Verify that creating transactions fails in read-only mode
        let result = fs_ro.db.new_transaction();
        assert!(
            result.is_err(),
            "new_transaction should fail in read-only mode"
        );
    }

    // === Tests from operations.rs ===

    #[tokio::test]
    async fn test_process_create_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let attr = SetAttributes {
            mode: SetMode::Set(0o644),
            uid: SetUid::Set(1000),
            gid: SetGid::Set(1000),
            ..Default::default()
        };

        let (file_id, fattr) = fs
            .create(&test_creds(), 0, b"test.txt", &attr)
            .await
            .unwrap();

        assert!(file_id > 0);
        assert_eq!(fattr.mode, 0o644);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.size, 0);

        // Check that the file was added to the directory
        let entry_key = KeyCodec::dir_entry_key(0, b"test.txt");
        let entry_data = fs.db.get_bytes(&entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_create_file_already_exists() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let attr = &SetAttributes::default();

        let _ = fs
            .create(&test_creds(), 0, b"test.txt", attr)
            .await
            .unwrap();

        let result = fs.create(&test_creds(), 0, b"test.txt", attr).await;
        assert!(matches!(result, Err(FsError::Exists)));
    }

    #[tokio::test]
    async fn test_process_mkdir() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        assert!(dir_id > 0);
        assert_eq!(fattr.mode, 0o777);
        assert_eq!(fattr.file_type, FileType::Directory);

        let new_dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match new_dir_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_mkdir_with_custom_attrs() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Test with custom mode
        let custom_attrs = SetAttributes {
            mode: SetMode::Set(0o700),
            uid: SetUid::Set(1001),
            gid: SetGid::Set(1001),
            size: SetSize::NoChange,
            atime: SetTime::SetToClientTime(crate::fs::types::Timestamp {
                seconds: 1234567890,
                nanoseconds: 0,
            }),
            mtime: SetTime::SetToClientTime(crate::fs::types::Timestamp {
                seconds: 1234567890,
                nanoseconds: 0,
            }),
        };

        let (_dir_id, fattr) = fs
            .mkdir(&test_creds(), 0, b"customdir", &custom_attrs)
            .await
            .unwrap();

        // Check that attributes were applied correctly
        assert_eq!(fattr.mode & 0o777, 0o700, "Custom mode should be applied");
        assert_eq!(fattr.uid, 1001, "Custom uid should be applied");
        assert_eq!(fattr.gid, 1001, "Custom gid should be applied");
        assert_eq!(
            fattr.atime.seconds, 1234567890,
            "Custom atime should be applied"
        );
        assert_eq!(
            fattr.mtime.seconds, 1234567890,
            "Custom mtime should be applied"
        );
    }

    #[tokio::test]
    async fn test_process_write_and_read() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = b"Hello, World!";
        let fattr = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::copy_from_slice(data),
            )
            .await
            .unwrap();

        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data.as_ref(), data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_write_partial_chunks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data1 = vec![b'A'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&data1),
        )
        .await
        .unwrap();

        let data2 = vec![b'B'; 50];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            50,
            &Bytes::copy_from_slice(&data2),
        )
        .await
        .unwrap();

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 100);
        assert_eq!(&read_data[0..50], &vec![b'A'; 50]);
        assert_eq!(&read_data[50..100], &vec![b'B'; 50]);
    }

    #[tokio::test]
    async fn test_process_write_across_chunks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"bigfile.txt", &SetAttributes::default())
            .await
            .unwrap();

        let chunk_size = CHUNK_SIZE;
        let data = vec![b'X'; chunk_size * 2 + 1024];

        let fattr = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::copy_from_slice(&data),
            )
            .await
            .unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data.as_ref(), &data[..]);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_remove_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"some data".to_vec()),
        )
        .await
        .unwrap();

        fs.remove(&(&test_auth()).into(), 0, b"test.txt")
            .await
            .unwrap();

        // Check that the file was removed from the directory
        let entry_key = KeyCodec::dir_entry_key(0, b"test.txt");
        let entry_data = fs.db.get_bytes(&entry_key).await.unwrap();
        assert!(entry_data.is_none());

        let result = fs.inode_store.get(file_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_remove_empty_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.remove(&(&test_auth()).into(), 0, b"testdir")
            .await
            .unwrap();

        let result = fs.inode_store.get(dir_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_remove_non_empty_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.create(
            &test_creds(),
            dir_id,
            b"file.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();

        let result = fs.remove(&(&test_auth()).into(), 0, b"testdir").await;
        assert!(matches!(result, Err(FsError::NotEmpty)));
    }

    #[tokio::test]
    async fn test_process_rename_same_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"old.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.rename(&(&test_auth()).into(), 0, b"old.txt", 0, b"new.txt")
            .await
            .unwrap();

        // Check old entry is gone and new entry exists
        let old_entry_key = KeyCodec::dir_entry_key(0, b"old.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        let new_entry_key = KeyCodec::dir_entry_key(0, b"new.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_rename_replace_existing() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create two files
        let (file1_id, _) = fs
            .create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(
            &(&test_auth()).into(),
            file1_id,
            0,
            &Bytes::from(b"content1".to_vec()),
        )
        .await
        .unwrap();

        let (file2_id, _) = fs
            .create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(
            &(&test_auth()).into(),
            file2_id,
            0,
            &Bytes::from(b"content2".to_vec()),
        )
        .await
        .unwrap();

        fs.rename(&(&test_auth()).into(), 0, b"file1.txt", 0, b"file2.txt")
            .await
            .unwrap();

        // Check that file1.txt no longer exists
        let old_entry_key = KeyCodec::dir_entry_key(0, b"file1.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        // Check that file2.txt exists and has file1's content
        let new_entry_key = KeyCodec::dir_entry_key(0, b"file2.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file1_id);

        // Verify content
        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file1_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data.as_ref(), b"content1");

        // Check that the original file2 inode is gone
        let result = fs.inode_store.get(file2_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_rename_across_directories() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir1_id, _) = fs
            .mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(&test_creds(), 0, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .create(
                &test_creds(),
                dir1_id,
                b"file.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        fs.rename(
            &(&test_auth()).into(),
            dir1_id,
            b"file.txt",
            dir2_id,
            b"moved.txt",
        )
        .await
        .unwrap();

        // Check file removed from dir1
        let old_entry_key = KeyCodec::dir_entry_key(dir1_id, b"file.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        // Check file added to dir2
        let new_entry_key = KeyCodec::dir_entry_key(dir2_id, b"moved.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);

        // Check entry counts
        let dir1_inode = fs.inode_store.get(dir1_id).await.unwrap();
        match dir1_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }

        let dir2_inode = fs.inode_store.get(dir2_id).await.unwrap();
        match dir2_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 1);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_rename_directory_entry_count() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a directory with two files
        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(
            &test_creds(),
            dir_id,
            b"file1.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();
        fs.create(
            &test_creds(),
            dir_id,
            b"file2.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();

        // Check initial entry count
        let dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 2),
            _ => panic!("Should be a directory"),
        }

        fs.rename(
            &(&test_auth()).into(),
            dir_id,
            b"file1.txt",
            dir_id,
            b"file2.txt",
        )
        .await
        .unwrap();

        // Check that entry count decreased by 1
        let dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 1),
            _ => panic!("Should be a directory"),
        }

        fs.remove(&(&test_auth()).into(), dir_id, b"file2.txt")
            .await
            .unwrap();

        // Directory should now be empty and removable
        let dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 0),
            _ => panic!("Should be a directory"),
        }

        // Should be able to remove the empty directory
        fs.remove(&(&test_auth()).into(), 0, b"testdir")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_setattr_file_size() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(vec![b'A'; 1000]),
        )
        .await
        .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(500),
            ..Default::default()
        };

        let fattr = fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();
        assert_eq!(fattr.size, 500);

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(read_data.len(), 500);
    }

    #[tokio::test]
    async fn test_read_beyond_truncated_chunk() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![b'A'; 300 * 1024];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&data),
        )
        .await
        .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(100 * 1024),
            ..Default::default()
        };
        fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 200 * 1024, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 0);
    }

    #[tokio::test]
    async fn test_process_symlink() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let target = b"/path/to/target";
        let attr = &SetAttributes::default();

        let (link_id, fattr) = fs
            .symlink(&test_creds(), 0, b"link", target, attr)
            .await
            .unwrap();

        assert!(link_id > 0);
        assert_eq!(fattr.file_type, FileType::Symlink);
        assert_eq!(fattr.size, target.len() as u64);

        let link_inode = fs.inode_store.get(link_id).await.unwrap();
        match link_inode {
            Inode::Symlink(symlink) => {
                assert_eq!(symlink.target, target);
            }
            _ => panic!("Should be a symlink"),
        }
    }

    #[tokio::test]
    async fn test_process_readdir() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        fs.create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();

        assert!(result.end);
        assert_eq!(result.entries.len(), 5);

        assert_eq!(result.entries[0].name, b".");
        assert_eq!(result.entries[1].name, b"..");

        let names: Vec<&[u8]> = result.entries[2..]
            .iter()
            .map(|e| e.name.as_ref())
            .collect();
        assert!(names.contains(&b"file1.txt".as_ref()));
        assert!(names.contains(&b"file2.txt".as_ref()));
        assert!(names.contains(&b"dir1".as_ref()));
    }

    #[tokio::test]
    async fn test_process_readdir_pagination() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        for i in 0..10 {
            fs.create(
                &test_creds(),
                0,
                format!("file{i}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let result1 = fs.readdir(&(&test_auth()).into(), 0, 0, 5).await.unwrap();
        assert!(!result1.end);
        assert_eq!(result1.entries.len(), 5);

        let last_id = result1.entries.last().unwrap().fileid;
        let result2 = fs
            .readdir(&(&test_auth()).into(), 0, last_id, 10)
            .await
            .unwrap();
        assert!(result2.end);
    }

    #[tokio::test]
    async fn test_process_rename_prevent_directory_cycles() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c
        let (a_id, _) = fs
            .mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();

        // Test 1: Try to rename /a into /a/b (direct descendant)
        let result = fs
            .rename(&(&test_auth()).into(), 0, b"a", b_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 2: Try to rename /a into /a/b/c (deeper descendant)
        let result = fs
            .rename(&(&test_auth()).into(), 0, b"a", c_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 3: Try to rename /a/b into /a/b/c (moving into immediate child)
        let result = fs
            .rename(&(&test_auth()).into(), a_id, b"b", c_id, b"b_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 4: Valid rename - moving /a/b/c to root
        let result = fs
            .rename(&(&test_auth()).into(), b_id, b"c", 0, b"c_moved")
            .await;
        assert!(result.is_ok());

        // Test 5: Valid rename - moving a file (not a directory) should work
        let (_file_id, _) = fs
            .create(&test_creds(), a_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();
        let result = fs
            .rename(
                &(&test_auth()).into(),
                a_id,
                b"file.txt",
                b_id,
                b"file_moved.txt",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_is_ancestor_of() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c/d
        let (a_id, _) = fs
            .mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();
        let (d_id, _) = fs
            .mkdir(&test_creds(), c_id, b"d", &SetAttributes::default())
            .await
            .unwrap();

        // Test ancestry relationships
        assert!(fs.is_ancestor_of(a_id, b_id).await.unwrap());
        assert!(fs.is_ancestor_of(a_id, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(a_id, d_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, d_id).await.unwrap());
        assert!(fs.is_ancestor_of(c_id, d_id).await.unwrap());

        // Test non-ancestry relationships
        assert!(!fs.is_ancestor_of(b_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(c_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(c_id, b_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, b_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, c_id).await.unwrap());

        // Test root relationships
        assert!(fs.is_ancestor_of(0, a_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, b_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, d_id).await.unwrap());
        assert!(!fs.is_ancestor_of(a_id, 0).await.unwrap());

        // Test self-relationships (should return true)
        assert!(fs.is_ancestor_of(a_id, a_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, b_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_readdir_with_hardlinks_stable_cookies() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create files with hardlinks
        let (file1_id, _) = fs
            .create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Create hardlinks
        fs.link(&(&test_auth()).into(), file1_id, 0, b"hardlink1.txt")
            .await
            .unwrap();
        fs.link(&(&test_auth()).into(), file1_id, 0, b"hardlink2.txt")
            .await
            .unwrap();

        // Create another file
        let (file2_id, _) = fs
            .create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();

        // First readdir - get all entries
        let result1 = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();
        assert_eq!(result1.entries.len(), 6); // . .. file1.txt hardlink1.txt hardlink2.txt file2.txt

        // With stable cookies, hardlinks have the SAME fileid (raw inode) but DIFFERENT cookies
        let file1_entry = &result1.entries[2];
        let hardlink1_entry = &result1.entries[3];
        let hardlink2_entry = &result1.entries[4];
        let file2_entry = &result1.entries[5];

        // All hardlinks point to the same inode
        assert_eq!(file1_entry.fileid, file1_id);
        assert_eq!(hardlink1_entry.fileid, file1_id);
        assert_eq!(hardlink2_entry.fileid, file1_id);
        assert_eq!(file2_entry.fileid, file2_id);

        // Cookies are unique and stable for pagination
        assert_ne!(file1_entry.cookie, hardlink1_entry.cookie);
        assert_ne!(hardlink1_entry.cookie, hardlink2_entry.cookie);
        assert_ne!(hardlink2_entry.cookie, file2_entry.cookie);

        // Test pagination using cookies - start after the first hardlink
        let result2 = fs
            .readdir(&(&test_auth()).into(), 0, hardlink1_entry.cookie, 10)
            .await
            .unwrap();
        assert_eq!(result2.entries.len(), 2); // hardlink2.txt file2.txt
        assert_eq!(result2.entries[0].name, b"hardlink2.txt");
        assert_eq!(result2.entries[1].name, b"file2.txt");
    }

    #[tokio::test]
    async fn test_max_hardlinks_limit() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a file
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Manually set nlink to just below the limit to avoid creating 65k files
        let mut inode = fs.inode_store.get(file_id).await.unwrap();
        match &mut inode {
            Inode::File(file) => {
                file.nlink = MAX_HARDLINKS_PER_INODE - 1;
            }
            _ => panic!("Expected file inode"),
        }
        let mut txn = fs.db.new_transaction().unwrap();
        fs.inode_store.save(&mut txn, file_id, &inode).unwrap();
        let mut seq_guard = fs.write_coordinator.allocate_sequence();
        fs.commit_transaction(txn, &mut seq_guard).await.unwrap();

        // Create one more hardlink - should succeed
        let result = fs
            .link(&(&test_auth()).into(), file_id, 0, b"last_link.txt")
            .await;
        assert!(result.is_ok());

        // Verify the file now has exactly MAX_HARDLINKS_PER_INODE links
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            Inode::File(file) => {
                assert_eq!(file.nlink, MAX_HARDLINKS_PER_INODE);
            }
            _ => panic!("Expected file inode"),
        }

        // Try to create one more hardlink - should fail
        let result = fs
            .link(&(&test_auth()).into(), file_id, 0, b"one_too_many.txt")
            .await;
        assert!(matches!(result, Err(FsError::TooManyLinks)));

        // Verify the file still has MAX_HARDLINKS_PER_INODE links
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            Inode::File(file) => {
                assert_eq!(file.nlink, MAX_HARDLINKS_PER_INODE);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_rename_hardlink_over_same_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            Inode::File(f) => assert_eq!(f.nlink, 1, "Initial nlink should be 1"),
            _ => panic!("Expected file inode"),
        }

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            Inode::File(f) => assert_eq!(f.nlink, 2, "After link, nlink should be 2"),
            _ => panic!("Expected file inode"),
        }

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match &root_inode {
            Inode::Directory(d) => assert_eq!(d.entry_count, 2, "Directory should have 2 entries"),
            _ => panic!("Expected directory inode"),
        }

        fs.rename(
            &(&test_auth()).into(),
            0,
            b"hardlink.txt",
            0,
            b"original.txt",
        )
        .await
        .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            Inode::File(f) => {
                assert_eq!(f.nlink, 1, "After rename, nlink should be 1");
                // Since nlink is 1, parent and name should be set
                assert_eq!(f.parent, Some(0), "Parent should be set to root");
                assert_eq!(f.name, Some(b"original.txt".to_vec()), "Name should be set");
            }
            _ => panic!("Expected file inode"),
        }

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match &root_inode {
            Inode::Directory(d) => assert_eq!(d.entry_count, 1, "Directory should have 1 entry"),
            _ => panic!("Expected directory inode"),
        }

        let result = fs.directory_store.get(0, b"hardlink.txt").await;
        assert!(matches!(result, Err(FsError::NotFound)));

        let stored_id = fs.directory_store.get(0, b"original.txt").await.unwrap();
        assert_eq!(stored_id, file_id, "original.txt should point to the file");

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data.as_ref(), b"test content");

        fs.remove(&(&test_auth()).into(), 0, b"original.txt")
            .await
            .unwrap();

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match &root_inode {
            Inode::Directory(d) => assert_eq!(d.entry_count, 0, "Directory should be empty"),
            _ => panic!("Expected directory inode"),
        }

        let result = fs.inode_store.get(file_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_parent_directory_execute_permissions() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"test_dir", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .create(
                &test_creds(),
                dir_id,
                b"test.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"initial data".to_vec()),
        )
        .await
        .unwrap();

        let no_exec_attrs = SetAttributes {
            mode: SetMode::Set(0o644),
            ..Default::default()
        };

        fs.setattr(&test_creds(), dir_id, &no_exec_attrs)
            .await
            .unwrap();

        let chmod_attrs = SetAttributes {
            mode: SetMode::Set(0o600),
            ..Default::default()
        };

        let result = fs.setattr(&test_creds(), file_id, &chmod_attrs).await;
        assert!(matches!(result, Err(FsError::PermissionDenied)));

        let result = fs.read_file(&(&test_auth()).into(), file_id, 0, 100).await;
        assert!(matches!(result, Err(FsError::PermissionDenied)));

        let result = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::from(b"new data".to_vec()),
            )
            .await;
        assert!(matches!(result, Err(FsError::PermissionDenied)));

        let exec_attrs = SetAttributes {
            mode: SetMode::Set(0o755),
            ..Default::default()
        };

        fs.setattr(&test_creds(), dir_id, &exec_attrs)
            .await
            .unwrap();

        fs.setattr(&test_creds(), file_id, &chmod_attrs)
            .await
            .unwrap();

        let (data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"initial data");

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"updated data".to_vec()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_dir_scan_entry_embeds_inode_on_create() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        assert_eq!(entry.name, b"test.txt");
        assert_eq!(entry.inode_id, file_id);
        assert!(
            entry.inode.is_some(),
            "Newly created file should have embedded inode"
        );

        let embedded = entry.inode.unwrap();
        match embedded {
            Inode::File(f) => {
                assert_eq!(f.nlink, 1);
                assert!(f.parent.is_some());
                assert!(f.name.is_some());
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_dir_scan_entry_updates_on_write() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"hello world".to_vec()),
        )
        .await
        .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        let embedded = entry.inode.expect("Should have embedded inode");
        match embedded {
            Inode::File(f) => {
                assert_eq!(f.size, 11, "Embedded inode should have updated size");
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_dir_scan_entry_updates_on_setattr() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let attrs = SetAttributes {
            mode: SetMode::Set(0o755),
            ..Default::default()
        };
        fs.setattr(&test_creds(), file_id, &attrs).await.unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        let embedded = entry.inode.expect("Should have embedded inode");
        match embedded {
            Inode::File(f) => {
                assert_eq!(
                    f.mode & 0o777,
                    0o755,
                    "Embedded inode should have updated mode"
                );
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_dir_scan_entry_becomes_reference_on_hardlink() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();
        assert!(
            entry.inode.is_some(),
            "Before hardlink, should have embedded inode"
        );
        drop(entries);

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();

        let entry1 = entries.next().await.unwrap().unwrap();
        let entry2 = entries.next().await.unwrap().unwrap();

        let (original, hardlink) = if entry1.name == b"original.txt" {
            (entry1, entry2)
        } else {
            (entry2, entry1)
        };

        assert!(
            original.inode.is_none(),
            "Original entry should be Reference after hardlink"
        );
        assert!(
            hardlink.inode.is_none(),
            "Hardlink entry should be Reference"
        );
    }

    #[tokio::test]
    async fn test_dir_scan_entry_restored_on_rename_over_same_inode() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        while let Some(entry) = entries.next().await {
            let entry = entry.unwrap();
            assert!(
                entry.inode.is_none(),
                "Both entries should be Reference with nlink=2"
            );
        }
        drop(entries);

        fs.rename(
            &(&test_auth()).into(),
            0,
            b"hardlink.txt",
            0,
            b"original.txt",
        )
        .await
        .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        assert_eq!(entry.name, b"original.txt");
        assert!(
            entry.inode.is_some(),
            "After rename-over-same-inode (nlink=1), should have embedded inode"
        );

        // Verify nlink is 1
        let embedded = entry.inode.unwrap();
        match embedded {
            Inode::File(f) => {
                assert_eq!(f.nlink, 1);
                assert!(f.parent.is_some(), "parent should be restored");
                assert!(f.name.is_some(), "name should be restored");
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_readdir_uses_embedded_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        let result = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();

        let file_entry = result
            .entries
            .iter()
            .find(|e| e.name == b"test.txt")
            .expect("Should find test.txt");

        assert_eq!(file_entry.fileid, file_id);
        assert_eq!(
            file_entry.attr.size, 12,
            "Should have correct size from embedded inode"
        );
    }

    #[tokio::test]
    async fn test_readdir_fetches_inode_for_hardlinks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let result = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();

        let original = result
            .entries
            .iter()
            .find(|e| e.name == b"original.txt")
            .expect("Should find original.txt");

        let hardlink = result
            .entries
            .iter()
            .find(|e| e.name == b"hardlink.txt")
            .expect("Should find hardlink.txt");

        // Both should have the same inode id and attributes
        assert_eq!(original.fileid, file_id);
        assert_eq!(hardlink.fileid, file_id);
        assert_eq!(original.attr.size, 12);
        assert_eq!(hardlink.attr.size, 12);
        assert_eq!(original.attr.nlink, 2);
        assert_eq!(hardlink.attr.nlink, 2);
    }
}
