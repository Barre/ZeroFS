use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use zerofs::fs::EXTENT_SIZE;
use zerofs::fs::ZeroFS;
use zerofs::fs::errors::FsError;
use zerofs::fs::inode::{Inode, InodeAttrs, InodeId};
use zerofs::fs::key_codec::{EXTENT_DOMAIN, KeyCodec, KeyPrefix, META_DOMAIN, ParsedKey};
use zerofs::fs::store::directory::DirScanValue;

const ROOT_INODE_ID: InodeId = 0;
const DIR_BASE_NLINK: u32 = 2;
const ID_SIZE: usize = std::mem::size_of::<InodeId>();

/// Extract the 8-byte big-endian id that follows the kind byte in a
/// `prefix`-kind key. Returns `None` if the key is too short, lacks the
/// expected v2 domain prefix, or the kind byte doesn't match.
fn parse_id(codec: &KeyCodec, prefix: KeyPrefix, key: &[u8]) -> Option<InodeId> {
    let expected_domain = if prefix == KeyPrefix::Extent {
        EXTENT_DOMAIN
    } else {
        META_DOMAIN
    };
    if !key.starts_with(expected_domain) {
        return None;
    }
    let kind_off = codec.kind_offset(prefix);
    if key.get(kind_off).copied() != Some(u8::from(prefix)) {
        return None;
    }
    let id_off = codec.id_offset(prefix);
    let id_end = id_off + ID_SIZE;
    let id_bytes: [u8; ID_SIZE] = key.get(id_off..id_end)?.try_into().ok()?;
    Some(InodeId::from_be_bytes(id_bytes))
}

#[derive(Debug, Default)]
pub struct ConsistencyReport {
    pub errors: Vec<ConsistencyError>,
    pub warnings: Vec<String>,
    pub stats: VerificationStats,
}

#[derive(Debug, Default)]
pub struct VerificationStats {
    pub inodes_checked: u64,
    pub directories_checked: u64,
    pub files_checked: u64,
    pub orphaned_inodes: u64,
}

#[derive(Debug)]
pub enum ConsistencyError {
    DirectoryCountMismatch {
        inode_id: InodeId,
        stored_count: u64,
        actual_count: u64,
    },
    DanglingReference {
        dir_id: InodeId,
        entry_name: Vec<u8>,
        missing_inode: InodeId,
    },
    OrphanedInode {
        inode_id: InodeId,
    },
    StatsCounterMismatch {
        metric: String,
        stored: u64,
        calculated: u64,
    },
    NlinkMismatch {
        inode_id: InodeId,
        stored_nlink: u32,
        actual_refs: u32,
    },
    StaleTombstone {
        inode_id: InodeId,
    },
    MissingExtents {
        inode_id: InodeId,
        file_size: u64,
        expected_extents: u64,
        found_extents: u64,
    },
    /// A committed FrameLoc points at a segment the read path can't return: a crash
    /// between the FrameLoc commit and the segment PUT (seal), or a torn/corrupt
    /// segment. The seal-before-flush barrier is supposed to make this impossible for
    /// durable state.
    UnreadableExtents {
        inode_id: InodeId,
        file_size: u64,
        error: String,
    },
    DirectoryNlinkMismatch {
        inode_id: InodeId,
        stored_nlink: u32,
        expected_nlink: u32,
        subdir_count: u32,
    },
    InodeCounterTooLow {
        stored_counter: u64,
        max_inode_id: u64,
    },
    OrphanedExtent {
        inode_id: InodeId,
        extent_count: u64,
    },
    DirEntryMissingScan {
        dir_id: InodeId,
        name: Vec<u8>,
        cookie: u64,
    },
    DirScanMissingEntry {
        dir_id: InodeId,
        name: Vec<u8>,
        cookie: u64,
    },
    DirEntryCookieMismatch {
        dir_id: InodeId,
        name: Vec<u8>,
        entry_cookie: u64,
        scan_cookie: u64,
    },
    StaleEmbeddedInode {
        dir_id: InodeId,
        name: Vec<u8>,
        inode_id: InodeId,
    },
    OrphanedDirEntry {
        dir_id: InodeId,
        name: Vec<u8>,
    },
    OrphanedDirScan {
        dir_id: InodeId,
        cookie: u64,
    },
    OrphanedDirCookie {
        dir_id: InodeId,
    },
    DirCookieCounterTooLow {
        dir_id: InodeId,
        stored_counter: u64,
        max_cookie: u64,
    },
    /// An open-unlink orphan-set entry survived a clean restart. The startup
    /// drain reclaims every orphan (no open handle survives a crash), so after
    /// a restart the set must be empty; a leftover means the drain failed.
    LeakedOrphan {
        inode_id: InodeId,
    },
}

impl ConsistencyReport {
    pub fn is_consistent(&self) -> bool {
        self.errors.is_empty()
    }
}

impl std::fmt::Display for ConsistencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectoryCountMismatch {
                inode_id,
                stored_count,
                actual_count,
            } => write!(
                f,
                "Directory {} entry_count mismatch: stored={}, actual={}",
                inode_id, stored_count, actual_count
            ),
            Self::DanglingReference {
                dir_id,
                entry_name,
                missing_inode,
            } => write!(
                f,
                "Directory {} has entry '{}' pointing to non-existent inode {}",
                dir_id,
                String::from_utf8_lossy(entry_name),
                missing_inode
            ),
            Self::OrphanedInode { inode_id } => {
                write!(
                    f,
                    "Inode {} exists but is not referenced by any directory",
                    inode_id
                )
            }
            Self::StatsCounterMismatch {
                metric,
                stored,
                calculated,
            } => write!(
                f,
                "Stats '{}' mismatch: stored={}, calculated={}",
                metric, stored, calculated
            ),
            Self::NlinkMismatch {
                inode_id,
                stored_nlink,
                actual_refs,
            } => write!(
                f,
                "Inode {} nlink mismatch: stored={}, actual references={}",
                inode_id, stored_nlink, actual_refs
            ),
            Self::StaleTombstone { inode_id } => {
                write!(
                    f,
                    "Tombstone exists for inode {} which still exists",
                    inode_id
                )
            }
            Self::MissingExtents {
                inode_id,
                file_size,
                expected_extents,
                found_extents,
            } => write!(
                f,
                "File {} (size={}) missing extents: expected={}, found={}",
                inode_id, file_size, expected_extents, found_extents
            ),
            Self::UnreadableExtents {
                inode_id,
                file_size,
                error,
            } => write!(
                f,
                "File {} (size={}) has unreadable extents (dangling FrameLoc or corrupt \
                 segment): {}",
                inode_id, file_size, error
            ),
            Self::DirectoryNlinkMismatch {
                inode_id,
                stored_nlink,
                expected_nlink,
                subdir_count,
            } => write!(
                f,
                "Directory {} nlink mismatch: stored={}, expected={} (2 + {} subdirs)",
                inode_id, stored_nlink, expected_nlink, subdir_count
            ),
            Self::InodeCounterTooLow {
                stored_counter,
                max_inode_id,
            } => write!(
                f,
                "Inode counter {} is not greater than max inode ID {} (risk of collision)",
                stored_counter, max_inode_id
            ),
            Self::OrphanedExtent {
                inode_id,
                extent_count,
            } => write!(
                f,
                "Found {} orphaned extents for inode {} (no inode or tombstone exists)",
                extent_count, inode_id
            ),
            Self::DirEntryMissingScan {
                dir_id,
                name,
                cookie,
            } => write!(
                f,
                "DIR_ENTRY exists for '{}' in dir {} (cookie={}) but no DIR_SCAN found",
                String::from_utf8_lossy(name),
                dir_id,
                cookie
            ),
            Self::DirScanMissingEntry {
                dir_id,
                name,
                cookie,
            } => write!(
                f,
                "DIR_SCAN exists for '{}' in dir {} (cookie={}) but no DIR_ENTRY found",
                String::from_utf8_lossy(name),
                dir_id,
                cookie
            ),
            Self::DirEntryCookieMismatch {
                dir_id,
                name,
                entry_cookie,
                scan_cookie,
            } => write!(
                f,
                "Cookie mismatch for '{}' in dir {}: DIR_ENTRY has {}, DIR_SCAN has {}",
                String::from_utf8_lossy(name),
                dir_id,
                entry_cookie,
                scan_cookie
            ),
            Self::StaleEmbeddedInode {
                dir_id,
                name,
                inode_id,
            } => write!(
                f,
                "Stale embedded inode for '{}' in dir {} (inode {}): doesn't match inode table",
                String::from_utf8_lossy(name),
                dir_id,
                inode_id
            ),
            Self::OrphanedDirEntry { dir_id, name } => write!(
                f,
                "DIR_ENTRY '{}' references non-existent directory {}",
                String::from_utf8_lossy(name),
                dir_id
            ),
            Self::OrphanedDirScan { dir_id, cookie } => write!(
                f,
                "DIR_SCAN entry (cookie={}) references non-existent directory {}",
                cookie, dir_id
            ),
            Self::OrphanedDirCookie { dir_id } => write!(
                f,
                "DIR_COOKIE counter exists for non-existent directory {}",
                dir_id
            ),
            Self::DirCookieCounterTooLow {
                dir_id,
                stored_counter,
                max_cookie,
            } => write!(
                f,
                "DIR_COOKIE counter {} for dir {} is not greater than max used cookie {}",
                stored_counter, dir_id, max_cookie
            ),
            Self::LeakedOrphan { inode_id } => write!(
                f,
                "Orphan-set entry for inode {} survived restart (startup drain did not reclaim it)",
                inode_id
            ),
        }
    }
}

impl std::fmt::Display for ConsistencyReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Consistency Report:")?;
        writeln!(f, "  Inodes checked: {}", self.stats.inodes_checked)?;
        writeln!(
            f,
            "  Directories checked: {}",
            self.stats.directories_checked
        )?;
        writeln!(f, "  Files checked: {}", self.stats.files_checked)?;
        writeln!(f, "  Orphaned inodes: {}", self.stats.orphaned_inodes)?;
        if self.errors.is_empty() {
            writeln!(f, "  Status: CONSISTENT")?;
        } else {
            writeln!(f, "  Status: INCONSISTENT ({} errors)", self.errors.len())?;
            for error in &self.errors {
                writeln!(f, "    - {}", error)?;
            }
        }
        if !self.warnings.is_empty() {
            writeln!(f, "  Warnings: {}", self.warnings.len())?;
            for warning in &self.warnings {
                writeln!(f, "    - {}", warning)?;
            }
        }
        Ok(())
    }
}

pub struct ConsistencyChecker<'a> {
    fs: &'a ZeroFS,
    codec: KeyCodec,
    report: ConsistencyReport,
    inode_refs: HashMap<InodeId, u32>,
    valid_inodes: HashSet<InodeId>,
    subdir_counts: HashMap<InodeId, u32>,
    tombstone_inodes: HashSet<InodeId>,
    directory_inodes: HashSet<InodeId>,
    /// Inodes recorded in the durable open-unlink orphan set. After a clean
    /// restart this is always empty (the startup drain reclaims them), but the
    /// online state, an inode with nlink==0, no directory entry, present in
    /// this set is legal and must not be flagged as an orphaned inode.
    orphan_inodes: HashSet<InodeId>,
}

impl<'a> ConsistencyChecker<'a> {
    pub fn new(fs: &'a ZeroFS) -> Self {
        // Failpoint test volumes are always created with v2 segmentation
        // (see `tests/failpoints/mod.rs`).
        Self {
            fs,
            codec: KeyCodec::new(),
            report: ConsistencyReport::default(),
            inode_refs: HashMap::new(),
            valid_inodes: HashSet::new(),
            subdir_counts: HashMap::new(),
            tombstone_inodes: HashSet::new(),
            directory_inodes: HashSet::new(),
            orphan_inodes: HashSet::new(),
        }
    }

    pub async fn verify_all(mut self) -> Result<ConsistencyReport, FsError> {
        self.enumerate_inodes().await?;
        self.enumerate_tombstones().await?;
        self.enumerate_orphans().await?;
        self.walk_directory_tree(0).await?;
        self.verify_directory_counts().await?;
        self.verify_nlink_counts().await?;
        self.verify_directory_nlinks().await?;
        self.find_orphaned_inodes()?;
        self.verify_orphan_set_drained();
        self.verify_stats_counters().await?;
        self.verify_tombstones().await?;
        self.verify_file_extents().await?;
        self.verify_inode_counter().await?;
        self.verify_orphaned_extents().await?;
        self.verify_dir_entry_scan_consistency().await?;
        self.verify_orphaned_directory_metadata().await?;
        self.verify_dir_cookie_counters().await?;

        Ok(self.report)
    }

    async fn enumerate_inodes(&mut self) -> Result<(), FsError> {
        let codec = &self.codec;
        let (start, end) = codec.prefix_range(KeyPrefix::Inode);
        let expected_len = codec.inode_key_size();

        let mut stream = self
            .fs
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if key.len() == expected_len
                && let Some(inode_id) = parse_id(codec, KeyPrefix::Inode, &key)
            {
                self.valid_inodes.insert(inode_id);
                self.report.stats.inodes_checked += 1;

                if let Ok(inode) = bincode::deserialize::<Inode>(&value)
                    && matches!(inode, Inode::Directory(_))
                {
                    self.directory_inodes.insert(inode_id);
                }
            }
        }

        Ok(())
    }

    async fn enumerate_tombstones(&mut self) -> Result<(), FsError> {
        let tombstones = match self.fs.tombstone_store.list().await {
            Ok(t) => t,
            Err(_) => return Ok(()),
        };
        futures::pin_mut!(tombstones);

        while let Some(result) = tombstones.next().await {
            if let Ok(entry) = result {
                self.tombstone_inodes.insert(entry.inode_id);
            }
        }

        Ok(())
    }

    async fn enumerate_orphans(&mut self) -> Result<(), FsError> {
        let orphans = match self.fs.orphan_store.list().await {
            Ok(o) => o,
            Err(_) => return Ok(()),
        };
        futures::pin_mut!(orphans);

        while let Some(result) = orphans.next().await {
            if let Ok(inode_id) = result {
                self.orphan_inodes.insert(inode_id);
            }
        }

        Ok(())
    }

    async fn walk_directory_tree(&mut self, dir_id: InodeId) -> Result<(), FsError> {
        let entries = match self.fs.directory_store.list(dir_id).await {
            Ok(e) => e,
            Err(FsError::NotFound) => return Ok(()), // Directory doesn't exist
            Err(e) => return Err(e),
        };
        futures::pin_mut!(entries);

        while let Some(result) = entries.next().await {
            let entry = match result {
                Ok(e) => e,
                Err(_) => continue,
            };

            *self.inode_refs.entry(entry.inode_id).or_insert(0) += 1;

            if !self.valid_inodes.contains(&entry.inode_id) {
                self.report
                    .errors
                    .push(ConsistencyError::DanglingReference {
                        dir_id,
                        entry_name: entry.name.clone(),
                        missing_inode: entry.inode_id,
                    });
                continue;
            }

            if let Ok(inode) = self.fs.inode_store.get(entry.inode_id).await {
                match &inode {
                    Inode::Directory(_) => {
                        self.report.stats.directories_checked += 1;
                        *self.subdir_counts.entry(dir_id).or_insert(0) += 1;
                        if entry.inode_id != dir_id && entry.inode_id != ROOT_INODE_ID {
                            Box::pin(self.walk_directory_tree(entry.inode_id)).await?;
                        }
                    }
                    Inode::File(_) => {
                        self.report.stats.files_checked += 1;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    async fn verify_directory_counts(&mut self) -> Result<(), FsError> {
        for &inode_id in &self.valid_inodes.clone() {
            if let Ok(Inode::Directory(dir)) = self.fs.inode_store.get(inode_id).await {
                let mut actual_count = 0u64;
                let entries = match self.fs.directory_store.list(inode_id).await {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                futures::pin_mut!(entries);
                while entries.next().await.is_some() {
                    actual_count += 1;
                }

                if dir.entry_count != actual_count {
                    self.report
                        .errors
                        .push(ConsistencyError::DirectoryCountMismatch {
                            inode_id,
                            stored_count: dir.entry_count,
                            actual_count,
                        });
                }
            }
        }
        Ok(())
    }

    async fn verify_nlink_counts(&mut self) -> Result<(), FsError> {
        for (&inode_id, &actual_refs) in &self.inode_refs {
            if let Ok(inode) = self.fs.inode_store.get(inode_id).await {
                let stored_nlink = inode.nlink();
                if !matches!(inode, Inode::Directory(_)) && stored_nlink != actual_refs {
                    self.report.errors.push(ConsistencyError::NlinkMismatch {
                        inode_id,
                        stored_nlink,
                        actual_refs,
                    });
                }
            }
        }
        Ok(())
    }

    fn find_orphaned_inodes(&mut self) -> Result<(), FsError> {
        for &inode_id in &self.valid_inodes {
            if inode_id == ROOT_INODE_ID {
                continue;
            }
            // An open-unlink orphan (nlink==0, present in the orphan set, no
            // directory entry) is legal while online
            if self.orphan_inodes.contains(&inode_id) {
                continue;
            }
            if !self.inode_refs.contains_key(&inode_id) {
                self.report
                    .errors
                    .push(ConsistencyError::OrphanedInode { inode_id });
                self.report.stats.orphaned_inodes += 1;
            }
        }
        Ok(())
    }

    fn verify_orphan_set_drained(&mut self) {
        // The checker runs after a clean restart, by which point the startup
        // drain must have reclaimed every orphan. A surviving entry means the
        // drain failed to make progress.
        for &inode_id in &self.orphan_inodes {
            self.report
                .errors
                .push(ConsistencyError::LeakedOrphan { inode_id });
        }
    }

    async fn verify_stats_counters(&mut self) -> Result<(), FsError> {
        let (stored_bytes, stored_inodes) = self.fs.global_stats.get_totals();
        let mut calculated_bytes = 0u64;
        let mut calculated_inodes = 0u64;

        for &inode_id in &self.valid_inodes {
            if inode_id == ROOT_INODE_ID || !self.inode_refs.contains_key(&inode_id) {
                continue;
            }
            if let Ok(inode) = self.fs.inode_store.get(inode_id).await {
                calculated_inodes += 1;
                if let Inode::File(f) = inode {
                    calculated_bytes += f.size;
                }
            }
        }

        if stored_bytes != calculated_bytes {
            self.report
                .errors
                .push(ConsistencyError::StatsCounterMismatch {
                    metric: "used_bytes".to_string(),
                    stored: stored_bytes,
                    calculated: calculated_bytes,
                });
        }

        if stored_inodes != calculated_inodes {
            self.report
                .errors
                .push(ConsistencyError::StatsCounterMismatch {
                    metric: "used_inodes".to_string(),
                    stored: stored_inodes,
                    calculated: calculated_inodes,
                });
        }

        Ok(())
    }

    async fn verify_tombstones(&mut self) -> Result<(), FsError> {
        let tombstones = match self.fs.tombstone_store.list().await {
            Ok(t) => t,
            Err(_) => return Ok(()),
        };
        futures::pin_mut!(tombstones);

        while let Some(result) = tombstones.next().await {
            let entry = match result {
                Ok(e) => e,
                Err(_) => continue,
            };

            if self.valid_inodes.contains(&entry.inode_id) {
                self.report.errors.push(ConsistencyError::StaleTombstone {
                    inode_id: entry.inode_id,
                });
            }
        }

        Ok(())
    }

    async fn verify_directory_nlinks(&mut self) -> Result<(), FsError> {
        for &inode_id in &self.valid_inodes.clone() {
            if let Ok(Inode::Directory(dir)) = self.fs.inode_store.get(inode_id).await {
                let subdir_count = self.subdir_counts.get(&inode_id).copied().unwrap_or(0);
                let expected_nlink = DIR_BASE_NLINK + subdir_count;

                if dir.nlink != expected_nlink {
                    self.report
                        .errors
                        .push(ConsistencyError::DirectoryNlinkMismatch {
                            inode_id,
                            stored_nlink: dir.nlink,
                            expected_nlink,
                            subdir_count,
                        });
                }
            }
        }
        Ok(())
    }

    async fn verify_file_extents(&mut self) -> Result<(), FsError> {
        for &inode_id in &self.valid_inodes.clone() {
            if !self.inode_refs.contains_key(&inode_id) {
                continue;
            }
            if let Ok(Inode::File(file)) = self.fs.inode_store.get(inode_id).await {
                if file.size == 0 {
                    continue;
                }
                let expected_extents = file.size.div_ceil(EXTENT_SIZE as u64);
                let start_key = self.codec.extent_key(inode_id, 0);
                let end_key = self.codec.extent_key(inode_id, expected_extents);

                let mut found_extents = 0u64;
                let stream = match self.fs.db.scan(start_key..end_key).await {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                futures::pin_mut!(stream);

                while let Some(result) = stream.next().await {
                    if result.is_ok() {
                        found_extents += 1;
                    }
                }

                if found_extents != expected_extents {
                    self.report.errors.push(ConsistencyError::MissingExtents {
                        inode_id,
                        file_size: file.size,
                        expected_extents,
                        found_extents,
                    });
                }

                // Beyond counting keys, every extent must actually be readable: read
                // the whole file back through the data plane. A committed FrameLoc
                // whose segment was never PUT (crash between commit and seal) reads
                // back as a dangling pointer, and a torn segment fails the per-frame
                // AEAD — both surface here, not in the key count above.
                if let Err(e) = self.fs.extent_store.read(inode_id, 0, file.size).await {
                    self.report
                        .errors
                        .push(ConsistencyError::UnreadableExtents {
                            inode_id,
                            file_size: file.size,
                            error: format!("{e:?}"),
                        });
                }
            }
        }
        Ok(())
    }

    async fn verify_inode_counter(&mut self) -> Result<(), FsError> {
        let max_inode_id = self
            .valid_inodes
            .iter()
            .copied()
            .max()
            .unwrap_or(ROOT_INODE_ID);
        let counter_key = self.codec.system_counter_key();
        let stored_counter = match self.fs.db.get_bytes(&counter_key).await {
            Ok(Some(data)) => KeyCodec::decode_counter(&data)?,
            Ok(None) if max_inode_id > ROOT_INODE_ID => ROOT_INODE_ID,
            Ok(None) => return Ok(()),
            Err(_) => return Err(FsError::IoError),
        };

        if stored_counter <= max_inode_id {
            self.report
                .errors
                .push(ConsistencyError::InodeCounterTooLow {
                    stored_counter,
                    max_inode_id,
                });
        }

        Ok(())
    }

    async fn verify_orphaned_extents(&mut self) -> Result<(), FsError> {
        let codec = &self.codec;
        let (start, end) = codec.prefix_range(KeyPrefix::Extent);

        let mut stream = self
            .fs
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        let mut orphaned_by_inode: HashMap<InodeId, u64> = HashMap::new();

        while let Some(result) = stream.next().await {
            let (key, _) = result.map_err(|_| FsError::IoError)?;
            if let Some(inode_id) = parse_id(codec, KeyPrefix::Extent, &key)
                && !self.valid_inodes.contains(&inode_id)
                && !self.tombstone_inodes.contains(&inode_id)
            {
                *orphaned_by_inode.entry(inode_id).or_insert(0) += 1;
            }
        }

        for (inode_id, extent_count) in orphaned_by_inode {
            self.report.errors.push(ConsistencyError::OrphanedExtent {
                inode_id,
                extent_count,
            });
        }

        Ok(())
    }

    async fn verify_dir_entry_scan_consistency(&mut self) -> Result<(), FsError> {
        for &dir_id in &self.directory_inodes.clone() {
            let mut dir_entries: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut dir_scans: HashMap<u64, Vec<u8>> = HashMap::new();
            let mut max_cookie: u64 = 0;

            let codec = &self.codec;
            let entry_prefix = codec.dir_entry_key(dir_id, b"");
            let entry_end = codec.dir_entry_key(dir_id + 1, b"");
            let dir_entry_id_end = codec.id_offset(KeyPrefix::DirEntry) + 8;

            let stream = match self.fs.db.scan(entry_prefix..entry_end).await {
                Ok(s) => s,
                Err(_) => continue,
            };
            futures::pin_mut!(stream);

            while let Some(result) = stream.next().await {
                let (key, value) = match result {
                    Ok(kv) => kv,
                    Err(_) => continue,
                };
                let name = key[dir_entry_id_end..].to_vec();
                if let Ok((inode_id, cookie)) = KeyCodec::decode_dir_entry(&value) {
                    dir_entries.insert(name.clone(), cookie);

                    if let Ok(inode) = self.fs.inode_store.get(inode_id).await {
                        let scan_key = codec.dir_scan_key(dir_id, cookie);
                        if let Ok(Some(scan_value)) = self.fs.db.get_bytes(&scan_key).await
                            && let Ok((_, dsv)) = Self::decode_dir_scan_value(&scan_value)
                            && let DirScanValue::WithInode {
                                inode: embedded, ..
                            } = dsv
                            && !inodes_equal(&embedded, &inode)
                        {
                            self.report
                                .errors
                                .push(ConsistencyError::StaleEmbeddedInode {
                                    dir_id,
                                    name: name.clone(),
                                    inode_id,
                                });
                        }
                    }
                }
            }

            let scan_start = bytes::Bytes::from(codec.dir_scan_prefix(dir_id));
            // End of the dir_id's scan range: the prefix for (dir_id + 1).
            let scan_end = bytes::Bytes::from(codec.dir_scan_prefix(dir_id + 1));

            let stream = match self.fs.db.scan(scan_start..scan_end).await {
                Ok(s) => s,
                Err(_) => continue,
            };
            futures::pin_mut!(stream);

            while let Some(result) = stream.next().await {
                let (key, value) = match result {
                    Ok(kv) => kv,
                    Err(_) => continue,
                };
                if let ParsedKey::DirScan { cookie } = codec.parse_key(&key) {
                    max_cookie = max_cookie.max(cookie);
                    if let Ok((name, _)) = Self::decode_dir_scan_value(&value) {
                        dir_scans.insert(cookie, name);
                    }
                }
            }

            for (name, cookie) in &dir_entries {
                if !dir_scans.contains_key(cookie) {
                    self.report
                        .errors
                        .push(ConsistencyError::DirEntryMissingScan {
                            dir_id,
                            name: name.clone(),
                            cookie: *cookie,
                        });
                } else if let Some(scan_name) = dir_scans.get(cookie)
                    && scan_name != name
                {
                    self.report
                        .errors
                        .push(ConsistencyError::DirEntryCookieMismatch {
                            dir_id,
                            name: name.clone(),
                            entry_cookie: *cookie,
                            scan_cookie: *cookie,
                        });
                }
            }

            for (cookie, name) in &dir_scans {
                let has_matching_entry =
                    dir_entries.get(name).map(|c| c == cookie).unwrap_or(false);
                if !has_matching_entry {
                    self.report
                        .errors
                        .push(ConsistencyError::DirScanMissingEntry {
                            dir_id,
                            name: name.clone(),
                            cookie: *cookie,
                        });
                }
            }

            let counter_key = codec.dir_cookie_counter_key(dir_id);
            if let Ok(Some(data)) = self.fs.db.get_bytes(&counter_key).await
                && let Ok(counter) = KeyCodec::decode_counter(&data)
                && max_cookie > 0
                && counter <= max_cookie
            {
                self.report
                    .errors
                    .push(ConsistencyError::DirCookieCounterTooLow {
                        dir_id,
                        stored_counter: counter,
                        max_cookie,
                    });
            }
        }

        Ok(())
    }

    fn decode_dir_scan_value(data: &[u8]) -> Result<(Vec<u8>, DirScanValue), FsError> {
        if data.len() < 4 {
            return Err(FsError::InvalidData);
        }
        let name_len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        if data.len() < 4 + name_len {
            return Err(FsError::InvalidData);
        }
        let name = data[4..4 + name_len].to_vec();
        let value: DirScanValue =
            bincode::deserialize(&data[4 + name_len..]).map_err(|_| FsError::InvalidData)?;
        Ok((name, value))
    }

    async fn verify_orphaned_directory_metadata(&mut self) -> Result<(), FsError> {
        let codec = &self.codec;
        let dir_entry_id_end = codec.id_offset(KeyPrefix::DirEntry) + 8;
        let (start, end) = codec.prefix_range(KeyPrefix::DirEntry);
        let mut stream = self
            .fs
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        while let Some(result) = stream.next().await {
            let (key, _) = result.map_err(|_| FsError::IoError)?;
            if key.len() > dir_entry_id_end
                && let Some(dir_id) = parse_id(codec, KeyPrefix::DirEntry, &key)
            {
                let name = key[dir_entry_id_end..].to_vec();
                if !self.directory_inodes.contains(&dir_id) && dir_id != ROOT_INODE_ID {
                    self.report
                        .errors
                        .push(ConsistencyError::OrphanedDirEntry { dir_id, name });
                }
            }
        }

        let (start, end) = codec.prefix_range(KeyPrefix::DirScan);
        let mut stream = self
            .fs
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        while let Some(result) = stream.next().await {
            let (key, _) = result.map_err(|_| FsError::IoError)?;
            if let ParsedKey::DirScan { cookie } = codec.parse_key(&key)
                && let Some(dir_id) = parse_id(codec, KeyPrefix::DirScan, &key)
                && !self.directory_inodes.contains(&dir_id)
                && dir_id != ROOT_INODE_ID
            {
                self.report
                    .errors
                    .push(ConsistencyError::OrphanedDirScan { dir_id, cookie });
            }
        }

        let (start, end) = codec.prefix_range(KeyPrefix::DirCookie);
        let expected_cookie_key_len = codec.id_offset(KeyPrefix::DirCookie) + 8;
        let mut stream = self
            .fs
            .db
            .scan(start..end)
            .await
            .map_err(|_| FsError::IoError)?;

        while let Some(result) = stream.next().await {
            let (key, _) = result.map_err(|_| FsError::IoError)?;
            if key.len() == expected_cookie_key_len
                && let Some(dir_id) = parse_id(codec, KeyPrefix::DirCookie, &key)
                && !self.directory_inodes.contains(&dir_id)
                && dir_id != ROOT_INODE_ID
            {
                self.report
                    .errors
                    .push(ConsistencyError::OrphanedDirCookie { dir_id });
            }
        }

        Ok(())
    }

    async fn verify_dir_cookie_counters(&mut self) -> Result<(), FsError> {
        Ok(())
    }
}

pub async fn verify_consistency(fs: &ZeroFS) -> Result<ConsistencyReport, FsError> {
    ConsistencyChecker::new(fs).verify_all().await
}

fn inodes_equal(a: &Inode, b: &Inode) -> bool {
    let a_bytes = bincode::serialize(a).unwrap_or_default();
    let b_bytes = bincode::serialize(b).unwrap_or_default();
    a_bytes == b_bytes
}
