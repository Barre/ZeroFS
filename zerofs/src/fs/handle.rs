//! Open-handle tracking and open-unlink reclaim: `OpenHandle` pins, the
//! reclaim drainer, and the startup orphan drain.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::stats;
use crate::fs::{EXTENT_SIZE, SMALL_FILE_TOMBSTONE_THRESHOLD, ZeroFS};
use ::tracing::{error, warn};
use dashmap::DashMap;
use futures::pin_mut;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc::UnboundedSender;

/// Decrement `id`'s open-handle count, removing the entry at zero; returns the
/// count after the decrement. Shared by `open_handle_dec` and `OpenHandle::drop`.
fn dec_open_handle(map: &DashMap<InodeId, u64>, id: InodeId) -> u64 {
    use dashmap::mapref::entry::Entry;
    match map.entry(id) {
        Entry::Occupied(mut e) => {
            let v = e.get().saturating_sub(1);
            if v == 0 {
                e.remove();
                0
            } else {
                *e.get_mut() = v;
                v
            }
        }
        Entry::Vacant(_) => 0,
    }
}

/// Pins an inode's open-handle count for the lifetime of one opened 9P fid.
/// Created (incrementing the count) when a fid is opened, and stored in that
/// fid's slot in the session table. Because it lives in the slot, every way a
/// fid goes away (clunk, close_all, a walk that overwrites it, the session
/// table being dropped) releases the count exactly once, with no release call
/// to forget or get wrong. Holds only the count map and the reclaim sender, not
/// the fs, so it forms no reference cycle.
#[derive(Debug)]
pub struct OpenHandle {
    inode_id: InodeId,
    open_handles: Arc<DashMap<InodeId, u64>>,
    reclaim_tx: UnboundedSender<InodeId>,
}

impl Drop for OpenHandle {
    fn drop(&mut self) {
        if dec_open_handle(&self.open_handles, self.inode_id) == 0 {
            // Last handle gone. Reclaim takes the inode lock and is async, so it
            // can't run in Drop; hand the inode to the drainer instead. A failed
            // send (no drainer, or shutdown) is fine — startup drain reclaims it.
            let _ = self.reclaim_tx.send(self.inode_id);
        }
    }
}

impl ZeroFS {
    /// Increment the open-handle count for `id`. MUST be invoked under
    /// `lock_manager.acquire(id)` together with the inode-liveness `get`, so
    /// that the increment and the "is the inode alive" check are serialized
    /// against `remove`/`rename`'s defer-vs-delete decision on the same id.
    pub fn open_handle_inc(&self, id: InodeId) {
        *self.open_handles.entry(id).or_insert(0) += 1;
    }

    /// Decrement the open-handle count, returning the count after. Only the
    /// `handle_closed` test path uses this; production decrements via the
    /// `OpenHandle` guard's drop.
    #[allow(dead_code)]
    pub fn open_handle_dec(&self, id: InodeId) -> u64 {
        dec_open_handle(&self.open_handles, id)
    }

    /// Current open-handle count for `id`. Consulted by `remove`/`rename`
    /// under the inode lock to choose defer-vs-delete.
    pub fn open_handle_count(&self, id: InodeId) -> u64 {
        self.open_handles.get(&id).map(|r| *r).unwrap_or(0)
    }

    /// Increment the open-handle count and return a guard that releases it on
    /// drop; the caller stores it in the fid slot. Like `open_handle_inc`, must
    /// be called under `lock_manager.acquire(id)` alongside the liveness `get`,
    /// so the increment is ordered against `remove`/`rename`'s defer decision.
    pub fn new_open_handle(&self, id: InodeId) -> OpenHandle {
        self.open_handle_inc(id);
        OpenHandle {
            inode_id: id,
            open_handles: self.open_handles.clone(),
            reclaim_tx: self.reclaim_tx.clone(),
        }
    }

    /// Spawn the reclaim drainer; call once after wrapping the fs in an `Arc`.
    /// It holds a `Weak` ref, so it adds no cycle and exits when the fs drops,
    /// and reclaims each inode an `OpenHandle` drop flagged as losing its last
    /// handle. Reclaim is idempotent, so duplicate/stale ids are harmless.
    pub fn start_reclaim_drainer(self: &Arc<Self>) {
        let mut rx = match self.reclaim_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => return, // already started
        };
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            while let Some(id) = rx.recv().await {
                match weak.upgrade() {
                    Some(fs) => fs.reclaim_if_unreferenced(id).await,
                    None => break,
                }
            }
        });
    }

    /// Synchronous decrement-then-reclaim. Production drives this through the
    /// `OpenHandle` guard (drop decrements) and the drainer; this entry point is
    /// kept for tests that want the reclaim to happen inline, with no drainer.
    #[allow(dead_code)]
    pub async fn handle_closed(&self, id: InodeId) {
        // Decrement outside the lock; reclaim_if_unreferenced re-reads the count
        // (and nlink) under it.
        if self.open_handle_dec(id) > 0 {
            return;
        }
        self.reclaim_if_unreferenced(id).await;
    }

    /// Reclaim `id` if it's now an unreferenced orphan. The count has already
    /// been decremented by the caller; re-check it under the inode lock (a reopen
    /// between the dec and the lock may have bumped it back up) and only reclaim
    /// at zero.
    pub async fn reclaim_if_unreferenced(&self, id: InodeId) {
        #[cfg(feature = "failpoints")]
        fail_point!(fp::RECLAIM_BEFORE_LOCK);

        let _guard = self.lock_manager.acquire(id).await;

        if self.open_handle_count(id) > 0 {
            return;
        }

        #[cfg(feature = "failpoints")]
        fail_point!(fp::RECLAIM_HOLDING_LOCK_BEFORE_DELETE);

        if let Err(e) = self.reclaim_orphan_inode(id).await {
            error!("Deferred reclaim of orphan inode {} failed: {:?}", id, e);
        }
    }

    /// Reclaim a single deferred-orphan inode `id`: delete its inode record,
    /// its extents (small files) or add a tombstone (large files), subtract its
    /// stats, and remove its orphan-set entry.
    async fn reclaim_orphan_inode(&self, id: InodeId) -> Result<(), FsError> {
        match self.inode_store.get(id).await {
            Ok(Inode::File(file)) if file.nlink == 0 => {
                let mut txn = self.db.new_transaction()?;
                let total_extents = file.size.div_ceil(EXTENT_SIZE as u64);
                if total_extents as usize <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                    self.extent_store
                        .delete_range(&mut txn, id, 0, total_extents)
                        .await?;
                } else {
                    self.tombstone_store.add(&mut txn, id, file.size);
                    self.stats
                        .tombstones_created
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.inode_store.delete(&mut txn, id);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::CLUNK_AFTER_RECLAIM_INODE_DELETE);

                txn.add_stats_delta(id, stats::size_delta(file.size, 0), -1);
                self.orphan_store.remove(&mut txn, id);
                self.write_coordinator.commit(txn).await?;
                self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Ok(Inode::Symlink(s)) if s.nlink == 0 => {
                // Dataless orphan
                let mut txn = self.db.new_transaction()?;
                self.inode_store.delete(&mut txn, id);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::CLUNK_AFTER_RECLAIM_INODE_DELETE);

                txn.add_stats_delta(id, stats::size_delta(0, 0), -1);
                self.orphan_store.remove(&mut txn, id);
                self.write_coordinator.commit(txn).await?;
                self.stats.links_deleted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Ok(Inode::Fifo(s))
            | Ok(Inode::Socket(s))
            | Ok(Inode::CharDevice(s))
            | Ok(Inode::BlockDevice(s))
                if s.nlink == 0 =>
            {
                // Dataless orphan
                let mut txn = self.db.new_transaction()?;
                self.inode_store.delete(&mut txn, id);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::CLUNK_AFTER_RECLAIM_INODE_DELETE);

                txn.add_stats_delta(id, stats::size_delta(0, 0), -1);
                self.orphan_store.remove(&mut txn, id);
                self.write_coordinator.commit(txn).await?;
                Ok(())
            }
            Ok(_) | Err(FsError::NotFound) => {
                // Not a reclaimable orphan (still linked, or already gone). This
                // runs on the last clunk of EVERY opened inode (normal files,
                // dirs), so only touch the DB when there is actually a stray
                // orphan key to clear — never commit a no-op txn on this path.
                if self.orphan_store.contains(id).await? {
                    let mut txn = self.db.new_transaction()?;
                    self.orphan_store.remove(&mut txn, id);
                    self.write_coordinator.commit(txn).await?;
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Drain the durable orphan set at startup. Every entry is reclaimable
    /// because no open handle survives a restart. Each reclaim is serialized
    /// on its inode id and is idempotent, so a crash mid-drain is safe: the
    /// not-yet-processed orphan keys are re-scanned on the next boot.
    pub(super) async fn drain_orphans(&self) -> anyhow::Result<()> {
        let mut ids = Vec::new();
        {
            let stream = self.orphan_store.list().await?;
            pin_mut!(stream);
            while let Some(entry) = stream.next().await {
                match entry {
                    Ok(id) => ids.push(id),
                    Err(e) => {
                        warn!("Skipping unreadable orphan-set entry during drain: {:?}", e);
                    }
                }
            }
        }

        for id in ids {
            let _guard = self.lock_manager.acquire(id).await;
            if let Err(e) = self.reclaim_orphan_inode(id).await {
                error!("Startup drain of orphan inode {} failed: {:?}", id, e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::fs::inode::Inode;

    #[cfg(feature = "failpoints")]
    use crate::failpoints as fp;
    use crate::fs::test_util::test_creds;
    use crate::fs::types::{FileType, SetAttributes};
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;
    use bytes::Bytes;
    use futures::pin_mut;
    use futures::stream::StreamExt;

    async fn orphan_ids(fs: &ZeroFS) -> Vec<InodeId> {
        let stream = fs.orphan_store.list().await.unwrap();
        pin_mut!(stream);
        let mut ids = Vec::new();
        while let Some(r) = stream.next().await {
            ids.push(r.unwrap());
        }
        ids
    }

    async fn make_file(fs: &ZeroFS, name: &[u8], contents: &[u8]) -> InodeId {
        let (id, _) = fs
            .create(&test_creds(), 0, name, &SetAttributes::default())
            .await
            .unwrap();
        fs.write(
            &(&test_auth()).into(),
            id,
            0,
            &Bytes::copy_from_slice(contents),
        )
        .await
        .unwrap();
        id
    }

    /// The deterministic repro: open a fid, unlink the name, the open fid must
    /// still read the data; only the last clunk reclaims the inode.
    #[tokio::test]
    async fn test_open_unlink_read_via_open_fid() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let data = b"open-unlink contents";
        let file_id = make_file(&fs, b"a", data).await;

        // `exec 3< a`
        fs.open_handle_inc(file_id);

        // `rm a` — defers because a handle is open.
        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();
        assert_eq!(orphan_ids(&fs).await, vec![file_id]);
        // Name is gone from the namespace.
        assert!(matches!(
            fs.lookup(&test_creds(), 0, b"a").await,
            Err(FsError::NotFound)
        ));

        // `cat <&3` — read through the still-open fid succeeds.
        let (read, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), data);

        // Write through the still-open fid after unlink must also succeed
        // (POSIX open-unlink covers read AND write).
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(b"WROTE-AFTER-UNLINK!!"),
        )
        .await
        .expect("write after unlink must succeed");

        // fd 3 closes — last handle, reclaim now.
        fs.handle_closed(file_id).await;
        assert!(matches!(
            fs.inode_store.get(file_id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    // A reopen that bumps the count before reclaim's recheck must make the
    // recheck abort — reclaim must never delete a re-referenced inode. A
    // failpoint pauses reclaim before it takes the inode lock so the reopen wins
    // the lock deterministically. Run with `--features failpoints`.
    #[cfg(feature = "failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_reclaim_aborts_on_concurrent_reopen() {
        let _scenario = fail::FailScenario::setup();
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let file_id = make_file(&fs, b"a", b"data").await;
        fs.open_handle_inc(file_id); // a handle is open
        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap(); // defer -> orphan
        fs.open_handle_dec(file_id); // last handle closed -> count 0, reclaimable
        assert_eq!(orphan_ids(&fs).await, vec![file_id]);

        fail::cfg(fp::RECLAIM_BEFORE_LOCK, "pause").unwrap();
        let f = fs.clone();
        let reclaim = tokio::spawn(async move { f.reclaim_if_unreferenced(file_id).await });

        // Let reclaim reach the pause, then reopen: take the inode lock and bump
        // the count exactly as lopen does (acquire + open_handle_inc).
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        {
            let _g = fs.lock_manager.acquire(file_id).await;
            fs.open_handle_inc(file_id);
        }

        // Resume reclaim: acquires the lock, rechecks count > 0, must abort.
        fail::cfg(fp::RECLAIM_BEFORE_LOCK, "off").unwrap();
        reclaim.await.unwrap();

        assert!(
            fs.inode_store.get(file_id).await.is_ok(),
            "reclaim must abort when a reopen raced in before the recheck"
        );
        assert_eq!(fs.open_handle_count(file_id), 1);
        assert_eq!(orphan_ids(&fs).await, vec![file_id]);
    }

    // The other ordering: a reopen that races into the reclaim window blocks on
    // the inode lock reclaim holds, then sees the inode gone once reclaim deletes
    // it — a clean failure, not a dangling reference. A failpoint pauses reclaim
    // while it holds the lock, after the recheck.
    #[cfg(feature = "failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_reopen_after_reclaim_fails_cleanly() {
        let _scenario = fail::FailScenario::setup();
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let file_id = make_file(&fs, b"a", b"data").await;
        fs.open_handle_inc(file_id);
        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();
        fs.open_handle_dec(file_id);

        fail::cfg(fp::RECLAIM_HOLDING_LOCK_BEFORE_DELETE, "pause").unwrap();
        let f = fs.clone();
        let reclaim = tokio::spawn(async move { f.reclaim_if_unreferenced(file_id).await });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // Reopen races in and blocks on the inode lock reclaim is holding.
        let f2 = fs.clone();
        let reopen = tokio::spawn(async move {
            let _g = f2.lock_manager.acquire(file_id).await;
            f2.inode_store.get(file_id).await // the liveness check lopen does
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // Resume reclaim: it deletes the inode and releases the lock; the reopen
        // then proceeds and must see the inode gone.
        fail::cfg(fp::RECLAIM_HOLDING_LOCK_BEFORE_DELETE, "off").unwrap();
        reclaim.await.unwrap();
        let reopen_result = reopen.await.unwrap();

        assert!(
            matches!(reopen_result, Err(FsError::NotFound)),
            "a reopen racing into the reclaim window must see the inode gone, not a dangling reference"
        );
        assert!(matches!(
            fs.inode_store.get(file_id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// Open-unlink also covers special files: a FIFO unlinked while a handle is
    /// open is deferred to the orphan set, then reclaimed at last clunk.
    #[tokio::test]
    async fn test_open_unlink_fifo_defers_and_reclaims() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (id, _) = fs
            .mknod(
                &test_creds(),
                0,
                b"p",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
            .unwrap();

        fs.open_handle_inc(id);
        fs.remove(&(&test_auth()).into(), 0, b"p").await.unwrap();
        // Deferred: name gone, inode kept alive, in the orphan set.
        assert_eq!(orphan_ids(&fs).await, vec![id]);
        assert!(matches!(
            fs.lookup(&test_creds(), 0, b"p").await,
            Err(FsError::NotFound)
        ));
        assert!(fs.inode_store.get(id).await.is_ok());

        // Last clunk reclaims it.
        fs.handle_closed(id).await;
        assert!(matches!(
            fs.inode_store.get(id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// Same for a symlink unlinked while a handle is open.
    #[tokio::test]
    async fn test_open_unlink_symlink_defers_and_reclaims() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (id, _) = fs
            .symlink(&test_creds(), 0, b"s", b"target", &SetAttributes::default())
            .await
            .unwrap();

        fs.open_handle_inc(id);
        fs.remove(&(&test_auth()).into(), 0, b"s").await.unwrap();
        assert_eq!(orphan_ids(&fs).await, vec![id]);
        assert!(matches!(
            fs.lookup(&test_creds(), 0, b"s").await,
            Err(FsError::NotFound)
        ));
        assert!(fs.inode_store.get(id).await.is_ok());

        fs.handle_closed(id).await;
        assert!(matches!(
            fs.inode_store.get(id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// Unlink with no open handle keeps the original synchronous-delete path
    /// and never touches the orphan set.
    #[tokio::test]
    async fn test_unlink_no_handle_deletes_immediately() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let file_id = make_file(&fs, b"a", b"data").await;

        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();
        assert!(matches!(
            fs.inode_store.get(file_id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// Two open handles: remove defers; the inode survives the first clunk and
    /// is reclaimed only on the second (last) clunk.
    #[tokio::test]
    async fn test_open_unlink_two_handles() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let file_id = make_file(&fs, b"a", b"two-handles").await;

        fs.open_handle_inc(file_id);
        fs.open_handle_inc(file_id);

        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();
        assert_eq!(orphan_ids(&fs).await, vec![file_id]);

        // First clunk: still alive.
        fs.handle_closed(file_id).await;
        assert!(fs.inode_store.get(file_id).await.is_ok());
        assert_eq!(orphan_ids(&fs).await, vec![file_id]);

        // Second (last) clunk: reclaimed.
        fs.handle_closed(file_id).await;
        assert!(matches!(
            fs.inode_store.get(file_id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// Reopen after defer keeps the inode alive across the original handle's
    /// clunk; reclaim only fires when the reopened handle also closes.
    #[tokio::test]
    async fn test_open_unlink_reopen_before_clunk() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let file_id = make_file(&fs, b"a", b"reopen").await;

        fs.open_handle_inc(file_id); // handle A
        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();

        // Reopen via the still-open fid (dup): handle B.
        fs.open_handle_inc(file_id);

        // Close A: count drops to 1, no reclaim.
        fs.handle_closed(file_id).await;
        assert!(fs.inode_store.get(file_id).await.is_ok());

        // Close B: last handle, reclaim.
        fs.handle_closed(file_id).await;
        assert!(matches!(
            fs.inode_store.get(file_id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// rename clobbering an open target defers the target's reclaim; the
    /// target's open fid still reads, and the last clunk reclaims it.
    #[tokio::test]
    async fn test_rename_over_open_target() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let src_id = make_file(&fs, b"src", b"source").await;
        let tgt_data = b"target-contents";
        let tgt_id = make_file(&fs, b"tgt", tgt_data).await;

        fs.open_handle_inc(tgt_id);

        fs.rename(&(&test_auth()).into(), 0, b"src", 0, b"tgt")
            .await
            .unwrap();
        assert_eq!(orphan_ids(&fs).await, vec![tgt_id]);

        // The clobbered target's open fid still reads its data.
        let (read, _) = fs
            .read_file(&(&test_auth()).into(), tgt_id, 0, tgt_data.len() as u32)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), tgt_data);
        // The new "tgt" name resolves to the source inode.
        let (resolved, _) = fs
            .directory_store
            .get_entry_with_cookie(0, b"tgt")
            .await
            .unwrap();
        assert_eq!(resolved, src_id);

        fs.handle_closed(tgt_id).await;
        assert!(matches!(
            fs.inode_store.get(tgt_id).await,
            Err(FsError::NotFound)
        ));
        assert!(orphan_ids(&fs).await.is_empty());
    }

    /// linkat resurrecting an open-unlinked inode (nlink 0->1) must clear its
    /// orphan-set key, so the last clunk does NOT reclaim a now-linked file.
    #[tokio::test]
    async fn test_link_resurrects_open_unlinked_clears_orphan() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let id = make_file(&fs, b"a", b"data").await;
        fs.open_handle_inc(id);
        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();
        assert_eq!(orphan_ids(&fs).await, vec![id]);

        // Give it a name again while still open.
        fs.link(&(&test_auth()).into(), id, 0, b"a2").await.unwrap();
        assert!(
            orphan_ids(&fs).await.is_empty(),
            "link must clear the orphan-set key"
        );

        // Last clunk must not reclaim it: it's linked again.
        fs.handle_closed(id).await;
        assert!(
            fs.inode_store.get(id).await.is_ok(),
            "resurrected inode must survive its last clunk"
        );
        assert_eq!(fs.lookup(&test_creds(), 0, b"a2").await.unwrap(), id);
    }

    /// Rename-clobber of an OPEN symlink and an OPEN special file defers reclaim
    /// (same as a File target) and reclaims at last clunk.
    #[tokio::test]
    async fn test_rename_over_open_symlink_and_special() {
        // Symlink target.
        {
            let fs = ZeroFS::new_in_memory().await.unwrap();
            make_file(&fs, b"src", b"x").await;
            let (sym_id, _) = fs
                .symlink(&test_creds(), 0, b"tgt", b"dest", &SetAttributes::default())
                .await
                .unwrap();
            fs.open_handle_inc(sym_id);
            fs.rename(&(&test_auth()).into(), 0, b"src", 0, b"tgt")
                .await
                .unwrap();
            assert_eq!(orphan_ids(&fs).await, vec![sym_id]);
            assert!(fs.inode_store.get(sym_id).await.is_ok());
            fs.handle_closed(sym_id).await;
            assert!(matches!(
                fs.inode_store.get(sym_id).await,
                Err(FsError::NotFound)
            ));
            assert!(orphan_ids(&fs).await.is_empty());
        }
        // FIFO (special-file) target.
        {
            let fs = ZeroFS::new_in_memory().await.unwrap();
            make_file(&fs, b"src", b"x").await;
            let (fifo_id, _) = fs
                .mknod(
                    &test_creds(),
                    0,
                    b"tgt",
                    FileType::Fifo,
                    &SetAttributes::default(),
                    None,
                )
                .await
                .unwrap();
            fs.open_handle_inc(fifo_id);
            fs.rename(&(&test_auth()).into(), 0, b"src", 0, b"tgt")
                .await
                .unwrap();
            assert_eq!(orphan_ids(&fs).await, vec![fifo_id]);
            assert!(fs.inode_store.get(fifo_id).await.is_ok());
            fs.handle_closed(fifo_id).await;
            assert!(matches!(
                fs.inode_store.get(fifo_id).await,
                Err(FsError::NotFound)
            ));
            assert!(orphan_ids(&fs).await.is_empty());
        }
    }

    /// Unlinking one name of a hardlinked (nlink==2) open file only decrements
    /// nlink; it must NOT enter the orphan set, and the inode stays fully live.
    #[tokio::test]
    async fn test_open_unlink_hardlinked_decrements_only() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let file_id = make_file(&fs, b"a", b"hardlink").await;
        fs.link(&(&test_auth()).into(), file_id, 0, b"b")
            .await
            .unwrap();

        fs.open_handle_inc(file_id);
        fs.remove(&(&test_auth()).into(), 0, b"a").await.unwrap();

        // nlink dropped to 1, inode untouched, orphan set NOT written.
        match fs.inode_store.get(file_id).await.unwrap() {
            Inode::File(f) => assert_eq!(f.nlink, 1),
            _ => panic!("expected file"),
        }
        assert!(orphan_ids(&fs).await.is_empty());

        // The remaining link still works after closing the handle.
        fs.handle_closed(file_id).await;
        assert!(fs.inode_store.get(file_id).await.is_ok());
    }
}
