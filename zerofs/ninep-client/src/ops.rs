//! Compound operations shared by the FUSE mount and `zerofs-client`.
//!
//! Each helper picks the negotiated ZeroFS fast path when available, else the
//! vanilla 9P2000.L sequence, and owns any fid it allocates: on success the
//! returned fid is the caller's to clunk, on error nothing is left behind.
//! Centralizing the dispatch means a new fast path is added once.

use crate::{ClientError, ClientResult, NinePClient};
use ninep_proto::{
    DirEntry, DirEntryPlus, GETATTR_ALL, Qid, SETATTR_ATIME, SETATTR_ATIME_SET, SETATTR_GID,
    SETATTR_MODE, SETATTR_MTIME, SETATTR_MTIME_SET, SETATTR_SIZE, SETATTR_UID, Stat, Tsetattr,
};
use std::collections::VecDeque;

/// A time to set: the server's clock, or an explicit instant.
#[derive(Clone, Copy, Debug)]
pub enum SetattrTime {
    Now,
    At { sec: u64, nsec: u64 },
}

/// Assembles a `Tsetattr` from optional changes, so the valid-bit bookkeeping
/// lives in one place. `None` fields are left untouched by the server.
pub struct SetattrBuilder {
    ts: Tsetattr,
}

impl SetattrBuilder {
    pub fn new(fid: u32) -> Self {
        Self {
            ts: Tsetattr {
                fid,
                valid: 0,
                mode: 0,
                uid: 0,
                gid: 0,
                size: 0,
                atime_sec: 0,
                atime_nsec: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
            },
        }
    }

    /// Mode is passed through verbatim; mask before calling if needed.
    pub fn mode(mut self, mode: Option<u32>) -> Self {
        if let Some(mode) = mode {
            self.ts.valid |= SETATTR_MODE;
            self.ts.mode = mode;
        }
        self
    }

    pub fn uid(mut self, uid: Option<u32>) -> Self {
        if let Some(uid) = uid {
            self.ts.valid |= SETATTR_UID;
            self.ts.uid = uid;
        }
        self
    }

    pub fn gid(mut self, gid: Option<u32>) -> Self {
        if let Some(gid) = gid {
            self.ts.valid |= SETATTR_GID;
            self.ts.gid = gid;
        }
        self
    }

    pub fn size(mut self, size: Option<u64>) -> Self {
        if let Some(size) = size {
            self.ts.valid |= SETATTR_SIZE;
            self.ts.size = size;
        }
        self
    }

    pub fn atime(mut self, atime: Option<SetattrTime>) -> Self {
        match atime {
            Some(SetattrTime::Now) => self.ts.valid |= SETATTR_ATIME,
            Some(SetattrTime::At { sec, nsec }) => {
                self.ts.valid |= SETATTR_ATIME | SETATTR_ATIME_SET;
                self.ts.atime_sec = sec;
                self.ts.atime_nsec = nsec;
            }
            None => {}
        }
        self
    }

    pub fn mtime(mut self, mtime: Option<SetattrTime>) -> Self {
        match mtime {
            Some(SetattrTime::Now) => self.ts.valid |= SETATTR_MTIME,
            Some(SetattrTime::At { sec, nsec }) => {
                self.ts.valid |= SETATTR_MTIME | SETATTR_MTIME_SET;
                self.ts.mtime_sec = sec;
                self.ts.mtime_nsec = nsec;
            }
            None => {}
        }
        self
    }

    pub fn build(self) -> Tsetattr {
        self.ts
    }
}

impl NinePClient {
    /// Setattr returning the post-op stat: `Tsetattrattr` when the v2 fast
    /// path is negotiated, else setattr + getattr.
    pub async fn setattr_stat(&self, ts: Tsetattr) -> ClientResult<Stat> {
        if self.extensions_v2_enabled() {
            return self.setattr_attr(ts).await;
        }
        let fid = ts.fid;
        self.setattr(ts).await?;
        self.getattr(fid, GETATTR_ALL).await
    }

    /// Walk `names` from `from` and stat the destination on the caller-supplied
    /// `newfid`: one round trip via `Twalkgetattr` when the v1 fast path is
    /// negotiated, else walk + getattr. The caller owns `newfid` (it is returned
    /// for convenience); on error nothing is left server-side and the caller
    /// returns the number to the allocator. A partial walk (the server's way of
    /// reporting a missing intermediate) surfaces as `ENOENT`.
    pub async fn walk_stat(
        &self,
        from: u32,
        newfid: u32,
        names: &[&[u8]],
    ) -> ClientResult<(u32, Stat)> {
        // Twalkgetattr is full-walk-only; an empty walk (clone) takes the
        // plain path.
        if !names.is_empty() && self.extensions_enabled() {
            // A successful walk_getattr is always a full walk, so the fid exists.
            return self
                .walk_getattr(from, newfid, names)
                .await
                .map(|(_, stat)| (newfid, stat));
        }
        match self.walk(from, newfid, names).await {
            // Only a full walk creates `newfid` (a partial one leaves it unset).
            Ok(qids) if qids.len() == names.len() => {}
            Ok(_) => return Err(ClientError::Errno(libc::ENOENT as u32)),
            Err(e) => return Err(e),
        }
        match self.getattr(newfid, GETATTR_ALL).await {
            Ok(stat) => Ok((newfid, stat)),
            // Walk created the fid; clunk it so nothing is left server-side.
            Err(e) => {
                let _ = self.clunk(newfid).await;
                Err(e)
            }
        }
    }

    /// Open `from`'s inode on the caller-supplied `newfid`, leaving `from`
    /// untouched: `Tlopenat` when the v2 fast path is negotiated, else clone +
    /// lopen. If `fallback_flags` is given, a refused open is retried once with
    /// them (the writeback O_WRONLY→O_RDWR upgrade dance). The caller owns
    /// `newfid`; on error nothing is left server-side.
    pub async fn open_clone(
        &self,
        from: u32,
        newfid: u32,
        flags: u32,
        fallback_flags: Option<u32>,
    ) -> ClientResult<(u32, Qid, u32)> {
        if self.extensions_v2_enabled() {
            let mut res = self.lopenat(from, newfid, flags).await;
            if res.is_err()
                && let Some(orig) = fallback_flags
            {
                res = self.lopenat(from, newfid, orig).await;
            }
            // On error no fid was created server-side.
            return res.map(|(qid, iounit)| (newfid, qid, iounit));
        }
        self.walk(from, newfid, &[]).await?;
        let mut res = self.lopen(newfid, flags).await;
        if res.is_err()
            && let Some(orig) = fallback_flags
        {
            res = self.lopen(newfid, orig).await;
        }
        match res {
            Ok((qid, iounit)) => Ok((newfid, qid, iounit)),
            // Clone created the fid; clunk it so nothing is left server-side.
            Err(e) => {
                let _ = self.clunk(newfid).await;
                Err(e)
            }
        }
    }

    /// Create and open `name` under `dfid` on the caller-supplied `newfid`,
    /// leaving `dfid` untouched: `Tlcreateattr` when the v2 fast path is
    /// negotiated (the stat comes free), else clone + lcreate (no stat). The
    /// caller owns `newfid`; on error nothing is left server-side.
    pub async fn create_open(
        &self,
        dfid: u32,
        newfid: u32,
        name: &[u8],
        flags: u32,
        mode: u32,
        gid: u32,
    ) -> ClientResult<(u32, Option<Stat>, u32)> {
        if self.extensions_v2_enabled() {
            // On error no fid was created server-side.
            return self
                .lcreateattr(dfid, newfid, name, flags, mode, gid)
                .await
                .map(|(stat, iounit)| (newfid, Some(stat), iounit));
        }
        self.walk(dfid, newfid, &[]).await?;
        match self.lcreate(newfid, name, flags, mode, gid).await {
            Ok((_qid, iounit)) => Ok((newfid, None, iounit)),
            // Clone created the fid; clunk it so nothing is left server-side.
            Err(e) => {
                let _ = self.clunk(newfid).await;
                Err(e)
            }
        }
    }

    /// Walk to `name` under `dfid` for its stat on the create-family fallback
    /// path (vanilla 9P2000.L, whose create messages carry no stat). A
    /// caller-supplied `newfid` is guarded by the caller, which then owns
    /// reclamation if this future errors or is cancelled; `None` self-manages
    /// (alloc + free on error), for callers that run to completion.
    async fn walk_stat_surplus(
        &self,
        dfid: u32,
        newfid: Option<u32>,
        name: &[u8],
    ) -> ClientResult<(Option<u32>, Stat)> {
        match newfid {
            Some(nf) => self
                .walk_stat(dfid, nf, &[name])
                .await
                .map(|(fid, stat)| (Some(fid), stat)),
            None => {
                let nf = self.alloc_fid();
                match self.walk_stat(dfid, nf, &[name]).await {
                    Ok((fid, stat)) => Ok((Some(fid), stat)),
                    Err(e) => {
                        self.free_fid(nf);
                        Err(e)
                    }
                }
            }
        }
    }

    /// mkdir returning the new directory's stat: `Tmkdirattr` when v2 (no fid
    /// bound), else mkdir + walk_stat. On the fallback the caller may pass a
    /// guarded `newfid` (it then owns reclamation on error/cancel) or `None` to
    /// self-manage; the walked fid, when used, is returned for the caller to
    /// keep or clunk.
    pub async fn mkdir_stat(
        &self,
        dfid: u32,
        newfid: Option<u32>,
        name: &[u8],
        mode: u32,
        gid: u32,
    ) -> ClientResult<(Option<u32>, Stat)> {
        if self.extensions_v2_enabled() {
            return self
                .mkdir_attr(dfid, name, mode, gid)
                .await
                .map(|s| (None, s));
        }
        self.mkdir(dfid, name, mode, gid).await?;
        self.walk_stat_surplus(dfid, newfid, name).await
    }

    /// symlink returning the new link's stat; fid semantics as [`Self::mkdir_stat`].
    pub async fn symlink_stat(
        &self,
        dfid: u32,
        newfid: Option<u32>,
        name: &[u8],
        target: &[u8],
        gid: u32,
    ) -> ClientResult<(Option<u32>, Stat)> {
        if self.extensions_v2_enabled() {
            return self
                .symlink_attr(dfid, name, target, gid)
                .await
                .map(|s| (None, s));
        }
        self.symlink(dfid, name, target, gid).await?;
        self.walk_stat_surplus(dfid, newfid, name).await
    }

    /// mknod returning the new node's stat; fid semantics as [`Self::mkdir_stat`].
    #[allow(clippy::too_many_arguments)]
    pub async fn mknod_stat(
        &self,
        dfid: u32,
        newfid: Option<u32>,
        name: &[u8],
        mode: u32,
        major: u32,
        minor: u32,
        gid: u32,
    ) -> ClientResult<(Option<u32>, Stat)> {
        if self.extensions_v2_enabled() {
            return self
                .mknod_attr(dfid, name, mode, major, minor, gid)
                .await
                .map(|s| (None, s));
        }
        self.mknod(dfid, name, mode, major, minor, gid).await?;
        self.walk_stat_surplus(dfid, newfid, name).await
    }

    /// Hard-link `fid`'s inode as `name` under `dfid`, returning the post-op
    /// stat (updated nlink); fid semantics as [`Self::mkdir_stat`].
    pub async fn link_stat(
        &self,
        dfid: u32,
        newfid: Option<u32>,
        fid: u32,
        name: &[u8],
    ) -> ClientResult<(Option<u32>, Stat)> {
        if self.extensions_v2_enabled() {
            return self.link_attr(dfid, fid, name).await.map(|s| (None, s));
        }
        self.link(dfid, fid, name).await?;
        self.walk_stat_surplus(dfid, newfid, name).await
    }
}

/// An entry type with a 9P readdir cookie.
pub trait DirEntryCookie {
    fn cookie(&self) -> u64;
}

impl DirEntryCookie for DirEntry {
    fn cookie(&self) -> u64 {
        self.offset
    }
}

impl DirEntryCookie for DirEntryPlus {
    fn cookie(&self) -> u64 {
        self.offset
    }
}

/// The readdir paging state machine: a read-ahead buffer, the cookie for the
/// next fetch, and the EOF latch.
pub struct ReaddirState<E> {
    /// Entries fetched but not yet delivered.
    pub buf: VecDeque<E>,
    /// 9P cookie for the next fetch.
    pub fetch_cookie: u64,
    /// The offset a sequential consumer would continue from (the cookie of
    /// the last delivered entry); used by [`Self::seek`] to detect rewinds.
    pub resume_offset: u64,
    /// The server has no more entries past `fetch_cookie`.
    pub eof: bool,
}

impl<E: DirEntryCookie> ReaddirState<E> {
    pub fn starting_at(offset: u64) -> Self {
        Self {
            buf: VecDeque::new(),
            fetch_cookie: offset,
            resume_offset: offset,
            eof: false,
        }
    }

    /// A consumer offset that doesn't continue where we left off means a seek
    /// or rewind: drop the read-ahead and restart there.
    pub fn seek(&mut self, offset: u64) {
        if offset != self.resume_offset {
            self.buf.clear();
            self.fetch_cookie = offset;
            self.resume_offset = offset;
            self.eof = false;
        }
    }

    /// Fold a fetched batch in: advance the cookie past it, or latch EOF if
    /// the server returned nothing.
    pub fn absorb(&mut self, entries: Vec<E>) {
        match entries.last() {
            Some(last) => self.fetch_cookie = last.cookie(),
            None => self.eof = true,
        }
        self.buf.extend(entries);
    }
}
