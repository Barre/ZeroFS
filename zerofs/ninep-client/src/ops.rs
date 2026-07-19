//! ZeroFS compound operations shared by the FUSE mount and `zerofs-client`.
//!
//! Returned fids belong to the caller. Errors leave no server-side fid.

use crate::{ClientResult, NinePClient};
use bytes::Bytes;
use ninep_proto::{
    DirEntry, DirEntryPlus, Qid, SETATTR_ATIME, SETATTR_ATIME_SET, SETATTR_GID, SETATTR_MODE,
    SETATTR_MTIME, SETATTR_MTIME_SET, SETATTR_SIZE, SETATTR_UID, Tsetattr,
};
use std::collections::VecDeque;

/// A time to set: the server's clock, or an explicit instant.
#[derive(Clone, Copy, Debug)]
pub enum SetattrTime {
    Now,
    At { sec: u64, nsec: u64 },
}

/// Builds a `Tsetattr`; `None` fields remain unchanged.
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
    /// Opens `from` on `newfid`. On error, `fallback_flags` permits one retry.
    /// The caller owns `newfid`.
    pub async fn open_clone(
        &self,
        from: u32,
        newfid: u32,
        flags: u32,
        fallback_flags: Option<u32>,
    ) -> ClientResult<(Qid, u32)> {
        let mut res = self.lopenat(from, newfid, flags).await;
        if res.is_err()
            && let Some(orig) = fallback_flags
        {
            res = self.lopenat(from, newfid, orig).await;
        }
        res
    }

    /// Opens `from` on `newfid` and optionally prefetches from offset zero.
    /// Returns data only when the prefetch reaches EOF. The caller owns `newfid`.
    pub async fn open_clone_prefetch(
        &self,
        from: u32,
        newfid: u32,
        flags: u32,
        prefetch: u32,
    ) -> ClientResult<(Qid, u32, Option<Bytes>)> {
        if prefetch > 0 {
            let (qid, iounit, data, eof) = self.lopenatread(from, newfid, flags, prefetch).await?;
            let buf = eof.then_some(data);
            return Ok((qid, iounit, buf));
        }
        let (qid, iounit) = self.open_clone(from, newfid, flags, None).await?;
        Ok((qid, iounit, None))
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

/// Readdir paging state.
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

    /// Resets read-ahead when `offset` differs from the current position.
    pub fn seek(&mut self, offset: u64) {
        if offset != self.resume_offset {
            self.buf.clear();
            self.fetch_cookie = offset;
            self.resume_offset = offset;
            self.eof = false;
        }
    }

    /// Adds a fetched batch and advances the next cookie. Empty input marks EOF.
    pub fn absorb(&mut self, entries: Vec<E>) {
        match entries.last() {
            Some(last) => self.fetch_cookie = last.cookie(),
            None => self.eof = true,
        }
        self.buf.extend(entries);
    }
}
