//! The data plane: the per-file byte model, the writer actor, and the
//! content / extent-key / crash oracle over the fixed data files.

use crate::actor::ActorContext;
use crate::{auth, creds};
use bytes::Bytes;
use rand::Rng;
use rand::rngs::StdRng;
use std::collections::BTreeSet;
use std::time::Duration;
use zerofs::fs::inode::InodeId;
use zerofs::fs::types::{SetAttributes, SetSize};
use zerofs::fs::{EXTENT_SIZE, ZeroFS};

#[derive(Clone, Debug, Hash)]
pub(crate) enum FileOp {
    Write {
        offset: usize,
        len: usize,
        tag: u64,
    },
    Truncate {
        size: usize,
    },
    /// Hole punch: zero `[offset, offset+length)` in place without changing the
    /// file size (never extends; the tail past EOF is untouched).
    Trim {
        offset: usize,
        length: usize,
    },
}

/// Relative weights for one file actor's operation classes.
#[derive(Clone, Copy)]
pub(crate) struct FileOpMix {
    write_end: u32,
    mutation_end: u32,
    read_end: u32,
    total: u32,
}

impl FileOpMix {
    pub(crate) fn new(write: u32, mutation: u32, read: u32, fsync: u32) -> Self {
        Self {
            write_end: write,
            mutation_end: write + mutation,
            read_end: write + mutation + read,
            total: write + mutation + read + fsync,
        }
    }
}

/// Deterministic byte pattern for a write, reconstructible from its tag.
pub(crate) fn pattern(tag: u64, len: usize) -> Vec<u8> {
    let mut s = tag ^ 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

pub(crate) fn apply(model: &mut Vec<u8>, op: &FileOp) {
    match op {
        FileOp::Write { offset, len, tag } => {
            let end = offset + len;
            if model.len() < end {
                model.resize(end, 0);
            }
            model[*offset..end].copy_from_slice(&pattern(*tag, *len));
        }
        FileOp::Truncate { size } => model.resize(*size, 0),
        FileOp::Trim { offset, length } => {
            // Zeros only the intersection with the live bytes; a hole past EOF
            // is a no-op (trim never grows the file).
            if *offset < model.len() {
                let end = (offset + length).min(model.len());
                model[*offset..end].fill(0);
            }
        }
    }
}

fn replay(ops: &[FileOp]) -> Vec<u8> {
    let mut m = Vec::new();
    for op in ops {
        apply(&mut m, op);
    }
    m
}

pub(crate) struct FileState {
    pub(crate) id: InodeId,
    pub(crate) name: Vec<u8>,
    /// Attempted ops, pushed before the call is awaited. At most one op past
    /// `acked` (the in-flight one when a mid-round crash cancelled the writer);
    /// whether it applied is decided post-crash by the prefix search.
    pub(crate) ops: Vec<FileOp>,
    /// Ops whose ack was observed. `cur` replays exactly these.
    pub(crate) acked: usize,
    /// Ops index covered by the last acked fsync: the crash-survival floor.
    pub(crate) fsynced: usize,
    /// Replay of `ops[..acked]`, maintained incrementally.
    pub(crate) cur: Vec<u8>,
    /// Every op ever attempted, crash-discarded ones included. Diagnostics
    /// only: fingerprinting which op mystery bytes came from.
    pub(crate) history: Vec<FileOp>,
}

impl FileState {
    pub(crate) fn with_initial_op(id: InodeId, name: Vec<u8>, op: FileOp) -> Self {
        let cur = replay(std::slice::from_ref(&op));
        Self {
            id,
            name,
            ops: vec![op.clone()],
            acked: 1,
            fsynced: 0,
            cur,
            history: vec![op],
        }
    }
}

/// One writer owns one file for a round: seeded writes, truncates, verified
/// reads, and occasional fsyncs. Reads must match the model exactly (single
/// writer per file). A mid-round crash signal cancels the in-flight op at its
/// current await point, leaving it attempted-but-unacked in the model.
/// Returns the updated state.
impl FileState {
    pub(crate) async fn run_round(
        mut self,
        mut rng: StdRng,
        ops: usize,
        mix: FileOpMix,
        cap: usize,
        context: ActorContext,
    ) -> Self {
        let fs = context.fs.clone();
        let digest = context.digest.clone();
        let mut crashed = context.crashed.clone();
        let auth = auth();
        let creds = creds();
        // Half the reads concentrate on one window so the same cross-segment
        // seams are re-read repeatedly, which is what accrues pair heat.
        let hot_base = rng.gen_range(0..cap - 3 * EXTENT_SIZE);
        for op_no in 0..ops {
            if *crashed.borrow() {
                break;
            }
            let dice = rng.gen_range(0..mix.total);
            if dice < mix.write_end {
                // Write: anywhere in the cap, sized to cross extent boundaries.
                let offset = rng.gen_range(0..cap - 1);
                let len = rng.gen_range(1..=EXTENT_SIZE * 5 / 2).min(cap - offset);
                let tag = rng.r#gen::<u64>();
                let data = Bytes::from(pattern(tag, len));
                let op = FileOp::Write { offset, len, tag };
                self.ops.push(op.clone());
                self.history.push(op.clone());
                tokio::select! {
                    biased;
                    r = fs.write(&auth, self.id, offset as u64, &data) => {
                        match r {
                            Ok(_) => {
                                apply(&mut self.cur, &op);
                                self.acked = self.ops.len();
                                context.publish_ack(self.acked);
                                digest.event(("w", self.id, op_no, offset, len, tag));
                            }
                            // A failpoint crash isolates the store before the kill
                            // signal lands, so an in-flight op can error first; it
                            // stays attempted-unacked.
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("write({}, {offset}, {len}): {e:?}", self.id),
                        }
                    }
                    _ = crashed.changed() => return self,
                }
            } else if dice < mix.mutation_end && rng.gen_bool(0.4) {
                // Hole punch: zero an interior range without changing the size,
                // exercising extent elision and the "hole vs missing key" oracle.
                let offset = rng.gen_range(0..cap);
                let length = rng.gen_range(1..=EXTENT_SIZE * 3);
                let op = FileOp::Trim { offset, length };
                self.ops.push(op.clone());
                self.history.push(op.clone());
                tokio::select! {
                    biased;
                    r = fs.trim(&auth, self.id, offset as u64, length as u64) => {
                        match r {
                            Ok(()) => {
                                apply(&mut self.cur, &op);
                                self.acked = self.ops.len();
                                context.publish_ack(self.acked);
                                digest.event(("h", self.id, op_no, offset, length));
                            }
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("trim({}, {offset}, {length}): {e:?}", self.id),
                        }
                    }
                    _ = crashed.changed() => return self,
                }
            } else if dice < mix.mutation_end {
                let size = rng.gen_range(0..=cap);
                let op = FileOp::Truncate { size };
                self.ops.push(op.clone());
                self.history.push(op.clone());
                let setattr = SetAttributes {
                    size: SetSize::Set(size as u64),
                    ..Default::default()
                };
                tokio::select! {
                    biased;
                    r = fs.setattr(&creds, self.id, &setattr) => {
                        match r {
                            Ok(_) => {
                                apply(&mut self.cur, &op);
                                self.acked = self.ops.len();
                                context.publish_ack(self.acked);
                                digest.event(("t", self.id, op_no, size));
                            }
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("truncate({}, {size}): {e:?}", self.id),
                        }
                    }
                    _ = crashed.changed() => return self,
                }
            } else if dice < mix.read_end {
                // Verified read: a sequential whole-file scan (crosses every
                // segment seam, heating pairs for chain compaction) or a point
                // read, uniform or clustered on the hot window.
                if rng.gen_bool(0.3) {
                    // One call per pass over the whole file: seams are detected
                    // per read call, so chunked scans would miss most of them.
                    let chunk = self.cur.len().max(EXTENT_SIZE);
                    let mut at = 0usize;
                    let mut interrupted = false;
                    while at < self.cur.len() {
                        tokio::select! {
                            biased;
                            r = fs.read_file(&auth, self.id, at as u64, chunk as u32) => {
                                let (got, _eof) = match r {
                                    Ok(v) => v,
                                    Err(_) if *crashed.borrow() => {
                                        interrupted = true;
                                        break;
                                    }
                                    Err(e) => panic!("scan({}, {at}): {e:?}", self.id),
                                };
                                let end = (at + chunk).min(self.cur.len());
                                assert_eq!(
                                    got.as_ref(),
                                    &self.cur[at..end],
                                    "scan mismatch: file {} at {at}",
                                    self.id
                                );
                                at = end;
                            }
                            _ = crashed.changed() => { interrupted = true; break; }
                        }
                    }
                    if interrupted {
                        // No digest event on the cancellation path: the crash
                        // signal wakes every waiter at the same instant and their
                        // relative poll order is not a defined ordering, so an
                        // emission here would put an order-free race into the
                        // trace the replay check requires to be total.
                        return self;
                    }
                    digest.event(("s", self.id, op_no, at));
                    let pause = rng.gen_range(1..=3);
                    tokio::time::sleep(Duration::from_millis(pause)).await;
                    continue;
                }
                let offset = if rng.gen_bool(0.5) {
                    hot_base + rng.gen_range(0..EXTENT_SIZE)
                } else {
                    rng.gen_range(0..cap)
                };
                let count = rng.gen_range(1..=EXTENT_SIZE * 3);
                tokio::select! {
                    biased;
                    r = fs.read_file(&auth, self.id, offset as u64, count as u32) => {
                        let (got, _eof) = match r {
                            Ok(v) => v,
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("read({}, {offset}, {count}): {e:?}", self.id),
                        };
                        let end = (offset + count).min(self.cur.len());
                        let expected: &[u8] = if offset >= self.cur.len() {
                            &[]
                        } else {
                            &self.cur[offset..end]
                        };
                        assert_eq!(
                            got.as_ref(),
                            expected,
                            "read mismatch: file {} offset {offset} count {count}",
                            self.id
                        );
                        digest.event(("r", self.id, op_no, offset, count, got.len()));
                    }
                    _ = crashed.changed() => return self,
                }
            } else {
                // Snapshot every file's acked count first: anything acked before
                // the flush starts is covered by it, so the ack below may raise
                // every file's floor, not just this one's.
                let snapshot = context.begin_fsync();
                tokio::select! {
                    biased;
                    r = fs.client_fsync() => {
                        match r {
                            Ok(()) => {
                            context.finish_fsync(snapshot);
                                self.fsynced = self.acked;
                                digest.event(("f", self.id, op_no));
                            }
                            // A failed fsync raises no floor: the safe direction.
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("fsync: {e:?}"),
                        }
                    }
                    // A cancelled fsync that actually flushed just leaves the
                    // floor lower than it could be.
                    _ = crashed.changed() => return self,
                }
            }
            let pause = rng.gen_range(1..=3);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
        self
    }
}

#[derive(Debug)]
struct ExtentKeySet(BTreeSet<u64>);

impl ExtentKeySet {
    async fn read(fs: &ZeroFS, id: InodeId) -> Self {
        use futures::StreamExt;
        let codec = zerofs::fs::key_codec::KeyCodec::new();
        let mut keys = BTreeSet::new();
        let mut stream = fs
            .db
            .scan(codec.extent_key(id, 0)..codec.extent_key(id, u64::MAX))
            .await
            .expect("extent key scan");
        while let Some(item) = stream.next().await {
            let (key, _) = item.expect("extent key scan item");
            if let Some(index) = codec.parse_extent_key(&key) {
                keys.insert(index);
            }
        }
        Self(keys)
    }

    fn verify(&self, model: &[u8], id: InodeId, seed: u64, round: usize) {
        let nonzero: BTreeSet<u64> = model
            .chunks(EXTENT_SIZE)
            .enumerate()
            .filter(|(_, chunk)| chunk.iter().any(|byte| *byte != 0))
            .map(|(index, _)| index as u64)
            .collect();
        let missing: Vec<u64> = nonzero.difference(&self.0).copied().collect();
        assert!(
            missing.is_empty(),
            "extent keys missing for nonzero content (seed {seed}, round {round}, file {id}): \
             {missing:?}"
        );

        let eof_extent = (model.len() as u64).div_ceil(EXTENT_SIZE as u64);
        let beyond_eof: Vec<u64> = self.0.range(eof_extent..).copied().collect();
        assert!(
            beyond_eof.is_empty(),
            "extent keys at or beyond EOF (seed {seed}, round {round}, file {id}, \
             eof {eof_extent}): {beyond_eof:?}"
        );
    }
}

/// A complete read through the filesystem, used as the observed side of the
/// content and crash-prefix oracles.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FileSnapshot(Vec<u8>);

impl FileSnapshot {
    pub(crate) async fn read(fs: &ZeroFS, id: InodeId) -> Self {
        let auth = auth();
        let mut bytes = Vec::new();
        let chunk = 2 * EXTENT_SIZE as u32;
        loop {
            let (got, eof) = fs
                .read_file(&auth, id, bytes.len() as u64, chunk)
                .await
                .unwrap_or_else(|e| {
                    panic!("full read of inode {id} at {} failed: {e:?}", bytes.len())
                });
            bytes.extend_from_slice(&got);
            if eof || got.is_empty() {
                return Self(bytes);
            }
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl FileState {
    pub(crate) async fn verify_extent_keys(&self, fs: &ZeroFS, seed: u64, round: usize) {
        ExtentKeySet::read(fs, self.id)
            .await
            .verify(&self.cur, self.id, seed, round);
    }

    /// Adopt the surviving prefix after a crash, subject to the fsync floor.
    pub(crate) fn reconcile_after_crash(
        mut self,
        observed: FileSnapshot,
        seed: u64,
        round: usize,
    ) -> Self {
        for k in self.fsynced..=self.ops.len() {
            if replay(&self.ops[..k]).as_slice() == observed.as_bytes() {
                eprintln!(
                    "dst: reconcile file {} round {round}: k={k} of {} ops ({} acked, floor {})",
                    self.id,
                    self.ops.len(),
                    self.acked,
                    self.fsynced
                );
                self.ops.truncate(k);
                self.acked = k;
                self.fsynced = k;
                self.cur = observed.into_bytes();
                return self;
            }
        }

        let observed = observed.as_bytes();
        let mut detail = String::new();
        let mut diff_at = None;
        for k in self.fsynced..=self.ops.len() {
            let expected = replay(&self.ops[..k]);
            let diff = expected
                .iter()
                .zip(observed)
                .position(|(a, b)| a != b)
                .map(|offset| {
                    diff_at = Some(offset);
                    format!(
                        "first content diff at {offset} (extent {})",
                        offset / EXTENT_SIZE
                    )
                })
                .unwrap_or_else(|| "contents equal over common length".into());
            detail.push_str(&format!(
                "\n  k={k} ({:?}): expected len {}, observed len {}, {diff}",
                self.ops.get(k.wrapping_sub(1)),
                expected.len(),
                observed.len(),
            ));
        }

        if let Some(offset) = diff_at {
            let window = &observed[offset..(offset + 16).min(observed.len())];
            detail.push_str(&format!(
                "\n  observed[{offset}..+16] = {window:02x?}, candidates:"
            ));
            for (index, op) in self.history.iter().enumerate() {
                if let FileOp::Write {
                    offset: write_offset,
                    len,
                    tag,
                } = op
                    && offset >= *write_offset
                    && offset < write_offset + len
                {
                    let bytes = pattern(*tag, *len);
                    let start = offset - write_offset;
                    let candidate = &bytes[start..(start + 16).min(bytes.len())];
                    detail.push_str(&format!(
                        "\n    history[{index}] Write{{offset {write_offset}, len {len}}}: {}",
                        if candidate == window {
                            "MATCHES"
                        } else {
                            "differs"
                        }
                    ));
                }
            }
        }

        panic!(
            "DST ORACLE VIOLATION (seed {seed}, round {round}): file {} (inode {}) post-crash \
             content ({} bytes) matches no op-prefix >= the fsync floor ({} of {} ops, {} acked). \
             An fsync-acked or prefix-consistent state was lost.{detail}",
            String::from_utf8_lossy(&self.name),
            self.id,
            observed.len(),
            self.fsynced,
            self.ops.len(),
            self.acked,
        )
    }
}

// Two writers mutate disjoint, extent-aligned regions of one size-pinned file.
// Their operations commute while still contending on the inode lock and write
// coordinator, so each region retains an independent prefix oracle.

/// One writer's disjoint slice of a shared inode.
pub(crate) struct Region {
    pub(crate) id: InodeId,
    /// Absolute byte offset of the region start (extent-aligned).
    pub(crate) base: usize,
    /// Region length in bytes (extent-aligned; `base + len <= cap`).
    pub(crate) len: usize,
    /// Ops in absolute offsets, always confined to `[base, base+len)`.
    pub(crate) ops: Vec<FileOp>,
    pub(crate) acked: usize,
    pub(crate) fsynced: usize,
    /// Full-file-length replay of `ops[..acked]`; only `[base, base+len)` is
    /// owned by this writer (the rest carries the peer region's bytes, never
    /// read here).
    pub(crate) cur: Vec<u8>,
}

impl Region {
    pub(crate) fn new(id: InodeId, base: usize, len: usize, file_len: usize) -> Self {
        Self {
            id,
            base,
            len,
            ops: Vec::new(),
            acked: 0,
            fsynced: 0,
            cur: vec![0u8; file_len],
        }
    }

    /// Project a whole-file image onto this region, zero-padding a short image.
    fn project(&self, image: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; self.len];
        let available = image.get(self.base..).unwrap_or_default();
        let copied = available.len().min(self.len);
        out[..copied].copy_from_slice(&available[..copied]);
        out
    }

    /// Run one actor round over this region.
    pub(crate) async fn run_round(
        mut self,
        mut rng: StdRng,
        ops: usize,
        context: ActorContext,
    ) -> Self {
        let fs = context.fs.clone();
        let digest = context.digest.clone();
        let mut crashed = context.crashed.clone();
        let slot = context.trace_slot();
        let auth = auth();
        let lo = self.base;
        let hi = self.base + self.len;
        for op_no in 0..ops {
            if *crashed.borrow() {
                break;
            }
            let dice = rng.gen_range(0..10u32);
            if dice < 6 {
                // Write confined to the region (crosses extent seams inside it).
                let offset = rng.gen_range(lo..hi);
                let len = rng.gen_range(1..=(EXTENT_SIZE * 2).min(hi - offset));
                let tag = rng.r#gen::<u64>();
                let data = Bytes::from(pattern(tag, len));
                let op = FileOp::Write { offset, len, tag };
                self.ops.push(op.clone());
                tokio::select! {
                    biased;
                    res = fs.write(&auth, self.id, offset as u64, &data) => {
                        match res {
                            Ok(_) => {
                                apply(&mut self.cur, &op);
                                self.acked = self.ops.len();
                                context.publish_ack(self.acked);
                                digest.event(("rw", self.id, slot, op_no, offset, len, tag));
                            }
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("region write({}, {offset}, {len}): {e:?}", self.id),
                        }
                    }
                    _ = crashed.changed() => return self,
                }
            } else if dice < 8 {
                // Hole punch confined to the region.
                let offset = rng.gen_range(lo..hi);
                let length = rng.gen_range(1..=(hi - offset));
                let op = FileOp::Trim { offset, length };
                self.ops.push(op.clone());
                tokio::select! {
                    biased;
                    res = fs.trim(&auth, self.id, offset as u64, length as u64) => {
                        match res {
                            Ok(()) => {
                                apply(&mut self.cur, &op);
                                self.acked = self.ops.len();
                                context.publish_ack(self.acked);
                                digest.event(("rh", self.id, slot, op_no, offset, length));
                            }
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("region trim({}, {offset}, {length}): {e:?}", self.id),
                        }
                    }
                    _ = crashed.changed() => return self,
                }
            } else if dice < 9 {
                // Verified read within the region: every byte is within EOF (the
                // size is pinned), so the read returns exactly `count` bytes.
                let offset = rng.gen_range(lo..hi);
                let count = rng.gen_range(1..=(hi - offset));
                tokio::select! {
                    biased;
                    res = fs.read_file(&auth, self.id, offset as u64, count as u32) => {
                        let (got, _eof) = match res {
                            Ok(v) => v,
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("region read({}, {offset}, {count}): {e:?}", self.id),
                        };
                        assert_eq!(
                            got.as_ref(),
                            &self.cur[offset..offset + count],
                            "region read mismatch: inode {} slot {slot} offset {offset} count {count}",
                            self.id
                        );
                        digest.event(("rr", self.id, slot, op_no, offset, count));
                    }
                    _ = crashed.changed() => return self,
                }
            } else {
                // fsync: snapshot every stream's acked (a whole-fs flush covers them
                // all), and on ack raise every floor.
                let snapshot = context.begin_fsync();
                tokio::select! {
                    biased;
                    res = fs.client_fsync() => {
                        match res {
                            Ok(()) => {
                                context.finish_fsync(snapshot);
                                self.fsynced = self.acked;
                                digest.event(("rf", self.id, slot, op_no));
                            }
                            Err(_) if *crashed.borrow() => return self,
                            Err(e) => panic!("region fsync: {e:?}"),
                        }
                    }
                    _ = crashed.changed() => return self,
                }
            }
            let pause = rng.gen_range(1..=3);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
        self
    }

    /// Adopt the region prefix visible in the recovered whole-file snapshot.
    pub(crate) fn reconcile_after_crash(
        mut self,
        observed: &FileSnapshot,
        seed: u64,
        round: usize,
    ) -> Self {
        let observed_region = self.project(observed.as_bytes());
        for k in self.fsynced..=self.ops.len() {
            if self.project(&replay(&self.ops[..k])) == observed_region {
                self.ops.truncate(k);
                self.acked = k;
                self.fsynced = k;
                self.cur = observed.as_bytes().to_vec();
                return self;
            }
        }

        panic!(
            "DST REGION ORACLE VIOLATION (seed {seed}, round {round}): inode {} region \
             [{}, {}) matches no op-prefix >= the fsync floor ({} of {} ops, {} acked). \
             A concurrent-write region lost an fsync-acked or prefix-consistent state.",
            self.id,
            self.base,
            self.base + self.len,
            self.fsynced,
            self.ops.len(),
            self.acked,
        );
    }
}
