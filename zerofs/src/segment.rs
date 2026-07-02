//! Immutable, self-describing data-plane segment objects (RFC-0025).
//!
//! A segment packs many per-extent frames, an AEAD-sealed reverse-map directory,
//! and a fixed plaintext footer (written last). Layout:
//!
//! ```text
//! [frame_0][frame_1]...[frame_{k-1}]   packed, no padding
//! [directory]                          AEAD-sealed reverse map (GC/recovery only)
//! [footer]                             64 bytes, plaintext, last
//! ```
//!
//! Each frame is length-prefixed and self-describing, so a read can walk a GET'd
//! byte range without consulting the directory:
//!
//! ```text
//! frame_i = [len: u32 LE][ sealed_frame ]      // len = byte length of sealed_frame
//! ```
//!
//! `sealed_frame` is [`crate::frame_codec`] output bound to
//! `AAD = b"F" || segid || frame_index || inode || extent`, so a frame cannot be
//! lifted to another segment, slot, or logical block. The directory is bound to
//! `AAD = b"D" || segid || k`.
//!
//! Note: "segment" here means a data-plane object, unrelated to SlateDB's
//! key-domain [`segment_extractor`](crate::segment_extractor).

#[cfg(test)]
use bytes::Bytes;

use crate::frame_codec::{CodecError, FrameCodec};

/// HKDF info label for the data-plane segment subkey. Domain-separated from the
/// block-transformer subkey so a block frame and a segment frame never share a key.
pub const SEGMENT_INFO: &[u8] = b"zerofs-v1-segment";

pub(crate) const FOOTER_LEN: usize = 64;
const MAGIC: &[u8; 4] = b"ZSEG";
const VERSION: u32 = 1;
const DIR_ENTRY_LEN: usize = 28; // byte_offset(8) + len(4) + inode(8) + extent(8)
pub(crate) const LEN_PREFIX: usize = 4;

// Footer field offsets within the trailing 64 bytes.
const F_MAGIC: usize = 0; // 4
const F_VERSION: usize = 4; // 4
const F_K: usize = 8; // 4
const F_DIR_OFFSET: usize = 12; // 8
const F_DIR_LEN: usize = 20; // 4
const F_SEALED_EPOCH: usize = 24; // 8
const F_COUNTER: usize = 32; // 8
const F_SEALED_SEQNO: usize = 40; // 8
const F_TOTAL_LEN: usize = 48; // 8
const F_CRC: usize = 56; // 4 (crc32c over bytes[dir_offset .. total_len - 8]: the directory + footer)
const F_RESERVED: usize = 60; // 4 (must be zero)

/// Logical segment identity. Epoch-namespaced so two leader terms can never
/// target the same object key. Stored as two u64s, never bit-packed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Segid {
    pub epoch: u64,
    pub counter: u64,
}

impl Segid {
    pub fn new(epoch: u64, counter: u64) -> Self {
        Self { epoch, counter }
    }

    fn to_le_bytes(self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&self.epoch.to_le_bytes());
        b[8..16].copy_from_slice(&self.counter.to_le_bytes());
        b
    }

    /// Object key under which this segment is stored. Sharded on the counter's
    /// low byte so consecutive segments round-robin across 256 object-store
    /// prefixes: classic S3 hot-prefixes on a monotonic tail otherwise, and reads
    /// are exact-key (FrameLoc -> path), so the shard costs the read path nothing.
    pub fn object_key(self) -> String {
        format!(
            "segments/{:02x}/{:016x}/{:016x}",
            self.counter & 0xff,
            self.epoch,
            self.counter
        )
    }

    /// Parse a `segments/<shard>/<epoch>/<counter>` object key back into a
    /// [`Segid`]. The shard is derived from the counter, so it's ignored here.
    pub fn from_object_key(key: &str) -> Option<Segid> {
        let rest = key.strip_prefix("segments/")?;
        let (_shard, rest) = rest.split_once('/')?;
        let (epoch, counter) = rest.split_once('/')?;
        Some(Segid::new(
            u64::from_str_radix(epoch, 16).ok()?,
            u64::from_str_radix(counter, 16).ok()?,
        ))
    }
}

/// One directory entry: where a frame lives and which logical block it backs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DirEntry {
    pub byte_offset: u64,
    pub len: u32,
    pub inode: u64,
    pub extent: u64,
}

/// Location of one extent's frame inside a sealed segment. Persisted as the value
/// of the extent's extent-index key via [`FrameLoc::encode`]/[`FrameLoc::decode`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameLoc {
    pub segid: Segid,
    pub frame_index: u32,
    /// Offset of the frame's length prefix within the segment.
    pub byte_offset: u64,
    /// Byte span covering the length prefix and the sealed frame.
    pub byte_len: u32,
}

impl FrameLoc {
    pub const ENCODED_LEN: usize = 32;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut b = [0u8; Self::ENCODED_LEN];
        b[0..8].copy_from_slice(&self.segid.epoch.to_le_bytes());
        b[8..16].copy_from_slice(&self.segid.counter.to_le_bytes());
        b[16..20].copy_from_slice(&self.frame_index.to_le_bytes());
        b[20..28].copy_from_slice(&self.byte_offset.to_le_bytes());
        b[28..32].copy_from_slice(&self.byte_len.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Option<FrameLoc> {
        if b.len() != Self::ENCODED_LEN {
            return None;
        }
        Some(FrameLoc {
            segid: Segid::new(rd_u64(b, 0), rd_u64(b, 8)),
            frame_index: rd_u32(b, 16),
            byte_offset: rd_u64(b, 20),
            byte_len: rd_u32(b, 28),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("segment too small: {0} bytes")]
    TooSmall(usize),
    #[error("bad segment magic")]
    BadMagic,
    #[error("unsupported segment version {0}")]
    BadVersion(u32),
    #[error("segment crc mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
    #[error("malformed segment: {0}")]
    Malformed(&'static str),
    #[error("segment footer segid {found:?} does not match requested key {expected:?}")]
    SegidMismatch { expected: Segid, found: Segid },
    #[error(transparent)]
    Codec(#[from] CodecError),
}

fn frame_aad(segid: Segid, frame_index: u32, inode: u64, extent: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 16 + 4 + 8 + 8);
    v.push(b'F');
    v.extend_from_slice(&segid.to_le_bytes());
    v.extend_from_slice(&frame_index.to_le_bytes());
    v.extend_from_slice(&inode.to_le_bytes());
    v.extend_from_slice(&extent.to_le_bytes());
    v
}

fn dir_aad(segid: Segid, k: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 16 + 4);
    v.push(b'D');
    v.extend_from_slice(&segid.to_le_bytes());
    v.extend_from_slice(&k.to_le_bytes());
    v
}

fn rd_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}
fn rd_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

/// Builds one segment by appending frames, then serializing directory + footer.
pub struct SegmentBuilder<'a> {
    codec: &'a FrameCodec,
    segid: Segid,
    buf: Vec<u8>,
    dir: Vec<DirEntry>,
}

impl<'a> SegmentBuilder<'a> {
    pub fn new(codec: &'a FrameCodec, segid: Segid) -> Self {
        Self {
            codec,
            segid,
            buf: Vec::new(),
            dir: Vec::new(),
        }
    }

    /// Current byte length of the packed frame region (the next frame's offset).
    pub fn byte_len(&self) -> u64 {
        self.buf.len() as u64
    }

    /// Seal `plaintext` for logical block `(inode, extent)` and append it.
    /// Returns the frame's index within the segment.
    pub fn add_frame(
        &mut self,
        inode: u64,
        extent: u64,
        plaintext: &[u8],
    ) -> Result<u32, SegmentError> {
        let frame_index = self.dir.len() as u32;
        let sealed = seal_frame(
            self.codec,
            self.segid,
            frame_index,
            inode,
            extent,
            plaintext,
        )?;
        let byte_offset = self.buf.len() as u64;
        let len = sealed.len() as u32;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(&sealed);
        self.dir.push(DirEntry {
            byte_offset,
            len,
            inode,
            extent,
        });
        Ok(frame_index)
    }

    /// Finalize the segment bytes: append the sealed directory and the footer.
    pub fn finish(self, sealed_seqno: u64) -> Result<Vec<u8>, SegmentError> {
        let SegmentBuilder {
            codec,
            segid,
            buf,
            dir,
        } = self;
        finalize_segment(codec, segid, buf, &dir, sealed_seqno)
    }
}

/// Compress+encrypt one extent into a sealed frame body (no length prefix), bound
/// to its `(segid, frame_index, inode, extent)` AAD. Shared by [`SegmentBuilder`]
/// and the data plane's open-segment buffer.
pub(crate) fn seal_frame(
    codec: &FrameCodec,
    segid: Segid,
    frame_index: u32,
    inode: u64,
    extent: u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, SegmentError> {
    Ok(codec.seal(plaintext, &frame_aad(segid, frame_index, inode, extent))?)
}

/// [`seal_frame`] over an already-compressed payload. The write path compresses
/// outside the open-segment lock (compression is the expensive half of the codec
/// and is independent of the AAD) and binds `(segid, frame_index)` here, under
/// the lock that assigns them.
pub(crate) fn seal_compressed_frame(
    codec: &FrameCodec,
    segid: Segid,
    frame_index: u32,
    inode: u64,
    extent: u64,
    compressed: Vec<u8>,
) -> Result<Vec<u8>, SegmentError> {
    Ok(codec.seal_compressed(compressed, &frame_aad(segid, frame_index, inode, extent))?)
}

/// Seal the segment directory into its AEAD frame. This is the one fallible step
/// of finalizing, kept separate from [`assemble_segment`] so a caller holding the
/// live open buffer can run it BEFORE taking/rotating that buffer: on failure the
/// buffer stays intact to retry, instead of its extent bytes being dropped while
/// the already-committed FrameLocs are left dangling.
pub(crate) fn seal_directory(
    codec: &FrameCodec,
    segid: Segid,
    dir: &[DirEntry],
) -> Result<Vec<u8>, SegmentError> {
    let k = dir.len() as u32;
    let mut dir_plain = Vec::with_capacity(dir.len() * DIR_ENTRY_LEN);
    for e in dir {
        dir_plain.extend_from_slice(&e.byte_offset.to_le_bytes());
        dir_plain.extend_from_slice(&e.len.to_le_bytes());
        dir_plain.extend_from_slice(&e.inode.to_le_bytes());
        dir_plain.extend_from_slice(&e.extent.to_le_bytes());
    }
    Ok(codec.seal(&dir_plain, &dir_aad(segid, k))?)
}

/// Append an already-sealed directory (from [`seal_directory`]) and the plaintext
/// CRC footer to a frame region, producing the final segment object bytes. `k` is
/// the directory entry count. Infallible: once [`seal_directory`] has succeeded,
/// finalization cannot fail partway, so a caller may take the buffer knowing the
/// segment will materialize. Shared by [`SegmentBuilder`] and the data plane's
/// open-segment buffer.
pub(crate) fn assemble_segment(
    segid: Segid,
    mut buf: Vec<u8>,
    k: u32,
    sealed_dir: &[u8],
    sealed_seqno: u64,
) -> Vec<u8> {
    let dir_offset = buf.len() as u64;
    let dir_len = sealed_dir.len() as u32;
    buf.extend_from_slice(sealed_dir);

    let total_len = (buf.len() + FOOTER_LEN) as u64;
    let mut footer = [0u8; FOOTER_LEN];
    footer[F_MAGIC..F_MAGIC + 4].copy_from_slice(MAGIC);
    footer[F_VERSION..F_VERSION + 4].copy_from_slice(&VERSION.to_le_bytes());
    footer[F_K..F_K + 4].copy_from_slice(&k.to_le_bytes());
    footer[F_DIR_OFFSET..F_DIR_OFFSET + 8].copy_from_slice(&dir_offset.to_le_bytes());
    footer[F_DIR_LEN..F_DIR_LEN + 4].copy_from_slice(&dir_len.to_le_bytes());
    footer[F_SEALED_EPOCH..F_SEALED_EPOCH + 8].copy_from_slice(&segid.epoch.to_le_bytes());
    footer[F_COUNTER..F_COUNTER + 8].copy_from_slice(&segid.counter.to_le_bytes());
    footer[F_SEALED_SEQNO..F_SEALED_SEQNO + 8].copy_from_slice(&sealed_seqno.to_le_bytes());
    footer[F_TOTAL_LEN..F_TOTAL_LEN + 8].copy_from_slice(&total_len.to_le_bytes());
    // F_RESERVED stays zero. The CRC covers only the metadata tail — the directory
    // plus footer[0..F_CRC], i.e. bytes[dir_offset .. total_len - 8]. Frames carry
    // per-frame AEAD, so they aren't re-covered here; this keeps the CRC a cheap
    // keyless check over the one plaintext region (the footer) and lets a reader
    // verify it from a ranged tail read instead of the whole object.
    buf.extend_from_slice(&footer);
    let crc_end = buf.len() - (FOOTER_LEN - F_CRC);
    let crc = crc32c::crc32c(&buf[dir_offset as usize..crc_end]);
    let crc_pos = buf.len() - FOOTER_LEN + F_CRC;
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Seal the directory and assemble the final segment bytes in one step, for
/// callers that own `buf` outright and have nothing to preserve on error
/// ([`SegmentBuilder::finish`], HA-takeover materialization). The open-segment
/// buffer instead uses [`seal_directory`] + [`assemble_segment`] so a seal error
/// can't drop it.
pub(crate) fn finalize_segment(
    codec: &FrameCodec,
    segid: Segid,
    buf: Vec<u8>,
    dir: &[DirEntry],
    sealed_seqno: u64,
) -> Result<Vec<u8>, SegmentError> {
    let sealed_dir = seal_directory(codec, segid, dir)?;
    Ok(assemble_segment(
        segid,
        buf,
        dir.len() as u32,
        &sealed_dir,
        sealed_seqno,
    ))
}

/// Metadata read out of a segment's 64-byte footer — enough to locate and verify
/// the directory from a ranged tail read, without the whole object.
pub(crate) struct FooterMeta {
    pub segid: Segid,
    pub k: u32,
    pub dir_offset: u64,
    pub dir_len: u32,
    pub crc: u32,
}

/// Validate a segment's footer against the object size and return its
/// [`FooterMeta`]. Checks magic/version/total_len/reserved and that the directory
/// abuts the footer (which also keeps the CRC slice `[dir_offset..total_len-8]`
/// in bounds).
pub(crate) fn parse_footer(footer: &[u8], object_size: u64) -> Result<FooterMeta, SegmentError> {
    if footer.len() != FOOTER_LEN {
        return Err(SegmentError::TooSmall(footer.len()));
    }
    if &footer[F_MAGIC..F_MAGIC + 4] != MAGIC {
        return Err(SegmentError::BadMagic);
    }
    let version = rd_u32(footer, F_VERSION);
    if version != VERSION {
        return Err(SegmentError::BadVersion(version));
    }
    let total_len = rd_u64(footer, F_TOTAL_LEN);
    if total_len != object_size {
        return Err(SegmentError::Malformed(
            "total_len does not match object size",
        ));
    }
    if rd_u32(footer, F_RESERVED) != 0 {
        return Err(SegmentError::Malformed("reserved footer bytes nonzero"));
    }
    let dir_offset = rd_u64(footer, F_DIR_OFFSET);
    let dir_len = rd_u32(footer, F_DIR_LEN);
    // The directory is the last thing before the footer, so it must abut it.
    let abut = dir_offset
        .checked_add(dir_len as u64)
        .and_then(|e| e.checked_add(FOOTER_LEN as u64));
    if abut != Some(total_len) {
        return Err(SegmentError::Malformed("directory does not abut footer"));
    }
    Ok(FooterMeta {
        segid: Segid::new(rd_u64(footer, F_SEALED_EPOCH), rd_u64(footer, F_COUNTER)),
        k: rd_u32(footer, F_K),
        dir_offset,
        dir_len,
        crc: rd_u32(footer, F_CRC),
    })
}

/// Verify and decode a directory from just its bytes plus the footer, for the
/// ranged directory read (no whole-object fetch). Checks the tail CRC (directory
/// + footer[0..F_CRC]) then AEAD-opens the directory.
pub(crate) fn decode_directory(
    codec: &FrameCodec,
    dir_bytes: &[u8],
    footer: &[u8],
    meta: &FooterMeta,
) -> Result<Vec<DirEntry>, SegmentError> {
    if dir_bytes.len() != meta.dir_len as usize || footer.len() != FOOTER_LEN {
        return Err(SegmentError::Malformed("directory/footer length mismatch"));
    }
    let computed = crc32c::crc32c_append(crc32c::crc32c(dir_bytes), &footer[..F_CRC]);
    if computed != meta.crc {
        return Err(SegmentError::CrcMismatch {
            stored: meta.crc,
            computed,
        });
    }
    let plain = codec.open(dir_bytes, &dir_aad(meta.segid, meta.k))?;
    parse_dir_entries(&plain, meta.k)
}

/// Decode `k` [`DirEntry`]s from the opened (plaintext) directory bytes.
fn parse_dir_entries(plain: &[u8], k: u32) -> Result<Vec<DirEntry>, SegmentError> {
    if plain.len() != k as usize * DIR_ENTRY_LEN {
        return Err(SegmentError::Malformed("directory length mismatch"));
    }
    let mut out = Vec::with_capacity(k as usize);
    for i in 0..k as usize {
        let o = i * DIR_ENTRY_LEN;
        out.push(DirEntry {
            byte_offset: rd_u64(plain, o),
            len: rd_u32(plain, o + 8),
            inode: rd_u64(plain, o + 12),
            extent: rd_u64(plain, o + 20),
        });
    }
    Ok(out)
}

/// Parsed, integrity-checked whole-object view over a segment's bytes. Test-only:
/// production reads go through the ranged `parse_footer`/`read_run`/`read_directory`
/// paths; this wraps them to assert the on-disk format end to end.
#[cfg(test)]
pub struct Segment {
    pub segid: Segid,
    pub k: u32,
    pub dir_offset: u64,
    pub dir_len: u32,
    pub sealed_seqno: u64,
    bytes: Bytes,
}

#[cfg(test)]
impl Segment {
    /// Parse the footer, verify magic/version/total_len, and verify the CRC over
    /// the metadata tail. The CRC is a keyless torn-write detector; per-frame
    /// AEAD remains the integrity authority on read.
    pub fn parse(bytes: Bytes) -> Result<Segment, SegmentError> {
        let n = bytes.len();
        if n < FOOTER_LEN {
            return Err(SegmentError::TooSmall(n));
        }
        let footer = &bytes[n - FOOTER_LEN..];
        let meta = parse_footer(footer, n as u64)?;
        // CRC slice: bytes[dir_offset .. n-8] (directory + footer[0..F_CRC]).
        let computed = crc32c::crc32c(&bytes[meta.dir_offset as usize..n - 8]);
        if computed != meta.crc {
            return Err(SegmentError::CrcMismatch {
                stored: meta.crc,
                computed,
            });
        }
        Ok(Segment {
            segid: meta.segid,
            k: meta.k,
            dir_offset: meta.dir_offset,
            dir_len: meta.dir_len,
            sealed_seqno: rd_u64(footer, F_SEALED_SEQNO),
            bytes,
        })
    }

    /// Read the `slots.len()` frames packed in `[byte_offset, byte_offset+byte_len)`,
    /// where `slots[i] = (inode, extent)` for the frame at absolute index
    /// `first_frame + i`. This is the data-plane (extent-driven) read path: it
    /// never consults the directory.
    pub fn read_range(
        &self,
        codec: &FrameCodec,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(u64, u64)],
    ) -> Result<Vec<Vec<u8>>, SegmentError> {
        let start = byte_offset as usize;
        let end = start
            .checked_add(byte_len as usize)
            .ok_or(SegmentError::Malformed("read range overflow"))?;
        let body_len = self.bytes.len() - FOOTER_LEN;
        if end > body_len {
            return Err(SegmentError::Malformed("read range exceeds frame region"));
        }
        read_frames_from_region(
            codec,
            &self.bytes[start..end],
            self.segid,
            first_frame,
            slots,
        )
    }

    /// Open and parse the AEAD-sealed directory (GC/coalescer/recovery path).
    pub fn directory(&self, codec: &FrameCodec) -> Result<Vec<DirEntry>, SegmentError> {
        let start = self.dir_offset as usize;
        let end = start + self.dir_len as usize;
        let plain = codec.open(&self.bytes[start..end], &dir_aad(self.segid, self.k))?;
        parse_dir_entries(&plain, self.k)
    }
}

/// Frames per run before decode fans out on rayon; below it the dispatch
/// overhead outweighs the parallelism (the write path's compression threshold,
/// mirrored).
const PARALLEL_OPEN_MIN_FRAMES: usize = 8;

/// Walk the length-prefixed frames packed in `region`, opening each under the
/// AAD derived from its absolute index and logical block.
pub(crate) fn read_frames_from_region(
    codec: &FrameCodec,
    region: &[u8],
    segid: Segid,
    first_frame: u32,
    slots: &[(u64, u64)],
) -> Result<Vec<Vec<u8>>, SegmentError> {
    // Pass 1, serial by nature: each frame's start is only known from the
    // previous frame's length prefix. Collect every frame's span and AAD inputs.
    let mut spans: Vec<(std::ops::Range<usize>, u32, u64, u64)> = Vec::with_capacity(slots.len());
    let mut pos = 0usize;
    for (i, &(inode, extent)) in slots.iter().enumerate() {
        if pos + LEN_PREFIX > region.len() {
            return Err(SegmentError::Malformed("frame length prefix out of bounds"));
        }
        let len = rd_u32(region, pos) as usize;
        pos += LEN_PREFIX;
        let frame_end = pos
            .checked_add(len)
            .ok_or(SegmentError::Malformed("frame length overflow"))?;
        if frame_end > region.len() {
            return Err(SegmentError::Malformed("frame body out of bounds"));
        }
        let fi = first_frame
            .checked_add(i as u32)
            .ok_or(SegmentError::Malformed("frame index overflow"))?;
        spans.push((pos..frame_end, fi, inode, extent));
        pos = frame_end;
    }

    // Pass 2: the AEAD verify + decompress is a pure function per frame, so a
    // large coalesced run decodes across cores. block_in_place needs the
    // multi-thread runtime (current-thread tests decode inline), and with no
    // runtime at all (unit tests, sync callers) rayon can run directly.
    let open = |(range, fi, inode, extent): &(std::ops::Range<usize>, u32, u64, u64)| -> Result<Vec<u8>, SegmentError> {
        Ok(codec.open(
            &region[range.clone()],
            &frame_aad(segid, *fi, *inode, *extent),
        )?)
    };
    if spans.len() >= PARALLEL_OPEN_MIN_FRAMES {
        let parallel = || {
            use rayon::prelude::*;
            spans.par_iter().map(open).collect::<Result<Vec<_>, _>>()
        };
        return match tokio::runtime::Handle::try_current() {
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(parallel)
            }
            Ok(_) => spans.iter().map(open).collect(),
            Err(_) => parallel(),
        };
    }
    spans.iter().map(open).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionConfig;

    fn codec() -> FrameCodec {
        FrameCodec::new(&[5u8; 32], SEGMENT_INFO, CompressionConfig::Zstd(3))
    }

    // A run past PARALLEL_OPEN_MIN_FRAMES decodes on rayon (no tokio runtime in
    // a plain #[test], so the direct-rayon branch) and must roundtrip with the
    // per-frame AADs intact and in order.
    #[test]
    fn large_run_roundtrips_through_parallel_decode() {
        let c = codec();
        let segid = Segid::new(3, 9);
        let frames: Vec<(u64, u64, Vec<u8>)> = (0..(PARALLEL_OPEN_MIN_FRAMES as u64 * 2))
            .map(|i| (7, i, vec![i as u8; 32 * 1024]))
            .collect();
        let bytes = build(&c, segid, &frames);
        let seg = Segment::parse(Bytes::from(bytes)).unwrap();
        let slots: Vec<(u64, u64)> = frames.iter().map(|(ino, ext, _)| (*ino, *ext)).collect();
        let got = seg
            .read_range(&c, 0, seg.dir_offset as u32, 0, &slots)
            .unwrap();
        for ((_, _, want), got) in frames.iter().zip(&got) {
            assert_eq!(want, got);
        }
    }

    fn sample_frames() -> Vec<(u64, u64, Vec<u8>)> {
        vec![
            (10, 0, vec![0u8; 0]),            // empty
            (10, 1, vec![0xAA; 100]),         // small
            (10, 2, vec![0x55; 32 * 1024]),   // full extent
            (42, 0, b"hello world".to_vec()), // different inode
        ]
    }

    fn build(codec: &FrameCodec, segid: Segid, frames: &[(u64, u64, Vec<u8>)]) -> Vec<u8> {
        let mut b = SegmentBuilder::new(codec, segid);
        for (ino, extent, data) in frames {
            b.add_frame(*ino, *extent, data).unwrap();
        }
        b.finish(777).unwrap()
    }

    #[test]
    fn object_key_round_trips_and_shards_on_counter() {
        for (epoch, counter) in [
            (0u64, 0u64),
            (5, 0x23b),
            (u64::MAX, u64::MAX),
            (9, 255),
            (9, 256),
        ] {
            let s = Segid::new(epoch, counter);
            let key = s.object_key();
            assert_eq!(Segid::from_object_key(&key), Some(s), "round-trips: {key}");
            let want = format!("segments/{:02x}/", counter & 0xff);
            assert!(
                key.starts_with(&want),
                "{key} must shard on the counter low byte"
            );
        }
        // Consecutive counters round-robin across 256 shards; wraps every 256.
        let shard = |c: u64| Segid::new(9, c).object_key()[9..11].to_string();
        assert_ne!(shard(0), shard(1));
        assert_eq!(shard(0), shard(256));
    }

    #[test]
    fn roundtrips_all_frames_via_directory_offsets() {
        let c = codec();
        let segid = Segid::new(3, 9);
        let frames = sample_frames();
        let bytes = build(&c, segid, &frames);
        let seg = Segment::parse(Bytes::from(bytes)).unwrap();

        assert_eq!(seg.segid, segid);
        assert_eq!(seg.sealed_seqno, 777);
        assert_eq!(seg.k as usize, frames.len());

        let dir = seg.directory(&c).unwrap();
        assert_eq!(dir.len(), frames.len());
        for (i, (e, (ino, extent, data))) in dir.iter().zip(&frames).enumerate() {
            assert_eq!(e.inode, *ino);
            assert_eq!(e.extent, *extent);
            // Read this single frame mid-segment at its directory offset, with the
            // correct absolute frame index. Exercises sub-range reads (what extents do).
            let got = seg
                .read_range(
                    &c,
                    e.byte_offset,
                    e.len + LEN_PREFIX as u32,
                    i as u32,
                    &[(*ino, *extent)],
                )
                .unwrap();
            assert_eq!(got.len(), 1);
            assert_eq!(&got[0], data);
        }

        // Contiguous read of every frame from offset 0.
        let slots: Vec<(u64, u64)> = frames.iter().map(|(i, c, _)| (*i, *c)).collect();
        let all = seg
            .read_range(&c, 0, seg.dir_offset as u32, 0, &slots)
            .unwrap();
        assert_eq!(all.len(), frames.len());
        for (got, (_, _, data)) in all.iter().zip(&frames) {
            assert_eq!(got, data);
        }
    }

    #[test]
    fn crc_detects_directory_corruption() {
        let c = codec();
        let bytes = build(&c, Segid::new(1, 1), &sample_frames());
        let dir_offset = Segment::parse(Bytes::from(bytes.clone()))
            .unwrap()
            .dir_offset as usize;
        let mut bytes = bytes;
        bytes[dir_offset] ^= 0xFF; // a byte inside the CRC-covered directory
        assert!(
            matches!(
                Segment::parse(Bytes::from(bytes)),
                Err(SegmentError::CrcMismatch { .. })
            ),
            "tail CRC must catch directory corruption"
        );
    }

    #[test]
    fn aead_catches_frame_tamper() {
        let c = codec();
        let mut bytes = build(&c, Segid::new(1, 1), &sample_frames());
        // A frame byte is outside the tail CRC, so parse still passes...
        bytes[40] ^= 0xFF;
        let seg = Segment::parse(Bytes::from(bytes)).unwrap();
        // ...but reading that frame fails its per-frame AEAD.
        let slots: Vec<(u64, u64)> = sample_frames().iter().map(|(i, c, _)| (*i, *c)).collect();
        let err = seg
            .read_range(&c, 0, seg.dir_offset as u32, 0, &slots)
            .unwrap_err();
        assert!(matches!(err, SegmentError::Codec(CodecError::Decrypt)));
    }

    #[test]
    fn frame_cannot_be_read_under_wrong_slot() {
        let c = codec();
        let frames = vec![(10u64, 5u64, vec![1u8; 200]), (10, 6, vec![2u8; 200])];
        let seg = Segment::parse(Bytes::from(build(&c, Segid::new(1, 1), &frames))).unwrap();

        // Correct slots: ok.
        seg.read_range(&c, 0, seg.dir_offset as u32, 0, &[(10, 5), (10, 6)])
            .unwrap();
        // Wrong extent for the first frame: AEAD rejects.
        let err = seg
            .read_range(&c, 0, seg.dir_offset as u32, 0, &[(10, 6), (10, 6)])
            .unwrap_err();
        assert!(matches!(err, SegmentError::Codec(CodecError::Decrypt)));
        // Wrong first_frame index: AEAD rejects.
        let err = seg
            .read_range(&c, 0, seg.dir_offset as u32, 1, &[(10, 5), (10, 6)])
            .unwrap_err();
        assert!(matches!(err, SegmentError::Codec(CodecError::Decrypt)));
    }

    #[test]
    fn frame_cannot_be_lifted_to_another_segment() {
        let c = codec();
        let frames = vec![(10u64, 5u64, vec![9u8; 300])];
        let seg_a = Segment::parse(Bytes::from(build(&c, Segid::new(1, 1), &frames))).unwrap();
        // Reading frame 0 of seg A under seg B's identity must fail.
        let err = read_frames_from_region(
            &c,
            &seg_a.bytes[0..seg_a.dir_offset as usize],
            Segid::new(2, 1), // different segid
            0,
            &[(10, 5)],
        )
        .unwrap_err();
        assert!(matches!(err, SegmentError::Codec(CodecError::Decrypt)));
    }

    #[test]
    fn frameloc_encode_roundtrips() {
        let l = FrameLoc {
            segid: Segid::new(7, 9),
            frame_index: 3,
            byte_offset: 12345,
            byte_len: 678,
        };
        assert_eq!(FrameLoc::decode(&l.encode()), Some(l));
        assert_eq!(FrameLoc::decode(&[0u8; 10]), None);
    }

    #[test]
    fn rejects_truncated_and_bad_magic() {
        assert!(matches!(
            Segment::parse(Bytes::from(vec![0u8; 10])),
            Err(SegmentError::TooSmall(10))
        ));
        let c = codec();
        let mut bytes = build(&c, Segid::new(1, 1), &sample_frames());
        let n = bytes.len();
        bytes[n - FOOTER_LEN] ^= 0xFF; // corrupt magic
        assert!(matches!(
            Segment::parse(Bytes::from(bytes)),
            Err(SegmentError::CrcMismatch { .. }) | Err(SegmentError::BadMagic)
        ));
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::config::CompressionConfig;
    use proptest::prelude::*;

    fn frames_strategy() -> impl Strategy<Value = Vec<(u64, u64, Vec<u8>)>> {
        prop::collection::vec(
            (
                0u64..4,
                0u64..8,
                prop::collection::vec(any::<u8>(), 0..2048),
            ),
            1..12,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Build -> parse -> contiguous read returns every frame's plaintext exactly.
        #[test]
        fn build_parse_read_roundtrips(frames in frames_strategy(), epoch in 0u64..1000, counter in 0u64..1000) {
            let c = FrameCodec::new(&[2u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
            let segid = Segid::new(epoch, counter);
            let mut b = SegmentBuilder::new(&c, segid);
            for (ino, extent, data) in &frames {
                b.add_frame(*ino, *extent, data).unwrap();
            }
            let bytes = b.finish(0).unwrap();
            let seg = Segment::parse(Bytes::from(bytes)).unwrap();
            let slots: Vec<(u64, u64)> = frames.iter().map(|(i, c, _)| (*i, *c)).collect();
            let got = seg.read_range(&c, 0, seg.dir_offset as u32, 0, &slots).unwrap();
            prop_assert_eq!(got.len(), frames.len());
            for (g, (_, _, data)) in got.iter().zip(&frames) {
                prop_assert_eq!(g, data);
            }
        }

        // A single-byte flip in the CRC-covered tail (directory + footer) is caught
        // at parse; frame bytes are covered by per-frame AEAD instead.
        #[test]
        fn single_byte_tail_corruption_is_detected(
            frames in frames_strategy(),
            seed in any::<u64>(),
        ) {
            let c = FrameCodec::new(&[2u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
            let mut b = SegmentBuilder::new(&c, Segid::new(1, 1));
            for (ino, extent, data) in &frames {
                b.add_frame(*ino, *extent, data).unwrap();
            }
            let bytes = b.finish(0).unwrap();
            let n = bytes.len();
            let dir_offset = Segment::parse(Bytes::from(bytes.clone()))
                .unwrap()
                .dir_offset as usize;
            let mut bytes = bytes;
            // Corrupt a byte in [dir_offset .. n-8] (the CRC-covered tail).
            let pos = dir_offset + (seed as usize) % ((n - 8) - dir_offset);
            bytes[pos] ^= 1;
            // matches! lives outside prop_assert! so the `{ .. }` pattern is not
            // mis-parsed as a format placeholder.
            let detected = matches!(
                Segment::parse(Bytes::from(bytes)),
                Err(SegmentError::CrcMismatch { .. })
                    | Err(SegmentError::BadMagic)
                    | Err(SegmentError::BadVersion(_))
                    | Err(SegmentError::Malformed(_))
            );
            prop_assert!(detected);
        }
    }
}
