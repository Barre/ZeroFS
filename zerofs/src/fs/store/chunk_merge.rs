//! Sub-chunk delta encoding for file chunks, resolved by a SlateDB merge operator.
//!
//! A partial write to a 32 KiB chunk used to read-modify-write the whole chunk
//! and `put` it back, so every small write created a full-chunk version.
//! Repeated writes to a hot chunk left many superseded 32 KiB versions that
//! compaction must later read just to drop.
//!
//! Instead, a partial write to an *existing* chunk records only the changed byte
//! range as a **merge operand** (a "delta"). A new chunk / full overwrite still
//! `put`s the whole chunk, which is the merge **base**. The merge operator
//! reconstructs the chunk by applying deltas (oldest->newest) onto the base, and
//! SlateDB resolves it on reads, flushes, and compaction.
//!
//! ## Encoding
//! A delta operand is a sparse patch, non-overlapping byte ranges:
//!   `[ MAGIC u8 ][ count u16 BE ][ (offset u16 BE, len u16 BE, bytes[len]) * count ]`
//! Ranges are stored sorted by offset and non-overlapping.

use bytes::{BufMut, Bytes, BytesMut};
use slatedb::{MergeOperator, MergeOperatorError};

use crate::fs::CHUNK_SIZE;

/// Operand format version/magic. Bumping this rejects any future format change.
const DELTA_MAGIC: u8 = 0xD1;

// In-chunk offsets and lengths are encoded as u16, so the encoding is only valid
// while a chunk fits in u16::MAX bytes; fail the build loudly if CHUNK_SIZE ever
// outgrows it rather than silently truncating offsets/lengths at runtime. The u16
// segment count is safe on its own: canonical segments are non-overlapping and
// non-abutting, so each occupies >= 2 bytes of the u16 offset space and there are
// at most u16::MAX/2 of them (at most CHUNK_SIZE/2 for offsets the write path
// produces; a corrupt operand with larger offsets stays bounded by the u16 space).
const _: () = assert!(CHUNK_SIZE <= u16::MAX as usize);

/// One contiguous written byte range within a chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Segment {
    offset: usize,
    data: Bytes,
}

impl Segment {
    #[inline]
    fn end(&self) -> usize {
        self.offset + self.data.len()
    }
}

fn encoding_error() -> MergeOperatorError {
    MergeOperatorError::Callback {
        message: "invalid chunk delta encoding".to_string(),
    }
}

/// Encode canonical (sorted, non-overlapping) segments as a delta operand.
fn encode_segments(segs: &[Segment]) -> Bytes {
    let body: usize = segs.iter().map(|s| 4 + s.data.len()).sum();
    let mut buf = BytesMut::with_capacity(1 + 2 + body);
    buf.put_u8(DELTA_MAGIC);
    buf.put_u16(segs.len() as u16);
    for s in segs {
        debug_assert!(s.offset <= u16::MAX as usize && s.data.len() <= u16::MAX as usize);
        buf.put_u16(s.offset as u16);
        buf.put_u16(s.data.len() as u16);
        buf.extend_from_slice(&s.data);
    }
    buf.freeze()
}

/// Parse a delta operand back into segments.
fn decode_segments(bytes: &[u8]) -> Result<Vec<Segment>, MergeOperatorError> {
    let mut b = bytes;
    if b.first() != Some(&DELTA_MAGIC) {
        return Err(encoding_error());
    }
    b = &b[1..];
    if b.len() < 2 {
        return Err(encoding_error());
    }
    let count = u16::from_be_bytes([b[0], b[1]]) as usize;
    b = &b[2..];
    let mut segs = Vec::with_capacity(count);
    for _ in 0..count {
        if b.len() < 4 {
            return Err(encoding_error());
        }
        let offset = u16::from_be_bytes([b[0], b[1]]) as usize;
        let len = u16::from_be_bytes([b[2], b[3]]) as usize;
        b = &b[4..];
        if b.len() < len {
            return Err(encoding_error());
        }
        segs.push(Segment {
            offset,
            data: Bytes::copy_from_slice(&b[..len]),
        });
        b = &b[len..];
    }
    if !b.is_empty() {
        return Err(encoding_error());
    }
    Ok(segs)
}

/// Overlay one newer segment onto the accumulator, overriding any overlap.
fn overlay_one(acc: &mut Vec<Segment>, n: Segment) {
    let (ns, ne) = (n.offset, n.end());
    let mut result = Vec::with_capacity(acc.len() + 1);
    for s in acc.drain(..) {
        let (ss, se) = (s.offset, s.end());
        if se <= ns || ss >= ne {
            result.push(s); // disjoint, keep as-is
            continue;
        }
        // Keep the portions of the older segment not covered by the newer one.
        if ss < ns {
            result.push(Segment {
                offset: ss,
                data: s.data.slice(0..ns - ss),
            });
        }
        if se > ne {
            result.push(Segment {
                offset: ne,
                data: s.data.slice(ne - ss..),
            });
        }
        // The overlapping middle is dropped; `n` supersedes it.
    }
    result.push(n);
    result.sort_by_key(|s| s.offset);
    *acc = result;
}

/// Merge abutting segments so the canonical form has no splittable adjacency.
fn canonicalize(segs: &mut Vec<Segment>) {
    if segs.len() < 2 {
        return;
    }
    segs.sort_by_key(|s| s.offset);
    let mut merged: Vec<Segment> = Vec::with_capacity(segs.len());
    for s in segs.drain(..) {
        match merged.last_mut() {
            Some(last) if last.end() == s.offset => {
                let mut buf = BytesMut::with_capacity(last.data.len() + s.data.len());
                buf.extend_from_slice(&last.data);
                buf.extend_from_slice(&s.data);
                last.data = buf.freeze();
            }
            _ => merged.push(s),
        }
    }
    *segs = merged;
}

/// Combine operands (oldest->newest) into canonical segments; newer wins overlaps.
fn combine(operands: &[Bytes]) -> Result<Vec<Segment>, MergeOperatorError> {
    let mut acc: Vec<Segment> = Vec::new();
    for op in operands {
        for seg in decode_segments(op)? {
            overlay_one(&mut acc, seg);
        }
    }
    canonicalize(&mut acc);
    Ok(acc)
}

/// Apply segments onto a base chunk, zero-filling any gap a write past the
/// base's end opens up.
fn materialize(base: &[u8], segs: &[Segment]) -> Bytes {
    let max_end = segs.iter().map(Segment::end).max().unwrap_or(0);
    let len = base.len().max(max_end);
    let mut buf = BytesMut::zeroed(len);
    buf[..base.len()].copy_from_slice(base);
    for s in segs {
        buf[s.offset..s.end()].copy_from_slice(&s.data);
    }
    buf.freeze()
}

/// Encode a single-range delta for a partial write of `data` at in-chunk
/// `offset`. Used by the chunk-store write path for partial writes to a chunk
/// that already has a base.
pub fn encode_delta(offset: usize, data: Bytes) -> Bytes {
    encode_segments(&[Segment { offset, data }])
}

/// The shared chunk merge operator handle to attach to every `Db`, `DbReader`,
/// and `Compactor` builder (`.with_merge_operator(chunk_merge_operator())`).
/// Configuring it is inert until a delta operand is actually written: a key with
/// only `Put`/`Delete` entries never invokes the operator.
pub fn chunk_merge_operator() -> std::sync::Arc<dyn MergeOperator + Send + Sync> {
    std::sync::Arc::new(ChunkMergeOperator)
}

/// Merge operator that reconstructs a chunk from a base value plus sub-chunk
/// delta operands. See the module docs for the encoding and invariants.
#[derive(Debug, Clone, Default)]
pub struct ChunkMergeOperator;

impl MergeOperator for ChunkMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.merge_batch(key, existing_value, std::slice::from_ref(&value))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let segs = combine(operands)?;
        Ok(match existing_value {
            // A base is present: produce the materialized full chunk (a Value).
            Some(base) => materialize(&base, &segs),
            // No base yet: stay sparse so we never clobber the eventual base.
            None => encode_segments(&segs),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Reference model: apply writes (oldest->newest) onto an optional base,
    /// extending with zeros as needed.
    fn apply_writes(base: Option<&[u8]>, writes: &[(usize, Vec<u8>)]) -> Vec<u8> {
        let mut v: Vec<u8> = base.map(<[u8]>::to_vec).unwrap_or_default();
        for (off, data) in writes {
            let end = off + data.len();
            if v.len() < end {
                v.resize(end, 0);
            }
            v[*off..end].copy_from_slice(data);
        }
        v
    }

    #[test]
    fn encode_decode_roundtrip() {
        let segs = vec![
            Segment {
                offset: 3,
                data: Bytes::from_static(b"abc"),
            },
            Segment {
                offset: 10,
                data: Bytes::from_static(b"XY"),
            },
        ];
        let encoded = encode_segments(&segs);
        assert_eq!(decode_segments(&encoded).unwrap(), segs);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode_segments(b"").is_err());
        assert!(decode_segments(b"\x00\x00\x00").is_err());
        assert!(decode_segments(&[DELTA_MAGIC, 0x00, 0x01]).is_err()); // count=1, truncated
    }

    #[test]
    fn newer_operand_wins_overlap() {
        let op = ChunkMergeOperator;
        let d1 = encode_delta(0, Bytes::from_static(b"aaaa"));
        let d2 = encode_delta(2, Bytes::from_static(b"BB")); // overlaps bytes 2..4
        let base = Bytes::from_static(&[0u8; 8]);
        let out = op
            .merge_batch(&Bytes::new(), Some(base), &[d1, d2])
            .unwrap();
        assert_eq!(&out[..], b"aaBB\0\0\0\0");
    }

    #[test]
    fn delta_extends_base_with_zero_fill() {
        let op = ChunkMergeOperator;
        let base = Bytes::from_static(b"hi"); // len 2
        let d = encode_delta(4, Bytes::from_static(b"Z")); // gap at [2,4)
        let out = op.merge_batch(&Bytes::new(), Some(base), &[d]).unwrap();
        assert_eq!(&out[..], b"hi\0\0Z");
    }

    #[test]
    fn no_base_stays_sparse_then_materializes() {
        let op = ChunkMergeOperator;
        let d1 = encode_delta(1, Bytes::from_static(b"xx"));
        let d2 = encode_delta(5, Bytes::from_static(b"y"));
        // No base --> a combined sparse operand (still decodable as a delta).
        let sparse = op
            .merge_batch(&Bytes::new(), None, &[d1.clone(), d2.clone()])
            .unwrap();
        assert!(decode_segments(&sparse).is_ok());
        // Applying that combined operand onto a base equals applying both deltas.
        let base = Bytes::from_static(&[b'.'; 8]);
        let out = op
            .merge_batch(&Bytes::new(), Some(base.clone()), &[sparse])
            .unwrap();
        let direct = op
            .merge_batch(&Bytes::new(), Some(base), &[d1, d2])
            .unwrap();
        assert_eq!(out, direct);
        assert_eq!(&out[..], b".xx..y..");
    }

    proptest! {
        // Resolving the operands matches the reference model, AND is invariant to
        // how SlateDB batches them: split the operands into arbitrary groups, fold
        // each group with no base into one sparse intermediate, then resolve the
        // intermediates against the base.
        #[test]
        fn batched_resolution_matches_reference(
            base in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..200usize)),
            writes in proptest::collection::vec(
                (0..200usize, proptest::collection::vec(any::<u8>(), 1..40usize)),
                0..40usize,
            ),
            splits in proptest::collection::vec(1usize..6, 1..40usize),
        ) {
            let op = ChunkMergeOperator;
            let base_bytes = base.as_ref().map(|b| Bytes::copy_from_slice(b));

            // Encode each write (oldest→newest) as a delta operand.
            let operands: Vec<Bytes> = writes
                .iter()
                .map(|(off, data)| encode_delta(*off, Bytes::copy_from_slice(data)))
                .collect();

            // Fold arbitrary-sized groups with no base into sparse intermediates,
            // mirroring MergeOperatorIterator::process_batch(None, ..).
            let mut intermediates = Vec::new();
            let mut idx = 0usize;
            let mut split_iter = splits.iter().cycle();
            while idx < operands.len() {
                let take = (*split_iter.next().unwrap()).min(operands.len() - idx);
                let group = &operands[idx..idx + take];
                intermediates.push(op.merge_batch(&Bytes::new(), None, group).unwrap());
                idx += take;
            }

            let got = op
                .merge_batch(&Bytes::new(), base_bytes.clone(), &intermediates)
                .unwrap();

            let expected = apply_writes(base.as_deref(), &writes);

            if base_bytes.is_some() {
                // With a base, the result is the materialized chunk.
                prop_assert_eq!(&got[..], &expected[..]);
            } else {
                // With no base, the result is sparse; materialize it against an
                // explicit zero base to compare against the reference.
                let zero = Bytes::copy_from_slice(&vec![0u8; expected.len()]);
                let mat = op.merge_batch(&Bytes::new(), Some(zero), &[got]).unwrap();
                prop_assert_eq!(&mat[..], &expected[..]);
            }
        }
    }
}
