//! Pure wire-tag layout and cursor arithmetic.

/// Every usable `u16` tag. `NOTAG` remains reserved by 9P.
pub(crate) const TAG_COUNT: usize = u16::MAX as usize;

pub(crate) const fn next_tag_index(index: usize, resident: usize) -> usize {
    if index + 1 < resident { index + 1 } else { 0 }
}

/// Find one vacant index inside the resident prefix, starting at `cursor` and
/// wrapping exactly once.
///
/// `next_zero` is the backing bitmap's "find next clear bit" operation. Keeping
/// the range and wrap policy here makes the production allocator independently
/// testable without emulating the kernel bitmap type.
pub(crate) fn next_free_resident_index(
    resident: usize,
    cursor: usize,
    mut next_zero: impl FnMut(usize) -> Option<usize>,
) -> Option<usize> {
    let resident = resident.min(TAG_COUNT);
    if resident == 0 {
        return None;
    }
    let start = cursor.min(resident - 1);
    if let Some(index) = next_zero(start) {
        if index < resident {
            return Some(index);
        }
    }
    if start != 0 {
        if let Some(index) = next_zero(0) {
            if index < start {
                return Some(index);
            }
        }
    }
    None
}

/// Geometric high-water growth, capped by the complete wire namespace.
pub(crate) fn next_resident_count(current: usize) -> usize {
    if current >= TAG_COUNT {
        return TAG_COUNT;
    }
    current
        .checked_mul(2)
        .unwrap_or(TAG_COUNT)
        .max(current.saturating_add(1))
        .min(TAG_COUNT)
}
