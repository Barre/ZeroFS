//! Pure wire-tag layout and cursor arithmetic.

/// Cancellation tags that remain available even when every ordinary tag is
/// occupied.
pub(crate) const FLUSH_SLOTS: usize = 4;

/// First wire tag available to ordinary requests.
pub(crate) const FIRST_NORMAL_TAG: usize = FLUSH_SLOTS;

/// Every usable `u16` tag other than the cancellation band.
pub(crate) const NORMAL_TAG_COUNT: usize = u16::MAX as usize - FLUSH_SLOTS;

/// Convert an ordinary wire tag to its allocation-bitmap index.
pub(crate) const fn normal_tag_index(tag: usize) -> Option<usize> {
    if tag >= FIRST_NORMAL_TAG && tag < u16::MAX as usize {
        Some(tag - FIRST_NORMAL_TAG)
    } else {
        None
    }
}

/// Convert an allocation-bitmap index to its ordinary wire tag.
pub(crate) const fn normal_wire_tag(index: usize) -> Option<usize> {
    if index < NORMAL_TAG_COUNT {
        Some(index + FIRST_NORMAL_TAG)
    } else {
        None
    }
}

pub(crate) const fn is_flush_tag(tag: usize) -> bool {
    tag < FIRST_NORMAL_TAG
}

pub(crate) const fn next_normal_index(index: usize, resident: usize) -> usize {
    if index + 1 < resident { index + 1 } else { 0 }
}

/// Find one vacant index inside the resident prefix, starting at `cursor` and
/// wrapping exactly once.
///
/// `next_zero` is the backing bitmap's "find next clear bit" operation. Keeping
/// the range and wrap policy here makes the production allocator independently
/// testable without emulating the kernel bitmap type.
pub(crate) fn next_free_resident_tag(
    resident: usize,
    cursor: usize,
    mut next_zero: impl FnMut(usize) -> Option<usize>,
) -> Option<usize> {
    let resident = resident.min(NORMAL_TAG_COUNT);
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

/// Geometric high-water growth, capped by the complete ordinary namespace.
pub(crate) fn next_resident_count(current: usize) -> usize {
    if current >= NORMAL_TAG_COUNT {
        return NORMAL_TAG_COUNT;
    }
    current
        .checked_mul(2)
        .unwrap_or(NORMAL_TAG_COUNT)
        .max(current.saturating_add(1))
        .min(NORMAL_TAG_COUNT)
}
