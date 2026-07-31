//! Allocating view of the shared byte-range arithmetic for 9P locks.
//!
//! The arithmetic lives in the allocation-free codec entry point included by
//! the kernel client. This module gives the server the surviving fragments as a
//! `Vec`.

use alloc::vec::Vec;

pub use crate::slice_codec::lock_range_end;

/// Remove one range from another, returning the surviving left and right
/// fragments as `(start, length)` pairs. A zero length extends to EOF.
pub fn subtract_lock_range(
    held_start: u64,
    held_length: u64,
    remove_start: u64,
    remove_length: u64,
) -> Vec<(u64, u64)> {
    crate::slice_codec::subtract_lock_range(held_start, held_length, remove_start, remove_length)
        .into_iter()
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn range_end_handles_eof_and_overflow() {
        assert_eq!(lock_range_end(10, 0), u64::MAX);
        assert_eq!(lock_range_end(10, 5), 15);
        assert_eq!(lock_range_end(u64::MAX, 10), u64::MAX);
    }

    #[test]
    fn subtraction_preserves_exact_surviving_fragments() {
        assert_eq!(
            subtract_lock_range(10, 90, 30, 20),
            vec![(10, 20), (50, 50)]
        );
        assert_eq!(subtract_lock_range(10, 0, 30, 20), vec![(10, 20), (50, 0)]);
        assert_eq!(subtract_lock_range(10, 0, 30, 0), vec![(10, 20)]);
        assert_eq!(subtract_lock_range(10, 20, 40, 10), vec![(10, 20)]);
        assert!(subtract_lock_range(10, 20, 0, 40).is_empty());
    }
}
