//! Shared byte-range arithmetic for 9P locks.

/// Exclusive end of a 9P lock range. A zero length extends to EOF.
pub fn lock_range_end(start: u64, length: u64) -> u64 {
    if length == 0 {
        u64::MAX
    } else {
        start.saturating_add(length)
    }
}

/// Remove one range from another, returning the surviving left and right
/// fragments as `(start, length)` pairs. A zero length extends to EOF.
pub fn subtract_lock_range(
    held_start: u64,
    held_length: u64,
    remove_start: u64,
    remove_length: u64,
) -> Vec<(u64, u64)> {
    let held_end = lock_range_end(held_start, held_length);
    let remove_end = lock_range_end(remove_start, remove_length);
    if held_start >= remove_end || remove_start >= held_end {
        return vec![(held_start, held_length)];
    }

    let mut survivors = Vec::with_capacity(2);

    if held_start < remove_start {
        let left_end = remove_start.min(held_end);
        if held_start < left_end {
            survivors.push((held_start, left_end - held_start));
        }
    }

    if remove_end < held_end {
        let right_start = remove_end.max(held_start);
        if right_start < held_end {
            let right_length = if held_end == u64::MAX {
                0
            } else {
                held_end - right_start
            };
            survivors.push((right_start, right_length));
        }
    }

    survivors
}

#[cfg(test)]
mod tests {
    use super::*;

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
