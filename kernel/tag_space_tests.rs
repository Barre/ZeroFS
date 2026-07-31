#[path = "client/tag_space.rs"]
mod tag_space;

use tag_space::{
    FIRST_NORMAL_TAG, FLUSH_SLOTS, NORMAL_TAG_COUNT, is_flush_tag, next_free_resident_tag,
    next_normal_index, next_resident_count, normal_tag_index, normal_wire_tag,
};

fn next_clear(occupied: &[bool], start: usize) -> Option<usize> {
    occupied
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, occupied)| (!occupied).then_some(index))
}

#[test]
fn layout_uses_every_wire_tag_except_notag() {
    assert_eq!(FLUSH_SLOTS, 4);
    assert_eq!(FIRST_NORMAL_TAG, 4);
    assert_eq!(NORMAL_TAG_COUNT, 65_531);

    for tag in 0..FIRST_NORMAL_TAG {
        assert!(is_flush_tag(tag));
        assert_eq!(normal_tag_index(tag), None);
    }

    assert_eq!(normal_wire_tag(0), Some(4));
    assert_eq!(normal_wire_tag(NORMAL_TAG_COUNT - 1), Some(65_534));
    assert_eq!(normal_wire_tag(NORMAL_TAG_COUNT), None);
    assert_eq!(normal_tag_index(4), Some(0));
    assert_eq!(normal_tag_index(65_534), Some(NORMAL_TAG_COUNT - 1));
    assert_eq!(normal_tag_index(u16::MAX as usize), None);
}

#[test]
fn cursor_visits_every_resident_tag_once_before_wrapping() {
    let mut cursor = 0;
    let mut seen = vec![false; NORMAL_TAG_COUNT];
    for _ in 0..NORMAL_TAG_COUNT {
        assert!(!seen[cursor]);
        seen[cursor] = true;
        cursor = next_normal_index(cursor, NORMAL_TAG_COUNT);
    }
    assert_eq!(cursor, 0);
    assert!(seen.into_iter().all(|visited| visited));
}

#[test]
fn vacancy_search_honours_cursor_wrap_and_resident_bound() {
    let mut occupied = vec![true; 12];
    occupied[9] = false;
    occupied[2] = false;
    // A clear bit beyond the resident prefix must never be returned.
    occupied[11] = false;

    assert_eq!(
        next_free_resident_tag(10, 7, |start| next_clear(&occupied, start)),
        Some(9)
    );
    occupied[9] = true;
    assert_eq!(
        next_free_resident_tag(10, 7, |start| next_clear(&occupied, start)),
        Some(2)
    );
    occupied[2] = true;
    assert_eq!(
        next_free_resident_tag(10, 7, |start| next_clear(&occupied, start)),
        None
    );
    assert_eq!(
        next_free_resident_tag(0, 0, |start| next_clear(&occupied, start)),
        None
    );
}

#[test]
fn resident_table_grows_geometrically_to_the_wire_limit() {
    let mut resident = 1_024;
    let mut previous = 0;
    while resident < NORMAL_TAG_COUNT {
        assert!(resident > previous);
        previous = resident;
        resident = next_resident_count(resident);
        assert!(resident <= NORMAL_TAG_COUNT);
    }
    assert_eq!(resident, NORMAL_TAG_COUNT);
    assert_eq!(next_resident_count(resident), NORMAL_TAG_COUNT);
}
