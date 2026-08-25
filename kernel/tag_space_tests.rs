#[path = "client/tag_space.rs"]
mod tag_space;

use tag_space::{TAG_COUNT, next_free_resident_index, next_resident_count, next_tag_index};

fn next_clear(occupied: &[bool], start: usize) -> Option<usize> {
    occupied
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, occupied)| (!occupied).then_some(index))
}

#[test]
fn layout_uses_every_wire_tag_except_notag() {
    assert_eq!(TAG_COUNT, 65_535);
}

#[test]
fn cursor_visits_every_resident_tag_once_before_wrapping() {
    let mut cursor = 0;
    let mut seen = vec![false; TAG_COUNT];
    for _ in 0..TAG_COUNT {
        assert!(!seen[cursor]);
        seen[cursor] = true;
        cursor = next_tag_index(cursor, TAG_COUNT);
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
        next_free_resident_index(10, 7, |start| next_clear(&occupied, start)),
        Some(9)
    );
    occupied[9] = true;
    assert_eq!(
        next_free_resident_index(10, 7, |start| next_clear(&occupied, start)),
        Some(2)
    );
    occupied[2] = true;
    assert_eq!(
        next_free_resident_index(10, 7, |start| next_clear(&occupied, start)),
        None
    );
    assert_eq!(
        next_free_resident_index(0, 0, |start| next_clear(&occupied, start)),
        None
    );
}

#[test]
fn resident_table_grows_geometrically_to_the_wire_limit() {
    let mut resident = 1_024;
    let mut previous = 0;
    while resident < TAG_COUNT {
        assert!(resident > previous);
        previous = resident;
        resident = next_resident_count(resident);
        assert!(resident <= TAG_COUNT);
    }
    assert_eq!(resident, TAG_COUNT);
    assert_eq!(next_resident_count(resident), TAG_COUNT);
}
