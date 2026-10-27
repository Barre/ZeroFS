#![no_main]

//! Exercise the canonical borrowed decoder, owned adapters, and directory views.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use ninep_proto::{P9_MAX_MSIZE, P9Message, slice_codec};

fuzz_target!(|data: &[u8]| {
    let Some((flag, input)) = data.split_first() else {
        return;
    };
    let dialect = flag & 1 != 0;
    let Ok(frame) = slice_codec::decode_frame(input, P9_MAX_MSIZE, dialect) else {
        return;
    };
    let owned = P9Message::from_owned_bytes_ctx(Bytes::copy_from_slice(input), dialect)
        .expect("valid frames must produce owned messages");

    let mut encoded = vec![0; input.len()];
    let length = slice_codec::encode_frame(
        &mut encoded,
        P9_MAX_MSIZE,
        frame.header.tag,
        frame.envelope,
        &frame.body,
        dialect,
    )
    .expect("decoded frames must encode");
    assert_eq!(length, input.len());
    assert_eq!(encoded, input);
    assert_eq!(owned.to_bytes_ctx(dialect).unwrap(), input);
    if !encoded.is_empty() {
        let short = encoded.len() - 1;
        assert!(
            slice_codec::encode_frame(
                &mut encoded[..short],
                P9_MAX_MSIZE,
                frame.header.tag,
                frame.envelope,
                &frame.body,
                dialect,
            )
            .is_err()
        );
    }
    if let slice_codec::Message::Rreaddir { data, .. }
    | slice_codec::Message::Rreaddirattr { data, .. } = frame.body
    {
        let plus = matches!(frame.body, slice_codec::Message::Rreaddirattr { .. });
        let mut remaining = data;
        while !remaining.is_empty() {
            let Ok((entry, consumed)) = slice_codec::decode_entry(remaining, plus) else {
                break;
            };
            assert!(consumed > 0 && consumed <= remaining.len());
            let mut encoded = vec![0; consumed];
            assert_eq!(
                slice_codec::encode_entry(&mut encoded, entry).unwrap(),
                consumed
            );
            assert_eq!(encoded, &remaining[..consumed]);
            remaining = &remaining[consumed..];
        }
    }
});
