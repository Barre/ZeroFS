#![no_main]

//! Fuzz decoding and canonical re-encoding for both 9P frame layouts.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use ninep_proto::{Message, P9Message};

fn counted_payload_count(message: &P9Message) -> Option<u32> {
    match &message.body {
        Message::Rread(message) => Some(message.count),
        Message::Rreaddir(message) => Some(message.count),
        Message::Rlopenatread(message) => Some(message.count),
        Message::Twrite(message) => Some(message.count),
        Message::Rreaddirattr(message) => Some(message.count),
        _ => None,
    }
}

fuzz_target!(|data: &[u8]| {
    let (op_id_enabled, frame) = match data.split_first() {
        Some((flag, rest)) => (flag & 1 == 1, rest),
        None => return,
    };

    let borrowed = P9Message::from_bytes_ctx(frame, op_id_enabled);
    let owned = P9Message::from_owned_bytes_ctx(Bytes::copy_from_slice(frame), op_id_enabled);
    assert_eq!(
        owned.is_ok(),
        borrowed.is_ok(),
        "owned and borrowed decoding disagree on frame validity"
    );
    let (Ok(decoded), Ok(owned)) = (borrowed, owned) else {
        return;
    };

    let once = decoded
        .to_bytes_ctx(op_id_enabled)
        .expect("a decoded message must re-encode");
    assert_eq!(owned.size, decoded.size, "decoded size fields disagree");
    assert_eq!(
        counted_payload_count(&owned),
        counted_payload_count(&decoded),
        "decoded count fields disagree"
    );
    assert_eq!(
        owned
            .to_bytes_ctx(op_id_enabled)
            .expect("an owned-decoded message must re-encode"),
        once,
        "owned and borrowed decoding disagree"
    );

    let redecoded = P9Message::from_bytes_ctx(&once, op_id_enabled)
        .expect("a freshly encoded frame must decode");
    let twice = redecoded
        .to_bytes_ctx(op_id_enabled)
        .expect("a re-decoded message must re-encode");

    assert_eq!(once, twice, "encode/decode is not a stable canonical form");
});
