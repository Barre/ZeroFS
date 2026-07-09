#![no_main]

//! Fuzz decoding and canonical re-encoding for both 9P frame layouts.

use libfuzzer_sys::fuzz_target;
use ninep_proto::P9Message;

fuzz_target!(|data: &[u8]| {
    let (op_id_enabled, frame) = match data.split_first() {
        Some((flag, rest)) => (flag & 1 == 1, rest),
        None => return,
    };

    let Ok(decoded) = P9Message::from_bytes_ctx(frame, op_id_enabled) else {
        return;
    };

    let once = decoded
        .to_bytes_ctx(op_id_enabled)
        .expect("a decoded message must re-encode");

    let redecoded = P9Message::from_bytes_ctx(&once, op_id_enabled)
        .expect("a freshly encoded frame must decode");
    let twice = redecoded
        .to_bytes_ctx(op_id_enabled)
        .expect("a re-decoded message must re-encode");

    assert_eq!(once, twice, "encode/decode is not a stable canonical form");
});
