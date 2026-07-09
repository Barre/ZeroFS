#![no_main]

//! Fuzz the NBD types decoded from client input.

use deku::prelude::*;
use libfuzzer_sys::fuzz_target;
use nbd_proto::*;

fn assert_stable<T>(bytes: &[u8])
where
    T: DekuContainerWrite + for<'a> DekuContainerRead<'a>,
{
    if let Ok((_, decoded)) = T::from_bytes((bytes, 0)) {
        let once = decoded.to_bytes().expect("a decoded value must re-encode");
        let (_, re) = T::from_bytes((&once, 0)).expect("a canonical frame must decode");
        let twice = re.to_bytes().expect("a re-decoded value must re-encode");
        assert_eq!(once, twice, "encode/decode is not a stable canonical form");
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((sel, rest)) = data.split_first() else {
        return;
    };
    match sel % 3 {
        0 => assert_stable::<NBDRequest>(rest),
        1 => assert_stable::<NBDOptionHeader>(rest),
        _ => assert_stable::<NBDClientFlags>(rest),
    }
});
