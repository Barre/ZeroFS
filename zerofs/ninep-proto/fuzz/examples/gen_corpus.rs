//! Generate seed inputs for the `decode` fuzz target.

use ninep_proto::*;
use std::fs;
use std::path::Path;

fn seed(dir: &Path, name: &str, tag: u16, body: Message, op_id: bool) {
    let frame = P9Message::new(tag, body).to_bytes_ctx(op_id).unwrap();
    let mut input = Vec::with_capacity(frame.len() + 1);
    input.push(op_id as u8);
    input.extend_from_slice(&frame);
    fs::write(dir.join(name), input).unwrap();
}

fn s(v: &str) -> P9String {
    P9String::new(v.as_bytes().to_vec())
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/decode");
    fs::create_dir_all(&dir).unwrap();
    let dir = dir.as_path();

    seed(
        dir,
        "tversion",
        0,
        Message::Tversion(Tversion {
            msize: 1 << 20,
            version: s("9P2000.L"),
        }),
        false,
    );
    seed(
        dir,
        "tattach",
        1,
        Message::Tattach(Tattach {
            fid: 0,
            afid: u32::MAX,
            uname: s("root"),
            aname: s(""),
            n_uname: 0,
        }),
        false,
    );
    seed(
        dir,
        "twalk_empty",
        2,
        Message::Twalk(Twalk {
            fid: 0,
            newfid: 1,
            nwname: 0,
            wnames: vec![],
        }),
        false,
    );
    seed(
        dir,
        "twalk_deep",
        3,
        Message::Twalk(Twalk {
            fid: 0,
            newfid: 1,
            nwname: 3,
            wnames: vec![s("a"), s("bb"), s("ccc")],
        }),
        false,
    );
    seed(
        dir,
        "tread",
        4,
        Message::Tread(Tread {
            fid: 1,
            offset: 4096,
            count: 8192,
        }),
        false,
    );
    seed(
        dir,
        "twrite",
        5,
        Message::Twrite(Twrite {
            fid: 1,
            offset: 0,
            count: 4,
            data: vec![0xde, 0xad, 0xbe, 0xef].into(),
        }),
        false,
    );
    let mkdir = || {
        Message::Tmkdir(Tmkdir {
            dfid: 0,
            name: s("newdir"),
            mode: 0o755,
            gid: 0,
        })
    };
    seed(dir, "tmkdir_plain", 6, mkdir(), false);
    seed(dir, "tmkdir_opid", 6, mkdir(), true);

    // Regression for allocation before validating the payload length.
    seed(
        dir,
        "twrite_count_bomb",
        7,
        Message::Twrite(Twrite {
            fid: 1,
            offset: 0,
            count: u32::MAX,
            data: vec![0xde, 0xad, 0xbe, 0xef].into(),
        }),
        false,
    );

    println!("wrote seeds to {}", dir.display());
}
