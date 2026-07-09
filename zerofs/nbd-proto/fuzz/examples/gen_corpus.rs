//! Generate seed inputs for the `decode` fuzz target.

use deku::prelude::*;
use nbd_proto::*;
use std::fs;
use std::path::Path;

fn seed(dir: &Path, name: &str, sel: u8, frame: Vec<u8>) {
    let mut input = Vec::with_capacity(frame.len() + 1);
    input.push(sel);
    input.extend_from_slice(&frame);
    fs::write(dir.join(name), input).unwrap();
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/decode");
    fs::create_dir_all(&dir).unwrap();
    let dir = dir.as_path();

    seed(
        dir,
        "request_write",
        0,
        NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: NBD_CMD_FLAG_FUA,
            cmd_type: NBDCommand::Write,
            cookie: 0x0102_0304_0506_0708,
            offset: 4096,
            length: 512,
        }
        .to_bytes()
        .unwrap(),
    );
    seed(
        dir,
        "option_go",
        1,
        NBDOptionHeader {
            magic: NBD_IHAVEOPT,
            option: NBD_OPT_GO,
            length: 32,
        }
        .to_bytes()
        .unwrap(),
    );
    seed(
        dir,
        "client_flags",
        2,
        NBDClientFlags {
            flags: NBD_FLAG_C_FIXED_NEWSTYLE,
        }
        .to_bytes()
        .unwrap(),
    );

    println!("wrote seeds to {}", dir.display());
}
