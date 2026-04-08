// 9P2000.L message type IDs
export const MsgType = {
  Rlerror: 7,
  Tstatfs: 8,
  Rstatfs: 9,
  Tlopen: 12,
  Rlopen: 13,
  Tlcreate: 14,
  Rlcreate: 15,
  Tsymlink: 16,
  Rsymlink: 17,
  Trename: 20,
  Rrename: 21,
  Treadlink: 22,
  Rreadlink: 23,
  Tgetattr: 24,
  Rgetattr: 25,
  Tsetattr: 26,
  Rsetattr: 27,
  Treaddir: 40,
  Rreaddir: 41,
  Tlock: 52,
  Rlock: 53,
  Tgetlock: 54,
  Rgetlock: 55,
  Tlink: 70,
  Rlink: 71,
  Tmkdir: 72,
  Rmkdir: 73,
  Trenameat: 74,
  Rrenameat: 75,
  Tunlinkat: 76,
  Runlinkat: 77,
  Tversion: 100,
  Rversion: 101,
  Tattach: 104,
  Rattach: 105,
  Tflush: 108,
  Rflush: 109,
  Twalk: 110,
  Rwalk: 111,
  Tread: 116,
  Rread: 117,
  Twrite: 118,
  Rwrite: 119,
  Tclunk: 120,
  Rclunk: 121,
} as const;

export const NOTAG = 0xffff;
export const NOFID = 0xffffffff;
export const QID_TYPE_DIR = 0x80;
export const QID_TYPE_SYMLINK = 0x02;
export const QID_TYPE_FILE = 0x00;
export const GETATTR_ALL = 0x00003fffn;
export const O_RDONLY = 0;
export const O_WRONLY = 1;
export const O_RDWR = 2;
export const AT_REMOVEDIR = 0x200;

export interface Qid {
  type: number;
  version: number;
  path: bigint;
}

export interface Stat {
  qid: Qid;
  mode: number;
  uid: number;
  gid: number;
  nlink: bigint;
  rdev: bigint;
  size: bigint;
  blksize: bigint;
  blocks: bigint;
  atimeSec: bigint;
  atimeNsec: bigint;
  mtimeSec: bigint;
  mtimeNsec: bigint;
  ctimeSec: bigint;
  ctimeNsec: bigint;
  btimeSec: bigint;
  btimeNsec: bigint;
  gen: bigint;
  dataVersion: bigint;
}

export interface DirEntry {
  qid: Qid;
  offset: bigint;
  type: number;
  name: string;
}

export interface P9Error {
  ecode: number;
}
