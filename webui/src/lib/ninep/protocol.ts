/**
 * 9P2000.L wire protocol encoder/decoder.
 *
 * Wire format: all integers are little-endian.
 * Message: size[4] type[1] tag[2] body[...]
 * Strings: len[2] data[len]
 */

import { MsgType, type Qid, type Stat, type DirEntry } from "./types";

class Writer {
  private buf: ArrayBuffer;
  private view: DataView;
  private pos: number;

  constructor(initialSize = 256) {
    this.buf = new ArrayBuffer(initialSize);
    this.view = new DataView(this.buf);
    this.pos = 0;
  }

  private grow(needed: number) {
    if (this.pos + needed <= this.buf.byteLength) return;
    let newSize = this.buf.byteLength * 2;
    while (newSize < this.pos + needed) newSize *= 2;
    const newBuf = new ArrayBuffer(newSize);
    new Uint8Array(newBuf).set(new Uint8Array(this.buf));
    this.buf = newBuf;
    this.view = new DataView(this.buf);
  }

  u8(v: number) {
    this.grow(1);
    this.view.setUint8(this.pos, v);
    this.pos += 1;
  }
  u16(v: number) {
    this.grow(2);
    this.view.setUint16(this.pos, v, true);
    this.pos += 2;
  }
  u32(v: number) {
    this.grow(4);
    this.view.setUint32(this.pos, v, true);
    this.pos += 4;
  }
  u64(v: bigint) {
    this.grow(8);
    this.view.setBigUint64(this.pos, v, true);
    this.pos += 8;
  }
  str(s: string) {
    const encoded = new TextEncoder().encode(s);
    this.u16(encoded.length);
    this.grow(encoded.length);
    new Uint8Array(this.buf, this.pos, encoded.length).set(encoded);
    this.pos += encoded.length;
  }
  bytes(data: Uint8Array) {
    this.grow(data.length);
    new Uint8Array(this.buf, this.pos, data.length).set(data);
    this.pos += data.length;
  }

  finish(): ArrayBuffer {
    return this.buf.slice(0, this.pos);
  }
}

class Reader {
  private view: DataView;
  private pos: number;
  private data: Uint8Array;

  constructor(buf: ArrayBuffer, offset = 0) {
    this.view = new DataView(buf);
    this.data = new Uint8Array(buf);
    this.pos = offset;
  }

  u8(): number {
    const v = this.view.getUint8(this.pos);
    this.pos += 1;
    return v;
  }
  u16(): number {
    const v = this.view.getUint16(this.pos, true);
    this.pos += 2;
    return v;
  }
  u32(): number {
    const v = this.view.getUint32(this.pos, true);
    this.pos += 4;
    return v;
  }
  u64(): bigint {
    const v = this.view.getBigUint64(this.pos, true);
    this.pos += 8;
    return v;
  }
  str(): string {
    const len = this.u16();
    const bytes = this.data.slice(this.pos, this.pos + len);
    this.pos += len;
    return new TextDecoder().decode(bytes);
  }
  rawBytes(len: number): Uint8Array {
    const bytes = this.data.slice(this.pos, this.pos + len);
    this.pos += len;
    return bytes;
  }
  qid(): Qid {
    return { type: this.u8(), version: this.u32(), path: this.u64() };
  }
  stat(): Stat {
    return {
      qid: this.qid(),
      mode: this.u32(),
      uid: this.u32(),
      gid: this.u32(),
      nlink: this.u64(),
      rdev: this.u64(),
      size: this.u64(),
      blksize: this.u64(),
      blocks: this.u64(),
      atimeSec: this.u64(),
      atimeNsec: this.u64(),
      mtimeSec: this.u64(),
      mtimeNsec: this.u64(),
      ctimeSec: this.u64(),
      ctimeNsec: this.u64(),
      btimeSec: this.u64(),
      btimeNsec: this.u64(),
      gen: this.u64(),
      dataVersion: this.u64(),
    };
  }
  remaining(): number {
    return this.data.byteLength - this.pos;
  }
  offset(): number {
    return this.pos;
  }
}

function encodeMessage(type: number, tag: number, bodyFn: (w: Writer) => void): ArrayBuffer {
  const w = new Writer();
  w.u32(0); // placeholder for size
  w.u8(type);
  w.u16(tag);
  bodyFn(w);
  const buf = w.finish();
  // Patch size field
  new DataView(buf).setUint32(0, buf.byteLength, true);
  return buf;
}

export function encodeTversion(tag: number, msize: number): ArrayBuffer {
  return encodeMessage(MsgType.Tversion, tag, (w) => {
    w.u32(msize);
    w.str("9P2000.L");
  });
}

export function encodeTattach(
  tag: number,
  fid: number,
  afid: number,
  uname: string,
  aname: string,
  nUname: number,
): ArrayBuffer {
  return encodeMessage(MsgType.Tattach, tag, (w) => {
    w.u32(fid);
    w.u32(afid);
    w.str(uname);
    w.str(aname);
    w.u32(nUname);
  });
}

export function encodeTwalk(tag: number, fid: number, newfid: number, wnames: string[]): ArrayBuffer {
  return encodeMessage(MsgType.Twalk, tag, (w) => {
    w.u32(fid);
    w.u32(newfid);
    w.u16(wnames.length);
    for (const name of wnames) w.str(name);
  });
}

export function encodeTlopen(tag: number, fid: number, flags: number): ArrayBuffer {
  return encodeMessage(MsgType.Tlopen, tag, (w) => {
    w.u32(fid);
    w.u32(flags);
  });
}

export function encodeTlcreate(
  tag: number,
  fid: number,
  name: string,
  flags: number,
  mode: number,
  gid: number,
): ArrayBuffer {
  return encodeMessage(MsgType.Tlcreate, tag, (w) => {
    w.u32(fid);
    w.str(name);
    w.u32(flags);
    w.u32(mode);
    w.u32(gid);
  });
}

export function encodeTread(tag: number, fid: number, offset: bigint, count: number): ArrayBuffer {
  return encodeMessage(MsgType.Tread, tag, (w) => {
    w.u32(fid);
    w.u64(offset);
    w.u32(count);
  });
}

export function encodeTwrite(tag: number, fid: number, offset: bigint, data: Uint8Array): ArrayBuffer {
  return encodeMessage(MsgType.Twrite, tag, (w) => {
    w.u32(fid);
    w.u64(offset);
    w.u32(data.length);
    w.bytes(data);
  });
}

export function encodeTclunk(tag: number, fid: number): ArrayBuffer {
  return encodeMessage(MsgType.Tclunk, tag, (w) => {
    w.u32(fid);
  });
}

export function encodeTreaddir(tag: number, fid: number, offset: bigint, count: number): ArrayBuffer {
  return encodeMessage(MsgType.Treaddir, tag, (w) => {
    w.u32(fid);
    w.u64(offset);
    w.u32(count);
  });
}

export function encodeTgetattr(tag: number, fid: number, requestMask: bigint): ArrayBuffer {
  return encodeMessage(MsgType.Tgetattr, tag, (w) => {
    w.u32(fid);
    w.u64(requestMask);
  });
}

export function encodeTmkdir(tag: number, dfid: number, name: string, mode: number, gid: number): ArrayBuffer {
  return encodeMessage(MsgType.Tmkdir, tag, (w) => {
    w.u32(dfid);
    w.str(name);
    w.u32(mode);
    w.u32(gid);
  });
}

export function encodeTrenameat(
  tag: number,
  olddirfid: number,
  oldname: string,
  newdirfid: number,
  newname: string,
): ArrayBuffer {
  return encodeMessage(MsgType.Trenameat, tag, (w) => {
    w.u32(olddirfid);
    w.str(oldname);
    w.u32(newdirfid);
    w.str(newname);
  });
}

export function encodeTunlinkat(tag: number, dirfid: number, name: string, flags: number): ArrayBuffer {
  return encodeMessage(MsgType.Tunlinkat, tag, (w) => {
    w.u32(dirfid);
    w.str(name);
    w.u32(flags);
  });
}

export function encodeTsymlink(tag: number, dfid: number, name: string, symtgt: string, gid: number): ArrayBuffer {
  return encodeMessage(MsgType.Tsymlink, tag, (w) => {
    w.u32(dfid);
    w.str(name);
    w.str(symtgt);
    w.u32(gid);
  });
}

export function encodeTreadlink(tag: number, fid: number): ArrayBuffer {
  return encodeMessage(MsgType.Treadlink, tag, (w) => {
    w.u32(fid);
  });
}

const SETATTR_MODE = 0x00000001;
const SETATTR_UID = 0x00000002;
const SETATTR_GID = 0x00000004;
const SETATTR_SIZE = 0x00000008;

export function encodeTsetattr(
  tag: number,
  fid: number,
  opts: { mode?: number; uid?: number; gid?: number; size?: bigint },
): ArrayBuffer {
  let valid = 0;
  if (opts.mode !== undefined) valid |= SETATTR_MODE;
  if (opts.uid !== undefined) valid |= SETATTR_UID;
  if (opts.gid !== undefined) valid |= SETATTR_GID;
  if (opts.size !== undefined) valid |= SETATTR_SIZE;
  return encodeMessage(MsgType.Tsetattr, tag, (w) => {
    w.u32(fid);
    w.u32(valid);
    w.u32(opts.mode ?? 0);
    w.u32(opts.uid ?? 0);
    w.u32(opts.gid ?? 0);
    w.u64(opts.size ?? 0n);
    w.u64(0n); // atime_sec
    w.u64(0n); // atime_nsec
    w.u64(0n); // mtime_sec
    w.u64(0n); // mtime_nsec
  });
}

export interface DecodedMessage {
  size: number;
  type: number;
  tag: number;
}

export interface RversionMsg extends DecodedMessage {
  type: typeof MsgType.Rversion;
  msize: number;
  version: string;
}

export interface RattachMsg extends DecodedMessage {
  type: typeof MsgType.Rattach;
  qid: Qid;
}

export interface RwalkMsg extends DecodedMessage {
  type: typeof MsgType.Rwalk;
  wqids: Qid[];
}

export interface RlopenMsg extends DecodedMessage {
  type: typeof MsgType.Rlopen;
  qid: Qid;
  iounit: number;
}

export interface RlcreateMsg extends DecodedMessage {
  type: typeof MsgType.Rlcreate;
  qid: Qid;
  iounit: number;
}

export interface RreadMsg extends DecodedMessage {
  type: typeof MsgType.Rread;
  data: Uint8Array;
}

export interface RwriteMsg extends DecodedMessage {
  type: typeof MsgType.Rwrite;
  count: number;
}

export interface RclunkMsg extends DecodedMessage {
  type: typeof MsgType.Rclunk;
}

export interface RreaddirMsg extends DecodedMessage {
  type: typeof MsgType.Rreaddir;
  entries: DirEntry[];
}

export interface RgetattrMsg extends DecodedMessage {
  type: typeof MsgType.Rgetattr;
  valid: bigint;
  stat: Stat;
}

export interface RmkdirMsg extends DecodedMessage {
  type: typeof MsgType.Rmkdir;
  qid: Qid;
}

export interface RsymlinkMsg extends DecodedMessage {
  type: typeof MsgType.Rsymlink;
  qid: Qid;
}

export interface RreadlinkMsg extends DecodedMessage {
  type: typeof MsgType.Rreadlink;
  target: string;
}

export interface RlerrorMsg extends DecodedMessage {
  type: typeof MsgType.Rlerror;
  ecode: number;
}

// Empty response types
export interface RsetattrMsg extends DecodedMessage {
  type: typeof MsgType.Rsetattr;
}
export interface RrenameMsg extends DecodedMessage {
  type: typeof MsgType.Rrename;
}
export interface RrenameatMsg extends DecodedMessage {
  type: typeof MsgType.Rrenameat;
}
export interface RunlinkatMsg extends DecodedMessage {
  type: typeof MsgType.Runlinkat;
}
export interface RlinkMsg extends DecodedMessage {
  type: typeof MsgType.Rlink;
}

export type ResponseMessage =
  | RversionMsg
  | RattachMsg
  | RwalkMsg
  | RlopenMsg
  | RlcreateMsg
  | RreadMsg
  | RwriteMsg
  | RclunkMsg
  | RreaddirMsg
  | RgetattrMsg
  | RmkdirMsg
  | RsymlinkMsg
  | RreadlinkMsg
  | RlerrorMsg
  | RsetattrMsg
  | RrenameMsg
  | RrenameatMsg
  | RunlinkatMsg
  | RlinkMsg;

function parseDirEntries(r: Reader, dataLen: number): DirEntry[] {
  const entries: DirEntry[] = [];
  const end = r.offset() + dataLen;
  while (r.offset() < end) {
    entries.push({
      qid: r.qid(),
      offset: r.u64(),
      type: r.u8(),
      name: r.str(),
    });
  }
  return entries;
}

export function decodeResponse(buf: ArrayBuffer): ResponseMessage {
  const r = new Reader(buf);
  const size = r.u32();
  const type = r.u8();
  const tag = r.u16();
  const base = { size, type, tag };

  switch (type) {
    case MsgType.Rlerror:
      return { ...base, type, ecode: r.u32() };
    case MsgType.Rversion:
      return { ...base, type, msize: r.u32(), version: r.str() };
    case MsgType.Rattach:
      return { ...base, type, qid: r.qid() };
    case MsgType.Rwalk: {
      const nwqid = r.u16();
      const wqids: Qid[] = [];
      for (let i = 0; i < nwqid; i++) wqids.push(r.qid());
      return { ...base, type, wqids };
    }
    case MsgType.Rlopen:
      return { ...base, type, qid: r.qid(), iounit: r.u32() };
    case MsgType.Rlcreate:
      return { ...base, type, qid: r.qid(), iounit: r.u32() };
    case MsgType.Rread: {
      const count = r.u32();
      return { ...base, type, data: r.rawBytes(count) };
    }
    case MsgType.Rwrite:
      return { ...base, type, count: r.u32() };
    case MsgType.Rclunk:
      return { ...base, type };
    case MsgType.Rreaddir: {
      const count = r.u32();
      return { ...base, type, entries: parseDirEntries(r, count) };
    }
    case MsgType.Rgetattr:
      return { ...base, type, valid: r.u64(), stat: r.stat() };
    case MsgType.Rmkdir:
      return { ...base, type, qid: r.qid() };
    case MsgType.Rsymlink:
      return { ...base, type, qid: r.qid() };
    case MsgType.Rreadlink:
      return { ...base, type, target: r.str() };
    case MsgType.Rsetattr:
    case MsgType.Rrename:
    case MsgType.Rrenameat:
    case MsgType.Runlinkat:
    case MsgType.Rlink:
      return { ...base, type } as ResponseMessage;
    default:
      throw new Error(`Unknown 9P response type: ${type}`);
  }
}
