/**
 * 9P2000.L WebSocket client.
 *
 * Manages tag allocation, request/response matching, fid allocation,
 * and provides a high-level filesystem API.
 */

import {
  encodeTversion,
  encodeTattach,
  encodeTwalk,
  encodeTlopen,
  encodeTlcreate,
  encodeTread,
  encodeTwrite,
  encodeTclunk,
  encodeTreaddir,
  encodeTgetattr,
  encodeTmkdir,
  encodeTrenameat,
  encodeTunlinkat,
  encodeTsymlink,
  encodeTreadlink,
  encodeTsetattr,
  decodeResponse,
  type ResponseMessage,
  type RlerrorMsg,
} from "./protocol";
import {
  MsgType,
  NOTAG,
  NOFID,
  GETATTR_ALL,
  O_RDONLY,
  AT_REMOVEDIR,
  QID_TYPE_DIR,
  type Stat,
  type DirEntry,
  type Qid,
} from "./types";

const DEFAULT_MSIZE = 1024 * 1024; // 1 MB

import { pooled } from "../async";
import { joinPath as join } from "../format";

const ROOT_FID = 0;
const HEADER_OVERHEAD = 11; // size[4] + type[1] + tag[2] + count[4]

export class P9Error extends Error {
  constructor(public ecode: number) {
    super(`9P error: ${ecode} (${errnoName(ecode)})`);
    this.name = "P9Error";
  }
}

/**
 * Convert a search query to a RegExp.
 * - `/pattern/` → explicit regex
 * - Contains `*` or `?` → glob (*, ?, [abc])
 * - Otherwise → case-insensitive substring match
 */
function queryToRegex(query: string): RegExp {
  // Explicit regex: /pattern/flags
  const reMatch = /^\/(.+)\/([gimsuy]*)$/.exec(query);
  if (reMatch) {
    try {
      return new RegExp(reMatch[1], reMatch[2] || "i");
    } catch {
      // Fall through to literal on invalid regex
    }
  }

  // Glob pattern: contains * or ?
  if (/[*?]/.test(query)) {
    // Convert glob to regex: * → .*, ? → ., escape rest
    const pattern = query
      .split("")
      .map((c) => {
        if (c === "*") return ".*";
        if (c === "?") return ".";
        return c.replace(/[.+^${}()|[\]\\]/g, "\\$&");
      })
      .join("");
    return new RegExp(pattern, "i");
  }

  // Plain substring (escape for regex safety)
  const escaped = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(escaped, "i");
}

function errnoName(code: number): string {
  const names: Record<number, string> = {
    1: "EPERM",
    2: "ENOENT",
    13: "EACCES",
    17: "EEXIST",
    20: "ENOTDIR",
    21: "EISDIR",
    22: "EINVAL",
    28: "ENOSPC",
    39: "ENOTEMPTY",
  };
  return names[code] ?? "UNKNOWN";
}

interface PendingRequest {
  resolve: (msg: ResponseMessage) => void;
  reject: (err: Error) => void;
}

export interface FileEntry {
  name: string;
  qid: Qid;
  type: number;
  isDir: boolean;
  isSymlink: boolean;
  resolvedIsDir: boolean; // follows symlinks
  size: bigint;
  mtimeSec: bigint;
  mode: number;
  uid: number;
  gid: number;
  nlink: bigint;
}

export interface FileInfo {
  stat: Stat;
  name: string;
  path: string;
  isDir: boolean;
  isSymlink: boolean;
}

export type ConnectionState = "disconnected" | "connecting" | "connected";

export interface TrafficStats {
  bytesSent: number;
  bytesReceived: number;
  ops: number;
}

export class NinePClient {
  private ws: WebSocket | null = null;
  private pending = new Map<number, PendingRequest>();
  private nextTag = 0;
  private nextFid = 1; // 0 is ROOT_FID
  private msize = DEFAULT_MSIZE;
  private stateListeners = new Set<(state: ConnectionState) => void>();
  private _state: ConnectionState = "disconnected";
  private reconnectUrl: string | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connecting = false;

  private _stats: TrafficStats = { bytesSent: 0, bytesReceived: 0, ops: 0 };
  private _prevStats: TrafficStats = { bytesSent: 0, bytesReceived: 0, ops: 0 };
  private statsListeners = new Set<(stats: TrafficStats) => void>();
  private statsInterval: ReturnType<typeof setInterval> | null = null;

  onStats(listener: (stats: TrafficStats) => void): () => void {
    this.statsListeners.add(listener);
    if (!this.statsInterval) {
      this.statsInterval = setInterval(() => {
        const snapshot = { ...this._stats };
        const delta: TrafficStats = {
          bytesSent: snapshot.bytesSent - this._prevStats.bytesSent,
          bytesReceived: snapshot.bytesReceived - this._prevStats.bytesReceived,
          ops: snapshot.ops - this._prevStats.ops,
        };
        this._prevStats = snapshot;
        for (const l of this.statsListeners) l(delta);
      }, 1000);
    }
    return () => {
      this.statsListeners.delete(listener);
      if (this.statsListeners.size === 0 && this.statsInterval) {
        clearInterval(this.statsInterval);
        this.statsInterval = null;
      }
    };
  }

  get state(): ConnectionState {
    return this._state;
  }

  private setState(state: ConnectionState) {
    this._state = state;
    for (const listener of this.stateListeners) listener(state);
  }

  onStateChange(listener: (state: ConnectionState) => void): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  enableAutoReconnect(url: string) {
    this.reconnectUrl = url;
    if (this._state === "disconnected" && !this.connecting) {
      this.connect(url).catch(() => {});
    }
  }

  private scheduleReconnect() {
    if (!this.reconnectUrl || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.reconnectUrl && !this.connecting) {
        this.connect(this.reconnectUrl).catch(() => {});
      }
    }, 500);
  }

  async connect(url: string): Promise<void> {
    if (this.connecting) return;
    this.connecting = true;
    this.setState("connecting");
    this.nextTag = 0;
    this.nextFid = 1;
    this.pending.clear();

    return new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.binaryType = "arraybuffer";

      ws.onopen = async () => {
        this.ws = ws;
        try {
          await this.handshake();
          this.connecting = false;
          this.setState("connected");
          resolve();
        } catch (e) {
          ws.close();
          reject(e);
        }
      };

      ws.onmessage = (event) => {
        if (event.data instanceof ArrayBuffer) {
          this.handleMessage(event.data);
        }
      };

      ws.onerror = () => {
        reject(new Error("WebSocket connection failed"));
      };

      ws.onclose = () => {
        this.ws = null;
        this.connecting = false;
        this.setState("disconnected");
        for (const [, req] of this.pending) {
          req.reject(new Error("Connection closed"));
        }
        this.pending.clear();
        this.scheduleReconnect();
      };
    });
  }

  disconnect() {
    this.reconnectUrl = null;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.close();
  }

  private async handshake(): Promise<void> {
    const versionResp = await this.rawSend(encodeTversion(NOTAG, DEFAULT_MSIZE), NOTAG);
    if (versionResp.type === MsgType.Rversion) {
      this.msize = versionResp.msize;
    }

    const attachResp = await this.rawSend(encodeTattach(this.allocTag(), ROOT_FID, NOFID, "root", "", 0), undefined);
    if (attachResp.type === MsgType.Rlerror) {
      throw new P9Error((attachResp as RlerrorMsg).ecode);
    }
  }

  private allocTag(): number {
    let tag = this.nextTag;
    while (this.pending.has(tag)) tag = tag + 1 < 0xffff ? tag + 1 : 0;
    this.nextTag = tag + 1 < 0xffff ? tag + 1 : 0;
    return tag;
  }

  private allocFid(): number {
    return this.nextFid++;
  }

  private rawSend(buf: ArrayBuffer, overrideTag?: number): Promise<ResponseMessage> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error("Not connected"));
    }

    const tag = overrideTag ?? new DataView(buf).getUint16(5, true);

    return new Promise((resolve, reject) => {
      this.pending.set(tag, { resolve, reject });
      this._stats.bytesSent += buf.byteLength;
      this._stats.ops++;
      this.ws!.send(buf);
    });
  }

  private handleMessage(buf: ArrayBuffer) {
    this._stats.bytesReceived += buf.byteLength;
    const msg = decodeResponse(buf);
    const req = this.pending.get(msg.tag);
    if (req) {
      this.pending.delete(msg.tag);
      req.resolve(msg);
    }
  }

  private async send(buf: ArrayBuffer): Promise<ResponseMessage> {
    const resp = await this.rawSend(buf);
    if (resp.type === MsgType.Rlerror) {
      throw new P9Error((resp as RlerrorMsg).ecode);
    }
    return resp;
  }

  private async walk(fid: number, newfid: number, names: string[]): Promise<Qid[]> {
    const tag = this.allocTag();
    const resp = await this.send(encodeTwalk(tag, fid, newfid, names));
    if (resp.type !== MsgType.Rwalk) throw new Error("Unexpected response to Twalk");
    return resp.wqids;
  }

  private async clunk(fid: number): Promise<void> {
    const tag = this.allocTag();
    await this.send(encodeTclunk(tag, fid));
  }

  private async walkPath(path: string): Promise<{ fid: number; qids: Qid[] }> {
    const fid = this.allocFid();
    const names = path.split("/").filter((s) => s.length > 0);
    const qids = await this.walk(ROOT_FID, fid, names);
    return { fid, qids };
  }

  private async walkToParent(path: string): Promise<{ dirFid: number; name: string }> {
    const parts = path.split("/").filter((s) => s.length > 0);
    if (parts.length === 0) throw new Error("Cannot get parent of root");
    const name = parts.pop()!;
    const dirFid = this.allocFid();
    await this.walk(ROOT_FID, dirFid, parts);
    return { dirFid, name };
  }

  /** Open a directory fid, read all entries, clunk fid, return filtered entries (no . or ..). */
  private async readDirEntries(fid: number, signal?: AbortSignal): Promise<DirEntry[]> {
    const entries: DirEntry[] = [];
    try {
      const tag1 = this.allocTag();
      await this.send(encodeTlopen(tag1, fid, O_RDONLY));
      let offset = 0n;
      const maxCount = this.msize - HEADER_OVERHEAD;
      while (!signal?.aborted) {
        const tag2 = this.allocTag();
        const resp = await this.send(encodeTreaddir(tag2, fid, offset, maxCount));
        if (resp.type !== MsgType.Rreaddir) break;
        if (resp.entries.length === 0) break;
        entries.push(...resp.entries);
        offset = resp.entries[resp.entries.length - 1].offset;
      }
    } finally {
      await this.clunk(fid);
    }
    return entries.filter((e) => e.name !== "." && e.name !== "..");
  }

  async listDirectory(path: string): Promise<FileEntry[]> {
    const dirFid = this.allocFid();
    const dirParts = path.split("/").filter((s) => s.length > 0);
    await this.walk(ROOT_FID, dirFid, dirParts);

    const readFid = this.allocFid();
    await this.walk(dirFid, readFid, []);
    const filtered = await this.readDirEntries(readFid);

    try {
      const results: FileEntry[] = [];
      await pooled(filtered, 20, async (e) => {
        let size = 0n;
        let mtimeSec = 0n;
        let mode = 0;
        let uid = 0;
        let gid = 0;
        let nlink = 0n;
        const childFid = this.allocFid();
        try {
          await this.walk(dirFid, childFid, [e.name]);
          const t = this.allocTag();
          const resp = await this.send(encodeTgetattr(t, childFid, GETATTR_ALL));
          if (resp.type === MsgType.Rgetattr) {
            size = resp.stat.size;
            mtimeSec = resp.stat.mtimeSec;
            mode = resp.stat.mode;
            uid = resp.stat.uid;
            gid = resp.stat.gid;
            nlink = resp.stat.nlink;
          }
        } catch {
          // stat failed, keep defaults
        } finally {
          try {
            await this.clunk(childFid);
          } catch {
            /* fid may not have been walked */
          }
        }
        results.push({
          name: e.name,
          qid: e.qid,
          type: e.type,
          isDir: (e.qid.type & QID_TYPE_DIR) !== 0,
          isSymlink: e.qid.type === 0x02,
          resolvedIsDir: (mode & 0o170000) === 0o040000,
          size,
          mtimeSec,
          mode,
          uid,
          gid,
          nlink,
        });
      });
      return results;
    } finally {
      await this.clunk(dirFid);
    }
  }

  async stat(path: string): Promise<FileInfo> {
    const { fid } = await this.walkPath(path);
    try {
      const tag = this.allocTag();
      const resp = await this.send(encodeTgetattr(tag, fid, GETATTR_ALL));
      if (resp.type !== MsgType.Rgetattr) throw new Error("Unexpected response");
      const parts = path.split("/").filter((s) => s.length > 0);
      return {
        stat: resp.stat,
        name: parts[parts.length - 1] ?? "/",
        path,
        isDir: (resp.stat.qid.type & QID_TYPE_DIR) !== 0,
        isSymlink: resp.stat.qid.type === 0x02,
      };
    } finally {
      await this.clunk(fid);
    }
  }

  async setattr(path: string, opts: { mode?: number; uid?: number; gid?: number }): Promise<void> {
    const { fid } = await this.walkPath(path);
    try {
      const tag = this.allocTag();
      const resp = await this.send(encodeTsetattr(tag, fid, opts));
      if (resp.type !== MsgType.Rsetattr) throw new Error("Unexpected response");
    } finally {
      await this.clunk(fid);
    }
  }

  async setattrRecursive(
    dirPath: string,
    opts: { mode?: number; uid?: number; gid?: number },
    signal?: AbortSignal,
  ): Promise<{ applied: number; failed: number }> {
    const acc = { applied: 0, failed: 0 };

    const walk = async (dir: string): Promise<void> => {
      try {
        await this.setattr(dir, opts);
        acc.applied++;
      } catch {
        acc.failed++;
      }

      const { fid } = await this.walkPath(dir);
      const children = await this.readDirEntries(fid, signal);
      await pooled(children, 8, async (e) => {
        if (signal?.aborted) return;
        const childPath = join(dir, e.name);
        const isDir = (e.qid.type & QID_TYPE_DIR) !== 0;
        try {
          if (isDir) {
            await walk(childPath);
          } else {
            await this.setattr(childPath, opts);
            acc.applied++;
          }
        } catch {
          acc.failed++;
        }
      });
    };

    await walk(dirPath);
    return acc;
  }

  async dirSize(
    dirPath: string,
    onProgress?: (stats: { size: bigint; files: number; dirs: number }) => void,
    signal?: AbortSignal,
  ): Promise<{ size: bigint; files: number; dirs: number }> {
    const acc = { size: 0n, files: 0, dirs: 0 };

    const walk = async (dir: string): Promise<void> => {
      const { fid } = await this.walkPath(dir);
      const children = await this.readDirEntries(fid, signal);
      await pooled(children, 8, async (e) => {
        if (signal?.aborted) return;
        const childPath = join(dir, e.name);
        const isDir = (e.qid.type & QID_TYPE_DIR) !== 0;
        try {
          if (isDir) {
            acc.dirs++;
            onProgress?.({ ...acc });
            await walk(childPath);
          } else {
            const info = await this.stat(childPath);
            acc.size += info.stat.size;
            acc.files++;
            onProgress?.({ ...acc });
          }
        } catch {
          // skip inaccessible entries
        }
      });
    };

    await walk(dirPath);
    return acc;
  }

  async readFileChunk(path: string, offset: number, length: number): Promise<Uint8Array> {
    const { fid } = await this.walkPath(path);
    try {
      const tag1 = this.allocTag();
      await this.send(encodeTlopen(tag1, fid, O_RDONLY));
      const tag2 = this.allocTag();
      const resp = await this.send(encodeTread(tag2, fid, BigInt(offset), length));
      if (resp.type !== MsgType.Rread) throw new Error("Unexpected response");
      return resp.data;
    } finally {
      await this.clunk(fid);
    }
  }

  async readFileHead(path: string, bytes = 4096): Promise<Uint8Array> {
    return this.readFileChunk(path, 0, bytes);
  }

  readFileStream(path: string): ReadableStream<Uint8Array> {
    let fid: number;
    let offset = 0n;
    return new ReadableStream({
      start: async () => {
        const result = await this.walkPath(path);
        fid = result.fid;
        const tag = this.allocTag();
        await this.send(encodeTlopen(tag, fid, O_RDONLY));
      },
      pull: async (controller) => {
        const maxChunk = this.msize - HEADER_OVERHEAD;
        const tag = this.allocTag();
        const resp = await this.send(encodeTread(tag, fid, offset, maxChunk));
        if (resp.type !== MsgType.Rread || resp.data.length === 0) {
          await this.clunk(fid);
          controller.close();
          return;
        }
        controller.enqueue(resp.data);
        offset += BigInt(resp.data.length);
      },
      cancel: async () => {
        await this.clunk(fid);
      },
    });
  }

  async readFile(path: string, signal?: AbortSignal): Promise<Uint8Array> {
    const { fid } = await this.walkPath(path);
    try {
      const tag1 = this.allocTag();
      await this.send(encodeTlopen(tag1, fid, O_RDONLY));

      const maxChunk = this.msize - HEADER_OVERHEAD;
      const chunks: Uint8Array[] = [];
      let offset = 0n;

      while (!signal?.aborted) {
        const tag2 = this.allocTag();
        const resp = await this.send(encodeTread(tag2, fid, offset, maxChunk));
        if (resp.type !== MsgType.Rread) throw new Error("Unexpected response");
        if (resp.data.length === 0) break;
        chunks.push(resp.data);
        offset += BigInt(resp.data.length);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");

      const totalLen = chunks.reduce((sum, c) => sum + c.length, 0);
      const result = new Uint8Array(totalLen);
      let pos = 0;
      for (const chunk of chunks) {
        result.set(chunk, pos);
        pos += chunk.length;
      }
      return result;
    } finally {
      await this.clunk(fid);
    }
  }

  async writeFile(path: string, data: Uint8Array, mode = 0o644, signal?: AbortSignal): Promise<void> {
    try {
      await this.remove(path);
    } catch {
      /* doesn't exist, fine */
    }
    const { dirFid, name } = await this.walkToParent(path);
    try {
      const tag1 = this.allocTag();
      await this.send(encodeTlcreate(tag1, dirFid, name, 0x241, mode, 0));

      const maxChunk = this.msize - HEADER_OVERHEAD - 4 - 8 - 4; // fid + offset + count
      let offset = 0n;

      while (offset < BigInt(data.length) && !signal?.aborted) {
        const end = Math.min(data.length, Number(offset) + maxChunk);
        const chunk = data.subarray(Number(offset), end);
        const tag2 = this.allocTag();
        await this.send(encodeTwrite(tag2, dirFid, offset, chunk));
        offset += BigInt(chunk.length);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
    } finally {
      await this.clunk(dirFid);
    }
  }

  // Atomic save via .swp + rename
  async saveFile(path: string, data: Uint8Array): Promise<void> {
    const parts = path.split("/");
    const fileName = parts.pop()!;
    const dirPath = parts.join("/") || "/";
    const swpPath = join(dirPath, `.${fileName}.swp`);

    await this.writeFile(swpPath, data);
    await this.rename(swpPath, path);
  }

  async mkdir(path: string, mode = 0o755): Promise<Qid> {
    const { dirFid, name } = await this.walkToParent(path);
    try {
      const tag = this.allocTag();
      const resp = await this.send(encodeTmkdir(tag, dirFid, name, mode, 0));
      if (resp.type !== MsgType.Rmkdir) throw new Error("Unexpected response");
      return resp.qid;
    } finally {
      await this.clunk(dirFid);
    }
  }

  async remove(path: string): Promise<void> {
    const { dirFid, name } = await this.walkToParent(path);
    try {
      const tag = this.allocTag();
      await this.send(encodeTunlinkat(tag, dirFid, name, 0));
    } finally {
      await this.clunk(dirFid);
    }
  }

  async removeDir(path: string): Promise<void> {
    const { dirFid, name } = await this.walkToParent(path);
    try {
      const tag = this.allocTag();
      await this.send(encodeTunlinkat(tag, dirFid, name, AT_REMOVEDIR));
    } finally {
      await this.clunk(dirFid);
    }
  }

  async removeDirRecursive(
    dirPath: string,
    onProgress?: (deleted: number, current: string) => void,
    signal?: AbortSignal,
  ): Promise<number> {
    const counter = { value: 0 };
    await this._removeDirRecursiveInner(dirPath, onProgress, signal, counter);
    return counter.value;
  }

  private async _removeDirRecursiveInner(
    dirPath: string,
    onProgress: ((deleted: number, current: string) => void) | undefined,
    signal: AbortSignal | undefined,
    counter: { value: number },
  ): Promise<void> {
    const { fid } = await this.walkPath(dirPath);
    const filtered = await this.readDirEntries(fid, signal);
    const files = filtered.filter((e) => !(e.qid.type & QID_TYPE_DIR));
    const dirs = filtered.filter((e) => (e.qid.type & QID_TYPE_DIR) !== 0);

    await pooled(files, 8, async (e) => {
      if (signal?.aborted) return;
      const childPath = join(dirPath, e.name);
      try {
        await this.remove(childPath);
      } catch {
        // may already be gone
      }
      counter.value++;
      onProgress?.(counter.value, childPath);
    });

    await pooled(dirs, 8, async (e) => {
      if (signal?.aborted) return;
      const childPath = join(dirPath, e.name);
      try {
        await this._removeDirRecursiveInner(childPath, onProgress, signal, counter);
      } catch {
        // may already be gone
      }
    });

    if (!signal?.aborted) {
      try {
        await this.removeDir(dirPath);
      } catch {
        /* may already be gone */
      }
      counter.value++;
      onProgress?.(counter.value, dirPath);
    }
  }

  async rename(oldPath: string, newPath: string): Promise<void> {
    const oldParts = oldPath.split("/").filter((s) => s.length > 0);
    const newParts = newPath.split("/").filter((s) => s.length > 0);
    const oldName = oldParts.pop()!;
    const newName = newParts.pop()!;

    const oldDirFid = this.allocFid();
    const newDirFid = this.allocFid();
    await this.walk(ROOT_FID, oldDirFid, oldParts);
    await this.walk(ROOT_FID, newDirFid, newParts);

    try {
      const tag = this.allocTag();
      await this.send(encodeTrenameat(tag, oldDirFid, oldName, newDirFid, newName));
    } finally {
      await this.clunk(oldDirFid);
      await this.clunk(newDirFid);
    }
  }

  async symlink(path: string, target: string): Promise<Qid> {
    const { dirFid, name } = await this.walkToParent(path);
    try {
      const tag = this.allocTag();
      const resp = await this.send(encodeTsymlink(tag, dirFid, name, target, 0));
      if (resp.type !== MsgType.Rsymlink) throw new Error("Unexpected response");
      return resp.qid;
    } finally {
      await this.clunk(dirFid);
    }
  }

  async readlink(path: string): Promise<string> {
    const { fid } = await this.walkPath(path);
    try {
      const tag = this.allocTag();
      const resp = await this.send(encodeTreadlink(tag, fid));
      if (resp.type !== MsgType.Rreadlink) throw new Error("Unexpected response");
      return resp.target;
    } finally {
      await this.clunk(fid);
    }
  }

  async download(
    path: string,
    onProgress?: (received: number, total: number) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    let totalSize = 0;
    if (onProgress) {
      try {
        const info = await this.stat(path);
        totalSize = Number(info.stat.size);
      } catch {
        /* ignored */
      }
    }

    const { fid } = await this.walkPath(path);
    try {
      const tag1 = this.allocTag();
      await this.send(encodeTlopen(tag1, fid, O_RDONLY));

      const maxChunk = this.msize - HEADER_OVERHEAD;
      const chunks: Uint8Array[] = [];
      let offset = 0n;
      let received = 0;

      while (!signal?.aborted) {
        const tag2 = this.allocTag();
        const resp = await this.send(encodeTread(tag2, fid, offset, maxChunk));
        if (resp.type !== MsgType.Rread) throw new Error("Unexpected response");
        if (resp.data.length === 0) break;
        chunks.push(resp.data);
        offset += BigInt(resp.data.length);
        received += resp.data.length;
        onProgress?.(received, totalSize);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");

      const totalLen = chunks.reduce((sum, c) => sum + c.length, 0);
      const result = new Uint8Array(totalLen);
      let pos = 0;
      for (const chunk of chunks) {
        result.set(chunk, pos);
        pos += chunk.length;
      }

      const blob = new Blob([result.slice().buffer as ArrayBuffer]);
      const url = URL.createObjectURL(blob);
      const parts = path.split("/");
      const filename = parts[parts.length - 1] ?? "download";
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
    } finally {
      await this.clunk(fid);
    }
  }

  async uploadBlob(
    targetPath: string,
    blob: Blob,
    onProgress?: (sent: number, total: number) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    try {
      await this.remove(targetPath);
    } catch {
      /* doesn't exist */
    }
    const { dirFid, name } = await this.walkToParent(targetPath);
    try {
      const tag1 = this.allocTag();
      await this.send(encodeTlcreate(tag1, dirFid, name, 0x241, 0o644, 0));

      const maxChunk = this.msize - HEADER_OVERHEAD - 4 - 8 - 4;
      let offset = 0;

      while (offset < blob.size && !signal?.aborted) {
        const end = Math.min(blob.size, offset + maxChunk);
        const slice = blob.slice(offset, end);
        const chunk = new Uint8Array(await slice.arrayBuffer());
        const tag2 = this.allocTag();
        await this.send(encodeTwrite(tag2, dirFid, BigInt(offset), chunk));
        offset = end;
        onProgress?.(offset, blob.size);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
    } finally {
      await this.clunk(dirFid);
    }
  }

  async search(
    basePath: string,
    query: string,
    onResult: (result: SearchResult) => void,
    signal?: AbortSignal,
    maxDepth = 10,
  ): Promise<void> {
    const regex = queryToRegex(query);
    await this.searchDir(basePath, regex, onResult, signal, 0, maxDepth);
  }

  private async searchDir(
    dirPath: string,
    regex: RegExp,
    onResult: (result: SearchResult) => void,
    signal: AbortSignal | undefined,
    depth: number,
    maxDepth: number,
  ): Promise<void> {
    if (signal?.aborted || depth > maxDepth) return;

    const { fid } = await this.walkPath(dirPath);
    const entries = await this.readDirEntries(fid, signal);

    const subdirs: string[] = [];
    for (const e of entries) {
      if (signal?.aborted) return;
      const childPath = join(dirPath, e.name);
      const isDir = (e.qid.type & QID_TYPE_DIR) !== 0;
      const match = regex.exec(e.name);
      if (match) {
        onResult({
          name: e.name,
          path: childPath,
          isDir,
          matchStart: match.index,
          matchEnd: match.index + match[0].length,
        });
      }
      if (isDir) subdirs.push(childPath);
    }

    await pooled(subdirs, 8, async (sub) => {
      if (signal?.aborted) return;
      try {
        await this.searchDir(sub, regex, onResult, signal, depth + 1, maxDepth);
      } catch {
        // skip inaccessible subdirs
      }
    });
  }
  async collectFiles(
    dirPath: string,
    basePath: string,
    onProgress: (file: string) => void,
    signal?: AbortSignal,
  ): Promise<{ path: string; data: Uint8Array }[]> {
    if (signal?.aborted) return [];

    const { fid } = await this.walkPath(dirPath);
    const filtered = await this.readDirEntries(fid, signal);

    const results: { path: string; data: Uint8Array }[] = [];
    await pooled(filtered, 8, async (e) => {
      if (signal?.aborted) return;
      const childPath = join(dirPath, e.name);
      const relativePath = basePath ? `${basePath}/${e.name}` : e.name;
      const isDir = (e.qid.type & QID_TYPE_DIR) !== 0;

      try {
        if (isDir) {
          const subFiles = await this.collectFiles(childPath, relativePath, onProgress, signal);
          results.push(...subFiles);
        } else {
          onProgress(relativePath);
          const data = await this.readFile(childPath, signal);
          results.push({ path: relativePath, data });
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === "AbortError") throw err;
      }
    });

    return results;
  }
}

export interface SearchResult {
  name: string;
  path: string;
  isDir: boolean;
  matchStart: number;
  matchEnd: number;
}

export const p9client = new NinePClient();
