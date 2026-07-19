import initWasm, {
  Client as SharedClient,
  DirectoryHandle as SharedDirectory,
  FileHandle as SharedFile,
} from "../../generated/zerofs-client/zerofs_client";
import { pooled } from "../async";
import { joinPath as join } from "../format";

export type ConnectionState = "disconnected" | "connecting" | "connected";

export interface TrafficStats {
  bytesSent: number;
  bytesReceived: number;
  ops: number;
}

interface UploadOptions {
  onProgress?: (sent: number, total: number) => void;
  signal?: AbortSignal;
  batch?: UploadBatch;
}

interface UploadBatch {
  files: Set<SharedFile>;
  finished: boolean;
}

interface WireTimestamp {
  secs: bigint | number;
  nanos: number;
}

type WireFileType = "File" | "Dir" | "Symlink" | "Fifo" | "Socket" | "CharDevice" | "BlockDevice" | "Unknown";

interface WireMetadata {
  ino: bigint | number;
  file_type: WireFileType;
  mode: number;
  nlink: bigint | number;
  uid: number;
  gid: number;
  size: bigint | number;
  block_size: bigint | number;
  blocks: bigint | number;
  rdev: bigint | number;
  atime: WireTimestamp;
  mtime: WireTimestamp;
  ctime: WireTimestamp;
  btime: WireTimestamp;
  data_version: bigint | number;
}

interface WireDirEntry {
  name: string;
  name_bytes: number[];
  name_is_utf8: boolean;
  file_type: WireFileType;
  ino: bigint | number;
  metadata: WireMetadata;
}

interface WireCapabilities {
  msize: number;
  max_read_chunk: number;
  max_write_chunk: number;
}

interface WireTrafficStats {
  bytes_sent: bigint | number;
  bytes_received: bigint | number;
  operations: bigint | number;
}

export interface FileEntry {
  name: string;
  isDir: boolean;
  isSymlink: boolean;
  resolvedIsDir: boolean;
  size: bigint;
  mtimeSec: bigint;
  mode: number;
  uid: number;
  gid: number;
  nlink: bigint;
}

export interface FileStat {
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
  dataVersion: bigint;
}

export interface FileInfo {
  stat: FileStat;
  name: string;
  path: string;
  isDir: boolean;
  isSymlink: boolean;
}

export interface SearchResult {
  name: string;
  path: string;
  isDir: boolean;
  matchStart: number;
  matchEnd: number;
}

const asBigInt = (value: bigint | number): bigint => BigInt(value);
let temporaryFileSequence = 0;

function isErrno(error: unknown, errno: number): boolean {
  return error instanceof Error && "ecode" in error && error.ecode === errno;
}

function toStat(metadata: WireMetadata): FileStat {
  return {
    mode: metadata.mode,
    uid: metadata.uid,
    gid: metadata.gid,
    nlink: asBigInt(metadata.nlink),
    rdev: asBigInt(metadata.rdev),
    size: asBigInt(metadata.size),
    blksize: asBigInt(metadata.block_size),
    blocks: asBigInt(metadata.blocks),
    atimeSec: asBigInt(metadata.atime.secs),
    atimeNsec: BigInt(metadata.atime.nanos),
    mtimeSec: asBigInt(metadata.mtime.secs),
    mtimeNsec: BigInt(metadata.mtime.nanos),
    ctimeSec: asBigInt(metadata.ctime.secs),
    ctimeNsec: BigInt(metadata.ctime.nanos),
    btimeSec: asBigInt(metadata.btime.secs),
    btimeNsec: BigInt(metadata.btime.nanos),
    dataVersion: asBigInt(metadata.data_version),
  };
}

function queryToRegex(query: string): RegExp {
  const explicit = /^\/(.+)\/([gimsuy]*)$/.exec(query);
  if (explicit) {
    try {
      return new RegExp(explicit[1], explicit[2] || "i");
    } catch {
      // Fall through to a literal search.
    }
  }
  if (/[*?]/.test(query)) {
    const pattern = query
      .split("")
      .map((char) => {
        if (char === "*") return ".*";
        if (char === "?") return ".";
        return char.replace(/[.+^${}()|[\]\\]/g, "\\$&");
      })
      .join("");
    return new RegExp(pattern, "i");
  }
  return new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
}

let wasmReady: Promise<unknown> | undefined;

export class ZeroFsClient {
  private client: SharedClient | null = null;
  private connectPromise: Promise<void> | null = null;
  private reconnectUrl: string | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private monitorTimer: ReturnType<typeof setInterval> | null = null;
  private stateListeners = new Set<(state: ConnectionState) => void>();
  private statsListeners = new Set<(stats: TrafficStats) => void>();
  private _state: ConnectionState = "disconnected";
  private previousStats: WireTrafficStats = { bytes_sent: 0, bytes_received: 0, operations: 0 };
  private maxReadChunk = 1024 * 1024;
  private maxWriteChunk = 1024 * 1024;

  get state(): ConnectionState {
    return this._state;
  }

  private setState(state: ConnectionState) {
    if (this._state === state) return;
    this._state = state;
    for (const listener of this.stateListeners) listener(state);
  }

  onStateChange(listener: (state: ConnectionState) => void): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  onStats(listener: (stats: TrafficStats) => void): () => void {
    this.statsListeners.add(listener);
    this.startMonitor();
    return () => this.statsListeners.delete(listener);
  }

  enableAutoReconnect(url: string) {
    this.reconnectUrl = url;
    void this.connect(url).catch(() => {});
  }

  private scheduleInitialReconnect() {
    if (!this.reconnectUrl || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.reconnectUrl) void this.connect(this.reconnectUrl).catch(() => {});
    }, 500);
  }

  private startMonitor() {
    if (this.monitorTimer) return;
    this.monitorTimer = setInterval(() => {
      if (!this.client) return;
      this.setState(this.client.connected ? "connected" : "disconnected");
      if (this.statsListeners.size === 0) return;
      const current = this.client.trafficStats() as WireTrafficStats;
      const delta: TrafficStats = {
        bytesSent: Number(asBigInt(current.bytes_sent) - asBigInt(this.previousStats.bytes_sent)),
        bytesReceived: Number(asBigInt(current.bytes_received) - asBigInt(this.previousStats.bytes_received)),
        ops: Number(asBigInt(current.operations) - asBigInt(this.previousStats.operations)),
      };
      this.previousStats = current;
      for (const listener of this.statsListeners) listener(delta);
    }, 1000);
  }

  async connect(url: string): Promise<void> {
    if (this.client) return;
    if (this.connectPromise) return this.connectPromise;
    this.setState("connecting");
    this.connectPromise = (async () => {
      wasmReady ??= initWasm().catch((error) => {
        wasmReady = undefined;
        throw error;
      });
      await wasmReady;
      this.client = await SharedClient.connect(url);
      const capabilities = this.client.capabilities() as WireCapabilities;
      this.maxReadChunk = capabilities.max_read_chunk;
      this.maxWriteChunk = capabilities.max_write_chunk;
      this.previousStats = this.client.trafficStats() as WireTrafficStats;
      this.setState("connected");
      this.startMonitor();
    })();
    try {
      await this.connectPromise;
    } catch (error) {
      this.client = null;
      this.setState("disconnected");
      this.scheduleInitialReconnect();
      throw error;
    } finally {
      this.connectPromise = null;
    }
  }

  private async fs(): Promise<SharedClient> {
    if (!this.client && this.reconnectUrl) await this.connect(this.reconnectUrl);
    if (!this.client) throw new Error("ZeroFS client is not connected");
    return this.client;
  }

  private async createTemporaryFile(
    fs: SharedClient,
    targetPath: string,
    mode: number,
  ): Promise<{ path: string; file: SharedFile }> {
    const slash = targetPath.lastIndexOf("/");
    const directory = slash <= 0 ? "/" : targetPath.slice(0, slash);
    for (;;) {
      const suffix = `${Date.now().toString(36)}-${(temporaryFileSequence++).toString(36)}-${Math.random()
        .toString(36)
        .slice(2)}`;
      const path = join(directory, `.zerofs-${suffix}.tmp`);
      try {
        const file = await fs.openFile(path, true, true, true, true, false, mode);
        return { path, file };
      } catch (error) {
        if (!isErrno(error, 17)) throw error;
      }
    }
  }

  private async writeAll(file: SharedFile, data: Uint8Array, signal?: AbortSignal): Promise<void> {
    let offset = 0;
    while (offset < data.length && !signal?.aborted) {
      const end = Math.min(data.length, offset + this.maxWriteChunk);
      await file.writeAt(BigInt(offset), data.subarray(offset, end));
      offset = end;
    }
    if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
  }

  private async readDirectoryEntries(fs: SharedClient, path: string, signal?: AbortSignal): Promise<WireDirEntry[]> {
    const directory: SharedDirectory = await fs.openDir(path);
    const entries: WireDirEntry[] = [];
    try {
      while (!signal?.aborted) {
        const batch = (await directory.nextBatch()) as WireDirEntry[];
        if (batch.length === 0) break;
        entries.push(...batch);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
      return entries;
    } finally {
      await directory.close();
    }
  }

  async listDirectory(path: string, signal?: AbortSignal): Promise<FileEntry[]> {
    const fs = await this.fs();
    const entries = await this.readDirectoryEntries(fs, path, signal);
    if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
    return entries.map((entry): FileEntry => {
      const metadata = entry.metadata;
      const isDir = entry.file_type === "Dir";
      return {
        name: entry.name,
        isDir,
        isSymlink: entry.file_type === "Symlink",
        resolvedIsDir: isDir,
        size: asBigInt(metadata.size),
        mtimeSec: asBigInt(metadata.mtime.secs),
        mode: metadata.mode,
        uid: metadata.uid,
        gid: metadata.gid,
        nlink: asBigInt(metadata.nlink),
      };
    });
  }

  async stat(path: string): Promise<FileInfo> {
    const metadata = (await (await this.fs()).stat(path)) as WireMetadata;
    const parts = path.split("/").filter(Boolean);
    return {
      stat: toStat(metadata),
      name: parts.at(-1) ?? "/",
      path,
      isDir: metadata.file_type === "Dir",
      isSymlink: metadata.file_type === "Symlink",
    };
  }

  async setattr(path: string, opts: { mode?: number; uid?: number; gid?: number }): Promise<void> {
    await (await this.fs()).setAttr(path, opts.mode, opts.uid, opts.gid);
  }

  async setattrRecursive(
    dirPath: string,
    opts: { mode?: number; uid?: number; gid?: number },
    signal?: AbortSignal,
  ): Promise<{ applied: number; failed: number }> {
    const result = { applied: 0, failed: 0 };
    const visit = async (path: string): Promise<void> => {
      try {
        await this.setattr(path, opts);
        result.applied++;
      } catch {
        result.failed++;
      }
      const children = await this.listDirectory(path, signal);
      await pooled(children, 8, async (entry) => {
        if (signal?.aborted) return;
        const child = join(path, entry.name);
        if (entry.isDir) {
          try {
            await visit(child);
          } catch {
            result.failed++;
          }
        } else {
          try {
            await this.setattr(child, opts);
            result.applied++;
          } catch {
            result.failed++;
          }
        }
      });
    };
    await visit(dirPath);
    return result;
  }

  async dirSize(
    dirPath: string,
    onProgress?: (stats: { size: bigint; files: number; dirs: number }) => void,
    signal?: AbortSignal,
  ): Promise<{ size: bigint; files: number; dirs: number }> {
    const result = { size: 0n, files: 0, dirs: 0 };
    const visit = async (path: string): Promise<void> => {
      const entries = await this.listDirectory(path, signal);
      await pooled(entries, 8, async (entry) => {
        if (signal?.aborted) return;
        try {
          if (entry.isDir) {
            result.dirs++;
            onProgress?.({ ...result });
            await visit(join(path, entry.name));
          } else {
            result.size += entry.size;
            result.files++;
            onProgress?.({ ...result });
          }
        } catch {
          // Skip inaccessible entries.
        }
      });
    };
    await visit(dirPath);
    return result;
  }

  async readFileChunk(path: string, offset: number, length: number): Promise<Uint8Array> {
    return (await this.fs()).readRange(path, BigInt(offset), length);
  }

  async readFileHead(path: string, bytes = 4096): Promise<Uint8Array> {
    return this.readFileChunk(path, 0, bytes);
  }

  async readFile(path: string, signal?: AbortSignal): Promise<Uint8Array> {
    const fs = await this.fs();
    const file = await fs.openFile(path, true, false, false, false, false, 0o644);
    const chunks: Uint8Array[] = [];
    let offset = 0n;
    try {
      while (!signal?.aborted) {
        const chunk = file.readAt(offset, this.maxReadChunk);
        const data = await chunk;
        if (data.length === 0) break;
        chunks.push(data);
        offset += BigInt(data.length);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
    } finally {
      await file.close();
    }
    const result = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
    let position = 0;
    for (const chunk of chunks) {
      result.set(chunk, position);
      position += chunk.length;
    }
    return result;
  }

  async writeFile(path: string, data: Uint8Array, mode = 0o644, signal?: AbortSignal): Promise<void> {
    const fs = await this.fs();
    const temporary = await this.createTemporaryFile(fs, path, mode);
    let renamed = false;
    try {
      await this.writeAll(temporary.file, data, signal);
      await fs.rename(temporary.path, path);
      renamed = true;
      await fs.sync();
    } finally {
      await temporary.file.close();
      if (!renamed) {
        try {
          await fs.removeFile(temporary.path);
        } catch {
          // Best-effort cleanup after a failed or cancelled write.
        }
      }
    }
  }

  beginUploadBatch(): UploadBatch {
    return { files: new Set(), finished: false };
  }

  async finishUploadBatch(batch: UploadBatch): Promise<void> {
    if (batch.finished) return;
    batch.finished = true;
    const fs = await this.fs();
    const files = [...batch.files];
    try {
      await fs.sync();
    } finally {
      batch.files.clear();
      await Promise.all(files.map((file) => file.close()));
    }
  }

  async saveFile(path: string, data: Uint8Array): Promise<void> {
    await this.writeFile(path, data);
  }

  async mkdir(path: string, mode = 0o755): Promise<void> {
    await (await this.fs()).createDir(path, mode);
  }

  async remove(path: string): Promise<void> {
    await (await this.fs()).removeFile(path);
  }

  async removeDir(path: string): Promise<void> {
    await (await this.fs()).removeDir(path);
  }

  async removeDirRecursive(
    dirPath: string,
    onProgress?: (deleted: number, current: string) => void,
    signal?: AbortSignal,
  ): Promise<number> {
    let deleted = 0;
    const visit = async (path: string): Promise<void> => {
      const entries = await this.listDirectory(path, signal);
      await pooled(entries, 8, async (entry) => {
        if (signal?.aborted) return;
        const child = join(path, entry.name);
        if (entry.isDir) {
          try {
            await visit(child);
          } catch {
            // It may already have disappeared or become inaccessible.
          }
        } else {
          try {
            await this.remove(child);
          } catch {
            // It may already have disappeared.
          }
          onProgress?.(++deleted, child);
        }
      });
      if (!signal?.aborted) {
        try {
          await this.removeDir(path);
        } catch {
          // It may already have disappeared.
        }
        onProgress?.(++deleted, path);
      }
    };
    await visit(dirPath);
    return deleted;
  }

  async rename(oldPath: string, newPath: string): Promise<void> {
    await (await this.fs()).rename(oldPath, newPath);
  }

  async symlink(path: string, target: string): Promise<void> {
    await (await this.fs()).symlink(target, path);
  }

  async readlink(path: string): Promise<string> {
    return new TextDecoder().decode(await (await this.fs()).readLink(path));
  }

  private async readToChunks(
    path: string,
    onProgress?: (received: number, total: number) => void,
    signal?: AbortSignal,
  ): Promise<Uint8Array[]> {
    const info = await this.stat(path);
    const total = Number(info.stat.size);
    const fs = await this.fs();
    const file = await fs.openFile(path, true, false, false, false, false, 0o644);
    const chunks: Uint8Array[] = [];
    let offset = 0n;
    try {
      while (!signal?.aborted) {
        const data = await file.readAt(offset, this.maxReadChunk);
        if (data.length === 0) break;
        chunks.push(data);
        offset += BigInt(data.length);
        onProgress?.(Number(offset), total);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
      return chunks;
    } finally {
      await file.close();
    }
  }

  async download(
    path: string,
    onProgress?: (received: number, total: number) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    const chunks = await this.readToChunks(path, onProgress, signal);
    const blobParts = chunks.map((chunk) => chunk.slice().buffer as ArrayBuffer);
    const url = URL.createObjectURL(new Blob(blobParts));
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = path.split("/").at(-1) ?? "download";
    anchor.click();
    URL.revokeObjectURL(url);
  }

  async uploadBlob(targetPath: string, blob: Blob, { onProgress, signal, batch }: UploadOptions = {}): Promise<void> {
    if (batch?.finished) throw new Error("Upload batch has already finished");
    const fs = await this.fs();
    const temporary = await this.createTemporaryFile(fs, targetPath, 0o644);
    const file = temporary.file;
    let offset = 0;
    let deferredClose = false;
    let renamed = false;
    try {
      while (offset < blob.size && !signal?.aborted) {
        const end = Math.min(blob.size, offset + this.maxWriteChunk);
        const data = new Uint8Array(await blob.slice(offset, end).arrayBuffer());
        await file.writeAt(BigInt(offset), data);
        offset = end;
        onProgress?.(offset, blob.size);
      }
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
      await fs.rename(temporary.path, targetPath);
      renamed = true;
      if (batch) {
        if (batch.finished) throw new Error("Upload batch finished while a file was still uploading");
        batch.files.add(file);
        deferredClose = true;
      } else {
        await file.syncAll();
      }
    } finally {
      if (!deferredClose) await file.close();
      if (!renamed) {
        try {
          await fs.removeFile(temporary.path);
        } catch {
          // Best-effort cleanup after a failed or cancelled upload.
        }
      }
    }
  }

  async search(
    basePath: string,
    query: string,
    onResult: (result: SearchResult) => void,
    signal?: AbortSignal,
    maxDepth = 10,
  ): Promise<void> {
    await this.searchDir(basePath, queryToRegex(query), onResult, signal, 0, maxDepth);
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
    const entries = await this.listDirectory(dirPath, signal);
    const subdirectories: string[] = [];
    for (const entry of entries) {
      if (signal?.aborted) return;
      const path = join(dirPath, entry.name);
      regex.lastIndex = 0;
      const match = regex.exec(entry.name);
      if (match) {
        onResult({
          name: entry.name,
          path,
          isDir: entry.isDir,
          matchStart: match.index,
          matchEnd: match.index + match[0].length,
        });
      }
      if (entry.isDir) subdirectories.push(path);
    }
    await pooled(subdirectories, 8, async (path) => {
      if (signal?.aborted) return;
      try {
        await this.searchDir(path, regex, onResult, signal, depth + 1, maxDepth);
      } catch {
        // Skip inaccessible directories.
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
    const entries = await this.listDirectory(dirPath, signal);
    const results: { path: string; data: Uint8Array }[] = [];
    await pooled(entries, 8, async (entry) => {
      if (signal?.aborted) return;
      const child = join(dirPath, entry.name);
      const relative = basePath ? `${basePath}/${entry.name}` : entry.name;
      try {
        if (entry.isDir) {
          results.push(...(await this.collectFiles(child, relative, onProgress, signal)));
        } else {
          onProgress(relative);
          results.push({ path: relative, data: await this.readFile(child, signal) });
        }
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") throw error;
      }
    });
    return results;
  }
}

export const p9client = new ZeroFsClient();
