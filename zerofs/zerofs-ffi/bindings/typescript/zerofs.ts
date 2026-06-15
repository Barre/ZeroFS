// Idiomatic TypeScript facade over the generated `uniffi-bindgen-node-js`
// package. Re-exports everything from the generated `./index.js` and layers the
// niceties uniffi cannot generate: explicit resource management (`await using`)
// and async iteration. Ship it next to the generated `index.js`.
//
//   import { Client } from "zerofs-client";
//
//   await using fs = await Client.connect("unix:/run/zerofs/9p.sock");
//   await fs.write("/hello.txt", new TextEncoder().encode("hi"));
//   await using dir = await fs.open_dir("/");
//   for await (const entry of dir) console.log(entry.name);
//   // fs and dir are disposed (closed) automatically at scope exit.
//
// `File` shadows the DOM `File`; alias on import if you need both:
//   import { File as ZfsFile } from "zerofs-client";

import {
  Client,
  Dir,
  File,
  FileType,
  type ConnectOptions,
  type DirEntry,
  type Metadata,
  type NodeKind,
  type OpenOptions,
  type SetAttrs,
  type SetTime,
} from "./index.js";

export * from "./index.js";

// Metadata crosses as a plain record (not a class), so these are free functions
// rather than methods: isDir(meta) instead of meta.file_type === FileType.Dir.

/** True if the entry is a regular file. */
export function isFile(m: Metadata): boolean {
  return m.file_type === FileType.File;
}
/** True if the entry is a directory. */
export function isDir(m: Metadata): boolean {
  return m.file_type === FileType.Dir;
}
/** True if the entry is a symbolic link. */
export function isSymlink(m: Metadata): boolean {
  return m.file_type === FileType.Symlink;
}
/** Permission bits only (mode & 0o7777). */
export function permissions(m: Metadata): number {
  return m.mode & 0o7777;
}

// canonicalize/read_link return raw bytes (lossless); these decode UTF-8.
const _utf8 = new TextDecoder();

declare module "./index.js" {
  interface Client {
    /** `canonicalize` decoded as a UTF-8 string. */
    canonicalizeStr(path: string): Promise<string>;
    /** `read_link` target decoded as a UTF-8 string. */
    readLinkStr(path: string): Promise<string>;
  }
}

Client.prototype.canonicalizeStr = async function (this: Client, path: string): Promise<string> {
  return _utf8.decode(await this.canonicalize(path));
};
Client.prototype.readLinkStr = async function (this: Client, path: string): Promise<string> {
  return _utf8.decode(await this.read_link(path));
};

// The generated bindings don't apply record field defaults, so callers would
// have to spell out every `OpenOptions` field. This fills the documented
// defaults (all flags false, mode 0o644) so `openOptions({ read: true })` works.
export function openOptions(opts: Partial<OpenOptions> = {}): OpenOptions {
  return {
    read: false,
    write: false,
    create: false,
    create_new: false,
    truncate: false,
    mode: 0o644,
    ...opts,
  };
}

// Type-only declarations; the runtime hooks (dispose, async-iterator, camelCase
// aliases) are installed below. camelCase names are declared here so callers get
// them fully typed alongside the generated snake_case ones.
declare module "./index.js" {
  interface Client extends AsyncDisposable {
    createDir(path: string, mode: number): Promise<Metadata>;
    createDirAll(path: string, mode: number): Promise<void>;
    hardLink(original: string, link: string): Promise<Metadata>;
    openDir(path: string): Promise<Dir>;
    readDir(path: string): Promise<Array<DirEntry>>;
    readLink(path: string): Promise<Uint8Array>;
    readRange(path: string, offset: bigint | number, len: number): Promise<Uint8Array>;
    removeDir(path: string): Promise<void>;
    removeDirAll(path: string): Promise<void>;
    removeFile(path: string): Promise<void>;
    setAttr(path: string, attrs: SetAttrs): Promise<Metadata>;
    setTimes(
      path: string,
      atime: SetTime | undefined,
      mtime: SetTime | undefined,
    ): Promise<Metadata>;
  }
  interface File extends AsyncDisposable {
    readAt(offset: bigint | number, len: number): Promise<Uint8Array>;
    writeAt(offset: bigint | number, data: Uint8Array): Promise<void>;
    setAttr(attrs: SetAttrs): Promise<Metadata>;
    setLen(size: bigint | number): Promise<void>;
    syncAll(): Promise<void>;
    syncData(): Promise<void>;
  }
  interface Dir extends AsyncDisposable, AsyncIterable<DirEntry> {
    createDirAt(name: Uint8Array, mode: number): Promise<Metadata>;
    linkAt(originalDir: Dir, originalName: Uint8Array, newName: Uint8Array): Promise<Metadata>;
    metadataAt(name: Uint8Array): Promise<Metadata>;
    mknodAt(name: Uint8Array, kind: NodeKind, mode: number): Promise<Metadata>;
    nextBatch(maxEntries: number | undefined): Promise<Array<DirEntry>>;
    openAt(name: Uint8Array, opts: OpenOptions): Promise<File>;
    openDirAt(name: Uint8Array): Promise<Dir>;
    readLinkAt(name: Uint8Array): Promise<Uint8Array>;
    removeDirAt(name: Uint8Array): Promise<void>;
    removeFileAt(name: Uint8Array): Promise<void>;
    renameAt(oldName: Uint8Array, newDir: Dir, newName: Uint8Array): Promise<void>;
    setAttr(attrs: SetAttrs): Promise<Metadata>;
    setAttrAt(name: Uint8Array, attrs: SetAttrs): Promise<Metadata>;
    symlinkAt(name: Uint8Array, target: Uint8Array): Promise<Metadata>;
  }
}

type Closable = { close(): Promise<void> };
const symbols = (o: object) => o as unknown as Record<symbol, unknown>;

// `await using` → close() on scope exit. close() never throws and is idempotent.
function installAsyncDispose(proto: object): void {
  symbols(proto)[Symbol.asyncDispose] = function (this: Closable): Promise<void> {
    return this.close();
  };
}
installAsyncDispose(Client.prototype);
installAsyncDispose(File.prototype);
installAsyncDispose(Dir.prototype);

// `for await (const entry of dir)` → one server batch at a time off the shared
// listing cursor until the directory is exhausted.
symbols(Dir.prototype)[Symbol.asyncIterator] = async function* (
  this: Dir,
): AsyncGenerator<DirEntry> {
  for (;;) {
    const batch = await this.next_batch(undefined);
    if (batch.length === 0) return;
    for (const entry of batch) yield entry;
  }
};

// WHATWG streams over a File, for use with `fetch`, `Response.body`,
// `pipeTo`/`pipeThrough`, `Blob`, etc.

// Per-pull read size. The client chunks larger reads internally, but keeping
// each `read_at` bounded means a short (sub-chunk) result reliably signals EOF.
const DEFAULT_CHUNK = 1 << 20; // 1 MiB

declare module "./index.js" {
  interface File {
    /**
     * A pull-based `ReadableStream` over the file's bytes from offset 0 to EOF.
     * Each pull issues one `read_at` of up to `chunkSize` bytes; a short read
     * ends the stream. The file handle stays open; dispose it when done.
     */
    readable(chunkSize?: number): ReadableStream<Uint8Array>;
    /**
     * A `WritableStream` that appends each chunk via `write_at` at a running
     * offset (starting at `offset`, default 0). Backpressure is honoured: the
     * next chunk isn't pulled until the current `write_at` resolves.
     */
    writable(offset?: number | bigint): WritableStream<Uint8Array>;
  }
}

File.prototype.readable = function (
  this: File,
  chunkSize: number = DEFAULT_CHUNK,
): ReadableStream<Uint8Array> {
  const len = Math.max(1, Math.floor(chunkSize));
  let offset = 0n;
  return new ReadableStream<Uint8Array>({
    pull: async (controller) => {
      const chunk = await this.read_at(offset, len);
      if (chunk.length === 0) {
        controller.close();
        return;
      }
      offset += BigInt(chunk.length);
      controller.enqueue(chunk);
    },
  });
};

File.prototype.writable = function (
  this: File,
  offset: number | bigint = 0n,
): WritableStream<Uint8Array> {
  let pos = BigInt(offset);
  return new WritableStream<Uint8Array>({
    write: async (chunk) => {
      if (chunk.length === 0) return;
      await this.write_at(pos, chunk);
      pos += BigInt(chunk.length);
    },
  });
};

// The generator emits Rust snake_case method names (`open_dir`, `next_batch`,
// …). For every own method with an underscore, install a camelCase alias
// delegating to it. The snake_case names stay; this is purely additive.
function snakeToCamel(name: string): string {
  return name.replace(/_([a-z0-9])/g, (_m, c: string) => c.toUpperCase());
}

function installCamelAliases(target: object, skip: ReadonlySet<string>): void {
  const obj = target as Record<string, unknown>;
  for (const name of Object.getOwnPropertyNames(target)) {
    if (skip.has(name) || !name.includes("_")) continue;
    const desc = Object.getOwnPropertyDescriptor(target, name);
    if (typeof desc?.value !== "function") continue;
    const camel = snakeToCamel(name);
    if (camel === name || camel in obj) continue;
    obj[camel] = function (this: unknown, ...args: unknown[]): unknown {
      return (obj[name] as (...a: unknown[]) => unknown).apply(this, args);
    };
  }
}

const SKIP_PROTO = new Set(["constructor"]);
installCamelAliases(Client.prototype, SKIP_PROTO);
installCamelAliases(File.prototype, SKIP_PROTO);
installCamelAliases(Dir.prototype, SKIP_PROTO);

// Static methods, e.g. `connect_with` → `connectWith`. `name`/`length`/`prototype`
// are non-underscore, so the filter skips them regardless of SKIP_STATIC.
const SKIP_STATIC = new Set(["prototype"]);
installCamelAliases(Client, SKIP_STATIC);
