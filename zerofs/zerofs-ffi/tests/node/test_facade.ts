// Runs against the generated Node package + facade (both copied next to this
// file at test time). Invoked with the server target as argv[2].
//
//   node test_facade.ts unix:/run/zerofs/9p.sock

import {
  Client,
  FileType,
  openOptions,
  isFile,
  isDir,
  isSymlink,
  ZeroFsErrorNotFound,
  type DirEntry,
} from "./zerofs.ts";

const target = process.argv[2];

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

async function collect(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  return concat(chunks);
}

async function main(): Promise<void> {
  // `await using`: the client closes automatically at scope exit.
  await using fs = await Client.connect(target);
  console.log("connected msize=", fs.capabilities().msize);

  await fs.create_dir_all("/node", 0o755);
  await fs.write("/node/a.txt", new TextEncoder().encode("hello from node"));
  if (
    new TextDecoder().decode(await fs.read("/node/a.txt")) !== "hello from node"
  ) {
    throw new Error("read mismatch");
  }

  const meta = await fs.stat("/node/a.txt");
  if (meta.file_type !== FileType.File) throw new Error("not a file");
  if (!(meta.mtime instanceof Date)) throw new Error("mtime is not a Date");

  // File handle, positioned I/O.
  await using f = await fs.open(
    "/node/big",
    openOptions({ read: true, write: true, create: true }),
  );
  await f.write_at(0n, new Uint8Array([1, 2, 3, 4]));
  if ((await f.read_at(0n, 4)).length !== 4)
    throw new Error("read_at mismatch");

  // Async iteration over a directory.
  const names: string[] = [];
  {
    await using dir = await fs.open_dir("/node");
    for await (const entry of dir as AsyncIterable<DirEntry>)
      names.push(entry.name);
  }
  names.sort();
  if (JSON.stringify(names) !== JSON.stringify(["a.txt", "big"])) {
    throw new Error("listing mismatch: " + names.join(","));
  }

  // Typed error.
  try {
    await fs.read("/node/missing");
    throw new Error("expected NotFound");
  } catch (e) {
    if (!(e instanceof ZeroFsErrorNotFound)) throw e;
  }

  // camelCase alias: `openDir` delegates to the generated `open_dir`.
  {
    await using dir = await fs.openDir("/node");
    const batch = await dir.nextBatch(undefined);
    if (!Array.isArray(batch))
      throw new Error("nextBatch did not return a batch");
  }

  // WRITE STREAMING: pipe a multi-chunk byte stream into a File via writable().
  // The chunks are deliberately uneven and >1 chunk so the running offset is
  // exercised, then read the whole thing back to confirm it landed contiguously.
  const parts = [
    new Uint8Array([0, 1, 2, 3, 4]),
    new Uint8Array(300_000).fill(7), // big middle chunk so the read side multi-pulls
    new TextEncoder().encode("tail"),
  ];
  const expected = concat(parts);
  {
    await using wf = await fs.open(
      "/node/stream",
      openOptions({ write: true, create: true, truncate: true }),
    );
    const src = new ReadableStream<Uint8Array>({
      pull(controller) {
        const next = parts.shift();
        if (next === undefined) controller.close();
        else controller.enqueue(next);
      },
    });
    await src.pipeTo(wf.writable());
  }
  if (!bytesEqual(await fs.read("/node/stream"), expected)) {
    throw new Error("writable() round-trip mismatch");
  }

  // READ STREAMING: collect readable() back and confirm it equals what we wrote,
  // forcing multiple pulls by using a small chunk size (multi-chunk read).
  {
    await using rf = await fs.open("/node/stream", openOptions({ read: true }));
    const stream = rf.readable(64 * 1024); // small chunk → many pulls over ~300 KB
    const collected = await collect(stream);
    if (!bytesEqual(collected, expected)) {
      throw new Error("readable() round-trip mismatch");
    }
    if (collected.length !== expected.length) {
      throw new Error("readable() length mismatch: " + collected.length);
    }
  }

  // Metadata predicates (free functions) + string path helpers.
  await fs.create_dir_all("/node/sub", 0o755);
  if (!isFile(await fs.stat("/node/a.txt"))) throw new Error("isFile wrong");
  if (!isDir(await fs.stat("/node/sub"))) throw new Error("isDir wrong");
  await fs.symlink("a.txt", "/node/link");
  if (!isSymlink(await fs.stat("/node/link")))
    throw new Error("isSymlink wrong");
  if ((await fs.readLinkStr("/node/link")) !== "a.txt")
    throw new Error("readLinkStr mismatch");
  if ((await fs.canonicalizeStr("/node/a.txt")) !== "/node/a.txt") {
    throw new Error("canonicalizeStr mismatch");
  }

  await fs.remove_dir_all("/node");
  console.log("NODE FACADE CHECKS PASSED");
}

await main();
