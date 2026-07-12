import fs from "node:fs";
import assert from "node:assert/strict";
import init, { Client } from "../src/generated/zerofs-client/zerofs_client.js";

const endpoint = process.argv[2];
if (!endpoint) throw new Error("usage: smoke-wasm-client.mjs ws://host/ws/9p");

const wasm = fs.readFileSync(new URL("../src/generated/zerofs-client/zerofs_client_bg.wasm", import.meta.url));
await init({ module_or_path: wasm });

const client = await Client.connect(endpoint);
const capabilities = client.capabilities();
assert.equal(capabilities.extensions_v1, true);
assert.equal(capabilities.extensions_v2, true);
assert.equal(typeof client.trafficStats().bytes_sent, "bigint");

await client.createDir("/wasm-smoke", 0o755);
const file = await client.openFile("/wasm-smoke/hello.txt", true, true, true, false, true, 0o644);
const contents = new TextEncoder().encode("hello from the shared browser client");
await file.writeAt(0n, contents);
await file.syncAll();

const dropEndpoint = new URL(endpoint);
dropEndpoint.protocol = dropEndpoint.protocol === "wss:" ? "https:" : "http:";
dropEndpoint.pathname = "/drop-connections";
const dropped = await fetch(dropEndpoint, { method: "POST" });
assert.equal(dropped.status, 204);
assert.deepEqual(await file.readAt(0n, contents.length), contents);
await file.close();

assert.deepEqual(await client.read("/wasm-smoke/hello.txt"), contents);
const directory = await client.openDir("/wasm-smoke");
const entries = await directory.nextBatch();
assert.equal(entries[0].name, "hello.txt");
assert.equal(Array.isArray(entries[0].name_bytes), true);
assert.equal(entries[0].metadata.size, BigInt(contents.length));
assert.deepEqual(await directory.nextBatch(), []);
await directory.close();

const batchedA = await client.openFile("/wasm-smoke/batched-a.txt", true, true, true, false, true, 0o644);
const batchedB = await client.openFile("/wasm-smoke/batched-b.txt", true, true, true, false, true, 0o644);
await batchedA.writeAt(0n, new TextEncoder().encode("one batch-level barrier"));
await batchedB.writeAt(0n, new TextEncoder().encode("for both files"));
const operationsBeforeSync = client.trafficStats().operations;
await client.sync();
assert.equal(client.trafficStats().operations - operationsBeforeSync, 1n);
await batchedA.close();
await batchedB.close();
await client.removeFile("/wasm-smoke/batched-a.txt");
await client.removeFile("/wasm-smoke/batched-b.txt");

await client.rename("/wasm-smoke/hello.txt", "/wasm-smoke/renamed.txt");
await client.removeFile("/wasm-smoke/renamed.txt");
await client.removeDir("/wasm-smoke");
await client.close();

process.stdout.write("shared WASM client smoke test passed\n");
process.exit(0);
