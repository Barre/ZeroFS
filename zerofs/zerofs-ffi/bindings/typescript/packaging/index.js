// Native-addon loader for the `zerofs-client` npm package.
//
// This file REPLACES the generator's auto-loading `index.js` at publish time.
// The generated `index.js` is just:
//
//     import { load as loadFfi } from "./zerofs_ffi-ffi.js";
//     loadFfi();                       // resolves a SIBLING libzerofs_ffi.so
//     export * from "./zerofs_ffi.js";
//
// That sibling-`.so` model assumes the cdylib lives next to the JS. We don't
// ship the `.so` inside the main package (it's platform-specific); instead each
// platform ships its own tiny `@zerofs/ffi-<target>` package carrying exactly
// one cdylib, wired in as an optionalDependency. npm's `os`/`cpu`/`libc`
// filters install only the one matching the host, so at runtime we resolve that
// package, read the absolute path of its bundled cdylib, and hand it to the
// generator's `load(libraryPath)` BEFORE anything touches `zerofs_ffi.js`.
//
// `zerofs_ffi.js`'s `ffiFunctions` are lazy (each call reads the loaded library
// on demand), so calling `load(path)` first and then re-exporting the module
// surface works regardless of import order.

import { load as loadFfi } from "./zerofs_ffi-ffi.js";

// The four supported targets, named to match the generator's
// `defaultBundledTarget()` (so a future switch to UniFFI bundledPrebuilds keeps
// the same identifiers): `<platform>-<arch>` for macOS, and
// `<platform>-<arch>-<gnu|musl>` for Linux (libc matters for the ABI).
function platformTarget() {
  const { platform, arch } = process;
  if (platform === "linux") {
    // `process.report` exposes the runtime's glibc version; its absence means a
    // musl host (Alpine). This is exactly how the generator distinguishes them.
    const glibc = process.report?.getReport?.().header?.glibcVersionRuntime;
    const libc = glibc == null ? "musl" : "gnu";
    return `${platform}-${arch}-${libc}`;
  }
  return `${platform}-${arch}`;
}

// `@zerofs/ffi-<target>` exposes `libraryPath` (the absolute path to its single
// bundled cdylib). Keep the require specifier a plain string literal per target
// so bundlers can statically see every candidate.
function platformPackageFor(target) {
  switch (target) {
    case "linux-x64-gnu":
      return "@zerofs/ffi-linux-x64-gnu";
    case "linux-arm64-gnu":
      return "@zerofs/ffi-linux-arm64-gnu";
    case "darwin-x64":
      return "@zerofs/ffi-darwin-x64";
    case "darwin-arm64":
      return "@zerofs/ffi-darwin-arm64";
    default:
      return null;
  }
}

async function resolveLibraryPath() {
  const target = platformTarget();
  const pkg = platformPackageFor(target);
  if (pkg == null) {
    throw new Error(
      `zerofs-client: unsupported platform target ${JSON.stringify(target)} ` +
        `(${process.platform}/${process.arch}). Supported: linux-x64-gnu, ` +
        `linux-arm64-gnu, darwin-x64, darwin-arm64.`,
    );
  }
  try {
    // The platform package is an ESM module exporting `libraryPath`; import it
    // (works regardless of the host's CJS/ESM interop version, unlike require).
    const { libraryPath } = await import(pkg);
    if (typeof libraryPath !== "string") {
      throw new Error(`package ${pkg} did not export a string \`libraryPath\``);
    }
    return libraryPath;
  } catch (cause) {
    throw new Error(
      `zerofs-client: the prebuilt native library for ${target} is not installed. ` +
        `It ships as the optional dependency ${pkg}. If your installer skipped ` +
        `optional dependencies (e.g. \`npm install --no-optional\`), reinstall ` +
        `without that flag.`,
      { cause },
    );
  }
}

// Load the platform cdylib up front, then expose the generated module surface.
// Top-level await is fine in ESM; `loadFfi` is idempotent for the same path.
loadFfi(await resolveLibraryPath());

export * from "./zerofs_ffi.js";
