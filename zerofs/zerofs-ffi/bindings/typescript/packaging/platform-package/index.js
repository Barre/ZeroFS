// Per-platform native package: ships exactly one prebuilt cdylib and exports
// the absolute path to it. The main `zerofs-client` package's loader `require()`s this
// and hands `libraryPath` to the generator's `load(libraryPath)`.
//
// assemble.sh substitutes the cdylib filename below at publish time:
// libzerofs_ffi.so (Linux), libzerofs_ffi.dylib (macOS), or zerofs_ffi.dll
// (Windows). The placeholder token is the URL argument, nowhere else.
import { fileURLToPath } from "node:url";

export const libraryPath = fileURLToPath(new URL("./__LIBFILE__", import.meta.url));
