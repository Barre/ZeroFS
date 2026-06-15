#!/usr/bin/env bash
# Generate foreign-language bindings for zerofs-ffi from the compiled cdylib.
#
#   ./bindings/generate.sh <language> <out-dir>
#   ./bindings/generate.sh version          # print the client-family version
#
# Languages: python      (via uniffi-bindgen, installed separately)
#            typescript   (via uniffi-bindgen-js, installed separately)
#            go           (via uniffi-bindgen-go, installed separately)
#
# Run from the `zerofs` workspace root (or anywhere; paths are resolved from
# this script's location).
set -euo pipefail

lang="${1:?usage: generate.sh <python|typescript|go|version> <out-dir>}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ws="$(cd "$here/../.." && pwd)"           # the `zerofs` workspace dir

# The "client family" version, single-sourced from zerofs-ffi/Cargo.toml. This
# is the one value npm package.json, the Go module tag, and the Python wheel all
# derive from. The grep one-liner is intentionally dependency-free (no toml
# parser) so CI steps in any language can read it the same way.
client_version() {
    grep -m1 '^version = ' "$here/../Cargo.toml" | sed -E 's/.*"(.*)".*/\1/'
}

# `generate.sh version` prints just the version (for `npm version`, the Go tag,
# stamping package.json, etc.) and exits before touching the cdylib.
if [ "$lang" = "version" ]; then
    client_version
    exit 0
fi

out="${2:?usage: generate.sh <language> <out-dir>}"

lib="$ws/target/debug/libzerofs_ffi.so"
[ -f "$lib" ] || lib="$ws/target/debug/libzerofs_ffi.dylib"
if [ ! -f "$lib" ]; then
    ( cd "$ws" && cargo build -p zerofs-ffi )
    lib="$ws/target/debug/libzerofs_ffi.so"
    [ -f "$lib" ] || lib="$ws/target/debug/libzerofs_ffi.dylib"
fi

mkdir -p "$out"

case "$lang" in
    python)
        # Requires the standalone CLI: pip install uniffi-bindgen==0.31.0
        uniffi-bindgen generate --library "$lib" --language "$lang" --out-dir "$out"
        ;;
    typescript | ts)
        # Runnable Node (ESM) bindings via koffi. Requires:
        #   cargo install uniffi-bindgen-node-js
        uniffi-bindgen-node-js generate --out-dir "$out" \
            --manifest-path "$here/../Cargo.toml" "$lib"
        # Work around an upstream bug in the generated timestamp converter (it
        # does float division `nanos / 1000000` and feeds the result to BigInt).
        sed -i 's#BigInt(nanos / 1000000)#BigInt(Math.floor(nanos / 1000000))#g' \
            "$out/runtime/ffi-converters.js"
        grep -qF 'Math.floor(nanos / 1000000)' "$out/runtime/ffi-converters.js" || {
            echo "generate.sh: timestamp patch did not apply; the upstream generator changed" >&2
            exit 1
        }
        cp "$lib" "$out/$(basename "$lib")"
        cp "$here/typescript/zerofs.ts" "$out/zerofs.ts"
        ;;
    go)
        # Runnable cgo bindings via uniffi-bindgen-go. Requires:
        #   cargo install uniffi-bindgen-go \
        #     --git https://github.com/NordSecurity/uniffi-bindgen-go \
        #     --tag v0.7.0+v0.31.0
        # The generator emits a `zerofs_ffi/` subdir holding zerofs_ffi.go +
        # zerofs_ffi.h. Build with CGO_CFLAGS=-I<that dir> and
        # CGO_LDFLAGS="-L target/debug -lzerofs_ffi"; run with
        # LD_LIBRARY_PATH=target/debug.
        uniffi-bindgen-go "$lib" -o "$out"
        # Work around a bug in this generator version: uniffiRustCallAsync
        # unconditionally lifts the return buffer even on the error path, so
        # every value-returning fallible async call (Read, Stat, Open, ...)
        # panics in FfiConverter*.Lift("EOF") when Rust returns an error. Gate
        # the lift on the C call status so it only runs on success. (A naive
        # `if err != nil` guard does not compile: err has the generic type E,
        # not error.) Same class as the Node ffi-converters.js sed fix above.
        perl -0pi -e 's/\tffiValue, err := rustCallWithError\(errConverter, func\(status \*C\.RustCallStatus\) F \{\n\t\treturn completeFunc\(rustFuture, status\)\n\t\}\)\n\treturn liftFunc\(ffiValue\), err\n\}/\tvar status C.RustCallStatus\n\tffiValue := completeFunc(rustFuture, &status)\n\terr := checkCallStatus(errConverter, status)\n\tif status.code != 0 {\n\t\tvar zero T\n\t\treturn zero, err\n\t}\n\treturn liftFunc(ffiValue), err\n}/' \
            "$out/zerofs_ffi/zerofs_ffi.go"
        grep -qF 'completeFunc(rustFuture, &status)' "$out/zerofs_ffi/zerofs_ffi.go" || {
            echo "generate.sh: async error-path patch did not apply; the upstream generator changed" >&2
            exit 1
        }
        # Into the generated package dir so the facade shares its package.
        cp "$here/go/facade.go" "$out/zerofs_ffi/facade.go"
        ;;
    *)
        echo "unknown language: $lang" >&2
        exit 2
        ;;
esac

echo "generated $lang bindings into $out"
