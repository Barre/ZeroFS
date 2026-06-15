#!/usr/bin/env bash
# Assemble npm packages for the ZeroFS Node binding from generated bindings and
# per-platform prebuilt cdylibs.
#
# Two modes (the CI matrix uses both):
#
#   assemble.sh platform <target> <cdylib-path> <out-dir>
#       Build ONE per-platform package (@zerofs/ffi-<target>) carrying exactly
#       one cdylib. Run once per matrix leg; upload <out-dir> as an artifact.
#
#   assemble.sh main <generated-dir> <out-dir>
#       Build the thin main `zerofs-client` package from generated bindings (produced
#       by `generate.sh typescript`). Run once on the publish host. Drops the
#       generator's auto-loading index.js and the bundled .so, swaps in the
#       optionalDependency loader.
#
# The version is stamped from zerofs-ffi/Cargo.toml via generate.sh's `version`
# subcommand (the single source of truth), so every package.json (main and all
# five platform packages) carries the identical client-family version.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"          # .../typescript/packaging
ts_dir="$(cd "$here/.." && pwd)"                              # .../typescript
gen="$ts_dir/../generate.sh"

version="$("$gen" version)"
[ -n "$version" ] || { echo "assemble.sh: empty version from generate.sh" >&2; exit 1; }

# target -> "pkgname os cpu libc cdylib_filename"
# os/cpu use Node's process.platform/process.arch identifiers. The `libc` token
# here is the value of npm's package.json `libc` FIELD, which npm matches
# against `process.report...glibcVersionRuntime`-derived detection: that value
# is `glibc` (NOT `gnu`) or `musl`; `-` means omit the field (non-Linux). The
# package NAME keeps the `-gnu` suffix (the Rust/uniffi target convention);
# only the field value differs. Targets mirror generate.sh's
# defaultBundledTarget() naming. We ship gnu/glibc builds (the manylinux
# default); musl is intentionally out of scope for the initial matrix.
target_row() {
  case "$1" in
    linux-x64-gnu)    echo "@zerofs/ffi-linux-x64-gnu linux x64 glibc libzerofs_ffi.so" ;;
    linux-arm64-gnu)  echo "@zerofs/ffi-linux-arm64-gnu linux arm64 glibc libzerofs_ffi.so" ;;
    darwin-x64)       echo "@zerofs/ffi-darwin-x64 darwin x64 - libzerofs_ffi.dylib" ;;
    darwin-arm64)     echo "@zerofs/ffi-darwin-arm64 darwin arm64 - libzerofs_ffi.dylib" ;;
    win32-x64)        echo "@zerofs/ffi-win32-x64 win32 x64 - zerofs_ffi.dll" ;;
    *) echo "assemble.sh: unknown target '$1'" >&2; return 1 ;;
  esac
}

cmd="${1:?usage: assemble.sh <platform|main> ...}"

case "$cmd" in
  platform)
    target="${2:?usage: assemble.sh platform <target> <cdylib-path> <out-dir>}"
    cdylib="${3:?usage: assemble.sh platform <target> <cdylib-path> <out-dir>}"
    out="${4:?usage: assemble.sh platform <target> <cdylib-path> <out-dir>}"
    read -r pkgname os cpu libc libfile < <(target_row "$target")
    [ -f "$cdylib" ] || { echo "assemble.sh: cdylib not found: $cdylib" >&2; exit 1; }

    rm -rf "$out"; mkdir -p "$out"
    cp "$cdylib" "$out/$libfile"
    # index.js: substitute the platform's cdylib filename.
    sed "s/__LIBFILE__/$libfile/g" "$here/platform-package/index.js" > "$out/index.js"
    cp "$here/platform-package/index.d.ts" "$out/index.d.ts"

    # JSON assembled by stamp.mjs (os/cpu/libc set structurally; libc only on
    # Linux). Node is always present in the npm CI job.
    node "$here/stamp.mjs" platform \
      "$here/platform-package/package.json.tmpl" "$out/package.json" \
      "$version" "$pkgname" "$target" "$libfile" "$os" "$cpu" "$libc"

    cat > "$out/README.md" <<EOF
# $pkgname

Prebuilt ZeroFS native library for \`$target\`. Installed automatically as an
optional dependency of the [\`zerofs-client\`](https://www.npmjs.com/package/zerofs-client)
package. You should not depend on this package directly.
EOF
    echo "assembled platform package $pkgname@$version -> $out"
    ;;

  main)
    generated="${2:?usage: assemble.sh main <generated-dir> <out-dir>}"
    out="${3:?usage: assemble.sh main <generated-dir> <out-dir>}"
    [ -d "$generated/runtime" ] || { echo "assemble.sh: $generated does not look like a generated TS dir" >&2; exit 1; }

    rm -rf "$out"; mkdir -p "$out/runtime"
    # Generated module surface + runtime (NO cdylib, NO generator index.js).
    cp "$generated"/zerofs_ffi.js "$generated"/zerofs_ffi.d.ts \
       "$generated"/zerofs_ffi-ffi.js "$generated"/zerofs_ffi-ffi.d.ts "$out/"
    cp "$generated"/runtime/* "$out/runtime/"

    # Our loader replaces the generator's auto-loading index.js.
    cp "$here/index.js" "$out/index.js"
    cp "$here/index.d.ts" "$out/index.d.ts"

    # The facade ships as compiled .js + .d.ts (built by the workflow's tsc
    # step into the generated dir alongside index.js).
    cp "$generated"/zerofs.js "$out/zerofs.js"
    cp "$generated"/zerofs.d.ts "$out/zerofs.d.ts"

    node "$here/stamp.mjs" main \
      "$here/package.json.tmpl" "$out/package.json" "$version"

    if [ -f "$ts_dir/README.md" ]; then
      cp "$ts_dir/README.md" "$out/README.md"
    else
      cat > "$out/README.md" <<EOF
# zerofs-client

Programmatic client for [ZeroFS](https://github.com/Barre/ZeroFS) from Node.

\`\`\`js
import { Client } from "zerofs-client";

await using fs = await Client.connect("unix:/run/zerofs/9p.sock");
await fs.write("/hello.txt", new TextEncoder().encode("hi"));
\`\`\`

The prebuilt native library is delivered per-platform via the
\`@zerofs/ffi-*\` optional dependencies; the right one is selected at install
and load time automatically.
EOF
    fi
    echo "assembled main package zerofs-client@$version -> $out"
    ;;

  *)
    echo "assemble.sh: unknown command '$cmd'" >&2
    exit 2
    ;;
esac
