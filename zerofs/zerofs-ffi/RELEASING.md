# Releasing the ZeroFS client family

This covers the **programmatic client** and its bindings, _not_ the ZeroFS
server. The server ships from the `v*` tags (`docker.yml`, `csi-docker.yml`,
`release-pgo.yml`); the client family ships from a dedicated `client-v*` tag so
the two never collide.

## What ships, and under one version

A single "client-family" version covers everything in this family:

| Artifact                                | Registry        | Version source                            |
| --------------------------------------- | --------------- | ----------------------------------------- |
| `zerofs-client` (Rust crate)            | crates.io       | client-family version                     |
| `zerofs-ffi` (cdylib)                   | _not published_ | client-family version (`publish = false`) |
| `zerofs-client` (Python wheel + sdist)  | PyPI            | client-family version                     |
| `zerofs-client` (+ `@zerofs/ffi-*`) npm | npm             | client-family version                     |
| Go module (subdir tag)                  | git tag         | client-family version                     |

The **single source of truth** for that version is the `version` field of
`zerofs/zerofs-ffi/Cargo.toml`. Everything else derives from it:

- `zerofs/zerofs-ffi/bindings/generate.sh version` prints it (dependency-free).
- Raw one-liner used across CI:
  `grep -m1 '^version = ' zerofs/zerofs-ffi/Cargo.toml | sed -E 's/.*"(.*)".*/\1/'`
- maturin reads it from `zerofs-ffi/Cargo.toml` for the wheel; the npm
  `stamp.mjs` stamps it into every `package.json`.

`ninep-proto` and `ninep-client` are a separate concern: they are
**independently versioned** on their own pre-1.0 line (their own
`Cargo.toml` `version`, currently `0.1.0`), distinct from both the server's
workspace version and the client-family version. They are published as part of
the crates.io chain only because `zerofs-client` depends on them; bump their
version by hand when the transport itself changes.

## One-command release flow

1. **Bump the version** (sets `zerofs-client` + `zerofs-ffi` and pins
   `zerofs-ffi`'s `zerofs-client` dependency to `=X.Y.Z`; idempotent,
   semver-validated):

   ```
   zerofs/scripts/release-client.sh X.Y.Z
   ```

2. **Commit** the bump:

   ```
   git add -A && git commit -m "Release client family X.Y.Z"
   ```

3. **Tag and push**: the tag drives the whole pipeline:

   ```
   git tag client-vX.Y.Z
   git push origin main client-vX.Y.Z
   ```

The push of `client-vX.Y.Z` triggers `.github/workflows/publish-client.yml`. Its
first job, `verify-version`, strips `client-v` from the tag and asserts it equals
`zerofs-ffi/Cargo.toml::version`, **failing the whole run on drift** before
anything is published. Every downstream job reads the agreed version from that
job's `version` output, so the family cannot ship mismatched versions.

`workflow_dispatch` (with a `version` input) is available for a dry run of the
gate logic; the actual publish/commit/tag-push steps are guarded by
`startsWith(github.ref, 'refs/tags/client-v')`, so a dispatch run never uploads
or pushes.

## The platform matrix

The PyPI and npm bindings each bundle a **platform-specific** cdylib, so those
registries need per-platform builds (the Go binding ships no prebuilt library;
see below). The default matrix is:

| Target          | Runner            | cdylib                |
| --------------- | ----------------- | --------------------- |
| linux x86_64    | `ubuntu-latest`   | `libzerofs_ffi.so`    |
| linux aarch64   | `ubuntu-24.04-arm` | `libzerofs_ffi.so`   |
| macOS x86_64    | `macos-13`        | `libzerofs_ffi.dylib` |
| macOS arm64     | `macos-14`        | `libzerofs_ffi.dylib` |
| Windows x86_64  | `windows-latest`  | `zerofs_ffi.dll`      |

For npm and Go, `generate.sh` generates bindings **from the debug cdylib** (the
release build strips the uniffi metadata the generators read) and the package
**bundles the release cdylib**. The Python wheel instead builds once with maturin
under the non-stripped `wheel` profile (same constraint: the metadata must
survive generation). `generate.sh` auto-detects `.so`/`.dylib`.

## What each registry job does

- **`publish-crates`**: publishes `ninep-proto -> ninep-client -> zerofs-client`
  in dependency order. `cargo publish` (>=1.66) waits for each new version to
  land in the registry index before returning, so the next crate's
  path-with-version dep resolves against the registry without a manual poll.
  `zerofs-ffi` is never published. Auth: `CARGO_REGISTRY_TOKEN`.
- **`publish-pypi`**: per-platform matrix building wheels with maturin
  (`PyO3/maturin-action`, uniffi mode) under the non-stripped `wheel` profile,
  using the external `uniffi-bindgen`: gnu + musl on linux x86_64/aarch64, plus
  macOS and Windows. The linux-x86_64 leg also builds the sdist. Auth: **GitHub
  OIDC Trusted Publishing** (no token).
- **`publish-npm`**: per-platform matrix building five `@zerofs/ffi-<target>`
  packages (each one cdylib) plus a thin main `zerofs-client` package (facade + generated
  JS, no cdylib) wired via `optionalDependencies`. The loader resolves the matching
  platform package at runtime. The main package is published last, after the leg
  polls npm until all five platform packages are live. Auth: `NPM_TOKEN`.
- **`publish-go`**: see below.

## Go: build from source

The Go module is an in-repo subdir module
(`github.com/Barre/ZeroFS/zerofs/zerofs-ffi/bindings/go`), released via Go's
subdir-tag convention `zerofs/zerofs-ffi/bindings/go/vX.Y.Z`.

`publish-go` regenerates the cgo binding (`zerofs_ffi.go` + `zerofs_ffi.h`),
commits it onto the default branch, and pushes the subdir tag from that commit.
It uploads nothing: the Go binding ships **no prebuilt native library**. Shipping
per-platform `.so`/`.dylib`/`.dll` files from a Rust repo is unacceptable, and
comparable cgo bindings (SlateDB's, for one) take the same stance. The generated
source is committed so `go get` needs no uniffi generator, but consumers build
the `zerofs-ffi` cdylib from source themselves and point cgo at it
(`CGO_CFLAGS`/`CGO_LDFLAGS` + `LD_LIBRARY_PATH`; see `bindings/go/README.md`).
This keeps the registry side to a bare git tag: no GitHub Release, no assets.

Because `publish-go` pushes the release commit + tag, the release flow assumes
the client family ships from `main`, and `main` branch protection must permit
pushes from the `github-actions[bot]` actor (or supply a PAT / a bypass).

## Maintainer setup (one-time, before the first tag)

The pipeline cannot self-provision accounts or secrets. Before tagging the first
real release:

### crates.io

- Add repository secret **`CARGO_REGISTRY_TOKEN`** (a token with publish scope).
- The three crate names (`ninep-proto`, `ninep-client`, `zerofs-client`) are
  **not yet on crates.io**. The first run claims them; the token owner must have
  the right to claim those names. Publishes are permanent (only yankable), so the
  job compiles each crate before upload (no `--no-verify`).

### PyPI

- Register a **Trusted Publisher** on the `zerofs-client` PyPI project pointing at
  owner `Barre` / repo `ZeroFS` / workflow `publish-client.yml`. Use a *pending
  publisher* for the very first release if the project does not exist yet.
- Confirm the **`zerofs-client`** distribution name is available/owned on PyPI. If a
  different name is needed, change `name` in `bindings/python/pyproject.toml`.
- No token is used (OIDC); the job has `permissions: id-token: write`. Add an
  `environment:` to the job if you want a protected GitHub environment gating the
  OIDC mint.

### npm

- Add repository secret **`NPM_TOKEN`** (an automation token).
- Own the unscoped **`zerofs-client`** package name, **and** create the **`@zerofs`**
  org/scope (for the five `@zerofs/ffi-*` packages) configured to allow public
  publishing. If `zerofs-client` is taken, switch the main package name to
  `@zerofs/client` in `bindings/typescript/packaging/package.json.tmpl`.

### GitHub (Go)

- Ensure `main` branch protection allows the release commit + subdir-tag push
  from `github-actions[bot]` (the job uses `contents: write` and
  `github.token`).

## Platform scope decisions to confirm

- **Linux libc:** gnu/glibc only (manylinux_2_28 floor, ~RHEL8/Ubuntu 18.10).
  musl/Alpine is out of scope; the npm loader detects musl and throws a clear
  "not installed" error rather than mis-loading a glibc lib. Adding
  `@zerofs/ffi-linux-{x64,arm64}-musl` later is a mechanical matrix addition.
- **macOS floors:** 13.0 (x86_64) / 14.0 (arm64), matching the runner images. No
  universal2 wheel; two separate macOS wheels.
- **aarch64 wheels** build under QEMU (slow but fine for a tagged cadence).
