# CI/e2e images built from the workspace `ci` profile binaries (the
# production zerofs image is the PGO build in the repository root Dockerfile).
# The build context must contain the `zerofs` and `zerofs-csi` binaries.
#
#   docker build --target gateway -t zerofs:e2e <ctx>
#   docker build --target csi -t zerofs-csi:e2e <ctx>
FROM debian:trixie-slim AS base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fuse3 \
    && rm -rf /var/lib/apt/lists/*

FROM base AS gateway
COPY zerofs /usr/local/bin/zerofs
ENTRYPOINT ["zerofs"]

# The node plugin spawns `zerofs mount`, so the csi image carries both
# binaries plus fusermount3 (from fuse3 in the base).
FROM base AS csi
COPY zerofs /usr/local/bin/zerofs
COPY zerofs-csi /usr/local/bin/zerofs-csi
ENTRYPOINT ["zerofs-csi"]
