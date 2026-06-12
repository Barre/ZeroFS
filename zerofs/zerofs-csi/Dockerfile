# Production zerofs-csi image, built by .github/workflows/csi-docker.yml.
#
# Layers on the published zerofs image so the node plugin's `zerofs mount`
# children run the same PGO binary as a standalone deployment; the build
# context only carries the cross-compiled zerofs-csi binaries
# (zerofs-csi-linux-<arch>, see the workflow).
ARG ZEROFS_IMAGE=ghcr.io/barre/zerofs:latest
FROM ${ZEROFS_IMAGE}

ARG TARGETPLATFORM

# Root, not the base image's uid-1001 user: the node plugin performs FUSE
# mounts under /var/lib/kubelet inside a privileged container.
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 \
    && rm -rf /var/lib/apt/lists/*

COPY zerofs-csi-linux-* /tmp/

RUN case "${TARGETPLATFORM}" in \
        "linux/amd64") mv /tmp/zerofs-csi-linux-amd64 /usr/local/bin/zerofs-csi ;; \
        "linux/arm64") mv /tmp/zerofs-csi-linux-arm64 /usr/local/bin/zerofs-csi ;; \
        *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac \
    && rm -f /tmp/zerofs-csi-linux-* \
    && chmod +x /usr/local/bin/zerofs-csi

ENTRYPOINT ["zerofs-csi"]
