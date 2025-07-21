FROM rust:1.88-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/zerofs

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/zerofs/target/release/zerofs /usr/local/bin/zerofs

RUN useradd -m -u 1001 zerofs
USER zerofs

ENV ZEROFS_NFS_HOST=0.0.0.0
ENV ZEROFS_NBD_HOST=0.0.0.0

EXPOSE 2049

ENTRYPOINT ["zerofs"]