# syntax=docker/dockerfile:1
# The braidpipe daemon. Build context is the repository root.

FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# The cache mounts keep the registry and incremental build state between
# builds; the binary must be copied out because the mount vanishes afterwards.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p braidpipe \
    && cp target/release/braidpipe /usr/local/bin/braidpipe

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/braidpipe /usr/local/bin/braidpipe
ENTRYPOINT ["braidpipe"]
