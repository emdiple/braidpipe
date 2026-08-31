# syntax=docker/dockerfile:1
# The braidpipe daemon. Build context is the repository root.
#
# The two bases are swappable so the NVIDIA path can build on nvidia/cuda; the
# gpu overlay sets both (see docker-compose.gpu.yml). They move as a pair: the
# binary is dynamically linked, so the builder's glibc must be no newer than the
# runtime's. trixie is 2.41, bookworm 2.36, Ubuntu 24.04 2.39 -- which is why the
# cuda variant drops the builder to bookworm.
ARG BUILDER_BASE=rust:1-trixie
ARG RUNTIME_BASE=debian:trixie-slim

FROM ${BUILDER_BASE} AS builder
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

# trixie ships GStreamer 1.26: bookworm's 1.22 decodebin3 cannot drive the
# two parse-time branches (video + --audio tap) and dies with `not-linked`.
# Ubuntu 24.04 (the cuda variant) is 1.24, past that bug but not yet 1.26.
FROM ${RUNTIME_BASE}
# va-driver-all supplies the VA-API backends (Intel media / Mesa for AMD) so
# vah264enc/dec light up when /dev/dri is passed in. The NVIDIA path needs no
# libraries here: gstreamer1.0-plugins-bad already carries libgstnvcodec.so on
# both bases, and it dlopens the driver's libcuda/libnvidia-encode/libnvcuvid,
# which only the NVIDIA container toolkit can inject at run time -- no image can
# ship them (see docker-compose.gpu.yml).
#
# The cuda base ships NVIDIA's own apt repo, and developer.download.nvidia.com
# geoblocks some regions with a 403 that fails the whole update. Nothing below
# comes from that repo, so drop it; a no-op on the debian base.
RUN rm -f /etc/apt/sources.list.d/cuda*.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav \
        va-driver-all ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/braidpipe /usr/local/bin/braidpipe
# NVIDIA container runtime hints, same as the official CUDA images: when the
# container is granted a GPU (--gpus all / the compose gpu overlay), these tell
# the toolkit to inject the driver's encode/decode libs and nvidia-smi. Inert
# everywhere else -- plain runc ignores them.
ENV NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,video,utility
ENTRYPOINT ["braidpipe"]
