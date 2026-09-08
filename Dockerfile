# syntax=docker/dockerfile:1
# The braidpipe daemon. Build context is the repository root.
#
# This is the GPU image as well: nothing here is CPU-only. See the runtime
# stage below and docker-compose.gpu.yml for why no cuda base is needed.
FROM rust:1-trixie AS builder
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
# The builder's glibc must be no newer than this stage's -- both are trixie
# (2.41), so they move together.
FROM debian:trixie-slim
# va-driver-all supplies the VA-API backends (Intel media / Mesa for AMD) so
# vah264enc/dec light up when /dev/dri is passed in. The NVIDIA path needs no
# libraries here: gstreamer1.0-plugins-bad already carries libgstnvcodec.so on
# this base, and it dlopens the driver's libcuda/libnvidia-encode/libnvcuvid,
# which only the NVIDIA container toolkit can inject at run time -- no image can
# ship them, on any base (see docker-compose.gpu.yml). That is why this image
# needs no cuda base to drive NVENC/NVDEC.
RUN apt-get update && apt-get install -y --no-install-recommends \
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
