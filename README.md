<h1 align="center">
  <img src="assets/braidpipe-wordmark.png" alt="braidpipe" width="420">
</h1>

<p align="center"><strong>Never-dark AI video middleware.</strong></p>

<p align="center">
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-FF6D0E"></a>
  <img alt="Rust 1.85+" src="https://img.shields.io/badge/rust-1.85%2B-FF6D0E?logo=rust&logoColor=white">
  <img alt="Python 3.10+" src="https://img.shields.io/badge/python-3.10%2B-FF6D0E?logo=python&logoColor=white">
  <img alt="GStreamer 1.20+" src="https://img.shields.io/badge/gstreamer-1.20%2B-FF6D0E">
  <img alt="Platform: Linux and macOS" src="https://img.shields.io/badge/platform-linux%20%7C%20macOS-FF6D0E">
</p>

---

braidpipe ingests a live video stream, hands raw frames to a Python process for AI/CV work over shared memory, re-encodes the result, and streams it out — and if that Python process crashes, stalls, or falls behind, the output stream keeps running on untouched frames instead of going dark.

The Rust daemon owns the media path. Python only ever sees pixels in a shared-memory slot and a small JSON message telling it which slot to look at. That separation is the whole point: an exception in a model's inference code must never be able to take the broadcast off air.

```text
                     ┌─────────────────────────────────┐
   SRT / UDP / RTP   │            braidpipe            │
 NDI / camera / file │                                 │
        ──────────►  │  decode ──┬── queue ─────────┐  │
                     │           │                  │  │
                     │           │            input-selector ──► encode ──►  RTMP / SRT
                     │           │                  │  │                     UDP / display
                     │           └── appsink ──┐  appsrc
                     │                         │     ▲  │
                     └─────────────────────────┼─────┼──┘
                                    shared memory + Unix datagrams
                                               ▼     │
                                        ┌────────────┴───┐
                                        │  Python worker │
                                        │  (numpy/cv2/…) │
                                        └────────────────┘
```

The `input-selector` decides, frame by frame, whether the viewer sees the AI branch or the raw passthrough branch. A watchdog flips it based on whether Python is actually keeping up.

## Table of contents

- [How the failover works](#how-the-failover-works)
- [Getting started](#getting-started)
  - [Requirements](#requirements)
  - [Install](#install)
  - [Quick start](#quick-start)
- [Streaming configuration](#streaming-configuration)
  - [Real-world pipelines](#real-world-pipelines)
  - [Output presets](#output-presets)
  - [GPU acceleration](#gpu-acceleration)
  - [Audio passthrough](#audio-passthrough)
  - [Command-line reference](#command-line-reference)
- [AI workers](#ai-workers)
  - [Writing a Python worker](#writing-a-python-worker)
  - [Bundled examples](#bundled-examples)
  - [Writing a worker in another language](#writing-a-worker-in-another-language)
  - [External worker mode](#external-worker-mode)
  - [Workers on another machine (tcp-raw)](#workers-on-another-machine-tcp-raw)
  - [The IPC contract](#the-ipc-contract)
- [Operations](#operations)
  - [Testing failover](#testing-failover)
  - [Measuring latency](#measuring-latency)
  - [Monitoring](#monitoring)
  - [Troubleshooting](#troubleshooting)
- [Project reference](#project-reference)
  - [Project layout](#project-layout)
  - [Development](#development)
  - [Known limitations](#known-limitations)
  - [Contributing](#contributing)
  - [License](#license)

## How the failover works

Availability is enforced at two independent levels, so a single missed frame is handled differently from a dead worker.

**Per frame (the relay).** For every frame tapped off the pipeline, the relay writes it into a shared-memory slot, signals Python, and waits for an acknowledgement. The budget is 1.5 frame periods — 50 ms at 30 fps. If the ack doesn't arrive in time, or shared memory is full, or Python reports a failure, the relay pushes the **original, unmodified frame** downstream and reclaims the slot. Timing jitter therefore costs you an un-overlaid frame, not a gap in the stream.

**Over time (the watchdog).** The relay reports each success and failure to a health counter. Thirty consecutive failures — about one second at 30 fps — mark the worker unhealthy, and the watchdog switches the `input-selector` to the passthrough branch. One successful roundtrip resets the counter and the AI branch is selected again. The AI branch starts out unselected and has to earn its place with a first successful frame, so a worker that never starts correctly can't take frames with it.

Both branch queues are `leaky=downstream`, which matters more than it looks: without leaky queues, buffers piling up on the *inactive* selector pad eventually block the `tee` and stall the entire pipeline, including the branch that was working fine.

## Getting started

Install the toolchain, build the workspace, and see the failover working on screen — nothing here needs a real video source.

### Requirements

| Component | Version | Notes |
| --- | --- | --- |
| Rust | 1.85+ | Edition 2024 |
| Python | 3.10+ | 3.13+ recommended, see note below |
| GStreamer | 1.20+ | Needed for `appsrc leaky-type`; developed against 1.28 |
| OS | Linux or macOS | POSIX shared memory + Unix datagram sockets |

GStreamer plugins depend on what you actually stream: `srt` for SRT, `x264`/`libav` for H.264, `rtmp` for RTMP output, `avfvideosrc` (macOS, in plugins-bad) or `v4l2src` (Linux, in plugins-good) for cameras, and a third-party plugin for NDI.

On Python 3.13+, `SharedMemory(track=False)` keeps Python's resource tracker from unlinking the Rust-owned segment when the worker exits. On older versions the bundled worker falls back automatically, but you may see a resource-tracker warning at shutdown and should restart the daemon rather than reusing the segment.

### Install

Debian / Ubuntu:

```bash
sudo apt install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
                 gstreamer1.0-plugins-good gstreamer1.0-plugins-bad \
                 gstreamer1.0-plugins-ugly gstreamer1.0-libav
```

macOS (Homebrew):

```bash
brew install gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugins-ugly gst-libav
```

Then build the workspace and set up the Python side:

```bash
git clone <your-fork-url> braidpipe
cd braidpipe
cargo build --release

python3 -m venv .venv
.venv/bin/pip install numpy opencv-python
```

The daemon prefers `.venv/bin/python3` when that path exists and otherwise falls back to `python3` on `PATH`, so a virtualenv at the repository root needs no extra configuration.

### Quick start

Run from the repository root so the default worker path resolves:

```bash
cargo run -p braidpipe --release
```

That builds a 1280×720 test pattern, runs it through the bundled worker (which draws a frame counter onto each frame), and displays the result with `autovideosink`. You should see the overlay text updating on a moving ball.

To confirm the media path alone, with no Python involved:

```bash
cargo run -p braidpipe --release -- --passthrough-only
```

## Streaming configuration

Everything between the input URI and the output URL: real sources and sinks, the encoder presets, GPU offload, audio, and the full flag reference.

### Real-world pipelines

**SRT in, RTMP out** — the common broadcast shape:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --sink 'videoconvert ! video/x-raw,format=I420 \
          ! x264enc tune=zerolatency bitrate=4000 speed-preset=veryfast key-int-max=60 \
          ! h264parse config-interval=-1 ! flvmux streamable=true \
          ! rtmp2sink sync=false location=rtmp://localhost/live/stream'
```

`sync=false` is not incidental — it is worth about 48 ms, for the reasons in [Measuring latency](#measuring-latency).

**NDI in**, with a plugin that registers the `ndi://` scheme:

```bash
cargo run -p braidpipe --release -- --uri 'ndi://Studio%20Camera' --sink 'videoconvert ! autovideosink'
```

**Raw RTP in**, which needs explicit caps and a depayloader — that's what `--source` is for:

```bash
cargo run -p braidpipe --release -- \
  --source 'udpsrc port=5000 caps="application/x-rtp,media=video,encoding-name=H264,payload=96" \
            ! rtph264depay ! h264parse ! decodebin3 ! videoconvert ! videoscale' \
  --sink 'videoconvert ! autovideosink'
```

**A webcam**, on macOS (`avfvideosrc`) or Linux (`v4l2src device=/dev/video0`):

```bash
cargo run -p braidpipe --release -- \
  --source 'avfvideosrc device-index=0 ! videoconvert ! videoscale ! video/x-raw,width=1280,height=720,framerate=30/1 ! videoconvert' \
  --sink 'videoconvert ! autovideosink'
```

The scaling matters: `--width`/`--height` fix the shared-memory slot geometry, and a camera negotiates whatever resolution it likes unless you tell it otherwise. List your devices with `gst-device-monitor-1.0 Video/Source` — the index is not always the one you expect, and on macOS index 0 is often an iPhone offering itself as a Continuity Camera. A camera that is present but not actually streaming sits in PLAYING and delivers nothing, which looks exactly like a braidpipe stall; check it in isolation first:

```bash
gst-launch-1.0 avfvideosrc device-index=0 num-buffers=10 ! fakesink
```

That should reach end-of-stream in a couple of seconds. If it hangs, the problem is the camera, not this project.

**1080p60:**

```bash
cargo run -p braidpipe --release -- --width 1920 --height 1080 --fps 60 \
  --source 'videotestsrc is-live=true pattern=ball ! video/x-raw,width=1920,height=1080,framerate=60/1 ! videoconvert' \
  --sink 'videoconvert ! autovideosink'
```

`--uri` and `--source` are mutually exclusive. Use `--uri` when GStreamer can figure out the source on its own (it picks `srtsrc ! decodebin3` for `srt://` and `uridecodebin3` for everything else); use `--source` when you need to spell out elements yourself.

### Output presets

Writing the sink by hand, as above, gives full control — but most deployments want one of a few well-understood points on the latency/bandwidth curve. `--output` plus `--preset` builds the whole encoder + mux + sink chain for you, the way ffmpeg's `-preset` expands into a bag of x264 options:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --preset lowlatency --output rtmp://localhost/live/stream
```

`--output` understands `rtmp://`, `srt://` and `udp://host:port`, and picks the right mux for each (FLV for RTMP, MPEG-TS for SRT/UDP). The daemon logs the sink it built at startup, so you can copy it out and use it as a `--sink` starting point.

| Preset | Encoder settings | GOP | VBV | Sink sync | SRT latency | Intent |
| --- | --- | --- | --- | --- | --- | --- |
| `zerolatency` | ultrafast + zerolatency, 6000 kbps | 1 s | 100 ms | `false` | 50 ms | Every latency lever pulled; bandwidth pays for it |
| `lowlatency` (default) | veryfast + zerolatency, 4500 kbps | 2 s | 200 ms | `false` | 125 ms | The measured sweet spot — same ~40 ms p50 as the tuned harness sink |
| `balanced` | medium + zerolatency, 3000 kbps | 2 s | 500 ms | `false` | 250 ms | Better compression, still no B-frame delay |
| `bandwidth` | slow, B-frames + lookahead, 1800 kbps | 4 s | 1000 ms | `true` | 500 ms | Minimum bits for the quality; adds several frames of encoder delay by design |

The VBV column is what makes the bitrate column mean something on the wire: it bounds how far above the target the encoder may burst, and how much encoded data a receiver has to be ready to buffer — so it is simultaneously a bandwidth cap and hidden latency. x264's own default (600 ms) would allow bursts more than half a second long.

Bitrates assume 720p30 — scale them for other formats. A preset only decides defaults; every parameter yields to an environment variable, so you can start from a profile and turn one knob:

| Variable | Overrides | Values |
| --- | --- | --- |
| `BRAIDPIPE_ENCODER` | encoder | `auto` (default — best GPU encoder, else x264), `x264`, `vtenc`, `nvenc`, `va`, `vaapi`, `qsv`, `mf`, `amf` — see [GPU acceleration](#gpu-acceleration) |
| `BRAIDPIPE_HW` | GPU decode/encode detection | `off` forces software both ways |
| `BRAIDPIPE_BITRATE_KBPS` | target bitrate | kbps |
| `BRAIDPIPE_SPEED_PRESET` | x264 speed preset | `ultrafast` … `placebo` |
| `BRAIDPIPE_ZEROLATENCY` | zero-latency tuning | `1`/`0` — x264 `tune=zerolatency`, vtenc `realtime` |
| `BRAIDPIPE_GOP_SECONDS` | keyframe interval | seconds |
| `BRAIDPIPE_VBV_BUF_MS` | x264 VBV buffer (burst bound) | milliseconds |
| `BRAIDPIPE_SINK_SYNC` | sink clock sync | `1`/`0` — see [Measuring latency](#measuring-latency) for why `0` is worth ~48 ms |
| `BRAIDPIPE_SRT_LATENCY_MS` | `srtsink` latency budget | milliseconds |

```bash
# lowlatency profile, but cap the bandwidth
BRAIDPIPE_BITRATE_KBPS=2500 cargo run -p braidpipe --release -- \
  --preset lowlatency --output srt://127.0.0.1:8888
```

Verified end-to-end with the [latency harness](#measuring-latency): `--preset lowlatency --output rtmp://…` measured 39.8 ms p50 / 45.6 ms p99 worker→receiver at 720p30, identical to the hand-tuned sink.

#### Measured bandwidth

Bitrate targets are promises until you look at the wire, so `scripts/preset-bandwidth.sh` runs each preset through the full daemon + worker path, captures the RTMP output, and reports what actually left the encoder. Content is a moving scene blended with 30% white noise — a stand-in for camera footage, hard enough to push rate control against its cap. 20-second runs at 720p30, startup excluded:

| Preset | Target | Mean on wire | Worst 1 s | Worst 250 ms burst | Output fps |
| --- | --- | --- | --- | --- | --- |
| `zerolatency` | 6000 kbps | 5123 | 5332 | 6481 | 30.0 |
| `lowlatency` | 4500 kbps | 4501 | 4525 | 4941 | 30.0 |
| `balanced` | 3000 kbps | 3000 | 3033 | 3291 | 30.0 |
| `bandwidth` | 1800 kbps | 64 | 142 | 439 | 30.0 |

Three things worth reading out of that table:

- **`lowlatency` and `balanced` hold their targets to within 1%,** and no preset's worst 250 ms burst exceeds its target by more than 10% — that is the VBV bound doing its job. Provision the link for the target plus ~10% and it will not be surprised.
- **`zerolatency` runs ~15% under target on hard content.** The 100 ms VBV is tight enough to constrain ABR itself, trading a little quality for the strictest burst bound. That is the correct trade for its use case.
- **`bandwidth` spends almost nothing on this content, and that is by design, not a bug.** Without `tune=zerolatency`, x264's mbtree lookahead rates every block by how much future frames can predict from it — and noise predicts nothing, so mbtree declines to encode it. The synthetic content is 30% noise; real footage has structure everywhere and will sit far closer to target. Either way the target is a hard ceiling, never a floor: this preset buys quality-per-bit, not constant bandwidth. (On pure noise — `BRAIDPIPE_BW_PATTERN=snow` — the effect is even starker, while the three zerolatency presets still hold their caps, since zerolatency disables mbtree.)

### GPU acceleration

On a machine with a capable GPU, both halves of the codec work move off the CPU automatically. The mechanism differs per platform because every OS has its own video API:

| Platform | Decode | Encode (H.264, first available wins) |
| --- | --- | --- |
| macOS | VideoToolbox (`vtdec_hw`, `vtdec`) | VideoToolbox (`vtenc_h264`) |
| Linux | NVDEC → VA-API (`va` plugin, then legacy `vaapi`) → QuickSync | `nvh264enc` → `vah264enc` → `qsvh264enc` → `vaapih264enc` |
| Windows | Direct3D 12 → Direct3D 11 → NVDEC → QuickSync | `nvh264enc` → `qsvh264enc` → `amfh264enc` → `mfh264enc` |

**Decoding** needs no pipeline changes at all. `decodebin3` picks decoders by element rank, so at startup the daemon promotes the platform's hardware decoders above the software `avdec_*` family and autoplugging does the rest — for any codec the source carries, on both `--uri` and custom `--source` pipelines that use a decodebin. The `Hardware decoders promoted for autoplugging` log line lists what was found. Frames still land in system memory for the AI branch, which needs the raw pixels anyway: the win is the decode itself, not zero-copy.

Either way, the daemon logs every codec element the pipeline actually ends up using, tagged hardware or software — the encoder at startup, the decoder as soon as the input's caps are known:

```
INFO braidpipe_engine::pipeline: Video encoder in use (hardware) element=vtenc_h264
INFO braidpipe_engine::pipeline: Video decoder in use (hardware) element=vtdec_hw
```

Whether the GPU is then actually doing work shows up in [Monitoring](#monitoring): `braidpipe_gpu_utilization_percent` samples machine-wide GPU load every 5 seconds (via `ioreg` on macOS, `nvidia-smi` or the amdgpu sysfs on Linux), NVIDIA additionally breaks out the dedicated `_encoder_`/`_decoder_` block utilization, and the Grafana dashboard has a GPU row for all of them.

**Encoding** is decided when `--output` builds the sink from a preset: the best hardware encoder present replaces the preset's x264 default, and the `Built sink from preset` log shows which one won. Detection is reliable because the NVIDIA/VA/QSV/AMF/MediaFoundation plugins only register their elements when the device probe succeeds — if `nvh264enc` exists in the registry, there is an NVENC-capable GPU behind it. The preset's parameters map onto each encoder's own vocabulary: bitrate and GOP always, the zero-latency switch per encoder (`realtime`, `low-latency`, `ultra-low-latency` usage), and the VBV burst bound where one is exposed (NVENC `vbv-buffer-size`, VA `cpb-size`).

Two knobs control this, each a CLI flag with an environment-variable twin (the flag wins when both are given):

- `--hw off` (or `BRAIDPIPE_HW=off`) — software everywhere: no decoder promotion, no encoder auto-pick.
- `--encoder <name>` (or `BRAIDPIPE_ENCODER=<name>`) — pin the encoder regardless of detection; `--encoder auto` explicitly re-enables detection. Hardware encoders trade some quality-per-bit for speed, so the `bandwidth` preset's intent is best served by pinning `x264`. Pinning the encoder leaves GPU *decoding* on — use `--hw off` to force software both ways.

A hand-written `--sink` bypasses encoder selection entirely — you name the encoder yourself.

### Audio passthrough

Real sources carry audio, and the output should too. `--audio` routes the source's audio around the AI branch — decoded, re-encoded to AAC, and joined back at the output muxer:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --audio --preset lowlatency --output rtmp://localhost/live/stream
```

**How sync works.** There is no dedicated sync machinery, because none is needed: the relay pushes every video frame back into the pipeline with its **original PTS** — whether the worker processed it or the deadline passed and it went through unchanged — and audio keeps the PTS the source gave it. The muxer pairs the two streams by timestamp, exactly as it would in a plain GStreamer pipeline. That also means failover cannot desynchronize anything: the input-selector switches video branches while audio never stops flowing.

Measured on a live SRT source (video + audio) relayed to RTMP at 720p30: steady-state packet cadence was exactly 33.33 ms for video and 21.33 ms for audio (1024 samples at 48 kHz), with under one video frame of relative drift over a 19 s run — both with the worker healthy and with a worker that missed every deadline.

`--audio` needs to know where audio comes from and where it goes:

- **Source** — with `--uri` this is automatic. With a custom `--source`, name your demuxer or decodebin `decoder` so the audio branch can tap it.
- **Sink** — with `--output` this is automatic (preset muxers are named). With a custom `--sink`, name your muxer `mux`.

| Variable | Overrides | Default |
| --- | --- | --- |
| `BRAIDPIPE_AUDIO_ENCODER` | AAC encoder element | `avenc_aac` (in gst-libav; `fdkaacenc`, `faac` also work) |
| `BRAIDPIPE_AUDIO_BITRATE_KBPS` | audio bitrate | `128` |
| `BRAIDPIPE_AUDIO_BRANCH` | the entire generated branch, verbatim | `decoder. ! queue ! audio/x-raw ! audioconvert ! audioresample ! avenc_aac bitrate=128000 ! aacparse ! queue ! mux.` |

If the source has no audio stream, don't pass `--audio` — the audio branch would wait forever for a pad that never appears and GStreamer fails the pipeline with a delayed-linking error.

### Command-line reference

| Flag | Default | Purpose |
| --- | --- | --- |
| `-i, --source <PIPELINE>` | test pattern | Explicit GStreamer source fragment |
| `--uri <URI>` | — | Input URI decoded by GStreamer (`srt://`, `udp://`, `rtp://`, `ndi://`, `file://`) |
| `-o, --sink <PIPELINE>` | `videoconvert ! autovideosink` | Output fragment appended after the selector |
| `--output <URL>` | — | Publish target (`rtmp://`, `srt://`, `udp://host:port`); builds the sink from `--preset` |
| `--preset <NAME>` | `lowlatency` | Latency/bandwidth profile for `--output`, see [Output presets](#output-presets) |
| `--hw <auto\|off>` | `auto` | GPU mode, see [GPU acceleration](#gpu-acceleration); `off` forces software decode and encode |
| `--encoder <NAME>` | `auto` | Encoder for `--output`: `auto` picks the best hardware encoder, or pin `x264`, `vtenc`, `nvenc`, `va`, `vaapi`, `qsv`, `mf`, `amf` |
| `--audio` | off | Carry source audio to the output muxer, see [Audio passthrough](#audio-passthrough) |
| `-p, --python-script <PATH>` | `python/braidpipe/worker.py` | Worker to launch |
| `--external-worker` | off | Don't spawn or supervise a worker; connect to an externally managed AI process, see [External worker mode](#external-worker-mode) |
| `-f, --fps <N>` | `30` | Frame rate; sets the relay deadline and watchdog tick |
| `--width <N>` / `--height <N>` | `1280` / `720` | Shared-memory slot geometry |
| `--rust-sock <PATH>` | `/tmp/braidpipe_rust.sock` | Where the daemon listens for acks and hellos |
| `--python-sock <PATH>` | `/tmp/braidpipe_python.sock` | Where the worker listens for notifications |
| `--worker-listen <IP:PORT>` | — | Also accept workers from other machines (tcp-raw), see [Workers on another machine](#workers-on-another-machine-tcp-raw) |
| `--passthrough-only` | off | Media path only; no worker, no shared memory |
| `--metrics-port <N>` | `9184` | Prometheus endpoint on 127.0.0.1, see [Monitoring](#monitoring); `0` disables |
| `--metrics-drain-ms <N>` | `2000` | How long to keep serving metrics after a shutdown signal, so the down state gets scraped |

`--width`/`--height` must match the frames your source actually produces after `videoscale`, because they define the slot size that both sides index into.

Set `RUST_LOG=debug` to see per-frame relay activity, including dropped and stale acks.

## AI workers

The other side of the shared memory: the contract a worker implements, the bundled examples, and every way to attach one — spawned by the daemon, started by hand, in a container, or on another machine.

### Writing a Python worker

A worker is a loop over one Unix datagram socket. `attach()` runs the handshake — it says hello to the daemon, which answers with the shared-memory segment's file descriptor — and returns a `SharedMemoryManager` giving you a zero-copy NumPy view of each slot, so mutating the array in place *is* writing to the output frame — there's no separate send step for pixels.

```python
import json, os, socket
from shm import attach

sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
if os.path.exists("/tmp/braidpipe_python.sock"):
    os.remove("/tmp/braidpipe_python.sock")
sock.bind("/tmp/braidpipe_python.sock")

shm = attach(sock, "/tmp/braidpipe_rust.sock")   # handshake; reads geometry from the header

while True:
    packet = json.loads(sock.recvfrom(512)[0])
    if "frame_id" not in packet:
        continue   # a control packet, not a frame
    frame = shm.get_slot_numpy_array(packet["slot_index"])   # (H, W, 3) uint8, RGB

    # ... your inference here; mutate `frame` in place ...

    shm.mark_slot_free(packet["slot_index"])
    try:
        sock.sendto(json.dumps({
            "frame_id": packet["frame_id"],
            "slot_index": packet["slot_index"],
            "processing_time_us": 0,
            "success": True,
        }).encode(), "/tmp/braidpipe_rust.sock")
    except OSError:
        pass   # a full socket buffer is not worth dying over
```

Four rules keep the stream healthy:

1. **Finish inside the budget.** You have 1.5 frame periods. Slower than that and your output is simply not used for that frame — correctness is preserved, but the overlay flickers. For heavy models, drop the input `--fps`, or run inference on every Nth frame and cache the result.
2. **Always free the slot.** The ring has four slots; leaking them starves the relay. Free the slot even on your own error paths.
3. **Always send an ack, and never die sending it.** Report `"success": false` for a failed frame — the relay treats that as a failure and passes the original through, which is exactly right. Wrap the send in `try/except OSError`; a full datagram buffer (`ENOBUFS`) is normal backpressure, not a fatal condition.
4. **Frames are RGB, not BGR.** OpenCV's conventions assume BGR, so the familiar `(0, 0, 255)` "red" renders as blue here. Use `(255, 0, 0)` for red, or convert with `cv2.cvtColor` if you're feeding a model trained on BGR.

Point `--python-script` at your own file. Because Python puts the script's own directory on `sys.path`, a worker living beside `shm.py` can `from shm import attach` directly; from elsewhere, add `python/braidpipe` to `sys.path` or install it as a package.

### Bundled examples

Four workers ship with the project, each self-contained and runnable as-is:

| Worker | Needs | Shows |
| --- | --- | --- |
| [worker.py](python/braidpipe/worker.py) | opencv | The minimum contract: a text overlay and a frame counter |
| [worker_edges.py](python/braidpipe/worker_edges.py) | opencv | A whole-frame pixel transform, and reporting `success: false` instead of dying |
| [worker_detect.py](python/braidpipe/worker_detect.py) | ultralytics, torch | A model too slow to run inline, moved to a thread with cached results |
| [worker_stamp.py](python/braidpipe/worker_stamp.py) | numpy | Instrumentation rather than transform — see [Measuring latency](#measuring-latency) |

```bash
cargo run -p braidpipe --release -- --python-script python/braidpipe/worker_edges.py
cargo run -p braidpipe --release -- --python-script python/braidpipe/worker_detect.py
```

`worker_edges.py` is the one to reach for when testing the plumbing: no model, no network, and it rewrites only the left half of the frame so the boundary between processed and untouched pixels is visible on screen.

`worker_detect.py` runs YOLO and is the more instructive one. A CPU inference pass does not fit in 50 ms, so the socket loop never waits for it — a detector thread takes a copy of every third frame, and every frame is annotated with the most recent boxes available. Boxes lag the picture slightly; no frame misses its deadline. It also caps `torch.set_num_threads` and the inference resolution, because torch left unbounded takes every core and starves the loop that only needs a millisecond of it. Without those caps the worker flaps between branches every few seconds; with them the AI branch stays selected. The first run downloads weights (~6 MB) into the working directory.

### Writing a worker in another language

Nothing in the contract is Python-specific. [crates/braidpipe-ipc/examples/worker.rs](crates/braidpipe-ipc/examples/worker.rs) is a complete worker in Rust — it runs the hello handshake, maps the received fd, reuses the daemon's own `ShmHeader`/`SlotHeader`/packet types so it cannot drift out of sync with them, transforms pixels, frees the slot, and acks.

A worker only ever *attaches* to shared memory, by mapping the fd the daemon hands it. Never call `ShmRingBuffer::create` from one: that makes a second, unrelated segment the daemon will never look at.

Run the daemon in [external worker mode](#external-worker-mode) and start your own worker alongside it:

```bash
# terminal 1 — creates the shared memory segment and streams
cargo run -p braidpipe --release -- --external-worker

# terminal 2 — the AI branch is selected on this worker's first good frame
cargo run -p braidpipe-ipc --release --example worker
```

Any language that can receive a file descriptor over a Unix datagram socket (`recvmsg` with `SCM_RIGHTS`), `mmap` it, and parse JSON can do the same. What it must implement is the [IPC contract](#the-ipc-contract) below, in full — the four rules above apply regardless of language.

### External worker mode

`--external-worker` is for AI processes the daemon should not own: a Docker container, a systemd service, something started by hand. The daemon skips spawning, supervising, and — importantly — terminating: shutting the daemon down leaves the external process running, because its lifecycle belongs to whoever started it.

Everything else is identical to managed mode. The daemon still creates the shared memory segment and binds its socket; the external process implements the same [IPC contract](#the-ipc-contract) that `worker.py` does (all the bundled workers run unchanged either way). Startup order doesn't matter: until a worker acks, the stream runs on passthrough, and the first good frame selects the AI branch — the same machinery that handles failover recovery. If the external process dies, the stream falls back to passthrough and picks up its replacement whenever one appears.

A full example — a live SRT relay with audio, joined later by the YOLO worker started by hand:

```bash
# terminal 1 — pulls SRT from :8890, serves clean passthrough on :8891 immediately
cargo run -p braidpipe --release -- \
  --uri "srt://127.0.0.1:8890?mode=caller" \
  --output "srt://127.0.0.1:8891?mode=listener" \
  --external-worker \
  --audio

# terminal 2 — whenever ready; the output switches to annotated frames on its first ack
.venv/bin/python3 python/braidpipe/worker_detect.py
```

In this mode nothing picks the interpreter for you — managed mode's automatic `.venv/bin/python3` preference belongs to the spawn path, so point at the environment that has the worker's dependencies yourself. The bundled workers (except the minimal `worker.py`) read `BRAIDPIPE_RUST_SOCK` / `BRAIDPIPE_PYTHON_SOCK` if the daemon's socket paths were overridden. Stopping the worker drops the stream back to passthrough within the failure streak (~1 s at 30 fps); relaunching it re-attaches through the same hello handshake, no daemon restart involved.

One metric changes meaning: the daemon can't know whether a process it doesn't own is alive, so in this mode `braidpipe_worker_up` means "delivered a successful AI frame within the last 2 seconds" rather than "my child process is running", and `worker_exits_total` / `worker_last_exit_code` / CPU / RSS are never populated.

For a containerized worker only the socket directory must cross the container boundary — mount it and the fd handshake does the rest, because a file descriptor passed over a Unix socket works across container namespaces with no `/dev/shm` mount or `--ipc=host` required. On macOS, Docker runs inside a VM, so neither sockets nor memory can cross — external mode there means a host process, not a container (or a worker using the tcp-raw transport below, which crosses anything TCP crosses).

### Workers on another machine (tcp-raw)

`--worker-listen IP:PORT` opens the worker negotiation to the network: UDP on that address answers hellos with a config packet, and TCP on the same port carries raw frames both ways. Same-host shm workers keep working alongside it — the transport is chosen per worker by where its hello arrives.

```bash
# machine A — the daemon
cargo run -p braidpipe --release -- --external-worker --worker-listen 0.0.0.0:7300

# machine B — any bundled worker, pointed at the daemon
BRAIDPIPE_DAEMON=192.168.1.10:7300 python3 worker_edges.py
```

The worker's hello (`{"type": "hello", "transports": ["tcp-raw"]}`) is answered with `{"type": "config", "transport": "tcp-raw", "data_port": …, "width": …, "height": …, "channels": …, "format": "rgb"}`. The worker then opens one TCP connection to `data_port`, and every frame in either direction is a fixed 24-byte header plus the raw pixels — [python/braidpipe/remote.py](python/braidpipe/remote.py) wraps this in the same attach-and-loop shape the shm side has:

| Field | Type | Meaning |
| --- | --- | --- |
| `frame_id` | `u64` LE | Matches the result to the frame |
| `time_us` | `u64` LE | Capture timestamp toward the worker; processing time on the way back |
| `payload_len` | `u32` LE | Always `width × height × channels` |
| `slot` | `u8` | Echo back unchanged; names the daemon-side slot |
| `flags` | `u8` + 2 pad | Bit 0 = success, on results |

Sending the result back **is** the ack — there are no separate datagrams. On the daemon side the result lands in the same shm slot a local worker would have written, so the relay, the deadline, and the watchdog treat both transports identically, and failover behaves exactly as in [External worker mode](#external-worker-mode): worker dies → passthrough within the failure streak, reconnect → AI branch on the first good frame. Backpressure is drop, not buffer: a frame that cannot be sent within one deadline tears the connection down rather than queueing stale video.

The frames are uncompressed, which makes bandwidth the deciding constraint — frames cross the wire twice (out and back):

| Geometry | Round trip @ 30 fps | Verdict |
| --- | --- | --- |
| 1280×720×3 (default) | ~1.3 Gbit/s | 2.5 GbE or better; a quiet gigabit link will drop frames |
| 1920×1080×3 | ~3.0 Gbit/s | 10 GbE territory |

Wire time also eats into the 1.5-frame deadline (a 720p frame takes ~24 ms each way on gigabit, ~2 ms on 10 GbE), so this transport wants a fast LAN. For constrained networks, run the worker's machine as an SRT hop with its own braidpipe instead — or wait for a compressed transport. Since tcp-raw is ordinary TCP/UDP, it also crosses boundaries the fd handshake cannot: a Docker container on macOS (publish the port with `-p 7300:7300 -p 7300:7300/udp`) or any VM.

### The IPC contract

**Shared memory** — one *anonymous* segment (a `memfd` on Linux, an unlinked POSIX object elsewhere) holding a 32-byte header followed by `slot_count` slots. Each slot is a 24-byte header plus `width × height × channels` bytes of pixels. The segment has no name anywhere: a worker gets in by sending `{"type": "hello"}` to the daemon's socket (optionally with a `"transports"` list; over UDS the answer is always shm), and the daemon replies with a config datagram (body `{"type": "config", "transport": "shm", …}` describing the ring geometry) carrying the segment's file descriptor as `SCM_RIGHTS` ancillary data. The kernel duplicates the descriptor into the worker, which `fstat`s it for the size and `mmap`s it. Because nothing is ever named, nothing can collide between instances, go stale after a crash, or need permission juggling — the kernel frees the segment when the last descriptor and mapping are gone. A worker may say hello before the daemon is up (retry until answered) or at any point after; replies are sent whenever frames are flowing.

All layouts are explicitly padded on the Rust side and mirrored by `struct` format strings in [python/braidpipe/shm.py](python/braidpipe/shm.py), which assert their own sizes at import — a mismatch fails loudly instead of silently reading garbage.

| Structure | Rust | Python format | Size |
| --- | --- | --- | --- |
| Segment header | `ShmHeader` | `<IIBB2xI16s` | 32 B |
| Slot header | `SlotHeader` | `<B7xQQ` | 24 B |

Slot ownership is a single atomic state byte: `FREE (0) → PROCESSING (2)` claimed by Rust with a compare-and-swap, `→ READY_FOR_AI (1)` once the pixels are written, and back to `FREE` by whichever side finishes — Python after processing, or Rust reclaiming a slot whose roundtrip failed. No locks, no ordering assumptions between processes beyond that byte.

**Control channel** — two Unix *datagram* sockets carrying one JSON object per message. Rust → Python announces a frame:

```json
{"frame_id": 1081, "slot_index": 2, "timestamp_us": 1769506390123456}
```

Python → Rust acknowledges it:

```json
{"frame_id": 1081, "slot_index": 2, "processing_time_us": 4210, "success": true}
```

Datagrams are used deliberately: they're unordered and droppable, which matches a real-time pipeline where a late frame has no value. The relay discards acks whose `frame_id` doesn't match the frame it's currently waiting on.

## Operations

Proving the availability claim and keeping an eye on it: failover drills, latency numbers, the metrics stack, and the usual failure modes.

### Testing failover

The interesting property is what happens when Python dies mid-stream. Start the daemon, wait for `branch=AiProcess`, then kill the worker using the PID from the log line `Python worker active pid=…`:

```bash
kill -9 <worker-pid>
```

Within about a second you should see `Successfully switched video stream branch branch=Passthrough`, the overlay disappear, and the stream continue without a stall, a black frame, or a dropped publisher connection. Measured on a 30 fps test pattern: the switch lands 1.0 s after the kill, and output frames keep arriving at exactly 30 fps across it.

**Killing the worker by pattern is harder than it looks, and most obvious attempts are wrong.**

`pkill -f 'worker.py'` matches the *whole* command line, and the daemon's own command line contains `--python-script python/braidpipe/worker.py` — so it kills the daemon too and proves nothing. Anchoring to the interpreter does not help either: `.venv/bin/python3` is a symlink chain, so the running process reports its resolved interpreter path (`…/Python.framework/Versions/3.14/Resources/Python.app/Contents/MacOS/Python` on Homebrew macOS), and a pattern containing `python3` never matches at all. It fails silently, which is worse than failing loudly.

Target the worker by parentage instead — it is the daemon's only child, on any platform:

```bash
pgrep -P "$(pgrep -x braidpipe)" -l     # check first: one PID, the worker
pkill -9 -P "$(pgrep -x braidpipe)"
```

Or just use the PID the daemon already printed, which is the same number.

Nothing respawns the worker (see [Known limitations](#known-limitations)), but you can start one by hand and the daemon picks it up on its next successful frame — verified: `branch=AiProcess` returns within a couple of seconds:

```bash
.venv/bin/python3 python/braidpipe/worker_edges.py
```

Run it from the repository root, and note that a hand-started worker is no longer a child of the daemon, so the `pgrep -P` trick above will not find it a second time.

There's also a manual SRT check that exercises URI ingestion end to end with a graphical sink:

```bash
bash scripts/e2e-srt-autovideosink.sh
```

### Measuring latency

```bash
bash scripts/rtmp-latency.sh
```

This publishes a test pattern over RTMP and reports how late every frame was when a receiver got it. It needs nothing installed beyond ffmpeg: `-listen 1` makes ffmpeg the RTMP server braidpipe publishes to *and* the decoder, so no media server sits in the middle inflating the number.

There is no OCR and no guessing. [worker_stamp.py](python/braidpipe/worker_stamp.py) writes the wall clock into every frame as a row of large black-and-white cells, and [rtmp_latency_probe.py](scripts/rtmp_latency_probe.py) reads it back out of the decoded pixels and subtracts. Both processes are on one machine reading one clock, so this is a true one-way measurement rather than a halved round trip. Big cells are the point: H.264 will smear a thin line, but block-coded black and white survive any bitrate worth streaming — the runs below decoded 100% of frames.

Measured on an M-series Mac, 1280x720 @ 30 fps, `x264enc tune=zerolatency speed-preset=ultrafast`, medians over ~700 frames:

| Leg | p50 | What it covers |
| --- | --- | --- |
| daemon → worker | **0.17 ms** | SHM write, UDS datagram, worker wake-up |
| worker → pipeline output | **6.1 ms** | SHM read-back, appsrc, videoconvert, sink |
| worker → RTMP receiver | **39.5 ms** | the above plus encode, flvmux, RTMP, demux, decode |

So braidpipe's own contribution is about **6 ms**, and everything else is the encoder and the transport. The IPC is not what costs you.

**The single biggest knob is `sync=false` on the sink**, worth 48 ms on its own:

| Sink | p50 |
| --- | --- |
| `rtmp2sink` (defaults) | 89.0 ms |
| `rtmp2sink sync=false` | 41.1 ms |

A syncing sink holds each buffer until its running time plus the pipeline's configured latency, and `processing-deadline` alone contributes 20 ms of that by default. A live source already paces the pipeline, so the clock has nothing left to contribute — it only delays. Pinning `video/x-raw,format=I420` before the encoder is worth another millisecond or so at the median, and stops the encoder inheriting 4:4:4 from the RGB the AI branch works in.

Hardware encoding is *not* automatically the low-latency choice here — VideoToolbox measured slower than x264 at ultrafast:

| Encoder | p50 |
| --- | --- |
| `x264enc tune=zerolatency speed-preset=ultrafast` | 40.5 ms |
| `vtenc_h264 realtime=true` | 46.5 ms |

Useful knobs, all environment variables:

| Variable | Default | |
| --- | --- | --- |
| `BRAIDPIPE_RTMP_DURATION` | `30` | seconds to measure |
| `BRAIDPIPE_RTMP_ENCODER` | `x264` | or `vtenc` |
| `BRAIDPIPE_RTMP_TUNED` | `1` | set `0` to reproduce the defaults row above |
| `BRAIDPIPE_RTMP_SINK` | | replace the sink string outright |
| `BRAIDPIPE_STAMP_BUSY_MS` | `0` | give the worker a fake per-frame cost |

That last one doubles as a failover test with numbers attached. A worker held 60 ms per frame is well past the 50 ms budget, and the run shows exactly what the design promises:

```
frames received : 402
  with barcode  : 0
  unreadable    : 402  (pre-keyframe, or passthrough frames)

worker->received: no samples
arrival interval  min= 16.30  p50= 33.33  p90= 35.49  p99= 37.87  max= 38.18  (ms, n=401)
```

Every stamped frame is gone — the AI branch never made its deadline once — and the output still arrives at a 33.33 ms median, which is 30 fps exactly. Nothing downstream could tell the worker had failed.

### Monitoring

The daemon serves Prometheus metrics on `http://127.0.0.1:9184/metrics` (change with `--metrics-port`, `0` disables). Most of the numbers were already being measured for the failover logic — the endpoint makes them visible: every ack carries the worker's processing time, the relay times every roundtrip, the bridge counts the failure streak. The instrumentation adds nothing to the frame path beyond lock-free counter increments.

What you get, by the question it answers:

| Question | Metrics |
| --- | --- |
| Is the AI output live *right now*? | `braidpipe_last_ai_frame_timestamp_seconds`, `braidpipe_active_branch`, `braidpipe_worker_up` |
| What's our availability? | `braidpipe_branch_seconds_total{branch}`, `braidpipe_branch_switches_total{direction}` |
| Is trouble coming? | `braidpipe_queue_depth`, `braidpipe_shm_slots_occupied`, `braidpipe_failure_streak`, `braidpipe_stale_acks_total`, roundtrip p99 vs `braidpipe_relay_deadline_seconds` |
| How fast is the worker? | `braidpipe_roundtrip_seconds` and `braidpipe_worker_processing_seconds` histograms |
| Is the stream healthy? | `braidpipe_input_fps`, `braidpipe_pts_discontinuities_total`, `braidpipe_av_skew_seconds`, `braidpipe_keyframes_total`, `braidpipe_bus_messages_total` |
| What's on the wire? | `braidpipe_sink_bytes_total`, `braidpipe_output_frames_total`, and full `braidpipe_srt_*` transport stats (RTT, loss, retransmits) when the pipeline has an SRT element |
| Are the processes healthy? | `process_*` for the daemon, `braidpipe_worker_cpu_seconds_total` / `braidpipe_worker_resident_memory_bytes` for the worker, `braidpipe_worker_exits_total` |
| Is the GPU doing the work? | `braidpipe_gpu_utilization_percent` and `braidpipe_gpu_memory_used_bytes` (machine-wide, sampled every 5s), plus `braidpipe_gpu_encoder_utilization_percent` / `braidpipe_gpu_decoder_utilization_percent` for the dedicated NVENC/NVDEC blocks on NVIDIA. Series exist only where the platform exposes the counter — absent means unmeasurable, not idle |

The A/V skew gauge is the audio-sync claim from [Audio passthrough](#audio-passthrough), continuously verified in production: both streams' running time at the muxer, subtracted.

A ready-made Grafana stack lives in [monitoring/](monitoring/):

```bash
cd monitoring && docker compose up -d
# Grafana: http://localhost:3000  (provisioned dashboard, no login)
# Prometheus: http://localhost:9090
```

It scrapes once a second and ships a provisioned dashboard (bandwidth, frame rates, latency percentiles against the deadline, branch state timeline, drops, backpressure, A/V skew, process usage, SRT transport) plus [alert rules](monitoring/prometheus/alerts.yml) for the conditions worth paging on: daemon unreachable, worker down, stuck in passthrough, output dark, >5% deadline misses, stale AI frames, A/V skew over 100 ms.

#### Shutdown and stale panels

A metric that stops being scraped keeps its last value on screen, so a daemon that dies looks identical to one that is healthy and idle. Three things prevent that misreading:

- **`braidpipe_up`** goes to `0` the moment a shutdown signal arrives, and the endpoint stays open for `--metrics-drain-ms` (default 2000, ≥ one scrape interval) so the down state is actually recorded before the process exits.
- **The dashboard gates on the scrape**, not on the daemon's own metrics. The Daemon panel reads `up{job="braidpipe"}` — Prometheus writes that even when the target is gone — and the availability, worker, and fps panels are conditioned on it, so they blank or read DOWN rather than freezing on the last healthy sample. `BraidpipeDown` alerts on the same series, and is the only rule that can fire when the daemon no longer exists to report anything.
- **Shutdown cannot hang.** Ctrl+C reaches the worker only when both share a terminal, so the daemon SIGTERMs it explicitly (SIGKILL after 2 s). Pipeline teardown is bounded by a guard that force-exits `--metrics-drain-ms` + 4 s after the signal: on macOS, `set_state(NULL)` on the GL video sink deadlocks against its own GL thread, and a daemon wedged there is the worst case of all — still alive, still serving, still reporting the last healthy sample.

### Troubleshooting

**No output at all, or garbled/duplicated frames.** Look for leftover daemons first — this is by far the most common cause. Old instances share the same socket paths and output URL, and they will happily fight over both:

```bash
pgrep -fl braidpipe
pkill -9 -f 'target/release/braidpipe'
```

Symptoms of cross-talk include `Discarded stale Python ack` with wildly out-of-range frame IDs, and RTMP sinks failing to connect because another publisher holds the URL.

**The pipeline reaches PLAYING and then nothing happens** — no frames, no branch switch, near-zero CPU, no error on the bus. With a live source this is almost always a latency negotiation failure inside the pipeline. The appsrc declares `max-latency=-1` to prevent it: an appsrc otherwise reports zero maximum latency while a live source reports a minimum of one frame period, the selector aggregates min > max, and the pipeline stalls silently. The two AI-side links (`tee` → appsink, and appsrc → selector) also carry their own `videoconvert` so the appsink's RGB requirement is never forced back through the source. If you see it again, the signature is in the GStreamer logs:

```
input-selector <sel:src>: minimum latency bigger than maximum latency
```

Raising `GST_DEBUG` to find it is reasonable, but redirect to a file you are willing to lose — that one error repeats per latency query and can produce gigabytes per minute.

**Output goes dark when the AI branch is selected.** Check the log for pipeline errors — the bus watcher surfaces asynchronous failures that would otherwise be silent. Then confirm your sink can accept the AI branch's caps, which are `video/x-raw,format=RGB` at the configured resolution.

**Worker hangs at startup without attaching.** The handshake reply is sent from the daemon's ack loop, which runs while frames are flowing — a worker attaches within a frame or two of the first one. If it never does, the daemon isn't receiving frames (check the source), or the two sides disagree on the socket paths.

**Worker exits with `OSError: [Errno 55/105] No buffer space available`.** The ack socket buffer filled up. The bundled worker catches this; a custom worker must too.

**`Python failed to respond within target deadline` repeating.** Inference is slower than 1.5 frame periods. Lower `--fps`, shrink the resolution, or process every Nth frame.

**Overlay colours look wrong.** Frames are RGB; OpenCV colour tuples are BGR. Swap the outer channels.

## Project reference

Where things live in the repository, how to work on it, and what is deliberately not built yet.

### Project layout

Ports and adapters, so the availability logic can be tested without GStreamer or Python in the loop:

| Path | Contents |
| --- | --- |
| [crates/braidpipe-core/](crates/braidpipe-core/) | The watchdog FSM and the `StreamController` / `ShmWriter` / `AiBridge` port traits. No GStreamer, no sockets. |
| [crates/braidpipe-engine/](crates/braidpipe-engine/) | GStreamer adapter: pipeline construction, branch switching, bus error reporting, and the macOS run-loop wrapper. |
| [crates/braidpipe-ipc/](crates/braidpipe-ipc/) | The shared-memory ring buffer and the Unix-datagram control bridge with its health tracking, plus [examples/worker.rs](crates/braidpipe-ipc/examples/worker.rs) — a worker written in Rust. |
| [crates/braidpipe/](crates/braidpipe/) | The daemon: CLI, wiring, worker supervision, [preset.rs](crates/braidpipe/src/preset.rs) — the latency/bandwidth profiles — and [relay.rs](crates/braidpipe/src/relay.rs) — the appsink → shm → Python → appsrc data path. |
| [python/braidpipe/](python/braidpipe/) | `shm.py` (the Rust layout mirror), `stamp.py` (the latency barcode), and four example workers: text overlay, edge transform, YOLO detection, and clock stamping. |
| [scripts/](scripts/) | Manual end-to-end checks, the latency harness, and the per-preset bandwidth measurement. |
| [monitoring/](monitoring/) | Prometheus + Grafana compose stack: scrape config, alert rules, provisioned dashboard. |
| [assets/](assets/) | Logo files: transparent wordmark and icon PNGs, plus a multi-size `.ico`. |

### Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --all
```

`relay.rs` is the place to start reading if you want to understand or change frame handling: it's short, and every failure path in it exists to protect the never-dark guarantee.

### Known limitations

- **No worker respawn.** If the Python process dies, the daemon logs it and stays in passthrough for the rest of the run. Restart the worker manually or supervise it externally.
- **Single video stream.** One source, one sink, one worker per daemon. Run multiple daemons with distinct socket paths for multiple streams — the shared memory is anonymous, so only the sockets need distinct names.
- **Full-frame RGB only.** The alpha-overlay compositing path — where Python returns just a mask to be blended, instead of a whole frame — is not implemented yet.
- **Frames are copied, not zero-copy, on the Rust side.** Each frame is copied out of the GStreamer buffer into shared memory and back. Python's view is genuinely zero-copy; Rust's is not.
- **No hardware-accelerated decode by default.** `decodebin3` picks whatever is available; wire an explicit hardware decoder through `--source` if you need one.
- **Video only.** Audio is not carried through the pipeline.
- **No Python SDK.** `shm.py` mirrors the shared-memory layout and nothing more: it is not packaged, not on PyPI, and has no importable name. Every worker re-implements the same socket loop, slot release, and ack handling by copying an example. A thin `braidpipe` package wrapping that loop would remove the copy-paste, and is the obvious next piece of work.

### Contributing

Issues and pull requests are welcome. Please run `cargo test --workspace`, `cargo clippy --workspace --all-targets`, and `cargo fmt --all` before opening a PR, and describe what you tested — for media changes, say which source and sink you actually ran, since plugin availability varies a lot between machines.

Commit messages follow Conventional Commits with a single-line subject, for example `fix(ipc): align shm layout with python`.

### License

Apache-2.0. See [LICENSE](LICENSE).

<p align="center">
  <img src="assets/braidpipe-icon.png" alt="" width="44">
</p>
