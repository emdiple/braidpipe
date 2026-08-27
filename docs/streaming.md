# Streaming configuration

Everything between the input URI and the output URL: real sources and sinks, the encoder presets, GPU offload, audio, and the full flag reference.

[← Back to the README](../README.md) · [AI workers](workers.md) · [Operations](operations.md)

- [Real-world pipelines](#real-world-pipelines)
- [Output presets](#output-presets)
  - [Recipe: NDI 1080p50, low latency, high quality](#recipe-ndi-1080p50-low-latency-high-quality)
  - [Measured bandwidth](#measured-bandwidth)
- [GPU acceleration](#gpu-acceleration)
- [Audio passthrough](#audio-passthrough)
- [Command-line reference](#command-line-reference)

## Real-world pipelines

**SRT in, RTMP out** — the common broadcast shape:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --sink 'videoconvert ! video/x-raw,format=I420 \
          ! x264enc tune=zerolatency bitrate=4000 speed-preset=veryfast key-int-max=60 \
          ! h264parse config-interval=-1 ! flvmux streamable=true \
          ! rtmp2sink sync=false location=rtmp://localhost/live/stream'
```

`sync=false` is not incidental — it is worth about 48 ms, for the reasons in [Measuring latency](operations.md#measuring-latency).

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

## Output presets

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
| `BRAIDPIPE_SINK_SYNC` | sink clock sync | `1`/`0` — see [Measuring latency](operations.md#measuring-latency) for why `0` is worth ~48 ms |
| `BRAIDPIPE_SRT_LATENCY_MS` | `srtsink` latency budget | milliseconds |

```bash
# lowlatency profile, but cap the bandwidth
BRAIDPIPE_BITRATE_KBPS=2500 cargo run -p braidpipe --release -- \
  --preset lowlatency --output srt://127.0.0.1:8888
```

Verified end-to-end with the [latency harness](operations.md#measuring-latency): `--preset lowlatency --output rtmp://…` measured 39.8 ms p50 / 45.6 ms p99 worker→receiver at 720p30, identical to the hand-tuned sink.

### Recipe: NDI 1080p50, low latency, high quality

When bandwidth is not a constraint and the goal is the lowest latency at the best picture, start from `lowlatency` and turn exactly two knobs:

```bash
BRAIDPIPE_BITRATE_KBPS=20000 BRAIDPIPE_SPEED_PRESET=fast \
cargo run -p braidpipe --release -- \
  --uri 'ndi://Studio%20Camera' \
  --width 1920 --height 1080 --fps 50 \
  --preset lowlatency --encoder auto \
  --output rtmp://localhost/live/stream
```

Why these values and nothing else:

- **`--preset lowlatency`** already pulls every latency lever that matters — `tune=zerolatency` (no B-frames, no lookahead), a 200 ms VBV burst bound, `sync=false` on the sink, a 2 s GOP (`key-int-max=100` at 50 fps). None of those need restating.
- **`BRAIDPIPE_BITRATE_KBPS=20000`** — the preset's 4500 kbps assumes 720p30; 1080p50 is ~3.7× the pixel rate, and for H.264 at this format quality saturates around 20–25 Mbps. Past that you are spending bits without seeing them.
- **`BRAIDPIPE_SPEED_PRESET=fast`** — the speed preset costs CPU, not latency, as long as the encoder keeps real time. `fast` buys compression efficiency over the preset's `veryfast`; go slower only if CPU headroom says so, and never shrink the VBV to compensate.
- **`--encoder auto`** — at 20 Mbps the quality gap between hardware encoders and x264 collapses, so let a GPU take the job and keep the CPU for the worker (which has a 20 ms/frame budget at 50 fps).

Do **not** reach for the `bandwidth` preset here: its lookahead and B-frames add several frames of encoder delay by design.

### Measured bandwidth

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

## GPU acceleration

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

Whether the GPU is then actually doing work shows up in [Monitoring](operations.md#monitoring): `braidpipe_gpu_utilization_percent` samples machine-wide GPU load every 5 seconds (via `ioreg` on macOS, `nvidia-smi` or the amdgpu sysfs on Linux), NVIDIA additionally breaks out the dedicated `_encoder_`/`_decoder_` block utilization, and the Grafana dashboard has a GPU row for all of them.

**Encoding** is decided when `--output` builds the sink from a preset: the best hardware encoder present replaces the preset's x264 default, and the `Built sink from preset` log shows which one won. Detection is reliable because the NVIDIA/VA/QSV/AMF/MediaFoundation plugins only register their elements when the device probe succeeds — if `nvh264enc` exists in the registry, there is an NVENC-capable GPU behind it. The preset's parameters map onto each encoder's own vocabulary: bitrate and GOP always, the zero-latency switch per encoder (`realtime`, `low-latency`, `ultra-low-latency` usage), and the VBV burst bound where one is exposed (NVENC `vbv-buffer-size`, VA `cpb-size`).

Two knobs control this, each a CLI flag with an environment-variable twin (the flag wins when both are given):

- `--hw off` (or `BRAIDPIPE_HW=off`) — software everywhere: no decoder promotion, no encoder auto-pick.
- `--encoder <name>` (or `BRAIDPIPE_ENCODER=<name>`) — pin the encoder regardless of detection; `--encoder auto` explicitly re-enables detection. Hardware encoders trade some quality-per-bit for speed, so the `bandwidth` preset's intent is best served by pinning `x264`. Pinning the encoder leaves GPU *decoding* on — use `--hw off` to force software both ways.

A hand-written `--sink` bypasses encoder selection entirely — you name the encoder yourself.

## Audio passthrough

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

## Command-line reference

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
| `--external-worker` | off | Don't spawn or supervise a worker; connect to an externally managed AI process, see [External worker mode](workers.md#external-worker-mode) |
| `-f, --fps <N>` | `30` | Frame rate; sets the relay deadline and watchdog tick |
| `--width <N>` / `--height <N>` | `1280` / `720` | Shared-memory slot geometry |
| `--rust-sock <PATH>` | `/tmp/braidpipe_rust.sock` | Where the daemon listens for acks and hellos |
| `--python-sock <PATH>` | `/tmp/braidpipe_python.sock` | Where the worker listens for notifications |
| `--worker-listen <IP:PORT>` | — | Also accept workers from other machines (tcp-raw), see [Workers on another machine](workers.md#workers-on-another-machine-tcp-raw) |
| `--passthrough-only` | off | Media path only; no worker, no shared memory |
| `--metrics-port <N>` | `9184` | Prometheus endpoint on 127.0.0.1, see [Monitoring](operations.md#monitoring); `0` disables |
| `--metrics-drain-ms <N>` | `2000` | How long to keep serving metrics after a shutdown signal, so the down state gets scraped |

`--width`/`--height` must match the frames your source actually produces after `videoscale`, because they define the slot size that both sides index into.

Set `RUST_LOG=debug` to see per-frame relay activity, including dropped and stale acks.
