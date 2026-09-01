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
                     ┌─────────────────────────────────────────────────────────┐
   SRT / UDP / RTP   │                        braidpipe                        │
 NDI / camera / file │                                                         │
        ──────────►  │  decode ──┬── queue ─────────┐                          │
                     │           │                  ▼                          │
                     │           │           input-selector ──► encode ──► sink│──────►  SRT / RTMP
                     │           │                  ▲                          │         UDP / display
                     │           └── appsink ──┐  appsrc                       │
                     │                         │    │                          │
                     └─────────────────────────┼────┼──────────────────────────┘
                                    shared memory + Unix datagrams
                                               ▼    │
                                        ┌───────────┴────┐
                                        │  Python worker │
                                        │  (numpy/cv2/…) │
                                        └────────────────┘
```

**How it stays up.** The `input-selector` decides, frame by frame, whether the viewer sees the AI branch or the raw passthrough branch. Two independent levels protect it: the relay gives each frame a **1.5 frame-period deadline** (50 ms at 30 fps) and pushes the original frame downstream if the worker misses it, and a watchdog switches the whole stream to passthrough after **30 consecutive failures** (~1 s), switching back on the first successful frame. A dead, hung, or never-started worker costs you the AI effect — never the stream. → [details](docs/operations.md#how-the-failover-works)

## Documentation

| Guide | What's in it |
| --- | --- |
| [Streaming configuration](docs/streaming.md) | Real sources and sinks, output presets and measured bandwidth, GPU acceleration, audio passthrough, the full CLI reference |
| [AI workers](docs/workers.md) | Writing a worker, the bundled examples, non-Python workers, external and remote (tcp-raw) attach modes, the IPC contract |
| [Operations](docs/operations.md) | How failover works, failover drills, measured latency, the Prometheus/Grafana stack, troubleshooting |

## Requirements

| Component | Version | Notes |
| --- | --- | --- |
| Rust | 1.85+ | Edition 2024 |
| Python | 3.10+ | Workers receive the shared-memory fd with `socket.recv_fds` |
| GStreamer | 1.20+ | Needed for `appsrc leaky-type`; developed against 1.28 |
| OS | Linux or macOS | Anonymous shared memory (fd-passing) + Unix datagram sockets |

GStreamer plugins depend on what you actually stream: `srt` for SRT, `x264`/`libav` for H.264, `rtmp` for RTMP output, `avfvideosrc` (macOS, in plugins-bad) or `v4l2src` (Linux, in plugins-good) for cameras, and a third-party plugin for NDI.

## Install

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

## Quick start

Run from the repository root so the default worker path resolves:

```bash
cargo run -p braidpipe --release
```

That builds a 1280×720 test pattern, runs every frame through the bundled raw worker ([python/braidpipe/worker.py](python/braidpipe/worker.py) — the template to copy for your own, it acks each frame untouched), and displays the result with `autovideosink`. The `Python worker active pid=…` log line is the proof the AI loop is closed; for a transform you can see, run the edge example below.

```bash
# the media path alone, no Python involved
cargo run -p braidpipe --release -- --passthrough-only

# a visible worker: a Canny edge transform on the left half of the frame
cargo run -p braidpipe --release -- --python-script examples/worker_edges.py

# kill the worker mid-stream and watch the output keep running
kill -9 <pid from the "Python worker active pid=…" log line>
```

## Docker

The repo ships a compose stack that runs the daemon and a worker as separate containers, sharing only a socket volume — the shared memory itself is anonymous, so its fd crosses the container boundary inside the daemon's socket reply, with no `/dev/shm` mount and no `ipc: host`:

```bash
# the daemon dials an SRT source on the host's :8890 -- OBS, MediaMTX, or a test feed:
gst-launch-1.0 videotestsrc is-live=true ! x264enc tune=zerolatency \
    ! mpegtsmux ! srtsink uri='srt://0.0.0.0:8890?mode=listener'

docker compose up --build
ffplay -fflags nobuffer 'srt://127.0.0.1:8891?latency=200'   # the edge-transformed feed
```

On a Linux host with an NVIDIA GPU, the GPU overlay moves decode and encode onto the card (NVDEC + `nvh264enc`); prerequisites and the VA-API alternative are in [streaming.md](docs/streaming.md#in-docker):

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build -d
```

After the first build, plain `docker compose up -d` is enough; `--build` is needed again only when source, Dockerfiles, or build args change — the rules (they apply to the GPU overlay too) are in [streaming.md](docs/streaming.md#in-docker).

`restart: unless-stopped` on the worker service is what closes the daemon's deliberate no-respawn gap: a crashed worker container is restarted by Docker, says hello again, and the stream returns from passthrough to the AI branch — measured at ~365 ms of passthrough for a hard worker crash. To watch that failover happen:

```bash
docker compose logs -f braidpipe | grep -i branch   # terminal 1: the branch switches
docker compose stop worker    # video keeps playing; the log flips to Passthrough
docker compose start worker   # worker says hello again; the log flips back to AiProcess
```

A manual `stop` (or `docker kill`) suppresses the restart policy, so the worker stays down as long as the test needs — Docker's auto-respawn only fires when the worker dies on its own, which is exactly the case the stream is protecting against. Swap the worker or the input/output by editing `command:` in [docker-compose.yml](docker-compose.yml); `docker compose --profile metrics up` additionally bridges the Prometheus endpoint to `http://127.0.0.1:9185/metrics`. On macOS, Docker Desktop's userspace UDP proxy can fail SRT handshakes on published ports — enable *Use kernel networking for UDP* in its network settings.

## A real stream

Ingest SRT, publish SRT, with the encoder built from a latency/bandwidth profile:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --preset lowlatency --output 'srt://0.0.0.0:8891?mode=listener'
# watch it: ffplay 'srt://127.0.0.1:8891?latency=200'
```

`--output` takes `rtmp://`, `srt://` or `udp://host:port` and picks the muxer to match; `--preset` is one of `zerolatency`, `lowlatency` (default), `balanced`, `bandwidth`. Add `--audio` to carry the source's audio around the AI branch. The flags you'll reach for most:

| Flag | Default | Purpose |
| --- | --- | --- |
| `--uri <URI>` / `-i, --source <PIPELINE>` | test pattern | Input, auto-decoded or spelled out as GStreamer elements |
| `--output <URL>` / `-o, --sink <PIPELINE>` | `autovideosink` | Output, built from a preset or written by hand |
| `--preset <NAME>` | `lowlatency` | Latency/bandwidth profile for `--output` |
| `-p, --python-script <PATH>` | `python/braidpipe/worker.py` | Worker to launch |
| `-f, --fps <N>`, `--width`, `--height` | `30`, `1280`, `720` | Frame rate and shared-memory slot geometry |
| `--external-worker`, `--worker-listen <IP:PORT>` | off | Attach a worker you started yourself, locally or on another machine |

Full reference, GPU flags, environment overrides and per-source recipes: [Streaming configuration](docs/streaming.md).

## Writing a worker

A worker is a loop over one Unix datagram socket: say hello, get the shared-memory fd back, then for each notification mutate a zero-copy NumPy view of the slot in place and ack it.

```python
shm = attach(sock, "/tmp/braidpipe_rust.sock")        # handshake

while True:
    packet = json.loads(sock.recvfrom(512)[0])
    frame = shm.get_slot_numpy_array(packet["slot_index"])   # (H, W, 3) uint8, RGB

    # ... your inference here; mutate `frame` in place ...

    shm.mark_slot_free(packet["slot_index"])
    sock.sendto(ack(packet, success=True), "/tmp/braidpipe_rust.sock")
```

Finish inside 1.5 frame periods, always free the slot, always ack (`"success": false` on failure is correct and safe), and remember frames are RGB, not BGR. [python/braidpipe/worker.py](python/braidpipe/worker.py) is that contract with an empty `process()` hook — copy it and fill the hook in. Three worked examples ship in [examples/](examples/) — edge transform, threaded YOLO detection, and clock stamping — and the contract is language-agnostic: [worker.rs](crates/braidpipe-ipc/examples/worker.rs) is the same worker in Rust.

Workers do not have to be launched by the daemon. `--external-worker` attaches a process you own (a container, a service, something started by hand), and `--worker-listen` accepts workers from other machines over the tcp-raw transport. Full contract and every attach mode: [AI workers](docs/workers.md).

## Monitoring

The daemon serves Prometheus metrics on `http://127.0.0.1:9184/metrics`, and [monitoring/](monitoring/) has a ready-made stack:

```bash
cd monitoring && docker compose up -d   # Grafana on :3000, Prometheus on :9090
```

The provisioned dashboard covers bandwidth, frame rates, latency percentiles against the deadline, the branch-state timeline, backpressure, A/V skew and SRT transport stats, with alert rules for the conditions worth paging on. See [Operations](docs/operations.md#monitoring).

## Project layout

Ports and adapters, so the availability logic can be tested without GStreamer or Python in the loop:

| Path | Contents |
| --- | --- |
| [crates/braidpipe-core/](crates/braidpipe-core/) | The watchdog FSM and the `StreamController` / `ShmWriter` / `AiBridge` port traits. No GStreamer, no sockets. |
| [crates/braidpipe-engine/](crates/braidpipe-engine/) | GStreamer adapter: pipeline construction, branch switching, bus error reporting, and the macOS run-loop wrapper. |
| [crates/braidpipe-ipc/](crates/braidpipe-ipc/) | The shared-memory ring buffer, the Unix-datagram control bridge with its health tracking, and the tcp-raw network transport, plus [examples/worker.rs](crates/braidpipe-ipc/examples/worker.rs) — a worker written in Rust. |
| [crates/braidpipe/](crates/braidpipe/) | The daemon: CLI, wiring, worker supervision, [preset.rs](crates/braidpipe/src/preset.rs) — the latency/bandwidth profiles — and [relay.rs](crates/braidpipe/src/relay.rs) — the appsink → shm → Python → appsrc data path. |
| [python/braidpipe/](python/braidpipe/) | The generic worker layer: `worker.py` (the raw template with an empty `process()` hook), `shm.py` (the Rust layout mirror), and `remote.py` (the tcp-raw client). |
| [examples/](examples/) | The demonstration workers — edge transform, threaded YOLO detection, clock stamping — plus `stamp.py`, the latency barcode they share with the probe. |
| [docs/](docs/) | The detailed guides linked above. |
| [scripts/](scripts/) | Manual end-to-end checks, the latency harness, and the per-preset bandwidth measurement. |
| [vmaf-test/](vmaf-test/) | VMAF quality measurement of the encode path over SRT — see [its README](vmaf-test/README.md). |
| [monitoring/](monitoring/) | Prometheus + Grafana compose stack: scrape config, alert rules, provisioned dashboard. |
| [assets/](assets/) | Logo files: transparent wordmark and icon PNGs, plus a multi-size `.ico`. |

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --all
```

`relay.rs` is the place to start reading if you want to understand or change frame handling: it's short, and every failure path in it exists to protect the never-dark guarantee.

## Measuring encode quality (with VMAF)

How much visual quality does the decode → re-encode path cost? [vmaf-test/](vmaf-test/) answers that with [VMAF](https://github.com/Netflix/vmaf): it streams a reference file into a `--passthrough-only` daemon over SRT, captures the SRT output, and scores the two against each other frame by frame.

```bash
python3 vmaf-test/run_vmaf_test.py source.mp4
```

Each run writes a folder under `vmaf-test/runs/` with the capture, the per-frame scores, the daemon log, and a markdown report whose headline is the pooled VMAF mean (≥ 93 is visually transparent). Presets, SRT latency, `BRAIDPIPE_*` overrides and arbitrary daemon flags are all parameters, and an already-running SRT feed can stand in for the built-in one — see [vmaf-test/README.md](vmaf-test/README.md).

## Known limitations

- **No worker respawn.** If the Python process dies, the daemon logs it and stays in passthrough for the rest of the run. Restart the worker manually or supervise it externally.
- **Single video stream.** One source, one sink, one worker per daemon. Run multiple daemons with distinct socket paths for multiple streams — the shared memory is anonymous, so only the sockets need distinct names.
- **Full-frame RGB only.** The alpha-overlay compositing path — where Python returns just a mask to be blended, instead of a whole frame — is not implemented yet.
- **Frames are copied, not zero-copy, on the Rust side.** Each frame is copied out of the GStreamer buffer into shared memory and back. Python's view is genuinely zero-copy; Rust's is not.
- **tcp-raw frames are uncompressed.** The remote-worker transport ships raw RGB, so it is LAN-only in practice; a compressed or subsampled wire format is not implemented yet (the config packet's `format` field exists so one can be negotiated later without breaking workers).
- **No Python SDK.** `shm.py` and `remote.py` mirror the two transports and nothing more: they are not packaged, not on PyPI, and have no importable name. Every worker re-implements the same socket loop, slot release, and ack handling by copying an example. A thin `braidpipe` package wrapping that loop would remove the copy-paste, and is the obvious next piece of work.

## Contributing

Issues and pull requests are welcome. Please run `cargo test --workspace`, `cargo clippy --workspace --all-targets`, and `cargo fmt --all` before opening a PR, and describe what you tested — for media changes, say which source and sink you actually ran, since plugin availability varies a lot between machines.

Commit messages follow Conventional Commits with a single-line subject, for example `fix(ipc): align shm layout with python`.

## License

Apache-2.0. See [LICENSE](LICENSE).

<p align="center">
  <img src="assets/braidpipe-icon.png" alt="" width="44">
</p>
