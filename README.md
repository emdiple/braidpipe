# Braidpipe

**Never-dark AI video middleware.** braidpipe ingests a live video stream, hands raw frames to a Python process for AI/CV work over shared memory, re-encodes the result, and streams it out — and if that Python process crashes, stalls, or falls behind, the output stream keeps running on untouched frames instead of going dark.

The Rust daemon owns the media path. Python only ever sees pixels in a shared-memory slot and a small JSON message telling it which slot to look at. That separation is the whole point: an exception in a model's inference code must never be able to take the broadcast off air.

```
                     ┌─────────────────────────────────┐
   SRT / UDP / RTP   │            braidpipe            │
   NDI / file / test │                                 │
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

- [Braidpipe](#braidpipe)
  - [Table of contents](#table-of-contents)
  - [How the failover works](#how-the-failover-works)
  - [Requirements](#requirements)
  - [Install](#install)
  - [Quick start](#quick-start)
  - [Real-world pipelines](#real-world-pipelines)
  - [Command-line reference](#command-line-reference)
  - [Writing a Python worker](#writing-a-python-worker)
  - [The IPC contract](#the-ipc-contract)
  - [Testing failover](#testing-failover)
  - [Troubleshooting](#troubleshooting)
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

## Requirements

| | |
| --- | --- |
| Rust | 1.85+ (edition 2024) |
| Python | 3.10+ (3.13+ recommended, see note) |
| GStreamer | 1.20+ for `appsrc leaky-type`; developed against 1.28 |
| OS | Linux or macOS (POSIX shared memory + Unix datagram sockets) |

GStreamer plugins depend on what you actually stream: `srt` for SRT, `x264`/`libav` for H.264, `rtmp` for RTMP output, and a third-party plugin for NDI.

On Python 3.13+, `SharedMemory(track=False)` keeps Python's resource tracker from unlinking the Rust-owned segment when the worker exits. On older versions the bundled worker falls back automatically, but you may see a resource-tracker warning at shutdown and should restart the daemon rather than reusing the segment.

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

That builds a 1280×720 test pattern, runs it through the bundled worker (which draws a frame counter onto each frame), and displays the result with `autovideosink`. You should see the overlay text updating on a moving ball.

To confirm the media path alone, with no Python involved:

```bash
cargo run -p braidpipe --release -- --passthrough-only
```

## Real-world pipelines

**SRT in, RTMP out** — the common broadcast shape:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --sink 'videoconvert ! x264enc tune=zerolatency bitrate=4000 speed-preset=veryfast key-int-max=60 \
          ! h264parse config-interval=-1 ! flvmux streamable=true \
          ! rtmpsink location="rtmp://localhost/live/stream live=1"'
```

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

**1080p60:**

```bash
cargo run -p braidpipe --release -- --width 1920 --height 1080 --fps 60 \
  --source 'videotestsrc is-live=true pattern=ball ! video/x-raw,width=1920,height=1080,framerate=60/1 ! videoconvert' \
  --sink 'videoconvert ! autovideosink'
```

`--uri` and `--source` are mutually exclusive. Use `--uri` when GStreamer can figure out the source on its own (it picks `srtsrc ! decodebin3` for `srt://` and `uridecodebin3` for everything else); use `--source` when you need to spell out elements yourself.

## Command-line reference

| Flag | Default | Purpose |
| --- | --- | --- |
| `-i, --source <PIPELINE>` | test pattern | Explicit GStreamer source fragment |
| `--uri <URI>` | — | Input URI decoded by GStreamer (`srt://`, `udp://`, `rtp://`, `ndi://`, `file://`) |
| `-o, --sink <PIPELINE>` | `videoconvert ! autovideosink` | Output fragment appended after the selector |
| `-p, --python-script <PATH>` | `python/braidpipe/worker.py` | Worker to launch |
| `-f, --fps <N>` | `30` | Frame rate; sets the relay deadline and watchdog tick |
| `--width <N>` / `--height <N>` | `1280` / `720` | Shared-memory slot geometry |
| `--shm-name <NAME>` | `/braidpipe_buffer` | POSIX shared memory object name |
| `--rust-sock <PATH>` | `/tmp/braidpipe_rust.sock` | Where the daemon listens for acks |
| `--python-sock <PATH>` | `/tmp/braidpipe_python.sock` | Where the worker listens for notifications |
| `--passthrough-only` | off | Media path only; no worker, no shared memory |

`--width`/`--height` must match the frames your source actually produces after `videoscale`, because they define the slot size that both sides index into.

Set `RUST_LOG=debug` to see per-frame relay activity, including dropped and stale acks.

## Writing a Python worker

A worker is a loop over one Unix datagram socket. `SharedMemoryManager` gives you a zero-copy NumPy view of the slot, so mutating the array in place *is* writing to the output frame — there's no separate send step for pixels.

```python
import json, os, socket
from shm import SharedMemoryManager

sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
if os.path.exists("/tmp/braidpipe_python.sock"):
    os.remove("/tmp/braidpipe_python.sock")
sock.bind("/tmp/braidpipe_python.sock")

shm = SharedMemoryManager()   # reads geometry from the header Rust wrote

while True:
    packet = json.loads(sock.recvfrom(512)[0])
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

Point `--python-script` at your own file. Because Python puts the script's own directory on `sys.path`, a worker living beside `shm.py` can `from shm import SharedMemoryManager` directly; from elsewhere, add `python/braidpipe` to `sys.path` or install it as a package.

## The IPC contract

**Shared memory** — one POSIX object holding a 32-byte header followed by `slot_count` slots. Each slot is a 24-byte header plus `width × height × channels` bytes of pixels. All layouts are explicitly padded on the Rust side and mirrored by `struct` format strings in [python/braidpipe/shm.py](python/braidpipe/shm.py), which assert their own sizes at import — a mismatch fails loudly instead of silently reading garbage.

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

## Testing failover

The interesting property is what happens when Python dies mid-stream. Start the daemon, wait for `branch=AiProcess`, then kill the worker using the PID from the log line `Python worker active pid=…`:

```bash
kill -9 <worker-pid>
```

Within about a second you should see `Successfully switched video stream branch branch=Passthrough`, the overlay disappear, and the stream continue without a stall, a black frame, or a dropped publisher connection.

Avoid `pkill -f 'worker.py'`. `pkill -f` matches the *whole* command line, and the daemon's own command line contains `--python-script python/braidpipe/worker.py` — so that pattern kills the daemon along with the worker and proves nothing. If you want a pattern, anchor it to the interpreter:

```bash
pgrep -fl '[p]ython3 python/braidpipe/worker.py'    # check first
pkill -9 -f '[p]ython3 python/braidpipe/worker.py'
```

Nothing respawns the worker (see [Known limitations](#known-limitations)), but you can start one by hand and the daemon will pick it up on its next successful frame:

```bash
.venv/bin/python3 python/braidpipe/worker.py
```

There's also a manual SRT check that exercises URI ingestion end to end with a graphical sink:

```bash
bash scripts/e2e-srt-autovideosink.sh
```

## Troubleshooting

**No output at all, or garbled/duplicated frames.** Look for leftover daemons first — this is by far the most common cause. Old instances share the same socket paths, shared-memory name, and output URL, and they will happily fight over all three:

```bash
pgrep -fl braidpipe
pkill -9 -f 'target/release/braidpipe'
```

Symptoms of cross-talk include `Discarded stale Python ack` with wildly out-of-range frame IDs, and RTMP sinks failing to connect because another publisher holds the URL.

**Output goes dark when the AI branch is selected.** Check the log for pipeline errors — the bus watcher surfaces asynchronous failures that would otherwise be silent. Then confirm your sink can accept the AI branch's caps, which are `video/x-raw,format=RGB` at the configured resolution.

**`Failed to set SHM size with ftruncate`.** A stale segment is still mapped by another process. `create()` unlinks before opening, which handles the usual case; if it persists, kill every braidpipe and worker process and try again.

**Worker exits with `OSError: [Errno 55/105] No buffer space available`.** The ack socket buffer filled up. The bundled worker catches this; a custom worker must too.

**`Python failed to respond within target deadline` repeating.** Inference is slower than 1.5 frame periods. Lower `--fps`, shrink the resolution, or process every Nth frame.

**Overlay colours look wrong.** Frames are RGB; OpenCV colour tuples are BGR. Swap the outer channels.

## Project layout

Ports and adapters, so the availability logic can be tested without GStreamer or Python in the loop:

| Path | Contents |
| --- | --- |
| [crates/braidpipe-core/](crates/braidpipe-core/) | The watchdog FSM and the `StreamController` / `ShmWriter` / `AiBridge` port traits. No GStreamer, no sockets. |
| [crates/braidpipe-engine/](crates/braidpipe-engine/) | GStreamer adapter: pipeline construction, branch switching, bus error reporting, and the macOS run-loop wrapper. |
| [crates/braidpipe-ipc/](crates/braidpipe-ipc/) | The shared-memory ring buffer and the Unix-datagram control bridge with its health tracking. |
| [crates/braidpipe/](crates/braidpipe/) | The daemon: CLI, wiring, worker supervision, and [relay.rs](crates/braidpipe/src/relay.rs) — the appsink → shm → Python → appsrc data path. |
| [python/braidpipe/](python/braidpipe/) | `shm.py` (the Rust layout mirror) and `worker.py` (a working example). |
| [scripts/](scripts/) | Manual end-to-end checks. |

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --all
```

`relay.rs` is the place to start reading if you want to understand or change frame handling: it's short, and every failure path in it exists to protect the never-dark guarantee.

## Known limitations

- **No worker respawn.** If the Python process dies, the daemon logs it and stays in passthrough for the rest of the run. Restart the worker manually or supervise it externally.
- **Single video stream.** One source, one sink, one worker per daemon. Run multiple daemons with distinct `--shm-name` and socket paths for multiple streams.
- **Full-frame RGB only.** The alpha-overlay compositing path — where Python returns just a mask to be blended, instead of a whole frame — is not implemented yet.
- **Frames are copied, not zero-copy, on the Rust side.** Each frame is copied out of the GStreamer buffer into shared memory and back. Python's view is genuinely zero-copy; Rust's is not.
- **No hardware-accelerated decode by default.** `decodebin3` picks whatever is available; wire an explicit hardware decoder through `--source` if you need one.
- **Video only.** Audio is not carried through the pipeline.

## Contributing

Issues and pull requests are welcome. Please run `cargo test --workspace`, `cargo clippy --workspace --all-targets`, and `cargo fmt --all` before opening a PR, and describe what you tested — for media changes, say which source and sink you actually ran, since plugin availability varies a lot between machines.

Commit messages follow Conventional Commits with a single-line subject, for example `fix(ipc): align shm layout with python`.

## License

Apache-2.0. See [LICENSE](LICENSE).
