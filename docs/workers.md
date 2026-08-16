# AI workers

The other side of the shared memory: the contract a worker implements, the bundled examples, and every way to attach one — spawned by the daemon, started by hand, in a container, or on another machine.

[← Back to the README](../README.md) · [Streaming configuration](streaming.md) · [Operations](operations.md)

- [Writing a Python worker](#writing-a-python-worker)
- [Bundled examples](#bundled-examples)
- [Writing a worker in another language](#writing-a-worker-in-another-language)
- [External worker mode](#external-worker-mode)
- [Workers on another machine (tcp-raw)](#workers-on-another-machine-tcp-raw)
- [The IPC contract](#the-ipc-contract)

## Writing a Python worker

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

## Bundled examples

Four workers ship with the project, each self-contained and runnable as-is:

| Worker | Needs | Shows |
| --- | --- | --- |
| [worker.py](../python/braidpipe/worker.py) | opencv | The minimum contract: a text overlay and a frame counter |
| [worker_edges.py](../python/braidpipe/worker_edges.py) | opencv | A whole-frame pixel transform, reporting `success: false` instead of dying, and both transports — set `BRAIDPIPE_DAEMON` for [tcp-raw](#workers-on-another-machine-tcp-raw) |
| [worker_detect.py](../python/braidpipe/worker_detect.py) | ultralytics, torch | A model too slow to run inline, moved to a thread with cached results |
| [worker_stamp.py](../python/braidpipe/worker_stamp.py) | numpy | Instrumentation rather than transform — see [Measuring latency](operations.md#measuring-latency) |

```bash
cargo run -p braidpipe --release -- --python-script python/braidpipe/worker_edges.py
cargo run -p braidpipe --release -- --python-script python/braidpipe/worker_detect.py
```

`worker_edges.py` is the one to reach for when testing the plumbing: no model, no network, and it rewrites only the left half of the frame so the boundary between processed and untouched pixels is visible on screen.

`worker_detect.py` runs YOLO and is the more instructive one. A CPU inference pass does not fit in 50 ms, so the socket loop never waits for it — a detector thread takes a copy of every third frame, and every frame is annotated with the most recent boxes available. Boxes lag the picture slightly; no frame misses its deadline. It also caps `torch.set_num_threads` and the inference resolution, because torch left unbounded takes every core and starves the loop that only needs a millisecond of it. Without those caps the worker flaps between branches every few seconds; with them the AI branch stays selected. The first run downloads weights (~6 MB) into the working directory.

## Writing a worker in another language

Nothing in the contract is Python-specific. [crates/braidpipe-ipc/examples/worker.rs](../crates/braidpipe-ipc/examples/worker.rs) is a complete worker in Rust — it runs the hello handshake, maps the received fd, reuses the daemon's own `ShmHeader`/`SlotHeader`/packet types so it cannot drift out of sync with them, transforms pixels, frees the slot, and acks.

A worker only ever *attaches* to shared memory, by mapping the fd the daemon hands it. Never call `ShmRingBuffer::create` from one: that makes a second, unrelated segment the daemon will never look at.

Run the daemon in [external worker mode](#external-worker-mode) and start your own worker alongside it:

```bash
# terminal 1 — creates the shared memory segment and streams
cargo run -p braidpipe --release -- --external-worker

# terminal 2 — the AI branch is selected on this worker's first good frame
cargo run -p braidpipe-ipc --release --example worker
```

Any language that can receive a file descriptor over a Unix datagram socket (`recvmsg` with `SCM_RIGHTS`), `mmap` it, and parse JSON can do the same. What it must implement is the [IPC contract](#the-ipc-contract) below, in full — the four rules above apply regardless of language.

## External worker mode

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

## Workers on another machine (tcp-raw)

`--worker-listen IP:PORT` opens the worker negotiation to the network: UDP on that address answers hellos with a config packet, and TCP on the same port carries raw frames both ways. Same-host shm workers keep working alongside it — the transport is chosen per worker by where its hello arrives.

```bash
# machine A — the daemon
cargo run -p braidpipe --release -- --external-worker --worker-listen 0.0.0.0:7300

# machine B — the edge worker, pointed at the daemon (of the bundled
# workers, worker_edges.py is the one with the remote-transport switch)
BRAIDPIPE_DAEMON=192.168.1.10:7300 python3 worker_edges.py
```

The worker's hello (`{"type": "hello", "transports": ["tcp-raw"]}`) is answered with `{"type": "config", "transport": "tcp-raw", "data_port": …, "width": …, "height": …, "channels": …, "format": "rgb"}`. The worker then opens one TCP connection to `data_port`, and every frame in either direction is a fixed 24-byte header plus the raw pixels — [python/braidpipe/remote.py](../python/braidpipe/remote.py) wraps this in the same attach-and-loop shape the shm side has:

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

## The IPC contract

**Shared memory** — one *anonymous* segment (a `memfd` on Linux, an unlinked POSIX object elsewhere) holding a 32-byte header followed by `slot_count` slots. Each slot is a 24-byte header plus `width × height × channels` bytes of pixels. The segment has no name anywhere: a worker gets in by sending `{"type": "hello"}` to the daemon's socket (optionally with a `"transports"` list; over UDS the answer is always shm), and the daemon replies with a config datagram (body `{"type": "config", "transport": "shm", …}` describing the ring geometry) carrying the segment's file descriptor as `SCM_RIGHTS` ancillary data. The kernel duplicates the descriptor into the worker, which `fstat`s it for the size and `mmap`s it. Because nothing is ever named, nothing can collide between instances, go stale after a crash, or need permission juggling — the kernel frees the segment when the last descriptor and mapping are gone. A worker may say hello before the daemon is up (retry until answered) or at any point after; replies are sent whenever frames are flowing.

All layouts are explicitly padded on the Rust side and mirrored by `struct` format strings in [python/braidpipe/shm.py](../python/braidpipe/shm.py), which assert their own sizes at import — a mismatch fails loudly instead of silently reading garbage.

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
