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

The `braidpipe` package in [python/](../python/) is the worker SDK: you write a function that mutates a frame, `braidpipe.run()` owns the loop — the handshake, the per-frame notification, the zero-copy NumPy view of the slot, freeing it, and the ack. Install it into whatever environment your worker runs in:

```bash
pip install -e python/    # from the repository root; numpy is its only dependency
```

A complete worker:

```python
import braidpipe

def process(frame):          # (H, W, 3) uint8, RGB — mutate it in place
    frame[:, :, 0] //= 2     # your inference here

if __name__ == "__main__":
    braidpipe.run(process)
```

Mutating the array in place *is* writing to the output frame — there is no separate send step for pixels. Give `process` a second parameter and `run()` passes a `FrameContext` with `frame_id`, `timestamp_us` (the daemon's hand-over clock, so `time.time_ns() // 1000 - ctx.timestamp_us` is the IPC delay), the frame geometry, and which transport is in use. Raising is safe: the exception is reported as `"success": false` and costs one passthrough frame, never the stream. The same script attaches over shared memory next to a local daemon, or over [tcp-raw](#workers-on-another-machine-tcp-raw) when `BRAIDPIPE_DAEMON=host:port` is set.

Four rules keep the stream healthy. `run()` discharges the middle two for you — they are listed because every other attach path (a raw loop, another language) must honor them too:

1. **Finish inside the budget.** You have 1.5 frame periods. Slower than that and your output is simply not used for that frame — correctness is preserved, but the overlay flickers. For heavy models, drop the input `--fps`, run inference on every Nth frame, or hand the model to `braidpipe.BackgroundModel`, which runs it on a thread over the newest frame while every frame is annotated with the latest cached result — [worker_detect.py](../examples/worker_detect.py) shows that end to end.
2. **Always free the slot.** The ring has four slots; leaking them starves the relay. Free the slot even on your own error paths.
3. **Always send an ack, and never die sending it.** Report `"success": false` for a failed frame — the relay treats that as a failure and passes the original through, which is exactly right. Wrap the send in `try/except OSError`; a full datagram buffer (`ENOBUFS`) is normal backpressure, not a fatal condition.
4. **Frames are RGB, not BGR.** OpenCV's conventions assume BGR, so the familiar `(0, 0, 255)` "red" renders as blue here. Use `(255, 0, 0)` for red, or convert with `cv2.cvtColor` if you're feeding a model trained on BGR.

Point `--python-script` at your own file. The simplest start is a copy of [worker.py](../python/braidpipe/worker.py): the template with an empty `process()` hook to fill in. The bundled scripts fall back to putting `python/` on `sys.path` themselves, so they run from a fresh checkout with nothing installed. Workers that want the loop rather than the callback can still build on the transport primitives — `attach()`/`SharedMemoryManager` for shared memory, `connect()`/`RemoteWorkerLink` for tcp-raw — which `braidpipe` exports unchanged; the raw protocol they speak is [the IPC contract](#the-ipc-contract) below.

## Bundled examples

The generic template lives with the transport layer; the demonstration workers live in [examples/](../examples/). Each is self-contained and runnable as-is:

| Worker | Needs | Shows |
| --- | --- | --- |
| [worker.py](../python/braidpipe/worker.py) | numpy | The template to copy: an empty `process()` hook handed to `braidpipe.run()`, nothing else |
| [worker_edges.py](../examples/worker_edges.py) | opencv | A whole-frame pixel transform in a single function — the shape most workers take |
| [worker_detect.py](../examples/worker_detect.py) | ultralytics, torch | A model too slow to run inline, moved off the hot path with `braidpipe.BackgroundModel` |
| [worker_stamp.py](../examples/worker_stamp.py) | numpy | Instrumentation rather than transform — see [Measuring latency](operations.md#measuring-latency) |

```bash
cargo run -p braidpipe --release -- --python-script examples/worker_edges.py
cargo run -p braidpipe --release -- --python-script examples/worker_detect.py
```

`worker_edges.py` is the one to reach for when testing the plumbing: no model, no network, and it rewrites only the left half of the frame so the boundary between processed and untouched pixels is visible on screen.

`worker_detect.py` runs YOLO and is the more instructive one. A CPU inference pass does not fit in 50 ms, so the frame loop never waits for it — `braidpipe.BackgroundModel` takes a copy of every third frame onto its own thread, and every frame is annotated with the most recent boxes available. Boxes lag the picture slightly; no frame misses its deadline. It also caps `torch.set_num_threads` and the inference resolution, because torch left unbounded takes every core and starves the loop that only needs a millisecond of it. Without those caps the worker flaps between branches every few seconds; with them the AI branch stays selected. The first run downloads weights (~6 MB) into the working directory.

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

The repository root's [docker-compose.yml](../docker-compose.yml) is a working example of exactly this arrangement: daemon and worker in separate containers, sharing only the directory holding the two sockets — the shared-memory fd crosses the container boundary inside the daemon's socket reply, so no `/dev/shm` mount or `ipc: host` is needed, and Docker's `restart: unless-stopped` supplies the worker respawn the daemon deliberately omits.

That compose stack is also the easiest place to watch the failover machinery work. With a stream flowing, stop the worker container and the picture keeps playing — only the AI effect disappears:

```bash
docker compose logs -f braidpipe | grep -i branch   # terminal 1: the branch switches
docker compose stop worker    # log flips to Passthrough within the failure streak (~1 s)
docker compose start worker   # the hello handshake re-attaches; log flips back to AiProcess
```

Note the distinction the restart policy draws: a manual `docker compose stop` (or `docker kill`) marks the container manually stopped, so `restart: unless-stopped` leaves it down until you start it again — which is what makes this a controlled test. Only a worker that dies on its own (a crash, an OOM kill, an unhandled exception) triggers Docker's automatic respawn, and then the whole cycle — passthrough, restart, hello, back to the AI branch — runs hands-free. The worker image has no `pkill` and the worker runs as PID 1, so a simulated in-container crash isn't available; a real one behaves exactly like the stop/start pair, just without you typing the second command.

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
.venv/bin/python3 examples/worker_detect.py
```

In this mode nothing picks the interpreter for you — managed mode's automatic `.venv/bin/python3` preference belongs to the spawn path, so point at the environment that has the worker's dependencies yourself. All the bundled workers read `BRAIDPIPE_RUST_SOCK` / `BRAIDPIPE_PYTHON_SOCK` if the daemon's socket paths were overridden. Stopping the worker drops the stream back to passthrough within the failure streak (~1 s at 30 fps); relaunching it re-attaches through the same hello handshake, no daemon restart involved.

One metric changes meaning: the daemon can't know whether a process it doesn't own is alive, so in this mode `braidpipe_worker_up` means "delivered a successful AI frame within the last 2 seconds" rather than "my child process is running", and `worker_exits_total` / `worker_last_exit_code` / CPU / RSS are never populated.

For a containerized worker only the socket directory must cross the container boundary — mount it and the fd handshake does the rest, because a file descriptor passed over a Unix socket works across container namespaces with no `/dev/shm` mount or `--ipc=host` required. On macOS, Docker runs inside a VM, so neither sockets nor memory can cross — external mode there means a host process, not a container (or a worker using the tcp-raw transport below, which crosses anything TCP crosses).

## Workers on another machine (tcp-raw)

`--worker-listen IP:PORT` opens the worker negotiation to the network: UDP on that address answers hellos with a config packet, and TCP on the same port carries raw frames both ways. Same-host shm workers keep working alongside it — the transport is chosen per worker by where its hello arrives.

```bash
# machine A — the daemon
cargo run -p braidpipe --release -- --external-worker --worker-listen 0.0.0.0:7300

# machine B — the edge worker, pointed at the daemon (BRAIDPIPE_DAEMON switches
# any worker built on braidpipe.run() to the tcp-raw transport)
BRAIDPIPE_DAEMON=192.168.1.10:7300 python3 examples/worker_edges.py
```

The worker's hello (`{"type": "hello", "transports": ["tcp-raw"]}`) is answered with `{"type": "config", "transport": "tcp-raw", "contract": 1, "data_port": …, "width": …, "height": …, "channels": …, "format": "rgb"}`. The worker then opens one TCP connection to `data_port`, and every frame in either direction is a fixed 24-byte header plus the raw pixels — [python/braidpipe/remote.py](../python/braidpipe/remote.py) wraps this in the same attach-and-loop shape the shm side has:

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

**Shared memory** — one *anonymous* segment (a `memfd` on Linux, an unlinked POSIX object elsewhere) holding a 32-byte header followed by `slot_count` slots. Each slot is a 24-byte header plus `width × height × channels` bytes of pixels. The segment has no name anywhere: a worker gets in by sending `{"type": "hello"}` to the daemon's socket (optionally with a `"transports"` list; over UDS the answer is always shm), and the daemon replies with a config datagram (body `{"type": "config", "transport": "shm", "contract": 1, …}` describing the ring geometry) carrying the segment's file descriptor as `SCM_RIGHTS` ancillary data. The kernel duplicates the descriptor into the worker, which `fstat`s it for the size and `mmap`s it. Because nothing is ever named, nothing can collide between instances, go stale after a crash, or need permission juggling — the kernel frees the segment when the last descriptor and mapping are gone. A worker may say hello before the daemon is up (retry until answered) or at any point after; replies are sent whenever frames are flowing.

All layouts are explicitly padded on the Rust side and mirrored by `struct` format strings in [python/braidpipe/shm.py](../python/braidpipe/shm.py), which assert their own sizes at import — a mismatch fails loudly instead of silently reading garbage. The `contract` field in every config packet guards the same boundary across versions: it names the IPC contract the daemon speaks (`IPC_CONTRACT_VERSION` in Rust, `braidpipe.CONTRACT_VERSION` in Python — bumped together on any incompatible change), and a worker that sees a number it does not speak refuses to attach at handshake time instead of misreading frames. A config with no field at all is a pre-versioning daemon (≤ 0.2.0, contract 1): the SDK warns and proceeds.

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
