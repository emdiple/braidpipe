# BrAIdpipe

Braidpipe is a Rust daemon for experimenting with Python-based video processing in a GStreamer pipeline. It separates media control from the Python worker through shared memory and Unix-domain datagram sockets.

> **Status:** the workspace compiles, and the media, IPC, watchdog, and worker components are in place. The GStreamer AppSink/AppSrc frame bridge and ACK-driven frame watchdog are not yet connected end to end. Do not use this project for production streaming yet.

## Architecture

The project uses a ports-and-adapters layout:

| Component | Responsibility |
| --- | --- |
| `braidpipe-core` | Watchdog and media/AI port definitions. |
| `braidpipe-engine` | GStreamer pipeline construction and branch switching. |
| `braidpipe-ipc` | POSIX shared-memory ring buffer and Unix-domain socket control bridge. |
| `braidpipe` | CLI daemon that wires the adapters together and starts the Python worker. |
| `python/braidpipe` | Example worker that opens the shared-memory buffer and handles control messages. |

```text
GStreamer source
       |
       +--> passthrough branch --> input-selector --> GStreamer sink
       |
       +--> AppSink --> shared-memory ring buffer <--> Python worker
                                              |
                                      Unix-domain datagrams
                                              |
                                           AppSrc
```

## Prerequisites

- Rust 1.85 or newer (edition 2024)
- Python 3.10 or newer
- GStreamer development libraries
- Linux with POSIX shared memory mounted at `/dev/shm`

For Debian or Ubuntu, install the native GStreamer packages with:

```bash
sudo apt install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev
```

The example Python worker also needs NumPy and OpenCV:

```bash
python3 -m pip install numpy opencv-python
```

## Build

```bash
cargo check --workspace
cargo build --release
```

## Run

The daemon starts the Python worker automatically. Run it from the repository root so the default worker path resolves correctly:

```bash
cargo run -p braidpipe --release
```

Use `--help` to view the available options:

```bash
cargo run -p braidpipe -- --help
```

For example, configure a 1080p, 60 FPS pipeline:

```bash
cargo run -p braidpipe --release -- \
  --width 1920 \
  --height 1080 \
  --fps 60 \
  --source 'videotestsrc ! video/x-raw,width=1920,height=1080,framerate=60/1 ! videoconvert' \
  --sink 'videoconvert ! autovideosink'
```

### URI inputs

Use `--uri` to select a GStreamer source for a URI and decode it with `uridecodebin3` before it enters the processing pipeline:

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --sink 'videoconvert ! autovideosink'
```

The installed GStreamer plugins must provide a URI source for the selected scheme. This can include `srt://`, `udp://`, `rtp://`, or `ndi://` when the corresponding plugins are installed. Use `--source` instead for streams that require explicit RTP caps, a depayloader, or other custom GStreamer elements; `--source` and `--uri` cannot be used together.

#### SRT listener

```bash
cargo run -p braidpipe --release -- \
  --uri 'srt://0.0.0.0:9000?mode=listener' \
  --sink 'videoconvert ! autovideosink'
```

#### NDI input

With an NDI GStreamer source plugin that registers the `ndi://` URI scheme:

```bash
cargo run -p braidpipe --release -- \
  --uri 'ndi://Studio%20Camera' \
  --sink 'videoconvert ! autovideosink'
```

#### UDP/RTP input

If the installed GStreamer plugins expose the stream as a URI, pass it directly:

```bash
cargo run -p braidpipe --release -- \
  --uri 'udp://0.0.0.0:5000' \
  --sink 'videoconvert ! autovideosink'
```

Raw RTP commonly needs the codec and payload type specified explicitly. In that case, use `--source`:

```bash
cargo run -p braidpipe --release -- \
  --source 'udpsrc port=5000 caps="application/x-rtp,media=video,encoding-name=H264,payload=96" ! rtph264depay ! h264parse ! decodebin3 ! videoconvert' \
  --sink 'videoconvert ! autovideosink'
```

## Manual SRT end-to-end check

On a Linux desktop with the GStreamer SRT and x264 plugins installed, run:

```bash
bash scripts/e2e-srt-autovideosink.sh
```

The script starts a local SRT test feed containing a moving ball, then starts Braidpipe with `--uri` and `autovideosink`. A video window displaying the moving ball confirms that URI ingestion, `uridecodebin3`, and the passthrough branch are working together. Press Ctrl-C to stop the receiver; the script then stops the sender.

The script uses `--passthrough-only` because the AI frame bridge is not connected end to end yet. It is deliberately a manual check: a graphical sink cannot be asserted reliably in headless CI.

## Python worker

The sample worker maps the buffer created by Rust, receives frame metadata via a Unix-domain datagram socket, draws an overlay, marks the slot free, and replies with an acknowledgement.

```python
from shm import SharedMemoryManager

shm = SharedMemoryManager("/dev/shm/braidpipe_buffer")
frame = shm.get_slot_numpy_array(slot_idx)

# Modify the NumPy view in place.
shm.mark_slot_free(slot_idx)
```

The Python process is normally launched by the daemon. To run it on its own, first start a process that creates the shared-memory buffer and binds the Rust socket.

## Current limitations

- Frames are not yet copied from GStreamer `AppSink` into the shared-memory ring buffer.
- Processed frames are not yet fed from shared memory into GStreamer `AppSrc`.
- The watchdog presently switches branches according to Python socket availability; it does not yet enforce per-frame ACK deadlines.
- The default sink is `autovideosink`, so running it requires a graphical display server.

## License

Apache-2.0
