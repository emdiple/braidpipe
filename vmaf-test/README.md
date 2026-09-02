# VMAF encoder quality test

Measures how much visual quality braidpipe's encode path costs, using
[VMAF](https://github.com/Netflix/vmaf). A source file is streamed over SRT
into a `--passthrough-only` daemon (no worker, encoder only), the SRT output
is captured, and the two are compared frame by frame.

## Requirements

- `ffmpeg` / `ffprobe` built with libvmaf (Homebrew's ffmpeg includes it)
- a built daemon: `cargo build --release`
- an H.264/H.265 source file — it is both what gets streamed in and the
  reference the output is scored against

## Usage

Self-contained (the script streams the source itself):

```bash
python3 vmaf-test/run_vmaf_test.py source.mp4
```

Against an already-running external SRT feed (must be a listener sending the
same file):

```bash
python3 vmaf-test/run_vmaf_test.py source.mp4 --feed 127.0.0.1:8890
```

Against a live non-SRT source (HTTP/UDP/RTP restream): no source file — the
incoming stream itself is recorded as the reference and the two captures are
aligned automatically by content. `--duration` is required, and the server
must accept two simultaneous clients (the daemon and the recorder):

```bash
python3 vmaf-test/run_vmaf_test.py --uri 'http://host:8000/play/ch1' --duration 60
```

Options: `--preset` (default `lowlatency`), `--latency` (SRT latency in ms,
both sides, default 200), `--duration N` (test only the first N seconds).
`BRAIDPIPE_*` environment variables are passed through to the daemon, and
anything after a standalone `--` becomes daemon CLI flags:

```bash
BRAIDPIPE_BITRATE_KBPS=16000 BRAIDPIPE_GOP_SECONDS=1 \
python3 vmaf-test/run_vmaf_test.py source.mp4 -- --fps 50 --width 1920 --height 1080
```

Streaming happens at real-time speed, so a run takes the clip's duration plus
scoring time.

## Output

Each run writes a folder under `runs/<timestamp>-<preset>/`:

- `report.md` — config, capture stats, pooled scores, verdict
- `distorted.ts` — the captured braidpipe output
- `vmaf.json` — per-frame libvmaf scores
- `daemon.log` — daemon output

The report's headline number is the *clean* mean: frames truncated by capture
shutdown at the very end of the stream are detected and excluded. Rough scale:
≥ 93 visually transparent, 80–93 noticeable on close inspection, < 80 clearly
visible.
