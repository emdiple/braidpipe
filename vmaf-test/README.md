# VMAF encoder quality test

Measures how much visual quality braidpipe's encode path costs, using
[VMAF](https://github.com/Netflix/vmaf). A source file is streamed over SRT
into a `--passthrough-only` daemon (no worker, encoder only), the SRT output
is captured, and the two are compared frame by frame.

## Requirements

- `ffmpeg` / `ffprobe` built with libvmaf. Homebrew's ffmpeg includes it;
  most Linux distro packages do **not** ("No such filter: 'libvmaf'") — grab
  a [static build](https://johnvansickle.com/ffmpeg/) and either put it on
  PATH or set `FFMPEG=/path/to/ffmpeg FFPROBE=/path/to/ffprobe`
- a built daemon: `cargo build --release`
- an H.264/H.265 source file — it is both what gets streamed in and the
  reference the output is scored against

## Usage

Self-contained (the script streams the source itself):

```bash
python3 vmaf-test/run_vmaf_test.py source.mp4
```

Against a streaming input that sends that same file (SRT, HTTP, UDP, RTP —
the file stays the VMAF reference, and the captures are aligned by content
automatically):

```bash
python3 vmaf-test/run_vmaf_test.py source.mp4 --uri 'srt://host:8890?mode=caller'
python3 vmaf-test/run_vmaf_test.py source.mp4 --feed 127.0.0.1:8890   # shorthand for an SRT listener feed
```

Against a live stream with no file behind it: the incoming stream itself is
recorded as the reference. `--duration` is required, and the server must
accept two simultaneous clients (the daemon and the recorder):

```bash
python3 vmaf-test/run_vmaf_test.py --uri 'http://host:8000/play/ch1' --duration 60
```

The daemon side is always `--passthrough-only` with the script's own SRT
output — only the encoding configuration is yours to vary. Audio is never
needed: VMAF scores video only.

Options: `--preset` (default `lowlatency`), `--latency` (SRT latency in ms,
both sides, default 200), `--duration N` (test only the first N seconds).
`BRAIDPIPE_*` environment variables are passed through to the daemon, and
anything after a standalone `--` becomes daemon CLI flags. A complete,
typical invocation — encoder config via env vars and flags, an SRT feed
sending the file, the file as reference:

```bash
# terminal 1: the feed (an SRT listener streaming the reference file)
ffmpeg -re -i ~/Videos/match_1080p50.mp4 -c copy -f mpegts \
  'srt://0.0.0.0:8890?mode=listener&latency=200'

# terminal 2: the test
BRAIDPIPE_BITRATE_KBPS=16000 BRAIDPIPE_GOP_SECONDS=1 BRAIDPIPE_SRT_LATENCY_MS=200 \
python3 vmaf-test/run_vmaf_test.py ~/Videos/match_1080p50.mp4 \
  --feed 127.0.0.1:8890 --latency 200 \
  -- --preset=lowlatency --width=1920 --height=1080 --fps=50
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
