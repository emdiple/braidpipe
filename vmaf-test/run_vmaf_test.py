#!/usr/bin/env python3
"""End-to-end VMAF test for braidpipe's encoder path.

Streams a source file into a passthrough-only braidpipe daemon over SRT,
captures the SRT output, scores it against the source with libvmaf, and
writes a per-run folder with the capture, raw scores, daemon log, and a
markdown report.

Usage:
    python3 run_vmaf_test.py yoursource.mp4
    python3 run_vmaf_test.py yoursource.mp4 --duration 40 -- --fps 50 --width 1920 --height 1080

Requires: ffmpeg/ffprobe with libvmaf, and a built braidpipe binary.
"""

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAEMON_BIN = REPO_ROOT / "target" / "release" / "braidpipe"

IN_PORT = 8890
OUT_PORT = 8891

# A frame scoring below this at the very end of the capture is shutdown
# truncation (the tail no longer lines up), not encoder behavior.
BROKEN_TAIL_THRESHOLD = 20.0


def probe(path: Path, entries: str) -> str:
    return subprocess.check_output(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", entries, "-of", "csv=p=0", str(path)],
        text=True,
    ).strip()


def probe_fps(path: Path) -> float:
    num, _, den = probe(path, "stream=r_frame_rate").partition("/")
    return float(num) / float(den or 1)


def count_frames(path: Path) -> int:
    return int(subprocess.check_output(
        ["ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0",
         "-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", str(path)],
        text=True,
    ).strip().splitlines()[0])


def wait_for_udp_port(port: int, daemon: subprocess.Popen, log: Path,
                      timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if daemon.poll() is not None:
            raise RuntimeError(
                f"daemon exited with code {daemon.returncode} before opening "
                f"port {port}; its output:\n{log.read_text()}"
            )
        listening = subprocess.run(
            ["lsof", "-nP", f"-iUDP:{port}"], capture_output=True
        ).returncode == 0
        if listening:
            return
        time.sleep(0.3)
    raise RuntimeError(f"daemon never opened UDP port {port}; see {log}")


def wait_capture_drained(path: Path, daemon: subprocess.Popen, log: Path,
                         quiet: float = 3.0, timeout: float = 60.0,
                         no_data_timeout: float = 15.0) -> None:
    """Waits until the capture file is non-empty and has stopped growing for
    `quiet` seconds. Fails fast if no data ever arrives or the daemon dies."""
    start = time.monotonic()
    deadline = start + timeout
    last_size, last_change = -1, time.monotonic()
    while time.monotonic() < deadline:
        size = path.stat().st_size if path.exists() else 0
        if size == 0:
            if daemon.poll() is not None:
                raise RuntimeError(
                    f"daemon exited (code {daemon.returncode}) before producing "
                    f"any output; its log:\n{log.read_text()}"
                )
            if time.monotonic() - start > no_data_timeout:
                raise RuntimeError(
                    f"no output data arrived within {no_data_timeout:.0f}s - "
                    "is the input feed still up and sending? daemon log tail:\n"
                    + "\n".join(log.read_text().splitlines()[-10:])
                )
        if size != last_size:
            last_size, last_change = size, time.monotonic()
        elif size > 0 and time.monotonic() - last_change >= quiet:
            return
        time.sleep(0.25)
    raise RuntimeError(f"capture never went quiet within {timeout:.0f}s")


def stop_process(proc: subprocess.Popen | None, sig: int = signal.SIGINT,
                 timeout: float = 10.0) -> None:
    """Signals a process and escalates to SIGKILL if it does not exit."""
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(sig)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def pool(scores: list[float]) -> dict:
    return {
        "frames": len(scores),
        "mean": statistics.mean(scores),
        "harmonic_mean": statistics.harmonic_mean(scores),
        "min": min(scores),
        "max": max(scores),
    }


def verdict(mean: float) -> str:
    if mean >= 93:
        return "visually transparent - the encode is indistinguishable from the source"
    if mean >= 80:
        return "good - differences noticeable only on close inspection"
    if mean >= 60:
        return "fair - visible quality loss"
    return "poor - clearly visible degradation"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("source", type=Path, help="reference video file")
    ap.add_argument("--preset", default="lowlatency", help="braidpipe output preset")
    ap.add_argument("--latency", type=int, default=200,
                    help="SRT latency budget in ms, applied to both the "
                         "receiving and sending side (default: 200)")
    ap.add_argument("--feed", metavar="HOST:PORT", default=None,
                    help="use an already-running external SRT feed (listener) as the "
                         "input instead of streaming the source file; the source "
                         "argument is still the VMAF reference and must be the same "
                         "content the feed sends")
    ap.add_argument("--duration", type=float, default=None,
                    help="only stream/score the first N seconds of the source")
    ap.add_argument("--workdir", type=Path, default=Path(__file__).resolve().parent,
                    help="runs are created under <workdir>/runs/")
    ap.epilog = ("everything after a standalone -- is passed to the braidpipe "
                 "daemon verbatim, e.g.: -- --fps 50 --width 1920 --height 1080")

    argv = sys.argv[1:]
    extras: list[str] = []
    if "--" in argv:
        split = argv.index("--")
        argv, extras = argv[:split], argv[split + 1:]
    args = ap.parse_args(argv)
    owned = {"--uri", "--source", "--output", "--passthrough-only"} & set(extras)
    if owned:
        sys.exit(f"the test script owns {', '.join(sorted(owned))} - it must control "
                 "the SRT ports and keep the worker out of the path")

    if not args.source.is_file():
        sys.exit(f"source not found: {args.source}")
    if not DAEMON_BIN.is_file():
        sys.exit(f"daemon binary not found: {DAEMON_BIN} (run: cargo build --release)")
    if args.feed:
        host, _, port = args.feed.rpartition(":")
        if not port.isdigit():
            sys.exit(f"--feed expects HOST:PORT, got: {args.feed}")
        if host in ("127.0.0.1", "localhost", "0.0.0.0"):
            up = subprocess.run(["lsof", "-nP", f"-iUDP:{port}"],
                                capture_output=True).returncode == 0
            if not up:
                sys.exit(f"nothing is listening on UDP port {port} - start your "
                         "SRT feed first (it must be an SRT listener)")

    preset = extras[extras.index("--preset") + 1] if "--preset" in extras else args.preset
    started_at = datetime.now()
    run_dir = args.workdir / "runs" / f"{started_at:%Y%m%d-%H%M%S}-{preset}"
    run_dir.mkdir(parents=True)
    distorted = run_dir / "distorted.ts"
    vmaf_json = run_dir / "vmaf.json"
    daemon_log = run_dir / "daemon.log"
    report_md = run_dir / "report.md"

    duration = args.duration or float(probe(args.source, "format=duration"))
    env_vars = {k: v for k, v in sorted(os.environ.items()) if k.startswith("BRAIDPIPE_")}

    daemon = capture = None
    try:
        if args.feed:
            in_uri = f"srt://{args.feed}?mode=caller&latency={args.latency}"
        else:
            in_uri = f"srt://0.0.0.0:{IN_PORT}?mode=listener&latency={args.latency}"
        daemon_cmd = [str(DAEMON_BIN), "--passthrough-only",
                      "--uri", in_uri,
                      "--output", f"srt://0.0.0.0:{OUT_PORT}?mode=listener&latency={args.latency}"]
        if "--preset" not in extras:
            daemon_cmd += ["--preset", args.preset]
        daemon_cmd += extras
        print(f"[1/5] starting daemon: {' '.join(daemon_cmd[1:])}")
        with open(daemon_log, "w") as log_fh:
            daemon = subprocess.Popen(
                daemon_cmd, cwd=REPO_ROOT,
                stdout=log_fh, stderr=subprocess.STDOUT,
            )
        wait_for_udp_port(OUT_PORT, daemon, daemon_log)

        print(f"[2/5] capturing output to {distorted.relative_to(args.workdir)}")
        capture = subprocess.Popen(
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
             "-i", f"srt://127.0.0.1:{OUT_PORT}?mode=caller&latency={args.latency}",
             "-c", "copy", str(distorted)],
        )

        if args.feed:
            print(f"[3/5] using external feed at {args.feed}; "
                  f"waiting for the stream to end (~{duration:.0f}s)")
        else:
            print(f"[3/5] streaming {args.source.name} ({duration:.1f}s at real-time speed)")
            feed_cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-re"]
            if args.duration:
                feed_cmd += ["-t", str(args.duration)]
            feed_cmd += ["-i", str(args.source), "-c", "copy", "-f", "mpegts",
                         f"srt://127.0.0.1:{IN_PORT}?mode=caller&latency={args.latency}"]
            subprocess.run(feed_cmd, check=True)

        wait_capture_drained(distorted, daemon, daemon_log, timeout=duration + 60)
        stop_process(capture, signal.SIGINT)
        stop_process(daemon, signal.SIGTERM)

        captured_frames = count_frames(distorted)
        expected_frames = round(duration * probe_fps(args.source))
        print(f"[4/5] captured {captured_frames}/{expected_frames} frames; scoring with libvmaf")
        subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostats",
             "-i", str(distorted), "-i", str(args.source),
             "-lavfi",
             "[0:v]setpts=PTS-STARTPTS[d];"
             f"[1:v]trim=end_frame={captured_frames},setpts=PTS-STARTPTS[r];"
             f"[d][r]libvmaf=log_path={vmaf_json}:log_fmt=json:"
             f"n_threads={os.cpu_count() or 4}",
             "-f", "null", "-"],
            check=True,
        )

        print("[5/5] writing report")
        frames = json.loads(vmaf_json.read_text())["frames"]
        scores = [f["metrics"]["vmaf"] for f in frames]

        # Split off the shutdown-truncated tail: trailing frames that score
        # near zero because the capture ended mid-stream, not because the
        # encoder failed.
        tail = 0
        while tail < len(scores) and scores[-1 - tail] < BROKEN_TAIL_THRESHOLD:
            tail += 1
        clean_scores = scores[:-tail] if tail else scores
        raw, clean = pool(scores), pool(clean_scores)
        worst = sorted(enumerate(clean_scores), key=lambda kv: kv[1])[:5]

        report = f"""# braidpipe VMAF report

| | |
| --- | --- |
| Date | {started_at:%Y-%m-%d %H:%M:%S} |
| Source | `{args.source}` |
| Resolution | {probe(args.source, 'stream=width,height').replace(',', 'x')} @ {probe_fps(args.source):g} fps |
| Streamed duration | {duration:.1f} s |
| Verdict | **{verdict(clean['mean'])}** |

## Configuration

```
{' '.join(daemon_cmd)}
```

Environment: {', '.join(f'`{k}={v}`' for k, v in env_vars.items()) or 'no BRAIDPIPE_* variables set'}

SRT latency: {args.latency} ms on both the receiving and sending side.

## Capture

{captured_frames} of ~{expected_frames} frames captured.
{f'{tail} trailing frames were cut off by capture shutdown and are excluded from the clean pooling below.' if tail else 'No truncated tail detected.'}

## Scores

| Pooling | Frames | Mean | Harmonic mean | Min | Max |
| --- | --- | --- | --- | --- | --- |
| Clean (tail excluded) | {clean['frames']} | **{clean['mean']:.2f}** | {clean['harmonic_mean']:.2f} | {clean['min']:.2f} | {clean['max']:.2f} |
| Raw (all frames) | {raw['frames']} | {raw['mean']:.2f} | {raw['harmonic_mean']:.2f} | {raw['min']:.2f} | {raw['max']:.2f} |

Worst frames (clean): {', '.join(f'#{i} = {s:.1f}' for i, s in worst)}

Reference scale: >= 93 visually transparent, 80-93 noticeable on close
inspection, below 80 clearly visible.

## Files

- `distorted.ts` - the captured braidpipe output
- `vmaf.json` - per-frame libvmaf scores
- `daemon.log` - braidpipe daemon output
"""
        report_md.write_text(report)

        print()
        print(f"VMAF mean {clean['mean']:.2f} (harmonic {clean['harmonic_mean']:.2f}, "
              f"min {clean['min']:.2f}) over {clean['frames']} frames")
        print(f"-> {verdict(clean['mean'])}")
        print(f"report: {report_md}")
        return 0
    finally:
        for proc in (capture, daemon):
            if proc and proc.poll() is None:
                proc.kill()


if __name__ == "__main__":
    sys.exit(main())
