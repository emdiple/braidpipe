#!/usr/bin/env python3
"""End-to-end VMAF test for braidpipe's encoder path.

Streams a source into a passthrough-only braidpipe daemon, captures the SRT
output, scores it against the reference with libvmaf, and writes a per-run
folder with the capture, raw scores, daemon log, and a markdown report.

Usage:
    python3 run_vmaf_test.py source.mp4                              # script streams the file itself
    python3 run_vmaf_test.py source.mp4 --feed 127.0.0.1:8890        # external SRT listener feed
    python3 run_vmaf_test.py source.mp4 --uri 'srt://host:8890?mode=caller'   # any input URI, file as reference
    python3 run_vmaf_test.py --uri 'http://host:8000/play/ch1' --duration 60  # live stream, no file

Requires: ffmpeg/ffprobe with libvmaf, and a built braidpipe binary.
"""

import argparse
import json
import os
import re
from collections import Counter
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAEMON_BIN = REPO_ROOT / "target" / "release" / "braidpipe"

# Point these at a different build when the system one lacks libvmaf
# (most Linux distro ffmpeg builds do).
FFMPEG = os.environ.get("FFMPEG", "ffmpeg")
FFPROBE = os.environ.get("FFPROBE", "ffprobe")

IN_PORT = 8890
OUT_PORT = 8891

# A frame scoring below this at the very end of the capture is shutdown
# truncation (the tail no longer lines up), not encoder behavior.
BROKEN_TAIL_THRESHOLD = 20.0

# Alignment probes: distorted frames matched against the reference by pixel
# difference. A match is trusted when a majority of probes imply the same
# frame offset - encoder distortion moves the absolute difference floor
# around, but only the true offset wins consistently.
ALIGN_PROBES = (10, 30, 50, 70, 90)


def ensure_libvmaf() -> None:
    try:
        filters = subprocess.run(
            [FFMPEG, "-hide_banner", "-filters"],
            capture_output=True, text=True,
        ).stdout
    except FileNotFoundError:
        sys.exit(f"'{FFMPEG}' not found - install ffmpeg, or point the FFMPEG "
                 "env var at a binary")
    if "libvmaf" not in filters:
        sys.exit(
            f"'{FFMPEG}' is built without libvmaf (distro packages usually are).\n"
            "Install a build that has it - e.g. the static build from\n"
            "https://johnvansickle.com/ffmpeg/ - and either put its ffmpeg/ffprobe\n"
            "on PATH or point the FFMPEG and FFPROBE env vars at them."
        )


def probe(path: Path, entries: str) -> str:
    # An MPEG-TS capture can list the same stream under two program entries;
    # keep the first line only.
    return subprocess.check_output(
        [FFPROBE, "-v", "error", "-select_streams", "v:0",
         "-show_entries", entries, "-of", "csv=p=0", str(path)],
        text=True,
    ).strip().splitlines()[0]


def probe_fps(path: Path) -> float:
    num, _, den = probe(path, "stream=r_frame_rate").partition("/")
    return float(num) / float(den or 1)


def count_frames(path: Path) -> int:
    return int(subprocess.check_output(
        [FFPROBE, "-v", "error", "-count_frames", "-select_streams", "v:0",
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
                         no_data_timeout: float = 15.0,
                         proc: subprocess.Popen | None = None) -> None:
    """Waits until the capture file is non-empty and has stopped growing for
    `quiet` seconds - or, if `proc` (the ffmpeg writing it) is given, until
    that process exits on its own. Fails fast if no data ever arrives or the
    daemon dies."""
    start = time.monotonic()
    deadline = start + timeout
    last_size, last_change = -1, time.monotonic()
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            return  # the writer finished; the file is final
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


def gray_frames(path: Path, count: int) -> list[bytes]:
    """Decodes the first `count` frames as 64x36 grayscale thumbnails."""
    raw = subprocess.run(
        [FFMPEG, "-v", "error", "-i", str(path), "-frames:v", str(count),
         "-vf", "scale=64:36", "-pix_fmt", "gray", "-f", "rawvideo", "-"],
        capture_output=True, check=True,
    ).stdout
    size = 64 * 36
    return [raw[i * size:(i + 1) * size] for i in range(len(raw) // size)]


def mean_abs_diff(a: bytes, b: bytes) -> float:
    return sum(abs(x - y) for x, y in zip(a, b)) / len(a)


def find_alignment(distorted: Path, reference: Path,
                   window: int = 600) -> tuple[int, int]:
    """Returns (distorted_start, reference_start): the frame indices at which
    the two captures show the same content. Needed when both sides recorded a
    live stream and connected at slightly different moments."""
    dist = gray_frames(distorted, max(ALIGN_PROBES) + 10)
    refs = gray_frames(reference, window)
    probes = [k for k in ALIGN_PROBES if k < len(dist)]
    if not probes or len(refs) < 30:
        raise RuntimeError("captures too short to align")

    offsets = []
    for k in probes:
        _, j = min((mean_abs_diff(dist[k], r), j) for j, r in enumerate(refs))
        offsets.append(j - k)
    offset, votes = Counter(offsets).most_common(1)[0]
    if votes * 2 <= len(probes):
        raise RuntimeError(
            f"could not align the captures (probe offsets disagree: {offsets}); "
            "are both recording the same stream?"
        )
    # The first captured frames of a mid-GOP join decode as garbage, so never
    # start the comparison right at frame 0.
    dist_start = max(10, -offset)
    return dist_start, dist_start + offset


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def codec_paths(log: Path) -> dict[str, str]:
    """Reads which video encoder/decoder the daemon picked and whether each
    runs on the GPU (hardware) or the CPU (software), from its log."""
    text = ANSI_RE.sub("", log.read_text()) if log.exists() else ""
    found = {}
    for role, kind, element in re.findall(
            r"Video (encoder|decoder) in use \((\w+)\).*?element=(\S+)", text):
        side = "GPU (hardware)" if kind == "hardware" else "CPU (software)"
        found[role] = f"{element} - {side}"
    return found


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
    ap.add_argument("source", type=Path, nargs="?", default=None,
                    help="reference video file (omit when using --uri)")
    ap.add_argument("--preset", default="lowlatency", help="braidpipe output preset")
    ap.add_argument("--latency", type=int, default=200,
                    help="SRT latency budget in ms, applied to both the "
                         "receiving and sending side (default: 200)")
    ap.add_argument("--feed", metavar="HOST:PORT", default=None,
                    help="use an already-running external SRT feed (listener) as the "
                         "input instead of streaming the source file; the source "
                         "argument is still the VMAF reference and must be the same "
                         "content the feed sends")
    ap.add_argument("--uri", default=None,
                    help="daemon input URI (srt://, http://, udp://, rtp://, ...). "
                         "With a source file the file is the VMAF reference and must "
                         "be the same content the stream sends (the captures are "
                         "aligned by content automatically). Without a source file "
                         "the incoming stream is recorded as the reference, which "
                         "requires --duration and a server that accepts two "
                         "simultaneous clients (the daemon and the recorder)")
    ap.add_argument("--duration", type=float, default=None,
                    help="seconds to stream/score (required with --uri; otherwise "
                         "defaults to the whole source file)")
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
                 "the input/output wiring and keep the worker out of the path")

    if args.uri and args.feed:
        sys.exit("--uri and --feed are mutually exclusive")
    if not args.uri and not args.source:
        sys.exit("a source file is required (or use --uri)")
    if args.uri and not args.source and not args.duration:
        sys.exit("--uri without a source file is treated as live; say how long "
                 "to record with --duration N")
    if args.source and not args.source.is_file():
        sys.exit(f"source not found: {args.source}")
    # Without a source file the incoming stream itself must be recorded to
    # serve as the reference.
    record_reference = bool(args.uri and not args.source)
    if not DAEMON_BIN.is_file():
        sys.exit(f"daemon binary not found: {DAEMON_BIN} (run: cargo build --release)")
    ensure_libvmaf()
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

    preset = args.preset
    for i, flag in enumerate(extras):
        if flag == "--preset":
            preset = extras[i + 1]
        elif flag.startswith("--preset="):
            preset = flag.split("=", 1)[1]
    started_at = datetime.now()
    run_dir = args.workdir / "runs" / f"{started_at:%Y%m%d-%H%M%S}-{preset}"
    run_dir.mkdir(parents=True)
    distorted = run_dir / "distorted.ts"
    vmaf_json = run_dir / "vmaf.json"
    daemon_log = run_dir / "daemon.log"
    report_md = run_dir / "report.md"
    reference = run_dir / "reference.ts" if record_reference else args.source

    duration = args.duration or float(probe(args.source, "format=duration"))
    env_vars = {k: v for k, v in sorted(os.environ.items()) if k.startswith("BRAIDPIPE_")}

    daemon = capture = recorder = None
    try:
        if args.uri:
            in_uri = args.uri
        elif args.feed:
            in_uri = f"srt://{args.feed}?mode=caller&latency={args.latency}"
        else:
            in_uri = f"srt://0.0.0.0:{IN_PORT}?mode=listener&latency={args.latency}"
        daemon_cmd = [str(DAEMON_BIN), "--passthrough-only",
                      "--uri", in_uri,
                      "--output", f"srt://0.0.0.0:{OUT_PORT}?mode=listener&latency={args.latency}"]
        if preset == args.preset and "--preset" not in " ".join(extras):
            daemon_cmd += ["--preset", args.preset]
        daemon_cmd += extras
        print(f"[1/5] starting daemon: {' '.join(daemon_cmd[1:])}")
        with open(daemon_log, "w") as log_fh:
            daemon = subprocess.Popen(
                daemon_cmd, cwd=REPO_ROOT,
                stdout=log_fh, stderr=subprocess.STDOUT,
            )
        wait_for_udp_port(OUT_PORT, daemon, daemon_log)

        if record_reference:
            print(f"[2/5] recording the input stream as reference "
                  f"({duration:.0f}s) and capturing the output")
            recorder = subprocess.Popen(
                [FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
                 "-i", args.uri, "-c", "copy", "-t", str(duration),
                 "-f", "mpegts", str(reference)],
            )
        else:
            print(f"[2/5] capturing output to {distorted.relative_to(args.workdir)}")
        capture_cmd = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
                       "-i", f"srt://127.0.0.1:{OUT_PORT}?mode=caller&latency={args.latency}",
                       "-c", "copy"]
        if args.uri:
            capture_cmd += ["-t", str(duration)]
        capture = subprocess.Popen(capture_cmd + [str(distorted)])

        if args.uri:
            print(f"[3/5] streaming from {args.uri} for {duration:.0f}s")
            wait_capture_drained(distorted, daemon, daemon_log,
                                 timeout=duration + 60, proc=capture)
            if recorder:
                wait_capture_drained(reference, daemon, daemon_log,
                                     timeout=duration + 60, proc=recorder)
        elif args.feed:
            print(f"[3/5] using external feed at {args.feed}; "
                  f"waiting for the stream to end (~{duration:.0f}s)")
            wait_capture_drained(distorted, daemon, daemon_log, timeout=duration + 60)
        else:
            print(f"[3/5] streaming {args.source.name} ({duration:.1f}s at real-time speed)")
            feed_cmd = [FFMPEG, "-hide_banner", "-loglevel", "error", "-re"]
            if args.duration:
                feed_cmd += ["-t", str(args.duration)]
            feed_cmd += ["-i", str(args.source), "-c", "copy", "-f", "mpegts",
                         f"srt://127.0.0.1:{IN_PORT}?mode=caller&latency={args.latency}"]
            subprocess.run(feed_cmd, check=True)
            wait_capture_drained(distorted, daemon, daemon_log, timeout=duration + 60)

        stop_process(capture, signal.SIGINT)
        stop_process(recorder, signal.SIGINT)
        stop_process(daemon, signal.SIGTERM)

        for path, what in ((distorted, "output capture"),
                           (reference, "reference recording")):
            if not path.is_file() or path.stat().st_size == 0:
                raise RuntimeError(
                    f"the {what} is empty - the stream never reached it; "
                    "daemon log tail:\n"
                    + "\n".join(daemon_log.read_text().splitlines()[-10:])
                )
        captured_frames = count_frames(distorted)
        expected_frames = round(duration * probe_fps(reference))
        if args.uri:
            ref_frames = count_frames(reference)
            out_start, ref_start = find_alignment(distorted, reference)
            usable = min(captured_frames - out_start, ref_frames - ref_start)
            print(f"[4/5] captured {captured_frames} frames; aligned at "
                  f"output+{out_start}/reference+{ref_start}; scoring {usable} frames")
        else:
            out_start = ref_start = 0
            usable = captured_frames
            print(f"[4/5] captured {captured_frames}/{expected_frames} frames; "
                  "scoring with libvmaf")
        subprocess.run(
            [FFMPEG, "-hide_banner", "-loglevel", "error", "-nostats",
             "-i", str(distorted), "-i", str(reference),
             "-lavfi",
             f"[0:v]trim=start_frame={out_start}:end_frame={out_start + usable},"
             "setpts=PTS-STARTPTS[d];"
             f"[1:v]trim=start_frame={ref_start}:end_frame={ref_start + usable},"
             "setpts=PTS-STARTPTS[r];"
             f"[d][r]libvmaf=log_path={vmaf_json}:log_fmt=json:"
             f"n_threads={os.cpu_count() or 4}",
             "-f", "null", "-"],
            check=True,
        )

        print("[5/5] writing report")
        codecs = codec_paths(daemon_log)
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

        source_label = args.uri or f"`{args.source}`"
        align_note = (
            f"Captures aligned automatically: output frame {out_start} matches "
            f"reference frame {ref_start}." if args.uri else "No alignment needed."
        )
        report = f"""# braidpipe VMAF report

| | |
| --- | --- |
| Date | {started_at:%Y-%m-%d %H:%M:%S} |
| Source | {source_label} |
| Resolution | {probe(reference, 'stream=width,height').replace(',', 'x')} @ {probe_fps(reference):g} fps |
| Streamed duration | {duration:.1f} s |
| Encoder | {codecs.get('encoder', 'not reported in the daemon log')} |
| Decoder | {codecs.get('decoder', 'not reported in the daemon log')} |
| Verdict | **{verdict(clean['mean'])}** |

## Configuration

```
{' '.join(daemon_cmd)}
```

Environment: {', '.join(f'`{k}={v}`' for k, v in env_vars.items()) or 'no BRAIDPIPE_* variables set'}

SRT latency: {args.latency} ms on both the receiving and sending side.

## Capture

{captured_frames} of ~{expected_frames} frames captured. {align_note}
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
{'- `reference.ts` - the recorded input stream (the VMAF reference)' + chr(10) if record_reference else ''}- `vmaf.json` - per-frame libvmaf scores
- `daemon.log` - braidpipe daemon output
"""
        report_md.write_text(report)

        print()
        print(f"VMAF mean {clean['mean']:.2f} (harmonic {clean['harmonic_mean']:.2f}, "
              f"min {clean['min']:.2f}) over {clean['frames']} frames")
        print(f"encoder: {codecs.get('encoder', 'unknown')}  |  "
              f"decoder: {codecs.get('decoder', 'unknown')}")
        print(f"-> {verdict(clean['mean'])}")
        print(f"report: {report_md}")
        return 0
    finally:
        for proc in (capture, recorder, daemon):
            if proc and proc.poll() is None:
                proc.kill()


if __name__ == "__main__":
    sys.exit(main())
