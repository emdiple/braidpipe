#!/usr/bin/env bash
#
# Measures what each preset actually puts on the wire, as opposed to what its
# bitrate target promises.
#
# The default content ('camera') is a moving scene blended with 30% white
# noise -- a stand-in for real footage, where sensor noise rides on structure.
# It is hard enough to force rate control against its cap, which is where
# burst behaviour and VBV limits become visible, without being pure entropy.
#
# Any videotestsrc pattern works too. Two instructive ones: 'ball' (easy
# content -- shows that x264 does not spend bits it does not need) and 'snow'
# (pure noise -- shows an x264 rate-control quirk: without tune=zerolatency,
# mbtree decides noise has no predictive value and spends almost nothing on
# it, so the 'bandwidth' preset undershoots massively there by design).
#
# The stream is captured with ffmpeg -listen and the numbers are computed from
# the container packets themselves: bytes per one-second bucket, worst 250 ms
# burst, and the frame rate that survived. The first two seconds are dropped
# from the buckets -- caps, the first keyframe and rate-control ramp-up say
# nothing about steady state.
#
# Environment:
#   BRAIDPIPE_BW_PORT      RTMP listen port     (default: 1935)
#   BRAIDPIPE_BW_DURATION  seconds per preset   (default: 20)
#   BRAIDPIPE_BW_PRESETS   space-separated list (default: all four)
#   BRAIDPIPE_BW_PATTERN   'camera' or a videotestsrc pattern (default: camera)
#   BRAIDPIPE_BW_WIDTH     frame width          (default: 1280)
#   BRAIDPIPE_BW_HEIGHT    frame height         (default: 720)
#   BRAIDPIPE_BW_FPS       frame rate           (default: 30)

set -euo pipefail

port=${BRAIDPIPE_BW_PORT:-1935}
duration=${BRAIDPIPE_BW_DURATION:-20}
presets=${BRAIDPIPE_BW_PRESETS:-"zerolatency lowlatency balanced bandwidth"}
pattern=${BRAIDPIPE_BW_PATTERN:-camera}
width=${BRAIDPIPE_BW_WIDTH:-1280}
height=${BRAIDPIPE_BW_HEIGHT:-720}
fps=${BRAIDPIPE_BW_FPS:-30}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)
cd "$repo_dir"

for tool in ffmpeg ffprobe cargo lsof; do
    command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done

python_exe=python3
[[ -x .venv/bin/python3 ]] && python_exe=.venv/bin/python3

workdir=$(mktemp -d)

cleanup() {
    for pid in "${braidpipe_pid:-}" "${ffmpeg_pid:-}"; do
        [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done
    pkill -f 'worker_stamp\.py$' 2>/dev/null || true
    rm -rf "$workdir"
}
trap cleanup EXIT INT TERM

echo "Building braidpipe (release)..."
cargo build -p braidpipe --release

listening() { lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; }

caps="video/x-raw,width=$width,height=$height,framerate=$fps/1"
if [[ "$pattern" == camera ]]; then
    gst-inspect-1.0 compositor >/dev/null 2>&1 ||
        { echo "Missing GStreamer element: compositor" >&2; exit 1; }
    # The compositor's output chain comes last: the daemon appends its tee to
    # the end of the source fragment.
    source="videotestsrc is-live=true pattern=ball ! $caps ! comp.sink_0 \
videotestsrc is-live=true pattern=snow ! $caps ! comp.sink_1 \
compositor name=comp sink_1::alpha=0.3 ! $caps"
else
    source="videotestsrc is-live=true pattern=$pattern ! $caps"
fi

analyse() { # <capture.flv> <target_kbps>
    ffprobe -v error -show_entries packet=codec_type,size,pts_time -of csv "$1" |
        "$python_exe" -c "
import sys
from collections import defaultdict

target = float(sys.argv[1])
sec = defaultdict(int)      # 1 s buckets, bytes
burst = defaultdict(int)    # 250 ms buckets, bytes
frames = []
for line in sys.stdin:
    # ffprobe csv field order is fixed by the packet section, not by the
    # -show_entries order: codec_type, pts_time, size
    parts = line.strip().split(',')
    if len(parts) < 4 or parts[2] in ('N/A', ''):
        continue
    _, kind, pts, size = parts[:4]
    t = float(pts)
    if t < 2.0:
        continue
    sec[int(t)] += int(size)
    burst[int(t * 4)] += int(size)
    if kind == 'video':
        frames.append(t)

if len(sec) < 3:
    sys.exit('capture too short to analyse')
# the last buckets of each granularity are partial
rates = sorted(v * 8 / 1000 for v in list(sec.values())[:-1])
peaks = sorted(v * 8 / 1000 / 0.25 for v in list(burst.values())[:-1])
mean = sum(rates) / len(rates)
p95 = rates[int((len(rates) - 1) * 0.95)]
fps = (len(frames) - 1) / (frames[-1] - frames[0])
print(f'    target={target:.0f}kbps  mean={mean:.0f}  1s-p95={p95:.0f}  '
      f'1s-max={rates[-1]:.0f}  250ms-peak={peaks[-1]:.0f}kbps  fps={fps:.1f}')
" "$2"
}

for preset in $presets; do
    capture="$workdir/$preset.flv"
    ffmpeg -hide_banner -loglevel error -listen 1 \
        -i "rtmp://127.0.0.1:$port/live/stream" -c copy -y "$capture" &
    ffmpeg_pid=$!

    for _ in $(seq 100); do listening && break; sleep 0.1; done
    listening || { echo "RTMP listener never came up" >&2; exit 1; }

    echo "Measuring preset '$preset' ($pattern, ${width}x${height}@${fps}, ${duration}s)..."
    ./target/release/braidpipe \
        --source "$source" \
        --preset "$preset" --output "rtmp://127.0.0.1:$port/live/stream" \
        --python-script python/braidpipe/worker_stamp.py \
        --width "$width" --height "$height" --fps "$fps" \
        >"$workdir/$preset.log" 2>&1 &
    braidpipe_pid=$!

    sleep "$duration"

    if ! kill -0 "$braidpipe_pid" 2>/dev/null; then
        echo "braidpipe exited early:" >&2
        cat "$workdir/$preset.log" >&2
        exit 1
    fi

    # Worker first (it is the daemon's child, see rtmp-latency.sh), then daemon.
    pkill -INT -P "$braidpipe_pid" 2>/dev/null || true
    sleep 0.3
    kill -INT "$braidpipe_pid" 2>/dev/null || true
    wait "$braidpipe_pid" 2>/dev/null || true
    braidpipe_pid=

    kill "$ffmpeg_pid" 2>/dev/null || true
    wait "$ffmpeg_pid" 2>/dev/null || true
    ffmpeg_pid=

    target=$(grep -o 'bitrate=[0-9]*' "$workdir/$preset.log" | head -1 | cut -d= -f2)
    analyse "$capture" "${target:-0}"
done
