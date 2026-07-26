#!/usr/bin/env bash
set -euo pipefail

if ! command -v gst-launch-1.0 >/dev/null; then
    echo "gst-launch-1.0 is required" >&2
    exit 1
fi

for element in videotestsrc x264enc mpegtsmux srtsink; do
    if ! gst-inspect-1.0 "$element" >/dev/null 2>&1; then
        echo "Missing GStreamer element: $element" >&2
        exit 1
    fi
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)
port=${BRAIDPIPE_E2E_SRT_PORT:-9000}

cleanup() {
    if [[ -n "${sender_pid:-}" ]]; then
        kill "$sender_pid" 2>/dev/null || true
        wait "$sender_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "Starting SRT test sender on port $port..."
gst-launch-1.0 -q \
    videotestsrc is-live=true pattern=ball ! \
    video/x-raw,width=640,height=360,framerate=30/1 ! \
    x264enc tune=zerolatency speed-preset=ultrafast key-int-max=30 ! \
    mpegtsmux ! \
    "srtsink uri=srt://127.0.0.1:$port?mode=listener" &
sender_pid=$!

sleep 1

echo "Starting Braidpipe. A moving ball should appear in an output window."
echo "Press Ctrl-C to stop the test."
cd "$repo_dir"
cargo run -p braidpipe --release -- \
    --uri "srt://127.0.0.1:$port?mode=caller" \
    --sink 'videoconvert ! autovideosink' \
    --passthrough-only
