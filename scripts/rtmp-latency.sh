#!/usr/bin/env bash
#
# Publishes a stamped test pattern over RTMP and measures how late each frame
# arrives at a receiver.
#
# ffmpeg does double duty: `-listen 1` makes it the RTMP server braidpipe
# publishes to, and it decodes straight to raw frames on stdout, which removes
# a media server's own buffering from the number. That makes this a measurement
# of braidpipe plus the encoder plus RTMP framing -- a real server such as
# nginx-rtmp or MediaMTX will add its own, usually far larger, GOP buffer.
#
# Environment:
#   BRAIDPIPE_RTMP_PORT      RTMP listen port          (default: 1935)
#   BRAIDPIPE_RTMP_DURATION  seconds to measure        (default: 30)
#   BRAIDPIPE_RTMP_WIDTH     frame width               (default: 1280)
#   BRAIDPIPE_RTMP_HEIGHT    frame height              (default: 720)
#   BRAIDPIPE_RTMP_FPS       frame rate                (default: 30)
#   BRAIDPIPE_RTMP_ENCODER   x264 | vtenc              (default: x264)
#   BRAIDPIPE_RTMP_TUNED     low-latency sink settings (default: 1, 0 to compare)
#   BRAIDPIPE_RTMP_SINK      replace the sink outright (default: built from the above)
#   BRAIDPIPE_STAMP_BUSY_MS  fake worker cost, ms      (default: 0)

set -euo pipefail

port=${BRAIDPIPE_RTMP_PORT:-1935}
duration=${BRAIDPIPE_RTMP_DURATION:-30}
width=${BRAIDPIPE_RTMP_WIDTH:-1280}
height=${BRAIDPIPE_RTMP_HEIGHT:-720}
fps=${BRAIDPIPE_RTMP_FPS:-30}
encoder=${BRAIDPIPE_RTMP_ENCODER:-x264}
tuned=${BRAIDPIPE_RTMP_TUNED:-1}
export BRAIDPIPE_STAMP_BUSY_MS=${BRAIDPIPE_STAMP_BUSY_MS:-0}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)
cd "$repo_dir"

for tool in ffmpeg cargo lsof; do
    command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done

for element in videotestsrc videoconvert h264parse flvmux rtmp2sink; do
    gst-inspect-1.0 "$element" >/dev/null 2>&1 ||
        { echo "Missing GStreamer element: $element" >&2; exit 1; }
done

case "$encoder" in
    x264)
        encode='x264enc tune=zerolatency speed-preset=ultrafast bitrate=4000 key-int-max=30'
        ;;
    vtenc)
        encode="vtenc_h264 realtime=true allow-frame-reordering=false bitrate=4000 max-keyframe-interval=$fps"
        ;;
    *)
        echo "BRAIDPIPE_RTMP_ENCODER must be x264 or vtenc" >&2
        exit 1
        ;;
esac
gst-inspect-1.0 "${encode%% *}" >/dev/null 2>&1 ||
    { echo "Missing GStreamer element: ${encode%% *}" >&2; exit 1; }

if [[ "$tuned" == 1 ]]; then
    # Pinning I420 stops the encoder inheriting 4:4:4 from the RGB the AI branch
    # deals in, which is twice the samples for no benefit over RTMP.
    #
    # sync=false is the real one. A network sink that syncs holds each buffer
    # until its running time plus the pipeline's configured latency, and the
    # basesink processing-deadline alone contributes 20ms of that. The live
    # source already paces the pipeline, so the clock has nothing left to add.
    caps='video/x-raw,format=I420 ! '
    sink_props='sync=false'
else
    caps=''
    sink_props=''
fi

sink_desc="videoconvert ! ${caps}${encode} ! h264parse ! flvmux streamable=true ! \
rtmp2sink $sink_props location=rtmp://127.0.0.1:$port/live/stream"
sink_desc=${BRAIDPIPE_RTMP_SINK:-$sink_desc}

python_exe=python3
[[ -x .venv/bin/python3 ]] && python_exe=.venv/bin/python3

workdir=$(mktemp -d)
daemon_log="$workdir/braidpipe.log"
ffmpeg_pidfile="$workdir/ffmpeg.pid"

cleanup() {
    for pid in "${braidpipe_pid:-}" "${consumer_pid:-}"; do
        [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done
    [[ -s "$ffmpeg_pidfile" ]] && kill "$(cat "$ffmpeg_pidfile")" 2>/dev/null || true
    # The worker is a child of the daemon, not of this shell, so it outlives a
    # daemon that died rather than exited.
    pkill -f 'worker_stamp\.py$' 2>/dev/null || true
    rm -rf "$workdir"
}
trap cleanup EXIT INT TERM

echo "Building braidpipe (release) so compile time stays out of the measurement..."
cargo build -p braidpipe --release

echo "Starting RTMP listener on port $port..."
# ffmpeg and the probe are one pipeline rather than two processes joined by a
# FIFO: a FIFO open blocks until both ends arrive, so an ffmpeg that fails to
# start would wedge the probe instead of ending the run.
#
# -threads 1 matters. Frame-threaded decoding holds several frames before it
# emits the first, which would show up as latency braidpipe never caused.
{
    ffmpeg -hide_banner -loglevel error \
        -fflags nobuffer -flags low_delay -analyzeduration 0 -probesize 32 \
        -threads 1 \
        -listen 1 -i "rtmp://127.0.0.1:$port/live/stream" \
        -f rawvideo -pix_fmt rgb24 - &
    echo $! >"$ffmpeg_pidfile"
    wait $!
} | "$python_exe" scripts/rtmp_latency_probe.py --width "$width" --height "$height" &
consumer_pid=$!

# Watch for the listening socket rather than dialling it: ffmpeg accepts the
# first connection as the publisher, so a `nc -z` probe consumes the slot and
# the real publisher is met with "Unable to read handshake".
listening() { lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; }

for _ in $(seq 100); do
    listening && break
    sleep 0.1
done
listening || { echo "RTMP listener never came up" >&2; exit 1; }

echo "Publishing ${width}x${height}@${fps} for ${duration}s via $encoder..."
# The built binary directly, not `cargo run`: cargo would sit between us and the
# daemon and the SIGINT below would stop the wrapper rather than the pipeline.
./target/release/braidpipe \
    --source "videotestsrc is-live=true pattern=ball ! video/x-raw,width=$width,height=$height,framerate=$fps/1" \
    --sink "$sink_desc" \
    --python-script python/braidpipe/worker_stamp.py \
    --width "$width" --height "$height" --fps "$fps" >"$daemon_log" 2>&1 &
braidpipe_pid=$!

sleep "$duration"

if ! kill -0 "$braidpipe_pid" 2>/dev/null; then
    echo "braidpipe exited early:" >&2
    cat "$daemon_log" >&2
    exit 1
fi

echo "Stopping publisher..."
# The worker never sees the daemon's SIGINT -- it is a child, not a group member
# -- and it needs one of its own to print its summary. Signalling it by parent
# rather than by name matters: the daemon's own argv contains the script path,
# so `pkill -f worker_stamp.py` would take the daemon down first and the summary
# would be lost to the race.
pkill -INT -P "$braidpipe_pid" 2>/dev/null || true
sleep 0.5

kill -INT "$braidpipe_pid" 2>/dev/null || true
wait "$braidpipe_pid" 2>/dev/null || true

[[ -s "$ffmpeg_pidfile" ]] && kill "$(cat "$ffmpeg_pidfile")" 2>/dev/null || true

# The probe prints its summary once the pipe closes.
wait "$consumer_pid" 2>/dev/null || true
consumer_pid=

echo
grep '^\[stamp\] FINAL' "$daemon_log" || grep '^\[stamp\] daemon' "$daemon_log" | tail -1 || true
echo
echo "daemon->worker is SHM + UDS delivery; worker->received is everything after:"
echo "SHM read-back, appsrc, videoconvert, $encoder, flvmux, RTMP, demux and decode."
