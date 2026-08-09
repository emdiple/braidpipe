#!/usr/bin/env python3
"""Reads raw frames from stdin and reports how old each one was on arrival.

Fed by ffmpeg decoding the RTMP stream braidpipe published, so the only clock
arithmetic is `now - the barcode worker_stamp.py wrote into the pixels`. Both
ends are on the same machine and both read CLOCK_REALTIME, so the difference is
a true one-way measurement rather than a halved round trip.

What it covers: worker hand-back -> SHM read -> appsrc -> videoconvert ->
encoder -> flvmux -> RTMP -> ffmpeg demux and decode. Everything from the moment
the worker was done with the frame to the moment a receiver could display it.

Usage:
    ffmpeg ... -f rawvideo -pix_fmt rgb24 - | rtmp_latency_probe.py --width W --height H
"""

import argparse
import os
import signal
import sys
import time

import numpy as np

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python", "braidpipe"),
)
from stamp import PAYLOAD_MASK, decode  # noqa: E402


def summarise(name: str, samples: list[float], unit: str = "ms") -> str:
    if not samples:
        return f"{name}: no samples"

    ordered = sorted(samples)
    last = len(ordered) - 1

    def at(fraction: float) -> float:
        return ordered[min(last, int(last * fraction))]

    return (
        f"{name:<22} min={ordered[0]:6.2f}  p50={at(0.50):6.2f}  "
        f"p90={at(0.90):6.2f}  p99={at(0.99):6.2f}  max={ordered[-1]:6.2f}  "
        f"({unit}, n={len(ordered)})"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--width", type=int, required=True)
    parser.add_argument("--height", type=int, required=True)
    parser.add_argument(
        "--report-every",
        type=int,
        default=150,
        help="frames between progress lines on stderr (0 to silence)",
    )
    args = parser.parse_args()

    frame_bytes = args.width * args.height * 3
    stdin = sys.stdin.buffer

    latency_ms: list[float] = []
    interval_ms: list[float] = []
    decoded = 0
    unreadable = 0
    previous_arrival = None

    # ffmpeg is killed out from under us at the end of a run; that is the normal
    # way this process finishes, not an error worth a traceback.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    try:
        while True:
            raw = stdin.read(frame_bytes)
            if len(raw) < frame_bytes:
                break

            arrival_us = time.time_ns() // 1000
            frame = np.frombuffer(raw, dtype=np.uint8).reshape(
                args.height, args.width, 3
            )

            if previous_arrival is not None:
                interval_ms.append((arrival_us - previous_arrival) / 1000.0)
            previous_arrival = arrival_us

            stamped_us = decode(frame)
            if stamped_us is None:
                # No barcode: either the decoder has not caught a keyframe yet,
                # or braidpipe is in passthrough and these are untouched frames.
                unreadable += 1
                continue

            decoded += 1
            # The barcode carries only the low bits of the clock, so the
            # subtraction has to happen in that same width. Latency is small and
            # positive, so masking the difference also handles the wrap for free.
            latency_ms.append(((arrival_us - stamped_us) & PAYLOAD_MASK) / 1000.0)

            if args.report_every and decoded % args.report_every == 0:
                print(summarise("worker->received", latency_ms), file=sys.stderr)

    except KeyboardInterrupt:
        pass

    print()
    print(f"frames received : {decoded + unreadable}")
    print(f"  with barcode  : {decoded}")
    print(f"  unreadable    : {unreadable}  (pre-keyframe, or passthrough frames)")
    print()
    print(summarise("worker->received", latency_ms))
    print(summarise("arrival interval", interval_ms))

    return 0 if decoded else 1


if __name__ == "__main__":
    sys.exit(main())
