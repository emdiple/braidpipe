"""Instrumentation worker: writes a clock barcode into every frame.

This is not a transform, it is a measuring stick. Each frame leaves with the
wall clock of the moment the worker touched it printed across the top row, so a
receiver on the far end of the encoder and the network can subtract that from
its own clock and get the real one-way latency of everything in between.

It also reports the leg it can see for itself: the daemon stamps each frame
with the time it handed it over (`ctx.timestamp_us`), so the gap between that
and the worker waking up is the IPC delivery cost.

Used by scripts/rtmp-latency.sh, but it works against any sink.

Usage:
    cargo run -p braidpipe --release -- --python-script examples/worker_stamp.py

Environment:
    BRAIDPIPE_STAMP_BUSY_MS   fake per-frame workload    (default: 0)
    BRAIDPIPE_STAMP_REPORT    frames between reports     (default: 150)
    BRAIDPIPE_RUST_SOCK       daemon's ack socket        (default: /tmp/braidpipe_rust.sock)
    BRAIDPIPE_PYTHON_SOCK     this worker's socket       (default: /tmp/braidpipe_python.sock)
"""

import os
import sys
import time

import numpy as np

try:
    import braidpipe
except ModuleNotFoundError:  # run from a source checkout, package not installed
    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")
    )
    import braidpipe

from stamp import encode

BUSY_MS = float(os.environ.get("BRAIDPIPE_STAMP_BUSY_MS", "0"))
REPORT_EVERY = int(os.environ.get("BRAIDPIPE_STAMP_REPORT", "150"))


def percentiles(samples: list[float]) -> str:
    ordered = sorted(samples)
    last = len(ordered) - 1

    def at(fraction: float) -> float:
        return ordered[min(last, int(last * fraction))]

    return (
        f"p50={at(0.50):6.2f}ms  p90={at(0.90):6.2f}ms  "
        f"p99={at(0.99):6.2f}ms  max={ordered[-1]:6.2f}ms  n={len(ordered)}"
    )


def burn(milliseconds: float) -> None:
    """Occupies the worker for a set time without sleeping.

    A sleep would leave the CPU free and understate what a real model costs the
    rest of the machine, which is half of what makes a worker miss its deadline.
    """
    until = time.perf_counter() + milliseconds / 1000.0
    while time.perf_counter() < until:
        pass


ipc_ms: list[float] = []


def process(frame: np.ndarray, ctx: braidpipe.FrameContext) -> None:
    now_us = time.time_ns() // 1000
    ipc_ms.append((now_us - ctx.timestamp_us) / 1000.0)

    if BUSY_MS:
        burn(BUSY_MS)
    # Stamped last, so the barcode carries the moment the frame was actually
    # handed back rather than the moment work began.
    encode(frame, time.time_ns() // 1000)

    if REPORT_EVERY and len(ipc_ms) % REPORT_EVERY == 0:
        print(f"[stamp] daemon->worker  {percentiles(ipc_ms)}", flush=True)


if __name__ == "__main__":
    print(f"[stamp] busy={BUSY_MS}ms", flush=True)
    braidpipe.run(process, name="stamp")
    if ipc_ms:
        print(f"[stamp] FINAL daemon->worker  {percentiles(ipc_ms)}", flush=True)
