"""The raw worker template: your code in `process()`, the SDK does the rest.

This is the file to copy when writing your own worker. The braidpipe package
owns the whole IPC contract — attach, one datagram per frame, a zero-copy NumPy
view of the slot, slot release, the ack — and an exception in `process()` costs
one passthrough frame, never the stream. The one thing this template does not
do is touch the pixels.

Demonstration workers live in examples/ — an edge transform, threaded YOLO
detection, and latency stamping.

Usage:
    cargo run -p braidpipe --release -- --python-script python/braidpipe/worker.py

Environment:
    BRAIDPIPE_DAEMON        a daemon's --worker-listen address; switches the
                            worker to the tcp-raw transport   (default: unset)
    BRAIDPIPE_RUST_SOCK     daemon's ack socket       (default: /tmp/braidpipe_rust.sock)
    BRAIDPIPE_PYTHON_SOCK   this worker's socket      (default: /tmp/braidpipe_python.sock)
"""

import numpy as np

try:
    import braidpipe
except ModuleNotFoundError:  # run from a source checkout, package not installed
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import braidpipe


def process(frame: np.ndarray) -> None:
    """Your code goes here. Modify `frame` in place; writing to the view *is*
    writing the output.

    Frames are RGB, not BGR. Finish inside 1.5 frame periods (50 ms at 30 fps)
    or the daemon falls back to passthrough; anything slower than that belongs
    on a thread with cached results — see braidpipe.BackgroundModel and
    examples/worker_detect.py. Take a second `ctx` parameter if you need frame
    ids or timestamps.
    """


if __name__ == "__main__":
    braidpipe.run(process)
