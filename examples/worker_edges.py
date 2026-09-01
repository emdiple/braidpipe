"""Edge-detection worker: a whole-frame pixel transform with no model and no network.

Where worker_detect.py *annotates* frames, this one *rewrites* them: the left
half of every frame is replaced with a Canny edge map. It is the example to
reach for when you want to check the plumbing — it needs nothing but OpenCV, runs
in a couple of milliseconds, and any change you make is impossible to miss on
screen.

Usage:
    cargo run -p braidpipe --release -- --python-script examples/worker_edges.py

    # or from another machine, against a daemon started with --worker-listen:
    BRAIDPIPE_DAEMON=192.168.1.10:7300 python3 examples/worker_edges.py

Environment:
    BRAIDPIPE_DAEMON        daemon's --worker-listen address; switches this
                            worker to the tcp-raw transport   (default: unset)
    BRAIDPIPE_EDGE_LOW      lower Canny threshold     (default: 80)
    BRAIDPIPE_EDGE_HIGH     upper Canny threshold     (default: 180)
    BRAIDPIPE_RUST_SOCK     daemon's ack socket       (default: /tmp/braidpipe_rust.sock)
    BRAIDPIPE_PYTHON_SOCK   this worker's socket      (default: /tmp/braidpipe_python.sock)
"""

import os

import cv2
import numpy as np

try:
    import braidpipe
except ModuleNotFoundError:  # run from a source checkout, package not installed
    import sys

    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")
    )
    import braidpipe

EDGE_LOW = int(os.environ.get("BRAIDPIPE_EDGE_LOW", "80"))
EDGE_HIGH = int(os.environ.get("BRAIDPIPE_EDGE_HIGH", "180"))

# Frames in shared memory are RGB, not BGR: this tuple is (R, G, B).
EDGE_TINT = np.array([255, 109, 14], dtype=np.float32) / 255.0


def apply_edges(frame: np.ndarray) -> None:
    """Replaces the left half of the frame with a tinted edge map, in place."""
    split = frame.shape[1] // 2
    left = frame[:, :split]

    # cv2.cvtColor wants a contiguous array; a column slice of the SHM view is
    # not one, so this copy is required rather than incidental.
    grey = cv2.cvtColor(np.ascontiguousarray(left), cv2.COLOR_RGB2GRAY)
    edges = cv2.Canny(grey, EDGE_LOW, EDGE_HIGH)

    # Broadcasting the tint over the mask writes straight back into the slot.
    left[:] = (edges[:, :, None] * EDGE_TINT).astype(np.uint8)

    # A one-pixel seam makes the boundary between processed and untouched pixels
    # obvious, which is what you actually want to see when testing failover.
    frame[:, split : split + 1] = (255, 109, 14)


if __name__ == "__main__":
    print(f"[edges] thresholds={EDGE_LOW}/{EDGE_HIGH}", flush=True)
    braidpipe.run(apply_edges, name="edges")
