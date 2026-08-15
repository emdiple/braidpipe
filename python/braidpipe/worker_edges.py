"""Edge-detection worker: a whole-frame pixel transform with no model and no network.

Where worker_detect.py *annotates* frames, this one *rewrites* them: the left
half of every frame is replaced with a Canny edge map. It is the example to
reach for when you want to check the plumbing — it needs nothing but OpenCV, runs
in a couple of milliseconds, and any change you make is impossible to miss on
screen.

Usage:
    cargo run -p braidpipe --release -- --python-script python/braidpipe/worker_edges.py

Environment:
    BRAIDPIPE_EDGE_LOW      lower Canny threshold     (default: 80)
    BRAIDPIPE_EDGE_HIGH     upper Canny threshold     (default: 180)
    BRAIDPIPE_RUST_SOCK     daemon's ack socket       (default: /tmp/braidpipe_rust.sock)
    BRAIDPIPE_PYTHON_SOCK   this worker's socket      (default: /tmp/braidpipe_python.sock)
"""

import json
import os
import socket
import time

import cv2
import numpy as np
from shm import attach

RUST_SOCK = os.environ.get("BRAIDPIPE_RUST_SOCK", "/tmp/braidpipe_rust.sock")
PYTHON_SOCK = os.environ.get("BRAIDPIPE_PYTHON_SOCK", "/tmp/braidpipe_python.sock")
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


def run_worker(rust_sock_path: str, python_sock_path: str) -> None:
    if os.path.exists(python_sock_path):
        os.remove(python_sock_path)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(python_sock_path)

    # Handshake: the daemon answers our hello with the shared-memory fd.
    shm = attach(sock, rust_sock_path)
    print(
        f"[edges] attached to SHM ({shm.width}x{shm.height} @ {shm.channels}ch), "
        f"thresholds={EDGE_LOW}/{EDGE_HIGH}",
        flush=True,
    )

    try:
        while True:
            packet = json.loads(sock.recvfrom(512)[0])
            if "frame_id" not in packet:
                continue  # a control packet, e.g. a duplicate handshake reply
            frame_id = packet["frame_id"]
            slot_idx = packet["slot_index"]
            started = time.perf_counter_ns()

            frame = shm.get_slot_numpy_array(slot_idx)

            # Report the failure instead of dying: the relay passes the original
            # frame through, and one good frame later the AI branch is back.
            success = True
            try:
                apply_edges(frame)
            except Exception as exc:
                success = False
                print(f"[edges] frame {frame_id} failed: {exc}", flush=True)

            shm.mark_slot_free(slot_idx)

            ack = {
                "frame_id": frame_id,
                "slot_index": slot_idx,
                "processing_time_us": (time.perf_counter_ns() - started) // 1000,
                "success": success,
            }
            try:
                sock.sendto(json.dumps(ack).encode("utf-8"), rust_sock_path)
            except OSError as exc:
                # A full datagram buffer is backpressure, not a fatal error.
                print(f"[edges] dropped ack for frame {frame_id}: {exc}", flush=True)

    except KeyboardInterrupt:
        print("[edges] shutting down cleanly...", flush=True)
    finally:
        sock.close()
        if os.path.exists(python_sock_path):
            os.remove(python_sock_path)


if __name__ == "__main__":
    run_worker(RUST_SOCK, PYTHON_SOCK)
