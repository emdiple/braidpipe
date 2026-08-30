"""The raw worker: the whole IPC contract and nothing else.

This is the file to copy when writing your own worker. It attaches to the
daemon's shared memory, receives one datagram per frame, exposes the frame as
a zero-copy NumPy array, frees the slot, and acks — the four obligations every
worker has. The one thing it does not do is touch the pixels: put your own
processing where `process()` is.

Demonstration workers live in examples/ — an edge transform, threaded YOLO
detection, and latency stamping.

Usage:
    cargo run -p braidpipe --release -- --python-script python/braidpipe/worker.py

Environment:
    BRAIDPIPE_RUST_SOCK     daemon's ack socket       (default: /tmp/braidpipe_rust.sock)
    BRAIDPIPE_PYTHON_SOCK   this worker's socket      (default: /tmp/braidpipe_python.sock)
"""

import json
import os
import socket
import time

import numpy as np

from shm import attach

RUST_SOCK = os.environ.get("BRAIDPIPE_RUST_SOCK", "/tmp/braidpipe_rust.sock")
PYTHON_SOCK = os.environ.get("BRAIDPIPE_PYTHON_SOCK", "/tmp/braidpipe_python.sock")


def process(frame: np.ndarray, frame_id: int) -> None:
    """Your code goes here. Modify `frame` in place; writing to the view *is*
    writing the output.

    Frames are RGB, not BGR. Finish inside 1.5 frame periods (50 ms at 30 fps)
    or the daemon falls back to passthrough; anything slower than that belongs
    on a thread with cached results — see examples/worker_detect.py.
    """


def run_worker(rust_sock_path: str, python_sock_path: str) -> None:
    if os.path.exists(python_sock_path):
        os.remove(python_sock_path)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(python_sock_path)

    # Handshake: the daemon answers our hello with the shared-memory fd.
    shm = attach(sock, rust_sock_path)
    print(f"[worker] attached to SHM ({shm.width}x{shm.height} @ {shm.channels}ch)", flush=True)

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
                process(frame, frame_id)
            except Exception as exc:
                success = False
                print(f"[worker] frame {frame_id} failed: {exc}", flush=True)

            # The slot is recycled the moment it is freed: copy any pixels you
            # need to keep before this line.
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
                print(f"[worker] dropped ack for frame {frame_id}: {exc}", flush=True)

    except KeyboardInterrupt:
        print("[worker] shutting down cleanly...", flush=True)
    finally:
        sock.close()
        if os.path.exists(python_sock_path):
            os.remove(python_sock_path)


if __name__ == "__main__":
    run_worker(RUST_SOCK, PYTHON_SOCK)
