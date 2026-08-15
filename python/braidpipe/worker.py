import json
import os
import socket
import time
import cv2
import numpy as np
from shm import attach

def run_worker(rust_sock_path: str, python_sock_path: str):
    # Clean up stale Python socket file if present
    if os.path.exists(python_sock_path):
        os.remove(python_sock_path)

    # Bind datagram socket to receive signals from Rust
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(python_sock_path)

    # Handshake: the daemon answers our hello with the shared-memory fd
    shm_mgr = attach(sock, rust_sock_path)
    print(f"[Text Worker] Attached to SHM ({shm_mgr.width}x{shm_mgr.height} @ {shm_mgr.channels}ch)")

    try:
        while True:
            # 1. Wait for frame metadata from Rust
            data, _ = sock.recvfrom(512)
            packet = json.loads(data.decode("utf-8"))
            if "frame_id" not in packet:
                continue  # a control packet (e.g. a duplicate handshake reply)
            frame_id = packet["frame_id"]
            slot_idx = packet["slot_index"]

            start_time = time.perf_counter_ns()

            # 2. Get the zero-copy NumPy view of the video frame
            frame: np.ndarray = shm_mgr.get_slot_numpy_array(slot_idx)

            # 3. Simple Text Overlay
            # Put a static label and dynamic frame counter onto the video
            cv2.putText(
                frame,
                f"LIVE STREAM OUTPUT | Frame: {frame_id}",
                (50, 80), # X, Y coordinates
                cv2.FONT_HERSHEY_SIMPLEX,
                1.5, # Font scale
                (255, 0, 0), # Red. Frames here are RGB, so this is (R, G, B) --
                             # the OpenCV habit of (0, 0, 255) renders blue.
                3, # Line thickness
            )

            # 4. Mark slot state as FREE for Rust reuse
            shm_mgr.mark_slot_free(slot_idx)

            # 5. Send acknowledgment packet back to Rust Watchdog.
            # A full socket buffer (ENOBUFS) just means Rust is not draining
            # acks right now; drop this ack instead of crashing the worker.
            elapsed_us = (time.perf_counter_ns() - start_time) // 1000
            ack_packet = {
                "frame_id": frame_id,
                "slot_index": slot_idx,
                "processing_time_us": elapsed_us,
                "success": True,
            }
            try:
                sock.sendto(json.dumps(ack_packet).encode("utf-8"), rust_sock_path)
            except OSError as exc:
                print(f"[Text Worker] Dropped ack for frame {frame_id}: {exc}")

    except KeyboardInterrupt:
        print("[Text Worker] Shutting down cleanly...")
    finally:
        sock.close()
        if os.path.exists(python_sock_path):
            os.remove(python_sock_path)

if __name__ == "__main__":
    run_worker("/tmp/braidpipe_rust.sock", "/tmp/braidpipe_python.sock")
