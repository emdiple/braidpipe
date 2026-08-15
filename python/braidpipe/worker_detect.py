"""Object-detection worker: YOLO on a background thread, boxes drawn every frame.

The relay gives a worker 1.5 frame periods — 50 ms at 30 fps — and a CPU YOLO
pass does not reliably fit in that. So this worker never makes the socket loop
wait for inference. It hands a copy of the newest frame to a detector thread and
annotates every frame with the most recent boxes that thread has produced.
Detections lag the picture by a frame or two; no frame ever misses its deadline.

That trade is the general pattern for any model too slow to run inline: decouple
"what is in the frame" (slow, cached) from "draw it" (fast, every frame).

Usage:
    cargo run -p braidpipe --release -- --python-script python/braidpipe/worker_detect.py

The first run downloads the model weights (~6 MB for yolov8n), so give it
network access and expect the AI branch to stay unselected until that finishes.

Environment:
    BRAIDPIPE_MODEL         model name or path        (default: yolov8n.pt)
    BRAIDPIPE_CONF          confidence threshold      (default: 0.35)
    BRAIDPIPE_IMGSZ         inference resolution      (default: 512)
    BRAIDPIPE_DETECT_EVERY  submit every Nth frame    (default: 3)
    BRAIDPIPE_TORCH_THREADS threads torch may use     (default: 2)
    BRAIDPIPE_RUST_SOCK     daemon's ack socket       (default: /tmp/braidpipe_rust.sock)
    BRAIDPIPE_PYTHON_SOCK   this worker's socket      (default: /tmp/braidpipe_python.sock)

The last three defaults exist to stop inference starving the socket loop. Left
unbounded, torch takes every core and the annotate-and-ack path misses its 50 ms
deadline even though it only needs a millisecond of CPU — the worker then flaps
between branches every few seconds. Capping the thread count and detecting on
every third frame keeps the loop responsive; raise them if you have cores spare.
"""

import json
import os
import socket
import sys
import threading
import time

import cv2
import numpy as np
from shm import attach

RUST_SOCK = os.environ.get("BRAIDPIPE_RUST_SOCK", "/tmp/braidpipe_rust.sock")
PYTHON_SOCK = os.environ.get("BRAIDPIPE_PYTHON_SOCK", "/tmp/braidpipe_python.sock")
MODEL = os.environ.get("BRAIDPIPE_MODEL", "yolov8n.pt")
CONF = float(os.environ.get("BRAIDPIPE_CONF", "0.35"))
IMGSZ = int(os.environ.get("BRAIDPIPE_IMGSZ", "512"))
DETECT_EVERY = max(1, int(os.environ.get("BRAIDPIPE_DETECT_EVERY", "3")))
TORCH_THREADS = max(1, int(os.environ.get("BRAIDPIPE_TORCH_THREADS", "2")))

# Frames in shared memory are RGB, not BGR: these tuples are (R, G, B).
BOX_COLOR = (255, 109, 14)
TEXT_COLOR = (255, 255, 255)


class Detector(threading.Thread):
    """Runs the model off the hot path and publishes the latest boxes it found."""

    def __init__(self, model_name: str, conf: float):
        super().__init__(daemon=True)
        # Imported here so a missing dependency surfaces at startup, before the
        # socket is bound, rather than mid-stream.
        import torch
        from ultralytics import YOLO

        # Left alone, torch grabs every core and the socket loop — which needs
        # barely any CPU — stops making its deadline.
        torch.set_num_threads(TORCH_THREADS)

        self._model = YOLO(model_name)
        self._conf = conf
        self._pending: np.ndarray | None = None
        self._boxes: list[tuple[int, int, int, int, str, float]] = []
        self._lock = threading.Lock()
        self._wake = threading.Event()

    def submit(self, frame: np.ndarray) -> None:
        """Offer a frame for detection; skipped if one is already queued."""
        with self._lock:
            if self._pending is not None:
                return
            # The shared-memory slot is recycled the moment it is freed, so the
            # thread must own its pixels rather than hold the zero-copy view.
            self._pending = frame.copy()
        self._wake.set()

    def boxes(self) -> list[tuple[int, int, int, int, str, float]]:
        with self._lock:
            return self._boxes

    def run(self) -> None:
        while True:
            self._wake.wait()
            self._wake.clear()

            with self._lock:
                frame, self._pending = self._pending, None
            if frame is None:
                continue

            try:
                result = self._model.predict(
                    frame, conf=self._conf, imgsz=IMGSZ, verbose=False
                )[0]
            except Exception as exc:  # a bad frame must not kill the thread
                print(f"[detect] inference error: {exc}", flush=True)
                continue

            boxes = []
            for box in result.boxes:
                x1, y1, x2, y2 = (int(v) for v in box.xyxy[0])
                label = result.names[int(box.cls[0])]
                boxes.append((x1, y1, x2, y2, label, float(box.conf[0])))

            with self._lock:
                self._boxes = boxes


def draw_boxes(frame: np.ndarray, boxes) -> None:
    """Annotates the frame in place; writing to the view *is* writing the output."""
    for x1, y1, x2, y2, label, conf in boxes:
        cv2.rectangle(frame, (x1, y1), (x2, y2), BOX_COLOR, 2)

        caption = f"{label} {conf:.2f}"
        (text_w, text_h), _ = cv2.getTextSize(caption, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
        cv2.rectangle(frame, (x1, y1 - text_h - 8), (x1 + text_w + 8, y1), BOX_COLOR, -1)
        cv2.putText(
            frame,
            caption,
            (x1 + 4, y1 - 5),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            TEXT_COLOR,
            1,
            cv2.LINE_AA,
        )


def run_worker(rust_sock_path: str, python_sock_path: str) -> None:
    if os.path.exists(python_sock_path):
        os.remove(python_sock_path)

    detector = Detector(MODEL, CONF)
    detector.start()

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(python_sock_path)

    # Handshake: the daemon answers our hello with the shared-memory fd.
    shm = attach(sock, rust_sock_path)
    print(
        f"[detect] attached to SHM ({shm.width}x{shm.height} @ {shm.channels}ch), "
        f"model={MODEL} conf={CONF}",
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

            # Detect on a fraction of frames, annotate all of them.
            if frame_id % DETECT_EVERY == 0:
                detector.submit(frame)
            draw_boxes(frame, detector.boxes())

            shm.mark_slot_free(slot_idx)

            ack = {
                "frame_id": frame_id,
                "slot_index": slot_idx,
                "processing_time_us": (time.perf_counter_ns() - started) // 1000,
                "success": True,
            }
            try:
                sock.sendto(json.dumps(ack).encode("utf-8"), rust_sock_path)
            except OSError as exc:
                # A full datagram buffer is backpressure, not a fatal error.
                print(f"[detect] dropped ack for frame {frame_id}: {exc}", flush=True)

    except KeyboardInterrupt:
        print("[detect] shutting down cleanly...", flush=True)
    finally:
        sock.close()
        if os.path.exists(python_sock_path):
            os.remove(python_sock_path)


if __name__ == "__main__":
    try:
        run_worker(RUST_SOCK, PYTHON_SOCK)
    except ModuleNotFoundError as exc:
        sys.exit(
            f"[detect] missing dependency: {exc}\n"
            "         install it with: .venv/bin/pip install ultralytics"
        )
