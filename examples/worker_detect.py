"""Object-detection worker: YOLO on a background thread, boxes drawn every frame.

The relay gives a worker 1.5 frame periods — 50 ms at 30 fps — and a CPU YOLO
pass does not reliably fit in that. So this worker never makes the frame loop
wait for inference: `braidpipe.BackgroundModel` runs the model on its own
thread over a copy of the newest frame, and every frame is annotated with the
most recent boxes that thread has produced. Detections lag the picture by a
frame or two; no frame ever misses its deadline.

That trade is the general pattern for any model too slow to run inline: decouple
"what is in the frame" (slow, cached) from "draw it" (fast, every frame).

Usage:
    cargo run -p braidpipe --release -- --python-script examples/worker_detect.py

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

The last three defaults exist to stop inference starving the frame loop. Left
unbounded, torch takes every core and the annotate-and-ack path misses its 50 ms
deadline even though it only needs a millisecond of CPU — the worker then flaps
between branches every few seconds. Capping the thread count and detecting on
every third frame keeps the loop responsive; raise them if you have cores spare.
"""

import os
import sys

import cv2
import numpy as np

try:
    import braidpipe
except ModuleNotFoundError:  # run from a source checkout, package not installed
    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python")
    )
    import braidpipe

MODEL = os.environ.get("BRAIDPIPE_MODEL", "yolov8n.pt")
CONF = float(os.environ.get("BRAIDPIPE_CONF", "0.35"))
IMGSZ = int(os.environ.get("BRAIDPIPE_IMGSZ", "512"))
DETECT_EVERY = max(1, int(os.environ.get("BRAIDPIPE_DETECT_EVERY", "3")))
TORCH_THREADS = max(1, int(os.environ.get("BRAIDPIPE_TORCH_THREADS", "2")))

# Frames in shared memory are RGB, not BGR: these tuples are (R, G, B).
BOX_COLOR = (255, 109, 14)
TEXT_COLOR = (255, 255, 255)

Box = tuple[int, int, int, int, str, float]


def load_model():
    """Loads YOLO and returns `infer(frame) -> boxes` for BackgroundModel.

    Everything is imported here so a missing dependency surfaces at startup,
    before the socket is bound, rather than mid-stream.
    """
    import torch
    from ultralytics import YOLO

    # Left alone, torch grabs every core and the frame loop — which needs
    # barely any CPU — stops making its deadline.
    torch.set_num_threads(TORCH_THREADS)

    model = YOLO(MODEL)

    def infer(frame: np.ndarray) -> list[Box]:
        result = model.predict(frame, conf=CONF, imgsz=IMGSZ, verbose=False)[0]
        boxes = []
        for box in result.boxes:
            x1, y1, x2, y2 = (int(v) for v in box.xyxy[0])
            label = result.names[int(box.cls[0])]
            boxes.append((x1, y1, x2, y2, label, float(box.conf[0])))
        return boxes

    return infer


def draw_boxes(frame: np.ndarray, boxes: list[Box]) -> None:
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


if __name__ == "__main__":
    try:
        detector = braidpipe.BackgroundModel(load_model(), initial=[])
    except ModuleNotFoundError as exc:
        sys.exit(
            f"[detect] missing dependency: {exc}\n"
            "         install it with: .venv/bin/pip install ultralytics"
        )
    print(f"[detect] model={MODEL} conf={CONF}", flush=True)

    def process(frame: np.ndarray, ctx: braidpipe.FrameContext) -> None:
        # Detect on a fraction of frames, annotate all of them. submit() copies
        # the frame, so the recycled shared-memory slot is never held.
        if ctx.frame_id % DETECT_EVERY == 0:
            detector.submit(frame)
        draw_boxes(frame, detector.latest())

    braidpipe.run(process, name="detect")
