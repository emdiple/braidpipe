"""Off-hot-path inference: run a slow model on a thread, read cached results.

The relay gives a worker 1.5 frame periods, and most real models do not fit in
that. The trade that works is to decouple "what is in the frame" (slow, cached)
from "draw it" (fast, every frame): submit an occasional frame to the model,
annotate every frame with the most recent answer. Results lag the picture by a
frame or two; no frame ever misses its deadline.

`BackgroundModel` is that pattern with the threading taken out of your hands:

    model = braidpipe.BackgroundModel(infer, initial=[])

    def process(frame, ctx):
        if ctx.frame_id % 3 == 0:
            model.submit(frame)
        draw(frame, model.latest())
"""

from __future__ import annotations

import threading
from typing import Callable, Generic, TypeVar

import numpy as np

T = TypeVar("T")


class BackgroundModel(Generic[T]):
    """Runs `infer(frame) -> result` on a daemon thread, one frame at a time.

    `submit()` copies the frame — the shared-memory slot is recycled the moment
    the handler returns — and is dropped, not queued, while a previous frame is
    still being inferred, so the model always works on the newest picture it
    can get. `latest()` returns the most recent result, or `initial` until the
    first inference completes. An exception in `infer` is logged and the result
    left unchanged; the thread never dies.
    """

    def __init__(self, infer: Callable[[np.ndarray], T], initial: T = None):
        self._infer = infer
        self._pending: np.ndarray | None = None
        self._latest: T = initial
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def submit(self, frame: np.ndarray) -> None:
        """Offer a frame for inference; skipped if one is already waiting."""
        with self._lock:
            if self._pending is not None:
                return
            self._pending = frame.copy()
        self._wake.set()

    def latest(self) -> T:
        with self._lock:
            return self._latest

    def _loop(self) -> None:
        while True:
            self._wake.wait()
            self._wake.clear()

            with self._lock:
                frame, self._pending = self._pending, None
            if frame is None:
                continue

            try:
                result = self._infer(frame)
            except Exception as exc:  # a bad frame must not kill the thread
                print(f"[background] inference error: {exc}", flush=True)
                continue

            with self._lock:
                self._latest = result
