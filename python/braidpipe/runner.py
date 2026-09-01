"""The worker loop as a library: write `process(frame)`, call `run(process)`.

Every worker has the same four obligations — attach, receive a notification per
frame, free the slot, ack — and the same failure contract: report an exception
with `"success": false` instead of dying, so the daemon passes the original
frame through and the stream never goes dark. This module owns all of that,
for both transports:

- shared memory + Unix datagrams when launched by (or next to) a local daemon,
- tcp-raw when `BRAIDPIPE_DAEMON=host:port` points at a remote one.

The processing callback is the only thing a worker author writes. It takes the
frame — an (H, W, 3) uint8 RGB array to mutate in place — and optionally a
`FrameContext`; raising is safe and costs one passthrough frame, but the
callback must finish inside 1.5 frame periods (50 ms at 30 fps) or the daemon
falls back to passthrough. Anything slower belongs off the hot path — see
`braidpipe.BackgroundModel`.
"""

from __future__ import annotations

import inspect
import json
import os
import socket
import time
from dataclasses import dataclass
from typing import Callable

import numpy as np

from .remote import connect
from .shm import attach

DEFAULT_RUST_SOCK = "/tmp/braidpipe_rust.sock"
DEFAULT_PYTHON_SOCK = "/tmp/braidpipe_python.sock"

ProcessFn = Callable[..., None]


@dataclass(frozen=True)
class FrameContext:
    """Everything known about the frame besides its pixels.

    `timestamp_us` is the wall clock the daemon recorded as it handed the frame
    over (written into the slot header locally, carried on the wire remotely),
    so `time.time_ns() // 1000 - ctx.timestamp_us` is the one-way IPC delay.
    """

    frame_id: int
    slot: int
    timestamp_us: int
    width: int
    height: int
    channels: int
    transport: str  # "shm" or "tcp-raw"


_registered: ProcessFn | None = None


def worker(process: ProcessFn) -> ProcessFn:
    """Marks `process` as this script's frame handler, so `run()` finds it
    without being passed anything. Sugar only: `run(process)` is the same."""
    global _registered
    _registered = process
    return process


def _wants_context(process: ProcessFn) -> bool:
    """Whether to call `process(frame, ctx)` or just `process(frame)`.

    Decided once from the signature, not per frame; anything uninspectable is
    given both arguments.
    """
    try:
        parameters = inspect.signature(process).parameters.values()
    except (TypeError, ValueError):
        return True
    positional = [
        p
        for p in parameters
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    return len(positional) >= 2 or any(p.kind == p.VAR_POSITIONAL for p in parameters)


def run(
    process: ProcessFn | None = None,
    *,
    daemon: str | None = None,
    rust_sock: str | None = None,
    python_sock: str | None = None,
    name: str = "worker",
) -> None:
    """Runs the worker loop until the daemon hangs up or Ctrl-C.

    Arguments not given fall back to the environment: `BRAIDPIPE_DAEMON`
    selects the tcp-raw transport, otherwise `BRAIDPIPE_RUST_SOCK` and
    `BRAIDPIPE_PYTHON_SOCK` name the local sockets. `name` is only the log
    prefix. Returns normally on shutdown, so final reporting can follow it.
    """
    if process is None:
        process = _registered
    if process is None:
        raise TypeError(
            "no frame handler: pass one to run() or decorate it with @braidpipe.worker"
        )
    wants_ctx = _wants_context(process)

    if daemon is None:
        daemon = os.environ.get("BRAIDPIPE_DAEMON")
    if daemon:
        _run_remote(process, wants_ctx, daemon, name)
    else:
        _run_shm(
            process,
            wants_ctx,
            rust_sock or os.environ.get("BRAIDPIPE_RUST_SOCK", DEFAULT_RUST_SOCK),
            python_sock or os.environ.get("BRAIDPIPE_PYTHON_SOCK", DEFAULT_PYTHON_SOCK),
            name,
        )


def _invoke(
    process: ProcessFn,
    wants_ctx: bool,
    frame: np.ndarray,
    ctx: FrameContext,
    name: str,
) -> bool:
    """Calls the handler; an exception becomes a failed (passthrough) frame."""
    try:
        if wants_ctx:
            process(frame, ctx)
        else:
            process(frame)
        return True
    except Exception as exc:
        print(f"[{name}] frame {ctx.frame_id} failed: {exc}", flush=True)
        return False


def _run_shm(
    process: ProcessFn,
    wants_ctx: bool,
    rust_sock_path: str,
    python_sock_path: str,
    name: str,
) -> None:
    if os.path.exists(python_sock_path):
        os.remove(python_sock_path)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(python_sock_path)

    # Handshake: the daemon answers our hello with the shared-memory fd.
    shm = attach(sock, rust_sock_path)
    print(
        f"[{name}] attached to SHM ({shm.width}x{shm.height} @ {shm.channels}ch)",
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

            # The daemon's write time must be read before the slot is freed,
            # or the next frame may already have overwritten the header.
            _, _, written_us = shm.read_slot_header(slot_idx)
            ctx = FrameContext(
                frame_id=frame_id,
                slot=slot_idx,
                timestamp_us=written_us,
                width=shm.width,
                height=shm.height,
                channels=shm.channels,
                transport="shm",
            )

            frame = shm.get_slot_numpy_array(slot_idx)
            success = _invoke(process, wants_ctx, frame, ctx, name)

            # The slot is recycled the moment it is freed: the handler must
            # have copied any pixels it wants to keep.
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
                print(f"[{name}] dropped ack for frame {frame_id}: {exc}", flush=True)

    except KeyboardInterrupt:
        print(f"[{name}] shutting down cleanly...", flush=True)
    finally:
        sock.close()
        shm.close()
        if os.path.exists(python_sock_path):
            os.remove(python_sock_path)


def _run_remote(process: ProcessFn, wants_ctx: bool, daemon: str, name: str) -> None:
    link = connect(daemon)
    print(
        f"[{name}] connected to daemon at {daemon} "
        f"({link.width}x{link.height} @ {link.channels}ch)",
        flush=True,
    )
    try:
        for frame_id, slot_idx, timestamp_us, frame in link.frames():
            started = time.perf_counter_ns()
            ctx = FrameContext(
                frame_id=frame_id,
                slot=slot_idx,
                timestamp_us=timestamp_us,
                width=link.width,
                height=link.height,
                channels=link.channels,
                transport="tcp-raw",
            )
            success = _invoke(process, wants_ctx, frame, ctx, name)
            link.send_processed(
                frame_id,
                slot_idx,
                frame,
                (time.perf_counter_ns() - started) // 1000,
                success,
            )
        print(f"[{name}] daemon hung up", flush=True)
    except KeyboardInterrupt:
        print(f"[{name}] shutting down cleanly...", flush=True)
    finally:
        link.close()
