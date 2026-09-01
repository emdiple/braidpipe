"""The braidpipe worker SDK: your function, our loop.

A complete worker is a processing function handed to `run()`:

    import braidpipe

    def process(frame):          # (H, W, 3) uint8, RGB — mutate it in place
        frame[:, :, 0] //= 2     # your inference here

    if __name__ == "__main__":
        braidpipe.run(process)

`run()` owns the handshake, the per-frame loop, slot release, acking (including
`"success": false` when the handler raises, so the stream falls back to
passthrough instead of going dark) and the transport: shared memory next to a
local daemon, tcp-raw when `BRAIDPIPE_DAEMON=host:port` is set. Take a second
`ctx` parameter for frame ids and timestamps, and use `BackgroundModel` for
models too slow for the 1.5-frame-period deadline.

The transports underneath (`attach`/`SharedMemoryManager` for shared memory,
`connect`/`RemoteWorkerLink` for tcp-raw) stay importable for workers that need
the loop itself.
"""

from .background import BackgroundModel
from .contract import CONTRACT_VERSION
from .remote import RemoteWorkerLink, connect
from .runner import FrameContext, run, worker
from .shm import SharedMemoryManager, attach

__version__ = "0.3.0"

__all__ = [
    "BackgroundModel",
    "CONTRACT_VERSION",
    "FrameContext",
    "RemoteWorkerLink",
    "SharedMemoryManager",
    "attach",
    "connect",
    "run",
    "worker",
    "__version__",
]
