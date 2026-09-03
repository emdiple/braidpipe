# braidpipe (worker SDK)

The Python worker SDK for [braidpipe](https://github.com/emdiple/braidpipe), the
never-dark AI video middleware. The Rust daemon owns the media path; this
package is how a Python process receives its frames — as zero-copy NumPy views
over shared memory locally, or over the tcp-raw transport from another machine —
and hands them back.

```bash
pip install braidpipe
```

A complete worker:

```python
import braidpipe

def process(frame):          # (H, W, 3) uint8, RGB — mutate it in place
    frame[:, :, 0] //= 2     # your inference here

if __name__ == "__main__":
    braidpipe.run(process)
```

`run()` owns everything else: the handshake, the per-frame notification loop,
freeing the shared-memory slot, acking (including `"success": false` when your
code raises, so the stream falls back to passthrough instead of going dark),
and the transport switch — set `BRAIDPIPE_DAEMON=host:port` and the same worker
attaches over the network instead.

This package does not contain the daemon. Build it from the
[repository](https://github.com/emdiple/braidpipe), which also has the full
worker contract, examples, and deadline rules in `docs/workers.md`.
