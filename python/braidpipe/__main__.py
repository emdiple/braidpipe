"""`python3 -m braidpipe`: the raw worker with no processing at all.

Attaches, acks every frame untouched, and proves the AI loop is closed — the
same thing python/braidpipe/worker.py does, without needing a path to a script.
"""

import numpy as np

from .runner import run


def process(frame: np.ndarray) -> None:
    """Deliberately empty: every frame passes through the loop unmodified."""


run(process, name="braidpipe")
