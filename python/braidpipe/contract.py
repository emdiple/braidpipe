"""The IPC contract version this SDK speaks, and the check against the daemon.

The contract is everything both sides must agree on: the shm/slot layouts, the
control-packet fields, and the tcp-raw wire format. The daemon stamps its
version into every config packet it answers a hello with; a mismatch here must
fail at attach time, loudly, because the alternative is silently misreading
shared memory.

A config with no version at all comes from a daemon older than versioning
(braidpipe <= 0.2.0). Those daemons speak contract 1, so attaching proceeds
with a warning rather than refusing streams that would work.
"""

CONTRACT_VERSION = 1


def check_contract(config: dict, transport: str) -> None:
    """Raises RuntimeError if the daemon's config names a different contract."""
    theirs = config.get("contract")
    if theirs is None:
        print(
            f"[braidpipe] warning: daemon sent no contract version over {transport} "
            f"(daemon <= 0.2.0); assuming contract {CONTRACT_VERSION}",
            flush=True,
        )
        return
    if theirs != CONTRACT_VERSION:
        raise RuntimeError(
            f"daemon speaks IPC contract {theirs}, this SDK speaks {CONTRACT_VERSION} "
            f"(over {transport}); upgrade whichever side is older instead of attaching "
            "and misreading frames"
        )
