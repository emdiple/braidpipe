"""Binary clock barcode shared by the stamping worker and the latency probe.

Measuring latency through a real encoder rules out anything delicate: H.264 will
happily smear a thin line or a lone pixel into its neighbours, and the RGB -> YUV
round trip clamps to limited range on the way. So the clock is written as a row
of large, pure black-or-white cells and read back from the middle of each one,
which survives lossy compression at any bitrate worth streaming.

Geometry is derived from the frame width alone, so the writer and the reader
agree without negotiating anything.
"""

PREAMBLE = 0xA5
PREAMBLE_BITS = 8

# Microseconds, low 40 bits. 2**40 us is a little under 13 days, so a run never
# has to reason about the counter wrapping mid-measurement.
PAYLOAD_BITS = 40
PAYLOAD_MASK = (1 << PAYLOAD_BITS) - 1

TOTAL_BITS = PREAMBLE_BITS + PAYLOAD_BITS


def cell_size(width: int) -> int:
    """Edge length in pixels of one barcode cell.

    Half the frame width is left free so the barcode never runs off the right
    edge, and cells stay at least 4px so the deblocking filter cannot close
    them up.
    """
    return max(4, width // (TOTAL_BITS * 2))


def barcode_width(width: int) -> int:
    return TOTAL_BITS * cell_size(width)


def encode(frame, timestamp_us: int) -> None:
    """Writes `timestamp_us` across the top-left of `frame`, in place."""
    cell = cell_size(frame.shape[1])
    bits = (PREAMBLE << PAYLOAD_BITS) | (timestamp_us & PAYLOAD_MASK)

    for i in range(TOTAL_BITS):
        high = (bits >> (TOTAL_BITS - 1 - i)) & 1
        x = i * cell
        frame[0:cell, x : x + cell] = 255 if high else 0


def decode(frame) -> int | None:
    """Reads a timestamp back out of `frame`, or None if no valid barcode.

    Returning None is the normal case for the first frames of a stream, where
    the decoder is still emitting whatever it managed to reconstruct before the
    first keyframe.
    """
    cell = cell_size(frame.shape[1])
    if frame.shape[0] < cell or frame.shape[1] < TOTAL_BITS * cell:
        return None

    # Sampling only the middle of each cell keeps block edges, chroma bleed and
    # the deblocking filter out of the reading.
    inset = max(1, cell // 4)

    bits = 0
    for i in range(TOTAL_BITS):
        x = i * cell + inset
        patch = frame[inset : cell - inset, x : x + cell - 2 * inset]
        bits = (bits << 1) | int(patch.mean() > 127)

    if (bits >> PAYLOAD_BITS) != PREAMBLE:
        return None

    return bits & PAYLOAD_MASK
