import mmap
import os
import socket as socket_module
import struct
import time
import numpy as np

# Slot State Constants matching Rust shm.rs
SLOT_FREE = 0
SLOT_READY_FOR_AI = 1
SLOT_PROCESSING = 2

# Struct layouts matching the explicit #[repr(C)] layouts in Rust shm.rs:
# ShmHeader: width u32, height u32, channels u8, slot_count u8, 2 pad bytes,
#            slot_size u32, 16 reserved bytes -> 32 bytes total
HEADER_FMT = "<IIBB2xI16s"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
assert HEADER_SIZE == 32

# SlotHeader: state u8, 7 pad bytes, frame_id u64, timestamp_us u64 -> 24 bytes
SLOT_HEADER_FMT = "<B7xQQ"
SLOT_HEADER_SIZE = struct.calcsize(SLOT_HEADER_FMT)
assert SLOT_HEADER_SIZE == 24

# The handshake: a worker announces itself with HELLO on the daemon's socket
# and receives a datagram carrying the shared segment's fd as SCM_RIGHTS
# ancillary data. The segment is anonymous -- the fd is the only way in.
HELLO = b'{"type":"hello"}'


def attach(sock: socket_module.socket, rust_sock_path: str, retry_interval: float = 1.0):
    """Says hello to the daemon until it answers with the shared-memory fd.

    `sock` must already be bound to this worker's own socket path, because the
    daemon addresses its reply there. Retries forever, so a worker may start
    before the daemon and simply wait for it.
    """
    previous_timeout = sock.gettimeout()
    sock.settimeout(retry_interval)
    try:
        while True:
            try:
                sock.sendto(HELLO, rust_sock_path)
            except OSError:
                # The daemon is not up (no socket file yet); try again.
                time.sleep(retry_interval)
                continue
            try:
                _msg, fds, _flags, _addr = socket_module.recv_fds(sock, 512, 1)
            except TimeoutError:
                continue
            except OSError:
                time.sleep(retry_interval)
                continue
            if fds:
                return SharedMemoryManager(fds[0])
            # A frame notification that raced the handshake; ignore it. The
            # daemon passes those frames through unchanged.
    finally:
        sock.settimeout(previous_timeout)


class SharedMemoryManager:
    def __init__(self, fd: int):
        # The fd received from the daemon describes the whole segment; its
        # size comes from the fd itself, and everything else from the header.
        size = os.fstat(fd).st_size
        self.shm = mmap.mmap(fd, size)
        os.close(fd)  # the mapping keeps the segment alive on its own

        # Unpack header metadata
        header_bytes = bytes(self.shm[:HEADER_SIZE])
        (
            self.width,
            self.height,
            self.channels,
            self.slot_count,
            self.slot_size,
            _,
        ) = struct.unpack(HEADER_FMT, header_bytes)

    def get_slot_numpy_array(self, slot_idx: int) -> np.ndarray:
        """Returns a zero-copy NumPy view over a specific SHM slot's pixel buffer."""
        slot_offset = HEADER_SIZE + (slot_idx * self.slot_size)
        payload_offset = slot_offset + SLOT_HEADER_SIZE
        payload_bytes = self.width * self.height * self.channels

        # Point NumPy directly to the shared memory buffer slice
        return np.ndarray(
            shape=(self.height, self.width, self.channels),
            dtype=np.uint8,
            buffer=self.shm,
            offset=payload_offset,
        )

    def read_slot_header(self, slot_idx: int) -> tuple[int, int, int]:
        """Returns (state, frame_id, timestamp_us) for a slot.

        `timestamp_us` is the wall clock the daemon recorded as it wrote the
        frame, so subtracting `time.time()` here gives the one-way IPC delay.
        """
        slot_offset = HEADER_SIZE + (slot_idx * self.slot_size)
        raw = bytes(self.shm[slot_offset : slot_offset + SLOT_HEADER_SIZE])
        return struct.unpack(SLOT_HEADER_FMT, raw)

    def mark_slot_free(self, slot_idx: int):
        """Resets the slot state back to FREE so Rust can write the next frame."""
        slot_offset = HEADER_SIZE + (slot_idx * self.slot_size)
        self.shm[slot_offset] = SLOT_FREE

    def close(self):
        self.shm.close()
