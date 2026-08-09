import mmap
import struct
import numpy as np
from multiprocessing import shared_memory

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

class SharedMemoryManager:
    def __init__(self, shm_name: str = "braidpipe_buffer"):
        # Strip leading slash/path if passed from CLI/defaults
        clean_name = shm_name.lstrip("/").replace("dev/shm/", "")
        
        # Attach to POSIX shared memory created by Rust (works on Linux & macOS).
        # track=False (Python 3.13+) stops the resource tracker from unlinking
        # the Rust-owned segment when this process exits.
        try:
            self.shm_obj = shared_memory.SharedMemory(name=clean_name, track=False)
        except TypeError:
            self.shm_obj = shared_memory.SharedMemory(name=clean_name)
        self.shm = self.shm_obj.buf

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
        self.shm_obj.close()