import mmap
import struct
import numpy as np

# Slot State Constants matching Rust shm.rs
SLOT_FREE = 0
SLOT_READY_FOR_AI = 1
SLOT_PROCESSING = 2

# Struct layouts matching #[repr(C)] in Rust
# ShmHeader: width(u32), height(u32), channels(u8), slot_count(u8), slot_size(u32), padding(18b)
HEADER_FMT = "<IIBB I 18s"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# SlotHeader: state(u8), frame_id(u64), timestamp_us(u64)
SLOT_HEADER_FMT = "<B Q Q"
SLOT_HEADER_SIZE = struct.calcsize(SLOT_HEADER_FMT)


class SharedMemoryManager:
    def __init__(self, shm_path: str = "/dev/shm/braidpipe_buffer"):
        # Open the POSIX shared memory file descriptor initialized by Rust
        self.file = open(shm_path, "r+b")
        self.shm = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_WRITE)

        # Unpack header metadata
        header_bytes = self.shm[:HEADER_SIZE]
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

        # Point NumPy directly to the shared memory memory map slice
        return np.ndarray(
            shape=(self.height, self.width, self.channels),
            dtype=np.uint8,
            buffer=self.shm,
            offset=payload_offset,
        )

    def mark_slot_free(self, slot_idx: int):
        """Resets the slot state back to FREE so Rust can write the next frame."""
        slot_offset = HEADER_SIZE + (slot_idx * self.slot_size)
        # Write SLOT_FREE (0) into the first byte of SlotHeader
        self.shm[slot_offset : slot_offset + 1] = bytes([SLOT_FREE])