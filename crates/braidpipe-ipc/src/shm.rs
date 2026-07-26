use braidpipe_core::ports::ai::{FrameMetadata, IpcError, ShmWriter};
use std::ffi::CString;
use std::ptr;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use tracing::{debug, info};

// Slot States for Lock-Free Sync
pub const SLOT_FREE: u8 = 0;
pub const SLOT_READY_FOR_AI: u8 = 1;
pub const SLOT_PROCESSING: u8 = 2;

/// Memory-mapped header at offset 0 of /dev/shm
#[repr(C)]
pub struct ShmHeader {
    pub width: u32,
    pub height: u32,
    pub channels: u8,
    pub slot_count: u8,
    pub slot_size: u32,
    pub _padding: [u8; 18], // Align to 32-byte boundary
}

/// Metadata control block preceding each slot's raw pixel buffer
#[repr(C)]
pub struct SlotHeader {
    pub state: AtomicU8,
    pub frame_id: AtomicU64,
    pub timestamp_us: AtomicU64,
}

pub struct ShmRingBuffer {
    shm_name: String,
    fd: i32,
    mmap_ptr: *mut u8,
    total_size: usize,
    header: &'static ShmHeader,
}

// Safety: The Shared Memory buffer is thread-safe due to atomic slot state management
unsafe impl Send for ShmRingBuffer {}
unsafe impl Sync for ShmRingBuffer {}

impl ShmRingBuffer {
    pub fn create(
        shm_name: &str,
        width: u32,
        height: u32,
        channels: u8,
        slot_count: u8,
    ) -> Result<Self, IpcError> {
        let slot_size = (width * height * channels as u32) as usize;
        let slot_header_size = std::mem::size_of::<SlotHeader>();
        let per_slot_total = slot_header_size + slot_size;
        let header_size = std::mem::size_of::<ShmHeader>();
        let total_size = header_size + (per_slot_total * slot_count as usize);

        let c_name = CString::new(shm_name)
            .map_err(|e| IpcError::ShmError(format!("Invalid SHM name: {e}")))?;

        unsafe {
            // 1. Open POSIX shared memory file descriptor (/dev/shm)
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o666);
            if fd < 0 {
                return Err(IpcError::ShmError("Failed to execute shm_open".into()));
            }

            // 2. Set memory region size
            if libc::ftruncate(fd, total_size as libc::off_t) != 0 {
                libc::close(fd);
                return Err(IpcError::ShmError(
                    "Failed to set SHM size with ftruncate".into(),
                ));
            }

            // 3. Memory-map (/dev/shm) into current process virtual memory space
            let mmap_ptr = libc::mmap(
                ptr::null_mut(),
                total_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            ) as *mut u8;

            if mmap_ptr == libc::MAP_FAILED as *mut u8 {
                libc::close(fd);
                return Err(IpcError::ShmError("Failed to mmap shared memory".into()));
            }

            // 4. Initialize Header at offset 0
            let header_ptr = mmap_ptr as *mut ShmHeader;
            ptr::write(
                header_ptr,
                ShmHeader {
                    width,
                    height,
                    channels,
                    slot_count,
                    slot_size: per_slot_total as u32,
                    _padding: [0; 18],
                },
            );

            // 5. Initialize each slot's Atomic headers to FREE state
            for i in 0..slot_count {
                let slot_offset = header_size + (i as usize * per_slot_total);
                let slot_header_ptr = mmap_ptr.add(slot_offset) as *mut SlotHeader;
                ptr::write(
                    slot_header_ptr,
                    SlotHeader {
                        state: AtomicU8::new(SLOT_FREE),
                        frame_id: AtomicU64::new(0),
                        timestamp_us: AtomicU64::new(0),
                    },
                );
            }

            info!(shm_name = %shm_name, bytes = total_size, "Created POSIX Shared Memory Ring Buffer");

            Ok(Self {
                shm_name: shm_name.to_string(),
                fd,
                mmap_ptr,
                total_size,
                header: &*header_ptr,
            })
        }
    }
}

impl Drop for ShmRingBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.mmap_ptr as *mut libc::c_void, self.total_size);
            libc::close(self.fd);
            let c_name = CString::new(self.shm_name.as_str()).unwrap();
            libc::shm_unlink(c_name.as_ptr());
            info!(shm_name = %self.shm_name, "Unlinked Shared Memory allocation");
        }
    }
}

impl ShmWriter for ShmRingBuffer {
    fn write_frame(&self, frame_id: u64, data: &[u8]) -> Result<FrameMetadata, IpcError> {
        let header_size = std::mem::size_of::<ShmHeader>();
        let per_slot_size = self.header.slot_size as usize;
        let slot_header_size = std::mem::size_of::<SlotHeader>();
        let payload_size = per_slot_size - slot_header_size;

        if data.len() > payload_size {
            return Err(IpcError::ShmError(format!(
                "Frame data size ({}) exceeds slot capacity ({})",
                data.len(),
                payload_size
            )));
        }

        // Search for a slot with SLOT_FREE state
        for i in 0..self.header.slot_count {
            let slot_offset = header_size + (i as usize * per_slot_size);

            unsafe {
                let slot_header_ptr = self.mmap_ptr.add(slot_offset) as *const SlotHeader;
                let slot_header = &*slot_header_ptr;

                // Atomic Compare-And-Swap (CAS): Try to move state FREE -> PROCESSING
                if slot_header
                    .state
                    .compare_exchange(
                        SLOT_FREE,
                        SLOT_PROCESSING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    // Copy pixel bytes into offset after SlotHeader
                    let pixel_buffer_ptr = self.mmap_ptr.add(slot_offset + slot_header_size);
                    ptr::copy_nonoverlapping(data.as_ptr(), pixel_buffer_ptr, data.len());

                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;

                    slot_header.frame_id.store(frame_id, Ordering::Release);
                    slot_header.timestamp_us.store(timestamp, Ordering::Release);

                    // Set state READY_FOR_AI so Python can consume it
                    slot_header
                        .state
                        .store(SLOT_READY_FOR_AI, Ordering::Release);

                    debug!(
                        slot = i,
                        frame_id = frame_id,
                        "Wrote video frame to SHM slot"
                    );

                    return Ok(FrameMetadata {
                        frame_id,
                        slot_index: i,
                        timestamp_us: timestamp,
                    });
                }
            }
        }

        // All slots were busy/processing!
        Err(IpcError::BufferFull)
    }
}
