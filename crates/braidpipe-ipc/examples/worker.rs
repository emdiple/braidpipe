//! A worker written in Rust — a drop-in replacement for a Python worker.
//!
//! Nothing about the contract is Python-specific: a worker is any process that
//! attaches to the daemon's shared-memory segment, listens on a Unix datagram
//! socket, mutates pixels in place, frees the slot, and acks. This example does
//! exactly that in about a hundred lines, reusing the daemon's own layout types
//! so it cannot drift out of sync with them.
//!
//! Unlike the daemon, a worker only ever *attaches* to shared memory. It must
//! never call `ShmRingBuffer::create`, which unlinks and reinitialises the
//! segment — that would pull the floor out from under the running daemon.
//!
//! # Running it
//!
//! The daemon always spawns its `--python-script`, so point that at a no-op and
//! run this worker yourself:
//!
//! ```sh
//! # terminal 1 — creates the SHM segment, streams, and stays in passthrough
//! cargo run -p braidpipe --release -- --python-script /dev/null
//!
//! # terminal 2 — the AI branch is selected on this worker's first good frame
//! cargo run -p braidpipe-ipc --release --example worker
//! ```
//!
//! Kill this process and the stream keeps running on untouched frames, exactly
//! as it does for a Python worker.

use braidpipe_ipc::shm::{SLOT_FREE, ShmHeader, SlotHeader};
use braidpipe_ipc::uds::{FrameProcessedPacket, FrameReadyPacket};
use std::ffi::CString;
use std::os::unix::net::UnixDatagram;
use std::sync::atomic::Ordering;
use std::time::Instant;
use std::{env, fs, io, ptr, slice};

const DEFAULT_SHM_NAME: &str = "/braidpipe_buffer";
const DEFAULT_RUST_SOCK: &str = "/tmp/braidpipe_rust.sock";
const DEFAULT_PYTHON_SOCK: &str = "/tmp/braidpipe_python.sock";

/// A read-write view of the segment the daemon created.
struct AttachedShm {
    base: *mut u8,
    len: usize,
    width: usize,
    height: usize,
    channels: usize,
    slot_count: u8,
    slot_stride: usize,
}

impl AttachedShm {
    fn attach(shm_name: &str) -> io::Result<Self> {
        let c_name = CString::new(shm_name).expect("shm name has no interior nul");

        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDWR, 0o666);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            // The daemon owns the size; read it rather than assuming it.
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                return Err(err);
            }
            let len = stat.st_size as usize;

            let base = libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            ) as *mut u8;
            libc::close(fd); // the mapping keeps the segment alive on its own

            if base == libc::MAP_FAILED as *mut u8 {
                return Err(io::Error::last_os_error());
            }

            let header = &*(base as *const ShmHeader);
            Ok(Self {
                base,
                len,
                width: header.width as usize,
                height: header.height as usize,
                channels: header.channels as usize,
                slot_count: header.slot_count,
                // `slot_size` in the header is the whole stride: SlotHeader + pixels.
                slot_stride: header.slot_size as usize,
            })
        }
    }

    fn slot_header(&self, slot_index: u8) -> &SlotHeader {
        let offset = size_of::<ShmHeader>() + (slot_index as usize * self.slot_stride);
        unsafe { &*(self.base.add(offset) as *const SlotHeader) }
    }

    /// The slot's pixels, as a mutable slice. Writing here *is* writing the
    /// output frame; there is no separate send step for pixel data.
    ///
    /// # Safety
    /// The caller must hold the slot — that is, act only on a slot the daemon
    /// just announced and has not yet been told is free.
    unsafe fn pixels_mut(&mut self, slot_index: u8) -> &mut [u8] {
        let offset = size_of::<ShmHeader>()
            + (slot_index as usize * self.slot_stride)
            + size_of::<SlotHeader>();
        let payload = self.width * self.height * self.channels;
        unsafe { slice::from_raw_parts_mut(self.base.add(offset), payload) }
    }

    /// Hands the slot back so the daemon can write the next frame into it.
    fn free_slot(&self, slot_index: u8) {
        self.slot_header(slot_index)
            .state
            .store(SLOT_FREE, Ordering::Release);
    }
}

impl Drop for AttachedShm {
    fn drop(&mut self) {
        // Unmap only. Unlinking is the daemon's job — it owns the segment.
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
    }
}

/// The processing step: greyscale the left half, and draw a progress bar whose
/// width tracks the frame counter. Replace this with your own work.
fn process(pixels: &mut [u8], width: usize, height: usize, channels: usize, frame_id: u64) {
    let split = width / 2;

    for y in 0..height {
        let row = y * width * channels;
        for x in 0..split {
            let px = row + (x * channels);
            let luma = (0.299 * f32::from(pixels[px])
                + 0.587 * f32::from(pixels[px + 1])
                + 0.114 * f32::from(pixels[px + 2])) as u8;
            pixels[px..px + 3].fill(luma);
        }
    }

    // Brand orange, RGB — frames are RGB here, not BGR.
    let bar_width = (frame_id as usize % width).max(1);
    for y in height.saturating_sub(24)..height.saturating_sub(8) {
        let row = y * width * channels;
        for x in 0..bar_width {
            let px = row + (x * channels);
            pixels[px] = 255;
            pixels[px + 1] = 109;
            pixels[px + 2] = 14;
        }
    }
}

fn main() -> io::Result<()> {
    let shm_name = env::var("BRAIDPIPE_SHM_NAME").unwrap_or_else(|_| DEFAULT_SHM_NAME.into());
    let rust_sock = env::var("BRAIDPIPE_RUST_SOCK").unwrap_or_else(|_| DEFAULT_RUST_SOCK.into());
    let python_sock =
        env::var("BRAIDPIPE_PYTHON_SOCK").unwrap_or_else(|_| DEFAULT_PYTHON_SOCK.into());

    let mut shm = AttachedShm::attach(&shm_name).map_err(|e| {
        io::Error::other(format!(
            "cannot attach to shared memory {shm_name}: {e} — is the daemon running?"
        ))
    })?;

    // A stale socket file survives a SIGKILL; clear it before binding.
    let _ = fs::remove_file(&python_sock);
    let socket = UnixDatagram::bind(&python_sock)?;

    println!(
        "[rust-worker] attached to {shm_name} ({}x{} @ {}ch, {} slots), listening on {python_sock}",
        shm.width, shm.height, shm.channels, shm.slot_count
    );

    // Geometry is fixed for the life of the segment; copy it out so reading it
    // does not clash with the mutable borrow of the pixel slice below.
    let (width, height, channels, slot_count) =
        (shm.width, shm.height, shm.channels, shm.slot_count);

    let mut buf = [0u8; 512];
    loop {
        let bytes = socket.recv(&mut buf)?;
        let notice: FrameReadyPacket = match serde_json::from_slice(&buf[..bytes]) {
            Ok(packet) => packet,
            Err(error) => {
                eprintln!("[rust-worker] malformed notification: {error}");
                continue;
            }
        };

        if notice.slot_index >= slot_count {
            eprintln!("[rust-worker] slot {} out of range", notice.slot_index);
            continue;
        }

        let started = Instant::now();

        // Safety: the daemon just announced this slot and will not touch it
        // again until free_slot below.
        let pixels = unsafe { shm.pixels_mut(notice.slot_index) };
        process(pixels, width, height, channels, notice.frame_id);

        shm.free_slot(notice.slot_index);

        let ack = FrameProcessedPacket {
            frame_id: notice.frame_id,
            slot_index: notice.slot_index,
            processing_time_us: started.elapsed().as_micros() as u64,
            success: true,
        };

        // A full datagram buffer (ENOBUFS) means the daemon is not draining
        // acks this instant. Drop the ack; never let it kill the worker.
        let payload = serde_json::to_vec(&ack).expect("ack always serializes");
        if let Err(error) = socket.send_to(&payload, &rust_sock) {
            eprintln!(
                "[rust-worker] dropped ack for frame {}: {error}",
                notice.frame_id
            );
        }
    }
}
