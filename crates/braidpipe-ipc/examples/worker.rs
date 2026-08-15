//! A worker written in Rust — a drop-in replacement for a Python worker.
//!
//! Nothing about the contract is Python-specific: a worker is any process that
//! binds a Unix datagram socket, says hello to the daemon, receives the shared
//! memory segment's file descriptor as SCM_RIGHTS ancillary data, mutates
//! pixels in place, frees the slot, and acks. This example does exactly that
//! in about a hundred lines, reusing the daemon's own layout types so it
//! cannot drift out of sync with them.
//!
//! The segment is anonymous — it has no name in any filesystem or SHM
//! namespace. The fd handed over during the hello handshake is the only way
//! in, which is also why a stale segment can never survive a crashed daemon.
//!
//! # Running it
//!
//! ```sh
//! # terminal 1 — creates the segment, streams, and stays in passthrough
//! cargo run -p braidpipe --release -- --external-worker
//!
//! # terminal 2 — the AI branch is selected on this worker's first good frame
//! cargo run -p braidpipe-ipc --release --example worker
//! ```
//!
//! Kill this process and the stream keeps running on untouched frames, exactly
//! as it does for a Python worker.

use braidpipe_ipc::shm::{SLOT_FREE, ShmHeader, SlotHeader};
use braidpipe_ipc::uds::{FrameProcessedPacket, FrameReadyPacket, WORKER_HELLO};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixDatagram;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::{env, fs, io, ptr, slice};

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
    /// Maps the fd received from the daemon's hello reply. The daemon owns
    /// the geometry; everything is read from the fd and the header.
    fn attach(fd: RawFd) -> io::Result<Self> {
        unsafe {
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
        // Unmap only; the daemon's fd keeps the segment alive.
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
    }
}

/// Receives one datagram, extracting an SCM_RIGHTS fd if one rode along.
fn recv_with_fd(socket: &UnixDatagram, buf: &mut [u8]) -> io::Result<(usize, Option<RawFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };
    let mut cmsg_buf = [0u64; 8];

    unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = libc::CMSG_SPACE(size_of::<RawFd>() as u32) as _;

        let bytes = libc::recvmsg(socket.as_raw_fd(), &mut msg, 0);
        if bytes < 0 {
            return Err(io::Error::last_os_error());
        }

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if !cmsg.is_null()
            && (*cmsg).cmsg_level == libc::SOL_SOCKET
            && (*cmsg).cmsg_type == libc::SCM_RIGHTS
        {
            let mut fd: RawFd = -1;
            ptr::copy_nonoverlapping(
                libc::CMSG_DATA(cmsg),
                &mut fd as *mut RawFd as *mut u8,
                size_of::<RawFd>(),
            );
            return Ok((bytes as usize, Some(fd)));
        }
        Ok((bytes as usize, None))
    }
}

/// Says hello to the daemon until it answers with the segment fd. Retries
/// forever, so this worker may start before the daemon does.
fn handshake(socket: &UnixDatagram, rust_sock: &str) -> io::Result<RawFd> {
    socket.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut buf = [0u8; 512];
    let mut waiting_since: Option<Instant> = None;

    let fd = loop {
        if socket.send_to(WORKER_HELLO, rust_sock).is_err() {
            if waiting_since.is_none() {
                println!("[rust-worker] daemon not up yet at {rust_sock}; waiting...");
                waiting_since = Some(Instant::now());
            }
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        match recv_with_fd(socket, &mut buf) {
            Ok((_, Some(fd))) => break fd,
            // A frame notification that raced the handshake, or a timeout:
            // either way, ask again.
            Ok((_, None)) => continue,
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                continue
            }
            Err(error) => return Err(error),
        }
    };

    socket.set_read_timeout(None)?;
    Ok(fd)
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
    let rust_sock = env::var("BRAIDPIPE_RUST_SOCK").unwrap_or_else(|_| DEFAULT_RUST_SOCK.into());
    let python_sock =
        env::var("BRAIDPIPE_PYTHON_SOCK").unwrap_or_else(|_| DEFAULT_PYTHON_SOCK.into());

    // A stale socket file survives a SIGKILL; clear it before binding.
    let _ = fs::remove_file(&python_sock);
    let socket = UnixDatagram::bind(&python_sock)?;

    let fd = handshake(&socket, &rust_sock)?;
    let mut shm = AttachedShm::attach(fd)?;

    println!(
        "[rust-worker] attached to shared memory ({}x{} @ {}ch, {} slots), listening on {python_sock}",
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
            // Control packets (a duplicate handshake reply) are not malformed,
            // just not for this loop.
            Err(_) if buf[..bytes].windows(6).any(|w| w == b"\"type\"") => continue,
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
