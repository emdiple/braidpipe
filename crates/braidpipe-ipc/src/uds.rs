use crate::net::RemoteState;
use crate::shm::ShmRingBuffer;
use braidpipe_core::ports::ai::{AiBridge, FrameMetadata, IpcError};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::net::UnixDatagram;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Consecutive frame-roundtrip failures before the worker is declared unhealthy
const MAX_FAILURE_STREAK: u32 = 30;

/// What a worker sends to announce itself. It may also carry a `transports`
/// list (`["shm", "tcp-raw"]`); absent means shm, which is all a Unix-socket
/// hello can be answered with anyway. The daemon's reply is a `config` packet
/// describing the chosen transport -- over UDS it carries the segment fd as
/// SCM_RIGHTS ancillary data, over UDP it carries the TCP data port instead.
pub const WORKER_HELLO: &[u8] = b"{\"type\":\"hello\"}";

/// Control packet sent from Rust -> Python
#[derive(Debug, Serialize, Deserialize)]
pub struct FrameReadyPacket {
    pub frame_id: u64,
    pub slot_index: u8,
    pub timestamp_us: u64,
}

/// Acknowledgment packet sent from Python -> Rust
#[derive(Debug, Serialize, Deserialize)]
pub struct FrameProcessedPacket {
    pub frame_id: u64,
    pub slot_index: u8,
    pub processing_time_us: u64,
    pub success: bool,
}

#[derive(Debug, Deserialize)]
struct ControlPacket {
    r#type: String,
}

pub struct UdsControlBridge {
    socket: UnixDatagram,
    python_sock_path: PathBuf,
    /// Held so the segment fd stays valid for every hello this bridge answers.
    shm: Arc<ShmRingBuffer>,
    failure_streak: AtomicU32,
    /// The config packet answering a UDS hello (the fd rides alongside it).
    shm_config: String,
    /// The tcp-raw transport for workers on other machines. Inert until
    /// [`enable_remote`] binds its listeners.
    remote: Arc<RemoteState>,
    /// Acks arriving over tcp-raw, merged into `await_processed_frame`.
    remote_acks: tokio::sync::Mutex<mpsc::Receiver<FrameProcessedPacket>>,
}

impl UdsControlBridge {
    pub async fn bind(
        rust_sock_path: &str,
        python_sock_path: &str,
        shm: Arc<ShmRingBuffer>,
    ) -> Result<Self, IpcError> {
        let rust_path = PathBuf::from(rust_sock_path);
        let python_path = PathBuf::from(python_sock_path);

        // Clean up stale socket file if left over from a previous crash
        if rust_path.exists() {
            let _ = tokio::fs::remove_file(&rust_path).await;
        }

        let socket = UnixDatagram::bind(&rust_path)
            .map_err(|e| IpcError::SocketError(format!("Failed to bind Rust UDS socket: {e}")))?;

        info!(
            rust_socket = %rust_sock_path,
            python_socket = %python_sock_path,
            "UDS Control Bridge initialized"
        );

        let header = shm.header();
        let shm_config = format!(
            concat!(
                "{{\"type\":\"config\",\"transport\":\"shm\",\"contract\":{},",
                "\"width\":{},\"height\":{},",
                "\"channels\":{},\"slots\":{},\"format\":\"rgb\"}}"
            ),
            crate::IPC_CONTRACT_VERSION,
            header.width,
            header.height,
            header.channels,
            header.slot_count
        );

        // Sized for a few stale acks beyond the one in flight; the reader
        // task blocks (and eventually detaches) rather than piling up more.
        let (ack_tx, ack_rx) = mpsc::channel(8);

        Ok(Self {
            socket,
            python_sock_path: python_path,
            shm: Arc::clone(&shm),
            // Start unhealthy: the AI branch must prove itself with one
            // successful frame roundtrip before the Watchdog may select it.
            failure_streak: AtomicU32::new(MAX_FAILURE_STREAK),
            shm_config,
            remote: Arc::new(RemoteState::new(shm, ack_tx)),
            remote_acks: tokio::sync::Mutex::new(ack_rx),
        })
    }

    /// Starts listening for remote workers on `listen`: UDP for the
    /// hello/config negotiation, TCP on the same port for the raw frame
    /// exchange. Returns the bound (control, data) addresses.
    pub async fn enable_remote(
        &self,
        listen: SocketAddr,
        fps: u32,
    ) -> Result<(SocketAddr, SocketAddr), IpcError> {
        self.remote.enable(listen, fps).await
    }

    /// Answers a worker's hello: the shm config packet, with ancillary data
    /// duplicating the segment fd into the worker. The worker fstats the fd
    /// for the size and reads the ring geometry from the segment header, so
    /// the body is informative rather than load-bearing.
    fn send_shm_fd(&self) {
        match send_fd_to(
            self.socket.as_raw_fd(),
            &self.python_sock_path,
            self.shm_config.as_bytes(),
            self.shm.fd(),
        ) {
            Ok(()) => info!("Worker said hello; passed it the shared-memory fd"),
            Err(error) => warn!(%error, "Could not send the shared-memory fd to the worker"),
        }
    }

    /// Called by the frame relay after a successful Python roundtrip
    pub fn record_success(&self) {
        self.failure_streak.store(0, Ordering::Relaxed);
        braidpipe_core::metrics::FAILURE_STREAK.set(0);
    }

    /// Called by the frame relay when Python misses a deadline or errors
    pub fn record_failure(&self) {
        let streak = self.failure_streak.fetch_add(1, Ordering::Relaxed) + 1;
        braidpipe_core::metrics::FAILURE_STREAK.set(i64::from(streak));
    }
}

/// Turns a worker's ack into the relay-facing result, whichever transport
/// carried it. The ack's processing time rides in the metadata's timestamp.
fn ack_to_metadata(packet: FrameProcessedPacket) -> Result<FrameMetadata, IpcError> {
    if !packet.success {
        return Err(IpcError::SocketError(
            "Worker reported inference failure".into(),
        ));
    }
    Ok(FrameMetadata {
        frame_id: packet.frame_id,
        slot_index: packet.slot_index,
        timestamp_us: packet.processing_time_us,
    })
}

impl AiBridge for UdsControlBridge {
    async fn notify_frame_ready(&self, meta: FrameMetadata) -> Result<(), IpcError> {
        // A remote worker takes the whole frame with the notification; a
        // local one is only told which slot to look at.
        if self.remote.attached() {
            return self.remote.send_frame(meta).await;
        }

        let packet = FrameReadyPacket {
            frame_id: meta.frame_id,
            slot_index: meta.slot_index,
            timestamp_us: meta.timestamp_us,
        };

        let json_bytes = serde_json::to_vec(&packet)
            .map_err(|e| IpcError::SocketError(format!("Failed to serialize UDS packet: {e}")))?;

        self.socket
            .send_to(&json_bytes, &self.python_sock_path)
            .await
            .map_err(|e| {
                IpcError::SocketError(format!("Failed to send datagram to Python: {e}"))
            })?;

        debug!(
            frame_id = meta.frame_id,
            slot = meta.slot_index,
            "Signaled Python via UDS"
        );
        Ok(())
    }

    async fn await_processed_frame(
        &self,
        frame_deadline: Duration,
    ) -> Result<FrameMetadata, IpcError> {
        let mut buf = [0u8; 512];
        let started = Instant::now();
        let mut remote_acks = self.remote_acks.lock().await;

        enum Event {
            Datagram(usize),
            RemoteAck(FrameProcessedPacket),
        }

        // The UDS socket carries acks and worker hellos, and tcp-raw acks
        // arrive on their own channel, so this loops: a hello is answered
        // with the segment fd and the wait continues on whatever deadline
        // budget is left.
        loop {
            let remaining = frame_deadline
                .checked_sub(started.elapsed())
                .ok_or(IpcError::DeadlineMissed)?;

            let event = timeout(remaining, async {
                tokio::select! {
                    read = self.socket.recv(&mut buf) => read.map(Event::Datagram),
                    ack = remote_acks.recv() => {
                        // The bridge owns a sender for as long as it lives.
                        Ok(Event::RemoteAck(ack.expect("remote ack channel closed")))
                    }
                }
            })
            .await;

            match event {
                Ok(Ok(Event::Datagram(bytes_read))) => {
                    let data = &buf[..bytes_read];

                    if let Ok(control) = serde_json::from_slice::<ControlPacket>(data) {
                        if control.r#type == "hello" {
                            self.send_shm_fd();
                        } else {
                            debug!(kind = %control.r#type, "Ignored unknown control packet");
                        }
                        continue;
                    }

                    let packet: FrameProcessedPacket = serde_json::from_slice(data)
                        .map_err(|e| IpcError::SocketError(format!("Invalid ACK JSON: {e}")))?;
                    return ack_to_metadata(packet);
                }
                Ok(Ok(Event::RemoteAck(packet))) => return ack_to_metadata(packet),
                Ok(Err(e)) => return Err(IpcError::SocketError(format!("UDS recv error: {e}"))),
                Err(_) => {
                    // Deadline missed! The Watchdog will catch this and trigger Passthrough Mode
                    warn!("Worker failed to respond within target deadline!");
                    return Err(IpcError::DeadlineMissed);
                }
            }
        }
    }

    async fn is_healthy(&self) -> bool {
        // The worker must be reachable -- a local socket file or a live
        // tcp-raw connection -- AND keeping up with the frame deadlines
        // (a stale socket file survives a SIGKILL).
        let reachable = self.python_sock_path.exists() || self.remote.attached();
        let healthy = reachable && self.failure_streak.load(Ordering::Relaxed) < MAX_FAILURE_STREAK;
        braidpipe_core::metrics::WORKER_HEALTHY.set(i64::from(healthy));
        healthy
    }
}

/// Sends `payload` as a datagram to `dest` with `fd` attached as SCM_RIGHTS
/// ancillary data. The kernel duplicates the descriptor into the receiver, so
/// the two processes end up with independent fds for the same segment.
///
/// std's ancillary-data API is still unstable, hence raw libc. The buffer is
/// u64-aligned, which satisfies cmsghdr alignment on every supported platform.
fn send_fd_to(socket_fd: RawFd, dest: &Path, payload: &[u8], fd: RawFd) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let dest_bytes = dest.as_os_str().as_bytes();
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if dest_bytes.len() >= addr.sun_path.len() {
        return Err(std::io::Error::other("socket path too long"));
    }
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (slot, byte) in addr.sun_path.iter_mut().zip(dest_bytes) {
        *slot = *byte as libc::c_char;
    }
    let addr_len = (std::mem::size_of::<libc::sockaddr_un>() - addr.sun_path.len()
        + dest_bytes.len()) as libc::socklen_t;

    let mut iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let mut cmsg_buf = [0u64; 8];

    unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_name = &mut addr as *mut _ as *mut libc::c_void;
        msg.msg_namelen = addr_len;
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) as _;

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
        std::ptr::copy_nonoverlapping(
            &fd as *const RawFd as *const u8,
            libc::CMSG_DATA(cmsg),
            std::mem::size_of::<RawFd>(),
        );

        if libc::sendmsg(socket_fd, &msg, 0) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixDatagram as StdUnixDatagram;

    /// Receives one datagram plus one SCM_RIGHTS fd -- the worker side of the
    /// handshake, kept here so the send path is tested against a real recvmsg.
    fn recv_fd(socket: &StdUnixDatagram, buf: &mut [u8]) -> (usize, RawFd) {
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
            msg.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) as _;

            let bytes = libc::recvmsg(socket.as_raw_fd(), &mut msg, 0);
            assert!(
                bytes >= 0,
                "recvmsg failed: {}",
                std::io::Error::last_os_error()
            );

            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            assert!(!cmsg.is_null(), "no ancillary data received");
            assert_eq!((*cmsg).cmsg_level, libc::SOL_SOCKET);
            assert_eq!((*cmsg).cmsg_type, libc::SCM_RIGHTS);

            let mut fd: RawFd = -1;
            std::ptr::copy_nonoverlapping(
                libc::CMSG_DATA(cmsg),
                &mut fd as *mut RawFd as *mut u8,
                std::mem::size_of::<RawFd>(),
            );
            (bytes as usize, fd)
        }
    }

    #[test]
    fn fd_passing_duplicates_a_live_descriptor() {
        let dir = std::env::temp_dir().join(format!("braidpipe-fdtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sender_path = dir.join("sender.sock");
        let receiver_path = dir.join("receiver.sock");
        let _ = std::fs::remove_file(&sender_path);
        let _ = std::fs::remove_file(&receiver_path);

        let sender = StdUnixDatagram::bind(&sender_path).unwrap();
        let receiver = StdUnixDatagram::bind(&receiver_path).unwrap();

        // Pass the fd of a real shared segment, as the daemon does.
        let payload = b"{\"type\":\"config\",\"transport\":\"shm\"}";
        let shm = ShmRingBuffer::create(4, 2, 3, 2).unwrap();
        send_fd_to(sender.as_raw_fd(), &receiver_path, payload, shm.fd()).unwrap();

        let mut buf = [0u8; 128];
        let (bytes, fd) = recv_fd(&receiver, &mut buf);
        assert_eq!(&buf[..bytes], payload);
        assert_ne!(
            fd,
            shm.fd(),
            "kernel must hand over a duplicate, not the same number"
        );

        // The duplicate describes the same segment: fstat sees its size.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(fd, &mut stat) }, 0);
        assert!(stat.st_size > 0);
        unsafe { libc::close(fd) };

        let _ = std::fs::remove_file(&sender_path);
        let _ = std::fs::remove_file(&receiver_path);
        let _ = std::fs::remove_dir(&dir);
    }
}
