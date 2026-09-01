//! The tcp-raw transport: raw frames to a worker on another machine.
//!
//! Where the shm transport hands a same-host worker the ring buffer itself,
//! this one streams the slots over the network. The negotiation stays the
//! same shape -- a worker says hello and is answered with a config packet --
//! but here the hello arrives over UDP and the config carries a TCP port
//! instead of a file descriptor. The worker then opens one TCP connection
//! and frames flow both ways on it: a fixed 24-byte header plus the raw
//! pixel payload, daemon -> worker for input, worker -> daemon for results.
//!
//! The shm ring stays the daemon's internal frame store either way. Sending
//! copies the slot's pixels onto the wire; a returned result is copied back
//! into the same slot and the slot freed, so the relay, the deadline logic,
//! and the watchdog cannot tell a remote worker from a local one.
//!
//! Backpressure is drop, not buffer: a send that cannot complete within one
//! frame deadline tears the connection down and the relay falls back to
//! passthrough -- exactly what a stalled local worker produces. A broker or
//! a blocking socket would instead queue stale frames, which is the one
//! behavior a never-go-dark pipeline cannot absorb.

use crate::shm::ShmRingBuffer;
use crate::uds::FrameProcessedPacket;
use braidpipe_core::ports::ai::{FrameMetadata, IpcError};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Wire header for one frame in either direction, 24 bytes little-endian:
/// frame_id u64 | time_us u64 | payload_len u32 | slot u8 | flags u8 | 2 pad.
/// `time_us` is the capture timestamp daemon -> worker and the worker's
/// self-reported processing time on the way back. Bit 0 of `flags` is
/// "success" on results. Python mirrors this as struct format "<QQIBB2x".
pub const WIRE_HEADER_SIZE: usize = 24;

const FLAG_SUCCESS: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireHeader {
    pub frame_id: u64,
    pub time_us: u64,
    pub payload_len: u32,
    pub slot: u8,
    pub flags: u8,
}

impl WireHeader {
    pub fn encode(&self) -> [u8; WIRE_HEADER_SIZE] {
        let mut buf = [0u8; WIRE_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.frame_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.time_us.to_le_bytes());
        buf[16..20].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[20] = self.slot;
        buf[21] = self.flags;
        buf
    }

    pub fn decode(buf: &[u8; WIRE_HEADER_SIZE]) -> Self {
        Self {
            frame_id: u64::from_le_bytes(buf[0..8].try_into().expect("8-byte slice")),
            time_us: u64::from_le_bytes(buf[8..16].try_into().expect("8-byte slice")),
            payload_len: u32::from_le_bytes(buf[16..20].try_into().expect("4-byte slice")),
            slot: buf[20],
            flags: buf[21],
        }
    }
}

#[derive(Debug, Deserialize)]
struct NetHello {
    r#type: String,
    #[serde(default)]
    transports: Option<Vec<String>>,
}

/// Shared state of the remote transport. Owned by the control bridge from
/// birth so its methods need no branching on "was remote enabled"; inert
/// (never attached) until [`enable`] binds the listeners.
pub struct RemoteState {
    shm: Arc<ShmRingBuffer>,
    ack_tx: mpsc::Sender<FrameProcessedPacket>,
    /// Write half of the current worker connection, tagged with its epoch so
    /// a reader that dies late cannot detach its successor.
    writer: Mutex<Option<(u64, OwnedWriteHalf)>>,
    epoch: AtomicU64,
    attached: AtomicBool,
    /// Frame deadline in microseconds; bounds how long a send may block.
    deadline_us: AtomicU64,
}

impl RemoteState {
    pub fn new(shm: Arc<ShmRingBuffer>, ack_tx: mpsc::Sender<FrameProcessedPacket>) -> Self {
        Self {
            shm,
            ack_tx,
            writer: Mutex::new(None),
            epoch: AtomicU64::new(0),
            attached: AtomicBool::new(false),
            deadline_us: AtomicU64::new(50_000),
        }
    }

    /// True while a remote worker holds a live data connection.
    pub fn attached(&self) -> bool {
        self.attached.load(Ordering::Relaxed)
    }

    fn deadline(&self) -> Duration {
        Duration::from_micros(self.deadline_us.load(Ordering::Relaxed))
    }

    async fn detach(&self, epoch: u64, reason: &str) {
        let mut writer = self.writer.lock().await;
        if let Some((current, _)) = writer.as_ref()
            && *current == epoch
        {
            *writer = None;
            self.attached.store(false, Ordering::Relaxed);
            info!(reason, "Remote worker detached");
        }
    }

    /// Attaches a freshly accepted connection, replacing any predecessor --
    /// a worker that reconnects must not be refused because its old
    /// connection has not timed out yet.
    async fn attach(self: &Arc<Self>, stream: TcpStream) {
        if let Err(error) = stream.set_nodelay(true) {
            warn!(%error, "Could not set TCP_NODELAY on worker connection");
        }
        let peer = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".into());
        let (read_half, write_half) = stream.into_split();

        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut writer = self.writer.lock().await;
            if writer.replace((epoch, write_half)).is_some() {
                warn!("New remote worker connection replaces the previous one");
            }
        }
        self.attached.store(true, Ordering::Relaxed);
        info!(peer, "Remote worker connected over tcp-raw");

        let state = Arc::clone(self);
        tokio::spawn(async move {
            let reason = state.read_results(read_half).await;
            state.detach(epoch, reason).await;
        });
    }

    /// Reads processed frames off the connection until it dies, landing each
    /// result in its shm slot and forwarding an ack to the bridge. Returns
    /// why the loop ended, for the detach log.
    async fn read_results(&self, mut read_half: tokio::net::tcp::OwnedReadHalf) -> &'static str {
        let payload_size = self.shm.frame_size();
        let mut header_buf = [0u8; WIRE_HEADER_SIZE];
        let mut payload = vec![0u8; payload_size];

        loop {
            if read_half.read_exact(&mut header_buf).await.is_err() {
                return "connection closed";
            }
            let header = WireHeader::decode(&header_buf);

            if header.payload_len as usize != payload_size {
                warn!(
                    got = header.payload_len,
                    expected = payload_size,
                    "Remote worker sent a mis-sized payload"
                );
                return "protocol error";
            }
            if read_half.read_exact(&mut payload).await.is_err() {
                return "connection closed mid-frame";
            }

            let success = header.flags & FLAG_SUCCESS != 0;
            if success && let Err(error) = self.shm.store_processed(header.slot, &payload) {
                warn!(%error, slot = header.slot, "Could not store a remote result");
                continue;
            }
            if !success {
                // The relay treats the ack itself as the failure signal; the
                // slot is reclaimed by the roundtrip's release path.
                debug!(frame_id = header.frame_id, "Remote worker reported failure");
            }

            let ack = FrameProcessedPacket {
                frame_id: header.frame_id,
                slot_index: header.slot,
                processing_time_us: header.time_us,
                success,
            };
            if self.ack_tx.send(ack).await.is_err() {
                return "bridge dropped";
            }
        }
    }

    /// Sends one frame to the attached worker: wire header plus the slot's
    /// pixels. A send that cannot finish within the frame deadline detaches
    /// the connection -- backpressure here must become passthrough, not a
    /// growing queue of stale frames.
    pub async fn send_frame(&self, meta: FrameMetadata) -> Result<(), IpcError> {
        let mut payload = vec![0u8; self.shm.frame_size()];
        self.shm.read_frame(meta.slot_index, &mut payload)?;

        let header = WireHeader {
            frame_id: meta.frame_id,
            time_us: meta.timestamp_us,
            payload_len: payload.len() as u32,
            slot: meta.slot_index,
            flags: 0,
        };

        let mut writer = self.writer.lock().await;
        let Some((_, write_half)) = writer.as_mut() else {
            return Err(IpcError::SocketError("no remote worker attached".into()));
        };

        let send = async {
            write_half.write_all(&header.encode()).await?;
            write_half.write_all(&payload).await?;
            Ok::<(), std::io::Error>(())
        };

        match timeout(self.deadline(), send).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                *writer = None;
                self.attached.store(false, Ordering::Relaxed);
                info!(reason = "send failed", "Remote worker detached");
                Err(IpcError::SocketError(format!(
                    "tcp-raw send failed: {error}"
                )))
            }
            Err(_) => {
                *writer = None;
                self.attached.store(false, Ordering::Relaxed);
                warn!("Remote worker cannot keep up with the frame rate; dropping it");
                Err(IpcError::DeadlineMissed)
            }
        }
    }

    /// Binds the negotiation (UDP) and data (TCP) listeners and spawns their
    /// accept loops. Returns the actually bound addresses, which differ from
    /// `listen` when it asked for port 0.
    pub async fn enable(
        self: &Arc<Self>,
        listen: SocketAddr,
        fps: u32,
    ) -> Result<(SocketAddr, SocketAddr), IpcError> {
        // The same 1.5-frame-period budget the relay grants a roundtrip.
        let deadline_us = (1.5 / f64::from(fps.max(1)) * 1e6) as u64;
        self.deadline_us.store(deadline_us, Ordering::Relaxed);

        let udp = UdpSocket::bind(listen)
            .await
            .map_err(|e| IpcError::SocketError(format!("Failed to bind worker UDP socket: {e}")))?;
        let tcp = TcpListener::bind(listen)
            .await
            .map_err(|e| IpcError::SocketError(format!("Failed to bind worker TCP socket: {e}")))?;

        let udp_addr = udp
            .local_addr()
            .map_err(|e| IpcError::SocketError(e.to_string()))?;
        let tcp_addr = tcp
            .local_addr()
            .map_err(|e| IpcError::SocketError(e.to_string()))?;

        info!(
            control = %udp_addr,
            data = %tcp_addr,
            "Listening for remote workers (tcp-raw)"
        );

        let config = self.tcp_config_json(tcp_addr.port());
        tokio::spawn(negotiation_loop(udp, config));

        let state = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match tcp.accept().await {
                    Ok((stream, _)) => state.attach(stream).await,
                    Err(error) => {
                        warn!(%error, "Failed to accept a worker connection");
                    }
                }
            }
        });

        Ok((udp_addr, tcp_addr))
    }

    /// The config packet answering a network hello: everything a remote
    /// worker needs to open the data connection and parse what flows on it.
    fn tcp_config_json(&self, data_port: u16) -> String {
        let h = self.shm.header();
        format!(
            concat!(
                "{{\"type\":\"config\",\"transport\":\"tcp-raw\",\"contract\":{},",
                "\"data_port\":{},",
                "\"width\":{},\"height\":{},\"channels\":{},\"format\":\"rgb\"}}"
            ),
            crate::IPC_CONTRACT_VERSION,
            data_port,
            h.width,
            h.height,
            h.channels
        )
    }
}

/// Answers UDP hellos with the tcp-raw config, forever. Unlike the UDS
/// handshake this needs no flowing frames -- the socket has its own task.
async fn negotiation_loop(udp: UdpSocket, config: String) {
    let mut buf = [0u8; 512];
    loop {
        let Ok((bytes, peer)) = udp.recv_from(&mut buf).await else {
            continue;
        };
        let Ok(hello) = serde_json::from_slice::<NetHello>(&buf[..bytes]) else {
            debug!(%peer, "Ignored malformed datagram on the worker control port");
            continue;
        };
        if hello.r#type != "hello" {
            continue;
        }

        let reply = match hello.transports {
            Some(ref t) if !t.iter().any(|t| t == "tcp-raw") => {
                warn!(%peer, "Worker asked for a transport this listener does not carry");
                "{\"type\":\"error\",\"reason\":\"only tcp-raw is available over the network\"}"
                    .to_string()
            }
            _ => config.clone(),
        };
        if let Err(error) = udp.send_to(reply.as_bytes(), peer).await {
            warn!(%error, %peer, "Could not answer a worker hello");
        } else {
            info!(%peer, "Remote worker said hello; sent it the tcp-raw config");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uds::UdsControlBridge;
    use braidpipe_core::ports::ai::{AiBridge, ShmWriter};

    #[test]
    fn wire_header_roundtrips() {
        let header = WireHeader {
            frame_id: 0x0123_4567_89ab_cdef,
            time_us: 42_000_000,
            payload_len: 1280 * 720 * 3,
            slot: 3,
            flags: FLAG_SUCCESS,
        };
        assert_eq!(WireHeader::decode(&header.encode()), header);
    }

    /// The whole remote loop against a real bridge: UDP hello answered with
    /// the tcp-raw config, a frame pushed over TCP, the inverted result
    /// landing back in the slot and its ack in `await_processed_frame`.
    #[tokio::test]
    async fn remote_worker_roundtrip_over_tcp() {
        let dir = std::env::temp_dir().join(format!("braidpipe-nettest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rust_sock = dir.join("rust.sock");
        let python_sock = dir.join("python.sock");
        let _ = std::fs::remove_file(&rust_sock);

        let shm = Arc::new(ShmRingBuffer::create(4, 2, 3, 2).unwrap());
        let bridge = UdsControlBridge::bind(
            rust_sock.to_str().unwrap(),
            python_sock.to_str().unwrap(),
            Arc::clone(&shm),
        )
        .await
        .unwrap();
        let (control_addr, _) = bridge
            .enable_remote("127.0.0.1:0".parse().unwrap(), 30)
            .await
            .unwrap();

        // Worker side of the negotiation.
        let worker_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        worker_udp
            .send_to(
                b"{\"type\":\"hello\",\"transports\":[\"tcp-raw\"]}",
                control_addr,
            )
            .await
            .unwrap();
        let mut buf = [0u8; 512];
        let (bytes, _) = worker_udp.recv_from(&mut buf).await.unwrap();
        let config: serde_json::Value = serde_json::from_slice(&buf[..bytes]).unwrap();
        assert_eq!(config["type"], "config");
        assert_eq!(config["transport"], "tcp-raw");
        assert_eq!(config["contract"], crate::IPC_CONTRACT_VERSION);
        assert_eq!(config["width"], 4);
        assert_eq!(config["height"], 2);
        assert_eq!(config["channels"], 3);
        let data_port = config["data_port"].as_u64().unwrap() as u16;

        let mut stream = TcpStream::connect(("127.0.0.1", data_port)).await.unwrap();

        // The accept loop attaches asynchronously; the notify falls back to
        // the (dead) UDS path until it has, so retry until the send lands.
        let pixels: Vec<u8> = (0..24).collect();
        let meta = shm.write_frame(7, &pixels).unwrap();
        let mut tries = 0;
        while bridge.notify_frame_ready(meta).await.is_err() {
            tries += 1;
            assert!(tries < 200, "remote worker never attached");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Worker receives the frame...
        let mut header_buf = [0u8; WIRE_HEADER_SIZE];
        stream.read_exact(&mut header_buf).await.unwrap();
        let header = WireHeader::decode(&header_buf);
        assert_eq!(header.frame_id, 7);
        assert_eq!(header.payload_len as usize, pixels.len());
        let mut payload = vec![0u8; pixels.len()];
        stream.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload, pixels);

        // ...processes it (inversion), and sends the result back.
        for byte in payload.iter_mut() {
            *byte = 255 - *byte;
        }
        let reply = WireHeader {
            frame_id: 7,
            time_us: 1234,
            payload_len: payload.len() as u32,
            slot: header.slot,
            flags: FLAG_SUCCESS,
        };
        stream.write_all(&reply.encode()).await.unwrap();
        stream.write_all(&payload).await.unwrap();

        let ack = bridge
            .await_processed_frame(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(ack.frame_id, 7);
        assert_eq!(ack.slot_index, header.slot);
        assert_eq!(ack.timestamp_us, 1234);

        let mut out = vec![0u8; pixels.len()];
        shm.read_frame(ack.slot_index, &mut out).unwrap();
        assert_eq!(out[0], 255);
        assert_eq!(out[23], 255 - 23);

        let _ = std::fs::remove_file(&rust_sock);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
