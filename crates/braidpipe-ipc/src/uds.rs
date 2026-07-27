use braidpipe_core::ports::ai::{AiBridge, FrameMetadata, IpcError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::net::UnixDatagram;
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Consecutive frame-roundtrip failures before the worker is declared unhealthy
const MAX_FAILURE_STREAK: u32 = 30;

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

pub struct UdsControlBridge {
    socket: UnixDatagram,
    python_sock_path: PathBuf,
    failure_streak: AtomicU32,
}

impl UdsControlBridge {
    pub async fn bind(rust_sock_path: &str, python_sock_path: &str) -> Result<Self, IpcError> {
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

        Ok(Self {
            socket,
            python_sock_path: python_path,
            // Start unhealthy: the AI branch must prove itself with one
            // successful frame roundtrip before the Watchdog may select it.
            failure_streak: AtomicU32::new(MAX_FAILURE_STREAK),
        })
    }

    /// Called by the frame relay after a successful Python roundtrip
    pub fn record_success(&self) {
        self.failure_streak.store(0, Ordering::Relaxed);
    }

    /// Called by the frame relay when Python misses a deadline or errors
    pub fn record_failure(&self) {
        self.failure_streak.fetch_add(1, Ordering::Relaxed);
    }
}

impl AiBridge for UdsControlBridge {
    async fn notify_frame_ready(&self, meta: FrameMetadata) -> Result<(), IpcError> {
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

        // Wrap socket read in a Tokio timeout corresponding to the Watchdog's 33ms frame budget
        let read_result = timeout(frame_deadline, self.socket.recv(&mut buf)).await;

        match read_result {
            Ok(Ok(bytes_read)) => {
                let packet: FrameProcessedPacket = serde_json::from_slice(&buf[..bytes_read])
                    .map_err(|e| IpcError::SocketError(format!("Invalid ACK JSON: {e}")))?;

                if !packet.success {
                    return Err(IpcError::SocketError(
                        "Python reported inference failure".into(),
                    ));
                }

                Ok(FrameMetadata {
                    frame_id: packet.frame_id,
                    slot_index: packet.slot_index,
                    timestamp_us: packet.processing_time_us,
                })
            }
            Ok(Err(e)) => Err(IpcError::SocketError(format!("UDS recv error: {e}"))),
            Err(_) => {
                // Deadline missed! The Watchdog will catch this and trigger Passthrough Mode
                warn!("Python failed to respond within target deadline!");
                Err(IpcError::DeadlineMissed)
            }
        }
    }

    async fn is_healthy(&self) -> bool {
        // The worker must both be listening on its socket AND keeping up with
        // the frame deadlines (a stale socket file survives a SIGKILL).
        self.python_sock_path.exists()
            && self.failure_streak.load(Ordering::Relaxed) < MAX_FAILURE_STREAK
    }
}
