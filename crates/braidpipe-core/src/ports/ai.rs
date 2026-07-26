use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameMetadata {
    pub frame_id: u64,
    pub slot_index: u8,
    pub timestamp_us: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("Shared Memory allocation or access failed: {0}")]
    ShmError(String),
    #[error("Socket communication error: {0}")]
    SocketError(String),
    #[error("Worker timed out: Python missed the frame deadline")]
    DeadlineMissed,
    #[error("Ring buffer exhausted: All SHM slots are currently busy")]
    BufferFull,
}

/// The abstraction for writing incoming GStreamer video frames to Shared Memory
pub trait ShmWriter: Send + Sync {
    /// Attempts to write raw video frame bytes into an available SHM slot.
    /// Returns the metadata containing the assigned slot index if successful.
    fn write_frame(&self, frame_id: u64, data: &[u8]) -> Result<FrameMetadata, IpcError>;
}

/// The abstraction for IPC signaling and Watchdog monitoring with Python
#[allow(async_fn_in_trait)]
pub trait AiBridge: Send + Sync {
    /// Sends a frame notification to Python over the UDS control channel
    async fn notify_frame_ready(&self, meta: FrameMetadata) -> Result<(), IpcError>;

    /// Awaits Python's response for a processed frame within a given timeout deadline
    async fn await_processed_frame(&self, timeout: Duration) -> Result<FrameMetadata, IpcError>;

    /// Checks if the Python worker process is alive and responding to heartbeats
    async fn is_healthy(&self) -> bool;
}
