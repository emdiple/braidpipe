use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveBranch {
    Passthrough,
    AiProcess,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("Failed to build pipeline: {0}")]
    BuildFailed(String),
    #[error("Pipeline state change error: {0}")]
    StateChangeFailed(String),
    #[error("Dynamic switch failed: {0}")]
    SwitchFailed(String),
}

/// The core Watchdog will call this trait without needing to know GStreamer exists.
#[async_trait]
pub trait StreamController: Send + Sync {
    /// Flips the GStreamer input-selector to Passthrough or AI
    async fn set_active_branch(&self, branch: ActiveBranch) -> Result<(), EngineError>;
    
    /// Returns the current active branch
    fn current_branch(&self) -> ActiveBranch;

    /// Starts the media pipeline
    async fn start(&self) -> Result<(), EngineError>;

    /// Stops the pipeline gracefully
    async fn stop(&self) -> Result<(), EngineError>;
}