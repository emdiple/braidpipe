use crate::ports::ai::AiBridge;
use crate::ports::media::{ActiveBranch, StreamController};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

pub struct Watchdog<M, A>
where
    M: StreamController,
    A: AiBridge,
{
    media: Arc<M>,
    ai: Arc<A>,
    health_check_interval: Duration,
}

impl<M, A> Watchdog<M, A>
where
    M: StreamController,
    A: AiBridge,
{
    pub fn new(media: Arc<M>, ai: Arc<A>, fps: u32) -> Self {
        let health_check_interval = Duration::from_secs_f64(1.0 / f64::from(fps.max(1)));

        Self {
            media,
            ai,
            health_check_interval,
        }
    }

    pub async fn run_loop(&mut self) {
        if let Err(error) = self.media.start().await {
            error!(%error, "Unable to start media pipeline");
            return;
        }

        let mut ticker = time::interval(self.health_check_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = ticker.tick() => self.update_active_branch().await,
            }
        }

        if let Err(error) = self.media.stop().await {
            error!(%error, "Unable to stop media pipeline");
        }
    }

    async fn update_active_branch(&self) {
        let target_branch = if self.ai.is_healthy().await {
            ActiveBranch::AiProcess
        } else {
            ActiveBranch::Passthrough
        };

        if self.media.current_branch() == target_branch {
            return;
        }

        if let Err(error) = self.media.set_active_branch(target_branch).await {
            warn!(%error, ?target_branch, "Unable to switch active stream branch");
        }
    }
}
