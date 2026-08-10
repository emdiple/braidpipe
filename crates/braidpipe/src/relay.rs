//! The frame relay: the data path of the AI branch.
//!
//! Frames tapped by the pipeline's `ai_sink` are copied into the shared memory
//! ring buffer, Python is signaled over UDS, and the processed pixels are read
//! back and pushed into `ai_src` so the input-selector's AI pad always has
//! data. If Python misses its deadline or errors, the original frame is pushed
//! through unchanged, so the output stream never goes dark even while the
//! Watchdog still has the AI branch selected.

use braidpipe_core::metrics;
use braidpipe_core::ports::ai::{AiBridge, IpcError, ShmWriter};
use braidpipe_ipc::shm::ShmRingBuffer;
use braidpipe_ipc::uds::UdsControlBridge;
use gstreamer as gst;
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

struct RawFrame {
    data: Vec<u8>,
    pts: Option<gst::ClockTime>,
    duration: Option<gst::ClockTime>,
    caps: Option<gst::Caps>,
}

/// Attaches the relay to the pipeline's app elements and spawns its async loop.
/// Must be called before the pipeline starts so the appsink caps take effect.
pub fn spawn(
    ai_sink: AppSink,
    ai_src: AppSrc,
    shm: Arc<ShmRingBuffer>,
    bridge: Arc<UdsControlBridge>,
    width: u32,
    height: u32,
    fps: u32,
) {
    // Constrain the AI tap to raw RGB frames matching the SHM slot geometry
    let caps = gst::Caps::builder("video/x-raw")
        .field("format", "RGB")
        .field("width", width as i32)
        .field("height", height as i32)
        .build();
    ai_sink.set_caps(Some(&caps));

    let (tx, rx) = mpsc::channel::<RawFrame>(1);

    ai_sink.set_callbacks(
        AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                if let Some(buffer) = sample.buffer()
                    && let Ok(map) = buffer.map_readable()
                {
                    // try_send: if the relay is still busy with the previous
                    // frame, drop this one instead of blocking the streaming
                    // thread.
                    let sent = tx.try_send(RawFrame {
                        data: map.as_slice().to_vec(),
                        pts: buffer.pts(),
                        duration: buffer.duration(),
                        caps: sample.caps().map(|c| c.to_owned()),
                    });
                    if sent.is_err() {
                        metrics::RELAY_CHANNEL_DROPS.inc();
                    }
                }
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );

    // Allow 1.5 frame periods for the full SHM + UDS roundtrip
    let deadline = Duration::from_secs_f64(1.5 / f64::from(fps.max(1)));
    metrics::RELAY_DEADLINE_SECONDS.set(deadline.as_secs_f64());

    info!(
        width,
        height,
        ?deadline,
        "Frame relay attached to AI branch"
    );
    tokio::spawn(relay_loop(rx, ai_src, shm, bridge, deadline));
}

async fn relay_loop(
    mut rx: mpsc::Receiver<RawFrame>,
    ai_src: AppSrc,
    shm: Arc<ShmRingBuffer>,
    bridge: Arc<UdsControlBridge>,
    deadline: Duration,
) {
    let mut frame_id: u64 = 0;
    let mut caps_applied = false;

    while let Some(frame) = rx.recv().await {
        frame_id += 1;

        // `data` doubles as the output buffer: on any AI failure it still
        // holds the original pixels, which then pass through unchanged.
        let mut data = frame.data;

        let started = Instant::now();
        match roundtrip(&shm, &bridge, frame_id, &mut data, deadline).await {
            Ok(processing_us) => {
                bridge.record_success();
                metrics::FRAMES_AI.inc();
                metrics::ROUNDTRIP_SECONDS.observe_seconds(started.elapsed().as_secs_f64());
                metrics::WORKER_PROCESSING_SECONDS
                    .observe_seconds(processing_us as f64 / 1e6);
                metrics::LAST_AI_FRAME_TIMESTAMP.set(metrics::unix_now());
            }
            Err(error) => {
                bridge.record_failure();
                metrics::FRAMES_PASSTHROUGH.inc();
                debug!(%error, frame_id, "AI roundtrip failed; passing frame through unchanged");
            }
        }

        if !caps_applied && let Some(caps) = &frame.caps {
            ai_src.set_caps(Some(caps));
            caps_applied = true;
        }

        let mut buffer = gst::Buffer::from_mut_slice(data);
        {
            let buffer_ref = buffer.get_mut().expect("newly created buffer is writable");
            buffer_ref.set_pts(frame.pts);
            buffer_ref.set_duration(frame.duration);
        }

        if let Err(error) = ai_src.push_buffer(buffer) {
            warn!(?error, "Failed to push frame into ai_src");
        }
    }
}

/// Writes a frame to SHM, signals Python, and waits for the matching ack.
/// On success `data` is overwritten with the processed pixels, and the
/// worker's self-reported processing time (µs) is returned.
async fn roundtrip(
    shm: &ShmRingBuffer,
    bridge: &UdsControlBridge,
    frame_id: u64,
    data: &mut [u8],
    deadline: Duration,
) -> Result<u64, IpcError> {
    let meta = shm.write_frame(frame_id, data).inspect_err(|_| {
        metrics::SHM_FULL.inc();
    })?;

    let result = notify_and_await(shm, bridge, meta, data, deadline).await;
    if result.is_err() {
        // Python never processed this frame; take the slot back so a dead or
        // slow worker cannot exhaust the ring buffer.
        shm.release_slot(meta.slot_index);
    }
    result
}

async fn notify_and_await(
    shm: &ShmRingBuffer,
    bridge: &UdsControlBridge,
    meta: braidpipe_core::ports::ai::FrameMetadata,
    data: &mut [u8],
    deadline: Duration,
) -> Result<u64, IpcError> {
    let frame_id = meta.frame_id;
    let slot_index = meta.slot_index;
    bridge.notify_frame_ready(meta).await?;

    let started = Instant::now();
    loop {
        let remaining = deadline
            .checked_sub(started.elapsed())
            .ok_or(IpcError::DeadlineMissed)?;

        let ack = bridge.await_processed_frame(remaining).await?;
        if ack.frame_id == frame_id {
            shm.read_frame(slot_index, data)?;
            // The bridge hands the ack's processing_time_us back through the
            // metadata's timestamp field.
            return Ok(ack.timestamp_us);
        }

        // A stale ack from a previously timed-out frame; keep waiting
        metrics::STALE_ACKS.inc();
        debug!(
            stale_id = ack.frame_id,
            expected_id = frame_id,
            "Discarded stale Python ack"
        );
    }
}
