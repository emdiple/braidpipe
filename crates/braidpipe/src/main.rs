mod metrics_server;
mod preset;
mod relay;

use braidpipe_core::fsm::Watchdog;
use braidpipe_core::ports::media::StreamController;
use braidpipe_engine::pipeline::GStreamerEngine;
use braidpipe_ipc::shm::ShmRingBuffer;
use braidpipe_ipc::uds::UdsControlBridge;
use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::Command;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_SOURCE: &str = "videotestsrc is-live=true pattern=ball ! video/x-raw,width=1280,height=720,framerate=30/1 ! videoconvert";

#[derive(Parser, Debug)]
#[command(author, version, about = "Zero-downtime AI video middleware daemon", long_about = None)]
struct Args {
    /// GStreamer source string for inputs requiring a custom launch pipeline
    #[arg(short = 'i', long, conflicts_with = "uri")]
    source: Option<String>,

    /// Input URI decoded by GStreamer's uridecodebin3 (for example: srt://, udp://, rtp://, or ndi://)
    #[arg(long, value_name = "URI", conflicts_with = "source")]
    uri: Option<String>,

    /// GStreamer sink string (e.g., "autovideosink" or "srtsink uri=srt://127.0.0.1:8888")
    #[arg(short = 'o', long, default_value = "videoconvert ! autovideosink")]
    sink: String,

    /// Output URL to publish to (rtmp://, srt:// or udp://host:port); builds the
    /// encoder and sink from --preset instead of requiring a full --sink string
    #[arg(long, value_name = "URL", conflicts_with = "sink")]
    output: Option<String>,

    /// Latency/bandwidth profile used with --output: zerolatency, lowlatency,
    /// balanced or bandwidth. Individual parameters yield to BRAIDPIPE_* env vars
    #[arg(long, default_value = "lowlatency", requires = "output")]
    preset: String,

    /// Carry the source's audio through to the output. Audio bypasses the AI
    /// branch and is re-joined at the muxer; sync is preserved by timestamps
    #[arg(long)]
    audio: bool,

    /// Path to the Python worker script or virtualenv executable
    #[arg(short, long, default_value = "python/braidpipe/worker.py")]
    python_script: PathBuf,

    /// Target stream frame rate (used to calculate 33ms/16ms Watchdog deadlines)
    #[arg(short, long, default_value_t = 30)]
    fps: u32,

    /// Frame width in pixels
    #[arg(long, default_value_t = 1280)]
    width: u32,

    /// Frame height in pixels
    #[arg(long, default_value_t = 720)]
    height: u32,

    /// POSIX Shared Memory handle name
    #[arg(long, default_value = "/braidpipe_buffer")]
    shm_name: String,

    /// Socket path for Rust UDS listener
    #[arg(long, default_value = "/tmp/braidpipe_rust.sock")]
    rust_sock: String,

    /// Socket path for Python UDS listener
    #[arg(long, default_value = "/tmp/braidpipe_python.sock")]
    python_sock: String,

    /// Keep the stream on the passthrough branch without starting the Python worker
    #[arg(long)]
    passthrough_only: bool,

    /// Port for the Prometheus /metrics endpoint on 127.0.0.1 (0 to disable)
    #[arg(long, default_value_t = 9184)]
    metrics_port: u16,
}

type AppError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> Result<(), AppError> {
    braidpipe_engine::gst_mac::run(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(run())
    })
}

async fn run() -> Result<(), AppError> {
    // 1. Initialize structured log output with RUST_LOG filter support
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!("Starting braidpipe daemon...");

    let sink = match args.output.as_deref() {
        Some(output) => {
            let desc = preset::build_sink(&args.preset, output, args.fps)?;
            info!(preset = %args.preset, sink = %desc, "Built sink from preset");
            desc
        }
        None => args.sink.clone(),
    };

    let audio_branch = if args.audio {
        let branch = preset::audio_branch()?;
        // The generated branch links two elements by name; make sure both
        // exist before GStreamer fails with a much less helpful message. A
        // BRAIDPIPE_AUDIO_BRANCH override may tap anything it likes.
        if std::env::var("BRAIDPIPE_AUDIO_BRANCH").is_err() {
            if args.uri.is_none()
                && !args
                    .source
                    .as_deref()
                    .is_some_and(|s| s.contains("name=decoder"))
            {
                return Err("--audio needs a source exposing audio: use --uri, or name \
                            your demuxer/decodebin 'decoder' in --source"
                    .into());
            }
            if args.output.is_none() && !sink.contains("name=mux") {
                return Err("--audio needs a muxer to join: use --output, or name \
                            your muxer 'mux' in --sink"
                    .into());
            }
        }
        info!(branch = %branch, "Audio passthrough enabled");
        Some(branch)
    } else {
        None
    };

    // 2. Initialize the Media Engine Adapter (GStreamer)
    info!("Constructing GStreamer dual-branch pipeline...");
    let engine = Arc::new(match args.uri.as_deref() {
        Some(uri) => {
            info!(%uri, "Creating pipeline from input URI");
            GStreamerEngine::new_from_uri(uri, &sink, audio_branch.as_deref())?
        }
        None => GStreamerEngine::new_with_branch(
            args.source.as_deref().unwrap_or(DEFAULT_SOURCE),
            &sink,
            audio_branch.as_deref(),
        )?,
    });

    if args.passthrough_only {
        info!("Starting in passthrough-only mode");
        let collector_engine = Arc::clone(&engine);
        metrics_server::spawn(
            args.metrics_port,
            vec![Box::new(move |out| {
                collector_engine.collect_runtime_metrics(out)
            })],
        );
        engine.start().await?;
        signal::ctrl_c().await?;
        engine.stop().await?;
        return Ok(());
    }

    // 3. Initialize Shared Memory Ring Buffer (/dev/shm)
    info!(
        name = %args.shm_name,
        resolution = %format!("{}x{}", args.width, args.height),
        "Allocating POSIX Shared Memory ring buffer..."
    );
    let shm_buffer = Arc::new(ShmRingBuffer::create(
        &args.shm_name,
        args.width,
        args.height,
        3, // RGB 3-channel
        4, // 4-slot ring buffer
    )?);

    // 4. Bind the Unix Domain Socket Signaling Adapter
    info!("Binding UDS control channels...");
    let ai_bridge = Arc::new(UdsControlBridge::bind(&args.rust_sock, &args.python_sock).await?);

    // 5. Attach the frame relay: appsink -> SHM -> Python -> appsrc
    relay::spawn(
        engine.get_ai_sink()?,
        engine.get_ai_src()?,
        Arc::clone(&shm_buffer),
        Arc::clone(&ai_bridge),
        args.width,
        args.height,
        args.fps,
    );

    // 6. Spawn the Python Worker Subprocess (Supervisor Pattern)
    // Prefer the project virtualenv so cv2/numpy are importable.
    let python_exe = if Path::new(".venv/bin/python3").exists() {
        ".venv/bin/python3"
    } else {
        "python3"
    };
    info!(python = python_exe, path = ?args.python_script, "Spawning Python worker subprocess...");
    let mut python_child = Command::new(python_exe)
        .arg(&args.python_script)
        .spawn()
        .map_err(|e| format!("Failed to execute Python worker: {e}"))?;

    let child_pid = python_child.id().unwrap_or(0);
    info!(pid = child_pid, "Python worker active");
    braidpipe_core::metrics::WORKER_UP.set(1);
    metrics_server::spawn_worker_sampler(child_pid);

    // 7. Expose the Prometheus endpoint, with collectors for the live state
    // the static registry cannot see: pipeline queues / SRT stats, and the
    // SHM ring occupancy.
    let collector_engine = Arc::clone(&engine);
    let collector_shm = Arc::clone(&shm_buffer);
    metrics_server::spawn(
        args.metrics_port,
        vec![
            Box::new(move |out| collector_engine.collect_runtime_metrics(out)),
            Box::new(move |out| {
                use std::fmt::Write;
                let (occupied, total) = collector_shm.occupied_slots();
                let _ = writeln!(
                    out,
                    "# HELP braidpipe_shm_slots_occupied Ring slots currently holding a frame\n\
                     # TYPE braidpipe_shm_slots_occupied gauge\n\
                     braidpipe_shm_slots_occupied {occupied}\n\
                     # TYPE braidpipe_shm_slots gauge\n\
                     braidpipe_shm_slots {total}"
                );
            }),
        ],
    );

    // 8. Instantiate the Watchdog FSM with injected Adapters
    let mut watchdog = Watchdog::new(engine, ai_bridge, args.fps);

    // 9. Spawn a background task to supervise the Python process handle
    tokio::spawn(async move {
        match python_child.wait().await {
            Ok(status) => {
                warn!(status = %status, "Python worker process exited");
                braidpipe_core::metrics::WORKER_UP.set(0);
                braidpipe_core::metrics::WORKER_EXITS.inc();
                braidpipe_core::metrics::WORKER_LAST_EXIT_CODE
                    .set(i64::from(status.code().unwrap_or(-1)));
            }
            Err(e) => error!(error = %e, "Error waiting on Python process handle"),
        }
    });

    // 10. Hand execution to the Watchdog event loop (Blocks until SIGINT/SIGTERM)
    info!("System initialized. Handing off execution to Watchdog FSM...");
    watchdog.run_loop().await;

    Ok(())
}
