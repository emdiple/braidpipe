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

const DEFAULT_SOURCE: &str =
    "videotestsrc is-live=true pattern=ball ! video/x-raw,width=1280,height=720,framerate=30/1 ! videoconvert";

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

    // 2. Initialize the Media Engine Adapter (GStreamer)
    info!("Constructing GStreamer dual-branch pipeline...");
    let engine = Arc::new(match args.uri.as_deref() {
        Some(uri) => {
            info!(%uri, "Creating pipeline from input URI");
            GStreamerEngine::new_from_uri(uri, &args.sink)?
        }
        None => GStreamerEngine::new(args.source.as_deref().unwrap_or(DEFAULT_SOURCE), &args.sink)?,
    });

    if args.passthrough_only {
        info!("Starting in passthrough-only mode");
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

    // 7. Instantiate the Watchdog FSM with injected Adapters
    let mut watchdog = Watchdog::new(engine, ai_bridge, args.fps);

    // 8. Spawn a background task to supervise the Python process handle
    tokio::spawn(async move {
        match python_child.wait().await {
            Ok(status) => warn!(status = %status, "Python worker process exited"),
            Err(e) => error!(error = %e, "Error waiting on Python process handle"),
        }
    });

    // 9. Hand execution to the Watchdog event loop (Blocks until SIGINT/SIGTERM)
    info!("System initialized. Handing off execution to Watchdog FSM...");
    watchdog.run_loop().await;

    Ok(())
}
