//! The /metrics endpoint: a deliberately tiny HTTP server.
//!
//! Prometheus needs exactly one thing -- GET returning text -- so this is a
//! raw TcpListener rather than a web framework. Every scrape renders the
//! static registry from braidpipe-core, then appends collector closures for
//! the values that only exist as live state (queue depths, SHM occupancy,
//! SRT transport stats).

use braidpipe_core::metrics;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

pub type Collector = Box<dyn Fn(&mut String) + Send + Sync>;

static START_TIME: OnceLock<f64> = OnceLock::new();

/// Binds 127.0.0.1:port and serves scrapes until the process exits.
/// `port` 0 disables the endpoint entirely.
pub fn spawn(port: u16, collectors: Vec<Collector>) {
    if port == 0 {
        return;
    }
    START_TIME.get_or_init(metrics::unix_now);

    tokio::spawn(async move {
        let listener = match TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => {
                info!(port, "Metrics endpoint listening");
                listener
            }
            Err(error) => {
                warn!(%error, port, "Could not bind metrics endpoint; metrics disabled");
                return;
            }
        };

        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };

            // Drain the request line; the path does not matter, every request
            // gets the metrics page.
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;

            let mut body = String::with_capacity(8192);
            render_build_info(&mut body);
            render_process(&mut body);
            metrics::render(&mut body);
            for collector in &collectors {
                collector(&mut body);
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
    });
}

fn render_build_info(out: &mut String) {
    use std::fmt::Write;
    let _ = writeln!(
        out,
        "# HELP braidpipe_build_info Build metadata as labels\n\
         # TYPE braidpipe_build_info gauge\n\
         braidpipe_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );
}

/// Standard process metrics for the daemon itself.
fn render_process(out: &mut String) {
    use std::fmt::Write;

    let _ = writeln!(
        out,
        "# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds {}",
        START_TIME.get().copied().unwrap_or(0.0)
    );

    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0 {
        let cpu = usage.ru_utime.tv_sec as f64
            + usage.ru_utime.tv_usec as f64 / 1e6
            + usage.ru_stime.tv_sec as f64
            + usage.ru_stime.tv_usec as f64 / 1e6;
        let _ = writeln!(
            out,
            "# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total {cpu}"
        );
        let _ = writeln!(
            out,
            "# TYPE process_resident_memory_bytes gauge\nprocess_resident_memory_bytes {}",
            resident_bytes(&usage)
        );
    }

    if let Ok(fds) = std::fs::read_dir("/dev/fd") {
        let _ = writeln!(
            out,
            "# TYPE process_open_fds gauge\nprocess_open_fds {}",
            fds.count()
        );
    }
}

#[cfg(target_os = "linux")]
fn resident_bytes(_usage: &libc::rusage) -> u64 {
    // statm field 2 is current resident pages.
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn resident_bytes(usage: &libc::rusage) -> u64 {
    // macOS reports ru_maxrss in bytes; peak rather than current, which is
    // the closest portable answer without mach-specific calls.
    usage.ru_maxrss as u64
}

/// Samples the worker child's CPU time and RSS via `ps` every few seconds.
/// Portable across macOS and Linux, and far simpler than per-platform
/// process-info syscalls for a 0.2 Hz poll.
pub fn spawn_worker_sampler(pid: u32) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;
            if metrics::WORKER_UP.get() == 0 {
                return;
            }
            let output = tokio::process::Command::new("ps")
                .args(["-o", "cputime=,rss=", "-p", &pid.to_string()])
                .output()
                .await;
            let Ok(output) = output else { continue };
            let text = String::from_utf8_lossy(&output.stdout);
            let mut fields = text.split_whitespace();
            if let (Some(cputime), Some(rss)) = (fields.next(), fields.next()) {
                if let Some(seconds) = parse_cputime(cputime) {
                    metrics::WORKER_CPU_SECONDS.set(seconds);
                }
                if let Ok(rss_kb) = rss.parse::<i64>() {
                    metrics::WORKER_RSS_BYTES.set(rss_kb * 1024);
                }
            }
        }
    });
}

/// External worker mode: `worker_up` cannot mean "my child is alive" for a
/// process the daemon does not own, so it means "delivered a successful AI
/// frame within the last 2 seconds" instead, derived from the timestamp the
/// relay already records on every good roundtrip.
pub fn spawn_external_worker_probe() {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            ticker.tick().await;
            let last = metrics::LAST_AI_FRAME_TIMESTAMP.get();
            let up = last > 0.0 && metrics::unix_now() - last < 2.0;
            metrics::WORKER_UP.set(i64::from(up));
        }
    });
}

/// Marks the daemon down and keeps serving long enough for Prometheus to
/// scrape that final state.
///
/// Without this the process disappears between scrapes and the last sample
/// Prometheus ever saw is a healthy one, so every dashboard panel freezes on
/// "worker up, AI available" forever. `drain_ms` should span at least one
/// scrape interval; a scrape lands mid-drain and records the truth.
pub async fn drain(port: u16, drain_ms: u64) {
    metrics::UP.set(0);
    metrics::WORKER_HEALTHY.set(0);
    if port == 0 || drain_ms == 0 {
        return;
    }
    info!(drain_ms, "Holding the metrics endpoint open so the final state is scraped");
    tokio::time::sleep(Duration::from_millis(drain_ms)).await;
}

/// Force-exits if a shutdown has not finished `deadline_ms` after the signal.
///
/// Tearing down a GStreamer pipeline can block indefinitely -- macOS video
/// sinks deadlock on `set_state(NULL)` while their GL thread holds the element
/// lock. A daemon wedged in that state is the worst outcome: still alive,
/// still answering scrapes, still reporting the last healthy sample. This
/// guarantees the process actually goes away.
pub fn spawn_shutdown_guard(deadline_ms: u64) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(deadline_ms)).await;
        warn!(deadline_ms, "Shutdown did not complete in time; forcing exit");
        std::process::exit(0);
    });
}

/// Stops the worker and waits for the supervisor task to observe its exit.
///
/// SIGTERM first so the worker can release its SHM mapping and socket; SIGKILL
/// only if it is still alive after `grace_ms`.
pub async fn terminate_worker(pid: u32, grace_ms: u64) {
    if pid == 0 || metrics::WORKER_UP.get() == 0 {
        return;
    }
    info!(pid, "Stopping the Python worker");
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };

    let deadline = tokio::time::Instant::now() + Duration::from_millis(grace_ms);
    while tokio::time::Instant::now() < deadline {
        if metrics::WORKER_UP.get() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    warn!(pid, "Worker did not exit on SIGTERM; sending SIGKILL");
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    tokio::time::sleep(Duration::from_millis(100)).await;
    metrics::WORKER_UP.set(0);
}

/// Parses ps cputime: `[[dd-]hh:]mm:ss[.cc]`.
fn parse_cputime(text: &str) -> Option<f64> {
    let (days, rest) = match text.split_once('-') {
        Some((d, rest)) => (d.parse::<f64>().ok()?, rest),
        None => (0.0, text),
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let mut seconds = 0.0;
    for part in &parts {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(days * 86_400.0 + seconds)
}

#[cfg(test)]
mod tests {
    use super::parse_cputime;

    #[test]
    fn cputime_formats() {
        assert_eq!(parse_cputime("0:03.45"), Some(3.45));
        assert_eq!(parse_cputime("2:15:04"), Some(8104.0));
        assert_eq!(parse_cputime("1-00:00:30"), Some(86_430.0));
        assert_eq!(parse_cputime("garbage"), None);
    }
}
