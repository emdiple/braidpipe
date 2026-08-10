//! Process-wide metrics, exposed in the Prometheus text format.
//!
//! Hand-rolled on atomics rather than a metrics crate: the daemon needs a
//! couple of dozen counters, gauges and fixed-bucket histograms, all updated
//! from hot paths (the relay loop, GStreamer streaming threads), and a lockless
//! `fetch_add` is the cheapest possible way to do that. Rendering walks the
//! statics and formats text; it runs once per scrape, never on the data path.
//!
//! Values that only exist at scrape time (queue depths, SRT stats, process
//! usage) are not here -- the HTTP server appends those through collector
//! closures so this module stays dependency-free.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Counter(AtomicU64);

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Integer gauge (worker pid up/down, failure streak, negotiated width...).
pub struct Gauge(AtomicI64);

impl Gauge {
    pub const fn new(initial: i64) -> Self {
        Self(AtomicI64::new(initial))
    }
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Float gauge stored as f64 bits (timestamps, seconds, skew).
pub struct GaugeF(AtomicU64);

impl Default for GaugeF {
    fn default() -> Self {
        Self::new()
    }
}

impl GaugeF {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
    pub fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

/// Counter of elapsed time, accumulated in microseconds, rendered in seconds.
pub struct SecondsCounter(AtomicU64);

impl Default for SecondsCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl SecondsCounter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
    pub fn add_seconds(&self, seconds: f64) {
        self.0
            .fetch_add((seconds * 1e6).max(0.0) as u64, Ordering::Relaxed);
    }
    pub fn seconds(&self) -> f64 {
        self.0.load(Ordering::Relaxed) as f64 / 1e6
    }
}

/// Latency buckets, in seconds. Chosen around the numbers this system actually
/// produces: sub-millisecond IPC, a 33-50ms frame budget, and pathologies
/// beyond it.
pub const LATENCY_BUCKETS: [f64; 13] = [
    0.0005, 0.001, 0.002, 0.005, 0.010, 0.015, 0.020, 0.030, 0.040, 0.050, 0.075, 0.150, 0.500,
];

pub struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS.len()],
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; LATENCY_BUCKETS.len()],
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe_seconds(&self, seconds: f64) {
        for (i, bound) in LATENCY_BUCKETS.iter().enumerate() {
            if seconds <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum_us
            .fetch_add((seconds * 1e6).max(0.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str) {
        use std::fmt::Write;
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (i, bound) in LATENCY_BUCKETS.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}_bucket{{le=\"{bound}\"}} {}",
                self.buckets[i].load(Ordering::Relaxed)
            );
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(
            out,
            "{name}_sum {}",
            self.sum_us.load(Ordering::Relaxed) as f64 / 1e6
        );
        let _ = writeln!(out, "{name}_count {count}");
    }
}

// --- The registry. Grouped the way the dashboard groups them. ---

// Frame outcomes and latency (updated by the relay)
pub static FRAMES_AI: Counter = Counter::new();
pub static FRAMES_PASSTHROUGH: Counter = Counter::new();
pub static ROUNDTRIP_SECONDS: Histogram = Histogram::new();
pub static WORKER_PROCESSING_SECONDS: Histogram = Histogram::new();
pub static RELAY_DEADLINE_SECONDS: GaugeF = GaugeF::new();
pub static LAST_AI_FRAME_TIMESTAMP: GaugeF = GaugeF::new();
pub static STALE_ACKS: Counter = Counter::new();
pub static RELAY_CHANNEL_DROPS: Counter = Counter::new();
pub static SHM_FULL: Counter = Counter::new();

// Failover (updated by the UDS bridge and the watchdog)
pub static FAILURE_STREAK: Gauge = Gauge::new(0);
pub static WORKER_HEALTHY: Gauge = Gauge::new(0);
pub static ACTIVE_BRANCH: Gauge = Gauge::new(0);
pub static BRANCH_SWITCHES_TO_AI: Counter = Counter::new();
pub static BRANCH_SWITCHES_TO_PASSTHROUGH: Counter = Counter::new();
pub static BRANCH_SECONDS_AI: SecondsCounter = SecondsCounter::new();
pub static BRANCH_SECONDS_PASSTHROUGH: SecondsCounter = SecondsCounter::new();

// Worker supervision (updated by the daemon's supervisor task)
pub static WORKER_UP: Gauge = Gauge::new(0);
pub static WORKER_EXITS: Counter = Counter::new();
pub static WORKER_LAST_EXIT_CODE: Gauge = Gauge::new(0);
pub static WORKER_CPU_SECONDS: GaugeF = GaugeF::new();
pub static WORKER_RSS_BYTES: Gauge = Gauge::new(0);

// Media path (updated by GStreamer pad probes in the engine)
pub static INPUT_FRAMES: Counter = Counter::new();
pub static INPUT_INTERVAL_SECONDS: Histogram = Histogram::new();
pub static PTS_DISCONTINUITIES: Counter = Counter::new();
pub static INPUT_WIDTH: Gauge = Gauge::new(0);
pub static INPUT_HEIGHT: Gauge = Gauge::new(0);
pub static INPUT_FPS: GaugeF = GaugeF::new();
pub static OUTPUT_FRAMES: Counter = Counter::new();
pub static SINK_BYTES: Counter = Counter::new();
pub static SINK_BUFFERS: Counter = Counter::new();
pub static KEYFRAMES: Counter = Counter::new();
pub static QUEUE_OVERRUNS_PASS: Counter = Counter::new();
pub static QUEUE_OVERRUNS_AI: Counter = Counter::new();
pub static BUS_ERRORS: Counter = Counter::new();
pub static BUS_WARNINGS: Counter = Counter::new();

/// Last observed PTS per stream at the muxer, in microseconds; -1 = never.
/// Their difference is the A/V skew, rendered only once both streams exist.
pub static LAST_VIDEO_PTS_US: AtomicI64 = AtomicI64::new(-1);
pub static LAST_AUDIO_PTS_US: AtomicI64 = AtomicI64::new(-1);

pub fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Renders every static metric. The caller appends scrape-time collectors.
pub fn render(out: &mut String) {
    use std::fmt::Write;

    macro_rules! counter {
        ($name:literal, $help:literal, $($label:literal => $c:expr),+) => {
            let _ = writeln!(out, "# HELP {} {}", $name, $help);
            let _ = writeln!(out, "# TYPE {} counter", $name);
            $( let _ = writeln!(out, "{}{} {}", $name, $label, $c.get()); )+
        };
    }
    macro_rules! gauge {
        ($name:literal, $help:literal, $value:expr) => {
            let _ = writeln!(out, "# HELP {} {}", $name, $help);
            let _ = writeln!(out, "# TYPE {} gauge", $name);
            let _ = writeln!(out, "{} {}", $name, $value);
        };
    }

    counter!("braidpipe_frames_total", "Frames pushed downstream by the relay, by outcome",
        "{outcome=\"ai\"}" => FRAMES_AI, "{outcome=\"passthrough\"}" => FRAMES_PASSTHROUGH);
    ROUNDTRIP_SECONDS.render(out, "braidpipe_roundtrip_seconds");
    WORKER_PROCESSING_SECONDS.render(out, "braidpipe_worker_processing_seconds");
    gauge!("braidpipe_relay_deadline_seconds", "Per-frame budget for the AI roundtrip", RELAY_DEADLINE_SECONDS.get());
    gauge!("braidpipe_last_ai_frame_timestamp_seconds", "Unix time of the last successful AI frame", LAST_AI_FRAME_TIMESTAMP.get());
    counter!("braidpipe_stale_acks_total", "Acks for frames that had already timed out", "" => STALE_ACKS);
    counter!("braidpipe_relay_channel_drops_total", "Frames dropped because the relay was still busy", "" => RELAY_CHANNEL_DROPS);
    counter!("braidpipe_shm_full_total", "Frames dropped because no SHM slot was free", "" => SHM_FULL);

    gauge!("braidpipe_failure_streak", "Consecutive failed roundtrips", FAILURE_STREAK.get());
    gauge!("braidpipe_worker_healthy", "1 while the worker is considered healthy", WORKER_HEALTHY.get());
    gauge!("braidpipe_active_branch", "1 while the AI branch is selected", ACTIVE_BRANCH.get());
    counter!("braidpipe_branch_switches_total", "Selector switches, by direction",
        "{direction=\"to_ai\"}" => BRANCH_SWITCHES_TO_AI,
        "{direction=\"to_passthrough\"}" => BRANCH_SWITCHES_TO_PASSTHROUGH);
    let _ = writeln!(out, "# HELP braidpipe_branch_seconds_total Stream time spent on each branch");
    let _ = writeln!(out, "# TYPE braidpipe_branch_seconds_total counter");
    let _ = writeln!(out, "braidpipe_branch_seconds_total{{branch=\"ai\"}} {}", BRANCH_SECONDS_AI.seconds());
    let _ = writeln!(out, "braidpipe_branch_seconds_total{{branch=\"passthrough\"}} {}", BRANCH_SECONDS_PASSTHROUGH.seconds());

    gauge!("braidpipe_worker_up", "1 while the Python worker process is running", WORKER_UP.get());
    counter!("braidpipe_worker_exits_total", "Times the worker process has exited", "" => WORKER_EXITS);
    gauge!("braidpipe_worker_last_exit_code", "Exit code of the most recent worker exit", WORKER_LAST_EXIT_CODE.get());
    gauge!("braidpipe_worker_cpu_seconds_total", "Cumulative CPU time of the worker process", WORKER_CPU_SECONDS.get());
    gauge!("braidpipe_worker_resident_memory_bytes", "Resident memory of the worker process", WORKER_RSS_BYTES.get());

    counter!("braidpipe_input_frames_total", "Frames arriving from the source", "" => INPUT_FRAMES);
    INPUT_INTERVAL_SECONDS.render(out, "braidpipe_input_interval_seconds");
    counter!("braidpipe_pts_discontinuities_total", "Input timestamps that went backwards or gapped", "" => PTS_DISCONTINUITIES);
    gauge!("braidpipe_input_width", "Negotiated input width", INPUT_WIDTH.get());
    gauge!("braidpipe_input_height", "Negotiated input height", INPUT_HEIGHT.get());
    gauge!("braidpipe_input_fps", "Negotiated input frame rate", INPUT_FPS.get());
    counter!("braidpipe_output_frames_total", "Frames leaving the selector toward the sink", "" => OUTPUT_FRAMES);
    counter!("braidpipe_sink_bytes_total", "Bytes handed to the final sink", "" => SINK_BYTES);
    counter!("braidpipe_sink_buffers_total", "Buffers handed to the final sink", "" => SINK_BUFFERS);
    counter!("braidpipe_keyframes_total", "Keyframes leaving the video encoder", "" => KEYFRAMES);
    counter!("braidpipe_queue_drop_events_total", "Queue overrun events, per branch queue",
        "{queue=\"q_pass\"}" => QUEUE_OVERRUNS_PASS, "{queue=\"q_ai\"}" => QUEUE_OVERRUNS_AI);
    counter!("braidpipe_bus_messages_total", "GStreamer bus messages, by severity",
        "{severity=\"error\"}" => BUS_ERRORS, "{severity=\"warning\"}" => BUS_WARNINGS);

    let (v, a) = (
        LAST_VIDEO_PTS_US.load(Ordering::Relaxed),
        LAST_AUDIO_PTS_US.load(Ordering::Relaxed),
    );
    if v >= 0 && a >= 0 {
        gauge!("braidpipe_av_skew_seconds", "Video minus audio PTS at the muxer", (v - a) as f64 / 1e6);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let h = Histogram::new();
        h.observe_seconds(0.0007); // > 0.0005, <= 0.001
        h.observe_seconds(0.012);
        h.observe_seconds(9.0); // beyond every bound: +Inf only
        let mut out = String::new();
        h.render(&mut out, "test_seconds");
        assert!(out.contains("test_seconds_bucket{le=\"0.0005\"} 0"));
        assert!(out.contains("test_seconds_bucket{le=\"0.001\"} 1"));
        assert!(out.contains("test_seconds_bucket{le=\"0.015\"} 2"));
        assert!(out.contains("test_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(out.contains("test_seconds_count 3"));
    }

    #[test]
    fn render_produces_labeled_families_and_conditional_skew() {
        FRAMES_AI.inc();
        let mut out = String::new();
        render(&mut out);
        assert!(out.contains("braidpipe_frames_total{outcome=\"ai\"}"));
        assert!(out.contains("# TYPE braidpipe_roundtrip_seconds histogram"));
        // both PTS sides unset: skew must be absent, not zero
        assert!(!out.contains("braidpipe_av_skew_seconds"));

        LAST_VIDEO_PTS_US.store(2_000_000, Ordering::Relaxed);
        LAST_AUDIO_PTS_US.store(1_900_000, Ordering::Relaxed);
        let mut out = String::new();
        render(&mut out);
        assert!(out.contains("braidpipe_av_skew_seconds 0.1"));
    }

    #[test]
    fn seconds_counter_accumulates_fractions() {
        let c = SecondsCounter::new();
        for _ in 0..30 {
            c.add_seconds(1.0 / 30.0);
        }
        assert!((c.seconds() - 1.0).abs() < 0.001);
    }
}
