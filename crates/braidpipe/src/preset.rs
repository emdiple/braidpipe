//! Named latency/bandwidth profiles that expand into a full encoder + sink
//! description, the way ffmpeg's `-preset` expands into a bag of x264 options.
//!
//! A preset only decides defaults. Every parameter can be overridden through an
//! environment variable, so a deployment can start from `lowlatency` and turn
//! one knob without writing the whole GStreamer sink by hand:
//!
//!   BRAIDPIPE_ENCODER       auto | x264 | vtenc | nvenc | va | vaapi | qsv | mf | amf
//!   BRAIDPIPE_HW            off disables GPU decode promotion and encoder auto-pick
//!   BRAIDPIPE_BITRATE_KBPS  encoder target bitrate
//!   BRAIDPIPE_SPEED_PRESET  x264 speed preset (ultrafast..placebo)
//!   BRAIDPIPE_ZEROLATENCY   1/0, per encoder: x264 tune=zerolatency, vtenc
//!                           realtime, nvenc low-latency preset, qsv/mf
//!                           low-latency, amf ultra-low-latency usage
//!   BRAIDPIPE_GOP_SECONDS   keyframe interval in seconds
//!   BRAIDPIPE_VBV_BUF_MS    x264 VBV buffer, bounds bitrate bursts
//!   BRAIDPIPE_SINK_SYNC     1/0, clock-sync the network sink
//!   BRAIDPIPE_SRT_LATENCY_MS  srtsink receive-side latency budget
//!   BRAIDPIPE_SRT_WAIT_FOR_CONNECTION  1/0, srtsink blocks until a caller
//!                           connects (default 0: run and drop output until
//!                           a viewer arrives, so the input is consumed
//!                           from the moment the pipeline starts)
//!   BRAIDPIPE_AUDIO_ENCODER      AAC encoder element (default avenc_aac)
//!   BRAIDPIPE_AUDIO_BITRATE_KBPS audio target bitrate (default 128)
//!   BRAIDPIPE_AUDIO_BRANCH       replace the generated audio branch outright
//!
//! `--sink` still accepts a raw pipeline string and bypasses all of this.

use std::fmt;

/// Everything a profile decides. Latency falls and bandwidth rises from the
/// bottom of this list to the top of the preset table below.
#[derive(Debug, Clone, PartialEq)]
pub struct Params {
    pub encoder: Encoder,
    pub speed_preset: &'static str,
    /// tune=zerolatency: no B-frames, no lookahead. Costs compression
    /// efficiency, saves several frame times of encoder delay.
    pub zerolatency: bool,
    pub bitrate_kbps: u32,
    pub gop_seconds: f64,
    /// x264 VBV buffer in milliseconds. This is what makes the bitrate number
    /// mean something on the wire: it bounds how far above the target the
    /// encoder may burst, and how much encoded data a decoder must be ready
    /// to buffer -- so it is both a bandwidth cap and hidden latency. x264's
    /// own default (600 ms) allows bursts over half a second long.
    pub vbv_buf_ms: u32,
    /// sync=false on the sink. Measured worth ~48ms: a syncing network sink
    /// holds each buffer until running-time + configured latency, and the live
    /// source already paces the pipeline.
    pub sync: bool,
    pub srt_latency_ms: u32,
    /// wait-for-connection on srtsink. GStreamer's default (true) holds the
    /// whole pipeline in preroll until the first viewer connects -- so the
    /// input is not even consumed. False keeps the stream running and drops
    /// output packets until a caller arrives, which fits a live relay.
    pub srt_wait_for_connection: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoder {
    /// Software fallback, available everywhere.
    X264,
    /// Apple VideoToolbox (macOS).
    Vtenc,
    /// NVIDIA NVENC (Linux, Windows).
    Nvenc,
    /// VA-API through the modern `va` plugin (Linux).
    Va,
    /// VA-API through the legacy `vaapi` plugin (Linux).
    Vaapi,
    /// Intel QuickSync (Linux, Windows).
    Qsv,
    /// Windows Media Foundation.
    Mf,
    /// AMD AMF (Windows).
    Amf,
}

impl Encoder {
    const NAMES: [(&'static str, Encoder); 8] = [
        ("x264", Encoder::X264),
        ("vtenc", Encoder::Vtenc),
        ("nvenc", Encoder::Nvenc),
        ("va", Encoder::Va),
        ("vaapi", Encoder::Vaapi),
        ("qsv", Encoder::Qsv),
        ("mf", Encoder::Mf),
        ("amf", Encoder::Amf),
    ];

    fn from_name(name: &str) -> Option<Encoder> {
        Self::NAMES
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, e)| *e)
    }

    /// Maps a GStreamer factory name reported by hardware detection.
    fn from_factory(factory: &str) -> Option<Encoder> {
        Some(match factory {
            "vtenc_h264" => Encoder::Vtenc,
            "nvh264enc" => Encoder::Nvenc,
            "vah264enc" => Encoder::Va,
            "vaapih264enc" => Encoder::Vaapi,
            "qsvh264enc" => Encoder::Qsv,
            "mfh264enc" => Encoder::Mf,
            "amfh264enc" => Encoder::Amf,
            _ => return None,
        })
    }
}

impl fmt::Display for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, _) = Self::NAMES.iter().find(|(_, e)| e == self).unwrap();
        f.write_str(name)
    }
}

pub const PRESET_NAMES: [&str; 4] = ["zerolatency", "lowlatency", "balanced", "bandwidth"];

/// Accepted values for the `--encoder` CLI flag: "auto" plus every pinnable
/// encoder name. Kept in sync with [`Encoder::NAMES`] by a test.
pub const ENCODER_VALUES: [&str; 9] = [
    "auto", "x264", "vtenc", "nvenc", "va", "vaapi", "qsv", "mf", "amf",
];

/// Profile defaults. Bitrates assume 720p30; override for other formats.
fn defaults(preset: &str) -> Option<Params> {
    let p = match preset {
        // Every latency lever pulled, bandwidth pays for it.
        "zerolatency" => Params {
            encoder: Encoder::X264,
            speed_preset: "ultrafast",
            zerolatency: true,
            bitrate_kbps: 6000,
            gop_seconds: 1.0,
            sync: false,
            vbv_buf_ms: 100,
            srt_latency_ms: 50,
            srt_wait_for_connection: false,
        },
        // The measured sweet spot: keeps the ~40ms p50 of the tuned harness
        // sink while veryfast claws back most of ultrafast's wasted bits.
        "lowlatency" => Params {
            encoder: Encoder::X264,
            speed_preset: "veryfast",
            zerolatency: true,
            bitrate_kbps: 4500,
            gop_seconds: 2.0,
            sync: false,
            vbv_buf_ms: 200,
            srt_latency_ms: 125,
            srt_wait_for_connection: false,
        },
        "balanced" => Params {
            encoder: Encoder::X264,
            speed_preset: "medium",
            zerolatency: true,
            bitrate_kbps: 3000,
            gop_seconds: 2.0,
            sync: false,
            vbv_buf_ms: 500,
            srt_latency_ms: 250,
            srt_wait_for_connection: false,
        },
        // Minimum bits for the quality: B-frames and lookahead come back,
        // which adds several frames of encoder delay by design.
        "bandwidth" => Params {
            encoder: Encoder::X264,
            speed_preset: "slow",
            zerolatency: false,
            bitrate_kbps: 1800,
            gop_seconds: 4.0,
            sync: true,
            vbv_buf_ms: 1000,
            srt_latency_ms: 500,
            srt_wait_for_connection: false,
        },
        _ => return None,
    };
    Some(p)
}

/// Builds the sink description for a preset and an output URL
/// (rtmp://, srt:// or udp://host:port), env overrides applied.
///
/// `encoder` is the `--encoder` CLI flag. Precedence: a non-auto flag pins
/// the codec outright; an explicit `--encoder auto` forces detection even
/// over BRAIDPIPE_ENCODER; no flag defers to the environment; and with
/// nothing pinned anywhere, the best hardware encoder this machine actually
/// has replaces the preset's x264 default (None from detection means no GPU
/// encoder or GPU mode off, and x264 stands).
pub fn build_sink(
    preset: &str,
    output: &str,
    fps: u32,
    encoder: Option<&str>,
) -> Result<String, String> {
    let mut params = resolve(preset, |key| std::env::var(key).ok())?;

    let pinned = match encoder {
        Some("auto") => false,
        Some(name) => {
            params.encoder = Encoder::from_name(name).ok_or_else(|| {
                format!(
                    "--encoder must be one of {}; got '{name}'",
                    ENCODER_VALUES.join(", ")
                )
            })?;
            true
        }
        None => std::env::var("BRAIDPIPE_ENCODER").is_ok_and(|v| v != "auto"),
    };
    if !pinned
        && let Some(detected) =
            braidpipe_engine::hwaccel::preferred_h264_encoder().and_then(Encoder::from_factory)
    {
        params.encoder = detected;
    }

    render(&params, output, fps)
}

/// Applies environment overrides on top of a preset's defaults. The lookup is
/// injected so tests do not have to mutate process-global state.
fn resolve(
    preset: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Params, String> {
    let mut p = defaults(preset).ok_or_else(|| {
        format!(
            "unknown preset '{preset}' (expected one of: {})",
            PRESET_NAMES.join(", ")
        )
    })?;

    if let Some(v) = env("BRAIDPIPE_ENCODER") {
        // "auto" keeps the default; build_sink() then swaps in the detected
        // hardware encoder, exactly as if the variable were unset.
        if v != "auto" {
            p.encoder = Encoder::from_name(&v).ok_or_else(|| {
                format!(
                    "BRAIDPIPE_ENCODER must be one of auto, {}; got '{v}'",
                    Encoder::NAMES.map(|(n, _)| n).join(", ")
                )
            })?;
        }
    }
    if let Some(v) = env("BRAIDPIPE_SPEED_PRESET") {
        // x264enc rejects bad values itself; leaking the string through keeps
        // this file from shadowing the encoder's own vocabulary.
        p.speed_preset = Box::leak(v.into_boxed_str());
    }
    if let Some(v) = env("BRAIDPIPE_ZEROLATENCY") {
        p.zerolatency = parse_bool("BRAIDPIPE_ZEROLATENCY", &v)?;
    }
    if let Some(v) = env("BRAIDPIPE_BITRATE_KBPS") {
        p.bitrate_kbps = parse_num("BRAIDPIPE_BITRATE_KBPS", &v)?;
    }
    if let Some(v) = env("BRAIDPIPE_GOP_SECONDS") {
        p.gop_seconds = parse_num("BRAIDPIPE_GOP_SECONDS", &v)?;
    }
    if let Some(v) = env("BRAIDPIPE_VBV_BUF_MS") {
        p.vbv_buf_ms = parse_num("BRAIDPIPE_VBV_BUF_MS", &v)?;
    }
    if let Some(v) = env("BRAIDPIPE_SINK_SYNC") {
        p.sync = parse_bool("BRAIDPIPE_SINK_SYNC", &v)?;
    }
    if let Some(v) = env("BRAIDPIPE_SRT_LATENCY_MS") {
        p.srt_latency_ms = parse_num("BRAIDPIPE_SRT_LATENCY_MS", &v)?;
    }
    if let Some(v) = env("BRAIDPIPE_SRT_WAIT_FOR_CONNECTION") {
        p.srt_wait_for_connection = parse_bool("BRAIDPIPE_SRT_WAIT_FOR_CONNECTION", &v)?;
    }
    Ok(p)
}

fn render(p: &Params, output: &str, fps: u32) -> Result<String, String> {
    let keyint = ((p.gop_seconds * f64::from(fps.max(1))).round() as u32).max(1);

    let encoder = match p.encoder {
        Encoder::X264 => {
            let tune = if p.zerolatency { " tune=zerolatency" } else { "" };
            format!(
                "x264enc speed-preset={}{tune} bitrate={} key-int-max={keyint} \
                 vbv-buf-capacity={}",
                p.speed_preset, p.bitrate_kbps, p.vbv_buf_ms
            )
        }
        // CBR is what makes the bitrate number mean something on VideoToolbox:
        // its default ABR undershoots easy content (encoding at an internal
        // ~0.5 quality point with the budget left unspent) and bursts far past
        // the target on hard scenes with no VBV to bound it. Constant bitrate
        // does both of the jobs x264's VBV does. The quality knob only applies
        // if rate control is ever ABR again; under CBR VideoToolbox ignores it.
        Encoder::Vtenc => format!(
            "vtenc_h264 realtime={} allow-frame-reordering={} bitrate={} \
             max-keyframe-interval={keyint} rate-control=cbr quality=0.65",
            p.zerolatency, !p.zerolatency, p.bitrate_kbps
        ),
        // The hardware encoders below all take kbps like x264, but each has
        // its own name for the GOP and its own shape of low-latency switch.
        // The VBV bound carries over where the encoder exposes one (NVENC's
        // vbv-buffer-size, VA's cpb-size, both in kbit).
        // cbr-ld-hq is the low-delay high-quality flavor of CBR and
        // zerolatency=true removes the reordering delay outright; b-adapt and
        // bframes only restate their defaults, but this tuning depends on
        // them, so they are pinned. The caps keep the encoder from quietly
        // negotiating down from high profile, which every NVENC supports.
        Encoder::Nvenc => format!(
            "nvh264enc bitrate={} gop-size={keyint} rc-mode={} preset={} \
             vbv-buffer-size={}{} ! video/x-h264,profile=high",
            p.bitrate_kbps,
            if p.zerolatency { "cbr-ld-hq" } else { "cbr" },
            if p.zerolatency { "low-latency-hq" } else { "hq" },
            p.bitrate_kbps * p.vbv_buf_ms / 1000,
            if p.zerolatency {
                " b-adapt=false bframes=0 zerolatency=true"
            } else {
                ""
            }
        ),
        Encoder::Va => format!(
            "vah264enc bitrate={} key-int-max={keyint} target-usage={} cpb-size={}",
            p.bitrate_kbps,
            if p.zerolatency { 6 } else { 4 },
            p.bitrate_kbps * p.vbv_buf_ms / 1000
        ),
        Encoder::Vaapi => format!(
            "vaapih264enc bitrate={} keyframe-period={keyint}",
            p.bitrate_kbps
        ),
        Encoder::Qsv => format!(
            "qsvh264enc bitrate={} gop-size={keyint} low-latency={}",
            p.bitrate_kbps, p.zerolatency
        ),
        Encoder::Mf => format!(
            "mfh264enc bitrate={} gop-size={keyint} low-latency={}",
            p.bitrate_kbps, p.zerolatency
        ),
        Encoder::Amf => format!(
            "amfh264enc bitrate={} gop-size={keyint} usage={}",
            p.bitrate_kbps,
            if p.zerolatency { "ultra-low-latency" } else { "transcoding" }
        ),
    };

    // The muxer is always named so an audio branch has something to link to
    // by reference (`mux.`), whether generated by audio_branch() or written
    // into a custom --sink by hand.
    let sync = format!("sync={}", p.sync);
    let mux_and_sink = if output.starts_with("rtmp://") {
        format!("flvmux name=mux streamable=true ! rtmp2sink {sync} location={output}")
    } else if output.starts_with("srt://") {
        // The leaky queue keeps a stalled or slow receiver from backpressuring
        // the encoder: packets drop at the sink instead of the stream falling
        // behind real time.
        format!(
            "mpegtsmux name=mux alignment=7 ! \
             queue max-size-buffers=3 leaky=downstream ! \
             srtsink {sync} wait-for-connection={} latency={} uri={output}",
            p.srt_wait_for_connection, p.srt_latency_ms
        )
    } else if let Some(rest) = output.strip_prefix("udp://") {
        let (host, port) = rest
            .rsplit_once(':')
            .filter(|(h, p)| !h.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
            .ok_or_else(|| format!("udp output must be udp://host:port, got '{output}'"))?;
        format!("mpegtsmux name=mux alignment=7 ! udpsink {sync} host={host} port={port}")
    } else {
        return Err(format!(
            "unsupported output '{output}' (expected rtmp://, srt:// or udp://host:port)"
        ));
    };

    // A 4:2:0 format pinned before the encoder: left to caps negotiation from
    // the RGB the AI branch deals in, videoconvert offers 4:4:4 -- twice the
    // samples for no benefit over these transports. x264 gets its native
    // planar I420; the hardware encoders are NV12-native (biplanar), which
    // spares them an internal repack per frame. Same chroma either way.
    let raw_format = if p.encoder == Encoder::X264 { "I420" } else { "NV12" };
    Ok(format!(
        "videoconvert ! video/x-raw,format={raw_format} ! {encoder} ! \
         h264parse config-interval=-1 ! {mux_and_sink}"
    ))
}

/// The audio path from the source's decoder to the output muxer.
///
/// Audio never enters the AI branch: it flows straight from `decoder.` (the
/// named decodebin of a `--uri` source) to `mux.` (the named muxer of a preset
/// sink). Synchronization comes from timestamps, not from this code -- the
/// relay pushes every video frame back with its original PTS, audio keeps the
/// PTS the source gave it, and the muxer pairs the two streams by clock. The
/// plain queues here just have to be deep enough to hold audio while the video
/// leg spends its AI budget and encoder delay, and the defaults (1 s) dwarf
/// both.
pub fn audio_branch(tap: Option<&str>) -> Result<String, String> {
    build_audio_branch(tap, |key| std::env::var(key).ok())
}

fn build_audio_branch(
    tap: Option<&str>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    if let Some(branch) = env("BRAIDPIPE_AUDIO_BRANCH") {
        return Ok(branch);
    }

    let encoder = env("BRAIDPIPE_AUDIO_ENCODER").unwrap_or_else(|| "avenc_aac".into());
    let bitrate_kbps: u32 = match env("BRAIDPIPE_AUDIO_BITRATE_KBPS") {
        Some(v) => parse_num("BRAIDPIPE_AUDIO_BITRATE_KBPS", &v)?,
        None => 128,
    };

    // The tap is where audio comes from: the named decodebin of a --uri
    // source by default, or a capture-card audio element (decklinkaudiosrc)
    // when the source has no demuxer to tap.
    let tap = tap.unwrap_or("decoder. ! queue ! audio/x-raw");

    // The common AAC encoders (avenc_aac, fdkaacenc, faac) all take bps.
    Ok(format!(
        "{tap} ! audioconvert ! audioresample ! \
         {encoder} bitrate={} ! aacparse ! queue ! mux.",
        bitrate_kbps * 1000
    ))
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(format!("{key} must be a boolean (1/0/true/false), got '{value}'")),
    }
}

fn parse_num<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("{key}: could not parse '{value}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn lowlatency_rtmp_matches_measured_tuned_sink() {
        let p = resolve("lowlatency", no_env).unwrap();
        let sink = render(&p, "rtmp://127.0.0.1:1935/live/stream", 30).unwrap();
        assert_eq!(
            sink,
            "videoconvert ! video/x-raw,format=I420 ! \
             x264enc speed-preset=veryfast tune=zerolatency bitrate=4500 key-int-max=60 \
             vbv-buf-capacity=200 ! \
             h264parse config-interval=-1 ! flvmux name=mux streamable=true ! \
             rtmp2sink sync=false location=rtmp://127.0.0.1:1935/live/stream"
        );
    }

    #[test]
    fn bandwidth_preset_drops_zerolatency_and_keeps_sync() {
        let p = resolve("bandwidth", no_env).unwrap();
        let sink = render(&p, "rtmp://example/live", 30).unwrap();
        assert!(!sink.contains("tune=zerolatency"));
        assert!(sink.contains("sync=true"));
        assert!(sink.contains("key-int-max=120"));
        assert!(sink.contains("vbv-buf-capacity=1000"));
    }

    #[test]
    fn env_overrides_win_over_preset_defaults() {
        let p = resolve("lowlatency", |key| match key {
            "BRAIDPIPE_BITRATE_KBPS" => Some("2500".into()),
            "BRAIDPIPE_SINK_SYNC" => Some("1".into()),
            "BRAIDPIPE_ENCODER" => Some("vtenc".into()),
            "BRAIDPIPE_VBV_BUF_MS" => Some("350".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(p.bitrate_kbps, 2500);
        assert_eq!(p.vbv_buf_ms, 350);
        assert!(p.sync);
        assert_eq!(p.encoder, Encoder::Vtenc);
        let sink = render(&p, "rtmp://example/live", 30).unwrap();
        assert!(sink.contains("vtenc_h264 realtime=true"));
        assert!(sink.contains("sync=true"));
    }

    #[test]
    fn srt_output_uses_mpegts_and_latency_budget() {
        let p = resolve("zerolatency", no_env).unwrap();
        let sink = render(&p, "srt://127.0.0.1:8888", 30).unwrap();
        assert!(sink.contains("mpegtsmux name=mux alignment=7"));
        assert!(sink.contains("queue max-size-buffers=3 leaky=downstream"));
        assert!(sink.contains(
            "srtsink sync=false wait-for-connection=false latency=50 uri=srt://127.0.0.1:8888"
        ));
    }

    #[test]
    fn srt_wait_for_connection_env_override() {
        let p = resolve("lowlatency", |key| {
            (key == "BRAIDPIPE_SRT_WAIT_FOR_CONNECTION").then(|| "1".to_string())
        })
        .unwrap();
        assert!(p.srt_wait_for_connection);
        let sink = render(&p, "srt://127.0.0.1:8888", 30).unwrap();
        assert!(sink.contains("wait-for-connection=true"));
    }

    #[test]
    fn udp_output_splits_host_and_port() {
        let p = resolve("lowlatency", no_env).unwrap();
        let sink = render(&p, "udp://239.0.0.1:5000", 30).unwrap();
        assert!(sink.contains("udpsink sync=false host=239.0.0.1 port=5000"));
        assert!(render(&p, "udp://nohost", 30).is_err());
    }

    #[test]
    fn audio_branch_links_decoder_to_mux() {
        let branch = build_audio_branch(None, no_env).unwrap();
        assert!(branch.starts_with("decoder. ! "));
        assert!(branch.ends_with(" ! mux."));
        assert!(branch.contains("avenc_aac bitrate=128000"));
    }

    #[test]
    fn audio_branch_tap_override_replaces_the_decoder() {
        let branch =
            build_audio_branch(Some("decklinkaudiosrc device-number=0 ! queue"), no_env).unwrap();
        assert!(branch.starts_with("decklinkaudiosrc device-number=0 ! queue ! audioconvert"));
        assert!(branch.ends_with(" ! mux."));
    }

    #[test]
    fn audio_branch_env_overrides() {
        let branch = build_audio_branch(None, |key| match key {
            "BRAIDPIPE_AUDIO_ENCODER" => Some("fdkaacenc".into()),
            "BRAIDPIPE_AUDIO_BITRATE_KBPS" => Some("96".into()),
            _ => None,
        })
        .unwrap();
        assert!(branch.contains("fdkaacenc bitrate=96000"));

        let replaced = build_audio_branch(None, |key| {
            (key == "BRAIDPIPE_AUDIO_BRANCH").then(|| "decoder. ! fakesink".to_string())
        })
        .unwrap();
        assert_eq!(replaced, "decoder. ! fakesink");
    }

    #[test]
    fn hardware_encoders_map_the_shared_params() {
        let with_encoder = |name: &'static str| {
            let p = resolve("lowlatency", move |key| {
                (key == "BRAIDPIPE_ENCODER").then(|| name.to_string())
            })
            .unwrap();
            render(&p, "srt://127.0.0.1:8888", 30).unwrap()
        };

        // lowlatency: 4500 kbps, 2s GOP at 30fps = 60, 200ms VBV = 900 kbit.
        let vtenc = with_encoder("vtenc");
        assert!(vtenc.contains(
            "vtenc_h264 realtime=true allow-frame-reordering=false bitrate=4500 \
             max-keyframe-interval=60 rate-control=cbr quality=0.65"
        ));

        let nvenc = with_encoder("nvenc");
        assert!(nvenc.contains(
            "nvh264enc bitrate=4500 gop-size=60 rc-mode=cbr-ld-hq preset=low-latency-hq \
             vbv-buffer-size=900 b-adapt=false bframes=0 zerolatency=true ! \
             video/x-h264,profile=high"
        ));
        assert!(nvenc.contains("video/x-raw,format=NV12"));

        let va = with_encoder("va");
        assert!(va.contains("vah264enc bitrate=4500 key-int-max=60 target-usage=6 cpb-size=900"));

        assert!(with_encoder("vaapi").contains("vaapih264enc bitrate=4500 keyframe-period=60"));
        assert!(with_encoder("qsv").contains("qsvh264enc bitrate=4500 gop-size=60 low-latency=true"));
        assert!(with_encoder("mf").contains("mfh264enc bitrate=4500 gop-size=60 low-latency=true"));
        assert!(with_encoder("amf")
            .contains("amfh264enc bitrate=4500 gop-size=60 usage=ultra-low-latency"));
    }

    #[test]
    fn auto_encoder_keeps_the_preset_default_for_later_detection() {
        let p = resolve("lowlatency", |key| {
            (key == "BRAIDPIPE_ENCODER").then(|| "auto".to_string())
        })
        .unwrap();
        assert_eq!(p.encoder, Encoder::X264);
    }

    #[test]
    fn cli_encoder_values_match_the_enum() {
        assert_eq!(ENCODER_VALUES[0], "auto");
        assert_eq!(ENCODER_VALUES.len(), Encoder::NAMES.len() + 1);
        for (name, encoder) in Encoder::NAMES {
            assert!(ENCODER_VALUES.contains(&name));
            assert_eq!(Encoder::from_name(name), Some(encoder));
        }
    }

    #[test]
    fn factory_names_round_trip_to_encoders() {
        assert_eq!(Encoder::from_factory("nvh264enc"), Some(Encoder::Nvenc));
        assert_eq!(Encoder::from_factory("vtenc_h264"), Some(Encoder::Vtenc));
        assert_eq!(Encoder::from_factory("vah264enc"), Some(Encoder::Va));
        assert_eq!(Encoder::from_factory("x264enc"), None);
    }

    #[test]
    fn unknown_preset_and_bad_env_are_reported() {
        assert!(resolve("warpspeed", no_env)
            .unwrap_err()
            .contains("zerolatency, lowlatency, balanced, bandwidth"));
        assert!(resolve("lowlatency", |key| {
            (key == "BRAIDPIPE_ZEROLATENCY").then(|| "maybe".to_string())
        })
        .is_err());
    }
}
