use braidpipe_core::metrics;
use braidpipe_core::ports::media::{ActiveBranch, EngineError, StreamController};
use gstreamer::prelude::*;
use gstreamer_app::{AppSink, AppSrc};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{error, info, warn};

/// Encoders whose src pads carry a meaningful DELTA_UNIT flag for keyframe
/// counting. A whitelist, because "name contains enc" would match audio.
const VIDEO_ENCODER_FACTORIES: &[&str] = &[
    "x264enc",
    "vtenc_h264",
    "vtenc_h264_hw",
    "openh264enc",
    "vp9enc",
    "nvh264enc",
    "vah264enc",
    "vaapih264enc",
    "qsvh264enc",
    "mfh264enc",
    "amfh264enc",
];

pub struct GStreamerEngine {
    pipeline: gstreamer::Pipeline,
    input_selector: gstreamer::Element,
    pad_passthrough: gstreamer::Pad,
    pad_ai: gstreamer::Pad,
    current_branch: Arc<Mutex<ActiveBranch>>,
    is_running: AtomicBool,
    /// Handles polled at metrics-scrape time, cached so a scrape never has to
    /// walk the pipeline: the two branch queues and any SRT elements.
    metric_queues: Vec<(&'static str, gstreamer::Element)>,
    srt_elements: Vec<gstreamer::Element>,
}

impl GStreamerEngine {
    /// Builds the GStreamer pipeline string dynamically based on source/sink specs
    pub fn new(source_pipeline: &str, sink_pipeline: &str) -> Result<Self, EngineError> {
        Self::new_with_branch(source_pipeline, sink_pipeline, None)
    }

    /// As [`new`], with an extra top-level branch appended to the description.
    ///
    /// This is how audio flows: the branch links elements that the source and
    /// sink fragments defined by name (typically `decoder.` to `mux.`),
    /// bypassing the video-only tee/selector graph entirely. Failover never
    /// touches it -- the input-selector switches video branches while audio
    /// keeps flowing, and the muxer keeps the two aligned by timestamp because
    /// the relay preserves PTS across the AI roundtrip.
    pub fn new_with_branch(
        source_pipeline: &str,
        sink_pipeline: &str,
        extra_branch: Option<&str>,
    ) -> Result<Self, EngineError> {
        gstreamer::init().map_err(|e| EngineError::BuildFailed(e.to_string()))?;

        // Before any decodebin exists: raise the rank of the platform's GPU
        // decoders so autoplugging prefers them (BRAIDPIPE_HW=off skips this).
        crate::hwaccel::promote_hardware_decoders();

        // Pipeline description string using GStreamer launch syntax.
        // q_pass is leaky so that, while the AI branch is selected, buffers
        // piling up on the inactive passthrough pad can never block the tee
        // and stall the whole pipeline.
        //
        // Each AI-side link converts for itself. A tee delivers one format to
        // every branch, so without these the appsink's RGB requirement is
        // forced all the way back through the source: fine for videotestsrc,
        // which offers RGB natively, but camera sources negotiate a native
        // format instead and the branch stalls with no error on the bus.
        //
        // max-latency=-1 on the appsrc is what makes live sources work at all.
        // An appsrc defaults to reporting zero maximum latency, and a live
        // source reports a minimum of one frame period; the selector then
        // aggregates min > max, latency configuration fails, and the pipeline
        // sits in PLAYING forever without ever delivering a buffer.
        let pipe_desc = format!(
            "{} ! tee name=t \
             t. ! queue name=q_pass leaky=downstream max-size-buffers=3 ! sel.sink_0 \
             t. ! queue name=q_ai leaky=downstream max-size-buffers=3 ! videoconvert ! appsink name=ai_sink sync=false max-buffers=1 drop=true \
             appsrc name=ai_src format=time is-live=true max-buffers=4 leaky-type=downstream min-latency=0 max-latency=-1 ! videoconvert ! sel.sink_1 \
             input-selector name=sel sync-streams=false ! {} {}",
            source_pipeline,
            sink_pipeline,
            extra_branch.unwrap_or("")
        );

        let element = gstreamer::parse::launch(&pipe_desc)
            .map_err(|e| EngineError::BuildFailed(e.to_string()))?;

        let pipeline = element
            .downcast::<gstreamer::Pipeline>()
            .map_err(|_| EngineError::BuildFailed("Failed to downcast to Pipeline".into()))?;

        let input_selector = pipeline
            .by_name("sel")
            .ok_or_else(|| EngineError::BuildFailed("Missing input-selector element".into()))?;

        // Request sink pads from input-selector to allow manual switching
        let pad_passthrough = input_selector
            .static_pad("sink_0")
            .ok_or_else(|| EngineError::BuildFailed("Missing sink_0 pad".into()))?;

        let pad_ai = input_selector
            .static_pad("sink_1")
            .ok_or_else(|| EngineError::BuildFailed("Missing sink_1 pad".into()))?;

        // One log line per codec element actually in use. The rank promotion
        // above only says what is *eligible*; this says what the stream is
        // processed with. Encoders from the launch string already exist, so
        // sweep once; decoders appear inside decodebin3 when the input's caps
        // are known, which is what the signal catches.
        pipeline.connect_deep_element_added(|_, _, element| Self::log_codec_element(element));
        for element in pipeline.iterate_recurse().into_iter().flatten() {
            Self::log_codec_element(&element);
        }

        let (metric_queues, srt_elements) = Self::attach_metrics(&pipeline, &input_selector);

        Ok(Self {
            pipeline,
            input_selector,
            pad_passthrough,
            pad_ai,
            current_branch: Arc::new(Mutex::new(ActiveBranch::Passthrough)),
            is_running: AtomicBool::new(false),
            metric_queues,
            srt_elements,
        })
    }

    /// Wires the pipeline into the metrics registry: pad probes for frame
    /// counting and timestamps, overrun signals on the branch queues, and
    /// cached handles for the values polled at scrape time. Every hook here is
    /// optional -- a pipeline without an encoder, muxer or SRT element simply
    /// produces fewer metrics.
    fn attach_metrics(
        pipeline: &gstreamer::Pipeline,
        input_selector: &gstreamer::Element,
    ) -> (
        Vec<(&'static str, gstreamer::Element)>,
        Vec<gstreamer::Element>,
    ) {
        // Input side: every frame the source delivers, its arrival jitter,
        // timestamp sanity, and the negotiated caps.
        if let Some(pad) = pipeline
            .by_name("t")
            .and_then(|tee| tee.static_pad("sink"))
        {
            let state: Mutex<(Option<Instant>, Option<gstreamer::ClockTime>)> =
                Mutex::new((None, None));
            pad.add_probe(
                gstreamer::PadProbeType::BUFFER | gstreamer::PadProbeType::EVENT_DOWNSTREAM,
                move |_, info| {
                    match &info.data {
                        Some(gstreamer::PadProbeData::Buffer(buffer)) => {
                            metrics::INPUT_FRAMES.inc();
                            let mut state = state.lock().unwrap();
                            let now = Instant::now();
                            if let Some(previous) = state.0.replace(now) {
                                metrics::INPUT_INTERVAL_SECONDS
                                    .observe_seconds((now - previous).as_secs_f64());
                            }
                            if let Some(pts) = buffer.pts() {
                                if let Some(last) = state.1 {
                                    // Backwards is always wrong; forwards is a
                                    // discontinuity once the gap passes 1.75x
                                    // the frame duration (when known).
                                    let gapped = buffer.duration().is_some_and(|d| {
                                        pts.nseconds()
                                            > last.nseconds() + d.nseconds() * 7 / 4
                                    });
                                    if pts < last || gapped {
                                        metrics::PTS_DISCONTINUITIES.inc();
                                    }
                                }
                                state.1 = Some(pts);
                            }
                        }
                        Some(gstreamer::PadProbeData::Event(event)) => {
                            if let gstreamer::EventView::Caps(caps) = event.view()
                                && let Some(s) = caps.caps().structure(0)
                            {
                                if let Ok(width) = s.get::<i32>("width") {
                                    metrics::INPUT_WIDTH.set(i64::from(width));
                                }
                                if let Ok(height) = s.get::<i32>("height") {
                                    metrics::INPUT_HEIGHT.set(i64::from(height));
                                }
                                if let Ok(fps) = s.get::<gstreamer::Fraction>("framerate")
                                    && fps.denom() != 0
                                {
                                    metrics::INPUT_FPS
                                        .set(f64::from(fps.numer()) / f64::from(fps.denom()));
                                }
                            }
                        }
                        _ => {}
                    }
                    gstreamer::PadProbeReturn::Ok
                },
            );
        }

        // Output side: frames actually leaving the selector toward the sink.
        if let Some(pad) = input_selector.static_pad("src") {
            pad.add_probe(gstreamer::PadProbeType::BUFFER, |_, _| {
                metrics::OUTPUT_FRAMES.inc();
                gstreamer::PadProbeReturn::Ok
            });
        }

        let mut srt_elements = Vec::new();
        for element in pipeline.iterate_elements().into_iter().flatten() {
            let Some(factory) = element.factory() else {
                continue;
            };
            let factory = factory.name();

            // Keyframe cadence, at the encoder's output where the DELTA_UNIT
            // flag is authoritative (post-mux, audio tags would pollute it).
            if VIDEO_ENCODER_FACTORIES.contains(&factory.as_str())
                && let Some(pad) = element.static_pad("src") {
                    pad.add_probe(gstreamer::PadProbeType::BUFFER, |_, info| {
                        if let Some(gstreamer::PadProbeData::Buffer(buffer)) = &info.data
                            && !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT)
                        {
                            metrics::KEYFRAMES.inc();
                        }
                        gstreamer::PadProbeReturn::Ok
                    });
                }

            if factory == "srtsink" || factory == "srtsrc" {
                srt_elements.push(element.clone());
            }
        }

        // Wire bytes: whatever finally leaves the process. Every terminal
        // element except the AI tap (an appsink of our own) counts.
        for sink in pipeline.iterate_sinks().into_iter().flatten() {
            if sink.name() == "ai_sink" {
                continue;
            }
            if let Some(pad) = sink.static_pad("sink") {
                pad.add_probe(gstreamer::PadProbeType::BUFFER, |_, info| {
                    if let Some(gstreamer::PadProbeData::Buffer(buffer)) = &info.data {
                        metrics::SINK_BYTES.add(buffer.size() as u64);
                        metrics::SINK_BUFFERS.inc();
                    }
                    gstreamer::PadProbeReturn::Ok
                });
            }
        }

        // A/V skew: last PTS of each stream entering the muxer. The audio pad
        // usually appears later (delayed linking from the decoder), so watch
        // pad-added as well as the pads that already exist.
        if let Some(mux) = pipeline.by_name("mux") {
            for pad in mux.sink_pads() {
                Self::attach_mux_pts_probe(&pad);
            }
            mux.connect_pad_added(|_, pad| {
                if pad.direction() == gstreamer::PadDirection::Sink {
                    Self::attach_mux_pts_probe(pad);
                }
            });
        }

        // Drop events on the branch queues. For a leaky queue, overrun is the
        // moment an old buffer is discarded to admit a new one.
        let mut metric_queues = Vec::new();
        for (name, counter) in [
            ("q_pass", &metrics::QUEUE_OVERRUNS_PASS),
            ("q_ai", &metrics::QUEUE_OVERRUNS_AI),
        ] {
            if let Some(queue) = pipeline.by_name(name) {
                let counter: &'static metrics::Counter = counter;
                queue.connect("overrun", false, move |_| {
                    counter.inc();
                    None
                });
                metric_queues.push((name, queue));
            }
        }

        (metric_queues, srt_elements)
    }

    /// Logs a video encoder/decoder the pipeline is actually using, tagged
    /// hardware or software from the factory's klass metadata.
    fn log_codec_element(element: &gstreamer::Element) {
        let Some(factory) = element.factory() else {
            return;
        };
        let Some(klass) = factory.metadata(gstreamer::ELEMENT_METADATA_KLASS) else {
            return;
        };
        if !klass.contains("Video") {
            return;
        }
        let direction = if klass.contains("Decoder") {
            "decoder"
        } else if klass.contains("Encoder") {
            "encoder"
        } else {
            return;
        };
        let implementation = if klass.contains("Hardware") {
            "hardware"
        } else {
            "software"
        };
        info!(element = %factory.name(), "Video {direction} in use ({implementation})");
    }

    fn attach_mux_pts_probe(pad: &gstreamer::Pad) {
        // 0 = undetermined (caps not negotiated yet), 1 = video, 2 = audio,
        // 3 = neither; resolved lazily on the first buffers.
        let kind = AtomicU8::new(0);
        pad.add_probe(gstreamer::PadProbeType::BUFFER, move |pad, info| {
            let Some(gstreamer::PadProbeData::Buffer(buffer)) = &info.data else {
                return gstreamer::PadProbeReturn::Ok;
            };
            let mut k = kind.load(Ordering::Relaxed);
            if k == 0
                && let Some(caps) = pad.current_caps()
                    && let Some(s) = caps.structure(0)
                {
                    k = if s.name().starts_with("video/") {
                        1
                    } else if s.name().starts_with("audio/") {
                        2
                    } else {
                        3
                    };
                    kind.store(k, Ordering::Relaxed);
                }
            // Raw PTS at a muxer is not comparable across pads: video
            // encoders shift timestamps by a large constant (1000 hours) to
            // keep DTS non-negative. Running time through the pad's segment
            // is the shared clock both streams actually mux on.
            let running_time = buffer.pts().and_then(|pts| {
                pad.sticky_event::<gstreamer::event::Segment>(0)?
                    .segment()
                    .downcast_ref::<gstreamer::ClockTime>()?
                    .to_running_time(pts)
            });
            if let Some(rt) = running_time {
                let us = (rt.nseconds() / 1000) as i64;
                match k {
                    1 => metrics::LAST_VIDEO_PTS_US.store(us, Ordering::Relaxed),
                    2 => metrics::LAST_AUDIO_PTS_US.store(us, Ordering::Relaxed),
                    _ => {}
                }
            }
            gstreamer::PadProbeReturn::Ok
        });
    }

    /// Appends the metrics that only exist as live pipeline state: branch
    /// queue depths and, when an SRT element is present, its transport
    /// statistics. Called by the metrics endpoint on every scrape.
    pub fn collect_runtime_metrics(&self, out: &mut String) {
        use std::fmt::Write;

        if !self.metric_queues.is_empty() {
            let _ = writeln!(
                out,
                "# HELP braidpipe_queue_depth Buffers currently held per branch queue\n\
                 # TYPE braidpipe_queue_depth gauge"
            );
            for (name, queue) in &self.metric_queues {
                let depth = queue.property::<u32>("current-level-buffers");
                let _ = writeln!(out, "braidpipe_queue_depth{{queue=\"{name}\"}} {depth}");
            }
        }

        for element in &self.srt_elements {
            let stats = element.property::<gstreamer::Structure>("stats");
            let element_name = element.name();
            for (field, value) in stats.iter() {
                let Some(value) = value_as_f64(value) else {
                    continue;
                };
                let metric = field.replace('-', "_");
                let _ = writeln!(
                    out,
                    "braidpipe_srt_{metric}{{element=\"{element_name}\"}} {value}"
                );
            }
        }
    }

    /// Builds a video source from a URI using GStreamer's URI source selection and decodebin3.
    pub fn new_from_uri(
        uri: &str,
        sink_pipeline: &str,
        extra_branch: Option<&str>,
    ) -> Result<Self, EngineError> {
        let source_pipeline = Self::uri_source_pipeline(uri)?;
        Self::new_with_branch(&source_pipeline, sink_pipeline, extra_branch)
    }

    fn uri_source_pipeline(uri: &str) -> Result<String, EngineError> {
        if uri.trim().is_empty() || !uri.contains("://") {
            return Err(EngineError::BuildFailed(
                "Input URI must include a scheme, such as srt://, udp://, rtp://, or ndi://".into(),
            ));
        }

        if uri.contains(['"', '\\']) {
            return Err(EngineError::BuildFailed(
                "Input URI cannot contain quotes or backslashes".into(),
            ));
        }

        if uri
            .get(..6)
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("srt://"))
        {
            return Ok(format!(
                "srtsrc uri=\"{uri}\" ! queue ! decodebin3 name=decoder decoder. ! queue ! video/x-raw ! videoconvert ! videoscale"
            ));
        }

        Ok(format!(
            "uridecodebin3 uri=\"{uri}\" name=decoder decoder. ! queue ! video/x-raw ! videoconvert ! videoscale"
        ))
    }

    /// Accessors for the IPC layer to hook into AppSink and AppSrc
    pub fn get_ai_sink(&self) -> Result<AppSink, EngineError> {
        self.pipeline
            .by_name("ai_sink")
            .ok_or_else(|| EngineError::BuildFailed("Missing ai_sink".into()))?
            .downcast::<AppSink>()
            .map_err(|_| EngineError::BuildFailed("Invalid AppSink".into()))
    }

    pub fn get_ai_src(&self) -> Result<AppSrc, EngineError> {
        self.pipeline
            .by_name("ai_src")
            .ok_or_else(|| EngineError::BuildFailed("Missing ai_src".into()))?
            .downcast::<AppSrc>()
            .map_err(|_| EngineError::BuildFailed("Invalid AppSrc".into()))
    }
}

/// SRT stats structures mix numeric types freely (rates are doubles, packet
/// counts are various integer widths); anything numeric becomes a metric and
/// everything else (caller lists, strings) is skipped.
fn value_as_f64(value: &gstreamer::glib::SendValue) -> Option<f64> {
    value
        .get::<f64>()
        .ok()
        .or_else(|| value.get::<i32>().ok().map(f64::from))
        .or_else(|| value.get::<u32>().ok().map(f64::from))
        .or_else(|| value.get::<i64>().ok().map(|v| v as f64))
        .or_else(|| value.get::<u64>().ok().map(|v| v as f64))
}

impl StreamController for GStreamerEngine {
    async fn set_active_branch(&self, branch: ActiveBranch) -> Result<(), EngineError> {
        let target_pad = match branch {
            ActiveBranch::Passthrough => &self.pad_passthrough,
            ActiveBranch::AiProcess => &self.pad_ai,
        };

        // Switch the input-selector active pad dynamically without stopping the stream
        self.input_selector.set_property("active-pad", target_pad);

        let mut current = self.current_branch.lock().unwrap();
        *current = branch;

        info!(branch = ?branch, "Successfully switched video stream branch");
        Ok(())
    }

    fn current_branch(&self) -> ActiveBranch {
        *self.current_branch.lock().unwrap()
    }

    async fn start(&self) -> Result<(), EngineError> {
        // Surface asynchronous pipeline failures (e.g. a sink losing its
        // connection) which are otherwise silently posted on the bus.
        if let Some(bus) = self.pipeline.bus() {
            std::thread::spawn(move || {
                use gstreamer::MessageView;
                for msg in bus.iter_timed(gstreamer::ClockTime::NONE) {
                    match msg.view() {
                        MessageView::Error(err) => {
                            metrics::BUS_ERRORS.inc();
                            error!(
                                source = ?err.src().map(|s| s.path_string()),
                                error = %err.error(),
                                debug = ?err.debug(),
                                "GStreamer pipeline error"
                            );
                        }
                        MessageView::Warning(_) => metrics::BUS_WARNINGS.inc(),
                        MessageView::Eos(_) => {
                            warn!("GStreamer pipeline reached end-of-stream");
                            break;
                        }
                        _ => {}
                    }
                }
            });
        }

        self.pipeline
            .set_state(gstreamer::State::Playing)
            .map_err(|e| EngineError::StateChangeFailed(e.to_string()))?;

        self.is_running.store(true, Ordering::SeqCst);
        info!("GStreamer engine pipeline started in PLAYING state");
        Ok(())
    }

    async fn stop(&self) -> Result<(), EngineError> {
        self.pipeline
            .set_state(gstreamer::State::Null)
            .map_err(|e| EngineError::StateChangeFailed(e.to_string()))?;

        self.is_running.store(false, Ordering::SeqCst);
        info!("GStreamer engine pipeline stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::GStreamerEngine;

    #[test]
    fn creates_a_decodebin3_pipeline_for_srt_uris() {
        let pipeline = GStreamerEngine::uri_source_pipeline("srt://127.0.0.1:9000")
            .expect("valid URI should build a source pipeline");

        assert!(pipeline.contains("srtsrc"));
        assert!(pipeline.contains("decodebin3"));
        assert!(pipeline.contains("uri=\"srt://127.0.0.1:9000\""));
    }

    #[test]
    fn creates_a_uri_decoder_pipeline_for_non_srt_uris() {
        let pipeline = GStreamerEngine::uri_source_pipeline("ndi://Studio%20Camera")
            .expect("valid URI should build a source pipeline");

        assert!(pipeline.contains("uridecodebin3"));
    }

    #[test]
    fn rejects_uris_without_a_scheme() {
        assert!(GStreamerEngine::uri_source_pipeline("127.0.0.1:9000").is_err());
    }
}
