use braidpipe_core::ports::media::{ActiveBranch, EngineError, StreamController};
use gstreamer::prelude::*;
use gstreamer_app::{AppSink, AppSrc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

pub struct GStreamerEngine {
    pipeline: gstreamer::Pipeline,
    input_selector: gstreamer::Element,
    pad_passthrough: gstreamer::Pad,
    pad_ai: gstreamer::Pad,
    current_branch: Arc<Mutex<ActiveBranch>>,
    is_running: AtomicBool,
}

impl GStreamerEngine {
    /// Builds the GStreamer pipeline string dynamically based on source/sink specs
    pub fn new(source_pipeline: &str, sink_pipeline: &str) -> Result<Self, EngineError> {
        gstreamer::init().map_err(|e| EngineError::BuildFailed(e.to_string()))?;

        // Pipeline description string using GStreamer launch syntax for construction
        let pipe_desc = format!(
            "{} ! tee name=t \
             t. ! queue name=q_pass ! sel.sink_0 \
             t. ! queue name=q_ai ! appsink name=ai_sink max-buffers=1 drop=true \
             appsrc name=ai_src ! sel.sink_1 \
             input-selector name=sel ! {}",
            source_pipeline, sink_pipeline
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

        Ok(Self {
            pipeline,
            input_selector,
            pad_passthrough,
            pad_ai,
            current_branch: Arc::new(Mutex::new(ActiveBranch::Passthrough)),
            is_running: AtomicBool::new(false),
        })
    }

    /// Builds a video source from a URI using GStreamer's URI source selection and decodebin3.
    pub fn new_from_uri(uri: &str, sink_pipeline: &str) -> Result<Self, EngineError> {
        let source_pipeline = Self::uri_source_pipeline(uri)?;
        Self::new(&source_pipeline, sink_pipeline)
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
                "srtsrc uri=\"{uri}\" ! queue ! decodebin3 name=decoder decoder. ! queue ! video/x-raw ! videoconvert"
            ));
        }

        Ok(format!(
            "uridecodebin3 uri=\"{uri}\" name=decoder decoder. ! queue ! video/x-raw ! videoconvert"
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
