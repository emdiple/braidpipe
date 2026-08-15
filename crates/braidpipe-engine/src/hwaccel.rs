//! GPU decode/encode selection, one list per platform.
//!
//! Decoding needs no pipeline changes at all: decodebin3 picks decoders by
//! element rank, so "use the GPU" is a registry tweak. At engine construction
//! the platform's hardware decoders are promoted above the software `avdec_*`
//! family and autoplugging does the rest. Every decoder listed here can also
//! negotiate plain system memory, so the existing `video/x-raw` branch caps
//! keep working — frames are decoded on the GPU and copied out, which moves
//! the expensive half off the CPU without a zero-copy rewrite downstream.
//!
//! Encoding is a choice the preset layer makes while rendering the sink
//! string: [`preferred_h264_encoder`] returns the best hardware encoder the
//! registry actually has (the NVIDIA/VA/QSV/AMF/MediaFoundation plugins only
//! register elements when their device probe succeeds), and the caller falls
//! back to x264 when there is none.
//!
//! `BRAIDPIPE_HW=off` disables both directions; `BRAIDPIPE_ENCODER` pins the
//! encoder regardless of what is detected.

use gstreamer as gst;
use gstreamer::prelude::*;
use std::sync::{Once, OnceLock};
use tracing::info;

/// Hardware decoders in preference order: earlier entries get a higher rank.
/// macOS: VideoToolbox (vtdec_hw usually already outranks avdec, but the
/// promotion makes that true regardless of plugin version).
#[cfg(target_os = "macos")]
const HW_DECODERS: &[&str] = &["vtdec_hw", "vtdec"];

/// Linux: NVDEC, then VA-API (the modern `va` plugin, then legacy `vaapi`),
/// then Intel QuickSync via the dedicated qsv plugin.
#[cfg(target_os = "linux")]
const HW_DECODERS: &[&str] = &[
    "nvh264dec",
    "nvh265dec",
    "nvvp9dec",
    "nvav1dec",
    "vah264dec",
    "vah265dec",
    "vavp9dec",
    "vaav1dec",
    "qsvh264dec",
    "qsvh265dec",
    "qsvav1dec",
    "vaapih264dec",
    "vaapih265dec",
];

/// Windows: Direct3D 12/11 (vendor-neutral, cover NVIDIA/AMD/Intel alike),
/// then the vendor-specific paths.
#[cfg(target_os = "windows")]
const HW_DECODERS: &[&str] = &[
    "d3d12h264dec",
    "d3d12h265dec",
    "d3d12vp9dec",
    "d3d12av1dec",
    "d3d11h264dec",
    "d3d11h265dec",
    "d3d11vp9dec",
    "d3d11av1dec",
    "nvh264dec",
    "nvh265dec",
    "qsvh264dec",
    "qsvh265dec",
];

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const HW_DECODERS: &[&str] = &[];

/// Hardware H.264 encoders in preference order.
#[cfg(target_os = "macos")]
const HW_ENCODERS: &[&str] = &["vtenc_h264"];

#[cfg(target_os = "linux")]
const HW_ENCODERS: &[&str] = &["nvh264enc", "vah264enc", "qsvh264enc", "vaapih264enc"];

#[cfg(target_os = "windows")]
const HW_ENCODERS: &[&str] = &["nvh264enc", "qsvh264enc", "amfh264enc", "mfh264enc"];

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const HW_ENCODERS: &[&str] = &[];

/// An explicit CLI choice (`--hw auto|off`). Set at most once, before the
/// engine is constructed; when present it wins over BRAIDPIPE_HW.
static CLI_OVERRIDE: OnceLock<bool> = OnceLock::new();

/// Records the `--hw` flag. Later calls are ignored.
pub fn set_enabled(enabled: bool) {
    let _ = CLI_OVERRIDE.set(enabled);
}

pub fn enabled() -> bool {
    CLI_OVERRIDE
        .get()
        .copied()
        .unwrap_or_else(|| enabled_from(std::env::var("BRAIDPIPE_HW").ok().as_deref()))
}

fn enabled_from(value: Option<&str>) -> bool {
    !matches!(value, Some("off" | "0" | "false" | "no"))
}

/// Raises the rank of every hardware decoder present so decodebin3 autoplugs
/// it ahead of the software decoders. Runs once per process.
pub fn promote_hardware_decoders() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if !enabled() {
            info!("GPU mode off (--hw off / BRAIDPIPE_HW=off): decoders keep their default rank");
            return;
        }
        if gst::init().is_err() {
            return;
        }

        // Earlier candidates end up with the higher rank, and ranks are only
        // ever raised — a decoder the distribution already trusts above this
        // scheme keeps its place.
        let base = i32::from(gst::Rank::PRIMARY) + 64;
        let mut promoted = Vec::new();
        for (position, name) in HW_DECODERS.iter().enumerate() {
            let Some(factory) = gst::ElementFactory::find(name) else {
                continue;
            };
            let target = base - position as i32;
            if i32::from(factory.rank()) < target {
                factory.set_rank(gst::Rank::from(target));
            }
            promoted.push(*name);
        }
        if promoted.is_empty() {
            info!("No hardware decoders in the registry; decodebin picks software");
        } else {
            info!(decoders = ?promoted, "Hardware decoders promoted for autoplugging");
        }
    });
}

/// The best hardware H.264 encoder available on this machine, or None when
/// there is none (or hardware is disabled) and the caller should use x264.
pub fn preferred_h264_encoder() -> Option<&'static str> {
    if !enabled() {
        return None;
    }
    gst::init().ok()?;
    HW_ENCODERS
        .iter()
        .copied()
        .find(|name| gst::ElementFactory::find(name).is_some())
}

#[cfg(test)]
mod tests {
    use super::enabled_from;

    #[test]
    fn hardware_is_on_by_default_and_off_only_on_request() {
        assert!(enabled_from(None));
        assert!(enabled_from(Some("auto")));
        assert!(enabled_from(Some("1")));
        assert!(!enabled_from(Some("off")));
        assert!(!enabled_from(Some("0")));
        assert!(!enabled_from(Some("false")));
        assert!(!enabled_from(Some("no")));
    }
}
