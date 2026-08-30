# Operations

Proving the availability claim and keeping an eye on it: how failover actually works, the drills that exercise it, latency numbers, the metrics stack, and the usual failure modes.

[← Back to the README](../README.md) · [Streaming configuration](streaming.md) · [AI workers](workers.md)

- [How the failover works](#how-the-failover-works)
- [Testing failover](#testing-failover)
- [Measuring latency](#measuring-latency)
- [Monitoring](#monitoring)
  - [Shutdown and stale panels](#shutdown-and-stale-panels)
- [Troubleshooting](#troubleshooting)

## How the failover works

Availability is enforced at two independent levels, so a single missed frame is handled differently from a dead worker.

**Per frame (the relay).** For every frame tapped off the pipeline, the relay writes it into a shared-memory slot, signals Python, and waits for an acknowledgement. The budget is 1.5 frame periods — 50 ms at 30 fps. If the ack doesn't arrive in time, or shared memory is full, or Python reports a failure, the relay pushes the **original, unmodified frame** downstream and reclaims the slot. Timing jitter therefore costs you an un-overlaid frame, not a gap in the stream.

**Over time (the watchdog).** The relay reports each success and failure to a health counter. Thirty consecutive failures — about one second at 30 fps — mark the worker unhealthy, and the watchdog switches the `input-selector` to the passthrough branch. One successful roundtrip resets the counter and the AI branch is selected again. The AI branch starts out unselected and has to earn its place with a first successful frame, so a worker that never starts correctly can't take frames with it.

Both branch queues are `leaky=downstream`, which matters more than it looks: without leaky queues, buffers piling up on the *inactive* selector pad eventually block the `tee` and stall the entire pipeline, including the branch that was working fine.

## Testing failover

The interesting property is what happens when Python dies mid-stream. Start the daemon, wait for `branch=AiProcess`, then kill the worker using the PID from the log line `Python worker active pid=…`:

```bash
kill -9 <worker-pid>
```

Within about a second you should see `Successfully switched video stream branch branch=Passthrough`, the overlay disappear, and the stream continue without a stall, a black frame, or a dropped publisher connection. Measured on a 30 fps test pattern: the switch lands 1.0 s after the kill, and output frames keep arriving at exactly 30 fps across it.

**Killing the worker by pattern is harder than it looks, and most obvious attempts are wrong.**

`pkill -f 'worker.py'` matches the *whole* command line, and the daemon's own command line contains `--python-script python/braidpipe/worker.py` — so it kills the daemon too and proves nothing. Anchoring to the interpreter does not help either: `.venv/bin/python3` is a symlink chain, so the running process reports its resolved interpreter path (`…/Python.framework/Versions/3.14/Resources/Python.app/Contents/MacOS/Python` on Homebrew macOS), and a pattern containing `python3` never matches at all. It fails silently, which is worse than failing loudly.

Target the worker by parentage instead — it is the daemon's only child, on any platform:

```bash
pgrep -P "$(pgrep -x braidpipe)" -l     # check first: one PID, the worker
pkill -9 -P "$(pgrep -x braidpipe)"
```

Or just use the PID the daemon already printed, which is the same number.

Nothing respawns the worker (see [Known limitations](../README.md#known-limitations)), but you can start one by hand and the daemon picks it up on its next successful frame — verified: `branch=AiProcess` returns within a couple of seconds:

```bash
.venv/bin/python3 examples/worker_edges.py
```

Run it from the repository root, and note that a hand-started worker is no longer a child of the daemon, so the `pgrep -P` trick above will not find it a second time.

There's also a manual SRT check that exercises URI ingestion end to end with a graphical sink:

```bash
bash scripts/e2e-srt-autovideosink.sh
```

## Measuring latency

```bash
bash scripts/rtmp-latency.sh
```

This publishes a test pattern over RTMP and reports how late every frame was when a receiver got it. It needs nothing installed beyond ffmpeg: `-listen 1` makes ffmpeg the RTMP server braidpipe publishes to *and* the decoder, so no media server sits in the middle inflating the number.

There is no OCR and no guessing. [worker_stamp.py](../examples/worker_stamp.py) writes the wall clock into every frame as a row of large black-and-white cells, and [rtmp_latency_probe.py](../scripts/rtmp_latency_probe.py) reads it back out of the decoded pixels and subtracts. Both processes are on one machine reading one clock, so this is a true one-way measurement rather than a halved round trip. Big cells are the point: H.264 will smear a thin line, but block-coded black and white survive any bitrate worth streaming — the runs below decoded 100% of frames.

Measured on an M-series Mac, 1280x720 @ 30 fps, `x264enc tune=zerolatency speed-preset=ultrafast`, medians over ~700 frames:

| Leg | p50 | What it covers |
| --- | --- | --- |
| daemon → worker | **0.17 ms** | SHM write, UDS datagram, worker wake-up |
| worker → pipeline output | **6.1 ms** | SHM read-back, appsrc, videoconvert, sink |
| worker → RTMP receiver | **39.5 ms** | the above plus encode, flvmux, RTMP, demux, decode |

So braidpipe's own contribution is about **6 ms**, and everything else is the encoder and the transport. The IPC is not what costs you.

**The single biggest knob is `sync=false` on the sink**, worth 48 ms on its own:

| Sink | p50 |
| --- | --- |
| `rtmp2sink` (defaults) | 89.0 ms |
| `rtmp2sink sync=false` | 41.1 ms |

A syncing sink holds each buffer until its running time plus the pipeline's configured latency, and `processing-deadline` alone contributes 20 ms of that by default. A live source already paces the pipeline, so the clock has nothing left to contribute — it only delays. Pinning `video/x-raw,format=I420` before the encoder is worth another millisecond or so at the median, and stops the encoder inheriting 4:4:4 from the RGB the AI branch works in.

Hardware encoding is *not* automatically the low-latency choice here — VideoToolbox measured slower than x264 at ultrafast:

| Encoder | p50 |
| --- | --- |
| `x264enc tune=zerolatency speed-preset=ultrafast` | 40.5 ms |
| `vtenc_h264 realtime=true` | 46.5 ms |

Useful knobs, all environment variables:

| Variable | Default | |
| --- | --- | --- |
| `BRAIDPIPE_RTMP_DURATION` | `30` | seconds to measure |
| `BRAIDPIPE_RTMP_ENCODER` | `x264` | or `vtenc` |
| `BRAIDPIPE_RTMP_TUNED` | `1` | set `0` to reproduce the defaults row above |
| `BRAIDPIPE_RTMP_SINK` | | replace the sink string outright |
| `BRAIDPIPE_STAMP_BUSY_MS` | `0` | give the worker a fake per-frame cost |

That last one doubles as a failover test with numbers attached. A worker held 60 ms per frame is well past the 50 ms budget, and the run shows exactly what the design promises:

```
frames received : 402
  with barcode  : 0
  unreadable    : 402  (pre-keyframe, or passthrough frames)

worker->received: no samples
arrival interval  min= 16.30  p50= 33.33  p90= 35.49  p99= 37.87  max= 38.18  (ms, n=401)
```

Every stamped frame is gone — the AI branch never made its deadline once — and the output still arrives at a 33.33 ms median, which is 30 fps exactly. Nothing downstream could tell the worker had failed.

## Monitoring

The daemon serves Prometheus metrics on `http://127.0.0.1:9184/metrics` (change with `--metrics-port`, `0` disables). Most of the numbers were already being measured for the failover logic — the endpoint makes them visible: every ack carries the worker's processing time, the relay times every roundtrip, the bridge counts the failure streak. The instrumentation adds nothing to the frame path beyond lock-free counter increments.

What you get, by the question it answers:

| Question | Metrics |
| --- | --- |
| Is the AI output live *right now*? | `braidpipe_last_ai_frame_timestamp_seconds`, `braidpipe_active_branch`, `braidpipe_worker_up` |
| What's our availability? | `braidpipe_branch_seconds_total{branch}`, `braidpipe_branch_switches_total{direction}` |
| Is trouble coming? | `braidpipe_queue_depth`, `braidpipe_shm_slots_occupied`, `braidpipe_failure_streak`, `braidpipe_stale_acks_total`, roundtrip p99 vs `braidpipe_relay_deadline_seconds` |
| How fast is the worker? | `braidpipe_roundtrip_seconds` and `braidpipe_worker_processing_seconds` histograms |
| Is the stream healthy? | `braidpipe_input_fps`, `braidpipe_pts_discontinuities_total`, `braidpipe_av_skew_seconds`, `braidpipe_keyframes_total`, `braidpipe_bus_messages_total` |
| What's on the wire? | `braidpipe_sink_bytes_total`, `braidpipe_output_frames_total`, and full `braidpipe_srt_*` transport stats (RTT, loss, retransmits) when the pipeline has an SRT element |
| Are the processes healthy? | `process_*` for the daemon, `braidpipe_worker_cpu_seconds_total` / `braidpipe_worker_resident_memory_bytes` for the worker, `braidpipe_worker_exits_total` |
| Is the GPU doing the work? | `braidpipe_gpu_utilization_percent` and `braidpipe_gpu_memory_used_bytes` (machine-wide, sampled every 5s), plus `braidpipe_gpu_encoder_utilization_percent` / `braidpipe_gpu_decoder_utilization_percent` for the dedicated NVENC/NVDEC blocks on NVIDIA. Series exist only where the platform exposes the counter — absent means unmeasurable, not idle |

The A/V skew gauge is the audio-sync claim from [Audio passthrough](streaming.md#audio-passthrough), continuously verified in production: both streams' running time at the muxer, subtracted.

A ready-made Grafana stack lives in [monitoring/](../monitoring/):

```bash
cd monitoring && docker compose up -d
# Grafana: http://localhost:3000  (provisioned dashboard, no login)
# Prometheus: http://localhost:9090
```

It scrapes once a second and ships a provisioned dashboard (bandwidth, frame rates, latency percentiles against the deadline, branch state timeline, drops, backpressure, A/V skew, process usage, SRT transport) plus [alert rules](../monitoring/prometheus/alerts.yml) for the conditions worth paging on: daemon unreachable, worker down, stuck in passthrough, output dark, >5% deadline misses, stale AI frames, A/V skew over 100 ms.

### Shutdown and stale panels

A metric that stops being scraped keeps its last value on screen, so a daemon that dies looks identical to one that is healthy and idle. Three things prevent that misreading:

- **`braidpipe_up`** goes to `0` the moment a shutdown signal arrives, and the endpoint stays open for `--metrics-drain-ms` (default 2000, ≥ one scrape interval) so the down state is actually recorded before the process exits.
- **The dashboard gates on the scrape**, not on the daemon's own metrics. The Daemon panel reads `up{job="braidpipe"}` — Prometheus writes that even when the target is gone — and the availability, worker, and fps panels are conditioned on it, so they blank or read DOWN rather than freezing on the last healthy sample. `BraidpipeDown` alerts on the same series, and is the only rule that can fire when the daemon no longer exists to report anything.
- **Shutdown cannot hang.** Ctrl+C reaches the worker only when both share a terminal, so the daemon SIGTERMs it explicitly (SIGKILL after 2 s). Pipeline teardown is bounded by a guard that force-exits `--metrics-drain-ms` + 4 s after the signal: on macOS, `set_state(NULL)` on the GL video sink deadlocks against its own GL thread, and a daemon wedged there is the worst case of all — still alive, still serving, still reporting the last healthy sample.

## Troubleshooting

**No output at all, or garbled/duplicated frames.** Look for leftover daemons first — this is by far the most common cause. Old instances share the same socket paths and output URL, and they will happily fight over both:

```bash
pgrep -fl braidpipe
pkill -9 -f 'target/release/braidpipe'
```

Symptoms of cross-talk include `Discarded stale Python ack` with wildly out-of-range frame IDs, and RTMP sinks failing to connect because another publisher holds the URL.

**The pipeline reaches PLAYING and then nothing happens** — no frames, no branch switch, near-zero CPU, no error on the bus. With a live source this is almost always a latency negotiation failure inside the pipeline. The appsrc declares `max-latency=-1` to prevent it: an appsrc otherwise reports zero maximum latency while a live source reports a minimum of one frame period, the selector aggregates min > max, and the pipeline stalls silently. The two AI-side links (`tee` → appsink, and appsrc → selector) also carry their own `videoconvert` so the appsink's RGB requirement is never forced back through the source. If you see it again, the signature is in the GStreamer logs:

```
input-selector <sel:src>: minimum latency bigger than maximum latency
```

Raising `GST_DEBUG` to find it is reasonable, but redirect to a file you are willing to lose — that one error repeats per latency query and can produce gigabytes per minute.

**Output goes dark when the AI branch is selected.** Check the log for pipeline errors — the bus watcher surfaces asynchronous failures that would otherwise be silent. Then confirm your sink can accept the AI branch's caps, which are `video/x-raw,format=RGB` at the configured resolution.

**Worker hangs at startup without attaching.** The handshake reply is sent from the daemon's ack loop, which runs while frames are flowing — a worker attaches within a frame or two of the first one. If it never does, the daemon isn't receiving frames (check the source), or the two sides disagree on the socket paths.

**Worker exits with `OSError: [Errno 55/105] No buffer space available`.** The ack socket buffer filled up. The bundled worker catches this; a custom worker must too.

**`Python failed to respond within target deadline` repeating.** Inference is slower than 1.5 frame periods. Lower `--fps`, shrink the resolution, or process every Nth frame.

**Overlay colours look wrong.** Frames are RGB; OpenCV colour tuples are BGR. Swap the outer channels.
