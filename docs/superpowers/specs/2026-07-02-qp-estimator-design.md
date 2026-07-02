# QP estimator for transcode

## Problem

Users pick a QP value for the encode modal without knowing how it trades off
size vs quality for the specific file. Want a quick, in-modal preview: encode
a short sample from the middle of the movie at a few QP values, report size
and quality per value, and suggest one.

## Trigger

Button ("📈 Estimate") next to the Quality (QP) field in the existing encode
modal (`templates/_modals.html`). Uses all other settings currently selected
in the modal (codec, gpu, preset, denoise, crop, width, format) — only QP is
swept.

## Backend flow

New async task, independent of the existing encode job queue (`_encode_jobs`
/ `_encode_queue_list`) — runs immediately, does not wait behind queued
encodes. Global lock: only one estimate may run at a time (409 if one is
already in flight) since it competes for the same GPU/CPU as any real encode
that might be running.

### Shared probe helper

Extract the ffprobe block currently inline in `_run_encode_job` (duration,
bit depth, HDR/color primaries/transfer/space/range, source fps, video/audio
stream lists) into `_probe_video(path: Path) -> dict`. Both `_run_encode_job`
and the new estimator call it — avoids duplicating ~60 lines.

### Steps

1. `_probe_video(source_path)`.
2. Compute sample window: `start = max(0, duration/2 - 30)`,
   `length = min(60, duration)`. If duration unknown/probe failed, fall back
   to `start=0, length=60`.
3. Extract sample via stream copy, video only, no re-encode:
   `ffmpeg -y -ss {start} -i {source} -t {length} -map 0:v:0 -c copy {tmp}/sample.mkv`
   into `tempfile.mkdtemp(prefix="mediastat-estimate-")`. Cleaned up in a
   `finally` block regardless of outcome.
4. If `config["crop"]` is set, run the existing `_detect_crop(sample_path, length)`
   against the *sample* (fast — 60s, not the full movie) rather than the
   source.
5. For QP in `[16, 18, 20, 22]`, sequentially:
   - Build cmd via existing `_build_ffmpeg_cmd(sample_path, out_path, {**config, "qp": qp}, hw, bit_depth, is_hdr, cp, tc, cs, cr, crop_filter=crop_filter, a_streams=[], s_streams=[], source_fps=source_fps)`.
   - Run it, time wall-clock duration, stat output size.
   - Compute SSIM: `ffmpeg -i {encoded} -i {sample} -lavfi "[1:v]{ref_filters}[ref];[0:v][ref]ssim" -f null -`
     where `ref_filters` mirrors whatever crop/scale was applied to the
     encode output (`crop=...` / `scale=W:-2`), so both inputs are the same
     dimensions. If neither crop nor width was set, filter is just
     `[0:v][1:v]ssim`. Parse the `All:` value from stderr.
   - Emit SSE `qp_done` event: `{qp, bytes, pct_of_sample, ssim, seconds}`.
6. Suggested QP = highest tested QP (of the 4) with `ssim >= 0.98` (most
   compression while staying near-lossless). If none reach 0.98, suggest
   QP 16 and set `warning: "even QP16 SSIM below target — this content may
   compress poorly, or check source quality"`.
7. Final SSE `done` event includes all 4 results plus, per result, an
   extrapolated full-file estimate: `source_file_size * (encoded_bytes / sample_raw_bytes)`,
   `sample_raw_bytes` = size of the stream-copied sample from step 3.
8. On any exception, emit SSE `error` event with a message and clean up.

### Endpoints

- `POST /encode/estimate?path=...` — body: same shape as `_make_encode_config`
  minus `qp` (codec/gpu/preset/denoise/crop/width/format/lang). Validates
  path same as `/encode`, returns `{estimate_id}` and starts the background
  task. 409 if an estimate is already running.
- `GET /encode/estimate/events?id=...` — SSE stream, same subscriber-queue
  pattern as the existing `/encode/events`. Events: `probing`, `extracting`,
  `qp_start{qp}`, `qp_done{qp,bytes,pct_of_sample,ssim,seconds}`,
  `done{suggested_qp,warning?,results:[...]}`, `error{message}`.
- No DB persistence — estimate state lives in memory only, same lifetime as
  the request/SSE connection. If the server restarts mid-estimate, it's just
  lost (same UX as losing an open modal).

## UI

- New button in `templates/_modals.html` encode modal, next to the QP field.
- Click opens an inline panel below the existing field grid — table with 4
  rows (QP 16/18/20/22), each showing a spinner until its `qp_done` event
  arrives, then: sample size, % of sample raw size, SSIM, encode time.
- On `done`: highlight the suggested row, show a "Use QP {n}" button that
  sets `#enc-qp` to the suggested value and collapses the panel. If a
  `warning` was returned, show it under the table.
- Button disabled (with tooltip) while an estimate is already running
  (tracked via the 409 / a local `estimating` flag).

## Edge cases

- Source shorter than 60s: sample = whole file, `start=0`.
- ffmpeg missing: same check as `/encode` — reject with a clear error before
  starting.
- Crop detection finds nothing: proceed without crop filter, same as real
  encode path.
- HDR/10-bit sources: `_build_ffmpeg_cmd` already branches on `is_hdr`/`bit_depth`,
  reused as-is.
- Temp dir always removed, even on error/exception (`finally`).
- Concurrent estimate requests: second request gets 409 immediately, no
  queuing.

## Out of scope

- No user-facing control over SSIM threshold or QP set — both are code
  constants (`0.98`, `[16,18,20,22]`).
- No VMAF (ffmpeg build has no `libvmaf` filter, confirmed via
  `ffmpeg -filters`).
- Does not touch the real encode job queue, DB schema, or `EncodeJob` class.
