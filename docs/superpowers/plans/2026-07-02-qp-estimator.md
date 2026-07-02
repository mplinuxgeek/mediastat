# QP Estimator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "📈 Estimate" button to the encode modal that extracts a 60s
sample from the middle of the selected file, transcodes it at QP 16/18/20/22
using the modal's other settings, and reports size/quality/speed per QP plus
a suggested value.

**Architecture:** Pure/testable logic (sample window math, SSIM parsing, SSIM
reference-filter construction, QP suggestion) lives in a new `encode_estimate.py`
module, mirroring the existing `cropdetect_utils.py` / `encode_output_resolution.py`
/ `encode_stream_selection.py` pattern. Async orchestration (spawning ffmpeg,
managing temp files, broadcasting progress) lives in `main.py` next to the
existing `_run_encode_job`, reusing `_build_ffmpeg_cmd`, `_detect_crop`, and a
newly-extracted `_probe_video()` helper (refactored out of `_run_encode_job`
so both code paths share one probe implementation). State broadcasts to the
browser as one full JSON snapshot per change over SSE, mirroring the existing
`/encode/events` pattern but on its own endpoint pair, with a single global
"only one estimate at a time" lock (409 if busy).

**Tech Stack:** FastAPI, ffmpeg (stream copy for sample extraction, existing
QP encoders, built-in `ssim` filter — no libvmaf available in this ffmpeg
build, confirmed via `ffmpeg -filters`), vanilla JS + SSE on the frontend,
`unittest`/`unittest.mock` for tests (existing convention in this repo — no
pytest config, no `httpx`/`TestClient` dependency present).

## Global Constraints

- No new third-party dependencies (`requirements.txt` unchanged) — everything
  needed (`tempfile`, `shutil`, `re`, `asyncio`, `json`, `time`) is already
  imported in `main.py` or is stdlib.
- Follow existing test style: `unittest.IsolatedAsyncioTestCase` for async
  code, mock `asyncio.create_subprocess_exec`, set `MEDIA_ROOT`/`CONFIG_PATH`/
  `DB_PATH` env vars before `import main` (see `test_encode_job_probe_failure.py`).
- SSIM threshold `0.98` and QP set `(16, 18, 20, 22)` and sample length `60`
  are code constants — no user-facing config for them (per spec, out of scope).
- Do NOT run `docker build`/`docker run`/restart any container as part of
  verifying this work — there is a live container currently mid-encode on
  this host. All verification is via `python3 -m unittest`, not the running
  container.
- New estimate work must not touch `_encode_jobs`, `_encode_queue_list`, or
  the `EncodeJob` DB schema — it is fully independent, in-memory only, no
  persistence.

---

## File Structure

- **Create** `encode_estimate.py` — pure helper functions: `sample_window`,
  `parse_ssim`, `build_ssim_ref_filter`, `suggest_qp`.
- **Create** `test_encode_estimate.py` — unit tests for the above, no mocking
  needed (pure functions).
- **Modify** `main.py`:
  - Extract `_probe_video()` helper from `_run_encode_job` (refactor, no
    behavior change).
  - Add estimate state/SSE plumbing (`_estimate_state`, `_estimate_subscribers`,
    `_broadcast_estimate`).
  - Add `_run_estimate()` orchestration function.
  - Add `POST /encode/estimate` and `GET /encode/estimate/events` routes.
- **Create** `test_encode_estimate_job.py` — tests for `_probe_video` and
  `_run_estimate` using the existing mock-subprocess pattern.
- **Modify** `Dockerfile`, `ha-addon/Dockerfile` — add
  `COPY encode_estimate.py .`.
- **Modify** `test_container_packaging.py` — assert both Dockerfiles copy
  `encode_estimate.py`.
- **Modify** `templates/_modals.html` — add Estimate button + results panel
  markup to the encode modal.
- **Modify** `static/app.js` — wire the button: start estimate, subscribe to
  SSE, render results table, "Use QP" button.

---

### Task 1: `encode_estimate.py` — sample window math

**Files:**
- Create: `encode_estimate.py`
- Test: `test_encode_estimate.py`

**Interfaces:**
- Produces: `sample_window(duration: float | None, sample_length: int = 60) -> tuple[float, float]`
  — returns `(start_seconds, length_seconds)`. If `duration` is `None`/`<= 0`,
  returns `(0.0, float(sample_length))`. Otherwise clamps `length` to
  `min(sample_length, duration)` and centers the window at the file's midpoint.

- [ ] **Step 1: Write the failing tests**

Create `test_encode_estimate.py`:

```python
import unittest

from encode_estimate import sample_window


class SampleWindowTests(unittest.TestCase):
    def test_long_file_centers_60s_window_on_midpoint(self):
        start, length = sample_window(3600.0, 60)
        self.assertEqual(length, 60.0)
        self.assertEqual(start, 1770.0)  # 1800 - 30

    def test_file_shorter_than_sample_length_uses_whole_file(self):
        start, length = sample_window(40.0, 60)
        self.assertEqual(start, 0.0)
        self.assertEqual(length, 40.0)

    def test_unknown_duration_falls_back_to_zero_start(self):
        start, length = sample_window(None, 60)
        self.assertEqual(start, 0.0)
        self.assertEqual(length, 60.0)

    def test_zero_duration_falls_back_to_zero_start(self):
        start, length = sample_window(0.0, 60)
        self.assertEqual(start, 0.0)
        self.assertEqual(length, 60.0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'encode_estimate'`

- [ ] **Step 3: Write minimal implementation**

Create `encode_estimate.py`:

```python
def sample_window(duration: float | None, sample_length: int = 60) -> tuple[float, float]:
    """Return (start_seconds, length_seconds) for a sample centered on the
    midpoint of a file `duration` seconds long. Falls back to (0, sample_length)
    when duration is unknown."""
    if not duration or duration <= 0:
        return 0.0, float(sample_length)
    length = min(float(sample_length), duration)
    start = max(0.0, (duration / 2.0) - (length / 2.0))
    return start, length
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: `OK` (4 tests)

- [ ] **Step 5: Commit**

```bash
git add encode_estimate.py test_encode_estimate.py
git commit -m "feat: add sample_window for QP estimator"
```

---

### Task 2: `encode_estimate.py` — SSIM parsing

**Files:**
- Modify: `encode_estimate.py`
- Modify: `test_encode_estimate.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `parse_ssim(stderr_text: str) -> float | None` — parses ffmpeg's
  `ssim` filter stderr output (format: `... All:0.987654 (22.084921)`), returns
  the last `All:` value found as a float, or `None` if no match.

- [ ] **Step 1: Write the failing tests**

Append to `test_encode_estimate.py`:

```python
from encode_estimate import parse_ssim


class ParseSsimTests(unittest.TestCase):
    def test_extracts_all_value_from_ffmpeg_ssim_line(self):
        stderr = (
            "frame=  100 fps=0.0 q=-0.0 Lsize=N/A time=00:00:04.00 bitrate=N/A\n"
            "[Parsed_ssim_0 @ 0x5],, SSIM Y:0.991234 U:0.995123 V:0.996001 "
            "All:0.987654 (19.023456)\n"
        )
        self.assertEqual(parse_ssim(stderr), 0.987654)

    def test_returns_last_match_when_multiple_lines_present(self):
        stderr = "All:0.900000 (1.0)\nAll:0.950000 (2.0)\n"
        self.assertEqual(parse_ssim(stderr), 0.95)

    def test_returns_none_when_no_match(self):
        self.assertIsNone(parse_ssim("some unrelated ffmpeg output\n"))

    def test_returns_none_on_empty_string(self):
        self.assertIsNone(parse_ssim(""))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: FAIL — `ImportError: cannot import name 'parse_ssim'`

- [ ] **Step 3: Write minimal implementation**

Add to `encode_estimate.py`:

```python
import re

_SSIM_ALL_RE = re.compile(r"All:([0-9.]+)")


def parse_ssim(stderr_text: str) -> float | None:
    """Extract the last 'All:<value>' SSIM score from ffmpeg's ssim filter
    stderr output. Returns None if no match is found."""
    matches = _SSIM_ALL_RE.findall(stderr_text)
    if not matches:
        return None
    try:
        return float(matches[-1])
    except ValueError:
        return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: `OK` (8 tests)

- [ ] **Step 5: Commit**

```bash
git add encode_estimate.py test_encode_estimate.py
git commit -m "feat: add parse_ssim for QP estimator"
```

---

### Task 3: `encode_estimate.py` — SSIM reference filter builder

**Files:**
- Modify: `encode_estimate.py`
- Modify: `test_encode_estimate.py`

**Interfaces:**
- Consumes: nothing from prior tasks directly (independent pure function).
- Produces: `build_ssim_ref_filter(crop_filter: str | None, width: int | None) -> str`
  — returns the `-lavfi` filter_complex string for comparing an encoded output
  (`[0:v]`) against the raw sample (`[1:v]`), applying the same crop/scale to
  the reference input that was applied to the encode, so both frames are the
  same dimensions before SSIM runs.

- [ ] **Step 1: Write the failing tests**

Append to `test_encode_estimate.py`:

```python
from encode_estimate import build_ssim_ref_filter


class BuildSsimRefFilterTests(unittest.TestCase):
    def test_no_crop_no_width_compares_directly(self):
        self.assertEqual(build_ssim_ref_filter(None, None), "[0:v][1:v]ssim")

    def test_crop_only_applies_crop_to_reference(self):
        self.assertEqual(
            build_ssim_ref_filter("1920:800:0:140", None),
            "[1:v]crop=1920:800:0:140[ref];[0:v][ref]ssim",
        )

    def test_width_only_applies_scale_to_reference(self):
        self.assertEqual(
            build_ssim_ref_filter(None, 1280),
            "[1:v]scale=1280:-2[ref];[0:v][ref]ssim",
        )

    def test_crop_and_width_apply_both_in_order(self):
        self.assertEqual(
            build_ssim_ref_filter("1920:800:0:140", 1280),
            "[1:v]crop=1920:800:0:140,scale=1280:-2[ref];[0:v][ref]ssim",
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_ssim_ref_filter'`

- [ ] **Step 3: Write minimal implementation**

Add to `encode_estimate.py`:

```python
def build_ssim_ref_filter(crop_filter: str | None, width: int | None) -> str:
    """Build the ffmpeg -lavfi filter comparing encoded output [0:v] to the
    raw sample [1:v], applying to the reference the same crop/scale that was
    applied to the encode so both frames have matching dimensions."""
    ref_ops = []
    if crop_filter:
        ref_ops.append(f"crop={crop_filter}")
    if width:
        ref_ops.append(f"scale={int(width)}:-2")
    if not ref_ops:
        return "[0:v][1:v]ssim"
    return f"[1:v]{','.join(ref_ops)}[ref];[0:v][ref]ssim"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: `OK` (12 tests)

- [ ] **Step 5: Commit**

```bash
git add encode_estimate.py test_encode_estimate.py
git commit -m "feat: add build_ssim_ref_filter for QP estimator"
```

---

### Task 4: `encode_estimate.py` — QP suggestion

**Files:**
- Modify: `encode_estimate.py`
- Modify: `test_encode_estimate.py`

**Interfaces:**
- Consumes: result dicts shaped `{"qp": int, "ssim": float | None, ...}` (the
  shape `_run_estimate` will produce in Task 6 — only `qp` and `ssim` keys
  are read here).
- Produces: `suggest_qp(results: list[dict], threshold: float = 0.98) -> tuple[int, str | None]`
  — `(suggested_qp, warning_or_None)`. Suggested QP is the **highest** QP
  among results whose `ssim >= threshold` (most compression while staying
  near-lossless). If none qualify, returns the **lowest** tested QP plus a
  warning string.

- [ ] **Step 1: Write the failing tests**

Append to `test_encode_estimate.py`:

```python
from encode_estimate import suggest_qp


class SuggestQpTests(unittest.TestCase):
    def test_picks_highest_qp_meeting_threshold(self):
        results = [
            {"qp": 16, "ssim": 0.995},
            {"qp": 18, "ssim": 0.990},
            {"qp": 20, "ssim": 0.981},
            {"qp": 22, "ssim": 0.960},
        ]
        qp, warning = suggest_qp(results, threshold=0.98)
        self.assertEqual(qp, 20)
        self.assertIsNone(warning)

    def test_falls_back_to_lowest_qp_with_warning_when_none_meet_threshold(self):
        results = [
            {"qp": 16, "ssim": 0.970},
            {"qp": 18, "ssim": 0.950},
            {"qp": 20, "ssim": 0.930},
            {"qp": 22, "ssim": 0.900},
        ]
        qp, warning = suggest_qp(results, threshold=0.98)
        self.assertEqual(qp, 16)
        self.assertIsNotNone(warning)
        self.assertIn("QP16", warning)

    def test_treats_missing_ssim_as_not_qualifying(self):
        results = [
            {"qp": 16, "ssim": None},
            {"qp": 18, "ssim": 0.99},
            {"qp": 20, "ssim": None},
        ]
        qp, warning = suggest_qp(results, threshold=0.98)
        self.assertEqual(qp, 18)
        self.assertIsNone(warning)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: FAIL — `ImportError: cannot import name 'suggest_qp'`

- [ ] **Step 3: Write minimal implementation**

Add to `encode_estimate.py`:

```python
def suggest_qp(results: list[dict], threshold: float = 0.98) -> tuple[int, str | None]:
    """Pick the highest tested QP whose SSIM meets `threshold` (most
    compression while staying near-lossless). Falls back to the lowest
    tested QP with a warning if none qualify."""
    passing = [r for r in results if r.get("ssim") is not None and r["ssim"] >= threshold]
    if passing:
        return max(passing, key=lambda r: r["qp"])["qp"], None
    lowest = min(results, key=lambda r: r["qp"])
    return lowest["qp"], (
        f"even QP{lowest['qp']} SSIM ({lowest.get('ssim')}) is below the "
        f"{threshold} target — this content may compress poorly, or check source quality"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest test_encode_estimate.py -v`
Expected: `OK` (15 tests)

- [ ] **Step 5: Commit**

```bash
git add encode_estimate.py test_encode_estimate.py
git commit -m "feat: add suggest_qp for QP estimator"
```

---

### Task 5: `main.py` — extract shared `_probe_video()` helper

**Files:**
- Modify: `main.py:3037-3116` (inside `_run_encode_job`)
- Test: `test_encode_estimate_job.py` (new)

**Interfaces:**
- Produces: `async def _probe_video(path: Path) -> dict` returning:
  ```python
  {
      "bit_depth": int | None, "is_hdr": bool, "is_dv": bool,
      "duration_sec": float | None, "source_fps": float | None,
      "cp": str, "tc": str, "cs": str, "cr": str,
      "vst": dict, "a_streams": list[dict], "s_streams": list[dict],
      "media_info": {
          "video_codec": str, "width": int, "height": int,
          "audio_codec": str, "audio_count": int, "sub_count": int,
      },
  }
  ```
  On ffprobe failure, returns the same shape with defaults (`None`/`False`/
  `""`/`[]`/`0`) and logs a warning — never raises.
- Consumes (from existing code): `_PROBE_SEM` (module-level `asyncio.Semaphore`
  at `main.py:89`), `log` (module logger).

This is a pure refactor: `_run_encode_job` currently inlines this probe logic
at `main.py:3049-3115`. Move it into `_probe_video`, call it from
`_run_encode_job`, and keep `_run_encode_job`'s job-specific side effects
(setting `job.input_media_info`, the DV warning log referencing `job_id`,
calling `_notify_encode(job_id)`) at the call site, not inside the helper.

- [ ] **Step 1: Write the failing test**

Create `test_encode_estimate_job.py`:

```python
import asyncio
import json
import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


def _fake_probe_json():
    return json.dumps({
        "streams": [
            {
                "codec_type": "video", "codec_name": "hevc",
                "width": 1920, "height": 800, "pix_fmt": "yuv420p10le",
                "color_primaries": "bt2020", "color_transfer": "smpte2084",
                "transfer_characteristics": "smpte2084",
                "color_space": "bt2020nc", "color_range": "tv",
                "r_frame_rate": "24000/1001", "side_data_list": [],
            },
            {"codec_type": "audio", "codec_name": "eac3", "tags": {"language": "eng"}},
        ],
        "format": {"duration": "7200.5"},
    }).encode()


class ProbeVideoTests(unittest.IsolatedAsyncioTestCase):
    async def test_parses_hdr_bit_depth_duration_and_media_info(self):
        async def _fake_exec(*args, **kwargs):
            proc = unittest.mock.AsyncMock()
            proc.communicate = unittest.mock.AsyncMock(return_value=(_fake_probe_json(), b""))
            return proc

        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_fake_exec):
            info = await main._probe_video(main.Path("/tmp/does-not-exist.mkv"))

        self.assertEqual(info["bit_depth"], 10)
        self.assertTrue(info["is_hdr"])
        self.assertFalse(info["is_dv"])
        self.assertEqual(info["duration_sec"], 7200.5)
        self.assertAlmostEqual(info["source_fps"], 23.976, places=2)
        self.assertEqual(info["media_info"]["video_codec"], "hevc")
        self.assertEqual(info["media_info"]["audio_count"], 1)

    async def test_probe_failure_returns_safe_defaults_without_raising(self):
        async def _boom(*args, **kwargs):
            raise OSError("ffprobe not found")

        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_boom):
            info = await main._probe_video(main.Path("/tmp/does-not-exist.mkv"))

        self.assertIsNone(info["bit_depth"])
        self.assertFalse(info["is_hdr"])
        self.assertIsNone(info["duration_sec"])
        self.assertEqual(info["a_streams"], [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_encode_estimate_job.py -v`
Expected: FAIL — `AttributeError: module 'main' has no attribute '_probe_video'`

- [ ] **Step 3: Extract `_probe_video` and update `_run_encode_job`**

In `main.py`, insert this new function directly above `_run_encode_job`
(currently at `main.py:3037`):

```python
async def _probe_video(path: Path) -> dict:
    """ffprobe a media file for encode-relevant metadata. Never raises —
    returns safe defaults and logs a warning on failure."""
    result = {
        "bit_depth": None, "is_hdr": False, "is_dv": False,
        "duration_sec": None, "source_fps": None,
        "cp": "", "tc": "", "cs": "", "cr": "",
        "vst": {}, "a_streams": [], "s_streams": [],
        "media_info": {
            "video_codec": "", "width": 0, "height": 0,
            "audio_codec": "", "audio_count": 0, "sub_count": 0,
        },
    }
    try:
        async with _PROBE_SEM:
            p = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "error",
                "-analyzeduration", "100M", "-probesize", "100M",
                "-show_streams", "-show_format",
                "-of", "json", str(path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(p.communicate(), timeout=30)
        probe = json.loads(stdout)
        streams = probe.get("streams", [])
        v_streams = [s for s in streams if s.get("codec_type") == "video"]
        a_streams = [s for s in streams if s.get("codec_type") == "audio"]
        s_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
        vst = v_streams[0] if v_streams else {}
        ast = a_streams[0] if a_streams else {}
        bps = vst.get("bits_per_raw_sample")
        bit_depth = None
        if bps:
            bit_depth = int(bps)
        elif "pix_fmt" in vst:
            m = re.search(r"(\d+)(?:le|be)$", vst["pix_fmt"])
            if m:
                bit_depth = int(m.group(1))
        cp = vst.get("color_primaries", "")
        tc = vst.get("transfer_characteristics", "")
        cs = vst.get("color_space", "")
        cr = vst.get("color_range", "")
        is_hdr = cp == "bt2020" or tc in ("smpte2084", "arib-std-b67")
        is_dv = False
        for sd in vst.get("side_data_list", []):
            if "dovi" in sd.get("side_data_type", "").lower():
                is_dv = True
                break
        duration_sec = None
        try:
            duration_sec = float(probe.get("format", {}).get("duration") or 0) or None
        except (TypeError, ValueError):
            pass
        source_fps = None
        try:
            _fn, _fd = (vst.get("r_frame_rate") or "0/1").split("/")
            source_fps = round(int(_fn) / max(int(_fd), 1), 3) or None
        except (ValueError, ZeroDivisionError):
            pass
        result.update({
            "bit_depth": bit_depth, "is_hdr": is_hdr, "is_dv": is_dv,
            "duration_sec": duration_sec, "source_fps": source_fps,
            "cp": cp, "tc": tc, "cs": cs, "cr": cr,
            "vst": vst, "a_streams": a_streams, "s_streams": s_streams,
            "media_info": {
                "video_codec": vst.get("codec_name", ""),
                "width": vst.get("width", 0),
                "height": vst.get("height", 0),
                "audio_codec": ast.get("codec_name", ""),
                "audio_count": len(a_streams),
                "sub_count": len(s_streams),
            },
        })
    except Exception as e:
        log.warning("Probe %s: could not get stream info: %s", path, e)
    return result
```

Then replace the inline probe block in `_run_encode_job` (`main.py:3049-3115`,
everything from `# Probe all streams...` through the `except Exception as e:`
that logs `"could not get stream info"`) with:

```python
        info = await _probe_video(input_path)
        bit_depth    = info["bit_depth"]
        is_hdr       = info["is_hdr"]
        is_dv        = info["is_dv"]
        duration_sec = info["duration_sec"]
        source_fps   = info["source_fps"]
        cp, tc, cs, cr = info["cp"], info["tc"], info["cs"], info["cr"]
        vst        = info["vst"]
        a_streams  = info["a_streams"]
        s_streams  = info["s_streams"]
        job.input_media_info = info["media_info"]
        if is_dv:
            log.warning("Encode %s: Dolby Vision detected — DV RPU metadata cannot be "
                        "preserved through re-encoding; output will be HDR10/HLG", job_id[:8])
        _notify_encode(job_id)
```

Leave everything after this block (the crop-detection section starting
`crop_filter: Optional[str] = None`) untouched — it already reads `vst`,
`duration_sec`, `cp`, `tc`, `cs`, `cr`, `source_fps`, `a_streams`, `s_streams`
by those exact names, so no further changes are needed there.

- [ ] **Step 4: Run tests to verify everything passes**

Run: `python3 -m unittest test_encode_estimate_job.py -v`
Expected: `OK` (2 tests)

Run: `python3 -m unittest test_encode_job_probe_failure.py -v`
Expected: `OK` (1 test) — confirms the refactor didn't change `_run_encode_job`'s
observable behavior.

- [ ] **Step 5: Commit**

```bash
git add main.py test_encode_estimate_job.py
git commit -m "refactor: extract _probe_video helper from _run_encode_job"
```

---

### Task 6: `main.py` — estimate orchestration and routes

**Files:**
- Modify: `main.py` (imports near line 23, new code near line 3036, new
  routes near line 3783 alongside the existing `/encode` routes)
- Modify: `test_encode_estimate_job.py`

**Interfaces:**
- Consumes: `sample_window`, `parse_ssim`, `build_ssim_ref_filter`, `suggest_qp`
  from `encode_estimate` (Tasks 1-4); `_probe_video` (Task 5); existing
  `_build_ffmpeg_cmd`, `_detect_crop`, `_hw_accel_info`, `_make_encode_config`,
  `safe_path`, `DELETE_TOKEN`.
- Produces:
  - Module state: `_estimate_state: dict`, `_estimate_subscribers: list[asyncio.Queue]`.
  - `_broadcast_estimate() -> None`
  - `async def _run_estimate(path: str, config: dict) -> None`
  - `POST /encode/estimate?path=...` → `{"status": "started"}` or 409/403/404/400
  - `GET /encode/estimate/events` → SSE stream of `{"type": "state", "state": {...}}`

- [ ] **Step 1: Write the failing test**

Append to `test_encode_estimate_job.py`:

```python
class RunEstimateTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_sweep_reports_four_results_and_suggestion(self):
        calls = []

        async def _fake_exec(*args, **kwargs):
            calls.append(args)
            proc = unittest.mock.AsyncMock()
            argv = args
            if argv[0] == "ffprobe":
                proc.communicate = unittest.mock.AsyncMock(return_value=(_fake_probe_json(), b""))
                proc.returncode = 0
            elif "-lavfi" in argv:  # ssim pass
                proc.communicate = unittest.mock.AsyncMock(
                    return_value=(b"", b"SSIM Y:0.99 U:0.99 V:0.99 All:0.990000 (20.0)\n"))
                proc.returncode = 0
            else:  # sample extraction or qp encode
                out_path = argv[-1]
                with open(out_path, "wb") as f:
                    f.write(b"0" * 1000)
                proc.communicate = unittest.mock.AsyncMock(return_value=(b"", b""))
                proc.returncode = 0
            return proc

        main._hw_accel_info = {"qsv": False, "nvenc": False, "vaapi": False, "amd": False, "dri_device": ""}
        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_fake_exec), \
             unittest.mock.patch("main.Path.stat") as mock_stat, \
             unittest.mock.patch("main.Path.exists", return_value=True):
            mock_stat.return_value = unittest.mock.Mock(st_size=1000)
            await main._run_estimate("/tmp/does-not-exist.mkv", main._make_encode_config({}))

        self.assertEqual(main._estimate_state["status"], "done")
        self.assertEqual(len(main._estimate_state["results"]), 4)
        self.assertEqual([r["qp"] for r in main._estimate_state["results"]], [16, 18, 20, 22])
        self.assertEqual(main._estimate_state["suggested_qp"], 22)
        self.assertIsNone(main._estimate_state["error"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_encode_estimate_job.py -v`
Expected: FAIL — `AttributeError: module 'main' has no attribute '_run_estimate'`

- [ ] **Step 3: Implement**

Add the import near the other local-module imports (`main.py:23`, right after
`from encode_stream_selection import build_stream_maps`):

```python
from encode_estimate import sample_window, parse_ssim, build_ssim_ref_filter, suggest_qp
```

Insert this block immediately after the `_probe_video` function added in
Task 5 (still before `_run_encode_job`):

```python
_ESTIMATE_QPS = (16, 18, 20, 22)
_ESTIMATE_SSIM_THRESHOLD = 0.98
_ESTIMATE_SAMPLE_LENGTH = 60

_estimate_state: dict = {"status": "idle", "results": [], "suggested_qp": None,
                          "warning": None, "error": None, "current_qp": None}
_estimate_subscribers: list[asyncio.Queue] = []


def _broadcast_estimate() -> None:
    msg = json.dumps({"type": "state", "state": _estimate_state})
    dead = []
    for q in _estimate_subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            _estimate_subscribers.remove(q)
        except ValueError:
            pass


async def _run_estimate(path: str, config: dict) -> None:
    global _estimate_state
    tmp_dir: Optional[str] = None
    try:
        _estimate_state = {"status": "probing", "results": [], "suggested_qp": None,
                            "warning": None, "error": None, "current_qp": None}
        _broadcast_estimate()

        input_path = Path(path)
        info = await _probe_video(input_path)

        start, length = sample_window(info["duration_sec"], _ESTIMATE_SAMPLE_LENGTH)

        tmp_dir = tempfile.mkdtemp(prefix="mediastat-estimate-")
        sample_path = Path(tmp_dir) / "sample.mkv"

        _estimate_state["status"] = "extracting"
        _broadcast_estimate()

        extract_cmd = [
            "ffmpeg", "-y", "-ss", str(start), "-i", str(input_path),
            "-t", str(length), "-map", "0:v:0", "-c", "copy", str(sample_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *extract_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0 or not sample_path.exists():
            raise RuntimeError(f"sample extraction failed: {stderr.decode(errors='replace')[-500:]}")

        sample_raw_bytes = sample_path.stat().st_size
        source_size = input_path.stat().st_size

        crop_filter = None
        if config.get("crop"):
            crop_filter = await _detect_crop(sample_path, length)

        width = config.get("width")

        for qp in _ESTIMATE_QPS:
            _estimate_state["status"] = "encoding"
            _estimate_state["current_qp"] = qp
            _broadcast_estimate()

            out_path = Path(tmp_dir) / f"qp{qp}.mkv"
            cmd, _encoder = _build_ffmpeg_cmd(
                str(sample_path), str(out_path), {**config, "qp": qp}, _hw_accel_info,
                info["bit_depth"], info["is_hdr"], info["cp"], info["tc"], info["cs"], info["cr"],
                crop_filter=crop_filter, a_streams=[], s_streams=[], source_fps=info["source_fps"],
            )
            t0 = time.time()
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            seconds = time.time() - t0
            if proc.returncode != 0 or not out_path.exists():
                raise RuntimeError(f"QP{qp} encode failed: {stderr.decode(errors='replace')[-500:]}")

            out_bytes = out_path.stat().st_size

            ssim_filter = build_ssim_ref_filter(crop_filter, width)
            ssim_cmd = [
                "ffmpeg", "-i", str(out_path), "-i", str(sample_path),
                "-lavfi", ssim_filter, "-f", "null", "-",
            ]
            proc = await asyncio.create_subprocess_exec(
                *ssim_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, ssim_stderr = await proc.communicate()
            ssim = parse_ssim(ssim_stderr.decode(errors="replace"))

            out_path.unlink(missing_ok=True)

            _estimate_state["results"].append({
                "qp": qp,
                "bytes": out_bytes,
                "pct_of_sample": round((out_bytes / sample_raw_bytes) * 100, 1) if sample_raw_bytes else None,
                "ssim": ssim,
                "seconds": round(seconds, 1),
                "estimated_full_bytes": int(source_size * (out_bytes / sample_raw_bytes)) if sample_raw_bytes else None,
            })
            _broadcast_estimate()

        suggested_qp, warning = suggest_qp(_estimate_state["results"], _ESTIMATE_SSIM_THRESHOLD)
        _estimate_state["status"] = "done"
        _estimate_state["suggested_qp"] = suggested_qp
        _estimate_state["warning"] = warning
        _broadcast_estimate()
    except Exception as e:
        _estimate_state["status"] = "error"
        _estimate_state["error"] = str(e)
        _broadcast_estimate()
        log.warning("Estimate failed: %s", e)
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
```

Add the two routes in `main.py` right after the existing `/encode/events`
route (after `main.py:3742`, before `@app.post("/encode")`):

```python
@app.post("/encode/estimate")
async def start_estimate(request: Request, path: str = Query(...)):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    if _estimate_state.get("status") in ("probing", "extracting", "encoding"):
        raise HTTPException(status_code=409, detail="An estimate is already running")
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    if not shutil.which("ffmpeg"):
        raise HTTPException(status_code=400, detail="ffmpeg not found in PATH")
    try:
        body = await request.json()
    except Exception:
        body = {}
    config = _make_encode_config({**body, "qp": 18})
    asyncio.create_task(_run_estimate(str(file_path), config))
    return {"status": "started"}


@app.get("/encode/estimate/events")
async def estimate_events(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    _estimate_subscribers.append(queue)
    init_msg = json.dumps({"type": "state", "state": _estimate_state})

    async def generate():
        try:
            yield f"data: {init_msg}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            try:
                _estimate_subscribers.remove(queue)
            except ValueError:
                pass

    return StreamingResponse(
        generate(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest test_encode_estimate_job.py -v`
Expected: `OK` (3 tests)

Run full existing suite to confirm no regression:
`python3 -m unittest discover -p "test_*.py" -v`
Expected: all `OK`

- [ ] **Step 5: Commit**

```bash
git add main.py test_encode_estimate_job.py
git commit -m "feat: add QP estimate orchestration and endpoints"
```

---

### Task 7: Dockerfiles + packaging test

**Files:**
- Modify: `Dockerfile:24-26`
- Modify: `ha-addon/Dockerfile:42-44`
- Modify: `test_container_packaging.py`

**Interfaces:** none (static file content only).

- [ ] **Step 1: Write the failing tests**

Append to `test_container_packaging.py` (inside `ContainerPackagingTests`,
before the closing of the class):

```python
    def test_main_image_copies_encode_estimate(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("COPY encode_estimate.py .", dockerfile)

    def test_ha_addon_image_copies_encode_estimate(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("COPY encode_estimate.py .", dockerfile)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest test_container_packaging.py -v`
Expected: FAIL — 2 new tests fail (`AssertionError`)

- [ ] **Step 3: Update both Dockerfiles**

In `Dockerfile`, change:

```dockerfile
COPY main.py .
COPY cropdetect_utils.py .
COPY encode_output_resolution.py .
COPY encode_stream_selection.py .
```

to:

```dockerfile
COPY main.py .
COPY cropdetect_utils.py .
COPY encode_output_resolution.py .
COPY encode_stream_selection.py .
COPY encode_estimate.py .
```

In `ha-addon/Dockerfile`, change:

```dockerfile
COPY main.py .
COPY cropdetect_utils.py .
COPY encode_output_resolution.py .
COPY encode_stream_selection.py .
```

to:

```dockerfile
COPY main.py .
COPY cropdetect_utils.py .
COPY encode_output_resolution.py .
COPY encode_stream_selection.py .
COPY encode_estimate.py .
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest test_container_packaging.py -v`
Expected: `OK` (8 tests)

- [ ] **Step 5: Commit**

```bash
git add Dockerfile ha-addon/Dockerfile test_container_packaging.py
git commit -m "chore: package encode_estimate.py into both container images"
```

Note: do NOT run `docker build` to verify this — per the global constraint,
there's a live container mid-encode on this host. The packaging test above
is sufficient verification (matches the existing pattern for the other three
helper modules, which are also only verified this way).

---

### Task 8: Encode modal UI — Estimate button and results panel

**Files:**
- Modify: `templates/_modals.html:82-151` (inside `#encode-modal .encode-field-grid`)

**Interfaces:**
- Consumes: existing modal field IDs `enc-qp`, `enc-preset`, `enc-codec`,
  `enc-gpu`, `enc-format`, `enc-denoise`, `enc-crop`, `enc-lang`, `enc-width`,
  `encode-file-path` (all already present in `_modals.html`).
- Produces: new DOM IDs for Task 9's JS to bind to: `estimate-btn`,
  `estimate-panel`, `estimate-rows` (container for 4 result rows, one per QP
  with `data-qp` attribute), `estimate-summary`, `estimate-use-btn`.

No test for this task — it's static markup with no logic; Task 9 exercises
it through a manual browser check.

- [ ] **Step 1: Add the button next to the QP field**

In `templates/_modals.html`, change:

```html
                <span class="encode-field-label">Quality (QP)</span>
                <input id="enc-qp" type="number" min="10" max="51" value="18" class="modal-input" style="width:72px">
                <span class="encode-field-hint">16 ≈ near-lossless · 18–20 high quality · 20–22 good · lower = better</span>
```

to:

```html
                <span class="encode-field-label">Quality (QP)</span>
                <div style="display:flex;align-items:center;gap:8px">
                    <input id="enc-qp" type="number" min="10" max="51" value="18" class="modal-input" style="width:72px">
                    <button id="estimate-btn" type="button" class="btn" style="padding:4px 10px;font-size:var(--fs-sm)"
                            onclick="startEstimate()" title="Encode a 60s sample from the middle at QP 16/18/20/22 and suggest a value">📈 Estimate</button>
                </div>
                <span class="encode-field-hint">16 ≈ near-lossless · 18–20 high quality · 20–22 good · lower = better</span>
```

- [ ] **Step 2: Add the results panel**

In `templates/_modals.html`, immediately after the `.encode-field-grid` closing
`</div>` (right before `<input type="hidden" id="encode-file-path">`), add:

```html
            <div id="estimate-panel" style="display:none;margin-top:14px;padding:12px;border:1px solid var(--border);border-radius:6px">
                <div style="font-size:var(--fs-sm);color:var(--muted);margin-bottom:8px">Sampling 60s from the middle of the file…</div>
                <table style="width:100%;border-collapse:collapse;font-size:var(--fs-sm)">
                    <thead>
                        <tr style="text-align:left;color:var(--muted)">
                            <th style="padding:4px">QP</th>
                            <th style="padding:4px">Size</th>
                            <th style="padding:4px">% of raw</th>
                            <th style="padding:4px">SSIM</th>
                            <th style="padding:4px">Time</th>
                            <th style="padding:4px">Est. full file</th>
                        </tr>
                    </thead>
                    <tbody id="estimate-rows"></tbody>
                </table>
                <div id="estimate-summary" style="margin-top:10px;display:flex;align-items:center;gap:10px;flex-wrap:wrap"></div>
            </div>
            <input type="hidden" id="encode-file-path">
```

- [ ] **Step 3: Commit**

```bash
git add templates/_modals.html
git commit -m "feat: add QP estimate button and results panel to encode modal"
```

---

### Task 9: Wire the Estimate button in `app.js`

**Files:**
- Modify: `static/app.js` (near `startEncode`, after line 104)

**Interfaces:**
- Consumes: DOM IDs from Task 8 (`estimate-btn`, `estimate-panel`,
  `estimate-rows`, `estimate-summary`); existing `escHtml`, `showToast`,
  `DELETE_TOKEN`, `BASE_PATH`-aware `fetch`/`EventSource` (already patched
  globally, see `app.js:4-23`); existing modal field IDs listed in Task 8.
  SSE payload shape from `_run_estimate`/`_broadcast_estimate`:
  `{"type":"state","state":{status,results:[{qp,bytes,pct_of_sample,ssim,seconds,estimated_full_bytes}],suggested_qp,warning,error,current_qp}}`.
- Produces: `startEstimate()`, `_renderEstimateState(state)`,
  `_useEstimatedQp(qp)` — no other file depends on these.

No unit test (this is DOM/SSE wiring, not pure logic) — verify via manual
browser check in Step 3.

- [ ] **Step 1: Implement**

Add after `startEncode()` (after `app.js:104`, before the `// ── Batch encode`
comment):

```javascript
    // ── QP estimate ──────────────────────────────────────────────
    let _estimateSource = null;

    function _fmtBytes(n) {
        if (n == null) return '—';
        const units = ['B', 'KB', 'MB', 'GB'];
        let i = 0, v = n;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return v.toFixed(1) + ' ' + units[i];
    }

    function _renderEstimateState(state) {
        const rows = state.results.map(r => `
            <tr data-qp="${r.qp}" style="${state.suggested_qp === r.qp ? 'font-weight:600' : ''}">
                <td style="padding:4px">${r.qp}</td>
                <td style="padding:4px">${_fmtBytes(r.bytes)}</td>
                <td style="padding:4px">${r.pct_of_sample != null ? r.pct_of_sample + '%' : '—'}</td>
                <td style="padding:4px">${r.ssim != null ? r.ssim.toFixed(4) : '—'}</td>
                <td style="padding:4px">${r.seconds}s</td>
                <td style="padding:4px">${_fmtBytes(r.estimated_full_bytes)}</td>
            </tr>`).join('');
        const pending = [16, 18, 20, 22].filter(qp => !state.results.some(r => r.qp === qp));
        const pendingRows = pending.map(qp => `
            <tr data-qp="${qp}" style="color:var(--muted)">
                <td style="padding:4px">${qp}</td>
                <td colspan="5" style="padding:4px">${state.current_qp === qp ? 'encoding…' : 'pending…'}</td>
            </tr>`).join('');
        document.getElementById('estimate-rows').innerHTML = rows + pendingRows;

        const summary = document.getElementById('estimate-summary');
        if (state.status === 'error') {
            summary.innerHTML = `<span style="color:var(--danger,#c0392b)">Estimate failed: ${escHtml(state.error || 'unknown error')}</span>`;
        } else if (state.status === 'done') {
            let html = `<span>Suggested: <strong>QP ${state.suggested_qp}</strong></span>
                <button class="btn btn-primary" style="padding:4px 10px;font-size:var(--fs-sm)" onclick="_useEstimatedQp(${state.suggested_qp})">Use QP ${state.suggested_qp}</button>`;
            if (state.warning) html += `<span style="color:var(--muted);font-size:var(--fs-xs)">${escHtml(state.warning)}</span>`;
            summary.innerHTML = html;
        } else {
            summary.innerHTML = '';
        }
    }

    async function startEstimate() {
        const path = document.getElementById('encode-file-path').value;
        if (!path) return;
        const btn = document.getElementById('estimate-btn');
        btn.disabled = true;
        document.getElementById('estimate-panel').style.display = 'block';
        document.getElementById('estimate-rows').innerHTML = '';
        document.getElementById('estimate-summary').innerHTML = '';

        const config = {
            preset:  document.getElementById('enc-preset').value,
            codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
            format:  document.getElementById('enc-format').value,
            denoise: document.getElementById('enc-denoise').value || null,
            crop:    document.getElementById('enc-crop').checked,
            lang:    document.getElementById('enc-lang').value.trim().toLowerCase() || 'eng',
            width:   document.getElementById('enc-width').value ? parseInt(document.getElementById('enc-width').value, 10) : null,
        };

        try {
            const resp = await fetch('/encode/estimate?path=' + encodeURIComponent(path), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                body: JSON.stringify(config),
            });
            if (!resp.ok) {
                const txt = await resp.text();
                showToast('Estimate failed: ' + escHtml(txt), 'error');
                btn.disabled = false;
                return;
            }
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
            btn.disabled = false;
            return;
        }

        if (_estimateSource) _estimateSource.close();
        _estimateSource = new EventSource('/encode/estimate/events');
        _estimateSource.onmessage = (evt) => {
            const msg = JSON.parse(evt.data);
            if (msg.type !== 'state') return;
            _renderEstimateState(msg.state);
            if (msg.state.status === 'done' || msg.state.status === 'error') {
                btn.disabled = false;
                _estimateSource.close();
                _estimateSource = null;
            }
        };
    }

    function _useEstimatedQp(qp) {
        document.getElementById('enc-qp').value = qp;
        document.getElementById('estimate-panel').style.display = 'none';
    }
```

- [ ] **Step 2: Sanity-check syntax**

Run: `node --check static/app.js`
Expected: no output (exit code 0)

- [ ] **Step 3: Manual browser check**

This step needs a running server with a real media file — the currently
running container is mid-encode and must not be touched per the global
constraints. Defer this manual check until the user confirms the container
is free (finished encoding or a separate dev instance is available), then:

1. Start the app locally against a test file.
2. Open the encode modal for a video file, click "📈 Estimate".
3. Confirm the panel appears, rows fill in as QP 16/18/20/22 complete, and
   a suggested QP + "Use QP N" button appear at the end.
4. Click "Use QP N" and confirm `#enc-qp` updates and the panel hides.

- [ ] **Step 4: Commit**

```bash
git add static/app.js
git commit -m "feat: wire QP estimate button to backend SSE stream"
```

---

## Post-plan verification

Run the full test suite once all tasks are complete:

```bash
python3 -m unittest discover -p "test_*.py" -v
```

Expected: all tests `OK`, no regressions. Do not `docker build`/`docker run`
as part of this — verification here is unit tests only per the global
constraint (live container is mid-encode).
