# QP Estimate Cancel + Reopen Reattach Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user stop an in-progress QP estimate sweep, and make reopening the encode modal for a file with a running estimate show live progress instead of a blank panel until it finishes.

**Architecture:** Add an `asyncio.Event` + tracked-subprocess-handle pair so `/encode/estimate/cancel` can both flag the run loop to stop at its next checkpoint and kill the currently-running ffmpeg process for an immediate stop (mirrors the existing `_imdbscan_cancel_event` / `/imdb/scan-cancel` pattern already in `main.py`). Add `GET /encode/estimate/state` so the frontend can ask "is an estimate running right now, and for which file" independent of the per-file finished-only history endpoint. Update `openEncodeModal` to check that live state before falling back to history, and add a Stop button that's shown/hidden based on run status.

**Tech Stack:** FastAPI + asyncio subprocesses (Python, `main.py`), vanilla JS (`static/app.js`), Jinja template (`templates/_modals.html`), `unittest.IsolatedAsyncioTestCase`.

## Global Constraints

- Follow existing cancel-endpoint pattern in `main.py` (see `/imdb/scan-cancel`, `/db/clean-cancel`): no `X-Delete-Token` check on the cancel endpoint itself — cancelling isn't a destructive/mutating action against files, just stops in-memory work.
- Cancelled runs are saved to `_estimate_history` same as `done`/`error`, keeping any partial QP rows that completed before the stop.
- Don't touch the QP sweep values, SSIM logic, or encode config building — this plan only adds stop/observability plumbing around the existing `_run_estimate` loop.
- Match existing code style: no docstrings beyond the terse one-liners already used in this file; comments only where the "why" isn't obvious from the code.

---

### Task 1: Backend cancel mechanism

**Files:**
- Modify: `main.py` (globals near line 3355-3363, `_run_estimate` at line 3391-3531, `start_estimate` at line 4307-4327; add new endpoint after `start_estimate`)
- Test: `test_encode_estimate_job.py`

**Interfaces:**
- Produces: module globals `main._estimate_cancel_event: asyncio.Event`, `main._estimate_proc: Optional[asyncio.subprocess.Process]`; new route `POST /encode/estimate/cancel` returning `{"cancelling": True}` or `{"error": "Not running"}`; `_estimate_state["status"]` can now be `"cancelling"` (transient, set the instant cancel is requested) and `"cancelled"` (terminal, same shape as `"error"`/`"done"` for history purposes).

- [ ] **Step 1: Add cancel globals next to the existing estimate globals**

In `main.py`, right after the `_estimate_history` declaration (around line 3363):

```python
_estimate_cancel_event = asyncio.Event()
_estimate_proc: Optional[asyncio.subprocess.Process] = None
```

- [ ] **Step 2: Track the current subprocess and check the cancel event at each checkpoint in `_run_estimate`**

Replace the whole body of `_run_estimate` (lines 3391-3531) with a version that (a) clears the cancel event at the top of a run, (b) stashes every subprocess it creates into the module-level `_estimate_proc` so the cancel endpoint can kill it immediately, and (c) checks `_estimate_cancel_event.is_set()` at each stage boundary and inside the per-QP progress-read loop, breaking out to a `"cancelled"` terminal state when tripped:

```python
async def _run_estimate(path: str, config: dict) -> None:
    global _estimate_state, _estimate_proc
    tmp_dir: Optional[str] = None
    _estimate_cancel_event.clear()

    def _cancelled() -> bool:
        if not _estimate_cancel_event.is_set():
            return False
        _estimate_state["status"] = "cancelled"
        _broadcast_estimate()
        return True

    try:
        _estimate_state = {"status": "probing", "results": [], "suggested_qp": None,
                            "warning": None, "error": None, "current_qp": None, "qp_progress": 0.0,
                            "path": path, "config": config}
        _broadcast_estimate()

        input_path = Path(path)
        info = await _probe_video(input_path)
        if _cancelled():
            return

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
        _estimate_proc = proc
        _, stderr = await proc.communicate()
        _estimate_proc = None
        if _cancelled():
            return
        if proc.returncode != 0 or not sample_path.exists():
            raise RuntimeError(f"sample extraction failed: {stderr.decode(errors='replace')[-500:]}")

        sample_raw_bytes = sample_path.stat().st_size
        source_size = input_path.stat().st_size

        crop_filter = None
        if config.get("crop"):
            crop_filter = await _detect_crop(sample_path, length)
        if _cancelled():
            return

        width = config.get("width")

        for qp in _ESTIMATE_QPS:
            if _cancelled():
                return

            _estimate_state["status"] = "encoding"
            _estimate_state["current_qp"] = qp
            _estimate_state["qp_progress"] = 0.0
            _broadcast_estimate()

            out_path = Path(tmp_dir) / f"qp{qp}.mkv"
            cmd, _encoder = _build_ffmpeg_cmd(
                str(sample_path), str(out_path), {**config, "qp": qp}, _hw_accel_info,
                info["bit_depth"], info["is_hdr"], info["cp"], info["tc"], info["cs"], info["cr"],
                crop_filter=crop_filter, a_streams=[], s_streams=[], source_fps=info["source_fps"],
            )
            t0 = time.time()
            # _build_ffmpeg_cmd always appends "-progress pipe:1 -nostats", so
            # stdout carries structured key=value progress lines (same format
            # _run_encode_job parses) — read it to estimate a live percentage
            # against the known sample length, since ffmpeg reports none itself.
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            _estimate_proc = proc
            stderr_chunks: list[bytes] = []

            async def _drain_qp_stderr(p=proc):
                async for raw in p.stderr:
                    stderr_chunks.append(raw)

            stderr_task = asyncio.create_task(_drain_qp_stderr())
            duration_us = int(length * 1e6) if length else 0
            last_broadcast = 0.0
            buf = ""
            while True:
                if _estimate_cancel_event.is_set():
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    break
                chunk = await proc.stdout.read(512)
                if not chunk:
                    break
                buf += chunk.decode(errors="replace")
                lines = buf.split("\n")
                buf = lines[-1]
                for line in lines[:-1]:
                    key, _, val = line.strip().partition("=")
                    if key == "out_time_us" and duration_us:
                        try:
                            _estimate_state["qp_progress"] = min(99.0, int(val) / duration_us * 100)
                        except (ValueError, ZeroDivisionError):
                            pass
                    if key in ("out_time_us", "fps"):
                        now = time.monotonic()
                        if now - last_broadcast >= 0.5:
                            _broadcast_estimate()
                            last_broadcast = now

            await proc.wait()
            _estimate_proc = None
            try:
                await asyncio.wait_for(stderr_task, timeout=2.0)
            except asyncio.TimeoutError:
                pass
            if _cancelled():
                return
            stderr = b"".join(stderr_chunks)
            seconds = time.time() - t0
            if proc.returncode != 0 or not out_path.exists():
                raise RuntimeError(f"QP{qp} encode failed: {stderr.decode(errors='replace')[-500:]}")

            _estimate_state["qp_progress"] = 100.0
            out_bytes = out_path.stat().st_size

            ssim_filter = build_ssim_ref_filter(crop_filter, width)
            ssim_cmd = [
                "ffmpeg", "-i", str(out_path), "-i", str(sample_path),
                "-lavfi", ssim_filter, "-f", "null", "-",
            ]
            proc = await asyncio.create_subprocess_exec(
                *ssim_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _estimate_proc = proc
            _, ssim_stderr = await proc.communicate()
            _estimate_proc = None
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
        _save_estimate_history(_estimate_history, path, _estimate_state, _ESTIMATE_HISTORY_MAX)
    except Exception as e:
        _estimate_state["status"] = "error"
        _estimate_state["error"] = str(e)
        _broadcast_estimate()
        _save_estimate_history(_estimate_history, path, _estimate_state, _ESTIMATE_HISTORY_MAX)
        log.warning("Estimate failed: %s", e)
    finally:
        _estimate_proc = None
        if _estimate_state.get("status") == "cancelled":
            _save_estimate_history(_estimate_history, path, _estimate_state, _ESTIMATE_HISTORY_MAX)
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
```

Note the `_cancelled()` helper both checks and — if tripped — sets state/broadcasts, so every call site only needs `if _cancelled(): return`. The history save for the cancelled case lives in `finally` (not inside `_cancelled()`) so it fires exactly once regardless of which checkpoint caught the cancellation.

- [ ] **Step 3: Add the cancel endpoint, and clear the event when a new run starts**

In `start_estimate` (around line 4307), add `_estimate_cancel_event.clear()` right before `_estimate_state["status"] = "starting"`:

```python
@app.post("/encode/estimate")
async def start_estimate(request: Request, path: str = Query(...)):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    file_path = safe_path(path)
    if _estimate_state.get("status") in ("starting", "probing", "extracting", "encoding"):
        raise HTTPException(status_code=409, detail="An estimate is already running")
    _estimate_cancel_event.clear()
    _estimate_state["status"] = "starting"
    ...
```

Then add a new route right after `start_estimate` (before `estimate_history`):

```python
@app.post("/encode/estimate/cancel")
async def cancel_estimate():
    """Ask a running QP sweep to stop at its next checkpoint, and kill
    whatever ffmpeg process is currently in flight for an immediate stop —
    otherwise the caller would have to wait for the current QP pass (or the
    whole sweep) to finish naturally."""
    if _estimate_state.get("status") not in ("starting", "probing", "extracting", "encoding"):
        return {"error": "Not running"}
    _estimate_cancel_event.set()
    if _estimate_proc and _estimate_proc.returncode is None:
        try:
            _estimate_proc.kill()
        except Exception:
            pass
    return {"cancelling": True}
```

- [ ] **Step 4: Write the failing test for cancellation mid-sweep**

Append to `test_encode_estimate_job.py`, inside a new test class after `RunEstimateTests`:

```python
class CancelEstimateTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_during_qp_loop_stops_and_saves_partial_history(self):
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
            elif "-progress" in argv:  # qp encode
                out_path = argv[-1]
                with open(out_path, "wb") as f:
                    f.write(b"0" * 1000)
                proc.stdout = _FakeProgressStdout(b"out_time_us=60000000\nfps=25.0\nprogress=end\n")
                proc.stderr = _EmptyAsyncIter()
                proc.wait = unittest.mock.AsyncMock(return_value=0)
                proc.returncode = 0
                # Second QP pass (qp=17) is where the cancel request lands.
                if len([a for a in calls if "-progress" in a]) == 2:
                    main._estimate_cancel_event.set()
            else:  # sample extraction
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

        self.assertEqual(main._estimate_state["status"], "cancelled")
        # QP 16 finished before cancel; QP 17's in-flight pass is cut short so
        # it never gets appended to results.
        self.assertEqual(len(main._estimate_state["results"]), 1)
        self.assertEqual(main._estimate_state["results"][0]["qp"], 16)

        cached = main._estimate_history.get("/tmp/does-not-exist.mkv")
        self.assertIsNotNone(cached)
        self.assertEqual(cached["status"], "cancelled")
        self.assertEqual(len(cached["results"]), 1)

    async def test_cancel_endpoint_returns_not_running_when_idle(self):
        main._estimate_state = {"status": "idle", "results": [], "suggested_qp": None,
                                 "warning": None, "error": None, "current_qp": None}
        result = await main.cancel_estimate()
        self.assertEqual(result, {"error": "Not running"})

    async def test_cancel_endpoint_sets_event_and_kills_proc_when_running(self):
        main._estimate_state = {"status": "encoding", "results": [], "suggested_qp": None,
                                 "warning": None, "error": None, "current_qp": 18}
        main._estimate_cancel_event.clear()
        fake_proc = unittest.mock.Mock()
        fake_proc.returncode = None
        main._estimate_proc = fake_proc
        try:
            result = await main.cancel_estimate()
            self.assertEqual(result, {"cancelling": True})
            self.assertTrue(main._estimate_cancel_event.is_set())
            fake_proc.kill.assert_called_once()
        finally:
            main._estimate_proc = None
            main._estimate_cancel_event.clear()
```

- [ ] **Step 5: Run the new tests to verify they fail first (endpoint/behavior not yet implemented), then pass after Steps 1-3**

Run: `python -m pytest test_encode_estimate_job.py -v -k Cancel`
Expected before implementation: `AttributeError: module 'main' has no attribute 'cancel_estimate'` (or `_estimate_cancel_event`).
After implementing Steps 1-3: all three tests PASS.

- [ ] **Step 6: Run the full existing estimate test suite to confirm no regression**

Run: `python -m pytest test_encode_estimate_job.py test_encode_estimate.py test_encode_estimate_history.py test_encode_estimate_apply.py -v`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add main.py test_encode_estimate_job.py
git commit -m "feat: add /encode/estimate/cancel to stop a running QP sweep"
```

---

### Task 2: Live estimate state endpoint

**Files:**
- Modify: `main.py` (add route near `estimate_history`, around line 4330-4337)
- Test: `test_encode_estimate_history.py`

**Interfaces:**
- Consumes: module global `_estimate_state` (already defined).
- Produces: `GET /encode/estimate/state` → returns `_estimate_state` dict as-is (same JSON shape SSE already streams), always 200 (never 404 — `status` is `"idle"` when nothing has ever run).

- [ ] **Step 1: Write the failing test**

Append to `test_encode_estimate_history.py`:

```python
class LiveEstimateStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_current_live_state_regardless_of_status(self):
        main._estimate_state = {
            "status": "encoding", "results": [{"qp": 16, "bytes": 100}],
            "suggested_qp": None, "warning": None, "error": None,
            "current_qp": 17, "qp_progress": 42.0, "path": "/media/movie.mkv",
        }
        result = await main.live_estimate_state()
        self.assertEqual(result["status"], "encoding")
        self.assertEqual(result["path"], "/media/movie.mkv")
        self.assertEqual(result["current_qp"], 17)

    async def test_returns_idle_when_nothing_has_run(self):
        main._estimate_state = {"status": "idle", "results": [], "suggested_qp": None,
                                 "warning": None, "error": None, "current_qp": None}
        result = await main.live_estimate_state()
        self.assertEqual(result["status"], "idle")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_encode_estimate_history.py -v -k LiveEstimateState`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'live_estimate_state'`

- [ ] **Step 3: Add the route**

In `main.py`, right after `start_estimate`/`cancel_estimate` and before `estimate_history` (around line 4330):

```python
@app.get("/encode/estimate/state")
async def live_estimate_state():
    """Return the in-memory state of whatever estimate is currently running
    (or last ran), regardless of which file it's for — lets the frontend
    check "is anything running, and for which path" independent of the
    per-file finished-only history in `_estimate_history`."""
    return _estimate_state
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_encode_estimate_history.py -v -k LiveEstimateState`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add main.py test_encode_estimate_history.py
git commit -m "feat: add /encode/estimate/state to expose live estimate status"
```

---

### Task 3: Frontend Stop button + reopen-modal reattach

**Files:**
- Modify: `templates/_modals.html:89-90` (add Stop button next to Estimate button)
- Modify: `static/app.js:154-365` (`openEncodeModal`, `_loadCachedEstimate`, `closeEncodeModal`, `startEstimate`, add `_stopEstimate` and a shared `_attachEstimateSource` helper)

**Interfaces:**
- Consumes: `GET /encode/estimate/state`, `POST /encode/estimate/cancel` (Task 1 & 2), existing `GET /encode/estimate/history`, `GET /encode/estimate/events` SSE, `_renderEstimateState(state)` (existing, unchanged signature).
- Produces: `_stopEstimate()` (global, called from the new Stop button's `onclick`), `_attachEstimateSource()` (module-scoped, wires a fresh `EventSource` to `_renderEstimateState` + button state, reused by both `startEstimate` and the reopen path).

- [ ] **Step 1: Add the Stop button markup**

In `templates/_modals.html`, replace lines 89-90:

```html
                    <button id="estimate-btn" type="button" class="btn" style="padding:4px 10px;font-size:var(--fs-sm)"
                            onclick="startEstimate()" title="Encode a 60s sample from the middle at QP 16/18/20/22 and suggest a value">📈 Estimate</button>
```

with:

```html
                    <button id="estimate-btn" type="button" class="btn" style="padding:4px 10px;font-size:var(--fs-sm)"
                            onclick="startEstimate()" title="Encode a 60s sample from the middle at QP 16/18/20/22 and suggest a value">📈 Estimate</button>
                    <button id="estimate-stop-btn" type="button" class="btn" style="padding:4px 10px;font-size:var(--fs-sm);display:none"
                            onclick="_stopEstimate()" title="Stop the running QP sweep">⏹ Stop</button>
```

- [ ] **Step 2: Add a shared `_attachEstimateSource` helper and rewrite `startEstimate` to use it**

In `static/app.js`, replace the body of `startEstimate` (lines 314-365) and add the new helper right above it:

```javascript
    // Wires a fresh EventSource to the live-estimate SSE stream, driving both
    // the results panel and the Estimate/Stop button pair. Shared by
    // startEstimate() (a run this tab just kicked off) and openEncodeModal()
    // (reattaching to a run already in progress from a previous modal open).
    function _attachEstimateSource() {
        if (_estimateSource) _estimateSource.close();
        document.getElementById('estimate-btn').disabled = true;
        document.getElementById('estimate-stop-btn').style.display = 'inline-block';
        _estimateSource = new EventSource('/encode/estimate/events');
        _estimateSource.onmessage = (evt) => {
            const msg = JSON.parse(evt.data);
            if (msg.type !== 'state') return;
            _renderEstimateState(msg.state);
            if (msg.state.status === 'done' || msg.state.status === 'error' || msg.state.status === 'cancelled') {
                document.getElementById('estimate-btn').disabled = false;
                document.getElementById('estimate-stop-btn').style.display = 'none';
                _estimateSource.close();
                _estimateSource = null;
            }
        };
    }

    async function _stopEstimate() {
        const btn = document.getElementById('estimate-stop-btn');
        btn.disabled = true;
        try {
            await fetch('/encode/estimate/cancel', { method: 'POST' });
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
        } finally {
            btn.disabled = false;
        }
    }

    async function startEstimate() {
        const path = document.getElementById('encode-file-path').value;
        if (!path) return;
        document.getElementById('estimate-panel').style.display = 'block';
        document.getElementById('estimate-status-line').textContent = 'Sampling 60s from the middle of the file…';
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
                return;
            }
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
            return;
        }

        _attachEstimateSource();
    }
```

Note `startEstimate` no longer manually flips `estimate-btn.disabled` around the fetch — `_attachEstimateSource` now owns disabling it (and showing Stop) for the whole run, including the reattach path where there's no fetch to wrap.

- [ ] **Step 3: Make `openEncodeModal` check live state before falling back to history**

Replace `openEncodeModal` (lines 154-164) and `_loadCachedEstimate` (lines 169-185) in `static/app.js`:

```javascript
    function openEncodeModal(btn) {
        const entry = btn.closest('.file-entry');
        const path  = entry.dataset.path;
        const name  = entry.querySelector('.file-stem').textContent.trim();
        document.getElementById('encode-modal-name').textContent = name;
        document.getElementById('encode-file-path').value = path;
        _renderCustomPresetButtons();
        applyEncodePreset('quality');
        document.getElementById('encode-modal').style.display = 'flex';
        document.getElementById('estimate-btn').disabled = false;
        document.getElementById('estimate-stop-btn').style.display = 'none';
        _loadEstimateForModal(path);
    }

    const _ESTIMATE_RUNNING_STATUSES = ['starting', 'probing', 'extracting', 'encoding'];

    // Reopening the modal previously always showed a blank panel until a
    // running estimate finished, even though the sweep kept running in the
    // background — the only source consulted was the finished-only history
    // endpoint. Check the live estimate state first so a run already in
    // progress for this exact file reattaches immediately with current
    // progress, instead of going dark until it completes.
    async function _loadEstimateForModal(path) {
        const panel = document.getElementById('estimate-panel');
        panel.style.display = 'none';
        document.getElementById('estimate-rows').innerHTML = '';
        document.getElementById('estimate-summary').innerHTML = '';

        try {
            const liveResp = await fetch('/encode/estimate/state');
            if (liveResp.ok) {
                const live = await liveResp.json();
                if (live.path === path && _ESTIMATE_RUNNING_STATUSES.includes(live.status)) {
                    document.getElementById('estimate-status-line').textContent =
                        'Sampling 60s from the middle of the file…';
                    panel.style.display = 'block';
                    _renderEstimateState(live);
                    _attachEstimateSource();
                    return;
                }
            }
        } catch (e) { /* live-state check failed — fall through to history */ }

        _loadCachedEstimate(path);
    }

    // Show a previous estimate for this exact file, if one is cached
    // server-side, instead of always starting blank — each file keeps its
    // own last result, so switching files never loses another file's numbers.
    async function _loadCachedEstimate(path) {
        const panel = document.getElementById('estimate-panel');
        try {
            const resp = await fetch('/encode/estimate/history?path=' + encodeURIComponent(path));
            if (!resp.ok) return;
            const state = await resp.json();
            document.getElementById('estimate-status-line').textContent =
                'Showing a previous estimate for this file — click Estimate to re-run.';
            panel.style.display = 'block';
            _renderEstimateState(state);
        } catch (e) {
            // No cached estimate for this file — leave the panel hidden.
        }
    }
```

- [ ] **Step 4: Close the SSE connection on modal close so it doesn't keep running invisibly once the user has navigated away**

Replace `closeEncodeModal` in `static/app.js`:

```javascript
    function closeEncodeModal() {
        document.getElementById('encode-modal').style.display = 'none';
        if (_estimateSource) { _estimateSource.close(); _estimateSource = null; }
        // Restore original onclick if it was overridden by batch mode
        const btn = document.querySelector('#encode-modal .btn-primary');
        if (btn && btn._originalOnclick) { btn.onclick = btn._originalOnclick; btn._originalOnclick = null; }
    }
```

This is safe because the estimate itself lives server-side in `_run_estimate` — closing the SSE only drops this tab's live view, and reopening the modal (Step 3) reattaches a fresh one.

- [ ] **Step 5: Manually verify in a browser**

Run: use the `run` skill to start the app, then:
1. Open a file's encode modal, click Estimate.
2. While it's running, close the modal, reopen it — confirm the panel shows current QP progress immediately (not blank), and the Stop button is visible.
3. Click Stop — confirm the sweep stops within a couple seconds, panel shows whatever QP rows completed, `estimate-btn` re-enables, `estimate-stop-btn` hides.
4. Start a fresh estimate and let it run to completion normally — confirm no regression (Done state, suggested QP, Apply-to-batch still work).

- [ ] **Step 6: Commit**

```bash
git add templates/_modals.html static/app.js
git commit -m "feat: add Stop button for QP estimates and reattach live progress on modal reopen"
```
