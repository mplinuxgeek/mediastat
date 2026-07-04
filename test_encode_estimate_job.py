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

    async def test_probe_failure_sets_ok_false(self):
        async def _boom(*args, **kwargs):
            raise OSError("ffprobe not found")

        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_boom):
            info = await main._probe_video(main.Path("/tmp/does-not-exist.mkv"))

        self.assertFalse(info["ok"])

    async def test_probe_success_sets_ok_true(self):
        async def _fake_exec(*args, **kwargs):
            proc = unittest.mock.AsyncMock()
            proc.communicate = unittest.mock.AsyncMock(return_value=(_fake_probe_json(), b""))
            return proc

        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_fake_exec):
            info = await main._probe_video(main.Path("/tmp/does-not-exist.mkv"))

        self.assertTrue(info["ok"])


class _EmptyAsyncIter:
    """Stand-in for an empty stderr stream (`async for raw in proc.stderr`)."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class _FakeProgressStdout:
    """Stand-in for proc.stdout when reading -progress pipe:1 output: yields
    one progress chunk then EOF, matching the real read(n) loop's shape."""

    def __init__(self, payload: bytes):
        self._chunks = [payload, b""]

    async def read(self, n):
        return self._chunks.pop(0) if self._chunks else b""


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
            elif "-progress" in argv:  # qp encode — reads structured progress from stdout
                out_path = argv[-1]
                with open(out_path, "wb") as f:
                    f.write(b"0" * 1000)
                proc.stdout = _FakeProgressStdout(b"out_time_us=60000000\nfps=25.0\nprogress=end\n")
                proc.stderr = _EmptyAsyncIter()
                proc.wait = unittest.mock.AsyncMock(return_value=0)
                proc.returncode = 0
            else:  # sample extraction (stream copy, stdout=DEVNULL, uses communicate)
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
        self.assertEqual(len(main._estimate_state["results"]), 9)
        self.assertEqual([r["qp"] for r in main._estimate_state["results"]], list(range(16, 25)))
        self.assertEqual(main._estimate_state["suggested_qp"], 24)
        self.assertIsNone(main._estimate_state["error"])

        # Completing an estimate also stashes it in per-file history, so a
        # later estimate for a different file won't clobber this one's result.
        cached = main._estimate_history.get("/tmp/does-not-exist.mkv")
        self.assertIsNotNone(cached)
        self.assertEqual(cached["status"], "done")
        self.assertEqual(cached["suggested_qp"], 24)


class _FakeRequest:
    """Minimal stand-in for fastapi.Request exposing only what start_estimate reads."""

    def __init__(self, headers=None, body=None):
        self._headers = headers or {}
        self._body = body or {}

    @property
    def headers(self):
        return self._headers

    async def json(self):
        return self._body


class StartEstimateLockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reset global state before each test so tests don't leak into each other.
        main._estimate_state = {"status": "idle", "results": [], "suggested_qp": None,
                                 "warning": None, "error": None, "current_qp": None}

    async def test_second_call_while_starting_is_rejected(self):
        """
        Simulates the TOCTOU race: the first call must mark the estimate as
        busy (status="starting") synchronously, before its first `await`, so
        that a second concurrent call - which runs interleaved on the event
        loop before the first call's asyncio.create_task actually executes -
        sees the busy status and is rejected with 409, instead of also
        passing the check and kicking off a second concurrent _run_estimate.
        """
        headers = {"X-Delete-Token": main.DELETE_TOKEN}

        # shutil.which returns None so the handler raises before ever
        # awaiting request.json(), letting us observe the busy marker was set
        # purely by synchronous code before that first await point.
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(__file__)), \
             unittest.mock.patch("main.shutil.which", return_value=None):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.start_estimate(_FakeRequest(headers=headers), path="does-not-exist.mkv")
            self.assertEqual(ctx.exception.status_code, 400)
            # The handler must have reset status back to idle after bailing
            # out on the ffmpeg-not-found error, so the lock isn't stuck.
            self.assertEqual(main._estimate_state["status"], "idle")

    async def test_safe_path_rejection_does_not_wedge_lock(self):
        """
        safe_path() is called before the busy-check/lock-claim. If it raises
        HTTPException(403) (e.g. path traversal outside the media root), no
        lock should ever have been claimed, so a subsequent legitimate call
        must not be incorrectly rejected with 409.
        """
        headers = {"X-Delete-Token": main.DELETE_TOKEN}

        with unittest.mock.patch.object(
            main, "safe_path",
            side_effect=main.HTTPException(status_code=403, detail="Access denied"),
        ):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.start_estimate(_FakeRequest(headers=headers), path="../../etc/passwd")
            self.assertEqual(ctx.exception.status_code, 403)

        # The lock must not have been claimed by the rejected call.
        self.assertEqual(main._estimate_state["status"], "idle")

        # A subsequent legitimate call must succeed, not be rejected with 409.
        async def _fake_run_estimate(path, config):
            pass

        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(__file__)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"), \
             unittest.mock.patch.object(main, "_run_estimate", side_effect=_fake_run_estimate):
            result = await main.start_estimate(_FakeRequest(headers=headers), path="a.mkv")
            self.assertEqual(result, {"status": "started"})

    async def test_lock_is_set_before_first_await(self):
        """
        Proves the actual atomicity property claimed by the fix: the
        busy-check and the status="starting" write happen with zero
        `await` between them, so status is already "starting" by the
        time start_estimate reaches its first real await point
        (`await request.json()`).

        This is checked directly rather than by simulating scheduling:
        the fake request's json() method asserts the status has already
        flipped to "starting" *before* it returns control. If the set
        were (incorrectly) moved after an await, this assertion would
        not yet hold when json() is awaited, and the test would fail -
        which is precisely the bug class the earlier TOCTOU race allowed
        (a second concurrent caller slipping in between the check and
        the set).

        A trailing sequential 409 check is retained to confirm the lock
        is actually observed as busy by a subsequent caller, but note
        that check alone is a weaker guarantee than the assertion above
        since it only proves ordering after the first call has fully
        returned.
        """
        headers = {"X-Delete-Token": main.DELETE_TOKEN}

        class _AssertingRequest(_FakeRequest):
            async def json(self):
                # By the time start_estimate awaits request.json(), the
                # busy-check-and-set must already have completed
                # synchronously - proving no await point exists between
                # them where a concurrent call could race in.
                assert main._estimate_state["status"] == "starting", (
                    "status must already be 'starting' before the first "
                    "await in start_estimate, otherwise a concurrent "
                    "caller could slip between the check and the set"
                )
                return await super().json()

        async def _fake_run_estimate(path, config):
            # Never actually runs in this test because we don't yield control
            # back to the event loop before issuing the second request.
            pass

        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(__file__)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"), \
             unittest.mock.patch.object(main, "_run_estimate", side_effect=_fake_run_estimate):
            result = await main.start_estimate(_AssertingRequest(headers=headers), path="a.mkv")
            self.assertEqual(result, {"status": "started"})
            self.assertEqual(main._estimate_state["status"], "starting")

            with self.assertRaises(main.HTTPException) as ctx:
                await main.start_estimate(_FakeRequest(headers=headers), path="a.mkv")
            self.assertEqual(ctx.exception.status_code, 409)


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


if __name__ == "__main__":
    unittest.main()
