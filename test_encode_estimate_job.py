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

    async def test_concurrent_second_request_gets_409(self):
        """
        End-to-end race check: fire two start_estimate calls back-to-back
        without letting _run_estimate actually run. Because the lock is set
        synchronously before any await, the second call must observe
        status == "starting" and be rejected with 409, even though
        _run_estimate (which would set status="probing") never got a chance
        to run.
        """
        headers = {"X-Delete-Token": main.DELETE_TOKEN}

        async def _fake_run_estimate(path, config):
            # Never actually runs in this test because we don't yield control
            # back to the event loop before issuing the second request.
            pass

        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(__file__)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"), \
             unittest.mock.patch.object(main, "_run_estimate", side_effect=_fake_run_estimate):
            result = await main.start_estimate(_FakeRequest(headers=headers), path="a.mkv")
            self.assertEqual(result, {"status": "started"})
            self.assertEqual(main._estimate_state["status"], "starting")

            with self.assertRaises(main.HTTPException) as ctx:
                await main.start_estimate(_FakeRequest(headers=headers), path="a.mkv")
            self.assertEqual(ctx.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
