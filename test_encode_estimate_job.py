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


if __name__ == "__main__":
    unittest.main()
