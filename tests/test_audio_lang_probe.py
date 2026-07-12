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


def _ffprobe_json(audio_language=None, audio_tag_key="language"):
    audio_stream = {"codec_type": "audio", "codec_name": "ac3"}
    if audio_language is not None:
        audio_stream["tags"] = {audio_tag_key: audio_language}
    return json.dumps({
        "streams": [
            {"codec_type": "video", "codec_name": "hevc", "width": 1920, "height": 1080},
            audio_stream,
        ],
        "format": {"duration": "3600.0", "tags": {}},
    }).encode()


class _FakeProc:
    def __init__(self, stdout: bytes):
        self._stdout = stdout

    async def communicate(self):
        return self._stdout, b""


class AudioLangProbeTests(unittest.IsolatedAsyncioTestCase):
    async def _run_with_stdout(self, stdout: bytes) -> dict:
        async def _fake_exec(*args, **kwargs):
            return _FakeProc(stdout)

        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_fake_exec):
            return await main.run_ffprobe(main.Path("/tmp/does-not-matter.mkv"))

    async def test_extracts_non_english_audio_language(self):
        result = await self._run_with_stdout(_ffprobe_json(audio_language="fre"))
        self.assertEqual(result["audio_lang"], "fre")

    async def test_extracts_english_audio_language(self):
        result = await self._run_with_stdout(_ffprobe_json(audio_language="eng"))
        self.assertEqual(result["audio_lang"], "eng")

    async def test_undetermined_language_tag_normalizes_to_empty(self):
        result = await self._run_with_stdout(_ffprobe_json(audio_language="und"))
        self.assertEqual(result["audio_lang"], "")

    async def test_missing_language_tag_is_empty(self):
        result = await self._run_with_stdout(_ffprobe_json(audio_language=None))
        self.assertEqual(result["audio_lang"], "")

    async def test_uppercase_language_tag_key_is_read(self):
        result = await self._run_with_stdout(_ffprobe_json(audio_language="GER", audio_tag_key="LANGUAGE"))
        self.assertEqual(result["audio_lang"], "ger")


if __name__ == "__main__":
    unittest.main()
