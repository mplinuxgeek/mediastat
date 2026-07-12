import os
import tempfile
import unittest

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class CopyModeStreamHandlingTests(unittest.TestCase):
    def test_copy_mode_keeps_matching_language_subtitles_and_sets_subtitle_codec(self):
        config = {"codec": "copy", "lang": "eng"}
        a_streams = [
            {"tags": {"language": "eng"}},
            {"tags": {"language": "spa"}},
        ]
        s_streams = [
            {"tags": {"language": "eng"}},
        ]
        cmd, encoder = main._build_ffmpeg_cmd(
            "/in.mkv", "/out.mkv", config, hw={},
            a_streams=a_streams, s_streams=s_streams,
        )
        self.assertEqual(encoder, "copy")
        self.assertIn("-c:s", cmd)
        self.assertEqual(cmd[cmd.index("-c:s") + 1], "copy")
        self.assertIn("0:s:0?", cmd)
        self.assertIn("0:a:0", cmd)
        self.assertNotIn("0:a:1", cmd)  # non-matching-language audio excluded


if __name__ == "__main__":
    unittest.main()
