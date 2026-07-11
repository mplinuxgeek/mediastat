import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class EncodeDefaultHelperTests(unittest.TestCase):
    def test_returns_configured_value_when_present(self):
        with unittest.mock.patch.object(main, "_ENCODE_DEFAULTS_CFG", {"qp": 22}):
            self.assertEqual(main._encode_default("qp", 18), 22)

    def test_falls_back_when_key_missing(self):
        with unittest.mock.patch.object(main, "_ENCODE_DEFAULTS_CFG", {}):
            self.assertEqual(main._encode_default("qp", 18), 18)


class MakeEncodeConfigDefaultsTests(unittest.TestCase):
    def test_hardcoded_fallbacks_used_when_no_config_override(self):
        with unittest.mock.patch.object(main, "_ENCODE_DEFAULTS_CFG", {}):
            config = main._make_encode_config({})
        self.assertEqual(config["qp"], 18)
        self.assertEqual(config["preset"], "quality")
        self.assertEqual(config["codec"], "hevc")
        self.assertEqual(config["format"], "mkv")
        self.assertEqual(config["lang"], "eng")
        self.assertIsNone(config["denoise"])
        self.assertFalse(config["crop"])
        self.assertIsNone(config["width"])

    def test_configured_defaults_are_used_when_body_omits_field(self):
        with unittest.mock.patch.object(main, "_ENCODE_DEFAULTS_CFG", {
            "qp": 22, "codec": "av1", "preset": "archive", "lang": "spa",
            "crop": True, "width": 1920, "denoise": "light",
        }):
            config = main._make_encode_config({})
        self.assertEqual(config["qp"], 22)
        self.assertEqual(config["codec"], "av1")
        self.assertEqual(config["preset"], "archive")
        self.assertEqual(config["lang"], "spa")
        self.assertTrue(config["crop"])
        self.assertEqual(config["width"], 1920)
        self.assertEqual(config["denoise"], "light")

    def test_explicit_request_body_overrides_configured_default(self):
        with unittest.mock.patch.object(main, "_ENCODE_DEFAULTS_CFG", {"qp": 22, "codec": "av1"}):
            config = main._make_encode_config({"qp": 16, "codec": "h264"})
        self.assertEqual(config["qp"], 16)
        self.assertEqual(config["codec"], "h264")

    def test_invalid_configured_default_still_falls_through_validation(self):
        with unittest.mock.patch.object(main, "_ENCODE_DEFAULTS_CFG", {"codec": "bogus", "preset": "nonsense"}):
            config = main._make_encode_config({})
        self.assertEqual(config["codec"], "hevc")
        self.assertEqual(config["preset"], "quality")


if __name__ == "__main__":
    unittest.main()
