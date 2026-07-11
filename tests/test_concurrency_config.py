import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class ConcurrencyConfigTests(unittest.TestCase):
    def test_uses_configured_value_when_present(self):
        with unittest.mock.patch.object(main, "_CONCURRENCY_CFG", {"probe_workers": 3}):
            self.assertEqual(main._concurrency("probe_workers", 8), 3)

    def test_falls_back_to_default_when_key_missing(self):
        with unittest.mock.patch.object(main, "_CONCURRENCY_CFG", {}):
            self.assertEqual(main._concurrency("probe_workers", 8), 8)

    def test_falls_back_to_default_on_non_numeric_value(self):
        with unittest.mock.patch.object(main, "_CONCURRENCY_CFG", {"probe_workers": "lots"}):
            self.assertEqual(main._concurrency("probe_workers", 8), 8)

    def test_clamps_zero_or_negative_to_one(self):
        with unittest.mock.patch.object(main, "_CONCURRENCY_CFG", {"probe_workers": 0}):
            self.assertEqual(main._concurrency("probe_workers", 8), 1)
        with unittest.mock.patch.object(main, "_CONCURRENCY_CFG", {"probe_workers": -5}):
            self.assertEqual(main._concurrency("probe_workers", 8), 1)


if __name__ == "__main__":
    unittest.main()
