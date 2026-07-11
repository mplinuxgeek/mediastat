import os
import tempfile
import unittest

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class HealthzTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_ok_status(self):
        result = await main.healthz()
        self.assertEqual(result, {"status": "ok"})


if __name__ == "__main__":
    unittest.main()
