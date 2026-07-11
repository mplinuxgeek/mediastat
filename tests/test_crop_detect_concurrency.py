import asyncio
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))
os.environ.setdefault("DB_PATH", tempfile.NamedTemporaryFile(suffix=".db", delete=False).name)

import main  # noqa: E402  (env vars must be set before import)


class FakeProc:
    def __init__(self, stderr: bytes):
        self._stderr = stderr

    async def communicate(self):
        return b"", self._stderr

    def kill(self):
        pass

    async def wait(self):
        return 0


class CropDetectConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_samples_run_concurrently_not_sequentially(self):
        in_flight = 0
        max_in_flight = 0
        lock = asyncio.Lock()

        async def fake_exec(*args, **kwargs):
            nonlocal in_flight, max_in_flight
            async with lock:
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.05)
            async with lock:
                in_flight -= 1
            return FakeProc(b"")

        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=fake_exec):
            await main._detect_crop(Path("/tmp/fake.mkv"), duration=600)

        self.assertGreater(
            max_in_flight, 1,
            "cropdetect samples ran one-at-a-time instead of concurrently",
        )


if __name__ == "__main__":
    unittest.main()
