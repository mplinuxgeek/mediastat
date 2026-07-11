import asyncio
import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402  (env vars must be set before import)


class EncodeJobProbeFailureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()

    async def test_job_fails_cleanly_when_ffprobe_raises(self):
        """A probe failure must not crash the job with a NameError."""

        async def _boom(*args, **kwargs):
            raise OSError("ffprobe not found")

        job = main.EncodeJob("job1", "/tmp/does-not-exist.mkv", "/tmp/out.mkv", {"lang": "eng"})
        main._encode_jobs[job.id] = job

        orig_exec = asyncio.create_subprocess_exec
        with unittest.mock.patch.object(asyncio, "create_subprocess_exec", side_effect=_boom):
            await main._run_encode_job(job.id)

        self.assertEqual(job.status, "failed")
        self.assertIsNotNone(job.error)
        self.assertNotIn("vst", job.error)
        self.assertNotIn("not associated with a value", job.error)


if __name__ == "__main__":
    unittest.main()
