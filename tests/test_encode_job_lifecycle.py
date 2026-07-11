import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class _FakeRequest:
    def __init__(self, headers=None, body=None):
        self.headers = headers or {}
        self._body = body if body is not None else {}

    async def json(self):
        return self._body


def _make_job(job_id: str, status: str) -> main.EncodeJob:
    job = main.EncodeJob(job_id, f"/tmp/{job_id}.mkv", f"/tmp/{job_id}-out.mkv", {"lang": "eng"})
    job.status = status
    return job


class StartEncodeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        main._encode_jobs.clear()
        main._encode_queue_list.clear()
        self._tmpfile = tempfile.NamedTemporaryFile(suffix=".mkv", delete=False)
        self._tmpfile.write(b"0" * 1000)
        self._tmpfile.close()
        self.path = self._tmpfile.name

    async def asyncTearDown(self):
        os.unlink(self.path)

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.start_encode(_FakeRequest({}), path=self.path)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_404_when_file_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path("/tmp/nope.mkv")):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.start_encode(_FakeRequest(headers), path="/tmp/nope.mkv")
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_queues_a_job_and_appears_in_encode_jobs(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(self.path)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"):
            result = await main.start_encode(_FakeRequest(headers), path=self.path)

        self.assertIn("job_id", result)
        job = main._encode_jobs[result["job_id"]]
        self.assertEqual(job.status, "queued")
        self.assertIn(result["job_id"], main._encode_queue_list)


class RetryDismissCancelSingleJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        main._encode_jobs.clear()
        main._encode_queue_list.clear()
        self.headers = {"X-Delete-Token": main.DELETE_TOKEN}

    async def test_retry_requeues_a_failed_job(self):
        job = _make_job("f1", "failed")
        main._encode_jobs["f1"] = job
        await main.retry_encode("f1", _FakeRequest(self.headers))
        self.assertEqual(job.status, "queued")
        self.assertIn("f1", main._encode_queue_list)

    async def test_retry_409_when_job_not_retryable(self):
        job = _make_job("r1", "running")
        main._encode_jobs["r1"] = job
        with self.assertRaises(main.HTTPException) as ctx:
            await main.retry_encode("r1", _FakeRequest(self.headers))
        self.assertEqual(ctx.exception.status_code, 409)

    async def test_retry_404_when_job_missing(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.retry_encode("nope", _FakeRequest(self.headers))
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_cancel_stops_a_queued_job(self):
        job = _make_job("q1", "queued")
        main._encode_jobs["q1"] = job
        main._encode_queue_list[:] = ["q1"]
        await main.cancel_encode("q1", _FakeRequest(self.headers))
        self.assertEqual(job.status, "cancelled")
        self.assertNotIn("q1", main._encode_queue_list)

    async def test_cancel_409_when_job_not_active(self):
        job = _make_job("d1", "done")
        main._encode_jobs["d1"] = job
        with self.assertRaises(main.HTTPException) as ctx:
            await main.cancel_encode("d1", _FakeRequest(self.headers))
        self.assertEqual(ctx.exception.status_code, 409)

    async def test_dismiss_removes_a_finished_job(self):
        job = _make_job("d2", "done")
        main._encode_jobs["d2"] = job
        await main.dismiss_encode("d2", _FakeRequest(self.headers))
        self.assertNotIn("d2", main._encode_jobs)

    async def test_dismiss_404_when_job_missing(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.dismiss_encode("nope", _FakeRequest(self.headers))
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_delete_output_unlinks_and_dismisses_job(self):
        job = _make_job("d3", "done")
        job.output_path = os.path.join(tempfile.gettempdir(), "test-d3-out.mkv")
        with open(job.output_path, "wb") as f:
            f.write(b"out")
        self.assertTrue(os.path.exists(job.output_path))
        main._encode_jobs["d3"] = job

        await main.delete_encode_output("d3", _FakeRequest(self.headers))
        self.assertNotIn("d3", main._encode_jobs)
        self.assertFalse(os.path.exists(job.output_path))

    async def test_delete_output_no_unlink_if_moved(self):
        job = _make_job("d4", "done")
        job.moved = True
        job.output_path = os.path.join(tempfile.gettempdir(), "test-d4-out.mkv")
        with open(job.output_path, "wb") as f:
            f.write(b"out")
        self.assertTrue(os.path.exists(job.output_path))
        main._encode_jobs["d4"] = job

        await main.delete_encode_output("d4", _FakeRequest(self.headers))
        self.assertNotIn("d4", main._encode_jobs)
        self.assertTrue(os.path.exists(job.output_path))
        os.unlink(job.output_path)

    async def test_delete_output_404_when_job_missing(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.delete_encode_output("nope", _FakeRequest(self.headers))
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_ffmpeg_cmd_save_and_load(self):
        job = _make_job("d5", "done")
        job.ffmpeg_cmd = "ffmpeg -y -i input.mkv -c:v libx265 output.mkv"
        await main._save_encode_job(job)
        
        main._encode_jobs.clear()
        await main._load_encode_jobs()
        
        restored = main._encode_jobs.get("d5")
        self.assertIsNotNone(restored)
        self.assertEqual(restored.ffmpeg_cmd, "ffmpeg -y -i input.mkv -c:v libx265 output.mkv")

    async def test_all_require_delete_token(self):
        job = _make_job("x1", "failed")
        main._encode_jobs["x1"] = job
        for coro in (
            main.retry_encode("x1", _FakeRequest({})),
            main.cancel_encode("x1", _FakeRequest({})),
            main.dismiss_encode("x1", _FakeRequest({})),
            main.delete_encode_output("x1", _FakeRequest({})),
        ):
            with self.assertRaises(main.HTTPException) as ctx:
                await coro
            self.assertEqual(ctx.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
