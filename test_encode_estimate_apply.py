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


class ApplyEstimateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        main._encode_jobs.clear()
        main._encode_queue_list.clear()
        main._estimate_state = {"status": "idle"}
        main._estimate_history.clear()
        self._tmpfile = tempfile.NamedTemporaryFile(suffix=".mkv", delete=False)
        self._tmpfile.write(b"0" * 1000)
        self._tmpfile.close()
        self.path = self._tmpfile.name

    async def asyncTearDown(self):
        os.unlink(self._tmpfile.name)

    def _done_state(self, suggested_qp=20, config=None, path=None):
        return {
            "status": "done",
            "path": path if path is not None else self.path,
            "suggested_qp": suggested_qp,
            "config": config or {"codec": "hevc", "gpu": "auto", "preset": "quality",
                                  "format": "mkv", "lang": "eng", "qp": 18},
            "results": [{"qp": suggested_qp, "ssim": 0.99}],
        }

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.apply_estimate(_FakeRequest({}), path=self.path)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_404_when_file_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with self.assertRaises(main.HTTPException) as ctx:
            await main.apply_estimate(_FakeRequest(headers), path="/tmp/does-not-exist-apply.mkv")
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_409_when_no_completed_estimate_anywhere(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        main._estimate_state = {"status": "idle"}
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(self.path)):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.apply_estimate(_FakeRequest(headers), path=self.path)
        self.assertEqual(ctx.exception.status_code, 409)

    async def test_queues_job_using_live_estimate_state(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        main._estimate_state = self._done_state(suggested_qp=22)
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(self.path)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"):
            result = await main.apply_estimate(_FakeRequest(headers), path=self.path)

        self.assertIn("job_id", result)
        job = main._encode_jobs[result["job_id"]]
        self.assertEqual(job.config["qp"], 22)
        self.assertEqual(job.config["codec"], "hevc")
        self.assertEqual(job.status, "queued")
        self.assertIn(result["job_id"], main._encode_queue_list)

    async def test_falls_back_to_history_when_live_state_is_a_different_file(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        main._estimate_state = self._done_state(suggested_qp=16, path="/tmp/some-other-file.mkv")
        main._estimate_history[self.path] = self._done_state(suggested_qp=24)
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(self.path)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"):
            result = await main.apply_estimate(_FakeRequest(headers), path=self.path)

        job = main._encode_jobs[result["job_id"]]
        self.assertEqual(job.config["qp"], 24)

    async def test_explicit_qp_in_body_overrides_suggested_qp(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        main._estimate_state = self._done_state(suggested_qp=20)
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(self.path)), \
             unittest.mock.patch.object(main.shutil, "which", return_value="/usr/bin/ffmpeg"):
            result = await main.apply_estimate(_FakeRequest(headers, body={"qp": 18}), path=self.path)

        job = main._encode_jobs[result["job_id"]]
        self.assertEqual(job.config["qp"], 18)

    async def test_400_when_ffmpeg_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        main._estimate_state = self._done_state()
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(self.path)), \
             unittest.mock.patch.object(main.shutil, "which", return_value=None):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.apply_estimate(_FakeRequest(headers), path=self.path)
        self.assertEqual(ctx.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
