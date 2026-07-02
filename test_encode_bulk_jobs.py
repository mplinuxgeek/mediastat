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
    def __init__(self, headers=None):
        self.headers = headers or {}


def _make_job(job_id: str, status: str) -> main.EncodeJob:
    job = main.EncodeJob(job_id, f"/tmp/{job_id}.mkv", f"/tmp/{job_id}-out.mkv", {"lang": "eng"})
    job.status = status
    return job


class RouteOrderingTests(unittest.TestCase):
    """The /encode/bulk/* routes must be registered before /encode/{job_id}...
    routes, since {job_id} is a wildcard that would otherwise swallow
    /encode/bulk/retry etc. as job_id="bulk"."""

    def _route_index(self, path: str, method: str) -> int:
        for i, route in enumerate(main.app.routes):
            if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
                return i
        raise AssertionError(f"route not found: {method} {path}")

    def test_bulk_cancel_registered_before_wildcard_delete(self):
        self.assertLess(
            self._route_index("/encode/bulk/cancel", "DELETE"),
            self._route_index("/encode/{job_id}", "DELETE"),
        )

    def test_bulk_retry_registered_before_wildcard_retry(self):
        self.assertLess(
            self._route_index("/encode/bulk/retry", "POST"),
            self._route_index("/encode/{job_id}/retry", "POST"),
        )

    def test_bulk_dismiss_registered_before_wildcard_dismiss(self):
        self.assertLess(
            self._route_index("/encode/bulk/dismiss", "DELETE"),
            self._route_index("/encode/{job_id}/dismiss", "DELETE"),
        )


class BulkCancelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        main._encode_jobs.clear()
        main._encode_queue_list.clear()

    async def test_cancels_only_running_and_queued_jobs(self):
        running = _make_job("running1", "running")
        queued = _make_job("queued1", "queued")
        done = _make_job("done1", "done")
        main._encode_jobs = {j.id: j for j in (running, queued, done)}
        main._encode_queue_list[:] = ["queued1"]

        result = await main.bulk_cancel_encode(_FakeRequest({"X-Delete-Token": main.DELETE_TOKEN}))

        self.assertEqual(result, {"cancelled": 2})
        self.assertEqual(running.status, "cancelled")
        self.assertEqual(queued.status, "cancelled")
        self.assertEqual(done.status, "done")  # untouched
        self.assertNotIn("queued1", main._encode_queue_list)

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.bulk_cancel_encode(_FakeRequest({}))
        self.assertEqual(ctx.exception.status_code, 403)


class BulkRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        main._encode_jobs.clear()
        main._encode_queue_list.clear()

    async def test_retries_only_cancelled_and_failed_jobs(self):
        cancelled = _make_job("cancelled1", "cancelled")
        failed = _make_job("failed1", "failed")
        done = _make_job("done1", "done")
        main._encode_jobs = {j.id: j for j in (cancelled, failed, done)}

        result = await main.bulk_retry_encode(_FakeRequest({"X-Delete-Token": main.DELETE_TOKEN}))

        self.assertEqual(result, {"retried": 2})
        self.assertEqual(cancelled.status, "queued")
        self.assertEqual(failed.status, "queued")
        self.assertEqual(done.status, "done")  # untouched
        self.assertIn("cancelled1", main._encode_queue_list)
        self.assertIn("failed1", main._encode_queue_list)

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.bulk_retry_encode(_FakeRequest({}))
        self.assertEqual(ctx.exception.status_code, 403)


class BulkDismissTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        main._encode_jobs.clear()
        main._encode_queue_list.clear()

    async def test_dismisses_only_finished_jobs(self):
        done = _make_job("done1", "done")
        cancelled = _make_job("cancelled1", "cancelled")
        failed = _make_job("failed1", "failed")
        running = _make_job("running1", "running")
        main._encode_jobs = {j.id: j for j in (done, cancelled, failed, running)}

        result = await main.bulk_dismiss_encode(_FakeRequest({"X-Delete-Token": main.DELETE_TOKEN}))

        self.assertEqual(result, {"dismissed": 3})
        self.assertNotIn("done1", main._encode_jobs)
        self.assertNotIn("cancelled1", main._encode_jobs)
        self.assertNotIn("failed1", main._encode_jobs)
        self.assertIn("running1", main._encode_jobs)  # untouched

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.bulk_dismiss_encode(_FakeRequest({}))
        self.assertEqual(ctx.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
