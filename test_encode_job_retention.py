import os
import tempfile
import time
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


def _make_job(job_id: str, status: str, finished_at) -> main.EncodeJob:
    job = main.EncodeJob(job_id, f"/tmp/{job_id}.mkv", f"/tmp/{job_id}-out.mkv", {"lang": "eng"})
    job.status = status
    job.finished_at = finished_at
    return job


class JobsToPruneTests(unittest.TestCase):
    def test_prunes_old_finished_jobs(self):
        now = 1_000_000.0
        old_done = _make_job("old", "done", now - 40 * 86400)
        jobs = {"old": old_done}
        self.assertEqual(main._jobs_to_prune(jobs, now, retention_days=30), ["old"])

    def test_keeps_recent_finished_jobs(self):
        now = 1_000_000.0
        recent = _make_job("recent", "done", now - 5 * 86400)
        jobs = {"recent": recent}
        self.assertEqual(main._jobs_to_prune(jobs, now, retention_days=30), [])

    def test_never_prunes_active_jobs_regardless_of_age(self):
        now = 1_000_000.0
        stale_but_active = _make_job("active", "running", now - 90 * 86400)
        stale_but_active.finished_at = None
        jobs = {"active": stale_but_active}
        self.assertEqual(main._jobs_to_prune(jobs, now, retention_days=30), [])

    def test_ignores_jobs_with_no_finished_at(self):
        now = 1_000_000.0
        job = _make_job("nofin", "done", None)
        jobs = {"nofin": job}
        self.assertEqual(main._jobs_to_prune(jobs, now, retention_days=30), [])

    def test_retention_days_zero_or_negative_disables_pruning(self):
        now = 1_000_000.0
        old = _make_job("old", "failed", now - 400 * 86400)
        jobs = {"old": old}
        self.assertEqual(main._jobs_to_prune(jobs, now, retention_days=0), [])
        self.assertEqual(main._jobs_to_prune(jobs, now, retention_days=-5), [])

    def test_prunes_cancelled_and_failed_too(self):
        now = 1_000_000.0
        jobs = {
            "c": _make_job("c", "cancelled", now - 60 * 86400),
            "f": _make_job("f", "failed", now - 60 * 86400),
        }
        self.assertEqual(sorted(main._jobs_to_prune(jobs, now, retention_days=30)), ["c", "f"])


class PruneOldEncodeJobsTests(unittest.IsolatedAsyncioTestCase):
    async def test_removes_pruned_jobs_from_memory_and_db(self):
        now = time.time()
        old = _make_job("old1", "done", now - 40 * 86400)
        recent = _make_job("recent1", "done", now - 1 * 86400)
        main._encode_jobs.clear()
        main._encode_jobs.update({"old1": old, "recent1": recent})

        with unittest.mock.patch.object(main, "ENCODE_JOB_RETENTION_DAYS", 30), \
             unittest.mock.patch.object(main, "_delete_encode_job_db", new=unittest.mock.AsyncMock()) as mock_delete:
            count = await main._prune_old_encode_jobs()

        self.assertEqual(count, 1)
        self.assertNotIn("old1", main._encode_jobs)
        self.assertIn("recent1", main._encode_jobs)
        mock_delete.assert_awaited_once_with("old1")


if __name__ == "__main__":
    unittest.main()
