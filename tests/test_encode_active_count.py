import os
import tempfile
import unittest

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


def _make_job(job_id: str, status: str, progress: float = 0.0) -> main.EncodeJob:
    job = main.EncodeJob(job_id, f"/tmp/{job_id}.mkv", f"/tmp/{job_id}-out.mkv", {"lang": "eng"})
    job.status = status
    job.progress = progress
    return job


class EncodeActiveCountTests(unittest.IsolatedAsyncioTestCase):
    async def test_counts_only_running_and_queued_jobs(self):
        jobs = [
            _make_job("r1", "running"),
            _make_job("q1", "queued"),
            _make_job("q2", "queued"),
            _make_job("d1", "done"),
            _make_job("f1", "failed"),
            _make_job("c1", "cancelled"),
        ]
        main._encode_jobs = {j.id: j for j in jobs}
        result = await main.encode_active_count()
        self.assertEqual(result["count"], 3)

    async def test_zero_when_nothing_active(self):
        main._encode_jobs = {"d1": _make_job("d1", "done")}
        result = await main.encode_active_count()
        self.assertEqual(result, {"count": 0, "avg_progress": None})

    async def test_zero_when_no_jobs_at_all(self):
        main._encode_jobs = {}
        result = await main.encode_active_count()
        self.assertEqual(result, {"count": 0, "avg_progress": None})

    async def test_avg_progress_averages_running_and_queued_jobs(self):
        # Queued jobs haven't started so they count as 0 progress, pulling
        # the average down — this is the "how far through this batch" signal.
        jobs = [
            _make_job("r1", "running", progress=80.0),
            _make_job("q1", "queued", progress=0.0),
        ]
        main._encode_jobs = {j.id: j for j in jobs}
        result = await main.encode_active_count()
        self.assertEqual(result["count"], 2)
        self.assertAlmostEqual(result["avg_progress"], 40.0)

    async def test_avg_progress_matches_single_running_job(self):
        main._encode_jobs = {"r1": _make_job("r1", "running", progress=55.5)}
        result = await main.encode_active_count()
        self.assertEqual(result["count"], 1)
        self.assertAlmostEqual(result["avg_progress"], 55.5)


if __name__ == "__main__":
    unittest.main()
