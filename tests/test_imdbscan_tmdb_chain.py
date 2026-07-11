import os
import sqlite3
import tempfile
import threading
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class ImdbScanTmdbChainTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        main._imdbscan_cancel_event.clear()
        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("DELETE FROM imdb_review_queue")
        conn.commit()
        conn.close()
        self._tmpdir = tempfile.TemporaryDirectory()
        (main.Path(self._tmpdir.name) / "Some Movie.mkv").write_bytes(b"0")
        self._orig_root = main.current_root
        main.current_root = main.Path(self._tmpdir.name)
        main._tmdb_cache_progress = {}

    async def asyncTearDown(self):
        main.current_root = self._orig_root
        self._tmpdir.cleanup()

    async def _run_scan(self):
        # Auto-match succeeds for every file (skip_manual doesn't matter here)
        fake_result = [{
            "tconst": "tt1", "primary_title": "Some Movie", "original_title": None,
            "start_year": 2020, "genres": "Drama", "runtime_minutes": 100,
            "average_rating": 7.0, "title_type": "movie",
        }]
        with unittest.mock.patch.object(main, "_imdb_search_sync", return_value=fake_result), \
             unittest.mock.patch.object(main, "_pick_auto_candidate", return_value=fake_result[0]):
            await main.asyncio.to_thread(main._imdbscan_thread, False, True)

    async def test_kicks_off_tmdb_cache_build_when_configured(self):
        done = threading.Event()
        with unittest.mock.patch.object(main, "TMDB_API_KEY", "fake-key"), \
             unittest.mock.patch.object(main, "_tmdb_cache_thread",
                                        side_effect=lambda *a, **k: done.set()) as mock_tmdb:
            await self._run_scan()
            self.assertTrue(done.wait(timeout=2), "background thread never ran")
        mock_tmdb.assert_called_once()

    async def test_does_not_run_when_tmdb_not_configured(self):
        with unittest.mock.patch.object(main, "TMDB_API_KEY", ""), \
             unittest.mock.patch.object(main, "_tmdb_cache_thread") as mock_tmdb:
            await self._run_scan()
        mock_tmdb.assert_not_called()

    async def test_does_not_run_when_a_cache_build_is_already_in_flight(self):
        main._tmdb_cache_progress = {"phase": "fetching"}
        with unittest.mock.patch.object(main, "TMDB_API_KEY", "fake-key"), \
             unittest.mock.patch.object(main, "_tmdb_cache_thread") as mock_tmdb:
            await self._run_scan()
        mock_tmdb.assert_not_called()

    async def test_does_not_run_when_scan_was_cancelled(self):
        with unittest.mock.patch.object(main, "TMDB_API_KEY", "fake-key"), \
             unittest.mock.patch.object(main, "_tmdb_cache_thread") as mock_tmdb, \
             unittest.mock.patch.object(main._imdbscan_cancel_event, "is_set", return_value=True):
            await self._run_scan()
        mock_tmdb.assert_not_called()
        self.assertEqual(main._imdbscan_progress["phase"], "cancelled")


if __name__ == "__main__":
    unittest.main()
