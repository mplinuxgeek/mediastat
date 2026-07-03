import os
import sqlite3
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


# These tests force cancellation deterministically by mocking is_set() to
# always return True, rather than pre-setting the real Event and calling the
# thread function synchronously — each thread function clears its own event
# as its very first statement (to reset stale state from a prior run), which
# would immediately wipe out a flag set before the call.


class DbCleanCancelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        main._dbclean_cancel_event.clear()
        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("DELETE FROM file_meta")
        conn.execute(
            "INSERT INTO file_meta (path, size) VALUES (?, ?)",
            ("/tmp/does-not-exist-dbclean-test.mkv", 1000),
        )
        conn.commit()
        conn.close()

    async def test_cancel_flag_stops_scan_and_skips_deletion(self):
        with unittest.mock.patch.object(main._dbclean_cancel_event, "is_set", return_value=True):
            await main.asyncio.to_thread(main._dbclean_thread)

        self.assertEqual(main._dbclean_progress["phase"], "cancelled")

        conn = sqlite3.connect(main.DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM file_meta").fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)  # row for the missing path was NOT removed

    async def test_without_cancel_it_runs_to_completion(self):
        await main.asyncio.to_thread(main._dbclean_thread)

        self.assertEqual(main._dbclean_progress["phase"], "done")
        conn = sqlite3.connect(main.DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM file_meta").fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)  # missing-path row was removed as expected


class SetDatesCancelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        main._setdates_cancel_event.clear()
        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("DELETE FROM file_imdb")
        conn.execute(
            "INSERT INTO file_imdb (path, tconst, start_year) VALUES (?, ?, ?)",
            ("/tmp/does-not-exist-setdates-test.mkv", "tt0000001", 2020),
        )
        conn.commit()
        conn.close()

    async def test_cancel_flag_stops_before_any_work(self):
        with unittest.mock.patch.object(main._setdates_cancel_event, "is_set", return_value=True):
            await main.asyncio.to_thread(main._setdates_thread)

        self.assertEqual(main._setdates_progress["phase"], "cancelled")
        self.assertEqual(main._setdates_progress["updated"], 0)
        self.assertEqual(main._setdates_progress["errors"], 0)


class ImdbScanCancelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        main._imdbscan_cancel_event.clear()
        self._tmpdir = tempfile.TemporaryDirectory()
        (main.Path(self._tmpdir.name) / "Some Movie (2020).mkv").write_bytes(b"0")
        self._orig_root = main.current_root
        main.current_root = main.Path(self._tmpdir.name)

    async def asyncTearDown(self):
        main.current_root = self._orig_root
        self._tmpdir.cleanup()

    async def test_cancel_flag_stops_during_search_phase(self):
        with unittest.mock.patch.object(main._imdbscan_cancel_event, "is_set", return_value=True):
            await main.asyncio.to_thread(main._imdbscan_thread, False, True)

        self.assertEqual(main._imdbscan_progress["phase"], "cancelled")
        # Nothing should have been written since the scan never reached matching/writing
        self.assertEqual(main._imdbscan_progress["auto"], 0)


if __name__ == "__main__":
    unittest.main()
