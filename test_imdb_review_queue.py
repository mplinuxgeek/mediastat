import json
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


class _FakeRequest:
    def __init__(self, headers=None, body=None):
        self.headers = headers or {}
        self._body = body if body is not None else {}

    async def json(self):
        return self._body


class ScanReviewEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("DELETE FROM imdb_review_queue")
        conn.execute(
            "INSERT INTO imdb_review_queue (path, filename, reason, results_json, created_at) "
            "VALUES (?,?,?,?,?)",
            ("/media/Some Movie.mkv", "Some Movie.mkv", "no exact match",
             json.dumps([{"tconst": "tt1", "primary_title": "Some Movie"}]), 1000.0),
        )
        conn.commit()
        conn.close()

    async def test_scan_review_returns_persisted_items(self):
        items = await main.imdb_scan_review()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["path"], "/media/Some Movie.mkv")
        self.assertEqual(items[0]["reason"], "no exact match")
        self.assertEqual(items[0]["results"], [{"tconst": "tt1", "primary_title": "Some Movie"}])

    async def test_skip_removes_the_item(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        await main.imdb_scan_review_skip(_FakeRequest(headers, {"path": "/media/Some Movie.mkv"}))
        items = await main.imdb_scan_review()
        self.assertEqual(items, [])

    async def test_skip_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.imdb_scan_review_skip(_FakeRequest({}, {"path": "/media/Some Movie.mkv"}))
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_skip_requires_path(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with self.assertRaises(main.HTTPException) as ctx:
            await main.imdb_scan_review_skip(_FakeRequest(headers, {}))
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_matching_a_file_removes_it_from_the_review_queue(self):
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path("/media/Some Movie.mkv")), \
             unittest.mock.patch("main._embed_file_meta", new=unittest.mock.AsyncMock()):
            await main.imdb_match(_FakeRequest(body={
                "path": "/media/Some Movie.mkv", "tconst": "tt1",
                "primary_title": "Some Movie", "embed_meta": False,
            }))
        items = await main.imdb_scan_review()
        self.assertEqual(items, [])


class ImdbScanThreadPersistsReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        main._imdbscan_cancel_event.clear()
        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("DELETE FROM imdb_review_queue")
        conn.commit()
        conn.close()
        self._tmpdir = tempfile.TemporaryDirectory()
        (main.Path(self._tmpdir.name) / "Ambiguous Movie.mkv").write_bytes(b"0")
        self._orig_root = main.current_root
        main.current_root = main.Path(self._tmpdir.name)

    async def asyncTearDown(self):
        main.current_root = self._orig_root
        self._tmpdir.cleanup()

    async def test_completed_scan_persists_review_items_to_db(self):
        # Force the file to land in the review list: _imdb_search_sync returns
        # one non-exact-match result, skip_manual=False so it's not discarded.
        fake_result = [{
            "tconst": "tt9", "primary_title": "Totally Different Title",
            "original_title": None, "start_year": 1999, "genres": "Drama",
            "runtime_minutes": 100, "average_rating": 5.0, "title_type": "movie",
        }]
        with unittest.mock.patch.object(main, "_imdb_search_sync", return_value=fake_result), \
             unittest.mock.patch.object(main, "_pick_auto_candidate", return_value=None):
            await main.asyncio.to_thread(main._imdbscan_thread, False, False)

        self.assertEqual(main._imdbscan_progress["phase"], "done")
        conn = sqlite3.connect(main.DB_PATH)
        rows = conn.execute("SELECT path, filename FROM imdb_review_queue").fetchall()
        conn.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "Ambiguous Movie.mkv")


if __name__ == "__main__":
    unittest.main()
