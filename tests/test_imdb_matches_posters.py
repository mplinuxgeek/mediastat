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


class ImdbMatchesPosterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()
        await main._init_tmdb()
        self._safe_path_patch = unittest.mock.patch.object(main, "safe_path", side_effect=lambda p: main.Path(p))
        self._safe_path_patch.start()
        self.addCleanup(self._safe_path_patch.stop)

        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("DELETE FROM file_imdb")
        conn.execute(
            "INSERT INTO file_imdb (path, tconst, primary_title, start_year, source, rating) "
            "VALUES (?,?,?,?,?,?)",
            ("/media/Movies/Movie With Poster.mkv", "tt100", "Movie With Poster", 2020, "imdb", 8.0),
        )
        conn.execute(
            "INSERT INTO file_imdb (path, tconst, primary_title, start_year, source, rating) "
            "VALUES (?,?,?,?,?,?)",
            ("/media/Movies/Movie No Poster.mkv", "tt200", "Movie No Poster", 2021, "imdb", 7.0),
        )
        conn.commit()
        conn.close()

        tdb = sqlite3.connect(main.TMDB_DB_PATH)
        tdb.execute("DELETE FROM tmdb_cache")
        tdb.execute(
            "INSERT INTO tmdb_cache (tconst, poster_path) VALUES (?, ?)",
            ("tt100", "/abc123.jpg"),
        )
        tdb.commit()
        tdb.close()

    async def test_includes_poster_path_when_tmdb_cache_has_one(self):
        result = await main.imdb_matches(dir="/media/Movies")
        info = result["/media/Movies/Movie With Poster.mkv"]
        self.assertEqual(info["poster_path"], "/abc123.jpg")

    async def test_poster_path_is_none_when_not_in_tmdb_cache(self):
        result = await main.imdb_matches(dir="/media/Movies")
        info = result["/media/Movies/Movie No Poster.mkv"]
        self.assertIsNone(info["poster_path"])

    async def test_still_works_when_tmdb_db_is_missing(self):
        os.unlink(main.TMDB_DB_PATH)
        result = await main.imdb_matches(dir="/media/Movies")
        self.assertEqual(len(result), 2)
        self.assertIsNone(result["/media/Movies/Movie With Poster.mkv"]["poster_path"])


if __name__ == "__main__":
    unittest.main()
