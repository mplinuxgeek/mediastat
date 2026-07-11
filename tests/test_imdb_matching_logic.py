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


class NormTitleTests(unittest.TestCase):
    def test_lowercases_and_strips_apostrophes(self):
        self.assertEqual(main._norm_title("Freddy's Revenge"), "freddys revenge")

    def test_strips_forward_slashes(self):
        self.assertEqual(main._norm_title("Self/less"), "selfless")

    def test_replaces_ampersand_with_and(self):
        self.assertEqual(main._norm_title("Fast & Furious"), "fast and furious")

    def test_replaces_punctuation_with_space_and_collapses_whitespace(self):
        self.assertEqual(main._norm_title("Mission: Impossible - Fallout!"), "mission impossible fallout")

    def test_curly_apostrophe_also_stripped(self):
        self.assertEqual(main._norm_title("Ocean’s Eleven"), "oceans eleven")


class ParseFilenamePyTests(unittest.TestCase):
    def test_extracts_title_and_year_from_parens(self):
        result = main._parse_filename_py("The Matrix (1999).mkv")
        self.assertEqual(result["title"], "The Matrix")
        self.assertEqual(result["year"], 1999)

    def test_extracts_title_and_year_from_brackets(self):
        result = main._parse_filename_py("Inception [2010].mp4")
        self.assertEqual(result["title"], "Inception")
        self.assertEqual(result["year"], 2010)

    def test_no_year_present(self):
        result = main._parse_filename_py("Some Random Movie.mkv")
        self.assertIsNone(result["year"])
        self.assertEqual(result["title"], "Some Random Movie")

    def test_strips_edition_suffix(self):
        result = main._parse_filename_py("Blade Runner (1982) Director's Cut.mkv")
        self.assertEqual(result["title"], "Blade Runner")
        self.assertEqual(result["year"], 1982)


class PickAutoCandidateTests(unittest.TestCase):
    def test_single_exact_title_and_year_match_is_picked(self):
        results = [
            {"tconst": "tt1", "primary_title": "The Matrix", "original_title": None,
             "start_year": 1999, "title_type": "movie", "runtime_minutes": 136},
            {"tconst": "tt2", "primary_title": "The Matrix Reloaded", "original_title": None,
             "start_year": 2003, "title_type": "movie", "runtime_minutes": 138},
        ]
        parsed = {"title": "The Matrix", "year": 1999}
        candidate = main._pick_auto_candidate(results, parsed)
        self.assertEqual(candidate["tconst"], "tt1")

    def test_no_candidate_when_multiple_exact_matches_and_no_runtime(self):
        # Same title/year (e.g. a remake released same year), no way to disambiguate
        results = [
            {"tconst": "tt1", "primary_title": "Overlap", "original_title": None,
             "start_year": 2020, "title_type": "movie", "runtime_minutes": 90},
            {"tconst": "tt2", "primary_title": "Overlap", "original_title": None,
             "start_year": 2020, "title_type": "movie", "runtime_minutes": 200},
        ]
        parsed = {"title": "Overlap", "year": 2020}
        self.assertIsNone(main._pick_auto_candidate(results, parsed))

    def test_runtime_disambiguates_between_exact_matches(self):
        results = [
            {"tconst": "tt1", "primary_title": "Overlap", "original_title": None,
             "start_year": 2020, "title_type": "movie", "runtime_minutes": 90},
            {"tconst": "tt2", "primary_title": "Overlap", "original_title": None,
             "start_year": 2020, "title_type": "movie", "runtime_minutes": 200},
        ]
        parsed = {"title": "Overlap", "year": 2020, "runtime": 92}
        candidate = main._pick_auto_candidate(results, parsed)
        self.assertEqual(candidate["tconst"], "tt1")

    def test_no_match_returns_none(self):
        results = [
            {"tconst": "tt1", "primary_title": "Totally Different", "original_title": None,
             "start_year": 2001, "title_type": "movie", "runtime_minutes": 100},
        ]
        parsed = {"title": "The Matrix", "year": 1999}
        self.assertIsNone(main._pick_auto_candidate(results, parsed))

    def test_prefers_movie_title_type_over_short_or_video(self):
        results = [
            {"tconst": "tt1", "primary_title": "Echo", "original_title": None,
             "start_year": 2015, "title_type": "video", "runtime_minutes": 10},
            {"tconst": "tt2", "primary_title": "Echo", "original_title": None,
             "start_year": 2015, "title_type": "movie", "runtime_minutes": 110},
        ]
        parsed = {"title": "Echo", "year": 2015}
        candidate = main._pick_auto_candidate(results, parsed)
        self.assertEqual(candidate["tconst"], "tt2")

    def test_apostrophe_normalization_matches_across_forms(self):
        results = [
            {"tconst": "tt1", "primary_title": "Ocean's Eleven", "original_title": None,
             "start_year": 2001, "title_type": "movie", "runtime_minutes": 116},
        ]
        parsed = {"title": "Oceans Eleven", "year": 2001}
        candidate = main._pick_auto_candidate(results, parsed)
        self.assertEqual(candidate["tconst"], "tt1")


class ImdbSearchSyncTests(unittest.TestCase):
    """_imdb_search_sync backs both /imdb/search and the scan thread's search
    phase — test it against a real (small, in-memory schema) sqlite DB."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE imdb_titles (
                tconst TEXT PRIMARY KEY, primary_title TEXT, original_title TEXT,
                start_year INTEGER, runtime_minutes INTEGER, genres TEXT,
                norm_title TEXT, norm_original_title TEXT, title_type TEXT
            )
        """)
        self.conn.execute("CREATE TABLE imdb_cast (tconst TEXT, cast_names TEXT)")
        self.conn.execute("CREATE TABLE imdb_ratings (tconst TEXT, average_rating REAL)")
        self.conn.execute(
            "INSERT INTO imdb_titles VALUES (?,?,?,?,?,?,?,?,?)",
            ("tt1", "The Matrix", None, 1999, 136, "Action,Sci-Fi", "the matrix", None, "movie"),
        )
        self.conn.execute(
            "INSERT INTO imdb_titles VALUES (?,?,?,?,?,?,?,?,?)",
            ("tt2", "The Matrix Reloaded", None, 2003, 138, "Action,Sci-Fi", "the matrix reloaded", None, "movie"),
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_finds_exact_normalized_title_match(self):
        results = main._imdb_search_sync("The Matrix", None, None, self.conn)
        tconsts = [r["tconst"] for r in results]
        self.assertIn("tt1", tconsts)

    def test_year_filter_narrows_to_matching_year(self):
        results = main._imdb_search_sync("The Matrix", 1999, None, self.conn)
        self.assertTrue(all(r["start_year"] == 1999 for r in results if r["tconst"] == "tt1"))

    def test_empty_query_returns_no_results(self):
        self.assertEqual(main._imdb_search_sync("", None, None, self.conn), [])

    def test_deduplicates_by_tconst_across_query_strategies(self):
        results = main._imdb_search_sync("The Matrix", None, None, self.conn)
        tconsts = [r["tconst"] for r in results]
        self.assertEqual(len(tconsts), len(set(tconsts)))


class ImdbSearchEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main._init_imdb()
        conn = sqlite3.connect(main.IMDB_DB_PATH)
        conn.execute("DELETE FROM imdb_titles")
        conn.execute(
            "INSERT INTO imdb_titles "
            "(tconst, primary_title, original_title, start_year, runtime_minutes, genres, "
            " norm_title, norm_original_title, title_type) VALUES (?,?,?,?,?,?,?,?,?)",
            ("tt100", "The Matrix", None, 1999, 136, "Action,Sci-Fi", "the matrix", None, "movie"),
        )
        conn.commit()
        conn.close()

    async def test_finds_match_by_normalized_title(self):
        # Explicit year=None/runtime=None: calling the route function directly
        # bypasses FastAPI's Query() default resolution, so the bare defaults
        # would otherwise be Query sentinel objects, not real None.
        results = await main.imdb_search(q="The Matrix", year=None, runtime=None)
        tconsts = [r["tconst"] for r in results]
        self.assertIn("tt100", tconsts)

    async def test_empty_query_returns_empty_list(self):
        self.assertEqual(await main.imdb_search(q="   ", year=None, runtime=None), [])

    async def test_strips_file_extension_from_query(self):
        results = await main.imdb_search(q="The Matrix.mkv", year=None, runtime=None)
        tconsts = [r["tconst"] for r in results]
        self.assertIn("tt100", tconsts)


class ImdbMatchEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._init_imdb()  # file_imdb lives here, not in init_db()
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            await db.execute("DELETE FROM file_imdb")
            await db.commit()

    async def test_inserts_a_match_row(self):
        with unittest.mock.patch.object(main, "safe_path", return_value=main.Path("/media/movie.mkv")):
            await main.imdb_match(_FakeRequest(body={
                "path": "/media/movie.mkv", "tconst": "tt100",
                "primary_title": "The Matrix", "start_year": 1999,
                "genres": "Action,Sci-Fi", "runtime_minutes": 136,
                "embed_meta": False,
            }))

        async with main.aiosqlite.connect(main.DB_PATH) as db:
            db.row_factory = main.aiosqlite.Row
            async with db.execute("SELECT * FROM file_imdb WHERE path = ?", ("/media/movie.mkv",)) as cur:
                row = await cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["tconst"], "tt100")
        self.assertEqual(row["primary_title"], "The Matrix")
        self.assertEqual(row["start_year"], 1999)

    async def test_requires_path_and_tconst(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.imdb_match(_FakeRequest(body={"path": "", "tconst": ""}))
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_set_dates_updates_file_mtime(self):
        with tempfile.NamedTemporaryFile(suffix=".mkv", delete=False) as f:
            f.write(b"0")
            media_path = f.name
        try:
            with unittest.mock.patch.object(main, "safe_path", return_value=main.Path(media_path)):
                await main.imdb_match(_FakeRequest(body={
                    "path": media_path, "tconst": "tt100",
                    "primary_title": "The Matrix", "start_year": 1999,
                    "embed_meta": False, "set_dates": True,
                }))
            mtime = main.Path(media_path).stat().st_mtime
            expected = main.datetime.datetime(1999, 1, 1, 12, 0, 0).timestamp()
            self.assertAlmostEqual(mtime, expected, delta=2)
        finally:
            os.unlink(media_path)


if __name__ == "__main__":
    unittest.main()
