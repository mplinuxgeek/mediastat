import os
import tempfile
import unittest
import unittest.mock
from collections import OrderedDict

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class SaveEstimateHistoryTests(unittest.TestCase):
    def test_stores_a_copy_of_the_state_keyed_by_path(self):
        history = OrderedDict()
        state = {"status": "done", "suggested_qp": 20}
        main._save_estimate_history(history, "/media/a.mkv", state, max_size=5)
        self.assertEqual(history["/media/a.mkv"], {"status": "done", "suggested_qp": 20})

    def test_stored_state_is_a_copy_not_a_reference(self):
        history = OrderedDict()
        state = {"status": "done", "results": [1, 2, 3]}
        main._save_estimate_history(history, "/media/a.mkv", state, max_size=5)
        state["results"].append(4)
        self.assertEqual(history["/media/a.mkv"]["results"], [1, 2, 3])

    def test_evicts_oldest_entry_once_over_max_size(self):
        history = OrderedDict()
        for i in range(5):
            main._save_estimate_history(history, f"/media/{i}.mkv", {"status": "done"}, max_size=5)
        self.assertEqual(list(history.keys()), [f"/media/{i}.mkv" for i in range(5)])

        main._save_estimate_history(history, "/media/5.mkv", {"status": "done"}, max_size=5)
        self.assertEqual(len(history), 5)
        self.assertNotIn("/media/0.mkv", history)
        self.assertIn("/media/5.mkv", history)

    def test_re_saving_an_existing_path_moves_it_to_most_recent(self):
        history = OrderedDict()
        main._save_estimate_history(history, "/media/a.mkv", {"status": "done"}, max_size=3)
        main._save_estimate_history(history, "/media/b.mkv", {"status": "done"}, max_size=3)
        main._save_estimate_history(history, "/media/a.mkv", {"status": "done", "suggested_qp": 22}, max_size=3)
        self.assertEqual(list(history.keys()), ["/media/b.mkv", "/media/a.mkv"])
        self.assertEqual(history["/media/a.mkv"]["suggested_qp"], 22)


class EstimateHistoryEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_404_when_nothing_cached_for_path(self):
        main._estimate_history.clear()
        with unittest.mock.patch.object(main, "safe_path", side_effect=lambda p: main.Path(p)):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.estimate_history(path="/media/nope.mkv")
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_returns_cached_state_for_matching_path(self):
        main._estimate_history.clear()
        main._estimate_history["/media/found.mkv"] = {"status": "done", "suggested_qp": 18}
        with unittest.mock.patch.object(main, "safe_path", side_effect=lambda p: main.Path(p)):
            result = await main.estimate_history(path="/media/found.mkv")
        self.assertEqual(result, {"status": "done", "suggested_qp": 18})


class LiveEstimateStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_current_live_state_regardless_of_status(self):
        main._estimate_state = {
            "status": "encoding", "results": [{"qp": 16, "bytes": 100}],
            "suggested_qp": None, "warning": None, "error": None,
            "current_qp": 17, "qp_progress": 42.0, "path": "/media/movie.mkv",
        }
        result = await main.live_estimate_state()
        self.assertEqual(result["status"], "encoding")
        self.assertEqual(result["path"], "/media/movie.mkv")
        self.assertEqual(result["current_qp"], 17)

    async def test_returns_idle_when_nothing_has_run(self):
        main._estimate_state = {"status": "idle", "results": [], "suggested_qp": None,
                                 "warning": None, "error": None, "current_qp": None}
        result = await main.live_estimate_state()
        self.assertEqual(result["status"], "idle")


if __name__ == "__main__":
    unittest.main()
