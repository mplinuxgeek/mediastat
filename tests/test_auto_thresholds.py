import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))
os.environ["DB_PATH"] = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name

import aiosqlite  # noqa: E402
import main  # noqa: E402

GB = 10 ** 9


class AutoThresholdTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._patches = [
            unittest.mock.patch.object(main, "DB_PATH", self._db.name),
            unittest.mock.patch.dict(main._AUTO, {"min_bytes": 15 * GB, "max_qp": 20}),
        ]
        for p in self._patches:
            p.start()
        await main.init_db()
        await main._auto_db_init()

    async def asyncTearDown(self):
        for p in self._patches:
            p.stop()
        os.unlink(self._db.name)

    async def _states(self):
        return {r["path"]: r["state"] for r in await main._auto_rows()}

    async def test_raising_min_size_drops_only_undecided_rows_below_it(self):
        await main._auto_set("/m/small-pending.mkv", 16 * GB, 1, "pending")
        await main._auto_set("/m/small-would.mkv", 18 * GB, 1, "would_encode", qp=18)
        await main._auto_set("/m/big-pending.mkv", 40 * GB, 1, "pending")
        await main._auto_set("/m/small-replaced.mkv", 16 * GB, 1, "replaced")
        await main._auto_set("/m/small-excluded.mkv", 16 * GB, 1, "excluded")
        await main._auto_set("/m/small-failed.mkv", 16 * GB, 1, "failed")

        dropped = await main._auto_set_thresholds(min_gb=25)

        self.assertEqual(dropped, 2)
        self.assertEqual(main._AUTO["min_bytes"], 25 * GB)
        self.assertEqual(await self._states(), {
            "/m/big-pending.mkv": "pending",
            "/m/small-replaced.mkv": "replaced",
            "/m/small-excluded.mkv": "excluded",
            "/m/small-failed.mkv": "failed",
        })

    async def test_lowering_min_size_drops_nothing(self):
        await main._auto_set("/m/a.mkv", 16 * GB, 1, "pending")
        self.assertEqual(await main._auto_set_thresholds(min_gb=5), 0)
        self.assertEqual(main._AUTO["min_bytes"], 5 * GB)
        self.assertEqual(await self._states(), {"/m/a.mkv": "pending"})

    async def test_values_are_clamped(self):
        await main._auto_set_thresholds(min_gb=0, max_qp=99)
        self.assertEqual(main._AUTO["min_bytes"], int(main._AUTO_MIN_GB_RANGE[0] * GB))
        self.assertEqual(main._AUTO["max_qp"], main._AUTO_MAX_QP_RANGE[1])
        await main._auto_set_thresholds(max_qp=1)
        self.assertEqual(main._AUTO["max_qp"], main._AUTO_MAX_QP_RANGE[0])

    async def test_max_qp_alone_leaves_size_and_rows_untouched(self):
        await main._auto_set("/m/a.mkv", 16 * GB, 1, "pending")
        self.assertEqual(await main._auto_set_thresholds(max_qp=18), 0)
        self.assertEqual(main._AUTO["max_qp"], 18)
        self.assertEqual(main._AUTO["min_bytes"], 15 * GB)
        self.assertEqual(await self._states(), {"/m/a.mkv": "pending"})

    async def test_saved_thresholds_survive_a_restart_and_override_env(self):
        await main._auto_set_thresholds(min_gb=8, max_qp=22)
        await main._auto_save_settings()
        main._AUTO.update(min_bytes=15 * GB, max_qp=20)  # what a fresh process reads from the environment
        await main._auto_load_settings()
        self.assertEqual(main._AUTO["min_bytes"], 8 * GB)
        self.assertEqual(main._AUTO["max_qp"], 22)


if __name__ == "__main__":
    unittest.main()
