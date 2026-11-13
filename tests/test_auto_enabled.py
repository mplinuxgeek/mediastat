import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))
os.environ["DB_PATH"] = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name

import main  # noqa: E402

GB = 10 ** 9


class _FakeRequest:
    def __init__(self, body=None, token=True):
        self.headers = {"X-Delete-Token": main.DELETE_TOKEN} if token else {}
        self._body = body if body is not None else {}

    async def json(self):
        return self._body


class AutoEnabledTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._patches = [
            unittest.mock.patch.object(main, "DB_PATH", self._db.name),
            unittest.mock.patch.dict(main._AUTO, {"enabled": False, "dirs": ["/movies"], "paused": False, "dry_run": True}),
            unittest.mock.patch.dict(main._schedule_config, {"enabled": False}),
        ]
        for p in self._patches:
            p.start()
        await main.init_db()
        await main._auto_db_init()
        main._auto_scan_now.clear()

    async def asyncTearDown(self):
        main._auto_scan_now.clear()
        for p in self._patches:
            p.stop()
        os.unlink(self._db.name)

    async def test_saved_switch_survives_a_restart_and_overrides_env(self):
        main._AUTO["enabled"] = True
        await main._auto_save_settings()
        main._AUTO["enabled"] = False  # what a fresh process reads from AUTO_ENCODE=0
        await main._auto_load_settings()
        self.assertTrue(main._AUTO["enabled"])

        main._AUTO["enabled"] = False
        await main._auto_save_settings()
        main._AUTO["enabled"] = True  # AUTO_ENCODE=1
        await main._auto_load_settings()
        self.assertFalse(main._AUTO["enabled"])

    async def test_env_decides_until_the_switch_is_used(self):
        main._AUTO["enabled"] = True
        await main._auto_load_settings()  # nothing saved yet
        self.assertTrue(main._AUTO["enabled"])

    async def test_endpoint_toggles_persists_and_requests_a_scan_on_enable(self):
        r = await main.auto_settings(_FakeRequest({"enabled": True}))
        self.assertTrue(r["enabled"])
        self.assertTrue(main._AUTO["enabled"])
        self.assertTrue(main._auto_scan_now.is_set())

        main._auto_scan_now.clear()
        r = await main.auto_settings(_FakeRequest({"enabled": False}))
        self.assertFalse(r["enabled"])
        self.assertFalse(main._auto_scan_now.is_set())
        self.assertFalse((await main.auto_status())["config"]["enabled"])

        main._AUTO["enabled"] = True  # a fresh process with AUTO_ENCODE=1 still comes up off
        await main._auto_load_settings()
        self.assertFalse(main._AUTO["enabled"])

    async def test_other_settings_leave_the_switch_alone(self):
        main._AUTO["enabled"] = True
        await main.auto_settings(_FakeRequest({"paused": True}))
        self.assertTrue(main._AUTO["enabled"])

    async def test_enabling_without_dirs_is_refused(self):
        main._AUTO["dirs"] = []
        with self.assertRaises(main.HTTPException) as ctx:
            await main.auto_settings(_FakeRequest({"enabled": True}))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertFalse(main._AUTO["enabled"])

    async def test_token_is_required(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.auto_settings(_FakeRequest({"enabled": True}, token=False))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertFalse(main._AUTO["enabled"])

    async def test_disabled_step_starts_no_new_work(self):
        await main._auto_set("/movies/a.mkv", 20 * GB, 1, "pending")
        with unittest.mock.patch.object(main, "_auto_evaluate") as evaluate:
            await main._auto_step()
            evaluate.assert_not_called()
            main._AUTO["enabled"] = True
            await main._auto_step()
            evaluate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
