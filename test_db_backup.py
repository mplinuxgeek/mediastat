import os
import sqlite3
import tempfile
import time
import unittest

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class BackupDbSyncTests(unittest.TestCase):
    def setUp(self):
        conn = sqlite3.connect(main.DB_PATH)
        conn.execute("CREATE TABLE IF NOT EXISTS marker (id INTEGER PRIMARY KEY, note TEXT)")
        conn.execute("DELETE FROM marker")
        conn.execute("INSERT INTO marker (note) VALUES ('hello')")
        conn.commit()
        conn.close()
        self._backup_dir = main.Path(main.DB_PATH).parent / "backups"

    def tearDown(self):
        if self._backup_dir.exists():
            for f in self._backup_dir.glob("mediastat-*.db"):
                f.unlink()

    def test_creates_a_real_readable_backup_file(self):
        result = main._backup_db_sync()
        backup_path = main.Path(result["path"])
        self.assertTrue(backup_path.exists())
        self.assertGreater(result["size"], 0)

        conn = sqlite3.connect(str(backup_path))
        note = conn.execute("SELECT note FROM marker").fetchone()[0]
        conn.close()
        self.assertEqual(note, "hello")

    def test_keeps_only_the_most_recent_max_keep_backups(self):
        for _ in range(7):
            main._backup_db_sync(max_keep=3)
            time.sleep(0.01)  # ensure distinct mtimes for ordering
        backups = list(self._backup_dir.glob("mediastat-*.db"))
        self.assertEqual(len(backups), 3)


class DbBackupEndpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._backup_dir = main.Path(main.DB_PATH).parent / "backups"

    def tearDown(self):
        if self._backup_dir.exists():
            for f in self._backup_dir.glob("mediastat-*.db"):
                f.unlink()

    async def test_creates_backup_and_appears_in_listing(self):
        # No X-Delete-Token check here, matching this page's other db actions
        # (/db/clean, /db/clean-cancel) which also have none.
        result = await main.db_backup()
        self.assertIn("path", result)

        listing = await main.list_db_backups()
        names = [b["name"] for b in listing["backups"]]
        self.assertIn(main.Path(result["path"]).name, names)


if __name__ == "__main__":
    unittest.main()
