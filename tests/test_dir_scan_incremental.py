import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import aiosqlite  # noqa: E402
import main  # noqa: E402


class ScanPlanTests(unittest.IsolatedAsyncioTestCase):
    """A rescan only has to touch files that are new, changed or gone."""

    async def asyncSetUp(self):
        self._old_db_path = main.DB_PATH
        self._db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        main.DB_PATH = self._db.name
        await main.init_db()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        main._dir_listing_cache.clear()

    async def asyncTearDown(self):
        main.DB_PATH = self._old_db_path
        self._tmpdir.cleanup()
        os.unlink(self._db.name)

    async def _cache(self, path: Path, mtime: float | None = None):
        async with aiosqlite.connect(main.DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO file_meta (path, size, mtime) VALUES (?, ?, ?)",
                (str(path), 1, path.stat().st_mtime if mtime is None else mtime),
            )
            await db.commit()

    def _touch(self, rel: str) -> Path:
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"0")
        return p

    async def test_unchanged_files_need_nothing(self):
        for name in ("a.mkv", "b.mkv"):
            await self._cache(self._touch(name))
        to_probe, removed = await main._scan_plan(self.root)
        self.assertEqual(to_probe, [])
        self.assertEqual(removed, [])

    async def test_only_new_and_modified_files_are_probed(self):
        await self._cache(self._touch("same.mkv"))
        modified = self._touch("modified.mkv")
        await self._cache(modified, mtime=modified.stat().st_mtime - 100)
        new = self._touch("new.mkv")
        to_probe, removed = await main._scan_plan(self.root)
        self.assertEqual(sorted(e.path for e in to_probe), sorted([str(modified), str(new)]))
        self.assertEqual(removed, [])

    async def test_vanished_files_are_reported_removed(self):
        await self._cache(self._touch("keep.mkv"))
        gone = self._touch("gone.mkv")
        await self._cache(gone)
        gone.unlink()
        to_probe, removed = await main._scan_plan(self.root)
        self.assertEqual(to_probe, [])
        self.assertEqual(removed, [str(gone)])

    async def test_removed_ignores_rows_owned_by_a_shown_subfolder(self):
        # "Show/" keeps its own listing (it has a subfolder), so its cached
        # rows belong to that listing, not to the parent's.
        nested = self._touch("Show/Season 1/e1.mkv")
        await self._cache(nested)
        direct_child_of_show = self.root / "Show" / "old.mkv"
        async with aiosqlite.connect(main.DB_PATH) as db:
            await db.execute("INSERT INTO file_meta (path, size, mtime) VALUES (?, 1, 1)", (str(direct_child_of_show),))
            await db.commit()
        to_probe, removed = await main._scan_plan(self.root)
        self.assertEqual(to_probe, [])
        self.assertEqual(removed, [])

    async def test_flattened_movie_folder_files_are_tracked(self):
        inside = self._touch("Movie (2020)/movie.mkv")
        await self._cache(inside)
        stale = self.root / "Movie (2020)" / "sample.mkv"
        async with aiosqlite.connect(main.DB_PATH) as db:
            await db.execute("INSERT INTO file_meta (path, size, mtime) VALUES (?, 1, 1)", (str(stale),))
            await db.commit()
        to_probe, removed = await main._scan_plan(self.root)
        self.assertEqual(to_probe, [])
        self.assertEqual(removed, [str(stale)])


if __name__ == "__main__":
    unittest.main()
