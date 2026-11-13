import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class DirFlatteningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        self.old_root = main.current_root
        self.old_real_root = main._real_root
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root_path = Path(self._tmpdir.name)
        main.current_root = self.root_path
        main._real_root = self.root_path

        # Clear listings cache
        main._dir_listing_cache.clear()

    async def asyncTearDown(self):
        main.current_root = self.old_root
        main._real_root = self.old_real_root
        self._tmpdir.cleanup()


    async def test_resolve_flattened_dir_contents(self):
        # 1. Direct file
        f_direct = self.root_path / "direct.mkv"
        f_direct.write_bytes(b"0")

        # 2. Movie folder (1-level-deep with only media files)
        movie_dir = self.root_path / "Awesome Movie (2020)"
        movie_dir.mkdir()
        f_movie = movie_dir / "movie.mp4"
        f_movie.write_bytes(b"0")

        # 3. TV show folder (contains further subdirectories, should NOT be flattened)
        show_dir = self.root_path / "Breaking Bad"
        show_dir.mkdir()
        season_dir = show_dir / "Season 1"
        season_dir.mkdir()
        f_ep = season_dir / "s01e01.mkv"
        f_ep.write_bytes(b"0")

        # 4. Season folder (1-level-deep, matches Season pattern, should NOT be flattened)
        season_dir_flat = self.root_path / "Season 02"
        season_dir_flat.mkdir()
        f_season_ep = season_dir_flat / "ep01.mkv"
        f_season_ep.write_bytes(b"0")

        # Resolve
        remaining_dirs, mkv_files, show_dirs = await main._resolve_flattened_dir_contents(self.root_path)

        # Remaining dirs should be "Breaking Bad" and "Season 02"
        self.assertEqual(len(remaining_dirs), 2)
        dir_names = {d.name for d in remaining_dirs}
        self.assertIn("Breaking Bad", dir_names)
        self.assertIn("Season 02", dir_names)

        # show_dirs should contain both "Breaking Bad" and "Season 02"
        self.assertIn("Breaking Bad", show_dirs)
        self.assertIn("Season 02", show_dirs)

        # Files list should contain "direct.mkv" and "movie.mp4" (flattened), but NOT "ep01.mkv"
        self.assertEqual(len(mkv_files), 2)
        paths = [e.path for e in mkv_files]
        self.assertIn(str(f_direct), paths)
        self.assertIn(str(f_movie), paths)
        self.assertNotIn(str(f_season_ep), paths)

        # 5. Resolve under show_dir ("Breaking Bad"). Since "Season 1" is inside it,
        # and "Season 1" is a season folder, it should NOT be flattened.
        remaining_show_dirs, show_mkv_files, show_show_dirs = await main._resolve_flattened_dir_contents(show_dir)
        self.assertEqual(len(remaining_show_dirs), 1)
        self.assertEqual(remaining_show_dirs[0].name, "Season 1")
        self.assertIn("Season 1", show_show_dirs)
        self.assertEqual(len(show_mkv_files), 0)



    async def test_probe_file_path_aware(self):
        # Test _probe_file on a flattened movie file
        movie_dir = self.root_path / "Awesome Movie (2020)"
        movie_dir.mkdir()
        f_movie = movie_dir / "movie.mp4"
        f_movie.write_bytes(b"0")

        # Insert metadata stub in DB
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            await db.execute(
                "INSERT INTO file_meta (path, size, video_codec) VALUES (?, ?, ?)",
                (str(f_movie), 12345, "h264")
            )
            await db.commit()

        # Probe without dir_path (default)
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            meta = await main._probe_file(db, f_movie)
            self.assertEqual(meta["name"], "movie.mp4")
            self.assertEqual(meta["stem"], "movie")

            # Probe with dir_path (should flatten name and stem)
            meta_flat = await main._probe_file(db, f_movie, self.root_path)
            self.assertEqual(meta_flat["name"], "Awesome Movie (2020)/movie.mp4")
            self.assertEqual(meta_flat["stem"], "Awesome Movie (2020)/movie")

            # Probe with dir_path and matching file stem name
            f_movie_match = movie_dir / "Awesome Movie (2020).mp4"
            f_movie_match.write_bytes(b"0")
            await db.execute(
                "INSERT INTO file_meta (path, size, video_codec) VALUES (?, ?, ?)",
                (str(f_movie_match), 12345, "h264")
            )
            await db.commit()

            meta_match = await main._probe_file(db, f_movie_match, self.root_path)
            self.assertEqual(meta_match["name"], "Awesome Movie (2020)/Awesome Movie (2020).mp4")
            self.assertEqual(meta_match["stem"], "Awesome Movie (2020)")

    async def test_scan_dir_with_flattening(self):
        # Movie folder (1-level-deep with only media files)
        movie_dir = self.root_path / "Awesome Movie (2020)"
        movie_dir.mkdir()
        f_movie = movie_dir / "movie.mp4"
        f_movie.write_bytes(b"0")

        # Insert metadata stub in DB
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            await db.execute(
                "INSERT INTO file_meta (path, size, video_codec) VALUES (?, ?, ?)",
                (str(f_movie), 12345, "h264")
            )
            await db.commit()

        contents, cached = await main.scan_dir(self.root_path)

        # "Awesome Movie (2020)" folder should NOT be in subdirs
        self.assertEqual(len(contents["subdirs"]), 0)
        self.assertEqual(contents["file_count"], 1)

        # Check cached files have path-aware name and stem
        self.assertEqual(len(cached), 1)
        self.assertEqual(cached[0]["name"], "Awesome Movie (2020)/movie.mp4")
        self.assertEqual(cached[0]["stem"], "Awesome Movie (2020)/movie")

    async def test_dir_check_with_flattening(self):
        # Movie folder (1-level-deep with only media files)
        movie_dir = self.root_path / "Awesome Movie (2020)"
        movie_dir.mkdir()
        f_movie = movie_dir / "movie.mp4"
        f_movie.write_bytes(b"0")

        # Initially, file not in DB -> dir_check should return changed = True
        res = await main.dir_check(str(self.root_path))
        self.assertTrue(res["changed"])

        # Insert metadata stub in DB
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            await db.execute(
                "INSERT INTO file_meta (path, size, video_codec) VALUES (?, ?, ?)",
                (str(f_movie), 12345, "h264")
            )
            await db.commit()

        # Listing cache might be warm, clear it
        main._dir_listing_cache.clear()

        # Now, file is in DB -> dir_check should return changed = False
        res2 = await main.dir_check(str(self.root_path))
        self.assertFalse(res2["changed"])

        # Delete the file -> dir_check should return changed = True
        f_movie.unlink()
        main._dir_listing_cache.clear()
        res3 = await main.dir_check(str(self.root_path))
        self.assertTrue(res3["changed"])


if __name__ == "__main__":
    unittest.main()
