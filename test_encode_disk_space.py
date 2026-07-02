import asyncio
import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


class TotalSizeTests(unittest.TestCase):
    def test_sums_sizes_of_existing_files(self):
        with tempfile.NamedTemporaryFile() as f1, tempfile.NamedTemporaryFile() as f2:
            f1.write(b"a" * 100)
            f1.flush()
            f2.write(b"b" * 250)
            f2.flush()
            total = main._total_size([main.Path(f1.name), main.Path(f2.name)])
        self.assertEqual(total, 350)

    def test_skips_files_that_raise_oserror(self):
        total = main._total_size([main.Path("/tmp/does-not-exist-at-all.mkv")])
        self.assertEqual(total, 0)

    def test_empty_list_returns_zero(self):
        self.assertEqual(main._total_size([]), 0)


class CheckFreeSpaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_raises_507_when_free_space_below_required(self):
        with unittest.mock.patch.object(
            main.shutil, "disk_usage", return_value=unittest.mock.Mock(total=1000, used=900, free=100),
        ), unittest.mock.patch.object(main, "_total_size", return_value=500):
            with self.assertRaises(main.HTTPException) as ctx:
                await main._check_free_space(main.Path("/tmp"), [main.Path("/tmp/movie.mkv")])
        self.assertEqual(ctx.exception.status_code, 507)
        self.assertIn("Not enough free space", ctx.exception.detail)

    async def test_does_not_raise_when_free_space_sufficient(self):
        with unittest.mock.patch.object(
            main.shutil, "disk_usage", return_value=unittest.mock.Mock(total=1000, used=100, free=900),
        ), unittest.mock.patch.object(main, "_total_size", return_value=500):
            await main._check_free_space(main.Path("/tmp"), [main.Path("/tmp/movie.mkv")])

    async def test_disk_usage_failure_is_best_effort_and_does_not_raise(self):
        with unittest.mock.patch.object(main.shutil, "disk_usage", side_effect=OSError("no such volume")):
            await main._check_free_space(main.Path("/tmp"), [main.Path("/tmp/movie.mkv")])


if __name__ == "__main__":
    unittest.main()
