import os
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


def _identity_safe_path(p):
    return main.Path(p)


class RenameFileTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.src = main.Path(self._tmpdir.name) / "Original Name.mkv"
        self.src.write_bytes(b"0")

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.rename_file(_FakeRequest({}), path=str(self.src), new_name="New Name.mkv")
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_rejects_path_separators_in_new_name(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with self.assertRaises(main.HTTPException) as ctx:
            await main.rename_file(_FakeRequest(headers), path=str(self.src), new_name="sub/evil.mkv")
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_renames_file_on_disk_and_returns_new_path(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            result = await main.rename_file(_FakeRequest(headers), path=str(self.src), new_name="New Name.mkv")

        new_path = main.Path(self._tmpdir.name) / "New Name.mkv"
        self.assertTrue(new_path.is_file())
        self.assertFalse(self.src.exists())
        self.assertEqual(result["name"], "New Name.mkv")

    async def test_409_when_target_name_already_exists(self):
        other = main.Path(self._tmpdir.name) / "Taken.mkv"
        other.write_bytes(b"0")
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.rename_file(_FakeRequest(headers), path=str(self.src), new_name="Taken.mkv")
        self.assertEqual(ctx.exception.status_code, 409)

    async def test_404_when_source_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.rename_file(_FakeRequest(headers), path=str(self.src.parent / "nope.mkv"), new_name="x.mkv")
        self.assertEqual(ctx.exception.status_code, 404)


class MoveToFolderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.f1 = main.Path(self._tmpdir.name) / "a.mkv"
        self.f2 = main.Path(self._tmpdir.name) / "b.mkv"
        self.f1.write_bytes(b"0")
        self.f2.write_bytes(b"0")

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.move_to_folder(_FakeRequest({}, {"paths": [str(self.f1)], "folder": "sub"}))
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_moves_files_into_new_subfolder(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            result = await main.move_to_folder(_FakeRequest(headers, {
                "paths": [str(self.f1), str(self.f2)], "folder": "Archive",
            }))
        dest_dir = main.Path(self._tmpdir.name) / "Archive"
        self.assertTrue((dest_dir / "a.mkv").is_file())
        self.assertTrue((dest_dir / "b.mkv").is_file())
        self.assertFalse(self.f1.exists())
        self.assertEqual(len(result["moved"]), 2)

    async def test_moves_files_into_absolute_folder(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        dest_dir = main.Path(self._tmpdir.name) / "AbsoluteDest"
        dest_dir.mkdir()
        with unittest.mock.patch.object(main, "safe_path", side_effect=lambda p: main.Path(p)):
            result = await main.move_to_folder(_FakeRequest(headers, {
                "paths": [str(self.f1), str(self.f2)], "folder": str(dest_dir),
            }))
        self.assertTrue((dest_dir / "a.mkv").is_file())
        self.assertTrue((dest_dir / "b.mkv").is_file())
        self.assertFalse(self.f1.exists())
        self.assertEqual(len(result["moved"]), 2)

    async def test_400_when_paths_or_folder_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with self.assertRaises(main.HTTPException) as ctx:
            await main.move_to_folder(_FakeRequest(headers, {"paths": [], "folder": "x"}))
        self.assertEqual(ctx.exception.status_code, 400)


class DeleteFileTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.target = main.Path(self._tmpdir.name) / "delete-me.mkv"
        self.target.write_bytes(b"0")
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            await db.execute(
                "INSERT INTO file_meta (path, size) VALUES (?, ?)", (str(self.target), 1)
            )
            await db.commit()

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.delete_file(_FakeRequest({}), path=str(self.target))
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_deletes_file_and_db_row(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            await main.delete_file(_FakeRequest(headers), path=str(self.target))

        self.assertFalse(self.target.exists())
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            async with db.execute("SELECT 1 FROM file_meta WHERE path = ?", (str(self.target),)) as cur:
                row = await cur.fetchone()
        self.assertIsNone(row)

    async def test_404_when_file_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.delete_file(_FakeRequest(headers), path=str(self.target.parent / "nope.mkv"))
        self.assertEqual(ctx.exception.status_code, 404)


class DeleteDirTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.target_dir = main.Path(self._tmpdir.name) / "folder"
        self.target_dir.mkdir()
        self.target_file = self.target_dir / "inside.mkv"
        self.target_file.write_bytes(b"0")
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            await db.execute(
                "INSERT INTO file_meta (path, size) VALUES (?, ?)", (str(self.target_file), 1)
            )
            await db.commit()

    async def asyncTearDown(self):
        self._tmpdir.cleanup()

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.delete_dir(_FakeRequest({}), path=str(self.target_dir))
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_deletes_directory_recursively_and_db_rows(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            await main.delete_dir(_FakeRequest(headers), path=str(self.target_dir))

        self.assertFalse(self.target_dir.exists())
        self.assertFalse(self.target_file.exists())
        async with main.aiosqlite.connect(main.DB_PATH) as db:
            async with db.execute("SELECT 1 FROM file_meta WHERE path = ?", (str(self.target_file),)) as cur:
                row = await cur.fetchone()
        self.assertIsNone(row)

    async def test_404_when_directory_missing(self):
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        with unittest.mock.patch.object(main, "safe_path", side_effect=_identity_safe_path):
            with self.assertRaises(main.HTTPException) as ctx:
                await main.delete_dir(_FakeRequest(headers), path=str(self.target_dir / "nope"))
        self.assertEqual(ctx.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
