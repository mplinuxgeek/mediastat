import asyncio
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))
os.environ["DB_PATH"] = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name

import main  # noqa: E402


class AutoTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await main.init_db()
        await main._auto_db_init()
        main._auto_finalize_failures.clear()
        self.tmp = Path(tempfile.mkdtemp())
        self.src = self.tmp / "movie.mkv"
        self.src.write_bytes(b"s" * 1000)
        st = self.src.stat()
        self.row = {"path": str(self.src), "size": st.st_size, "mtime": int(st.st_mtime), "qp": 18, "est_bytes": 400, "state": "queued"}
        await main._auto_set(self.row["path"], self.row["size"], self.row["mtime"], "queued", qp=18, est_bytes=400)
        self.out = self.tmp / "out.mkv"
        self.out.write_bytes(b"o" * 300)
        self.job = main.EncodeJob("j1", str(self.src), str(self.out), {"lang": "all"})
        self.job.status = "done"

    async def _state(self):
        return (await main._auto_rows("WHERE path=?", (str(self.src),)))[0]

    def _probe(self, codec="hevc"):
        async def fake(p):
            return {"ok": True, "duration_sec": 100.0, "media_info": {"video_codec": codec, "height": 1080}, "a_streams": [1], "s_streams": []}
        return unittest.mock.patch.object(main, "_probe_video", fake)

    async def test_finalize_error_keeps_output_and_retries(self):
        with self._probe(), unittest.mock.patch.object(main.auto_encode, "replace_original", side_effect=OSError("No space left")), \
                unittest.mock.patch.object(main, "_auto_notify", unittest.mock.AsyncMock()):
            await main._auto_finalize(self.row, self.job)
        r = await self._state()
        assert r["state"] == "queued" and "will retry" in r["reason"]
        assert self.out.exists()

    async def test_finalize_gives_up_after_retries_but_keeps_file(self):
        main._auto_finalize_failures[str(self.src)] = main._AUTO_FINALIZE_RETRIES - 1
        with self._probe(), unittest.mock.patch.object(main.auto_encode, "replace_original", side_effect=OSError("boom")), \
                unittest.mock.patch.object(main, "_auto_notify", unittest.mock.AsyncMock()) as notify:
            await main._auto_finalize(self.row, self.job)
        r = await self._state()
        assert r["state"] == "verify_failed" and str(self.out) in r["reason"]
        assert self.out.exists() and notify.await_count == 1

    async def test_verify_rejection_deletes_output(self):
        with self._probe(codec="h264"), unittest.mock.patch.object(main, "_auto_notify", unittest.mock.AsyncMock()):
            await main._auto_finalize(self.row, self.job)
        assert (await self._state())["state"] == "verify_failed"
        assert not self.out.exists()

    async def test_scan_skips_in_flight_and_prunes_missing(self):
        await main._auto_set(str(self.tmp / "gone.mkv"), 5, 5, "pending")
        main._AUTO["dirs"] = [str(self.tmp)]
        with unittest.mock.patch.object(main.auto_encode, "find_candidates",
                                        return_value=[{"path": str(self.src), "size": 999, "mtime": 1}]):
            await main._auto_scan()
        rows = {r["path"]: r for r in await main._auto_rows()}
        assert rows[str(self.src)]["state"] == "queued" and rows[str(self.src)]["size"] == 1000
        assert str(self.tmp / "gone.mkv") not in rows

    async def test_queue_waits_for_scratch_space(self):
        main._AUTO["scratch"] = str(self.tmp)
        with unittest.mock.patch.object(main.shutil, "disk_usage", lambda p: type("U", (), {"free": 10})()), \
                unittest.mock.patch.object(main, "_queue_file_encode") as q:
            await main._auto_queue({**self.row, "state": "would_encode"}, 18, 400, "DV note", {"qp": 18})
        q.assert_not_called()
        r = await self._state()
        assert r["state"] == "would_encode" and "waiting for scratch space" in r["reason"] and r["reason"].startswith("DV note")


class RestoreExcludeTests(AutoTests):
    class _Req:
        headers = {"X-Delete-Token": main.DELETE_TOKEN}
        def __init__(self, body): self._b = body
        async def json(self): return self._b

    async def test_restore_puts_original_back_and_excludes(self):
        final, trashed = main.auto_encode.replace_original(self.out, self.src)
        await main._auto_set(str(final), 300, int(final.stat().st_mtime), "replaced", saved_bytes=700, trashed=str(trashed))
        with unittest.mock.patch.object(main.auto_encode, "plex_refresh", return_value="ok"):
            await main.auto_restore(self._Req({"path": str(final)}))
        assert self.src.read_bytes() == b"s" * 1000
        r = await self._state()
        assert r["state"] == "excluded" and r["size"] == 1000

    async def test_restore_without_trash_is_409(self):
        await main._auto_set(str(self.src), 300, 1, "replaced", reason="original kept in 20240101-000000; x")
        with self.assertRaises(main.HTTPException) as ctx:
            await main.auto_restore(self._Req({"path": str(self.src)}))
        assert ctx.exception.status_code == 409

    async def test_exclude_and_include(self):
        await main._auto_set(str(self.src), 1000, 1, "pending")
        await main.auto_exclude(self._Req({"path": str(self.src)}))
        assert (await self._state())["state"] == "excluded"
        # rescan with changed size/mtime must not revive it
        with unittest.mock.patch.object(main.auto_encode, "find_candidates", return_value=[{"path": str(self.src), "size": 5, "mtime": 9}]):
            await main._auto_scan()
        assert (await self._state())["state"] == "excluded"
        await main.auto_exclude(self._Req({"path": str(self.src), "excluded": False}))
        assert (await self._state())["state"] == "pending"
