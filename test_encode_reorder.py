import os
import tempfile
import unittest

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


class ReorderEncodeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        main._encode_queue_list[:] = ["a", "b", "c", "d"]
        self.headers = {"X-Delete-Token": main.DELETE_TOKEN}

    async def test_front_moves_job_to_index_zero(self):
        await main.reorder_encode("c", _FakeRequest(self.headers, {"direction": "front"}))
        self.assertEqual(main._encode_queue_list, ["c", "a", "b", "d"])

    async def test_front_on_already_first_job_is_a_no_op(self):
        await main.reorder_encode("a", _FakeRequest(self.headers, {"direction": "front"}))
        self.assertEqual(main._encode_queue_list, ["a", "b", "c", "d"])

    async def test_front_preserves_relative_order_of_remaining_jobs(self):
        await main.reorder_encode("d", _FakeRequest(self.headers, {"direction": "front"}))
        self.assertEqual(main._encode_queue_list, ["d", "a", "b", "c"])

    async def test_up_and_down_still_work(self):
        await main.reorder_encode("b", _FakeRequest(self.headers, {"direction": "up"}))
        self.assertEqual(main._encode_queue_list, ["b", "a", "c", "d"])
        await main.reorder_encode("b", _FakeRequest(self.headers, {"direction": "down"}))
        self.assertEqual(main._encode_queue_list, ["a", "b", "c", "d"])

    async def test_invalid_direction_rejected(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.reorder_encode("a", _FakeRequest(self.headers, {"direction": "sideways"}))
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_job_not_in_queue_404s(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.reorder_encode("nope", _FakeRequest(self.headers, {"direction": "front"}))
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_requires_delete_token(self):
        with self.assertRaises(main.HTTPException) as ctx:
            await main.reorder_encode("a", _FakeRequest({}, {"direction": "front"}))
        self.assertEqual(ctx.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
