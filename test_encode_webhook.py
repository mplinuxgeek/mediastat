import json
import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


def _make_job(input_name="Movie.mkv", output_name="Movie (qp18).mkv", error=None) -> main.EncodeJob:
    job = main.EncodeJob("job1", f"/media/{input_name}", f"/media/{output_name}", {"lang": "eng"})
    job.error = error
    return job


class BuildWebhookPayloadTests(unittest.TestCase):
    def test_generic_done_payload(self):
        job = _make_job()
        body, headers = main._build_webhook_payload("generic", "done", job)
        payload = json.loads(body)
        self.assertEqual(payload["event"], "done")
        self.assertEqual(payload["job_id"], "job1")
        self.assertEqual(payload["input_name"], "Movie.mkv")
        self.assertIsNone(payload["error"])
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_generic_failed_payload_includes_error(self):
        job = _make_job(error="ffmpeg exited with code 1")
        body, _ = main._build_webhook_payload("generic", "failed", job)
        payload = json.loads(body)
        self.assertEqual(payload["event"], "failed")
        self.assertEqual(payload["error"], "ffmpeg exited with code 1")

    def test_discord_payload_wraps_message_in_content_field(self):
        job = _make_job()
        body, headers = main._build_webhook_payload("discord", "done", job)
        payload = json.loads(body)
        self.assertIn("Movie.mkv", payload["content"])
        self.assertIn("finished", payload["content"])
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_discord_failed_message_includes_error_text(self):
        job = _make_job(error="disk full")
        body, _ = main._build_webhook_payload("discord", "failed", job)
        payload = json.loads(body)
        self.assertIn("disk full", payload["content"])

    def test_ntfy_payload_is_plain_text_with_title_header(self):
        job = _make_job()
        body, headers = main._build_webhook_payload("ntfy", "done", job)
        self.assertIn("Movie.mkv", body.decode())
        self.assertEqual(headers["Title"], "mediastat")


class SendEncodeWebhookTests(unittest.IsolatedAsyncioTestCase):
    async def test_does_nothing_when_no_webhook_url_configured(self):
        with unittest.mock.patch.object(main, "WEBHOOK_URL", ""), \
             unittest.mock.patch.object(main, "_post_webhook_sync") as mock_post:
            await main._send_encode_webhook("done", _make_job())
        mock_post.assert_not_called()

    async def test_posts_to_configured_url_when_set(self):
        with unittest.mock.patch.object(main, "WEBHOOK_URL", "https://example.invalid/hook"), \
             unittest.mock.patch.object(main, "WEBHOOK_STYLE", "generic"), \
             unittest.mock.patch.object(main, "_post_webhook_sync") as mock_post:
            await main._send_encode_webhook("done", _make_job())
        mock_post.assert_called_once()
        self.assertEqual(mock_post.call_args[0][0], "https://example.invalid/hook")

    async def test_swallows_exceptions_from_the_post(self):
        with unittest.mock.patch.object(main, "WEBHOOK_URL", "https://example.invalid/hook"), \
             unittest.mock.patch.object(main, "_post_webhook_sync", side_effect=OSError("network down")):
            await main._send_encode_webhook("done", _make_job())  # must not raise


if __name__ == "__main__":
    unittest.main()
