import unittest
from pathlib import Path


ROOT = Path(__file__).parent


class ContainerPackagingTests(unittest.TestCase):
    def test_main_image_copies_cropdetect_utils(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("COPY cropdetect_utils.py .", dockerfile)

    def test_main_image_copies_encode_stream_selection(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("COPY encode_stream_selection.py .", dockerfile)

    def test_main_image_copies_encode_output_resolution(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("COPY encode_output_resolution.py .", dockerfile)

    def test_ha_addon_image_copies_cropdetect_utils(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("COPY cropdetect_utils.py .", dockerfile)

    def test_ha_addon_image_copies_encode_stream_selection(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("COPY encode_stream_selection.py .", dockerfile)

    def test_ha_addon_image_copies_encode_output_resolution(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("COPY encode_output_resolution.py .", dockerfile)

    def test_main_image_copies_encode_estimate(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("COPY encode_estimate.py .", dockerfile)

    def test_ha_addon_image_copies_encode_estimate(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("COPY encode_estimate.py .", dockerfile)

    def test_main_image_has_healthcheck(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("HEALTHCHECK", dockerfile)
        self.assertIn("/healthz", dockerfile)

    def test_ha_addon_image_has_healthcheck(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("HEALTHCHECK", dockerfile)
        self.assertIn("/healthz", dockerfile)


if __name__ == "__main__":
    unittest.main()
