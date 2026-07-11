import unittest
from pathlib import Path


ROOT = Path(__file__).parent.parent


class ContainerPackagingTests(unittest.TestCase):
    def test_main_image_copies_src(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertTrue(
            "COPY src/ /app/" in dockerfile or "COPY src/ ." in dockerfile or "COPY src/ ./" in dockerfile,
            "Dockerfile should copy src directory to /app"
        )

    def test_ha_addon_image_copies_src(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertTrue(
            "COPY src/ /app/" in dockerfile or "COPY src/ ." in dockerfile or "COPY src/ ./" in dockerfile,
            "ha-addon/Dockerfile should copy src directory to /app"
        )

    def test_main_image_has_healthcheck(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("HEALTHCHECK", dockerfile)
        self.assertIn("/healthz", dockerfile)

    def test_ha_addon_image_has_healthcheck(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("HEALTHCHECK", dockerfile)
        self.assertIn("/healthz", dockerfile)

    def test_main_image_labels_git_sha_and_build_date(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertIn("ARG GIT_SHA", dockerfile)
        self.assertIn("ARG BUILD_DATE", dockerfile)
        self.assertIn("org.opencontainers.image.revision", dockerfile)

    def test_ha_addon_image_labels_git_sha_and_build_date(self):
        dockerfile = (ROOT / "ha-addon" / "Dockerfile").read_text()
        self.assertIn("ARG GIT_SHA", dockerfile)
        self.assertIn("ARG BUILD_DATE", dockerfile)
        self.assertIn("org.opencontainers.image.revision", dockerfile)


if __name__ == "__main__":
    unittest.main()
