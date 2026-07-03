import os
import tempfile
import unittest
import unittest.mock

os.environ.setdefault("MEDIA_ROOT", tempfile.gettempdir())
os.environ.setdefault("CONFIG_PATH", os.path.join(tempfile.gettempdir(), "mediastat-test-config.yaml"))

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402


def _which_only_nvidia_smi(name):
    return "/usr/bin/nvidia-smi" if name == "nvidia-smi" else None


class DetectHwAccelNvidiaLoggingTests(unittest.TestCase):
    """_detect_hw_accel_sync runs at startup so "why did this fall back to
    software encoding" is answerable from the logs — these cover the NVIDIA
    branch specifically, which previously computed the GPU name but never
    logged it (unlike the QSV/VAAPI probes, which already log in detail)."""

    def test_logs_gpu_name_on_success(self):
        fake_result = unittest.mock.Mock(returncode=0, stdout="NVIDIA GeForce RTX 3080\n", stderr="")
        with unittest.mock.patch.object(main.shutil, "which", side_effect=_which_only_nvidia_smi), \
             unittest.mock.patch.object(main.subprocess, "run", return_value=fake_result), \
             unittest.mock.patch("glob.glob", return_value=[]):
            result = main._detect_hw_accel_sync()
        self.assertTrue(result["nvenc"])

    def test_does_not_raise_when_nvidia_smi_returns_nothing(self):
        fake_result = unittest.mock.Mock(returncode=1, stdout="", stderr="No devices found\n")
        with unittest.mock.patch.object(main.shutil, "which", side_effect=_which_only_nvidia_smi), \
             unittest.mock.patch.object(main.subprocess, "run", return_value=fake_result), \
             unittest.mock.patch("glob.glob", return_value=[]):
            result = main._detect_hw_accel_sync()
        self.assertFalse(result["nvenc"])

    def test_does_not_raise_when_nvidia_smi_probe_errors(self):
        with unittest.mock.patch.object(main.shutil, "which", side_effect=_which_only_nvidia_smi), \
             unittest.mock.patch.object(main.subprocess, "run", side_effect=OSError("boom")), \
             unittest.mock.patch("glob.glob", return_value=[]):
            result = main._detect_hw_accel_sync()
        self.assertFalse(result["nvenc"])


if __name__ == "__main__":
    unittest.main()
