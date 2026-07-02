import unittest

from encode_estimate import sample_window, parse_ssim


class SampleWindowTests(unittest.TestCase):
    def test_long_file_centers_60s_window_on_midpoint(self):
        start, length = sample_window(3600.0, 60)
        self.assertEqual(length, 60.0)
        self.assertEqual(start, 1770.0)  # 1800 - 30

    def test_file_shorter_than_sample_length_uses_whole_file(self):
        start, length = sample_window(40.0, 60)
        self.assertEqual(start, 0.0)
        self.assertEqual(length, 40.0)

    def test_unknown_duration_falls_back_to_zero_start(self):
        start, length = sample_window(None, 60)
        self.assertEqual(start, 0.0)
        self.assertEqual(length, 60.0)

    def test_zero_duration_falls_back_to_zero_start(self):
        start, length = sample_window(0.0, 60)
        self.assertEqual(start, 0.0)
        self.assertEqual(length, 60.0)


class ParseSsimTests(unittest.TestCase):
    def test_extracts_all_value_from_ffmpeg_ssim_line(self):
        stderr = (
            "frame=  100 fps=0.0 q=-0.0 Lsize=N/A time=00:00:04.00 bitrate=N/A\n"
            "[Parsed_ssim_0 @ 0x5],, SSIM Y:0.991234 U:0.995123 V:0.996001 "
            "All:0.987654 (19.023456)\n"
        )
        self.assertEqual(parse_ssim(stderr), 0.987654)

    def test_returns_last_match_when_multiple_lines_present(self):
        stderr = "All:0.900000 (1.0)\nAll:0.950000 (2.0)\n"
        self.assertEqual(parse_ssim(stderr), 0.95)

    def test_returns_none_when_no_match(self):
        self.assertIsNone(parse_ssim("some unrelated ffmpeg output\n"))

    def test_returns_none_on_empty_string(self):
        self.assertIsNone(parse_ssim(""))


if __name__ == "__main__":
    unittest.main()
