import unittest

from encode_estimate import sample_window, parse_ssim, build_ssim_ref_filter, suggest_qp


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


class BuildSsimRefFilterTests(unittest.TestCase):
    def test_no_crop_no_width_compares_directly(self):
        self.assertEqual(build_ssim_ref_filter(None, None), "[0:v][1:v]ssim")

    def test_crop_only_applies_crop_to_reference(self):
        self.assertEqual(
            build_ssim_ref_filter("1920:800:0:140", None),
            "[1:v]crop=1920:800:0:140[ref];[0:v][ref]ssim",
        )

    def test_width_only_applies_scale_to_reference(self):
        self.assertEqual(
            build_ssim_ref_filter(None, 1280),
            "[1:v]scale=1280:-2[ref];[0:v][ref]ssim",
        )

    def test_crop_and_width_apply_both_in_order(self):
        self.assertEqual(
            build_ssim_ref_filter("1920:800:0:140", 1280),
            "[1:v]crop=1920:800:0:140,scale=1280:-2[ref];[0:v][ref]ssim",
        )


class SuggestQpTests(unittest.TestCase):
    def test_picks_highest_qp_meeting_threshold(self):
        results = [
            {"qp": 16, "ssim": 0.995},
            {"qp": 18, "ssim": 0.990},
            {"qp": 20, "ssim": 0.981},
            {"qp": 22, "ssim": 0.960},
        ]
        qp, warning = suggest_qp(results, threshold=0.98)
        self.assertEqual(qp, 20)
        self.assertIsNone(warning)

    def test_falls_back_to_lowest_qp_with_warning_when_none_meet_threshold(self):
        results = [
            {"qp": 16, "ssim": 0.970},
            {"qp": 18, "ssim": 0.950},
            {"qp": 20, "ssim": 0.930},
            {"qp": 22, "ssim": 0.900},
        ]
        qp, warning = suggest_qp(results, threshold=0.98)
        self.assertEqual(qp, 16)
        self.assertIsNotNone(warning)
        self.assertIn("QP16", warning)

    def test_treats_missing_ssim_as_not_qualifying(self):
        results = [
            {"qp": 16, "ssim": None},
            {"qp": 18, "ssim": 0.99},
            {"qp": 20, "ssim": None},
        ]
        qp, warning = suggest_qp(results, threshold=0.98)
        self.assertEqual(qp, 18)
        self.assertIsNone(warning)


if __name__ == "__main__":
    unittest.main()
