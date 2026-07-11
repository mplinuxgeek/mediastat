import unittest

import cropdetect_utils as crop


class CropDetectTests(unittest.TestCase):
    def test_default_cropdetect_limit_uses_normalized_8bit_threshold(self):
        self.assertAlmostEqual(crop.default_cropdetect_limit(), 24 / 255, places=6)

    def test_parse_cropdetect_lines_keeps_all_detected_crops(self):
        stderr = """
        [Parsed_cropdetect_0 @ 0x1] x1:0 x2:1919 y1:138 y2:941 w:1920 h:804 x:0 y:138 pts:1 t:0.04 crop=1920:804:0:138
        [Parsed_cropdetect_0 @ 0x1] x1:0 x2:1919 y1:0 y2:1079 w:1920 h:1080 x:0 y:0 pts:2 t:0.08 crop=1920:1080:0:0
        [Parsed_cropdetect_0 @ 0x1] x1:0 x2:1919 y1:138 y2:941 w:1920 h:804 x:0 y:138 pts:3 t:0.12 crop=1920:804:0:138
        """

        self.assertEqual(
            crop.extract_cropdetect_values(stderr),
            ["1920:804:0:138", "1920:1080:0:0", "1920:804:0:138"],
        )

    def test_choose_dominant_crop_prefers_majority_across_samples(self):
        samples = [
            ["1920:804:0:138", "1920:804:0:138", "1920:1080:0:0"],
            ["1920:804:0:138"],
            ["1920:804:0:138", "1920:800:0:140"],
            ["1920:1080:0:0"],
            ["1920:804:0:138"],
            ["1920:800:0:140"],
        ]

        self.assertEqual(
            crop.choose_dominant_crop(samples),
            "1920:804:0:138",
        )

    def test_choose_dominant_crop_ignores_noop_when_real_crop_exists(self):
        samples = [
            ["1920:1080:0:0", "1920:804:0:138"],
            ["1920:1080:0:0"],
            ["1920:804:0:138"],
            ["1920:1080:0:0"],
            ["1920:804:0:138"],
            ["1920:804:0:138"],
        ]

        self.assertEqual(
            crop.choose_dominant_crop(samples, source_width=1920, source_height=1080),
            "1920:804:0:138",
        )

    def test_choose_dominant_crop_returns_none_when_no_crop_repeats(self):
        samples = [
            ["1920:804:0:138"],
            ["1920:800:0:140"],
            ["1918:804:2:138"],
            ["1920:802:0:139"],
            [],
            ["1920:1080:0:0"],
        ]

        self.assertIsNone(crop.choose_dominant_crop(samples, min_samples=2))


if __name__ == "__main__":
    unittest.main()
