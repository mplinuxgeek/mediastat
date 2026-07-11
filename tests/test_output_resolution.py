import unittest

from encode_output_resolution import derive_output_resolution


class OutputResolutionTests(unittest.TestCase):
    def test_crop_and_width_scaling_report_final_even_dimensions(self):
        self.assertEqual(
            derive_output_resolution(3840, 2160, "3840:1608:0:276", 1920),
            (1920, 804),
        )

    def test_crop_without_scaling_reports_cropped_dimensions(self):
        self.assertEqual(
            derive_output_resolution(3840, 2160, "3840:1608:0:276", None),
            (3840, 1608),
        )


if __name__ == "__main__":
    unittest.main()
