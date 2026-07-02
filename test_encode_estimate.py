import unittest

from encode_estimate import sample_window


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


if __name__ == "__main__":
    unittest.main()
