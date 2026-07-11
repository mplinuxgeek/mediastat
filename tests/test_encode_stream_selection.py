import unittest

from encode_stream_selection import build_stream_maps


class EncodeStreamSelectionTests(unittest.TestCase):
    def test_keeps_only_matching_or_untagged_subtitles_for_selected_language(self):
        audio_streams = [
            {"tags": {"language": "eng"}},
            {"tags": {"language": "spa"}},
            {"tags": {}},
        ]
        subtitle_streams = [
            {"tags": {"language": "en"}},
            {"tags": {"language": "eng"}},
            {"tags": {"language": "english"}},
            {"tags": {"language": "spa"}},
            {"tags": {}},
        ]

        self.assertEqual(
            build_stream_maps("eng", audio_streams, subtitle_streams),
            ["-map", "0:v:0", "-map", "0:a:0", "-map", "0:a:2", "-map", "0:s:0?", "-map", "0:s:1?", "-map", "0:s:2?", "-map", "0:s:4?", "-map", "0:t?"],
        )

    def test_all_keeps_full_input_mapping(self):
        self.assertEqual(
            build_stream_maps("all", [{"tags": {"language": "eng"}}], [{"tags": {"language": "spa"}}]),
            ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"],
        )

    def test_matches_audio_tagged_with_terminology_code_when_config_uses_bibliographic_code(self):
        # lang="fre" (ISO 639-2/B) requested; source audio tagged "fra" (ISO 639-2/T) — same language.
        audio_streams = [{"tags": {"language": "fra"}}]

        self.assertEqual(
            build_stream_maps("fre", audio_streams, []),
            ["-map", "0:v:0", "-map", "0:a:0", "-map", "0:t?"],
        )

    def test_matches_subtitle_tagged_with_bibliographic_code_when_config_uses_terminology_code(self):
        # lang="deu" (ISO 639-2/T) requested; source subtitle tagged "ger" (ISO 639-2/B) — same language.
        audio_streams = [{"tags": {"language": "deu"}}]
        subtitle_streams = [{"tags": {"language": "ger"}}]

        self.assertEqual(
            build_stream_maps("deu", audio_streams, subtitle_streams),
            ["-map", "0:v:0", "-map", "0:a:0", "-map", "0:s:0?", "-map", "0:t?"],
        )

    def test_matches_two_letter_iso_code_against_three_letter_tag(self):
        # lang="es" (ISO 639-1) requested; source audio tagged "spa" (ISO 639-2).
        audio_streams = [{"tags": {"language": "spa"}}]

        self.assertEqual(
            build_stream_maps("es", audio_streams, []),
            ["-map", "0:v:0", "-map", "0:a:0", "-map", "0:t?"],
        )

    def test_copy_mode_drops_subtitles_and_attachments(self):
        audio_streams = [
            {"tags": {"language": "eng"}},
            {"tags": {"language": "spa"}},
        ]
        subtitle_streams = [
            {"tags": {"language": "eng"}},
        ]
        self.assertEqual(
            build_stream_maps("eng", audio_streams, subtitle_streams, codec="copy"),
            ["-map", "0:v:0", "-map", "0:a:0"],
        )

    def test_copy_mode_with_all_keeps_video_and_all_audio(self):
        audio_streams = [
            {"tags": {"language": "eng"}},
            {"tags": {"language": "spa"}},
        ]
        subtitle_streams = [
            {"tags": {"language": "eng"}},
        ]
        self.assertEqual(
            build_stream_maps("all", audio_streams, subtitle_streams, codec="copy"),
            ["-map", "0:v:0", "-map", "0:a?"],
        )


if __name__ == "__main__":
    unittest.main()
