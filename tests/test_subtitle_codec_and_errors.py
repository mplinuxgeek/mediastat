import unittest

from encode_stream_selection import build_stream_maps, subtitle_codec_args
from ffmpeg_errors import summarize_ffmpeg_error


def _sub(codec, lang="eng"):
    return {"codec_name": codec, "tags": {"language": lang}}


class SubtitleCodecArgsTests(unittest.TestCase):
    def test_compatible_subtitles_are_just_copied(self):
        subs = [_sub("subrip"), _sub("hdmv_pgs_subtitle")]
        self.assertEqual(subtitle_codec_args(["-map", "0:s?"], subs, ".mkv"), ["-c:s", "copy"])

    def test_mov_text_is_converted_for_mkv(self):
        subs = [_sub("mov_text"), _sub("subrip")]
        self.assertEqual(subtitle_codec_args(["-map", "0:s?"], subs, "mkv"),
                         ["-c:s", "copy", "-c:s:0", "srt"])

    def test_conversion_targets_output_index_not_input_index(self):
        # Language filtering keeps input subs 0, 2 and 5 → output subs 0, 1, 2.
        subs = [_sub("mov_text"), _sub("mov_text", "spa"), _sub("subrip"),
                _sub("mov_text", "por"), _sub("mov_text", "ara"), _sub("mov_text")]
        audio = [{"tags": {"language": "eng"}}]
        maps = build_stream_maps("eng", audio, subs)
        self.assertEqual([m for m in maps if m.startswith("0:s:")], ["0:s:0?", "0:s:2?", "0:s:5?"])
        self.assertEqual(subtitle_codec_args(maps, subs, ".mkv"),
                         ["-c:s", "copy", "-c:s:0", "srt", "-c:s:2", "srt"])

    def test_text_subtitles_are_converted_for_mp4(self):
        subs = [_sub("subrip"), _sub("ass"), _sub("mov_text"), _sub("hdmv_pgs_subtitle")]
        self.assertEqual(subtitle_codec_args(["-map", "0:s?"], subs, ".mp4"),
                         ["-c:s", "copy", "-c:s:0", "mov_text", "-c:s:1", "mov_text"])

    def test_no_stream_info_means_plain_copy(self):
        self.assertEqual(subtitle_codec_args(["-map", "0:s?"], None, ".mkv"), ["-c:s", "copy"])
        self.assertEqual(subtitle_codec_args(["-map", "0:s?"], [], ".mkv"), ["-c:s", "copy"])


class SummarizeFfmpegErrorTests(unittest.TestCase):
    # Verbatim tail of a real failure: MP4 mov_text subtitles copied into MKV.
    REAL = [
        "Press [q] to stop, [?] for help",
        "[matroska @ 0x565107401f00] Subtitle codec 94213 is not supported.",
        "[out#0/matroska @ 0x5651074afac0] Could not write header (incorrect codec parameters ?): Function not implemented",
        "[vf#0:0 @ 0x565107313900] Error sending frames to consumers: Function not implemented",
        "[vf#0:0 @ 0x565107313900] Task finished with error code: -38 (Function not implemented)",
        "[vf#0:0 @ 0x565107313900] Terminating thread with return code -38 (Function not implemented)",
        "[out#0/matroska @ 0x5651074afac0] Nothing was written into output file, because at least one of its streams received no packets.",
        "frame=    0 fps=0.0 q=0.0 Lsize=       0KiB time=N/A bitrate=N/A speed=N/A",
        "Conversion failed!",
    ]

    def test_reports_the_cause_not_the_final_conversion_failed(self):
        summary = summarize_ffmpeg_error(self.REAL)
        self.assertEqual(summary, "Subtitle codec 94213 is not supported. · "
                                  "Could not write header (incorrect codec parameters ?): Function not implemented")
        self.assertNotIn("Conversion failed", summary)

    def test_falls_back_to_last_line_when_nothing_names_a_problem(self):
        self.assertEqual(summarize_ffmpeg_error(["frame= 10", "Conversion failed!"]), "Conversion failed!")

    def test_empty_input(self):
        self.assertEqual(summarize_ffmpeg_error([]), "")
        self.assertEqual(summarize_ffmpeg_error(["", "  "]), "")

    def test_duplicates_collapse_and_summary_is_capped(self):
        lines = ["[a @ 0x1] Invalid data found"] * 5 + [f"[b @ 0x2] error number {i}" for i in range(6)]
        summary = summarize_ffmpeg_error(lines)
        self.assertEqual(summary.split(" · "), ["Invalid data found", "error number 0", "error number 1"])


if __name__ == "__main__":
    unittest.main()
