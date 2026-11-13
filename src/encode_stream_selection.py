from __future__ import annotations


# ISO 639-1 (2-letter) and ISO 639-2 bibliographic/terminology (3-letter) codes
# that refer to the same language. ffprobe tags streams with whichever code the
# muxing tool used, so a config lang of "fre" must still match audio/subtitles
# tagged "fra" (and vice versa).
_LANGUAGE_CODE_GROUPS = [
    {"en", "eng", "english"},
    {"fr", "fre", "fra"},
    {"de", "ger", "deu"},
    {"es", "spa"},
    {"it", "ita"},
    {"nl", "dut", "nld"},
    {"pt", "por"},
    {"ru", "rus"},
    {"ja", "jpn"},
    {"zh", "chi", "zho"},
    {"ko", "kor"},
    {"sv", "swe"},
    {"no", "nor"},
    {"da", "dan"},
    {"fi", "fin"},
    {"pl", "pol"},
    {"cs", "cze", "ces"},
    {"el", "gre", "ell"},
    {"tr", "tur"},
    {"ar", "ara"},
    {"hi", "hin"},
    {"is", "ice", "isl"},
    {"ro", "rum", "ron"},
    {"sk", "slo", "slk"},
    {"he", "heb"},
    {"th", "tha"},
    {"vi", "vie"},
    {"uk", "ukr"},
    {"hu", "hun"},
    {"fa", "per", "fas"},
    {"ms", "may", "msa"},
    {"cy", "wel", "cym"},
    {"eu", "baq", "eus"},
    {"ka", "geo", "kat"},
    {"sq", "alb", "sqi"},
    {"hy", "arm", "hye"},
    {"mk", "mac", "mkd"},
    {"my", "bur", "mya"},
    {"mi", "mao", "mri"},
    {"bo", "tib", "bod"},
]

_LANGUAGE_ALIASES = {
    code: group for group in _LANGUAGE_CODE_GROUPS for code in group
}


def _stream_language(tags: dict | None) -> str:
    tags = tags or {}
    return str(tags.get("language") or tags.get("LANGUAGE") or "").strip().lower()


def _language_aliases(lang: str) -> set[str]:
    norm = str(lang).strip().lower()
    return _LANGUAGE_ALIASES.get(norm, {norm})


def build_stream_maps(lang: str, audio_streams: list | None, subtitle_streams: list | None) -> list[str]:
    norm_lang = str(lang or "").strip().lower()

    if not norm_lang or norm_lang == "all":
        return ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"]

    audio_streams = audio_streams or []
    subtitle_streams = subtitle_streams or []
    lang_aliases = _language_aliases(norm_lang)

    matched_audio = [
        idx for idx, stream in enumerate(audio_streams)
        if (stream_lang := _stream_language(stream.get("tags"))) in lang_aliases
        or stream_lang in ("", "und")
    ]
    if not matched_audio:
        return ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"]

    matched_subtitles = [
        idx for idx, stream in enumerate(subtitle_streams)
        if (stream_lang := _stream_language(stream.get("tags"))) in lang_aliases
        or stream_lang in ("", "und")
    ]

    maps = ["-map", "0:v:0"]
    for idx in matched_audio:
        maps += ["-map", f"0:a:{idx}"]
    for idx in matched_subtitles:
        maps += ["-map", f"0:s:{idx}?"]
    maps += ["-map", "0:t?"]
    return maps


# Subtitle codecs that can't be stream-copied into the other container and
# what to convert them to instead. MP4's text subtitle format (mov_text /
# tx3g) is not valid in Matroska, and MP4 accepts no text format but its own.
# Bitmap subtitles (PGS, VobSub) have no text equivalent and are left alone.
_MKV_SUBTITLE_CONVERSIONS = {"mov_text": "srt"}
_MP4_SUBTITLE_CONVERSIONS = {codec: "mov_text" for codec in ("subrip", "srt", "ass", "ssa", "webvtt", "text")}


def subtitle_codec_args(maps: list[str], subtitle_streams: list | None, output_format: str) -> list[str]:
    """ffmpeg subtitle codec options to go with build_stream_maps() output.

    Subtitles are stream-copied, except those whose codec the output
    container can't hold — those get a per-stream conversion, so one
    incompatible track doesn't fail the whole encode at "could not write
    header". `output_format` is the container extension ("mkv", "mp4").
    """
    args = ["-c:s", "copy"]
    subtitle_streams = subtitle_streams or []
    fmt = str(output_format or "").lower().lstrip(".")
    conversions = _MKV_SUBTITLE_CONVERSIONS if fmt in ("mkv", "mka", "webm") else \
        _MP4_SUBTITLE_CONVERSIONS if fmt in ("mp4", "m4v", "mov") else {}
    if not conversions or not subtitle_streams:
        return args

    # Output subtitle streams are numbered in the order they are mapped.
    if "0:s?" in maps:
        mapped = list(range(len(subtitle_streams)))
    else:
        mapped = [int(m[4:].rstrip("?")) for m in maps if m.startswith("0:s:")]
    for out_idx, in_idx in enumerate(mapped):
        if in_idx >= len(subtitle_streams):
            continue
        codec = str(subtitle_streams[in_idx].get("codec_name") or "").lower()
        if codec in conversions:
            args += [f"-c:s:{out_idx}", conversions[codec]]
    return args
