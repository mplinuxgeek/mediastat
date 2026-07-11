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


def build_stream_maps(lang: str, audio_streams: list | None, subtitle_streams: list | None, codec: str = "") -> list[str]:
    norm_lang = str(lang or "").strip().lower()
    is_copy = (codec == "copy")

    if not norm_lang or norm_lang == "all":
        if is_copy:
            return ["-map", "0:v:0", "-map", "0:a?"]
        else:
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
        if is_copy:
            return ["-map", "0:v:0", "-map", "0:a?"]
        else:
            return ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"]

    matched_subtitles = [
        idx for idx, stream in enumerate(subtitle_streams)
        if (stream_lang := _stream_language(stream.get("tags"))) in lang_aliases
        or stream_lang in ("", "und")
    ]

    maps = ["-map", "0:v:0"]
    for idx in matched_audio:
        maps += ["-map", f"0:a:{idx}"]
    if not is_copy:
        for idx in matched_subtitles:
            maps += ["-map", f"0:s:{idx}?"]
        maps += ["-map", "0:t?"]
    return maps
