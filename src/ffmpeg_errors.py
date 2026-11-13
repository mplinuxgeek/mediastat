"""Turn ffmpeg's stderr into something worth showing a user."""
from __future__ import annotations

import re

# Lines ffmpeg prints after the real problem: they only say that it gave up.
_FOLLOW_ON = (
    "conversion failed",
    "task finished with error code",
    "terminating thread with return code",
    "error sending frames to consumers",
    "nothing was written into output file",
    "error while filtering",
    "error submitting",
)
_ERROR_WORDS = ("error", "failed", "invalid", "cannot", "could not", "unable", "not supported",
                "unsupported", "no such", "not found", "denied", "no space")
_LOG_PREFIX = re.compile(r"^\[[^\]]*@ 0x[0-9a-f]+\]\s*")


def summarize_ffmpeg_error(stderr_lines: list[str], max_lines: int = 3) -> str:
    """The lines that explain why ffmpeg failed, oldest first, joined with ' · '.

    ffmpeg reports the cause first and then a cascade of consequences ending
    in "Conversion failed!", so the *last* error-looking line is the least
    useful one. Prefer the earliest lines that name a problem, skip the
    follow-on noise, and fall back to the final line when nothing matches.
    """
    lines = [l.strip() for l in stderr_lines if l and l.strip()]
    if not lines:
        return ""
    causes: list[str] = []
    for line in lines:
        low = line.lower()
        if any(n in low for n in _FOLLOW_ON) or not any(w in low for w in _ERROR_WORDS):
            continue
        text = _LOG_PREFIX.sub("", line)
        if text and text not in causes:
            causes.append(text)
    if not causes:
        return _LOG_PREFIX.sub("", lines[-1])
    return " · ".join(causes[:max_lines])
