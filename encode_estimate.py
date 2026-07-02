import re

_SSIM_ALL_RE = re.compile(r"All:([0-9.]+)")


def sample_window(duration: float | None, sample_length: int = 60) -> tuple[float, float]:
    """Return (start_seconds, length_seconds) for a sample centered on the
    midpoint of a file `duration` seconds long. Falls back to (0, sample_length)
    when duration is unknown."""
    if not duration or duration <= 0:
        return 0.0, float(sample_length)
    length = min(float(sample_length), duration)
    start = max(0.0, (duration / 2.0) - (length / 2.0))
    return start, length


def parse_ssim(stderr_text: str) -> float | None:
    """Extract the last 'All:<value>' SSIM score from ffmpeg's ssim filter
    stderr output. Returns None if no match is found."""
    matches = _SSIM_ALL_RE.findall(stderr_text)
    if not matches:
        return None
    try:
        return float(matches[-1])
    except ValueError:
        return None
