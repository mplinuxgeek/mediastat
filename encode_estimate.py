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


def build_ssim_ref_filter(crop_filter: str | None, width: int | None) -> str:
    """Build the ffmpeg -lavfi filter comparing encoded output [0:v] to the
    raw sample [1:v], applying to the reference the same crop/scale that was
    applied to the encode so both frames have matching dimensions."""
    ref_ops = []
    if crop_filter:
        ref_ops.append(f"crop={crop_filter}")
    if width:
        ref_ops.append(f"scale={int(width)}:-2")
    if not ref_ops:
        return "[0:v][1:v]ssim"
    return f"[1:v]{','.join(ref_ops)}[ref];[0:v][ref]ssim"


def suggest_qp(results: list[dict], threshold: float = 0.98) -> tuple[int, str | None]:
    """Pick the highest tested QP whose SSIM meets `threshold` (most
    compression while staying near-lossless). Falls back to the lowest
    tested QP with a warning if none qualify."""
    passing = [r for r in results if r.get("ssim") is not None and r["ssim"] >= threshold]
    if passing:
        return max(passing, key=lambda r: r["qp"])["qp"], None
    lowest = min(results, key=lambda r: r["qp"])
    return lowest["qp"], (
        f"even QP{lowest['qp']} SSIM ({lowest.get('ssim')}) is below the "
        f"{threshold} target — this content may compress poorly, or check source quality"
    )
