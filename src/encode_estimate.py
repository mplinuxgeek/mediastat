import math
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


def suggest_qp(results: list[dict], threshold: float = 0.99) -> tuple[int, str | None]:
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


# --- Automatic QP selection -------------------------------------------------
# SSIM compresses into a tiny range at high quality (0.9949..0.9970 across
# QP16..24 on a 4K HDR film), so a fixed threshold can't separate good from
# great. Converting to dB, -10*log10(1-ssim), spreads that range out and lets
# us compare runs relative to the best (lowest-QP) one.

EARLY_STOP_SSIM = 0.99
EARLY_STOP_MAX_BYTES = 6_000_000_000
DB_WINDOW = 0.3
MIN_SAVING = 0.20


def ssim_db(ssim: float | None) -> float | None:
    """SSIM expressed in dB: -10*log10(1-ssim). Higher is better."""
    if ssim is None:
        return None
    if ssim >= 1.0:
        return float("inf")
    return -10.0 * math.log10(1.0 - ssim)


def min_bitrate_bps(width: int | None, is_hdr: bool = False) -> float:
    """Lowest average bitrate we consider acceptable for a given frame width,
    so noisy/grainy content can't be pushed to a tiny file just because SSIM
    still looks fine."""
    w = width or 0
    if w >= 3000:
        return 4_000_000 if is_hdr else 3_000_000
    if w >= 1800:
        return 2_000_000
    return 1_000_000


def _saving_ok(r: dict, source_bytes: int | None) -> bool:
    est = r.get("estimated_full_bytes")
    if not source_bytes or est is None:
        return True  # can't judge; don't block
    return est <= source_bytes * (1.0 - MIN_SAVING)


def early_stop_ok(first: dict, source_bytes: int | None) -> bool:
    """True when the lowest-QP run is already good and small enough that
    testing higher QPs is pointless: SSIM above EARLY_STOP_SSIM, estimated
    size under EARLY_STOP_MAX_BYTES, and a worthwhile saving over the source."""
    ssim = first.get("ssim")
    est = first.get("estimated_full_bytes")
    if ssim is None or est is None:
        return False
    return (ssim > EARLY_STOP_SSIM and est < EARLY_STOP_MAX_BYTES
            and _saving_ok(first, source_bytes))


def suggest_qp_auto(results: list[dict], source_bytes: int | None, duration_sec: float | None,
                    width: int | None, is_hdr: bool = False, max_qp: int | None = None) -> tuple[int | None, str | None]:
    """Pick the highest QP that is (a) within DB_WINDOW dB of the best run,
    (b) at or above the resolution's minimum bitrate and (c) at least
    MIN_SAVING smaller than the source. If the lowest-QP run passes
    early_stop_ok it is chosen outright. Returns (qp, note); qp is None when
    no run is worth encoding (note says why). max_qp caps the choice (runs above it are ignored)."""
    scored = sorted((r for r in results if r.get("ssim") is not None), key=lambda r: r["qp"])
    if not scored:
        return None, "no SSIM results; cannot choose a QP"
    first = scored[0]
    if early_stop_ok(first, source_bytes):
        return first["qp"], (f"QP{first['qp']}: SSIM {first['ssim']:.4f} and under "
                             f"{EARLY_STOP_MAX_BYTES / 1e9:.0f} GB, so higher QPs were not tested")
    best_db = max(ssim_db(r["ssim"]) for r in scored)
    floor_bps = min_bitrate_bps(width, is_hdr)
    ok = []
    for r in scored:
        if max_qp and r["qp"] > max_qp:
            continue
        if best_db - ssim_db(r["ssim"]) > DB_WINDOW:
            continue
        est = r.get("estimated_full_bytes")
        if est is not None and duration_sec and est * 8 / duration_sec < floor_bps:
            continue
        if not _saving_ok(r, source_bytes):
            continue
        ok.append(r)
    if ok:
        return max(ok, key=lambda r: r["qp"])["qp"], None
    return None, ("no QP gives a worthwhile saving (>= "
                  f"{int(MIN_SAVING * 100)}% smaller) within {DB_WINDOW} dB of the best quality "
                  "and above the minimum bitrate" + (f", at QP{max_qp} or lower" if max_qp else "") + "; leaving the file as is")


def sweep_can_stop(results: list[dict]) -> bool:
    """True once the newest run is more than DB_WINDOW dB below the best run:
    SSIM only falls as QP rises, so no higher QP can qualify and testing more
    would be wasted time. Used by the automatic pipeline only."""
    scored = [r for r in results if r.get("ssim") is not None]
    if len(scored) < 2:
        return False
    best = max(ssim_db(r["ssim"]) for r in scored)
    return best - ssim_db(scored[-1]["ssim"]) > DB_WINDOW
