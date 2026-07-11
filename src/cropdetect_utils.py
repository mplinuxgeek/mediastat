from collections import Counter
from typing import Iterable, Optional
import re


_CROP_RE = re.compile(r"crop=(\d+:\d+:\d+:\d+)")


def default_cropdetect_limit() -> float:
    # Match the previous 8-bit threshold intent (24) while letting ffmpeg
    # scale it correctly for 10-bit/HDR sources when given as a normalized float.
    return 24 / 255


def extract_cropdetect_values(stderr_text: str) -> list[str]:
    values: list[str] = []
    for line in stderr_text.splitlines():
        if "crop=" not in line or "Parsed_cropdetect" not in line:
            continue
        match = _CROP_RE.search(line)
        if match:
            values.append(match.group(1))
    return values


def _parse_crop(raw_crop: str) -> Optional[tuple[int, int, int, int]]:
    try:
        width, height, x, y = (int(part) for part in raw_crop.split(":"))
    except (TypeError, ValueError):
        return None
    return width, height, x, y


def _is_effective_crop(
    raw_crop: str,
    source_width: Optional[int] = None,
    source_height: Optional[int] = None,
) -> bool:
    parsed = _parse_crop(raw_crop)
    if not parsed:
        return False

    crop_width, crop_height, crop_x, crop_y = parsed
    if crop_width <= 0 or crop_height <= 0 or crop_x < 0 or crop_y < 0:
        return False

    if source_width and source_height:
        if crop_width + crop_x > source_width or crop_height + crop_y > source_height:
            return False
        if crop_width == source_width and crop_height == source_height and crop_x == 0 and crop_y == 0:
            return False

    return True


def _choose_sample_crop(
    sample_crops: Iterable[str],
    source_width: Optional[int] = None,
    source_height: Optional[int] = None,
) -> Optional[str]:
    filtered = [
        raw_crop for raw_crop in sample_crops
        if _is_effective_crop(raw_crop, source_width=source_width, source_height=source_height)
    ]
    if not filtered:
        return None

    counts = Counter(filtered)
    best_crop = None
    best_count = -1
    for raw_crop in filtered:
        count = counts[raw_crop]
        if count > best_count:
            best_crop = raw_crop
            best_count = count
    return best_crop


def choose_dominant_crop(
    sample_groups: Iterable[Iterable[str]],
    source_width: Optional[int] = None,
    source_height: Optional[int] = None,
    min_samples: int = 2,
) -> Optional[str]:
    sample_votes: list[str] = []
    for sample_crops in sample_groups:
        best_crop = _choose_sample_crop(
            sample_crops,
            source_width=source_width,
            source_height=source_height,
        )
        if best_crop:
            sample_votes.append(best_crop)

    if not sample_votes:
        return None

    counts = Counter(sample_votes)
    best_crop, best_count = counts.most_common(1)[0]
    if best_count < min_samples:
        return None
    return best_crop
