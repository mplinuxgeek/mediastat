from __future__ import annotations


def derive_output_resolution(
    source_width: int | None,
    source_height: int | None,
    crop_filter: str | None,
    target_width: int | None,
) -> tuple[int, int]:
    width = int(source_width or 0)
    height = int(source_height or 0)
    if width <= 0 or height <= 0:
        return 0, 0

    if crop_filter:
        try:
            crop_width, crop_height, _crop_x, _crop_y = (int(v) for v in crop_filter.split(":"))
        except (TypeError, ValueError):
            crop_width = crop_height = 0
        if crop_width > 0 and crop_height > 0:
            width, height = crop_width, crop_height

    scaled_width = int(target_width or 0)
    if scaled_width > 0 and width > 0 and height > 0:
        scaled_height = max(2, int(round((height * scaled_width / width) / 2.0) * 2))
        return scaled_width, scaled_height
    return width, height
