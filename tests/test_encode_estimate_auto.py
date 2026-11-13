import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from encode_estimate import ssim_db, early_stop_ok, suggest_qp_auto

GB = 1_000_000_000
SRC_4K = int(20.9 * 1024**3)
DUR = 2 * 3600


def _rows(table, src):
    return [{"qp": qp, "ssim": s, "estimated_full_bytes": int(gb * GB)} for qp, s, gb in table]


# Real estimate tables from MediaStat runs (4K HDR film, 20.9 GiB source).
BIG_4K = [(16, 0.9970, 4.6), (17, 0.9968, 4.0), (18, 0.9965, 3.4), (19, 0.9963, 3.0),
          (20, 0.9960, 2.5), (21, 0.9957, 2.2), (22, 0.9953, 1.9), (23, 0.9949, 1.7), (24, 0.9949, 1.7)]
# Already-compressed source: encodes at QP16-20 are as big as the original.
ALREADY_SMALL = [(16, 0.9769, 5.3), (17, 0.9768, 5.0), (18, 0.9768, 4.8), (19, 0.9765, 4.5), (20, 0.9758, 4.0)]
ALREADY_SMALL_SRC = int(4.9 * GB)


def test_ssim_db():
    assert abs(ssim_db(0.99) - 20.0) < 1e-9
    assert ssim_db(None) is None


def test_early_stop_on_4k_example():
    rows = _rows(BIG_4K, SRC_4K)
    assert early_stop_ok(rows[0], SRC_4K)
    qp, note = suggest_qp_auto(rows[:1], SRC_4K, DUR, 3840, True)
    assert qp == 16 and "not tested" in note


def test_early_stop_needs_size_under_6gb_and_ssim():
    big = {"qp": 16, "ssim": 0.998, "estimated_full_bytes": 7 * GB}
    assert not early_stop_ok(big, 30 * GB)
    low = {"qp": 16, "ssim": 0.985, "estimated_full_bytes": 3 * GB}
    assert not early_stop_ok(low, 30 * GB)


def test_early_stop_blocked_when_no_saving():
    first = {"qp": 16, "ssim": 0.995, "estimated_full_bytes": 4 * GB}
    assert not early_stop_ok(first, 4.5 * GB)  # only ~11% smaller


def test_dB_window_picks_highest_qp_within_window():
    # Pretend QP16 wasn't small enough to stop: source huge, sweep is full.
    rows = _rows(BIG_4K, SRC_4K)
    for r in rows:
        r["estimated_full_bytes"] = int(r["estimated_full_bytes"] * 3)  # 13.8 GB at QP16
    qp, note = suggest_qp_auto(rows, 60 * GB, DUR, 3840, True)
    assert qp == 17 and note is None  # 0.28 dB below best; QP18 is 0.67 dB below


def test_bitrate_floor_blocks_tiny_files():
    rows = [{"qp": 16, "ssim": 0.9970, "estimated_full_bytes": 9 * GB},
            {"qp": 17, "ssim": 0.9968, "estimated_full_bytes": 1 * GB}]  # 1 GB/2h ~ 1.1 Mbps
    qp, _ = suggest_qp_auto(rows, 40 * GB, DUR, 3840, True)
    assert qp == 16


def test_skip_when_already_compressed():
    qp, note = suggest_qp_auto(_rows(ALREADY_SMALL, ALREADY_SMALL_SRC), ALREADY_SMALL_SRC, DUR, 1920, False)
    assert qp is None and "no QP gives a worthwhile saving" in note


def test_no_ssim_results():
    qp, note = suggest_qp_auto([{"qp": 16, "ssim": None}], 10 * GB, DUR, 1920)
    assert qp is None and note


def test_sweep_can_stop_once_past_window():
    from encode_estimate import sweep_can_stop
    rows = _rows(BIG_4K, SRC_4K)
    assert not sweep_can_stop(rows[:2])          # 0.28 dB below best: still in window
    assert sweep_can_stop(rows[:3])              # 0.67 dB below best: nothing higher can qualify
    assert not sweep_can_stop(rows[:1])
