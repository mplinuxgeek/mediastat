import os, sys, time
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import auto_encode as ae


def _touch(p, size, age=7200):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.truncate(size)
    t = time.time() - age
    os.utime(p, (t, t))


def test_find_candidates_filters(tmp_path):
    _touch(tmp_path / "A (2020)/A.mkv", 20_000)
    _touch(tmp_path / "small/S.mkv", 1_000)
    _touch(tmp_path / "new/N.mkv", 20_000, age=60)          # too fresh
    _touch(tmp_path / ".mediastat-trash/x/T.mkv", 20_000)   # trash
    _touch(tmp_path / "B/B (qp16).mkv", 20_000)             # our own output
    _touch(tmp_path / "C/.C.mkv.mediastat.tmp", 20_000)
    _touch(tmp_path / "D/notes.txt", 20_000)
    got = [os.path.basename(c["path"]) for c in ae.find_candidates([str(tmp_path)], 10_000)]
    assert got == ["A.mkv"]


def _info(dur=7200.0, codec="hevc", audio=1, ok=True):
    return {"ok": ok, "duration_sec": dur, "media_info": {"video_codec": codec}, "a_streams": [1] * audio}


def test_verify_output():
    assert ae.verify_output(_info(codec="h264", audio=2), _info(), 100, 30)[0]
    assert not ae.verify_output(_info(), _info(dur=7100), 100, 30)[0]
    assert not ae.verify_output(_info(), _info(codec="h264"), 100, 30)[0]
    assert not ae.verify_output(_info(), _info(audio=0), 100, 30)[0]
    assert not ae.verify_output(_info(), _info(), 100, 95)[0]
    assert not ae.verify_output(_info(), _info(ok=False), 100, 30)[0]


def test_replace_original_and_purge(tmp_path):
    orig = tmp_path / "Movie (2020)" / "Movie (2020).mp4"
    orig.parent.mkdir()
    orig.write_bytes(b"o" * 100)
    new = tmp_path / "scratch.mkv"
    new.write_bytes(b"n" * 30)
    final, trashed = ae.replace_original(new, orig)
    assert final.name == "Movie (2020).mkv" and final.read_bytes() == b"n" * 30
    assert trashed.read_bytes() == b"o" * 100 and not orig.exists()
    assert not list(orig.parent.glob(".*mediastat.tmp"))
    # fresh trash survives; old trash is purged
    assert ae.purge_trash([str(tmp_path)], 7) == []
    old = time.time() + 8 * 86400
    removed = ae.purge_trash([str(tmp_path)], 7, now=old)
    assert len(removed) == 1 and not trashed.exists() and final.exists()


def test_replace_restores_original_on_failure(tmp_path, monkeypatch):
    orig = tmp_path / "M.mkv"
    orig.write_bytes(b"o" * 50)
    new = tmp_path / "n.mkv"
    new.write_bytes(b"n" * 10)
    real = os.rename
    def flaky(a, b):
        if str(a).endswith(".mediastat.tmp"):
            raise OSError("boom")
        return real(a, b)
    monkeypatch.setattr(os, "rename", flaky)
    try:
        ae.replace_original(new, orig)
        assert False
    except OSError:
        pass
    assert orig.read_bytes() == b"o" * 50


def test_purge_trash_for_space_oldest_first(tmp_path, monkeypatch):
    for stamp in ("20240101-000000", "20240102-000000", "20240103-000000"):
        _touch(tmp_path / f".mediastat-trash/{stamp}/f.mkv", 1000)
    monkeypatch.setattr(ae.shutil, "disk_usage", lambda p: type("U", (), {"free": 500})())
    # need 2000 more free: first two (oldest) stamps go, newest survives
    removed = ae.purge_trash_for_space([str(tmp_path)], 2500)
    assert [os.path.basename(r) for r in removed] == ["20240101-000000", "20240102-000000"]
    assert (tmp_path / ".mediastat-trash/20240103-000000/f.mkv").exists()
    # plenty of space: nothing touched
    monkeypatch.setattr(ae.shutil, "disk_usage", lambda p: type("U", (), {"free": 10**12})())
    assert ae.purge_trash_for_space([str(tmp_path)], 2500) == []


def test_expected_stream_counts():
    assert ae.expected_stream_counts(["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?"], 3, 4) == (3, 4)
    assert ae.expected_stream_counts(["-map", "0:v:0", "-map", "0:a:0", "-map", "0:a:2", "-map", "0:s:1?"], 3, 4) == (2, 1)


def test_verify_output_streams_and_resolution():
    src = {**_info(audio=3), "media_info": {"video_codec": "h264", "height": 2160, "width": 3840}, "s_streams": [1, 2]}
    good = {**_info(audio=3), "media_info": {"video_codec": "hevc", "height": 2160, "width": 3840}, "s_streams": [1, 2]}
    assert ae.verify_output(src, good, 100, 30, expected_audio=3, expected_subs=2)[0]
    assert not ae.verify_output(src, {**good, "a_streams": [1]}, 100, 30, expected_audio=3)[0]
    assert not ae.verify_output(src, {**good, "s_streams": []}, 100, 30, expected_subs=2)[0]
    cropped = {**good, "media_info": {"video_codec": "hevc", "height": 1080, "width": 1920}}
    assert not ae.verify_output(src, cropped, 100, 30)[0]


def test_purge_for_space_keeps_recent_and_scopes_device(tmp_path, monkeypatch):
    old = time.strftime("%Y%m%d-%H%M%S", time.localtime(time.time() - 3 * 86400))
    new = time.strftime("%Y%m%d-%H%M%S", time.localtime(time.time() - 3600))
    _touch(tmp_path / f".mediastat-trash/{old}/f.mkv", 1000)
    _touch(tmp_path / f".mediastat-trash/{new}/f.mkv", 1000)
    monkeypatch.setattr(ae.shutil, "disk_usage", lambda p: type("U", (), {"free": 0})())
    assert ae.purge_trash_for_space([str(tmp_path)], 10**9, only_dev=-1) == []      # other filesystem: untouched
    removed = ae.purge_trash_for_space([str(tmp_path)], 10**9)                       # recent stamp protected
    assert [os.path.basename(r) for r in removed] == [old]
    assert ae.trash_usage([str(tmp_path)]) == 1000


def test_restore_original_same_name_and_new_name(tmp_path):
    orig = tmp_path / "M.mkv"
    orig.write_bytes(b"o" * 100)
    new = tmp_path / "enc.mkv"
    new.write_bytes(b"n" * 10)
    final, trashed = ae.replace_original(new, orig)
    assert ae.restore_original(final, trashed) == orig
    assert orig.read_bytes() == b"o" * 100 and not trashed.parent.exists()
    # .mp4 original replaced by .mkv: restoring removes the .mkv
    mp4 = tmp_path / "N.mp4"
    mp4.write_bytes(b"o" * 100)
    final, trashed = ae.replace_original(new, mp4)
    assert final.suffix == ".mkv" and ae.find_trashed(final, trashed.parent.name) == trashed
    assert ae.restore_original(final, trashed) == mp4 and not final.exists()


def test_suggest_qp_respects_cap():
    from encode_estimate import suggest_qp_auto
    rows = [{"qp": q, "ssim": 0.998 - (q - 16) * 0.00002, "estimated_full_bytes": int((8 - (q - 16) * 0.4) * 1e9)} for q in range(16, 25)]
    uncapped, _ = suggest_qp_auto(rows, 30 * 10**9, 7200, 1920)
    capped, _ = suggest_qp_auto(rows, 30 * 10**9, 7200, 1920, max_qp=18)
    assert uncapped > 18 and capped == 18
    none, note = suggest_qp_auto([r for r in rows if r["qp"] > 20], 30 * 10**9, 7200, 1920, max_qp=20)
    assert none is None and "QP20 or lower" in note
