"""Helpers for MediaStat's automatic re-encode pipeline: candidate scan, output
verification, trash handling (originals are kept for a while, never deleted
immediately) and the Plex refresh. Pure/IO helpers only; orchestration lives in main.py."""
import json
import os
import re
import shutil
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

VIDEO_EXTS = {".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts", ".wmv"}
TRASH_NAME = ".mediastat-trash"
TMP_SUFFIX = ".mediastat.tmp"
_TRASH_DIR_RE = re.compile(r"^\d{8}-\d{6}$")


def find_candidates(roots, min_bytes: int, min_age_s: float = 3600, now: float | None = None) -> list[dict]:
    """Video files >= min_bytes under roots, biggest first. Skips dot files/dirs
    (trash, temp files), our own '(qpNN)' outputs and files modified recently
    (still downloading/unpacking)."""
    now = now or time.time()
    out = []
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            for fn in filenames:
                if fn.startswith(".") or Path(fn).suffix.lower() not in VIDEO_EXTS or re.search(r"\(qp\d+(-\w+)?\)", fn):
                    continue
                p = os.path.join(dirpath, fn)
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                if st.st_size >= min_bytes and now - st.st_mtime >= min_age_s:
                    out.append({"path": p, "size": st.st_size, "mtime": int(st.st_mtime)})
    return sorted(out, key=lambda c: -c["size"])


def expected_stream_counts(maps: list[str], n_audio: int, n_subs: int) -> tuple[int, int]:
    """Audio/subtitle stream counts an encode should keep, from build_stream_maps() args."""
    audio = sum(1 for m in maps if re.fullmatch(r"0:a:\d+\??", m))
    subs = sum(1 for m in maps if re.fullmatch(r"0:s:\d+\??", m))
    return (n_audio if "0:a?" in maps else audio), (n_subs if "0:s?" in maps else subs)


def verify_output(src: dict, out: dict, src_size: int, out_size: int, max_ratio: float = 0.9,
                  expected_audio: int | None = None, expected_subs: int | None = None) -> tuple[bool, str]:
    """Decide whether an encoded output is safe to replace the original.
    src/out are _probe_video() results. expected_audio/expected_subs are the stream
    counts the encode was meant to keep (None = only require some audio if the source had any)."""
    if not out.get("ok"):
        return False, "output could not be probed"
    sd, od = src.get("duration_sec") or 0, out.get("duration_sec") or 0
    if not sd or not od:
        return False, "missing duration"
    if abs(sd - od) > max(3.0, sd * 0.003):
        return False, f"duration differs (source {sd:.1f}s, output {od:.1f}s)"
    if out.get("media_info", {}).get("video_codec") not in ("hevc", "h265"):
        return False, f"unexpected video codec {out.get('media_info', {}).get('video_codec')!r}"
    out_audio, out_subs = len(out.get("a_streams") or []), len(out.get("s_streams") or [])
    if src.get("a_streams") and not out_audio:
        return False, "output has no audio"
    if expected_audio is not None and out_audio < expected_audio:
        return False, f"output has {out_audio} audio stream(s), expected {expected_audio}"
    if expected_subs and not out_subs:
        return False, f"output has no subtitles, expected {expected_subs}"
    sm, om = src.get("media_info", {}), out.get("media_info", {})
    sh, oh = sm.get("height") or 0, om.get("height") or 0
    if sh and oh and abs(sh - oh) > sh * 0.02:
        return False, f"resolution changed ({sm.get('width')}x{sh} -> {om.get('width')}x{oh})"
    if out_size >= src_size * max_ratio:
        return False, f"output not meaningfully smaller ({out_size} vs {src_size})"
    return True, "ok"


def trash_original(orig: Path, now: float | None = None) -> Path:
    """Move the original into <its library dir>/.mediastat-trash/<stamp>/ (same
    filesystem rename: instant, no extra space). Returns the new path."""
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now or time.time()))
    dest_dir = orig.parent / TRASH_NAME / stamp
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / orig.name
    os.rename(orig, dest)
    return dest


def find_trashed(final: Path, stamp: str) -> Path | None:
    """Locate the trashed original of an already-replaced file by its stamp dir name
    (for rows recorded before the trashed path was stored)."""
    d = final.parent / TRASH_NAME / stamp
    if not _TRASH_DIR_RE.match(stamp) or not d.is_dir():
        return None
    return next((f for f in sorted(d.iterdir()) if f.is_file() and f.stem == final.stem), None)


def restore_original(final: Path, trashed: Path) -> Path:
    """Put a trashed original back in its library folder and discard the encoded
    replacement. Returns the restored path."""
    if trashed.parent.parent.name != TRASH_NAME or not trashed.is_file():
        raise FileNotFoundError(f"original is no longer in the trash: {trashed}")
    dest = trashed.parent.parent.parent / trashed.name
    os.replace(trashed, dest)  # atomically replaces the encoded file when the names match
    if final != dest:
        final.unlink(missing_ok=True)
    try:
        trashed.parent.rmdir()
    except OSError:
        pass
    return dest


def replace_original(new_file: Path, orig: Path, now: float | None = None) -> tuple[Path, Path]:
    """Copy new_file next to orig as a hidden temp file, trash the original, then
    rename the temp into place (original stem + .mkv). Restores the original if the
    final rename fails. Returns (final_path, trashed_original)."""
    final = orig.with_suffix(".mkv")
    tmp = orig.parent / f".{final.name}{TMP_SUFFIX}"
    try:
        shutil.copyfile(new_file, tmp)
        if tmp.stat().st_size != new_file.stat().st_size:
            raise OSError("copy size mismatch")
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        trashed = trash_original(orig, now)
        try:
            os.rename(tmp, final)
        except OSError:
            os.rename(trashed, orig)
            raise
        return final, trashed
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def purge_trash(roots, days: float, now: float | None = None) -> list[str]:
    """Delete trash stamps older than `days`. Only touches dirs named like our
    stamp inside a '.mediastat-trash' directory. Returns deleted paths."""
    now = now or time.time()
    removed = []
    for root in roots:
        for dirpath, dirnames, _ in os.walk(root):
            if os.path.basename(dirpath) == TRASH_NAME:
                for d in list(dirnames):
                    if not _TRASH_DIR_RE.match(d):
                        continue
                    try:
                        ts = time.mktime(time.strptime(d, "%Y%m%d-%H%M%S"))
                    except ValueError:
                        continue
                    if now - ts > days * 86400:
                        shutil.rmtree(os.path.join(dirpath, d), ignore_errors=True)
                        removed.append(os.path.join(dirpath, d))
                dirnames[:] = []
            else:
                dirnames[:] = [d for d in dirnames if not d.startswith(".") or d == TRASH_NAME]
    return removed


def _trash_stamps(roots) -> list[tuple[float, str]]:
    """(timestamp, path) of every trash stamp dir under roots."""
    found = []
    for root in roots:
        for dirpath, dirnames, _ in os.walk(root):
            if os.path.basename(dirpath) == TRASH_NAME:
                for d in dirnames:
                    if _TRASH_DIR_RE.match(d):
                        try:
                            found.append((time.mktime(time.strptime(d, "%Y%m%d-%H%M%S")), os.path.join(dirpath, d)))
                        except ValueError:
                            continue
                dirnames[:] = []
            else:
                dirnames[:] = [d for d in dirnames if not d.startswith(".") or d == TRASH_NAME]
    return found


def _tree_size(path: str) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for fn in filenames:
            try:
                total += os.lstat(os.path.join(dirpath, fn)).st_size
            except OSError:
                pass
    return total


def trash_usage(roots) -> int:
    """Total bytes held in trash stamps under roots."""
    return sum(_tree_size(path) for _, path in _trash_stamps(roots))


def purge_trash_for_space(roots, min_free_bytes: int, keep_recent_s: float = 86400, only_dev: int | None = None,
                          now: float | None = None) -> list[str]:
    """Where a filesystem has less than min_free_bytes free, delete its trash stamps
    oldest first until it has enough (or only stamps younger than keep_recent_s remain,
    so a low disk caused by something else can't empty the whole trash). only_dev limits
    this to one filesystem (st_dev). Returns deleted paths."""
    now = now or time.time()
    by_dev: dict[int, list[tuple[float, str]]] = {}
    for ts, path in _trash_stamps(roots):
        if now - ts < keep_recent_s:
            continue
        try:
            dev = os.stat(path).st_dev
        except OSError:
            continue
        if only_dev is None or dev == only_dev:
            by_dev.setdefault(dev, []).append((ts, path))
    removed = []
    for stamps in by_dev.values():
        stamps.sort()
        try:
            free = shutil.disk_usage(stamps[0][1]).free
        except OSError:
            continue
        for _, path in stamps:
            if free >= min_free_bytes:
                break
            size = _tree_size(path)
            shutil.rmtree(path, ignore_errors=True)
            free += size
            removed.append(path)
    return removed


def plex_refresh(conf_path: str, file_path: str) -> str:
    """Ask Plex to scan just the folder containing file_path. conf JSON:
    {plex_url, token, path_map: [[local_prefix, plex_prefix], ...]}. Never raises."""
    try:
        conf = json.load(open(conf_path))
        folder = os.path.dirname(file_path)
        plex_path = None
        for lp, pp in sorted(conf["path_map"], key=lambda m: -len(m[0])):
            if folder == lp.rstrip("/") or folder.startswith(lp.rstrip("/") + "/"):
                plex_path = pp.rstrip("/") + folder[len(lp.rstrip("/")):]
                break
        if not plex_path:
            return "no path mapping"
        base = conf["plex_url"].rstrip("/")
        q = urllib.parse.urlencode({"X-Plex-Token": conf["token"]})
        with urllib.request.urlopen(f"{base}/library/sections?{q}", timeout=15) as r:
            body = r.read()
        best = None
        for d in ET.fromstring(body).iter("Directory"):
            for loc in d.iter("Location"):
                lp = loc.get("path", "").rstrip("/")
                if (plex_path == lp or plex_path.startswith(lp + "/")) and (best is None or len(lp) > len(best[1])):
                    best = (d.get("key"), lp)
        if not best:
            return "not in any Plex library"
        q = urllib.parse.urlencode({"path": plex_path, "X-Plex-Token": conf["token"]})
        with urllib.request.urlopen(f"{base}/library/sections/{best[0]}/refresh?{q}", timeout=15) as r:
            return f"asked Plex to scan {plex_path} (HTTP {r.status})"
    except Exception as e:  # noqa: BLE001
        return f"plex refresh failed: {type(e).__name__}: {e}"
