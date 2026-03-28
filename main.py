import os
import re
import json
import shutil
import logging
import asyncio
import math
import secrets
import subprocess
import tempfile
import time
import platform
import datetime
import xml.etree.ElementTree as ET
from pathlib import Path
import urllib.request
from urllib.parse import quote, urlencode
from typing import Optional

import yaml

from contextlib import asynccontextmanager

import uvicorn.config as _uvc_config

_LOG_FMT  = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# Patch uvicorn's default log config before it applies it at startup
_uvc_config.LOGGING_CONFIG["formatters"]["default"] = {"format": _LOG_FMT, "datefmt": _DATE_FMT}
_uvc_config.LOGGING_CONFIG["formatters"]["access"]  = {"format": _LOG_FMT, "datefmt": _DATE_FMT}
_uvc_config.LOGGING_CONFIG.setdefault("loggers", {})[""] = {
    "handlers": ["default"], "level": "INFO",
}

from fastapi import FastAPI, Form, Request, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse, FileResponse
from fastapi.templating import Jinja2Templates
import aiosqlite

log = logging.getLogger(__name__)

# Random token generated at startup — must be present in X-Delete-Token header
DELETE_TOKEN = secrets.token_hex(32)

DEFAULT_ROOT = Path(os.environ.get("MEDIA_ROOT", "/media"))
DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "mediastat.db"))
CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", str(Path(__file__).parent / "config.yaml")))


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


_config = _load_config()

# Optional TMDB API key — set tmdb_api_key in config.yaml to enable TMDB search
TMDB_API_KEY: str = (_config.get("tmdb_api_key") or "").strip()

# Directories listed in config.yaml: [{label, path}, ...]
CONFIGURED_DIRS: list[dict] = [
    {"label": d.get("label", ""), "path": str(Path(d["path"]).expanduser())}
    for d in (_config.get("directories") or [])
    if d.get("path")
]

# ALLOWED_ROOTS: derived from config directories (plus legacy env var + DEFAULT_ROOT)
_allowed_roots_env = os.environ.get("ALLOWED_ROOTS", "")
ALLOWED_ROOTS = [
    Path(r).expanduser().resolve()
    for r in _allowed_roots_env.split("|")
    if r
] + [Path(d["path"]).resolve() for d in CONFIGURED_DIRS] + [DEFAULT_ROOT.resolve()]

# Active scan root — loaded from DB on startup, changeable at runtime
current_root: Path = DEFAULT_ROOT
_real_root: Path = Path(os.path.realpath(DEFAULT_ROOT))  # cached realpath, updated with current_root

MEDIA_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".ts", ".m4v"}

# Concurrency caps
_PROBE_SEM = asyncio.Semaphore(8)   # max parallel ffprobe processes
_DU_SEM    = asyncio.Semaphore(8)   # max parallel du processes
_SCAN_SEM  = asyncio.Semaphore(4)   # max in-flight scan tasks (DB conn + ffprobe)

# In-process TTL cache for directory sizes  {path_str: (size_bytes, timestamp)}
_dir_size_cache: dict[str, tuple[int, float]] = {}
_DIR_SIZE_TTL = 300  # seconds

# Short-lived cache for directory listings — avoids re-listing SMB dirs on every request
# {path_str: (entries: list[Path], timestamp)}
_dir_listing_cache: dict[str, tuple[list, float]] = {}
_DIR_LISTING_TTL = 30  # seconds


async def _list_dir(dir_path: Path) -> list[os.DirEntry]:
    key = str(dir_path)
    now = time.monotonic()
    cached = _dir_listing_cache.get(key)
    if cached and (now - cached[1]) < _DIR_LISTING_TTL:
        return cached[0]
    try:
        def _scan():
            with os.scandir(dir_path) as it:
                return list(it)
        entries = await asyncio.to_thread(_scan)
    except PermissionError:
        return []  # don't cache errors — let the next request retry
    if len(_dir_listing_cache) >= 500:
        for k in sorted(_dir_listing_cache, key=lambda k: _dir_listing_cache[k][1])[:250]:
            del _dir_listing_cache[k]
    _dir_listing_cache[key] = (entries, now)
    return entries


_duration_cache: dict[str, float] = {}  # path → duration, cached across seeks


@asynccontextmanager
async def lifespan(app: FastAPI):
    global current_root, _real_root, _hw_accel_info
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    await init_db()
    await _init_imdb()
    await _init_tmdb()
    current_root = await load_root()
    _real_root = Path(os.path.realpath(current_root))
    # Load persisted schedule config
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key = 'encode_schedule'") as cur:
            row = await cur.fetchone()
        if row:
            try:
                _schedule_config.update(json.loads(row[0]))
            except Exception:
                pass
    _hw_accel_info = await asyncio.to_thread(_detect_hw_accel_sync)
    hw = _hw_accel_info
    log.info("ffmpeg       : %s", "found" if shutil.which("ffmpeg") else "NOT FOUND — encoding unavailable")
    if hw.get("qsv"):
        log.info("GPU encoder  : Intel QSV (hevc_qsv)")
    elif hw.get("nvenc"):
        log.info("GPU encoder  : NVIDIA NVENC (hevc_nvenc)")
    elif hw.get("vaapi"):
        log.info("GPU encoder  : VA-API hevc_vaapi — %s", "AMD" if hw.get("amd") else "Intel (QSV unavailable)")
    else:
        log.info("GPU encoder  : none detected — software libx265 will be used")
    await _load_encode_jobs()
    worker = asyncio.create_task(_encode_worker())
    yield
    worker.cancel()
    try:
        await worker
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["pathquote"] = lambda p: quote(str(p), safe="")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    qs = f"?{request.url.query}" if request.url.query else ""
    log.info("%s %s%s", request.method, request.url.path, qs)
    # Capture HA ingress base path so templates and redirects can use it
    request.state.ingress_path = request.headers.get("X-Ingress-Path", "").rstrip("/")
    return await call_next(request)


# ── IMDB ─────────────────────────────────────────────────────────────────────
IMDB_DB_PATH = str(Path(DB_PATH).parent / "imdb.db")
TMDB_DB_PATH = str(Path(DB_PATH).parent / "tmdb.db")
IMDB_DATA_URL       = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_PRINCIPALS_URL = "https://datasets.imdbws.com/title.principals.tsv.gz"
IMDB_NAMES_URL      = "https://datasets.imdbws.com/name.basics.tsv.gz"
IMDB_RATINGS_URL    = "https://datasets.imdbws.com/title.ratings.tsv.gz"
_imdb_progress: dict = {}
_setdates_progress: dict = {"phase": "idle", "done": 0, "total": 0, "skipped": 0, "updated": 0, "errors": 0}


def _norm_title(s: str) -> str:
    """Normalise a title for matching: lowercase, remove apostrophes and slashes
    (Freddy's→Freddys, Self/less→Selfless), replace other punctuation with space."""
    s = s.lower().replace("&", " and ")
    s = re.sub(r"['/\u2019]", "", s)   # remove apostrophes and forward-slashes
    return re.sub(r"\s+", " ", re.sub(r"[:\",.?!\-\u2013\u2014]", " ", s)).strip()


async def _init_imdb():
    """Create IMDB schema (imdb.db + file_imdb table in main DB)."""
    async with aiosqlite.connect(IMDB_DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS imdb_titles (
                tconst              TEXT PRIMARY KEY,
                primary_title       TEXT,
                original_title      TEXT,
                start_year          INTEGER,
                runtime_minutes     INTEGER,
                genres              TEXT,
                norm_title          TEXT,
                norm_original_title TEXT,
                title_type          TEXT
            )
        """)
        # Migrations for existing DBs
        for col in ("norm_title TEXT", "norm_original_title TEXT", "title_type TEXT"):
            try:
                await db.execute(f"ALTER TABLE imdb_titles ADD COLUMN {col}")
                await db.commit()
            except Exception:
                pass  # column already exists
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_imdb_norm
            ON imdb_titles(norm_title, start_year)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_imdb_norm_orig
            ON imdb_titles(norm_original_title, start_year)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS imdb_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS imdb_cast (
                tconst      TEXT PRIMARY KEY,
                cast_names  TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS imdb_ratings (
                tconst         TEXT PRIMARY KEY,
                average_rating REAL,
                num_votes      INTEGER
            )
        """)
        # FTS5 index for fast text search (content= links to imdb_titles)
        await db.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS imdb_fts
            USING fts5(primary_title, content=imdb_titles, content_rowid=rowid)
        """)
        await db.commit()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS file_imdb (
                path            TEXT PRIMARY KEY,
                tconst          TEXT NOT NULL,
                primary_title   TEXT,
                start_year      INTEGER,
                genres          TEXT,
                runtime_minutes INTEGER,
                matched_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                source          TEXT DEFAULT 'imdb',
                release_date    TEXT
            )
        """)
        # Migrations for existing DBs
        for col, defn in [("source", "TEXT DEFAULT 'imdb'"), ("release_date", "TEXT"), ("rating", "REAL")]:
            try:
                await db.execute(f"ALTER TABLE file_imdb ADD COLUMN {col} {defn}")
            except Exception:
                pass  # column already exists
        await db.commit()


async def _init_tmdb():
    """Create / migrate TMDB cache schema in tmdb.db."""
    async with aiosqlite.connect(TMDB_DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tmdb_cache (
                tconst           TEXT PRIMARY KEY,
                tmdb_id          INTEGER,
                media_type       TEXT,
                title            TEXT,
                original_title   TEXT,
                release_date     TEXT,
                overview         TEXT,
                vote_average     REAL,
                vote_count       INTEGER,
                popularity       REAL,
                poster_path      TEXT,
                backdrop_path    TEXT,
                genre_ids        TEXT,
                original_language TEXT,
                fetched_at       TEXT
            )
        """)
        # Migrations for databases created before the schema was extended
        for col, defn in [
            ("original_title",    "TEXT"),
            ("overview",          "TEXT"),
            ("vote_average",      "REAL"),
            ("vote_count",        "INTEGER"),
            ("popularity",        "REAL"),
            ("poster_path",       "TEXT"),
            ("backdrop_path",     "TEXT"),
            ("genre_ids",         "TEXT"),
            ("original_language", "TEXT"),
        ]:
            try:
                await db.execute(f"ALTER TABLE tmdb_cache ADD COLUMN {col} {defn}")
            except Exception:
                pass  # column already exists
        await db.commit()


def _parse_media_filename(filename: str) -> tuple[str, int | None]:
    """Extract (title, year) from filenames like 'The Terminator (1984).mkv'."""
    stem = Path(filename).stem
    # "Title (Year)" or "Title (Year) [extras]"
    m = re.match(r'^(.+?)\s*\((\d{4})\)', stem)
    if m:
        return m.group(1).strip(), int(m.group(2))
    # "Title.Year.rest" Plex-style
    m = re.match(r'^(.+?)\.((?:19|20)\d{2})(?:\.|$)', stem)
    if m:
        return m.group(1).replace('.', ' ').strip(), int(m.group(2))
    # Year anywhere in name
    m = re.search(r'\b((?:19|20)\d{2})\b', stem)
    if m:
        return stem[:m.start()].strip(' -_.'), int(m.group(1))
    return stem, None


def _dl_gz_tracked(url: str, key: str, dl_bytes: dict, dl_total: dict) -> bytes:
    """Download a .gz file, writing per-key byte counts into shared dicts."""
    import urllib.request
    with urllib.request.urlopen(url, timeout=300) as resp:
        dl_total[key] = int(resp.headers.get("Content-Length", 0))
        chunks: list[bytes] = []
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
            dl_bytes[key] += len(chunk)
    return b"".join(chunks)


def _imdb_import_thread():
    """Download and import IMDB titles, cast, name, and ratings data. Runs in a thread."""
    global _imdb_progress
    import gzip, io, sqlite3
    from concurrent.futures import ThreadPoolExecutor

    try:
        # ── Phase 1: download all four files in parallel ───────────
        dl_bytes: dict[str, int] = {"titles": 0, "cast": 0, "names": 0, "ratings": 0}
        dl_total: dict[str, int] = {"titles": 0, "cast": 0, "names": 0, "ratings": 0}
        raw: dict[str, bytes] = {}
        _imdb_progress = {"phase": "downloading", "file": "all", "downloaded": 0, "total": 0, "rows": 0, "done": False}

        def _dl_one(key: str, url: str) -> None:
            raw[key] = _dl_gz_tracked(url, key, dl_bytes, dl_total)
            _imdb_progress["downloaded"] = sum(dl_bytes.values())
            _imdb_progress["total"] = sum(dl_total.values())

        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [
                pool.submit(_dl_one, "titles",  IMDB_DATA_URL),
                pool.submit(_dl_one, "cast",    IMDB_PRINCIPALS_URL),
                pool.submit(_dl_one, "names",   IMDB_NAMES_URL),
                pool.submit(_dl_one, "ratings", IMDB_RATINGS_URL),
            ]
            for fut in futs:
                fut.result()  # re-raise any download exception

        # ── Phase 2a: count matching titles for accurate progress ──────
        _TITLE_TYPES = {b"movie", b"tvMovie", b"tvMiniSeries", b"tvSpecial", b"video", b"short"}
        _imdb_progress = {"phase": "counting", "rows": 0, "total_titles": 0, "done": False}
        total_titles = 0
        with gzip.open(io.BytesIO(raw["titles"])) as f:
            next(f)  # skip header
            for line in f:
                p2 = line.split(b"\t", 2)
                if len(p2) >= 2 and p2[1] in _TITLE_TYPES:
                    total_titles += 1

        # ── Phase 2b: parse titles, write to DB, build imported_tconsts ─
        _imdb_progress = {"phase": "parsing", "file": "titles", "rows": 0, "total_titles": total_titles, "done": False}
        conn = sqlite3.connect(IMDB_DB_PATH, timeout=60)
        try:
            conn.execute("DELETE FROM imdb_titles")
            conn.execute("DELETE FROM imdb_cast")
            conn.execute("DELETE FROM imdb_ratings")
            conn.commit()

            imported_tconsts: set[str] = set()
            rows = 0
            batch: list[tuple] = []
            with gzip.open(io.BytesIO(raw["titles"])) as f:
                next(f)  # skip header
                for line in f:
                    parts = line.decode("utf-8", errors="replace").rstrip("\r\n").split("\t")
                    if len(parts) < 9:
                        continue
                    tconst, ttype, ptitle, otitle, _, syear, _, rmin, genres = parts[:9]
                    if ttype not in ("movie", "tvMovie", "tvMiniSeries", "tvSpecial", "video", "short"):
                        continue
                    norm_o = _norm_title(otitle) if otitle and otitle != r"\N" else None
                    batch.append((
                        tconst, ptitle, otitle if otitle != r"\N" else None,
                        int(syear) if syear != r"\N" else None,
                        int(rmin) if rmin != r"\N" else None,
                        None if genres == r"\N" else genres,
                        _norm_title(ptitle),
                        norm_o if norm_o != _norm_title(ptitle) else None,
                        ttype,
                    ))
                    imported_tconsts.add(tconst)
                    rows += 1
                    if len(batch) >= 5000:
                        conn.executemany("INSERT OR REPLACE INTO imdb_titles VALUES (?,?,?,?,?,?,?,?,?)", batch)
                        conn.commit()
                        batch = []
                        _imdb_progress["rows"] = rows
            if batch:
                conn.executemany("INSERT OR REPLACE INTO imdb_titles VALUES (?,?,?,?,?,?,?,?,?)", batch)
                conn.commit()
            _imdb_progress["rows"] = rows
        finally:
            conn.close()
        del raw["titles"]

        # ── Phase 3: parse principals + ratings in parallel ────────
        # Both only need `imported_tconsts` (read-only), so safe to run together.
        _imdb_progress = {"phase": "parsing", "file": "cast+ratings", "rows": rows, "done": False}
        cast_map: dict[str, list[tuple[int, str]]] = {}
        rating_rows: list[tuple] = []

        def _parse_cast() -> None:
            with gzip.open(io.BytesIO(raw["cast"])) as f:
                next(f)
                for line in f:
                    parts = line.decode("utf-8", errors="replace").rstrip("\r\n").split("\t")
                    if len(parts) < 4:
                        continue
                    tconst, ordering, nconst, category = parts[0], parts[1], parts[2], parts[3]
                    if tconst not in imported_tconsts or category not in ("actor", "actress"):
                        continue
                    try:
                        ord_int = int(ordering)
                    except ValueError:
                        continue
                    if ord_int <= 5:
                        cast_map.setdefault(tconst, []).append((ord_int, nconst))

        def _parse_ratings() -> None:
            with gzip.open(io.BytesIO(raw["ratings"])) as f:
                next(f)
                for line in f:
                    parts = line.decode("utf-8", errors="replace").rstrip("\r\n").split("\t")
                    if len(parts) < 3:
                        continue
                    tconst, avg, votes = parts[0], parts[1], parts[2]
                    if tconst not in imported_tconsts:
                        continue
                    try:
                        rating_rows.append((tconst, float(avg), int(votes)))
                    except ValueError:
                        continue

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(_parse_cast)
            f2 = pool.submit(_parse_ratings)
            f1.result()
            f2.result()
        del raw["cast"], raw["ratings"]

        # ── Phase 4: parse names (needs needed_nconsts from cast_map) ──
        _imdb_progress = {"phase": "parsing", "file": "names", "rows": rows, "done": False}
        needed_nconsts: set[str] = {nc for entries in cast_map.values() for _, nc in entries}
        name_map: dict[str, str] = {}
        with gzip.open(io.BytesIO(raw["names"])) as f:
            next(f)
            for line in f:
                parts = line.decode("utf-8", errors="replace").rstrip("\r\n").split("\t")
                if len(parts) < 2:
                    continue
                nconst, pname = parts[0], parts[1]
                if nconst in needed_nconsts:
                    name_map[nconst] = pname
        del raw["names"]

        # ── Phase 5: write cast + ratings to DB, rebuild FTS ──────
        _imdb_progress = {"phase": "indexing", "rows": rows, "done": False}
        conn = sqlite3.connect(IMDB_DB_PATH, timeout=60)
        try:
            cast_batch: list[tuple] = []
            for tconst, entries in cast_map.items():
                entries.sort(key=lambda x: x[0])
                names = [name_map[nc] for _, nc in entries if nc in name_map]
                if names:
                    cast_batch.append((tconst, ", ".join(names)))
                if len(cast_batch) >= 5000:
                    conn.executemany("INSERT OR REPLACE INTO imdb_cast VALUES (?,?)", cast_batch)
                    conn.commit()
                    cast_batch = []
            if cast_batch:
                conn.executemany("INSERT OR REPLACE INTO imdb_cast VALUES (?,?)", cast_batch)
                conn.commit()

            for i in range(0, len(rating_rows), 5000):
                conn.executemany("INSERT OR REPLACE INTO imdb_ratings VALUES (?,?,?)", rating_rows[i:i + 5000])
                conn.commit()

            conn.execute("INSERT INTO imdb_fts(imdb_fts) VALUES('rebuild')")
            conn.execute("INSERT OR REPLACE INTO imdb_meta VALUES ('updated_at', datetime('now'))")
            conn.execute(f"INSERT OR REPLACE INTO imdb_meta VALUES ('count', '{rows}')")
            conn.commit()
        finally:
            conn.close()

        _imdb_progress = {"phase": "done", "rows": rows, "done": True}
    except Exception as exc:
        _imdb_progress = {"phase": "error", "error": str(exc), "done": True}


@app.post("/imdb/start-download")
async def imdb_start_download():
    if _imdb_progress.get("phase") in ("downloading", "parsing", "indexing"):
        return {"error": "Import already in progress"}
    import threading
    threading.Thread(target=_imdb_import_thread, daemon=True).start()
    return {"started": True}


@app.get("/imdb/download-progress")
async def imdb_download_progress():
    async def _gen():
        while True:
            p = dict(_imdb_progress)
            yield f"data: {json.dumps(p)}\n\n"
            if p.get("done"):
                break
            await asyncio.sleep(0.5)
    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


_TMDB_INSERT = (
    "INSERT OR REPLACE INTO tmdb_cache "
    "(tconst, tmdb_id, media_type, title, original_title, release_date, "
    " overview, vote_average, vote_count, popularity, "
    " poster_path, backdrop_path, genre_ids, original_language, fetched_at) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))"
)


def _tmdb_row_from_api(tconst: str, item: dict, media_type: str) -> tuple:
    """Extract a full tmdb_cache row tuple from a TMDB API result item."""
    if media_type == "movie":
        title    = item.get("title") or item.get("original_title")
        orig     = item.get("original_title")
        rel_date = item.get("release_date") or None
    else:
        title    = item.get("name") or item.get("original_name")
        orig     = item.get("original_name")
        rel_date = item.get("first_air_date") or None
    genre_ids = json.dumps(item.get("genre_ids") or [])
    return (
        tconst,
        item.get("id"),
        media_type,
        title,
        orig if orig != title else None,
        rel_date,
        item.get("overview") or None,
        item.get("vote_average"),
        item.get("vote_count"),
        item.get("popularity"),
        item.get("poster_path"),
        item.get("backdrop_path"),
        genre_ids,
        item.get("original_language"),
    )


_tmdb_cache_progress: dict = {}


def _tmdb_cache_thread(force: bool = False) -> None:
    global _tmdb_cache_progress
    import sqlite3, time

    try:
        with sqlite3.connect(DB_PATH, timeout=60) as conn:
            rows = conn.execute(
                "SELECT DISTINCT tconst FROM file_imdb WHERE tconst LIKE 'tt%'"
            ).fetchall()
        tconsts = [r[0] for r in rows]
        total = len(tconsts)
        _tmdb_cache_progress = {
            "phase": "fetching", "done": 0, "total": total,
            "found": 0, "not_found": 0, "errors": 0, "finished": False,
        }

        with sqlite3.connect(TMDB_DB_PATH, timeout=60) as conn:
            done = found = not_found = errors = 0
            for tconst in tconsts:
                if not force:
                    if conn.execute(
                        "SELECT 1 FROM tmdb_cache WHERE tconst=?", (tconst,)
                    ).fetchone():
                        done += 1
                        _tmdb_cache_progress.update({"done": done})
                        continue

                try:
                    data = _tmdb_api_get(f"/find/{tconst}", {"external_source": "imdb_id"})
                    row = None
                    for item in (data.get("movie_results") or [])[:1]:
                        row = _tmdb_row_from_api(tconst, item, "movie")
                    if not row:
                        for item in (data.get("tv_results") or [])[:1]:
                            row = _tmdb_row_from_api(tconst, item, "tv")
                    if row:
                        conn.execute(_TMDB_INSERT, row)
                        conn.commit()
                        found += 1
                    else:
                        not_found += 1
                except Exception:
                    errors += 1

                done += 1
                _tmdb_cache_progress.update({"done": done, "found": found,
                                             "not_found": not_found, "errors": errors})
                time.sleep(0.1)  # ~10 req/s — within TMDB free-tier limit

        _tmdb_cache_progress = {
            "phase": "done", "done": done, "total": total,
            "found": found, "not_found": not_found, "errors": errors, "finished": True,
        }
    except Exception as exc:
        _tmdb_cache_progress = {"phase": "error", "error": str(exc), "finished": True}


@app.get("/tmdb/status")
async def tmdb_status():
    return {"configured": bool(TMDB_API_KEY)}


@app.post("/tmdb/build-cache")
async def tmdb_build_cache(request: Request):
    if not TMDB_API_KEY:
        raise HTTPException(503, "TMDB not configured")
    body = await request.json()
    force = bool(body.get("force", False))
    if _tmdb_cache_progress.get("phase") == "fetching":
        return {"error": "Already running"}
    import threading
    threading.Thread(target=_tmdb_cache_thread, kwargs={"force": force}, daemon=True).start()
    return {"started": True}


@app.get("/tmdb/cache-progress")
async def tmdb_cache_progress_sse():
    async def _gen():
        while True:
            p = dict(_tmdb_cache_progress)
            yield f"data: {json.dumps(p)}\n\n"
            if p.get("finished"):
                break
            await asyncio.sleep(0.5)
    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/tmdb/cache-status")
async def tmdb_cache_status_route():
    try:
        async with aiosqlite.connect(TMDB_DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM tmdb_cache") as cur:
                count = (await cur.fetchone())[0]
            async with db.execute(
                "SELECT MAX(fetched_at) FROM tmdb_cache"
            ) as cur:
                row = await cur.fetchone()
                last_fetched = row[0] if row else None
    except Exception:
        count, last_fetched = 0, None
    return {
        "count": count,
        "last_fetched": last_fetched,
        "running": _tmdb_cache_progress.get("phase") == "fetching",
    }


def _tmdb_api_get(path: str, params: dict) -> dict:
    """Synchronous TMDB API call — run via asyncio.to_thread."""
    p = dict(params)
    p["api_key"] = TMDB_API_KEY
    url = "https://api.themoviedb.org/3" + path + "?" + urlencode(p)
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read())


@app.get("/tmdb/search")
async def tmdb_search(q: str = Query(...), year: Optional[int] = Query(None)):
    if not TMDB_API_KEY:
        raise HTTPException(503, "TMDB API key not configured — add tmdb_api_key to config.yaml")
    q = q.strip()
    if not q:
        return []

    results: list[dict] = []
    base_params: dict = {"query": q, "include_adult": "false", "language": "en-US", "page": "1"}

    # Movies
    movie_params = dict(base_params)
    if year:
        movie_params["year"] = str(year)
    try:
        data = await asyncio.to_thread(_tmdb_api_get, "/search/movie", movie_params)
        for item in data.get("results", [])[:10]:
            rd = item.get("release_date") or ""
            results.append({
                "tconst":          f"tmdb:{item['id']}",
                "primary_title":   item.get("title", ""),
                "original_title":  item.get("original_title", ""),
                "release_date":    rd,
                "start_year":      int(rd[:4]) if rd else None,
                "runtime_minutes": None,
                "genres":          "",
                "cast_names":      "",
                "source":          "tmdb",
                "media_type":      "movie",
            })
    except Exception as exc:
        log.warning("TMDB movie search failed: %s", exc)

    # TV series
    tv_params = dict(base_params)
    if year:
        tv_params["first_air_date_year"] = str(year)
    try:
        tv_data = await asyncio.to_thread(_tmdb_api_get, "/search/tv", tv_params)
        for item in tv_data.get("results", [])[:5]:
            rd = item.get("first_air_date") or ""
            results.append({
                "tconst":          f"tmdb:tv:{item['id']}",
                "primary_title":   item.get("name", ""),
                "original_title":  item.get("original_name", ""),
                "release_date":    rd,
                "start_year":      int(rd[:4]) if rd else None,
                "runtime_minutes": None,
                "genres":          "",
                "cast_names":      "",
                "source":          "tmdb",
                "media_type":      "tv",
            })
    except Exception as exc:
        log.warning("TMDB TV search failed: %s", exc)

    return results


@app.post("/tmdb/enrich")
async def tmdb_enrich(request: Request):
    """Fetch full release date from TMDB using the stored IMDb tconst and update file_imdb.
    Checks local tmdb_cache first; falls back to the API on a cache miss and stores the result."""
    if not TMDB_API_KEY:
        raise HTTPException(503, "TMDB not configured")
    body = await request.json()
    path   = body.get("path", "").strip()
    tconst = body.get("tconst", "").strip()
    if not path or not tconst or not tconst.startswith("tt"):
        raise HTTPException(400, "path and IMDb tconst (tt…) required")
    safe_path(path)

    release_date: Optional[str] = None

    # ── 1. Check local cache ──────────────────────────────────────────
    try:
        async with aiosqlite.connect(TMDB_DB_PATH) as db:
            async with db.execute(
                "SELECT release_date FROM tmdb_cache WHERE tconst=?", (tconst,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    release_date = row[0]
    except Exception:
        pass

    # ── 2. Cache miss — fetch from TMDB API and store ─────────────────
    if not release_date:
        try:
            data = await asyncio.to_thread(
                _tmdb_api_get, f"/find/{tconst}", {"external_source": "imdb_id"}
            )
        except Exception as exc:
            raise HTTPException(502, f"TMDB lookup failed: {exc}")

        cache_row: Optional[tuple] = None
        for item in (data.get("movie_results") or [])[:1]:
            cache_row    = _tmdb_row_from_api(tconst, item, "movie")
            release_date = cache_row[5]  # index of release_date in the tuple
        if not cache_row:
            for item in (data.get("tv_results") or [])[:1]:
                cache_row    = _tmdb_row_from_api(tconst, item, "tv")
                release_date = cache_row[5]

        if cache_row:
            try:
                async with aiosqlite.connect(TMDB_DB_PATH) as db:
                    await db.execute(_TMDB_INSERT, cache_row)
                    await db.commit()
            except Exception:
                pass

    # ── 3. Persist to file_imdb ───────────────────────────────────────
    if release_date:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE file_imdb SET release_date=? WHERE path=?", (release_date, path)
            )
            await db.commit()
    return {"ok": True, "release_date": release_date}


@app.get("/imdb/status")
async def imdb_status():
    try:
        async with aiosqlite.connect(IMDB_DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM imdb_titles") as cur:
                count = (await cur.fetchone())[0]
            async with db.execute("SELECT value FROM imdb_meta WHERE key='updated_at'") as cur:
                row = await cur.fetchone()
                updated_at = row[0] if row else None
        return {"downloaded": count > 0, "count": count, "updated_at": updated_at,
                "in_progress": _imdb_progress.get("phase") not in (None, "done", "error")}
    except Exception:
        return {"downloaded": False, "count": 0, "updated_at": None, "in_progress": False}


@app.get("/imdb/search")
async def imdb_search(q: str = Query(...), year: Optional[int] = Query(None), runtime: Optional[int] = Query(None)):
    q = q.strip()
    if not q:
        return []
    # Strip file extension if query looks like a filename (e.g. "Movie.mkv", "Title (2023).mp4")
    q = re.sub(r'\.[a-zA-Z0-9]{2,5}$', '', q).strip()
    if not q:
        return []

    q_norm = _norm_title(q)
    cols = "t.tconst, t.primary_title, t.original_title, t.start_year, t.runtime_minutes, t.genres, c.cast_names, r.average_rating, t.title_type"
    keys = ["tconst", "primary_title", "original_title", "start_year", "runtime_minutes", "genres", "cast_names", "average_rating", "title_type"]
    from_clause = "imdb_titles t LEFT JOIN imdb_cast c ON t.tconst = c.tconst LEFT JOIN imdb_ratings r ON t.tconst = r.tconst"
    results: list[dict] = []
    seen: set[str] = set()

    def _rows(raw):
        out = []
        for r in raw:
            d = dict(zip(keys, r))
            if d["tconst"] not in seen:
                seen.add(d["tconst"])
                out.append(d)
        return out

    try:
        async with aiosqlite.connect(IMDB_DB_PATH) as db:
            # 0. Direct tconst lookup (e.g. tt1234567)
            if re.match(r'^tt\d+$', q, re.IGNORECASE):
                async with db.execute(
                    f"SELECT {cols} FROM {from_clause} WHERE t.tconst=?", (q,)
                ) as cur:
                    results += _rows(await cur.fetchall())
                return results  # tconst is unique, no need for further tiers

            # 1. Exact normalised primary OR original title + year
            if year:
                async with db.execute(
                    f"SELECT {cols} FROM {from_clause}"
                    " WHERE (t.norm_title=? OR t.norm_original_title=?) AND t.start_year=?",
                    (q_norm, q_norm, year)
                ) as cur:
                    results += _rows(await cur.fetchall())

            # 2. Exact normalised primary OR original title, any year
            if len(results) < 5:
                async with db.execute(
                    f"SELECT {cols} FROM {from_clause}"
                    " WHERE t.norm_title=? OR t.norm_original_title=?"
                    " ORDER BY t.start_year DESC LIMIT 10",
                    (q_norm, q_norm)
                ) as cur:
                    results += _rows(await cur.fetchall())

            # 3. FTS phrase search
            if len(results) < 5:
                try:
                    fts_q = '"' + q_norm.replace('"', '""') + '"'
                    async with db.execute(
                        f"SELECT {cols} FROM {from_clause} WHERE t.rowid IN"
                        " (SELECT rowid FROM imdb_fts WHERE imdb_fts MATCH ?)"
                        " ORDER BY CASE WHEN t.start_year=? THEN 0 ELSE 1 END, t.start_year DESC LIMIT 20",
                        (fts_q, year or 0)
                    ) as cur:
                        results += _rows(await cur.fetchall())
                except Exception:
                    pass

            # 4. LIKE fallback on norm_title
            if len(results) < 5:
                async with db.execute(
                    f"SELECT {cols} FROM {from_clause} WHERE t.norm_title LIKE ?"
                    " ORDER BY CASE WHEN t.start_year=? THEN 0 ELSE 1 END, t.start_year DESC LIMIT 20",
                    (f"%{q_norm}%", year or 0)
                ) as cur:
                    results += _rows(await cur.fetchall())
    except Exception as exc:
        log.error("IMDB search error: %s", exc)

    # Sort by runtime proximity when caller provides a file duration (in minutes)
    if runtime and results:
        def _rt_key(r):
            rm = r.get("runtime_minutes")
            if rm is None:
                return 9999
            return abs(rm - runtime)
        results.sort(key=_rt_key)

    return results[:20]


def _embed_file_meta_sync(path: str, tconst: str, title: str,
                           year: Optional[int], genres: Optional[str],
                           original_title: Optional[str] = None,
                           runtime_minutes: Optional[int] = None) -> None:
    """Write IMDB metadata into the file container (blocking, run in executor)."""
    # Preserve mtime so tag writes don't make the file appear newly modified
    stat = os.stat(path)
    orig_times = (stat.st_atime, stat.st_mtime)

    ext = Path(path).suffix.lower()
    if ext == ".mkv":
        # Build Matroska XML tags
        tags_el = ET.Element("Tags")
        tag_el  = ET.SubElement(tags_el, "Tag")
        ET.SubElement(tag_el, "Targets")  # empty = global scope
        for name, val in [
            ("TITLE", title),
            ("ORIGINAL_TITLE", original_title if original_title and original_title != title else None),
            ("DATE_RELEASED", str(year) if year else None),
            ("GENRE", genres),
            ("IMDB", tconst),
            ("RUNTIME", str(runtime_minutes) if runtime_minutes else None),
        ]:
            if val:
                s = ET.SubElement(tag_el, "Simple")
                ET.SubElement(s, "Name").text  = name
                ET.SubElement(s, "String").text = val
        tree = ET.ElementTree(tags_el)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                         suffix=".xml", delete=False) as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<!DOCTYPE Tags SYSTEM "matroska-tags.dtd">\n')
            tree.write(f, encoding="unicode")
            tmpfile = f.name
        try:
            if not shutil.which("mkvpropedit"):
                log.debug("mkvpropedit not found, skipping MKV tag embed for %s", path)
                return
            subprocess.run(
                ["mkvpropedit", path,
                 "--edit", "info", "--set", f"title={title}",
                 "--tags", f"global:{tmpfile}"],
                check=False, capture_output=True,
            )
        finally:
            Path(tmpfile).unlink(missing_ok=True)

    elif ext == ".mp4":
        try:
            from mutagen.mp4 import MP4, MP4FreeForm  # type: ignore
        except ImportError:
            log.debug("mutagen not installed, skipping MP4 tag embed for %s", path)
            return
        try:
            tags = MP4(path)
            if tags.tags is None:
                tags.add_tags()
            tags.tags["\xa9nam"] = [title]
            if year:
                tags.tags["\xa9day"] = [str(year)]
            if genres:
                tags.tags["\xa9gen"] = [genres.replace(",", ";")]
            tags.tags["----:com.apple.iTunes:IMDB"] = [MP4FreeForm(tconst.encode())]
            if original_title and original_title != title:
                tags.tags["----:com.apple.iTunes:ORIGINAL_TITLE"] = [MP4FreeForm(original_title.encode())]
            if runtime_minutes:
                tags.tags["----:com.apple.iTunes:RUNTIME"] = [MP4FreeForm(str(runtime_minutes).encode())]
            tags.save()
        except Exception as exc:
            log.warning("mutagen MP4 tag write failed for %s: %s", path, exc)

    # Restore original mtime (and atime) so tag writes are invisible to the file browser
    try:
        os.utime(path, orig_times)
    except Exception as exc:
        log.debug("Could not restore mtime for %s: %s", path, exc)


async def _embed_file_meta(path: str, tconst: str, title: str,
                            year: Optional[int], genres: Optional[str],
                            original_title: Optional[str] = None,
                            runtime_minutes: Optional[int] = None) -> None:
    try:
        await asyncio.to_thread(_embed_file_meta_sync, path, tconst, title, year,
                                genres, original_title, runtime_minutes)
    except Exception as exc:
        log.warning("embed_file_meta failed for %s: %s", path, exc)


def _read_mp4_tconst_sync(path: str) -> Optional[str]:
    """Read embedded tconst from MP4 iTunes freeform atom (blocking)."""
    try:
        from mutagen.mp4 import MP4  # type: ignore
        tags = MP4(path).tags
        if tags:
            val = tags.get("----:com.apple.iTunes:IMDB")
            if val:
                return bytes(val[0]).decode("utf-8", errors="ignore").strip() or None
    except Exception:
        pass
    return None


@app.post("/imdb/match")
async def imdb_match(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    path = body.get("path", "").strip()
    tconst = body.get("tconst", "").strip()
    if not path or not tconst:
        raise HTTPException(400, "path and tconst required")
    safe_path(path)  # security check
    source       = body.get("source", "imdb")
    release_date = (body.get("release_date") or "").strip() or None
    rating_raw   = body.get("average_rating")
    rating       = float(rating_raw) if rating_raw is not None else None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO file_imdb "
            "(path, tconst, primary_title, start_year, genres, runtime_minutes, source, release_date, rating) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (path, tconst, body.get("primary_title"), body.get("start_year"),
             body.get("genres"), body.get("runtime_minutes"), source, release_date, rating)
        )
        await db.commit()
    # Optionally set file mtime in the same request, avoiding a second round-trip
    if body.get("set_dates"):
        start_year = body.get("start_year")
        if release_date and len(release_date) == 10:
            yr, mo, dy = int(release_date[:4]), int(release_date[5:7]), int(release_date[8:10])
            ts = datetime.datetime(yr, mo, dy, 12, 0, 0).timestamp()
        elif start_year:
            ts = datetime.datetime(int(start_year), 1, 1, 12, 0, 0).timestamp()
        else:
            ts = None
        if ts:
            await asyncio.to_thread(os.utime, path, (ts, ts))
    # Embed metadata into the file container in the background (unless caller opted out)
    # Skip metadata embedding for TMDB matches (no embedded tconst for non-IMDb IDs)
    if body.get("embed_meta", True) and source == "imdb":
        asyncio.create_task(_embed_file_meta(
            path, tconst,
            body.get("primary_title") or "",
            body.get("start_year"),
            body.get("genres"),
            body.get("original_title"),
            body.get("runtime_minutes"),
        ))
    return {"ok": True}


@app.delete("/imdb/match")
async def imdb_unmatch(path: str = Query(...)):
    safe_path(path)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM file_imdb WHERE path=?", (path,))
        await db.commit()
    return {"ok": True}


@app.post("/imdb/set-release-date")
async def imdb_set_release_date(request: Request):
    """Set file mtime (and atime) to Jan 1 of the IMDB release year."""
    body = await request.json()
    path = body.get("path", "").strip()
    if not path:
        raise HTTPException(400, "path required")
    safe_path(path)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT start_year, release_date FROM file_imdb WHERE path=?", (path,)
        ) as cur:
            row = await cur.fetchone()
    if not row or (not row[0] and not row[1]):
        raise HTTPException(400, "No match with date for this file")
    release_date = row[1]  # YYYY-MM-DD from TMDB, or None
    if release_date and len(release_date) == 10:
        yr, mo, dy = int(release_date[:4]), int(release_date[5:7]), int(release_date[8:10])
        ts = datetime.datetime(yr, mo, dy, 12, 0, 0).timestamp()
        return_date = release_date
    else:
        yr = int(row[0])
        ts = datetime.datetime(yr, 1, 1, 12, 0, 0).timestamp()
        return_date = str(yr)
    await asyncio.to_thread(os.utime, path, (ts, ts))
    return {"ok": True, "date": return_date}


@app.post("/imdb/set-release-dates")
async def imdb_set_release_dates(request: Request):
    """Bulk-set mtime for all IMDB-matched files under a given directory."""
    body = await request.json()
    paths = body.get("paths") or []
    updated = 0
    errors = 0
    for path in paths:
        try:
            safe_path(path)
        except HTTPException:
            errors += 1
            continue
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT start_year, release_date FROM file_imdb WHERE path=?", (path,)
                ) as cur:
                    row = await cur.fetchone()
            if row and (row[0] or row[1]):
                rd = row[1]
                if rd and len(rd) == 10:
                    yr, mo, dy = int(rd[:4]), int(rd[5:7]), int(rd[8:10])
                    ts = datetime.datetime(yr, mo, dy, 12, 0, 0).timestamp()
                else:
                    ts = datetime.datetime(int(row[0]), 1, 1, 12, 0, 0).timestamp()
                await asyncio.to_thread(os.utime, path, (ts, ts))
                updated += 1
        except Exception as exc:
            log.warning("set-release-date failed for %s: %s", path, exc)
            errors += 1
    return {"ok": True, "updated": updated, "errors": errors}


def _setdates_thread() -> None:
    """Background thread: set mtime for all IMDb-matched files, skipping those already correct.

    For files whose file_imdb.release_date is NULL, checks tmdb_cache for a full date and
    backfills file_imdb.release_date in the process.
    """
    global _setdates_progress
    _setdates_progress = {"phase": "running", "done": 0, "total": 0,
                          "skipped": 0, "updated": 0, "errors": 0, "error_log": []}
    try:
        import sqlite3 as _sq3

        # Load tmdb_cache release dates (tconst → release_date) into memory
        tmdb_dates: dict[str, str] = {}
        try:
            tc = _sq3.connect(TMDB_DB_PATH)
            for row in tc.execute("SELECT tconst, release_date FROM tmdb_cache WHERE release_date IS NOT NULL"):
                tmdb_dates[row[0]] = row[1]
            tc.close()
        except Exception:
            pass  # tmdb.db may not exist

        conn = _sq3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT path, tconst, start_year, release_date FROM file_imdb "
            "WHERE start_year IS NOT NULL OR release_date IS NOT NULL"
        ).fetchall()

        total = len(rows)
        _setdates_progress["total"] = total

        for i, (fpath, tconst, start_year, release_date) in enumerate(rows):
            _setdates_progress["done"] = i

            # Backfill release_date from tmdb_cache if missing
            if not release_date and tconst and tconst in tmdb_dates:
                release_date = tmdb_dates[tconst]
                conn.execute(
                    "UPDATE file_imdb SET release_date=? WHERE path=?",
                    (release_date, fpath)
                )
                conn.commit()

            # Compute target timestamp
            if release_date and len(release_date) == 10:
                yr, mo, dy = int(release_date[:4]), int(release_date[5:7]), int(release_date[8:10])
                ts = datetime.datetime(yr, mo, dy, 12, 0, 0).timestamp()
            elif start_year:
                ts = datetime.datetime(int(start_year), 1, 1, 12, 0, 0).timestamp()
            else:
                _setdates_progress["skipped"] += 1
                continue
            try:
                p = Path(fpath)
                if not p.exists():
                    _setdates_progress["errors"] += 1
                    _setdates_progress["error_log"].append(f"{Path(fpath).name}: file not found")
                    continue
                if abs(p.stat().st_mtime - ts) < 2.0:
                    _setdates_progress["skipped"] += 1
                    continue
                os.utime(fpath, (ts, ts))
                _setdates_progress["updated"] += 1
            except Exception as exc:
                log.warning("set-dates-bg: %s: %s", fpath, exc)
                _setdates_progress["errors"] += 1
                _setdates_progress["error_log"].append(f"{Path(fpath).name}: {exc}")

        conn.close()
        _setdates_progress["done"] = total
        _setdates_progress["phase"] = "done"
    except Exception as exc:
        _setdates_progress["phase"] = "error"
        _setdates_progress["error"] = str(exc)


@app.post("/imdb/set-dates-bg")
async def set_dates_bg():
    if _setdates_progress.get("phase") == "running":
        return {"error": "Already running"}
    import threading
    threading.Thread(target=_setdates_thread, daemon=True).start()
    return {"started": True}


@app.get("/imdb/set-dates-progress")
async def set_dates_progress_sse():
    async def _gen():
        while True:
            p = dict(_setdates_progress)
            yield f"data: {json.dumps(p)}\n\n"
            if p.get("phase") in ("done", "error", "idle"):
                break
            await asyncio.sleep(0.5)
    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/imdb/set-dates-status")
async def set_dates_status():
    return _setdates_progress


@app.get("/imdb/matches")
async def imdb_matches(dir: str = Query(...)):
    dir_path = safe_path(dir)
    prefix = str(dir_path) + "/"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT path, tconst, primary_title, start_year, genres, runtime_minutes, source, release_date, rating "
            "FROM file_imdb WHERE path LIKE ?",
            (prefix + "%",)
        ) as cur:
            rows = await cur.fetchall()
    result = {}
    for path, tconst, primary_title, start_year, genres, runtime_minutes, source, release_date, rating in rows:
        # Only direct children (no further slashes beyond the prefix)
        if "/" not in path[len(prefix):]:
            result[path] = {
                "tconst": tconst, "primary_title": primary_title,
                "start_year": start_year, "genres": genres,
                "runtime_minutes": runtime_minutes,
                "source": source or "imdb",
                "release_date": release_date,
                "average_rating": rating,
            }
    return result


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS file_meta (
                path        TEXT PRIMARY KEY,
                size        INTEGER,
                mtime       REAL,
                video_codec TEXT,
                audio_codec TEXT,
                width       INTEGER,
                height      INTEGER,
                duration_min INTEGER,
                scanned_at   REAL,
                duration_sec REAL,
                hdr_type     TEXT
            )
        """)
        # Migrations: add columns to existing databases
        for col_sql in [
            "ALTER TABLE file_meta ADD COLUMN scanned_at REAL",
            "ALTER TABLE file_meta ADD COLUMN duration_sec REAL",
            "ALTER TABLE file_meta ADD COLUMN hdr_type TEXT",
        ]:
            try:
                await db.execute(col_sql)
            except Exception:
                pass  # column already exists
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS encode_jobs (
                id          TEXT PRIMARY KEY,
                input_path  TEXT NOT NULL,
                output_path TEXT NOT NULL,
                config      TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'queued',
                encoder     TEXT DEFAULT '',
                input_size  INTEGER DEFAULT 0,
                output_size INTEGER DEFAULT 0,
                started_at  REAL DEFAULT 0,
                finished_at REAL,
                error       TEXT,
                created_at  REAL NOT NULL
            )
        """)
        await db.commit()


async def load_root() -> Path:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key = 'root'") as cur:
            row = await cur.fetchone()
            return Path(row[0]) if row else DEFAULT_ROOT


async def save_root(path: Path):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('root', ?)",
            (str(path),),
        )
        await db.commit()


def codec_css_class(codec: str) -> str:
    c = (codec or "").lower()
    if c in ("hevc", "h265"):  return "codec-h265"
    if c in ("h264", "avc"):   return "codec-h264"
    if c == "av1":             return "codec-av1"
    if c == "vp9":             return "codec-vp9"
    return "codec-other"


BROWSER_SAFE_AUDIO = {"aac", "mp3", "opus", "flac", "vorbis"}


def ext_css_class(ext: str) -> str:
    e = (ext or "").lower()
    if e == ".mkv": return "ext-mkv"
    if e == ".mp4": return "ext-mp4"
    if e == ".avi": return "ext-avi"
    return "ext-other"


def res_css_class(width: int, height: int) -> str:
    w, h = width or 0, height or 0
    if w >= 3000 or h >= 2000: return "res-4k"
    if w >= 1700 or h >= 900:  return "res-1080"
    if w > 0 or h > 0:         return "res-low"
    return "res-unknown"


def hdr_css_class(hdr_type: str) -> str:
    return {"DV": "hdr-dv", "HDR": "hdr-hdr", "HLG": "hdr-hlg"}.get(hdr_type or "", "hdr-sdr")


_GB = 1024 ** 3

def size_css_class(size_bytes: int) -> str:
    if size_bytes >= 20 * _GB: return "size-red"
    if size_bytes >= 15 * _GB: return "size-amber"
    return "size-green"


_VALID_HDR_TYPES = frozenset({"DV", "HDR", "HLG", "SDR"})


def _hdr_type_from_stream(video: dict) -> str:
    """Return 'DV', 'HDR', 'HLG', or 'SDR' from ffprobe stream data."""
    profile = (video.get("profile") or "").lower()
    if "dolby vision" in profile:
        result = "DV"
    else:
        ct = video.get("color_transfer") or ""
        if ct == "smpte2084":
            result = "HDR"
        elif ct == "arib-std-b67":
            result = "HLG"
        else:
            # Fall back to checking primaries/colorspace — some encodes omit the
            # transfer tag but still signal bt2020 wide-gamut content.
            cp = video.get("color_primaries") or ""
            cs = video.get("color_space") or ""
            result = "HDR" if ("bt2020" in cp or "bt2020" in cs) else "SDR"
    return result if result in _VALID_HDR_TYPES else "SDR"


async def run_ffprobe(path: Path) -> dict:
    async with _PROBE_SEM:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries",
            "stream=codec_name,codec_type,width,height,color_transfer,color_primaries,color_space,profile:format=duration,tags",
            "-of", "json", str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            log.warning("ffprobe timed out for %s", path)
            return {"video_codec": "N/A", "audio_codec": "N/A", "width": None, "height": None,
                    "duration_min": 0, "duration_sec": 0.0, "hdr_type": "SDR"}

    try:
        data = json.loads(stdout)
        video = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), {})
        audio = next((s for s in data.get("streams", []) if s.get("codec_type") == "audio"), {})
        fmt = data.get("format", {})
        duration = float(fmt.get("duration") or 0)
        # Extract embedded IMDB tconst from container tags (MKV global tags)
        fmt_tags = fmt.get("tags") or {}
        embedded_tconst = fmt_tags.get("IMDB") or fmt_tags.get("imdb") or None
        return {
            "video_codec": video.get("codec_name", "N/A"),
            "audio_codec": audio.get("codec_name", "N/A"),
            "width": video.get("width"),
            "height": video.get("height"),
            "duration_min": round(duration / 60),
            "duration_sec": duration,
            "hdr_type": _hdr_type_from_stream(video),
            "_embedded_tconst": embedded_tconst,
        }
    except Exception:
        log.warning("ffprobe parse failed for %s", path, exc_info=True)
        return {"video_codec": "N/A", "audio_codec": "N/A", "width": None, "height": None,
                "duration_min": 0, "duration_sec": 0.0, "hdr_type": "SDR"}


def _fmt_duration(secs: float | None) -> str:
    """Format seconds into a human-readable duration string."""
    if not secs:
        return ""
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    h, rem = divmod(secs, 3600)
    m = rem // 60
    if h:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{m}m"


async def get_file_meta(db: aiosqlite.Connection, path: Path) -> dict:
    stat = await asyncio.to_thread(path.stat)
    db.row_factory = aiosqlite.Row
    async with db.execute(
        "SELECT * FROM file_meta WHERE path = ? AND mtime = ?",
        (str(path), stat.st_mtime),
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            return dict(row)

    meta = await run_ffprobe(path)
    embedded_tconst = meta.pop("_embedded_tconst", None)
    # ffprobe doesn't surface iTunes freeform atoms — read MP4 tag via mutagen
    if not embedded_tconst and path.suffix.lower() == ".mp4":
        embedded_tconst = await asyncio.to_thread(_read_mp4_tconst_sync, str(path))
    meta.update({"path": str(path), "size": stat.st_size, "mtime": stat.st_mtime,
                 "scanned_at": time.time()})
    await db.execute(
        """INSERT OR REPLACE INTO file_meta
           (path, size, mtime, video_codec, audio_codec, width, height, duration_min, scanned_at, duration_sec, hdr_type)
           VALUES (:path, :size, :mtime, :video_codec, :audio_codec, :width, :height, :duration_min, :scanned_at, :duration_sec, :hdr_type)""",
        meta,
    )
    # Restore IMDB match from embedded file tag if not already in DB
    if embedded_tconst:
        async with db.execute(
            "SELECT 1 FROM file_imdb WHERE path=?", (str(path),)
        ) as cur:
            if not await cur.fetchone():
                # Look up title/year from imdb.db to populate properly
                imdb_info: dict = {}
                try:
                    async with aiosqlite.connect(IMDB_DB_PATH) as idb:
                        async with idb.execute(
                            "SELECT primary_title, start_year, genres, runtime_minutes"
                            " FROM imdb_titles WHERE tconst=?", (embedded_tconst,)
                        ) as icur:
                            irow = await icur.fetchone()
                            if irow:
                                imdb_info = {
                                    "primary_title": irow[0], "start_year": irow[1],
                                    "genres": irow[2], "runtime_minutes": irow[3],
                                }
                except Exception:
                    pass
                await db.execute(
                    "INSERT OR IGNORE INTO file_imdb"
                    " (path, tconst, primary_title, start_year, genres, runtime_minutes)"
                    " VALUES (?,?,?,?,?,?)",
                    (str(path), embedded_tconst, imdb_info.get("primary_title"),
                     imdb_info.get("start_year"), imdb_info.get("genres"),
                     imdb_info.get("runtime_minutes")),
                )
    await db.commit()
    return meta


def human_size(size_bytes: int) -> str:
    n = float(size_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PiB"


async def dir_total_size(path: Path) -> int:
    """Returns total size in bytes, or -1 on error."""
    key = str(path)
    now = time.monotonic()
    cached = _dir_size_cache.get(key)
    if cached and (now - cached[1]) < _DIR_SIZE_TTL:
        return cached[0]

    try:
        async with _DU_SEM:
            proc = await asyncio.create_subprocess_exec(
                "du", "-sb", str(path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        parts = stdout.decode().split()
        result = int(parts[0]) if parts else -1
    except Exception:
        result = -1

    if result != -1:
        if len(_dir_size_cache) >= 500:
            for k in sorted(_dir_size_cache, key=lambda k: _dir_size_cache[k][1])[:250]:
                del _dir_size_cache[k]
        _dir_size_cache[key] = (result, now)
    return result


async def _probe_file(db: aiosqlite.Connection, f: Path) -> dict | None:
    try:
        meta = await get_file_meta(db, f)
        meta["name"] = f.name
        meta["stem"] = f.stem
        meta["ext"]  = f.suffix.lower()
        meta["human_size"]  = human_size(meta.get("size") or 0)
        meta["size_class"]  = size_css_class(meta.get("size") or 0)
        meta["codec_class"] = codec_css_class(meta.get("video_codec", ""))
        meta["ext_class"]      = ext_css_class(f.suffix)
        meta["res_class"]      = res_css_class(meta.get("width") or 0, meta.get("height") or 0)
        meta["hdr_class"]      = hdr_css_class(meta.get("hdr_type") or "")
        meta["needs_transcode"] = (meta.get("audio_codec") or "").lower() not in BROWSER_SAFE_AUDIO
        meta["duration_label"] = _fmt_duration(meta.get("duration_sec") or (meta.get("duration_min") or 0) * 60)
        return meta
    except Exception:
        log.warning("Failed to probe %s", f, exc_info=True)
        return None


async def scan_dir(dir_path: Path) -> tuple[dict, list[dict]]:
    """Single iterdir() — returns (contents, cached_files).

    No stat() calls, no du — just a directory listing plus a DB lookup.
    Staleness is handled by the background /dir-check endpoint.
    """
    entries = await _list_dir(dir_path)
    if not entries and not dir_path.is_dir():
        return {"subdirs": [], "file_count": 0}, []

    dirs      = sorted([e for e in entries if e.is_dir()],  key=lambda x: x.name.lower())
    mkv_files = sorted(
        [e for e in entries if e.is_file() and os.path.splitext(e.name)[1].lower() in MEDIA_EXTENSIONS],
        key=lambda x: x.name.lower(),
    )

    subdirs = [{"path": Path(d.path), "name": d.name, "size": "?", "size_bytes": -1, "mtime": 0}
               for d in dirs]
    contents = {"subdirs": subdirs, "file_count": len(mkv_files)}

    if not mkv_files:
        return contents, []

    paths = [e.path for e in mkv_files]
    placeholders = ",".join("?" * len(paths))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"SELECT * FROM file_meta WHERE path IN ({placeholders})", paths
        ) as cur:
            rows = await cur.fetchall()

    by_path = {row["path"]: dict(row) for row in rows}
    cached = []
    for f in mkv_files:
        row = by_path.get(f.path)
        if not row:
            continue
        stem, ext = os.path.splitext(f.name)
        row["name"]            = f.name
        row["stem"]            = stem
        row["ext"]             = ext.lower()
        row["human_size"]      = human_size(row["size"])
        row["size_class"]      = size_css_class(row.get("size") or 0)
        row["codec_class"]     = codec_css_class(row.get("video_codec") or "")
        row["ext_class"]       = ext_css_class(ext)
        row["res_class"]       = res_css_class(row.get("width") or 0, row.get("height") or 0)
        row["hdr_class"]       = hdr_css_class(row.get("hdr_type") or "")
        row["needs_transcode"] = (row.get("audio_codec") or "").lower() not in BROWSER_SAFE_AUDIO
        row["duration_label"] = _fmt_duration(row.get("duration_sec") or (row.get("duration_min") or 0) * 60)
        cached.append(row)

    return contents, cached


async def _file_scan_events(dir_path: Path, request: Request):
    """Async generator yielding SSE events as MKV files are probed."""
    entries = await _list_dir(dir_path)
    if not entries and not dir_path.is_dir():
        yield f"data: {json.dumps({'type': 'done', 'html': ''})}\n\n"
        return

    mkv_files = sorted(
        [f for f in entries if f.is_file() and os.path.splitext(f.name)[1].lower() in MEDIA_EXTENSIONS],
        key=lambda x: x.name.lower(),
    )
    total = len(mkv_files)

    if total == 0:
        yield f"data: {json.dumps({'type': 'done', 'html': ''})}\n\n"
        return

    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

    queue: asyncio.Queue = asyncio.Queue()
    done_count = 0
    files_meta: list[dict] = []

    async with aiosqlite.connect(DB_PATH) as db:
        async def probe_one(f) -> None:
            async with _SCAN_SEM:
                try:
                    meta = await _probe_file(db, Path(f.path))
                except Exception:
                    meta = None
            await queue.put(meta)

        tasks = [asyncio.create_task(probe_one(f)) for f in mkv_files]

        for _ in range(total):
            if await request.is_disconnected():
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                return
            meta = await queue.get()
            done_count += 1
            file_html = ""
            if meta:
                files_meta.append(meta)
                file_html = templates.env.get_template("_files_table.html").render(files=[meta])
            yield f"data: {json.dumps({'type': 'progress', 'done': done_count, 'total': total, 'file_html': file_html})}\n\n"

        await asyncio.gather(*tasks, return_exceptions=True)

    files_meta.sort(key=lambda x: x["name"].lower())
    html = templates.env.get_template("_files_table.html").render(files=files_meta)
    yield f"data: {json.dumps({'type': 'done', 'html': html})}\n\n"


def safe_path(path: str) -> Path:
    # Snapshot globals to avoid TOCTOU race with concurrent /set-root
    root = current_root
    rroot = _real_root
    # normpath for fast lexical cleanup, realpath to catch symlink escapes
    p = Path(os.path.normpath(path))
    try:
        p.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")
    real = Path(os.path.realpath(p))
    try:
        real.relative_to(rroot)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")
    return p


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "media_root": str(current_root),
        "configured_dirs": CONFIGURED_DIRS,
        "delete_token": DELETE_TOKEN,
        "error": request.query_params.get("error"),
        "ingress_path": request.state.ingress_path,
    })


@app.post("/set-root")
async def set_root(request: Request, path: str = Form(...)):
    global current_root, _real_root
    base = request.state.ingress_path
    p = Path(path).expanduser().resolve()
    if ALLOWED_ROOTS and not any(p == allowed or p.is_relative_to(allowed) for allowed in ALLOWED_ROOTS):
        return RedirectResponse(f"{base}/?error={quote(path)}+not+permitted", status_code=303)
    if not p.is_dir():
        return RedirectResponse(f"{base}/?error={quote(path)}+not+found", status_code=303)
    current_root = p
    _real_root = Path(os.path.realpath(p))
    await save_root(p)
    return RedirectResponse(f"{base}/", status_code=303)


@app.get("/dir", response_class=HTMLResponse)
async def expand_dir(request: Request, path: str = Query(...)):
    dir_path = safe_path(path)
    if not dir_path.is_dir():
        raise HTTPException(status_code=404)
    contents, cached_files = await scan_dir(dir_path)
    return templates.TemplateResponse("dir_contents.html", {
        "request": request,
        "contents": contents,
        "dir_path": str(dir_path),
        "cached_files": cached_files,
    })


@app.get("/disk-usage")
async def disk_usage_endpoint(path: str = Query(...)):
    """Return filesystem disk usage for the volume containing `path`."""
    dir_path = safe_path(path)
    if not dir_path.exists():
        raise HTTPException(status_code=404)
    try:
        st = await asyncio.to_thread(shutil.disk_usage, str(dir_path))
        return {
            "path":       str(dir_path),
            "name":       dir_path.name or str(dir_path),
            "total":      st.total,
            "used":       st.used,
            "free":       st.free,
            "used_pct":   round(st.used / st.total * 100, 1) if st.total else 0,
            "free_pct":   round(st.free / st.total * 100, 1) if st.total else 0,
            "total_human": human_size(st.total),
            "used_human":  human_size(st.used),
            "free_human":  human_size(st.free),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dir-size")
async def dir_size(path: str = Query(...)):
    """Returns the total size of a directory (runs du). Used for lazy size loading."""
    dir_path = safe_path(path)
    if not dir_path.is_dir():
        raise HTTPException(status_code=404)
    size = await dir_total_size(dir_path)
    return {"size": size, "human": human_size(size) if size >= 0 else "?"}


@app.get("/dir-check")
async def dir_check(path: str = Query(...)):
    """Lightweight FS vs DB diff — no ffprobe. Returns {changed: bool}."""
    dir_path = safe_path(path)
    if not dir_path.is_dir():
        raise HTTPException(status_code=404)

    entries = await _list_dir(dir_path)
    fs_paths = {e.path for e in entries if e.is_file() and os.path.splitext(e.name)[1].lower() in MEDIA_EXTENSIONS}

    dir_prefix = str(dir_path) + "/"
    escaped = dir_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT path FROM file_meta WHERE path LIKE ? ESCAPE '\\'",
            (escaped + "%",),
        ) as cur:
            rows = await cur.fetchall()
    db_paths = {row[0] for row in rows if "/" not in row[0][len(dir_prefix):]}

    return {"changed": fs_paths != db_paths}


@app.get("/dir-scan")
async def dir_scan(request: Request, path: str = Query(...)):
    dir_path = safe_path(path)
    if not dir_path.is_dir():
        raise HTTPException(status_code=404)
    return StreamingResponse(
        _file_scan_events(dir_path, request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/stream")
async def stream_file(path: str = Query(...)):
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(file_path, media_type="video/x-matroska")


_HLS_SEG_SECS = 10


async def _probe_duration(path: Path) -> float:
    key = str(path)
    if key in _duration_cache:
        return _duration_cache[key]
    async with _PROBE_SEM:
        if key in _duration_cache:
            return _duration_cache[key]
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
            "-of", "csv=p=0", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return 0.0
    try:
        duration = float(stdout.strip())
    except (ValueError, AttributeError):
        duration = 0.0
    if duration > 0:
        _duration_cache[key] = duration
    return duration


@app.get("/hls/playlist.m3u8")
async def hls_playlist(
    path:   str = Query(...),
    vcodec: str = Query(default=""),
    height: int = Query(default=0),
    token:  str = Query(default=""),  # per-play cache-buster passed through to segments
):
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404)

    duration = await _probe_duration(file_path)
    if duration <= 0:
        raise HTTPException(status_code=503, detail="Could not determine file duration")
    num_segs = math.ceil(duration / _HLS_SEG_SECS)
    encoded  = quote(path, safe="")
    vc       = quote(vcodec, safe="")

    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{_HLS_SEG_SECS}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-INDEPENDENT-SEGMENTS",
    ]
    for i in range(num_segs):
        seg_dur = min(_HLS_SEG_SECS, duration - i * _HLS_SEG_SECS)
        lines += [
            f"#EXTINF:{seg_dur:.3f},",
            f"/hls/segment?path={encoded}&seq={i}&vcodec={vc}&height={height}&token={token}",
        ]
    lines.append("#EXT-X-ENDLIST")

    return Response("\n".join(lines), media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-cache, no-store"})


@app.get("/hls/segment")
async def hls_segment(
    request: Request,
    path:    str = Query(...),
    seq:     int = Query(...),
    vcodec:  str = Query(default=""),
    height:  int = Query(default=0),
    token:   str = Query(default=""),  # cache-buster, ignored server-side
):
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404)

    start = seq * _HLS_SEG_SECS

    # Always transcode to H.264 — copy mode fails on SMB mounts because the
    # keyframe snap-back produces wrong content (seek discards frames only when
    # decoding; copy mode has no discard phase and outputs from the wrong keyframe).
    video_args = ["-c:v", "libx264", "-preset", "veryfast", "-crf", "22"]
    if height > 1080:
        video_args += ["-vf", "scale=-2:1080"]

    cmd = [
        "ffmpeg",
        "-ss", str(start), "-i", str(file_path),
        "-t", str(_HLS_SEG_SECS),
        *video_args,
        "-c:a", "aac", "-ac", "2", "-b:a", "192k",
        "-sn",
        "-output_ts_offset", str(start),
        "-f", "mpegts", "pipe:1",
    ]

    async def generate():
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            while True:
                if await request.is_disconnected():
                    break
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()

    return StreamingResponse(generate(), media_type="video/mp2t",
                             headers={"Cache-Control": "no-store"})


@app.post("/rename")
async def rename_file(request: Request, path: str = Query(...), new_name: str = Query(...)):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Missing or invalid delete token")
    # Reject any path separators or reserved characters in the new name
    if "/" in new_name or "\\" in new_name or new_name != new_name.strip() or not new_name:
        raise HTTPException(status_code=400, detail="Invalid filename")
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404)
    new_path = file_path.parent / new_name
    if new_path.exists():
        raise HTTPException(status_code=409, detail="A file with that name already exists")
    file_path.rename(new_path)
    async with aiosqlite.connect(DB_PATH) as db:
        # Remove any stale row for the target path before updating
        await db.execute("DELETE FROM file_meta WHERE path = ?", (str(new_path),))
        await db.execute(
            "UPDATE file_meta SET path = ? WHERE path = ?",
            (str(new_path), str(file_path)),
        )
        await db.commit()
    _dir_listing_cache.pop(str(file_path.parent), None)
    return {"path": str(new_path), "name": new_path.name}


@app.post("/move-to-folder")
async def move_to_folder(request: Request):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Missing or invalid delete token")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    paths: list = body.get("paths", [])
    folder_name: str = body.get("folder", "").strip()
    if not paths or not folder_name:
        raise HTTPException(status_code=400, detail="paths and folder are required")
    if "/" in folder_name or "\\" in folder_name or not folder_name:
        raise HTTPException(status_code=400, detail="Invalid folder name")

    moved, errors = [], []
    target_dir: Path | None = None
    for raw_path in paths:
        try:
            src = safe_path(raw_path)
            if not src.is_file():
                errors.append({"path": raw_path, "error": "not a file"})
                continue
            dest_dir = src.parent / folder_name
            dest_dir.mkdir(exist_ok=True)
            if target_dir is None:
                target_dir = dest_dir
            dest = dest_dir / src.name
            if dest.exists():
                errors.append({"path": raw_path, "error": "destination exists"})
                continue
            src.rename(dest)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM file_meta WHERE path = ?", (str(dest),))
                await db.execute(
                    "UPDATE file_meta SET path = ? WHERE path = ?",
                    (str(dest), str(src)),
                )
                await db.commit()
            _dir_listing_cache.pop(str(src.parent), None)
            moved.append(str(dest))
        except Exception as exc:
            errors.append({"path": raw_path, "error": str(exc)})

    return {"moved": moved, "errors": errors, "folder": str(target_dir) if target_dir else None}


@app.get("/file-info")
async def file_info(path: str = Query(...)):
    """Return detailed ffprobe info for a single file."""
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404)
    async with _PROBE_SEM:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_streams", "-show_format",
            "-print_format", "json",
            str(file_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise HTTPException(status_code=504, detail="ffprobe timed out")
    try:
        data = json.loads(stdout)
    except Exception:
        raise HTTPException(status_code=500, detail="ffprobe parse error")

    fmt = data.get("format", {})
    streams = data.get("streams", [])

    def _tag(s, *keys):
        tags = s.get("tags") or {}
        for k in keys:
            v = tags.get(k) or tags.get(k.upper())
            if v:
                return v
        return None

    st = await asyncio.to_thread(file_path.stat)
    result = {
        "path":     str(file_path),
        "name":     file_path.name,
        "size":     st.st_size,
        "format":   fmt.get("format_long_name") or fmt.get("format_name", ""),
        "duration": float(fmt.get("duration") or 0),
        "bitrate":  int(fmt.get("bit_rate") or 0),
        "video": [],
        "audio": [],
        "subtitle": [],
    }
    for s in streams:
        stype = s.get("codec_type")
        lang  = _tag(s, "language") or ""
        title = _tag(s, "title") or ""
        if stype == "video":
            fr_num, fr_den = (s.get("r_frame_rate") or "0/1").split("/")
            fps = round(int(fr_num) / max(int(fr_den), 1), 3)
            result["video"].append({
                "codec":     s.get("codec_name", ""),
                "profile":   s.get("profile", ""),
                "width":     s.get("width"),
                "height":    s.get("height"),
                "pix_fmt":   s.get("pix_fmt", ""),
                "fps":       fps,
                "bitrate":   int(s.get("bit_rate") or 0),
                "hdr":       "bt2020" in (s.get("color_space") or "") or "smpte2084" in (s.get("color_transfer") or ""),
                "color_space": s.get("color_space", ""),
                "lang":      lang,
                "title":     title,
            })
        elif stype == "audio":
            result["audio"].append({
                "codec":      s.get("codec_name", ""),
                "channels":   s.get("channels", 0),
                "channel_layout": s.get("channel_layout", ""),
                "sample_rate": int(s.get("sample_rate") or 0),
                "bitrate":    int(s.get("bit_rate") or 0),
                "lang":       lang,
                "title":      title,
            })
        elif stype == "subtitle":
            result["subtitle"].append({
                "codec": s.get("codec_name", ""),
                "lang":  lang,
                "title": title,
            })
    return result


@app.delete("/file")
async def delete_file(request: Request, path: str = Query(...)):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Missing or invalid delete token")
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404)
    file_path.unlink()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM file_meta WHERE path = ?", (str(file_path),))
        await db.commit()
    _dir_size_cache.pop(str(file_path.parent), None)
    _dir_listing_cache.pop(str(file_path.parent), None)
    return Response(status_code=204)


@app.post("/rescan")
async def rescan(request: Request):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    _dir_size_cache.clear()
    _dir_listing_cache.clear()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM file_meta")
        await db.commit()
    return Response(status_code=204)


# ── Video encode job system ─────────────────────────────────────────────────


class EncodeJob:
    __slots__ = (
        "id", "input_path", "output_path", "input_name", "output_name",
        "config", "status", "progress", "current_fps", "avg_fps",
        "eta", "encoder", "input_size", "output_size",
        "started_at", "finished_at", "error", "created_at",
        "input_media_info", "moved",
        "_proc", "_task",
    )

    def __init__(self, job_id: str, input_path: str, output_path: str, config: dict,
                 created_at: float | None = None):
        self.id = job_id
        self.input_path = input_path
        self.output_path = output_path
        self.input_name = os.path.basename(input_path)
        self.output_name = os.path.basename(output_path)
        self.config = config
        self.status = "queued"
        self.progress = 0.0
        self.current_fps = 0.0
        self.avg_fps = 0.0
        self.eta = "--"
        self.encoder = ""
        self.input_size = 0
        self.output_size = 0
        self.started_at = 0.0
        self.finished_at: float | None = None
        self.error: str | None = None
        self.created_at: float = created_at if created_at is not None else time.time()
        self.input_media_info: dict = {}
        self.moved: bool = False
        self._proc = None
        self._task = None

    def to_dict(self) -> dict:
        return {s: getattr(self, s) for s in self.__slots__ if not s.startswith("_")}


_hw_accel_info: dict = {}
_encode_jobs: dict[str, EncodeJob] = {}
_encode_subscribers: list[asyncio.Queue] = []
_encode_queue_list: list[str] = []
_encode_queue_event: asyncio.Event = asyncio.Event()
_schedule_config: dict = {"enabled": False, "start": 22, "end": 6}


def _schedule_active() -> bool:
    """Returns True if encoding is allowed right now (or no schedule is set)."""
    if not _schedule_config.get("enabled"):
        return True
    start = int(_schedule_config.get("start", 0))
    end   = int(_schedule_config.get("end",   24))
    now   = time.localtime().tm_hour
    if start == end:
        return True
    if start < end:
        return start <= now < end
    return now >= start or now < end


def _enqueue_job(job_id: str) -> None:
    _encode_queue_list.append(job_id)
    _encode_queue_event.set()
    _broadcast_queue_order()


def _broadcast_queue_order() -> None:
    msg = json.dumps({"type": "queue_order", "order": list(_encode_queue_list)})
    dead = []
    for q in _encode_subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            _encode_subscribers.remove(q)
        except ValueError:
            pass


def _job_dict(job: EncodeJob) -> dict:
    d = job.to_dict()
    try:
        d["queue_pos"] = _encode_queue_list.index(job.id)
    except ValueError:
        d["queue_pos"] = -1
    return d


async def _save_encode_job(job: EncodeJob) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO encode_jobs
               (id, input_path, output_path, config, status, encoder,
                input_size, output_size, started_at, finished_at, error, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (job.id, job.input_path, job.output_path,
             json.dumps(job.config), job.status, job.encoder,
             job.input_size, job.output_size,
             job.started_at, job.finished_at, job.error, job.created_at),
        )
        await db.commit()


async def _delete_encode_job_db(job_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM encode_jobs WHERE id = ?", (job_id,))
        await db.commit()


async def _load_encode_jobs() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM encode_jobs ORDER BY created_at"
        ) as cur:
            rows = await cur.fetchall()

    for row in rows:
        job = EncodeJob(
            row["id"], row["input_path"], row["output_path"],
            json.loads(row["config"]), created_at=row["created_at"],
        )
        job.status      = row["status"]
        job.encoder     = row["encoder"] or ""
        job.input_size  = row["input_size"] or 0
        job.output_size = row["output_size"] or 0
        job.started_at  = row["started_at"] or 0.0
        job.finished_at = row["finished_at"]
        job.error       = row["error"]
        _encode_jobs[job.id] = job

    # Re-queue anything that was in-flight when the server last stopped
    for job in list(_encode_jobs.values()):
        if job.status in ("queued", "running"):
            job.status     = "queued"
            job.progress   = 0.0
            job.started_at = 0.0
            job.finished_at = None
            job.error      = None
            _encode_queue_list.append(job.id)
    if _encode_queue_list:
        _encode_queue_event.set()

    if rows:
        log.info("Restored %d encode job(s) from DB", len(rows))


def _detect_hw_accel_sync() -> dict:
    import glob as _glob
    result = {"qsv": False, "nvenc": False, "nvenc_cuvid": False, "amd": False, "vaapi": False, "av1_hw": False, "dri_device": ""}
    if shutil.which("nvidia-smi"):
        try:
            r = subprocess.run(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                capture_output=True, text=True, timeout=5,
            )
            result["nvenc"] = r.returncode == 0 and bool(r.stdout.strip())
        except Exception:
            pass
    if result["nvenc"] and shutil.which("ffmpeg"):
        try:
            r = subprocess.run(
                ["ffmpeg", "-decoders"], capture_output=True, text=True, timeout=5,
            )
            result["nvenc_cuvid"] = "hevc_cuvid" in r.stdout
        except Exception:
            pass
    for dev in sorted(_glob.glob("/dev/dri/renderD*")):
        vfiles = _glob.glob(f"/sys/class/drm/{os.path.basename(dev)}/device/vendor")
        if not vfiles:
            continue
        try:
            with open(vfiles[0]) as f:
                vid = f.read().strip()
            if vid == "0x8086":
                _libva_env = {**os.environ, "LIBVA_DRIVER_NAME": "iHD"}
                if shutil.which("ffmpeg"):
                    # Test QSV first (Gen12+ oneVPL path)
                    try:
                        r = subprocess.run(
                            ["ffmpeg", "-v", "error",
                             "-init_hw_device", f"vaapi=va:{dev}",
                             "-init_hw_device", "qsv=hw@va",
                             "-filter_hw_device", "hw",
                             "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=1",
                             "-vframes", "1", "-c:v", "hevc_qsv", "-f", "null", "-"],
                            capture_output=True, timeout=15,
                            env=_libva_env,
                        )
                        if r.returncode == 0:
                            result["qsv"]   = True
                            result["vaapi"] = True
                            result["dri_device"] = dev
                            log.info("QSV probe    : hevc_qsv functional (%s)", dev)
                            # Probe AV1 QSV — only available on Arc / Meteor Lake+;
                            # Alder/Raptor Lake iGPUs support AV1 decode but not encode.
                            try:
                                r2 = subprocess.run(
                                    ["ffmpeg", "-v", "error",
                                     "-init_hw_device", f"vaapi=va:{dev}",
                                     "-init_hw_device", "qsv=hw@va",
                                     "-filter_hw_device", "hw",
                                     "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=24",
                                     "-vframes", "1",
                                     "-vf", "format=nv12,hwupload=extra_hw_frames=64",
                                     "-c:v", "av1_qsv", "-f", "null", "-"],
                                    capture_output=True, timeout=15,
                                    env=_libva_env,
                                )
                                result["av1_hw"] = r2.returncode == 0
                                log.info("AV1 QSV probe: %s", "supported" if result["av1_hw"] else "not supported (hw encode unavailable — will use libsvtav1)")
                            except Exception as e:
                                log.warning("AV1 QSV probe: failed (%s)", e)
                        else:
                            err = (r.stderr or b"").decode(errors="replace").strip().splitlines()
                            log.warning("QSV probe    : hevc_qsv unavailable — testing hevc_vaapi")
                            for line in err[-3:]:
                                log.warning("QSV probe    : %s", line)
                    except Exception as e:
                        log.warning("QSV probe    : test failed (%s) — testing hevc_vaapi", e)
                    # If QSV didn't work, test plain VAAPI before declaring it available
                    if not result.get("qsv"):
                        try:
                            r = subprocess.run(
                                ["ffmpeg", "-v", "error",
                                 "-vaapi_device", dev,
                                 "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=1",
                                 "-vframes", "1",
                                 "-vf", "format=nv12,hwupload",
                                 "-c:v", "hevc_vaapi", "-f", "null", "-"],
                                capture_output=True, timeout=15,
                                env=_libva_env,
                            )
                            if r.returncode == 0:
                                result["vaapi"] = True
                                result["dri_device"] = dev
                                log.info("VAAPI probe  : hevc_vaapi functional (%s)", dev)
                                # Probe av1_vaapi support
                                try:
                                    r3 = subprocess.run(
                                        ["ffmpeg", "-v", "error",
                                         "-vaapi_device", dev,
                                         "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=24",
                                         "-vframes", "1",
                                         "-vf", "format=nv12,hwupload",
                                         "-c:v", "av1_vaapi", "-f", "null", "-"],
                                        capture_output=True, timeout=15,
                                        env=_libva_env,
                                    )
                                    result["av1_hw"] = r3.returncode == 0
                                    log.info("AV1 VAAPI probe: %s", "supported" if result["av1_hw"] else "not supported — will use libsvtav1")
                                except Exception as e:
                                    log.warning("AV1 VAAPI probe: failed (%s)", e)
                            else:
                                err = (r.stderr or b"").decode(errors="replace").strip().splitlines()
                                log.warning("VAAPI probe  : hevc_vaapi unavailable — software encoding only")
                                for line in err[-3:]:
                                    log.warning("VAAPI probe  : %s", line)
                        except Exception as e:
                            log.warning("VAAPI probe  : test failed (%s) — software encoding only", e)
            elif vid == "0x1002":
                result["amd"]   = True
                if shutil.which("ffmpeg"):
                    try:
                        r = subprocess.run(
                            ["ffmpeg", "-v", "error",
                             "-vaapi_device", dev,
                             "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=1",
                             "-vframes", "1",
                             "-vf", "format=nv12,hwupload",
                             "-c:v", "hevc_vaapi", "-f", "null", "-"],
                            capture_output=True, timeout=15,
                            env={**os.environ},
                        )
                        if r.returncode == 0:
                            result["vaapi"] = True
                            result["dri_device"] = dev
                            log.info("VAAPI probe  : hevc_vaapi functional (%s)", dev)
                        else:
                            log.warning("VAAPI probe  : AMD hevc_vaapi unavailable — software encoding only")
                    except Exception as e:
                        log.warning("VAAPI probe  : AMD test failed (%s)", e)
        except Exception:
            pass
    return result


def _notify_encode(job_id: str) -> None:
    job = _encode_jobs.get(job_id)
    if not job:
        return
    msg = json.dumps({"type": "update", "job": _job_dict(job)})
    dead = []
    for q in _encode_subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            _encode_subscribers.remove(q)
        except ValueError:
            pass


async def _encode_worker() -> None:
    """Serial encode queue — runs one job at a time."""
    while True:
        # Wait until there's work to do
        if not _encode_queue_list:
            _encode_queue_event.clear()
            if not _encode_queue_list:   # re-check after clear to avoid race
                await _encode_queue_event.wait()
            continue
        # Respect encode schedule
        if not _schedule_active():
            await asyncio.sleep(60)
            continue
        job_id = _encode_queue_list[0]
        job = _encode_jobs.get(job_id)
        if not job or job.status != "queued":
            _encode_queue_list.pop(0)
            _broadcast_queue_order()
            continue
        _encode_queue_list.pop(0)
        _broadcast_queue_order()
        try:
            await _run_encode_job(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Encode worker error for job %s: %s", job_id, exc, exc_info=True)


def _choose_encoder_name(hw: dict, gpu_pref: str, codec: str = "hevc") -> str:
    """Return ffmpeg encoder name based on available hardware, user preference, and codec."""
    _SW    = {"hevc": "libx265",    "h264": "libx264",    "av1": "libsvtav1"}
    _QSV   = {"hevc": "hevc_qsv",  "h264": "h264_qsv",  "av1": "av1_qsv"}
    _NVENC = {"hevc": "hevc_nvenc","h264": "h264_nvenc", "av1": "av1_nvenc"}
    _VAAPI = {"hevc": "hevc_vaapi","h264": "h264_vaapi", "av1": "av1_vaapi"}
    sw = _SW.get(codec, "libx265")

    def _with_av1_guard(enc_map: dict, fallback: str) -> str:
        # AV1 hardware encode requires explicit runtime support (not all GPUs have it).
        # Fall back to libsvtav1 if the startup probe found it unsupported.
        if codec == "av1" and not hw.get("av1_hw"):
            return sw
        return enc_map.get(codec, fallback)

    if gpu_pref == "none":   return sw
    if gpu_pref == "intel":
        if hw.get("qsv"):   return _with_av1_guard(_QSV,   "hevc_qsv")
        if hw.get("vaapi"): return _with_av1_guard(_VAAPI, "hevc_vaapi")
        return sw
    if gpu_pref == "nvidia": return _with_av1_guard(_NVENC, "hevc_nvenc") if hw.get("nvenc") else sw
    if gpu_pref == "amd":    return _with_av1_guard(_VAAPI, "hevc_vaapi") if hw.get("amd")   else sw
    # Auto: prefer QSV (Intel) > NVENC > VAAPI (Intel/AMD) > software
    if hw.get("qsv"):    return _with_av1_guard(_QSV,   "hevc_qsv")
    if hw.get("nvenc"):  return _with_av1_guard(_NVENC, "hevc_nvenc")
    if hw.get("vaapi"):  return _with_av1_guard(_VAAPI, "hevc_vaapi")
    return sw


_HQDN3D_PRESETS = {
    "ultralight": "1:1:3:3",
    "light":      "2:2:5:5",
    "medium":     "4:3:6:4.5",
    "strong":     "6:5:10:7",
    "stronger":   "8:7:12:9",
    "verystrong": "10:9:15:11",
}


def _build_ffmpeg_cmd(
    input_path: str, output_path: str, config: dict,
    hw: dict, bit_depth: Optional[int] = None, is_hdr: bool = False,
    color_primaries: str = "", transfer_characteristics: str = "",
    color_space: str = "", color_range: str = "",
    crop_filter: Optional[str] = None, a_streams: Optional[list] = None,
    source_fps: Optional[float] = None,
) -> tuple[list[str], str]:
    gpu_pref = config.get("gpu", "auto")
    codec    = config.get("codec", "hevc")
    encoder  = _choose_encoder_name(hw, gpu_pref, codec)
    is_qsv   = encoder.endswith("_qsv")
    is_vaapi = encoder.endswith("_vaapi")
    is_nvenc = encoder.endswith("_nvenc")
    is_av1   = "av1" in encoder
    is_10bit = is_hdr or bool(bit_depth and bit_depth >= 10)
    qp       = config.get("qp", 18)
    denoise  = config.get("denoise")
    width    = config.get("width")

    _PRESET_MAP = {"speed": "veryfast", "fast": "fast", "balanced": "medium",
                   "quality": "slow", "archive": "veryslow"}
    _NVENC_PRESET_MAP = {"speed": "p1", "fast": "p2", "balanced": "p3",
                         "quality": "p4", "archive": "p6"}
    preset      = _PRESET_MAP.get(config.get("preset", "quality"), "slow")
    nvenc_preset = _NVENC_PRESET_MAP.get(config.get("preset", "fast"), "p2")

    cmd = ["ffmpeg", "-y",
           "-analyzeduration", "100M", "-probesize", "100M"]

    dri_dev = hw.get("dri_device") or "/dev/dri/renderD128"
    if is_qsv:
        cmd += ["-init_hw_device", f"vaapi=va:{dri_dev}",
                "-init_hw_device", "qsv=hw@va",
                "-filter_hw_device", "hw"]
    elif is_vaapi:
        cmd += ["-vaapi_device", dri_dev]
    elif is_nvenc and hw.get("nvenc_cuvid") and codec != "h264":
        # H264 NVENC can't consume CUDA frames — skip cuvid for H264 entirely
        cmd += ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]

    cmd += ["-i", input_path]

    # Build color metadata flags from actual source values; nothing is hardcoded.
    # For HDR sources missing primaries/trc we fall back to the bt2020/smpte2084
    # defaults that cover the vast majority of HDR10 content.
    cp = color_primaries or ("bt2020"    if is_hdr else "")
    tc = transfer_characteristics or ("smpte2084" if is_hdr else "")
    cs = color_space   # e.g. "bt2020nc", "bt709", ""
    cr = color_range   # e.g. "tv", "pc", ""

    # Flat list of -flag value pairs for all non-empty color metadata fields
    color_meta: list[str] = []
    if cp: color_meta += ["-color_primaries", cp]
    if tc: color_meta += ["-color_trc",        tc]
    if cs: color_meta += ["-colorspace",        cs]
    if cr: color_meta += ["-color_range",       cr]

    denoise_params = _HQDN3D_PRESETS.get(denoise, "") if denoise else ""

    if is_qsv:
        pix_fmt = "p010le" if is_10bit else "nv12"
        vf = []
        if crop_filter:
            vf.append(f"crop={crop_filter}")
        if denoise_params:
            vf.append(f"hqdn3d={denoise_params}")
        if is_av1 and source_fps:
            # QSV and VAAPI AV1 runtimes reject fractional framerates (e.g. 23.976).
            # Normalise to the nearest integer fps before hwupload.
            vf.append(f"fps={round(source_fps)}")
        vf.append(f"format={pix_fmt}")
        if width:
            vf.append(f"scale={int(width)}:-2")
        vf.append("hwupload=extra_hw_frames=64")
        cmd += ["-vf", ",".join(vf)]
        cmd += ["-c:v", encoder, "-global_quality", str(qp), "-preset", preset]
        if is_10bit and not is_av1:
            cmd += ["-profile:v", "main10"]
        cmd += color_meta
    elif is_vaapi:
        pix_fmt = "p010" if is_10bit else "nv12"
        vf = []
        if crop_filter:
            vf.append(f"crop={crop_filter}")
        if denoise_params:
            vf.append(f"hqdn3d={denoise_params}")
        vf.append(f"format={pix_fmt}")
        if width:
            vf.append(f"scale={int(width)}:-2")
        vf.append("hwupload")
        cmd += ["-vf", ",".join(vf)]
        cmd += ["-c:v", encoder, "-qp", str(qp)]
        if is_10bit and not is_av1:
            cmd += ["-profile:v", "main10"]
        cmd += color_meta
    elif is_nvenc:
        vf = []
        if codec == "h264":
            # H264 NVENC requires 8-bit nv12 — always convert regardless of source
            vf.append("format=nv12")
        if crop_filter:
            vf.append(f"crop={crop_filter}")
        if denoise_params:
            vf.append(f"hqdn3d={denoise_params}")
        if width:
            vf.append(f"scale={int(width)}:-2")
        if vf and hw.get("nvenc_cuvid") and codec != "h264":
            # CPU filters can't operate on CUDA frames — wrap with download/upload.
            # H264 uses CPU decode so its frames are already in system memory.
            pix_fmt = "p010le" if is_10bit else "nv12"
            vf = [f"hwdownload,format={pix_fmt}"] + vf + ["hwupload"]
        if vf:
            cmd += ["-vf", ",".join(vf)]
        cmd += ["-c:v", encoder, "-rc:v", "constqp", "-qp:v", str(qp), "-preset", nvenc_preset]
        if is_10bit and not is_av1 and codec != "h264":
            cmd += ["-profile:v", "main10"]
        cmd += color_meta
    else:  # software: libx265, libx264, libsvtav1
        vf = []
        if crop_filter:
            vf.append(f"crop={crop_filter}")
        if denoise_params:
            vf.append(f"hqdn3d={denoise_params}")
        if width:
            vf.append(f"scale={int(width)}:-2")
        if vf:
            cmd += ["-vf", ",".join(vf)]
        if encoder == "libsvtav1":
            cmd += ["-c:v", encoder, "-crf", str(qp), "-preset", "6"]  # SVT-AV1 preset 0-13
        else:
            cmd += ["-c:v", encoder, "-crf", str(qp), "-preset", preset]
        if encoder == "libx265" and is_hdr and cp and tc:
            # Embed HDR10 metadata in the x265 bitstream; use source colormatrix
            # (cs) when available, otherwise default to bt2020nc.
            colormatrix = cs if cs else "bt2020nc"
            cmd += ["-x265-params",
                    f"hdr-opt=1:repeat-headers=1"
                    f":colorprim={cp}:transfer={tc}:colormatrix={colormatrix}"]
        # Also set container-level color metadata for all paths
        cmd += color_meta

    # Copy all audio/subtitle streams; -ignore_unknown drops streams the
    # container doesn't support (e.g. PGS subs in MP4) rather than failing.
    cmd += ["-c:a", "copy", "-c:s", "copy"]
    lang = config.get("lang")
    if lang and a_streams:
        # Map only video, audio matching requested language (or untagged), and subtitles
        stream_tags = [(s.get("tags") or {}) for s in a_streams]
        matched_audio = [
            idx for idx, tags in enumerate(stream_tags)
            if not (tags.get("language") or tags.get("LANGUAGE") or "")
            or (tags.get("language") or tags.get("LANGUAGE") or "").lower() in (lang.lower(), "und")
        ]
        if matched_audio:
            cmd += ["-map", "0:v", "-map", "0:s?"]
            for idx in matched_audio:
                cmd += ["-map", f"0:a:{idx}"]
            cmd += ["-ignore_unknown"]
        else:
            cmd += ["-map", "0", "-ignore_unknown"]
    else:
        cmd += ["-map", "0", "-ignore_unknown"]
    # Structured progress to stdout; suppress the normal stats line on stderr
    cmd += ["-progress", "pipe:1", "-nostats"]
    cmd += [output_path]

    return cmd, encoder


_LANG_RE = re.compile(r'^[a-z]{2,8}$')

def _validated_lang(value: object) -> str:
    s = str(value).strip().lower()
    return s if _LANG_RE.match(s) else "eng"


async def _detect_crop(path: Path, duration: Optional[float]) -> Optional[str]:
    """Run cropdetect on a 2-minute sample. Returns 'W:H:X:Y' string or None."""
    sample_start = int((duration or 0) * 0.1) if duration and duration > 120 else 60
    cmd = [
        "ffmpeg", "-ss", str(sample_start), "-i", str(path),
        "-t", "120", "-vf", "cropdetect=limit=24:round=2:reset=0",
        "-f", "null", "-",
    ]
    try:
        async with _PROBE_SEM:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=150)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return None
        crops = [l for l in stderr.decode(errors="replace").splitlines()
                 if "crop=" in l and "Parsed_cropdetect" in l]
        if not crops:
            return None
        m = re.search(r'crop=(\d+:\d+:\d+:\d+)', crops[-1])
        return m.group(1) if m else None
    except Exception:
        return None


async def _run_encode_job(job_id: str) -> None:
    job = _encode_jobs.get(job_id)
    if not job or job.status == "cancelled":
        return
    try:
        job.status = "running"
        job.started_at = time.time()
        _notify_encode(job_id)
        await _save_encode_job(job)

        input_path = Path(job.input_path)

        # Probe all streams + format: HDR/bit-depth/DV detection, duration for progress
        bit_depth: Optional[int] = None
        is_hdr = False
        is_dv  = False
        duration_sec: Optional[float] = None
        source_fps: Optional[float] = None
        cp = tc = cs = cr = ""
        try:
            async with _PROBE_SEM:
                p = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "error",
                    "-analyzeduration", "100M", "-probesize", "100M",
                    "-show_streams", "-show_format",
                    "-of", "json", str(input_path),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await asyncio.wait_for(p.communicate(), timeout=30)
            probe = json.loads(stdout)
            streams = probe.get("streams", [])
            v_streams = [s for s in streams if s.get("codec_type") == "video"]
            a_streams = [s for s in streams if s.get("codec_type") == "audio"]
            s_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
            vst = v_streams[0] if v_streams else {}
            ast = a_streams[0] if a_streams else {}
            bps = vst.get("bits_per_raw_sample")
            if bps:
                bit_depth = int(bps)
            elif "pix_fmt" in vst:
                m = re.search(r"(\d+)(?:le|be)$", vst["pix_fmt"])
                if m:
                    bit_depth = int(m.group(1))
            cp = vst.get("color_primaries", "")
            tc = vst.get("transfer_characteristics", "")
            cs = vst.get("color_space", "")
            cr = vst.get("color_range", "")
            is_hdr = cp == "bt2020" or tc in ("smpte2084", "arib-std-b67")
            # Dolby Vision: detected via side_data RPU entry
            for sd in vst.get("side_data_list", []):
                if "dovi" in sd.get("side_data_type", "").lower():
                    is_dv = True
                    break
            if is_dv:
                log.warning("Encode %s: Dolby Vision detected — DV RPU metadata cannot be "
                            "preserved through re-encoding; output will be HDR10/HLG", job_id[:8])
            try:
                duration_sec = float(probe.get("format", {}).get("duration") or 0) or None
            except (TypeError, ValueError):
                pass
            try:
                _fn, _fd = (vst.get("r_frame_rate") or "0/1").split("/")
                source_fps = round(int(_fn) / max(int(_fd), 1), 3) or None
            except (ValueError, ZeroDivisionError):
                pass
            job.input_media_info = {
                "video_codec": vst.get("codec_name", ""),
                "width":       vst.get("width", 0),
                "height":      vst.get("height", 0),
                "audio_codec": ast.get("codec_name", ""),
                "audio_count": len(a_streams),
                "sub_count":   len(s_streams),
            }
            _notify_encode(job_id)
        except Exception as e:
            log.warning("Encode %s: could not get stream info: %s", job_id[:8], e)

        crop_filter: Optional[str] = None
        if job.config.get("crop"):
            raw_crop = await _detect_crop(input_path, duration_sec)
            if raw_crop:
                # Validate crop against stored (not display) dimensions.
                # cropdetect runs on decoded/scaled frames so it may return
                # display dimensions, but the crop filter works on stored pixels.
                # If the crop values exceed stored dimensions (anamorphic content)
                # or are a no-op, discard them to avoid EINVAL from ffmpeg.
                src_w = vst.get("width") or 0
                src_h = vst.get("height") or 0
                try:
                    cw, ch, cx, cy = (int(v) for v in raw_crop.split(":"))
                    if src_w and src_h and (cw + cx > src_w or ch + cy > src_h):
                        log.warning(
                            "Encode %s: cropdetect %s exceeds stored dimensions "
                            "%dx%d — skipping crop", job_id[:8], raw_crop, src_w, src_h,
                        )
                    elif src_w and src_h and cw == src_w and ch == src_h and cx == 0 and cy == 0:
                        log.info("Encode %s: cropdetect %s is a no-op — skipping", job_id[:8], raw_crop)
                    else:
                        crop_filter = raw_crop
                        log.info("Encode %s: cropdetect → crop=%s", job_id[:8], crop_filter)
                except (ValueError, TypeError):
                    log.warning("Encode %s: could not parse cropdetect value %r", job_id[:8], raw_crop)
            else:
                log.info("Encode %s: cropdetect found no crop", job_id[:8])

        cmd, encoder = _build_ffmpeg_cmd(
            job.input_path, job.output_path, job.config, _hw_accel_info,
            bit_depth, is_hdr, cp, tc, cs, cr,
            crop_filter=crop_filter, a_streams=a_streams, source_fps=source_fps,
        )
        job.encoder = encoder
        try:
            st = await asyncio.to_thread(input_path.stat)
            job.input_size = st.st_size
        except Exception:
            pass
        _notify_encode(job_id)

        log.info("Encode %s: %s", job_id[:8], " ".join(cmd))
        ff_env = {**os.environ, "LIBVA_DRIVER_NAME": "iHD"}
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,  # -progress pipe:1 → structured progress
            stderr=asyncio.subprocess.PIPE,  # ffmpeg log output
            env=ff_env,
        )
        job._proc = proc

        stderr_lines: list[str] = []

        async def _drain_stderr():
            async for raw in proc.stderr:
                line = raw.decode(errors="replace").rstrip()
                if line:
                    stderr_lines.append(line)

        stderr_task = asyncio.create_task(_drain_stderr())
        duration_us = int(duration_sec * 1e6) if duration_sec else 0
        last_notify = 0.0
        buf = ""

        # ffmpeg -progress pipe:1 emits key=value lines; parse for progress updates
        while True:
            chunk = await proc.stdout.read(512)
            if not chunk:
                break
            buf += chunk.decode(errors="replace")
            lines = buf.split("\n")
            buf = lines[-1]
            for line in lines[:-1]:
                key, _, val = line.strip().partition("=")
                if key == "out_time_us" and duration_us:
                    try:
                        job.progress = min(99.0, int(val) / duration_us * 100)
                    except (ValueError, ZeroDivisionError):
                        pass
                elif key == "fps":
                    try:
                        job.current_fps = float(val)
                        job.avg_fps = float(val)
                    except ValueError:
                        pass
                elif key == "speed":
                    pass  # ETA is calculated client-side from elapsed time and progress
                if key in ("out_time_us", "fps", "total_size"):
                    now = time.monotonic()
                    if now - last_notify >= 0.5:
                        try:
                            op = Path(job.output_path)
                            if op.exists():
                                job.output_size = (await asyncio.to_thread(op.stat)).st_size
                        except Exception:
                            pass
                        _notify_encode(job_id)
                        last_notify = now

        await proc.wait()
        try:
            await asyncio.wait_for(stderr_task, timeout=2.0)
        except asyncio.TimeoutError:
            pass

        if job.status != "cancelled":
            if proc.returncode == 0:
                job.status = "done"
                job.progress = 100.0
                try:
                    job.output_size = Path(job.output_path).stat().st_size
                except Exception:
                    pass
            else:
                job.status = "failed"
                err_lines = [l for l in stderr_lines if any(
                    kw in l.lower() for kw in ("error", "failed", "invalid", "cannot", "unable")
                )]
                detail = (err_lines[-1] if err_lines else stderr_lines[-1]) if stderr_lines else ""
                job.error = f"ffmpeg exited with code {proc.returncode}" + (f": {detail}" if detail else "")
                if stderr_lines:
                    log.error("Encode %s stderr tail:\n%s", job_id[:8], "\n".join(stderr_lines[-20:]))

    except asyncio.CancelledError:
        job.status = "cancelled"
        if job._proc and job._proc.returncode is None:
            try:
                job._proc.kill()
                await asyncio.wait_for(job._proc.wait(), timeout=5.0)
            except Exception:
                pass
        raise

    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        log.error("Encode job %s failed: %s", job_id, exc, exc_info=True)

    finally:
        if job.finished_at is None:
            job.finished_at = time.time()
        job._proc = None
        if job.status in ("failed", "cancelled"):
            try:
                op = Path(job.output_path)
                if op.exists():
                    op.unlink()
            except Exception:
                log.warning("Could not remove partial output %s", job.output_path)
        # Only notify/save if the job wasn't dismissed while we were running
        if job_id in _encode_jobs:
            _notify_encode(job_id)
            await _save_encode_job(job)


def _get_driver_info() -> dict:
    import glob as _glob
    info: dict = {
        "dri_devices": [],
        "vaapi_driver": None,
        "vaapi_profiles": 0,
        "libvpl": False,
        "intel_iHD": False,
        "intel_i965": False,
        "mesa_va": False,
    }

    # DRI render nodes
    info["dri_devices"] = sorted(_glob.glob("/dev/dri/renderD*"))

    # Library presence via ldconfig cache
    try:
        r = subprocess.run(["ldconfig", "-p"], capture_output=True, text=True, timeout=5)
        libs = r.stdout
        info["libvpl"]    = "libvpl.so"    in libs
        info["intel_iHD"] = "iHD_drv_video" in libs or bool(_glob.glob("/usr/lib/x86_64-linux-gnu/dri/iHD_drv_video.so"))
        info["intel_i965"]= "i965_drv_video" in libs or bool(_glob.glob("/usr/lib/x86_64-linux-gnu/dri/i965_drv_video.so"))
        info["mesa_va"]   = "radeonsi_drv_video" in libs or "nouveau_drv_video" in libs or bool(_glob.glob("/usr/lib/x86_64-linux-gnu/dri/radeonsi_drv_video.so"))
    except Exception:
        pass

    # vainfo — tells us which driver is active and how many profiles it exposes
    if shutil.which("vainfo"):
        try:
            r = subprocess.run(["vainfo"], capture_output=True, text=True, timeout=5)
            output = r.stdout + r.stderr
            m = re.search(r"Driver version:\s*(.+)", output)
            if m:
                info["vaapi_driver"] = m.group(1).strip()
            info["vaapi_profiles"] = len(re.findall(r"VAProfile", output))
        except Exception:
            pass

    return info


def _get_system_info() -> dict:
    """Return CPU model, core count, and total RAM."""
    info: dict = {"cpu_model": None, "cpu_cores": None, "ram_total_gb": None}
    # CPU model — try /proc/cpuinfo first (Linux), fall back to platform
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text(errors="replace")
        for line in cpuinfo.splitlines():
            if line.startswith("model name"):
                info["cpu_model"] = line.split(":", 1)[1].strip()
                break
        # Count physical cores (unique core id + physical id pairs)
        cores = set()
        phys = pkg = None
        for line in cpuinfo.splitlines():
            if line.startswith("physical id"):
                pkg = line.split(":", 1)[1].strip()
            elif line.startswith("core id"):
                phys = line.split(":", 1)[1].strip()
            elif line == "" and pkg is not None and phys is not None:
                cores.add((pkg, phys))
                pkg = phys = None
        info["cpu_cores"] = len(cores) if cores else os.cpu_count()
    except Exception:
        info["cpu_model"] = platform.processor() or platform.machine() or None
        info["cpu_cores"] = os.cpu_count()
    # Total RAM from /proc/meminfo
    try:
        meminfo = Path("/proc/meminfo").read_text(errors="replace")
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                kb = int(line.split()[1])
                info["ram_total_gb"] = round(kb / 1024 / 1024, 1)
                break
    except Exception:
        pass
    return info


def _collect_hw_debug() -> dict:
    """Collect verbose hardware diagnostics for the JSON export."""
    import glob as _glob
    dbg: dict = {
        "env": {},
        "dri_devices": [],
        "kernel_modules": {},
        "lspci_display": None,
        "vainfo_iHD": None,
        "vainfo_default": None,
        "ffmpeg_probe_qsv": {},
        "ffmpeg_probe_vaapi": {},
        "ffmpeg_probe_nvenc": {},
    }

    # Relevant environment variables
    for var in ("LIBVA_DRIVER_NAME", "LIBVA_DRIVERS_PATH", "LIBVA_TRACE",
                "DRI_PRIME", "MESA_VK_DEVICE_SELECT", "VK_ICD_FILENAMES"):
        val = os.environ.get(var)
        if val is not None:
            dbg["env"][var] = val

    # Per-device info: path, permissions, vendor id, driver symlink
    for dev in sorted(_glob.glob("/dev/dri/renderD*")):
        entry: dict = {"path": dev}
        try:
            st = os.stat(dev)
            entry["mode"] = oct(st.st_mode)
            entry["gid"]  = st.st_gid
        except Exception as e:
            entry["stat_error"] = str(e)
        # Vendor ID from sysfs
        vfiles = _glob.glob(f"/sys/class/drm/{os.path.basename(dev)}/device/vendor")
        if vfiles:
            try:
                entry["vendor_id"] = Path(vfiles[0]).read_text().strip()
            except Exception:
                pass
        # device/uevent for driver name
        uevent_path = f"/sys/class/drm/{os.path.basename(dev)}/device/uevent"
        try:
            uevent = Path(uevent_path).read_text(errors="replace")
            for line in uevent.splitlines():
                if line.startswith("DRIVER="):
                    entry["kernel_driver"] = line.split("=", 1)[1]
                    break
        except Exception:
            pass
        dbg["dri_devices"].append(entry)

    # Kernel modules: are the relevant drivers loaded?
    try:
        modules_text = Path("/proc/modules").read_text(errors="replace")
        loaded = {line.split()[0] for line in modules_text.splitlines() if line}
        for mod in ("i915", "xe", "amdgpu", "radeon", "nvidia", "nvidia_drm",
                    "nvidia_uvm", "nouveau"):
            dbg["kernel_modules"][mod] = mod in loaded
    except Exception:
        pass

    # lspci — display/VGA devices
    if shutil.which("lspci"):
        try:
            r = subprocess.run(["lspci", "-mm", "-d", "::0300"],
                               capture_output=True, text=True, timeout=5)
            vga = r.stdout.strip()
            r2  = subprocess.run(["lspci", "-mm", "-d", "::0302"],
                                 capture_output=True, text=True, timeout=5)
            _3d = r2.stdout.strip()
            dbg["lspci_display"] = "\n".join(filter(None, [vga, _3d])) or None
        except Exception:
            pass

    # vainfo with iHD forced, then with default driver
    def _run_vainfo(env_override: dict) -> dict:
        if not shutil.which("vainfo"):
            return {"error": "vainfo not found"}
        env = {**os.environ, **env_override}
        try:
            r = subprocess.run(["vainfo", "--display", "drm", "--device",
                                 dbg["dri_devices"][0]["path"]] if dbg["dri_devices"]
                                else ["vainfo"],
                               capture_output=True, text=True, timeout=10, env=env)
            return {
                "returncode": r.returncode,
                "stdout": r.stdout.strip(),
                "stderr": r.stderr.strip(),
            }
        except Exception as e:
            return {"error": str(e)}

    dbg["vainfo_iHD"]     = _run_vainfo({"LIBVA_DRIVER_NAME": "iHD"})
    dbg["vainfo_default"] = _run_vainfo({})

    # Fresh ffmpeg encoder probes with full stderr captured
    def _probe(cmd: list, env_override: dict | None = None) -> dict:
        if not shutil.which("ffmpeg"):
            return {"error": "ffmpeg not found"}
        env = {**os.environ, **(env_override or {})}
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=20, env=env)
            return {
                "returncode": r.returncode,
                "stderr": r.stderr.decode(errors="replace").strip(),
            }
        except Exception as e:
            return {"error": str(e)}

    devs = [d["path"] for d in dbg["dri_devices"]]
    iHD_env = {"LIBVA_DRIVER_NAME": "iHD"}

    if devs:
        dev = devs[0]
        dbg["ffmpeg_probe_qsv"] = _probe(
            ["ffmpeg", "-v", "verbose",
             "-init_hw_device", f"vaapi=va:{dev}",
             "-init_hw_device", "qsv=hw@va",
             "-filter_hw_device", "hw",
             "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=1",
             "-vframes", "1", "-c:v", "hevc_qsv", "-f", "null", "-"],
            iHD_env,
        )
        dbg["ffmpeg_probe_vaapi"] = _probe(
            ["ffmpeg", "-v", "verbose",
             "-vaapi_device", dev,
             "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=1",
             "-vframes", "1",
             "-vf", "format=nv12,hwupload",
             "-c:v", "hevc_vaapi", "-f", "null", "-"],
            iHD_env,
        )
    else:
        dbg["ffmpeg_probe_qsv"]   = {"skipped": "no /dev/dri/renderD* devices found"}
        dbg["ffmpeg_probe_vaapi"] = {"skipped": "no /dev/dri/renderD* devices found"}

    if shutil.which("nvidia-smi"):
        dbg["ffmpeg_probe_nvenc"] = _probe(
            ["ffmpeg", "-v", "verbose",
             "-f", "lavfi", "-i", "nullsrc=size=320x240:rate=1",
             "-vframes", "1", "-c:v", "hevc_nvenc", "-f", "null", "-"],
        )
    else:
        dbg["ffmpeg_probe_nvenc"] = {"skipped": "nvidia-smi not found"}

    return dbg


@app.get("/encode/info")
async def encode_info():
    """Return component versions and hardware encoder support."""
    result: dict = {"hw": _hw_accel_info, "versions": {}, "ff_encoders": {},
                    "drivers": await asyncio.to_thread(_get_driver_info),
                    "system": await asyncio.to_thread(_get_system_info)}

    # ffmpeg version
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-version",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(p.communicate(), timeout=5)
        first = stdout.decode(errors="replace").splitlines()[0]
        m = re.search(r"version\s+(\S+)", first)
        result["versions"]["ffmpeg"] = m.group(1) if m else first.strip()
    except Exception:
        result["versions"]["ffmpeg"] = None

    # ffprobe version
    try:
        p = await asyncio.create_subprocess_exec(
            "ffprobe", "-version",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(p.communicate(), timeout=5)
        first = stdout.decode(errors="replace").splitlines()[0]
        m = re.search(r"version\s+(\S+)", first)
        result["versions"]["ffprobe"] = m.group(1) if m else first.strip()
    except Exception:
        result["versions"]["ffprobe"] = None

    # mkvtoolnix (mkvmerge) version
    try:
        p = await asyncio.create_subprocess_exec(
            "mkvmerge", "--version",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(p.communicate(), timeout=5)
        first = stdout.decode(errors="replace").splitlines()[0]
        m = re.search(r"v(\S+)", first)
        result["versions"]["mkvmerge"] = m.group(1) if m else first.strip()
    except Exception:
        result["versions"]["mkvmerge"] = None

    # python3-mutagen version
    try:
        import importlib.metadata as _ilm
        result["versions"]["mutagen"] = _ilm.version("mutagen")
    except Exception:
        result["versions"]["mutagen"] = None

    # Check which HEVC encoders are compiled into this ffmpeg build
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-encoders", "-v", "quiet",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(p.communicate(), timeout=10)
        enc_text = (stdout + stderr).decode(errors="replace")
        result["ff_encoders"] = {
            "qsv":     "hevc_qsv"   in enc_text,
            "vaapi":   "hevc_vaapi" in enc_text,
            "nvenc":   "hevc_nvenc" in enc_text,
            "libx265": "libx265"    in enc_text,
        }
    except Exception:
        result["ff_encoders"] = {"qsv": False, "vaapi": False, "nvenc": False, "libx265": False}

    result["debug"] = await asyncio.to_thread(_collect_hw_debug)
    return result


@app.get("/encode", response_class=HTMLResponse)
async def encode_page(request: Request):
    return templates.TemplateResponse("encode.html", {
        "request": request,
        "delete_token": DELETE_TOKEN,
        "ingress_path": request.state.ingress_path,
    })


@app.get("/databases", response_class=HTMLResponse)
async def databases_page(request: Request):
    return templates.TemplateResponse("databases.html", {
        "request": request,
        "ingress_path": request.state.ingress_path,
    })


_dbclean_progress: dict = {"phase": "idle", "checked": 0, "total": 0,
                           "meta_removed": 0, "imdb_removed": 0, "tmdb_removed": 0}


def _dbclean_thread() -> None:
    """Remove file_meta / file_imdb rows whose paths no longer exist, then prune tmdb_cache."""
    global _dbclean_progress
    _dbclean_progress = {"phase": "running", "checked": 0, "total": 0,
                         "meta_removed": 0, "imdb_removed": 0, "tmdb_removed": 0}
    try:
        import sqlite3 as _sq3
        conn = _sq3.connect(DB_PATH)

        meta_paths  = [r[0] for r in conn.execute("SELECT path FROM file_meta").fetchall()]
        imdb_paths  = [r[0] for r in conn.execute("SELECT path FROM file_imdb").fetchall()]
        all_paths   = list(dict.fromkeys(meta_paths + imdb_paths))  # unique, order-preserving
        total = len(all_paths)
        _dbclean_progress["total"] = total

        missing: set[str] = set()
        for i, p in enumerate(all_paths):
            _dbclean_progress["checked"] = i + 1
            if not Path(p).exists():
                missing.add(p)

        if missing:
            placeholders = ",".join("?" * len(missing))
            args = list(missing)
            meta_cur = conn.execute(
                f"DELETE FROM file_meta WHERE path IN ({placeholders})", args)
            _dbclean_progress["meta_removed"] = meta_cur.rowcount
            imdb_cur = conn.execute(
                f"DELETE FROM file_imdb WHERE path IN ({placeholders})", args)
            _dbclean_progress["imdb_removed"] = imdb_cur.rowcount
            conn.commit()

        # Prune tmdb_cache entries whose tconst is no longer in file_imdb
        try:
            tc = _sq3.connect(TMDB_DB_PATH)
            cur = tc.execute(
                "DELETE FROM tmdb_cache WHERE tconst NOT IN "
                f"(SELECT DISTINCT tconst FROM file_imdb WHERE tconst IS NOT NULL)"
            )
            _dbclean_progress["tmdb_removed"] = cur.rowcount
            tc.commit()
            tc.close()
        except Exception:
            pass

        conn.close()
        _dbclean_progress["phase"] = "done"
    except Exception as exc:
        _dbclean_progress["phase"] = "error"
        _dbclean_progress["error"] = str(exc)


@app.post("/db/clean")
async def db_clean():
    if _dbclean_progress.get("phase") == "running":
        return {"error": "Already running"}
    import threading
    threading.Thread(target=_dbclean_thread, daemon=True).start()
    return {"started": True}


@app.get("/db/clean-progress")
async def db_clean_progress():
    async def _gen():
        while True:
            p = dict(_dbclean_progress)
            yield f"data: {json.dumps(p)}\n\n"
            if p.get("phase") in ("done", "error", "idle"):
                break
            await asyncio.sleep(0.5)
    return StreamingResponse(_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/db/clean-status")
async def db_clean_status():
    return _dbclean_progress


@app.get("/db/stats")
async def db_stats():
    import os
    result: dict = {}
    try:
        result["app_db_size"] = os.path.getsize(DB_PATH)
    except OSError:
        result["app_db_size"] = 0
    try:
        result["imdb_db_size"] = os.path.getsize(IMDB_DB_PATH)
    except OSError:
        result["imdb_db_size"] = 0
    try:
        result["tmdb_db_size"] = os.path.getsize(TMDB_DB_PATH)
    except OSError:
        result["tmdb_db_size"] = 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM file_meta") as cur:
            result["file_count"] = (await cur.fetchone())[0]
        try:
            async with db.execute("SELECT COUNT(*) FROM file_imdb") as cur:
                result["imdb_match_count"] = (await cur.fetchone())[0]
        except Exception:
            result["imdb_match_count"] = 0
    return result


@app.get("/encode/events")
async def encode_events(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    _encode_subscribers.append(queue)
    init_msg = json.dumps({
        "type": "init",
        "jobs": [_job_dict(j) for j in _encode_jobs.values()],
        "hw": _hw_accel_info,
        "queue_order": list(_encode_queue_list),
        "schedule": _schedule_config,
    })

    async def generate():
        try:
            yield f"data: {init_msg}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            try:
                _encode_subscribers.remove(queue)
            except ValueError:
                pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/encode")
async def start_encode(request: Request, path: str = Query(...)):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    file_path = safe_path(path)
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    config = _make_encode_config(body)

    fmt = config["format"]
    _codec_tag = {"h264": "-h264", "av1": "-av1"}.get(config.get("codec", "hevc"), "")
    base_stem = f"{file_path.stem} (qp{config['qp']}{_codec_tag})"
    active_outputs = {j.output_path for j in _encode_jobs.values() if j.status in ("queued", "running")}
    candidate = file_path.parent / f"{base_stem}.{fmt}"
    counter = 2
    while str(candidate) in active_outputs or candidate.exists():
        candidate = file_path.parent / f"{base_stem}-{counter}.{fmt}"
        counter += 1
    output_path = candidate
    job_id = secrets.token_hex(8)
    job = EncodeJob(job_id, str(file_path), str(output_path), config)
    _encode_jobs[job_id] = job

    if not shutil.which("ffmpeg"):
        job.status = "failed"
        job.finished_at = time.time()
        job.error = "ffmpeg not found in PATH"
        _notify_encode(job_id)
        await _save_encode_job(job)
        return {"job_id": job_id, "error": job.error}

    _notify_encode(job_id)
    await _save_encode_job(job)
    _enqueue_job(job_id)
    return {"job_id": job_id, "output": str(output_path)}


def _make_encode_config(body: dict) -> dict:
    """Validate and normalise encode config from a request body dict."""
    fmt = str(body.get("format", "mkv")).lower()
    if fmt not in ("mkv", "mp4"):
        fmt = "mkv"
    gpu = str(body.get("gpu", "auto"))
    if gpu not in ("auto", "intel", "nvidia", "amd", "none"):
        gpu = "auto"
    preset_val = str(body.get("preset", "quality"))
    if preset_val not in ("quality", "balanced", "fast", "speed", "archive"):
        preset_val = "quality"
    codec = str(body.get("codec", "hevc")).lower()
    if codec not in ("hevc", "h264", "av1"):
        codec = "hevc"
    return {
        "qp":      max(1, min(51, int(body.get("qp", 18)))),
        "preset":  preset_val,
        "gpu":     gpu,
        "codec":   codec,
        "format":  fmt,
        "denoise": body.get("denoise") if body.get("denoise") in ("ultralight", "light", "medium", "strong", "stronger", "verystrong") else None,
        "crop":    bool(body.get("crop", False)),
        "width":   int(body["width"]) if body.get("width") else None,
        "lang":    _validated_lang(body.get("lang", "eng")),
    }


def _queue_file_encode(file_path: Path, config: dict) -> str | None:
    """Create and enqueue one encode job. Returns job_id or None if ffmpeg missing."""
    fmt = config["format"]
    _codec_tag = {"h264": "-h264", "av1": "-av1"}.get(config.get("codec", "hevc"), "")
    base_stem = f"{file_path.stem} (qp{config['qp']}{_codec_tag})"
    active_outputs = {j.output_path for j in _encode_jobs.values() if j.status in ("queued", "running")}
    candidate = file_path.parent / f"{base_stem}.{fmt}"
    counter = 2
    while str(candidate) in active_outputs or candidate.exists():
        candidate = file_path.parent / f"{base_stem}-{counter}.{fmt}"
        counter += 1
    job_id = secrets.token_hex(8)
    job = EncodeJob(job_id, str(file_path), str(candidate), config)
    _encode_jobs[job_id] = job
    if not shutil.which("ffmpeg"):
        job.status = "failed"
        job.finished_at = time.time()
        job.error = "ffmpeg not found in PATH"
        _notify_encode(job_id)
        return None
    _notify_encode(job_id)
    return job_id


@app.post("/encode/folder")
async def start_folder_encode(request: Request, path: str = Query(...)):
    """Recursively queue encode jobs for every media file under a directory."""
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    folder = safe_path(path)
    if not folder.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    config = _make_encode_config(body)

    def _walk_media(base: Path) -> list[Path]:
        result = []
        for root, _, files in os.walk(base):
            for fname in files:
                if os.path.splitext(fname)[1].lower() in MEDIA_EXTENSIONS:
                    result.append(Path(root) / fname)
        result.sort()
        return result

    media_files = await asyncio.to_thread(_walk_media, folder)

    queued: list[str] = []
    for fpath in media_files:
        job_id = _queue_file_encode(fpath, config)
        if job_id:
            queued.append(job_id)
            await _save_encode_job(_encode_jobs[job_id])

    for job_id in queued:
        _enqueue_job(job_id)

    return {"queued": len(queued), "total": len(media_files)}


@app.delete("/encode/{job_id}")
async def cancel_encode(job_id: str, request: Request):
    """Cancel a running or queued job. Keeps it in the list for retry/dismiss."""
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    job = _encode_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in ("running", "queued"):
        raise HTTPException(status_code=409, detail="Job is not active")
    job.status = "cancelled"
    job.finished_at = time.time()
    try:
        _encode_queue_list.remove(job_id)
        _broadcast_queue_order()
    except ValueError:
        pass
    if job._proc and job._proc.returncode is None:
        try:
            job._proc.kill()
        except Exception:
            pass
    _notify_encode(job_id)
    await _save_encode_job(job)
    return Response(status_code=204)


@app.post("/encode/{job_id}/retry")
async def retry_encode(job_id: str, request: Request):
    """Re-queue a cancelled or failed job."""
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    job = _encode_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in ("cancelled", "failed"):
        raise HTTPException(status_code=409, detail="Job is not in a retryable state")
    job.status = "queued"
    job.progress = 0.0
    job.current_fps = 0.0
    job.avg_fps = 0.0
    job.eta = "--"
    job.encoder = ""
    job.output_size = 0
    job.started_at = 0.0
    job.finished_at = None
    job.error = None
    job._proc = None
    _notify_encode(job_id)
    await _save_encode_job(job)
    _enqueue_job(job_id)
    return Response(status_code=204)


@app.delete("/encode/{job_id}/dismiss")
async def dismiss_encode(job_id: str, request: Request):
    """Remove a finished (done/cancelled/failed) job from the list."""
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    job = _encode_jobs.pop(job_id, None)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status in ("running", "queued"):
        # Safety: kill if somehow still active
        job.status = "cancelled"
        try:
            _encode_queue_list.remove(job_id)
            _broadcast_queue_order()
        except ValueError:
            pass
        if job._proc and job._proc.returncode is None:
            try:
                job._proc.kill()
            except Exception:
                pass
    await _delete_encode_job_db(job_id)
    msg = json.dumps({"type": "remove", "job_id": job_id})
    for q in _encode_subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass
    return Response(status_code=204)


@app.post("/encode/{job_id}/move")
async def move_encode(job_id: str, request: Request):
    """Move output file over input file, replacing the original."""
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    job = _encode_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "done":
        raise HTTPException(status_code=400, detail="Job must be in done state to move")
    src = Path(job.output_path)
    dst = Path(job.input_path)
    if not src.exists():
        raise HTTPException(status_code=400, detail="Output file not found")
    try:
        await asyncio.to_thread(shutil.move, str(src), str(dst))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    job.moved = True
    # Invalidate stale metadata so next dir scan picks up the replaced file
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM file_meta WHERE path = ?", (str(dst),))
        await db.commit()
    parent = str(dst.parent)
    _dir_size_cache.pop(parent, None)
    _dir_listing_cache.pop(parent, None)
    _notify_encode(job_id)
    await _save_encode_job(job)
    return Response(status_code=204)


@app.post("/encode/{job_id}/reorder")
async def reorder_encode(job_id: str, request: Request):
    """Move a queued job up or down in the queue."""
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    direction = body.get("direction")
    if direction not in ("up", "down"):
        raise HTTPException(status_code=400, detail="direction must be 'up' or 'down'")
    if job_id not in _encode_queue_list:
        raise HTTPException(status_code=404, detail="Job not in queue")
    idx = _encode_queue_list.index(job_id)
    if direction == "up" and idx > 0:
        _encode_queue_list[idx], _encode_queue_list[idx - 1] = _encode_queue_list[idx - 1], _encode_queue_list[idx]
    elif direction == "down" and idx < len(_encode_queue_list) - 1:
        _encode_queue_list[idx], _encode_queue_list[idx + 1] = _encode_queue_list[idx + 1], _encode_queue_list[idx]
    _broadcast_queue_order()
    return Response(status_code=204)


@app.get("/encode/stats")
async def encode_stats():
    """Aggregate statistics for all encode jobs in the DB."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='done'      THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN status='failed'    THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status='done' THEN input_size  ELSE 0 END) AS total_input,
                SUM(CASE WHEN status='done' THEN output_size ELSE 0 END) AS total_output,
                AVG(CASE WHEN status='done' AND started_at > 0 AND finished_at IS NOT NULL
                    THEN finished_at - started_at ELSE NULL END) AS avg_duration
            FROM encode_jobs
        """) as cur:
            row = await cur.fetchone()
    if not row or not row[0]:
        return {"total": 0, "done": 0, "failed": 0, "cancelled": 0,
                "total_input": 0, "total_output": 0, "saved_bytes": 0, "avg_duration_secs": None}
    total, done, failed, cancelled, total_input, total_output, avg_dur = row
    saved = max(0, (total_input or 0) - (total_output or 0))
    return {
        "total": total or 0,
        "done": done or 0,
        "failed": failed or 0,
        "cancelled": cancelled or 0,
        "total_input": total_input or 0,
        "total_output": total_output or 0,
        "saved_bytes": saved,
        "avg_duration_secs": round(avg_dur) if avg_dur else None,
    }


@app.get("/recently-added")
async def recently_added(limit: int = Query(default=50, le=200)):
    """Return the most recently scanned files by mtime."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM file_meta ORDER BY COALESCE(scanned_at, mtime) DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
    files = []
    for row in rows:
        r = dict(row)
        p = Path(r["path"])
        r["name"]            = p.name
        r["stem"]            = p.stem
        r["ext"]             = p.suffix.lower()
        r["human_size"]      = human_size(r["size"])
        r["size_class"]      = size_css_class(r.get("size") or 0)
        r["codec_class"]     = codec_css_class(r.get("video_codec") or "")
        r["ext_class"]       = ext_css_class(p.suffix)
        r["res_class"]       = res_css_class(r.get("width") or 0, r.get("height") or 0)
        r["hdr_class"]       = hdr_css_class(r.get("hdr_type") or "")
        r["needs_transcode"] = (r.get("audio_codec") or "").lower() not in BROWSER_SAFE_AUDIO
        r["duration_label"] = _fmt_duration(r.get("duration_sec") or (r.get("duration_min") or 0) * 60)
        files.append(r)
    return {"files": files}


@app.get("/encode/schedule")
async def get_schedule():
    return _schedule_config


@app.post("/encode/schedule")
async def set_schedule(request: Request):
    if request.headers.get("X-Delete-Token") != DELETE_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    enabled = bool(body.get("enabled", False))
    start   = max(0, min(23, int(body.get("start", 22))))
    end     = max(0, min(23, int(body.get("end", 6))))
    _schedule_config.update({"enabled": enabled, "start": start, "end": end})
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('encode_schedule', ?)",
            (json.dumps(_schedule_config),),
        )
        await db.commit()
    return _schedule_config


_NOISE_RE = re.compile(
    r'\b(19|20)\d{2}\b|\b(2160|1080|720|480)[pi]\b|\b4k\b'
    r'|\b(bluray|bdrip|webrip|web-dl|hdtv|dvdrip|x264|x265|hevc|avc|h264|h265|aac|dts|ac3|remux|extended|remastered|unrated|theatrical)\b',
    re.IGNORECASE,
)


def _norm_for_dupe(stem: str) -> str:
    s = stem.lower().replace(".", " ").replace("_", " ").replace("-", " ")
    s = _NOISE_RE.sub(" ", s)
    return " ".join(s.split())


def _dice_sim(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if len(a) < 2 or len(b) < 2:
        return 0.0
    def bigrams(s: str) -> list[str]:
        return [s[i:i+2] for i in range(len(s) - 1)]
    ba, bb = bigrams(a), bigrams(b)
    counts: dict[str, int] = {}
    for g in bb:
        counts[g] = counts.get(g, 0) + 1
    common = 0
    for g in ba:
        if counts.get(g, 0) > 0:
            common += 1
            counts[g] -= 1
    return (2 * common) / (len(ba) + len(bb))


@app.get("/dupes")
async def find_dupes(threshold: float = Query(default=0.9, ge=0.5, le=1.0)):
    """Return groups of likely-duplicate files by name similarity across the whole DB."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT path, size, video_codec, width, height FROM file_meta ORDER BY path"
        ) as cur:
            rows = await cur.fetchall()

    files = []
    for row in rows:
        p = Path(row["path"])
        norm = _norm_for_dupe(p.stem)
        if not norm:
            continue
        files.append({
            "path":  row["path"],
            "stem":  p.stem,
            "norm":  norm,
            "size":  human_size(row["size"]) if row["size"] else "",
            "codec": row["video_codec"] or "",
            "res":   f"{row['width']}×{row['height']}" if row["width"] and row["height"] else "",
        })

    def _find_groups(flist: list, thresh: float) -> list:
        # Greedy O(N²) grouping — transitivity not guaranteed for N-way dupes
        used = [False] * len(flist)
        result = []
        for i in range(len(flist)):
            if used[i]:
                continue
            group = [{"file": flist[i], "score": 1.0}]
            for j in range(i + 1, len(flist)):
                if used[j]:
                    continue
                score = _dice_sim(flist[i]["norm"], flist[j]["norm"])
                if score >= thresh:
                    group.append({"file": flist[j], "score": round(score, 3)})
                    used[j] = True
            if len(group) > 1:
                used[i] = True
                result.append(group)
        return result

    groups = await asyncio.to_thread(_find_groups, files, threshold)
    return {"groups": groups}


@app.get("/health-check")
async def health_check(request: Request):
    """SSE stream — ffprobes all media files under current_root and reports issues."""

    def _collect_files(root: Path) -> list[Path]:
        files = []
        for dirpath, _, filenames in os.walk(str(root)):
            for fname in sorted(filenames):
                if os.path.splitext(fname)[1].lower() in MEDIA_EXTENSIONS:
                    files.append(Path(dirpath) / fname)
        return files

    async def generate():
        all_files = await asyncio.to_thread(_collect_files, current_root)
        total = len(all_files)
        yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"
        issues = []
        for i, fpath in enumerate(all_files):
            if await request.is_disconnected():
                return
            probe_task = asyncio.create_task(run_ffprobe(fpath))
            try:
                # Poll for disconnect every 2 s so we can bail early
                while not probe_task.done():
                    done, _ = await asyncio.wait({probe_task}, timeout=2.0)
                    if not done and await request.is_disconnected():
                        probe_task.cancel()
                        await asyncio.gather(probe_task, return_exceptions=True)
                        return
                meta = probe_task.result()
                file_issues = []
                if not meta.get("video_codec") or meta["video_codec"] == "N/A":
                    file_issues.append("no video stream")
                if not meta.get("audio_codec") or meta["audio_codec"] == "N/A":
                    file_issues.append("no audio stream")
                if not meta.get("duration_sec") and not meta.get("duration_min"):
                    file_issues.append("zero duration")
                if file_issues:
                    issues.append({"path": str(fpath), "name": fpath.name, "issues": file_issues})
            except Exception as e:
                issues.append({"path": str(fpath), "name": fpath.name, "issues": [str(e)]})
            yield f"data: {json.dumps({'type': 'progress', 'done': i + 1, 'total': total})}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'issues': issues, 'scanned': total})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8080)
