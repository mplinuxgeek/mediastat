# MediaStat

Media browser, health checker, and HandBrake/ffmpeg transcoder for a movie/TV
library. Ships primarily as a Home Assistant add-on (with ingress into the HA
sidebar), but also runs standalone in plain Docker.

## Features

- Browse configured media directories, inspect file/container details, and
  stream files directly (including HLS) from the UI.
- Disk usage, duplicate detection, and library stats (`/disk-usage`, `/dupes`,
  `/db/stats`).
- IMDb/TMDb metadata matching and scanning, with a review queue for
  ambiguous matches (`/imdb-scan`, `/imdb/matches`, `/tmdb/search`).
- Hardware-accelerated encoding (Intel QSV/VA-API, AMD VA-API, NVIDIA NVENC,
  or software x265) with:
  - Crop detection, output resolution and stream-selection logic.
  - QP estimation via an SSIM-based sweep, with live progress, cancel, and
    reattach-on-reopen support (`/encode/estimate*`).
  - Bulk encode jobs, retention, and a job history/schedule view.
- SQLite-backed job/database state with backup and cleanup endpoints.

## Layout

```
src/                  # application code (FastAPI app, templates, static assets)
  templates/_base.html #   shared page shell: header, nav (Library/Jobs/Auto/IMDb/System), theme
  static/app.css       #   one stylesheet for every page (tokens + shared components)
  static/core.js       #   shared JS: ingress shim, toasts, formatters, live Jobs badge
  static/app.js        #   Library page only
tests/                # pytest suite
ha-addon/              # Home Assistant add-on packaging (config.yaml, Dockerfile, run.sh)
Dockerfile             # standalone (non-HA) image, NVIDIA CUDA base
docker-compose.yml     # standalone deployment via the root Dockerfile
run.sh                 # local dev loop: build ha-addon image, run it, mount SMB shares
run_local_docker.sh    # run an already-built ha-addon image standalone
build.sh               # build (and optionally push) the ha-addon image for one/all arches
release.sh             # bump ha-addon/config.yaml version, commit, tag, push
.github/workflows/     # CI: test suite + tag-triggered GHCR build/publish
```

Two build paths exist side by side — see [docs/docker_build.md](docs/docker_build.md)
and [docs/ha_addon_build.md](docs/ha_addon_build.md) for which one to use.

## Configuration

The app reads a YAML config (`CONFIG_PATH`, default `config.yaml` next to
`main.py`):

```yaml
tmdb_api_key: "your-tmdb-v3-key"
directories:
  - label: Movies
    path: /media/Movies
  - label: TV
    path: /media/TV
```

Environment variables:

| Variable       | Default                          | Purpose                          |
|----------------|-----------------------------------|-----------------------------------|
| `MEDIA_ROOT`   | `/media`                          | Default browse root              |
| `DB_PATH`      | `<app dir>/mediastat.db`          | SQLite file/job metadata cache   |
| `CONFIG_PATH`  | `<app dir>/config.yaml`           | Path to the YAML config above    |

Auto-encode (off unless `AUTO_ENCODE=1` or it is enabled on the Auto page):

| Variable            | Default      | Purpose                                                              |
|---------------------|--------------|----------------------------------------------------------------------|
| `AUTO_ENCODE`       | `0`          | Enable the auto re-encode worker; the Enable/Disable switch on the Auto page overrides this once used |
| `AUTO_DRY_RUN`      | `1`          | Decide and log only; the UI toggle overrides this once saved         |
| `AUTO_DIRS`         | (none)       | Colon-separated library roots to scan                                |
| `AUTO_MIN_GB`       | `15`         | Only consider files at least this big; the Auto page overrides this once saved |
| `AUTO_PRESET`       | `archive`    | Encode preset                                                        |
| `AUTO_SCRATCH`      | `/scratch`   | Where encodes are written before replacing the original              |
| `AUTO_TRASH_DAYS`   | `7`          | Originals stay in `.mediastat-trash` this long                       |
| `AUTO_MIN_FREE_GB`  | `0` (off)    | If > 0, below this free space the oldest trash (older than 24h) is purged |
| `AUTO_MAX_QP`       | `20`         | The auto pipeline never picks a QP above this; the Auto page overrides this once saved |
| `AUTO_INTERVAL`     | `300`        | Seconds between work steps                                           |
| `AUTO_SCAN_HOURS`   | `3`          | Hours between library scans                                          |
| `AUTO_PLEX_CONF`    | `/data/plex.json` | Plex refresh config                                             |
| `AUTO_WINDOW`       | (none)       | Default run window, e.g. `23-7`                                      |

When `webhook_url` is set, auto-encode also sends replaced / rejected / failed / trash-purged messages.

Under the HA add-on, `config.yaml` is written from the add-on options UI on
every start instead (see `ha-addon/run.sh`).

## Running locally without Docker

```bash
pip install -r requirements.txt
cd src
MEDIA_ROOT=/path/to/media uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

## Running in Docker

See [docs/docker_build.md](docs/docker_build.md).

## Home Assistant add-on

See [docs/ha_addon_build.md](docs/ha_addon_build.md).

## Tests

```bash
pip install pytest
python3 -m pytest tests/ -v
```

CI (`.github/workflows/build-addon.yml`) runs this same suite on every push,
before building/publishing on tagged releases.
