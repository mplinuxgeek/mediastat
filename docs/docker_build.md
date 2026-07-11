# Docker build

There are two separate images in this repo. Pick the one that matches what
you're doing.

| | Root `Dockerfile` | `ha-addon/Dockerfile` |
|---|---|---|
| Base image | `nvidia/cuda:12.6.0-base-ubuntu24.04` | `ghcr.io/home-assistant/<arch>-base-debian` |
| GPU support | NVIDIA NVENC only | Intel QSV/VA-API, AMD VA-API, or software |
| Intended use | Standalone Docker (no Home Assistant) | HA add-on, and local dev via `run.sh` |
| Built by | `docker build .` / `docker-compose` | `build.sh`, `run.sh`, CI |

If you just want a container running today outside Home Assistant with an
NVIDIA GPU, use the root `Dockerfile` + `docker-compose.yml` (this section).
If you're testing add-on behavior locally (HA-specific env vars, ingress,
Intel/AMD hardware encoding), use `ha-addon/Dockerfile` via `build.sh` /
`run.sh` instead — see [ha_addon_build.md](ha_addon_build.md).

## Standalone: root Dockerfile + docker-compose

Build context is the repo root; the image copies `src/` in directly.

```bash
docker compose build
```

Edit `docker-compose.yml` first — at minimum the media bind mount:

```yaml
volumes:
  - /path/to/your/media:/media:ro   # ← change this
  - mediastat_data:/data
```

Then run:

```bash
docker compose up -d
```

The app listens on `:8080` (mapped to `:8080` on the host by default). State
(SQLite DB, config) persists in the `mediastat_data` named volume, mounted at
`/data`.

### Plain `docker build`/`docker run` (no compose)

```bash
docker build -t mediastat .
docker run -d \
    -p 8080:8080 \
    -v /path/to/your/media:/media:ro \
    -v mediastat_data:/data \
    -e MEDIA_ROOT=/media \
    -e DB_PATH=/data/mediastat.db \
    mediastat
```

### NVIDIA GPU passthrough

The root Dockerfile only targets NVENC. To actually use the GPU at runtime,
add `--gpus all` (plain `docker run`) or the equivalent `deploy.resources`
block under `docker-compose.yml`'s `mediastat` service, and make sure the host
has the NVIDIA Container Toolkit installed.

## Building the ha-addon image locally instead

If you want the same image the add-on ships (Intel/AMD hardware encoding,
`/data` layout matching HA, etc.) without going through Home Assistant:

```bash
./build.sh              # builds for your host arch, loads into local Docker
./run.sh                 # builds via ha-addon/Dockerfile + runs it, with SMB mounts
./run_local_docker.sh    # runs an image build.sh already produced
```

Details in [ha_addon_build.md](ha_addon_build.md).
