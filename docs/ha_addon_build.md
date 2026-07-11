# Home Assistant add-on build & release

MediaStat ships as a Home Assistant add-on backed by `ha-addon/Dockerfile`
(Debian base matching HA's supervisor images, Intel QSV/VA-API + AMD VA-API
hardware encoding, software fallback). This is also the image used for local
dev via `run.sh`.

## Files involved

| File | Purpose |
|---|---|
| `ha-addon/config.yaml` | Add-on manifest — name, version, options schema, ports, ingress, device/map permissions |
| `ha-addon/build.yaml` | Base image per architecture, OCI labels |
| `ha-addon/Dockerfile` | Image build — installs ffmpeg/mkvtoolnix/VA-API drivers, copies `src/` in as the app |
| `ha-addon/run.sh` | Container entrypoint — reads HA options via `bashio`, writes `/data/config.yaml`, sets up GPU render-node group access, execs uvicorn |
| `.github/workflows/build-addon.yml` | CI: runs `pytest`, then on a `v*` tag builds + pushes to GHCR and bumps `config.yaml`'s version to match |
| `build.sh` | Local equivalent of the CI build step, for one arch or all |
| `run.sh` | Local dev loop: build the ha-addon image, mount SMB shares, run the container, supports `--update`/`--rollback` |
| `run_local_docker.sh` | Run an already-built ha-addon image standalone (no SMB mounting) |
| `release.sh` | Bump the version, commit, tag, and push — the thing that actually triggers CI |

The build context for `ha-addon/Dockerfile` is the **repo root** (not
`ha-addon/`) — it does `COPY src/ .` and `COPY requirements.txt .` directly
from the top level, then `COPY ha-addon/run.sh /run.sh`. Both `build.sh` and
the GitHub Actions workflow build with root as context; don't try to
`docker build ha-addon/` on its own, it won't find `src/`.

## Local dev loop

```bash
./run.sh              # build ha-addon/Dockerfile, mount SMB shares, run on :8081
./run.sh --update      # git pull --ff-only first (skipped if tree is dirty), then rebuild
./run.sh --rollback    # recreate the container from the previous image (:prev tag), no rebuild
```

This bypasses the HA supervisor and `run.sh`'s bashio entrypoint — it runs
uvicorn directly with `main:app`, so it's a faster loop for iterating without
a real HA instance. It bind-mounts SMB shares over CIFS and mounts a fixed
`/tmp/mediastat-data` as `/data`.

## Building for release

```bash
./build.sh                       # local arch only, --load into Docker
./build.sh --arch amd64          # explicit arch
./build.sh --push                # all arches in BASE_IMAGES, pushed + multi-arch manifest
```

Version tags come from `ha-addon/config.yaml`'s `version:` field —
`build.sh` reads it, so bump the version first (see below) if you want the
built tag to reflect it.

## Cutting a release (the part that actually publishes)

CI only builds and publishes to GHCR on a pushed `v*` **tag** — a plain
commit to `main` does nothing. `release.sh` does the whole sequence:

```bash
./release.sh            # patch bump: 1.0.35 -> 1.0.36
./release.sh minor       # 1.0.35 -> 1.1.0
./release.sh major       # 1.0.35 -> 2.0.0
```

It will refuse to run with a dirty working tree, and refuses to reuse an
existing tag. It bumps `ha-addon/config.yaml`, commits, pushes `main`, tags
`vX.Y.Z`, and pushes the tag — which fires
`.github/workflows/build-addon.yml`:

1. `test` job — runs `pytest tests/`.
2. `build` job — builds the image per arch in the matrix, pushes to
   `ghcr.io/<owner>/mediastat-addon-<arch>:<version>` and `:latest`.
3. `update-version` job — re-checks `ha-addon/config.yaml`'s version matches
   the tag and commits/pushes if it drifted (a safety net; `release.sh`
   normally already leaves it in sync).

## Installing as a Home Assistant add-on repository

Add this repo's URL under **Settings → Add-ons → Add-on Store → ⋮ → Repositories**
in Home Assistant (`repository.yaml` at the repo root is what HA reads to
list it). HA then pulls `image:` from `ha-addon/config.yaml`
(`ghcr.io/mplinuxgeek/mediastat-addon-{arch}`) per-architecture instead of
building from source.

## Known drift to watch

- `ha-addon/build.yaml` still pins `bookworm` as the base image for local/HA
  supervisor-driven builds, while `ha-addon/Dockerfile`'s default `BUILD_FROM`
  and the CI workflow both use `trixie`. If HA ever builds this add-on itself
  (rather than pulling the prebuilt `image:`), `build.yaml` wins and it'll get
  `bookworm`.

- `build.sh` tags images as `<image>:<version>-<arch>` (e.g.
  `mediastat-addon:1.0.35-amd64`), but CI and `ha-addon/config.yaml`'s
  `image:` field both use `<image>-<arch>:<version>` (e.g.
  `mediastat-addon-amd64:1.0.35`). A `./build.sh --push` won't produce
  something HA can actually pull as configured — only the CI-published tags
  match what `config.yaml` expects.
