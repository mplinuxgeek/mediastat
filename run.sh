#!/bin/bash
set -e

SMB_SERVER="192.168.1.50"
SMB_SHARE="movies"
HOST_MEDIA="/mnt/mediastat-movies"
HOST_DATA="/tmp/mediastat-data"
HOST_CONFIG="${HOST_DATA}/config.yaml"
IMAGE_TAG="mediastat-local"
CONTAINER_NAME="mediastat-local"

# ./run.sh --rollback — skip the build entirely and recreate the container
# from the image tagged :prev (the one running before the last rebuild),
# for when a rebuild turns out bad. Only one rollback level is kept.
#
# ./run.sh --update — pull the latest commit before rebuilding, turning
# "update mediastat" into one command. Skipped (with a warning) if the
# working tree has uncommitted changes, rather than risking a pull that
# conflicts with in-progress local edits.
ROLLBACK=false
UPDATE=false
case "${1:-}" in
    --rollback) ROLLBACK=true ;;
    --update)   UPDATE=true ;;
esac

if [ "${UPDATE}" = true ]; then
    if [ -n "$(git status --porcelain)" ]; then
        echo "Working tree has uncommitted changes — skipping git pull. Commit/stash first, or run ./run.sh without --update to rebuild as-is." >&2
    else
        echo "Pulling latest commit..."
        git pull --ff-only
    fi
fi

UID_=$(id -u)
GID_=$(id -g)

mkdir -p "${HOST_DATA}"
sudo mkdir -p "${HOST_MEDIA}"

# Mount the SMB share via the kernel (CIFS). Unlike GVFS this gives a normal
# path with no funny characters and is readable by root, so Docker can bind-mount it.
# Left mounted after this script exits (container is detached and keeps using it).
if ! mountpoint -q "${HOST_MEDIA}"; then
    sudo mount -t cifs "//${SMB_SERVER}/${SMB_SHARE}" "${HOST_MEDIA}" \
        -o "guest,vers=3.0,uid=${UID_},gid=${GID_},rw,file_mode=0664,dir_mode=0775"
fi

if [ ! -f "${HOST_CONFIG}" ]; then
    cat > "${HOST_CONFIG}" <<'EOF'
directories:
  - label: "Movies"
    path: "/media/Movies"
EOF
fi

if [ "${ROLLBACK}" = true ]; then
    if ! docker image inspect "${IMAGE_TAG}:prev" >/dev/null 2>&1; then
        echo "No ${IMAGE_TAG}:prev image to roll back to (need at least one prior successful build/rerun)." >&2
        exit 1
    fi
    echo "Rolling back: ${IMAGE_TAG}:prev -> ${IMAGE_TAG}:latest"
    docker tag "${IMAGE_TAG}:prev" "${IMAGE_TAG}:latest"
else
    # Save the currently-running image as :prev before replacing it, so a bad
    # rebuild can be undone with `./run.sh --rollback` instead of re-cloning
    # and rebuilding an old commit.
    if docker image inspect "${IMAGE_TAG}:latest" >/dev/null 2>&1; then
        docker tag "${IMAGE_TAG}:latest" "${IMAGE_TAG}:prev"
    fi

    GIT_SHA=$(git rev-parse HEAD 2>/dev/null || echo unknown)
    BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    docker build -f ha-addon/Dockerfile \
        --build-arg GIT_SHA="${GIT_SHA}" \
        --build-arg BUILD_DATE="${BUILD_DATE}" \
        -t "${IMAGE_TAG}:latest" .
fi

# Recreate the container from the freshly built image. Run this script again
# after any rebuild to pick up the new image; old container is discarded.
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

# Bypass the HA-bashio entrypoint (no supervisor here) and run uvicorn directly.
docker run -d \
    --name "${CONTAINER_NAME}" \
    --restart unless-stopped \
    -p 8081:8080 \
    -e CONFIG_PATH=/data/config.yaml \
    -e DB_PATH=/data/mediastat.db \
    -e MEDIA_ROOT=/media/Movies \
    -e LIBVA_DRIVER_NAME=iHD \
    -v "${HOST_DATA}:/data" \
    -v "${HOST_MEDIA}:/media/Movies:rw" \
    --device /dev/dri \
    --entrypoint /app/.venv/bin/uvicorn \
    "${IMAGE_TAG}" \
    main:app --host 0.0.0.0 --port 8080 --loop asyncio --log-level info \
        --proxy-headers --forwarded-allow-ips '*'

echo "mediastat-local up on :8081. logs: docker logs -f ${CONTAINER_NAME}"
echo "update to the latest commit + rebuild + restart: ./run.sh --update"
echo "roll back a bad update with: ./run.sh --rollback"
