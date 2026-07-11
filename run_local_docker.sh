#!/usr/bin/env bash
# Run a locally built mediastat image standalone (no HA supervisor).
# Build one first with: ./build.sh
set -euo pipefail

IMAGE="${IMAGE:-ghcr.io/mplinuxgeek/mediastat-addon:latest-amd64}"
PORT="${PORT:-8080}"
MEDIA_DIR="${MEDIA_DIR:-$HOME/Videos}"
CONTAINER_NAME="${CONTAINER_NAME:-mediastat-local}"

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    echo "Image ${IMAGE} not found locally. Build it first with: ./build.sh" >&2
    exit 1
fi

docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

docker run -d --gpus all \
    --name "${CONTAINER_NAME}" \
    -p "${PORT}:8080" \
    -v "${MEDIA_DIR}:/media" \
    -v mediastat_data:/data \
    "${IMAGE}"

echo "mediastat up on :${PORT}. logs: docker logs -f ${CONTAINER_NAME}"
