#!/usr/bin/env bash
# Build (and optionally push) the MediaStat Home Assistant add-on image.
#
# Usage:
#   ./build.sh                     # build for local arch, load into Docker
#   ./build.sh --push              # build for all arches and push to registry
#   ./build.sh --arch amd64        # build for a specific arch
#   ./build.sh --tag myuser/mediastat  # override image name
#
# Prerequisites:
#   docker (with buildx and the containerd image store enabled for multi-arch)
#   OR: the HA addon builder:
#     docker run --rm -it --name builder \
#       --privileged \
#       -v /var/run/docker.sock:/var/run/docker.sock \
#       -v "$(pwd)/ha-addon":/data \
#       homeassistant/amd64-builder \
#       --all -t /data

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
ADDON_DIR="$(cd "$(dirname "$0")/ha-addon" && pwd)"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${IMAGE:-ghcr.io/mplinuxgeek/mediastat-addon}"
VERSION="$(grep '^version:' "${ADDON_DIR}/config.yaml" | awk '{print $2}' | tr -d '"')"

# Architectures → HA base image mapping (from build.yaml)
declare -A BASE_IMAGES=(
    [amd64]="ghcr.io/home-assistant/amd64-base-debian:bookworm"
    [aarch64]="ghcr.io/home-assistant/aarch64-base-debian:bookworm"
    [armv7]="ghcr.io/home-assistant/armv7-base-debian:bookworm"
)

# ── Argument parsing ──────────────────────────────────────────────────────────
PUSH=false
TARGET_ARCH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --push)        PUSH=true ;;
        --arch)        TARGET_ARCH="$2"; shift ;;
        --tag|--image) IMAGE="$2"; shift ;;
        -h|--help)
            sed -n '2,14p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# ── Copy app sources into addon dir for the Docker build context ──────────────
echo "==> Syncing app sources into ha-addon/"
cp "${PROJECT_DIR}/main.py" "${ADDON_DIR}/main.py"
cp "${PROJECT_DIR}/requirements.txt" "${ADDON_DIR}/requirements.txt"
rm -rf "${ADDON_DIR}/templates"
cp -r "${PROJECT_DIR}/templates" "${ADDON_DIR}/templates"

# ── Determine which arches to build ──────────────────────────────────────────
if [[ -n "${TARGET_ARCH}" ]]; then
    ARCHES=("${TARGET_ARCH}")
elif [[ "${PUSH}" == "true" ]]; then
    ARCHES=("${!BASE_IMAGES[@]}")
else
    # Local build: detect host arch and map to HA arch name
    HOST_ARCH="$(uname -m)"
    case "${HOST_ARCH}" in
        x86_64)  ARCHES=(amd64) ;;
        aarch64) ARCHES=(aarch64) ;;
        armv7l)  ARCHES=(armv7) ;;
        *)
            echo "Unknown host arch '${HOST_ARCH}', defaulting to amd64"
            ARCHES=(amd64) ;;
    esac
fi

echo "==> Building version ${VERSION} for: ${ARCHES[*]}"
echo "==> Image: ${IMAGE}"
echo

# ── Build ─────────────────────────────────────────────────────────────────────
BUILT_TAGS=()
for ARCH in "${ARCHES[@]}"; do
    BASE="${BASE_IMAGES[${ARCH}]:-}"
    if [[ -z "${BASE}" ]]; then
        echo "ERROR: No base image configured for arch '${ARCH}'" >&2
        exit 1
    fi

    TAG="${IMAGE}:${VERSION}-${ARCH}"
    LATEST_TAG="${IMAGE}:latest-${ARCH}"

    echo "--- Building ${ARCH} (base: ${BASE}) ---"
    docker build \
        --build-arg BUILD_FROM="${BASE}" \
        --platform "linux/${ARCH/aarch64/arm64}" \
        --tag "${TAG}" \
        --tag "${LATEST_TAG}" \
        $( [[ "${PUSH}" == "true" ]] && echo "--push" || echo "--load" ) \
        "${ADDON_DIR}"

    BUILT_TAGS+=("${TAG}")
    echo "    Built: ${TAG}"
    echo
done

# ── Create and push a multi-arch manifest when pushing ───────────────────────
if [[ "${PUSH}" == "true" && "${#BUILT_TAGS[@]}" -gt 1 ]]; then
    echo "==> Creating multi-arch manifest ${IMAGE}:${VERSION}"
    docker manifest create "${IMAGE}:${VERSION}" "${BUILT_TAGS[@]}" --amend
    docker manifest push "${IMAGE}:${VERSION}"
    docker manifest create "${IMAGE}:latest" \
        $(for arch in "${ARCHES[@]}"; do echo "${IMAGE}:latest-${arch}"; done) --amend
    docker manifest push "${IMAGE}:latest"
fi

echo "==> Done."
if [[ "${PUSH}" == "false" ]]; then
    echo
    echo "To test locally, add this to your HA configuration.yaml or use the"
    echo "Samba/SSH add-on to place the 'ha-addon' folder in your addons directory:"
    echo
    echo "  /addons/mediastat/  ← copy ha-addon/ contents here, then"
    echo "  reload add-ons in HA > Settings > Add-ons > ⋮ > Reload"
fi
