#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
# MediaStat — Home Assistant add-on entrypoint

# ── Read options from HA UI ───────────────────────────────────────────────────
MEDIA_PATH=$(bashio::config 'media_path')
LOG_LEVEL=$(bashio::config 'log_level')

# Fall back to /media (the standard HA media directory) if not set
MEDIA_PATH="${MEDIA_PATH:-/media}"
LOG_LEVEL="${LOG_LEVEL:-info}"

bashio::log.info "MediaStat starting"
bashio::log.info "Media path : ${MEDIA_PATH}"
bashio::log.info "Log level  : ${LOG_LEVEL}"

# ── Bootstrap /data/config.yaml if it doesn't exist yet ─────────────────────
CONFIG_FILE="/data/config.yaml"
if [ ! -f "${CONFIG_FILE}" ]; then
    bashio::log.info "Creating default ${CONFIG_FILE}"
    cat > "${CONFIG_FILE}" <<EOF
# MediaStat directory configuration
# Add or remove entries to customise your library.
# Paths must be accessible inside the container.
# The HA media directory is mounted at /media.
# The HA share directory is mounted at /share (read-only).
#
# Example:
# directories:
#   - label: Movies
#     path: /media/movies
#   - label: TV Shows
#     path: /media/tv

directories:
  - label: Media
    path: ${MEDIA_PATH}
EOF
fi

# ── GPU / hardware acceleration setup ────────────────────────────────────────
# Intel QSV and AMD VA-API both expose render nodes under /dev/dri.
# The GID of renderD128 on the host often differs from the 'video' group inside
# the container, so we re-map it at runtime to avoid "permission denied" errors.
if ls /dev/dri/renderD* >/dev/null 2>&1; then
    RENDER_GID="$(stat -c '%g' /dev/dri/renderD128 2>/dev/null || echo '')"
    if [[ -n "${RENDER_GID}" ]]; then
        # Ensure a group with the right GID exists and add root to it
        if ! getent group "${RENDER_GID}" >/dev/null 2>&1; then
            groupadd -g "${RENDER_GID}" render_host
        fi
        RENDER_GROUP="$(getent group "${RENDER_GID}" | cut -d: -f1)"
        usermod -aG "${RENDER_GROUP}" root 2>/dev/null || true
        bashio::log.info "GPU: /dev/dri present, render GID=${RENDER_GID} (group: ${RENDER_GROUP})"
    fi
    # Report what vainfo sees (informational — failure is non-fatal)
    vainfo 2>&1 | grep -E 'VA-API|vainfo|driver' | head -5 \
        | while IFS= read -r line; do bashio::log.info "vainfo: ${line}"; done || true
else
    bashio::log.info "GPU: /dev/dri not found — software encoding only"
    bashio::log.info "     To enable hardware encoding, add 'devices: [/dev/dri]'"
    bashio::log.info "     to ha-addon/config.yaml and rebuild."
fi

# NVIDIA: nvidia-smi must be callable; the host needs nvidia-container-runtime.
if command -v nvidia-smi >/dev/null 2>&1; then
    GPU_NAME="$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 || echo '')"
    if [[ -n "${GPU_NAME}" ]]; then
        bashio::log.info "GPU: NVIDIA detected — ${GPU_NAME}"
    fi
fi

# ── Environment variables for the app ────────────────────────────────────────
export MEDIA_ROOT="${MEDIA_PATH}"
export DB_PATH="/data/mediastat.db"
export CONFIG_PATH="${CONFIG_FILE}"

# ── Start uvicorn ─────────────────────────────────────────────────────────────
cd /app
exec /app/.venv/bin/uvicorn main:app \
    --host 0.0.0.0 \
    --port 8080 \
    --log-level "${LOG_LEVEL}" \
    --proxy-headers \
    --forwarded-allow-ips "*"
