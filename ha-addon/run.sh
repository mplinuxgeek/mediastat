#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
# MediaStat — Home Assistant add-on entrypoint

# ── Read options from HA UI ───────────────────────────────────────────────────
LOG_LEVEL=$(bashio::config 'log_level')
LOG_LEVEL="${LOG_LEVEL:-info}"

bashio::log.info "MediaStat starting"
bashio::log.info "Log level  : ${LOG_LEVEL}"

# ── Write /data/config.yaml from HA options on every start ───────────────────
# This keeps the app config in sync with whatever the user set in the HA UI.
CONFIG_FILE="/data/config.yaml"
bashio::log.info "Writing ${CONFIG_FILE} from add-on options"

printf 'directories:\n' > "${CONFIG_FILE}"
DIR_COUNT=$(bashio::config 'directories | length')
for i in $(seq 0 $((DIR_COUNT - 1))); do
    LABEL=$(bashio::config "directories[${i}].label")
    PATH_VAL=$(bashio::config "directories[${i}].path")
    bashio::log.info "  Directory: ${LABEL} → ${PATH_VAL}"
    printf '  - label: "%s"\n    path: "%s"\n' "${LABEL}" "${PATH_VAL}" >> "${CONFIG_FILE}"
done

# Use the first directory's path as MEDIA_ROOT (the app's default browse root)
MEDIA_ROOT_VAL=$(bashio::config 'directories[0].path')
MEDIA_ROOT_VAL="${MEDIA_ROOT_VAL:-/media}"

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
export MEDIA_ROOT="${MEDIA_ROOT_VAL}"
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
