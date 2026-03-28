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

TMDB_KEY=$(bashio::config 'tmdb_api_key' 2>/dev/null || echo "")
if [[ -n "${TMDB_KEY}" ]]; then
    bashio::log.info "TMDB API key: configured"
    printf 'tmdb_api_key: "%s"\n' "${TMDB_KEY}" >> "${CONFIG_FILE}"
else
    bashio::log.info "TMDB API key: not set"
fi

# Use the first directory's path as MEDIA_ROOT (the app's default browse root)
MEDIA_ROOT_VAL=$(bashio::config 'directories[0].path')
MEDIA_ROOT_VAL="${MEDIA_ROOT_VAL:-/media}"

# ── GPU / hardware acceleration setup ────────────────────────────────────────
# Force Intel iHD VA-API driver so the correct driver is used on Gen9–Gen12+.
# Without this, libva may auto-select i965 (which doesn't support Gen12) and
# VAAPI initialisation fails even when the hardware and libraries are present.
export LIBVA_DRIVER_NAME=iHD

# Intel QSV and AMD VA-API both expose render nodes under /dev/dri.
# We use setpriv (util-linux) to start uvicorn with the render group added to
# its supplementary groups — usermod alone doesn't affect the running process.
RENDER_GID=""
RENDER_GROUP=""
if ls /dev/dri/renderD* >/dev/null 2>&1; then
    RENDER_DEV="$(ls /dev/dri/renderD* 2>/dev/null | sort | head -1)"
    RENDER_GID="$(stat -c '%g' "${RENDER_DEV}" 2>/dev/null || echo '')"
    if [[ -n "${RENDER_GID}" ]]; then
        if ! getent group "${RENDER_GID}" >/dev/null 2>&1; then
            groupadd -g "${RENDER_GID}" render_host
        fi
        RENDER_GROUP="$(getent group "${RENDER_GID}" | cut -d: -f1)"
        bashio::log.info "GPU: /dev/dri present (${RENDER_DEV}), render GID=${RENDER_GID} (${RENDER_GROUP})"
    fi
    # Report what vainfo sees (informational — failure is non-fatal)
    LIBVA_DRIVER_NAME=iHD vainfo 2>&1 | grep -E 'VA-API|vainfo|driver|version' | head -5 \
        | while IFS= read -r line; do bashio::log.info "vainfo: ${line}"; done || true
else
    bashio::log.info "GPU: /dev/dri not found — software encoding only"
    bashio::log.info "     To enable hardware encoding, uncomment 'devices: [/dev/dri]'"
    bashio::log.info "     in ha-addon/config.yaml and rebuild."
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

UVICORN_ARGS=(
    main:app
    --host 0.0.0.0
    --port 8080
    --loop asyncio
    --log-level "${LOG_LEVEL}"
    --proxy-headers
    --forwarded-allow-ips "*"
)

# If a render group was detected, use setpriv to add the render GID to the
# process's supplementary groups. usermod -aG is not enough because it only
# modifies /etc/group — the running process inherits its groups at exec time.
if [[ -n "${RENDER_GID}" ]]; then
    # Current supplementary groups + render GID (avoid duplicates)
    CURRENT_GROUPS="$(id -G | tr ' ' ',')"
    exec setpriv --inh-caps=-all --groups "${CURRENT_GROUPS},${RENDER_GID}" \
        /app/.venv/bin/uvicorn "${UVICORN_ARGS[@]}"
else
    exec /app/.venv/bin/uvicorn "${UVICORN_ARGS[@]}"
fi
