ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm
FROM ${BUILD_FROM}

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        ffmpeg \
        handbrake-cli \
        vainfo \
        libva-drm2 \
        # needed by run.sh GPU setup
        util-linux \
    && rm -rf /var/lib/apt/lists/*

# Install VA-API drivers (Intel/AMD) — amd64 only; silently skipped on arm
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        intel-media-va-driver-non-free \
        i965-va-driver-shaders \
        mesa-va-drivers \
    ; apt-get clean && rm -rf /var/lib/apt/lists/* ; true

# Ensure the standard DRM groups exist so run.sh can add the process to them
RUN groupadd -f -g 44 video 2>/dev/null || true \
    && groupadd -f -g 109 render 2>/dev/null || true

WORKDIR /app

# Install Python dependencies into a venv so they don't clash with system Python
COPY requirements.txt .
RUN python3 -m venv /app/.venv \
    && /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY templates/ templates/

# Ensure persistent data directory exists
RUN mkdir -p /data

# run.sh is the entrypoint for HA add-ons
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
