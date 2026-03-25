ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm
FROM ${BUILD_FROM}

# HandBrake version — resolved at build time from GitHub latest release if not overridden.
ARG HB_VERSION=

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        ffmpeg \
        curl \
        vainfo \
        libva-drm2 \
        libvpl2 \
        # needed by run.sh GPU setup
        util-linux \
    && rm -rf /var/lib/apt/lists/*

# Install official HandBrake CLI binary (includes QSV/NVENC/VCE support).
# If HB_VERSION is empty, resolve the latest release tag from GitHub API.
RUN HB_VER="${HB_VERSION}" ; \
    if [ -z "${HB_VER}" ]; then \
        HB_VER="$(curl -fsSL https://api.github.com/repos/HandBrake/HandBrake/releases/latest \
            | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"; \
        echo "Resolved HandBrake latest release: ${HB_VER}"; \
    fi \
    && curl -fsSL \
        "https://github.com/HandBrake/HandBrake/releases/download/${HB_VER}/HandBrakeCLI-${HB_VER}-x86_64.tar.bz2" \
        -o /tmp/hb.tar.bz2 \
    && tar -xjf /tmp/hb.tar.bz2 -C /usr/local/bin HandBrakeCLI \
    && chmod +x /usr/local/bin/HandBrakeCLI \
    && rm /tmp/hb.tar.bz2

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
