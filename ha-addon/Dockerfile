ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:trixie
FROM ${BUILD_FROM}

# The HA base image only has 'main' in its apt sources.
# non-free-firmware is required for intel-media-va-driver-non-free (QSV/VA-API).
RUN sed -i 's/^Components: main$/Components: main non-free non-free-firmware/' \
        /etc/apt/sources.list.d/debian.sources

# ffmpeg handles all encoding (hevc_qsv via Intel oneVPL, hevc_vaapi fallback,
# or libx265 for software).
# libmfx-gen1  — oneVPL GPU runtime for Gen 12+ (Alder Lake / Raptor Lake)
# libmfx1      — legacy Media SDK runtime for Gen 6–11 (Skylake–Rocket Lake)
# libvpl2      — oneVPL dispatcher that links ffmpeg to whichever runtime is present
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        ffmpeg \
        vainfo \
        libva-drm2 \
        libvpl2 \
        libmfx-gen1.2 \
        intel-media-va-driver-non-free \
        i965-va-driver-shaders \
        mesa-va-drivers \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

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
COPY ha-addon/run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
