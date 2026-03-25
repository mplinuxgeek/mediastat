FROM python:3.12-slim

# Enable contrib and non-free repos for Intel VA-API drivers
RUN sed -i 's/ main$/ main contrib non-free non-free-firmware/' \
        /etc/apt/sources.list /etc/apt/sources.list.d/*.list 2>/dev/null || true

RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        handbrake-cli \
        # Intel QSV: iHD driver (Gen8+) and i965 driver (older)
        intel-media-va-driver-non-free \
        i965-va-driver-shaders \
        # AMD VCE: Mesa VA-API
        mesa-va-drivers \
        # VA-API runtime libs
        libva-drm2 \
        vainfo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY templates/ templates/

RUN mkdir -p /data /media

ENV MEDIA_ROOT=/media
ENV DB_PATH=/data/mediastat.db

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
