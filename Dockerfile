FROM nvidia/cuda:12.6.0-base-ubuntu24.04

# Build metadata — passed via --build-arg, defaults so a plain `docker build`
# with no args still succeeds. Lets `docker inspect` show what's actually
# running instead of every image looking identical after a few rebuilds.
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}"

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        ffmpeg \
        mkvtoolnix \
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
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY src/ /app/

RUN mkdir -p /data /media

ENV MEDIA_ROOT=/media
ENV DB_PATH=/data/mediastat.db

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz', timeout=3)" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
