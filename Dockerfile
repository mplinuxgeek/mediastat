FROM nvidia/cuda:12.6.0-base-ubuntu24.04

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

COPY main.py .
COPY cropdetect_utils.py .
COPY encode_output_resolution.py .
COPY encode_stream_selection.py .
COPY encode_estimate.py .
COPY templates/ templates/
COPY static/ static/

RUN mkdir -p /data /media

ENV MEDIA_ROOT=/media
ENV DB_PATH=/data/mediastat.db

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
