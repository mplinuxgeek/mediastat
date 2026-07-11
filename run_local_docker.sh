#!/bin/bash
sudo docker run -d --gpus all \
  -p 8080:8080 \
  -v "$HOME/Videos:/media" \
  -v mediastat_data:/data \
  mediastat
