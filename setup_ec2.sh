#!/usr/bin/env bash
# Install OS packages and Python deps for bunny_hls_merge_tool on EC2 / Ubuntu.
# Run from this directory:  chmod +x setup_ec2.sh && ./setup_ec2.sh

set -euo pipefail
cd "$(dirname "$0")"

echo "==> Detecting OS..."
if command -v apt-get >/dev/null 2>&1; then
  echo "==> apt: installing python3, venv, pip, ffmpeg..."
  sudo apt-get update -qq
  sudo apt-get install -y python3 python3-venv python3-pip ffmpeg curl ca-certificates
elif command -v yum >/dev/null 2>&1; then
  echo "==> yum: installing python3, pip, ffmpeg (Amazon Linux / RHEL — adjust if ffmpeg unavailable)..."
  sudo yum install -y python3 python3-pip || true
  if ! command -v ffmpeg >/dev/null 2>&1; then
    echo "WARN: ffmpeg not in default repos. Install RPM Fusion / static build, then re-run."
  fi
else
  echo "WARN: Unknown package manager. Install Python 3.9+, pip, and ffmpeg manually."
fi

echo "==> Python venv + aiohttp (required by bunny_stream_hls_merge_to_mp4.py)..."
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install "aiohttp>=3.9.0" "boto3>=1.34.0"

echo "==> ffmpeg version:"
ffmpeg -version | head -1 || true

echo "==> Done. Next:"
echo "    1. cp .env.example .env   # set APIKEY, ORGID, Edmingle base, drm_migration_s3_*"
echo "    2. DRM_MIGRATION_MERGE_METHOD=hls|zip (default hls); Bunny IDs come from worker/next, not BUNNY_STREAM_*"
echo "    3. ./.venv/bin/python3 drm_hls_migration_worker.py"
echo "       or: ./.venv/bin/python3 run_merge.py | run_zip_merge.py | run_hls_merge.py"
