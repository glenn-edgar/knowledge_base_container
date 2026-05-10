#!/usr/bin/env bash
# docker_build.sh -- build the nanodatacenter/robot-sim image.
# Requires nanodatacenter/luajit-base:latest.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_TAG="${IMAGE_TAG:-nanodatacenter/robot-sim:latest}"

if ! docker image inspect nanodatacenter/luajit-base:latest >/dev/null 2>&1; then
    echo "ERROR: nanodatacenter/luajit-base:latest not found." >&2
    exit 1
fi

echo "=== Building $IMAGE_TAG ==="
docker build -t "$IMAGE_TAG" "$SCRIPT_DIR"

docker images "$IMAGE_TAG" \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
