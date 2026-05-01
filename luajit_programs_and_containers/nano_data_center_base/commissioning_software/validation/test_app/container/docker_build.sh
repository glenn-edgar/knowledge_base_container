#!/usr/bin/env bash
# docker_build.sh -- build the nanodatacenter/test-app image. Requires
# nanodatacenter/openresty-base:latest (which requires luajit-base).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_TAG="${IMAGE_TAG:-nanodatacenter/test-app:latest}"

for base in nanodatacenter/luajit-base:latest nanodatacenter/openresty-base:latest; do
    if ! docker image inspect "$base" >/dev/null 2>&1; then
        echo "ERROR: $base not found." >&2
        exit 1
    fi
done

echo "=== Building $IMAGE_TAG ==="
docker build -t "$IMAGE_TAG" "$SCRIPT_DIR"

docker images "$IMAGE_TAG" \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
