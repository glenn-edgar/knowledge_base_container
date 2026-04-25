#!/usr/bin/env bash
# docker_build.sh -- Build the docker-host-broker image.
#
# Unlike luajit_base / openresty_base, this container has no host-side
# staging step: the multi-stage Dockerfile pulls Go deps and compiles
# inside the build itself. Run from this directory:
#
#   ./docker_build.sh
#
# Output: docker image nanodatacenter/docker-host-broker:latest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Building docker-host-broker image ==="
docker build -t nanodatacenter/docker-host-broker:latest "$SCRIPT_DIR"

docker images nanodatacenter/docker-host-broker:latest \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
