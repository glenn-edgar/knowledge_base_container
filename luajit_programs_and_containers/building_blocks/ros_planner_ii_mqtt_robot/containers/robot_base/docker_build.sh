#!/usr/bin/env bash
# docker_build.sh -- build mycorp/robot_base:1.0 (and :latest).
#
# Self-contained: just docker build the supervisor tree. The DSL JSON
# is compiled inside the image (no host-side luajit/chain_tree dep).
#
# Prereq: nanodatacenter/luajit-base:latest must already be built locally
#   (see nano_data_center_base/luajit/luajit_base/container/docker_build.sh)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! docker image inspect nanodatacenter/luajit-base:latest >/dev/null 2>&1; then
    echo "ERROR: nanodatacenter/luajit-base:latest not present locally." >&2
    echo "Build it first via:" >&2
    echo "  cd <repo>/nano_data_center_base/luajit/luajit_base/container && ./docker_build.sh" >&2
    exit 1
fi

echo "=== Building mycorp/robot_base:1.0 ==="
docker build -t mycorp/robot_base:1.0 -t mycorp/robot_base:latest "$SCRIPT_DIR"

docker images mycorp/robot_base:1.0 \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
