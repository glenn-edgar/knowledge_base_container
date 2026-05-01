#!/usr/bin/env bash
# docker_build.sh -- build the nanodatacenter/observability image.
# Requires nanodatacenter/openresty-base:latest (which requires luajit-base).
#
# Stages DCS runtime libs (kb_log, kb_rule, kb_exception, posix_time,
# pg_connector) into _staged_lib/ so the analyzer processes can require
# them. These are added after luajit_base was built, so they're not yet
# in the base image layer; staging here keeps the container self-contained
# without forcing a luajit_base rebuild on every edit to a DCS lib.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_TAG="${IMAGE_TAG:-nanodatacenter/observability:latest}"

for base in nanodatacenter/luajit-base:latest nanodatacenter/openresty-base:latest; do
    if ! docker image inspect "$base" >/dev/null 2>&1; then
        echo "ERROR: $base not found. Build bases first." >&2
        exit 1
    fi
done

# --- Stage DCS runtime libs into the build context ---
# NDC_BASE = nano_data_center_base/. This script lives at
# <NDC_BASE>/platform_containers/observability/container/, climb 3 levels.
NDC_BASE="${NDC_BASE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
DCS_RT="$NDC_BASE/commissioning_software/system_node_control/runtime/dcs_host"

STAGE="$SCRIPT_DIR/_staged_lib"
echo "=== Staging DCS runtime libs from $DCS_RT ==="
rm -rf "$STAGE"
mkdir -p "$STAGE"
for f in kb_log.lua kb_rule.lua kb_exception.lua posix_time.lua pg_connector.lua; do
    src="$DCS_RT/$f"
    [[ -r "$src" ]] || { echo "ERROR: missing $src" >&2; exit 1; }
    cp "$src" "$STAGE/"
done

echo "=== Building $IMAGE_TAG ==="
docker build -t "$IMAGE_TAG" "$SCRIPT_DIR"

docker images "$IMAGE_TAG" \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
