#!/usr/bin/env bash
# docker_build.sh -- stage lunar_rover-class sources, then docker build
# mycorp/lunar_rover-class:1.0.
#
# After the dongle_base refactor (2026-05-02), this image only ships
# class-specific bits:
#   - robot_sim main.c + dongle_threads.c + drive_base_robot.c (linked
#     against dongle_base's libcomm.a + libphysics.a)
#   - tunables.bin (from physics_config.json at build time)
#   - comm_manifest.bin (per-class wire catalogue)
#   - remote.json (mission state machine)
#   - capabilities.lua, class_processes.json, config.template.json
#
# Generic infrastructure (libcomm/libphysics, mqtt_robot_main + deps,
# chain_tree dict-runtime, mqtt_pubsub) lives in dongle_base.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROVER_SRC="$(cd "$SCRIPT_DIR/../.." && pwd)"

if ! docker image inspect mycorp/dongle_base:1.0 >/dev/null 2>&1; then
    echo "ERROR: mycorp/dongle_base:1.0 not present locally." >&2
    echo "Build it first: containers/dongle_base/docker_build.sh" >&2
    exit 1
fi

STAGE="$SCRIPT_DIR/stage"
echo "=== Staging lunar_rover-class sources into $STAGE ==="
rm -rf "$STAGE"
mkdir -p "$STAGE/libcomm" "$STAGE/robot_sim"

# Class-specific C: drive_base impl + sim host. main.c will move
# up to dongle_base when the vtable abstraction lands; for now it's
# tied to drive_base_robot's symbols and stays here.
cp "$ROVER_SRC/robot_sim/main.c"            "$STAGE/robot_sim/"
cp "$ROVER_SRC/robot_sim/dongle_threads.c"  "$STAGE/robot_sim/"
cp "$ROVER_SRC/robot_sim/dongle_skeleton.h" "$STAGE/robot_sim/"
cp "$ROVER_SRC/libcomm/drive_base_robot.c"  "$STAGE/libcomm/"
cp "$ROVER_SRC/libcomm/drive_base_robot.h"  "$STAGE/libcomm/"

# Pre-staged manifest binary + ffi loader (drive-base wire catalogue).
cp "$ROVER_SRC/comm_manifest.bin"     "$STAGE/"
cp "$ROVER_SRC/comm_manifest_ffi.lua" "$STAGE/"

# Mission state machine (compiled chain-tree IR) + scenario configs.
cp "$ROVER_SRC/remote.json"          "$STAGE/"
cp "$ROVER_SRC/physics_config.json"  "$STAGE/"
cp "$ROVER_SRC/sim_map.json"         "$STAGE/"
cp "$ROVER_SRC/build_drive_base_tunables.lua" "$STAGE/"

# Class-side Lua (drive-base capabilities only; everything else lives
# in dongle_base).
cp "$ROVER_SRC/capabilities.lua"     "$STAGE/"

# Container build inputs
cp "$SCRIPT_DIR/Makefile.container"   "$STAGE/Makefile"
cp "$SCRIPT_DIR/config.template.json" "$STAGE/"
cp "$SCRIPT_DIR/class_processes.json" "$STAGE/"

echo "  staged $(find "$STAGE" -name '*.lua' | wc -l) Lua files"
echo "  staged $(find "$STAGE" -name '*.c'   | wc -l) C sources"

echo ""
echo "=== Building mycorp/lunar_rover-class:1.0 ==="
docker build -t mycorp/lunar_rover-class:1.0 \
             -t mycorp/lunar_rover-class:latest \
             "$SCRIPT_DIR"

docker images mycorp/lunar_rover-class:1.0 \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
