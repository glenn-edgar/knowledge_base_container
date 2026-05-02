#!/usr/bin/env bash
# docker_build.sh -- stage generic infrastructure into dongle_base.
#
# What dongle_base is: USB protocol + internal software bus + virtual-
# robot framework + master-side Lua. Consumed by class images that
# implement specific logical_robot types (drive_base, arm, lidar pod...).
#
# What stays in classes (NOT here):
#   - logical_robot impls (e.g. drive_base_robot.c)
#   - robot_sim main.c + dongle_threads.c (still hosts class plugin
#     directly today; abstraction-via-vtable is a later refactor)
#   - per-class manifest entries -> comm_manifest.bin
#   - physics_config.json values -> tunables.bin
#   - mission state machine -> remote.json
#   - capabilities, class_processes, config.template

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROVER_SRC="$(cd "$SCRIPT_DIR/../.." && pwd)"           # ros_planner_ii_mqtt_robot/
BB_ROOT="$(cd "$ROVER_SRC/.." && pwd)"                 # building_blocks/

if ! docker image inspect mycorp/robot_base:1.0 >/dev/null 2>&1; then
    echo "ERROR: mycorp/robot_base:1.0 not present locally." >&2
    echo "Build it first: containers/robot_base/docker_build.sh" >&2
    exit 1
fi

STAGE="$SCRIPT_DIR/stage"
echo "=== Staging dongle_base sources into $STAGE ==="
rm -rf "$STAGE"
mkdir -p "$STAGE/libcomm" "$STAGE/lua_modules" \
         "$STAGE/native_libs" "$STAGE/test_harness"

# C build inputs (libcomm + physics_core; NO robot_sim main here).
cp "$ROVER_SRC/libcomm/"*.c "$STAGE/libcomm/"
cp "$ROVER_SRC/libcomm/"*.h "$STAGE/libcomm/"
cp "$ROVER_SRC/physics_core.c" "$STAGE/"
cp "$ROVER_SRC/physics_pipe.h" "$STAGE/"

# Pre-staged generated header (committed in the source tree). The
# .bin lives with the CLASS, not the base, but the C code references
# the .h-shaped catalogue of opcode/struct layouts.
cp "$ROVER_SRC/comm_manifest.h"     "$STAGE/"
cp "$ROVER_SRC/comm_manifest_bin.h" "$STAGE/"

# Generic Lua (master-side; class-agnostic).
for f in comm_ffi.lua dongle_hal.lua physics_ffi.lua ct_comm.lua \
         drive_base_ffi.lua \
         mqtt_robot_main.lua mqtt_robot_config.lua robot_hal.lua \
         robot_controller.lua remote_user_functions.lua; do
    cp "$ROVER_SRC/$f" "$STAGE/lua_modules/"
done

# json_util (chain_tree DSL pipeline) + chain_tree dict-runtime
cp "$BB_ROOT/chain_tree_luajit/lua_dsl/luajit_pipeline/json_util.lua" \
   "$STAGE/lua_modules/"
mkdir -p "$STAGE/lua_modules/runtime_dict"
cp "$BB_ROOT/chain_tree_luajit/runtime_dict/"*.lua \
   "$STAGE/lua_modules/runtime_dict/"

# ros_planner_ii master-side helpers
for f in ct_loader_pure.lua fn_registry.lua link_client.lua mqtt_transport.lua; do
    cp "$BB_ROOT/ros_planner_ii/runtime/$f" "$STAGE/lua_modules/"
done
cp "$BB_ROOT/ros_planner_ii/hub_dsl/protocol/command_packets.lua" \
   "$STAGE/lua_modules/"

# mqtt_pubsub Lua + native lib (libmqtt_pubsub links libmosquitto1)
mkdir -p "$STAGE/lua_modules/lib"
cp "$BB_ROOT/knowledge_base/mqtt/lib/mqtt_pubsub.lua" \
   "$STAGE/lua_modules/lib/"
cp "$BB_ROOT/knowledge_base/mqtt/libmqtt_pubsub.so" \
   "$STAGE/native_libs/"

# Test harness (mission-side peer + scenario runner). Lives with the
# base so any class image inherits a runnable harness. The peer is
# the reusable building block; mock_planner + random_paths are the
# legacy entry points that both delegate to it.
cp "$ROVER_SRC/planner_test_peer.lua"  "$STAGE/test_harness/"
cp "$ROVER_SRC/test_mock_planner.lua"  "$STAGE/test_harness/"
cp "$ROVER_SRC/test_random_paths.lua"  "$STAGE/test_harness/"
# Phase-2 controller contract fixture (pure Lua; usable from inside or
# outside the container — see docs/controller/contract.md).
cp "$ROVER_SRC/robot_controller_test_peer.lua"     "$STAGE/test_harness/"
cp "$ROVER_SRC/test_robot_controller_contract.lua" "$STAGE/test_harness/"

# Container build inputs
cp "$SCRIPT_DIR/Makefile.container" "$STAGE/Makefile"

echo "  staged $(find "$STAGE" -name '*.lua' | wc -l) Lua files"
echo "  staged $(find "$STAGE" -name '*.c'   | wc -l) C sources"

echo ""
echo "=== Building mycorp/dongle_base:1.0 ==="
docker build -t mycorp/dongle_base:1.0 \
             -t mycorp/dongle_base:latest \
             "$SCRIPT_DIR"

docker images mycorp/dongle_base:1.0 \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
