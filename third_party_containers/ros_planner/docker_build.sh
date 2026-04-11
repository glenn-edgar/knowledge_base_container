#!/bin/bash
# docker_build.sh -- Build the ros-planner container image.
#
# Stages prebuilt .so files and Lua modules into the build context,
# then runs docker build. Fast — no compilation inside the container.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BB_DIR="$SCRIPT_DIR/../../luajit_programs_and_containers/building_blocks"

if [ ! -d "$BB_DIR/ros_planner_ii" ]; then
    echo "ERROR: building_blocks directory not found at: $BB_DIR"
    exit 1
fi

echo "=== Building runtime KB ==="

KB_CONSTRUCT="$BB_DIR/ros_planner_ii/hub_dsl/kb_construct"
RUNTIME_DB="$KB_CONSTRUCT/surface_ops.db"
CT_DSL="$BB_DIR/chain_tree_luajit/lua_dsl"
SQLITE_KB="$BB_DIR/knowledge_base/sqlite3/construct_kb"

# Always rebuild to ensure the image gets a fresh KB
rm -f "$RUNTIME_DB"
(
    cd "$KB_CONSTRUCT"
    LUA_PATH="$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;?.lua;;" \
    luajit -e "arg={'surface_ops.db'}; dofile('construct_surface_ops.lua')"
) 2>&1 | tail -3

if [ ! -f "$RUNTIME_DB" ]; then
    echo "ERROR: Runtime KB build failed: $RUNTIME_DB not created"
    exit 1
fi

# Verify the KB has boards
BOARD_COUNT=$(sqlite3 "$RUNTIME_DB" "SELECT COUNT(*) FROM knowledge_base WHERE path LIKE '%boards%'" 2>/dev/null || echo "0")
if [ "$BOARD_COUNT" -eq 0 ]; then
    echo "ERROR: Runtime KB has no boards"
    exit 1
fi
echo "  Runtime KB: $RUNTIME_DB ($BOARD_COUNT board entries)"

# Copy to Sqlite_Data so the mounted volume has the same DB (best-effort;
# the orchestrator's KB build handles this authoritatively)
SQLITE_DATA="${SQLITE_DATA:-/home/gedgar/Sqlite_Data}"
mkdir -p "$SQLITE_DATA" 2>/dev/null || true
cp "$RUNTIME_DB" "$SQLITE_DATA/surface_ops.db" 2>/dev/null \
    && echo "  Copied to: $SQLITE_DATA/surface_ops.db" \
    || echo "  (skipped Sqlite_Data copy — file owned by root, orchestrator handles this)"

echo ""
echo "=== Staging Lua modules ==="

# Clean previous staging
rm -rf "$SCRIPT_DIR/lua"
mkdir -p "$SCRIPT_DIR/lua"

# ros_planner_ii — planner modules
cp -r "$BB_DIR/ros_planner_ii/action_server/lib"   "$SCRIPT_DIR/lua/action_server"
cp -r "$BB_DIR/ros_planner_ii/global_planner/lib"  "$SCRIPT_DIR/lua/global_planner"
cp -r "$BB_DIR/ros_planner_ii/local_planner/lib"   "$SCRIPT_DIR/lua/local_planner"
cp -r "$BB_DIR/ros_planner_ii/hub_dsl/protocol"    "$SCRIPT_DIR/lua/protocol"
cp -r "$BB_DIR/ros_planner_ii/hub_dsl/hub_functions" "$SCRIPT_DIR/lua/hub_functions"
cp -r "$BB_DIR/ros_planner_ii/hub_dsl/kb_construct" "$SCRIPT_DIR/lua/kb_construct"
cp -r "$BB_DIR/ros_planner_ii/runtime"             "$SCRIPT_DIR/lua/runtime"

# MQTT robot (self-contained)
cp -r "$BB_DIR/ros_planner_ii_mqtt_robot"          "$SCRIPT_DIR/lua/mqtt_robot"

# ChainTree runtime (needed by robot side only, but container may run robot too)
cp -r "$BB_DIR/chain_tree_luajit/runtime_dict"     "$SCRIPT_DIR/lua/ct_runtime"
cp -r "$BB_DIR/chain_tree_luajit/lua_dsl/luajit_pipeline" "$SCRIPT_DIR/lua/ct_json"
cp -r "$BB_DIR/chain_tree_luajit/lua_dsl"          "$SCRIPT_DIR/lua/ct_dsl"
cp -r "$BB_DIR/chain_tree_luajit/lua_dsl/lua_support" "$SCRIPT_DIR/lua/lua_support"

# Knowledge base Lua wrappers (keep lib/ subdir for require("lib.xxx"))
mkdir -p "$SCRIPT_DIR/lua/nats" "$SCRIPT_DIR/lua/mqtt"
cp -r "$BB_DIR/knowledge_base/nats/lib"            "$SCRIPT_DIR/lua/nats/lib"
cp -r "$BB_DIR/knowledge_base/mqtt/lib"            "$SCRIPT_DIR/lua/mqtt/lib"
cp -r "$BB_DIR/knowledge_base/sqlite3/construct_kb" "$SCRIPT_DIR/lua/sqlite_kb"

# Entrypoint + server
cp "$BB_DIR/ros_scripts/planner_server.lua"        "$SCRIPT_DIR/lua/planner_server.lua"
cp "$BB_DIR/ros_scripts/bootstrap_container.lua"   "$SCRIPT_DIR/lua/bootstrap_container.lua"

echo "  Staged $(find "$SCRIPT_DIR/lua" -name '*.lua' | wc -l) Lua files"

echo ""
echo "=== Building Docker image ==="
docker build \
    -t nanodatacenter/ros-planner:latest \
    "$SCRIPT_DIR"

echo ""
echo "=== Build complete ==="
docker images nanodatacenter/ros-planner:latest --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
