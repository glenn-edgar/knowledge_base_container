#!/bin/bash
# build_trees.sh -- Compile hub and remote DSL files to JSON IR
#
# Usage: ./build_trees.sh [robot_name]
#
# Default robot: test_robot
#
# Compiles:
#   robots/<robot>/hub_dsl.lua    -> robots/<robot>/hub.json + hub_debug.yaml
#   robots/<robot>/remote_dsl.lua -> robots/<robot>/remote.json + remote_debug.yaml

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CT_DSL="$SCRIPT_DIR/../../chain_tree_luajit/lua_dsl"

ROBOT="${1:-test_robot}"
ROBOT_DIR="$SCRIPT_DIR/robots/$ROBOT"

if [ ! -d "$ROBOT_DIR" ]; then
    echo "Error: robot directory not found: $ROBOT_DIR"
    echo "Available robots:"
    for d in "$SCRIPT_DIR/robots"/*/; do
        echo "  $(basename "$d")"
    done
    exit 1
fi

export LUA_PATH="$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$SCRIPT_DIR/lib/?.lua;?.lua;;"

echo "=== Building Hub Trees ($ROBOT) ==="
cd "$ROBOT_DIR"
luajit hub_dsl.lua hub.json
echo ""

echo "=== Building Remote Trees ($ROBOT) ==="
cd "$ROBOT_DIR"
luajit remote_dsl.lua remote.json
echo ""

echo "Done."
