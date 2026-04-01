#!/bin/bash
# build.sh -- Compile test_robot remote DSL to JSON IR
#
# Usage: ./build.sh
# Output: remote.json + remote_debug.yaml

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CT_DSL="$SCRIPT_DIR/../../../chain_tree_luajit/lua_dsl"

export LUA_PATH="$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;?.lua;;"

echo "=== Building Remote DSL (test_robot) ==="
cd "$SCRIPT_DIR"
luajit remote_dsl.lua remote.json
echo ""
echo "Done."
