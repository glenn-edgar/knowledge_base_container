#!/bin/bash
# build.sh -- Compile hub DSL to JSON IR
#
# Usage: ./build.sh
# Output: hub.json + hub_debug.yaml

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CT_DSL="$SCRIPT_DIR/../../chain_tree_luajit/lua_dsl"

export LUA_PATH="$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$SCRIPT_DIR/?.lua;$SCRIPT_DIR/protocol/?.lua;$SCRIPT_DIR/kb/?.lua;?.lua;;"

echo "=== Building Hub DSL ==="
cd "$SCRIPT_DIR"
luajit hub_dsl.lua hub.json
echo ""
echo "Done."
