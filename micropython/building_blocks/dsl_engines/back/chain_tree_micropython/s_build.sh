#!/bin/bash
#============================================================================
# s_build.sh - Combined ChainTree MicroPython build script
#
# Two-stage pipeline:
#   Stage 1: Lua DSL -> JSON IR  (via luajit + chain_tree_master.lua)
#   Stage 2: JSON IR -> _module_mpy.py  (via luajit + main_micropython.lua)
#
# Usage: ./s_build.sh <lua_test_file> <output_dir> [handle_name]
#
# Example:
#   ./s_build.sh dsl_tests/basic_test/basic_test.lua dsl_tests/basic_test/ basic_test
#============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="${SCRIPT_DIR}/lua_dsl/luajit_pipeline"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <lua_test_file> <output_dir> [handle_name]"
    echo ""
    echo "  lua_test_file : path to the .lua DSL file"
    echo "  output_dir    : directory for generated _module_mpy.py"
    echo "  handle_name   : name prefix for output (default: derived from filename)"
    exit 1
fi

LUA_TEST_FILE="$1"
OUTPUT_DIR="$2"

if [ ! -f "$LUA_TEST_FILE" ]; then
    echo "Error: Lua test file not found: $LUA_TEST_FILE"
    exit 1
fi

BASENAME=$(basename "$LUA_TEST_FILE" .lua)
HANDLE_NAME="${3:-$BASENAME}"

mkdir -p "$OUTPUT_DIR"

# Resolve to absolute paths
LUA_TEST_FILE="$(cd "$(dirname "$LUA_TEST_FILE")" && pwd)/$(basename "$LUA_TEST_FILE")"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

JSON_FILE="${OUTPUT_DIR}/${BASENAME}.json"

echo "============================================================"
echo "ChainTree MicroPython Build"
echo "============================================================"
echo "  DSL source:   $LUA_TEST_FILE"
echo "  JSON IR:      $JSON_FILE"
echo "  Output dir:   $OUTPUT_DIR"
echo "  Handle name:  $HANDLE_NAME"
echo ""

# Stage 1: Lua DSL -> JSON
echo "--- Stage 1: Lua DSL -> JSON ---"
export LUA_PATH="${SCRIPT_DIR}/lua_dsl/?.lua;${SCRIPT_DIR}/lua_dsl/?/init.lua;${SCRIPT_DIR}/?.lua;${SCRIPT_DIR}/?/init.lua;;"
luajit "$LUA_TEST_FILE" "$JSON_FILE"
echo ""

# Stage 2: JSON -> MicroPython module
echo "--- Stage 2: JSON -> MicroPython module ---"
luajit "${PIPELINE_DIR}/main_micropython.lua" "$JSON_FILE" "$OUTPUT_DIR" "$HANDLE_NAME"
