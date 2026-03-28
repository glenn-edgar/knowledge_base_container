#!/bin/bash
# st_build.sh - Scan Tree LuaJIT build pipeline
#
# Usage: ./st_build.sh <dsl_source.lua> <output_dir>
#
# Steps:
#   1. Run the DSL script to produce JSON intermediate
#   2. Run codegen_luajit.lua to produce Lua const descriptor
#
# Prerequisites: luajit in PATH

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <dsl_source.lua> <output_dir>"
    exit 1
fi

DSL_SOURCE="$1"
OUTPUT_DIR="$2"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DSL_DIR="$(cd "$(dirname "$0")/../scan_tree_c/scan_tree_dsl" && pwd)"

if [ ! -f "$DSL_SOURCE" ]; then
    echo "Error: DSL source not found: $DSL_SOURCE"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

JSON_FILE="$OUTPUT_DIR/$(basename "${DSL_SOURCE%.lua}").json"

# Step 1: Run DSL to produce JSON (reuses scan_tree_c DSL)
echo "=== DSL: $DSL_SOURCE ==="
LUA_PATH="$DSL_DIR/?.lua;;" luajit "$DSL_SOURCE" "$JSON_FILE"

# Step 2: Run LuaJIT code generator
echo "=== Codegen: $JSON_FILE -> $OUTPUT_DIR ==="
luajit "$SCRIPT_DIR/codegen_luajit.lua" "$JSON_FILE" "$OUTPUT_DIR"

echo "=== Done ==="
