#!/bin/bash
# st_build.sh - Scan Tree build pipeline
#
# Usage: ./st_build.sh <dsl_source.lua> <output_dir>
#
# Steps:
#   1. Run the DSL script to produce JSON intermediate
#   2. Run the code generator to produce .h and .c const data tables
#
# Prerequisites:
#   - luajit in PATH
#   - fnv1a.so built (gcc -shared -fPIC -o scan_tree_dsl/fnv1a.so scan_tree_dsl/fnv1a.c)

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <dsl_source.lua> <output_dir>"
    exit 1
fi

DSL_SOURCE="$1"
OUTPUT_DIR="$2"

# Resolve script directory (where st_build.sh lives = project root)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DSL_DIR="$SCRIPT_DIR/scan_tree_dsl"

# Validate inputs
if [ ! -f "$DSL_SOURCE" ]; then
    echo "Error: DSL source not found: $DSL_SOURCE"
    exit 1
fi

if [ ! -f "$DSL_DIR/scan_tree_dsl.lua" ]; then
    echo "Error: scan_tree_dsl.lua not found in $DSL_DIR"
    exit 1
fi

if [ ! -f "$DSL_DIR/codegen_c.lua" ]; then
    echo "Error: codegen_c.lua not found in $DSL_DIR"
    exit 1
fi

# Build fnv1a.so if missing
if [ ! -f "$DSL_DIR/fnv1a.so" ]; then
    echo "Building fnv1a.so..."
    gcc -shared -fPIC -o "$DSL_DIR/fnv1a.so" "$DSL_DIR/fnv1a.c"
fi

mkdir -p "$OUTPUT_DIR"

# Derive JSON intermediate path
JSON_FILE="$OUTPUT_DIR/$(basename "${DSL_SOURCE%.lua}").json"

# Step 1: Run DSL to produce JSON
echo "=== DSL: $DSL_SOURCE ==="
LUA_PATH="$DSL_DIR/?.lua;;" luajit "$DSL_SOURCE" "$JSON_FILE"

# Step 2: Run code generator
echo "=== Codegen: $JSON_FILE -> $OUTPUT_DIR ==="
luajit "$DSL_DIR/codegen_c.lua" "$JSON_FILE" "$OUTPUT_DIR"

echo "=== Done ==="

