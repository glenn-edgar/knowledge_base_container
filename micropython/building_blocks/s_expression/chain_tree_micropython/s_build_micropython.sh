#!/bin/bash
# s_build_micropython.sh - Generate MicroPython module from ChainTree JSON
#
# Usage:
#   ./s_build_micropython.sh <input.json> <output_dir> [handle_name]
#
# Example:
#   ./s_build_micropython.sh config.json ./generated my_chaintree

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="${SCRIPT_DIR}/lua_dsl/luajit_pipeline"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input.json> <output_dir> [handle_name]"
    exit 1
fi

luajit "${PIPELINE_DIR}/main_micropython.lua" "$@"
