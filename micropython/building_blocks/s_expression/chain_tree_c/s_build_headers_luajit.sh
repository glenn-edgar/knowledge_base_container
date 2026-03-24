#!/bin/bash
# s_build_headers_luajit.sh - Run ChainTree header generation via LuaJIT
#
# Usage:
#   ./s_build_headers_luajit.sh <input_file> <output_dir> [handle_name] [--no-support]
#
# Example:
#   ./s_build_headers_luajit.sh config.json ./generated my_chaintree

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="${SCRIPT_DIR}/lua_dsl/luajit_pipeline"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_file> <output_dir> [handle_name] [--no-support]"
    exit 1
fi

luajit "${PIPELINE_DIR}/main.lua" "$@"