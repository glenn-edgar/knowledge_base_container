#!/bin/bash
# s_build_binary.sh - Generate ChainTree binary image via LuaJIT
#
# Usage:
#   ./s_build_binary.sh <input_file> <output_dir> [handle_name]
#
# Example:
#   ./s_build_binary.sh config.json ./generated my_chaintree
#
# Outputs:
#   {handle_name}.ctb        - Binary image for mmap/load
#   {handle_name}_image.h    - C array for firmware embedding
#
# Requires:
#   - luajit
#   - libfnv1a.so (built from lua_dsl/binary_image/c_lib/)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_DIR="${SCRIPT_DIR}/lua_dsl/binary_image/luajit"
PIPELINE_DIR="${SCRIPT_DIR}/lua_dsl/luajit_pipeline"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_file> <output_dir> [handle_name]"
    exit 1
fi

# Add pipeline directory to Lua path so stage1-5 modules are found
export LUA_PATH="${PIPELINE_DIR}/?.lua;${BINARY_DIR}/?.lua;${LUA_PATH:-;}"

# Add c_lib to library path for libfnv1a.so
C_LIB_DIR="${SCRIPT_DIR}/lua_dsl/binary_image/c_lib"
export LD_LIBRARY_PATH="${C_LIB_DIR}:${LD_LIBRARY_PATH:-}"

luajit "${BINARY_DIR}/main_binary.lua" "$@" --c-header