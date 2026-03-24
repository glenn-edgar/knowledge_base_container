#!/bin/bash
# s_build_json.sh - Run a ChainTree LuaJIT DSL test and generate JSON output
#
# Usage: ./s_build_json.sh <lua_test_file> <output_directory>
#
# Example: ./s_build_json.sh dsl_tests/basic_test/basic_test.lua dsl_tests/basic_test

if [ $# -ne 2 ]; then
    echo "Usage: $0 <lua_test_file> <output_directory>"
    echo "  lua_test_file   : path to the .lua test file"
    echo "  output_directory: directory for the .json output"
    exit 1
fi

LUA_TEST_FILE="$1"
OUTPUT_DIR="$2"

if [ ! -f "$LUA_TEST_FILE" ]; then
    echo "Error: Lua test file not found: $LUA_TEST_FILE"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

BASENAME=$(basename "$LUA_TEST_FILE" .lua)
JSON_FILE="${OUTPUT_DIR}/${BASENAME}.json"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export LUA_PATH="${SCRIPT_DIR}/lua_dsl/?.lua;${SCRIPT_DIR}/lua_dsl/?/init.lua;${SCRIPT_DIR}/?.lua;${SCRIPT_DIR}/?/init.lua;;"

echo "Running: $LUA_TEST_FILE"
echo "Output:  $JSON_FILE"

luajit "$LUA_TEST_FILE" "$JSON_FILE"
