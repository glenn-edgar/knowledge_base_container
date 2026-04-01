#!/bin/bash
# run_test.sh -- Run a single virtual planner test
#
# Usage: ./run_test.sh <test_dir> [robot_name]
#        ./run_test.sh dsl_tests/ct_full_simulation
#        ./run_test.sh dsl_tests/ct_full_simulation test_robot

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <test_dir> [robot_name]"
    echo "  e.g.: $0 dsl_tests/ct_full_simulation"
    echo ""
    echo "Available tests:"
    for d in dsl_tests/*/; do
        if [ -f "$d/main.lua" ]; then
            echo "  $d"
        fi
    done
    exit 1
fi

TEST_DIR="$1"
ROBOT="${2:-test_robot}"

if [ ! -f "$TEST_DIR/main.lua" ]; then
    echo "Error: $TEST_DIR/main.lua not found"
    exit 1
fi

# Paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROBOT_DIR="$SCRIPT_DIR/robots/$ROBOT"
CT_RUNTIME="$SCRIPT_DIR/../../chain_tree_luajit/runtime_dict"
CT_JSON="$SCRIPT_DIR/../../chain_tree_luajit/lua_dsl/luajit_pipeline"

# Build LUA_PATH
export LUA_PATH="$SCRIPT_DIR/lib/?.lua;$SCRIPT_DIR/hub_functions/?.lua;$ROBOT_DIR/?.lua;$SCRIPT_DIR/hub_templates/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;?.lua;;"

# Environment vars for test main.lua
export VP_BOARDS="$SCRIPT_DIR/boards/"
export VP_MODELS="$SCRIPT_DIR/remote_models/"
export VP_ROBOT_DIR="$ROBOT_DIR/"

echo "=== Virtual Planner Test: $TEST_DIR (robot: $ROBOT) ==="
echo ""

cd "$TEST_DIR"
luajit main.lua
