#!/bin/bash
# run_planner.sh -- Run the global planner only
#
# Usage: ./run_planner.sh [board_dir] [strategy_file]
#
# Defaults:
#   board_dir:     boards/workshop_floor
#   strategy_file: strategy_full.lua
#
# Output: global_plan.json and global_plan.yaml in the board directory.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CT_JSON="$SCRIPT_DIR/../../chain_tree_luajit/lua_dsl/luajit_pipeline"

BOARD_DIR="${1:-boards/workshop_floor}"
STRATEGY="${2:-strategy_full.lua}"

BOARD_FILE="$BOARD_DIR/board.lua"
STRATEGY_FILE="$BOARD_DIR/$STRATEGY"

if [ ! -f "$BOARD_FILE" ]; then
    echo "Error: board file not found: $BOARD_FILE"
    echo ""
    echo "Usage: $0 [board_dir] [strategy_file]"
    echo ""
    echo "Available boards:"
    for d in boards/*/; do
        if [ -f "$d/board.lua" ]; then
            echo "  $d"
            for s in "$d"strategy_*.lua; do
                [ -f "$s" ] && echo "    $(basename $s)"
            done
        fi
    done
    exit 1
fi

if [ ! -f "$STRATEGY_FILE" ]; then
    echo "Error: strategy file not found: $STRATEGY_FILE"
    exit 1
fi

export LUA_PATH="$SCRIPT_DIR/lib/?.lua;$CT_JSON/?.lua;?.lua;;"

echo "=== Global Planner ==="
echo "Board:    $BOARD_FILE"
echo "Strategy: $STRATEGY_FILE"
echo ""

cd "$SCRIPT_DIR"
luajit -e "
local global_planner = require('global_planner')
local vn       = dofile('$BOARD_FILE')
local strategy = dofile('$STRATEGY_FILE')
local plan     = global_planner.plan(strategy, vn)
global_planner.print_plan(plan)
print()
global_planner.write_json(plan, '$BOARD_DIR/global_plan.json')
global_planner.write_yaml(plan, '$BOARD_DIR/global_plan.yaml')
"
