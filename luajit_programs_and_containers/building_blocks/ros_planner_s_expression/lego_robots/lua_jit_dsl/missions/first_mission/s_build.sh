#!/bin/bash
#-----------------------------------------------------------------------
# s_build.sh — Build script for first_mission
#
# Compiles the mission DSL into all output artifacts:
#   plan_expanded.txt, mission_tree.lua, robot_kb.sqlite, bindings.json
#
# Run from this directory:
#   ./s_build.sh [--plan] [--tree] [--kb] [--bindings] [--all]
#
# Default with no flags: --all
#-----------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLANNER_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

COMPILER="$PLANNER_ROOT/lua_dsl/compile_mission.lua"
EQUIPMENT="$PLANNER_ROOT/lego_robots/lua_jit_dsl/equipment_dsl.lua"
MAP="$PLANNER_ROOT/lego_robots/lua_jit_dsl/map_dsl.lua"
MISSION="$SCRIPT_DIR/mission_dsl.lua"
OUTPUT_DIR="$SCRIPT_DIR"

# Verify files exist
for f in "$COMPILER" "$EQUIPMENT" "$MAP" "$MISSION"; do
    if [ ! -f "$f" ]; then
        echo "ERROR: missing file: $f" >&2
        exit 1
    fi
done

echo "Building first_mission..."
echo "  compiler:  $COMPILER"
echo "  equipment: $EQUIPMENT"
echo "  map:       $MAP"
echo "  mission:   $MISSION"
echo "  output:    $OUTPUT_DIR"
echo ""

luajit "$COMPILER" "$EQUIPMENT" "$MAP" "$MISSION" "$OUTPUT_DIR" "$@"
