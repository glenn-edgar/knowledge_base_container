#!/bin/bash
# submit_mission.sh -- Submit a mission and track its progress.
#
# Usage: ./submit_mission.sh <mission.json>
#
# Mission JSON format:
#   {
#     "robot_id": "rover_1",
#     "board": "landing_zone",
#     "start": "lander_pad",
#     "stops": [
#       {"node": "mining_zone_b", "action": "deliver_part",
#        "params": {"arm_target": -45, "arm_speed": 80, "payload_type": 1}},
#       {"node": "lander_pad"}
#     ],
#     "bookend": true
#   }
#
# The client submits to the NATS job queue, then polls KV for
# status updates and the final result.
#
# Environment:
#   NATS_SERVER   (default: nats://127.0.0.1:4222)
#   VMRT_KB_SITE  (default: moonbase.alpha.surface_ops)

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <mission.json>"
    exit 1
fi

MISSION_FILE="$1"
if [ ! -f "$MISSION_FILE" ]; then
    echo "Error: mission file not found: $MISSION_FILE"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BB_DIR="$SCRIPT_DIR/.."

# Module paths
PLANNER="$BB_DIR/ros_planner_ii"
RUNTIME="$PLANNER/runtime"
HUB_DSL="$PLANNER/hub_dsl"
KB_CONSTRUCT="$HUB_DSL/kb_construct"
CT_BASE="$BB_DIR/chain_tree_luajit"
NATS_BASE="$BB_DIR/knowledge_base/nats"
MQTT_BASE="$BB_DIR/knowledge_base/mqtt"
SQLITE_KB="$BB_DIR/knowledge_base/sqlite3/construct_kb"

export LUA_PATH="$PLANNER/action_server/lib/?.lua;$PLANNER/global_planner/lib/?.lua;$PLANNER/local_planner/lib/?.lua;$HUB_DSL/protocol/?.lua;$HUB_DSL/?.lua;$HUB_DSL/hub_functions/?.lua;$HUB_DSL/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME/?.lua;$CT_BASE/lua_dsl/luajit_pipeline/?.lua;$CT_BASE/lua_dsl/?.lua;$CT_BASE/lua_dsl/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_BASE/lib/?.lua;$MQTT_BASE/?.lua;$MQTT_BASE/lib/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

export NATS_SERVER="${NATS_SERVER:-nats://127.0.0.1:4222}"
export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"

exec luajit "$SCRIPT_DIR/mission_client.lua" "$(cd "$(dirname "$MISSION_FILE")" && pwd)/$(basename "$MISSION_FILE")"
