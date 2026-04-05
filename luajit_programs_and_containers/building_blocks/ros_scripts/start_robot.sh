#!/bin/bash
# start_robot.sh -- Start an MQTT robot process.
#
# Usage: ./start_robot.sh <config.json>
#
# Config JSON fields:
#   robot_id      (required)  Robot identifier
#   site          (required)  KB site namespace
#   remote_json   (required)  Path to ChainTree remote JSON
#   mqtt_host     (optional)  MQTT broker host (default: localhost)
#   mqtt_port     (optional)  MQTT broker port (default: 1883)
#   robot_class   (optional)  Robot class name
#   energy_max    (optional)  Max energy (default: 10000)
#   wire_format   (optional)  "json" or "cbor" (default: json)
#   capabilities  (optional)  Array of supported virtual nodes

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <config.json>"
    exit 1
fi

CONFIG_FILE="$1"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: config file not found: $CONFIG_FILE"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BB_DIR="$SCRIPT_DIR/.."

# Module paths
PLANNER="$BB_DIR/ros_planner_ii"
RUNTIME="$PLANNER/runtime"
ROBOT_DIR="$PLANNER/robots/test_robot"
HUB_DSL="$PLANNER/hub_dsl"
KB_CONSTRUCT="$HUB_DSL/kb_construct"
CT_BASE="$BB_DIR/chain_tree_luajit"
CT_RUNTIME="$CT_BASE/runtime_dict"
CT_JSON="$CT_BASE/lua_dsl/luajit_pipeline"
CT_DSL="$CT_BASE/lua_dsl"
NATS_BASE="$BB_DIR/knowledge_base/nats"
MQTT_BASE="$BB_DIR/knowledge_base/mqtt"
MQTT_ROBOT="$BB_DIR/ros_planner_ii_mqtt_robot"
SQLITE_KB="$BB_DIR/knowledge_base/sqlite3/construct_kb"

export LUA_PATH="$PLANNER/action_server/lib/?.lua;$PLANNER/global_planner/lib/?.lua;$PLANNER/local_planner/lib/?.lua;$HUB_DSL/protocol/?.lua;$HUB_DSL/?.lua;$HUB_DSL/hub_functions/?.lua;$HUB_DSL/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME/?.lua;$ROBOT_DIR/?.lua;$MQTT_ROBOT/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_BASE/lib/?.lua;$MQTT_BASE/?.lua;$MQTT_BASE/lib/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

export MQTT_HOST="${MQTT_HOST:-localhost}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"

echo "Starting MQTT robot: $CONFIG_FILE"
exec luajit "$MQTT_ROBOT/mqtt_robot_main.lua" "$CONFIG_FILE"
