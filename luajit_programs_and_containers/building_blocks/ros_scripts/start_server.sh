#!/bin/bash
# start_server.sh -- Start the planner action server (persistent).
#
# Usage: ./start_server.sh [--rebuild]
#
# The server:
#   1. Builds KB + DSL artifacts if --rebuild or missing
#   2. Exports KB to NATS KV
#   3. Connects MQTT hub transport
#   4. Runs action_server in persistent mode (drains NATS job queue)
#
# Submit missions via: ./submit_mission.sh <mission.json>
#
# Environment:
#   NATS_SERVER   (default: nats://127.0.0.1:4222)
#   MQTT_HOST     (default: localhost)
#   MQTT_PORT     (default: 1883)
#   VMRT_KB_SITE  (default: moonbase.alpha.surface_ops)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BB_DIR="$SCRIPT_DIR/.."

# Module paths
PLANNER="$BB_DIR/ros_planner_ii"
RUNTIME="$PLANNER/runtime"
ROBOT_DIR="$BB_DIR/ros_planner_ii_mqtt_robot"
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

export LUA_PATH="$PLANNER/action_server/lib/?.lua;$PLANNER/global_planner/lib/?.lua;$PLANNER/local_planner/lib/?.lua;$HUB_DSL/protocol/?.lua;$HUB_DSL/?.lua;$HUB_DSL/hub_functions/?.lua;$HUB_DSL/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME/?.lua;$ROBOT_DIR/?.lua;$MQTT_ROBOT/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_BASE/lib/?.lua;$MQTT_BASE/?.lua;$MQTT_BASE/lib/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

export NATS_SERVER="${NATS_SERVER:-nats://127.0.0.1:4222}"
export MQTT_HOST="${MQTT_HOST:-localhost}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"

# Build KB + DSL if requested or missing
if [ "${1:-}" = "--rebuild" ] || [ ! -f "$KB_CONSTRUCT/surface_ops.db" ]; then
    echo "=== Building KB ==="
    (cd "$KB_CONSTRUCT" && rm -f surface_ops.db && \
        luajit -e "arg={'surface_ops.db'}; dofile('construct_surface_ops.lua')" 2>&1 | tail -3)
    echo ""
fi

if [ "${1:-}" = "--rebuild" ] || [ ! -f "$HUB_DSL/hub.json" ]; then
    echo "=== Building Hub DSL ==="
    (cd "$HUB_DSL" && rm -f hub.json hub_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
    echo ""
fi

if [ "${1:-}" = "--rebuild" ] || [ ! -f "$ROBOT_DIR/remote.json" ]; then
    echo "=== Building Robot DSL ==="
    (cd "$ROBOT_DIR" && rm -f remote.json remote_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
    echo ""
fi

# Return to script dir after builds
cd "$SCRIPT_DIR"

# Export KB to NATS KV
echo "=== Exporting KB to NATS ==="
luajit -e "
    local kb_exporter = require('kb_exporter')
    local stats = kb_exporter.export({
        db_file     = '$KB_CONSTRUCT/surface_ops.db',
        nats_server = '$NATS_SERVER',
        bucket      = 'kb_export',
    })
    print(string.format('Exported: %d keys', stats.keys_written or 0))
"
echo ""

echo "=== Starting Planner Server ==="
echo "  NATS: $NATS_SERVER"
echo "  MQTT: $MQTT_HOST:$MQTT_PORT"
echo "  Site: $VMRT_KB_SITE"
echo ""
echo "Submit missions via: ./submit_mission.sh <mission.json>"
echo "Press Ctrl+C to stop."
echo ""

exec luajit "$SCRIPT_DIR/planner_server.lua"
