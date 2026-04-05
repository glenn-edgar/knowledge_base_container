#!/bin/bash
# start_planner_system.sh -- Start/stop the ROS Planner II system entities.
#
# Each entity is an independent process. The planner does NOT start or stop
# robots or bridges -- this script manages their lifecycle.
#
# Usage:
#   ./start_planner_system.sh start [entity...]   Start entities (default: all)
#   ./start_planner_system.sh stop                 Stop all running entities
#   ./start_planner_system.sh status               Show running entities
#
# Entities:
#   nats_robot    Independent NATS robot (LuaJIT, Pi Zero 2 template)
#   mqtt_robot    Independent MQTT robot (LuaJIT, ESP32/Pico template)
#   mqtt_bridge   NATS<->MQTT bridge (multi-robot, auto-discover)
#
# Environment:
#   ROBOT_ID       Robot ID (default: rover_1)
#   NATS_SERVER    NATS URL (default: nats://127.0.0.1:4222)
#   MQTT_HOST      MQTT broker host (default: localhost)
#   MQTT_PORT      MQTT broker port (default: 1883)
#   VMRT_KB_SITE   KB site name (default: moonbase.alpha.surface_ops)
#
# Config files:
#   NATS robot:  ros_planner_ii_nats_robot/robot.json
#   MQTT robot:  ros_planner_ii_mqtt_robot/mqtt_robot.json

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="/tmp/ros_planner_ii"

NATS_ROBOT_DIR="$SCRIPT_DIR/ros_planner_ii_nats_robot"
MQTT_ROBOT_DIR="$SCRIPT_DIR/ros_planner_ii_mqtt_robot"
MQTT_BRIDGE_DIR="$SCRIPT_DIR/ros_planner_ii_mqtt_bridge"
PLANNER_DIR="$SCRIPT_DIR/ros_planner_ii"

# Shared library paths
RUNTIME_DIR="$PLANNER_DIR/runtime"
HUB_DSL_DIR="$PLANNER_DIR/hub_dsl"
CT_BASE="$SCRIPT_DIR/chain_tree_luajit"
CT_RUNTIME="$CT_BASE/runtime_dict"
CT_JSON="$CT_BASE/lua_dsl/luajit_pipeline"
CT_DSL="$CT_BASE/lua_dsl"
NATS_BASE="$SCRIPT_DIR/knowledge_base/nats"
NATS_LIB="$NATS_BASE/lib"
MQTT_BASE="$SCRIPT_DIR/knowledge_base/mqtt"
MQTT_LIB="$MQTT_BASE/lib"
SQLITE_KB="$SCRIPT_DIR/knowledge_base/sqlite3/construct_kb"
KB_CONSTRUCT="$HUB_DSL_DIR/kb_construct"
LOCAL_PLANNER_DIR="$PLANNER_DIR/local_planner/lib"
ACTION_SERVER_DIR="$PLANNER_DIR/action_server/lib"
GLOBAL_PLANNER_DIR="$PLANNER_DIR/global_planner/lib"
ROBOT_DIR="$PLANNER_DIR/robots/test_robot"

export LUA_PATH="$ACTION_SERVER_DIR/?.lua;$GLOBAL_PLANNER_DIR/?.lua;$LOCAL_PLANNER_DIR/?.lua;$HUB_DSL_DIR/protocol/?.lua;$HUB_DSL_DIR/?.lua;$HUB_DSL_DIR/hub_functions/?.lua;$HUB_DSL_DIR/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME_DIR/?.lua;$ROBOT_DIR/?.lua;$NATS_ROBOT_DIR/?.lua;$MQTT_ROBOT_DIR/?.lua;$MQTT_BRIDGE_DIR/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_LIB/?.lua;$MQTT_BASE/?.lua;$MQTT_LIB/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME_DIR/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

export ROBOT_ID="${ROBOT_ID:-rover_1}"
export NATS_SERVER="${NATS_SERVER:-nats://127.0.0.1:4222}"
export MQTT_HOST="${MQTT_HOST:-localhost}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"

# ---------------------------------------------------------------------------
# PID management
# ---------------------------------------------------------------------------

mkdir -p "$PID_DIR"

save_pid() {
    echo "$2" > "$PID_DIR/$1.pid"
}

read_pid() {
    local pidfile="$PID_DIR/$1.pid"
    if [ -f "$pidfile" ]; then
        cat "$pidfile"
    fi
}

is_running() {
    local pid
    pid=$(read_pid "$1")
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

stop_entity() {
    local name="$1"
    local pid
    pid=$(read_pid "$name")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo "Stopping $name (pid=$pid)"
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null || true
        rm -f "$PID_DIR/$name.pid"
    else
        rm -f "$PID_DIR/$name.pid"
    fi
}

# ---------------------------------------------------------------------------
# Start entities
# ---------------------------------------------------------------------------

start_nats_robot() {
    if is_running nats_robot; then
        echo "nats_robot already running (pid=$(read_pid nats_robot))"
        return
    fi

    local config="${NATS_ROBOT_CONFIG:-$NATS_ROBOT_DIR/robot.json}"
    if [ ! -f "$config" ]; then
        echo "ERROR: NATS robot config not found: $config"
        echo "  Copy robot.json.example to robot.json and edit."
        return 1
    fi

    luajit "$NATS_ROBOT_DIR/robot_main.lua" "$config" &
    save_pid nats_robot $!
    echo "nats_robot started (pid=$!, config=$config)"
}

start_mqtt_robot() {
    if is_running mqtt_robot; then
        echo "mqtt_robot already running (pid=$(read_pid mqtt_robot))"
        return
    fi

    local config="${MQTT_ROBOT_CONFIG:-$MQTT_ROBOT_DIR/mqtt_robot.json}"
    if [ ! -f "$config" ]; then
        echo "ERROR: MQTT robot config not found: $config"
        echo "  Create mqtt_robot.json with robot_id, site, mqtt_host, etc."
        return 1
    fi

    luajit "$MQTT_ROBOT_DIR/mqtt_robot_main.lua" "$config" &
    save_pid mqtt_robot $!
    echo "mqtt_robot started (pid=$!, config=$config)"
}

start_mqtt_bridge() {
    if is_running mqtt_bridge; then
        echo "mqtt_bridge already running (pid=$(read_pid mqtt_bridge))"
        return
    fi

    luajit "$MQTT_BRIDGE_DIR/mqtt_bridge.lua" "$MQTT_HOST" "$MQTT_PORT" &
    save_pid mqtt_bridge $!
    echo "mqtt_bridge started (pid=$!)"
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

do_start() {
    local entities=("$@")
    if [ ${#entities[@]} -eq 0 ]; then
        entities=(mqtt_bridge mqtt_robot nats_robot)
    fi

    for entity in "${entities[@]}"; do
        case "$entity" in
            nats_robot)   start_nats_robot ;;
            mqtt_robot)   start_mqtt_robot ;;
            mqtt_bridge)  start_mqtt_bridge ;;
            *)            echo "Unknown entity: $entity" ;;
        esac
    done
}

do_stop() {
    stop_entity nats_robot
    stop_entity mqtt_robot
    stop_entity mqtt_bridge
}

do_status() {
    for name in nats_robot mqtt_robot mqtt_bridge; do
        if is_running "$name"; then
            echo "$name: running (pid=$(read_pid $name))"
        else
            echo "$name: stopped"
        fi
    done
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${1:-}" in
    start)   shift; do_start "$@" ;;
    stop)    do_stop ;;
    status)  do_status ;;
    *)       echo "Usage: $0 {start [entity...]|stop|status}"
             echo "Entities: nats_robot, mqtt_robot, mqtt_bridge"
             exit 1 ;;
esac
