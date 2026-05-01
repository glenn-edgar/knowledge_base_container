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

export LUA_PATH="$PLANNER/action_server/lib/?.lua;$PLANNER/global_planner/lib/?.lua;$PLANNER/local_planner/lib/?.lua;$HUB_DSL/protocol/?.lua;$HUB_DSL/?.lua;$HUB_DSL/hub_functions/?.lua;$HUB_DSL/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME/?.lua;$ROBOT_DIR/?.lua;$MQTT_ROBOT/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_BASE/lib/?.lua;$MQTT_BASE/?.lua;$MQTT_BASE/lib/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

export MQTT_HOST="${MQTT_HOST:-localhost}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"

# HAL_MODE selects the robot_hal backend:
#   sim    (default) — direct FFI into libphysics inside this process
#   dongle           — libcomm pty into a robot_sim subprocess running
#                      the four-thread dongle architecture (Tracks A+C,
#                      slices L1..L5)
HAL_MODE="${HAL_MODE:-sim}"
export HAL_MODE

ROBOT_SIM_PID=""

cleanup_robot_sim() {
    if [ -n "$ROBOT_SIM_PID" ]; then
        kill "$ROBOT_SIM_PID" 2>/dev/null || true
        wait "$ROBOT_SIM_PID" 2>/dev/null || true
    fi
}
trap cleanup_robot_sim EXIT INT TERM

if [ "$HAL_MODE" = "dongle" ]; then
    # Dongle mode: spawn robot_sim, capture its pty path, hand it
    # to mqtt_robot_main via ROBOT_SIM_PTY env var.
    DONGLE_TYPE="${DONGLE_TYPE:-1}"          # 1 = DRIVE_BASE
    DONGLE_INSTANCE="${DONGLE_INSTANCE:-1}"
    SLAVE_ADDR="${SLAVE_ADDR:-1}"

    # Build tunables blob from the robot's physics_config.json (Q3
    # mirror — drive_base reads this binary the same way it would
    # read NVS on embedded).
    PHYSICS_CFG="$(dirname "$CONFIG_FILE")/physics_config.json"
    if [ ! -f "$PHYSICS_CFG" ]; then
        PHYSICS_CFG="$MQTT_ROBOT/physics_config.json"
    fi
    TUNABLES_BIN="$(mktemp -t drive_base_tunables.XXXXXX.bin)"
    luajit "$MQTT_ROBOT/build_drive_base_tunables.lua" \
           "$PHYSICS_CFG" "$TUNABLES_BIN"

    # Spawn robot_sim. Its stdout reveals "PTY=/dev/pts/N" which we
    # capture via a coproc, then "READY".
    coproc ROBOT_SIM { "$MQTT_ROBOT/robot_sim/robot_sim" \
                       --type "$DONGLE_TYPE" \
                       --instance "$DONGLE_INSTANCE" \
                       --addr "$SLAVE_ADDR" \
                       --tunables "$TUNABLES_BIN" 2>&1 ; }
    ROBOT_SIM_PID="$ROBOT_SIM_PID"

    # Read until we see PTY= line.
    PTY_PATH=""
    while IFS= read -r line <&"${ROBOT_SIM[0]}"; do
        case "$line" in
            PTY=*)   PTY_PATH="${line#PTY=}" ;;
            READY)   break ;;
            *)       echo "[robot_sim] $line" ;;
        esac
    done

    if [ -z "$PTY_PATH" ]; then
        echo "Error: robot_sim did not publish PTY path"
        exit 1
    fi

    export ROBOT_SIM_PTY="$PTY_PATH"
    echo "Starting MQTT robot: $CONFIG_FILE (HAL_MODE=dongle, pty=$PTY_PATH)"
else
    echo "Starting MQTT robot: $CONFIG_FILE (HAL_MODE=$HAL_MODE)"
fi

# Cannot exec — we need the trap to clean up robot_sim. Run in the
# foreground and wait. SIGTERM/SIGINT to start_robot.sh will fire the
# trap.
luajit "$MQTT_ROBOT/mqtt_robot_main.lua" "$CONFIG_FILE"
ROBOT_RC=$?
exit $ROBOT_RC
