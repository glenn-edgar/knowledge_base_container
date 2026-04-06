#!/bin/bash
# run_tests.sh -- Run all ros_planner_ii tests
#
# Usage: ./run_tests.sh                    (mqtt_direct — default)
#        ./run_tests.sh all                (planner + mqtt_direct + hub_rt + sequencer + action)
#        ./run_tests.sh mqtt_direct        (MQTT direct: planner → MQTT → robot, primary)
#        ./run_tests.sh hub_rt             (hub_runtime module — MQTT transport)
#        ./run_tests.sh sequencer          (sequencer — MQTT transport)
#        ./run_tests.sh action             (action server — MQTT transport)
#        ./run_tests.sh planner            (global planner — no transport)
#        ./run_tests.sh kv_writer          (KV writer unit test)
#        ./run_tests.sh link_manager       (link manager unit test)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/.."
RUNTIME_DIR="$ROOT_DIR/runtime"
HUB_DSL_DIR="$ROOT_DIR/hub_dsl"
ROBOT_DIR="$ROOT_DIR/../ros_planner_ii_mqtt_robot"
CT_BASE="$ROOT_DIR/../chain_tree_luajit"
CT_RUNTIME="$CT_BASE/runtime_dict"
CT_JSON="$CT_BASE/lua_dsl/luajit_pipeline"
CT_DSL="$CT_BASE/lua_dsl"

# NATS library paths
NATS_BASE="$ROOT_DIR/../knowledge_base/nats"
NATS_LIB="$NATS_BASE/lib"

# MQTT library paths
MQTT_BASE="$ROOT_DIR/../knowledge_base/mqtt"
MQTT_LIB="$MQTT_BASE/lib"
MQTT_ROBOT_DIR="$ROOT_DIR/../ros_planner_ii_mqtt_robot"

# SQLite KB paths
SQLITE_KB="$ROOT_DIR/../knowledge_base/sqlite3/construct_kb"
KB_CONSTRUCT="$HUB_DSL_DIR/kb_construct"

# LUA_PATH: include NATS and MQTT libs
LOCAL_PLANNER_DIR="$ROOT_DIR/local_planner/lib"
ACTION_SERVER_DIR="$ROOT_DIR/action_server/lib"
GLOBAL_PLANNER_DIR="$ROOT_DIR/global_planner/lib"
export LUA_PATH="$ACTION_SERVER_DIR/?.lua;$GLOBAL_PLANNER_DIR/?.lua;$LOCAL_PLANNER_DIR/?.lua;$HUB_DSL_DIR/protocol/?.lua;$HUB_DSL_DIR/?.lua;$HUB_DSL_DIR/hub_functions/?.lua;$HUB_DSL_DIR/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME_DIR/?.lua;$ROBOT_DIR/?.lua;$MQTT_ROBOT_DIR/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_LIB/?.lua;$MQTT_BASE/?.lua;$MQTT_LIB/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME_DIR/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"

export ROBOT_ID="${ROBOT_ID:-rover_1}"
export NATS_SERVER="${NATS_SERVER:-nats://127.0.0.1:4222}"
export MQTT_HOST="${MQTT_HOST:-localhost}"
export MQTT_PORT="${MQTT_PORT:-1883}"

# Build KB and robot DSL (hub.json no longer needed — state machine hub_runtime)
build_all() {
    cd "$KB_CONSTRUCT"
    rm -f surface_ops.db
    luajit -e "arg={'surface_ops.db'}; dofile('construct_surface_ops.lua')" 2>&1 | tail -3

    (cd "$ROBOT_DIR" && rm -f remote.json remote_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
    echo ""
}
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

# Helper: start MQTT robot subprocess
# Usage: start_mqtt_robot [no_db] [wire_format]
#   no_db: skip VMRT_KB_DB to avoid SQLite lock contention when hub also writes
#   wire_format: "json" (default) or "cbor"
start_mqtt_robot() {
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"
    export MQTT_HOST="${MQTT_HOST:-localhost}"
    export MQTT_PORT="${MQTT_PORT:-1883}"
    export VMRT_WIRE_FORMAT="${2:-json}"
    if [ "${1:-}" != "no_db" ]; then
        export VMRT_KB_DB="$KB_CONSTRUCT/surface_ops.db"
    else
        unset VMRT_KB_DB
    fi
    luajit "$MQTT_ROBOT_DIR/remote_mqtt_ct.lua" "$ROBOT_ID" &
    MQTT_ROBOT_PID=$!
    echo "MQTT robot started (pid=$MQTT_ROBOT_PID, wire=$VMRT_WIRE_FORMAT)"
}

# Helper: cleanup subprocess
cleanup_pid() {
    local PID=$1
    sleep 0.5
    if kill -0 $PID 2>/dev/null; then
        kill $PID 2>/dev/null
        wait $PID 2>/dev/null || true
    else
        wait $PID 2>/dev/null || true
    fi
}

# Helper: send shutdown via MQTT (direct to robot, no bridge)
send_mqtt_shutdown() {
    luajit -e "
        local mqtt_hub = require('mqtt_hub_transport')
        local json = require('json_util')
        local hub = mqtt_hub.new('${MQTT_HOST:-localhost}', ${MQTT_PORT:-1883}, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        hub:connect()
        hub:send_rpc('$ROBOT_ID', json.encode({packet_type=255, seq=99}))
        local ffi = require('ffi')
        ffi.cdef[[ int usleep(unsigned int usec); ]]
        ffi.C.usleep(200000)
        hub:close()
    " 2>/dev/null
}

run_mqtt_direct() {
    echo "=== MQTT Direct Integration Test (primary) ==="
    build_all

    cd "$SCRIPT_DIR"
    start_mqtt_robot no_db

    sleep 2
    luajit test_mqtt_direct.lua
    local TEST_RC=$?

    cleanup_pid $MQTT_ROBOT_PID
    echo ""
    return $TEST_RC
}

run_mqtt_cbor() {
    echo "=== MQTT CBOR Integration Test ==="
    build_all

    cd "$SCRIPT_DIR"
    start_mqtt_robot no_db cbor

    sleep 2
    luajit test_mqtt_cbor.lua
    local TEST_RC=$?

    cleanup_pid $MQTT_ROBOT_PID
    echo ""
    return $TEST_RC
}

run_action() {
    echo "=== Action Server Test (MQTT) ==="
    build_all

    cd "$SCRIPT_DIR"
    start_mqtt_robot no_db

    sleep 2
    luajit test_action_server.lua
    local TEST_RC=$?

    # Send shutdown via MQTT
    send_mqtt_shutdown
    sleep 1

    cleanup_pid $MQTT_ROBOT_PID
    echo ""
    return $TEST_RC
}

run_planner() {
    echo "=== Global Planner Test ==="
    build_all
    cd "$SCRIPT_DIR"
    luajit test_global_planner.lua
    local TEST_RC=$?

    echo ""
    return $TEST_RC
}

run_sequencer() {
    echo "=== Sequencer Test (MQTT) ==="
    build_all

    cd "$SCRIPT_DIR"
    start_mqtt_robot no_db

    sleep 2
    luajit test_sequencer.lua
    local TEST_RC=$?

    cleanup_pid $MQTT_ROBOT_PID
    echo ""
    return $TEST_RC
}

case "${1:-mqtt_direct}" in
    mqtt_direct)        run_mqtt_direct ;;
    mqtt_cbor)          run_mqtt_cbor ;;
    planner)            run_planner ;;
    sequencer)          run_sequencer ;;
    action)             run_action ;;
    kv_writer)          echo "=== KV Writer Unit Test ==="; cd "$SCRIPT_DIR"; luajit test_kv_writer.lua ;;
    link_manager)       echo "=== Link Manager Unit Test ==="; cd "$SCRIPT_DIR"; luajit test_link_manager.lua ;;
    link_client)        echo "=== Link Client Unit Test ==="; cd "$SCRIPT_DIR"; luajit test_link_client.lua ;;
    all)                run_planner; run_mqtt_direct; run_mqtt_cbor; run_sequencer; run_action ;;
    *)                  echo "Usage: $0 [mqtt_direct|mqtt_cbor|hub_rt|sequencer|planner|action|kv_writer|link_manager|link_client|all]"; exit 1 ;;
esac
