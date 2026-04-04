#!/bin/bash
# test_c_robot.sh -- End-to-end test: C robot + LuaJIT bridge + planner
#
# Uses the existing Planner II test infrastructure:
#   - Builds KB and ChainTree images
#   - Exports KB to NATS KV
#   - Starts C robot (CBOR wire format)
#   - Starts LuaJIT MQTT bridge
#   - Runs test_mqtt_robot.lua (mission via action server)
#   - Shuts down and reports results
#
# Prerequisites: mosquitto on localhost:1883, NATS on localhost:4222
#
# Usage: ./test_c_robot.sh [json|cbor]

set -e

WIRE_FORMAT="${1:-cbor}"

# --- Paths ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
C_ROBOT_BIN="$SCRIPT_DIR/robot_main"

# Planner II root (LuaJIT side) — use absolute path
PLANNER_ROOT="/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii"
TESTS_DIR="$PLANNER_ROOT/tests"
ROOT_DIR="$PLANNER_ROOT"
RUNTIME_DIR="$ROOT_DIR/runtime"
HUB_DSL_DIR="$ROOT_DIR/hub_dsl"
ROBOT_DIR="$ROOT_DIR/robots/test_robot"
CT_BASE="$ROOT_DIR/../chain_tree_luajit"
CT_RUNTIME="$CT_BASE/runtime_dict"
CT_JSON="$CT_BASE/lua_dsl/luajit_pipeline"
CT_DSL="$CT_BASE/lua_dsl"
NATS_BASE="$ROOT_DIR/../knowledge_base/nats"
NATS_LIB="$NATS_BASE/lib"
MQTT_BASE="$ROOT_DIR/../knowledge_base/mqtt"
MQTT_LIB="$MQTT_BASE/lib"
MQTT_ROBOT_DIR="$ROOT_DIR/robots/mqtt_robot"
MQTT_BRIDGE_DIR="$ROOT_DIR/mqtt_bridge"
SQLITE_KB="$ROOT_DIR/../knowledge_base/sqlite3/construct_kb"
KB_CONSTRUCT="$HUB_DSL_DIR/kb_construct"
LOCAL_PLANNER_DIR="$ROOT_DIR/local_planner/lib"
ACTION_SERVER_DIR="$ROOT_DIR/action_server/lib"
GLOBAL_PLANNER_DIR="$ROOT_DIR/global_planner/lib"
ROBOT_PROCESS_DIR="$ROOT_DIR/robots/robot_process"

# --- Environment ---
export ROBOT_ID="${ROBOT_ID:-rover_1}"
export NATS_SERVER="${NATS_SERVER:-nats://127.0.0.1:4222}"
export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"
export LUA_PATH="$ACTION_SERVER_DIR/?.lua;$GLOBAL_PLANNER_DIR/?.lua;$LOCAL_PLANNER_DIR/?.lua;$HUB_DSL_DIR/protocol/?.lua;$HUB_DSL_DIR/?.lua;$HUB_DSL_DIR/hub_functions/?.lua;$HUB_DSL_DIR/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME_DIR/?.lua;$ROBOT_DIR/?.lua;$ROBOT_PROCESS_DIR/?.lua;$MQTT_ROBOT_DIR/?.lua;$MQTT_BRIDGE_DIR/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_LIB/?.lua;$MQTT_BASE/?.lua;$MQTT_LIB/?.lua;$TESTS_DIR/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME_DIR/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"

echo "=== C Robot End-to-End Test (wire_format=$WIRE_FORMAT) ==="
echo ""

# --- 1. Build KB and ChainTree ---
echo "--- Building KB and ChainTree ---"
cd "$KB_CONSTRUCT"
rm -f surface_ops.db
luajit -e "arg={'surface_ops.db'}; dofile('construct_surface_ops.lua')" 2>&1 | tail -3

(cd "$HUB_DSL_DIR" && rm -f hub.json hub_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
(cd "$ROBOT_DIR" && rm -f remote.json remote_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
echo ""

# --- 2. Build C robot ---
echo "--- Building C robot ---"
(cd "$SCRIPT_DIR" && make -s)
echo ""

# --- 3. Build runtime (for socket tests, needed by some libs) ---
(cd "$RUNTIME_DIR" && make -s 2>/dev/null || true)

# --- 4. Export KB to NATS KV ---
echo "--- Exporting KB to NATS KV ---"
cd "$TESTS_DIR"
luajit -e "
    local kb_exporter = require('kb_exporter')
    kb_exporter.export({ db_file='$KB_CONSTRUCT/surface_ops.db', nats_server='$NATS_SERVER', bucket='kb_export' })
    print('KB exported to NATS.')
"

# --- 5. Flush stale NATS queues and status ---
echo "--- Flushing stale queues ---"
luajit -e "
    local site = '$VMRT_KB_SITE'
    local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, site)
    tx:flush()
    tx:close()
    local ks_lib = require('lib.nats_key_store')
    local ks = ks_lib.KeyStore.new({
        server = '$NATS_SERVER',
        bucket = site:gsub('%.', '_') .. '_robot_status',
        create_bucket = true, history = 1,
    })
    ks:connect()
    pcall(function() ks:delete(site .. '.robots.$ROBOT_ID.status.state') end)
    pcall(function() ks:delete(site .. '.robots.$ROBOT_ID.status.energy') end)
    pcall(function() ks:delete(site .. '.robots.$ROBOT_ID.status.bitmask') end)
    ks:disconnect(); ks:destroy()
    print('Queues flushed, stale status cleared.')
"

# --- 5b. Clear stale MQTT retained messages ---
SITE_MQTT=$(echo "$VMRT_KB_SITE" | tr '.' '/')
mosquitto_pub -t "$SITE_MQTT/robots/$ROBOT_ID/status/state" -r -n -q 1 2>/dev/null
mosquitto_pub -t "$SITE_MQTT/robots/$ROBOT_ID/status/energy" -r -n -q 1 2>/dev/null
mosquitto_pub -t "$SITE_MQTT/robots/$ROBOT_ID/status/bitmask" -r -n -q 1 2>/dev/null
echo "  MQTT retained cleared"

# --- 6. Create C robot config ---
ROBOT_CONFIG="/tmp/test_c_robot_$ROBOT_ID.json"
cat > "$ROBOT_CONFIG" <<REOF
{
    "robot_id": "$ROBOT_ID",
    "site": "$VMRT_KB_SITE",
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "robot_class": "lunar_rover",
    "energy_max": 10000,
    "energy_infinite": false,
    "wire_format": "$WIRE_FORMAT"
}
REOF
echo "  Config: $ROBOT_CONFIG (wire_format=$WIRE_FORMAT)"

# --- 7. Start C robot ---
echo ""
echo "--- Starting C robot ---"
ROBOT_LOG="/tmp/c_robot_$$.log"
"$C_ROBOT_BIN" "$ROBOT_CONFIG" > "$ROBOT_LOG" 2>&1 &
C_ROBOT_PID=$!
echo "C robot started (pid=$C_ROBOT_PID)"
sleep 3

# --- 8. Start MQTT bridge ---
echo "--- Starting bridge ---"
cd "$TESTS_DIR"
luajit "$MQTT_BRIDGE_DIR/mqtt_bridge.lua" &
BRIDGE_PID=$!
echo "Bridge started (pid=$BRIDGE_PID)"
sleep 5

# --- 9. Run test ---
echo ""
echo "--- Running test_mqtt_robot.lua ---"
cd "$TESTS_DIR"
luajit test_mqtt_robot.lua
TEST_RC=$?

# --- 10. Send shutdown ---
echo ""
echo "--- Sending shutdown ---"
luajit -e "
    local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '$VMRT_KB_SITE')
    local json = require('json_util')
    tx:send_rpc(json.encode({packet_type=255, seq=99}))
    tx:close()
" 2>/dev/null
sleep 2

# --- 11. Cleanup ---
for PID in $C_ROBOT_PID $BRIDGE_PID; do
    if kill -0 $PID 2>/dev/null; then
        kill $PID 2>/dev/null
        wait $PID 2>/dev/null || true
    else
        wait $PID 2>/dev/null || true
    fi
done

rm -f "$ROBOT_CONFIG"

echo ""
echo "--- Robot Log ---"
cat "$ROBOT_LOG" 2>/dev/null
rm -f "$ROBOT_LOG"

echo ""
if [ $TEST_RC -eq 0 ]; then
    echo "=== C ROBOT TEST PASSED ==="
else
    echo "=== C ROBOT TEST FAILED ==="
fi

exit $TEST_RC
