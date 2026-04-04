#!/bin/bash
# run_tests.sh -- Run all ros_planner_ii tests
#
# Usage: ./run_tests.sh                    (all tests)
#        ./run_tests.sh loopback           (raw packet loopback — socket based)
#        ./run_tests.sh chaintree_loopback (step 1: hub CT + remote loopback — socket)
#        ./run_tests.sh nats               (NATS loopback — no sockets)
#        ./run_tests.sh nats_ct            (NATS + ChainTree — full stack)
#        ./run_tests.sh hub_rt             (hub_runtime module — same route via hub_runtime API)
#        ./run_tests.sh remote             (remote robot — assumes robot already on NATS)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/.."
RUNTIME_DIR="$ROOT_DIR/runtime"
HUB_DSL_DIR="$ROOT_DIR/hub_dsl"
ROBOT_DIR="$ROOT_DIR/robots/test_robot"
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
MQTT_ROBOT_DIR="$ROOT_DIR/robots/mqtt_robot"
MQTT_BRIDGE_DIR="$ROOT_DIR/mqtt_bridge"

# SQLite KB paths
SQLITE_KB="$ROOT_DIR/../knowledge_base/sqlite3/construct_kb"
KB_CONSTRUCT="$HUB_DSL_DIR/kb_construct"

# Build runtime (socket-based, still needed for loopback tests)
echo "=== Building runtime ==="
(cd "$RUNTIME_DIR" && make -s)
echo ""

# LUA_PATH: include NATS libs
LOCAL_PLANNER_DIR="$ROOT_DIR/local_planner/lib"
ACTION_SERVER_DIR="$ROOT_DIR/action_server/lib"
GLOBAL_PLANNER_DIR="$ROOT_DIR/global_planner/lib"
ROBOT_PROCESS_DIR="$ROOT_DIR/robots/robot_process"
export LUA_PATH="$ACTION_SERVER_DIR/?.lua;$GLOBAL_PLANNER_DIR/?.lua;$LOCAL_PLANNER_DIR/?.lua;$HUB_DSL_DIR/protocol/?.lua;$HUB_DSL_DIR/?.lua;$HUB_DSL_DIR/hub_functions/?.lua;$HUB_DSL_DIR/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME_DIR/?.lua;$ROBOT_DIR/?.lua;$ROBOT_PROCESS_DIR/?.lua;$MQTT_ROBOT_DIR/?.lua;$MQTT_BRIDGE_DIR/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_LIB/?.lua;$MQTT_BASE/?.lua;$MQTT_LIB/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME_DIR/?.so;$NATS_BASE/?.so;$MQTT_BASE/?.so;;"

export ROBOT_ID="${ROBOT_ID:-rover_1}"
export NATS_SERVER="${NATS_SERVER:-nats://127.0.0.1:4222}"

# Build KB, then trees (KB must exist before hub_dsl.lua merges VN data)
build_all() {
    cd "$KB_CONSTRUCT"
    rm -f surface_ops.db
    luajit -e "arg={'surface_ops.db'}; dofile('construct_surface_ops.lua')" 2>&1 | tail -3

    (cd "$HUB_DSL_DIR" && rm -f hub.json hub_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
    (cd "$ROBOT_DIR" && rm -f remote.json remote_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
    echo ""
}
export LD_LIBRARY_PATH="$NATS_BASE:$MQTT_BASE:${LD_LIBRARY_PATH:-}"

run_loopback() {
    echo "=== Raw Packet Loopback Test (socket) ==="
    cd "$SCRIPT_DIR"
    luajit test_loopback.lua
    echo ""
}

run_chaintree_loopback() {
    echo "=== ChainTree Loopback Test (socket, step 1) ==="
    (cd "$HUB_DSL_DIR" && rm -f hub.json hub_debug.yaml && chmod +x build.sh && ./build.sh 2>&1 | tail -1)
    echo ""
    cd "$SCRIPT_DIR"
    luajit test_chaintree_loopback.lua
    echo ""
}

run_nats() {
    echo "=== NATS Loopback Test ==="

    # Flush stale queues
    cd "$SCRIPT_DIR"
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('Queues flushed.')
    "

    # Start remote as background process
    luajit "$ROBOT_DIR/remote_nats.lua" "$ROBOT_ID" &
    REMOTE_PID=$!
    echo "Remote started (pid=$REMOTE_PID)"

    # Run hub test
    sleep 1  # let remote connect to NATS
    luajit test_nats_loopback.lua
    local TEST_RC=$?

    # Cleanup: remote should have exited from shutdown command
    # Wait briefly, then force kill if still alive
    sleep 0.5
    if kill -0 $REMOTE_PID 2>/dev/null; then
        kill $REMOTE_PID 2>/dev/null
        wait $REMOTE_PID 2>/dev/null || true
    else
        wait $REMOTE_PID 2>/dev/null || true
    fi

    echo ""
    return $TEST_RC
}

run_nats_ct() {
    echo "=== NATS ChainTree Full Test ==="
    build_all

    # Flush stale NATS queues BEFORE starting any process
    cd "$SCRIPT_DIR"
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('Queues flushed.')
    "

    # Start remote ChainTree process (with KB database)
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    export VMRT_KB_DB="$KB_CONSTRUCT/surface_ops.db"
    export VMRT_KB_SITE="moonbase.alpha.surface_ops"
    luajit "$ROBOT_DIR/remote_nats_ct.lua" "$ROBOT_ID" &
    REMOTE_PID=$!
    echo "Remote CT started (pid=$REMOTE_PID)"

    sleep 1
    luajit test_nats_chaintree.lua
    local TEST_RC=$?

    sleep 0.5
    if kill -0 $REMOTE_PID 2>/dev/null; then
        kill $REMOTE_PID 2>/dev/null
        wait $REMOTE_PID 2>/dev/null || true
    else
        wait $REMOTE_PID 2>/dev/null || true
    fi

    echo ""
    return $TEST_RC
}

run_hub_rt() {
    echo "=== Hub Runtime Module Test ==="
    build_all

    # Flush stale NATS queues
    cd "$SCRIPT_DIR"
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('Queues flushed.')
    "

    # Start remote ChainTree process
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    export VMRT_KB_DB="$KB_CONSTRUCT/surface_ops.db"
    export VMRT_KB_SITE="moonbase.alpha.surface_ops"
    luajit "$ROBOT_DIR/remote_nats_ct.lua" "$ROBOT_ID" &
    REMOTE_PID=$!
    echo "Remote CT started (pid=$REMOTE_PID)"

    sleep 1
    luajit test_hub_runtime.lua
    local TEST_RC=$?

    sleep 0.5
    if kill -0 $REMOTE_PID 2>/dev/null; then
        kill $REMOTE_PID 2>/dev/null
        wait $REMOTE_PID 2>/dev/null || true
    else
        wait $REMOTE_PID 2>/dev/null || true
    fi

    echo ""
    return $TEST_RC
}

run_action() {
    echo "=== Action Server Test ==="
    build_all

    # Flush stale NATS queues
    cd "$SCRIPT_DIR"
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('Queues flushed.')
    "

    # Start remote ChainTree process
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    export VMRT_KB_DB="$KB_CONSTRUCT/surface_ops.db"
    export VMRT_KB_SITE="moonbase.alpha.surface_ops"
    luajit "$ROBOT_DIR/remote_nats_ct.lua" "$ROBOT_ID" &
    REMOTE_PID=$!
    echo "Remote CT started (pid=$REMOTE_PID)"

    sleep 1
    luajit test_action_server.lua
    local TEST_RC=$?

    sleep 0.5
    if kill -0 $REMOTE_PID 2>/dev/null; then
        kill $REMOTE_PID 2>/dev/null
        wait $REMOTE_PID 2>/dev/null || true
    else
        wait $REMOTE_PID 2>/dev/null || true
    fi

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
    echo "=== Sequencer Test ==="
    build_all

    # Flush stale NATS queues
    cd "$SCRIPT_DIR"
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('Queues flushed.')
    "

    # Start remote ChainTree process
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    export VMRT_KB_DB="$KB_CONSTRUCT/surface_ops.db"
    export VMRT_KB_SITE="moonbase.alpha.surface_ops"
    luajit "$ROBOT_DIR/remote_nats_ct.lua" "$ROBOT_ID" &
    REMOTE_PID=$!
    echo "Remote CT started (pid=$REMOTE_PID)"

    sleep 1
    luajit test_sequencer.lua
    local TEST_RC=$?

    sleep 0.5
    if kill -0 $REMOTE_PID 2>/dev/null; then
        kill $REMOTE_PID 2>/dev/null
        wait $REMOTE_PID 2>/dev/null || true
    else
        wait $REMOTE_PID 2>/dev/null || true
    fi

    echo ""
    return $TEST_RC
}

run_robot() {
    echo "=== Independent Robot Test ==="
    build_all

    # Export KB to NATS KV
    cd "$SCRIPT_DIR"
    luajit -e "
        local kb_exporter = require('kb_exporter')
        kb_exporter.export({ db_file='$KB_CONSTRUCT/surface_ops.db', nats_server='$NATS_SERVER', bucket='kb_export' })
        print('KB exported to NATS.')
    "

    # Flush stale NATS queues and clear stale status
    luajit -e "
        local site = '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}'
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, site)
        tx:flush()
        tx:close()
        -- Clear stale robot status so slot claim works
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

    # Create robot config JSON
    local ROBOT_CONFIG="/tmp/test_robot_$ROBOT_ID.json"
    cat > "$ROBOT_CONFIG" <<REOF
{
    "robot_id": "$ROBOT_ID",
    "site": "${VMRT_KB_SITE:-moonbase.alpha.surface_ops}",
    "nats_server": "$NATS_SERVER",
    "robot_class": "lunar_rover",
    "remote_json": "$ROBOT_DIR/remote.json"
}
REOF

    # Start independent robot process
    export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"
    luajit "$ROBOT_PROCESS_DIR/robot_main.lua" "$ROBOT_CONFIG" &
    REMOTE_PID=$!
    echo "Independent robot started (pid=$REMOTE_PID)"

    sleep 2
    luajit test_independent_robot.lua
    local TEST_RC=$?

    # Send shutdown via NATS (submit a shutdown command to rpc queue)
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        local json = require('json_util')
        tx:send_rpc(json.encode({packet_type=255, seq=99}))
        tx:close()
    " 2>/dev/null

    sleep 1
    if kill -0 $REMOTE_PID 2>/dev/null; then
        kill $REMOTE_PID 2>/dev/null
        wait $REMOTE_PID 2>/dev/null || true
    else
        wait $REMOTE_PID 2>/dev/null || true
    fi

    rm -f "$ROBOT_CONFIG"
    echo ""
    return $TEST_RC
}

run_remote() {
    echo "=== Remote Robot Test ==="
    echo "  Robot assumed running externally. No spawn, no cleanup."
    build_all

    # Export KB to NATS KV (remote robot reads class config from here)
    cd "$SCRIPT_DIR"
    luajit -e "
        local kb_exporter = require('kb_exporter')
        kb_exporter.export({ db_file='$KB_CONSTRUCT/surface_ops.db', nats_server='$NATS_SERVER', bucket='kb_export' })
        print('KB exported to NATS.')
    "

    # Flush stale queues (but NOT robot status — robot owns that)
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('Queues flushed.')
    "

    luajit test_remote_robot.lua
    local TEST_RC=$?

    echo ""
    return $TEST_RC
}

run_mqtt_bridge() {
    echo "=== MQTT Bridge Test ==="
    build_all

    # Flush stale NATS queues
    cd "$SCRIPT_DIR"
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        tx:flush()
        tx:close()
        print('NATS queues flushed.')
    "

    # Start MQTT robot process
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    export VMRT_KB_DB="$KB_CONSTRUCT/surface_ops.db"
    export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"
    luajit "$MQTT_ROBOT_DIR/remote_mqtt_ct.lua" "$ROBOT_ID" &
    MQTT_ROBOT_PID=$!
    echo "MQTT robot started (pid=$MQTT_ROBOT_PID)"

    # Start NATS↔MQTT bridge (multi-robot, no robot_id arg)
    luajit "$MQTT_BRIDGE_DIR/mqtt_bridge.lua" &
    BRIDGE_PID=$!
    echo "Bridge started (pid=$BRIDGE_PID)"

    sleep 2

    luajit test_mqtt_bridge.lua
    local TEST_RC=$?

    # Cleanup
    sleep 2
    for PID in $MQTT_ROBOT_PID $BRIDGE_PID; do
        if kill -0 $PID 2>/dev/null; then
            kill $PID 2>/dev/null
            wait $PID 2>/dev/null || true
        else
            wait $PID 2>/dev/null || true
        fi
    done

    echo ""
    return $TEST_RC
}

run_mqtt_robot() {
    echo "=== MQTT Independent Robot Test ==="
    build_all

    # Export KB to NATS KV (action server reads class config from here)
    cd "$SCRIPT_DIR"
    luajit -e "
        local kb_exporter = require('kb_exporter')
        kb_exporter.export({ db_file='$KB_CONSTRUCT/surface_ops.db', nats_server='$NATS_SERVER', bucket='kb_export' })
        print('KB exported to NATS.')
    "

    # Flush stale NATS queues and clear stale status
    luajit -e "
        local site = '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}'
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

    # Create MQTT robot config JSON (wire_format passed as $1, default "json")
    local WIRE_FORMAT="${1:-json}"
    local ROBOT_CONFIG="/tmp/test_mqtt_robot_$ROBOT_ID.json"
    cat > "$ROBOT_CONFIG" <<REOF
{
    "robot_id": "$ROBOT_ID",
    "site": "${VMRT_KB_SITE:-moonbase.alpha.surface_ops}",
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "robot_class": "lunar_rover",
    "remote_json": "$ROBOT_DIR/remote.json",
    "energy_max": 10000,
    "energy_infinite": false,
    "wire_format": "$WIRE_FORMAT",
    "capabilities": [
        "init_check", "path_spline", "path_line", "path_wall",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle"
    ]
}
REOF
    echo "  wire_format: $WIRE_FORMAT"

    # Start MQTT robot first (publishes retained state with wire_format)
    export VMRT_KB_SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"
    luajit "$MQTT_ROBOT_DIR/mqtt_robot_main.lua" "$ROBOT_CONFIG" &
    MQTT_ROBOT_PID=$!
    echo "MQTT robot started (pid=$MQTT_ROBOT_PID)"
    sleep 1

    # Start NATS↔MQTT bridge (multi-robot, discovers from MQTT status)
    luajit "$MQTT_BRIDGE_DIR/mqtt_bridge.lua" &
    BRIDGE_PID=$!
    echo "Bridge started (pid=$BRIDGE_PID)"
    sleep 2

    # Run test (checks NATS KV for mirrored status, runs mission via action server)
    luajit test_mqtt_robot.lua
    local TEST_RC=$?

    # Send shutdown via NATS (goes through bridge to MQTT robot)
    luajit -e "
        local tx = require('nats_transport').hub_side('$ROBOT_ID', nil, '${VMRT_KB_SITE:-moonbase.alpha.surface_ops}')
        local json = require('json_util')
        tx:send_rpc(json.encode({packet_type=255, seq=99}))
        tx:close()
    " 2>/dev/null

    sleep 2
    for PID in $MQTT_ROBOT_PID $BRIDGE_PID; do
        if kill -0 $PID 2>/dev/null; then
            kill $PID 2>/dev/null
            wait $PID 2>/dev/null || true
        else
            wait $PID 2>/dev/null || true
        fi
    done

    rm -f "$ROBOT_CONFIG"
    echo ""
    return $TEST_RC
}

run_mqtt_multi() {
    echo "=== Multi-Robot MQTT Bridge Test ==="
    echo "  rover_1: JSON wire format"
    echo "  rover_2: CBOR wire format"
    build_all

    # Export KB to NATS KV
    cd "$SCRIPT_DIR"
    luajit -e "
        local kb_exporter = require('kb_exporter')
        kb_exporter.export({ db_file='$KB_CONSTRUCT/surface_ops.db', nats_server='$NATS_SERVER', bucket='kb_export' })
        print('KB exported to NATS.')
    "

    # Flush NATS queues and clear status for both robots
    local SITE="${VMRT_KB_SITE:-moonbase.alpha.surface_ops}"
    for RID in rover_1 rover_2; do
        luajit -e "
            local site = '$SITE'
            local tx = require('nats_transport').hub_side('$RID', nil, site)
            tx:flush(); tx:close()
            local ks_lib = require('lib.nats_key_store')
            local ks = ks_lib.KeyStore.new({
                server = '$NATS_SERVER',
                bucket = site:gsub('%.', '_') .. '_robot_status',
                create_bucket = true, history = 1,
            })
            ks:connect()
            pcall(function() ks:delete(site .. '.robots.$RID.status.state') end)
            pcall(function() ks:delete(site .. '.robots.$RID.status.energy') end)
            pcall(function() ks:delete(site .. '.robots.$RID.status.bitmask') end)
            ks:disconnect(); ks:destroy()
        "
    done
    echo "Queues flushed, status cleared for rover_1 + rover_2."

    export VMRT_KB_SITE="$SITE"

    # Robot 1: JSON wire format
    local R1_CONFIG="/tmp/test_mqtt_rover_1.json"
    cat > "$R1_CONFIG" <<REOF
{
    "robot_id": "rover_1",
    "site": "$SITE",
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "robot_class": "lunar_rover",
    "remote_json": "$ROBOT_DIR/remote.json",
    "energy_max": 10000,
    "energy_infinite": false,
    "wire_format": "json",
    "capabilities": [
        "init_check", "path_spline", "path_line", "path_wall",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle"
    ]
}
REOF

    # Robot 2: CBOR wire format
    local R2_CONFIG="/tmp/test_mqtt_rover_2.json"
    cat > "$R2_CONFIG" <<REOF
{
    "robot_id": "rover_2",
    "site": "$SITE",
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "robot_class": "lunar_rover",
    "remote_json": "$ROBOT_DIR/remote.json",
    "energy_max": 10000,
    "energy_infinite": false,
    "wire_format": "cbor",
    "capabilities": [
        "init_check", "path_spline", "path_line", "path_wall",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle"
    ]
}
REOF

    # Start both robots
    luajit "$MQTT_ROBOT_DIR/mqtt_robot_main.lua" "$R1_CONFIG" &
    R1_PID=$!
    echo "Robot rover_1 started (pid=$R1_PID, wire=json)"

    luajit "$MQTT_ROBOT_DIR/mqtt_robot_main.lua" "$R2_CONFIG" &
    R2_PID=$!
    echo "Robot rover_2 started (pid=$R2_PID, wire=cbor)"
    sleep 1

    # Start single multi-robot bridge
    luajit "$MQTT_BRIDGE_DIR/mqtt_bridge.lua" &
    BRIDGE_PID=$!
    echo "Bridge started (pid=$BRIDGE_PID)"
    sleep 2

    # Run multi-robot test
    luajit test_mqtt_multi_robot.lua
    local TEST_RC=$?

    # Shutdown both robots via NATS
    for RID in rover_1 rover_2; do
        luajit -e "
            local tx = require('nats_transport').hub_side('$RID', nil, '$SITE')
            local json = require('json_util')
            tx:send_rpc(json.encode({packet_type=255, seq=99}))
            tx:close()
        " 2>/dev/null
    done

    sleep 2
    for PID in $R1_PID $R2_PID $BRIDGE_PID; do
        if kill -0 $PID 2>/dev/null; then
            kill $PID 2>/dev/null
            wait $PID 2>/dev/null || true
        else
            wait $PID 2>/dev/null || true
        fi
    done

    rm -f "$R1_CONFIG" "$R2_CONFIG"
    echo ""
    return $TEST_RC
}

case "${1:-nats}" in
    loopback)           run_loopback ;;
    chaintree_loopback) run_chaintree_loopback ;;
    nats)               run_nats ;;
    nats_ct)            run_nats_ct ;;
    hub_rt)             run_hub_rt ;;
    planner)            run_planner ;;
    sequencer)          run_sequencer ;;
    action)             run_action ;;
    robot)              run_robot ;;
    remote)             run_remote ;;
    mqtt_bridge)        run_mqtt_bridge ;;
    mqtt_robot)         run_mqtt_robot json ;;
    mqtt_robot_cbor)    run_mqtt_robot cbor ;;
    mqtt_multi)         run_mqtt_multi ;;
    all)                run_loopback; run_chaintree_loopback; run_nats; run_nats_ct; run_hub_rt; run_sequencer; run_planner; run_action; run_robot ;;
    *)                  echo "Usage: $0 [loopback|chaintree_loopback|nats|nats_ct|hub_rt|sequencer|planner|action|robot|remote|mqtt_bridge|mqtt_robot|all]"; exit 1 ;;
esac
