#!/bin/bash
# run_tests.sh -- Run all ros_planner_ii tests
#
# Usage: ./run_tests.sh                    (all tests)
#        ./run_tests.sh loopback           (raw packet loopback — socket based)
#        ./run_tests.sh chaintree_loopback (step 1: hub CT + remote loopback — socket)
#        ./run_tests.sh nats               (NATS loopback — no sockets)
#        ./run_tests.sh nats_ct            (NATS + ChainTree — full stack)
#        ./run_tests.sh hub_rt             (hub_runtime module — same route via hub_runtime API)

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
export LUA_PATH="$ACTION_SERVER_DIR/?.lua;$GLOBAL_PLANNER_DIR/?.lua;$LOCAL_PLANNER_DIR/?.lua;$HUB_DSL_DIR/protocol/?.lua;$HUB_DSL_DIR/?.lua;$HUB_DSL_DIR/hub_functions/?.lua;$HUB_DSL_DIR/kb/?.lua;$KB_CONSTRUCT/?.lua;$SQLITE_KB/?.lua;$RUNTIME_DIR/?.lua;$ROBOT_DIR/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$NATS_BASE/?.lua;$NATS_LIB/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"
export LUA_CPATH="$RUNTIME_DIR/?.so;$NATS_BASE/?.so;;"

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
export LD_LIBRARY_PATH="$NATS_BASE:${LD_LIBRARY_PATH:-}"

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

case "${1:-nats}" in
    loopback)           run_loopback ;;
    chaintree_loopback) run_chaintree_loopback ;;
    nats)               run_nats ;;
    nats_ct)            run_nats_ct ;;
    hub_rt)             run_hub_rt ;;
    planner)            run_planner ;;
    sequencer)          run_sequencer ;;
    action)             run_action ;;
    all)                run_loopback; run_chaintree_loopback; run_nats; run_nats_ct; run_hub_rt; run_sequencer; run_planner; run_action ;;
    *)                  echo "Usage: $0 [loopback|chaintree_loopback|nats|nats_ct|hub_rt|sequencer|planner|action|all]"; exit 1 ;;
esac
