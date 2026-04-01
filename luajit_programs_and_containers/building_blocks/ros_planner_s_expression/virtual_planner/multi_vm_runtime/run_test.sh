#!/bin/bash
# run_test.sh -- Run multi-VM runtime tests
#
# Usage: ./run_test.sh           (runs all tests)
#        ./run_test.sh ringbuf   (ringbuffer only)
#        ./run_test.sh thread    (thread only)
#        ./run_test.sh e2e       (end-to-end simulation)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Build first
make -s

VP_ROOT="$SCRIPT_DIR/.."
ROBOT="${VMRT_ROBOT:-test_robot}"
ROBOT_DIR="$VP_ROOT/robots/$ROBOT"
CT_RUNTIME="$VP_ROOT/../../chain_tree_luajit/runtime_dict"
CT_JSON="$VP_ROOT/../../chain_tree_luajit/lua_dsl/luajit_pipeline"

export LUA_PATH="$SCRIPT_DIR/?.lua;$VP_ROOT/lib/?.lua;$VP_ROOT/hub_functions/?.lua;$ROBOT_DIR/?.lua;$VP_ROOT/hub_templates/?.lua;$CT_RUNTIME/?.lua;$CT_JSON/?.lua;?.lua;;"
export LUA_CPATH="$SCRIPT_DIR/?.so;;"

run_ringbuf() {
    echo "=== Ringbuffer Tests ==="
    luajit test_ringbuf.lua
    echo ""
}

run_thread() {
    echo "=== Thread Tests ==="
    luajit test_thread.lua
    echo ""
}

run_e2e() {
    echo "=== End-to-End Multi-VM Test (robot: $ROBOT) ==="
    # Build trees first
    (cd "$VP_ROOT" && bash build_trees.sh "$ROBOT")
    echo ""
    export VMRT_HUB_JSON="$ROBOT_DIR/hub.json"
    export VMRT_REMOTE_JSON="$ROBOT_DIR/remote.json"
    luajit main_vm.lua
    echo ""
}

case "${1:-all}" in
    ringbuf) run_ringbuf ;;
    thread)  run_thread ;;
    e2e)     run_e2e ;;
    all)     run_ringbuf; run_thread; run_e2e ;;
    *)       echo "Usage: $0 [ringbuf|thread|e2e|all]"; exit 1 ;;
esac
