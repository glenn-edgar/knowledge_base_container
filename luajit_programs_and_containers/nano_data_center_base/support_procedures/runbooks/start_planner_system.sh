#!/bin/bash
# start_planner_system.sh -- Manage the embedded data center.
#
# Uses the orchestrator container for lifecycle management.
# The orchestrator reads the dependency graph from Postgres KB
# and starts/stops containers in topological order.
#
# Usage:
#   ./start_planner_system.sh start      Start all containers
#   ./start_planner_system.sh stop       Stop all (except Postgres)
#   ./start_planner_system.sh restart    Stop then start
#   ./start_planner_system.sh status     Show container and network status
#   ./start_planner_system.sh build      Rebuild KB (stop → build → start)
#   ./start_planner_system.sh rebuild    Build orchestrator image, then KB rebuild
#
# Prerequisites:
#   - Postgres container running (pg-vector)
#   - Orchestrator image built (nanodatacenter/orchestrator:latest)
#
# Environment:
#   DOCKER_NETWORK  Docker network name (default: planner-net)
#   SQLITE_DATA     SQLite extract directory (default: /home/gedgar/Sqlite_Data)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTAINERS_DIR="$SCRIPT_DIR/../../third_party_containers"
ORCH_DIR="$CONTAINERS_DIR/orchestrator"
NETWORK="${DOCKER_NETWORK:-planner-net}"
SQLITE_DATA="${SQLITE_DATA:-/home/gedgar/Sqlite_Data}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

check_container_running() {
    docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true
}

ensure_network() {
    docker network create "$NETWORK" 2>/dev/null && echo "Created network: $NETWORK" || true
}

connect_infra() {
    # Connect infra containers to planner-net (idempotent)
    for name in nats-js-ram mosquitto-ram-ws_main; do
        if check_container_running "$name"; then
            docker network connect "$NETWORK" "$name" 2>/dev/null \
                && echo "  Connected $name to $NETWORK" \
                || echo "  $name already on $NETWORK"
        else
            echo "  WARNING: $name is not running"
        fi
    done
}

ensure_orchestrator_image() {
    if ! docker image inspect nanodatacenter/orchestrator:latest >/dev/null 2>&1; then
        echo "  Building orchestrator image..."
        bash "$ORCH_DIR/docker_build.sh"
        echo ""
    fi
}

# Run orchestrator container (one-shot, auto-removed)
run_orchestrator() {
    local mode="$1"
    shift
    docker run --rm \
        --name "orchestrator-${mode}" \
        --network host \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v "$SQLITE_DATA:/data" \
        -e PG_HOST="${PG_HOST:-127.0.0.1}" \
        -e PG_PORT="${PG_PORT:-5432}" \
        -e PG_DBNAME="${PG_DBNAME:-knowledge_base}" \
        -e PG_USER="${PG_USER:-gedgar}" \
        -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}" \
        -e SQLITE_DATA="/data" \
        -e SQLITE_HOST_PATH="$SQLITE_DATA" \
        nanodatacenter/orchestrator:latest \
        luajit "$@"
}

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------

do_start() {
    echo "=== Starting System ==="
    echo ""

    # Step 1: Network
    echo "--- Network ---"
    ensure_network
    connect_infra
    echo ""

    # Step 2: Ensure orchestrator image exists
    ensure_orchestrator_image

    # Step 3: Start all containers via orchestrator
    run_orchestrator start startup.lua --exclude postgres \
        --network "$NETWORK" --connect nats_server,mqtt_broker

    echo ""
    echo "=== System Started ==="
    echo "  Web UI:          http://localhost:8080/"
    echo "  Submit mission:  bash ros_scripts/submit_mission.sh ros_scripts/moonbase_mission.json"
    echo "  Start robot:     bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_config.json"
}

# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------

do_stop() {
    echo "=== Stopping System ==="

    ensure_orchestrator_image

    # Stop all except Postgres (keep KB source of truth alive)
    run_orchestrator stop shutdown.lua --exclude postgres
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

do_build() {
    echo "=== KB Rebuild ==="

    ensure_orchestrator_image

    # KB rebuild: stop → master_build → sqlite extract → start
    run_orchestrator build kb_build.lua
}

# ---------------------------------------------------------------------------
# Rebuild (build orchestrator image first, then KB rebuild)
# ---------------------------------------------------------------------------

do_rebuild() {
    echo "=== Full Rebuild ==="
    echo ""

    echo "--- Building orchestrator image ---"
    bash "$ORCH_DIR/docker_build.sh"
    echo ""

    # Also rebuild planner container image
    echo "--- Building planner image ---"
    bash "$CONTAINERS_DIR/ros_planner/docker_build.sh"
    echo ""

    do_build
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

do_status() {
    echo "=== System Status ==="
    echo ""

    echo "Network: $NETWORK"
    local net_containers=$(docker network inspect "$NETWORK" \
        --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null)
    if [ -n "$net_containers" ]; then
        echo "  Containers: $net_containers"
    else
        echo "  Network not found or empty"
    fi
    echo ""

    echo "Containers:"
    for name in nats-js-ram mosquitto-ram-ws_main pg-vector openresty-gateway \
                kv-bridge fleet-manager telemetry-collector \
                surface_ops-planner warehouse_ops-planner; do
        if check_container_running "$name"; then
            local net=$(docker inspect "$name" \
                --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)
            printf "  %-25s running  [%s]\n" "$name" "$net"
        else
            printf "  %-25s stopped\n" "$name"
        fi
    done
    echo ""

    if check_container_running surface_ops-planner; then
        echo "Planner log (last 5 lines):"
        docker logs surface_ops-planner 2>&1 | tail -5 | sed 's/^/  /'
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${1:-status}" in
    start)   do_start ;;
    stop)    do_stop ;;
    restart) do_stop; echo ""; do_start ;;
    status)  do_status ;;
    build)   do_build ;;
    rebuild) do_rebuild ;;
    *)       echo "Usage: $0 {start|stop|restart|status|build|rebuild}"
             exit 1 ;;
esac
