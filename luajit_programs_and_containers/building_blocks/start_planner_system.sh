#!/bin/bash
# start_planner_system.sh -- Manage the ROS Planner II system.
#
# Creates the planner-net Docker network, ensures infra containers are
# connected, builds the KB, and starts/stops the planner container.
#
# Usage:
#   ./start_planner_system.sh start     Build KB, start planner container
#   ./start_planner_system.sh stop      Stop planner + kv-bridge containers
#   ./start_planner_system.sh restart   Stop then start
#   ./start_planner_system.sh status    Show container and network status
#   ./start_planner_system.sh build     Build KB only (no container start)
#
# Prerequisites:
#   - Postgres, NATS, MQTT containers running
#   - Postgres has KB data (or run master_build first)
#
# Environment:
#   DOCKER_NETWORK  Docker network name (default: planner-net)
#   SQLITE_DATA     SQLite extract directory (default: /home/gedgar/Sqlite_Data)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTAINERS_DIR="$SCRIPT_DIR/../../third_party_containers"
KB_DSL_DIR="$SCRIPT_DIR/kb_dsl/scripts"
NETWORK="${DOCKER_NETWORK:-planner-net}"
SQLITE_DATA="${SQLITE_DATA:-/home/gedgar/Sqlite_Data}"

# Infra containers that must be on planner-net
INFRA_CONTAINERS="nats-js-ram mosquitto-ram-ws_main"

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
    for name in $INFRA_CONTAINERS; do
        if check_container_running "$name"; then
            docker network connect "$NETWORK" "$name" 2>/dev/null \
                && echo "  Connected $name to $NETWORK" \
                || echo "  $name already on $NETWORK"
        else
            echo "  WARNING: $name is not running"
        fi
    done
}

# ---------------------------------------------------------------------------
# Build KB
# ---------------------------------------------------------------------------

do_build() {
    echo "=== Building KB (Postgres → SQLite) ==="

    if ! pg_isready -h localhost -q 2>/dev/null; then
        echo "ERROR: Postgres is not running"
        exit 1
    fi

    cd "$KB_DSL_DIR"
    luajit master_build.lua 2>&1 | grep -E "^(===|  Planner|  Extracting|    ->|\[OK\]|---)" | head -20
    cd "$SCRIPT_DIR"

    if [ ! -f "$SQLITE_DATA/surface_ops.db" ]; then
        echo "ERROR: SQLite extract failed — $SQLITE_DATA/surface_ops.db not found"
        exit 1
    fi

    echo ""
}

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------

do_start() {
    echo "=== Starting Planner System ==="
    echo ""

    # Step 1: Network
    echo "--- Network ---"
    ensure_network
    connect_infra
    echo ""

    # Step 2: Build KB
    do_build

    # Step 3: kv-bridge
    echo "--- KV Bridge ---"
    if check_container_running kv-bridge; then
        # Check if on the right network
        local kvnet=$(docker inspect kv-bridge --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)
        if echo "$kvnet" | grep -q "$NETWORK"; then
            echo "  kv-bridge already running on $NETWORK"
        else
            echo "  Restarting kv-bridge on $NETWORK..."
            bash "$CONTAINERS_DIR/kv_bridge/docker_run.sh"
        fi
    else
        echo "  Starting kv-bridge..."
        bash "$CONTAINERS_DIR/kv_bridge/docker_run.sh"
    fi
    echo ""

    # Step 4: Planner container
    echo "--- Planner Container ---"
    bash "$CONTAINERS_DIR/ros_planner/docker_build.sh" 2>&1 | grep -E "^(===|  Runtime|  Staged|  Image)" | head -5
    echo ""
    bash "$CONTAINERS_DIR/ros_planner/docker_run.sh"
    echo ""

    # Step 5: Wait for planner to be ready
    echo "--- Waiting for planner startup ---"
    for i in $(seq 1 30); do
        if docker logs surface_ops-planner 2>&1 | grep -q "Action server ready"; then
            echo "  Planner ready!"
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo "  WARNING: Planner did not become ready in 30s"
            echo "  Check: docker logs -f surface_ops-planner"
        fi
        sleep 1
    done

    echo ""
    echo "=== System Started ==="
    echo "  Submit missions: bash ros_scripts/submit_mission.sh ros_scripts/moonbase_mission.json"
    echo "  Start robot:     bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_config.json"
    echo "  Planner logs:    docker logs -f surface_ops-planner"
}

# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------

do_stop() {
    echo "=== Stopping Planner System ==="
    docker rm -f surface_ops-planner 2>/dev/null && echo "  Stopped surface_ops-planner" || echo "  surface_ops-planner not running"
    docker rm -f kv-bridge 2>/dev/null && echo "  Stopped kv-bridge" || echo "  kv-bridge not running"
    echo "=== Stopped ==="
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

do_status() {
    echo "=== Planner System Status ==="
    echo ""

    echo "Network: $NETWORK"
    local net_containers=$(docker network inspect "$NETWORK" --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null)
    if [ -n "$net_containers" ]; then
        echo "  Containers: $net_containers"
    else
        echo "  Network not found or empty"
    fi
    echo ""

    echo "Containers:"
    for name in nats-js-ram mosquitto-ram-ws_main pg-vector kv-bridge surface_ops-planner; do
        if check_container_running "$name"; then
            local net=$(docker inspect "$name" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)
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
    *)       echo "Usage: $0 {start|stop|restart|status|build}"
             exit 1 ;;
esac
