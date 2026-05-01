#!/usr/bin/env bash
# rebuild_and_start.sh -- end-to-end build/KB/start for the nano_data_center
# cluster. Replaces the multi-step boot manual from README.md.
#
# Modes:
#   --start    (default) just start dcs.lua on cpu_01 + cpu_02
#   --kb       rebuild KB + slice + stage, then start
#   --full     rebuild all 6 images + KB + slice + stage + start
#   --stop     stop dcs.lua on both CPUs (no restart)
#
# Prereqs (all modes): infra containers up (pg-vector, nats-js-ram,
# mosquitto-ram-ws_main, kv-bridge). Run install_infra.sh under
# commissioning_software/system_node_control/construction/ if not.
#
# Usage:
#   ./rebuild_and_start.sh                  # just start
#   ./rebuild_and_start.sh --kb             # rebuild KB then start
#   ./rebuild_and_start.sh --full           # rebuild everything then start
#   ./rebuild_and_start.sh --stop           # stop both CPUs
#
# Idempotent: re-running --start when cluster is up is a no-op (stop.sh
# first if you want a clean restart).

set -euo pipefail

MODE="${1:---start}"

# Resolve NDC_BASE from this script's location:
# <NDC_BASE>/support_procedures/runbooks/rebuild_and_start.sh -> climb 2
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export NDC_BASE="${NDC_BASE:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

CONSTR="$NDC_BASE/commissioning_software/system_node_control/construction"
DEP="$NDC_BASE/commissioning_software/system_node_control/deployment"

echo "=== rebuild_and_start.sh mode=$MODE ==="
echo "    NDC_BASE=$NDC_BASE"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

check_infra() {
    local missing=0
    for c in pg-vector nats-js-ram mosquitto-ram-ws_main kv-bridge; do
        if ! docker ps --format '{{.Names}}' | grep -qx "$c"; then
            echo "  MISSING infra container: $c" >&2
            missing=1
        fi
    done
    if [[ $missing -eq 1 ]]; then
        echo "Run $CONSTR/install_infra.sh first." >&2
        exit 1
    fi
    echo "  infra: pg-vector, nats-js-ram, mosquitto-ram-ws_main, kv-bridge all up"
}

ensure_broker() {
    if docker ps --format '{{.Names}}' | grep -qx docker-host-broker; then
        echo "  docker-host-broker: already up"
        return 0
    fi
    echo "  starting docker-host-broker"
    local pg_pw
    pg_pw=$(docker exec pg-vector printenv POSTGRES_PASSWORD)
    docker rm -f docker-host-broker 2>/dev/null || true
    docker run -d --name docker-host-broker \
        --restart unless-stopped \
        -v /var/run/docker.sock:/var/run/docker.sock:ro \
        -v /proc:/host/proc:ro \
        --network planner-net \
        -p 127.0.0.1:9100:9100 \
        -e SITE=moonbase.alpha.dcs \
        -e NATS_URL=nats://nats-js-ram:4222 \
        -e HTTP_ADDR=0.0.0.0:9100 \
        -e PG_DSN="host=pg-vector port=5432 dbname=knowledge_base user=gedgar password=$pg_pw" \
        nanodatacenter/docker-host-broker:latest >/dev/null
    sleep 4
    if ! curl -sS -m 3 http://127.0.0.1:9100/v1/health >/dev/null; then
        echo "  ERROR: docker-host-broker did not become healthy" >&2
        docker logs docker-host-broker | tail -20 >&2
        exit 1
    fi
    echo "  docker-host-broker: healthy"
}

build_images() {
    echo "=== building 6 images from $NDC_BASE ==="
    ( cd "$NDC_BASE/luajit/luajit_base/container"             && bash docker_build.sh )
    ( cd "$NDC_BASE/luajit/openresty_base/container"          && bash docker_build.sh )
    ( cd "$NDC_BASE/commissioning_software/infrastructure/docker_host_broker/container" && bash docker_build.sh )
    ( cd "$NDC_BASE/commissioning_software/validation/test_app/container"               && bash docker_build.sh )
    ( cd "$NDC_BASE/platform_containers/dcs_console/container"        && bash docker_build.sh )
    ( cd "$NDC_BASE/platform_containers/observability/container"      && bash docker_build.sh )
    # If broker image was rebuilt, force-recreate so we run the new bits.
    docker rm -f docker-host-broker 2>/dev/null || true
}

build_kb_and_stage() {
    echo "=== build_kb.sh + slice_bootstrap.sh + stage_deploy.sh ==="
    ( cd "$CONSTR" && bash build_kb.sh )
    ( cd "$CONSTR" && bash slice_bootstrap.sh )
    ( cd "$CONSTR" && bash stage_deploy.sh --mode=dev )
    # First-time staging seeds env.sh without POSTGRES_PASSWORD; inject from pg-vector.
    local pg_pw
    pg_pw=$(docker exec pg-vector printenv POSTGRES_PASSWORD)
    for cpu in cpu_01 cpu_02; do
        if ! grep -q '^export POSTGRES_PASSWORD=' "$DEP/$cpu/env.sh"; then
            echo "export POSTGRES_PASSWORD=$pg_pw" >> "$DEP/$cpu/env.sh"
            echo "  injected POSTGRES_PASSWORD into $cpu/env.sh"
        fi
    done
}

stop_cluster() {
    echo "=== stopping cluster ==="
    [[ -x "$DEP/cpu_01/stop.sh" ]] && "$DEP/cpu_01/stop.sh" || true
    [[ -x "$DEP/cpu_02/stop.sh" ]] && "$DEP/cpu_02/stop.sh" || true
}

start_cluster() {
    echo "=== starting cluster ==="
    # setsid -f forks AND becomes a new session leader in one syscall, fully
    # detaching from the script. The previous "( setsid nohup CMD & disown )"
    # pattern works interactively but hangs in non-interactive bash because
    # the subshell keeps the backgrounded watchdog in its job table despite
    # disown.
    setsid -f bash -c "cd '$DEP/cpu_01' && exec ./start.sh" </dev/null >/dev/null 2>&1
    sleep 3
    setsid -f bash -c "cd '$DEP/cpu_02' && exec ./start.sh" </dev/null >/dev/null 2>&1
    sleep 30
    echo "    cpu_01 latest:"; tail -3 "$DEP/cpu_01/error.log" | sed 's/^/      /'
    echo "    cpu_02 latest:"; tail -3 "$DEP/cpu_02/error.log" | sed 's/^/      /'
    echo
    echo "=== docker ps ==="
    docker ps --format "    {{.Names}}\t{{.Status}}"
}

# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------

case "$MODE" in
    --stop)
        stop_cluster
        exit 0
        ;;
    --start)
        check_infra
        ensure_broker
        if [[ ! -r "$DEP/cpu_01/bootstrap.db" || ! -r "$DEP/cpu_02/bootstrap.db" ]]; then
            echo "  bootstrap.db missing; running --kb path"
            build_kb_and_stage
        fi
        start_cluster
        ;;
    --kb)
        check_infra
        ensure_broker
        stop_cluster
        build_kb_and_stage
        start_cluster
        ;;
    --full)
        check_infra
        build_images
        ensure_broker
        stop_cluster
        build_kb_and_stage
        start_cluster
        ;;
    *)
        echo "usage: $0 [--start|--kb|--full|--stop]" >&2
        exit 2
        ;;
esac

echo
echo "Target convergence: master sys_ready=true node_op=true; slave node_op=true."
echo "If not yet converged, watch with:"
echo "    tail -f $DEP/cpu_01/error.log"
