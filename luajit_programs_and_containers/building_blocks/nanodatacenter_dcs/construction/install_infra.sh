#!/usr/bin/env bash
# install_infra.sh -- One-time setup of DCS infrastructure containers.
#
# Wraps the proven scripts under ~/knowledge_base_assembly/third_party_containers/
# to bring up the four containers DCS supervises:
#   pg-vector              (postgres)
#   nats-js-ram            (NATS JetStream)
#   mosquitto-ram-ws_main  (MQTT)
#   kv-bridge              (HTTP -> NATS KV bridge)
#
# Idempotent: skips containers that already exist (any state).
# After this runs, DCS owns start/stop via docker start/stop.
#
# Pass --recreate to docker-rm any existing infra container first.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPC_ROOT="${HOME}/knowledge_base_assembly/third_party_containers"

[[ -d "$TPC_ROOT" ]] || {
    echo "missing $TPC_ROOT (third_party_containers); update the path" >&2
    exit 1
}

RECREATE=0
for arg in "$@"; do
    case "$arg" in
        --recreate) RECREATE=1 ;;
        -h|--help)
            grep '^# ' "$0" | sed 's/^# //'
            exit 0 ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

# secrets needed for pg-vector POSTGRES_PASSWORD env
SECRETS_FILE="${HOME}/.config/nanodatacenter/secrets.env"
if [[ -f "$SECRETS_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
else
    echo "missing $SECRETS_FILE -- run setup_secrets.sh first" >&2
    exit 1
fi
[[ -n "${PG_PASSWORD:-}" ]] || { echo "PG_PASSWORD not set after sourcing $SECRETS_FILE" >&2; exit 1; }
export POSTGRES_PASSWORD="$PG_PASSWORD"

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

container_exists() { docker container inspect "$1" >/dev/null 2>&1; }

ensure_container() {
    local name="$1"; shift
    local create_cmd=("$@")     # script + args to run if container doesn't exist
    if container_exists "$name"; then
        if [[ $RECREATE -eq 1 ]]; then
            echo "  $name exists -- --recreate: removing"
            docker rm -f "$name" >/dev/null
        else
            echo "  $name exists -- skip"
            return 0
        fi
    fi
    echo "  creating $name"
    ( cd "${create_cmd[0]%/*}" && bash "${create_cmd[@]}" >/dev/null )
}

# --------------------------------------------------------------------------
# planner-net network (kv-bridge needs it for DNS to other infra)
# --------------------------------------------------------------------------

if ! docker network inspect planner-net >/dev/null 2>&1; then
    echo "creating planner-net docker network"
    docker network create planner-net >/dev/null
else
    echo "planner-net exists -- skip"
fi

# --------------------------------------------------------------------------
# pg-vector
# --------------------------------------------------------------------------
ensure_container pg-vector \
    "$TPC_ROOT/postgres/pg_17_containers/pg_17_vector.sh"

# --------------------------------------------------------------------------
# nats-js-ram
# --------------------------------------------------------------------------
ensure_container nats-js-ram \
    "$TPC_ROOT/nats/docker_run.sh"

# --------------------------------------------------------------------------
# mosquitto-ram-ws_main
# --------------------------------------------------------------------------
ensure_container mosquitto-ram-ws_main \
    "$TPC_ROOT/mosquito/docker_create.sh" main

# --------------------------------------------------------------------------
# kv-bridge (depends on mosquitto + nats DNS via planner-net)
# --------------------------------------------------------------------------
ensure_container kv-bridge \
    "$TPC_ROOT/kv_bridge/docker_run.sh"

# --------------------------------------------------------------------------
# attach all four to planner-net (kv-bridge resolves the others by name)
# --------------------------------------------------------------------------

for c in pg-vector nats-js-ram mosquitto-ram-ws_main kv-bridge; do
    if ! docker network inspect planner-net | grep -q "\"Name\": \"$c\""; then
        echo "  attaching $c to planner-net"
        docker network connect planner-net "$c" 2>/dev/null || true
    fi
done

# --------------------------------------------------------------------------
# summary
# --------------------------------------------------------------------------

echo
echo "infra installed. current state:"
docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' \
    | grep -E '^(NAMES|pg-vector|nats-js-ram|mosquitto-ram-ws_main|kv-bridge)' \
    || true

echo
echo "next step: DCS will manage start/stop via build_output/<cpu>/start.sh"
echo "(run setup_secrets.sh first if you haven't, then build_kb.sh + slice_bootstrap.sh)"
