#!/usr/bin/env bash
# slice_bootstrap.sh -- Generate per-role bootstrap.db files + start.sh wrappers.
# Sources operator secrets so POSTGRES_PASSWORD is set.
#
# Usage: ./slice_bootstrap.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NDC_BASE="${NDC_BASE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
PG_KB_DIR="$NDC_BASE/commissioning_software/kb/postgres/construct_kb"
SQLITE_KB_DIR="$NDC_BASE/commissioning_software/kb/sqlite3/construct_kb"
DCS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

[[ -d "$PG_KB_DIR"     ]] || { echo "missing $PG_KB_DIR"     >&2; exit 1; }
[[ -d "$SQLITE_KB_DIR" ]] || { echo "missing $SQLITE_KB_DIR" >&2; exit 1; }

SECRETS_FILE="${HOME}/.config/nanodatacenter/secrets.env"
[[ -f "$SECRETS_FILE" ]] || { echo "missing $SECRETS_FILE -- run setup_secrets.sh" >&2; exit 1; }
# shellcheck disable=SC1090
source "$SECRETS_FILE"
[[ -n "${PG_PASSWORD:-}" ]] || { echo "PG_PASSWORD not set after sourcing $SECRETS_FILE" >&2; exit 1; }

export POSTGRES_PASSWORD="$PG_PASSWORD"
export LUA_PATH="$SQLITE_KB_DIR/?.lua;$PG_KB_DIR/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"

cd "$SCRIPT_DIR"
luajit slice_bootstrap.lua "$@"

# After slicing, drop a start.sh into each per-CPU dir under deployment/.
# start.sh execs into runtime/dcs_host/dcs.lua via the wired runtime/ link
# (set up later by stage_deploy.sh).
START_TEMPLATE="$SCRIPT_DIR/start.sh.template"
STOP_TEMPLATE="$SCRIPT_DIR/stop.sh.template"
[[ -f "$START_TEMPLATE" ]] || { echo "missing $START_TEMPLATE" >&2; exit 1; }
[[ -f "$STOP_TEMPLATE"  ]] || { echo "missing $STOP_TEMPLATE"  >&2; exit 1; }

shopt -s nullglob
for cpu_dir in "$DCS_ROOT"/deployment/*/; do
    [[ -d "$cpu_dir" ]] || continue
    cp "$START_TEMPLATE" "$cpu_dir/start.sh"
    chmod +x "$cpu_dir/start.sh"
    echo "  wrote ${cpu_dir}start.sh"
    cp "$STOP_TEMPLATE" "$cpu_dir/stop.sh"
    chmod +x "$cpu_dir/stop.sh"
    echo "  wrote ${cpu_dir}stop.sh"
done
