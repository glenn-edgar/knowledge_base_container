#!/usr/bin/env bash
# stage_deploy.sh -- Wire runtime/ into each deployment/<cpu>/ directory,
# and seed env.sh on first stage.
#
# Expected ordering:
#   1. build_kb.sh          (master PG KB)
#   2. slice_bootstrap.sh   (deployment/<cpu>/bootstrap.db + start.sh)
#   3. stage_deploy.sh      (this script: runtime link + env.sh seed)
#
# Modes:
#   --mode=dev   (default) symlink deployment/<cpu>/runtime -> ../../runtime
#   --mode=prod            copy ../runtime tree into deployment/<cpu>/runtime
#
# env.sh is seeded once from the template below and then OWNED BY
# THE OPERATOR. Subsequent re-stages never overwrite an existing env.sh.
#
# Usage:
#   ./stage_deploy.sh
#   ./stage_deploy.sh --mode=prod
set -euo pipefail

MODE=dev
for arg in "$@"; do
    case "$arg" in
        --mode=dev)  MODE=dev ;;
        --mode=prod) MODE=prod ;;
        *) echo "unknown arg: $arg" >&2; echo "usage: $0 [--mode=dev|--mode=prod]" >&2; exit 2 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DCS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNTIME="$DCS_ROOT/runtime"
DEPLOY="$DCS_ROOT/deployment"

[[ -d "$RUNTIME" ]] || { echo "missing $RUNTIME" >&2; exit 1; }
[[ -d "$DEPLOY"  ]] || { echo "missing $DEPLOY (did slice_bootstrap run?)" >&2; exit 1; }

# Per-CPU staging. Iterates over any directory under deployment/
# that has a bootstrap.db (i.e. was produced by slice_bootstrap).
shopt -s nullglob
cpu_dirs=("$DEPLOY"/*/)
if [[ ${#cpu_dirs[@]} -eq 0 ]]; then
    echo "no deployment/<cpu>/ directories found. Run slice_bootstrap.sh first." >&2
    exit 1
fi

for cpu_dir in "${cpu_dirs[@]}"; do
    cpu_id="$(basename "$cpu_dir")"
    [[ -r "$cpu_dir/bootstrap.db" ]] || {
        echo "skipping $cpu_id: no bootstrap.db" >&2; continue; }

    # --- runtime wire-up ---
    runtime_target="$cpu_dir/runtime"
    case "$MODE" in
        dev)
            rm -rf "$runtime_target"
            ln -s "../../runtime" "$runtime_target"
            echo "  $cpu_id: symlinked runtime/ (dev mode)"
            ;;
        prod)
            rm -rf "$runtime_target"
            cp -r "$RUNTIME" "$runtime_target"
            echo "  $cpu_id: copied runtime/ tree (prod mode)"
            ;;
    esac

    # --- env.sh seed (first time only) ---
    env_file="$cpu_dir/env.sh"
    if [[ ! -f "$env_file" ]]; then
        cat > "$env_file" <<'SEED'
# env.sh -- operator-owned environment for this CPU.
#
# This file is seeded ONCE by stage_deploy.sh. Subsequent re-stages
# will NOT overwrite it. Fill in the blanks below, then run ./start.sh.

# Postgres connect (master-side KB).
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=knowledge_base
export PG_USER=gedgar

# Password. Prefer sourcing from the operator's secrets file rather
# than hardcoding here. The secrets file is optional; if it exists,
# start.sh will source it automatically.
# export POSTGRES_PASSWORD=...

# Optional: log level, etc.
# export DCS_LOG_LEVEL=info
SEED
        chmod 600 "$env_file"
        echo "  $cpu_id: seeded env.sh (OPERATOR: edit before first start)"
    else
        echo "  $cpu_id: env.sh exists; not touched"
    fi
done

echo "stage_deploy complete (mode=$MODE)"
