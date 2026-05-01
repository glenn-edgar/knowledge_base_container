#!/usr/bin/env bash
# =============================================================================
# phase6_preflight.sh -- Pre-rebuild gotcha-catcher for Phase 6.1 cutover.
#
# Read-only. Reports PASS/FAIL per check. Run BEFORE build_kb.sh + slice_
# bootstrap.sh + dcs.lua restart, per PHASE_6_1_HANDOFF.md.
#
# Exits non-zero if any check fails.
# Usage:
#   POSTGRES_PASSWORD=... ./phase6_preflight.sh
# =============================================================================

set -uo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

fail_count=0
warn_count=0

pass()  { printf "  [${GREEN}PASS${NC}] %s\n" "$1"; }
fail()  { printf "  [${RED}FAIL${NC}] %s\n" "$1"; fail_count=$((fail_count + 1)); }
warn()  { printf "  [${YELLOW}WARN${NC}] %s\n" "$1"; warn_count=$((warn_count + 1)); }
info()  { printf "  ----   %s\n" "$1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DCS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NDC_BASE="${NDC_BASE:-$(cd "$DCS_ROOT/../.." && pwd)}"

echo "=== Phase 6.1 pre-flight checks ==="
echo

# -----------------------------------------------------------------------------
echo "[1] dcs.lua processes are STOPPED"
# -----------------------------------------------------------------------------
dcs_pids=$(pgrep -af "dcs\.lua" | grep -v phase6_preflight || true)
if [[ -z "$dcs_pids" ]]; then
  pass "no dcs.lua processes running"
else
  fail "dcs.lua processes still running:"
  echo "$dcs_pids" | sed 's/^/         /'
  info "stop them with: pkill -TERM -f 'start_dcs.sh|dcs.lua' && sleep 5"
fi

# -----------------------------------------------------------------------------
echo
echo "[2] Watchdog scripts are STOPPED"
# -----------------------------------------------------------------------------
wd_pids=$(pgrep -af "start_dcs\.sh\|start\.sh" | grep -v phase6_preflight || true)
if [[ -z "$wd_pids" ]]; then
  pass "no watchdog scripts running"
else
  fail "watchdog scripts still running (would respawn dcs.lua):"
  echo "$wd_pids" | sed 's/^/         /'
fi

# -----------------------------------------------------------------------------
echo
echo "[3] All 4 infra containers UP"
# -----------------------------------------------------------------------------
for name in pg-vector nats-js-ram mosquitto-ram-ws_main kv-bridge; do
  status=$(docker ps --filter "name=^${name}$" --format "{{.Status}}" 2>/dev/null || true)
  if [[ -n "$status" ]] && [[ "$status" == Up* ]]; then
    pass "$name: $status"
  else
    fail "$name not Up (status: ${status:-not found})"
  fi
done

# -----------------------------------------------------------------------------
echo
echo "[4] POSTGRES_PASSWORD env set"
# -----------------------------------------------------------------------------
if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
  pass "POSTGRES_PASSWORD set (${#POSTGRES_PASSWORD} chars)"
else
  fail "POSTGRES_PASSWORD not exported"
  info "try: export POSTGRES_PASSWORD=\$(docker exec pg-vector printenv POSTGRES_PASSWORD)"
fi

# -----------------------------------------------------------------------------
echo
echo "[5] pg-vector accepts queries"
# -----------------------------------------------------------------------------
if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
  if docker exec pg-vector psql -U gedgar -d knowledge_base -c "SELECT 1" >/dev/null 2>&1; then
    pass "psql 'SELECT 1' returned"
  else
    fail "psql 'SELECT 1' failed -- pg-vector may be up but not accepting queries"
  fi
else
  warn "skipped (POSTGRES_PASSWORD missing)"
fi

# -----------------------------------------------------------------------------
echo
echo "[6] No held locks on knowledge_base tables"
# -----------------------------------------------------------------------------
if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
  locks=$(docker exec pg-vector psql -U gedgar -d knowledge_base -tAc "
    SELECT count(*) FROM pg_locks l
     JOIN pg_class c ON l.relation = c.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public'
       AND c.relname LIKE 'knowledge_base%'
       AND l.granted = true
       AND l.mode IN ('AccessExclusiveLock','ExclusiveLock','ShareRowExclusiveLock')
  " 2>/dev/null || echo "?")
  if [[ "$locks" == "0" ]]; then
    pass "no exclusive locks on knowledge_base_* tables"
  elif [[ "$locks" == "?" ]]; then
    warn "lock check query failed (proceed cautiously)"
  else
    fail "$locks exclusive locks held on knowledge_base_* (a stray transaction?)"
    info "investigate: docker exec pg-vector psql -U gedgar -d knowledge_base -c 'SELECT * FROM pg_locks WHERE granted = true AND mode LIKE \"%ExclusiveLock\"'"
  fi
else
  warn "skipped (POSTGRES_PASSWORD missing)"
fi

# -----------------------------------------------------------------------------
echo
echo "[7] Required scripts exist + executable"
# -----------------------------------------------------------------------------
for s in build_kb.sh slice_bootstrap.sh; do
  if [[ -x "$SCRIPT_DIR/$s" ]]; then
    pass "$s present + executable"
  elif [[ -f "$SCRIPT_DIR/$s" ]]; then
    fail "$s present but NOT executable (chmod +x $SCRIPT_DIR/$s)"
  else
    fail "$s missing at $SCRIPT_DIR/"
  fi
done

# -----------------------------------------------------------------------------
echo
echo "[8] Per-CPU deployment dirs exist (for slice output)"
# -----------------------------------------------------------------------------
deploy_root="$DCS_ROOT/deployment"
if [[ -d "$deploy_root" ]]; then
  cpu_dirs=$(find "$deploy_root" -maxdepth 1 -type d -name 'cpu_*' | sort)
  if [[ -n "$cpu_dirs" ]]; then
    while read -r d; do
      pass "$(basename "$d")/ exists"
    done <<< "$cpu_dirs"
  else
    fail "no deployment/cpu_*/ directories found"
  fi
else
  fail "deployment/ not found at $deploy_root"
fi

# -----------------------------------------------------------------------------
echo
echo "[9] Phase 6.1 module files present"
# -----------------------------------------------------------------------------
modules=(
  "$NDC_BASE/commissioning_software/kb/postgres/data_structures/kb_sync_queue.lua"
  "$NDC_BASE/commissioning_software/kb/postgres/construct_kb/construct_sync_queue.lua"
  "$SCRIPT_DIR/subsystems/sync_queues.lua"
  "$SCRIPT_DIR/../runtime/dcs_host/sync_rpc.lua"
)
for m in "${modules[@]}"; do
  if [[ -f "$m" ]]; then
    pass "$(basename "$m") present"
  else
    fail "$(basename "$m") MISSING -- Phase 6.1 commit not on disk"
  fi
done

# -----------------------------------------------------------------------------
echo
echo "[10] Chain-tree JSON regenerated (handlers match)"
# -----------------------------------------------------------------------------
dcs_json="$DCS_ROOT/runtime/chain_tree/dcs.json"
if [[ -f "$dcs_json" ]]; then
  if grep -q "VERIFY_ALL_PEERS_ACTIVE" "$dcs_json"; then
    pass "dcs.json contains VERIFY_ALL_PEERS_ACTIVE (Phase 6.1 handlers wired)"
  else
    fail "dcs.json present but missing Phase 6.1 handlers"
    info "rebuild: cd $DCS_ROOT/runtime/chain_tree && ./build_dsl.sh"
  fi
  if grep -q "VERIFY_SYNC_QUORUM_OR_TIMEOUT\|cluster_go" "$dcs_json"; then
    fail "dcs.json still references deleted handlers (stale)"
  else
    pass "dcs.json clean of deleted bit-mask handler names"
  fi
else
  fail "dcs.json missing at $dcs_json"
fi

# -----------------------------------------------------------------------------
echo
echo "=== Summary ==="
if [[ "$fail_count" -eq 0 ]] && [[ "$warn_count" -eq 0 ]]; then
  printf "${GREEN}all checks passed -- safe to run build_kb.sh${NC}\n"
  exit 0
elif [[ "$fail_count" -eq 0 ]]; then
  printf "${YELLOW}%d warning(s); review then proceed${NC}\n" "$warn_count"
  exit 0
else
  printf "${RED}%d FAILURE(S), %d warning(s) -- DO NOT run build_kb.sh${NC}\n" \
    "$fail_count" "$warn_count"
  exit 1
fi
