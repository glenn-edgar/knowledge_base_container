#!/usr/bin/env bash
# =============================================================================
# migrate_phase7.sh -- Phase 7 multi-tenant migration: drop+rebuild prep.
#
# Wipes the deprecated single-tenant data so kb_build (Phase 7 C2) can lay
# down per-tenant paths without conflicting with stale rows / NATS keys.
#
# Default mode is DRY-RUN (lists what would be wiped, changes nothing).
# Pass --apply to actually wipe; requires typing "YES, WIPE" to confirm.
#
# Requires env: APP_SYSTEM, APP_SITE, PG_PASSWORD. Optional env override:
# PG_HOST, PG_PORT, PG_DB, PG_USER, NATS_HOST, NATS_PORT, NATS_NETWORK.
#
# What it touches:
#   PG (knowledge_base):
#     - rows under  system.<sys>.site.<S>.boards.*    (deprecated; new path: planner.<ns>.boards.*)
#     - rows under  system.<sys>.site.<S>.robots.*    (deprecated; new path: planner.<ns>.robots.*)
#   PG (knowledge_base_fs_node + knowledge_base_fs_blob):
#     - fs_node rows under system.<sys>.site.<S>.boards.*  (board content + metadata)
#     - fs_blob rows orphaned by the above (no remaining fs_node refs)
#   NATS-KV:
#     - bucket  <site>_action_server   (was the shared per-site bucket; now per-tenant)
#     - bucket  <site>_mission_log     (same)
#
# What it does NOT touch:
#   - app_containers.*  (infra, owned by node_control; Q2 keeps at site-level)
#   - cpu.*             (infra)
#   - infrastructure.registry.*  (shared resources, Q4)
#   - Any planner.<ns>.* subtree (the NEW path scheme)
#   - Any per-tenant bucket <site>_planner_<ns>_action_server (new buckets)
#
# Per the locked design (project_phase7_multitenant_design.md):
#   Q1=path-prefix scoping  Q2=data-only subtree  Q3=per-tenant NATS bucket
#   Q4=infrastructure.registry.* shared zone  Q5=one planner per tenant
#   Q6=no app auth  Q7=this drop+rebuild approach
# =============================================================================

set -uo pipefail   # NOT -e: handle deletion failures gracefully

usage() {
  cat <<EOF
usage: $0 [--dry-run|--apply] [--help]

Default is --dry-run (no changes; lists what would be wiped).
--apply requires typing "YES, WIPE" to confirm destructive action.

Env required: APP_SYSTEM, APP_SITE, PG_PASSWORD
Env optional: PG_HOST=host.docker.internal  PG_PORT=5432  PG_DB=knowledge_base
              PG_USER=gedgar  NATS_HOST=nats-js-ram  NATS_PORT=4222
              NATS_NETWORK=planner-net
EOF
}

# ----- Parse args -----
APPLY=0
for arg in "$@"; do
  case "$arg" in
    --apply)    APPLY=1 ;;
    --dry-run)  APPLY=0 ;;
    -h|--help)  usage; exit 0 ;;
    *)          echo "unknown arg: $arg" >&2; usage; exit 1 ;;
  esac
done

# ----- Preflight -----
SYSTEM="${APP_SYSTEM:-}"
SITE="${APP_SITE:-}"
PG_HOST="${PG_HOST:-host.docker.internal}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-knowledge_base}"
PG_USER="${PG_USER:-gedgar}"
NATS_HOST="${NATS_HOST:-nats-js-ram}"
NATS_PORT="${NATS_PORT:-4222}"
NATS_NETWORK="${NATS_NETWORK:-planner-net}"

if [ -z "$SYSTEM" ]; then
  echo "ERROR: APP_SYSTEM env var required" >&2; exit 2
fi
if [ -z "$SITE" ]; then
  echo "ERROR: APP_SITE env var required" >&2; exit 2
fi
if [ -z "${PG_PASSWORD:-}" ]; then
  echo "ERROR: PG_PASSWORD env var required" >&2; exit 2
fi

# Site bucket name follows action_server's convention (dots -> underscores)
SITE_BUCKET="${SITE//./_}"

DEPRECATED_BUCKETS=(
  "${SITE_BUCKET}_action_server"
  "${SITE_BUCKET}_mission_log"
)
DEPRECATED_PG_PATHS=(
  "system.${SYSTEM}.site.${SITE}.boards"
  "system.${SYSTEM}.site.${SITE}.robots"
)

PSQL=(env "PGPASSWORD=$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" \
        -U "$PG_USER" -d "$PG_DB" -tA)
NATS=(docker run --rm --network "$NATS_NETWORK" natsio/nats-box:latest \
        nats --server "nats://$NATS_HOST:$NATS_PORT")

# ----- Print plan -----
echo "==============================================================="
echo "Phase 7 migration: drop + rebuild prep"
echo "==============================================================="
if [ "$APPLY" -eq 1 ]; then
  echo "Mode: APPLY (will modify state)"
else
  echo "Mode: DRY-RUN (no changes)"
fi
echo "System: $SYSTEM"
echo "Site:   $SITE"
echo "Pg:     $PG_USER@$PG_HOST:$PG_PORT/$PG_DB"
echo "NATS:   $NATS_HOST:$NATS_PORT (via docker net $NATS_NETWORK)"
echo
echo "Deprecated NATS-KV buckets to delete:"
for b in "${DEPRECATED_BUCKETS[@]}"; do echo "  - $b"; done
echo
echo "Deprecated PG subtrees to delete (knowledge_base + fs_node):"
for p in "${DEPRECATED_PG_PATHS[@]}"; do echo "  - $p.* (subtree)"; done
echo "Plus orphan blobs in knowledge_base_fs_blob (no remaining fs_node refs)."
echo

# ----- Inventory current state -----
echo "==============================================================="
echo "Current state"
echo "==============================================================="
for p in "${DEPRECATED_PG_PATHS[@]}"; do
  count=$("${PSQL[@]}" -c "SELECT count(*) FROM knowledge_base WHERE path <@ '$p'::ltree;" 2>/dev/null || echo "?")
  echo "  pg knowledge_base under '$p': $count rows"
done
fs_count=$("${PSQL[@]}" -c "SELECT count(*) FROM knowledge_base_fs_node WHERE path <@ 'system.${SYSTEM}.site.${SITE}.boards'::ltree;" 2>/dev/null || echo "?")
blob_count=$("${PSQL[@]}" -c "SELECT count(*) FROM knowledge_base_fs_blob;" 2>/dev/null || echo "?")
echo "  pg knowledge_base_fs_node under boards subtree: $fs_count rows"
echo "  pg knowledge_base_fs_blob (all): $blob_count rows"
echo
for b in "${DEPRECATED_BUCKETS[@]}"; do
  # `nats kv info` output line is "          Values Stored: <N>"
  values=$("${NATS[@]}" kv info "$b" 2>/dev/null | grep -E "^[[:space:]]+Values Stored:" | awk '{print $3}')
  if [ -z "$values" ]; then values="(missing)"; fi
  echo "  nats KV bucket '$b': $values values"
done

# ----- Dry-run exit -----
if [ "$APPLY" -eq 0 ]; then
  echo
  echo "==============================================================="
  echo "DRY-RUN complete -- no changes made."
  echo "Re-run with --apply to wipe (requires YES, WIPE confirmation)."
  echo "==============================================================="
  exit 0
fi

# ----- Confirmation gate -----
echo
echo "==============================================================="
echo "*** APPLY MODE -- DESTRUCTIVE ***"
echo "==============================================================="
echo "Type the literal string"
echo "    YES, WIPE"
echo "to proceed, or anything else to abort:"
read -r CONFIRM
if [ "$CONFIRM" != "YES, WIPE" ]; then
  echo "Aborted."
  exit 1
fi

# ----- Wipe PG -----
echo
echo "==============================================================="
echo "Wiping PG"
echo "==============================================================="
for p in "${DEPRECATED_PG_PATHS[@]}"; do
  echo "  DELETE FROM knowledge_base WHERE path <@ '$p'::ltree"
  "${PSQL[@]}" -c "DELETE FROM knowledge_base WHERE path <@ '$p'::ltree;" 2>&1 | head -3
done
echo "  DELETE FROM knowledge_base_fs_node WHERE path <@ 'system.${SYSTEM}.site.${SITE}.boards'::ltree"
"${PSQL[@]}" -c "DELETE FROM knowledge_base_fs_node WHERE path <@ 'system.${SYSTEM}.site.${SITE}.boards'::ltree;" 2>&1 | head -3
echo "  DELETE orphan blobs (no fs_node references)"
"${PSQL[@]}" -c "DELETE FROM knowledge_base_fs_blob WHERE sha256 NOT IN (SELECT DISTINCT sha256 FROM knowledge_base_fs_node WHERE sha256 IS NOT NULL);" 2>&1 | head -3

# ----- Wipe NATS -----
echo
echo "==============================================================="
echo "Wiping NATS-KV"
echo "==============================================================="
for b in "${DEPRECATED_BUCKETS[@]}"; do
  echo "  nats kv del $b --force"
  "${NATS[@]}" kv del "$b" --force 2>&1 | grep -vE "^$" | head -3 || true
done

# ----- Done -----
echo
echo "==============================================================="
echo "DONE -- cluster is clean for Phase 7 path scheme"
echo "==============================================================="
cat <<EOF

Next steps (Phase 7 follow-up commits):

  1. Re-run kb_build with the new per-tenant path scheme (Phase 7 C2).
     kb_build emits boards/robots under planner.<ns>.* per-tenant.

  2. Restart mission_planner_<NN> containers. On first publish each
     creates new per-tenant NATS-KV buckets:
       <site>_planner_<ns>_action_server
       <site>_planner_<ns>_mission_log

  3. Author boards under the new namespace via the board DSL
     (construction/scripts/board_dsl/) and upload them to the
     per-tenant file_store path (planner_ui or compile_board.lua).

  4. Run the Phase 7 cluster smoke (six scenarios in
     project_planner_implementation_plan.md Phase 7 section).
EOF
