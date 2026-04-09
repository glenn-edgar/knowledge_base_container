#!/bin/bash
# docker_build.sh -- Build the orchestrator container image.
#
# Stages Lua modules and prebuilt libraries into the build context.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BB_DIR="$SCRIPT_DIR/../../luajit_programs_and_containers/building_blocks"

if [ ! -d "$BB_DIR/orchestrator" ]; then
    echo "ERROR: building_blocks/orchestrator not found at: $BB_DIR"
    exit 1
fi

echo "=== Staging orchestrator modules ==="

# Clean previous staging
rm -rf "$SCRIPT_DIR/lua" "$SCRIPT_DIR/prebuilt_libs" \
       "$SCRIPT_DIR/prebuilt_lua_libs" "$SCRIPT_DIR/prebuilt_lua_share"

mkdir -p "$SCRIPT_DIR/lua/orchestrator" \
         "$SCRIPT_DIR/lua/kb_search" \
         "$SCRIPT_DIR/lua/kb_construct" \
         "$SCRIPT_DIR/lua/sqlite_kb" \
         "$SCRIPT_DIR/prebuilt_libs" \
         "$SCRIPT_DIR/prebuilt_lua_libs/dbd" \
         "$SCRIPT_DIR/prebuilt_lua_share"

# Orchestrator modules
cp "$BB_DIR/orchestrator/graph.lua"    "$SCRIPT_DIR/lua/orchestrator/"
cp "$BB_DIR/orchestrator/startup.lua"  "$SCRIPT_DIR/lua/"
cp "$BB_DIR/orchestrator/shutdown.lua" "$SCRIPT_DIR/lua/"
cp "$BB_DIR/orchestrator/kb_build.lua" "$SCRIPT_DIR/lua/"

# KB search (Postgres query interface)
cp "$BB_DIR/knowledge_base/postgres/data_structures/kb_search.lua" \
   "$SCRIPT_DIR/lua/kb_search/"

# KB construction (for kb_build mode — runs master_build.lua)
cp "$BB_DIR/kb_dsl/scripts/master_build.lua"      "$SCRIPT_DIR/lua/"
cp "$BB_DIR/kb_dsl/scripts/site_config.lua"        "$SCRIPT_DIR/lua/"
cp "$BB_DIR/kb_dsl/scripts/physical_tree.lua"      "$SCRIPT_DIR/lua/"
cp "$BB_DIR/kb_dsl/scripts/software_tree.lua"      "$SCRIPT_DIR/lua/"
cp "$BB_DIR/kb_dsl/scripts/planner_tree.lua"       "$SCRIPT_DIR/lua/"
cp "$BB_DIR/kb_dsl/scripts/sqlite_extract.lua"     "$SCRIPT_DIR/lua/"

# Postgres construct_kb modules
cp "$BB_DIR/knowledge_base/postgres/construct_kb/"*.lua \
   "$SCRIPT_DIR/lua/kb_construct/"

# SQLite construct_kb modules (for sqlite_extract)
cp "$BB_DIR/knowledge_base/sqlite3/construct_kb/"*.lua \
   "$SCRIPT_DIR/lua/sqlite_kb/"

# Planner data files (needed by planner_tree during KB build)
for f in "$BB_DIR/kb_dsl/scripts/"*_planner_data.lua; do
    [ -f "$f" ] && cp "$f" "$SCRIPT_DIR/lua/"
done

# Prebuilt shared libraries
# DBI PostgreSQL driver
cp /usr/local/lib/lua/5.1/dbd/postgresql.so "$SCRIPT_DIR/prebuilt_lua_libs/dbd/"

# DBI Lua module
cp /usr/local/share/lua/5.1/DBI.lua "$SCRIPT_DIR/prebuilt_lua_share/"

# dkjson (if available as file)
[ -f /usr/local/share/lua/5.1/dkjson.lua ] && \
    cp /usr/local/share/lua/5.1/dkjson.lua "$SCRIPT_DIR/prebuilt_lua_share/"

# ltree extension for SQLite
cp /usr/local/lib/ltree.so "$SCRIPT_DIR/prebuilt_libs/"

echo "  Staged $(find "$SCRIPT_DIR/lua" -name '*.lua' | wc -l) Lua files"

echo ""
echo "=== Building Docker image ==="
docker build \
    -t nanodatacenter/orchestrator:latest \
    "$SCRIPT_DIR"

echo ""
echo "=== Build complete ==="
docker images nanodatacenter/orchestrator:latest \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
