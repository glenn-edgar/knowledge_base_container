#!/bin/bash
# Launch KB sidecar with correct library paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NATS_LIB_DIR="$SCRIPT_DIR/../../knowledge_base/nats"

export LD_LIBRARY_PATH="$NATS_LIB_DIR:${LD_LIBRARY_PATH:-}"

cd "$SCRIPT_DIR"
exec luajit main.lua "$@"
