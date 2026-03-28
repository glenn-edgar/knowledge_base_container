#!/usr/bin/env bash
# ===========================================================================
# dsl_tests_micropython — build helper
#
# Usage:
#   ./build.sh              build C binary (incremental_binary)
#   ./build.sh run          build + run C-only test
#   ./build.sh run-mpy      run MicroPython bridge test
#   ./build.sh run-mpy-user validate user_functions.py syntax
#   ./build.sh clean        clean build artifacts
#   ./build.sh clean-all    clean build + libraries
#   ./build.sh rebuild      clean-all + build
# ===========================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/incremental_binary"

cmd="${1:-all}"

make -C "$BUILD_DIR" "$cmd"
