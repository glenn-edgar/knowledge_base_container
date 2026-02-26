#!/bin/bash
# =============================================================================
# s_build_headers.sh - ChainTree JSON/YAML to C Header Generation
#
# Usage:
#   ./s_build_headers.sh <input_file> <output_dir> [handle_name] [--no-support]
#
# Arguments:
#   input_file   : Path to ChainTree .json (preferred) or .yaml configuration
#   output_dir   : Directory where generated .h/.c files will be written
#   handle_name  : Name for the handle type used in all generated symbols
#                  (default: chaintree_handle)
#   --no-support : Skip generating the generic chaintree_support.h/.c files
#                  (useful when multiple instances share a single support file)
#
# Examples:
#   ./s_build_headers.sh dsl_tests/build/config.json ./generated my_controller
#   ./s_build_headers.sh dsl_tests/build/config.yaml ./generated my_controller
#   ./s_build_headers.sh second.json ./output second_tree --no-support
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_file> <output_dir> [handle_name] [--no-support]"
    echo ""
    echo "  input_file   : Path to ChainTree .json or .yaml configuration"
    echo "  output_dir   : Directory for generated .h/.c files"
    echo "  handle_name  : Handle type name (default: chaintree_handle)"
    echo "  --no-support : Skip generating chaintree_support.h/.c"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_DIR="$2"
shift 2

HANDLE_NAME="chaintree_handle"
NO_SUPPORT=""

for arg in "$@"; do
    case "$arg" in
        --no-support)
            NO_SUPPORT="--no-support"
            ;;
        *)
            HANDLE_NAME="$arg"
            ;;
    esac
done

if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file not found: $INPUT_FILE"
    exit 1
fi

PIPELINE_DIR="${SCRIPT_DIR}/lua_dsl/yaml_to_headers_python"
if [ ! -f "${PIPELINE_DIR}/__main__.py" ]; then
    echo "Error: Pipeline not found at: $PIPELINE_DIR"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "ChainTree -> C Header Pipeline"
echo "============================================================"
echo "  Source:      $INPUT_FILE"
echo "  Output:      $OUTPUT_DIR"
echo "  Handle name: $HANDLE_NAME"
[ -n "$NO_SUPPORT" ] && echo "  Support:     skipped (--no-support)"
echo ""

export PYTHONPATH="${SCRIPT_DIR}/lua_dsl:${PYTHONPATH}"

python3 -m yaml_to_headers_python \
    "$INPUT_FILE" \
    "$OUTPUT_DIR" \
    "$HANDLE_NAME" \
    $NO_SUPPORT

echo ""
echo "Generated files in: $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"/*.h "$OUTPUT_DIR"/*.c 2>/dev/null || echo "  (no files generated)"