#!/bin/bash
# run_all_tests.sh -- Run all virtual planner tests
#
# Usage: ./run_all_tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PASS=0
FAIL=0
TOTAL=0

for test_dir in dsl_tests/*/; do
    if [ -f "$test_dir/main.lua" ]; then
        TOTAL=$((TOTAL + 1))
        echo "========================================"
        if bash run_test.sh "$test_dir"; then
            PASS=$((PASS + 1))
        else
            FAIL=$((FAIL + 1))
            echo "FAILED: $test_dir"
        fi
        echo ""
    fi
done

echo "========================================"
echo "Results: $PASS/$TOTAL passed, $FAIL failed"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
