#!/bin/bash
# Build + run the KB0 host test (static-link chain_tree, --gc-sections).
#   kb0.lua -> kb0.json -> generated incr/ -> compiled with the static runtime libs.
set -e
CT="$(cd "$(dirname "$0")/../../.." && pwd)"     # chain_tree_c root
export LUA_PATH="${CT}/lua_dsl/?.lua;${CT}/lua_dsl/?/init.lua;${CT}/?.lua;;"

cd "$(dirname "$0")"
rm -rf incr && mkdir -p incr

echo "== gen: kb0.lua -> kb0.json -> incr/ =="
luajit kb0.lua kb0.json >/dev/null
luajit "${CT}/lua_dsl/luajit_pipeline/main.lua" kb0.json incr chaintree_handle >/dev/null

echo "== compile (gc-sections) =="
gcc -O2 -std=c11 -D_POSIX_C_SOURCE=200809L -ffunction-sections -fdata-sections \
    -Iincr -I"${CT}/runtime_h/include" -I"${CT}/runtime_functions/include" \
    main.c user_one_shot_functions.c incr/*.c \
    "${CT}/runtime_functions/libcfl_core_functions.a" "${CT}/runtime_h/libcfl_core.a" -lm \
    -Wl,--gc-sections -o kb0_test

echo "== run =="
./kb0_test
