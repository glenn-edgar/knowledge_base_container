#!/bin/bash
cd "$(dirname "$0")"
export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH

make || exit 1
echo ""

luajit test/test_kv_store.lua
luajit test/test_queue.lua