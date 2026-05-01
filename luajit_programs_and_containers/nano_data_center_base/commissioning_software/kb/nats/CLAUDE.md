# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

NATS messaging bindings for LuaJIT. Five C libraries with LuaJIT FFI wrappers, providing KeyStore (JetStream KV), KbStore (hierarchical knowledge base), JobQueue, RPC (client/server), and PubSub.

## Architecture

Two layers:
- **C libraries** (`key_store/`, `rpc/`, `pub_sub/`) — each has its own Makefile, `src/`, `include/`, `test/`, and `build/` directories. Built as `.so` shared libs. Dependencies: `libnats` (nats.c), `libcjson`, `libpthread`.
- **LuaJIT FFI bindings** (`lib/`) — one `.lua` file per C library, plus `lib/nats.lua` as unified entry point. Use `ffi.load()` to load the `.so` files at runtime.

Key dependency: KbStore and JobQueue both depend on KeyStore internally (their `.so` files link in `nats_key_store.o`).

Design patterns in the FFI bindings:
- Opaque C handles managed by Lua wrapper objects
- Caller-frees-strings: Lua copies C strings immediately then calls `free()`
- Callbacks (PubSub, RPC server) use `ffi.cast` with GC anchoring
- Status codes map to Lua `error()` — catch with `pcall()`
- `nil` return from get operations means "not found" (no error)
- Each module exposes `_C` for raw FFI library handle access

## Building the C Libraries

Each C subdirectory has its own Makefile:

```bash
# Build all three key_store libraries (KeyStore, KbStore, JobQueue)
cd key_store && make libs

# Build RPC library
cd rpc && make libs

# Build PubSub library
cd pub_sub && make libs
```

Prerequisites: `libcjson-dev`, `nats.c` (built from source), `gcc`.

## Running Tests

All tests require a NATS server at `127.0.0.1:4222`. KeyStore/KbStore/JobQueue tests need JetStream enabled; RPC and PubSub do not.

```bash
# Start NATS with JetStream
docker run -d -p 4222:4222 nats:latest -js

# C-level tests (from each subdirectory)
cd key_store && make run-test      # KV + KB tests
cd key_store && make run-test-jq   # JobQueue tests
cd rpc && make run-test
cd pub_sub && make run-test

# LuaJIT integration tests (from repo root)
export LD_LIBRARY_PATH=/path/to/built/shared/libs
luajit test/test_key_store.lua
luajit test/test_pubsub.lua
luajit test/test_rpc.lua
```

The top-level `.so` symlinks in the repo root are convenience copies; `LD_LIBRARY_PATH` must include wherever the real built libraries live.

## JetStream vs Plain NATS

| Requires JetStream | Plain NATS only |
|---|---|
| KeyStore, KbStore, JobQueue | RPC, PubSub |
