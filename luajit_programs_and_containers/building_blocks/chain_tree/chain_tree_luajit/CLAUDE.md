# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ChainTree LuaJIT is a pure-LuaJIT port of the ChainTree control flow framework. It unifies behavior trees, state machines, and sequential control flows into a single LuaJIT execution engine. The C reference implementation lives in `../chain_tree_c/`.

Both versions share the same Lua DSL frontend and JSON intermediate representation (IR).

## Running the DSL Pipeline

### Stage 1: Lua DSL to JSON IR
```bash
./s_build_json.sh <lua_test_file> <output_directory>
```
Requires `luajit`. Sets `LUA_PATH` to resolve `lua_dsl/lua_support/` modules.

### Stage 2: Load JSON IR in LuaJIT runtime
No code generation step needed — the LuaJIT runtime loads JSON IR directly via `cfl_json_loader.lua`.

## ChainTree LuaJIT Runtime (`runtime/`)

Pure Lua modules replacing the C runtime libraries (`runtime_h/`, `runtime_binary/`, `runtime_functions/` in chain_tree_c).

### Usage
```lua
local cfl_runtime = require("cfl_runtime")
local loader      = require("cfl_json_loader")
local builtins    = require("cfl_builtins")
local sm          = require("cfl_state_machine")

-- Load JSON IR
local flash = loader.load("my_test.json")

-- Register built-in + user functions
loader.register_functions(flash, builtins, sm, user_functions)

-- Create and run
local handle = cfl_runtime.create({ delta_time = 0.1, max_ticks = 500 }, flash)
cfl_runtime.reset(handle)
cfl_runtime.add_test(handle, 0)  -- 0-based KB index
cfl_runtime.run(handle)
```

### Module Architecture
```
cfl_runtime.lua          -- Top-level: create, reset, run, destroy
  cfl_engine.lua         -- Engine: KB activation, node execution, flag management
    cfl_tree_walker.lua  -- Iterative DFS tree walker (port of CT_Tree_Walker.c)
  cfl_event_queue.lua    -- Dual-priority ring buffer
  cfl_timer.lua          -- Timer system (second/minute/hour/day events)
  cfl_blackboard.lua     -- Shared mutable blackboard + constant records
  cfl_json_loader.lua    -- Load JSON IR into runtime Lua tables
  cfl_builtins.lua       -- All built-in main/boolean/one-shot functions
  cfl_state_machine.lua  -- State machine functions
  cfl_common.lua         -- FNV-1a hash, node helpers, per-node state
  cfl_definitions.lua    -- Constants, return codes, event types
```

### Key Design Differences from C Version
- **Pure Lua tables** for all data structures (no FFI)
- **JSON IR loaded directly** via cjson — no binary image (.ctb) needed
- **Node-local state** via `handle.node_state[node_id]` Lua tables (replaces C arena allocator)
- **String-keyed blackboard** (no byte offsets needed in Lua)
- **Function dispatch** via integer index into array (same as C, but holds Lua functions)
- **GC handles memory** — no perm/heap/arena allocators needed

### Function Signatures (same semantics as C)
- **Main**: `fn(handle, bool_fn_idx, node_idx, event_type, event_id, event_data) -> return_code`
- **Boolean**: `fn(handle, node_idx, event_type, event_id, event_data) -> bool`
- **One-shot**: `fn(handle, node_idx) -> nil`

### Return Codes
`CFL_CONTINUE(0)`, `CFL_HALT(1)`, `CFL_TERMINATE(2)`, `CFL_RESET(3)`, `CFL_DISABLE(4)`, `CFL_SKIP_CONTINUE(5)`, `CFL_TERMINATE_SYSTEM(6)`

## S-Expression Engine (`s_expression/`)

Pure LuaJIT port of the S-Expression engine (from `building_blocks/s_expression/lua_runtime/`).

### Usage
```lua
local se = require("se_runtime")
local mod = se.new_module(module_data, builtins)
local inst = se.new_instance(mod, "tree_name")
local result = se.tick(inst)
```

### DSL Compiler
```bash
luajit s_expression/lua_dsl/s_compile.lua <input.lua> --all-bin --outdir=<dir>
```

## Avro Packet DSL (`c_avro_packets/`)

Lua DSL for generating fixed-layout C message structs. Run with `luajit <schema>.lua`.

## JSON IR Schema

The JSON IR (`lua_dsl/README_dsl_schema.md`) is the stable contract between the DSL frontend and all backends. Schema version "1.0". Key structure:
- `nodes{}` — all tree nodes keyed by ltree path, with `label_dict` (functions, links, parent) and `node_dict` (runtime config)
- `ltree_to_index{}` — ltree path to original index mapping
- `kb_metadata{}` — per-KB config (memory factor, aliases)
- `event_string_table{}`, `bitmask_table{}` — name-to-index maps
- `blackboard{}` — optional mutable shared state definition
