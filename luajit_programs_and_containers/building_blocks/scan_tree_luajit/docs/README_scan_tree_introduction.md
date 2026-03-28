# Scan Tree — LuaJIT Runtime

## Purpose

The scan tree is a logic evaluation subsystem that runs between behavior tree
tick intervals. Its job is to compute hierarchical bitmaps that tell the
behavior tree what is true, what has faulted, and what has never been evaluated.
The behavior tree does not evaluate sensor data or check conditions directly —
it reads the bitmaps the scan tree has already prepared.

This separates the "what is the state of the world" question from the "what
should we do about it" question. The scan tree answers the first question
deterministically and completely before the behavior tree begins its tick.

## LuaJIT Port

This is a pure LuaJIT port of the scan_tree_c runtime. The C system generates
const C data tables from a LuaJIT DSL. This port replaces the C runtime and
C code generator with LuaJIT equivalents, while reusing the same DSL and JSON
intermediate format.

### What Changed

| Component | C Version | LuaJIT Version |
|-----------|-----------|----------------|
| DSL | `scan_tree_dsl.lua` | Same (shared) |
| VFT helpers | `vft_helpers.lua` | Same (shared) |
| JSON intermediate | `{name}.json` | Same format |
| Code generator | `codegen_c.lua` → `.h` files | `codegen_luajit.lua` → `.lua` descriptor |
| Runtime engine | `scan_tree.c` (C99) | `st_runtime.lua` (LuaJIT) |
| Builtins | `builtins.c` (14 VFTs) | `st_builtins.lua` (13 VFTs) |
| Display | `st_display.c` | `st_display.lua` |
| User VFTs | Separate `.c` file | Lua table passed to descriptor |

### What Stayed the Same

- DSL syntax and semantics — identical `.lua` DSL source files
- JSON intermediate format — byte-for-byte compatible
- Buffer addressing — 0-indexed positions, `buffer:start-count` format
- Three-state model — ACTIVE (1), FAULT (0), NOT_OP (-1)
- Change-driven evaluation — raw dependency bitmasks, dirty marking
- Hierarchical ANSI display — same visual output as C version
- FNV-1a path lookup — same hash algorithm, binary search

## System Components

### DSL (Shared with scan_tree_c)

The DSL defines the structure of the scan tree — what buffers exist, how they
are organized into levels, and what logic connects inputs to outputs. It uses a
stack-based open/close pattern where every `_start` call returns a handle that
must be passed to the matching `_end` call.

The DSL outputs a JSON intermediate file. This separation allows the same JSON
to be consumed by either the C code generator or the LuaJIT code generator.

**Files:** `scan_tree_c/scan_tree_dsl/scan_tree_dsl.lua`, `scan_tree_c/scan_tree_dsl/vft_helpers.lua`

### Code Generator (codegen_luajit.lua)

The code generator reads the JSON intermediate and produces a Lua module
returning a factory function. Calling the factory (with optional user VFTs)
returns a system descriptor table that the runtime consumes.

The generator produces one file:

- `{name}.lua` — buffer descriptors, node descriptors with function references,
  lookup table, raw dependency bitmasks, buffer ID constants

**Files:** `codegen_luajit.lua`

### Runtime Library (LuaJIT)

The runtime is the engine that evaluates the scan tree. It takes a descriptor
table (produced by codegen), creates all working storage, and evaluates the
tree bottom-up each cycle.

The library provides:

- **Initialization** — `Handle.new(desc)` creates working storage from the
  descriptor table. Raw buffers get double-buffered 0-indexed arrays. Layer
  buffers get value/not_active/shadow/states arrays.
- **Cycle evaluation** — `handle:cycle()` detects raw buffer changes, marks
  dirty nodes via bitmask, evaluates bottom-up, updates three-state arrays.
- **Buffer access** — `handle:buf_data(buf_id)` for data, `handle:raw_current(buf_id)`
  for write access, `handle:layer_states(buf_id)` for state arrays,
  `handle:lookup_path(path)` for dynamic access.
- **Builtin VFTs** — 13 system evaluation functions in `st_builtins.lua`.
- **Hierarchical display** — ANSI-colored fault tree rendering in `st_display.lua`.

**Files:** `st_runtime.lua`, `st_builtins.lua`, `st_display.lua`

## Development Flow

```
 1. Define          2. Generate JSON       3. Generate Lua       4. Run
 ─────────────      ──────────────────     ─────────────────     ──────────
 Write .lua DSL     luajit my_tree.lua     luajit codegen_luajit my_tree.json
 - raw buffers      → my_tree.json         → my_tree.lua         luajit main.lua
 - levels
 - sub-levels
 - VFTs
```

Or use the build script:

```bash
./st_build.sh my_tree_dsl.lua output_dir/
```

This runs steps 2 and 3. The DSL source requires `scan_tree_dsl.lua` and
`vft_helpers.lua` from `scan_tree_c/scan_tree_dsl/` on the `LUA_PATH`.

## Documentation

| Document | Contents |
|----------|----------|
| [Design Overview](README_scan_tree_introduction.md) | This document — purpose, components, development flow |
| [DSL and Runtime Library](README_dsl_runtime_library.md) | Runtime API, evaluation model, buffer access patterns |
| [Builtin VFT Reference](README_builtin_functions.md) | All 13 system VFTs — DSL syntax, Lua signatures, roles, behavior |
| [Pump Station Test](README_pump_test.md) | 2-level example with user VFT, test walkthrough |
| [VFT Fuse Test](README_vtf_fuse_test.md) | 4-level example with fuses, 11-step scenario |
