# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ChainTree is a control flow framework that unifies behavior trees, state machines, and sequential control flows into a single C execution engine. It targets platforms from 32KB ARM Cortex-M microcontrollers to 8GB+ servers.

## Build Commands

### Core runtime library (`runtime_h/`)
```bash
cd runtime_h && make          # builds libcfl_core.a and libcfl_core.so
cd runtime_h && make clean
```

### Binary-image runtime variant (`runtime_binary/`)
```bash
cd runtime_binary && make     # builds libcfl_binarycore.a and libcfl_binarycore.so
```

### Runtime functions library (`runtime_functions/`)
```bash
cd runtime_functions && make  # builds libcfl_core_functions.a/.so
```
Depends on `runtime_binary/include/` headers.

### Test applications (`dsl_tests/`)
```bash
# Header-based test (links runtime_h + runtime_functions)
cd dsl_tests/incremental_build && make

# Binary-image test (links runtime_binary + runtime_functions, auto-rebuilds libs)
cd dsl_tests/incremental_binary && make
cd dsl_tests/incremental_binary && make clean-all   # cleans app + both libraries
```

### Cross-compilation for 32-bit ARM
```bash
make CC=arm-none-eabi-gcc CFLAGS+="-DCFL_32BIT -mcpu=cortex-m4 -mthumb"
```

## Code Generation Pipeline

The system uses a two-stage pipeline: Lua DSL frontend produces JSON IR, then backend code generators consume it.

### Stage 1: Lua DSL to JSON
```bash
./s_build_json.sh <lua_test_file> <output_directory>
```
Requires `luajit`. Sets `LUA_PATH` to resolve `lua_dsl/lua_support/` modules.

### Stage 2: JSON to C headers (two equivalent backends)
```bash
# LuaJIT backend
./s_build_headers_luajit.sh <input.json> <output_dir> [handle_name] [--no-support]

# Python backend
python -m lua_dsl.yaml_to_headers_python <input.json> <output_dir> [handle_name] [--no-support]
```

### Stage 2 (alternative): JSON to binary image (.ctb)
```bash
./s_build_headers_binary.sh <input.json> <output_dir> [handle_name]
```
Produces `{handle_name}.ctb` (mmap-loadable) and `{handle_name}_image.h` (C array for firmware embedding). Requires `libfnv1a.so` in `lua_dsl/binary_image/`.

## Architecture

### Two runtime variants

- **`runtime_h/`** — Header-based runtime (`libcfl_core`). The codegen pipeline generates 9 matched .h/.c file pairs that compile directly into the application.
- **`runtime_binary/`** — Binary-image runtime (`libcfl_binarycore`). Loads a single `.ctb` binary image at runtime via mmap, with zero-copy and no generated C code. Functions are resolved by FNV-1a 32-bit hash at startup.

Both share the same layered internal architecture:
```
cfl_runtime → cfl_engine → {cfl_event_queue, cfl_timer_system, cfl_heap_arena} → {cfl_heap, cfl_perm} → cfl_global_definitions
```

Most application code only needs `#include "cfl_runtime.h"`.

### Code generation pipeline stages (shared by LuaJIT and Python backends)

1. **stage1_handle** — Load JSON IR
2. **stage2_node_index** — Build node indices
3. **stage3_function_index** — Build function indices (main, one-shot, boolean)
4. **stage4_link_table** — Build link tables
5. **stage5_node_data** — Encode node data (JSON records)
6. **stage6_codegen** — Emit C headers/source, OR **stage6_binary** — Emit `.ctb` binary image

### S-Expression Engine (`s_expression/`)

A separate execution engine that compiles Lua DSL definitions into flat parameter arrays (ROM) and evaluates them via a tick-driven interpreter. Designed for the same embedded targets as ChainTree.

**Building the runtime library:**
```bash
cd s_expression/runtime && make          # builds ../lib/libs_s_engine.a, installs headers to ../include/s_engine/
cd s_expression/runtime && make cleanall # removes lib + installed headers
```

**Compiling DSL test definitions:**
```bash
# Via wrapper script (recommended)
./s_expression/s_build.sh <entry_point.lua> <output_dir>

# Direct invocation with full options
luajit s_expression/lua_dsl/s_compile.lua <input.lua> --all-bin --outdir=<dir>
luajit s_expression/lua_dsl/s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --all-bin
luajit s_expression/lua_dsl/s_compile.lua <input.lua> --dump   # debug dump
luajit s_expression/lua_dsl/s_compile.lua <input.lua> --64bit --all-bin  # 64-bit mode
```

The compiler produces: `_test.h` (tree hashes), `_records.h` (record definitions), `_user_functions.h` (function prototypes), `_user_registration.c` (registration tables), `_bin_32.h` (binary ROM), and `_debug.h`.

**Building and running tests:**
```bash
cd s_expression/dsl_tests/<test_name> && make      # builds and links against libs_s_engine.a
cd s_expression/dsl_tests/<test_name> && make run   # build + execute
cd s_expression/dsl_tests/<test_name> && make rebuild  # clean everything including runtime, then build
```

**Key architecture differences from ChainTree:**
- Trees are flat parameter arrays (`s_expr_param_t`, 8 bytes on 32-bit, 16 bytes on 64-bit) evaluated by an interpreter rather than compiled node structures walked by an engine.
- Three function types: `main` (returns `s_expr_result_t`), `oneshot` (void), `pred` (returns bool) — resolved by FNV-1a hash at module init.
- Blackboard system: typed record/field access via generated field offsets. DSL defines `RECORD`/`FIELD`, C code accesses via `S_EXPR_GET_FIELD(inst, param, type)`.
- Built-in composites include `se_event_dispatch`, `se_field_dispatch`, `se_state_machine`, sequences, loops, delays, and spawn.
- 32/64-bit controlled by `MODULE_IS_64BIT` define (default 0). Affects hash width (FNV-1a 32 vs 64-bit), parameter struct size, and numeric types (`ct_int_t`, `ct_float_t`).
- Event queue (16 slots) with tick-based processing: regular `SE_EVENT_TICK` plus user events via save/set/execute/restore pattern on `tick_type`.

### Avro packet DSL (`c_avro_packets/`)

Separate Lua DSL for generating fixed-layout C message structs for embedded wire protocols. Run schema files directly with `luajit <schema>.lua`. Produces `.h` headers, optional `.bin` blobs, and const packet initializers. Integrates with ChainTree streaming subsystem via `make_port` / `make_control_port`.

### Shared Blackboard (`cfl_blackboard.h`)

A single mutable blackboard shared across all knowledge bases, with support for read-only constant records. Defined in both `runtime_h/` and `runtime_binary/`.

- **Types**: `cfl_bb_field_t` (field descriptor), `cfl_bb_record_t` (blackboard layout), `cfl_bb_const_record_t` (ROM constant record), `cfl_bb_table_t` (groups blackboard + const records, stored in `chaintree_handle_t`).
- **Lifecycle**: `cfl_bb_init()` allocates from `cfl_perm` and copies defaults during `cfl_runtime_create()`. `cfl_bb_reset()` restores defaults during `cfl_runtime_reset()`.
- **Fast access** (compile-time offsets): `CFL_BB_FIELD(handle, offset, type)` and `CFL_BB_CONST_FIELD(data_ptr, offset, type)`.
- **Dynamic access** (by FNV-1a hash): `cfl_bb_field_by_hash()`, `cfl_bb_field_by_name()`, typed getters/setters (`cfl_bb_get_int32`, `cfl_bb_set_float`, etc.).
- **Constant records**: `cfl_bb_const_find(handle, hash)` returns a `cfl_bb_const_record_t*` pointing into ROM. Fields within accessed via `cfl_bb_const_field_by_hash()`.
- If `flash_handle->bb_table` is NULL, the blackboard is silently skipped (backward compatible).
- **DSL definition** (can appear anywhere in the Lua file):
  ```lua
  ct:define_blackboard("system_state")       -- one per configuration
      ct:bb_field("mode",        "int32",  0)
      ct:bb_field("temperature", "float",  20.0)
      ct:bb_field("debug_ptr",   "uint64", 0)
  ct:end_blackboard()

  ct:define_const_record("calibration")      -- unlimited, unique names
      ct:const_field("gain",   "float",  1.5)
      ct:const_field("max",    "int32",  1000)
  ct:end_const_record()
  ```
  Supported types: `int32`, `uint32`, `uint16`, `float`, `uint64`. Fields are auto-aligned. Default values are copied into the blackboard at startup and restored on reset.
- **Pipeline output (binary path)**: JSON IR gets a `"blackboard"` top-level key. `stage6_binary` embeds BBRD and CREC sections in the `.ctb` image (field descriptors with FNV-1a hashes, typed defaults/data blobs). `cfl_image_loader.c` parses these sections and populates `handle.bb_table` automatically. Also emits `{name}_blackboard.h` with offset `#define`s only — no generated C code.
- **Pipeline output (header path)**: `stage6_codegen` generates `{name}_blackboard.h` (offset `#define`s, extern declarations) and `{name}_blackboard.c` (field descriptors, defaults blob, hash init function, `bb_table`). The generated handle `.c` sets `.bb_table = &{uid}_bb_table`.

### Key design details

- Platform auto-detects 64-bit; override with `-DCFL_32BIT` or `-DCFL_64BIT`. Controls alignment (4 vs 8 bytes) and event queue widths.
- Memory: permanent bump allocator (`cfl_perm`), general heap with coalescing (`cfl_heap`), up to 254 concurrent arenas (`cfl_heap_arena_allocate`).
- Binary image format uses magic `CTB1`, CRC32 checksums, FNV-1a function hashing with collision detection at generation time.
- JSON IR (see `lua_dsl/README_dsl_schema.md`) is the stable contract between frontends and backends. `schema_version: "1.0"`.
- Link order matters when linking: functions library before core library (e.g., `$(FUNC_LIB) $(CORE_LIB) -lm`).
