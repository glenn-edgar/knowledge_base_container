# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

S-Expression Engine with Lua DSL compiler. A two-stage system:
1. **Lua DSL compiler** (`lua_dsl/`) — LuaJIT scripts that compile Lua DSL definitions into C headers and binary token streams
2. **C runtime** (`runtime/`) — embedded S-expression evaluator that executes the compiled token streams

The engine originated as a microcode layer for ChainTree behavior trees but also operates standalone. It uses S-expression-structured token streams (not bytecode) for control flow, evaluated via a Tcl-like model.

## Build Commands

### Build the runtime library
```bash
cd runtime && make          # builds ../lib/libs_s_engine.a
```
The runtime/src/ directory contains a vendored Lua 5.3 source tree (built separately if needed).

### Compile a DSL script (generate C headers + binary)
```bash
# Using the build script (preferred):
./s_build.sh <entry_point.lua> <output_dir>

# Direct compiler invocation:
cd lua_dsl && luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --all-bin --outdir=<dir>

# Text outputs only (no binary):
luajit s_compile.lua <input.lua> --all --outdir=<dir>

# See all options:
luajit s_compile.lua --help
```

### Build and run a test
```bash
cd dsl_tests/basic_primitive_test && make      # builds 'main' executable
cd dsl_tests/basic_primitive_test && make run   # builds and runs
```

### Clean
```bash
cd dsl_tests/<test> && make clean       # local objects only
cd dsl_tests/<test> && make cleanall    # including runtime lib
cd dsl_tests/<test> && make rebuild     # full clean + rebuild
```

## Architecture

### Compilation Pipeline

```
DSL script (.lua)  -->  s_compile.lua  -->  C headers (.h) + binary (.bin)
                         |                    |
                    lua_dsl/              dsl_tests/<test>/
                    s_expr_dsl.lua            main.c (links against runtime)
                    s_engine_helpers.lua       |
                    s_expr_generators.lua      v
                    s_expr_debug.lua      libs_s_engine.a (runtime library)
```

The compiler outputs per-module:
- `<name>.h` — module header with token enum definitions
- `<name>_records.h` — record/blackboard field definitions
- `<name>_user_functions.h` — user function prototypes
- `<name>_user_registration.c` — function registration code
- `<name>_debug.h` — debug symbol map
- `<name>_32.bin` / `<name>_bin_32.h` — compiled binary token stream (32-bit default)
- `<name>_dump_32.h` — binary dump for inspection

### DSL Helper Modules (`lua_dsl/se_helpers_dir/`)

The helper library is modular, loaded in dependency order by `s_engine_helpers.lua`:
- `se_predicates.lua` — boolean predicate composition (AND/OR/NOT)
- `se_control_flow.lua` — sequence, if/then, fork, while, cond
- `se_timing_events.lua` — waits, delays, verify, event queueing
- `se_state_machine.lua` — state machine and event dispatch
- `se_dictionary.lua` — dictionary/JSON structures
- `se_oneshot.lua` — one-shot operations (log, set field, inc/dec)
- `se_quad_ops.lua` — arithmetic, comparison, logical operations
- `se_stack_frame.lua` — stack frame variable management
- `s_engine_equation.lua` — expression compilation
- `se_chain_tree.lua` — ChainTree integration helpers
- `se_function_dict.lua` — function dictionary support

### C Runtime (`runtime/`)

Key components:
- `s_engine_eval.{c,h}` — core S-expression evaluator
- `s_engine_module.{c,h}` — module loading and management
- `s_engine_node.{c,h}` — tree node evaluation
- `s_engine_init.{c,h}` — engine initialization
- `s_engine_types.h` — core types (`s_expr_param_t` token format, `s_engine_handle_t`, etc.)
- `s_engine_stack.{c,h}` — parameter stack
- `s_engine_event_queue.{c,h}` — event queue system
- `s_engine_builtins.{c,h}` — builtin function dispatch (split across `_delays`, `_dict`, `_flow_control`, `_oneshot`, `_pred`, `_quads`, `_stack`, `_verify`, `_spawn` headers)
- `se_lua53_bridge.{c,h}` — Lua 5.3 runtime integration for hybrid C/Lua user functions

### Test Structure (`dsl_tests/`)

Each test is a self-contained directory with:
- `<name>.lua` — DSL definition script
- `main.c` — test harness that initializes engine, registers user functions, and ticks
- `Makefile` — builds against `../../lib/libs_s_engine.a` and `../../include/s_engine/`
- Generated headers/binaries from the DSL compiler

Tests with user-defined Lua callbacks also include `app_lua_functions.lua`.

## Key Concepts

- **32-bit vs 64-bit mode**: Token size is configurable. Default is 32-bit (`use_32bit()` / `--32bit`). Use `--64bit` for 64-bit targets.
- **Module**: A compiled unit containing trees, records, and function registrations. Created with `start_module()`.
- **Tree**: A single S-expression program within a module. Created with `start_tree()`. Exactly one top-level node required.
- **User functions**: C functions registered at runtime, referenced by hash in the token stream. Three types: oneshot (immediate), main (tick-based), pred (predicate/boolean).
- **Lua functions**: Lua 5.3 functions callable via trampoline bridge (`lua_oneshot()`, `lua_main()`, `lua_pred()`).
- **Blackboard records**: Shared state fields accessible by name hash from the token stream.
- **FNV-1a hashing**: All string references (function names, field names, dictionary keys) are converted to 32-bit FNV-1a hashes at compile time.
