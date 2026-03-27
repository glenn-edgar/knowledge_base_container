# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **dual-target S-Expression DSL compiler** that translates a Lua-based domain-specific language into either C binaries or Lua/MicroPython modules. The two targets live in parallel subdirectories:

- **s_expression_c/** — Generates C headers, binary blobs, and a static library (`libs_s_engine.a`) for native execution
- **s_expression_micropython/** — Generates pure Lua/MicroPython modules for interpreted execution on embedded devices

Both share the same DSL compiler frontend (`lua_dsl/s_compile.lua`) but diverge at code generation.

## Build Commands

### Prerequisites
- LuaJIT must be installed and on PATH

### Compile a DSL source file

```bash
# C target (generates headers + binary)
cd s_expression_c
./s_build.sh dsl_tests/basic_primitive_test/basic_primitive_test.lua dsl_tests/basic_primitive_test/

# MicroPython target (generates Lua/Python modules)
cd s_expression_micropython
./s_build.sh dsl_tests/basic_primitive_test/basic_primitive_test.lua dsl_tests/basic_primitive_test/
```

Both invoke: `luajit s_compile.lua <input> --helpers=s_engine_helpers.lua <target_flag> --outdir=<dir>`
- C uses `--all-bin`; MicroPython uses `--micropython`

### Build the C runtime library

```bash
cd s_expression_c/runtime
make          # Builds ../lib/libs_s_engine.a and installs headers to ../include/s_engine/
make clean    # Remove object files
make cleanall # Remove objects, library, and installed headers
```

Compiler: `gcc -Wall -Wextra -std=c11 -O2 -g -DMODULE_IS_64BIT=0`

### Build and run a C test

```bash
cd s_expression_c/dsl_tests/external_tree_test
make        # Compiles main.c + user_functions.c, links against libs_s_engine.a
./main      # Run the test
```

### Run a Lua test (MicroPython target)

```bash
cd s_expression_micropython/dsl_tests/basic_primitive_test
luajit main.lua
```

### Compiler CLI reference

```bash
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --all-bin    # C: all outputs + binary
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --micropython # MicroPython module
luajit s_compile.lua <input.lua> --dump                                       # Debug dump to stdout
luajit s_compile.lua <input.lua> --all                                        # All text outputs (no binary)
luajit s_compile.lua <input.lua> --64bit --all-bin                            # Force 64-bit mode
luajit s_compile.lua --help                                                   # Full help
```

## Architecture

### Compilation Pipeline

```
DSL source (.lua)
    → lua_dsl/s_compile.lua (LuaJIT compiler driver)
        → lua_dsl/s_expr_dsl.lua (parser/validator)
        → lua_dsl/s_engine_helpers.lua (loads 14 helpers from se_helpers_dir/)
        → lua_dsl/s_expr_generators.lua (code generation)
    → C target: headers (.h), binary (.bin), registration (.c)
    → Lua target: _module.lua or _module_mpy.py
```

### Key Design Decisions

- **Hash-based dispatch**: All function names are FNV-1a 32-bit hashed. The runtime looks up builtins and user functions by hash, not string.
- **Flat parameter array**: The binary format (v5.1) serializes tree nodes as a flat array of `s_expr_param_t` structs (8 bytes each in 32-bit mode), enabling zero-copy loading in C.
- **Three function categories**: Functions are classified as `m_call` (main/tick), `p_call` (predicate/boolean), or `o_call`/`io_call` (one-shot). Each has separate dispatch tables.
- **Blackboard pattern**: State is stored in record fields (the "blackboard") accessed by hash. DSL defines record layout; runtime manages field storage.

### Source Layout (within each target directory)

| Directory | Purpose |
|-----------|---------|
| `lua_dsl/` | DSL compiler — `s_compile.lua` (driver), `s_expr_dsl.lua` (parser), `s_expr_generators.lua` (codegen) |
| `lua_dsl/se_helpers_dir/` | 14 modular helper files: predicates, control flow, timing, state machine, equations, etc. |
| `runtime/` | C runtime engine — eval loop, module init, node state, stack, event queue, dictionary |
| `include/s_engine/` | Public C headers (auto-installed by runtime Makefile) |
| `lua_runtime/` | **(MicroPython only)** Pure Lua runtime: `se_runtime.lua` (core engine) + 12 `se_builtins_*.lua` files |
| `dsl_tests/` | ~18 test directories, each with DSL source, test harness, and generated outputs |
| `lib/` | Built artifacts (`libs_s_engine.a` for C) |

### Test Structure

Each test directory (`dsl_tests/<test_name>/`) follows a consistent pattern:
- `<test_name>.lua` — DSL source defining module, records, and trees
- `main.c` or `main.lua` — Test harness that loads module, ticks engine, verifies behavior
- `se_path.lua` — Sets up Lua package paths for the DSL compiler
- Generated outputs (headers, modules, binaries) — produced by `s_build.sh`

Test categories: basic primitives, control flow (sequence, loop, state machine), dispatch, function dictionary, stack operations, equations, JSON, callbacks, and hardware examples (car window controller, external tree).
