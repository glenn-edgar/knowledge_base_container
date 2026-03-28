# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **S-Expression DSL engine** — a LuaJIT-based compiler and multi-target runtime for an S-Expression behavior-tree DSL. It lives under a single directory:

- **s_expression_micropython/** — Contains the DSL compiler, three runtime implementations (C, Lua, MicroPython), and 15 test suites

The compiler frontend (`lua_dsl/s_compile.lua`) generates output for all targets via CLI flags. The primary focus is MicroPython/Lua interpreted execution on embedded devices (Lego SPIKE Prime), though the C runtime and static library are also maintained here.

## Build Commands

### Prerequisites
- LuaJIT must be installed and on PATH

### Compile a DSL source file

```bash
cd s_expression_micropython
./s_build.sh dsl_tests/basic_primitive_test/basic_primitive_test.lua dsl_tests/basic_primitive_test/
```

This invokes: `luajit s_compile.lua <input> --helpers=s_engine_helpers.lua --micropython --outdir=<dir>`

The `s_build.sh` script is hardcoded to `--micropython`. To generate other targets, call the compiler directly:
```bash
cd s_expression_micropython/lua_dsl
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --all-bin --outdir=<dir>    # C headers + binary
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --lua --outdir=<dir>        # Lua module
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --micropython --outdir=<dir> # MicroPython module
```

### Build the C runtime library

```bash
cd s_expression_micropython/runtime
make          # Builds ../lib/libs_s_engine.a and installs headers to ../include/s_engine/
make clean    # Remove object files
make cleanall # Remove objects, library, and installed headers
```

Compiler: `gcc -Wall -Wextra -std=c11 -O2 -g -DMODULE_IS_64BIT=0`

### Run a Lua test

```bash
cd s_expression_micropython/dsl_tests/basic_primitive_test
luajit main.lua
```

### Run a MicroPython test

```bash
cd s_expression_micropython/dsl_tests/basic_primitive_test
micropython main_mpy.py
```

### Compiler CLI reference

```bash
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --all-bin     # C: all outputs + binary
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --micropython  # MicroPython module
luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --lua          # Lua module
luajit s_compile.lua <input.lua> --dump                                        # Debug dump to stdout
luajit s_compile.lua <input.lua> --all                                         # All text outputs (no binary)
luajit s_compile.lua <input.lua> --64bit --all-bin                             # Force 64-bit mode
luajit s_compile.lua --help                                                    # Full help
```

## Architecture

### Compilation Pipeline

```
DSL source (.lua)
    → lua_dsl/s_compile.lua (LuaJIT compiler driver)
        → lua_dsl/s_expr_dsl.lua (parser/validator)
        → lua_dsl/s_engine_helpers.lua (loads 14 helpers from se_helpers_dir/)
        → lua_dsl/s_expr_generators.lua (code generation)
    → C target:    headers (.h), binary (.bin), registration (.c)
    → Lua target:  <name>_module.lua
    → MicroPython: <name>_module_mpy.py
```

### Key Design Decisions

- **Hash-based dispatch**: All function names are FNV-1a 32-bit hashed. The runtime looks up builtins and user functions by hash, not string.
- **Flat parameter array**: The binary format (v5.1) serializes tree nodes as a flat array of `s_expr_param_t` structs (8 bytes each in 32-bit mode), enabling zero-copy loading in C.
- **Three function categories**: Functions are classified as `m_call` (main/tick), `p_call` (predicate/boolean), or `o_call`/`io_call` (one-shot). Each has separate dispatch tables.
- **Blackboard pattern**: State is stored in record fields (the "blackboard") accessed by hash. DSL defines record layout; runtime manages field storage.

### Source Layout (under `s_expression_micropython/`)

| Directory | Purpose |
|-----------|---------|
| `lua_dsl/` | DSL compiler — `s_compile.lua` (driver), `s_expr_dsl.lua` (parser), `s_expr_generators.lua` (codegen), `s_expr_debug.lua` |
| `lua_dsl/se_helpers_dir/` | 14 modular helper files: predicates, control flow, timing, state machine, equations, dictionary, etc. |
| `runtime/` | C runtime engine (39 .c/.h files) — eval loop, module init, node state, stack, event queue, dictionary |
| `include/s_engine/` | Public C headers (auto-installed by runtime Makefile) |
| `lib/` | Built artifact: `libs_s_engine.a` |
| `lua_runtime/` | Pure Lua runtime: `se_runtime.lua` (core engine) + 11 `se_builtins_*.lua` files + `se_stack.lua` |
| `mpy_runtime/` | MicroPython runtime: `se_runtime.py` + `se_runtime_spike.py` (SPIKE Prime variant) + 12 `se_builtins_*.py` files + `se_stack.py` |
| `dsl_tests/` | 15 test directories, each with DSL source, test harness, and generated outputs |
| `dsl_tests/docs/` | Design and reference documentation (~20 READMEs) |
| `docs/` | MkDocs source; `site/` has built output |

### Three Runtimes

All three runtimes implement the same builtin function categories and share identical semantics:

| Category | C header | Lua file | Python file |
|----------|----------|----------|-------------|
| Core engine | `s_engine_eval.c` | `se_runtime.lua` | `se_runtime.py` |
| Delays/timing | `s_engine_builtins_delays.h` | `se_builtins_delays.lua` | `se_builtins_delays.py` |
| Dictionary | `s_engine_builtins_dict.h` | `se_builtins_dict.lua` | `se_builtins_dict.py` |
| Dispatch | `s_engine_builtins_dispatch.h` | `se_builtins_dispatch.lua` | `se_builtins_dispatch.py` |
| Flow control | `s_engine_builtins_flow_control.h` | `se_builtins_flow_control.lua` | `se_builtins_flow_control.py` |
| One-shot | `s_engine_builtins_oneshot.h` | `se_builtins_oneshot.lua` | `se_builtins_oneshot.py` |
| Predicates | `s_engine_builtins_pred.h` | `se_builtins_pred.lua` | `se_builtins_pred.py` |
| Quads | `s_engine_builtins_quads.h` | `se_builtins_quads.lua` | `se_builtins_quads.py` |
| Return codes | `s_engine_builtins_return_codes.h` | `se_builtins_return_codes.lua` | `se_builtins_return_codes.py` |
| Spawn | `s_engine_builtins_spawn.h` | `se_builtins_spawn.lua` | `se_builtins_spawn.py` |
| Stack | `s_engine_builtins_stack.h` | `se_builtins_stack.lua` | `se_builtins_stack.py` |
| Verify | `s_engine_builtins_verify.h` | `se_builtins_verify.lua` | `se_builtins_verify.py` |

The MicroPython runtime also has `se_runtime_spike.py` for Lego SPIKE Prime hardware adaptation.

### DSL Compiler Helpers (se_helpers_dir/)

14 modular helpers loaded by `s_engine_helpers.lua`:

| Helper | Domain |
|--------|--------|
| `se_predicates.lua` | Boolean predicate functions |
| `se_control_flow.lua` | Sequence, loop, while |
| `se_state_machine.lua` | State machine composite |
| `s_engine_equation.lua` | Stack-based equations |
| `se_timing_events.lua` | Wait, delay, timeout |
| `se_dictionary.lua` | Dictionary/blackboard ops |
| `se_function_dict.lua` | Function dictionary dispatch |
| `se_oneshot.lua` | One-shot action functions |
| `se_stack_frame.lua` | Stack frame management |
| `se_quad_ops.lua` | Quad operations |
| `se_p_quad_ops.lua` | Predicate quad operations |
| `se_result_codes.lua` | Return code handling |
| `se_field_validation.lua` | Field validation |
| `se_chain_tree.lua` | ChainTree integration bridge |

### Test Structure

Each test directory (`dsl_tests/<test_name>/`) follows a consistent pattern:
- `<test_name>.lua` — DSL source defining module, records, and trees
- `main.lua` — LuaJIT test harness
- `main_mpy.py` — MicroPython test harness (where applicable)
- `se_path.lua` — Sets up Lua package paths for the runtime
- `<test_name>_module.lua` — Generated Lua module
- `<test_name>_module_mpy.py` — Generated MicroPython module

Test categories (15 tests): basic_primitive_test, advanced_primitive_test, callback_function, car_window_controller, complex_sequence, dispatch, external_tree_test, function_dictionary, json, loop_test, return_test, stack_equations, stack_test, state_machine.
