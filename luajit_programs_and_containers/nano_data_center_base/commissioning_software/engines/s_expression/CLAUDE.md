# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A LuaJIT S-Expression engine — both a **DSL compiler** (`lua_dsl/`) that generates C headers, binary modules, and Lua module files, and a **pure-Lua runtime** (`lua_runtime/`) that executes those modules. This is a port of a C-based ChainTree S-Expression engine to LuaJIT.

## Build & Run Commands

### Compile a DSL test to a Lua module
```bash
# From repo root:
./s_build.sh dsl_tests/complex_sequence/complex_sequence.lua dsl_tests/complex_sequence/

# Or directly (from lua_dsl/):
cd lua_dsl && luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --lua --outdir=<dir>
```

### Run a test
```bash
# Each test dir has a main.lua that is the test harness:
cd dsl_tests/basic_primitive_test && luajit main.lua

# Some tests accept module name and tree name/hash args:
cd dsl_tests/dispatch && luajit main.lua my_module 0xDEADBEEF
```

### Compiler help
```bash
cd lua_dsl && luajit s_compile.lua --help
```

### Generate C headers + binary (for C runtime, not Lua runtime)
```bash
cd lua_dsl && luajit s_compile.lua <input.lua> --helpers=s_engine_helpers.lua --all-bin --outdir=<dir>
```

## Architecture

### Two-phase system

1. **DSL Compiler** (`lua_dsl/s_compile.lua`) — reads a Lua DSL file (e.g., `basic_primitive_test.lua`) that uses global DSL functions (`start_module`, `se_sequence`, `se_pred_and`, etc.) to define trees. Outputs `_module.lua` files containing serialized module data as Lua tables.

2. **Runtime** (`lua_runtime/se_runtime.lua`) — pure execution machinery with no builtins embedded. Takes a compiled module + user-supplied builtin tables, creates instances, and ticks them.

### DSL Compiler internals (`lua_dsl/`)
- `s_compile.lua` — CLI entry point and code generation (C headers, binary, Lua module serialization)
- `s_expr_dsl.lua` — core DSL library defining all global functions available in DSL files (node builders, predicates, etc.)
- `s_expr_generators.lua` — C header and binary output generators
- `s_expr_debug.lua` — debug output generation
- `s_engine_helpers.lua` — modular loader that includes all `se_helpers_dir/*.lua` sub-modules (equations, predicates, control flow, timing, state machines, quads, dictionaries, stack frames, chain tree functions)

### Runtime internals (`lua_runtime/`)
- `se_runtime.lua` — the engine: `new_module()`, `new_instance()`, `tick()`, `register_fns()`, `validate_module()`. Implements INIT/TICK/TERMINATE lifecycle for all call types (m_call, pt_m_call, o_call, io_call, p_call, p_call_composite).
- `se_stack.lua` — call-stack data structure for stack frame builtins
- `se_builtins_*.lua` — 11 builtin modules, each returning a table of functions. Registered via `se_runtime.register_fns(mod, table)`.

### Test structure (`dsl_tests/`)
Each test directory contains:
- `<name>.lua` — DSL source defining the module's trees and predicates
- `<name>_module.lua` — pre-compiled module data (output of s_compile.lua)
- `main.lua` — test harness that loads runtime, registers builtins, creates instance, runs tick loop
- `se_path.lua` — path setup (prepends `../../lua_runtime/` to `package.path`)
- `user_functions.lua` — test-specific user functions (predicates, oneshots) matching C test harness

### Key design decisions
- **Tree structure preserved**: module_data nodes are used directly as nested tables, not flattened to a param array. `children[]` and `params[]` are separate arrays on each node.
- **Builtins are user-supplied**: engine has zero builtins. All are passed in via `register_fns()` or `merge_fns()`.
- **Blackboard is a plain Lua table**: string-keyed, lives on `inst.blackboard`.
- **Time source is injectable**: `mod.get_time` defaults to `os.clock`, override after `new_module()`.
- **Node call types**: `m_call` (main), `pt_m_call` (pointer-passing main), `o_call`/`io_call` (oneshot), `p_call`/`p_call_composite` (predicate).

## Important Conventions

- All runtime `require()` paths assume `lua_runtime/` is on `package.path`. Tests use `se_path.lua` to set this up.
- DSL compiler must run from the `lua_dsl/` directory (helpers use `dofile()` with relative paths).
- Node indices are 0-based internally (matching C), but Lua arrays are 1-based. Child access uses `node.children[i+1]` where `i` is the 0-based index.
- Params in Lua tree layout: non-callable params go to `node.params[]`, callable children go to `node.children[]` — this differs from the C flat layout where both share one array.
