# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Directory Layout

This directory contains two variants of the ChainTree codebase:

- **`chain_tree_c/`** — C reference implementation. Full C runtimes, code generation pipelines, S-Expression C engine, and test suites. See `chain_tree_c/CLAUDE.md`.
- **`chain_tree_luajit/`** — Pure LuaJIT port. Replaces C runtimes with LuaJIT modules, loads JSON IR directly (no binary image). See `chain_tree_luajit/CLAUDE.md`.

Both share the same Lua DSL frontend (`lua_dsl/`) and JSON intermediate representation.

## What is ChainTree

ChainTree is a control flow framework that unifies behavior trees, state machines, and sequential control flows. It has two execution engines:
1. **ChainTree** — node structures walked by an engine (iterative DFS)
2. **S-Expression Engine** — flat parameter arrays evaluated by a tick-driven interpreter

## Quick Start

### C variant
```bash
cd chain_tree_c/runtime_binary && make
cd chain_tree_c/dsl_tests/incremental_binary && make
```

### LuaJIT variant
```bash
# Generate JSON IR from DSL
cd chain_tree_luajit && ./s_build_json.sh <lua_file> <output_dir>

# Run with LuaJIT runtime (no compilation needed)
cd chain_tree_luajit && luajit dsl_tests/<test>/test.lua
```

## Prerequisites

- **LuaJIT**: required for both variants (DSL frontend + LuaJIT runtime)
- **lua-cjson**: required for the LuaJIT runtime JSON loader
- **C compiler + make**: required only for `chain_tree_c/`
