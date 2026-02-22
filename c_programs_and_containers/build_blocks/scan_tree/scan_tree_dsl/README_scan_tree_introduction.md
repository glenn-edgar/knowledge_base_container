# Scan Tree

## Purpose

The scan tree is a logic evaluation subsystem that runs between behavior tree
tick intervals. Its job is to compute hierarchical bitmaps that tell the
behavior tree what is true, what has faulted, and what has never been evaluated.
The behavior tree does not evaluate sensor data or check conditions directly —
it reads the bitmaps the scan tree has already prepared.

This separates the "what is the state of the world" question from the "what
should we do about it" question. The scan tree answers the first question
deterministically and completely before the behavior tree begins its tick.

### Relationship to PLEXIL

This is similar in purpose to the invariant check phase in PLEXIL, where
conditions are evaluated before plan nodes execute. The scan tree differs
in two ways:

1. **Structure over ad-hoc logic.** PLEXIL invariant checks are arbitrary
   boolean expressions attached to individual plan nodes. The scan tree
   organizes all condition evaluation into a declared hierarchy of levels,
   where each level's outputs feed the level above it. The evaluation order
   is fixed by the structure, not determined at runtime by which plan nodes
   happen to be active.

2. **Separation from the plan.** In PLEXIL, invariant conditions are embedded
   in the plan itself — each node carries its own guard expressions. In the
   scan tree architecture, all condition evaluation is extracted into a
   standalone subsystem with its own definition language. The behavior tree
   reads the results but does not participate in the evaluation. This means
   the condition logic can be developed, tested, and verified independently
   of the behavior tree.

The result is a pre-computed truth table that the behavior tree consumes.
Every bit in the hierarchy has a known state (ACTIVE, FAULT, or NOT_OP)
before the first behavior tree node executes.

## System Components

The scan tree has three components: a construction DSL, a C runtime library,
and a code generator that bridges them.

### DSL (LuaJIT)

The DSL defines the structure of the scan tree — what buffers exist, how
they are organized into levels, and what logic connects inputs to outputs.
It uses a stack-based open/close pattern where every `_start` call returns
a handle that must be passed to the matching `_end` call. This enforces
balanced tree construction at definition time.

The DSL outputs a JSON intermediate file. It does not generate C code
directly. This separation allows the JSON to be consumed by other tools
(visualization, validation, documentation generators) without depending
on the C code generator.

**Files:** `scan_tree_dsl.lua`, `vft_helpers.lua`

### Code Generator (LuaJIT)

The code generator reads the JSON intermediate and produces C header files
containing `static const` data tables. No `.c` files are generated. No init
functions. No mutable statics. The generated headers contain only const
descriptor tables that the runtime library walks at init time.

The generator produces up to three headers depending on what the tree uses:

- `{name}.h` — buffer defines, node descriptors, lookup table, system
  descriptor
- `{name}_user_vft.h` — prototypes for application-defined evaluation
  functions (only if user VFTs are present)
- `{name}_fuse_actions.h` — prototypes for fuse action callbacks (only if
  fuses are present)

**Files:** `codegen_c.lua`, `fnv1a.c` (hash function shared library)

### Runtime Library (C99)

The runtime library is the engine that evaluates the scan tree. It takes a
pointer to the const descriptor tables, allocates all working storage in a
single `calloc`, and evaluates the tree bottom-up each cycle.

The library provides:

- **Initialization** — `st_init()` allocates working storage from const
  descriptors. One call, one allocation, deterministic size.
- **Cycle evaluation** — `st_cycle()` detects raw buffer changes, marks
  dirty nodes, evaluates bottom-up, updates the three-state arrays.
- **Buffer access** — type-checked cached pointers for zero-overhead reads,
  FNV-1a key lookup for dynamic access.
- **Builtin VFTs** — 14 system evaluation functions (logic, comparison,
  voting, stateful, protection).
- **Hierarchical display** — ANSI-colored fault tree rendering with cached
  display order.

The runtime has no recursion, no dynamic allocation after init, and compiles
clean under `-Wall -Wextra -std=c99 -O2`. It targets everything from 32KB
ARM Cortex-M to multi-gigabyte servers.

**Files:** `scan_tree.h`, `scan_tree.c`, `builtins.h`, `builtins.c`,
`st_display.h`, `st_display.c`

## Development Flow

```
 1. Define          2. Generate JSON       3. Generate C         4. Build + Run
 ─────────────      ──────────────────     ─────────────────     ──────────────
 Write .lua         luajit my_tree.lua     luajit codegen_c.lua  gcc ... -o app
 - raw buffers      → my_tree.json         my_tree.json out/     ./app
 - levels                                  → my_tree.h
 - sub-levels                              → my_tree_user_vft.h
 - VFTs                                    → my_tree_fuse_actions.h
```

**Step 1: Define.** Write a Lua script that declares raw I/O buffers (sensor
readings, actuator commands, operator signals), organizes them into evaluation
levels, and connects inputs to outputs using VFT instantiations. The DSL
validates buffer types, pin positions, and parameter ranges at definition time.

**Step 2: Generate JSON.** Run the Lua script. It produces a JSON file
describing the complete tree structure — buffers, levels, sub-levels, virtual
functions, and their wiring. This file is human-readable and can be inspected
or processed by other tools.

**Step 3: Generate C.** Run the code generator on the JSON. It produces
header files with const data tables. The application includes the main header
and passes the system descriptor to `st_init()`.

**Step 4: Build and run.** Compile the application against the runtime library
and the generated header. The application writes sensor data to raw buffer
pointers, calls `st_cycle()`, and reads the resulting bitmaps. The behavior
tree reads these bitmaps to make decisions.

### Iteration

The DSL catches structural errors at definition time (unbalanced scopes,
type mismatches, out-of-range pin positions). The code generator catches
wiring errors (missing buffers, role conflicts). The runtime catches access
errors (type-checked raw pointers return NULL on mismatch). Each stage
validates before passing to the next.

To modify the tree, edit the Lua script and re-run steps 2-4. The runtime
library and application code do not change — only the generated header
changes.

## Documentation

| Document | Contents |
|----------|----------|
| [System README](scan_tree_v2/README.md) | DSL API, codegen details, runtime internals, display model |
| [Builtin VFT Reference](builtins_reference/README.md) | All 14 system VFTs — DSL syntax, C signatures, roles, behavior |
| [Pump Station Test](pump_station_test/README.md) | 2-level example with user-defined VFT, test walkthrough |
| [VFT Fuse Test](water_plant_test/README.md) | 4-level example with fuses, fuse concept, 11-step scenario |
