# Scan Tree LuaJIT — DSL and Runtime Library

A hierarchical bitmap-driven fault evaluation engine. Defines I/O structure
and fault logic in a LuaJIT DSL, generates Lua const descriptor tables, and
evaluates bottom-up at runtime with change-driven execution.

## System Architecture

```
  ┌────────────┐       ┌────────────┐       ┌─────────────────┐
  │  Lua DSL   │──────>│   JSON     │──────>│  LuaJIT Codegen │
  │            │       │ intermediate│       │                 │
  │ buffers    │       │            │       │ {name}.lua      │
  │ levels     │       │            │       │                 │
  │ sub-levels │       │            │       │                 │
  │ VFTs       │       │            │       │                 │
  └────────────┘       └────────────┘       └────────┬────────┘
                                                     │
                                                     v
  ┌──────────────────────────────────────────────────────────────┐
  │                   LuaJIT Runtime                             │
  │                                                              │
  │  st_runtime.lua  - engine: init, cycle, evaluate, lookup     │
  │  st_builtins.lua - system VFTs: and, or, fuse, comparisons  │
  │  st_display.lua  - hierarchical ANSI-colored fault display   │
  │  user_functions.lua - application-provided VFTs              │
  │                                                              │
  │  Application requires descriptor, passes user VFTs, runs     │
  └──────────────────────────────────────────────────────────────┘
```

The generated `.lua` module returns a factory function. The runtime creates a
Handle from the descriptor and manages all working storage internally.

## Data-Flow Evaluation

The scan tree uses **change-driven evaluation**. Only nodes affected by changed
inputs execute on any given cycle. The mechanism has two parts: a precomputed
dependency bitmask in the const tables, and a runtime dirty marking pass.

### How It Works

Each node descriptor carries a `raw_deps` bitmask — a table of integers where
bit N is set if the node transitively depends on raw buffer N. The codegen
computes this by walking the graph bottom-up: direct raw buffer reads set bits
directly, layer buffer reads inherit the union from all writer nodes.

At runtime, `handle:cycle()` performs three steps:

1. **Swap** (`swap_raw`) — compares each raw buffer's current vs previous data.
   Sets `changed=true` only on buffers where data differs.

2. **Mark dirty** (`mark_dirty`) — builds a bitmask of which raw buffers
   changed. For each node, ANDs the node's `raw_deps` against the changed set.
   If zero overlap, the node is not dirty and will not execute.

3. **Evaluate** (`evaluate`) — walks the node array in order (bottom-up by
   construction), skips clean nodes. Dirty nodes call their VFT function and
   write the result. No downstream propagation scan needed.

## DSL (scan_tree_dsl.lua)

The DSL constructs a hierarchical tree using a stack-based open/close pattern.
Every `_start` returns a handle that must be passed to the matching `_end`.

### Core API

```lua
local scan_tree = require("scan_tree_dsl")
local vft = require("vft_helpers")
local dsl = scan_tree.ScanTreeDSL.new()
```

**Tree scope:**
```lua
local tree = dsl:SCAN_TREE_start("my_system")
-- ... define buffers and levels ...
dsl:SCAN_TREE_end(tree)
```

**Raw I/O buffers** — defined at tree scope:

```lua
local buf = dsl:define_buffer("sensor_data", "float", 4, "Sensor readings",
    {units = "volts"})
dsl:define_pin(buf, "ch0", 0, "Channel 0")
dsl:end_buffer(buf)
```

Supported types: `bool`, `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t`,
`int8_t`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`.

**Levels** — ordered evaluation stages:

```lua
local l0 = dsl:SCAN_TREE_level_start("power")
-- define layer buffer + VFTs
dsl:SCAN_TREE_level_end(l0)
```

**Layer buffers** — defined inside levels or sub-levels. Must be `bool` type:

```lua
local out = dsl:define_buffer("power_output", "bool", 2, "Power status")
dsl:define_pin(out, "grid_ok", 0, "Grid power available")
dsl:end_buffer(out)
```

**Sub-levels** — nested scopes within a level:

```lua
local sl = dsl:SCAN_TREE_sub_level("group_a")
-- define buffer + VFTs
dsl:SCAN_TREE_sub_level_end(sl)
```

**VFT instantiation** — `"buffer_name:start-count"` format:

```lua
dsl:instantiate_vft(vft.VFT_or, "power_output:0-1", "power_inputs:0-3")
```

### Output

```lua
dsl:write_json("my_system.json")
dsl:print_summary()
```

## Code Generator (codegen_luajit.lua)

Reads the JSON intermediate and produces a Lua module.

```bash
luajit codegen_luajit.lua my_system.json output_dir/
```

### Generated Module Structure

The generated `{name}.lua` returns a factory function:

```lua
local make_desc = require("my_system")
local desc = make_desc(user_vfts)  -- user_vfts is optional
```

The descriptor table contains:

| Field | Contents |
|-------|----------|
| `desc.buf_descs` | Buffer descriptors (1-indexed, buf_id = index - 1) |
| `desc.node_descs` | Node descriptors with function references and raw_deps |
| `desc.lookup` | Sorted lookup table for path/key search |
| `desc.n_bufs`, `n_nodes`, `n_raw`, `n_layer` | Counts |
| `desc.fuse_table` | Fuse action map (node index → action name) |
| `desc.IDS` | Buffer ID constants for direct access |

### Buffer ID Constants

The `desc.IDS` table provides named constants:

```lua
local IDS = desc.IDS
local power = h:raw_current(IDS.MY_SYSTEM_POWER_INPUTS)
local states, sz = h:layer_states(IDS.MY_SYSTEM_POWER_OUTPUT)
```

## Runtime Library (st_runtime.lua)

### Initialization

```lua
local st_runtime = require("st_runtime")
local Handle = st_runtime.Handle

local make_desc = require("my_system")
local desc = make_desc(user_vfts)
local h = Handle.new(desc)
```

`Handle.new()` creates all working storage: raw double-buffers, layer
value/not_active/shadow/states arrays, per-node state and dirty flags.
All arrays are 0-indexed for C compatibility.

### Evaluation Cycle

```lua
-- Application main loop
while running do
    -- Write sensor data to raw buffers
    power[0] = read_power()
    pump_cur[0] = read_current(0)

    -- Evaluate
    h:cycle()    -- swap + mark_dirty + evaluate

    -- Read results from cached state arrays
    if plant_states[0] == 0 then
        trigger_alarm()
    end
end
```

### Buffer Access

**Cached access** — grab references once, read/write directly:

```lua
-- At init
local power = h:raw_current(IDS.MY_SYSTEM_POWER_INPUTS)
local states, sz = h:layer_states(IDS.MY_SYSTEM_POWER_OUTPUT)

-- In loop — direct 0-indexed access
power[0] = 1
h:cycle()
if states[0] == 0 then -- FAULT
```

**Path lookup** — for dynamic access:

```lua
local buf_id = h:lookup_path("my_system.pump_current")
if buf_id >= 0 then
    local data = h:buf_data(buf_id)
end
```

### Handle API Reference

| Method | Returns | Description |
|--------|---------|-------------|
| `Handle.new(desc)` | handle | Create from descriptor table |
| `h:cycle()` | — | swap + mark_dirty + evaluate |
| `h:buf_data(buf_id)` | 0-indexed array | Raw current or layer value data |
| `h:raw_current(buf_id)` | 0-indexed array | Raw buffer current data (writeable) |
| `h:get_state(buf_id, pos)` | int8 | Layer state at position (-1/0/1) |
| `h:layer_states(buf_id)` | array, size | Layer states array + count |
| `h:lookup_path(path)` | buf_id or -1 | FNV-1a lookup by path string |
| `h:lookup_key(key)` | buf_id or -1 | Direct key lookup |
| `h:print_info()` | — | Print system summary |

### Three-State Model

Every layer buffer position exists in one of three states:

| State | Value | Meaning |
|-------|-------|---------|
| ACTIVE | 1 | Condition is true / healthy |
| FAULT | 0 | Condition is false / faulted |
| NOT_OP | -1 | Never evaluated — no data has reached this position |

### Display (st_display.lua)

```lua
local st_display = require("st_display")
local disp = st_display.Display.new(h)

disp:print_tree(h)    -- render ANSI-colored fault tree
```

The display builds its rendering order once at init — levels from highest to
lowest, children indented under parents. Green T for ACTIVE, red F for FAULT,
grey N for NOT_OP.

## File Inventory

### Runtime Library

| File | Description |
|------|-------------|
| `st_runtime.lua` | Engine — Handle object, init, cycle, evaluate, lookup |
| `st_builtins.lua` | System VFT implementations (13 builtins) |
| `st_display.lua` | Hierarchical ANSI-colored fault display |

### Code Generator

| File | Description |
|------|-------------|
| `codegen_luajit.lua` | Reads JSON, emits Lua descriptor module with raw_deps |

### Build Flow

```bash
# Using build script (recommended)
./st_build.sh my_tree_dsl.lua output_dir/

# Manual steps
LUA_PATH="path/to/scan_tree_dsl/?.lua;;" luajit my_tree_dsl.lua my_tree.json
luajit codegen_luajit.lua my_tree.json output_dir/
luajit main.lua
```

## Design Constraints

**Single runtime.** No FFI, no C dependencies. Pure LuaJIT tables and
functions. Runs anywhere LuaJIT runs.

**Same DSL and JSON.** The C and LuaJIT versions share the identical DSL and
JSON intermediate format. A tree defined for the C target works unmodified
with the LuaJIT codegen.

**0-indexed buffers.** All buffer data arrays are 0-indexed for compatibility
with the C version's addressing model. Pin positions and buffer offsets in the
DSL use the same zero-based numbering.

**Bottom-up deterministic evaluation.** Node order matches declaration order.
Each node evaluates at most once per cycle, and only if its inputs changed.

**Change-driven execution.** In steady state with no input changes, zero nodes
execute. When a single sensor changes, only the affected subtree evaluates.
