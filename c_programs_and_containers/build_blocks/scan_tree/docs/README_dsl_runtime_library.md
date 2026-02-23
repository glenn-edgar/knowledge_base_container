# Scan Tree v2

A hierarchical bitmap-driven fault evaluation engine for embedded control
systems. Defines I/O structure and fault logic in a LuaJIT DSL, generates
const C data tables, and evaluates bottom-up at runtime with zero dynamic
allocation after init.

## System Architecture

```
  ┌────────────┐       ┌────────────┐       ┌─────────────────┐
  │  Lua DSL   │──────>│   JSON     │──────>│  C Code Gen     │
  │            │       │ intermediate│       │                 │
  │ buffers    │       │            │       │ {name}.h        │
  │ levels     │       │            │       │ {name}_user_vft.h│
  │ sub-levels │       │            │       │ {name}_fuse_actions.h│
  │ VFTs       │       │            │       │                 │
  └────────────┘       └────────────┘       └────────┬────────┘
                                                     │
                                                     v
  ┌──────────────────────────────────────────────────────────────┐
  │                     C Runtime                                │
  │                                                              │
  │  scan_tree.c    - engine: init, cycle, evaluate, lookup      │
  │  builtins.c     - system VFTs: and, or, fuse, comparisons   │
  │  st_display.c   - hierarchical ANSI-colored fault display    │
  │  user_vft.c     - application-provided evaluation functions  │
  │                                                              │
  │  Application links against runtime + generated .h            │
  │  No generated .c files. No init functions. No mutable statics.│
  └──────────────────────────────────────────────────────────────┘
```

The generated `.h` contains only `static const` tables. The runtime library
walks these tables via a descriptor pointer passed to `st_init()`. All working
storage is allocated in a single `calloc` at init.

## Data-Flow Evaluation

The scan tree uses **change-driven evaluation**. Only nodes affected by
changed inputs execute on any given cycle. The mechanism has two parts:
a precomputed dependency bitmask in the const tables, and a runtime dirty
marking pass that uses it.

### How It Works

Each node descriptor carries a `raw_deps` bitmask — a bitfield where bit N
is set if the node transitively depends on raw buffer N. The codegen computes
this by walking the graph bottom-up: if a node reads a raw buffer directly,
that bit is set. If a node reads a layer buffer, it inherits the union of
`raw_deps` from every node that writes to that layer. The result is a
transitive closure computed once at build time.

At runtime, `st_cycle()` performs three steps:

1. **Swap** (`st_swap_raw`) — compares each raw buffer's current vs previous
   data. Sets `changed=1` only on buffers where the data actually differs.

2. **Mark dirty** (`st_mark_dirty`) — builds a bitmask of which raw buffers
   changed. For each node, ANDs the node's `raw_deps` against the changed
   set. If zero overlap, the node is not marked dirty and will not execute.

3. **Evaluate** (`st_evaluate`) — walks the node array bottom-up, skips any
   node where `dirty=0`. Dirty nodes call their VFT function and write the
   result. No downstream propagation scan is needed — the bitmask already
   accounts for transitive dependencies.

### Example

A pump station has 9 nodes and 5 raw buffers. When only `motor_current`
changes (one raw buffer), the mark dirty pass identifies 7 dependent nodes
and skips 2 that depend only on `power_status`:

```
After changing only motor_current[0]:
  node[0] -> power_output[0]    dirty=0  (depends on power_status only)
  node[1] -> group_a[0]         dirty=1  (depends on motor_current)
  node[2] -> group_a[1]         dirty=1
  node[3] -> group_a[2]         dirty=1  (reads group_a layer -> inherits)
  node[4] -> group_b[0]         dirty=1
  node[5] -> group_b[1]         dirty=1
  node[6] -> group_b[2]         dirty=1
  node[7] -> actuation[0]       dirty=1  (reads group_a -> inherits)
  node[8] -> actuation[1]       dirty=0  (depends on power_output only)
```

In steady state (no inputs changing), zero nodes execute. When a single
sensor changes, only the subtree rooted at that sensor runs. For a 32-node
water treatment plant where one chlorine reading changes, roughly 6 of 32
nodes execute instead of all 32.

### Cost Comparison

The previous brute-force approach marked all nodes dirty if any raw buffer
changed, then ran an O(N²) scan after each node to propagate changes
downstream. The data-flow approach replaces both with O(N) bitmask
operations computed from const data:

| Nodes | Brute force (1 buf changed) | Data-flow | Speedup |
|-------|----------------------------|-----------|---------|
| 9     | ~675 ops                   | ~150 ops  | 4.5×    |
| 32    | ~6,080 ops                 | ~400 ops  | 15×     |
| 64    | ~21,440 ops                | ~680 ops  | 31×     |
| 128   | ~83,840 ops                | ~1,200 ops| 70×     |

The N² term in brute force (downstream dependency scan) dominates quickly.
Data-flow evaluation scales linearly with the affected subtree size.

## DSL (scan_tree_dsl.lua)

The DSL constructs a hierarchical tree using a stack-based open/close pattern.
Every `_start` returns a handle that must be passed to the matching `_end`,
enforcing balanced structure at construction time.

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

**Raw I/O buffers** — defined at tree scope. These are the system's external
inputs and outputs (sensor readings, actuator commands, operator signals):

```lua
local buf = dsl:define_buffer("sensor_data", "float", 4, "Sensor readings",
    {units = "volts"})    -- optional JSON properties
dsl:define_pin(buf, "ch0", 0, "Channel 0")
dsl:define_pin(buf, "ch1", 1, "Channel 1")
dsl:end_buffer(buf)
```

Supported types: `bool`, `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t`,
`int8_t`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`.

**Levels** — ordered evaluation stages. Level 0 evaluates first (foundation),
highest level evaluates last (top-level output):

```lua
local l0 = dsl:SCAN_TREE_level_start("power")
-- define layer buffer + VFTs
dsl:SCAN_TREE_level_end(l0)

local l1 = dsl:SCAN_TREE_level_start("actuation")
-- can reference level 0 outputs
dsl:SCAN_TREE_level_end(l1)
```

**Layer buffers** — defined inside levels or sub-levels. Must be `bool` type.
These hold the evaluation results (the fault state):

```lua
local out = dsl:define_buffer("power_output", "bool", 2, "Power status")
dsl:define_pin(out, "grid_ok",   0, "Grid power available")
dsl:define_pin(out, "backup_ok", 1, "Backup power available")
dsl:end_buffer(out)
```

**Sub-levels** — nested scopes within a level for organizing complex logic.
Each sub-level gets its own output buffer. Sub-levels can nest:

```lua
local sl = dsl:SCAN_TREE_sub_level("group_a")
local sl_out = dsl:define_buffer("group_a_output", "bool", 3, "Group A")
dsl:define_pin(sl_out, "p0_ok", 0, "Pump 0 healthy")
dsl:end_buffer(sl_out)
-- VFTs within this sub-level
dsl:SCAN_TREE_sub_level_end(sl)
```

**VFT instantiation** — connects inputs to outputs using the
`"buffer_name:start-count"` parameter format:

```lua
dsl:instantiate_vft(vft.VFT_or,
    "power_output:0-1",       -- output: 1 bit at position 0
    "power_inputs:0-3")       -- input: 3 bits starting at position 0
```

### Lexical Scoping Rules

Levels export only their output buffer. Sub-level internals (scratch buffers,
intermediate results) are encapsulated. A VFT in level 1 can read level 0's
output buffer but cannot reach into level 0's sub-level scratch buffers.

This enforces the geological model: each stratum's internal structure is
hidden from the layers above. Only the surface (output buffer) is exposed.

### Output

```lua
dsl:write_json("my_system.json")
dsl:print_summary()
```

## Code Generator (codegen_c.lua)

Reads the JSON intermediate and produces const C data tables.

```bash
luajit codegen_c.lua my_system.json output_dir/
```

### Generated Files

| File | Contents |
|------|----------|
| `{name}.h` | Buffer ID/KEY/SIZE defines, pin position defines, `static const` descriptor tables, system descriptor |
| `{name}_user_vft.h` | Prototypes for user-defined VFT functions (only if user VFTs exist) |
| `{name}_fuse_actions.h` | Prototypes for fuse action callbacks (only if fuses exist) |

### Generated Data Structures

**Buffer descriptors** (`st_buf_desc_t[]`) — path, FNV-1a key, size, element
size, layer/raw flag, buffer index, level number:

```c
{"my_system.power_inputs", 0xA925F093u, 3, 1, 0, 0, 255},  /* raw, level=0xFF */
{"my_system.power.power_output", 0x051ADDFCu, 1, 1, 1, 0, 0},  /* layer, level=0 */
```

**Node descriptors** (`st_node_desc_t[]`) — function pointer, output
buffer/position, input descriptors with roles, input count, and raw
dependency bitmask:

```c
{st_vft_or, 5, 0,                           /* func, output buf, output pos */
 {{1, 0, 2, 0}, {0,0,0,0}, ...},            /* inputs[ST_MAX_INPUTS] */
 1,                                          /* n_inputs */
 {0x00000002u, 0x00000000u, ...}}            /* raw_deps[ST_MAX_RAW_WORDS] */
```

The `raw_deps` bitmask is a transitive closure computed by the codegen.
Bit N is set if the node depends (directly or through layer buffers) on
raw buffer with `buf_index` N. The runtime uses this to skip nodes whose
inputs haven't changed.

**Lookup table** (`st_lookup_entry_t[]`) — sorted by FNV-1a key for binary
search at runtime.

**Fuse table** (`st_fuse_entry_t[]`) — maps node IDs to action callback
function pointers.

**Buffer ID defines** — compile-time constants for direct buffer access:

```c
#define MY_SYSTEM_POWER_INPUTS_ID 0
#define MY_SYSTEM_POWER_INPUTS_KEY 0xA925F093u
#define MY_SYSTEM_POWER_INPUTS_SIZE 3
#define MY_SYSTEM_POWER_INPUTS_GRID 0       /* pin position */
```

### FNV-1a Hash

Buffer lookup uses FNV-1a for path-to-ID resolution. The hash is computed
at codegen time and embedded in the const tables. Runtime lookup is binary
search on the sorted key table, with linear probe for collisions and path
string verification.

```c
/* fnv1a.c — shared between codegen (via FFI) and runtime */
uint32_t fnv1a_hash(const char *str) {
    uint32_t h = 2166136261u;
    while (*str) { h ^= (uint8_t)*str++; h *= 16777619u; }
    return h;
}
```

For codegen, this is compiled as a shared library (`fnv1a.so`) loaded via
LuaJIT FFI. The runtime uses the same algorithm in `scan_tree.c`.

## Runtime Library (scan_tree.c)

### Initialization

```c
st_handle_t h;
st_init(&h, &my_system_desc);   /* single calloc for all working storage */
```

`st_init` walks the const descriptor tables, calculates total memory needed
(raw double-buffers + layer value/not_active/shadow/states arrays + node
state bytes + dirty flags), allocates it in one `calloc`, and carves out
the individual arrays. No further allocation occurs.

### Evaluation Cycle

```c
/* Application main loop */
while (running) {
    read_sensors(power, pump_cur);     /* write to cached raw buffer pointers */
    st_cycle(&h);                      /* swap + mark dirty + evaluate */
    if (plant_states[0] == 0)          /* read from cached layer state pointers */
        trigger_alarm();
}
```

`st_cycle()` performs three operations:

1. **st_swap_raw** — compares current vs previous raw buffers, sets changed
   flags, copies current to previous.
2. **st_mark_dirty** — builds a bitmask of which raw buffers changed, then
   ANDs each node's precomputed `raw_deps` against it. Only nodes with
   overlapping dependencies are marked dirty. If no raw buffers changed,
   all nodes are marked clean in a single `memset`.
3. **st_evaluate** — walks the node descriptor array in order (bottom-up by
   construction). Skips clean nodes. For each dirty node, calls the VFT
   function and writes the result to the output layer position. No downstream
   propagation scan is needed — the dependency bitmask already covers
   transitive dependencies.

After evaluation, the engine updates shadow copies. The three-state arrays
(`int8_t`: 1=ACTIVE, 0=FAULT, -1=NOT_OP) are maintained incrementally by
`layer_write` during evaluation.

### Buffer Access

Two access patterns, chosen at init time:

**Cached pointer access** — zero overhead per read:

```c
/* At init — grab pointers once, valid for handle lifetime */
float   *pump_cur = ST_RAW_PTR(&h, MY_SYSTEM_PUMP_CURRENT_ID, float);
const int8_t *states = st_layer_states(&h, MY_SYSTEM_POWER_OUTPUT_ID, &sz);

/* In loop — direct array access, no function calls */
pump_cur[0] = read_adc(0);
st_cycle(&h);
if (states[0] == 0) { /* FAULT */ }
```

`ST_RAW_PTR` is type-checked at runtime — returns NULL if `sizeof(type)`
doesn't match the buffer's element size.

**Path/key lookup** — for dynamic or diagnostic access:

```c
int32_t id = st_lookup_path(&h, "my_system.pump_current");
int32_t id = st_lookup_key(&h, MY_SYSTEM_PUMP_CURRENT_KEY);
```

### Three-State Model

Every layer buffer position exists in one of three states:

| State | Value | Meaning |
|-------|-------|---------|
| ACTIVE | 1 | Condition is true / healthy / normal |
| FAULT | 0 | Condition is false / faulted / abnormal |
| NOT_OP | -1 | Never evaluated — no data has reached this position |

NOT_OP exists because layer positions start unwritten. Until a VFT writes
to a position, it remains NOT_OP. This distinguishes "the system hasn't
evaluated this yet" from "the system evaluated it and it's faulted."

The three-state array (`int8_t *states` in `st_layer_rt_t`) is maintained
incrementally — `layer_write` updates `states[pos]` on every VFT output
write. The `st_layer_states()` function returns a stable pointer to this
array, allowing the application to cache it at init and read directly in
the main loop without function call overhead.

## Compile-Time Tuning Constants

Two constants in `scan_tree.h` control the fixed-size arrays in every node
descriptor. Both affect const table size, which matters on flash-constrained
targets.

### ST_MAX_INPUTS

```c
#define ST_MAX_INPUTS 8
```

Sets the maximum number of input bindings per node. Each `st_input_desc_t`
is 7 bytes, so this field costs `ST_MAX_INPUTS × 7` bytes per node in the
const table.

| ST_MAX_INPUTS | Bytes per node (inputs) | Wasted per typical node |
|---------------|------------------------|------------------------|
| 2 | 14 | 0 (tight fit) |
| 4 | 28 | 14 |
| **8 (default)** | **56** | **42** |
| 16 | 112 | 98 |

Most VFTs use 1-2 inputs. The fuse uses 2 (INPUT + CLEAR). Comparison VFTs
use 2 (A + B). No current builtin exceeds 2. The unused slots are
zero-filled in the const table but never touched at runtime — VFTs loop
over `n_inputs`, not `ST_MAX_INPUTS`.

**When to reduce:** If all your VFTs use ≤ 2 inputs, set to 4 (leaves
headroom for user VFTs). Only set to 2 if you are certain no VFT will
ever need more.

### ST_MAX_RAW_WORDS

```c
#define ST_MAX_RAW_WORDS 8  /* supports up to 256 raw buffers */
```

Sets the size of the `raw_deps` dependency bitmask per node. Each word is
a `uint32_t` holding 32 bits, so `ST_MAX_RAW_WORDS × 32` = maximum raw
buffers.

| ST_MAX_RAW_WORDS | Max raw buffers | Bytes per node |
|------------------|----------------|----------------|
| 1 | 32 | 4 |
| 2 | 64 | 8 |
| 4 | 128 | 16 |
| **8 (default)** | **256** | **32** |

The runtime only checks `ceil(n_raw / 32)` words per node. If you have 5
raw buffers, only word 0 is examined — the other 7 words sit in the const
table but are never read.

**When to reduce:** If your system has ≤ 32 raw buffers (which covers
most embedded control applications), set to 1 and save 28 bytes per node.

### Tuning for 32KB ARM Cortex-M

On a 32KB flash target, const table size is the primary constraint. A
system with N nodes occupies roughly:

```
Per node:  ST_MAX_INPUTS × 7  +  ST_MAX_RAW_WORDS × 4  +  12 bytes overhead
Default:   8 × 7 + 8 × 4 + 12 = 100 bytes per node
Tuned:     4 × 7 + 1 × 4 + 12 =  44 bytes per node
```

| Nodes | Default (100 B/node) | Tuned (44 B/node) |
|-------|---------------------|-------------------|
| 9 | 900 B | 396 B |
| 16 | 1,600 B | 704 B |
| 32 | 3,200 B | 1,408 B |
| 64 | 6,400 B | 2,816 B |

Add buffer descriptors (~20 bytes each), lookup table (6 bytes each),
string literals for paths, and working storage (RAM, not flash). A
practical budget for a 32KB target:

```
Flash budget:     ~24KB available (after runtime code)
Tuned node_descs: ~44 bytes/node
Buffer descs:     ~20 bytes/buffer
Path strings:     ~40 bytes/buffer average
Lookup:           ~6 bytes/buffer
```

This supports roughly **100+ nodes with 20 buffers** in 4KB of const
data, leaving 20KB for the runtime library and application code.

**Recommended settings for Cortex-M0/M3/M4 with ≤ 32KB flash:**

```c
#define ST_MAX_INPUTS    4   /* sufficient for all builtins */
#define ST_MAX_RAW_WORDS 1   /* supports up to 32 raw buffers */
```

**Settings for larger targets (64KB+ flash or RAM-based systems):**

Leave at defaults. The 56-byte difference per node is negligible when
flash is not the bottleneck.

## Display Model (st_display.c)

### The Geological Metaphor

The scan tree evaluates like geological strata. Level 0 is bedrock
(infrastructure — power, safety interlocks). Level 1 is the next layer up
(equipment health). Level 2 aggregates process readiness. Level 3 is the
surface (plant-level status).

Faults propagate upward like geological faults — a crack in the bedrock
(power loss) propagates through every layer above it. But a surface-level
fault (one pump overcurrent) may not penetrate deeper layers if redundancy
absorbs it.

The display renders this model visually:

```
=== water_plant ===
plant_status.plant_output [T F]              ← surface: operational but degraded
process.process_output [F T T]               ← one process faulted
  process.intake_ready_check... [F T]        ← intake failed, infra ok
  process.treat_ready_check...  [T T]        ← treatment healthy
  process.dist_ready_check...   [T T]        ← distribution healthy
equipment.equip_output [F T T T]             ← intake equipment faulted
  equipment.intake_pumps... [T F T F]        ← overcurrent + fuse blown
  equipment.dosing...       [F F]            ← dosing normal
  equipment.dist_pumps...   [F F F F]        ← dist normal
infrastructure.infra_output [T T T]          ← bedrock: solid
  infrastructure.safety_check... [F]         ← no alarms
```

The top-level output appears first. The foundation appears last. An operator
reads top-down to understand system status, then drills down to find the
fault origin. The ANSI coloring makes fault locations immediately visible:
green T for active, red F for fault, grey N for not yet evaluated.

### Cached Display Hierarchy

The display builds its rendering order once at init:

```c
st_display_t *disp = st_display_init(&h);   /* build hierarchy once */
st_display_tree(disp, &h);                  /* render — flat array walk */
st_display_destroy(disp);                   /* cleanup */
```

`st_display_init` collects layer buffers, sorts by level (highest first),
resolves parent/child relationships from path structure, computes indentation,
and caches title pointers. `st_display_tree` is a single loop over the
pre-computed array — no path parsing or sorting at render time.

The `level` field in `st_buf_desc_t` (populated by codegen from the JSON
`level_number`) drives the sort order. Raw buffers carry level 0xFF and are
excluded from display.

### Three-State Display Encoding

| Symbol | Color | State | Meaning |
|--------|-------|-------|---------|
| **T** | Green | ACTIVE | Condition met |
| **F** | Red | FAULT | Condition failed |
| **N** | Grey | NOT_OP | Not yet evaluated |

## File Inventory

### DSL and Code Generator

| File | Description |
|------|-------------|
| `scan_tree_dsl.lua` | Core DSL engine — tree/level/buffer/VFT construction |
| `vft_helpers.lua` | VFT definitions — system builtins + user VFT factory |
| `codegen_c.lua` | C code generator — reads JSON, emits .h files with raw_deps bitmasks |
| `fnv1a.c` | FNV-1a hash — compiled to .so for LuaJIT FFI |

### Runtime Library

| File | Description |
|------|-------------|
| `scan_tree.h` | Public API — types, function prototypes, macros (`ST_RAW_PTR`) |
| `scan_tree.c` | Engine — init, cycle, evaluate, lookup, swap, diagnostics |
| `builtins.h` | System VFT prototypes |
| `builtins.c` | System VFT implementations (14 builtins) |
| `st_display.h` | Display API — init/destroy/tree |
| `st_display.c` | Hierarchical ANSI-colored fault display |

### Build Flow

```bash
# 1. Compile FNV-1a shared library (once)
gcc -shared -fPIC -o fnv1a.so fnv1a.c

# 2. Run DSL to produce JSON
LUA_PATH="./?.lua;;" luajit my_system.lua my_system.json

# 3. Generate C headers
luajit codegen_c.lua my_system.json output_dir/

# 4. Compile application
gcc -Wall -Wextra -std=c99 -O2 \
    -o my_test my_test.c scan_tree.c builtins.c st_display.c
```

## Design Constraints

**No generated .c files.** The codegen emits only `.h` files containing
`static const` data. The application includes the header and passes the
descriptor address to `st_init()`.

**Single allocation.** All working storage comes from one `calloc` in
`st_init()`. No further heap activity during operation. Suitable for
embedded systems with deterministic memory requirements.

**Zero warnings.** Compiles clean with `-Wall -Wextra -std=c99 -O2` on
GCC and Clang.

**Bottom-up deterministic evaluation.** Node order in the descriptor array
matches declaration order, which is bottom-up by construction (level 0
nodes first, highest level last). No iterative convergence. Each node
evaluates at most once per cycle, and only if its inputs changed.

**Change-driven execution.** The raw dependency bitmask (`raw_deps`) enables
the runtime to skip nodes whose transitive inputs haven't changed. In
steady state with no input changes, zero nodes execute. When a single
sensor changes, only the affected subtree evaluates.

**Const descriptor tables.** The `st_system_desc_t` and everything it
points to is `const`. The runtime never writes to descriptor memory. On
embedded targets this can live in flash.

## Target Range

Designed to span from 32KB ARM Cortex-M microcontrollers to 8GB+ servers.
The const data tables are compact (a 32-node system is under 2KB of const
data with tuned constants, under 4KB with defaults). Working storage scales
linearly with buffer count and node count. The engine has no recursion, no
dynamic allocation after init, and no floating-point in the core path
(float is only used by comparison VFTs that the application opts into).

For Cortex-M targets, reduce `ST_MAX_INPUTS` to 4 and `ST_MAX_RAW_WORDS`
to 1. This cuts per-node const data from 100 bytes to 44 bytes with no
loss of functionality for systems with ≤ 32 raw buffers and ≤ 4 inputs
per VFT (which covers all 14 builtin VFTs).