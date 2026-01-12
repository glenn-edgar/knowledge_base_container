# ChainTree Hierarchical Bit Map DSL

**ChainTree Hierarchical Bit Map DSL** is a lightweight, "zero-copy" engine for managing hierarchical system state in embedded systems. It allows you to define a tree of devices (Robots, Conveyors, Sensors) and automatically propagates bit-level logic (like Alarms, Safety Interlocks, or Ready States) up the hierarchy in real-time.

It separates **Definition** (Lua DSL) from **Runtime** (C99), generating highly optimized, read-only data structures that require no string parsing on the embedded target.

---

## 1. The Problem

In industrial automation, state is hierarchical:

* If a **Joint** has an error, the **Robot** is faulted.
* If the **Robot** is faulted, the **Work Cell** is red.
* If the **Work Cell** is red, the **Production Line** stops.

Hardcoding this logic (`if robot.fault || conveyor.fault ...`) is brittle. ChainTree Hierarchical Bit Map DSL solves this by defining **Bitspaces** with merge rules.

---

## 2. DSL Tutorial

The system is defined using a stack-based Lua DSL. Create a file named `schema.lua`.

### Step A: Schema Header

```lua
local S = require("schema_builder")

-- Schema header: variable name, schema name, version
local schema = S.start_schema("schema", "FactoryDemo", "1.0.0")

-- Optional constraints
S.options("max_ram", 8192, "max_rom", 16384, "max_depth", 5)
```

### Step B: Define "Bitspaces" (The Channels)

Bitspaces are parallel layers of logic that run through your entire tree. The merge type defines how **aggregate nodes compute their state from children** during `propagate()`. Leaf nodes simply store their bits directly - the merge type doesn't affect them.

```lua
local bs = S.start_bitspaces("bs")

  -- PRIORITY merge: aggregate gets highest priority child state
  -- (first arg = highest priority, last = lowest)
  S.bitspace_priority("STATE", "ESTOP", "FAULT", "WARNING", "RUNNING", "IDLE")
  
  -- OR merge: aggregate bit set if ANY child has it set
  S.bitspace_or("ALARM")
  
  -- OR + Latch: stays set until cleared at LEAF source
  S.bitspace_or_latch("ALARM_LATCHED")
  
  -- OR + Latch + Safe: requires leaf source inactive before clear
  S.bitspace_or_latch_safe("SAFETY_ALARM")
  
  -- MASK merge: leaves can filter which bits propagate up
  S.bitspace_mask("ALARM_ACK", "OR")
  
  -- MASK + Latch
  S.bitspace_mask_latch("ALARM_ACK_LATCHED", "OR")
  
  -- AND merge: aggregate bit set only if ALL children have it set
  S.bitspace_and("READY")
  
  -- AND + Latch
  S.bitspace_and_latch("READY_LATCHED")
  
  -- More examples
  S.bitspace_or("INHIBIT")
  S.bitspace_or("CMD_REQ")
  S.bitspace_or("CMD_ACK")
  S.bitspace_and("PERMIT")
  
  -- PRIORITY + Latch: captures worst state seen
  S.bitspace_priority_latch("WORST_STATE", "ESTOP", "FAULT", "WARNING", "RUNNING", "IDLE")

S.end_(bs)
```

**Remember:** These merge rules define aggregate behavior. Leaf nodes just store bits - you write to them with `set_bit()`, and `propagate()` computes all aggregate values using these rules.

### Bitspace Convenience Functions Reference

| Function | Merge | Latch | Clear Requires Inactive | Notes |
|----------|-------|-------|------------------------|-------|
| `S.bitspace_or(name)` | OR | No | - | Any child set → parent set |
| `S.bitspace_or_latch(name)` | OR | Yes | No | Stays set until cleared |
| `S.bitspace_or_latch_safe(name)` | OR | Yes | Yes | Must clear source first |
| `S.bitspace_and(name)` | AND | No | - | All children set → parent set |
| `S.bitspace_and_latch(name)` | AND | Yes | No | Latched AND |
| `S.bitspace_mask(name, base)` | MASK | No | - | Filtered by runtime mask |
| `S.bitspace_mask_latch(name, base)` | MASK | Yes | No | Masked + latched |
| `S.bitspace_priority(name, ...)` | PRIORITY | No | - | Varargs = priority order |
| `S.bitspace_priority_latch(name, ...)` | PRIORITY | Yes | No | Latched priority |

### Step C: Define "Classes" (The Hardware)

Define the memory layout for your **leaf** devices only. Aggregate classes (Cell, Line, Plant) are auto-generated from the tree structure with bank sizes computed as max(children).

Each class must explicitly define a bank size for **every** bitspace defined in Step B. Use 0 to opt-out of a bitspace. The order must match the bitspace definition order.

```lua
local classes = S.start_classes("classes")

  -- RobotArm - full featured device (leaf class)
  -- Bank sizes must be listed in SAME ORDER as bitspaces were defined
  local cls = S.start_class("cls", "RobotArm",
    "STATE", 8,              -- matches bitspace_priority("STATE", ...)
    "ALARM", 32,             -- matches bitspace_or("ALARM")
    "ALARM_LATCHED", 32,     -- matches bitspace_or_latch("ALARM_LATCHED")
    "SAFETY_ALARM", 16,      -- matches bitspace_or_latch_safe("SAFETY_ALARM")
    "ALARM_ACK", 16,         -- matches bitspace_mask("ALARM_ACK", "OR")
    "ALARM_ACK_LATCHED", 16, -- matches bitspace_mask_latch("ALARM_ACK_LATCHED", "OR")
    "READY", 8,              -- matches bitspace_and("READY")
    "READY_LATCHED", 8,      -- matches bitspace_and_latch("READY_LATCHED")
    "INHIBIT", 8,            -- matches bitspace_or("INHIBIT")
    "PERMIT", 8,             -- matches bitspace_and("PERMIT")
    "CMD_REQ", 8,            -- matches bitspace_or("CMD_REQ")
    "CMD_ACK", 8,            -- matches bitspace_or("CMD_ACK")
    "WORST_STATE", 8         -- matches bitspace_priority_latch("WORST_STATE", ...)
  )
    -- Named bits (optional, generates #defines)
    local bits = S.start_bits("bits", "ALARM")
      S.bit("OverTorque")
      S.bit("OverTemp")
      S.bit("CommLost")
      S.bit("EncoderFault")
      S.bit("LowAirPress")
      S.bit("NeedsGrease")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "STATE")
      S.bit("Idle")
      S.bit("Running")
      S.bit("Warning")
      S.bit("Fault")
      S.bit("EStop")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "CMD_REQ")
      S.bit("Start")
      S.bit("Stop")
      S.bit("Reset")
      S.bit("Home")
    S.end_(bits)
  S.end_(cls)

  -- Sensor - minimal device (opt-out of some bitspaces with 0)
  -- MUST list ALL bitspaces even if not participating
  local cls = S.start_class("cls", "Sensor",
    "STATE", 8,              -- participates
    "ALARM", 8,              -- participates  
    "ALARM_LATCHED", 8,      -- participates
    "SAFETY_ALARM", 0,       -- 0 = doesn't participate
    "ALARM_ACK", 0,          -- 0 = doesn't participate
    "ALARM_ACK_LATCHED", 0,  -- 0 = doesn't participate
    "READY", 8,              -- participates
    "READY_LATCHED", 8,      -- participates
    "INHIBIT", 0,            -- 0 = doesn't participate
    "PERMIT", 0,             -- 0 = doesn't participate
    "CMD_REQ", 0,            -- 0 = doesn't participate
    "CMD_ACK", 0,            -- 0 = doesn't participate
    "WORST_STATE", 8         -- participates
  )
    -- Named bits for participating bitspaces
    local bits = S.start_bits("bits", "STATE")
      S.bit("Idle")
      S.bit("Active")
      S.bit("Fault")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "ALARM")
      S.bit("OutOfRange")
      S.bit("CalExpired")
      S.bit("CommFail")
    S.end_(bits)
    
    local bits = S.start_bits("bits", "READY")
      S.bit("Calibrated")
      S.bit("InRange")
    S.end_(bits)
  S.end_(cls)

S.end_(classes)
```

**Validation Errors:**

The schema builder validates class definitions:

```
Class 'Sensor' missing banks: CMD_REQ, CMD_ACK
       (use 0 to explicitly opt-out)

Class 'RobotArm' unknown bank 'ALRM' - did you mean 'ALARM'?
```

### Step D: Define the Tree (The Topology)

Build the hierarchy using nested `start_node` / `end_` calls. The engine infers parent/child relationships from nesting. **Aggregate classes are auto-generated** - you don't need to define Cell, Line, or Plant classes.

```lua
local nodes = S.start_nodes("nodes")

  -- Root node (aggregate class "Plant" auto-generated)
  local plant = S.start_node("plant", "Plant", "Plant")
  
    local line1 = S.start_node("line1", "Line1", "Line")  -- "Line" auto-generated
    
      local cell1 = S.start_node("cell1", "Cell1", "Cell")  -- "Cell" auto-generated
      
        -- Leaf nodes use defined classes
        local robot1 = S.start_node("robot1", "Robot1", "RobotArm")
          -- Static configuration data
          S.config("Config.Motion.MaxSpeed", 1500)
          S.config("Config.Motion.MaxAccel", 4.0)
          S.config("Config.Safety.Enabled", true)
          S.config("Config.Name", "Welder_01")
        S.end_(robot1)
        
        local sensor1 = S.start_node("sensor1", "Sensor1", "Sensor")
        S.end_(sensor1)
        
      S.end_(cell1)
      
    S.end_(line1)
    
  S.end_(plant)

S.end_(nodes)

-- Finalize and return schema
S.end_(schema)
return S.build()
```

---

## 3. Merge Semantics

### Key Concept: Leaves Define, Aggregates Merge

**Only leaf nodes can define/write bit values.** Everything else "ripples up" through the merge rules.

```
Plant (aggregate)     ← STATE computed by PRIORITY merge
  └── Line1 (aggregate)   ← STATE computed by PRIORITY merge  
        └── Cell1 (aggregate)   ← STATE computed by PRIORITY merge
              ├── Robot1 (LEAF)    ← STATE = RUNNING (set by application)
              ├── Robot2 (LEAF)    ← STATE = FAULT (set by application)
              └── Sensor1 (LEAF)   ← STATE = IDLE (set by application)
```

**The merge type (OR, AND, PRIORITY, MASK) only applies at aggregate nodes during `propagate()`.**

At leaf nodes:
- You **write** bits directly via `set_bit()`
- The bitspace type doesn't matter - leaves just store their own state
- Merge rules are ignored (no children to merge)

At aggregate nodes:
- You **cannot write** bits (EXCEPTION if you try)
- State is **computed** by `propagate()` using the merge rule
- Result is the combination of all descendant leaf states

### Bitspace Types Reference

| Bitspace Function | Merge Rule | Aggregate Computes |
|-------------------|------------|-------------------|
| `bitspace_or(name)` | OR | Any descendant bit set → parent bit set |
| `bitspace_and(name)` | AND | All descendant bits set → parent bit set |
| `bitspace_priority(name, ...)` | PRIORITY | Highest priority state among descendants |
| `bitspace_mask(name, base)` | MASK + base | Filtered merge (masks at leaves only) |

**Latch variants** (`*_latch`, `*_latch_safe`) add latching behavior - bits stay set until explicitly cleared at the **leaf** where they originated.

### OR Merge (Alarm Propagation)

The OR merge propagates any set bit upward. If **any** child has an alarm, the parent has an alarm.

```
Plant (ALARM = 0x03)        ← OR of all children
  ├── Line1 (ALARM = 0x01)  ← OR of CellA, CellB
  │     ├── CellA (ALARM = 0x01)  ← Robot1 has alarm
  │     │     ├── Robot1 (ALARM = 0x01)  ← OverTorque set
  │     │     └── Conv1  (ALARM = 0x00)
  │     └── CellB (ALARM = 0x00)
  └── Line2 (ALARM = 0x02)
        └── CellC (ALARM = 0x02)  ← Different alarm bit
```

### AND Merge (Ready Propagation)

The AND merge requires all children to have a bit set for the parent to have it set.

```
Plant (READY = 0x00)        ← All children must be ready
  ├── Line1 (READY = 0x01)  ← All cells ready
  │     ├── CellA (READY = 0x01)
  │     └── CellB (READY = 0x01)
  └── Line2 (READY = 0x00)  ← CellC not ready, so Line2 not ready
        └── CellC (READY = 0x00)
```

### PRIORITY Merge (State Determination)

Priority merge selects the highest-priority state from children. States are defined with explicit ordering (first = highest priority):

```lua
S.bitspace_priority("STATE", "FAULT", "WARNING", "RUNNING", "IDLE")
```

Parent state = minimum index among all children (highest priority wins).

### MASK Merge (Selective Blocking)

MASK allows optional blocking of specific bits before they propagate upward. **Masks are leaf-node only** - aggregate nodes don't have masks.

```lua
S.bitspace_mask("ALARM_FILTERED", "OR")  -- base_merge is OR
```

Runtime mask modification (leaf nodes only):

```c
// Set mask (1 = allow, 0 = block)
cfl_hbit_set_mask(&tree, BS_ALARM_FILTERED, 0xFC, "Plant.Line1.Cell1.Robot1");

// Clear mask (restore all bits)  
cfl_hbit_clear_mask(&tree, BS_ALARM_FILTERED, "Plant.Line1.Cell1.Robot1");
```

**EXCEPTION** is raised if `set_mask` or `clear_mask` is called on a non-leaf node.

### LATCH Modifier (First-Fault Capture)

LATCH is a modifier that can be applied to any merge type. Once a bit is set, it remains set until explicitly cleared.

```lua
S.bitspace_or_latch("ALARM_LATCHED")
S.bitspace_or_latch_safe("SAFETY_ALARM")  -- requires source inactive to clear
```

**Latch clear is leaf-only** - EXCEPTION if called on aggregate node:

```c
// Clear latch (leaf nodes only)
cfl_hbit_clear_latch(&tree, BS_ALARM_LATCHED, "Plant.Line1.Cell1.Robot1");

// Clear specific bits
cfl_hbit_clear_latch_bits(&tree, BS_ALARM_LATCHED, 0x03, "Plant.Line1.Cell1.Robot1");

// Bulk clear all nodes (OK for any level)
cfl_hbit_clear_all_latches(&tree, BS_ALARM_LATCHED);
```

---

## 4. The Build Process

### Generating Code

```bash
luajit codegen.lua schema.lua
```

### Output Files

1. **`generated_<Name>.bin`**: Binary descriptor (ROM)
2. **`generated_<Name>.bin.h`**: C header with embedded binary
3. **`generated_<Name>_hashes.h`**: All symbolic defines

### Generated Header Contents

The `_hashes.h` file contains everything needed for compile-time safe code:

```c
/* Schema Info */
#define FACTORYDEMO_NODE_COUNT 13
#define FACTORYDEMO_BITSPACE_COUNT 13
#define FACTORYDEMO_CLASS_COUNT 6

/* Bitspace IDs (indices for API calls) */
#define FACTORYDEMO_BS_STATE 0
#define FACTORYDEMO_BS_ALARM 1
#define FACTORYDEMO_BS_ALARM_LATCHED 2
// ...

/* Node Indices (for _n functions) */
#define FACTORYDEMO_NODE_PLANT 0
#define FACTORYDEMO_NODE_PLANT_LINE1 1
#define FACTORYDEMO_NODE_PLANT_LINE1_CELL1 2
#define FACTORYDEMO_NODE_PLANT_LINE1_CELL1_ROBOT1 3
// ...

/* Bit Indices (per class/bitspace) */
#define FACTORYDEMO_BIT_ROBOTARM_ALARM_OVERTORQUE 0
#define FACTORYDEMO_BIT_ROBOTARM_ALARM_OVERTEMP 1
#define FACTORYDEMO_BIT_ROBOTARM_STATE_IDLE 0
#define FACTORYDEMO_BIT_ROBOTARM_STATE_RUNNING 1
// ...

/* Bank Sizes (bits) */
#define FACTORYDEMO_BANK_ROBOTARM_ALARM 32
#define FACTORYDEMO_BANK_SENSOR_ALARM 8
// ...

/* Path Hashes (for debugging) */
#define FACTORYDEMO_HASH_PLANT_LINE1_CELL1_ROBOT1 0x84924135U
// ...
```

---

## 5. C Runtime API

### Leaf vs Aggregate Node Rules

| Operation | Leaf Node | Aggregate Node | Notes |
|-----------|-----------|----------------|-------|
| `set_bit` / `set_bit_n` | ✅ | ❌ EXCEPTION | Aggregates computed by propagate |
| `set_bits_mask` | ✅ | ❌ EXCEPTION | Aggregates computed by propagate |
| `clear_bank` | ✅ | ❌ EXCEPTION | Aggregates computed by propagate |
| `clear_bit` | ✅ | ❌ EXCEPTION | Aggregates computed by propagate |
| `get_bit` / `get_bit_n` | ✅ Read state | ✅ Read propagated | Both work |
| `get_bits` / `get_bits_n` | ✅ Read state | ✅ Read propagated | Both work |
| `set_mask` / `clear_mask` | ✅ | ❌ EXCEPTION | Masks are leaf-only |
| `get_mask` | ✅ | Returns NULL | No mask storage for aggregates |
| `clear_latch` / `clear_latch_n` | ✅ | ❌ EXCEPTION | Must clear at source |
| `clear_latch_bits` / `clear_latch_bits_n` | ✅ | ❌ EXCEPTION | Must clear at source |
| `clear_all_latches` | ✅ Bulk clear | ✅ Bulk clear | Clears all nodes |
| `get_bit_edge` / `get_bit_edge_n` | ✅ | ✅ | Detect any changes |
| `get_parent_n` | ✅ | ✅ | Tree navigation |
| `get_children_n` | ✅ (returns 0) | ✅ | Tree navigation |

### Initialization

```c
#include "cfl_hbit.h"
#include "generated_FactoryDemo.bin.h"
#include "generated_FactoryDemo_hashes.h"

cfl_hbit_t tree;

// Initialize from compiled-in descriptor
cfl_hbit_init(&tree, generated_FactoryDemo_bin, sizeof(generated_FactoryDemo_bin), NULL);

// Or from file
cfl_hbit_init_from_file(&tree, "factory.bin", NULL);
```

### Path-Based API (String Lookup)

```c
// Set a bit (writes to shadow buffer)
cfl_hbit_set_bit(&tree, FACTORYDEMO_BS_ALARM, 
                 FACTORYDEMO_BIT_ROBOTARM_ALARM_OVERTORQUE, 
                 true, "Plant.Line1.Cell1.Robot1");

// Printf-style paths
cfl_hbit_set_bit(&tree, FACTORYDEMO_BS_ALARM, 0, true,
                 "Plant.Line%d.Cell%d.Robot%d", line, cell, robot);

// Get a bit (reads from current buffer)
int val = cfl_hbit_get_bit(&tree, FACTORYDEMO_BS_ALARM, 0, "Plant.Line1.Cell1.Robot1");

// Get entire bank
const uint8_t* bits = cfl_hbit_get_bits(&tree, FACTORYDEMO_BS_ALARM, "Plant.Line1.Cell1");
```

### Node-Indexed API (Fast Path)

For tight loops, lookup the node once and reuse the index:

```c
// Lookup once
int32_t robot1 = cfl_hbit_find_node_path(&tree, "Plant.Line1.Cell1.Robot1");

// Fast access with _n suffix functions
cfl_hbit_set_bit_n(&tree, FACTORYDEMO_BS_ALARM, 0, true, robot1);
cfl_hbit_set_bit_n(&tree, FACTORYDEMO_BS_ALARM, 1, true, robot1);

int val = cfl_hbit_get_bit_n(&tree, FACTORYDEMO_BS_ALARM, 0, robot1);
const uint8_t* bits = cfl_hbit_get_bits_n(&tree, FACTORYDEMO_BS_ALARM, robot1);

// Edge detection
int edge = cfl_hbit_get_bit_edge_n(&tree, FACTORYDEMO_BS_ALARM, 0, robot1);
// Returns: 1 = rising, -1 = falling, 0 = no change

// Latch clear (leaf only)
cfl_hbit_clear_latch_n(&tree, FACTORYDEMO_BS_ALARM_LATCHED, robot1);
```

### Tree Navigation

```c
// Get parent
int32_t cell = cfl_hbit_get_parent_n(&tree, robot1);

// Get children
int32_t children[10];
int count = cfl_hbit_get_children_n(&tree, cell, children, 10);

// Get child count (fast, from descriptor)
int num_children = cfl_hbit_get_child_count_n(&tree, cell);

// Check if leaf
bool is_leaf = cfl_hbit_is_leaf(&tree, "Plant.Line1.Cell1.Robot1");
```

### Synchronization

```c
// Swap shadow → current and propagate up tree
cfl_hbit_sync(&tree);

// Or separately:
cfl_hbit_swap(&tree);       // O(1) pointer swap
cfl_hbit_propagate(&tree);  // O(n) bottom-up propagation
```

### Edge Detection

```c
// Single bit edge (no malloc)
int edge = cfl_hbit_get_bit_edge(&tree, FACTORYDEMO_BS_ALARM, 0, "Plant.Line1.Cell1.Robot1");
// Returns: 1 = rising (0→1), -1 = falling (1→0), 0 = no change
```

### Configuration Data

```c
int32_t speed = cfl_hbit_get_config_int(&tree, "Plant.Line1.Cell1.Robot1.Config.Motion.MaxSpeed");
float accel = cfl_hbit_get_config_float(&tree, "Plant.Line1.Cell1.Robot1.Config.Motion.MaxAccel");
bool enabled = cfl_hbit_get_config_bool(&tree, "Plant.Line1.Cell1.Robot1.Config.Safety.Enabled");
const char* name = cfl_hbit_get_config_string(&tree, "Plant.Line1.Cell1.Robot1.Config.Name");
```

---

## 6. Error Handling

All programming errors trigger **EXCEPTION** (configurable handler). This catches bugs during development:

| Condition | Exception Message |
|-----------|-------------------|
| Tree not initialized | `"tree not initialized"` |
| Invalid bitspace ID | `"invalid bitspace id"` |
| Invalid node index | `"invalid node index"` |
| Path not found | `"path not found"` |
| Bit index out of range | `"bit index out of range"` |
| NULL pointer | `"NULL tree/mask/callback"` |
| set_mask on non-leaf | `"set_mask called on non-leaf node"` |
| clear_latch on non-leaf | `"clear_latch called on non-leaf node"` |
| Memory allocation failed | `"malloc failed to allocate memory"` |
| Invalid descriptor | `"invalid descriptor magic number"` |

**Runtime condition** (returns status code):
- `CFL_HBIT_ERR_SOURCE_ACTIVE` - trying to clear safety latch while source still active

### Custom Exception Handler

```c
// Default: calls abort()
// Override with:
void my_exception_handler(const char* msg, const char* file, int line) {
    printf("HBIT EXCEPTION: %s at %s:%d\n", msg, file, line);
    while(1);  // Halt for debugging
}

#define EXCEPTION(msg) my_exception_handler(msg, __FILE__, __LINE__)
```

---

## 7. Memory Layout

### Descriptor (ROM)
- Binary blob containing tree structure, bitspace definitions, class layouts
- Generated at compile time, never modified

### Arenas (RAM)
- Contiguous memory blocks per bitspace
- Three buffers: shadow (write), current (read), prev (edge detect)
- Additional latch/live buffers for latching bitspaces

### Leaf Mask Storage (RAM)
- Only leaf nodes have mask storage
- Aggregate masks computed during propagation

```
Memory Summary (example):
  Descriptor:     1968 bytes (ROM)
  Arenas:         1690 bytes (RAM)
  Leaf masks:      276 bytes (RAM)
  Total RAM:      1966 bytes
```

---

## 8. Complete Example

### Schema (schema.lua)

```lua
local S = require("schema_builder")

local schema = S.start_schema("schema", "SmallFactory", "1.0.0")
S.options("max_ram", 4096, "max_depth", 4)

local bs = S.start_bitspaces("bs")
  S.bitspace_or("ALARM")
  S.bitspace_or_latch("ALARM_LATCHED")
  S.bitspace_and("READY")
  S.bitspace_priority("STATE", "ESTOP", "FAULT", "WARNING", "RUNNING", "IDLE")
S.end_(bs)

local classes = S.start_classes("classes")

  local cls = S.start_class("cls", "Robot",
    "ALARM", 32, "ALARM_LATCHED", 32, "READY", 8, "STATE", 8)
    local bits = S.start_bits("bits", "ALARM")
      S.bit("OverTorque")
      S.bit("OverTemp")
      S.bit("CommLost")
    S.end_(bits)
  S.end_(cls)

  local cls = S.start_class("cls", "Sensor",
    "ALARM", 8, "ALARM_LATCHED", 8, "READY", 8, "STATE", 8)
  S.end_(cls)

S.end_(classes)

local nodes = S.start_nodes("nodes")
  local plant = S.start_node("plant", "Factory", "Plant")
    local line = S.start_node("line", "Line1", "Line")
      local cell = S.start_node("cell", "CellA", "Cell")
        local robot = S.start_node("robot", "Robot1", "Robot")
          S.config("MaxTorque", 50.0)
        S.end_(robot)
        local sensor = S.start_node("sensor", "Sensor1", "Sensor")
        S.end_(sensor)
      S.end_(cell)
    S.end_(line)
  S.end_(plant)
S.end_(nodes)

S.end_(schema)
return S.build()
```

### Application (main.c)

```c
#include "cfl_hbit.h"
#include "generated_SmallFactory.bin.h"
#include "generated_SmallFactory_hashes.h"

static cfl_hbit_t g_tree;

void on_alarm_change(cfl_hbit_t* tree, int bs, uint32_t hash,
                     const uint8_t* old_bits, const uint8_t* new_bits, void* ud) {
    if (*new_bits & ~*old_bits) {
        printf("ALARM RAISED on node 0x%08X\n", hash);
    }
}

int main(void) {
    // Initialize
    cfl_hbit_init(&g_tree, generated_SmallFactory_bin, 
                  sizeof(generated_SmallFactory_bin), NULL);
    
    cfl_hbit_register_callback(&g_tree, SMALLFACTORY_BS_ALARM, on_alarm_change, NULL);
    
    // Lookup node once for fast access
    int32_t robot1 = cfl_hbit_find_node_path(&g_tree, "Factory.Line1.CellA.Robot1");
    
    // Set alarm using fast path
    cfl_hbit_set_bit_n(&g_tree, SMALLFACTORY_BS_ALARM, 
                       SMALLFACTORY_BIT_ROBOT_ALARM_OVERTORQUE, true, robot1);
    
    // Sync and propagate
    cfl_hbit_sync(&g_tree);
    
    // Check factory-level alarm
    const uint8_t* alarms = cfl_hbit_get_bits(&g_tree, SMALLFACTORY_BS_ALARM, "Factory");
    if (alarms && *alarms) {
        printf("Factory has alarms: 0x%02X\n", *alarms);
    }
    
    // Check edge
    int edge = cfl_hbit_get_bit_edge_n(&g_tree, SMALLFACTORY_BS_ALARM, 0, robot1);
    if (edge == 1) {
        printf("Rising edge on OverTorque alarm\n");
    }
    
    cfl_hbit_destroy(&g_tree);
    return 0;
}
```

---

## 9. Key Design Decisions

### Leaf-Only Classes
- Only define classes for actual devices (leaf nodes)
- Aggregate classes (Cell, Line, Plant) auto-generated
- Bank sizes computed as max(children) for each bitspace

### Leaf-Only Masks and Latches
- Masks only stored for leaf nodes (saves RAM)
- `set_mask`/`clear_mask` EXCEPTION on non-leaf
- `clear_latch`/`clear_latch_bits` EXCEPTION on non-leaf
- Rationale: Aggregate state is derived from children

### Node-Indexed API
- `cfl_hbit_find_node_path()` returns cached index
- `_n` suffix functions avoid repeated hash lookup
- Use for tight loops and performance-critical code

### Exception-Based Error Handling
- All programming errors trigger EXCEPTION
- Catches bugs during development
- Only `CFL_HBIT_ERR_SOURCE_ACTIVE` returns status (runtime condition)

### Generated Header Defines
- `*_BS_*` - Bitspace IDs (indices)
- `*_NODE_*` - Node indices
- `*_BIT_*_*_*` - Bit indices (class, bitspace, name)
- `*_BANK_*_*` - Bank sizes
- `*_HASH_*` - Path hashes (debugging)

---

## License

MIT License - See LICENSE file for details.