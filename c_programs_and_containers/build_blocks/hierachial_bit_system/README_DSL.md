# Hierarchical Bitmap DSL Reference

## Overview

The Hierarchical Bitmap DSL provides a declarative language for defining tree-structured status/alarm systems for embedded control applications. The DSL uses Lua as its host language and generates optimized C headers with compile-time constants and lookup tables.

## Core Concepts

### Tree Structure Philosophy

The DSL builds **hierarchical aggregation trees** where:
- **Leaf nodes** contain actual hardware status bits (sensors, valves, motors)
- **Aggregate nodes** automatically summarize their children using OR or AND logic
- Bits propagate **up the tree** from leaves to roots
- Each level provides a summarized view of all descendants

### Buffer Types

Three buffer types control how bits propagate up the tree:

| Type | Merge Logic | Latching | Masking | Use Case |
|------|-------------|----------|---------|----------|
| `OR_LATCH` | OR merge | Yes | No | Alarms that stick until cleared |
| `OR_MASK` | OR merge | No | Yes | Alarms with enable/disable control |
| `AND` | AND merge | No | No | Ready/healthy states (all must be true) |

**OR_LATCH**: Parent bit is set if ANY child bit is set. Once set, remains latched until explicitly cleared.

**OR_MASK**: Parent bit is set if ANY child bit is set AND that child's mask bit is enabled. Allows runtime suppression of specific alarms.

**AND**: Parent bit is set only if ALL children's bits are set. Used for "all ready" or "all healthy" indicators.

## DSL Structure

### 1. Schema Declaration

Every schema file begins with a schema declaration:
```lua
local S = require("schema_builder")

S.schema("my_system", "1.0.0")
```

This initializes the schema builder and sets the name (used for generated file names and C identifiers).

### 2. Buffer Definitions

Buffers must be defined before classes. Each buffer represents a parallel bit array through the entire tree:
```lua
S.buffer("ALARM_LATCHED", "OR_LATCH")  -- Latching alarms
S.buffer("ALARM_MASK", "OR_MASK")      -- Maskable alarms  
S.buffer("STATUS_FLAGS", "AND")        -- Ready indicators
```

**Rules:**
- Buffer names must be unique
- Valid types: `"OR_LATCH"`, `"OR_MASK"`, `"AND"`
- All classes must declare allocation for all buffers

### 3. Class Definitions

Classes define the **structure of leaf nodes** - the actual hardware interfaces.

#### Basic Class Syntax
```lua
S.class("ClassName", {buffer1 = bits, buffer2 = bits, ...})
  -- Optional: name the bits
  S.bits("buffer_name", "bit0", "bit1", "bit2", ...)
S.end_class()
```

#### CRITICAL: Balanced Open/Close

**Every `S.class()` MUST have a matching `S.end_class()`**
```lua
-- CORRECT
S.class("Motor", {ALARM = 8, STATUS = 4})
  S.bits("ALARM", "overtemp", "overcurrent", "stall")
  S.bits("STATUS", "enabled", "running", "ready")
S.end_class()  -- ✓ Properly closed

-- INCORRECT
S.class("Motor", {ALARM = 8, STATUS = 4})
  S.bits("ALARM", "overtemp", "overcurrent")
-- Missing S.end_class() - WILL ERROR!
```

#### Buffer Allocation

Each class must specify bit allocation for **every defined buffer**:
```lua
-- If you have 3 buffers, specify all 3
S.buffer("ALARM", "OR_LATCH")
S.buffer("ENABLE", "OR_MASK")
S.buffer("STATUS", "AND")

-- This class uses ALARM and STATUS, but not ENABLE
S.class("SimpleSensor", {ALARM = 8, ENABLE = 0, STATUS = 2})
  S.bits("ALARM", "fault", "disconnected")
  S.bits("STATUS", "powered", "ready")
S.end_class()
```

**Use `0` to opt out of a buffer.** The validator will error if any buffer is missing.

#### Bit Naming

The `S.bits()` function names individual bits within a buffer. These names become **compile-time constants** in the generated header:
```lua
S.class("Valve", {ALARM = 8, STATUS = 4})
  S.bits("ALARM", "overcurrent", "stuck_open", "stuck_closed", "leak")
  S.bits("STATUS", "enabled", "calibrated", "ready")
S.end_class()
```

Generates in `.h` file:
```c
#define MY_SYSTEM_VALVE_ALARM_OVERCURRENT 0
#define MY_SYSTEM_VALVE_ALARM_STUCK_OPEN 1
#define MY_SYSTEM_VALVE_ALARM_STUCK_CLOSED 2
#define MY_SYSTEM_VALVE_ALARM_LEAK 3

#define MY_SYSTEM_VALVE_STATUS_ENABLED 0
#define MY_SYSTEM_VALVE_STATUS_CALIBRATED 1
#define MY_SYSTEM_VALVE_STATUS_READY 2
```

**Your C code can then use these names directly:**
```c
// Set a specific alarm bit
cfl_hbit_shadow_set_bit(inst, 
    MY_SYSTEM_BUF_ALARM,
    valve_node_id,
    MY_SYSTEM_VALVE_ALARM_OVERCURRENT);
```

**Rules for bit naming:**
- Optional (you can omit `S.bits()` entirely)
- Number of names must not exceed buffer size
- Indices are 0-based in order of declaration
- Names become uppercase in C with underscores

### 4. Node Tree Definition

Nodes instantiate classes into a hierarchy. The tree structure uses **balanced open/close operators**.

#### Basic Node Syntax
```lua
S.node("node_name", "ClassName")
  -- Child nodes go here
S.end_node()
```

#### CRITICAL: Balanced Tree Structure

**Every `S.node()` MUST have a matching `S.end_node()`**
```lua
-- CORRECT - Properly balanced
S.node("SYSTEM", "SystemAggregate")
  S.node("STATION_1", "StationAggregate")
    S.node("VALVE_1", "Valve")
    S.end_node()  -- ✓ Closes VALVE_1
    S.node("VALVE_2", "Valve")
    S.end_node()  -- ✓ Closes VALVE_2
  S.end_node()  -- ✓ Closes STATION_1
  S.node("STATION_2", "StationAggregate")
    S.node("VALVE_1", "Valve")
    S.end_node()  -- ✓ Closes VALVE_1
  S.end_node()  -- ✓ Closes STATION_2
S.end_node()  -- ✓ Closes SYSTEM

-- INCORRECT - Unbalanced
S.node("SYSTEM", "SystemAggregate")
  S.node("STATION_1", "StationAggregate")
    S.node("VALVE_1", "Valve")
    S.end_node()
  -- Missing S.end_node() for STATION_1!
  S.node("STATION_2", "StationAggregate")
  S.end_node()
S.end_node()
-- WILL ERROR: "Unclosed nodes: SYSTEM.STATION_1"
```

#### Leaf vs. Aggregate Nodes

**ONLY LEAF NODES REQUIRE USER-DEFINED CLASSES**

A node is automatically a **leaf** if it has no children:
```lua
S.node("VALVE_1", "Valve")  -- Leaf node (no children)
S.end_node()
```

A node is automatically an **aggregate** if it has children:
```lua
S.node("STATION_1", "StationAggregate")  -- Aggregate (has children)
  S.node("VALVE_1", "Valve")
  S.end_node()
  S.node("VALVE_2", "Valve")
  S.end_node()
S.end_node()
```

**For aggregate nodes, classes are AUTO-GENERATED** by computing the maximum buffer sizes of all children:
```lua
-- User defines leaf class
S.class("Valve", {ALARM = 8, STATUS = 4})
  S.bits("ALARM", "fault", "stuck")
  S.bits("STATUS", "ready", "enabled")
S.end_class()

-- User creates tree
S.node("STATION", "StationAggregate")  -- Class doesn't exist yet!
  S.node("VALVE_1", "Valve")           -- Child: ALARM=8, STATUS=4
  S.end_node()
  S.node("VALVE_2", "Valve")           -- Child: ALARM=8, STATUS=4
  S.end_node()
  S.node("SENSOR", "Sensor")           -- Child: ALARM=4, STATUS=2
  S.end_node()
S.end_node()

-- Code generator automatically creates:
-- StationAggregate class with {ALARM = max(8,8,4) = 8, STATUS = max(4,4,2) = 4}
```

**Why auto-generate aggregate classes?**
- Parent nodes need enough bits to OR/AND all children's bits
- Maximum size ensures no information is lost during propagation
- Eliminates manual calculation and maintenance
- Aggregate classes never need bit names (only leaves do)

#### Node Paths and Identifiers

Node names build hierarchical paths using dots:
```lua
S.node("SYSTEM", "Root")
  S.node("STATION_1", "Station")
    S.node("VALVE", "Valve")
    S.end_node()  -- Full path: SYSTEM.STATION_1.VALVE
  S.end_node()
S.end_node()
```

Generated as compile-time constants:
```c
typedef enum {
    MY_SYSTEM_NODE_SYSTEM = 0,
    MY_SYSTEM_NODE_SYSTEM_STATION_1 = 1,
    MY_SYSTEM_NODE_SYSTEM_STATION_1_VALVE = 2,
} my_system_node_id_t;
```

And as hash constants for runtime lookup:
```c
#define MY_SYSTEM_HASH_SYSTEM 0xA1B2C3D4U
#define MY_SYSTEM_HASH_SYSTEM_STATION_1 0xB2C3D4E5U
#define MY_SYSTEM_HASH_SYSTEM_STATION_1_VALVE 0xC3D4E5F6U
```

### 5. Schema Completion

Every schema file must end with:
```lua
return S.build()
```

This triggers validation and returns the complete schema table for code generation.

## Complete Example
```lua
local S = require("schema_builder")

-- 1. Declare schema
S.schema("irrigation", "1.0.0")

-- 2. Define buffers
S.buffer("ALARM", "OR_LATCH")
S.buffer("ENABLE", "OR_MASK")  
S.buffer("READY", "AND")

-- 3. Define leaf classes
S.class("Valve", {ALARM = 8, ENABLE = 8, READY = 4})
  S.bits("ALARM", "overcurrent", "stuck_open", "stuck_closed", "leak",
                  "overtemp", "low_pressure", "high_pressure")
  S.bits("ENABLE", "overcurrent", "stuck_open", "stuck_closed", "leak",
                   "overtemp", "low_pressure", "high_pressure")
  S.bits("READY", "powered", "calibrated", "enabled", "ok")
S.end_class()

S.class("Pump", {ALARM = 4, ENABLE = 4, READY = 2})
  S.bits("ALARM", "overload", "cavitation", "overheat")
  S.bits("ENABLE", "overload", "cavitation", "overheat")
  S.bits("READY", "powered", "primed")
S.end_class()

-- 4. Define node tree (only leaves need defined classes)
S.node("SYSTEM", "SystemAggregate")  -- Auto-generated class

  S.node("ZONE_1", "ZoneAggregate")  -- Auto-generated class
    S.node("VALVE_1", "Valve")       -- User-defined class
    S.end_node()
    S.node("VALVE_2", "Valve")
    S.end_node()
    S.node("PUMP", "Pump")           -- User-defined class
    S.end_node()
  S.end_node()

  S.node("ZONE_2", "ZoneAggregate")  -- Auto-generated class
    S.node("VALVE_1", "Valve")
    S.end_node()
    S.node("VALVE_2", "Valve")
    S.end_node()
  S.end_node()

S.end_node()

-- 5. Build and return
return S.build()
```

## Validation Rules

The schema builder enforces:

1. **Buffer uniqueness**: No duplicate buffer names
2. **Buffer completeness**: All classes must specify all buffers
3. **Class completeness**: All leaf nodes must reference defined classes
4. **Balanced structure**: All `S.class()` have matching `S.end_class()`
5. **Balanced tree**: All `S.node()` have matching `S.end_node()`
6. **Bit count**: Named bits cannot exceed buffer size
7. **Valid buffer types**: Only `OR_LATCH`, `OR_MASK`, `AND` allowed

Validation errors are collected and reported together:
```
Schema errors:
  Duplicate buffer name: 'ALARM'
  Class 'Valve' missing buffer 'STATUS' (use 0 to opt out)
  Unclosed nodes: SYSTEM.ZONE_1
  Class 'Motor' buffer 'ALARM' has 8 bits but 10 names provided
```

## Code Generation

Run the code generator:
```bash
luajit codegen.lua my_schema.lua output/
```

Generates two files:

### `generated_<name>.h`
- Include in your application code
- Contains enums, constants, and inline lookup functions
- No storage overhead (all `static const` or `inline`)

### `generated_<name>_data.h`
- Include in **exactly ONE** `.c` file
- Contains static data tables (node descriptors, arena layouts)
- Provides `<name>_config` struct for runtime initialization

## Usage in C Code
```c
#include "cfl_hbit.h"
#include "generated_irrigation.h"
#include "generated_irrigation_data.h"

// Create instance
cfl_hbit_instance_t* inst = cfl_hbit_create(
    &allocator, 
    (const cfl_hbit_config_t*)&irrigation_config);

// Set alarm using generated constants
cfl_hbit_shadow_set_bit(inst,
    IRRIGATION_BUF_ALARM,
    IRRIGATION_NODE_SYSTEM_ZONE_1_VALVE_1,
    IRRIGATION_VALVE_ALARM_OVERCURRENT);

// Propagate up tree
cfl_hbit_sync_and_propagate(inst);

// Check if entire zone has any alarms
bool zone_alarm = cfl_hbit_read_bit(inst,
    IRRIGATION_BUF_ALARM,
    IRRIGATION_NODE_SYSTEM_ZONE_1,
    0);  // Bit 0 of aggregate contains OR of all children
```

## Best Practices

### Naming Conventions

- **Buffers**: `SCREAMING_SNAKE_CASE` (e.g., `ALARM_LATCHED`, `STATUS_FLAGS`)
- **Classes**: `PascalCase` with suffix (e.g., `Valve_Leaf`, `Zone_Aggregate`)
- **Nodes**: `SCREAMING_SNAKE_CASE` (e.g., `ZONE_1`, `VALVE_BANK_A`)
- **Bits**: `lowercase_snake` (e.g., `overcurrent`, `stuck_open`)

### Tree Design

1. **Keep trees shallow** (3-4 levels max) for faster propagation
2. **Group related hardware** under common parents
3. **Use descriptive names** that match physical layout
4. **Mirror physical hierarchy** (building → floor → room → device)

### Class Organization

1. **Define leaf classes first** before building tree
2. **Group similar devices** into common classes
3. **Use 0 for unused buffers** rather than omitting
4. **Name important bits** that will be referenced in code
5. **Leave unnamed bits** for future expansion or debugging

### Aggregate Classes

1. **Never manually define** aggregate classes - let auto-generation handle it
2. **Use descriptive aggregate names** that indicate grouping level
3. **Trust the auto-sizing** - it computes the maximum needed

## Common Errors

### Unbalanced Structure
```lua
-- ERROR: Missing end_class()
S.class("Valve", {ALARM = 8})
  S.bits("ALARM", "fault")
S.node("VALVE_1", "Valve")  -- WRONG: Started node without closing class
```

**Fix:** Always close structures before starting new ones:
```lua
S.class("Valve", {ALARM = 8})
  S.bits("ALARM", "fault")
S.end_class()  -- ✓ Close class first

S.node("VALVE_1", "Valve")
S.end_node()
```

### Missing Buffer Allocation
```lua
S.buffer("ALARM", "OR_LATCH")
S.buffer("STATUS", "AND")

-- ERROR: Missing STATUS buffer
S.class("Valve", {ALARM = 8})
  S.bits("ALARM", "fault")
S.end_class()
```

**Fix:** Specify all buffers, use 0 to opt out:
```lua
S.class("Valve", {ALARM = 8, STATUS = 0})
  S.bits("ALARM", "fault")
S.end_class()
```

### Undefined Leaf Class
```lua
S.node("VALVE_1", "Valve")  -- ERROR: Class 'Valve' doesn't exist
S.end_node()
```

**Fix:** Define the class before using it:
```lua
S.class("Valve", {ALARM = 8})
S.end_class()

S.node("VALVE_1", "Valve")  -- ✓ Now it works
S.end_node()
```

### Defining Aggregate Classes
```lua
-- DON'T DO THIS: Manually defining aggregate class
S.class("StationAggregate", {ALARM = 8, STATUS = 4})
S.end_class()

S.node("STATION", "StationAggregate")
  S.node("VALVE", "Valve")
  S.end_node()
S.end_node()
```

**Better:** Let auto-generation handle it:
```lua
-- Just use the name - class will be auto-generated with correct sizes
S.node("STATION", "StationAggregate")  
  S.node("VALVE", "Valve")
  S.end_node()
S.end_node()
```

## Advanced Topics

### Multiple Root Trees

You can define multiple independent trees in one schema:
```lua
S.node("ALARM_TREE", "AlarmRoot")
  -- Alarm hierarchy
S.end_node()

S.node("STATUS_TREE", "StatusRoot")
  -- Status hierarchy  
S.end_node()
```

Each root propagates independently. Useful for separating concerns (alarms vs. status vs. enables).

### Mixing Buffer Types

Different buffers can use different logic on the same tree:
```lua
S.buffer("ALARM", "OR_LATCH")   -- Any alarm propagates up
S.buffer("ENABLE", "OR_MASK")   -- Masked enables propagate
S.buffer("READY", "AND")        -- All must be ready

S.class("Device", {ALARM = 8, ENABLE = 8, READY = 4})
  -- Same tree, different propagation logic per buffer
S.end_class()
```

This lets you express: "Any alarm anywhere is critical" (OR) while also tracking "All devices ready" (AND).

## Summary

The Hierarchical Bitmap DSL provides:

✓ **Declarative tree specification** with balanced open/close operators  
✓ **Automatic aggregate class generation** - only define leaf classes  
✓ **Compile-time constants** for bit names and node IDs  
✓ **Type-safe C code** with no magic numbers  
✓ **Efficient packed representation** in embedded systems  
✓ **Three propagation modes** (OR_LATCH, OR_MASK, AND)  
✓ **Clear validation errors** for structural mistakes  

All complexity is handled at code-generation time, resulting in zero-overhead runtime access to hierarchical status information.