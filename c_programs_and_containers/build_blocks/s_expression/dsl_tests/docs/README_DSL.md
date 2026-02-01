# S-Expression Engine DSL v5.2

A LuaJIT-based domain-specific language for defining behavior trees, state machines, and sequential control flows that compile to zero-copy binary modules for embedded systems.

## Overview

The S-Expression Engine DSL provides a high-level Lua API for defining structured control flow that compiles to efficient binary bytecode. The system targets embedded platforms from 32KB ARM Cortex-M microcontrollers to full ARM64/AMD64 systems with 8GB+ RAM.

**Key Features:**
- Zero-copy binary loading (cast pointer directly from ROM)
- Two binary formats: 32-bit (8-byte params) and 64-bit (16-byte params)
- FNV-1a 32-bit hash-based function dispatch
- Typed blackboard fields with compile-time layout
- Nested record support with embedded structures
- Dict/list/array/tuple data structures
- Composable predicate API

---

## File Structure

### Core DSL Files

| File | Purpose |
|------|---------|
| `s_expr_dsl.lua` | Main DSL library - provides all DSL functions, type system, hash functions |
| `s_expr_generators.lua` | Code generators for C headers and binary modules (loaded by s_expr_dsl.lua) |
| `s_expr_debug.lua` | Debug output generation (loaded by s_expr_dsl.lua) |
| `s_compile.lua` | Command-line compiler that processes DSL files |
| `s_engine_helpers.lua` | High-level helper functions wrapping engine builtins |

### Reference DSL Files

| File | Purpose |
|------|---------|
| `s_expr_tutorial.lua` | Basic record types, field access, arrays, constants |
| `state_machine.lua` | State machine patterns using `se_state_machine` and `se_field_dispatch` |

### Generated Output Files

| File Pattern | Content |
|--------------|---------|
| `<base>_records.h` | C struct definitions for records |
| `<base>.h` | Module header with hashes and string table |
| `<base>_debug.h` | Debug hash-to-name mappings |
| `<base>_user_functions.h` | User function prototypes |
| `<base>_user_registration.c` | Function registration code |
| `<base>_32.bin` / `<base>_64.bin` | Binary module for runtime loading |
| `<base>_bin_32.h` / `<base>_bin_64.h` | Binary as C array for ROM embedding |
| `<base>_dump_32.h` / `<base>_dump_64.h` | Human-readable parameter dump |

---

## DSL Structure

### Module Definition

Every DSL file follows this structure:

```lua
local M = require("s_expr_dsl")
local mod = start_module("module_name")
use_32bit()  -- or use_64bit()
set_debug(true)  -- optional

-- Records (data structures)
RECORD("my_record")
    FIELD("counter", "int32")
    FIELD("temperature", "float")
END_RECORD()

-- Constants (pre-initialized records)
CONST("defaults", "my_record")
    VALUE("counter", 0)
    VALUE("temperature", 20.0)
END_CONST()

-- Trees (behavior trees)
start_tree("my_tree")
    use_record("my_record")
    use_defaults("defaults")
    -- tree content
end_tree("my_tree")

return end_module(mod)
```

---

## Record Types

### Scalar Fields

```lua
RECORD("ScalarDemo")
    FIELD("counter", "int32")      -- 32-bit signed integer
    FIELD("flags", "uint32")       -- 32-bit unsigned integer
    FIELD("temperature", "float")  -- 32-bit float
    FIELD("timestamp", "int64")    -- 64-bit signed integer
    FIELD("checksum", "uint64")    -- 64-bit unsigned integer
    FIELD("precise", "double")     -- 64-bit float
END_RECORD()
```

**Note:** Sub-32-bit types (`int8`, `uint8`, `int16`, `uint16`, `bool`, `char`) are NOT allowed in `FIELD()` because 32-bit writes would corrupt adjacent fields. Use `CHAR_ARRAY()` for strings.

### Array Fields

```lua
RECORD("ArrayDemo")
    CHAR_ARRAY("name", 32)           -- Character buffer (min 4 bytes)
    INT32_ARRAY("int_values", 4)     -- Array of 4 int32
    FLOAT32_ARRAY("float_values", 4) -- Array of 4 float
END_RECORD()
```

### Embedded Records

```lua
RECORD("Vector3")
    FIELD("x", "float")
    FIELD("y", "float")
    FIELD("z", "float")
END_RECORD()

RECORD("Transform")
    FIELD("position", "Vector3")  -- Embedded record
    FIELD("rotation", "Vector3")
    FIELD("scale", "float")
END_RECORD()
```

### Pointer Fields

```lua
RECORD("LinkedNode")
    FIELD("value", "int32")
    FIELD("pad", "uint32")
    PTR64_FIELD("next", "LinkedNode")  -- Always 64-bit storage
    PTR64_FIELD("data", "void")
END_RECORD()
```

---

## Tree Definition

### Basic Structure

```lua
start_tree("tree_name")
    use_record("blackboard_record")      -- Associate blackboard type
    use_defaults("constant_name")        -- Optional: initialize from constant
    
    -- Tree content goes here
    
end_tree("tree_name")
```

### Call Types

| Function | Type | Returns | Use Case |
|----------|------|---------|----------|
| `o_call("NAME")` | Oneshot | void | Fire-once initialization |
| `io_call("NAME")` | Init-oneshot | void | Survives reset, runs once |
| `m_call("NAME")` | Main | result code | Primary execution |
| `p_call("NAME")` | Predicate | bool | Condition checking |
| `pt_m_call("NAME")` | Pointer main | result code | Allocates pointer slot |

### Parameter Functions

```lua
local c = m_call("FUNCTION_NAME")
    int(42)              -- 32/64-bit signed integer
    uint(0xDEADBEEF)     -- 32/64-bit unsigned integer
    flt(3.14159)         -- 32/64-bit float
    str("hello")         -- String (indexed in string table)
    str_ptr("world")     -- String pointer
    str_hash("key")      -- Pre-computed FNV-1a hash
    field_ref("counter") -- Field offset reference
    nested_field_ref("position.x")  -- Nested field path
    const_ref("defaults")           -- Constant reference
end_call(c)
```

### Data Structures

```lua
-- List
local l = list_start("my_list")
    int(1)
    int(2)
    int(3)
list_end(l)

-- Dictionary
local d = dict_start("my_dict")
    local k1 = key("name")
        str("value")
    key_end(k1)
dict_end(d)

-- Array
local a = array_start("my_array")
    int(10)
    int(20)
array_end(a)

-- Tuple
local t = tuple_start("my_tuple")
    int(1)
    str("two")
tuple_end(t)

-- JSON shorthand
json({ key1 = "value1", key2 = 123, nested = { a = 1, b = 2 } })
json_hash({ key1 = "value1" })  -- Uses hash keys for faster lookup
```

---

## Helper Functions (s_engine_helpers.lua)

### Sequence Control

```lua
se_sequence(function()
    -- Children execute in order
    se_log("Step 1")
    se_tick_delay(100)
    se_log("Step 2")
end)
```

### State Machine

```lua
-- Pattern 1: Using se_state_machine with action functions
actions_fn = {}

actions_fn[1] = function()
    se_log("State 0")
    se_tick_delay(100)
    se_set_field("state", 1)
    se_return_halt()
end

actions_fn[2] = function()
    se_log("State 1")
    se_tick_delay(100)
    se_set_field("state", 2)
    se_return_halt()
end

actions_fn[3] = function()
    se_log("State 2 - final")
    se_return_terminate()
end

se_state_machine("state", actions_fn)
```

### Field Dispatch (with se_case)

```lua
-- Pattern 2: Using se_field_dispatch with se_case
case_fn = {}

case_fn[1] = function() 
    se_case(0, function()
        se_sequence(function()
            se_log("Case 0")
            se_tick_delay(100)
            se_set_field("state", 1)
            se_return_halt()
        end)
    end) 
end

case_fn[2] = function() 
    se_case(1, function()
        se_sequence(function()
            se_log("Case 1")
            se_return_terminate()
        end)
    end) 
end

case_fn[3] = function() 
    se_case('default', function()
        se_log("Default case")
        se_return_halt()
    end) 
end

se_field_dispatch("state", case_fn)
```

### Field Operations

```lua
se_set_field("counter", 42)     -- Set field value (oneshot)
se_i_set_field("state", 0)      -- Set field on init only (io-oneshot)
```

### Delays and Timing

```lua
se_tick_delay(100)              -- Wait 100 ticks
```

### Logging

```lua
se_log("Debug message")         -- Output via debug callback
```

### Return Codes

```lua
se_return_continue()            -- SE_CONTINUE (0) - keep going
se_return_halt()                -- SE_HALT (1) - stop this tick
se_return_terminate()           -- SE_TERMINATE (2) - shut down tree
se_return_reset()               -- SE_RESET (3) - reset node
se_return_disable()             -- SE_DISABLE (4) - disable node
se_return_skip_continue()       -- SE_SKIP_CONTINUE (5)
se_return_function_halt()       -- SE_FUNCTION_HALT (6)
se_return_function_reset()      -- SE_FUNCTION_RESET (7)
se_return_function_terminate()  -- SE_FUNCTION_TERMINATE (8)
```

---

## Result Codes

| Code | Name | Value | Meaning |
|------|------|-------|---------|
| SE_CONTINUE | Continue | 0 | Keep processing siblings |
| SE_HALT | Halt | 1 | Stop this tick, resume next tick |
| SE_TERMINATE | Terminate | 2 | Shut down the tree |
| SE_RESET | Reset | 3 | Reset current node |
| SE_DISABLE | Disable | 4 | Disable current node |
| SE_SKIP_CONTINUE | Skip Continue | 5 | Skip to next sibling |
| SE_FUNCTION_HALT | Function Halt | 6 | Halt at function level |
| SE_FUNCTION_RESET | Function Reset | 7 | Reset function |
| SE_FUNCTION_TERMINATE | Function Terminate | 8 | Terminate function |
| SE_PIPELINE_TERMINATE | Pipeline Terminate | 9 | Terminate pipeline |
| SE_PIPELINE_RESET_CONTINUE | Pipeline Reset Continue | 10 | Reset and continue |
| SE_PIPELINE_RESET_HALT | Pipeline Reset Halt | 11 | Reset and halt |
| SE_PIPELINE_DISABLE | Pipeline Disable | 12 | Disable pipeline |

---

## Compilation

### Basic Usage

```bash
luajit s_compile.lua input.lua --all --outdir=output/
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--header=<file>` | Generate main C header |
| `--user=<file>` | Generate user function header |
| `--reg=<file>` | Generate user registration code |
| `--records=<file>` | Generate records header |
| `--debug=<file>` | Generate debug header |
| `--binary=<file>` | Generate binary module (.bin) |
| `--binary-h=<file>` | Generate binary as C header |
| `--dump-h=<file>` | Generate parameter dump header |
| `--helpers=<file>` | Load helper functions |
| `--dump` | Print debug dump to stdout |
| `--all` | Generate all text outputs |
| `--all-bin` | Generate all outputs including binary |
| `--outdir=<dir>` | Output directory |
| `--32bit` | Force 32-bit mode (default) |
| `--64bit` | Force 64-bit mode |

### Examples

```bash
# Generate all files for 32-bit target
luajit s_compile.lua state_machine.lua --helpers=s_engine_helpers.lua --all-bin --outdir=generated/

# Generate only binary header for 64-bit
luajit s_compile.lua my_module.lua --binary-h=my_module_bin_64.h --64bit

# Debug dump only
luajit s_compile.lua my_module.lua --dump
```

---

## Reference: state_machine.lua

The `state_machine.lua` file demonstrates two patterns for implementing state machines:

### Pattern 1: se_field_dispatch with se_case

Uses explicit case values with duplicate detection:

```lua
case_fn = {}

case_fn[1] = function() 
    se_case(0, function()
        se_sequence(function()
            se_log("State 0")
            se_tick_delay(100)
            se_set_field("state", 1)
            se_return_halt()
        end)
    end) 
end
-- ... more cases ...

start_tree("state_machine_test")
    use_record("state_machine_blackboard")
    se_sequence(function()
        se_i_set_field("state", 0)
        se_log("State machine test started")
        se_state_machine("state", case_fn)
    end)
end_tree("state_machine_test")
```

### Pattern 2: se_state_machine with index-based dispatch

Uses array index as state value (0-indexed internally):

```lua
actions_fn = {}

actions_fn[1] = function()  -- State 0
    se_log("State 0")
    se_tick_delay(100)
    se_set_field("state", 1)
    se_return_halt()
end
-- ... more actions ...

start_tree("state_machine_test_alt")
    use_record("state_machine_blackboard")
    local seq0 = m_call("SE_SEQUENCE")
        se_i_set_field("state", 0)
        se_log("State machine test started (alt)")
        se_state_machine("state", actions_fn)
    end_call(seq0)
end_tree("state_machine_test_alt")
```

### State Flow

```
┌─────────────────────────────────────────────────────────┐
│  Tick 0-99:   state=0, State 0 running, returns SE_HALT │
│  Tick 99:     State 0 sets state=1                      │
│  Tick 100-199: state=1, State 1 running, returns SE_HALT│
│  Tick 199:    State 1 sets state=2                      │
│  Tick 200-299: state=2, State 2 running                 │
│  Tick 299:    State 2 returns SE_TERMINATE              │
└─────────────────────────────────────────────────────────┘
```

---

## Reference: s_expr_tutorial.lua

The `s_expr_tutorial.lua` file demonstrates:

1. **Basic scalar types** - int32, uint32, float, int64, uint64, double
2. **Array types** - CHAR_ARRAY, INT32_ARRAY, FLOAT32_ARRAY
3. **Embedded records** - Vector3 inside Transform
4. **Pointer slots** - PTR64_FIELD for runtime-assigned pointers
5. **Constants** - Pre-initialized record values
6. **Blackboard access** - Direct struct access in C
7. **Slot access** - field_ref parameter passing
8. **Nested field access** - nested_field_ref for embedded records

Each tree uses a "verify pattern": write a value, then verify with an expected value parameter.

---

## Engine Built-in Functions

These are registered automatically via `s_engine_register_builtins()`:

| DSL Helper | Engine Function | Type | Description |
|------------|-----------------|------|-------------|
| `se_sequence` | `SE_SEQUENCE` | main | Execute children in order |
| `se_state_machine` | `SE_STATE_MACHINE` | main | Field-based state dispatch |
| `se_field_dispatch` | `SE_FIELD_DISPATCH` | main | Value-based dispatch |
| `se_tick_delay` | `SE_TICK_DELAY` | main | Wait N ticks |
| `se_set_field` | `SE_SET_FIELD` | oneshot | Set blackboard field |
| `se_log` | `SE_LOG` | oneshot | Debug output |
| `se_return_*` | `SE_RETURN_*` | main | Return specific codes |

---

## Binary Format

The binary module format (v5.2) provides:

- **Magic**: `0x42584553` ("SEXB")
- **Version**: `0x0502`
- **Direct s_expr_param_t structs** - no decoding needed
- **Zero-copy loading** - cast pointer directly from ROM
- **Position-independent** - no absolute addresses

### 32-bit vs 64-bit

| Mode | Param Size | Pointer Size | Use Case |
|------|------------|--------------|----------|
| 32-bit | 8 bytes | 4 bytes | ARM Cortex-M, ESP32 |
| 64-bit | 16 bytes | 8 bytes | ARM64, AMD64, servers |

---

## License

MIT License - See individual repository LICENSE files.