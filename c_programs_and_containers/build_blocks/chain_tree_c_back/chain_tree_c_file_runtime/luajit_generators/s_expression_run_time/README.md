# ChainTree S-Expression Binary Module System

Complete solution for compiling S-expression DSL modules to binary format for ARM32/ARM64 targets.

## Files Overview

### Core DSL Library
- **s_expr_dsl.lua** - Main DSL library providing all functions for defining modules, records, trees, constants, and generating output

### Compiler
- **s_compile.lua** - Command-line compiler that processes DSL files and generates outputs

### CFL Helpers
- **s_cfl_helpers.lua** - High-level DSL wrappers for common CFL patterns (pipelines, delays, state machines, events)

### Runtime Headers (C)
- **s_engine_types.h** - Core runtime types (s_result_t, s_engine_ctx_t, etc.) and inline helpers
- **s_expr_binary.h** - Binary format specification (wire format structures, opcodes, type tags)
- **s_expr_binary_loader.h** - Runtime loader for loading binary modules from ROM/RAM/socket

### Utilities
- **s_binary_dump.lua** - Binary module inspector for debugging
- **s_binary_gen.lua** - Standalone binary generator (also integrated into s_expr_dsl.lua)

### Test/Example
- **chain_flow_dsl_tests.lua** - Comprehensive test module demonstrating all features

## Quick Start

```bash
# Generate all outputs (headers + binary)
luajit s_compile.lua chain_flow_dsl_tests.lua --all-bin --outdir=output/

# Generate only binary file
luajit s_compile.lua mymodule.lua --binary=mymodule.bin

# Generate binary as C header (for ROM embedding)
luajit s_compile.lua mymodule.lua --binary-h=mymodule_bin.h

# Inspect a binary
luajit s_binary_dump.lua output/chain_flow_dsl_tests.bin
```

## Compiler Options

```
--header=<file>     Main C header with hashes, strings, function tables
--user=<file>       User function prototypes (oneshot, main, pred)
--reg=<file>        Function registration code
--records=<file>    Standalone records header (struct definitions)
--binary=<file>     Binary module file
--binary-h=<file>   Binary as const uint8_t array
--helpers=<file>    Load helper functions (repeatable)
--dump              Debug dump of module structure
--all               Generate all text outputs
--all-bin           Generate all outputs including binary
--outdir=<dir>      Output directory (default: current)
--32bit             Generate for 32-bit pointers (default)
--64bit             Generate for 64-bit pointers
```

## DSL Usage

### Module Definition
```lua
require("s_expr_dsl")

start_module("my_module")
use_32bit()  -- or use_64bit()

-- Define records, trees, constants...

end_module()
```

### Records (Blackboard Schemas)
```lua
RECORD("my_blackboard")
    FIELD("counter", "uint32")
    FIELD("temperature", "float")
    FIELD("enabled", "bool")
    CHAR_ARRAY("name", 32)
    PTR_FIELD("next", "my_blackboard")  -- pointer to self
END_RECORD()

-- Embedded records
RECORD("position")
    FIELD("x", "float")
    FIELD("y", "float")
END_RECORD()

RECORD("robot")
    FIELD("pos", "position")  -- embedded
    FIELD("speed", "float")
END_RECORD()
```

### Trees (Behavior Trees)
```lua
start_tree("my_tree")
use_record("my_blackboard")

-- Oneshot calls (run once, return immediately)
o_call("LOG_MESSAGE")
    str("Starting tree")
end_call()

-- Main calls (tick-based, can return CONTINUE/HALT/TERMINATE)
m_call("WAIT_DELAY")
    uint(1000)  -- ms
    result(SE_HALT)
end_call()

-- Predicate calls (boolean check)
p_call("CHECK_FLAG")
    field_ref("enabled")
end_call()

end_tree()
```

### Constants (ROM Data)
```lua
CONST("default_pid", "pid_gains")
    VALUE("kp", 1.5)
    VALUE("ki", 0.1)
    VALUE("kd", 0.05)
END_CONST()
```

### CFL Helper Functions
```lua
require("s_cfl_helpers")

-- Pipeline (sequence of steps)
cfl_pipeline("pipeline_1")
    o_call("STEP_1") end_call()
    o_call("STEP_2") end_call()
end_call()

-- State machine
cfl_state_machine("state_field")
    -- states are dispatched by value
end_call()

-- Wait for event
cfl_wait_event("TIMER_TICK", SE_CONTINUE)

-- Copy constant to field
cfl_copy_const("target_field", "default_pid")
```

## Binary Format

The binary format is little-endian and position-independent:

```
┌────────────────────────────┐
│ Header (32 bytes)          │  Magic, version, counts, total_size
├────────────────────────────┤
│ Directory (32 bytes)       │  8 section offsets
├────────────────────────────┤
│ Trees                      │  name_hash, record_idx, bytecode_offset/size
├────────────────────────────┤
│ Records                    │  name_hash, field_count, size, field_table_offset
├────────────────────────────┤
│ Fields                     │  name_hash, type_tag, flags, offset, size, aux
├────────────────────────────┤
│ String Blob                │  Length-prefixed, 4-byte aligned
├────────────────────────────┤
│ Constants                  │  name_hash, record_idx, data_size, data_offset
├────────────────────────────┤
│ Constant Data              │  Raw bytes matching record layouts
├────────────────────────────┤
│ Function Tables            │  oneshot[], main[], pred[] hash arrays
├────────────────────────────┤
│ Bytecode                   │  Node headers + opcode/data pairs
└────────────────────────────┘
```

## C Runtime Integration

### Loading from ROM
```c
#include "s_expr_binary.h"
#include "s_expr_binary_loader.h"
#include "my_module_bin.h"  // Generated binary header

// Load static ROM data
sexb_load_result_t result = SEXB_LOAD_STATIC(my_module_module_bin);
if (result.error != SEXB_OK) {
    // Handle error
}

sexb_module_t* mod = result.module;

// Find tree by hash
const sexb_tree_t* tree = sexb_find_tree(mod, MY_TREE_HASH);

// Find record
const sexb_record_t* rec = sexb_find_record(mod, MY_BLACKBOARD_HASH);

// Get field offset
const sexb_field_t* field = sexb_find_field(rec, FIELD_COUNTER_HASH);
uint32_t* counter = (uint32_t*)((uint8_t*)blackboard + field->offset);

// Cleanup
sexb_free(&result);
```

### Loading from Socket/File
```c
// Read data into buffer...
uint8_t* data = ...;
size_t size = ...;

// Validate
sexb_error_t err = sexb_validate(data, size);
if (err != SEXB_OK) {
    // Handle error
}

// Load (makes internal copy)
sexb_load_result_t result = sexb_load_copy(data, size);

// Use module...

// Cleanup (frees the copy)
sexb_free_copy(&result);
```

## Generated Output Files

Running with `--all-bin`:
- `module.h` - Main header with hashes, string table, function declarations
- `module_records.h` - Standalone C structs for blackboard types
- `module_user_functions.h` - Function prototypes for user implementation
- `module_user_registration.c` - Function registration code
- `module.bin` - Binary module file
- `module_bin.h` - Binary as C array for ROM embedding

## Type System

| DSL Type | C Type | Tag | Size |
|----------|--------|-----|------|
| int8 | int8_t | 0x01 | 1 |
| int16 | int16_t | 0x02 | 2 |
| int32 | int32_t | 0x03 | 4 |
| int64 | int64_t | 0x04 | 8 |
| uint8 | uint8_t | 0x05 | 1 |
| uint16 | uint16_t | 0x06 | 2 |
| uint32 | uint32_t | 0x07 | 4 |
| uint64 | uint64_t | 0x08 | 8 |
| float | float | 0x09 | 4 |
| double | double | 0x0A | 8 |
| bool | bool/uint8_t | 0x0B | 1 |
| char | char | 0x0C | 1 |
| char[] | char[N] | 0x0D | N |
| ptr | void* | 0x0E | 4/8 |
| embedded | struct | 0x0F | varies |

## Function Types

| Call Type | Tag | Description |
|-----------|-----|-------------|
| o_call | 0x01 | Oneshot - runs once, no tick state |
| m_call | 0x02 | Main - tick-based, maintains state |
| p_call | 0x03 | Predicate - returns bool |
| pt_m_call | 0x04 | Protothread main - resumable |
| io_call | 0x05 | Init oneshot - survives reset |
| p_call_bit | 0x06 | Bit predicate - for bit blocks |

## Result Codes

```lua
SE_CONTINUE = 0           -- Continue running
SE_HALT = 1               -- Halt this branch
SE_FUNCTION_TERMINATE = 2 -- Terminate function
SE_RESET = 3              -- Reset tree
SE_ERROR = 4              -- Error occurred
```

## ARM32/ARM64 Compatibility

The binary format is designed for cross-platform compatibility:
- All integers are fixed-size (no size_t, no pointers in wire format)
- Offsets are relative, not absolute addresses
- Same binary works on ARM32 and ARM64
- Loader resolves offsets to pointers at load time
- 4-byte minimum alignment throughout