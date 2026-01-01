# ChainTree S-Expression DSL v3.0

A LuaJIT-based domain-specific language for defining behavior trees, state machines, and sequential control flows that compile to embedded C code. Designed for resource-constrained systems from 32KB ARM Cortex-M microcontrollers to 8GB+ servers.

## Overview

The ChainTree DSL provides:

- **Declarative tree definitions** using S-expression-style syntax in Lua
- **Hash-based function dispatch** for efficient runtime lookup
- **Blackboard records** with embedded structs and pointer fields
- **Compile-time validation** catching errors before runtime
- **Generated C code** with type-safe function signatures
- **Debug/release builds** via conditional compilation

## File Structure
```
project/
├── s_expr_dsl.lua              # DSL library (DO NOT EDIT)
├── s_compile.lua               # Compiler driver
├── my_module.lua               # Your module definition
│
├── my_module_module.h          # Generated: trees, params, module def
├── my_module_user_functions.h  # Generated: user function prototypes
├── my_module_user_registration.c # Generated: registration tables
│
└── my_module_impl.c            # Your implementation (USER CREATES)
```

## Quick Start

### 1. Create Module Definition
```lua
-- my_module.lua
start_module("my_module")

-- Optional: enable debug mode
set_debug(false)

-- Define blackboard structure
RECORD("robot_state")
    FIELD("position_x", "float")
    FIELD("position_y", "float")
    FIELD("speed", "float")
    FIELD("state", "uint8")
END_RECORD()

-- Define behavior tree
start_tree("main_control")
    use_record("robot_state")
    
    local c = m_call("CFL_SEQUENCE")
        local a = o_call("READ_SENSORS")
        end_call(a)
        
        local b = m_call("PROCESS_INPUT")
            field_ref("position_x")
            field_ref("position_y")
        end_call(b)
        
        -- Debug logging (only in debug builds)
        if is_debug() then
            local d = o_call("CFL_LOG")
                str("Processing complete")
            end_call(d)
        end
    end_call(c)
    
end_tree("main_control")

return end_module("my_module")
```

### 2. Compile
```bash
luajit s_compile.lua my_module.lua --header=my_module_module.h
```

This generates three files:
- `my_module_module.h` - Module definition, trees, parameters
- `my_module_user_functions.h` - User function prototypes
- `my_module_user_registration.c` - Registration tables and load function

### 3. Implement User Functions

Create `my_module_impl.c`:
```c
#include "my_module_user_functions.h"

// DSL: READ_SENSORS  hash: 0x12345678
void read_sensors_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data)
{
    robot_state_t* bb = (robot_state_t*)inst->blackboard;
    // Read sensors into blackboard
    bb->position_x = read_sensor_x();
    bb->position_y = read_sensor_y();
}

// DSL: PROCESS_INPUT  hash: 0x87654321
s_expr_result_t process_input_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data)
{
    robot_state_t* bb = (robot_state_t*)inst->blackboard;
    // Process and return result
    return SE_CONTINUE;
}
```

### 4. Initialize at Runtime
```c
#include "my_module_module.h"
#include "my_module_user_functions.h"

void init_system(cfl_runtime_handle_t* handle) {
    // Load module
    s_expr_module_t* mod = s_expr_module_create(&my_module_module, allocator);
    
    // Register CFL system functions
    load_cfl_s_functions(handle);
    
    // Register user functions
    load_user_s_functions(handle);
}
```

## Compiler Usage
```
ChainTree S-Expression Compiler v3.0

Usage: luajit s_compile.lua <input.lua> [options]

Options:
  --bin=<file>         Generate binary file (.bin)
  --header=<file>      Generate C module header file (.h)
  --user-header=<file> Generate user functions header (.h)
  --user-reg=<file>    Generate user registration C file (.c)
  --name=<name>        Base name for generated symbols (default: from input)
  --dump               Show tree structure
  --stats              Show module statistics
  --help               Show this help

Examples:
  luajit s_compile.lua motor.lua --header=motor_module.h
  luajit s_compile.lua motor.lua --header=motor_module.h --user-header=motor_user.h --user-reg=motor_user.c
  luajit s_compile.lua motor.lua --dump --stats
```

If `--header` is specified without `--user-header` and `--user-reg`, user files are auto-generated with `_user_functions.h` and `_user_registration.c` suffixes.

## DSL Reference

### Module Structure
```lua
start_module("module_name")
    -- Records, pools, trees
return end_module("module_name")
```

### Debug Control
```lua
set_debug(true)   -- Enable debug mode
set_debug(false)  -- Disable debug mode (default)

if is_debug() then
    -- Code only included in debug builds
end
```

### Records (Blackboard Structures)

#### Basic Fields
```lua
RECORD("my_record")
    FIELD("name", "type")           -- Single field
    FIELD("name", "type", count)    -- Array field
END_RECORD()
```

**Supported Types:**

| Type | C Type | Size | Alignment |
|------|--------|------|-----------|
| `int8` | `int8_t` | 1 | 1 |
| `uint8` | `uint8_t` | 1 | 1 |
| `int16` | `int16_t` | 2 | 2 |
| `uint16` | `uint16_t` | 2 | 2 |
| `int32` | `int32_t` | 4 | 4 |
| `uint32` | `uint32_t` | 4 | 4 |
| `int64` | `int64_t` | 8 | 8 |
| `uint64` | `uint64_t` | 8 | 8 |
| `float` | `float` | 4 | 4 |
| `double` | `double` | 8 | 8 |
| `bool` | `bool` | 1 | 1 |

#### Embedded Records

Records can contain other records as embedded (inline) fields:
```lua
RECORD("vector3")
    FIELD("x", "float")
    FIELD("y", "float")
    FIELD("z", "float")
END_RECORD()

RECORD("transform")
    FIELD("position", "vector3")      -- Embedded (12 bytes inline)
    FIELD("rotation", "vector3")      -- Embedded (12 bytes inline)
    FIELD("scale", "float")
END_RECORD()

RECORD("path")
    FIELD("waypoints", "vector3", 10) -- Array of 10 embedded vector3
    FIELD("count", "uint16")
END_RECORD()
```

**Note:** Embedded records must be defined BEFORE they are used.

#### Pointer Fields

Use `PTR_FIELD` for pointers to other records:
```lua
RECORD("node")
    FIELD("value", "int32")
    PTR_FIELD("next", "node")         -- Pointer to node_t
    PTR_FIELD("parent", "node")       -- Pointer to node_t
END_RECORD()
```

**⚠️ Memory Management:** Pointer fields require user-managed memory. You must `malloc` and `free` the pointed-to memory yourself.
```c
node_t* node = get_blackboard(inst);
node->next = (node_t*)malloc(sizeof(node_t));  // User allocates
// ...
free(node->next);  // User frees
node->next = NULL;
```

### Trees
```lua
start_tree("tree_name")
    use_record("record_name")  -- Optional: associate blackboard
    
    -- Tree content (calls, parameters)
    
end_tree("tree_name")
```

### Function Calls

#### Oneshot Functions (void return, run once)
```lua
local c = o_call("FUNCTION_NAME")
    -- parameters
end_call(c)

-- With SURVIVES_RESET flag (persists across tree reset)
local c = io_call("INIT_FUNCTION")
    -- parameters
end_call(c)
```

#### Main Functions (s_expr_result_t return)
```lua
local c = m_call("FUNCTION_NAME")
    -- parameters
end_call(c)

-- With pointer tracking
local c = pt_m_call("FUNCTION_NAME")
    -- parameters (tracked for resume)
end_call(c)
```

#### Predicate Functions (bool return)
```lua
local c = p_call("FUNCTION_NAME")
    -- parameters
end_call(c)
```

### Parameters
```lua
int(42)              -- Signed integer
uint(100)            -- Unsigned integer
flt(3.14159)         -- Float
str("hello")         -- String (stored as hash)
slot_ref("slot_name") -- Pool slot reference
field_ref("field")   -- Blackboard field reference
nested_field_ref("pos.x")  -- Nested field in embedded record
```

### Lists
```lua
local l = list_start("items")
    int(1)
    int(2)
    int(3)
list_end(l)
```

### Pools and Slots
```lua
defpool("timers", "timer_t")
defslot("main_timer", "timers")
defslot("aux_timer", "timers")
```

### Platform Configuration
```lua
use_64bit()   -- 64-bit pointers (8 bytes)
use_32bit()   -- 32-bit pointers (4 bytes, default)
```

## Function Types

### System Functions (CFL_ prefix)

Built-in control flow functions provided by the runtime:

- `CFL_SEQUENCE` - Execute children in order, fail on first failure
- `CFL_SELECTOR` - Execute children until first success
- `CFL_PARALLEL` - Execute children concurrently
- `CFL_REPEAT` - Repeat child N times
- `CFL_WHILE` - Repeat while predicate is true
- `CFL_IF` - Conditional execution
- `CFL_LOG` - Debug logging
- `CFL_NOP` - No operation

### User Functions (no CFL_ prefix)

Custom functions you implement. Named entries generate function prototypes:

| DSL Name | Generated C Function |
|----------|---------------------|
| `MY_ACTION` (oneshot) | `my_action_oneshot(...)` |
| `MY_BEHAVIOR` (main) | `my_behavior_main(...)` |
| `MY_CHECK` (predicate) | `my_check_boolean(...)` |

## Generated Files

### Module Header (`*_module.h`)

Contains:
- Record structure definitions (C structs)
- Field descriptor arrays
- Function hash tables
- Tree parameter arrays
- Tree definitions
- Module definition

**DO NOT EDIT** - Regenerate from DSL.

### User Functions Header (`*_user_functions.h`)

Contains:
- Function prototypes for all user functions
- `load_user_s_functions()` declaration

**DO NOT EDIT** - Regenerate from DSL.

### User Registration (`*_user_registration.c`)

Contains:
- Named function entry tables
- Hash-based lookup tables
- `init_user_function_tables()` - Builds hash tables
- `load_user_s_functions()` - Registers functions with modules

**DO NOT EDIT** - Regenerate from DSL.

### User Implementation (`*_impl.c`)

**YOU CREATE THIS FILE.** Implements the user functions declared in the header.

## Function Signatures

### Oneshot Function
```c
void my_function_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data);
```

### Main Function
```c
s_expr_result_t my_function_main(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data);
```

**Return Values:**

| Value | Meaning |
|-------|---------|
| `SE_CONTINUE` | Success, continue execution |
| `SE_HALT` | Pause, resume next tick |
| `SE_TERMINATE` | Terminate tree |
| `SE_RESET` | Reset tree to initial state |
| `SE_DISABLE` | Disable tree |
| `SE_FUNCTION_TERMINATE` | Terminate current function |
| `SE_SKIP_CONTINUE` | Skip remaining siblings |
| `SE_FUNCTION_HALT` | Halt current function |
| `SE_FUNCTION_RESET` | Reset current function |

### Predicate Function
```c
bool my_function_boolean(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data);
```

## Multi-Module Support

The registration system supports multiple modules:
```c
void load_user_s_functions(cfl_runtime_handle_t* handle) {
    // Iterates all modules and registers functions with each
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        if (!modules[i]) continue;
        s_expr_module_register_oneshot(modules[i], &user_oneshot_table);
        s_expr_module_register_main(modules[i], &user_main_table);
        s_expr_module_register_pred(modules[i], &user_pred_table);
    }
}
```

## Debug vs Release Builds

Use `set_debug()` and `is_debug()` for conditional compilation:
```lua
start_module("my_module")

set_debug(false)  -- Set to true for debug builds

start_tree("control")
    local c = m_call("CFL_SEQUENCE")
        local a = m_call("DO_WORK")
        end_call(a)
        
        if is_debug() then
            local d = o_call("CFL_LOG")
                str("Work completed")
            end_call(d)
        end
    end_call(c)
end_tree("control")

return end_module("my_module")
```

Build scripts:
```bash
# Release build
luajit s_compile.lua my_module.lua --header=my_module.h

# Debug build (edit set_debug(true) in file)
luajit s_compile.lua my_module.lua --header=my_module_debug.h
```

## Error Handling

The DSL provides compile-time error detection:

- **Mismatched start/end**: `end_call('x') does not match m_call('y')`
- **Unclosed braces**: `unclosed: call('seq'), list('items')`
- **Hash collisions**: `HASH COLLISION in main table: 'X' collides with 'Y'`
- **Unknown types**: `Unknown field type: bad_type`
- **Missing records**: `Unknown record: undefined_record`
- **Duplicate definitions**: `Record already defined: my_record`

## Best Practices

1. **Define records before use** - Embedded records must exist before referencing
2. **Use consistent naming** - `UPPER_CASE` for DSL names, generates `lower_case_suffix` in C
3. **Manage pointer memory** - `PTR_FIELD` requires manual malloc/free
4. **Use debug mode for development** - Conditional logging aids debugging
5. **Validate with --dump** - Inspect tree structure before integration
6. **Keep trees focused** - One responsibility per tree

## Requirements

- LuaJIT 2.0+
- C99 compiler for generated code

## License

MIT License