# Lua 5.3+ Bridge for ChainTree CFL

## Overview

The Lua 5.3 bridge allows ChainTree user functions to be written in Lua instead of C. The CFL engine and all built-in functions remain in C for performance. Only application-specific functions (the ones you write) cross the bridge.

This means you write behavior tree logic in Lua while the engine, event queue, timer system, heap, arena allocator, JSON decoder, and 133 built-in functions execute at native speed.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Your Application                │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Lua User │  │ Lua User │  │  Lua User     │  │
│  │ One-Shot │  │ Boolean  │  │  Main Fn      │  │
│  └────┬─────┘  └────┬─────┘  └──────┬────────┘  │
│       │              │               │           │
│  ┌────▼──────────────▼───────────────▼────────┐  │
│  │          cfl_lua53_bridge.c                │  │
│  │   32 one-shot + 32 boolean + 32 main       │  │
│  │   trampoline slots (X-macro generated)     │  │
│  └────────────────────┬───────────────────────┘  │
│                       │                          │
│  ┌────────────────────▼───────────────────────┐  │
│  │           CFL C Runtime Engine             │  │
│  │  runtime_binary + runtime_functions        │  │
│  │  (133 built-in functions in C)             │  │
│  └────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

## How Trampolines Work

The CFL engine dispatches functions by index into arrays of C function pointers. Lua functions cannot be stored in these arrays directly. The bridge solves this with **pre-generated trampoline functions**.

At compile time, an X-macro generates 32 unique C functions per type:

```c
// Generated at compile time — each one knows its slot number
static void os_tramp_0(void *h, unsigned ni) { cfl_lua_os_dispatch(0, h, ni); }
static void os_tramp_1(void *h, unsigned ni) { cfl_lua_os_dispatch(1, h, ni); }
// ... up to os_tramp_31
```

At registration time, when you register a Lua function:
1. The bridge assigns the next available slot (say slot 3)
2. Stores the Lua function as a Lua registry reference at `g_os_refs[3]`
3. Registers `os_tramp_3` as the C function pointer in the CFL image loader

At dispatch time, when the CFL engine calls `os_tramp_3(handle, node_index)`:
1. The trampoline extracts `lua_State*` from `handle->user_handle`
2. Looks up the Lua function via `lua_rawgeti(L, LUA_REGISTRYINDEX, g_os_refs[3])`
3. Pushes arguments and calls via `lua_pcall`
4. Maps the return value back to C

## Three Function Types

### One-Shot Functions

Called once when a node initializes or terminates. No return value.

**C signature:** `void fn(void *handle, unsigned node_index)`

**Lua signature:** `function my_one_shot(handle, node_index)`

The `handle` argument is a lightuserdata (pointer) passed to `cfl.*` bridge functions.

```lua
function activate_valve_one_shot(handle, node_index)
    local state = cfl.json_extract_string(handle, node_index, "node_dict.state")
    if state == "open" then
        print("Valve is open")
    end
end
```

### Main Functions

Called every tick for active nodes. Return a CFL result code.

**C signature:** `unsigned fn(void *handle, unsigned bool_fn_idx, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)`

**Lua signature:** `function my_main(handle, bool_fn_idx, node_index, event_type, event_id, event_data)`

Return values: `cfl.CONTINUE` (0), `cfl.HALT` (1), `cfl.TERMINATE` (2), `cfl.RESET` (3), `cfl.DISABLE` (4), `cfl.SKIP_CONTINUE` (5).

```lua
function sm_event_filtering_main_main(handle, bool_fn_idx, node_index,
                                       event_type, event_id, event_data)
    local ptr = cfl.arena_get(handle, node_index)
    local filter_id = cfl.read_i32(ptr, 0)  -- SM_EVENT_ID offset
    if event_id == filter_id then
        return cfl.CONTINUE
    end
    return cfl.CONTINUE
end
```

### Boolean Functions

Evaluate conditions. Return true/false. Receive events for lifecycle management.

**C signature:** `bool fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)`

**Lua signature:** `function my_bool(handle, node_index, event_type, event_id, event_data)`

```lua
function while_test_boolean(handle, node_index, event_type, event_id, event_data)
    local ptr = cfl.arena_get(handle, node_index)
    if event_id == cfl.INIT_EVENT then
        -- allocate and initialize loop counter
        local count_ptr = cfl.heap_alloc(handle, 4)
        cfl.write_ptr(ptr, 8, count_ptr)  -- WHILE_AUXILIARY_DATA offset
        local count = cfl.json_extract_int32(handle, node_index, "node_dict.user_data.count")
        cfl.write_i32(count_ptr, 0, count)
        return false
    end
    if event_id == cfl.TERMINATE_EVENT then
        local aux = cfl.read_ptr(ptr, 8)
        if aux then cfl.heap_free(handle, aux) end
        return false
    end
    -- Normal tick: compare current iteration to target
    local current = cfl.read_i32(ptr, 0)  -- WHILE_CURRENT_ITERATION offset
    local aux = cfl.read_ptr(ptr, 8)
    local target = cfl.read_i32(aux, 0)
    return current < target
end
```

## The "cfl" Lua Module

`cfl_lua_bridge_init(L)` creates a global `cfl` table with these functions:

### JSON Node Data Extraction

Every node can have attached JSON data (the `node_dict`). These functions decode it:

| Function | Arguments | Returns |
|----------|-----------|---------|
| `cfl.json_extract_string(handle, node_index, path)` | Dot-separated path | string or nil |
| `cfl.json_extract_int32(handle, node_index, path)` | | integer |
| `cfl.json_extract_float(handle, node_index, path)` | | number |
| `cfl.json_extract_bool(handle, node_index, path)` | | boolean |
| `cfl.json_print_node(handle, node_index)` | | (prints to stdout) |

### Blackboard Access

The blackboard is shared mutable state. Fields are accessed by name (FNV-1a hashed internally):

| Function | Arguments | Returns |
|----------|-----------|---------|
| `cfl.bb_get_int32(handle, field_name)` | | integer |
| `cfl.bb_set_int32(handle, field_name, value)` | | |
| `cfl.bb_get_float(handle, field_name)` | | number |
| `cfl.bb_set_float(handle, field_name, value)` | | |
| `cfl.bb_get_uint32(handle, field_name)` | | integer |
| `cfl.bb_set_uint32(handle, field_name, value)` | | |

### Arena and Heap Memory

The CFL runtime uses arena allocators for per-node state. Lua functions that interact with C runtime data structures need raw memory access:

| Function | Arguments | Returns |
|----------|-----------|---------|
| `cfl.arena_get(handle, node_index)` | | lightuserdata (ptr) or nil |
| `cfl.smart_alloc(handle, node_index, size)` | | lightuserdata (ptr) or nil |
| `cfl.additional_alloc(handle, node_index, size)` | | lightuserdata (ptr) or nil |
| `cfl.heap_alloc(handle, size)` | | lightuserdata (ptr) or nil |
| `cfl.heap_free(handle, ptr)` | | |

### Raw Memory Read/Write

For accessing C struct fields in arena memory. You need to know the field offsets (from `offsetof()` on the C struct):

| Function | Arguments | Returns |
|----------|-----------|---------|
| `cfl.read_i32(ptr, offset)` | | integer |
| `cfl.write_i32(ptr, offset, value)` | | |
| `cfl.read_u16(ptr, offset)` | | integer |
| `cfl.read_float(ptr, offset)` | | number |
| `cfl.read_ptr(ptr, offset)` | | lightuserdata or nil |
| `cfl.write_ptr(ptr, offset, value)` | | |

### Runtime Queries

| Function | Arguments | Returns |
|----------|-----------|---------|
| `cfl.get_event_index(handle, event_name)` | | integer (-1 if not found) |
| `cfl.get_node_parent(handle, node_index)` | | integer |
| `cfl.event_data_to_u16(event_data)` | | integer (extracts node ID from event_data pointer) |

### Constants

**Return codes:** `cfl.CONTINUE` (0), `cfl.HALT` (1), `cfl.TERMINATE` (2), `cfl.RESET` (3), `cfl.DISABLE` (4), `cfl.SKIP_CONTINUE` (5)

**Events:** `cfl.INIT_EVENT` (0), `cfl.TERMINATE_EVENT` (1), `cfl.START_TESTS` (2), `cfl.RAISE_EXCEPTION_EVENT` (12), `cfl.CHANGE_STATE_EVENT` (17), `cfl.RECOVERY_CHECK_EVENT` (0xFFFE), `cfl.RECOVERY_SEQ_EVAL` (0)

## C Struct Offset Convention

Lua functions that interact with C runtime data structures need to know field offsets. These are computed with `offsetof()` and declared as constants at the top of the Lua file:

```lua
-- 64-bit platform offsets (from offsetof() on C structs)
local WHILE_CURRENT_ITERATION = 0   -- int32_t
local WHILE_AUXILIARY_DATA    = 8   -- void*

local EXC_LOGGING_DATA     = 0    -- void*
local EXC_AUXILIARY_DATA   = 8    -- void*
local EXC_ORIGINAL_NODE_ID = 20   -- uint16_t
local EXC_EXCEPTION_TYPE   = 36   -- enum (int)
```

These offsets change between 32-bit and 64-bit platforms. For production, generate them from a C program using `offsetof()`.

## Registration Flow

```c
// In main.c:

// 1. Create Lua state and load bridge
lua_State *L = luaL_newstate();
luaL_openlibs(L);
cfl_lua_bridge_init(L);

// 2. Load user function definitions
luaL_dofile(L, "user_functions.lua");

// 3. Load binary image and register built-in C functions
cfl_image_loader_t img;
cfl_embedded_load(image_data, image_size, &img);
cfl_register_all_functions(&img);

// 4. Register Lua functions (pops function from stack)
lua_getglobal(L, "activate_valve_one_shot");
cfl_lua_bridge_register_one_shot(&img, "activate_valve_one_shot", L);

// 5. Validate all functions registered
cfl_image_validate(&img);

// 6. Create runtime and attach Lua state
cfl_runtime_handle_t *handle = cfl_runtime_create(&perm, params, test_handle);
cfl_lua_bridge_attach(handle, L);  // stores L in handle->user_handle

// 7. Run
cfl_runtime_run(handle);
```

## Mixed C and Lua Functions

Functions that require direct C struct access (Avro packets, streaming pipelines, drone control) stay in C. Everything else can be Lua. The same binary image works with both — registration just points to different function pointers.

```c
// Register avro functions from C
cfl_image_register_one_shot(&img, "generate_avro_packet_one_shot", generate_avro_packet_one_shot);

// Register application logic from Lua
lua_getglobal(L, "activate_valve_one_shot");
cfl_lua_bridge_register_one_shot(&img, "activate_valve_one_shot", L);
```

## Building

The Lua bridge test links against the CFL runtime libraries and Lua 5.4:

```makefile
CORE_LIB = ../../runtime_binary/libcfl_binarycore.a
FUNC_LIB = ../../runtime_functions/libcfl_core_functions.a
SE_LIB   = ../../s_expression/lib/libs_s_engine.a

$(TARGET): $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB)
    $(CC) -o $@ $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB) -llua5.4 -lm
```

## Test Results

26/26 tests pass with the Lua 5.3 bridge (Avro/streaming/drone functions in C, all others in Lua).
