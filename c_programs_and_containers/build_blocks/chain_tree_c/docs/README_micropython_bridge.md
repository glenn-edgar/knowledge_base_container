# MicroPython Bridge for ChainTree CFL

## Overview

The MicroPython bridge compiles the CFL engine directly into a custom MicroPython firmware. User functions are written in Python. The same C engine, same binary images, same built-in functions — but application logic is Python instead of C or Lua.

This targets 256KB+ embedded systems (ESP32, STM32, RISC-V) where MicroPython is the application runtime and CFL provides deterministic real-time control flow underneath.

## Architecture

The MicroPython bridge supports two modes:

1. **Pure Python** — all user functions in Python (simple tests, prototyping)
2. **Hybrid C/Python** — avro/streaming/drone functions in C, application logic in Python (production, matching the Lua 5.3 pattern)

```
┌──────────────────────────────────────────────────────┐
│                 MicroPython Runtime                  │
│  ┌────────────────────────────────────────────────┐  │
│  │           test_first.py (Python)               │  │
│  │  import cfl                                    │  │
│  │  cfl.load_embedded_image(data)                 │  │
│  │  cfl.register_c_user_functions()  # C funcs    │  │
│  │  cfl.register_one_shot("name", py_func)        │  │
│  │  handle = cfl.create_runtime()                 │  │
│  │  cfl.run(handle)                               │  │
│  └───────────────────┬────────────────────────────┘  │
│                      │                               │
│  ┌───────────────────▼────────────────────────────┐  │
│  │         cfl_mp_bridge.c (C module)             │  │
│  │  Trampoline dispatch (32 per type)             │  │
│  │  "cfl" module: json, bb, arena, heap, memory   │  │
│  │  Lifecycle: load, register, validate, run      │  │
│  └───────┬───────────────────────────┬────────────┘  │
│          │                           │               │
│  ┌───────▼──────────┐  ┌────────────▼─────────────┐  │
│  │  C User Funcs    │  │  CFL C Runtime Engine    │  │
│  │  avro, streaming │  │  runtime_binary +        │  │
│  │  drone control   │  │  runtime_functions +     │  │
│  │  (compiled in)   │  │  s_expression            │  │
│  └──────────────────┘  └──────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

## Building the Custom MicroPython

### Prerequisites

```bash
# Clone MicroPython source
git clone --depth 1 https://github.com/micropython/micropython.git micropython_source
cd micropython_source
make -C mpy-cross              # Build the cross-compiler
cd ports/unix && make submodules  # Initialize submodules
```

### Build for Development (Unix Port)

```bash
cd micropython_source/ports/unix
make CFLAGS_EXTRA="-Wno-error=double-promotion -Wno-error=float-conversion" \
     USER_C_MODULES=/path/to/chain_tree_c/bridges
```

This produces `build-standard/micropython` — a MicroPython binary with the `cfl` module built in.

### Build for ESP32

```bash
cd micropython_source/ports/esp32
make CFLAGS_EXTRA="-Wno-error=double-promotion -Wno-error=float-conversion" \
     USER_C_MODULES=/path/to/chain_tree_c/bridges \
     BOARD=XIAO_ESP32S3
```

### Build for STM32

```bash
cd micropython_source/ports/stm32
make CFLAGS_EXTRA="-Wno-error=double-promotion -Wno-error=float-conversion" \
     USER_C_MODULES=/path/to/chain_tree_c/bridges \
     BOARD=your_board
```

### What Gets Compiled In

The `micropython.mk` file tells MicroPython's build system what to include:

```makefile
# Bridge source (scanned for MP_QSTR_ and MP_REGISTER_MODULE)
SRC_USERMOD_C += $(CFL_MOD_DIR)/cfl_mp_bridge.c

# CFL runtime (library code, not scanned for MP macros)
SRC_USERMOD_LIB_C += $(wildcard $(CFL_ROOT)/runtime_binary/src/*.c)
SRC_USERMOD_LIB_C += $(wildcard $(CFL_ROOT)/runtime_functions/src/*.c)
SRC_USERMOD_LIB_C += $(wildcard $(CFL_ROOT)/s_expression/runtime/*.c)

# C user functions (avro/streaming/drone — hybrid mode)
SRC_USERMOD_C += $(CFL_MPY_TEST)/user_avro_test_file.c
SRC_USERMOD_C += $(CFL_MPY_TEST)/user_streaming_boolean.c
SRC_USERMOD_C += $(CFL_MPY_TEST)/user_node_control_boolean_fns.c
SRC_USERMOD_C += $(CFL_MPY_TEST)/cfl_c_user_functions.c
```

The C user function files have `#pragma GCC diagnostic ignored` for `-Wdouble-promotion` and `-Wfloat-conversion` to satisfy MicroPython's `-Werror` build.

## The "cfl" Python Module

After building, `import cfl` gives you the full CFL API from Python.

### Lifecycle Functions

```python
import cfl

# Load a binary image from bytes (e.g., read from file or flash)
with open("chaintree_handle.ctb", "rb") as f:
    data = f.read()
cfl.load_embedded_image(data)     # Returns True/False

# Register C user functions (avro/streaming/drone — compiled into binary)
cfl.register_c_user_functions()   # Weak symbol — no-op if not linked

# Register Python user functions
cfl.register_one_shot("name", my_function)
cfl.register_main("name", my_function)
cfl.register_boolean("name", my_function)

# Validate all functions are registered
missing = cfl.validate()          # Returns count of missing functions

# Create and run
handle = cfl.create_runtime()     # Returns handle (int) or None
cfl.add_test(handle, 3)           # Activate test by index
result = cfl.run(handle)          # Returns True/False

# Cleanup
cfl.cleanup()

# Query
cfl.get_node_count()              # Total nodes in image
cfl.get_kb_count()                # Number of knowledge bases
```

### JSON Node Data

```python
s = cfl.json_extract_string(handle, node_index, "node_dict.state")
i = cfl.json_extract_int32(handle, node_index, "node_dict.timeout")
f = cfl.json_extract_float(handle, node_index, "node_dict.threshold")
b = cfl.json_extract_bool(handle, node_index, "node_dict.enabled")
cfl.json_print_node(handle, node_index)  # Debug print to stdout
```

### Blackboard

```python
cfl.bb_set_int32(handle, "mode", 42)
mode = cfl.bb_get_int32(handle, "mode")

cfl.bb_set_float(handle, "temperature", 98.6)
temp = cfl.bb_get_float(handle, "temperature")

cfl.bb_set_uint32(handle, "error_count", 7)
count = cfl.bb_get_uint32(handle, "error_count")
```

### Arena and Heap Memory

```python
ptr = cfl.arena_get(handle, node_index)             # Get node's arena data
ptr = cfl.smart_alloc(handle, node_index, 16)       # Allocate arena memory
ptr = cfl.additional_alloc(handle, node_index, 8)   # Additional arena alloc
ptr = cfl.heap_alloc(handle, 4)                      # Allocate heap memory
cfl.heap_free(handle, ptr)                            # Free heap memory
```

### Raw Memory Access

Pointers are represented as Python integers. Read/write C struct fields by offset:

```python
val = cfl.read_u8(ptr, 0)        # Read uint8_t at ptr+0
val = cfl.read_bool(ptr, 0)      # Read bool (uint8_t != 0) at ptr+0
val = cfl.read_i32(ptr, 0)       # Read int32_t at ptr+0
cfl.write_i32(ptr, 0, 42)        # Write int32_t at ptr+0
val = cfl.read_u16(ptr, 20)      # Read uint16_t at ptr+20
child = cfl.read_ptr(ptr, 8)     # Read void* at ptr+8
cfl.write_ptr(ptr, 8, child)     # Write void* at ptr+8
```

### Runtime Queries

```python
idx = cfl.get_event_index(handle, "MY_EVENT")  # -1 if not found
parent = cfl.get_node_parent(handle, node_index)
node_id = cfl.event_data_to_u16(event_data)    # Extract from event pointer
```

### Constants

```python
# Return codes
cfl.CONTINUE        # 0
cfl.HALT             # 1
cfl.TERMINATE        # 2
cfl.RESET            # 3
cfl.DISABLE          # 4
cfl.SKIP_CONTINUE    # 5

# Events
cfl.INIT_EVENT              # 0
cfl.TERMINATE_EVENT         # 1
cfl.START_TESTS             # 2
cfl.RAISE_EXCEPTION_EVENT   # 12
cfl.CHANGE_STATE_EVENT      # 17
cfl.RECOVERY_CHECK_EVENT    # 0xFFFE
cfl.RECOVERY_SEQ_EVAL       # 0
```

## Writing User Functions

### One-Shot (initialization/termination)

```python
def activate_valve_one_shot(handle, node_index):
    state = cfl.json_extract_string(handle, node_index, "node_dict.state")
    if state == "open":
        print("Valve is open")
```

### Main (tick function, returns result code)

```python
def my_main_function(handle, bool_fn_idx, node_index,
                     event_type, event_id, event_data):
    # Process events and return a CFL result code
    return cfl.CONTINUE
```

### Boolean (condition check)

```python
def while_test_boolean(handle, node_index, event_type, event_id, event_data):
    ptr = cfl.arena_get(handle, node_index)
    if event_id == cfl.INIT_EVENT:
        count_ptr = cfl.heap_alloc(handle, 4)
        cfl.write_ptr(ptr, 8, count_ptr)  # WHILE_AUXILIARY_DATA
        count = cfl.json_extract_int32(handle, node_index, "node_dict.user_data.count")
        cfl.write_i32(count_ptr, 0, count)
        return False
    if event_id == cfl.TERMINATE_EVENT:
        aux = cfl.read_ptr(ptr, 8)
        if aux is not None:
            cfl.heap_free(handle, aux)
        return False
    current = cfl.read_i32(ptr, 0)  # WHILE_CURRENT_ITERATION
    aux = cfl.read_ptr(ptr, 8)
    target = cfl.read_i32(aux, 0)
    return current < target
```

## Exception Handling in Trampolines

Python exceptions in user functions are caught by the bridge via MicroPython's NLR (Non-Local Return) mechanism:

```c
nlr_buf_t nlr;
if (nlr_push(&nlr) == 0) {
    mp_obj_t result = mp_call_function_n_kw(func, nargs, 0, args);
    nlr_pop();
    return mp_obj_get_int(result);
} else {
    // Python exception — print and return safe default
    mp_obj_print_exception(&mp_plat_print, MP_OBJ_FROM_PTR(nlr.ret_val));
    return CFL_HALT;
}
```

One-shot exceptions are silently caught (void return). Main function exceptions return `CFL_HALT`. Boolean exceptions return `false`.

## GC Root Safety

The `mp_obj_t` function references in the trampoline tables (`g_main_funcs`, `g_os_funcs`, `g_bool_funcs`) are global C arrays. MicroPython's garbage collector scans the BSS/data segments on the unix port, so these are automatically visible as GC roots.

On constrained embedded ports, if the GC doesn't scan C globals, you would need to register these via `MP_STATE_PORT`. The current implementation works correctly on the unix port and standard embedded ports.

## Differences from Lua 5.3 Bridge

| Aspect | Lua 5.3 | MicroPython |
|--------|---------|-------------|
| Host binary | C `main.c` embeds `lua_State` | Custom MicroPython binary |
| Test runner | C main loads `.lua` file | Python script runs directly |
| C/scripted mix | C main registers both | `cfl.register_c_user_functions()` from Python |
| Function refs | Lua registry references (int) | `mp_obj_t` in global arrays |
| Exception safety | `lua_pcall` | NLR push/pop |
| GC roots | Lua registry (always rooted) | C globals (scanned by GC) |
| Lifecycle API | C-side only | Python-callable (`cfl.load_embedded_image`, etc.) |
| Build system | Standard Makefile + `-llua5.4` | MicroPython `USER_C_MODULES` |

Both bridges support the same hybrid pattern: performance-critical functions (avro packets, streaming pipelines, drone control) stay in C while application logic (valves, sequences, exceptions, blackboard, state machines) is written in the scripting language.

## Platform-Specific Configuration

The `cfl_mp_config.h` header (in `bridges/cfl_micropython/`) provides platform-tiered sizing:

```c
#if defined(__aarch64__)
    #define CFL_MAX_TREE_DEPTH    64
    #define CFL_MAX_NODES         1024
    #define CFL_ARENA_SIZE        (64 * 1024)
#elif defined(STM32F413xx)      // SPIKE Prime
    #define CFL_MAX_TREE_DEPTH    32
    #define CFL_MAX_NODES         256
    #define CFL_ARENA_SIZE        (8 * 1024)
#elif defined(STM32H7xx)        // M7 targets
    #define CFL_MAX_TREE_DEPTH    48
    #define CFL_MAX_NODES         512
    #define CFL_ARENA_SIZE        (32 * 1024)
#endif
```

## Test Results

**26/26 tests pass** with the hybrid C/Python build. Avro/streaming/drone functions run as compiled C; all other user functions run as Python through the bridge.

Test categories verified: columns, forks, state machines, supervisors, exceptions, while loops, bitmask events, watchdogs, blackboard, avro packets, streaming pipelines, and drone control.

## Complete Test Example

```python
import cfl
import sys

def activate_valve(handle, node_index):
    state = cfl.json_extract_string(handle, node_index, "node_dict.state")
    if state == "open":
        print("Valve is open")

def while_test(handle, node_index, event_type, event_id, event_data):
    ptr = cfl.arena_get(handle, node_index)
    if event_id == cfl.INIT_EVENT:
        count_ptr = cfl.heap_alloc(handle, 4)
        cfl.write_ptr(ptr, 8, count_ptr)
        count = cfl.json_extract_int32(handle, node_index, "node_dict.user_data.count")
        cfl.write_i32(count_ptr, 0, count)
        return False
    if event_id == cfl.TERMINATE_EVENT:
        aux = cfl.read_ptr(ptr, 8)
        if aux is not None:
            cfl.heap_free(handle, aux)
        return False
    current = cfl.read_i32(ptr, 0)
    aux = cfl.read_ptr(ptr, 8)
    target = cfl.read_i32(aux, 0)
    return current < target

def main():
    test_index = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    with open("chaintree_handle.ctb", "rb") as f:
        data = f.read()

    cfl.load_embedded_image(data)

    # Register C functions (avro/streaming/drone — compiled into binary)
    cfl.register_c_user_functions()

    # Register Python functions
    cfl.register_one_shot("activate_valve_one_shot", activate_valve)
    cfl.register_boolean("while_test_boolean", while_test)
    # ... register remaining Python functions ...

    if cfl.validate() > 0:
        print("Missing functions")
        return

    handle = cfl.create_runtime()
    cfl.add_test(handle, test_index)
    result = cfl.run(handle)
    print("Result:", result)
    cfl.cleanup()

main()
```

## Hybrid C/Python Registration Pattern

The `cfl_c_user_functions.c` file provides `cfl_register_c_user_functions()`, which is declared as a weak symbol in the bridge. This means:

- If the file is compiled in, `cfl.register_c_user_functions()` registers all C functions
- If the file is not compiled in, the call is a no-op (weak symbol resolves to NULL)

To add your own C functions to the hybrid build:

1. Write the C function with standard CFL signatures
2. Add the `extern` declaration and registration call to `cfl_c_user_functions.c`
3. Add the `.c` file to `micropython.mk` under `SRC_USERMOD_C`
4. Rebuild the custom MicroPython binary

```c
// In cfl_c_user_functions.c:
extern void my_new_one_shot(void *, unsigned);
REG_OS("my_new_one_shot", my_new_one_shot);
```
