# S_Engine Return Code Tests

## Overview

This test suite validates the S_Engine's return code system by defining minimal trees that return each possible result code. The tests verify that return codes propagate correctly from DSL source through compilation to runtime execution.

## Test Structure

### DSL Source (Lua)
```lua
local M = require("s_expr_dsl")
local mod = start_module("return_tests")
use_32bit()
set_debug(true)

-- Application result codes
start_tree("return_continue_test")
    se_return_continue()
end_tree("return_continue_test")

start_tree("return_terminate_test")
    se_return_terminate()
end_tree("return_terminate_test")

-- ... additional trees for each return code ...

local result = end_module(mod)
```

Each tree contains a single node that immediately returns a specific result code. This isolates the return code mechanism from any composite node logic.

### Generated Outputs

The DSL compiler produces:

| File | Purpose |
|------|---------|
| `return_tests_32.bin` | Binary module for file-based loading |
| `return_tests_bin_32.h` | C header with embedded binary (ROM loading) |
| `return_tests.h` | Tree hash definitions for lookup |
| `return_tests_user_functions.h` | User function stubs (if any) |

### Tree Hash Definitions
```c
// From return_tests.h
#define RETURN_CONTINUE_TEST_HASH           0x...
#define RETURN_TERMINATE_TEST_HASH          0x...
#define RETURN_RESET_TEST_HASH              0x...
#define RETURN_HALT_TEST_HASH               0x...
#define RETURN_SKIP_CONTINUE_TEST_HASH      0x...
#define RETURN_FUNCTION_HALT_TEST_HASH      0x...
#define RETURN_FUNCTION_RESET_TEST_HASH     0x...
#define RETURN_FUNCTION_TERMINATE_TEST_HASH 0x...
#define RETURN_PIPELINE_TERMINATE_TEST_HASH 0x...
#define RETURN_PIPELINE_RESET_CONTINUE_TEST_HASH 0x...
#define RETURN_PIPELINE_RESET_HALT_TEST_HASH     0x...
```

Trees are identified by FNV-1a hashes of their names, enabling O(1) lookup without string comparison.

---

## Runtime Interface

### Engine Initialization

The S_Engine supports two loading modes:

**ROM Loading (Embedded Binary)**
```c
#include "return_tests_bin_32.h"

uint8_t err = s_engine_init_from_rom(
    &engine,
    return_tests_module_bin_32,      // Embedded binary data
    RETURN_TESTS_MODULE_BIN_32_SIZE, // Size constant
    allocator,
    NULL                             // User context
);
```

Used for embedded systems where the module is compiled into flash.

**File Loading**
```c
uint8_t err = s_engine_init_from_file(
    &engine,
    "return_tests_32.bin",           // File path
    allocator,
    NULL                             // User context
);
```

Used for development or systems with filesystem access.

### Allocator Interface

The S_Engine uses a pluggable allocator for all dynamic memory:
```c
typedef struct {
    void* (*malloc)(void* ctx, size_t size);
    void  (*free)(void* ctx, void* ptr);
    void* ctx;                        // User context passed to malloc/free
    double (*get_time)(void* ctx);    // Monotonic time source
} s_expr_allocator_t;
```

This allows integration with custom memory managers, arena allocators, or RTOS heap implementations.
```c
// Example: Simple malloc wrapper
static void* simple_malloc(void* ctx, size_t size) {
    (void)ctx;
    return malloc(size);
}

static void simple_free(void* ctx, void* ptr) {
    (void)ctx;
    free(ptr);
}

s_expr_allocator_t alloc = {
    .malloc   = simple_malloc,
    .free     = simple_free,
    .ctx      = NULL,
    .get_time = linux_get_time
};
```

### Function Registration

After loading, register built-in and user functions:
```c
// Register S_Engine built-in functions (pipeline, if_then_else, etc.)
s_engine_register_builtins(&engine);

// Register application-specific functions (optional)
// register_user_functions(&engine);

// Set debug callback (optional)
s_expr_module_set_debug(&engine.module, debug_callback);
```

### Validation

Before execution, validate that all function references resolve:
```c
uint8_t err = s_engine_validate(&engine);
if (err != S_EXPR_ERR_OK) {
    printf("Validation failed: %s\n", s_expr_error_str(err));
    printf("Missing hash: 0x%08X at index %d\n", 
           engine.module.error_hash, 
           engine.module.error_index);
    return;
}
```

Validation catches:
- Missing function implementations
- Hash collisions
- Corrupted binary data

### Tree Instantiation

Create a tree instance by hash:
```c
s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
    &engine.module,
    RETURN_CONTINUE_TEST_HASH,
    0                              // Flags (reserved)
);

if (!tree) {
    printf("Failed to create tree\n");
    return;
}
```

Each tree instance has its own:
- Node state array (`node_states[]`)
- Pointer array (if any nodes require it)
- Execution context

Multiple instances of the same tree definition can exist simultaneously.

### Tree Execution

Tick the tree with an event:
```c
s_expr_result_t result = s_expr_node_tick(
    tree,
    SE_EVENT_TICK,                 // Event type
    NULL                           // Event data (optional)
);
```

The tick function:
1. Delivers the event to the root node
2. Executes the tree according to its structure
3. Returns the propagated result code

### Event Types
```c
typedef enum {
    SE_EVENT_INIT,      // Initialize tree (run oneshot functions)
    SE_EVENT_TICK,      // Normal execution tick
    SE_EVENT_TERM,      // Termination request
    SE_EVENT_RESET,     // Reset request
} s_expr_event_t;
```

### Tree Cleanup
```c
s_expr_tree_free(tree);
```

Releases node states, pointer slots, and the instance structure.

### Engine Cleanup
```c
s_engine_free(&engine);
```

Releases all module resources, function tables, and allocated memory.

---

## Test Execution Flow
```
┌─────────────────────────────────────────────────────────────────┐
│                        Test Runner                              │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. Initialize allocator                                         │
│    - malloc/free wrappers                                       │
│    - time source                                                │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Load module (ROM or file)                                    │
│    - Parse binary header                                        │
│    - Map tree/function/string tables                            │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Register functions                                           │
│    - Built-in composites (pipeline, sequence, etc.)             │
│    - Built-in primitives (return codes, field access, etc.)     │
│    - User functions (application-specific)                      │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Validate                                                     │
│    - Resolve all function hashes                                │
│    - Check for missing implementations                          │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. For each test tree:                                          │
│    a. Create tree instance by hash                              │
│    b. Tick with SE_EVENT_TICK                                   │
│    c. Verify returned result code                               │
│    d. Free tree instance                                        │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Free engine                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Test Cases

### Application Result Codes

| Test | DSL Function | Expected Result | Scope |
|------|--------------|-----------------|-------|
| `return_continue_test` | `se_return_continue()` | `SE_CONTINUE` (0) | ChainTree |
| `return_terminate_test` | `se_return_terminate()` | `SE_TERMINATE` (2) | ChainTree |
| `return_reset_test` | `se_return_reset()` | `SE_RESET` (3) | ChainTree |
| `return_halt_test` | `se_return_halt()` | `SE_HALT` (1) | ChainTree |
| `return_skip_continue_test` | `se_return_skip_continue()` | `SE_SKIP_CONTINUE` (5) | ChainTree |

These codes pass through to the ChainTree walker unchanged.

### Function Result Codes

| Test | DSL Function | Expected Result | Scope |
|------|--------------|-----------------|-------|
| `return_function_halt_test` | `se_return_function_halt()` | `SE_FUNCTION_HALT` (6) | S-expression function |
| `return_function_reset_test` | `se_return_function_reset()` | `SE_FUNCTION_RESET` (7) | S-expression function |
| `return_function_terminate_test` | `se_return_function_terminate()` | `SE_FUNCTION_TERMINATE` (8) | S-expression function |

These codes are handled at the S_Engine function boundary.

### Pipeline Result Codes

| Test | DSL Function | Expected Result | Scope |
|------|--------------|-----------------|-------|
| `return_pipeline_terminate_test` | `se_return_pipeline_terminate()` | `SE_PIPELINE_TERMINATE` (9) | Composite node |
| `return_pipeline_reset_continue_test` | `se_return_pipeline_reset_continue()` | `SE_PIPELINE_RESET_CONTINUE` (10) | Composite node |
| `return_pipeline_reset_halt_test` | `se_return_pipeline_reset_halt()` | `SE_PIPELINE_RESET_HALT` (11) | Composite node |

These codes are handled internally by composite nodes (pipeline, sequence, state_machine, etc.).

---

## Debug Output

With `set_debug(true)` in the DSL and a debug callback registered, execution traces are available:
```c
static void debug_callback(s_expr_tree_instance_t* inst, const char* msg) {
    (void)inst;
    printf("  [DEBUG] %s\n", msg);
}

s_expr_module_set_debug(&engine.module, debug_callback);
```

Example output:
```
Testing RETURN_CONTINUE...
  [DEBUG] TICK: return_continue_test
  [DEBUG] INVOKE: se_return_continue -> SE_CONTINUE
  result: 0 (expected SE_CONTINUE=0)
```

---

## Result Code Reference
```c
typedef enum {
    // APPLICATION RESULT CODES (pass through to ChainTree)
    SE_CONTINUE           = 0,
    SE_HALT               = 1,
    SE_TERMINATE          = 2,
    SE_RESET              = 3,
    SE_DISABLE            = 4,
    SE_SKIP_CONTINUE      = 5,
    
    // FUNCTION RESULT CODES (handled at function boundary)
    SE_FUNCTION_HALT      = 6,
    SE_FUNCTION_RESET     = 7,
    SE_FUNCTION_TERMINATE = 8,
    
    // PIPELINE RESULT CODES (handled by composite nodes)
    SE_PIPELINE_TERMINATE      = 9,
    SE_PIPELINE_RESET_CONTINUE = 10,
    SE_PIPELINE_RESET_HALT     = 11,
    SE_PIPELINE_DISABLE        = 12,
} s_expr_result_t;
```

---

## Building and Running
```bash
# Compile DSL to binary
lua return_tests.lua

# Build test executable
gcc -o return_tests_runner \
    return_tests_runner.c \
    s_engine_*.c \
    -I. -lm

# Run tests
./return_tests_runner
```

Expected output:
```
╔════════════════════════════════════════════════════════════════╗
║           S-EXPRESSION ENGINE TEST SUITE                       ║
╚════════════════════════════════════════════════════════════════╝

Loading module from ROM...

=== Initializing Engine ===
✅ Module loaded successfully
   Trees:    11
   Records:  0
   Strings:  0
   Oneshot:  0
   Main:     11
   Pred:     0

=== Registering Functions ===
✅ Built-in functions registered
✅ User functions registered
✅ Debug callback set

=== Validating Function Resolution ===
✅ All functions resolved successfully

=== Return Value Tests ===

Testing RETURN_CONTINUE...
  result: 0 (expected SE_CONTINUE=0)
Testing RETURN_TERMINATE...
  result: 2 (expected SE_TERMINATE=2)
Testing RETURN_RESET...
  result: 3 (expected SE_RESET=3)
...

=== All Return Value Tests Complete ===
```