# Return Tests Module

This module tests all S-Expression engine return codes by providing minimal trees that return each specific result code.

## Overview

The `return_tests` module contains 11 single-node trees, each returning a different `s_expr_result_t` value. This validates that the engine correctly propagates return codes from tree execution.

## Module Structure

```
return_tests
└── Trees (11)
    ├── return_continue_test
    ├── return_terminate_test
    ├── return_reset_test
    ├── return_halt_test
    ├── return_skip_continue_test
    ├── return_function_halt_test
    ├── return_function_reset_test
    ├── return_function_terminate_test
    ├── return_pipeline_terminate_test
    ├── return_pipeline_reset_continue_test
    └── return_pipeline_reset_halt_test
```

## Return Codes

### Application Result Codes

| Code | Value | DSL Function | Description |
|------|-------|--------------|-------------|
| `SE_CONTINUE` | 0 | `se_return_continue()` | Continue execution, process next tick |
| `SE_HALT` | 1 | `se_return_halt()` | Halt execution, resume on next tick |
| `SE_TERMINATE` | 2 | `se_return_terminate()` | Tree completed successfully |
| `SE_RESET` | 3 | `se_return_reset()` | Reset tree state |
| `SE_DISABLE` | 4 | `se_return_disable()` | Disable node |
| `SE_SKIP_CONTINUE` | 5 | `se_return_skip_continue()` | Skip remaining siblings, continue parent |

### Function Result Codes

| Code | Value | DSL Function | Description |
|------|-------|--------------|-------------|
| `SE_FUNCTION_HALT` | 6 | `se_return_function_halt()` | Function wants to halt |
| `SE_FUNCTION_RESET` | 7 | `se_return_function_reset()` | Function wants reset |
| `SE_FUNCTION_TERMINATE` | 8 | `se_return_function_terminate()` | Function wants termination |

### Pipeline Result Codes

| Code | Value | DSL Function | Description |
|------|-------|--------------|-------------|
| `SE_PIPELINE_TERMINATE` | 9 | `se_return_pipeline_terminate()` | Terminate pipeline |
| `SE_PIPELINE_RESET_CONTINUE` | 10 | `se_return_pipeline_reset_continue()` | Reset pipeline, continue |
| `SE_PIPELINE_RESET_HALT` | 11 | `se_return_pipeline_reset_halt()` | Reset pipeline, halt |

## DSL Implementation

Each tree is minimal - just a return statement:

```lua
local mod = start_module("return_tests")
use_32bit()
set_debug(true)

start_tree("return_continue_test")
    se_return_continue()
end_tree("return_continue_test")

start_tree("return_terminate_test")
    se_return_terminate()
end_tree("return_terminate_test")

-- ... etc for each return code

return end_module(mod)
```

## Test Harness (main.c)

### Structure

```
main()
├── Load from ROM → validate
├── Load from file → validate
└── test_return_tests()
    └── run_return_value_tests()
        ├── test_return_continue()
        ├── test_return_terminate()
        ├── test_return_reset()
        ├── test_return_halt()
        ├── test_return_skip_continue()
        ├── test_return_function_halt()
        ├── test_return_function_reset()
        ├── test_return_function_terminate()
        ├── test_return_pipeline_terminate()
        ├── test_return_pipeline_reset_continue()
        └── test_return_pipeline_reset_halt()
```

### Test Pattern

Each test function follows the same pattern:

```c
static void test_return_continue(s_engine_handle_t* engine) {
    printf("Testing RETURN_CONTINUE...\n");
    
    // Create tree by hash
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        RETURN_CONTINUE_TEST_HASH,
        0
    );
    
    if (!tree) {
        printf("  ❌ FAILED: Could not create tree\n");
        exit(1);
    }
    
    // Execute single tick
    s_expr_result_t result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    
    // Report result
    printf("  result: %d (expected SE_CONTINUE=%d)\n", result, SE_CONTINUE);
    
    s_expr_tree_free(tree);
}
```

### Dependencies

| File | Purpose |
|------|---------|
| `return_tests.h` | Tree hash definitions |
| `return_tests_bin_32.h` | Binary module (ROM) |
| `return_tests_user_functions.h` | User function declarations (empty for this module) |

### Loading Methods

**From ROM**:
```c
load_from_rom(&engine, &alloc, return_tests_module_bin_32, RETURN_TESTS_MODULE_BIN_32_SIZE);
```

**From File**:
```c
load_from_file(&engine, &alloc, "return_tests_32.bin");
```

## Generated Files

| File | Description |
|------|-------------|
| `return_tests.h` | Tree hash constants |
| `return_tests_bin_32.h` | 32-bit binary as C array |
| `return_tests_bin_64.h` | 64-bit binary as C array |
| `return_tests_32.bin` | 32-bit binary file |
| `return_tests_64.bin` | 64-bit binary file |
| `return_tests_user_functions.h` | User function prototypes (none for this module) |

## Expected Output

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
   Main:     0
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
Testing RETURN_HALT...
  result: 1 (expected SE_HALT=1)
Testing RETURN_SKIP_CONTINUE...
  result: 5 (expected SE_SKIP_CONTINUE=5)
Testing RETURN_FUNCTION_HALT...
  result: 6 (expected SE_FUNCTION_HALT=6)
Testing RETURN_FUNCTION_RESET...
  result: 7 (expected SE_FUNCTION_RESET=7)
Testing RETURN_FUNCTION_TERMINATE...
  result: 8 (expected SE_FUNCTION_TERMINATE=8)
Testing RETURN_PIPELINE_TERMINATE...
  result: 9 (expected SE_PIPELINE_TERMINATE=9)
Testing RETURN_PIPELINE_RESET_CONTINUE...
  result: 10 (expected SE_PIPELINE_RESET_CONTINUE=10)
Testing RETURN_PIPELINE_RESET_HALT...
  result: 11 (expected SE_PIPELINE_RESET_HALT=11)

=== All Return Value Tests Complete ===
```

## Use Cases

### When to Use Each Return Code

| Code | Use Case |
|------|----------|
| `SE_CONTINUE` | Normal completion, ready for more work |
| `SE_HALT` | In progress, call again next tick |
| `SE_TERMINATE` | Tree finished, no more ticks needed |
| `SE_RESET` | Request tree reset |
| `SE_DISABLE` | Disable this node |
| `SE_SKIP_CONTINUE` | Skip siblings in sequence |
| `SE_FUNCTION_HALT` | Function-level halt signal |
| `SE_FUNCTION_RESET` | Function-level reset signal |
| `SE_FUNCTION_TERMINATE` | Function-level terminate signal |
| `SE_PIPELINE_TERMINATE` | Terminate entire pipeline |
| `SE_PIPELINE_RESET_CONTINUE` | Reset pipeline, keep running |
| `SE_PIPELINE_RESET_HALT` | Reset pipeline, stop |

### Typical Behavior Tree Usage

```lua
-- Sequence that runs until child halts
se_sequence(function()
    se_action_1()           -- returns CONTINUE
    se_action_2()           -- returns HALT (in progress)
    -- sequence stops here, returns HALT
    se_action_3()           -- not reached this tick
end)

-- On next tick, sequence resumes at action_2
```

## Building

```bash
# Compile DSL
lua s_expr_dsl.lua return_tests.lua

# Compile test
gcc -o return_tests main.c \
    -I./include \
    -L./lib -ls_engine
```

## Running

```bash
./return_tests
```

## No User Functions Required

This module uses only built-in return functions - no custom user functions needed. The `return_tests_user_functions.h` header exists but declares no functions.