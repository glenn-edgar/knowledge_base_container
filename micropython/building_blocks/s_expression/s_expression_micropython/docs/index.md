# S-Expression Engine -- MicroPython Port

This is the MicroPython port of the S-Expression DSL engine. It provides the same behavior-tree execution model as the C engine, but runs as pure MicroPython on resource-constrained devices like the LEGO SPIKE Prime hub.

## Architecture

The system has three layers:

```
DSL source (.lua)
    -> LuaJIT compiler (s_compile.lua)
        -> _module_mpy.py (all-tuple constant data, ROM-resident when frozen)

se_runtime.py + builtins (full runtime, 15 files)
    OR
se_runtime_spike.py (slim single-file runtime for SPIKE Prime)

User functions (application-specific, e.g. spike_drivebase_straight)
```

### Compilation Pipeline

The same LuaJIT DSL compiler used for the C target generates MicroPython module data via the `--micropython` flag:

```bash
cd s_expression_micropython
./s_build.sh dsl_tests/basic_primitive_test/basic_primitive_test.lua dsl_tests/basic_primitive_test/
```

This produces a `_module_mpy.py` file containing the entire tree structure as nested tuples -- zero dicts, zero heap allocation when frozen into firmware.

### ROM-Resident Design

The key design principle: **all static data is tuples**. When a MicroPython module is frozen into firmware (via `manifest.py`), tuple literals reside in flash/ROM and are never copied to the heap.

| Data | Format | Location |
|------|--------|----------|
| Node tuples | `(name, hash, func_idx, node_idx, call_type, ...)` | ROM |
| Param tuples | `(type, value, order)` | ROM |
| Tree tuples | `(name, hash, node_count, ...)` | ROM |
| Function tables | `("NAME1", "NAME2", ...)` | ROM |
| Node states | `{"flags": int, "state": int, "user_data": int}` | Heap |
| Blackboard fields | `{"field_name": value, ...}` | Heap |

### Two Runtime Variants

**Full runtime** (`se_runtime.py` + 11 `se_builtins_*.py` + `se_stack.py`):
All builtins, all features. 101K source, 15 files. For desktop testing and full-featured deployments.

**Slim runtime** (`se_runtime_spike.py`):
Single file, 24K source. Only the builtins needed for SPIKE Prime missions. Add more functions by copying them in as needed.

## Quick Start

```python
import sys
sys.path.insert(0, "mpy_runtime")

import se_runtime
import se_builtins_all
import my_module_mpy as module_data

# Load module with all builtins
mod = se_runtime.new_module(module_data, se_builtins_all.builtins)

# Register application-specific functions
se_runtime.register_fns(mod, my_user_functions)

# Create instance and tick
inst = se_runtime.new_instance(mod, "my_tree")
result = se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)
```

## Memory Budget (SPIKE Prime)

| Component | Flash (frozen) | Heap |
|-----------|---------------|------|
| Slim runtime | ~10K | 0 |
| Mission module (~300 nodes) | ~60K | 0 |
| User functions (~15 fns) | ~5K | 0 |
| **Subtotal: flash** | **~75K of 100K** | |
| Module tables (new_module) | | ~4K |
| Instance state (new_instance) | | ~28K |
| **Subtotal: heap** | | **~32K of 256K** |
| Per tick | | 0 |

## Test Results

10 of 11 DSL tests pass under `micropython`:

| Test | Status | Ticks |
|------|--------|-------|
| return_test (18 trees) | PASS | 1 each |
| loop_test | PASS | 221 |
| state_machine | PASS | 363 |
| dispatch | PASS | 42 |
| advanced_primitive | PASS | 42 |
| complex_sequence | PASS | 438 |
| callback_function | PASS | 1 |
| stack_test | PASS | 841 |
| stack_equations | PASS | 841 |
| json | PASS | 1 |
| function_dictionary | ERROR | setup issue |
