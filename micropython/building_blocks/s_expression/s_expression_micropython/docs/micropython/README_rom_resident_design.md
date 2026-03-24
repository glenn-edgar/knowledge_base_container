# ROM-Resident Module Data Design

## Problem

MicroPython on embedded devices (SPIKE Prime: 256K heap, ~100K available flash for user modules) cannot afford to construct large data structures on the heap at import time. The C engine uses a flat array of 64-bit constants that lives in ROM. The MicroPython port must achieve the same: static module data in flash, zero heap cost.

## Solution: All-Tuple Format

MicroPython's frozen module system stores tuple literals in flash/ROM. The DSL compiler generates `_module_mpy.py` files where **every data structure is a tuple** -- zero dicts anywhere in the module.

### What Goes in ROM (When Frozen)

- Node tuples: `("SE_SEQUENCE", 0xABCD, 2, 5, "m_call", 0, 0, None, (), (...))`
- Param tuples: `("int", 42, 0)`
- Tree tuples: `("my_tree", 0x1234, 47, 0, None, 0xFFFF, (nodes...))`
- String tables: `("FUNC_A", "FUNC_B", "FUNC_C")`
- Function name tables: `("SE_SEQUENCE", "SE_CHAIN_FLOW", ...)`

### What Goes on the Heap (Always)

- `new_module()`: function lookup dicts (`_oneshot_idx`, `_main_idx`, `_pred_idx`, `_tree_by_name`) -- ~4K for a typical module
- `new_instance()`: `node_states` dict (one entry per node), `blackboard` dict -- scales with tree size
- User context (`inst["user_ctx"]`)

### Generator Changes

The Lua generator (`s_compile.lua`) was modified to:

1. **Pre-compute `func_index`** -- position of each function name in its table (oneshot/main/pred), stored at `N_FUNC_INDEX` (position 2) in the node tuple. Eliminates runtime name-to-index lookup.

2. **Pre-compute `node_index`** -- DFS pre-order index, stored at `N_NODE_INDEX` (position 3). Eliminates runtime tree-walking to assign indices.

3. **Output trees as tuple of tuples** -- not a dict. Tree lookup uses `tree_order` tuple as index.

4. **Output records, string_index, events as tuples** -- not dicts.

### Before vs After

**Before (dict-based, heap-allocated):**
```python
trees = {}
trees["my_tree"] = {
    "name": "my_tree",
    "name_hash": 0x1234,
    "node_count": 47,
    "nodes": ((...),),
}
```

**After (all-tuple, ROM-resident):**
```python
trees = (
    (
        "my_tree",       # T_NAME
        0x1234,          # T_NAME_HASH
        47,              # T_NODE_COUNT
        0,               # T_POINTER_COUNT
        None,            # T_RECORD_NAME
        0xFFFF,          # T_DEFAULTS_INDEX
        (                # T_NODES
            (...),       # root node tuple
        ),
    ),
)
```

### Runtime Impact

The runtime (`se_runtime.py`) indexes tuples directly:

```python
# Old: node["func_name"] -- dict lookup, node was heap dict
# New: node[N_FUNC_NAME] -- tuple index, node is ROM tuple

# Old: tree["node_count"] -- dict lookup, tree was heap dict
# New: tree[T_NODE_COUNT] -- tuple index, tree is ROM tuple
```

No `_wrap_node()` function. No heap copies of tree data. The only heap allocation is for mutable per-instance state (node_states, blackboard).

## Measured Results (basic_primitive_test, 47 nodes)

| Phase | Heap (unfrozen) | Heap (frozen) |
|-------|----------------|---------------|
| Module data import | 15,488 bytes | 0 |
| Runtime import | 13,920 bytes | 0 |
| `new_module()` | 3,008 bytes | 3,008 bytes |
| `new_instance()` | 5,600 bytes | 5,600 bytes |
| `tick_once()` | 0 bytes | 0 bytes |
| **Total** | **38K** | **~9K** |
