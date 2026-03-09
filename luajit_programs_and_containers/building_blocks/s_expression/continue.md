# ChainTree S-Expression Engine — LuaJIT Runtime: Session Continuation

## What Was Accomplished

### Session 1 (prior): se_runtime.lua
The core execution engine was completed. Pure machinery, no builtins embedded. Implements:
- `new_module(module_data, user_fns)` — DFS-annotates trees, builds dispatch arrays
- `new_instance(mod, tree_name)` — allocates node_states (all ACTIVE), blackboard, event queue, pointer array
- `tick(inst, event_id, event_data)` — main tick + internal event queue drain loop
- Full INIT/TICK/TERMINATE lifecycle for m_call, pt_m_call, o_call, io_call, p_call, p_call_composite
- `merge_fns(...)` — assembles builtin tables for `new_module`

### Session 2 (this session): Builtins split out + full builtin library

**Design change**: All builtins removed from se_runtime.lua. Engine is pure machinery. Builtins are user-supplied tables passed to `new_module`.

**New files produced** (all in outputs/):

| File | Lines | C source |
|---|---|---|
| `se_runtime.lua` | 690 | s_engine_eval.c, node.c, module.c |
| `se_stack.lua` | 142 | s_expr_stack_t |
| `se_builtins_flow_control.lua` | 333 | s_engine_builtins_flow_control.h |
| `se_builtins_dispatch.lua` | 174 | s_engine_builtins_dispatch.h |
| `se_builtins_pred.lua` | 157 | s_engine_builtins_pred.h |
| `se_builtins_oneshot.lua` | 131 | s_engine_builtins_oneshot.h |
| `se_builtins_return_codes.lua` | 49 | s_engine_builtins_return_codes.h |
| `se_builtins_delays.lua` | 243 | s_engine_builtins_delays.h |
| `se_builtins_verify.lua` | 183 | s_engine_builtins_verify.h |
| `se_builtins_stack.lua` | 302 | s_engine_builtins_stack.h |
| `se_builtins_spawn.lua` | 406 | s_engine_builtins_spawn.h |
| `se_builtins_quads.lua` | 328 | s_engine_builtins_quads.h |
| `se_builtins_dict.lua` | 271 | se_load_dictionary.c |

**Total**: ~3,400 lines of translated Lua across 13 files.

---

## Architecture Summary

### Usage pattern

```lua
local se_runtime = require("se_runtime")
local module_data = require("basic_primitive_test_module")

local fns = se_runtime.merge_fns(
    require("se_builtins_flow_control"),
    require("se_builtins_dispatch"),
    require("se_builtins_pred"),
    require("se_builtins_oneshot"),
    require("se_builtins_return_codes"),
    require("se_builtins_delays"),
    require("se_builtins_verify"),
    require("se_builtins_stack"),
    require("se_builtins_spawn"),
    require("se_builtins_quads"),
    require("se_builtins_dict"),
    { my_user_fn = function(inst, node, event_id, event_data) ... end }
)

local mod  = se_runtime.new_module(module_data, fns)
local inst = se_runtime.new_instance(mod, "my_tree")

repeat
    result = se_runtime.tick(inst)
until se_runtime.is_complete(result) or tick >= max_ticks
```

### Key design decisions

**Tree structure preserved**: `module_data` tree nodes are used directly. No flattening to a flat param array. `child_count` → `#node.children`, child access → `node.children[i+1]`. This makes MicroPython/Python ports straightforward.

**Blackboard is a Lua table**: String-keyed `inst.blackboard`. `field_ref` params carry the field name string directly (resolved at compile time by the DSL).

**pt_m_call pointer slots**: `inst.pointer_array[inst.pointer_base]` is a Lua table `{ptr=nil, u64=0, f64=0.0}`. The engine sets `pointer_base` before calling a pt_m_call node. Builtins call `get_u64/set_u64/get_f64/set_f64` to use the slot.

**Per-node extended state**: `node_states[i]` is a Lua table. Builtins can add extra fields (`user_u64`, `user_f64`, `wait_target`, `wait_remain`, `cached_fn`) without breaking anything. `get_user_u64/set_user_u64/get_user_f64/set_user_f64` are the standard accessors.

**Time source**: `inst.mod.get_time()` — set to `os.clock` by default in `new_module`. Override by assigning `mod.get_time = my_clock_fn` after `new_module`.

**Stack**: `se_stack.lua` implements a simple Lua-native call stack. `inst.stack` is nil unless the tree was created with a stack (se_spawn_tree/se_spawn_and_tick_tree pass `stack_size`). `se_builtins_stack` requires it to be present.

**Dictionaries**: In C, dictionaries are ROM binary structures. In Lua they are plain tables stored in the blackboard. Keys are hashes (numbers) or strings. Values are Lua functions with main-function signature. `se_load_dictionary` stores a table reference into a blackboard field.

**Terminators**: `terminate_action_at_index` and `reset_action_at_index` from the C source are already implemented in `se_runtime.lua` as `child_terminate` and `child_reset_recursive` respectively. These are internal engine functions exposed via `M.child_terminate` and `M.child_reset_recursive`.

### Critical correctness notes

**se_wait_event**: stores `ns.wait_target` and `ns.wait_remain` as separate Lua fields on node_states (Lua cannot pack two 32-bit values into a native 64-bit integer without FFI). The C packs them as `(target << 32 | count)` in a uint64.

**se_frame_free**: is an **m_call**, not a oneshot. The C implementation has a reversed early-return: it returns SE_PIPELINE_CONTINUE for anything that is NOT SE_EVENT_INIT. Only SE_EVENT_INIT pops the frame. The TERMINATE branch in the C is unreachable due to the early-return ordering — this is faithfully reproduced.

**se_frame_allocate**: iterates node.children directly (all callables). The C iterates params starting at index 3 to skip the 3 uint non-callable params. In Lua, params and children are already separate arrays, so iteration starts at child index 1 (1-based).

**se_spawn_and_tick_tree**: drains the **parent's** event queue after ticking the child, forwarding each event to the child. This mirrors the second `while` loop in the C implementation.

**se_verify** param layout: `params[1]` = reset_flag, `children[0]` = pred (0-based), `children[1]` = error oneshot (0-based). The C uses logical child indices — in the C flat layout pred is at child 0, reset_flag is a non-callable at position 1, error is at child 2. In Lua tree layout, non-callables go to params[], callables go to children[].

**se_p_quad vs se_quad**: The accumulate opcodes (0x40–0x4D) overlap with `MOVE` (0x40) in the SE_QUAD opcode space. They are only defined and valid in SE_P_QUAD context. When called as SE_QUAD, opcode 0x40 is `MOVE`. When called as SE_P_QUAD, 0x40–0x4D are the accumulate variants. This is safe because SE_QUAD and SE_P_QUAD are separate dispatch slots.

---

## se_runtime.lua API Reference

### Public functions
```lua
M.new_module(module_data, user_fns) -> mod
M.new_instance(mod, tree_name) -> inst
M.tick(inst, event_id?, event_data?) -> result_code
M.is_complete(result) -> bool
M.merge_fns(...tables...) -> merged_table

-- Event queue
M.event_push(inst, tick_type, event_id, event_data)
M.event_pop(inst) -> tick_type, event_id, event_data
M.event_count(inst) -> n
M.event_clear(inst)

-- Extended state accessors (for pt_m_call builtins)
M.get_u64(inst, node) / M.set_u64(inst, node, v)   -- pointer slot u64
M.get_f64(inst, node) / M.set_f64(inst, node, v)   -- pointer slot f64
M.get_user_u64(inst, node) / M.set_user_u64(inst, node, v)  -- per-node
M.get_user_f64(inst, node) / M.set_user_f64(inst, node, v)  -- per-node
M.get_state(inst, node) / M.set_state(inst, node, v)        -- ns.state

-- Child helpers (for builtin use)
M.child_count(node) -> n
M.child_invoke(inst, node, idx_0based, event_id, event_data) -> result
M.child_invoke_pred(inst, node, idx_0based) -> bool
M.child_invoke_oneshot(inst, node, idx_0based)
M.child_terminate(inst, node, idx_0based)
M.child_reset(inst, node, idx_0based)
M.child_reset_recursive(inst, node, idx_0based)
M.children_terminate_all(inst, node)
M.children_reset_all(inst, node)

-- Param accessors
M.param_int(node, i)           -- 1-based
M.param_float(node, i)
M.param_str(node, i)
M.param_field_name(node, i)
M.field_get(inst, node, i)
M.field_set(inst, node, i, value)
M.param_result(node, i)

-- Misc
M.get_ns(inst, node_index) -> node_state
M.invoke_oneshot(inst, node)
M.default_get_time           -- os.clock
```

### inst structure
```lua
inst = {
    mod,                     -- module reference
    tree,                    -- tree definition
    node_states = {},        -- [node_index] = {flags, state, user_data, ...extras}
    node_count,
    pointer_array = {},      -- [ptr_idx] = {ptr=nil, u64=0, f64=0.0, ...extras}
    slot_flags = {},
    pointer_count,
    blackboard = {},         -- string-keyed Lua table
    current_node_index = 0,
    current_event_id = 0,
    current_event_data = nil,
    in_pointer_call = false,
    pointer_base = 0,
    stack = nil,             -- se_stack instance (if created)
    tick_type = 0,
    current_dict = nil,      -- set by se_exec_dict_dispatch on INIT
    event_queue = {},        -- circular buffer size 16
    event_queue_head = 0,
    event_queue_count = 0,
}
```

### mod structure (after new_module)
```lua
mod = {
    module_data,             -- original module_data table
    oneshot_fns = {},        -- [func_index] -> fn(inst, node)
    main_fns    = {},        -- [func_index] -> fn(inst, node, event_id, event_data)
    pred_fns    = {},        -- [func_index] -> fn(inst, node) -> bool
    trees_by_hash = {},      -- [name_hash] -> tree_name  (for spawn)
    get_time = os.clock,     -- injectable; override after new_module()
}
```

---

## What Needs Testing

The full builtin library is written but **nothing has been tested end-to-end yet**. The critical path is:

1. **Compile a test module** with `s_compile.lua` from a known DSL file (e.g., `basic_primitive_test.lua`) to produce `basic_primitive_test_module.lua`
2. **Run the module** through `se_runtime.new_module` + `new_instance` + `tick` loop
3. **Compare results** against C test harness output for the same module

### Likely first issues to encounter

- `new_module` DFS annotation: verify `node_index` assignment matches C pre-order traversal
- `pointer_base` setting for pt_m_call nodes: needs to match `pointer_index` from module_data
- `se_frame_allocate` / `se_frame_free` interaction: frame must be pushed once per tick, not per invocation
- `se_stack.push_frame` base_ptr math: verify params remain accessible after push
- `param_int` on hash params: `{type="str_hash", value={hash=0x..., str="..."}}` — callers expecting an integer may get a table

### Test harness pattern (mirrors C)

```lua
local se_runtime = require("se_runtime")
local module_data = require("basic_primitive_test_module")

local fns = se_runtime.merge_fns(
    require("se_builtins_flow_control"),
    require("se_builtins_pred"),
    require("se_builtins_oneshot"),
    require("se_builtins_return_codes")
    -- add more as needed
)

local mod  = se_runtime.new_module(module_data, fns)
local inst = se_runtime.new_instance(mod, "basic_primitive_test")

local tick_count = 0
local max_ticks  = 1000
local result

repeat
    result = se_runtime.tick(inst)
    tick_count = tick_count + 1
    print(string.format("tick %d: result=%d", tick_count, result))
until se_runtime.is_complete(result) or tick_count >= max_ticks

print(string.format("done: result=%d ticks=%d", result, tick_count))
```

---

## Pending: Remaining C Sources Not Yet Translated

These C builtins exist in the C engine but have not been translated:

- `s_engine_builtins_scan_graph.h` — scan_graph/scan_tree integration (industrial fault logic)
- `s_engine_builtins_alarm.h` — ISA-18.2 alarm management
- Any custom user builtins in the production irrigation controller

These are application-layer builtins, not engine builtins, so they can be added as user-supplied functions without modifying the core files.

---

## File Locations

All files should be placed in the same directory so `require("se_runtime")` etc. resolve without path manipulation.

```
se_runtime.lua                  -- engine (pure machinery)
se_stack.lua                    -- call-stack data structure
se_builtins_flow_control.lua    -- se_sequence, se_fork, se_while, se_if_then_else, etc.
se_builtins_dispatch.lua        -- se_trigger_on_change, se_state_machine, se_field_dispatch
se_builtins_pred.lua            -- se_pred_and/or/not, se_field_eq/gt/lt, etc.
se_builtins_oneshot.lua         -- se_log, se_set_field, se_queue_event, etc.
se_builtins_return_codes.lua    -- se_return_continue ... se_return_pipeline_skip_continue
se_builtins_delays.lua          -- se_tick_delay, se_time_delay, se_wait_event, se_wait, se_wait_timeout
se_builtins_verify.lua          -- se_verify, se_verify_and_check_elapsed_time/events
se_builtins_stack.lua           -- se_frame_allocate, se_frame_free, se_stack_frame_instance
se_builtins_spawn.lua           -- se_spawn_tree, se_tick_tree, se_spawn_and_tick_tree, se_exec_fn/dict
se_builtins_quads.lua           -- se_quad (oneshot), se_p_quad (pred)
se_builtins_dict.lua            -- se_load_dictionary, se_dict_extract_*, se_dict_store_ptr
```