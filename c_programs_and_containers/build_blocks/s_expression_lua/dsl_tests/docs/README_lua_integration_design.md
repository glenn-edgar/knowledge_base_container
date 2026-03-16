# S-Expression Engine with Lua 5.3 Bridge

## Deployment Notes

This implementation uses a stock Lua 5.3 source distribution compiled directly into the runtime library. No modifications were made to the Lua source. In a production deployment, several optimizations apply:

**Precompiled Lua scripts.** The `.lua` function registration scripts would be compiled to Lua bytecode via `luac` and loaded as `.luac` files. This eliminates the Lua parser and compiler from the runtime, significantly reducing the Lua library footprint. On a 500KB ARM target, removing the parser/compiler saves approximately 50–60KB of flash — meaningful when every kilobyte counts.

**Reduced Lua runtime.** If all Lua scripts are precompiled, the `luac` compiler, parser, lexer, and code generator modules can be stripped from the Lua 5.3 build entirely. The runtime only needs the VM core, standard libraries actually referenced by user functions, and the auxiliary library for the C API. Libraries like `io`, `os`, and `debug` can be omitted if not needed by user scripts.

**Bump allocator compatibility.** The C engine performs zero heap allocation during tree execution. All binary module data lives in flash ROM and is accessed via zero-copy pointer casting. The only heap allocations occur at startup (tree instance creation, node state arrays, blackboard) and within the Lua VM itself. Since the engine's own allocations are create-once/free-once, a bump allocator (arena allocator) is a natural fit for the engine side. The Lua VM requires a general-purpose allocator due to garbage collection, but Lua 5.3 supports a custom allocator function passed to `lua_newstate()`, allowing the Lua heap to be isolated to a dedicated memory region or pool.

**User Lua function discipline.** The bridge itself allocates only a params table per trampoline call, which is collected when the function returns. However, user-written Lua functions may allocate freely — creating tables, strings, closures. On RAM-constrained targets, Lua function authors should minimize allocations, prefer pre-allocated tables, and be aware that each trampoline call creates a temporary params table proportional to the node's parameter count. For hard real-time paths, keep functions in C; use Lua for configuration, diagnostics, and application logic where occasional GC pauses are acceptable.

---

## Design Overview

The S-Expression Engine is a deterministic control runtime targeting resource-constrained ARM systems in the 500KB–8MB RAM range. It unifies behavior trees, state machines, and sequential control flows into a single tree-structured execution model where stateful nodes are driven by a flat parameter stream compiled from a LuaJIT DSL.

The core engine runs entirely in C with zero heap allocation during execution. All tree structures, parameter streams, string tables, and dictionary data are compiled into constant binary images that can be placed directly in flash ROM and executed via zero-copy pointer casting.

Lua 5.3 extends this system by providing a scripting layer for functions that don't require hard real-time response. The engine remains the scheduler — it walks the tree, dispatches functions by hash, and manages node state. When a dispatched function happens to be implemented in Lua rather than C, a trampoline bridges into the Lua VM and back. From the engine's perspective, there is no difference between a C function and a Lua function. Both are void/result/bool function pointers in the same hash-indexed table.

### Why This Architecture

Industrial and embedded control systems need three things simultaneously: deterministic timing for the control loop, scriptability for application logic that changes between deployments, and minimal RAM footprint on 500KB-class ARM MCUs.

Pure C gives you the first and third but not the second. Pure Lua gives you the second but not the first or third. This architecture gives you all three by keeping the scheduler and data structures in C while allowing individual node functions to be implemented in Lua when scripting flexibility matters more than microsecond-level timing.

The key insight is that Lua tables — configuration data, JSON structures, dictionary lookups — are the primary source of heap allocation in embedded Lua applications. By compiling Lua tables into the binary parameter stream at build time, the runtime can navigate arbitrarily nested dictionary structures without allocating a single byte of heap RAM. The Lua VM is only invoked for function execution, never for data storage.

### System Layers

The system has four layers:

**Layer 1: DSL (LuaJIT, build time)** — A LuaJIT-based domain-specific language where module authors define records, trees, events, constants, and function calls. Lua tables written in the DSL are compiled into the binary parameter stream as dictionary structures (both string-keyed and hash-keyed variants). The DSL also declares which functions are implemented in Lua versus C.

**Layer 2: Compiler (LuaJIT, build time)** — The `s_compile.lua` compiler processes DSL files and generates C headers, binary module files, function registration code, and Lua trampoline wiring. A single DSL file produces all artifacts needed for both the C build and the Lua runtime.

**Layer 3: C Runtime Engine (C, execution time)** — The engine loads binary modules (from ROM or file), creates tree instances, and runs the tick loop. It walks the parameter stream, dispatches oneshot/main/predicate functions by hash, manages node state, handles events, and processes result codes. The engine sets `inst->current_func_hash` before every dispatch so trampolines know which Lua function to invoke.

**Layer 4: Lua 5.3 Bridge (C + Lua, execution time)** — A thin C bridge layer that maintains a registry mapping function hashes to Lua closures. Trampoline functions registered in the C function tables intercept dispatch, look up the matching Lua function, push structured arguments, call into the Lua VM, and map the return value back to the C type system. An inst metatable exposes blackboard access, string table lookup, and dictionary navigation as Lua methods.

---

## DSL Changes for Lua Support

### Function Declaration

Three new DSL functions declare that a function will be implemented in Lua rather than C:

```lua
lua_oneshot("MY_LUA_FUNCTION")
lua_main("MY_LUA_HANDLER")
lua_pred("MY_LUA_CHECK")
```

Or as a batch declaration:

```lua
LUA_FUNCTIONS {
    oneshot = { "MY_LUA_INIT", "MY_LUA_LOG" },
    main    = { "MY_LUA_HANDLER" },
    pred    = { "MY_LUA_CHECK" },
}
```

These declarations register the function names in the normal oneshot/main/pred lists (so the binary module includes their hashes) and additionally mark them as Lua-implemented so the code generators know to emit trampolines instead of C prototypes.

### Tree Usage

Inside a tree, Lua functions are called exactly like C functions. There is no syntactic difference:

```lua
start_tree("my_tree")
    se_function_interface(function()
        -- C builtin
        se_log("starting")
        
        -- Lua oneshot — identical call syntax
        local c = o_call("MY_LUA_INIT")
        end_call(c)
        
        -- Lua main — identical call syntax
        local m = m_call("MY_LUA_HANDLER")
            field_ref("sensor_value")
            flt(25.0)
        end_call(m)
        
        se_return_function_terminate()
    end)
end_tree("my_tree")
```

### Generated Outputs

The compiler produces three additional files when Lua functions are declared:

**`<module>_lua_registration.c`** — C source file containing function table entries that point all Lua function hashes to the type-appropriate trampoline (`se_lua_oneshot_trampoline`, `se_lua_main_trampoline`, or `se_lua_pred_trampoline`).

**`<module>_lua_registration.h`** — Header declaring `void <module>_register_lua(s_expr_module_t* module)` so the application can call it.

**`<module>_lua_hashes.h`** — Hash manifest with `#define` entries for each Lua function hash, used as a reference when writing the Lua registration script.

These are generated automatically by `--all` when Lua functions are present, or individually via `--lua-reg=`, `--lua-reg-h=`, and `--lua-hashes=`.

### Filtering

The existing `_user_functions.h` and `_user_registration.c` outputs now exclude Lua-declared functions. A function is either C-native (gets a prototype and direct function pointer) or Lua (gets a trampoline). Never both.

---

## Runtime Changes for Lua Support

### Bridge Architecture

The bridge consists of two files:

**`se_lua53_bridge.h`** — Public API header defining the trampoline signatures, the `se_lua_user_handle_t` struct, and the `se_lua_get_state()` inline that extracts `lua_State*` from the tree instance.

**`se_lua53_bridge.c`** — Implementation containing:

- Parameter marshalling (`push_one_param`, `push_params_table`) that converts the C `s_expr_param_t` array into a Lua table with type-appropriate fields per opcode
- Instance userdata with a metatable providing blackboard access methods
- A Lua-side registry (`se_bridge` global) with a `register()` function
- Three trampoline functions (oneshot, main, pred) that look up Lua functions by hash and dispatch with six arguments: inst userdata, params table, event_type, event_id, event_data (light userdata), and raw C params pointer (light userdata)

### Lua State Wiring

The `lua_State*` pointer is stored in the allocator context field, which propagates through the module to every tree instance:

```c
se_lua_user_handle_t lua_handle = {
    .lua_state = L
};

s_expr_allocator_t alloc = {
    .malloc = simple_malloc,
    .free = simple_free,
    .ctx = &lua_handle,
    .get_time = linux_get_time
};
```

The trampoline extracts it via `se_lua_get_state(inst)` which reads `inst->module->alloc.ctx`.

### Engine Dispatch Change

One field was added to `s_expr_tree_instance_t`:

```c
s_expr_hash_t current_func_hash;
```

The evaluator sets it before every function dispatch in `dispatch_oneshot`, `dispatch_main`, and `dispatch_pred`:

```c
inst->current_func_hash = mod->def->oneshot_hashes[func_idx];
mod->oneshot_fns[func_idx](inst, args, arg_count, event_type, event_id, event_data);
```

For C functions this field is ignored. For trampolines it is the key used to look up the Lua function in the registry.

### Instance Metatable Methods

Lua functions access the C runtime through methods on the `inst` userdata:

| Method | Signature | Description |
|--------|-----------|-------------|
| `read_i32` | `inst:read_i32(offset)` | Read int32 from blackboard |
| `write_i32` | `inst:write_i32(offset, value)` | Write int32 to blackboard |
| `read_u32` | `inst:read_u32(offset)` | Read uint32 from blackboard |
| `write_u32` | `inst:write_u32(offset, value)` | Write uint32 to blackboard |
| `read_f32` | `inst:read_f32(offset)` | Read float from blackboard |
| `write_f32` | `inst:write_f32(offset, value)` | Write float to blackboard |
| `read_f32_ptr` | `inst:read_f32_ptr(ptr)` | Read float via light userdata pointer |
| `read_i32_ptr` | `inst:read_i32_ptr(ptr)` | Read int32 via light userdata pointer |
| `ptr_to_offset` | `inst:ptr_to_offset(ptr)` | Convert light userdata to integer offset |
| `get_string` | `inst:get_string(index)` | Look up string table entry |
| `get_node_count` | `inst:get_node_count()` | Get tree node count |
| `dict_extract_str` | `inst:dict_extract_str(dict_off, path, dest_off, type)` | Generic string-path extraction (type: "int"/"uint"/"float"/"bool"/"hash") |
| `dict_extract_keys` | `inst:dict_extract_keys(dict_off, keys, dest_off, type)` | Generic hash-path extraction (type: "int"/"uint"/"float"/"bool"/"hash") |
| `dict_extract_hash_str` | `inst:dict_extract_hash_str(dict_off, path, dest_off)` | String-path dict hash extraction (hash type only) |
| `dict_extract_hash_keys` | `inst:dict_extract_hash_keys(dict_off, keys, dest_off)` | Hash-path dict hash extraction (hash type only) |
| `dict_store_ptr_str` | `inst:dict_store_ptr_str(dict_off, path, dest_off)` | Resolve string path, store pointer in PTR64 field |
| `dict_store_ptr_keys` | `inst:dict_store_ptr_keys(dict_off, keys, dest_off)` | Resolve hash path, store pointer in PTR64 field |
| `store_raw_param_ptr` | `inst:store_raw_param_ptr(raw_params, index, dest_off)` | Store C param address into PTR64 (for dict loading) |

New methods are added by defining a static `l_inst_*` function in `se_lua53_bridge.c` and adding it to the `inst_methods[]` table. The metatable is built once during `se_lua_bridge_init()`.

### Startup Sequence

```c
// 1. Create Lua state and load scripts
lua_State *L = luaL_newstate();
luaL_openlibs(L);
se_lua_bridge_init(L);            // Creates registry, metatable, se_bridge global
luaL_dofile(L, "my_functions.lua"); // Lua registers functions by hash

// 2. Wire Lua state into allocator
se_lua_user_handle_t lua_handle = { .lua_state = L };
s_expr_allocator_t alloc = { ..., .ctx = &lua_handle };

// 3. Load engine and register functions
s_engine_load_from_rom(&engine, &alloc, ...);
my_module_register_all(&engine.module);  // C-native functions
my_module_register_lua(&engine.module);  // Lua trampolines

// 4. Run
// Engine dispatches by hash — trampolines are transparent
```

---

## Writing Lua Functions

### Registration Pattern

Every Lua function file follows the same structure:

```lua
local bridge = se_bridge

local function fnv1a_32(str)
    local hash = 0x811c9dc5
    local prime = 0x01000193
    for i = 1, #str do
        hash = hash ~ string.byte(str, i)
        hash = (hash * prime) & 0xFFFFFFFF
    end
    return hash
end
```

Then register functions by hash and type:

```lua
bridge.register(fnv1a_32("MY_FUNCTION_NAME"), "oneshot",
    function(inst, params, event_type, event_id, event_data, raw_params)
        -- function body
        -- raw_params is light userdata; most functions ignore it
    end
)
```

The string passed to `fnv1a_32()` must exactly match the name used in the DSL declaration and tree calls. Case matters.

### Function Signatures

All three function types receive the same six arguments:

| Argument | Type | Description |
|----------|------|-------------|
| `inst` | userdata | Tree instance with metatable methods |
| `params` | table | Array of param tables (1-indexed) |
| `event_type` | integer | Event type (TICK, INIT, TERMINATE, user) |
| `event_id` | integer | Event ID |
| `event_data` | light userdata | Opaque pointer from C (pass back to inst methods) |
| `raw_params` | light userdata | Raw C param array pointer (for store_raw_param_ptr) |

The sixth argument (`raw_params`) is a light userdata pointing to the original C `s_expr_param_t` array. Most functions ignore it. It is only needed by dictionary loading functions that must store a pointer into the compiled binary param stream via `inst:store_raw_param_ptr()`.

Return values differ by type:

- **Oneshot**: No return value (void)
- **Main**: Return an integer result code (`SE_PIPELINE_CONTINUE`, `SE_HALT`, etc.)
- **Predicate**: Return a boolean

### Param Table Structure

Each entry in the `params` table has a `type` byte, an `opcode` (type masked to 5 bits), and type-specific fields:

```lua
-- Integer param
params[1].opcode    -- 0x00 (INT)
params[1].int_val   -- the integer value

-- Field reference
params[2].opcode       -- 0x0B (FIELD)
params[2].field_offset -- byte offset into blackboard
params[2].field_size   -- field size in bytes

-- String hash
params[3].opcode    -- 0x03 (STR_HASH)
params[3].str_hash  -- pre-computed FNV-1a hash

-- String table index
params[4].opcode    -- 0x0D (STR_IDX)
params[4].str_index -- index into string table
params[4].str_len   -- string length
```

### Handling event_data

The `event_data` argument arrives as light userdata (an opaque `void*`). Lua cannot dereference it directly. Use inst methods to interpret it:

```lua
-- Read a float from blackboard at the offset encoded in event_data
local value = inst:read_f32_ptr(event_data)

-- Get the raw offset as an integer
local offset = inst:ptr_to_offset(event_data)

-- Read an int32 from that offset
local ival = inst:read_i32_ptr(event_data)
```

### Blackboard Access

Read and write blackboard fields using byte offsets from the param's `field_offset`:

```lua
bridge.register(fnv1a_32("MY_HANDLER"), "main",
    function(inst, params, event_type, event_id, event_data, raw_params)
        -- params[1] is a FIELD ref to "sensor_value"
        local offset = params[1].field_offset
        local current = inst:read_f32(offset)
        
        if current > 100.0 then
            inst:write_f32(offset, 100.0)  -- clamp
        end
        
        return 12  -- SE_PIPELINE_CONTINUE
    end
)
```

### Dictionary Navigation

Lua functions can navigate compiled dictionary structures without heap allocation. The bridge provides generic typed extraction and pointer storage methods that wrap the C dictionary APIs:

```lua
-- Generic string-path extraction (type = "int"|"uint"|"float"|"bool"|"hash")
inst:dict_extract_str(dict_field_offset, "path.to.key", dest_field_offset, "float")

-- Generic hash-path extraction
inst:dict_extract_keys(dict_field_offset, {hash1, hash2}, dest_field_offset, "int")

-- Store pointer to resolved sub-dictionary in a PTR64 field
inst:dict_store_ptr_str(dict_field_offset, "path.to.subdict", dest_ptr_offset)
inst:dict_store_ptr_keys(dict_field_offset, {hash1, hash2}, dest_ptr_offset)

-- Store raw param stream pointer (for dictionary loading)
inst:store_raw_param_ptr(raw_params, param_index, dest_ptr_offset)
```

These methods call into the C dictionary navigation code (`se_dict_string.h`, `se_dict_hash.h`) and write results directly to the blackboard. The dictionary binary data stays in flash ROM throughout. The Lua function never sees the binary format — it passes offsets and paths, and C does the traversal.

---

## Test Applications

### Dispatch Test

Demonstrates the basic Lua bridge pattern. A C oneshot function (`display_event_info`) is replaced with a Lua implementation. The DSL declares `LUA_FUNCTIONS { oneshot = { "DISPLAY_EVENT_INFO" } }` and the tree uses `o_call("DISPLAY_EVENT_INFO")` unchanged. The Lua function receives event data as light userdata and uses `inst:read_f32_ptr()` to read blackboard values through the opaque pointer.

Key validation: the tree produces identical output whether the function is implemented in C or Lua.

### Callback Function Test

Demonstrates Lua functions called indirectly through the function dictionary mechanism. A function pointer is stored in a blackboard PTR64 field via `se_load_function`, then executed via `se_exec_function`. The loaded function tree contains `o_call("LUA_CALLBACK_FN")` which dispatches through the trampoline to Lua.

Key validation: Lua functions work correctly when invoked through the indirect call path, not just direct tree dispatch.

### State Machine Test

Demonstrates Lua functions within a state machine. `CFL_DISABLE_CHILDREN` and `CFL_ENABLE_CHILD` are declared as Lua oneshots. The state machine cycles through states 0→1→2→0, with each state transition disabling all children and enabling one via the Lua functions.

Key validation: Lua functions work correctly in the context of state transitions, where node enable/disable state management must remain consistent across ticks.

### JSON Dictionary Test

Demonstrates complete dictionary operation replacement — every `SE_DICT_*` and `SE_LOAD_DICTIONARY` C builtin replaced with a Lua implementation. Fourteen Lua oneshot functions cover the full dictionary API: loading (string-keyed and hash-keyed), typed extraction (int, uint, float, bool, hash) for both string paths and hash paths, and pointer storage for both path variants.

The Lua functions access both dictionary formats compiled into flash:

- **String-keyed dictionaries** (`json()` in DSL) — navigated via `inst:dict_extract_str()` and `inst:dict_store_ptr_str()`, which wrap `se_dicts_resolve()` from the C string dictionary API. Dot-separated paths like `"integers.nested.deep.value"` are resolved at runtime without string allocation.

- **Hash-keyed dictionaries** (`json_hash()` in DSL) — navigated via `inst:dict_extract_keys()` and `inst:dict_store_ptr_keys()`, which wrap `se_dicth_resolve()` from the C hash dictionary API. Pre-computed FNV-1a hash arrays provide O(n) key lookup with zero string comparison.

The test runs five passes: string-path scalar extraction, hash-path scalar extraction, array element access by index, string-path pointer storage with sub-dictionary extraction, and hash-path pointer storage with sub-dictionary extraction. Each pass verifies extracted values against known expected results.

The dictionary loading functions (`LUA_LOAD_DICTIONARY`, `LUA_LOAD_DICTIONARY_HASH`) use the raw C param pointer — the sixth trampoline argument — to store the address of the compiled dictionary structure directly into a blackboard PTR64 field via `inst:store_raw_param_ptr()`. This is the only operation that requires the raw param pointer; all subsequent extraction and pointer storage operations work through the PTR64 field.

Key validation: all 36 extraction results match the C builtin outputs exactly. Both string-keyed and hash-keyed dictionary formats are fully accessible from Lua. Dictionary data compiled from Lua tables at build time remains in flash ROM with zero heap allocation during navigation. The Lua functions never construct tables to hold dictionary content — they parse the param stream, call C bridge methods, and C writes results directly to the blackboard.

---

## Memory Model

| Component | Location | Allocation |
|-----------|----------|------------|
| Binary module (params, strings, dicts) | Flash ROM | Zero-copy, compile time |
| Tree instance, node states | Heap | One allocation at tree creation |
| Blackboard | Heap | One allocation at tree creation |
| Lua VM | Heap | Created once at startup |
| Lua function registry | Lua registry | Created once at startup |
| Dictionary navigation | Stack only | Zero heap during traversal |
| Param marshalling to Lua | Lua stack | Per-call, freed on return |

The critical property: dictionary data compiled from Lua tables in the DSL lives in the binary module (flash ROM on embedded targets). When Lua functions need to navigate this data, they call into C methods that traverse the binary format in-place. No Lua tables are created for the dictionary content. The only Lua allocations are the params table created per trampoline call, which is collected when the function returns.