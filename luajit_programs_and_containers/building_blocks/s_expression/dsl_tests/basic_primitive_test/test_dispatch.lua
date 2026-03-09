-- ============================================================================
-- test_dispatch.lua
-- LuaJIT translation of test_dispatch.c
--
-- The tick loop is written explicitly — one call to tick_once() per iteration,
-- followed by a manual event-queue drain — mirroring the C driver exactly.
--
-- Result-code predicates (result_is_complete, result_is_terminate) are
-- application-defined here rather than baked into the runtime.  Override
-- them at the bottom of this file to suit your module's semantics.
--
-- Usage:
--   luajit test_dispatch.lua [module_name] [tree_name_or_hash]
--
-- Defaults to MODULE_NAME / first tree in module if args are omitted.
-- ============================================================================

local ffi = require("ffi")

-- ============================================================================
-- CONFIGURATION  — edit these for your module
-- ============================================================================

local MODULE_NAME       = (arg and arg[1]) or "complex_sequence_module"
local TREE_NAME_OR_HASH = (arg and arg[2]) or nil   -- nil = first tree in module
local MAX_TICKS         = 500
local TICK_DELAY_SEC    = 0.1   -- mirrors delay_seconds(0.1) in C

-- ============================================================================
-- FFI: delay + wall-clock time  (mirrors nanosleep / clock_gettime in C)
-- ============================================================================

ffi.cdef[[
    struct timespec { long tv_sec; long tv_nsec; };
    int nanosleep(const struct timespec* req, struct timespec* rem);
    int clock_gettime(int clk_id, struct timespec* tp);
]]

local CLOCK_REALTIME = 0

local function delay_seconds(sec)
    if sec <= 0 then return end
    local ts = ffi.new("struct timespec")
    ts.tv_sec  = math.floor(sec)
    ts.tv_nsec = math.floor((sec % 1) * 1e9)
    ffi.C.nanosleep(ts, nil)
end

local function get_wall_time()
    local ts = ffi.new("struct timespec")
    ffi.C.clock_gettime(CLOCK_REALTIME, ts)
    return tonumber(ts.tv_sec) + tonumber(ts.tv_nsec) * 1e-9
end

-- ============================================================================
-- RUNTIME + BUILTINS
-- ============================================================================

local se_runtime = require("se_runtime")

-- Wire real wall time into the runtime default
se_runtime.default_get_time = get_wall_time

-- register_builtins: call register_fns once per builtin library.
-- Add user-defined function tables the same way after this block.
-- validate_module() (called inside new_instance) will report any gaps.
local function register_builtins(mod)
    se_runtime.register_fns(mod, require("se_builtins_flow_control"))
    se_runtime.register_fns(mod, require("se_builtins_dispatch"))
    se_runtime.register_fns(mod, require("se_builtins_pred"))
    se_runtime.register_fns(mod, require("se_builtins_oneshot"))
    se_runtime.register_fns(mod, require("se_builtins_return_codes"))
    se_runtime.register_fns(mod, require("se_builtins_delays"))
    se_runtime.register_fns(mod, require("se_builtins_verify"))
    se_runtime.register_fns(mod, require("se_builtins_stack"))
    se_runtime.register_fns(mod, require("se_builtins_spawn"))
    se_runtime.register_fns(mod, require("se_builtins_quads"))
    se_runtime.register_fns(mod, require("se_builtins_dict"))
    -- se_runtime.register_fns(mod, require("my_user_fns"))
end

-- ============================================================================
-- RESULT CODE HELPERS
-- These are APPLICATION-DEFINED predicates, not runtime policy.
-- result_is_complete and result_is_terminate match the C test driver exactly.
-- Override these here if your module uses different completion semantics.
-- ============================================================================

local R = se_runtime   -- shorthand alias

local RESULT_NAMES = {
    [R.SE_CONTINUE]               = "CONTINUE",
    [R.SE_HALT]                   = "HALT",
    [R.SE_TERMINATE]              = "TERMINATE",
    [R.SE_RESET]                  = "RESET",
    [R.SE_DISABLE]                = "DISABLE",
    [R.SE_SKIP_CONTINUE]          = "SKIP_CONTINUE",
    [R.SE_FUNCTION_CONTINUE]      = "FUNCTION_CONTINUE",
    [R.SE_FUNCTION_HALT]          = "FUNCTION_HALT",
    [R.SE_FUNCTION_TERMINATE]     = "FUNCTION_TERMINATE",
    [R.SE_FUNCTION_RESET]         = "FUNCTION_RESET",
    [R.SE_FUNCTION_DISABLE]       = "FUNCTION_DISABLE",
    [R.SE_FUNCTION_SKIP_CONTINUE] = "FUNCTION_SKIP_CONTINUE",
    [R.SE_PIPELINE_CONTINUE]      = "PIPELINE_CONTINUE",
    [R.SE_PIPELINE_HALT]          = "PIPELINE_HALT",
    [R.SE_PIPELINE_TERMINATE]     = "PIPELINE_TERMINATE",
    [R.SE_PIPELINE_RESET]         = "PIPELINE_RESET",
    [R.SE_PIPELINE_DISABLE]       = "PIPELINE_DISABLE",
    [R.SE_PIPELINE_SKIP_CONTINUE] = "PIPELINE_SKIP_CONTINUE",
}

local function result_to_str(r)
    return RESULT_NAMES[r] or string.format("UNKNOWN(%d)", r)
end

-- Application-defined: what counts as "done, stop the loop"
-- Matches C result_is_complete exactly: TERMINATE + DISABLE families only.
-- Note: HALT variants are NOT included — those mean "still running, try again".
local function result_is_complete(r)
    return r == R.SE_TERMINATE
        or r == R.SE_FUNCTION_TERMINATE
        or r == R.SE_PIPELINE_TERMINATE
        or r == R.SE_DISABLE
        or r == R.SE_FUNCTION_DISABLE
        or r == R.SE_PIPELINE_DISABLE
end

-- Application-defined: what counts as "normal clean termination" for PASS/FAIL
local function result_is_terminate(r)
    return r == R.SE_TERMINATE
        or r == R.SE_FUNCTION_TERMINATE
        or r == R.SE_PIPELINE_TERMINATE
end

-- ============================================================================
-- MODULE LOAD HELPERS
-- ============================================================================

local function load_module(module_name)
    local ok, module_data = pcall(require, module_name)
    if not ok then
        return nil, tostring(module_data)
    end

    -- new_module builds structure only — no functions registered yet
    local mod = se_runtime.new_module(module_data)
    mod.get_time = get_wall_time

    -- Register builtins (and any user-defined function tables)
    register_builtins(mod)

    -- validate_module is called inside new_instance; we can also call it
    -- here to surface missing functions before attempting to run anything.
    local valid, missing = se_runtime.validate_module(mod)
    if not valid then
        local names = {}
        for _, m in ipairs(missing) do
            names[#names+1] = string.format("[%s] %s", m.kind, m.name)
        end
        return nil, "missing functions:\n  " .. table.concat(names, "\n  ")
    end

    return mod
end

local function print_module_stats(module_data)
    local tree_count   = module_data.tree_order and #module_data.tree_order or 0
    local oneshot_count= module_data.oneshot_funcs and #module_data.oneshot_funcs or 0
    local main_count   = module_data.main_funcs    and #module_data.main_funcs    or 0
    local pred_count   = module_data.pred_funcs    and #module_data.pred_funcs    or 0
    print(string.format("   Trees:    %d", tree_count))
    print(string.format("   Oneshot:  %d", oneshot_count))
    print(string.format("   Main:     %d", main_count))
    print(string.format("   Pred:     %d", pred_count))
end

local function resolve_tree_name(mod, hint)
    if hint then
        if mod.module_data.trees and mod.module_data.trees[hint] then
            return hint
        end
        local hash = tonumber(hint)
        if hash and mod.trees_by_hash[hash] then
            return mod.trees_by_hash[hash]
        end
        if hint:sub(1,2) ~= "0x" then
            hash = tonumber("0x" .. hint)
            if hash and mod.trees_by_hash[hash] then
                return mod.trees_by_hash[hash]
            end
        end
        return nil, "tree not found for hint: " .. hint
    end
    if mod.module_data.test_tree_name then
        return mod.module_data.test_tree_name
    end
    local order = mod.module_data.tree_order
    if order and order[1] then return order[1] end
    return nil, "no trees in module"
end

-- ============================================================================
-- TEST_DISPATCH
-- Mirrors C test_dispatch() with the event queue drain loop written out
-- explicitly.  This is a line-for-line translation of the C loop:
--
--   do {
--       result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);   // tick_once
--       delay_seconds(0.1);
--
--       event_count = s_expr_event_queue_count(tree);
--       while (event_count > 0) {
--           s_expr_event_pop(tree, &tick_type, &event_id, &event_data);
--           saved = tree->tick_type;
--           tree->tick_type = tick_type;
--           event_result = s_expr_node_tick(tree, event_id, event_data);
--           tree->tick_type = saved;
--           if (result_is_complete(event_result)) { result = event_result; break; }
--           event_count = s_expr_event_queue_count(tree);
--       }
--   } while (!result_is_complete(result) && tick_count < max_ticks);
-- ============================================================================

local function test_dispatch(mod, tree_name)
    print("\n╔════════════════════════════════════════╗")
    print("║    LOOP TEST                           ║")
    print("╚════════════════════════════════════════╝")
    print(string.format("\nTesting tree: %s", tree_name))
    print("Testing dispatch with tick loop...\n")

    local inst = se_runtime.new_instance(mod, tree_name)

    local tick_count = 0
    local result

    print("  Running tick loop...")

    repeat
        -- ----------------------------------------------------------------
        -- Primary tick  (mirrors: result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL))
        -- ----------------------------------------------------------------
        result = se_runtime.tick_once(inst, R.SE_EVENT_TICK, nil)
        tick_count = tick_count + 1

        delay_seconds(TICK_DELAY_SEC)

        print(string.format(
            "------------------------>    Tick %3d: result=%s",
            tick_count, result_to_str(result)))

        -- ----------------------------------------------------------------
        -- Event queue drain  (mirrors the inner while loop in C)
        -- ----------------------------------------------------------------
        local event_count = se_runtime.event_count(inst)

        while event_count > 0 do
            local tick_type, event_id, event_data = se_runtime.event_pop(inst)

            local saved_tick_type = inst.tick_type
            inst.tick_type = tick_type

            local event_result = se_runtime.tick_once(inst, event_id, event_data)

            inst.tick_type = saved_tick_type

            if result_is_complete(event_result) then
                result = event_result
                break
            end

            event_count = se_runtime.event_count(inst)
        end

    until result_is_complete(result) or tick_count >= MAX_TICKS

    -- ----------------------------------------------------------------
    -- Results  (mirrors C printf block)
    -- ----------------------------------------------------------------
    print(string.format("\n  Total ticks: %d", tick_count))
    print(string.format("  Final result: %s", result_to_str(result)))

    if result_is_terminate(result) then
        print("\n  ✅ PASSED - Tree terminated normally")
    elseif tick_count >= MAX_TICKS then
        print("\n  ❌ FAILED - Max ticks exceeded without termination")
    elseif result_is_complete(result) then
        print("\n  ✅ PASSED - Tree completed (disabled)")
    else
        print("\n  ❌ FAILED - Unexpected result")
    end
end

-- ============================================================================
-- MAIN  (mirrors C main())
-- ============================================================================

print()
print("╔════════════════════════════════════════════════════════════════╗")
print("║           S-EXPRESSION ENGINE DISPATCH TEST                    ║")
print("╚════════════════════════════════════════════════════════════════╝")
print()

-- ---- Test 1: require() load — analogous to s_engine_load_from_rom --------

print("=== Loading module from require() ===\n")

local mod, err = load_module(MODULE_NAME)
if not mod then
    print(string.format("❌ FATAL: Failed to load module '%s': %s", MODULE_NAME, err))
    os.exit(1)
end

print("✅ Module loaded successfully")
print_module_stats(mod.module_data)

local tree_name, tree_err = resolve_tree_name(mod, TREE_NAME_OR_HASH)
if not tree_name then
    print(string.format("❌ FATAL: %s", tree_err))
    os.exit(1)
end

test_dispatch(mod, tree_name)

-- ---- Test 2: alternate path load — analogous to s_engine_load_from_file --

print("\n\n=== Loading module from alternate path ===\n")

local alt_name = MODULE_NAME .. "_file"
local mod2, err2 = load_module(alt_name)

if not mod2 then
    print(string.format("⚠️  WARNING: Could not load '%s': %s", alt_name, err2))
    print("   This is OK if running without the alternate module file.")
else
    print(string.format("✅ Module '%s' loaded successfully", alt_name))
    print_module_stats(mod2.module_data)

    local tree_name2, tree_err2 = resolve_tree_name(mod2, TREE_NAME_OR_HASH)
    if not tree_name2 then
        print(string.format("⚠️  WARNING: %s -- skipping", tree_err2))
    else
        test_dispatch(mod2, tree_name2)
    end
end

print("\n✅ All tests completed!\n")