-- ============================================================================
-- callback_lua_functions.lua
-- Lua 5.3 callback function implementation
--
-- Registered via se_bridge (global set by se_lua_bridge_init in C)
-- ============================================================================

local bridge = se_bridge

-- ============================================================================
-- FNV-1a 32-bit hash (Lua 5.3 native integers)
-- ============================================================================

local function fnv1a_32(str)
    local hash = 0x811c9dc5
    local prime = 0x01000193

    for i = 1, #str do
        hash = hash ~ string.byte(str, i)
        hash = (hash * prime) & 0xFFFFFFFF
    end

    return hash
end

-- ============================================================================
-- Result codes (match s_engine_types.h)
-- ============================================================================

local SE_CONTINUE               = 0
local SE_HALT                   = 1
local SE_TERMINATE              = 2
local SE_FUNCTION_CONTINUE      = 6
local SE_FUNCTION_TERMINATE     = 8
local SE_PIPELINE_CONTINUE      = 12
local SE_PIPELINE_TERMINATE     = 14

-- ============================================================================
-- Event types
-- ============================================================================

local SE_EVENT_INIT      = 0xFFFC
local SE_EVENT_TERMINATE = 0xFFFD
local SE_EVENT_TICK      = 0xFFFB

-- ============================================================================
-- MAIN: LUA_CALLBACK_FN
--
-- This is the actual callback logic, implemented entirely in Lua.
-- The engine calls this via the trampoline with:
--   inst, params, event_type, event_id, event_data
--
-- Must return an s_expr_result_t integer.
-- ============================================================================

bridge.register(fnv1a_32("LUA_CALLBACK_FN"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        print("  [LUA_CALLBACK_FN] Callback executing in Lua!")
        print("  [LUA_CALLBACK_FN] Doing some Lua-side work...")
        print(string.format("  [LUA_CALLBACK_FN] event_type=%d, event_id=%d",
            event_type, event_id))
        print("  [LUA_CALLBACK_FN] Callback complete")
    end
)
-- ============================================================================
-- Registration summary
-- ============================================================================

print(string.format("  Registered: LUA_CALLBACK_FN (hash=0x%08X) as main",
    fnv1a_32("LUA_CALLBACK_FN")))

print("  Lua callback function registration complete")

