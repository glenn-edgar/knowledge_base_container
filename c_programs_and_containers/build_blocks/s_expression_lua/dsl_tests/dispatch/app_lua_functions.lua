-- ============================================================================
-- app_lua_functions.lua
-- Lua 5.3 implementations of dispatch_test user functions
--
-- Registered via se_bridge (global set by se_lua_bridge_init in C)
-- ============================================================================

local bridge = se_bridge

-- ============================================================================
-- FNV-1a 32-bit hash (Lua 5.3 native integers)
-- Matches the C/LuaJIT implementation in the DSL
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
-- S-Expression event IDs (must match generated header / DSL EVENTS{})
-- ============================================================================

local SE_EVENT_TICK = 0xFFFB

-- ============================================================================
-- ONESHOT: display_event_info
--
-- Args from trampoline:
--   inst        - userdata with :read_i32(), :read_f32(), :read_f32_ptr(), etc.
--   params      - table of param entries
--   event_type  - integer
--   event_id    - integer
--   event_data  - light userdata (void* from C, opaque to Lua)
-- ============================================================================

bridge.register(fnv1a_32("DISPLAY_EVENT_INFO"), "oneshot",
    function(inst, params, event_type, event_id, event_data)
        -- Skip tick events
        if event_id == SE_EVENT_TICK then
            return
        end

        print(string.format(
            "******************[display_event_info] Displaying event info"))
        print(string.format(
            "******************[display_event_info] Event type: %d, Event ID: %d",
            event_type, event_id))

        -- event_data is light userdata — get the offset as integer
        local offset = inst:ptr_to_offset(event_data)
        print(string.format(
            "******************[display_event_info] Event data %d", offset))

        -- Read float from blackboard via the opaque pointer
        local value = inst:read_f32_ptr(event_data)
        print(string.format(
            "******************[display_event_info] Value: %f", value))
    end
)

-- ============================================================================
-- Registration summary
-- ============================================================================

print(string.format("  Registered: display_event_info (hash=0x%08X) as oneshot",
    fnv1a_32("display_event_info")))

print("  Lua function registration complete")