-- callback_function_test.lua
-- Test for callback function implemented in Lua
--
-- The callback is a Lua main function that gets loaded into a
-- blackboard pointer field and executed via se_exec_function.

local mod = start_module("callback_function")

-- ============================================================================
-- LUA FUNCTION DECLARATIONS
-- ============================================================================

LUA_FUNCTIONS {
    oneshot = { "LUA_CALLBACK_FN" },
}

-- ============================================================================
-- RECORD: callback_function_blackboard
-- ============================================================================

RECORD("callback_function_blackboard")
    PTR64_FIELD("fn_ptr", "void")
END_RECORD()

start_tree("callback_function")
use_record("callback_function_blackboard")

-- ============================================================================
-- CALLBACK: implemented in Lua via trampoline
-- ============================================================================

local fns = function()
    se_sequence_once(function()
        se_log("callback function called -- from Lua")
        
        -- Call into the Lua main function
        local cb = o_call("LUA_CALLBACK_FN")
        end_call(cb)
        
        se_log("callback function returned from Lua")
    end)
end

-- ============================================================================
-- MAIN PROGRAM
-- ============================================================================

se_function_interface(function()
    se_log("callback test started")
    se_load_function("fn_ptr", fns)
    se_exec_function("fn_ptr")
    se_log("callback test finished")
    se_return_function_terminate()
end)

end_tree("callback_function")

return end_module(mod)