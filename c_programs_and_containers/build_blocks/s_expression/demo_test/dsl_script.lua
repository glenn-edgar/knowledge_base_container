---
---



local M = require("s_expr_dsl")
local mod = start_module("s_expr_dsl_test")
use_32bit()
set_debug(true)
----

RECORD("state_machine_blackboard")
    FIELD("state", "int32")
END_RECORD()






local SM_STATE_1 = 1
local SM_STATE_2 = 2
local SM_STATE_3 = 3

-- ============================================================================
-- TEST 4: State Machine
-- ============================================================================

actions_fn = {}
actions_fn[1] = function()
    se_log("State 0")
    local o0=o_call("CFL_DISABLE_CHILDREN")
    end_call(o0)
    local o1=o_call("CFL_ENABLE_CHILD")
       int(0)
    end_call(o1)
    se_tick_delay(100)
    se_set_field("state", 1)
    se_return_halt()
end

actions_fn[2] = function()
    se_log("State 1")
    local o0=o_call("CFL_DISABLE_CHILDREN")
    end_call(o0)
    local o1=o_call("CFL_ENABLE_CHILD")
       int(1)
    end_call(o1)
    se_tick_delay(100)
    se_set_field("state", 2)
    se_return_halt()
end

actions_fn[3] = function()
    se_log("State 2")
    local o0=o_call("CFL_DISABLE_CHILDREN")
    end_call(o0)
    local o1=o_call("CFL_ENABLE_CHILD")
       int(2)
    end_call(o1)
    se_tick_delay(100)
    se_log("State 2 terminated")
    se_return_terminate()
end

start_tree("state_machine_test")
    use_record("state_machine_blackboard")

    localseq0  = m_call("SE_SEQUENCE")
         se_i_set_field("state", 0)
         se_log("State machine test started")
        

         se_state_machine("state",actions_fn)
     
    end_call(seq0)
    
end_tree("state_machine_test")

local result = end_module(mod)
print("Module compiled successfully: " .. result.name)

--print(M.write_debug_header(result))
return result