-- ============================================================================
-- s_expr_dsl_test.lua
-- Comprehensive Test Module for S-Expression Engine DSL v5.1
-- 
-- This test exercises ALL language features:
--   - All primitive types (int8-64, uint8-64, float, double, bool, char)
--   - CHAR_ARRAY fields
--   - PTR_FIELD (pointer fields)
--   - Embedded records (nested structs)
--   - Constants with nested field values
--   - All call types: o_call, m_call, p_call, pt_m_call, io_call, p_call_composite
--   - All parameter types: int, uint, flt, str, str_ptr, field_ref, nested_field_ref, const_ref
--   - All result codes
--   - Composable predicates (AND, OR, NOT, NOR, NAND, XOR)
--   - Helper functions from s_engine_helpers.lua
--   - list_start/list_end for dispatch cases
--
-- Usage: luajit s_compile.lua s_expr_dsl_test.lua --helpers=s_engine_helpers.lua --all-bin
-- ============================================================================

-- NOTE: Do not require s_expr_dsl or s_engine_helpers here.
-- The compiler (s_compile.lua) loads them automatically.

-- ============================================================================
-- BEGIN MODULE
-- ============================================================================
local M = require("s_expr_dsl")
local mod = start_module("s_expr_dsl_test")
use_32bit()
set_debug(true)

-- ============================================================================
-- RECORD DEFINITIONS - Testing All Types
-- ============================================================================

-- Record with all primitive types
RECORD("all_primitives")
    FIELD("field_int8", "int8")
    FIELD("field_int16", "int16")
    FIELD("field_int32", "int32")
    FIELD("field_int64", "int64")
    FIELD("field_uint8", "uint8")
    FIELD("field_uint16", "uint16")
    FIELD("field_uint32", "uint32")
    FIELD("field_uint64", "uint64")
    FIELD("field_float", "float")
    FIELD("field_double", "double")
    FIELD("field_bool", "bool")
    FIELD("field_char", "char")
END_RECORD()

-- Record with char array
RECORD("string_container")
    CHAR_ARRAY("name", 32)
    CHAR_ARRAY("description", 64)
    FIELD("length", "uint16")
END_RECORD()

-- Simple inner record for embedding
RECORD("vector3d")
    FIELD("x", "float")
    FIELD("y", "float")
    FIELD("z", "float")
END_RECORD()

-- Another inner record
RECORD("pid_gains")
    FIELD("kp", "float")
    FIELD("ki", "float")
    FIELD("kd", "float")
END_RECORD()

-- Mid-level record with embedded record
RECORD("motor_state")
    FIELD("position", "vector3d")      -- embedded
    FIELD("velocity", "vector3d")      -- embedded
    FIELD("torque", "float")
    FIELD("enabled", "bool")
END_RECORD()

-- Top-level record with multiple embedded records (3 levels deep)
RECORD("system_state")
    FIELD("motor", "motor_state")      -- embedded (contains vector3d)
    FIELD("pid", "pid_gains")          -- embedded
    FIELD("system_time", "uint32")
    FIELD("error_code", "uint16")
END_RECORD()

-- Self-referential record for linked list
RECORD("list_node")
    FIELD("data", "int32")
    PTR_FIELD("next", "list_node")     -- pointer to self
END_RECORD()

-- Record with pointer fields to various types
RECORD("pointer_container")
    FIELD("id", "uint32")
    PTR_FIELD("node_ptr", "list_node")
    PTR_FIELD("vector_ptr", "vector3d")
    PTR_FIELD("char_ptr", "char")
    FIELD("count", "uint16")
END_RECORD()

-- Complex blackboard combining many features
RECORD("test_blackboard")
    FIELD("state", "int32")
    FIELD("command", "int32")
    FIELD("event_id", "int32")
    FIELD("counter", "uint32")
    FIELD("temperature", "float")
    FIELD("enabled", "bool")
    CHAR_ARRAY("message", 48)
    FIELD("motor", "motor_state")      -- embedded with nested embedding
    FIELD("gains", "pid_gains")        -- embedded
    PTR_FIELD("data_ptr", "list_node")
    FIELD("flags", "uint32")
END_RECORD()

-- ============================================================================
-- CONSTANT DEFINITIONS
-- ============================================================================

-- Simple constant
CONST("default_gains", "pid_gains")
    VALUE("kp", 2.5)
    VALUE("ki", 0.5)
    VALUE("kd", 0.1)
END_CONST()

-- Constant with nested field paths
CONST("default_motor", "motor_state")
    VALUE("position.x", 0.0)
    VALUE("position.y", 0.0)
    VALUE("position.z", 0.0)
    VALUE("velocity.x", 0.0)
    VALUE("velocity.y", 0.0)
    VALUE("velocity.z", 0.0)
    VALUE("torque", 0.0)
    VALUE("enabled", false)
END_CONST()

-- Full blackboard default
CONST("blackboard_defaults", "test_blackboard")
    VALUE("state", 0)
    VALUE("command", 0)
    VALUE("event_id", 0)
    VALUE("counter", 0)
    VALUE("temperature", 25.0)
    VALUE("enabled", true)
    VALUE("motor.position.x", 100.0)
    VALUE("motor.position.y", 200.0)
    VALUE("motor.position.z", 300.0)
    VALUE("motor.velocity.x", 0.0)
    VALUE("motor.velocity.y", 0.0)
    VALUE("motor.velocity.z", 0.0)
    VALUE("motor.torque", 0.0)
    VALUE("motor.enabled", false)
    VALUE("gains.kp", 1.0)
    VALUE("gains.ki", 0.2)
    VALUE("gains.kd", 0.05)
    VALUE("flags", 0)
END_CONST()

-- ============================================================================
-- TEST TREE 1: All Call Types
-- Tests: o_call, m_call, p_call, pt_m_call, io_call
-- ============================================================================

start_tree("test_all_call_types")
    use_record("test_blackboard")
    
    -- io_call: Init oneshot (survives reset)
    local init = io_call("INIT_SYSTEM")
        str_ptr("Initializing system")
        field_ref("state")
        int(0)
    end_call(init)
    
   
    se_log("System initialized")
    
    -- p_call: Predicate
    local pred = p_call("CHECK_ENABLED")
        field_ref("enabled")
    end_call(pred)
    
    -- m_call: Main function with children
    local main = m_call("PROCESS_STATE")
        field_ref("state")
        
        -- Nested o_call
        local nested = o_call("UPDATE_COUNTER")
            field_ref("counter")
            uint(1)
        end_call(nested)
        
        result(SE_CONTINUE)
    end_call(main)
    
    -- pt_m_call: Protothread main (has pointer slot)
    local pt = pt_m_call("WAIT_FOR_EVENT")
        int(100)  -- timeout
    end_call(pt)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 2: All Parameter Types
-- Tests: int, uint, flt, str, str_ptr, field_ref, nested_field_ref, const_ref
-- ============================================================================

start_tree("test_all_param_types")
    use_record("test_blackboard")
    
    local test = m_call("TEST_PARAMS")
        -- int parameter
        int(-12345)
        
        -- uint parameter
        uint(0xDEADBEEF)
        
        -- float parameter
        flt(3.14159)
        
        -- str parameter (indexed string)
        str("Hello, World!")
        
        -- str_ptr parameter (string pointer)
        str_ptr("This is a longer string for testing")
        
        -- Simple field reference
        field_ref("counter")
        
        -- Nested field reference (2 levels)
        nested_field_ref("gains.kp")
        
        -- Deeply nested field reference (3 levels)
        nested_field_ref("motor.position.x")
        
        -- Constant reference
        const_ref("default_gains")
        
        result(SE_CONTINUE)
    end_call(test)
    
end_tree()

-- ============================================================================
-- TEST TREE 3: All Result Codes
-- Tests: SE_CONTINUE, SE_TERMINATE, SE_RESET, SE_DISABLE, SE_HALT,
--        SE_SKIP_CONTINUE, SE_FUNCTION_HALT, SE_FUNCTION_RESET, SE_FUNCTION_TERMINATE
-- ============================================================================

start_tree("test_result_codes_1")
    use_record("test_blackboard")
    se_return_continue()
end_tree()

start_tree("test_result_codes_2")
    use_record("test_blackboard")
    se_return_terminate()
end_tree()

start_tree("test_result_codes_3")
    use_record("test_blackboard")
    se_return_reset()
end_tree()

start_tree("test_result_codes_4")
    use_record("test_blackboard")
    se_return_disable()
end_tree()

start_tree("test_result_codes_5")
    use_record("test_blackboard")
    se_return_halt()
end_tree()

start_tree("test_result_codes_6")
    use_record("test_blackboard")
    se_return_skip_continue()
end_tree()

start_tree("test_result_codes_7")
    use_record("test_blackboard")
    se_return_function_halt()
end_tree()

start_tree("test_result_codes_8")
    use_record("test_blackboard")
    se_return_function_reset()
end_tree()

start_tree("test_result_codes_9")
    use_record("test_blackboard")
    se_return_function_terminate()
end_tree()
-- ============================================================================
-- TEST TREE 4: Composable Predicates
-- Tests: se_pred_and, se_pred_or, se_pred_not, se_pred_nor, se_pred_nand, se_pred_xor
-- Tests nested composition: (A AND B) OR (C AND D)
-- ============================================================================

start_tree("test_composable_predicates")
    use_record("test_blackboard")
    
-- Simple AND
local p1 = se_pred_and()
    se_pred("PRED_A")
    se_pred("PRED_B")
end_call(p1)

-- Simple OR
local p2 = se_pred_or()
    se_pred("PRED_C")
    se_pred("PRED_D")
end_call(p2)

-- NOT (single child)
local p3 = se_pred_not()
    se_pred("PRED_E")
end_call(p3)

-- Complex nested: (A AND B) OR (C AND D)
local complex = se_pred_or()
    local and1 = se_pred_and()
        se_pred("SENSOR_A_READY")
        se_pred("SENSOR_B_READY")
    end_call(and1)
    local and2 = se_pred_and()
        se_pred("TIMEOUT_EXPIRED")
        se_pred("RETRY_AVAILABLE")
    end_call(and2)
end_call(complex)

-- Triple nested: NOT((A OR B) AND (C OR D))
local triple = se_pred_not()
    local inner_and = se_pred_and()
        local or1 = se_pred_or()
            se_pred("FLAG_1")
            se_pred("FLAG_2")
        end_call(or1)
        local or2 = se_pred_or()
            se_pred("FLAG_3")
            se_pred("FLAG_4")
        end_call(or2)
    end_call(inner_and)
end_call(triple)

-- NOR
local p4 = se_pred_nor()
    se_pred("PRED_F")
    se_pred("PRED_G")
end_call(p4)

-- NAND
local p5 = se_pred_nand()
    se_pred("PRED_H")
    se_pred("PRED_I")
end_call(p5)

-- XOR
local p6 = se_pred_xor()
    se_pred("PRED_J")
    se_pred("PRED_K")
end_call(p6)

    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 5: Helper Functions - Pipeline and Delays
-- Tests: se_pipeline, se_tick_delay, se_time_delay, se_wait_event, se_nop, se_log
-- ============================================================================

start_tree("test_pipeline_and_delays")
    use_record("test_blackboard")
    
    se_pipeline(function()
        se_log("Starting pipeline test")
        
        -- Tick delay
        se_tick_delay(100)
        
        -- Time delay
        se_time_delay(1.5)
        
        -- Wait for event
        se_wait_event(42, 3)
        
        -- Wait event once (convenience)
        se_wait_event_once(99)
        
        -- NOP
        se_nop()
        
        se_log("Pipeline complete")
        
        result(SE_FUNCTION_TERMINATE)
    end)
    
end_tree()

-- ============================================================================
-- TEST TREE 6: Helper Functions - Conditionals
-- Tests: se_if_then_else, se_if_then, se_trigger_on_change,
--        se_on_rising_edge, se_on_falling_edge
-- ============================================================================

start_tree("test_conditionals")
    use_record("test_blackboard")
    
    -- Full if-then-else
    se_if_then_else(
        function()  -- predicate
            local p = p_call("CHECK_CONDITION") end_call(p)
        end,
        function()  -- then branch
            se_log("Condition is true")
        end,
        function()  -- else branch
            se_log("Condition is false")
        end
    )
    
    -- Simple if-then (no else)
    se_if_then(
        function()
            local p = p_call("ANOTHER_CONDITION") end_call(p)
        end,
        function()
            se_log("Another condition is true")
        end
    )
    
    -- Trigger on change
    se_trigger_on_change(0,
        function()  -- predicate
            local p = p_call("MONITOR_STATE") end_call(p)
        end,
        function()  -- on rising (0->1)
            se_log("State went high")
        end,
        function()  -- on falling (1->0)
            se_log("State went low")
        end
    )
    
    -- Rising edge only
    se_on_rising_edge(
        function()
            local p = p_call("BUTTON_PRESSED") end_call(p)
        end,
        function()
            se_log("Button was pressed")
        end
    )
    
    -- Falling edge only
    se_on_falling_edge(
        function()
            local p = p_call("SENSOR_ACTIVE") end_call(p)
        end,
        function()
            se_log("Sensor went inactive")
        end
    )
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 7: Helper Functions - State Machine
-- Tests: se_state_machine, se_state_actions
-- ============================================================================

local STATE_IDLE = 0
local STATE_RUNNING = 1
local STATE_PAUSED = 2
local STATE_DONE = 3

start_tree("test_state_machine")
    use_record("test_blackboard")
    
    se_state_machine("state", {
        -- State 0: IDLE
        function()
            se_log("State: IDLE")
            local set = o_call("SET_STATE")
                field_ref("state")
                int(STATE_RUNNING)
            end_call(set)
            result(SE_HALT)
        end,
        
        -- State 1: RUNNING
        function()
            se_log("State: RUNNING")
            se_tick_delay(50)
            local set = o_call("SET_STATE")
                field_ref("state")
                int(STATE_PAUSED)
            end_call(set)
            result(SE_HALT)
        end,
        
        -- State 2: PAUSED
        function()
            se_log("State: PAUSED")
            se_tick_delay(25)
            local set = o_call("SET_STATE")
                field_ref("state")
                int(STATE_DONE)
            end_call(set)
            result(SE_HALT)
        end,
        
        -- State 3: DONE
        function()
            se_log("State: DONE")
            result(SE_FUNCTION_TERMINATE)
        end,
    })
    
end_tree()

-- ============================================================================
-- TEST TREE 8: Helper Functions - Dispatch
-- Tests: se_dispatch, se_field_dispatch, se_event_dispatch
-- ============================================================================

local CMD_IDLE = 0
local CMD_START = 1
local CMD_STOP = 2
local CMD_RESET = 3

local EVT_TIMER = 100
local EVT_BUTTON = 101
local EVT_SENSOR = 102

start_tree("test_dispatch")
    use_record("test_blackboard")
    
    -- Field dispatch
    se_field_dispatch("command", {
        { CMD_IDLE, function()
            se_log("Command: IDLE")
            result(SE_CONTINUE)
        end },
        { CMD_START, function()
            se_log("Command: START")
            result(SE_HALT)
        end },
        { CMD_STOP, function()
            se_log("Command: STOP")
            result(SE_HALT)
        end },
        { CMD_RESET, function()
            se_log("Command: RESET")
            result(SE_FUNCTION_RESET)
        end },
    })
    
    -- Event dispatch
    se_event_dispatch({
        { EVT_TIMER, function()
            se_log("Event: TIMER")
        end },
        { EVT_BUTTON, function()
            se_log("Event: BUTTON")
        end },
        { EVT_SENSOR, function()
            se_log("Event: SENSOR")
        end },
    })
    
end_tree()

-- ============================================================================
-- TEST TREE 9: Predicate Helpers
-- Tests: se_true, se_false, se_check_event
-- ============================================================================

start_tree("test_predicate_helpers")
    use_record("test_blackboard")
    
    -- Always true
    se_true()
    
    -- Always false
    se_false()
    
    -- Check for specific events
    se_check_event(EVT_TIMER, EVT_BUTTON, EVT_SENSOR)
    
    -- Complex predicate using helpers
    local complex = se_pred_or()
        se_true()
        local inner = se_pred_and()
            se_false()
            local p = p_call("CUSTOM_PRED") end_call(p)
        end_call(inner)
    end_call(complex)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 10: Nested Field Access
-- Tests deep field access through embedded records
-- ============================================================================

start_tree("test_nested_fields")
    use_record("test_blackboard")
    
    se_pipeline(function()
        -- Access 3 levels deep: motor.position.x
        local set_pos = o_call("SET_VECTOR")
            nested_field_ref("motor.position.x")
            nested_field_ref("motor.position.y")
            nested_field_ref("motor.position.z")
            flt(100.0)
            flt(200.0)
            flt(300.0)
        end_call(set_pos)
        
        -- Access 3 levels deep: motor.velocity
        local set_vel = o_call("SET_VECTOR")
            nested_field_ref("motor.velocity.x")
            nested_field_ref("motor.velocity.y")
            nested_field_ref("motor.velocity.z")
            flt(1.0)
            flt(2.0)
            flt(3.0)
        end_call(set_vel)
        
        -- Access 2 levels: motor.torque
        local set_torque = o_call("SET_FLOAT")
            nested_field_ref("motor.torque")
            flt(50.0)
        end_call(set_torque)
        
        -- Access 2 levels: gains.kp/ki/kd
        local set_gains = o_call("SET_PID")
            nested_field_ref("gains.kp")
            nested_field_ref("gains.ki")
            nested_field_ref("gains.kd")
            flt(1.5)
            flt(0.3)
            flt(0.08)
        end_call(set_gains)
        
        -- Access 1 level (no nesting)
        local set_counter = o_call("SET_UINT")
            field_ref("counter")
            uint(12345)
        end_call(set_counter)
        
        result(SE_FUNCTION_TERMINATE)
    end)
    
end_tree()

-- ============================================================================
-- TEST TREE 11: Multiple pt_m_call (Pointer Slots)
-- Tests multiple protothread functions with pointer tracking
-- ============================================================================

start_tree("test_pointer_slots")
    use_record("test_blackboard")
    
    -- First pt_m_call - gets pointer_index 0
    local pt1 = pt_m_call("PROTOTHREAD_1")
        int(100)
    end_call(pt1)
    
    -- Second pt_m_call - gets pointer_index 1
    local pt2 = pt_m_call("PROTOTHREAD_2")
        int(200)
    end_call(pt2)
    
    -- Third pt_m_call - gets pointer_index 2
    local pt3 = pt_m_call("PROTOTHREAD_3")
        int(300)
    end_call(pt3)
    
    -- Nested pt_m_call inside m_call
    local outer = m_call("OUTER_FUNC")
        -- Fourth pt_m_call - gets pointer_index 3
        local pt4 = pt_m_call("NESTED_PROTOTHREAD")
            int(400)
        end_call(pt4)
    end_call(outer)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 12: Complex Nested Structure
-- Tests deeply nested call structure with mixed types
-- ============================================================================

start_tree("test_complex_nesting")
    use_record("test_blackboard")
    
    local outer = m_call("LEVEL_1")
        str_ptr("Level 1")
        
        local level2a = m_call("LEVEL_2A")
            str_ptr("Level 2A")
            
            local level3a = o_call("LEVEL_3A")
                str_ptr("Level 3A")
            end_call(level3a)
            
            local level3b = m_call("LEVEL_3B")
                str_ptr("Level 3B")
                
                local level4 = pt_m_call("LEVEL_4")
                    str_ptr("Level 4")
                    int(42)
                end_call(level4)
                
            end_call(level3b)
            
        end_call(level2a)
        
        local level2b = m_call("LEVEL_2B")
            str_ptr("Level 2B")
            
            -- Nested predicate composition
            local pred = se_pred_and()
                local p1 = p_call("DEEP_PRED_1") end_call(p1)
                local inner_or = se_pred_or()
                    local p2 = p_call("DEEP_PRED_2") end_call(p2)
                    local p3 = p_call("DEEP_PRED_3") end_call(p3)
                end_call(inner_or)
            end_call(pred)
            
        end_call(level2b)
        
        result(SE_CONTINUE)
    end_call(outer)
    
end_tree()

-- ============================================================================
-- END MODULE
-- ============================================================================

local result = end_module(mod)
print("Module compiled successfully: " .. result.name)
print("  Trees: " .. #result.tree_order)
print("  Records: " .. #result.record_order)
print("  Constants: " .. #result.const_order)
print("  Strings: " .. #result.string_table)
print("  Oneshot functions: " .. #result.oneshot_funcs)
print("  Main functions: " .. #result.main_funcs)
print("  Pred functions: " .. #result.pred_funcs)
-- Debug: Print actual structure
print("\n=== DEBUG: Module structure ===")
print("record_order type: " .. type(result.record_order))
if result.record_order then
    for i, rec in ipairs(result.record_order) do
        print(string.format("  [%d] type=%s", i, type(rec)))
        if type(rec) == "table" then
            for k, v in pairs(rec) do
                print(string.format("      %s = %s", tostring(k), tostring(v)))
            end
        else
            print("      value = " .. tostring(rec))
        end
    end
end
print("=== END DEBUG ===\n")

print(M.write_debug_header(result))
return result