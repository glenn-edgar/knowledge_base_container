-- ============================================================================
-- s_expr_dsl_test.lua
-- Comprehensive Test Module for S-Expression Engine DSL v5.2
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
--   - Dictionary structures (dict_start/dict_end, key_start/key_end)
--   - Array structures (array_start/array_end)
--   - Tuple structures (tuple_start/tuple_end)
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
    FIELD("hash_state", "uint32")
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
    VALUE("state", 1)
    VALUE("hash_state", 0x12345678)
    VALUE("command", 2)
    VALUE("event_id", 03)
    VALUE("counter", 5)
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
    VALUE("flags", 4)
END_CONST()


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


start_tree("test_pipeline_and_delays")
    use_record("test_blackboard")
    
    se_pipeline(function()
        se_log("Starting pipeline test")
        
        -- Tick delay
        se_tick_delay(100)
        se_log("Tick delay complete")
        -- Time delay
        se_time_delay(1.5)
        se_log("Time delay complete")
        -- Wait for event
        se_wait_event(42, 3)
        se_log("Wait for event 42 3 complete")
        -- Wait event once (convenience)
        se_wait_event_once(99)
        se_log("Wait event once 99 complete")
        -- NOP
        se_nop()
        
        se_log("Pipeline complete")
        
        se_return_function_terminate()
    end)
    
end_tree()

-- ============================================================================
-- TEST TREE 6: Helper Functions - Conditionals
-- Tests: se_if_then_else, se_if_then, se_trigger_on_change,
--        se_on_rising_edge, se_on_falling_edge
-- ============================================================================

start_tree("test_conditionals")
    use_record("test_blackboard")
    
    se_pipeline(function()
        se_log("Starting test_conditionals")
        
        -- Full if-then-else
        se_if_then_else(
            function()
                local p = p_call("CHECK_CONDITION") end_call(p)
            end,
            function()
                se_log("Condition is true")
                se_return_continue()
                
            end,
            function()
                se_log("Condition is false")
                se_return_continue()
            end
        )
        
        -- Simple if-then (no else)
        se_if_then(
            function()
                local p = p_call("ANOTHER_CONDITION") end_call(p)
            end,
            function()
                se_log("Another condition is true")
                se_return_continue()
            end
        )
        
        -- Trigger on change
        se_trigger_on_change(0,
            function()
                local p = p_call("MONITOR_STATE") end_call(p)
            end,
            function()
                se_log("State went high")
                se_return_continue()
            end,
            function()
                se_log("State went low")
                se_return_continue()
            end
        )
        
        -- Rising edge only
        se_on_rising_edge(
            function()
                local p = p_call("BUTTON_PRESSED") end_call(p)
            end,
            function()
                se_log("Button was pressed")
                se_return_continue()
            end
        )
        
        -- Falling edge only
        se_on_falling_edge(
            function()
                local p = p_call("SENSOR_ACTIVE") end_call(p)
            end,
            function()
                se_log("Sensor went inactive")
                se_return_continue()
            end
        )
        
        se_log("Ending test_conditionals")
        se_return_function_terminate()
    end)
    
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
    
    se_pipeline(function()
    local set = io_call("SET_STATE")
        field_ref("state")
        int(STATE_IDLE)
    end_call(set)
    se_state_machine("state", {
        -- State 0: IDLE
        function()
            se_log("State: IDLE")
            local set = o_call("SET_STATE")
                field_ref("state")
                int(STATE_RUNNING)
            end_call(set)
            se_return_halt()
        end,
        
        -- State 1: RUNNING
        function()
            se_log("State: RUNNING")
            se_tick_delay(50)
            local set = o_call("SET_STATE")
                field_ref("state")
                int(STATE_PAUSED)
            end_call(set)
            se_return_halt()
        end,
        
        -- State 2: PAUSED
        function()
            se_log("State: PAUSED")
            se_tick_delay(25)
            local set = o_call("SET_STATE")
                field_ref("state")
                int(STATE_DONE)
            end_call(set)
            se_return_halt()
        end,
        
        -- State 3: DONE
        function()
            se_log("State: DONE")
            se_return_function_terminate()
        end,
    })
    end)
end_tree()

-- ============================================================================
-- TEST TREE 8: Helper Functions - Dispatch
-- Tests: se_dispatch, se_field_dispatch, se_event_dispatch
-- ============================================================================

-- Command constants
local CMD_IDLE = 0
local CMD_START = 1
local CMD_STOP = 2
local CMD_RESET = 3

-- Event constants
local EVT_TIMER = 100
local EVT_BUTTON = 101
local EVT_SENSOR = 102

-- ============================================================================
-- TEST: Field Dispatch with different command values
-- ============================================================================

start_tree("test_field_dispatch_idle")
    use_record("test_blackboard")
    
    se_pipeline(function()
        -- Set command to IDLE
        local set = io_call("SET_STATE")
            field_ref("command")
            int(CMD_IDLE)
        end_call(set)
        
        se_field_dispatch("command", {
            { CMD_IDLE, function()
                se_log("✅ Command: IDLE")
                se_return_function_terminate()
            end },
            { CMD_START, function()
                se_log("❌ Command: START (wrong)")
                se_return_function_terminate()
            end },
            { CMD_STOP, function()
                se_log("❌ Command: STOP (wrong)")
                se_return_function_terminate()
            end },
            { CMD_RESET, function()
                se_log("❌ Command: RESET (wrong)")
                se_return_function_terminate()
            end },
        })
        
        
    end)
end_tree()

start_tree("test_field_dispatch_start")
    use_record("test_blackboard")
    
    se_pipeline(function()
        -- Set command to START
        local set = io_call("SET_STATE")
            field_ref("command")
            int(CMD_START)
        end_call(set)
        
        se_field_dispatch("command", {
            { CMD_IDLE, function()
                se_log("❌ Command: IDLE (wrong)")
                se_return_function_terminate()
            end },
            { CMD_START, function()
                se_log("✅ Command: START")
                se_return_function_terminate()
            end },
            { CMD_STOP, function()
                se_log("❌ Command: STOP (wrong)")
                se_return_function_terminate()
            end },
            { CMD_RESET, function()
                se_log("❌ Command: RESET (wrong)")
                se_return_function_terminate()
            end },
        })
        
        se_return_function_terminate()
    end)
end_tree()

start_tree("test_field_dispatch_stop")
    use_record("test_blackboard")
    
    se_pipeline(function()
        -- Set command to STOP
        local set = io_call("SET_STATE")
            field_ref("command")
            int(CMD_STOP)
        end_call(set)
        
        se_field_dispatch("command", {
            { CMD_IDLE, function()
                se_log("❌ Command: IDLE (wrong)")
                se_return_function_terminate()
            end },
            { CMD_START, function()
                se_log("❌ Command: START (wrong)")
                se_return_function_terminate()
            end },
            { CMD_STOP, function()
                se_log("✅ Command: STOP")
                se_return_function_terminate()
            end },
            { CMD_RESET, function()
                se_log("❌ Command: RESET (wrong)")
                se_return_function_terminate()
            end },
        })
        
        se_return_function_terminate()
    end)
end_tree()

-- ============================================================================
-- TEST: Event Dispatch with different events
-- ============================================================================

start_tree("test_event_dispatch")
    use_record("test_blackboard")
    
    se_pipeline(function()
        se_event_dispatch({
            { EVT_TIMER, function()
                se_log("✅ Event: TIMER")
                se_return_function_terminate()
            end },
            { EVT_BUTTON, function()
                se_log("✅ Event: BUTTON")
                se_return_function_terminate()
            end },
            { EVT_SENSOR, function()
                se_log("✅ Event: SENSOR")
                se_return_function_terminate()
            end },
        })
        
        se_return_function_terminate()
    end)
end_tree()


-- ============================================================================
-- TEST TREE 13: Basic List Structures
-- Tests: list_start/list_end for simple grouping
-- ============================================================================

start_tree("test_basic_lists")
    use_record("test_blackboard")
    p1 = se_pipeline(function()
    -- Function that takes a list of integers
    local fn1 = m_call("PROCESS_INT_LIST")
        l1 = list_start("l1")
            int(10)
            int(20)
            int(30)
            int(40)
            int(50)
        list_end(l1)
        se_return_continue()
    end_call(fn1)
    
    -- Function with multiple lists
    local fn2 = m_call("MULTI_LIST_FUNC")
        -- First list: coordinates
        l2 = list_start("l2")
            flt(1.0)
            flt(2.0)
            flt(3.0)
        list_end(l2)
        -- Second list: more coordinates
        l3 = list_start("l3")
            flt(4.0)
            flt(5.0)
            flt(6.0)
        list_end(l3)
        se_return_continue()
    end_call(fn2)
    
    -- Nested lists
    local fn3 = m_call("NESTED_LISTS")
        l4 = list_start("l4")  -- outer
            int(1)
            l5 = list_start("l5")  -- inner 1
                int(2)
                int(3)
            list_end(l5)
            int(4)
            l6 = list_start("l6")  -- inner 2
                int(5)
                int(6)
                int(7)
            list_end(l6)
            int(8)
        list_end(l4)
        se_return_continue()
    end_call(fn3)
    
    se_return_halt()
    end)
end_tree()


-- ============================================================================
-- TEST TREE 14: Dictionary Structures
-- Tests: dict_start/dict_end, key/key_hash for key-value lookups
-- ============================================================================

start_tree("test_dictionary_basic")
    use_record("test_blackboard")
    se_pipeline(function()
    se_set_hash("hash_state", "idle") 
    se_hash_dispatch("hash_state", {
        {"idle", function()
            se_pipeline(function()
                se_log("idle")
                se_set_hash("hash_state", "running")
                se_return_halt()
            end)
           
        end},
        {"running", function()
            se_pipeline(function()
                se_log("running")
                se_set_hash("hash_state", "idle")
                se_return_halt()
            end)
        end},
    }, SE_CONTINUE)
    
    se_return_halt()
    end)
end_tree()

-- ============================================================================
-- TEST TREE 15: Dictionary with Nested Actions
-- Tests: Dictionary values containing function calls
-- ============================================================================

start_tree("test_dictionary_with_actions")
    use_record("test_blackboard")
    local p0 = m_call("SE_PIPELINE")
    i0 = io_call("SE_SET_HASH")
        field_ref("hash_state")
        str_hash("start")
    end_call(i0)
    -- Dictionary where values are action sequences
    local dispatch = m_call("SE_NAMED_STATE_MACHINE")
        field_ref("hash_state")  -- dispatch key field
        local d2 = dict_start("d2")
            local k1=key("start")
                local p1 = m_call("SE_PIPELINE")
                    se_log("start state")
                    se_set_hash("hash_state", "stop")
                    se_return_halt()
                end_call(p1)
            key_end(k1)
            
            local k2=key("stop")
                local p2 = m_call("SE_PIPELINE")
                    se_log("stop state")
                    se_set_hash("hash_state", "pause")
                    se_return_continue()
                end_call(p2)
            key_end(k2)
            
            local k3=key("pause")
                local p3 = m_call("SE_PIPELINE")
                    se_log("pause state")
                    se_set_hash("hash_state", "reset")
                    se_return_continue()
                end_call(p3)
            key_end(k3)

            local k4=key("reset")
                local p4 = m_call("SE_PIPELINE")
                    se_log("reset state")
                    se_set_hash("hash_state", "start")
                    se_return_continue()
                end_call(p4)
            key_end(k4)
        dict_end(d2)
        se_return_continue()
    end_call(dispatch)
    
    se_return_continue()
end_tree()

-- ============================================================================
-- TEST TREE 16: Array Structures
-- Tests: array_start/array_end for indexed collections
-- ============================================================================

start_tree("test_array_basic")
    use_record("test_blackboard")
    use_defaults("blackboard_defaults")
    local root = m_call("SE_PIPELINE")
        -- Simple array of integers (index-based access)
        local fn1 = m_call("ARRAY_ACCESS")
           local a1 = array_start("a1")
                int(100)   -- index 0
                int(200)   -- index 1
                int(300)   -- index 2
                int(400)   -- index 3
                int(500)   -- index 4
            array_end(a1)
            int(2)  -- access index 2 -> should get 300
            se_return_continue()
        end_call(fn1)
        
        -- Array of field references
        local fn2 = m_call("FIELD_ARRAY")
           local a2 = array_start("a2")
                field_ref("state")
                field_ref("command")
                field_ref("counter")
                nested_field_ref("gains.kp")
                nested_field_ref("motor.position.x")
            array_end(a2)
            se_return_continue()
        end_call(fn2)
        
        -- Nested arrays (2D array)
        local fn3 = m_call("MATRIX_2D")
           local a3 = array_start("a3")  -- rows
               local a35 = array_start("a35")  -- row 0
                    flt(1.0) flt(0.0) flt(0.0)
                array_end(a35)
             local a4 =  array_start("a4")  -- row 1
                    flt(0.0) flt(1.0) flt(0.0)
                array_end(a4)
                local a5 = array_start("a5")  -- row 2
                    flt(0.0) flt(0.0) flt(1.0)
                array_end(a5)
            array_end(a3)
            se_return_continue()
        end_call(fn3)
        
        se_return_continue()
    end_call(root)
end_tree()

-- ============================================================================
-- TEST TREE 17: Tuple Structures
-- Tests: tuple_start/tuple_end for fixed-size heterogeneous collections
-- ============================================================================

start_tree("test_tuple_basic")
    use_record("test_blackboard")
    local p1 = m_call("SE_PIPELINE")
    -- Tuple: (string, int, float) - like a struct
    local fn1 = m_call("PROCESS_TUPLE")
        local t1 = tuple_start("t1")
            str_ptr("sensor_1")
            int(42)
            flt(3.14159)
        tuple_end(t1)
        se_return_continue()
    end_call(fn1)
    
    -- Multiple tuples (like a table of records)
    local fn2 = m_call("TUPLE_TABLE")
        local a1 = array_start("a1")
            local t1 = tuple_start("t1")
                str_ptr("temp_sensor")
                int(1)
                flt(25.5)
                uint(0x01)  -- flags
            tuple_end(t1)
            local t2 = tuple_start("t2")
                str_ptr("pressure_sensor")
                int(2)
                flt(101.3)
                uint(0x02)
            tuple_end(t2)
            local t3 = tuple_start("t3")
                str_ptr("humidity_sensor")
                int(3)
                flt(65.0)
                uint(0x04)
            tuple_end(t3)
        array_end(a1)
        se_return_continue()
    end_call(fn2)
    
    -- Tuple with nested structures
    local fn3 = m_call("COMPLEX_TUPLE")
       local t4 = tuple_start("t4")
            str_ptr("motor_config")
            -- nested dict for parameters
           local d1 = dict_start("d1")
                local k1=key("max_speed")
                    flt(1000.0)
                key_end(k1)
                local k2=key("acceleration")
                    flt(50.0)
                key_end(k2)
            dict_end(d1)
            -- nested array for limits
           local a2 = array_start("a2")
                flt(-100.0)  -- min
                flt(100.0)   -- max
            array_end(a2)
        tuple_end(t4)
        se_return_continue()
    end_call(fn3)
    
    se_return_continue()
    end_call(p1)
end_tree()

-- ============================================================================
-- TEST TREE 18: Named State Machine with Dictionary
-- Tests: Dictionary-based state machine dispatch
-- ============================================================================
--[[
start_tree("test_named_state_machine")
    use_record("test_blackboard")
    
    -- State machine using string state names in dictionary
    local sm = m_call("SE_NAMED_STATE_MACHINE")
        field_ref("state")  -- current state field (stores hash)
        local d = dict_start()
            local k1 = key("INIT")
                se_log("State: INIT")
                local set1 = o_call("SET_STATE_HASH")
                    field_ref("state")
                    str_hash("READY")  -- transition to READY
                end_call(set1)
                result(SE_HALT)
            key_end(k1)
            
            local k2 = key("READY")
                se_log("State: READY")
                local pred = p_call("CHECK_START_CONDITION")
                end_call(pred)
                local set2 = o_call("SET_STATE_HASH")
                    field_ref("state")
                    str_hash("RUNNING")
                end_call(set2)
                result(SE_HALT)
            key_end(k2)
            
            local k3 = key("RUNNING")
                se_log("State: RUNNING")
                se_tick_delay(10)
                local set3 = o_call("SET_STATE_HASH")
                    field_ref("state")
                    str_hash("COMPLETE")
                end_call(set3)
                result(SE_HALT)
            key_end(k3)
            
            local k4 = key("COMPLETE")
                se_log("State: COMPLETE")
                result(SE_FUNCTION_TERMINATE)
            key_end(k4)
            
            local k5 = key("ERROR")
                se_log("State: ERROR")
                result(SE_FUNCTION_TERMINATE)
            key_end(k5)
        dict_end(d)
    end_call(sm)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 19: Event Dispatch with Dictionary
-- Tests: Dictionary-based event routing
-- ============================================================================

start_tree("test_dict_event_dispatch")
    use_record("test_blackboard")
    
    -- Event handler dictionary
    local handler = m_call("SE_NAMED_EVENT_DISPATCH")
        local d = dict_start()
            local k1 = key("TIMER_TICK")
                se_log("Event: TIMER_TICK")
                local inc = o_call("INCREMENT_COUNTER")
                    field_ref("counter")
                end_call(inc)
            key_end(k1)
            
            local k2 = key("BUTTON_PRESS")
                se_log("Event: BUTTON_PRESS")
                local toggle = o_call("TOGGLE_ENABLED")
                    field_ref("enabled")
                end_call(toggle)
            key_end(k2)
            
            local k3 = key("SENSOR_TRIGGER")
                se_log("Event: SENSOR_TRIGGER")
                local read = o_call("READ_SENSOR")
                    field_ref("temperature")
                end_call(read)
            key_end(k3)
            
            local k4 = key("SHUTDOWN")
                se_log("Event: SHUTDOWN")
                result(SE_FUNCTION_TERMINATE)
            key_end(k4)
            
            local k5 = key("RESET")
                se_log("Event: RESET")
                result(SE_RESET)
            key_end(k5)
        dict_end(d)
    end_call(handler)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 20: Complex Nested Structures
-- Tests: Deeply nested combination of dict/array/tuple/list
-- ============================================================================

start_tree("test_complex_structures")
    use_record("test_blackboard")
    
    -- Configuration structure: dict containing arrays of tuples
    local config = m_call("LOAD_CONFIG")
        local d1 = dict_start()
            local k1 = key("sensors")
                local a1 = array_start()
                    local t1 = tuple_start()
                        str_ptr("temp_1")
                        int(0)
                        flt(0.0)
                        flt(100.0)
                    tuple_end(t1)
                    local t2 = tuple_start()
                        str_ptr("temp_2")
                        int(1)
                        flt(-40.0)
                        flt(125.0)
                    tuple_end(t2)
                array_end(a1)
            key_end(k1)
            
            local k2 = key("actuators")
                local a2 = array_start()
                    local t3 = tuple_start()
                        str_ptr("motor_1")
                        int(0)
                        local d2 = dict_start()
                            local km1 = key("min") flt(-100.0) key_end(km1)
                            local km2 = key("max") flt(100.0) key_end(km2)
                            local km3 = key("default") flt(0.0) key_end(km3)
                        dict_end(d2)
                    tuple_end(t3)
                    local t4 = tuple_start()
                        str_ptr("valve_1")
                        int(1)
                        local d3 = dict_start()
                            local kv1 = key("min") flt(0.0) key_end(kv1)
                            local kv2 = key("max") flt(1.0) key_end(kv2)
                            local kv3 = key("default") flt(0.0) key_end(kv3)
                        dict_end(d3)
                    tuple_end(t4)
                array_end(a2)
            key_end(k2)
            
            local k3 = key("timing")
                local d4 = dict_start()
                    local kt1 = key("tick_rate") int(100) key_end(kt1)
                    local kt2 = key("watchdog_ms") int(1000) key_end(kt2)
                    local kt3 = key("startup_delay") flt(0.5) key_end(kt3)
                dict_end(d4)
            key_end(k3)
            
            local k4 = key("flags")
                local l1 = list_start()
                    uint(0x01)  -- ENABLE_LOGGING
                    uint(0x02)  -- ENABLE_WATCHDOG
                    uint(0x04)  -- ENABLE_SAFETY
                    uint(0x08)  -- DEBUG_MODE
                list_end(l1)
            key_end(k4)
        dict_end(d1)
        result(SE_CONTINUE)
    end_call(config)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 21: Association List (alist) Style
-- Tests: Lisp-style alist using list with key-value pairs
-- ============================================================================

start_tree("test_alist_style")
    use_record("test_blackboard")
    
    -- Lisp-style association list: ((key1 . val1) (key2 . val2) ...)
    -- Using tuples inside a list to represent cons cells
    local fn = m_call("PROCESS_ALIST")
        local l = list_start()
            local t1 = tuple_start() str_hash("name") str_ptr("test_system") tuple_end(t1)
            local t2 = tuple_start() str_hash("version") int(1) tuple_end(t2)
            local t3 = tuple_start() str_hash("enabled") int(1) tuple_end(t3)
            local t4 = tuple_start() str_hash("timeout") flt(5.0) tuple_end(t4)
        list_end(l)
        result(SE_CONTINUE)
    end_call(fn)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 22: Property List (plist) Style
-- Tests: Alternating key-value pattern
-- ============================================================================

start_tree("test_plist_style")
    use_record("test_blackboard")
    
    -- Property list style: (key1 val1 key2 val2 ...)
    local fn = m_call("PROCESS_PLIST")
        local l = list_start()
            str_hash("name")    str_ptr("motor_controller")
            str_hash("channel") int(3)
            str_hash("gain")    flt(2.5)
            str_hash("enabled") int(1)
            str_hash("mode")    str_hash("auto")
        list_end(l)
        result(SE_CONTINUE)
    end_call(fn)
    
    result(SE_CONTINUE)
end_tree()

-- ============================================================================
-- TEST TREE 23: Dispatch Table with Mixed Structure Types
-- Tests: Using different structure types for different dispatch cases
-- ============================================================================

start_tree("test_mixed_dispatch")
    use_record("test_blackboard")
    
    local dispatch = m_call("MIXED_STRUCTURE_DISPATCH")
        field_ref("command")
        local d = dict_start()
            -- Command 0: Simple value
            local k0 = key_hash(0)
                int(0)
                str_ptr("idle")
            key_end(k0)
            
            -- Command 1: Array of actions
            local k1 = key_hash(1)
                local a = array_start()
                    local a1 = o_call("ACTION_1") end_call(a1)
                    local a2 = o_call("ACTION_2") end_call(a2)
                    local a3 = o_call("ACTION_3") end_call(a3)
                array_end(a)
            key_end(k1)
            
            -- Command 2: Tuple of parameters
            local k2 = key_hash(2)
                local t = tuple_start()
                    flt(1.0)
                    flt(2.0)
                    int(100)
                    uint(0xFF)
                tuple_end(t)
            key_end(k2)
            
            -- Command 3: Nested dict
            local k3 = key_hash(3)
                local d2 = dict_start()
                    local ka = key("sub_cmd_a")
                        local b1 = o_call("SUB_ACTION_A") end_call(b1)
                    key_end(ka)
                    local kb = key("sub_cmd_b")
                        local b2 = o_call("SUB_ACTION_B") end_call(b2)
                    key_end(kb)
                dict_end(d2)
            key_end(k3)
        dict_end(d)
        result(SE_CONTINUE)
    end_call(dispatch)
    
    result(SE_CONTINUE)
end_tree()

--]]
--========================================================================
-- FINALIZE MODULE
-- ============================================================================

local result = end_module(mod)
print("Module compiled successfully: " .. result.name)

print(M.write_debug_header(result))
return result