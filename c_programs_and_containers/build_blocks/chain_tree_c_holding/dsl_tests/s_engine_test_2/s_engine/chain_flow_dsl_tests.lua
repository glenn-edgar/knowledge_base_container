-- ============================================================================
-- BEGIN MODULE
-- ============================================================================
local mod = start_module("chain_flow_dsl_tests")
use_32bit()
-- ============================================================================
-- RECORD DEFINITIONS (Blackboard Schemas)
-- ============================================================================

-- Record for test 2 (no fields needed, but define for consistency)
RECORD("test2_blackboard")
    FIELD("placeholder", "int32")  -- unused but record can't be empty
END_RECORD()

-- Record for state machine tests
RECORD("state_machine_blackboard")
    FIELD("state", "int32")
    FIELD("state_b", "int32")
END_RECORD()

-- Record for robot command dispatch
RECORD("robot_blackboard")
    FIELD("command", "int32")
END_RECORD()

-- Record for event dispatch with multiple fields
RECORD("event_blackboard")
    FIELD("timer_count", "int32")
    FIELD("sensor_value", "int32")
    FIELD("event_id", "int32")
END_RECORD()

-- ============================================================================
-- TEST 2: Boolean logic with p_call
-- ============================================================================
local function pred_fn()
    local p1 = cfl_s_bit_or_start()
        local p2 = cfl_s_bit_and_start()
            cfl_bit_entry(0, 1)
        end_call(p2)
        local p3 = cfl_s_bit_and_start()
            cfl_bit_entry(2, 3)
        end_call(p3)
    end_call(p1)
end

start_tree("s_expression_test_2")
    -- Note: This test doesn't actually use any blackboard fields

    se_function_interface(function()
        cfl_i_disable_children()

        se_trigger_on_change(0, pred_fn, cfl_enable_children, cfl_disable_children)
        se_return_continue()
    end)

end_tree("s_expression_test_2")

-- ============================================================================
-- DISPATCH TEST: Event dispatch — listens for CFL_SECOND_EVENT
-- ============================================================================

local CFL_SECOND_EVENT = 5

local event_actions_fn = {}

event_actions_fn[1] = function()
    se_event_case(CFL_SECOND_EVENT, function()
        se_chain_flow(function()
            se_log("CFL_SECOND_EVENT received")
            se_log("event_actions_fn[1] terminated")
            se_return_pipeline_reset()
        end)
    end)
end

event_actions_fn[2] = function()
    se_event_case('default', function()
        se_chain_flow(function()
            se_return_pipeline_halt()
        end)
    end)
end

start_tree("dispatch_test")

    se_function_interface(function()
        se_event_dispatch(event_actions_fn)
    end)

end_tree("dispatch_test")

-- ============================================================================
-- TEST 4: State Machine with ChainTree child control
--
-- The s-engine state machine cycles through states, each state
-- disables all ChainTree children then enables one (or more).
-- This controls which ChainTree columns are active.
-- ============================================================================

local case_fn_4 = {}

case_fn_4[1] = function()
    se_case(0, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("State 0: enabling child 0")
                cfl_disable_children()
                cfl_enable_child(0)
                se_tick_delay(100)
                se_set_field("state_b", 1)
                se_log("State 0: terminated")
                se_return_pipeline_disable()
            end)
        end)
    end)
end

case_fn_4[2] = function()
    se_case(1, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("State 1: enabling child 1")
                cfl_disable_children()
                cfl_enable_child(1)
                se_tick_delay(100)
                se_set_field("state_b", 2)
                se_log("State 1: terminated")
                se_return_pipeline_disable()
            end)
        end)
    end)
end

case_fn_4[3] = function()
    se_case(2, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("State 2: enabling children 2 and 3")
                cfl_disable_children()
                cfl_enable_child(2)
                cfl_enable_child(3)
                se_tick_delay(100)
                se_log("State 2: terminated")
                se_return_function_terminate()
            end)
        end)
    end)
end

case_fn_4[4] = function()
    se_case('default', function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Default state: terminating")
                se_tick_delay(10)
                se_return_pipeline_disable()
            end)
        end)
    end)
end

start_tree("s_expression_test_4")
    use_record("state_machine_blackboard")

    se_function_interface(function()
        se_i_set_field("state_b", 0)
        se_log("State machine test started")
        se_fork_join(function()
            se_state_machine("state_b", case_fn_4)
        end)
        se_log("State machine test finished")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_4")

-- ============================================================================
-- ROBOT COMMAND CONSTANTS
-- ============================================================================

local MOTOR_LEFT  = 0
local MOTOR_RIGHT = 1

local CMD_IDLE    = 0
local CMD_FORWARD = 1
local CMD_BACK    = 2
local CMD_LEFT    = 3
local CMD_RIGHT   = 4
local CMD_STOP    = 5

-- ============================================================================
-- TEST 7: Command Dispatch (field-based)
-- ============================================================================

local cmd_cases = {}

cmd_cases[1] = function()
    se_case(CMD_FORWARD, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Moving forward")
                local c1 = o_call("TEST_31_SET_MOTOR") int(MOTOR_LEFT) int(100) end_call(c1)
                local c2 = o_call("TEST_31_SET_MOTOR") int(MOTOR_RIGHT) int(100) end_call(c2)
                se_tick_delay(50)
                se_set_field("command", CMD_BACK)
                se_return_pipeline_disable()
            end)
        end)
    end)
end

cmd_cases[2] = function()
    se_case(CMD_BACK, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Moving backward")
                local c1 = o_call("TEST_31_SET_MOTOR") int(MOTOR_LEFT) int(-100) end_call(c1)
                local c2 = o_call("TEST_31_SET_MOTOR") int(MOTOR_RIGHT) int(-100) end_call(c2)
                se_tick_delay(50)
                se_set_field("command", CMD_LEFT)
                se_return_pipeline_disable()
            end)
        end)
    end)
end

cmd_cases[3] = function()
    se_case(CMD_LEFT, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Turning left")
                local c1 = o_call("TEST_31_SET_MOTOR") int(MOTOR_LEFT) int(-50) end_call(c1)
                local c2 = o_call("TEST_31_SET_MOTOR") int(MOTOR_RIGHT) int(50) end_call(c2)
                se_tick_delay(25)
                se_set_field("command", CMD_RIGHT)
                se_return_pipeline_disable()
            end)
        end)
    end)
end

cmd_cases[4] = function()
    se_case(CMD_RIGHT, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Turning right")
                local c1 = o_call("TEST_31_SET_MOTOR") int(MOTOR_LEFT) int(50) end_call(c1)
                local c2 = o_call("TEST_31_SET_MOTOR") int(MOTOR_RIGHT) int(-50) end_call(c2)
                se_tick_delay(25)
                se_set_field("command", CMD_STOP)
                se_return_pipeline_disable()
            end)
        end)
    end)
end

cmd_cases[5] = function()
    se_case(CMD_STOP, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Stopping")
                local c1 = o_call("TEST_31_SET_MOTOR") int(MOTOR_LEFT) int(0) end_call(c1)
                local c2 = o_call("TEST_31_SET_MOTOR") int(MOTOR_RIGHT) int(0) end_call(c2)
                se_return_function_terminate()
            end)
        end)
    end)
end

cmd_cases[6] = function()
    se_case(CMD_IDLE, function()
        se_sequence(function()
            se_chain_flow(function()
                se_log("Idle - SHOULD NOT HAPPEN")
                se_return_function_terminate()
            end)
        end)
    end)
end

start_tree("s_expression_test_7")
    use_record("robot_blackboard")

    se_function_interface(function()
        se_i_set_field("command", CMD_FORWARD)
        se_field_dispatch("command", cmd_cases)
    end)

end_tree("s_expression_test_7")

-- ============================================================================
-- EVENT CONSTANTS
-- ============================================================================

local LED_STATUS  = 0
local LED_ALARM   = 1

local EVT_TICK      = 4
local EVT_TIMER     = 0xEE01
local EVT_BUTTON    = 0xEE02
local EVT_SENSOR    = 0xEE03
local EVT_ALARM     = 0xEE04
local EVT_SHUTDOWN  = 0xEE05

-- ============================================================================
-- TEST 8: Event Dispatch
-- ============================================================================

local evt_cases = {}

evt_cases[1] = function()
    se_event_case(EVT_TIMER, function()
        se_sequence(function()
        se_log("Timer expired")
            local proc = m_call("TEST_32_PROCESS_SCHEDULED_TASKS")
            end_call(proc)
            se_return_halt()
        end)

    end)
end

evt_cases[2] = function()
    se_event_case(EVT_BUTTON, function()
        se_sequence(function()
        se_log("Button pressed")
        se_wait_event(EVT_BUTTON, 10)
        local tog = o_call("TEST_32_TOGGLE_LED")
            int(LED_STATUS)
        end_call(tog)
        se_return_halt()
        end)
    end)
end

evt_cases[3] = function()
    se_event_case(EVT_SENSOR, function()
        se_sequence(function()
        se_log("Sensor reading")
        local chk = m_call("TEST_32_CHECK_THRESHOLD")
            field_ref("sensor_value")
            int(50)
        end_call(chk)
        se_return_halt()
        end)
    end)
end

evt_cases[4] = function()
    se_event_case(EVT_ALARM, function()
        se_sequence(function()
        se_log("ALARM TRIGGERED")
        local buzz = o_call("TEST_32_ENABLE_BUZZER")
        end_call(buzz)
        se_log("ALARM BUZZER ENABLED")
        local led = o_call("TEST_32_SET_LED")
            int(LED_ALARM)
            int(1)
        end_call(led)
        local notify = o_call("TEST_32_NOTIFY_SYSTEM")
           str("ALARM")
        end_call(notify)
        cfl_internal_event(EVT_SHUTDOWN, 1)
        se_log("ALARM SETTING")
        se_return_halt()
        end)
    end)
end

evt_cases[5] = function()
    se_event_case(EVT_SHUTDOWN, function()
        se_sequence(function()
        se_log("Shutdown requested")
        local dis = o_call("TEST_32_DISABLE_ALL_OUTPUTS")
        end_call(dis)
        local sav = o_call("TEST_32_SAVE_STATE")
        end_call(sav)
        se_return_function_terminate()
        end)
    end)
end

evt_cases[6] = function()
    se_event_case('default', function()
        se_sequence(function()
        local gen = pt_m_call("TEST_32_GENERATE_INTERNAL_EVENTS")
        end_call(gen)
        local bg = m_call("TEST_32_RUN_BACKGROUND_TASKS")
        end_call(bg)
        se_return_halt()
        end)
    end)
end

start_tree("s_expression_test_8")
    use_record("event_blackboard")

    se_function_interface(function()
        se_event_dispatch(evt_cases)
    end)

end_tree("s_expression_test_8")


-- ============================================================================
-- RECORD DEFINITIONS for trees 10-16
-- ============================================================================

-- Inner record: 3D Vector
RECORD("vector3d")
    FIELD("x", "float")
    FIELD("y", "float")
    FIELD("z", "float")
END_RECORD()

-- Inner record: PID gains
RECORD("pid_gains")
    FIELD("kp", "float")
    FIELD("ki", "float")
    FIELD("kd", "float")
END_RECORD()

-- Mid-level record: Motor state (contains vector3d)
RECORD("motor_state")
    FIELD("position", "vector3d")
    FIELD("velocity", "vector3d")
    FIELD("torque", "float")
    FIELD("enabled", "int32")
END_RECORD()

-- Top-level record: Complete system state
RECORD("system_state")
    FIELD("motor", "motor_state")
    FIELD("pid", "pid_gains")
    FIELD("system_time", "uint32")
    FIELD("error_code", "uint32")
END_RECORD()

-- ============================================================================
-- TEST 10: Nested field access — SET using builtins, READ using user fns
-- ============================================================================

start_tree("s_expression_test_10")
    use_record("system_state")

    se_function_interface(function()
        se_log("Setting motor position")
        -- Set motor.position (3 levels deep) using SE_SET_FIELD_FLOAT
        local c1 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("motor.position.x") flt(100.0) end_call(c1)
        local c2 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("motor.position.y") flt(200.0) end_call(c2)
        local c3 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("motor.position.z") flt(300.0) end_call(c3)

        -- Set motor.velocity
        local c4 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("motor.velocity.x") flt(1.5) end_call(c4)
        local c5 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("motor.velocity.y") flt(2.5) end_call(c5)
        local c6 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("motor.velocity.z") flt(3.5) end_call(c6)

        -- Set PID gains (2 levels deep)
        local c7 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("pid.kp") flt(1.0) end_call(c7)
        local c8 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("pid.ki") flt(0.1) end_call(c8)
        local c9 = o_call("SE_SET_FIELD_FLOAT") nested_field_ref("pid.kd") flt(0.01) end_call(c9)

        -- Set top-level fields using SE_SET_FIELD (int)
        se_set_field("system_time", 12345678)
        se_set_field("error_code", 0)

        se_log("Reading and verifying")
        -- Verify using user READ functions
        local c12 = o_call("TEST_33_READ_VECTOR")
            nested_field_ref("motor.position.x") nested_field_ref("motor.position.y") nested_field_ref("motor.position.z")
            flt(100.0) flt(200.0) flt(300.0)
        end_call(c12)
        local c13 = o_call("TEST_33_READ_VECTOR")
            nested_field_ref("motor.velocity.x") nested_field_ref("motor.velocity.y") nested_field_ref("motor.velocity.z")
            flt(1.5) flt(2.5) flt(3.5)
        end_call(c13)
        local c14 = o_call("TEST_33_READ_PID")
            nested_field_ref("pid.kp") nested_field_ref("pid.ki") nested_field_ref("pid.kd")
            flt(1.0) flt(0.1) flt(0.01)
        end_call(c14)
        local c15 = o_call("TEST_33_READ_SYSTEM")
            field_ref("system_time") field_ref("error_code")
            uint(12345678) uint(0)
        end_call(c15)

        se_return_function_terminate()
    end)

end_tree("s_expression_test_10")

-- ============================================================================
-- RECORD DEFINITIONS WITH POINTER FIELDS
-- ============================================================================

RECORD("node_data")
    FIELD("id", "uint32")
    FIELD("value", "float")
    FIELD("flags", "uint32")
END_RECORD()

RECORD("list_node")
    FIELD("data", "int32")
    PTR64_FIELD("next", "list_node")
END_RECORD()

RECORD("sensor_reading")
    FIELD("timestamp", "uint32")
    FIELD("temperature", "float")
    FIELD("pressure", "float")
    FIELD("humidity", "float")
END_RECORD()

RECORD("system_context")
    FIELD("system_id", "uint32")
    PTR64_FIELD("primary_node", "node_data")
    PTR64_FIELD("backup_node", "node_data")
    PTR64_FIELD("sensor", "sensor_reading")
    PTR64_FIELD("task_list", "list_node")
    FIELD("node_count", "uint32")
END_RECORD()

-- ============================================================================
-- TEST 11: Pointer field set/get (uses SE_SET_FIELD for system_id, node_count)
-- ============================================================================

start_tree("s_expression_test_11")
    use_record("system_context")

    se_function_interface(function()
        se_log("Test 34: Pointer field test starting")

        -- Set system_id using builtin
        se_set_field("system_id", 0x12345678)

        -- Allocate nodes (user functions — need malloc)
        local c2 = o_call("TEST_34_ALLOC_NODE") field_ref("primary_node") uint(100) flt(3.14159) uint(0x0F) end_call(c2)
        local c3 = o_call("TEST_34_ALLOC_NODE") field_ref("backup_node") uint(200) flt(2.71828) uint(0xF0) end_call(c3)
        local c4 = o_call("TEST_34_ALLOC_SENSOR") field_ref("sensor") uint(1000000) flt(25.5) flt(1013.25) flt(65.0) end_call(c4)

        -- Set node_count using builtin
        se_set_field("node_count", 2)

        se_log("Verifying pointer fields...")
        local c6 = o_call("TEST_34_READ_NODE") field_ref("primary_node") uint(100) flt(3.14159) uint(0x0F) end_call(c6)
        local c7 = o_call("TEST_34_READ_NODE") field_ref("backup_node") uint(200) flt(2.71828) uint(0xF0) end_call(c7)
        local c8 = o_call("TEST_34_READ_SENSOR") field_ref("sensor") uint(1000000) flt(25.5) flt(1013.25) flt(65.0) end_call(c8)
        local c9 = o_call("TEST_34_READ_UINT32") field_ref("system_id") uint(0x12345678) end_call(c9)
        local c10 = o_call("TEST_34_READ_UINT16") field_ref("node_count") uint(2) end_call(c10)

        se_log("Testing NULL pointer handling...")
        local c11 = o_call("TEST_34_CHECK_NULL") field_ref("task_list") uint(1) end_call(c11)
        local c12 = o_call("TEST_34_CHECK_NULL") field_ref("primary_node") uint(0) end_call(c12)

        se_log("Freeing allocated nodes...")
        local c13 = o_call("TEST_34_FREE_PTR") field_ref("primary_node") end_call(c13)
        local c14 = o_call("TEST_34_FREE_PTR") field_ref("backup_node") end_call(c14)
        local c15 = o_call("TEST_34_FREE_PTR") field_ref("sensor") end_call(c15)

        se_log("Test 34: PASSED")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_11")

-- ============================================================================
-- TEST 12: Linked list with pointer fields
-- ============================================================================

start_tree("s_expression_test_12")
    use_record("system_context")

    se_function_interface(function()
        se_log("Test 35: Linked list test starting")
        local c1 = o_call("TEST_35_BUILD_LIST") field_ref("task_list") uint(3) end_call(c1)
        local c2 = o_call("TEST_35_TRAVERSE_LIST") field_ref("task_list") uint(3) end_call(c2)
        local c3 = o_call("TEST_35_FREE_LIST") field_ref("task_list") end_call(c3)
        local c4 = o_call("TEST_34_CHECK_NULL") field_ref("task_list") uint(1) end_call(c4)
        se_log("Test 35: PASSED")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_12")

-- ============================================================================
-- TEST 13: Pointer sharing
-- ============================================================================

start_tree("s_expression_test_13")
    use_record("system_context")

    se_function_interface(function()
        se_log("Test 36: Pointer sharing test starting")
        local c1 = o_call("TEST_34_ALLOC_NODE") field_ref("primary_node") uint(999) flt(42.0) uint(0xAB) end_call(c1)
        local c2 = o_call("TEST_36_COPY_PTR") field_ref("backup_node") field_ref("primary_node") end_call(c2)
        local c3 = o_call("TEST_36_VERIFY_SAME_PTR") field_ref("primary_node") field_ref("backup_node") end_call(c3)
        local c4 = o_call("TEST_36_MODIFY_NODE_VALUE") field_ref("backup_node") flt(99.99) end_call(c4)
        local c5 = o_call("TEST_34_READ_NODE") field_ref("primary_node") uint(999) flt(99.99) uint(0xAB) end_call(c5)
        local c6 = o_call("TEST_34_FREE_PTR") field_ref("primary_node") end_call(c6)
        local c7 = o_call("TEST_36_CLEAR_PTR") field_ref("backup_node") end_call(c7)
        se_log("Test 36: PASSED")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_13")

-- ============================================================================
-- RECORD DEFINITIONS for test 14
-- ============================================================================

RECORD("network_config_a")
    FIELD("ip_addr", "uint32")
    FIELD("port", "uint32")
    FIELD("timeout_ms", "uint32")
END_RECORD()

RECORD("sensor_data_a")
    FIELD("temperature", "float")
    FIELD("pressure", "float")
    FIELD("humidity", "float")
    FIELD("timestamp", "uint32")
END_RECORD()

RECORD("device_info_a")
    CHAR_ARRAY("name", 32)
    CHAR_ARRAY("serial", 16)
    FIELD("version", "uint32")
    FIELD("enabled", "int32")
END_RECORD()

RECORD("system_state_a")
    FIELD("network", "network_config_a")
    FIELD("sensors", "sensor_data_a")
    FIELD("device", "device_info_a")
    FIELD("error_code", "uint32")
    FIELD("run_count", "uint32")
    PTR64_FIELD("device_ptr","char")
END_RECORD()

-- ============================================================================
-- TEST 14: Static buffer copy + JSON reads
-- ============================================================================

start_tree("s_expression_test_14")
    use_record("system_state_a")

    se_function_interface(function()
        se_log("Test 37: Static buffer copy starting")
        local c1 = o_call("TEST_37_COPY_STATIC_NETWORK")
            nested_field_ref("network.ip_addr") nested_field_ref("network.port") nested_field_ref("network.timeout_ms")
        end_call(c1)
        local c2 = o_call("TEST_37_VERIFY_NETWORK")
            nested_field_ref("network.ip_addr") nested_field_ref("network.port") nested_field_ref("network.timeout_ms")
            uint(0xC0A80001) uint(8080) uint(5000)
        end_call(c2)
        se_log("Static buffer copy verified")

        -- JSON reads using CFL bridge oneshots
        se_log("Test 37: JSON sensor reads")
        cfl_json_read_float("sensors.temperature", "node_dict.column_data.user_data.sensors.temperature")
        cfl_json_read_float("sensors.pressure", "node_dict.column_data.user_data.sensors.pressure")
        cfl_json_read_float("sensors.humidity", "node_dict.column_data.user_data.sensors.humidity")
        cfl_json_read_uint("sensors.timestamp", "node_dict.column_data.user_data.sensors.timestamp")
        local c3 = o_call("TEST_37_VERIFY_SENSORS")
            nested_field_ref("sensors.temperature") nested_field_ref("sensors.pressure")
            nested_field_ref("sensors.humidity") nested_field_ref("sensors.timestamp")
            flt(25.5) flt(1013.25) flt(65.0) uint(1000000)
        end_call(c3)
        se_log("JSON sensor reads verified")

        se_log("Test 37: JSON device reads")
        cfl_json_read_string_buf("device.name", "node_dict.column_data.user_data.device.name")
        cfl_json_read_string_buf("device.serial", "node_dict.column_data.user_data.device.serial")
        cfl_json_read_uint("device.version", "node_dict.column_data.user_data.device.version")
        cfl_json_read_bool("device.enabled", "node_dict.column_data.user_data.device.enabled")
        local c4 = o_call("TEST_37_VERIFY_DEVICE_NAME") nested_field_ref("device.name") str_ptr("TestDevice") end_call(c4)
        local c5 = o_call("TEST_37_VERIFY_DEVICE_SERIAL") nested_field_ref("device.serial") str_ptr("SN12345") end_call(c5)
        local c6 = o_call("TEST_37_VERIFY_DEVICE_INFO")
            nested_field_ref("device.version") nested_field_ref("device.enabled")
            uint(0x0102) uint(1)
        end_call(c6)
        se_log("JSON device reads verified")

        se_log("Test 37: JSON top-level reads")
        cfl_json_read_uint("error_code", "node_dict.column_data.user_data.error_code")
        cfl_json_read_uint("run_count", "node_dict.column_data.user_data.run_count")
        local c7 = o_call("TEST_37_VERIFY_TOP_LEVEL")
            field_ref("error_code") field_ref("run_count") uint(0) uint(42)
        end_call(c7)
        se_log("JSON top-level reads verified")

        local c8 = o_call("TEST_37_DUMP_STATE")
            field_ref("network") field_ref("sensors") field_ref("device")
            field_ref("error_code") field_ref("run_count")
        end_call(c8)

        se_log("Test 37: String pointer read")
        cfl_json_read_string_ptr("device_ptr", "node_dict.column_data.user_data.device.name")
        local c9 = o_call("TEST_37_VERIFY_STRING_PTR") field_ref("device_ptr") str_ptr("TestDevice") end_call(c9)

        se_log("Test 37: PASSED")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_14")

-- ============================================================================
-- TEST 15: Constant record copy
-- ============================================================================

RECORD("pid_gains_a")
    FIELD("kp", "float")
    FIELD("ki", "float")
    FIELD("kd", "float")
END_RECORD()

RECORD("test38_bb")
    FIELD("gains", "pid_gains_a")
    FIELD("verified", "int32")
END_RECORD()

CONST("test_pid", "pid_gains_a")
    VALUE("kp", 2.5)
    VALUE("ki", 0.5)
    VALUE("kd", 0.1)
END_CONST()

CONST("test38_defaults", "test38_bb")
    VALUE("gains.kp", 1.0)
    VALUE("gains.ki", 0.2)
    VALUE("gains.kd", 0.05)
    VALUE("verified", 0)
END_CONST()

start_tree("s_expression_test_15")
    use_record("test38_bb")

    se_function_interface(function()
        -- Copy full blackboard from ROM using CFL bridge
        cfl_copy_const_full("test38_defaults")
        local c1 = o_call("TEST_38_VERIFY_DEFAULTS") field_ref("gains") end_call(c1)

        -- Copy just gains field from different constant using CFL bridge
        cfl_copy_const("gains", "test_pid")
        local c2 = o_call("TEST_38_VERIFY_TEST_PID") field_ref("gains") end_call(c2)

        se_log("Test 38: PASS")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_15")

-- ============================================================================
-- TEST 16: Pointer field with external init
-- ============================================================================

RECORD("pid_gains_c")
    FIELD("kp", "float")
    FIELD("ki", "float")
    FIELD("kd", "float")
END_RECORD()

RECORD("test39_bb")
    FIELD("gains", "pid_gains_c")
    PTR64_FIELD("gains_ptr","pid_gains_c")
END_RECORD()

start_tree("s_expression_test_16")
    use_record("test39_bb")

    se_function_interface(function()
        local c1 = o_call("TEST_39_VERIFY_GAINS") field_ref("gains") end_call(c1)
        local c2 = o_call("TEST_39_VERIFY_POINTER") field_ref("gains") end_call(c2)
        se_log("Test 39: PASS")
        se_return_function_terminate()
    end)

end_tree("s_expression_test_16")

-- ============================================================================
-- END MODULE
-- ============================================================================

return_value = end_module(mod)
return return_value
