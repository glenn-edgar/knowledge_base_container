
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
            cfl_bit_entry(0,1)
        end_call(p2)
        local p3 = cfl_s_bit_and_start()
            cfl_bit_entry(2, 3)
        end_call(p3)
    end_call(p1)
end

start_tree("s_expression_test_2")
    -- Note: This test doesn't actually use any blackboard fields
    
    local c1 = io_call("CFL_DISABLE_CHILDREN")
    end_call(c1)
    
    cfl_trigger_on_change(0, pred_fn, cfl_enable_children, cfl_disable_children)
    result(SE_CONTINUE)
     

end_tree("s_expression_test_2")



-- ============================================================================
-- STATE MACHINE CONSTANTS
-- ============================================================================

local SM_STATE_1 = 1
local SM_STATE_2 = 2
local SM_STATE_3 = 3

-- ============================================================================
-- TEST 4: State Machine
-- ============================================================================

start_tree("s_expression_test_4")
    use_record("state_machine_blackboard")

    -- Initialize state to 0
    local init = io_call("TEST_30_SET_STATE")
        field_ref("state_b")
        int(0)
    end_call(init)
    
    -- State machine using helper
    cfl_state_machine("state_b", {
        -- State 0
        function()
            cfl_disable_children()
            cfl_enable_child(0)
            cfl_tick_delay(100)
            local t1 = o_call("TEST_30_SET_STATE")
                field_ref("state_b")
                int(1)
            end_call(t1)
            result(SE_HALT)
        end,
        
        -- State 1
        function()
            cfl_disable_children()
            cfl_enable_child(1)
            cfl_tick_delay(100)
            local t2 = o_call("TEST_30_SET_STATE")
                field_ref("state_b")
                int(2)
            end_call(t2)
            result(SE_HALT)
        end,
        
        -- State 2
        function()
            cfl_disable_children()
            cfl_enable_child(2)
            cfl_enable_child(3)
            cfl_tick_delay(100)
            result(SE_FUNCTION_TERMINATE)
        end,
    })

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
-- Helper for user motor function
-- ============================================================================

local function set_motor(motor_id, speed)
    local c = o_call("TEST_31_SET_MOTOR")
        int(motor_id)
        int(speed)
    end_call(c)
end

local function set_command(cmd)
    local c = o_call("TEST_31_SET_STATE")
        field_ref("command")
        int(cmd)
    end_call(c)
end

-- ============================================================================
-- TEST 7: Command Dispatch (field-based)
-- ============================================================================

start_tree("s_expression_test_7")
    use_record("robot_blackboard")

    -- Initialize state
    local init = io_call("TEST_31_SET_STATE")
        field_ref("command")
        int(CMD_FORWARD)
    end_call(init)
    
    -- Dispatch on command field
    cfl_field_dispatch("command", {
        -- CMD_FORWARD (1)
        { CMD_FORWARD, function()
            cfl_log("Moving forward")
            set_motor(MOTOR_LEFT, 100)
            set_motor(MOTOR_RIGHT, 100)
            cfl_tick_delay(50)
            set_command(CMD_BACK)
            result(SE_HALT)
        end },
        
        -- CMD_BACK (2)
        { CMD_BACK, function()
            cfl_log("Moving backward")
            set_motor(MOTOR_LEFT, -100)
            set_motor(MOTOR_RIGHT, -100)
            cfl_tick_delay(50)
            set_command(CMD_LEFT)
            result(SE_HALT)
        end },
        
        -- CMD_LEFT (3)
        { CMD_LEFT, function()
            cfl_log("Turning left")
            set_motor(MOTOR_LEFT, -50)
            set_motor(MOTOR_RIGHT, 50)
            cfl_tick_delay(25)
            set_command(CMD_RIGHT)
            result(SE_HALT)
        end },
        
        -- CMD_RIGHT (4)
        { CMD_RIGHT, function()
            cfl_log("Turning right")
            set_motor(MOTOR_LEFT, 50)
            set_motor(MOTOR_RIGHT, -50)
            cfl_tick_delay(25)
            set_command(CMD_STOP)
            result(SE_HALT)
        end },
        
        -- CMD_STOP (5)
        { CMD_STOP, function()
            cfl_log("Stopping")
            set_motor(MOTOR_LEFT, 0)
            set_motor(MOTOR_RIGHT, 0)
            result(SE_FUNCTION_TERMINATE)
        end },
        
        -- CMD_IDLE (0) - default
        { CMD_IDLE, function()
            cfl_log("Idle - SHOULD NOT HAPPEN")
            result(SE_FUNCTION_TERMINATE)
        end },
    })

end_tree("s_expression_test_7")


-- ============================================================================
-- TEST 8: Event Dispatch
-- ============================================================================

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

start_tree("s_expression_test_8")
    use_record("event_blackboard")

    cfl_event_dispatch({
        -- EVT_TIMER (0xEE01)
        { EVT_TIMER, function()
            cfl_log("Timer expired")
            local proc = m_call("TEST_32_PROCESS_SCHEDULED_TASKS")
            end_call(proc)
            result(SE_HALT)
        end },
        
        -- EVT_BUTTON (0xEE02)
        { EVT_BUTTON, function()
            cfl_log("Button pressed")
            cfl_wait_event(EVT_BUTTON, 10)
            local tog = o_call("TEST_32_TOGGLE_LED")
                int(LED_STATUS)
            end_call(tog)
            result(SE_HALT)
        end },
        
        -- EVT_SENSOR (0xEE03)
        { EVT_SENSOR, function()
            cfl_log("Sensor reading")
            local chk = m_call("TEST_32_CHECK_THRESHOLD")
                field_ref("sensor_value")
                int(50)
            end_call(chk)
            result(SE_HALT)
        end },
        
        -- EVT_ALARM (0xEE04)
        { EVT_ALARM, function()
            cfl_log("ALARM TRIGGERED")
            local buzz = o_call("TEST_32_ENABLE_BUZZER")
            end_call(buzz)
            local led = o_call("TEST_32_SET_LED")
                int(LED_ALARM)
                int(1)
            end_call(led)
            local notify = o_call("TEST_32_NOTIFY_SYSTEM")
                str("ALARM")
            end_call(notify)
            cfl_internal_event(EVT_SHUTDOWN, 1)
            cfl_log("ALARM SETTING")
            result(SE_HALT)
        end },
        
        -- EVT_SHUTDOWN (0xEE05)
        { EVT_SHUTDOWN, function()
            cfl_log("Shutdown requested")
            local dis = o_call("TEST_32_DISABLE_ALL_OUTPUTS")
            end_call(dis)
            local sav = o_call("TEST_32_SAVE_STATE")
            end_call(sav)
            result(SE_FUNCTION_TERMINATE)
        end },
        
        -- Default (EVT_TICK)
        { EVT_TICK, function()
            local gen = pt_m_call("TEST_32_GENERATE_INTERNAL_EVENTS")
            end_call(gen)
            local bg = m_call("TEST_32_RUN_BACKGROUND_TASKS")
            end_call(bg)
            result(SE_CONTINUE)
        end },
    })

end_tree("s_expression_test_8")

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
    FIELD("position", "vector3d")      -- embedded vector3d
    FIELD("velocity", "vector3d")      -- embedded vector3d
    FIELD("torque", "float")
    FIELD("enabled", "bool")
END_RECORD()

-- Top-level record: Complete system state
RECORD("system_state")
    FIELD("motor", "motor_state")      -- embedded motor_state
    FIELD("pid", "pid_gains")          -- embedded pid_gains
    FIELD("system_time", "uint32")
    FIELD("error_code", "uint16")
END_RECORD()

-- ============================================================================
-- TREE 1: Test nested field access
-- ============================================================================

start_tree("s_expression_test_10")
    use_record("system_state")
    local p1 = m_call("CFL_PIPELINE")
    -- Initialize motor position (3 levels deep: motor.position.x)
        local c1 = o_call("CFL_LOG")
            str_ptr("Setting motor position")
        end_call(c1)
        
        local c2 = o_call("TEST_33_SET_VECTOR")
            nested_field_ref("motor.position.x")
            nested_field_ref("motor.position.y")
            nested_field_ref("motor.position.z")
            flt(100.0)
            flt(200.0)
            flt(300.0)
        end_call(c2)
        
        -- Initialize motor velocity
        local c3 = o_call("TEST_33_SET_VECTOR")
            nested_field_ref("motor.velocity.x")
            nested_field_ref("motor.velocity.y")
            nested_field_ref("motor.velocity.z")
            flt(1.5)
            flt(2.5)
            flt(3.5)
        end_call(c3)
        
        -- Set PID gains (2 levels deep: pid.kp)
        local c4 = o_call("TEST_33_SET_PID")
            nested_field_ref("pid.kp")
            nested_field_ref("pid.ki")
            nested_field_ref("pid.kd")
            flt(1.0)
            flt(0.1)
            flt(0.01)
        end_call(c4)
        
        -- Set top-level fields (no nesting)
        local c5 = o_call("TEST_33_SET_SYSTEM")
            field_ref("system_time")
            field_ref("error_code")
            uint(12345678)
            uint(0)
        end_call(c5)

        local c6 = o_call("CFL_LOG")
        str_ptr("Reading motor position")
    end_call(c6)
    
    local c7 = o_call("TEST_33_READ_VECTOR")
        nested_field_ref("motor.position.x")
        nested_field_ref("motor.position.y")
        nested_field_ref("motor.position.z")
        flt(100.0)
        flt(200.0)
        flt(300.0)
    end_call(c7)
    
    -- Initialize motor velocity
    local c8 = o_call("TEST_33_READ_VECTOR")
        nested_field_ref("motor.velocity.x")
        nested_field_ref("motor.velocity.y")
        nested_field_ref("motor.velocity.z")
        flt(1.5)
        flt(2.5)
        flt(3.5)
    end_call(c8)
    
    -- Set PID gains (2 levels deep: pid.kp)
    local c9 = o_call("TEST_33_READ_PID")
        nested_field_ref("pid.kp")
        nested_field_ref("pid.ki")
        nested_field_ref("pid.kd")
        flt(1.0)
        flt(0.1)
        flt(0.01)
    end_call(c9)
    
    -- Set top-level fields (no nesting)
    local c10 = o_call("TEST_33_READ_SYSTEM")
        field_ref("system_time")
        field_ref("error_code")
        uint(12345678)
        uint(0)
    end_call(c10)
        result(SE_FUNCTION_TERMINATE)
    end_call(p1)
end_tree("s_expression_test_10")
-- ============================================================================
-- test_ptr_field.lua
-- DSL test demonstrating PTR_FIELD (pointer to record) usage
-- ============================================================================


-- ============================================================================
-- RECORD DEFINITIONS WITH POINTER FIELDS
-- ============================================================================

-- Target record: Node data (what we point to)
RECORD("node_data")
    FIELD("id", "uint32")
    FIELD("value", "float")
    FIELD("flags", "uint8")
END_RECORD()

-- Target record: Linked list node
RECORD("list_node")
    FIELD("data", "int32")
    PTR_FIELD("next", "list_node")  -- pointer to next node (self-referential)
END_RECORD()

-- Target record: Sensor reading
RECORD("sensor_reading")
    FIELD("timestamp", "uint32")
    FIELD("temperature", "float")
    FIELD("pressure", "float")
    FIELD("humidity", "float")
END_RECORD()

-- Main record with pointer fields
RECORD("system_context")
    FIELD("system_id", "uint32")
    PTR_FIELD("primary_node", "node_data")      -- pointer to node_data
    PTR_FIELD("backup_node", "node_data")       -- another pointer to node_data
    PTR_FIELD("sensor", "sensor_reading")       -- pointer to sensor_reading
    PTR_FIELD("task_list", "list_node")         -- pointer to linked list head
    FIELD("node_count", "uint16")
END_RECORD()

-- ============================================================================
-- TREE 1: Test pointer field set/get
-- ============================================================================

start_tree("s_expression_test_11")
    use_record("system_context")
    p1 = m_call("CFL_PIPELINE")
        -- Log start
        local c1 = o_call("CFL_LOG")
            str_ptr("Test 34: Pointer field test starting")
        end_call(c1)
        
        -- Set system_id first
        local c2 = o_call("TEST_34_SET_UINT32")
            field_ref("system_id")
            uint(0x12345678)
        end_call(c2)
        
        -- Allocate and set primary_node
        local c3 = o_call("TEST_34_ALLOC_NODE")
            field_ref("primary_node")       -- pointer field to set
            uint(100)                        -- id
            flt(3.14159)                     -- value
            uint(0x0F)                       -- flags
        end_call(c3)
        
        -- Allocate and set backup_node
        local c4 = o_call("TEST_34_ALLOC_NODE")
            field_ref("backup_node")
            uint(200)
            flt(2.71828)
            uint(0xF0)
        end_call(c4)
        
        -- Allocate and set sensor reading
        local c5 = o_call("TEST_34_ALLOC_SENSOR")
            field_ref("sensor")
            uint(1000000)                    -- timestamp
            flt(25.5)                        -- temperature
            flt(1013.25)                     -- pressure
            flt(65.0)                        -- humidity
        end_call(c5)
        
        -- Set node_count
        local c6 = o_call("TEST_34_SET_UINT16")
            field_ref("node_count")
            uint(2)
        end_call(c6)
        
        -- Now verify everything
        local c7 = o_call("CFL_LOG")
            str_ptr("Verifying pointer fields...")
        end_call(c7)
        
        -- Read and verify primary_node
        local c8 = o_call("TEST_34_READ_NODE")
            field_ref("primary_node")
            uint(100)                        -- expected id
            flt(3.14159)                     -- expected value
            uint(0x0F)                       -- expected flags
        end_call(c8)
        
        -- Read and verify backup_node
        local c9 = o_call("TEST_34_READ_NODE")
            field_ref("backup_node")
            uint(200)
            flt(2.71828)
            uint(0xF0)
        end_call(c9)
        
        -- Read and verify sensor
        local c10 = o_call("TEST_34_READ_SENSOR")
            field_ref("sensor")
            uint(1000000)
            flt(25.5)
            flt(1013.25)
            flt(65.0)
        end_call(c10)
        
        -- Verify system_id and node_count
        local c11 = o_call("TEST_34_READ_UINT32")
            field_ref("system_id")
            uint(0x12345678)
        end_call(c11)
        
        local c12 = o_call("TEST_34_READ_UINT16")
            field_ref("node_count")
            uint(2)
        end_call(c12)
        
        -- Test NULL pointer check
        local c13 = o_call("CFL_LOG")
            str_ptr("Testing NULL pointer handling...")
        end_call(c13)
        
        -- task_list should be NULL (not set)
        local c14 = o_call("TEST_34_CHECK_NULL")
            field_ref("task_list")
            uint(1)                          -- expect NULL (1=true)
        end_call(c14)
        
        -- primary_node should NOT be NULL
        local c15 = o_call("TEST_34_CHECK_NULL")
            field_ref("primary_node")
            uint(0)                          -- expect NOT NULL (0=false)
        end_call(c15)
        
        -- Free allocated memory
        local c16 = o_call("CFL_LOG")
            str_ptr("Freeing allocated nodes...")
        end_call(c16)
        
        local c17 = o_call("TEST_34_FREE_PTR")
            field_ref("primary_node")
        end_call(c17)
        
        local c18 = o_call("TEST_34_FREE_PTR")
            field_ref("backup_node")
        end_call(c18)
        
        local c19 = o_call("TEST_34_FREE_PTR")
            field_ref("sensor")
        end_call(c19)
        
        local c20 = o_call("CFL_LOG")
            str_ptr("Test 34: PASSED")
        end_call(c20)
        result(SE_FUNCTION_TERMINATE)
    end_call(p1)
end_tree("s_expression_test_11")

-- ============================================================================
-- TREE 2: Test linked list with pointer fields
-- ============================================================================

start_tree("s_expression_test_12")
    use_record("system_context")
    p1 = m_call("CFL_PIPELINE")
        local c1 = o_call("CFL_LOG")
            str_ptr("Test 35: Linked list test starting")
        end_call(c1)
        
        -- Build a linked list: head -> node1 -> node2 -> NULL
        local c2 = o_call("TEST_35_BUILD_LIST")
            field_ref("task_list")           -- pointer to head
            uint(3)                          -- number of nodes to create
        end_call(c2)
        
        -- Traverse and verify the list
        local c3 = o_call("TEST_35_TRAVERSE_LIST")
            field_ref("task_list")
            uint(3)                          -- expected node count
        end_call(c3)
        
        -- Free the entire list
        local c4 = o_call("TEST_35_FREE_LIST")
            field_ref("task_list")
        end_call(c4)
        
        -- Verify list is now NULL
        local c5 = o_call("TEST_34_CHECK_NULL")
            field_ref("task_list")
            uint(1)                          -- expect NULL
        end_call(c5)
        
        local c6 = o_call("CFL_LOG")
            str_ptr("Test 35: PASSED")
        end_call(c6)
        result(SE_FUNCTION_TERMINATE)
    end_call(p1)
end_tree("s_expression_test_12")

-- ============================================================================
-- TREE 3: Test pointer sharing (two fields point to same data)
-- ============================================================================

start_tree("s_expression_test_13")
    use_record("system_context")
    p1 = m_call("CFL_PIPELINE")
        local c1 = o_call("CFL_LOG")
            str_ptr("Test 36: Pointer sharing test starting")
        end_call(c1)
        
        -- Allocate primary_node
        local c2 = o_call("TEST_34_ALLOC_NODE")
            field_ref("primary_node")
            uint(999)
            flt(42.0)
            uint(0xAB)
        end_call(c2)
        
        -- Make backup_node point to the SAME node as primary_node
        local c3 = o_call("TEST_36_COPY_PTR")
            field_ref("backup_node")         -- destination
            field_ref("primary_node")        -- source
        end_call(c3)
        
        -- Verify both point to same data
        local c4 = o_call("TEST_36_VERIFY_SAME_PTR")
            field_ref("primary_node")
            field_ref("backup_node")
        end_call(c4)
        
        -- Modify through backup_node
        local c5 = o_call("TEST_36_MODIFY_NODE_VALUE")
            field_ref("backup_node")
            flt(99.99)                       -- new value
        end_call(c5)
        
        -- Verify change visible through primary_node (proves sharing)
        local c6 = o_call("TEST_34_READ_NODE")
            field_ref("primary_node")
            uint(999)                        -- id unchanged
            flt(99.99)                       -- value changed!
            uint(0xAB)                       -- flags unchanged
        end_call(c6)
        
        -- Free only once (shared pointer)
        local c7 = o_call("TEST_34_FREE_PTR")
            field_ref("primary_node")
        end_call(c7)
        
        -- Clear backup_node to avoid dangling pointer
        local c8 = o_call("TEST_36_CLEAR_PTR")
            field_ref("backup_node")
        end_call(c8)
        
        local c9 = o_call("CFL_LOG")
            str_ptr("Test 36: PASSED")
        end_call(c9)
        result(SE_FUNCTION_TERMINATE)
    end_call(p1)
end_tree("s_expression_test_13")

-- ============================================================================
-- test_static_and_json.lua
-- Test: Static buffer copy + JSON reads with verification
-- ============================================================================



-- ============================================================================
-- RECORD DEFINITIONS
-- ============================================================================

RECORD("network_config_a")
    FIELD("ip_addr", "uint32")
    FIELD("port", "uint16")
    FIELD("timeout_ms", "uint16")
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
    FIELD("version", "uint16")
    FIELD("enabled", "bool")
END_RECORD()

RECORD("system_state_a")
    FIELD("network", "network_config_a")      -- embedded
    FIELD("sensors", "sensor_data_a")         -- embedded
    FIELD("device", "device_info_a")          -- embedded
    FIELD("error_code", "uint32")
    FIELD("run_count", "uint32")
    PTR_FIELD("device_ptr","char")
END_RECORD()

-- ============================================================================
-- TEST TREE: Static buffer copy + JSON reads
-- ============================================================================

start_tree("s_expression_test_14")
    use_record("system_state_a")
    p1 = m_call("CFL_PIPELINE")
    -- ========================================================================
        -- PART 1: Static buffer copy
        -- ========================================================================
        
        local c1 = o_call("CFL_LOG")
            str_ptr("Test 37: Static buffer copy starting")
        end_call(c1)
        
        -- Copy static network config into record
        local c2 = o_call("TEST_37_COPY_STATIC_NETWORK")
            nested_field_ref("network.ip_addr")
            nested_field_ref("network.port")
            nested_field_ref("network.timeout_ms")
        end_call(c2)
        
        -- Verify static copy
        local c3 = o_call("TEST_37_VERIFY_NETWORK")
            nested_field_ref("network.ip_addr")
            nested_field_ref("network.port")
            nested_field_ref("network.timeout_ms")
            uint(0xC0A80001)    -- expected: 192.168.0.1
            uint(8080)          -- expected port
            uint(5000)          -- expected timeout
        end_call(c3)
        
        local c4 = o_call("CFL_LOG")
            str_ptr("Static buffer copy verified")
        end_call(c4)
        
        -- ========================================================================
        -- PART 2: JSON reads - Sensor data
        -- ========================================================================
        
        local c5 = o_call("CFL_LOG")
            str_ptr("Test 37: JSON sensor reads starting")
        end_call(c5)
        
        cfl_json_read_float("sensors.temperature", "node_dict.column_data.user_data.sensors.temperature")
        cfl_json_read_float("sensors.pressure", "node_dict.column_data.user_data.sensors.pressure")
        cfl_json_read_float("sensors.humidity", "node_dict.column_data.user_data.sensors.humidity")
        cfl_json_read_uint("sensors.timestamp", "node_dict.column_data.user_data.sensors.timestamp")
        
        -- Verify sensor reads
        local c6 = o_call("TEST_37_VERIFY_SENSORS")
            nested_field_ref("sensors.temperature")
            nested_field_ref("sensors.pressure")
            nested_field_ref("sensors.humidity")
            nested_field_ref("sensors.timestamp")
            flt(25.5)           -- expected temperature
            flt(1013.25)        -- expected pressure
            flt(65.0)           -- expected humidity
            uint(1000000)       -- expected timestamp
        end_call(c6)
        
        local c7 = o_call("CFL_LOG")
            str_ptr("JSON sensor reads verified")
        end_call(c7)
        
        -- ========================================================================
        -- PART 3: JSON reads - Device info
        -- ========================================================================
        
        local c8 = o_call("CFL_LOG")
            str_ptr("Test 37: JSON device reads starting")
        end_call(c8)
        
        cfl_json_read_string_buf("device.name", "node_dict.column_data.user_data.device.name")
        cfl_json_read_string_buf("device.serial", "node_dict.column_data.user_data.device.serial")
        cfl_json_read_uint("device.version", "node_dict.column_data.user_data.device.version")
        cfl_json_read_bool("device.enabled", "node_dict.column_data.user_data.device.enabled")
        
        -- Verify device reads
        local c9 = o_call("TEST_37_VERIFY_DEVICE_NAME")
            nested_field_ref("device.name")
            str_ptr("TestDevice")   -- expected name
        end_call(c9)
        
        local c10 = o_call("TEST_37_VERIFY_DEVICE_SERIAL")
            nested_field_ref("device.serial")
            str_ptr("SN12345")      -- expected serial
        end_call(c10)
        
        local c11 = o_call("TEST_37_VERIFY_DEVICE_INFO")
            nested_field_ref("device.version")
            nested_field_ref("device.enabled")
            uint(0x0102)        -- expected version (1.2)
            uint(1)             -- expected enabled (true)
        end_call(c11)
        
        local c12 = o_call("CFL_LOG")
            str_ptr("JSON device reads verified")
        end_call(c12)
        
        -- ========================================================================
        -- PART 4: JSON reads - Top level fields
        -- ========================================================================
        
        local c13 = o_call("CFL_LOG")
            str_ptr("Test 37: JSON top-level reads starting")
        end_call(c13)
        
        cfl_json_read_uint("error_code", "node_dict.column_data.user_data.error_code")
        cfl_json_read_uint("run_count", "node_dict.column_data.user_data.run_count")
        
        -- Verify top level reads
        local c14 = o_call("TEST_37_VERIFY_TOP_LEVEL")
            field_ref("error_code")
            field_ref("run_count")
            uint(0)             -- expected error_code
            uint(42)            -- expected run_count
        end_call(c14)
        
        local c15 = o_call("CFL_LOG")
            str_ptr("JSON top-level reads verified")
        end_call(c15)
        
        -- ========================================================================
        -- PART 5: Final verification - dump all values
        -- ========================================================================
        
        local c16 = o_call("CFL_LOG")
            str_ptr("Test 37: Final state dump")
        end_call(c16)
        
        local c17 = o_call("TEST_37_DUMP_STATE")
            field_ref("network")
            field_ref("sensors")
            field_ref("device")
            field_ref("error_code")
            field_ref("run_count")
        end_call(c17)
        
        local c18 = o_call("CFL_LOG")
            str_ptr("Test 37: PASSED")
        end_call(c18)

        local c19 = o_call("CFL_LOG")
            str_ptr("Test 37: String pointer read starting")
        end_call(c19)
        -- Read device.name into device_ptr (char*)
        cfl_json_read_string_ptr("device_ptr", "node_dict.column_data.user_data.device.name")

        -- Verify string pointer
        local c20 = o_call("TEST_37_VERIFY_STRING_PTR")
            field_ref("device_ptr")
            str_ptr("TestDevice")   -- expected value
        end_call(c20)
        -- ========================================================================
        -- END: Terminate
        -- ========================================================================
        
        result(SE_FUNCTION_TERMINATE)
     p1=end_call(p1)   
end_tree("s_expression_test_14")


--============================================================================
-- TEST 38: Constant Record Copy Functions
-- Demonstrates CFL_COPY_CONST and CFL_COPY_CONST_FULL
--============================================================================

RECORD("pid_gains_a")
    FIELD("kp", "float")
    FIELD("ki", "float")
    FIELD("kd", "float")
END_RECORD()

RECORD("test38_bb")
    FIELD("gains", "pid_gains_a")
    FIELD("verified", "bool")
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
    VALUE("verified", false)
END_CONST()

start_tree("s_expression_test_15")
    use_record("test38_bb")
    
    cfl_pipeline(function()
        -- Test 1: Copy full blackboard from ROM
        cfl_copy_const_full("test38_defaults")
        
        -- User function verifies: kp=1.0, ki=0.2, kd=0.05
        local c = o_call("TEST_38_VERIFY_DEFAULTS")
            field_ref("gains")
        end_call(c)
        
        -- Test 2: Copy just the gains field from different constant
        cfl_copy_const("gains", "test_pid")
        
        -- User function verifies: kp=2.5, ki=0.5, kd=0.1
        local c = o_call("TEST_38_VERIFY_TEST_PID")
            field_ref("gains")
        end_call(c)
        
        cfl_log("Test 38: PASS")
        result(SE_FUNCTION_TERMINATE)
    end)
end_tree("s_expression_test_15")



--============================================================================
-- TEST 39: Constant Record Copy Functions
-- Demonstrates CFL_COPY_CONST and CFL_COPY_CONST_FULL
--============================================================================

RECORD("pid_gains_c")
    FIELD("kp", "float")
    FIELD("ki", "float")
    FIELD("kd", "float")
END_RECORD()

RECORD("test39_bb")
    FIELD("gains", "pid_gains_c")
    PTR_FIELD("gains_ptr","pid_gains_c")
END_RECORD()


start_tree("s_expression_test_16")
    use_record("test39_bb")
    
    cfl_pipeline(function()
        -- Test 1: Copy full blackboard from ROM
        
        
        -- User function verifies: kp=2.5, ki=0.5, kd=0.1
        local c = o_call("TEST_39_VERIFY_GAINS")
            field_ref("gains")
        end_call(c)
        -- User function verifies the pointer
        local d =  o_call("TEST_39_VERIFY_POINTER")
            field_ref("gains")
        end_call(d)
        
        cfl_log("Test 38: PASS")
        result(SE_FUNCTION_TERMINATE)
    end)
end_tree("s_expression_test_16")

-- ============================================================================
-- END MODULE
-- ============================================================================





return_value = end_module(mod)
print("DEBUG: return_value =", return_value)
return return_value