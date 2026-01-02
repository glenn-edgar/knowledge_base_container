
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
            result(SE_CONTINUE)
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
            result(SE_CONTINUE)
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
            result(SE_CONTINUE)
        end },
        
        -- CMD_BACK (2)
        { CMD_BACK, function()
            cfl_log("Moving backward")
            set_motor(MOTOR_LEFT, -100)
            set_motor(MOTOR_RIGHT, -100)
            cfl_tick_delay(50)
            set_command(CMD_LEFT)
            result(SE_CONTINUE)
        end },
        
        -- CMD_LEFT (3)
        { CMD_LEFT, function()
            cfl_log("Turning left")
            set_motor(MOTOR_LEFT, -50)
            set_motor(MOTOR_RIGHT, 50)
            cfl_tick_delay(25)
            set_command(CMD_RIGHT)
            result(SE_CONTINUE)
        end },
        
        -- CMD_RIGHT (4)
        { CMD_RIGHT, function()
            cfl_log("Turning right")
            set_motor(MOTOR_LEFT, 50)
            set_motor(MOTOR_RIGHT, -50)
            cfl_tick_delay(25)
            set_command(CMD_STOP)
            result(SE_CONTINUE)
        end },
        
        -- CMD_STOP (5)
        { CMD_STOP, function()
            cfl_log("Stopping")
            set_motor(MOTOR_LEFT, 0)
            set_motor(MOTOR_RIGHT, 0)
            result(SE_CONTINUE)
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
            result(SE_CONTINUE)
        end },
        
        -- EVT_BUTTON (0xEE02)
        { EVT_BUTTON, function()
            cfl_log("Button pressed")
            cfl_wait_event(EVT_BUTTON, 10)
            local tog = o_call("TEST_32_TOGGLE_LED")
                int(LED_STATUS)
            end_call(tog)
            result(SE_CONTINUE)
        end },
        
        -- EVT_SENSOR (0xEE03)
        { EVT_SENSOR, function()
            cfl_log("Sensor reading")
            local chk = m_call("TEST_32_CHECK_THRESHOLD")
                field_ref("sensor_value")
                int(50)
            end_call(chk)
            result(SE_CONTINUE)
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
            result(SE_CONTINUE)
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
        
        -- Default (EVT_TICK or unknown) - case 0
        { 0, function()
            local gen = m_call("TEST_32_GENERATE_INTERNAL_EVENTS")
                field_ref("event_id")
            end_call(gen)
            local bg = m_call("TEST_32_RUN_BACKGROUND_TASKS")
            end_call(bg)
            result(SE_CONTINUE)
        end },
    })

end_tree("s_expression_test_8")

return_value = end_module(mod)
print("DEBUG: return_value =", return_value)
return return_value