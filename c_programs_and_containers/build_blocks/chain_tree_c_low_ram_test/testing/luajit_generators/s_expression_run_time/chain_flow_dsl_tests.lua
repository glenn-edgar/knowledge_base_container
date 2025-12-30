dofile("s_expr_dsl.lua")

-- ============================================================================
-- BEGIN MODULE
-- ============================================================================
local mod = start_module("chain_flow_dsl_tests", { is_64bit = false })

defpool("node_state", "node_state_t")
defslot("branch_1", "node_state")

-- ============================================================================
-- TEST 2: Boolean logic with p_call
-- ============================================================================

start_tree("s_expression_test_2")

    local c1 = o_call("CFL_DISABLE_CHILDREN")
    end_call(c1)
    
    local m1 = m_call("TEST_29_DF_CONTROL")
        -- First param: nested boolean expression
        local p1 = p_call("CFL_S_BIT_OR")
            local p2 = p_call("CFL_S_BIT_AND")
                int(0)
                int(1)
            end_call(p2)
            local p3 = p_call("CFL_S_BIT_AND")
                int(2)
                int(3)
            end_call(p3)
        end_call(p1)
        -- Second param: enable callback (function index)
        uint(0)  -- CFL_ENABLE_CHILDREN index
        -- Third param: disable callback (function index)  
        uint(1)  -- CFL_DISABLE_CHILDREN index
    end_call(m1)

end_tree("s_expression_test_2")

-- ============================================================================
-- STATE MACHINE CONSTANTS
-- ============================================================================

local SM_STATE_1 = 1
local SM_STATE_2 = 2
local SM_STATE_3 = 3

defpool("state_machine_state", "state_machine_state_t")
defslot("test_30_state_machine_state", "state_machine_state")
defslot("test_30_state_machine_state_b", "state_machine_state")

-- ============================================================================
-- TEST 4: State Machine
-- ============================================================================

start_tree("s_expression_test_4")

    -- Initialize state to 0
    local init = io_call("TEST_30_SET_STATE")
        slot_ref("test_30_state_machine_state_b")
        int(0)
    end_call(init)
    
    -- State machine
    local sm = m_call("CFL_STATE_MACHINE")
        slot_ref("test_30_state_machine_state_b")
        
        -- State 0
        local s0 = m_call("CFL_STATE_ACTIONS")
            local a1 = o_call("CFL_DISABLE_CHILDREN")
            end_call(a1)
            local a2 = o_call("CFL_ENABLE_CHILD")
                int(0)
            end_call(a2)
            local d1 = pt_m_call("CFL_TICK_DELAY")
                int(100)
            end_call(d1)
            local t1 = m_call("TEST_30_SET_STATE")
                slot_ref("test_30_state_machine_state_b")
                int(1)
            end_call(t1)
            int(SE_FUNCTION_RESET)
        end_call(s0)
        
        -- State 1
        local s1 = m_call("CFL_STATE_ACTIONS")
            local b1 = o_call("CFL_DISABLE_CHILDREN")
            end_call(b1)
            local b2 = o_call("CFL_ENABLE_CHILD")
                int(1)
            end_call(b2)
            local d2 = pt_m_call("CFL_TICK_DELAY")
                int(100)
            end_call(d2)
            local t2 = m_call("TEST_30_SET_STATE")
                slot_ref("test_30_state_machine_state_b")
                int(2)
            end_call(t2)
            int(SE_FUNCTION_RESET)
        end_call(s1)
        
        -- State 2
        local s2 = m_call("CFL_STATE_ACTIONS")
            local c1 = o_call("CFL_DISABLE_CHILDREN")
            end_call(c1)
            local c2 = o_call("CFL_ENABLE_CHILD")
                int(2)
            end_call(c2)
            local c3 = o_call("CFL_ENABLE_CHILD")
                int(3)
            end_call(c3)
            local d3 = pt_m_call("CFL_TICK_DELAY")
                int(100)
            end_call(d3)
            int(SE_FUNCTION_TERMINATE)
        end_call(s2)
        
    end_call(sm)

end_tree("s_expression_test_4")

-- ============================================================================
-- ROBOT COMMAND CONSTANTS
-- ============================================================================

local MOTOR_LEFT  = 0
local MOTOR_RIGHT = 1

local LED_STATUS = 0
local LED_ALARM  = 1

local CMD_IDLE    = 0
local CMD_FORWARD = 1
local CMD_BACK    = 2
local CMD_LEFT    = 3
local CMD_RIGHT   = 4
local CMD_STOP    = 5

defpool("cmd_pool", "int32_t")
defslot("robot_command", "cmd_pool")

-- ============================================================================
-- EVENT CONSTANTS
-- ============================================================================

local EVT_TICK      = 4
local EVT_TIMER     = 0xEE01
local EVT_BUTTON    = 0xEE02
local EVT_SENSOR    = 0xEE03
local EVT_ALARM     = 0xEE04
local EVT_SHUTDOWN  = 0xEE05

defpool("counter_pool", "int32_t")
defslot("timer_count", "counter_pool")
defpool("sensor_pool", "int32_t")
defslot("sensor_value", "sensor_pool")
defpool("event_pool", "int32_t")
defslot("event_id", "event_pool")

-- ============================================================================
-- TEST 7: Command Dispatch (slot-based)
-- ============================================================================

start_tree("s_expression_test_7")

    -- Initialize state
    local init = io_call("TEST_31_SET_STATE")
        slot_ref("robot_command")
        int(CMD_FORWARD)
    end_call(init)
    
    -- Dispatch on robot_command
    local disp = m_call("CFL_DISPATCH")
        slot_ref("robot_command")
        
        -- CMD_FORWARD (1)
        local l1 = list_start("fwd")
            int(CMD_FORWARD)
            local sa1 = m_call("CFL_STATE_ACTIONS")
                local log1 = o_call("CFL_LOG")
                    str("Moving forward")
                end_call(log1)
                local m1 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_LEFT)
                    int(100)
                end_call(m1)
                local m2 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_RIGHT)
                    int(100)
                end_call(m2)
                local d1 = pt_m_call("CFL_TICK_DELAY")
                    int(50)
                end_call(d1)
                local ns1 = m_call("TEST_31_SET_STATE")
                    slot_ref("robot_command")
                    int(CMD_BACK)
                end_call(ns1)
                int(SE_FUNCTION_RESET)
            end_call(sa1)
        list_end(l1)
        
        -- CMD_BACK (2)
        local l2 = list_start("back")
            int(CMD_BACK)
            local sa2 = m_call("CFL_STATE_ACTIONS")
                local log2 = o_call("CFL_LOG")
                    str("Moving backward")
                end_call(log2)
                local m3 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_LEFT)
                    int(-100)
                end_call(m3)
                local m4 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_RIGHT)
                    int(-100)
                end_call(m4)
                local d2 = pt_m_call("CFL_TICK_DELAY")
                    int(50)
                end_call(d2)
                local ns2 = m_call("TEST_31_SET_STATE")
                    slot_ref("robot_command")
                    int(CMD_LEFT)
                end_call(ns2)
                int(SE_FUNCTION_RESET)
            end_call(sa2)
        list_end(l2)
        
        -- CMD_LEFT (3)
        local l3 = list_start("left")
            int(CMD_LEFT)
            local sa3 = m_call("CFL_STATE_ACTIONS")
                local log3 = o_call("CFL_LOG")
                    str("Turning left")
                end_call(log3)
                local m5 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_LEFT)
                    int(-50)
                end_call(m5)
                local m6 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_RIGHT)
                    int(50)
                end_call(m6)
                local d3 = pt_m_call("CFL_TICK_DELAY")
                    int(25)
                end_call(d3)
                local ns3 = m_call("TEST_31_SET_STATE")
                    slot_ref("robot_command")
                    int(CMD_RIGHT)
                end_call(ns3)
                int(SE_FUNCTION_RESET)
            end_call(sa3)
        list_end(l3)
        
        -- CMD_RIGHT (4)
        local l4 = list_start("right")
            int(CMD_RIGHT)
            local sa4 = m_call("CFL_STATE_ACTIONS")
                local log4 = o_call("CFL_LOG")
                    str("Turning right")
                end_call(log4)
                local m7 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_LEFT)
                    int(50)
                end_call(m7)
                local m8 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_RIGHT)
                    int(-50)
                end_call(m8)
                local d4 = pt_m_call("CFL_TICK_DELAY")
                    int(25)
                end_call(d4)
                local ns4 = m_call("TEST_31_SET_STATE")
                    slot_ref("robot_command")
                    int(CMD_STOP)
                end_call(ns4)
                int(SE_FUNCTION_RESET)
            end_call(sa4)
        list_end(l4)
        
        -- CMD_STOP (5)
        local l5 = list_start("stop")
            int(CMD_STOP)
            local sa5 = m_call("CFL_STATE_ACTIONS")
                local log5 = o_call("CFL_LOG")
                    str("Stopping")
                end_call(log5)
                local m9 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_LEFT)
                    int(0)
                end_call(m9)
                local m10 = o_call("TEST_31_SET_MOTOR")
                    int(MOTOR_RIGHT)
                    int(0)
                end_call(m10)
                int(SE_FUNCTION_TERMINATE)
            end_call(sa5)
        list_end(l5)
        
        -- CMD_IDLE (0) - default
        local l6 = list_start("idle")
            int(CMD_IDLE)
            local sa6 = m_call("CFL_STATE_ACTIONS")
                local log6 = o_call("CFL_LOG")
                    str("Idle - SHOULD NOT HAPPEN")
                end_call(log6)
                int(SE_FUNCTION_TERMINATE)
            end_call(sa6)
        list_end(l6)
        
    end_call(disp)

end_tree("s_expression_test_7")

-- ============================================================================
-- TEST 8: Event Dispatch
-- ============================================================================

start_tree("s_expression_test_8")

    local evd = m_call("CFL_EVENT_DISPATCH")
        
        -- EVT_TIMER (0xEE01)
        local e1 = list_start("timer")
            int(EVT_TIMER)
            local sa1 = m_call("CFL_STATE_ACTIONS")
                local log1 = o_call("CFL_LOG")
                    str("Timer expired")
                end_call(log1)
                local proc1 = m_call("TEST_32_PROCESS_SCHEDULED_TASKS")
                end_call(proc1)
                int(SE_HALT)
            end_call(sa1)
        list_end(e1)
        
        -- EVT_BUTTON (0xEE02)
        local e2 = list_start("button")
            int(EVT_BUTTON)
            local sa2 = m_call("CFL_STATE_ACTIONS")
                local log2 = o_call("CFL_LOG")
                    str("Button pressed")
                end_call(log2)
                local deb = m_call("TEST_32_DEBOUNCE")
                    slot_ref("timer_count")
                    int(10)
                end_call(deb)
                local tog = o_call("TEST_32_TOGGLE_LED")
                    int(LED_STATUS)
                end_call(tog)
                int(SE_HALT)
            end_call(sa2)
        list_end(e2)
        
        -- EVT_SENSOR (0xEE03)
        local e3 = list_start("sensor")
            int(EVT_SENSOR)
            local sa3 = m_call("CFL_STATE_ACTIONS")
                local log3 = o_call("CFL_LOG")
                    str("Sensor reading")
                end_call(log3)
                local chk = m_call("TEST_32_CHECK_THRESHOLD")
                    slot_ref("sensor_value")
                    int(50)
                end_call(chk)
                int(SE_HALT)
            end_call(sa3)
        list_end(e3)
        
        -- EVT_ALARM (0xEE04)
        local e4 = list_start("alarm")
            int(EVT_ALARM)
            local sa4 = m_call("CFL_STATE_ACTIONS")
                local log4 = o_call("CFL_LOG")
                    str("ALARM TRIGGERED")
                end_call(log4)
                local buzz = o_call("TEST_32_ENABLE_BUZZER")
                end_call(buzz)
                local led = o_call("TEST_32_SET_LED")
                    int(LED_ALARM)
                    int(1)
                end_call(led)
                local notify = o_call("TEST_32_NOTIFY_SYSTEM")
                    str("ALARM")
                end_call(notify)
                local ievt = o_call("CFL_INTERNAL_EVENT")
                    int(EVT_SHUTDOWN)
                    int(1)
                end_call(ievt)
                local log5 = o_call("CFL_LOG")
                    str("ALARM SETTING")
                end_call(log5)
                int(SE_HALT)
            end_call(sa4)
        list_end(e4)
        
        -- EVT_SHUTDOWN (0xEE05)
        local e5 = list_start("shutdown")
            int(EVT_SHUTDOWN)
            local sa5 = m_call("CFL_STATE_ACTIONS")
                local log6 = o_call("CFL_LOG")
                    str("Shutdown requested")
                end_call(log6)
                local dis = o_call("TEST_32_DISABLE_ALL_OUTPUTS")
                end_call(dis)
                local sav = o_call("TEST_32_SAVE_STATE")
                end_call(sav)
                int(SE_FUNCTION_TERMINATE)
            end_call(sa5)
        list_end(e5)
        
        -- Default (EVT_TICK or unknown)
        local e6 = list_start("default")
            int(0)
            local sa6 = m_call("CFL_STATE_ACTIONS")
                local gen = m_call("TEST_32_GENERATE_INTERNAL_EVENTS")
                    slot_ref("event_id")
                end_call(gen)
                local bg = m_call("TEST_32_RUN_BACKGROUND_TASKS")
                end_call(bg)
                int(SE_HALT)
            end_call(sa6)
        list_end(e6)
        
    end_call(evd)

end_tree("s_expression_test_8")

return end_module(mod)