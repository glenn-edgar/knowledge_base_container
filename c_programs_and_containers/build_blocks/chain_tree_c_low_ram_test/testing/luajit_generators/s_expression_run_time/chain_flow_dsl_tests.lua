dofile("s_expr_dsl.lua")

-- ============================================================================
-- GENSYM HELPER (if not in DSL yet)
-- ============================================================================


-- Log message helper
function log(msg)
    oneshot("CFL_LOG", str(msg))
end

-- Delay helper  
function delay(ms)
    main("CFL_DELAY", int(ms))
end

function boolean_test()
    local c = condition("cond")
    local top = bool_or("or")
        local inner = bool_and("and_1")
            bool_fn("CFL_READ_BIT", int(0))
            bool_fn("CFL_READ_BIT", int(1))
        end_bool_and(inner)
        local inner_2 = bool_and("and_2")
            bool_fn("CFL_READ_BIT", int(2))
            bool_fn("CFL_READ_BIT", int(3))
        end_bool_and(inner_2)
    end_bool_or(top)
    end_condition(c)
    
end

function null_action()
    local a = action("then")
        quote("SE_CONTINUE")
    end_action(a)
    
end

function enable_children_action()
    local a = action("then")
        local p = pipeline("enable_children")
        main("CFL_ENABLE_CHILDREN")
        main("TEST_29_SET_STATE", str("node_state"),slot_ref("branch_1"), int(1))
        quote("SE_CONTINUE")
        end_pipeline(p)
    end_action(a)
    
end

function disable_children_action()
    local a = action("else")
        local p = pipeline("disable_children")
        main("CFL_DISABLE_CHILDREN")
        main("TEST_29_SET_STATE", str("node_state"),slot_ref("branch_1"), int(0))
        quote("SE_CONTINUE")
        end_pipeline(p)
    end_action(a)
    
end

function check_state()
    local c = condition("cond")
    bool_fn("TEST_29_READ_STATE", str("node_state"),slot_ref("branch_1"))
    end_condition(c)
    
end

function active_state()
    local a = action("then")
        local b= if_then_else("active")
        boolean_test()
        null_action()
        disable_children_action()
        end_if_then_else(b)
    end_action(a)

end

function inactive_state()
    local a = action("then")
        local b= if_then_else("inactive")
        boolean_test()
        enable_children_action()
        null_action()
        end_if_then_else(b)
    end_action(a)
    
end



-- ============================================================================
-- BEGIN MODULE
-- ============================================================================
local mod = start_module("chain_flow_dsl_tests", { is_64bit = false })

defpool("node_state", "node_state_t")
defslot("branch_1", "node_state")




local tree1 = start_tree("s_expression_test_1")
    
    local main_pipeline = pipeline("main")
    oneshot("TEST_29_SET_STATE", str("node_state"),slot_ref("branch_1"), int(0))
    oneshot("CFL_DISABLE_CHILDREN")
     local n = if_then_else("check_state")
        check_state()
        active_state()
        inactive_state()
        end_if_then_else(n)
        quote("SE_CONTINUE")
    end_pipeline(main_pipeline)
end_tree(tree1)

local function bit_test()
    
end

local tree2 = start_tree("s_expression_test_2")
    local main_pipeline = pipeline("main")
    oneshot("CFL_DISABLE_CHILDREN")
         
    main("TEST_29_DF_CONTROL",
    p_call("CFL_S_BIT_OR",
        
            p_call("CFL_S_BIT_AND", 0, 1),
            p_call("CFL_S_BIT_AND", 2, 3)
        
    ),
    oneshot_ref("CFL_ENABLE_CHILDREN"),
    oneshot_ref("CFL_DISABLE_CHILDREN")
)                                   
    end_pipeline(main_pipeline)
end_tree(tree2)


local SM_STATE_1 = 1
local SM_STATE_2 = 2
local SM_STATE_3 = 3

defpool("state_machine_state", "state_machine_state_t")
defslot("test_30_state_machine_state", "state_machine_state")
defslot("test_30_state_machine_state_b", "state_machine_state")

-- ============================================================================
-- STATE MACHINE
-- ============================================================================

local function test_30_state_machine()
    local sm = cond("state_machine")
    
        local s1 = clause("state_1")
            local c1 = condition("cond")
                bool_fn("TEST_30_CHECK_STATE", slot_ref("test_30_state_machine_state"), int(SM_STATE_1))
            end_condition(c1)
            local a1 = action("action")
                local p1 = pipeline("state_1")
                    oneshot("CFL_LOG", str("state 1 active"))
                    oneshot("CFL_DISABLE_CHILDREN")
                    oneshot("CFL_ENABLE_CHILD", 0)
                    main("CFL_TICK_DELAY", int(100))
                    oneshot("TEST_30_SET_STATE", slot_ref("test_30_state_machine_state"), int(SM_STATE_2))
                    oneshot("CFL_LOG", str("state 1 terminate"))
                    quote("SE_FUNCTION_RESET")
                end_pipeline(p1)
            end_action(a1)
        end_clause(s1)
        
        local s2 = clause("state_2")
            local c2 = condition("cond")
                bool_fn("TEST_30_CHECK_STATE", slot_ref("test_30_state_machine_state"), int(SM_STATE_2))
            end_condition(c2)
            local a2 = action("action")
                local p2 = pipeline("state_2")
                    oneshot("CFL_LOG", str("state 2 active"))
                    oneshot("CFL_DISABLE_CHILDREN")
                    oneshot("CFL_ENABLE_CHILD", 1)
                    main("CFL_TICK_DELAY", int(100))
                    oneshot("TEST_30_SET_STATE", slot_ref("test_30_state_machine_state"), int(SM_STATE_3))
                    oneshot("CFL_LOG", str("state 2 terminate"))
                    quote("SE_FUNCTION_RESET")
                end_pipeline(p2)
            end_action(a2)
        end_clause(s2)
        
        local s3 = clause("state_3")
            local c3 = condition("cond")
                bool_fn("TEST_30_CHECK_STATE", slot_ref("test_30_state_machine_state"), int(SM_STATE_3))
            end_condition(c3)
            local a3 = action("action")
                local p3 = pipeline("state_3")
                    oneshot("CFL_LOG", str("state 3 active"))
                    oneshot("CFL_DISABLE_CHILDREN")
                    oneshot("CFL_ENABLE_CHILD", 2)
                    oneshot("CFL_ENABLE_CHILD", 3)
                    main("CFL_TICK_DELAY", int(100))
                    oneshot("CFL_LOG", str("state 3 terminate"))
                    quote("SE_FUNCTION_TERMINATE")
                end_pipeline(p3)
            end_action(a3)
        end_clause(s3)
        
        local df = default_clause("state_unknown")
            local ad = action("action")
                oneshot("CFL_EXCEPTION", str("UNKNOWN_STATE test_30_state_machine"))
        
            end_action(ad)
        end_default_clause(df)
        
    end_cond(sm)
end

-- ============================================================================
-- TREE
-- ============================================================================

local tree3 = start_tree("s_expression_test_3")
    local p = pipeline("main")
        
        -- Runs ONLY during s_expr_tree_init(), NOT on SE_FUNCTION_RESET
        init_once("TEST_30_SET_STATE", slot_ref("test_30_state_machine_state"), int(SM_STATE_1))
        
        -- State machine runs on every tick
        test_30_state_machine()
        
    end_pipeline(p)
end_tree(tree3)



local tree4 = start_tree("s_expression_test_4")
    local p = pipeline("main")
        
        -- Initialize state to 0
        init_once("TEST_30_SET_STATE", slot_ref("test_30_state_machine_state_b"), int(0))
        
        -- Compact state machine - 1 node instead of ~20
        main("CFL_STATE_MACHINE",
            slot_ref("test_30_state_machine_state_b"),
            
            -- State 0
            m_call("CFL_STATE_ACTIONS",
                o_call("CFL_DISABLE_CHILDREN"),
                o_call("CFL_ENABLE_CHILD", 0),
                m_call("CFL_TICK_DELAY", int(100)),
                m_call("TEST_30_SET_STATE", slot_ref("test_30_state_machine_state_b"), int(1)),
                int(SE_FUNCTION_RESET)
            ),
            
            -- State 1
            m_call("CFL_STATE_ACTIONS",
                o_call("CFL_DISABLE_CHILDREN"),
                o_call("CFL_ENABLE_CHILD", 1),
                m_call("CFL_TICK_DELAY", int(100)),
                m_call("TEST_30_SET_STATE", slot_ref("test_30_state_machine_state_b"), int(2)),
                int(SE_FUNCTION_RESET)
            ),
            
            -- State 2
            m_call("CFL_STATE_ACTIONS",
                o_call("CFL_DISABLE_CHILDREN"),
                o_call("CFL_ENABLE_CHILD", 2),
                o_call("CFL_ENABLE_CHILD", 3),
                m_call("CFL_TICK_DELAY", int(100)),
                int(SE_FUNCTION_TERMINATE)
            )
        )
        
    end_pipeline(p)
end_tree(tree4)


-- Command routing based on slot value

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

-- Slot for robot_command (missing)
defpool("cmd_pool", "int32_t")
defslot("robot_command", "cmd_pool")


local tree = start_tree("s_expression_test_5")
    local p = pipeline("main")
        init_once("TEST_31_SET_STATE", slot_ref("robot_command"), int(CMD_FORWARD))
        local d = dispatch("command_router", slot_ref("robot_command"))
        
            local c1 = case("forward", int(CMD_FORWARD))
                local a1 = action("action")
                    local p1 = pipeline("forward_action")
                        oneshot("CFL_LOG", str("Moving forward"))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_LEFT), int(100))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_RIGHT), int(100))
                        main("CFL_TICK_DELAY", int(50))
                        main("TEST_31_SET_STATE", slot_ref("robot_command"), int(CMD_BACK))
                        quote("SE_HALT")
                    end_pipeline(p1)
                end_action(a1)
            end_case(c1)
            
            local c2 = case("back", int(CMD_BACK))
                local a2 = action("action")
                    local p2 = pipeline("back_action")
                        oneshot("CFL_LOG", str("Moving backward"))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_LEFT), int(-100))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_RIGHT), int(-100))
                        main("CFL_TICK_DELAY", int(50))
                        main("TEST_31_SET_STATE", slot_ref("robot_command"), int(CMD_LEFT))
                        quote("SE_HALT")
                    end_pipeline(p2)
                end_action(a2)
            end_case(c2)
            
            local c3 = case("left", int(CMD_LEFT))
                local a3 = action("action")
                    local p3 = pipeline("left_action")
                        oneshot("CFL_LOG", str("Turning left"))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_LEFT), int(-50))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_RIGHT), int(50))
                        main("CFL_TICK_DELAY", int(25))
                        main("TEST_31_SET_STATE", slot_ref("robot_command"), int(CMD_RIGHT))
                        quote("SE_HALT")
                    end_pipeline(p3)
                end_action(a3)
            end_case(c3)
            
            local c4 = case("right", int(CMD_RIGHT))
                local a4 = action("action")
                    local p4 = pipeline("right_action")
                        oneshot("CFL_LOG", str("Turning right"))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_LEFT), int(50))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_RIGHT), int(-50))
                        main("CFL_TICK_DELAY", int(25))
                        main("TEST_31_SET_STATE", slot_ref("robot_command"), int(CMD_STOP))
                        quote("SE_HALT")
                    end_pipeline(p4)
                end_action(a4)
            end_case(c4)
            
            local c5 = case("stop", int(CMD_STOP))
                local a5 = action("action")
                    local p5 = pipeline("stop_action")
                        oneshot("CFL_LOG", str("Stopping"))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_LEFT), int(0))
                        oneshot("TEST_31_SET_MOTOR", int(MOTOR_RIGHT), int(0))
                        quote("SE_FUNCTION_TERMINATE")
                    end_pipeline(p5)
                end_action(a5)
            end_case(c5)
            
            local df = default_case("idle")
                local ad = action("action")
                    local p6 = pipeline("idle_action")
                        oneshot("CFL_LOG", str("Idle"))
                        oneshot("CFL_LOG", str("SHOULD NOT HAPPEN"))
                        quote("SE_TERMINATE")
                    end_pipeline(p6)
                end_action(ad)
            end_default_case(df)
            
        end_dispatch(d)
        
    end_pipeline(p)
end_tree(tree)

-- Event handling based on event_id


local EVT_TICK      = 4
local EVT_TIMER     = 0xEE01
local EVT_BUTTON    = 0xEE02
local EVT_SENSOR    = 0xEE03
local EVT_ALARM     = 0xEE04
local EVT_SHUTDOWN  = 0xEE05

local LED_STATUS = 0
local LED_ALARM  = 1

-- Slot definitions (already have these)
defpool("counter_pool", "int32_t")
defslot("timer_count", "counter_pool")
defpool("sensor_pool", "int32_t")
defslot("sensor_value", "sensor_pool")
defpool("event_pool", "int32_t")
defslot("event_id", "event_pool")




local tree = start_tree("s_expression_test_6")
    local p = pipeline("main")
        
        local d = event_dispatch("event_router")
        
            local c1 = case("timer", int(EVT_TIMER))
                local a1 = action("action")
                    local p1 = pipeline("timer_action")
                        oneshot("CFL_LOG", str("Timer expired"))
                        main("TEST_32_PROCESS_SCHEDULED_TASKS")
                        quote("SE_HALT")
                    end_pipeline(p1)
                end_action(a1)
            end_case(c1)
            
            local c2 = case("button", int(EVT_BUTTON))
                local a2 = action("action")
                    local p2 = pipeline("button_action")
                        oneshot("CFL_LOG", str("Button pressed"))
                        main("TEST_32_DEBOUNCE", slot_ref("timer_count"), int(10))
                        oneshot("TEST_32_TOGGLE_LED", int(LED_STATUS))
                        quote("SE_HALT")
                    end_pipeline(p2)
                end_action(a2)
            end_case(c2)
            
            local c3 = case("sensor", int(EVT_SENSOR))
                local a3 = action("action")
                    local p3 = pipeline("sensor_action")
                        oneshot("CFL_LOG", str("Sensor reading"))
                        main("TEST_32_CHECK_THRESHOLD", slot_ref("sensor_value"), int(50))
                        
                        quote("SE_HALT")
                    end_pipeline(p3)
                end_action(a3)
            end_case(c3)
            
            local c4 = case("alarm", int(EVT_ALARM))
                local a4 = action("action")
                    local p4 = pipeline("alarm_action")
                        oneshot("CFL_LOG", str("ALARM TRIGGERED"))
                        oneshot("TEST_32_ENABLE_BUZZER")
                        oneshot("TEST_32_SET_LED", int(LED_ALARM), int(1))
                        oneshot("TEST_32_NOTIFY_SYSTEM", str("ALARM"))
                        oneshot("CFL_INTERNAL_EVENT", int(EVT_SHUTDOWN), int(1))
                        oneshot("CFL_LOG", str("ALARM SETTING"));
                        quote("SE_HALT")
                    end_pipeline(p4)
                end_action(a4)
            end_case(c4)
            
            local c5 = case("shutdown", int(EVT_SHUTDOWN))
                local a5 = action("action")
                    local p5 = pipeline("shutdown_action")
                        oneshot("CFL_LOG", str("Shutdown requested"))
                        oneshot("TEST_32_DISABLE_ALL_OUTPUTS")
                        oneshot("TEST_32_SAVE_STATE")
                        quote("SE_FUNCTION_TERMINATE")
                    end_pipeline(p5)
                end_action(a5)
            end_case(c5)
            
            local df = default_case("tick")
                local ad = action("action")
                    local p6 = pipeline("tick_action")
                    -- EVT_TICK or unknown - normal tick processing
                        main("TEST_32_GENERATE_INTERNAL_EVENTS", slot_ref("event_id"))
                        main("TEST_32_RUN_BACKGROUND_TASKS")
                        quote("SE_HALT")
                    end_pipeline(p6)
                end_action(ad)
            end_default_case(df)
            
        end_event_dispatch(d)
        
    end_pipeline(p)
end_tree(tree)

return end_module(mod)



