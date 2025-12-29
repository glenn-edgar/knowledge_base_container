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






return end_module(mod)



