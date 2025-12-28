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
    --[[
    main("TEST_29_DF_CONTROL",     open_brace(),
                                        pred_ref("CFL_S_BIT_OR") ,
                                        open_brace(),
                                             open_brace(), pred_ref("CFL_S_BIT_AND"),0,1, close_brace(),
                                              open_brace(), pred_ref("CFL_S_BIT_AND"),2,3, close_brace(),
                                        close_brace(),
                                    close_brace(),
                                    oneshot_ref("CFL_ENABLE_CHILDREN"),
                                    oneshot_ref("CFL_DISABLE_CHILDREN") )
    --]]          
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
return end_module(mod)



