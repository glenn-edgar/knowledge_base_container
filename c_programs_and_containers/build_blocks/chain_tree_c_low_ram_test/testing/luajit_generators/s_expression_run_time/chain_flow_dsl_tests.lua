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
        oneshot("CFL_ENABLE_CHILDREN")
        oneshot("TEST_29_SET_STATE", str("node_state"),slot_ref("branch_1"), int(1))
        quote("SE_CONTINUE")
        end_pipeline(p)
    end_action(a)
    
end

function disable_children_action()
    local a = action("else")
        local p = pipeline("disable_children")
        oneshot("CFL_DISABLE_CHILDREN")
        oneshot("TEST_29_SET_STATE", str("node_state"),slot_ref("branch_1"), int(0))
        quote("SE_HALT")
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
     local n = if_then_else("check_state")
        check_state()
        active_state()
        inactive_state()
        end_if_then_else(n)
        quote("SE_HALT")
    end_pipeline(main_pipeline)
end_tree(tree1)

return end_module(mod)



