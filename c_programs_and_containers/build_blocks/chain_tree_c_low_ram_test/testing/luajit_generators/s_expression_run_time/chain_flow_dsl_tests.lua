dofile("s_expr_dsl.lua")

-- ============================================================================
-- GENSYM HELPER (if not in DSL yet)
-- ============================================================================
local _gensym_counter = 0
function gensym(prefix)
    _gensym_counter = _gensym_counter + 1
    return (prefix or "g") .. "_" .. _gensym_counter
end

-- Log message helper
function log(msg)
    oneshot("CFL_LOG", str(msg))
end

-- Delay helper  
function delay(ms)
    main("CFL_DELAY", int(ms))
end

-- ============================================================================
-- BEGIN MODULE
-- ============================================================================
local mod = start_module("chain_flow_dsl_tests", { is_64bit = false })


local tree1 = start_tree("s_expression_test_1")
    
    local main_pipeline = pipeline("main")
        local n = if_then_else("check")  -- Changed from if_then
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
        
            local a = action("then")
                local p = pipeline()
                    log("complex condition met")
                    oneshot("CFL_ENABLE_CHILDREN")
                    quote("SE_CONTINUE")
                end_pipeline(p)
            end_action(a)
            
            local e = action("else")
                local p1 = pipeline()
                    log("complex condition not met")
                    oneshot("CFL_DISABLE_CHILDREN")
                    quote("SE_HALT")
                end_pipeline(p1)
            end_action(e)
        end_if_then_else(n)  -- Changed from end_if_then
        
        quote("SE_HALT")
    end_pipeline(main_pipeline)
end_tree(tree1)

return end_module(mod)