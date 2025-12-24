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
    oneshot("LOG", str(msg))
end

-- Delay helper  
function delay(ms)
    main("DELAY", int(ms))
end

-- ============================================================================
-- BEGIN MODULE
-- ============================================================================
start_module("chain_flow_dsl_tests", { is_64bit = false })


start_tree("s_expression_test_1")
    local n = gensym("check")
    pipeline("main")
        if_then(n)
            local c = gensym("cond")
            condition(c)
                local top = gensym("or")
                bool_or(top)
                    local inner = gensym("and_1")
                    bool_and(inner)
                        bool_fn("CFL_READ_BIT 0")
                        bool_fn("CFL_READ_BIT 1")
                    end_bool_and(inner)
                    local inner_2 = gensym("and_2")
                    bool_and(inner_2)
                        bool_fn("CFL_READ_BIT 2")
                        bool_fn("CFL_READ_BIT 3")
                    end_bool_and(inner_2)
                end_bool_or(top)
            end_condition(c)
            local a = gensym("act")
            action(a)
                log("complex condition met")
                quote("CFL_CONTINUE")
            end_action(a)
        end_if_then(n)
        quote("")
    end_pipeline("main")
end_tree("nested_bool_test")


return end_module("chain_flow_dsl_tests")