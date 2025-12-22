-- test_comprehensive.lua
-- Comprehensive test module for S-node engine
-- Tests all DSL constructs and control flows
-- Uses gensym() for auto-generated internal names

dofile("s_expr_dsl.lua")

-- ============================================================================
-- GENSYM HELPER (if not in DSL yet)
-- ============================================================================
local _gensym_counter = 0
function gensym(prefix)
    _gensym_counter = _gensym_counter + 1
    return (prefix or "g") .. "_" .. _gensym_counter
end

-- ============================================================================
-- MACRO HELPERS - reusable patterns
-- ============================================================================

-- Simple if-then pattern
function when(pred_name, action_fn)
    local n = gensym("when")
    if_then(n)
        condition(gensym("c"))
            bool_fn(pred_name)
        end_condition(gensym("c"))
        action(gensym("a"))
            action_fn()
        end_action(gensym("a"))
    end_if_then(n)
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
start_module("test_module", { is_64bit = false })

-- ============================================================================
-- Tree 1: Simple Pipeline
-- Tests: pipeline, oneshot, main, quote
-- Expected: LED_ON -> DELAY(500) -> LED_OFF -> DELAY(500) -> CONTINUE
-- ============================================================================
start_tree("simple_pipeline")
    pipeline("main")
        oneshot("LED_ON")
        delay(500)
        oneshot("LED_OFF")
        delay(500)
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("simple_pipeline")

-- ============================================================================
-- Tree 2: If-Then (no else)
-- Tests: if_then, condition, action, bool_fn
-- Expected: If READY -> LOG("ready") -> CONTINUE, else CONTINUE
-- ============================================================================
start_tree("if_then_test")
    local n = gensym("check")
    pipeline("main")
        if_then(n)
            local c = gensym("cond")
            condition(c)
                bool_fn("IS_READY")
            end_condition(c)
            local a = gensym("act")
            action(a)
                log("system is ready")
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("if_then_test")

-- ============================================================================
-- Tree 3: If-Then-Else
-- Tests: if_then_else with both branches
-- Expected: If READY -> START, else -> INIT
-- ============================================================================
start_tree("if_else_test")
    local n = gensym("startup")
    if_then_else(n)
        local c = gensym("cond")
        condition(c)
            bool_fn("IS_READY")
        end_condition(c)
        
        local t = gensym("then")
        action(t)
            local p = gensym("pipe")
            pipeline(p)
                log("starting")
                main("START_MOTOR")
                quote("CFL_CONTINUE")
            end_pipeline(p)
        end_action(t)
        
        local e = gensym("else")
        action(e)
            local p = gensym("pipe")
            pipeline(p)
                log("initializing")
                main("INIT_SYSTEM")
                quote("CFL_CONTINUE")
            end_pipeline(p)
        end_action(e)
    end_if_then_else(n)
end_tree("if_else_test")

-- ============================================================================
-- Tree 4: Boolean AND
-- Tests: bool_and with multiple conditions
-- Expected: All must be true for action
-- ============================================================================
start_tree("bool_and_test")
    local n = gensym("check")
    pipeline("main")
        if_then(n)
            local c = gensym("cond")
            condition(c)
                local ba = gensym("and")
                bool_and(ba)
                    bool_fn("IS_READY")
                    bool_fn("IS_CALIBRATED")
                    bool_fn("HAS_POWER")
                end_bool_and(ba)
            end_condition(c)
            local a = gensym("act")
            action(a)
                log("all conditions met")
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("bool_and_test")

-- ============================================================================
-- Tree 5: Boolean OR
-- Tests: bool_or with short-circuit
-- Expected: Any true triggers action
-- ============================================================================
start_tree("bool_or_test")
    local n = gensym("check")
    pipeline("main")
        if_then(n)
            local c = gensym("cond")
            condition(c)
                local bo = gensym("or")
                bool_or(bo)
                    bool_fn("HAS_FAULT")
                    bool_fn("HAS_WARNING")
                    bool_fn("HAS_TIMEOUT")
                end_bool_or(bo)
            end_condition(c)
            local a = gensym("act")
            action(a)
                local p = gensym("pipe")
                pipeline(p)
                    log("error detected")
                    oneshot("ALARM_ON")
                    quote("CFL_TERMINATE")
                end_pipeline(p)
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("bool_or_test")

-- ============================================================================
-- Tree 6: Boolean NOT
-- Tests: bool_not negation
-- Expected: Inverts condition
-- ============================================================================
start_tree("bool_not_test")
    local n = gensym("check")
    pipeline("main")
        if_then(n)
            local c = gensym("cond")
            condition(c)
                local bn = gensym("not")
                bool_not(bn)
                    bool_fn("IS_READY")
                end_bool_not(bn)
            end_condition(c)
            local a = gensym("act")
            action(a)
                log("not ready yet")
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("bool_not_test")

-- ============================================================================
-- Tree 7: Nested Booleans
-- Tests: Complex nested boolean expression
-- Expected: (READY AND CALIBRATED) OR OVERRIDE
-- ============================================================================
start_tree("nested_bool_test")
    local n = gensym("check")
    pipeline("main")
        if_then(n)
            local c = gensym("cond")
            condition(c)
                local top = gensym("or")
                bool_or(top)
                    local inner = gensym("and")
                    bool_and(inner)
                        bool_fn("IS_READY")
                        bool_fn("IS_CALIBRATED")
                    end_bool_and(inner)
                    bool_fn("HAS_OVERRIDE")
                end_bool_or(top)
            end_condition(c)
            local a = gensym("act")
            action(a)
                log("complex condition met")
            end_action(a)
        end_if_then(n)
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("nested_bool_test")

-- ============================================================================
-- Tree 8: Cond (multi-way)
-- Tests: cond with multiple clauses and default
-- Expected: First matching clause executes
-- ============================================================================
start_tree("cond_test")
    local cn = gensym("cond")
    cond(cn)
        -- Error clause
        local cl1 = gensym("clause")
        clause(cl1)
            local c1 = gensym("c")
            condition(c1)
                bool_fn("HAS_FAULT")
            end_condition(c1)
            local a1 = gensym("a")
            action(a1)
                local p = gensym("pipe")
                pipeline(p)
                    log("fault detected")
                    oneshot("ALARM_ON")
                    quote("CFL_TERMINATE")
                end_pipeline(p)
            end_action(a1)
        end_clause(cl1)
        
        -- Warning clause
        local cl2 = gensym("clause")
        clause(cl2)
            local c2 = gensym("c")
            condition(c2)
                bool_fn("HAS_WARNING")
            end_condition(c2)
            local a2 = gensym("a")
            action(a2)
                local p = gensym("pipe")
                pipeline(p)
                    log("warning detected")
                    quote("CFL_HALT")
                end_pipeline(p)
            end_action(a2)
        end_clause(cl2)
        
        -- Timeout clause
        local cl3 = gensym("clause")
        clause(cl3)
            local c3 = gensym("c")
            condition(c3)
                bool_fn("HAS_TIMEOUT")
            end_condition(c3)
            local a3 = gensym("a")
            action(a3)
                local p = gensym("pipe")
                pipeline(p)
                    log("timeout")
                    quote("CFL_RESET")
                end_pipeline(p)
            end_action(a3)
        end_clause(cl3)
        
        -- Default
        local def = gensym("default")
        default_clause(def)
            local ad = gensym("a")
            action(ad)
                quote("CFL_CONTINUE")
            end_action(ad)
        end_default_clause(def)
    end_cond(cn)
end_tree("cond_test")

-- ============================================================================
-- Tree 9: Dispatch (event routing)
-- Tests: dispatch with case matching
-- Expected: Match event to case, execute action
-- ============================================================================
start_tree("dispatch_test")
    local d = gensym("dispatch")
    dispatch(d, "event_type")
        -- Init event
        local cs1 = gensym("case")
        case(cs1, "EVT_INIT")
            local a1 = gensym("a")
            action(a1)
                local p = gensym("pipe")
                pipeline(p)
                    log("init event")
                    main("INIT_SYSTEM")
                    quote("CFL_CONTINUE")
                end_pipeline(p)
            end_action(a1)
        end_case(cs1)
        
        -- Start event
        local cs2 = gensym("case")
        case(cs2, "EVT_START")
            local a2 = gensym("a")
            action(a2)
                local p = gensym("pipe")
                pipeline(p)
                    log("start event")
                    main("START_MOTOR")
                    quote("CFL_CONTINUE")
                end_pipeline(p)
            end_action(a2)
        end_case(cs2)
        
        -- Stop event
        local cs3 = gensym("case")
        case(cs3, "EVT_STOP")
            local a3 = gensym("a")
            action(a3)
                local p = gensym("pipe")
                pipeline(p)
                    log("stop event")
                    main("STOP_MOTOR")
                    quote("CFL_HALT")
                end_pipeline(p)
            end_action(a3)
        end_case(cs3)
        
        -- Default
        local def = gensym("default")
        default_case(def)
            local ad = gensym("a")
            action(ad)
                local p = gensym("pipe")
                pipeline(p)
                    log("unknown event")
                    quote("CFL_CONTINUE")
                end_pipeline(p)
            end_action(ad)
        end_default_case(def)
    end_dispatch(d)
end_tree("dispatch_test")

-- ============================================================================
-- Tree 10: Debug wrapper
-- Tests: dbg node wrapping pipeline
-- Expected: Debug message, then execute child
-- ============================================================================
start_tree("debug_test")
    local d = gensym("dbg")
    dbg(d, "entering debug_test tree")
        pipeline("main")
            log("inside debug wrapper")
            quote("CFL_CONTINUE")
        end_pipeline("main")
    end_dbg(d)
end_tree("debug_test")

-- ============================================================================
-- Tree 11: Oneshot behavior
-- Tests: oneshot only fires once per reset
-- Expected: First tick fires, subsequent ticks skip
-- ============================================================================
start_tree("oneshot_test")
    pipeline("main")
        oneshot("INCREMENT_COUNTER")
        oneshot("INCREMENT_COUNTER")
        oneshot("INCREMENT_COUNTER")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("oneshot_test")

-- ============================================================================
-- Tree 12: Main function returns
-- Tests: main function control code propagation
-- Expected: DELAY returns HALT until done, then CONTINUE
-- ============================================================================
start_tree("main_return_test")
    pipeline("main")
        delay(1000)
        log("delay complete")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("main_return_test")

-- ============================================================================
-- Tree 13: Parameter types
-- Tests: All parameter types
-- Expected: Function receives correct param types
-- ============================================================================
start_tree("param_types_test")
    pipeline("main")
        main("TEST_PARAMS", int(-42), uint(0xDEADBEEF), flt(3.14159), str("hello"))
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("param_types_test")

-- ============================================================================
-- Tree 14: Empty pipeline (minimal)
-- Tests: Minimal pipeline returns CONTINUE
-- Expected: Immediate CONTINUE
-- ============================================================================
start_tree("empty_pipeline_test")
    pipeline("main")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("empty_pipeline_test")

-- ============================================================================
-- Tree 15: Deeply nested
-- Tests: Deep nesting of structures
-- Expected: Proper evaluation through all levels
-- ============================================================================
start_tree("deep_nest_test")
    pipeline("level1")
        local n1 = gensym("if")
        if_then(n1)
            local c1 = gensym("c")
            condition(c1)
                local and1 = gensym("and")
                bool_and(and1)
                    bool_fn("IS_READY")
                    local or1 = gensym("or")
                    bool_or(or1)
                        bool_fn("HAS_POWER")
                        local not1 = gensym("not")
                        bool_not(not1)
                            bool_fn("HAS_FAULT")
                        end_bool_not(not1)
                    end_bool_or(or1)
                end_bool_and(and1)
            end_condition(c1)
            local a1 = gensym("a")
            action(a1)
                pipeline("level2")
                    local cn = gensym("cond")
                    cond(cn)
                        local cl1 = gensym("clause")
                        clause(cl1)
                            local cc1 = gensym("c")
                            condition(cc1)
                                bool_fn("IS_CALIBRATED")
                            end_condition(cc1)
                            local ca1 = gensym("a")
                            action(ca1)
                                log("calibrated path")
                            end_action(ca1)
                        end_clause(cl1)
                        local def = gensym("default")
                        default_clause(def)
                            local ca2 = gensym("a")
                            action(ca2)
                                log("default path")
                            end_action(ca2)
                        end_default_clause(def)
                    end_cond(cn)
                end_pipeline("level2")
            end_action(a1)
        end_if_then(n1)
        quote("CFL_CONTINUE")
    end_pipeline("level1")
end_tree("deep_nest_test")

-- ============================================================================
-- Tree 16: Brace data lists
-- Tests: PARAM_OPEN for data lists
-- ============================================================================
start_tree("data_list_test")
    pipeline("main")
        main("PROCESS_ARRAY",
            open_brace(),
                int(10), int(20), int(30), int(40),
            close_brace()
        )
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("data_list_test")

-- ============================================================================
-- Tree 17: Callable S-expressions
-- Tests: PARAM_OPEN_CALL for executable S-exprs
-- ============================================================================
start_tree("sexpr_test")
    pipeline("main")
        -- (ADD 1 2) -> callable because first element is function ref
        main("EVAL",
            open_brace(),
                main_ref("ADD"),
                int(1),
                int(2),
            close_brace()
        )
        -- Nested: (MUL (ADD 1 2) (SUB 10 3))
        main("EVAL_NESTED",
            open_brace(),
                main_ref("MUL"),
                open_brace(),
                    main_ref("ADD"),
                    int(1),
                    int(2),
                close_brace(),
                open_brace(),
                    main_ref("SUB"),
                    int(10),
                    int(3),
                close_brace(),
            close_brace()
        )
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("sexpr_test")

-- ============================================================================
-- Tree 18: Function references
-- Tests: main_ref, oneshot_ref, pred_ref parameters
-- ============================================================================
start_tree("func_ref_test")
    pipeline("main")
        main("REGISTER_CALLBACKS",
            main_ref("ON_SUCCESS"),
            main_ref("ON_FAILURE"),
            oneshot_ref("CLEANUP")
        )
        main("FILTER",
            pred_ref("IS_VALID"),
            open_brace(),
                int(1), int(2), int(3), int(4), int(5),
            close_brace()
        )
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("func_ref_test")

return end_module("test_module")