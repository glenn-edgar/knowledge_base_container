-- test_comprehensive.lua
-- Comprehensive test module for S-node engine
-- Tests all DSL constructs and control flows

start_module("test_module")

-- ============================================================================
-- Tree 1: Simple Pipeline
-- Tests: pipeline, oneshot, main, quote
-- Expected: LED_ON -> DELAY(500) -> LED_OFF -> DELAY(500) -> CONTINUE
-- ============================================================================
start_tree("simple_pipeline")
    pipeline("main")
        oneshot("LED_ON")
        main("DELAY", int32(500))
        oneshot("LED_OFF")
        main("DELAY", int32(500))
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("simple_pipeline")

-- ============================================================================
-- Tree 2: If-Then (no else)
-- Tests: if_then, condition, action, bool_fn
-- Expected: If READY -> LOG("ready") -> CONTINUE, else CONTINUE
-- ============================================================================
start_tree("if_then_test")
    pipeline("main")
        if_then("check_ready")
            condition("c")
                bool_fn("IS_READY")
            end_condition("c")
            action("a")
                oneshot("LOG", str("system is ready"))
            end_action("a")
        end_if_then("check_ready")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("if_then_test")

-- ============================================================================
-- Tree 3: If-Then-Else
-- Tests: if_then_else with both branches
-- Expected: If READY -> START, else -> INIT
-- ============================================================================
start_tree("if_else_test")
    if_then_else("startup")
        condition("c")
            bool_fn("IS_READY")
        end_condition("c")
        action("then")
            pipeline("start_seq")
                oneshot("LOG", str("starting"))
                main("START_MOTOR")
                quote("CFL_CONTINUE")
            end_pipeline("start_seq")
        end_action("then")
        action("else")
            pipeline("init_seq")
                oneshot("LOG", str("initializing"))
                main("INIT_SYSTEM")
                quote("CFL_CONTINUE")
            end_pipeline("init_seq")
        end_action("else")
    end_if_then_else("startup")
end_tree("if_else_test")

-- ============================================================================
-- Tree 4: Boolean AND
-- Tests: bool_and with multiple conditions
-- Expected: All must be true for action
-- ============================================================================
start_tree("bool_and_test")
    pipeline("main")
        if_then("all_checks")
            condition("c")
                bool_and("all")
                    bool_fn("IS_READY")
                    bool_fn("IS_CALIBRATED")
                    bool_fn("HAS_POWER")
                end_bool_and("all")
            end_condition("c")
            action("a")
                oneshot("LOG", str("all conditions met"))
            end_action("a")
        end_if_then("all_checks")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("bool_and_test")

-- ============================================================================
-- Tree 5: Boolean OR
-- Tests: bool_or with short-circuit
-- Expected: Any true triggers action
-- ============================================================================
start_tree("bool_or_test")
    pipeline("main")
        if_then("any_error")
            condition("c")
                bool_or("errors")
                    bool_fn("HAS_FAULT")
                    bool_fn("HAS_WARNING")
                    bool_fn("HAS_TIMEOUT")
                end_bool_or("errors")
            end_condition("c")
            action("a")
                pipeline("error_seq")
                    oneshot("LOG", str("error detected"))
                    oneshot("ALARM_ON")
                    quote("CFL_TERMINATE")
                end_pipeline("error_seq")
            end_action("a")
        end_if_then("any_error")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("bool_or_test")

-- ============================================================================
-- Tree 6: Boolean NOT
-- Tests: bool_not negation
-- Expected: Inverts condition
-- ============================================================================
start_tree("bool_not_test")
    pipeline("main")
        if_then("not_ready")
            condition("c")
                bool_not("inv")
                    bool_fn("IS_READY")
                end_bool_not("inv")
            end_condition("c")
            action("a")
                oneshot("LOG", str("not ready yet"))
            end_action("a")
        end_if_then("not_ready")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("bool_not_test")

-- ============================================================================
-- Tree 7: Nested Booleans
-- Tests: Complex nested boolean expression
-- Expected: (READY AND CALIBRATED) OR OVERRIDE
-- ============================================================================
start_tree("nested_bool_test")
    pipeline("main")
        if_then("complex")
            condition("c")
                bool_or("top")
                    bool_and("normal")
                        bool_fn("IS_READY")
                        bool_fn("IS_CALIBRATED")
                    end_bool_and("normal")
                    bool_fn("HAS_OVERRIDE")
                end_bool_or("top")
            end_condition("c")
            action("a")
                oneshot("LOG", str("complex condition met"))
            end_action("a")
        end_if_then("complex")
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("nested_bool_test")

-- ============================================================================
-- Tree 8: Cond (multi-way)
-- Tests: cond with multiple clauses and default
-- Expected: First matching clause executes
-- ============================================================================
start_tree("cond_test")
    cond("state_check")
        clause("error")
            condition("c1")
                bool_fn("HAS_FAULT")
            end_condition("c1")
            action("a1")
                pipeline("err_seq")
                    oneshot("LOG", str("fault detected"))
                    oneshot("ALARM_ON")
                    quote("CFL_TERMINATE")
                end_pipeline("err_seq")
            end_action("a1")
        end_clause("error")
        clause("warning")
            condition("c2")
                bool_fn("HAS_WARNING")
            end_condition("c2")
            action("a2")
                pipeline("warn_seq")
                    oneshot("LOG", str("warning detected"))
                    quote("CFL_HALT")
                end_pipeline("warn_seq")
            end_action("a2")
        end_clause("warning")
        clause("timeout")
            condition("c3")
                bool_fn("HAS_TIMEOUT")
            end_condition("c3")
            action("a3")
                pipeline("timeout_seq")
                    oneshot("LOG", str("timeout"))
                    quote("CFL_RESET")
                end_pipeline("timeout_seq")
            end_action("a3")
        end_clause("timeout")
        default_clause("ok")
            action("a4")
                quote("CFL_CONTINUE")
            end_action("a4")
        end_default_clause("ok")
    end_cond("state_check")
end_tree("cond_test")

-- ============================================================================
-- Tree 9: Dispatch (event routing)
-- Tests: dispatch with case matching
-- Expected: Match event to case, execute action
-- ============================================================================
start_tree("dispatch_test")
    dispatch("event_handler", "event_type")
        case("init", str("EVT_INIT"))
            action("a1")
                pipeline("init_seq")
                    oneshot("LOG", str("init event"))
                    main("INIT_SYSTEM")
                    quote("CFL_CONTINUE")
                end_pipeline("init_seq")
            end_action("a1")
        end_case("init")
        case("start", str("EVT_START"))
            action("a2")
                pipeline("start_seq")
                    oneshot("LOG", str("start event"))
                    main("START_MOTOR")
                    quote("CFL_CONTINUE")
                end_pipeline("start_seq")
            end_action("a2")
        end_case("start")
        case("stop", str("EVT_STOP"))
            action("a3")
                pipeline("stop_seq")
                    oneshot("LOG", str("stop event"))
                    main("STOP_MOTOR")
                    quote("CFL_HALT")
                end_pipeline("stop_seq")
            end_action("a3")
        end_case("stop")
        default_case("unknown")
            action("a4")
                pipeline("unknown_seq")
                    oneshot("LOG", str("unknown event"))
                    quote("CFL_CONTINUE")
                end_pipeline("unknown_seq")
            end_action("a4")
        end_default_case("unknown")
    end_dispatch("event_handler")
end_tree("dispatch_test")

-- ============================================================================
-- Tree 10: Debug wrapper
-- Tests: dbg node wrapping pipeline
-- Expected: Debug message, then execute child
-- ============================================================================
start_tree("debug_test")
    dbg("trace", "entering debug_test tree")
        pipeline("main")
            oneshot("LOG", str("inside debug wrapper"))
            quote("CFL_CONTINUE")
        end_pipeline("main")
    end_dbg("trace")
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
        main("DELAY", int32(1000))
        oneshot("LOG", str("delay complete"))
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
        main("TEST_PARAMS", int32(-42), uint32(0xDEADBEEF), float32(3.14159), str("hello"))
        quote("CFL_CONTINUE")
    end_pipeline("main")
end_tree("param_types_test")

-- ============================================================================
-- Tree 14: Empty pipeline
-- Tests: Empty pipeline returns CONTINUE
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
        if_then("check1")
            condition("c1")
                bool_and("and1")
                    bool_fn("IS_READY")
                    bool_or("or1")
                        bool_fn("HAS_POWER")
                        bool_not("not1")
                            bool_fn("HAS_FAULT")
                        end_bool_not("not1")
                    end_bool_or("or1")
                end_bool_and("and1")
            end_condition("c1")
            action("a1")
                pipeline("level2")
                    cond("inner_cond")
                        clause("c1")
                            condition("cc1")
                                bool_fn("IS_CALIBRATED")
                            end_condition("cc1")
                            action("ca1")
                                oneshot("LOG", str("calibrated path"))
                            end_action("ca1")
                        end_clause("c1")
                        default_clause("c2")
                            action("ca2")
                                oneshot("LOG", str("default path"))
                            end_action("ca2")
                        end_default_clause("c2")
                    end_cond("inner_cond")
                end_pipeline("level2")
            end_action("a1")
        end_if_then("check1")
        quote("CFL_CONTINUE")
    end_pipeline("level1")
end_tree("deep_nest_test")

return end_module("test_module")