-- test_comprehensive.lua
-- Comprehensive test module for S-node engine
-- Tests all DSL constructs and control flows
-- Updated for DSL v2.6 with slotted blackboards

-- ============================================================================
-- MACRO HELPERS - reusable patterns
-- ============================================================================

-- Simple if-then pattern
function when(pred_name, action_fn)
    local n = if_then("when")
        local c = condition()
            bool_fn(pred_name)
        end_condition(c)
        local a = action()
            action_fn()
        end_action(a)
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
local mod = start_module("test_module", { is_64bit = false })

-- ============================================================================
-- POOL DEFINITIONS (Slotted Blackboards)
-- ============================================================================

-- Motor control pool
defpool("motor_state", "motor_state_t")
defslot("motor_main", "motor_state")
defslot("motor_aux", "motor_state")

-- LED control pool
defpool("led_state", "led_state_t")
defslot("led_status", "led_state")
defslot("led_alarm", "led_state")

-- System state pool
defpool("system_state", "system_state_t")
defslot("sys_main", "system_state")

-- Alarm pool
defpool("alarm_state", "alarm_state_t")
defslot("alarm_main", "alarm_state")

-- Counter pool
defpool("counter_state", "counter_state_t")
defslot("counter_a", "counter_state")
defslot("counter_b", "counter_state")

-- ============================================================================
-- Tree 1: Simple Pipeline
-- Tests: pipeline, oneshot, main, quote, slot_ref
-- Expected: LED_ON -> DELAY(500) -> LED_OFF -> DELAY(500) -> CONTINUE
-- ============================================================================
local tree1 = start_tree("simple_pipeline")
    local p = pipeline("main")
        oneshot("LED_ON", slot_ref("led_status"))
        delay(500)
        oneshot("LED_OFF", slot_ref("led_status"))
        delay(500)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree1)

-- ============================================================================
-- Tree 2: If-Then (no else)
-- Tests: if_then, condition, action, bool_fn
-- Expected: If READY -> LOG("ready") -> CONTINUE, else CONTINUE
-- ============================================================================
local tree2 = start_tree("if_then_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                bool_fn("IS_READY", slot_ref("sys_main"))
            end_condition(c)
            local a = action()
                log("system is ready")
            end_action(a)
        end_if_then(n)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree2)

-- ============================================================================
-- Tree 3: If-Then-Else
-- Tests: if_then_else with both branches
-- Expected: If READY -> START, else -> INIT
-- ============================================================================
local tree3 = start_tree("if_else_test")
    local n = if_then_else("startup")
        local c = condition()
            bool_fn("IS_READY", slot_ref("sys_main"))
        end_condition(c)
        
        local t = action("then")
            local p = pipeline()
                log("starting")
                main("START_MOTOR", slot_ref("motor_main"))
                quote("SE_CONTINUE")
            end_pipeline(p)
        end_action(t)
        
        local e = action("else")
            local p = pipeline()
                log("initializing")
                main("INIT_SYSTEM", slot_ref("sys_main"))
                quote("SE_CONTINUE")
            end_pipeline(p)
        end_action(e)
    end_if_then_else(n)
end_tree(tree3)

-- ============================================================================
-- Tree 4: Boolean AND
-- Tests: bool_and with multiple conditions
-- Expected: All must be true for action
-- ============================================================================
local tree4 = start_tree("bool_and_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local ba = bool_and()
                    bool_fn("IS_READY", slot_ref("sys_main"))
                    bool_fn("IS_CALIBRATED", slot_ref("sys_main"))
                    bool_fn("HAS_POWER", slot_ref("sys_main"))
                end_bool_and(ba)
            end_condition(c)
            local a = action()
                log("all conditions met")
            end_action(a)
        end_if_then(n)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree4)

-- ============================================================================
-- Tree 5: Boolean OR
-- Tests: bool_or with short-circuit
-- Expected: Any true triggers action
-- ============================================================================
local tree5 = start_tree("bool_or_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local bo = bool_or()
                    bool_fn("HAS_FAULT", slot_ref("sys_main"))
                    bool_fn("HAS_WARNING", slot_ref("sys_main"))
                    bool_fn("HAS_TIMEOUT", slot_ref("sys_main"))
                end_bool_or(bo)
            end_condition(c)
            local a = action()
                local pp = pipeline()
                    log("error detected")
                    oneshot("ALARM_ON", slot_ref("alarm_main"))
                    quote("SE_TERMINATE")
                end_pipeline(pp)
            end_action(a)
        end_if_then(n)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree5)

-- ============================================================================
-- Tree 6: Boolean NOT
-- Tests: bool_not negation
-- Expected: Inverts condition
-- ============================================================================
local tree6 = start_tree("bool_not_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local bn = bool_not()
                    bool_fn("IS_READY", slot_ref("sys_main"))
                end_bool_not(bn)
            end_condition(c)
            local a = action()
                log("not ready yet")
            end_action(a)
        end_if_then(n)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree6)

-- ============================================================================
-- Tree 7: Nested Booleans
-- Tests: Complex nested boolean expression
-- Expected: (READY AND CALIBRATED) OR OVERRIDE
-- ============================================================================
local tree7 = start_tree("nested_bool_test")
    local p = pipeline("main")
        local n = if_then("check")
            local c = condition()
                local top = bool_or()
                    local inner = bool_and()
                        bool_fn("IS_READY", slot_ref("sys_main"))
                        bool_fn("IS_CALIBRATED", slot_ref("sys_main"))
                    end_bool_and(inner)
                    bool_fn("HAS_OVERRIDE", slot_ref("sys_main"))
                end_bool_or(top)
            end_condition(c)
            local a = action()
                log("complex condition met")
            end_action(a)
        end_if_then(n)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree7)

-- ============================================================================
-- Tree 8: Cond (multi-way)
-- Tests: cond with multiple clauses and default
-- Expected: First matching clause executes
-- ============================================================================
local tree8 = start_tree("cond_test")
    local cn = cond()
        -- Error clause
        local cl1 = clause()
            local c1 = condition()
                bool_fn("HAS_FAULT", slot_ref("sys_main"))
            end_condition(c1)
            local a1 = action()
                local p = pipeline()
                    log("fault detected")
                    oneshot("ALARM_ON", slot_ref("alarm_main"))
                    quote("SE_TERMINATE")
                end_pipeline(p)
            end_action(a1)
        end_clause(cl1)
        
        -- Warning clause
        local cl2 = clause()
            local c2 = condition()
                bool_fn("HAS_WARNING", slot_ref("sys_main"))
            end_condition(c2)
            local a2 = action()
                local p = pipeline()
                    log("warning detected")
                    quote("SE_HALT")
                end_pipeline(p)
            end_action(a2)
        end_clause(cl2)
        
        -- Timeout clause
        local cl3 = clause()
            local c3 = condition()
                bool_fn("HAS_TIMEOUT", slot_ref("sys_main"))
            end_condition(c3)
            local a3 = action()
                local p = pipeline()
                    log("timeout")
                    quote("SE_RESET")
                end_pipeline(p)
            end_action(a3)
        end_clause(cl3)
        
        -- Default
        local def = default_clause()
            local ad = action()
                quote("SE_CONTINUE")
            end_action(ad)
        end_default_clause(def)
    end_cond(cn)
end_tree(tree8)

-- ============================================================================
-- Tree 9: Dispatch (event routing)
-- Tests: dispatch with case matching
-- Expected: Match event to case, execute action
-- ============================================================================
local tree9 = start_tree("dispatch_test")
    local d = dispatch("event_type")
        -- Init event
        local cs1 = case("EVT_INIT")
            local a1 = action()
                local p = pipeline()
                    log("init event")
                    main("INIT_SYSTEM", slot_ref("sys_main"))
                    quote("SE_CONTINUE")
                end_pipeline(p)
            end_action(a1)
        end_case(cs1)
        
        -- Start event
        local cs2 = case("EVT_START")
            local a2 = action()
                local p = pipeline()
                    log("start event")
                    main("START_MOTOR", slot_ref("motor_main"))
                    quote("SE_CONTINUE")
                end_pipeline(p)
            end_action(a2)
        end_case(cs2)
        
        -- Stop event
        local cs3 = case("EVT_STOP")
            local a3 = action()
                local p = pipeline()
                    log("stop event")
                    main("STOP_MOTOR", slot_ref("motor_main"))
                    quote("SE_HALT")
                end_pipeline(p)
            end_action(a3)
        end_case(cs3)
        
        -- Default
        local def = default_case()
            local ad = action()
                local p = pipeline()
                    log("unknown event")
                    quote("SE_CONTINUE")
                end_pipeline(p)
            end_action(ad)
        end_default_case(def)
    end_dispatch(d)
end_tree(tree9)

-- ============================================================================
-- Tree 10: Debug wrapper
-- Tests: dbg node wrapping pipeline
-- Expected: Debug message, then execute child
-- ============================================================================
local tree10 = start_tree("debug_test")
    local d = dbg("entering debug_test tree")
        local p = pipeline("main")
            log("inside debug wrapper")
            quote("SE_CONTINUE")
        end_pipeline(p)
    end_dbg(d)
end_tree(tree10)

-- ============================================================================
-- Tree 11: Oneshot behavior with slots
-- Tests: oneshot only fires once per reset, different slots independent
-- Expected: Each slot increments independently
-- ============================================================================
local tree11 = start_tree("oneshot_test")
    local p = pipeline("main")
        oneshot("INCREMENT_COUNTER", slot_ref("counter_a"))
        oneshot("INCREMENT_COUNTER", slot_ref("counter_a"))
        oneshot("INCREMENT_COUNTER", slot_ref("counter_b"))
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree11)

-- ============================================================================
-- Tree 12: Main function returns
-- Tests: main function control code propagation
-- Expected: DELAY returns HALT until done, then CONTINUE
-- ============================================================================
local tree12 = start_tree("main_return_test")
    local p = pipeline("main")
        delay(1000)
        log("delay complete")
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree12)

-- ============================================================================
-- Tree 13: Parameter types with slot
-- Tests: All parameter types including slot_ref
-- Expected: Function receives correct param types
-- ============================================================================
local tree13 = start_tree("param_types_test")
    local p = pipeline("main")
        main("TEST_PARAMS", 
            slot_ref("sys_main"),
            int(-42), 
            uint(0xDEADBEEF), 
            flt(3.14159), 
            str("hello"))
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree13)

-- ============================================================================
-- Tree 14: Empty pipeline (minimal)
-- Tests: Minimal pipeline returns CONTINUE
-- Expected: Immediate CONTINUE
-- ============================================================================
local tree14 = start_tree("empty_pipeline_test")
    local p = pipeline("main")
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree14)

-- ============================================================================
-- Tree 15: Deeply nested
-- Tests: Deep nesting of structures
-- Expected: Proper evaluation through all levels
-- ============================================================================
local tree15 = start_tree("deep_nest_test")
    local p1 = pipeline("level1")
        local n1 = if_then()
            local c1 = condition()
                local and1 = bool_and()
                    bool_fn("IS_READY", slot_ref("sys_main"))
                    local or1 = bool_or()
                        bool_fn("HAS_POWER", slot_ref("sys_main"))
                        local not1 = bool_not()
                            bool_fn("HAS_FAULT", slot_ref("sys_main"))
                        end_bool_not(not1)
                    end_bool_or(or1)
                end_bool_and(and1)
            end_condition(c1)
            local a1 = action()
                local p2 = pipeline("level2")
                    local cn = cond()
                        local cl1 = clause()
                            local cc1 = condition()
                                bool_fn("IS_CALIBRATED", slot_ref("sys_main"))
                            end_condition(cc1)
                            local ca1 = action()
                                log("calibrated path")
                            end_action(ca1)
                        end_clause(cl1)
                        local def = default_clause()
                            local ca2 = action()
                                log("default path")
                            end_action(ca2)
                        end_default_clause(def)
                    end_cond(cn)
                end_pipeline(p2)
            end_action(a1)
        end_if_then(n1)
        quote("SE_CONTINUE")
    end_pipeline(p1)
end_tree(tree15)

-- ============================================================================
-- Tree 16: Brace data lists
-- Tests: PARAM_OPEN for data lists
-- ============================================================================
local tree16 = start_tree("data_list_test")
    local p = pipeline("main")
        main("PROCESS_ARRAY",
            slot_ref("sys_main"),
            open_brace(),
                int(10), int(20), int(30), int(40),
            close_brace()
        )
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree16)

-- ============================================================================
-- Tree 17: Callable S-expressions
-- Tests: PARAM_OPEN_CALL for executable S-exprs
-- ============================================================================
local tree17 = start_tree("sexpr_test")
    local p = pipeline("main")
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
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree17)

-- ============================================================================
-- Tree 18: Function references with slots
-- Tests: main_ref, oneshot_ref, pred_ref parameters combined with slots
-- ============================================================================
local tree18 = start_tree("func_ref_test")
    local p = pipeline("main")
        main("REGISTER_CALLBACKS",
            slot_ref("sys_main"),
            main_ref("ON_SUCCESS"),
            main_ref("ON_FAILURE"),
            oneshot_ref("CLEANUP")
        )
        main("FILTER",
            slot_ref("sys_main"),
            pred_ref("IS_VALID"),
            open_brace(),
                int(1), int(2), int(3), int(4), int(5),
            close_brace()
        )
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree18)

-- ============================================================================
-- Tree 19: Multi-slot operations
-- Tests: Same function operating on different slots
-- ============================================================================
local tree19 = start_tree("multi_slot_test")
    local p = pipeline("main")
        -- Same function, different blackboard slots
        main("START_MOTOR", slot_ref("motor_main"))
        main("START_MOTOR", slot_ref("motor_aux"))
        
        -- Check both motors
        local n = if_then("check_motors")
            local c = condition()
                local ba = bool_and()
                    bool_fn("IS_RUNNING", slot_ref("motor_main"))
                    bool_fn("IS_RUNNING", slot_ref("motor_aux"))
                end_bool_and(ba)
            end_condition(c)
            local a = action()
                log("both motors running")
            end_action(a)
        end_if_then(n)
        
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree19)

-- ============================================================================
-- Tree 20: Cross-pool operations
-- Tests: Single tree using multiple pools
-- ============================================================================
local tree20 = start_tree("cross_pool_test")
    local p = pipeline("main")
        -- Check system state
        local n = if_then("startup_check")
            local c = condition()
                bool_fn("IS_READY", slot_ref("sys_main"))
            end_condition(c)
            local a = action()
                local pp = pipeline()
                    -- Control motor
                    main("START_MOTOR", slot_ref("motor_main"))
                    -- Set LED
                    oneshot("LED_ON", slot_ref("led_status"))
                    -- Increment counter
                    oneshot("INCREMENT_COUNTER", slot_ref("counter_a"))
                end_pipeline(pp)
            end_action(a)
        end_if_then(n)
        quote("SE_CONTINUE")
    end_pipeline(p)
end_tree(tree20)

-- ============================================================================
-- END MODULE
-- ============================================================================
return end_module(mod)