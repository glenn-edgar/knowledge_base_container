--============================================================================
-- test_dsl_constructs.lua
-- Demonstrates ChainTree DSL v2.2 constructs
--============================================================================

dofile("s_expr_dsl.lua")

start_module("test_constructs", { is_64bit = true })

--============================================================================
-- TREE 1: Basic pipeline with oneshot and main functions
--============================================================================
start_tree("basic_pipeline")

    pipeline("init_sequence")
        oneshot("init_hardware")
        oneshot("load_config", str("config.json"))
        main("run_diagnostics", int(100), flt(3.14))
        oneshot("log_status", str("initialized"))
    end_pipeline("init_sequence")

end_tree("basic_pipeline")

--============================================================================
-- TREE 2: If-then construct
--============================================================================
start_tree("simple_if")

    if_then("check_sensor")
        condition("sensor_ready")
            bool_fn("is_sensor_ready", int(0))
        end_condition("sensor_ready")
        action("read_it")
            oneshot("read_sensor", int(0))
        end_action("read_it")
    end_if_then("check_sensor")

end_tree("simple_if")

--============================================================================
-- TREE 3: If-then-else construct
--============================================================================
start_tree("if_else_example")

    if_then_else("check_threshold")
        condition("above_limit")
            bool_fn("value_above", int(100))
        end_condition("above_limit")
        action("handle_high")
            oneshot("trigger_alarm")
        end_action("handle_high")
        action("handle_normal")
            oneshot("log_normal")
        end_action("handle_normal")
    end_if_then_else("check_threshold")

end_tree("if_else_example")

--============================================================================
-- TREE 4: Cond (multi-way branch)
--============================================================================
start_tree("cond_example")

    cond("state_machine")
        clause("idle_state")
            condition("is_idle")
                bool_fn("check_state", str("idle"))
            end_condition("is_idle")
            action("do_idle")
                oneshot("process_idle")
            end_action("do_idle")
        end_clause("idle_state")
        
        clause("running_state")
            condition("is_running")
                bool_fn("check_state", str("running"))
            end_condition("is_running")
            action("do_running")
                pipeline("run_tasks")
                    oneshot("task_a")
                    oneshot("task_b")
                end_pipeline("run_tasks")
            end_action("do_running")
        end_clause("running_state")
        
        clause("error_state")
            condition("is_error")
                bool_fn("check_state", str("error"))
            end_condition("is_error")
            action("do_error")
                oneshot("handle_error")
            end_action("do_error")
        end_clause("error_state")
        
        default_clause("fallback")
            action("do_default")
                oneshot("log_unknown_state")
            end_action("do_default")
        end_default_clause("fallback")
    end_cond("state_machine")

end_tree("cond_example")

--============================================================================
-- TREE 5: Boolean combinators (and, or, not)
--============================================================================
start_tree("boolean_logic")

    if_then("complex_condition")
        condition("all_systems_go")
            bool_and("both_ready")
                bool_fn("sensor_a_ready")
                bool_or("backup_available")
                    bool_fn("sensor_b_ready")
                    bool_fn("sensor_c_ready")
                end_bool_or("backup_available")
            end_bool_and("both_ready")
        end_condition("all_systems_go")
        action("proceed")
            oneshot("start_operation")
        end_action("proceed")
    end_if_then("complex_condition")

end_tree("boolean_logic")

--============================================================================
-- TREE 6: Dispatch (switch-like)
--============================================================================
start_tree("dispatch_example")

    dispatch("command_handler", "cmd")
        case("start_cmd", "start")
            action("do_start")
                oneshot("execute_start")
            end_action("do_start")
        end_case("start_cmd")
        
        case("stop_cmd", "stop")
            action("do_stop")
                oneshot("execute_stop")
            end_action("do_stop")
        end_case("stop_cmd")
        
        case("multi_cmd", {"pause", "suspend"})  -- multiple patterns
            action("do_pause")
                oneshot("execute_pause")
            end_action("do_pause")
        end_case("multi_cmd")
        
        default_case("unknown_cmd")
            action("do_unknown")
                oneshot("log_unknown_command")
            end_action("do_unknown")
        end_default_case("unknown_cmd")
    end_dispatch("command_handler")

end_tree("dispatch_example")

--============================================================================
-- TREE 7: Simple braces - data list (PARAM_OPEN)
--============================================================================
start_tree("data_braces")

    pipeline("process_data")
        -- Data list: first element is NOT a function
        -- Becomes PARAM_OPEN (not callable)
        main("process_array",
            open_brace(),
                int(10),
                int(20),
                int(30),
            close_brace()
        )
        
        -- Nested data lists
        main("process_matrix",
            open_brace(),
                open_brace(),
                    int(1), int(2), int(3),
                close_brace(),
                open_brace(),
                    int(4), int(5), int(6),
                close_brace(),
            close_brace()
        )
    end_pipeline("process_data")

end_tree("data_braces")

--============================================================================
-- TREE 8: Callable braces - S-expressions (PARAM_OPEN_CALL)
--============================================================================
start_tree("callable_braces")

    pipeline("eval_expressions")
        -- Callable S-expr: first element IS a function ref
        -- Becomes PARAM_OPEN_CALL
        main("eval",
            open_brace(),
                main_ref("add"),
                int(1),
                int(2),
            close_brace()
        )
        
        -- Nested callable expressions: (mul (add 1 2) (sub 5 3))
        main("eval_nested",
            open_brace(),
                main_ref("mul"),
                open_brace(),
                    main_ref("add"),
                    int(1),
                    int(2),
                close_brace(),
                open_brace(),
                    main_ref("sub"),
                    int(5),
                    int(3),
                close_brace(),
            close_brace()
        )
    end_pipeline("eval_expressions")

end_tree("callable_braces")

--============================================================================
-- TREE 9: Mixed braces - data and callable together
--============================================================================
start_tree("mixed_braces")

    pipeline("mixed_example")
        -- Function with both data and callable args
        main("map_function",
            -- First arg: the function to apply (callable)
            open_brace(),
                main_ref("square"),
            close_brace(),
            -- Second arg: the data to process (data list)
            open_brace(),
                int(1), int(2), int(3), int(4),
            close_brace()
        )
        
        -- Complex: conditional execution with S-expr
        main("conditional_eval",
            pred_ref("should_run"),           -- predicate to check
            open_brace(),                      -- callable if true
                main_ref("compute"),
                int(100),
            close_brace(),
            open_brace(),                      -- data: fallback values
                int(0),
            close_brace()
        )
    end_pipeline("mixed_example")

end_tree("mixed_braces")

--============================================================================
-- TREE 10: Function references as parameters
--============================================================================
start_tree("function_refs")

    pipeline("callback_example")
        -- Pass function references as callbacks
        main("register_callbacks",
            main_ref("on_success"),
            main_ref("on_failure"),
            oneshot_ref("cleanup")
        )
        
        -- Higher-order: function that takes predicate
        main("find_first",
            pred_ref("is_valid"),
            open_brace(),
                int(1), int(2), int(3), int(4), int(5),
            close_brace()
        )
        
        -- Chain of transformations
        main("pipeline_funcs",
            open_brace(),
                main_ref("parse"),
                main_ref("validate"),
                main_ref("transform"),
                main_ref("output"),
            close_brace()
        )
    end_pipeline("callback_example")

end_tree("function_refs")

--============================================================================
-- TREE 11: Debug wrapper
--============================================================================
start_tree("debug_example")

    dbg("trace_execution", "Entering main pipeline")
        pipeline("traced_ops")
            oneshot("step_1")
            oneshot("step_2")
            oneshot("step_3")
        end_pipeline("traced_ops")
    end_dbg("trace_execution")

end_tree("debug_example")

--============================================================================
-- TREE 12: Deeply nested structure
--============================================================================
start_tree("deeply_nested")

    pipeline("outer")
        if_then_else("level1")
            condition("check1")
                bool_fn("condition_a")
            end_condition("check1")
            action("then1")
                cond("level2")
                    clause("case_x")
                        condition("check_x")
                            bool_and("x_and")
                                bool_fn("x_ready")
                                bool_fn("x_valid")
                            end_bool_and("x_and")
                        end_condition("check_x")
                        action("do_x")
                            main("process_x",
                                open_brace(),
                                    main_ref("transform"),
                                    open_brace(),
                                        int(1), int(2),
                                    close_brace(),
                                close_brace()
                            )
                        end_action("do_x")
                    end_clause("case_x")
                    default_clause("case_default")
                        action("do_default")
                            oneshot("fallback")
                        end_action("do_default")
                    end_default_clause("case_default")
                end_cond("level2")
            end_action("then1")
            action("else1")
                oneshot("skip_processing")
            end_action("else1")
        end_if_then_else("level1")
    end_pipeline("outer")

end_tree("deeply_nested")

--============================================================================
-- Return the module generator
--============================================================================
return end_module("test_constructs")