-- example_64bit.lua
-- Demonstrates 64-bit flag and generic int/uint/float types

dofile("s_expr_dsl.lua")
defpool("motor_state", "motor_state_t")
defslot("motor_main", "motor_state")


start_module("sensor_system")
--use_64bit()
start_tree("main_loop")
    pipeline("process")
        -- Generic types: int, uint, flt
        oneshot("init_sensor", int(-100), uint(200))
        oneshot("set_threshold", flt(3.14159))
        
        -- Function references
        oneshot("set_callback", main_ref("error_handler"))
        oneshot("set_filter", oneshot_ref("apply_filter"))
        oneshot("set_check", pred_ref("is_valid"))
        
        -- S-expression capability with braces
        oneshot("user_expr",
            open_brace(),
                int(1),
                int(2),
                open_brace(),
                    int(3),
                    int(4),
                close_brace(),
            close_brace()
        )
        
        main("run_sensors")
    end_pipeline("process")
end_tree("main_loop")

start_tree("error_handler")
    pipeline("handle")
        oneshot("log_error", str("sensor failure"))
        quote("CFL_HALT")
    end_pipeline("handle")
end_tree("error_handler")

return end_module("sensor_system")