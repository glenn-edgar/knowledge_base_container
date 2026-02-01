local M = require("s_expr_dsl")
local mod = start_module("basic_primitive_test")
use_32bit()
set_debug(true)

start_tree("basic_primitive_test")
    
    se_function_interface(function()
        
        -- Trigger 1: Simple single bit test
        se_trigger_on_change(0,
            function()
                local pred = p_call("TEST_BIT")
                    int(0)  -- bit index
                end_call(pred)
            end,
            function()
                se_chain_flow(function()
                    local rise = o_call("ON_BIT0_RISE")
                    end_call(rise)
                    se_log("ON_BIT0_RISE")
                    se_return_continue()
                end)
            end,
            function()
                se_chain_flow(function()
                    local fall = o_call("ON_BIT0_FALL")
                    end_call(fall)
                    se_log("ON_BIT0_FALL")
                    se_return_continue()
                end)
            end
        )
        
        -- Trigger 2: AND of two bits
        se_trigger_on_change(0,
            function()
                local pred = p_call("SE_PRED_AND")
                    local p1 = p_call("TEST_BIT") int(1) end_call(p1)
                    local p2 = p_call("TEST_BIT") int(2) end_call(p2)
                end_call(pred)
            end,
            function()
                se_chain_flow(function()
                    local rise = o_call("ON_BITS_12_RISE")
                    end_call(rise)
                    se_log("ON_BITS_12_RISE")
                    se_return_continue()
                end)
            end,
            function()
                se_chain_flow(function()
                    local fall = o_call("ON_BITS_12_FALL")
                    end_call(fall)
                    se_log("ON_BITS_12_FALL")
                    se_return_continue()
                end)
            end
        )
        
        -- Trigger 3: OR of two bits
        se_trigger_on_change(0,
            function()
                local pred = p_call("SE_PRED_OR")
                    local p1 = p_call("TEST_BIT") int(3) end_call(p1)
                    local p2 = p_call("TEST_BIT") int(4) end_call(p2)
                end_call(pred)
            end,
            function()
                se_chain_flow(function()
                    local rise = o_call("ON_BITS_34_RISE")
                    end_call(rise)
                    se_log("ON_BITS_34_RISE")
                    se_return_continue()
                end)
            end,
            function()
                se_chain_flow(function()
                    local fall = o_call("ON_BITS_34_FALL")
                    end_call(fall)
                    se_log("ON_BITS_34_FALL")
                    se_return_continue()
                end)
            end
        )
        
        -- Trigger 4: NOT of a bit (inverted logic)
        se_trigger_on_change(1,
            function()
                local pred = p_call("SE_PRED_NOT")
                    local p1 = p_call("TEST_BIT") int(5) end_call(p1)
                end_call(pred)
            end,
            function()
                se_chain_flow(function()
                    local clear = o_call("ON_BIT5_CLEAR")
                    end_call(clear)
                    se_log("ON_BIT5_CLEAR")
                    se_return_continue()
                end)
            end,
            function()
                se_chain_flow(function()
                    local set = o_call("ON_BIT5_SET")
                    end_call(set)
                    se_log("ON_BIT5_SET")
                    se_return_continue()
                end)
            end
        )
        
        se_return_continue()
    end)
end_tree()

return end_module(mod)