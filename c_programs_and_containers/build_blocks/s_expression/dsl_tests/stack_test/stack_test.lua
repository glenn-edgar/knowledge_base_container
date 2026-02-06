-- stack_test.lua
-- Test for stack frame and quad operations

local mod = start_module("stack_test")

-- ============================================================================
-- RECORD DEFINITION
-- ============================================================================

RECORD("stack_test_state")
    FIELD("int_val_1", "int32")
    FIELD("int_val_2", "int32")
    FIELD("int_val_3", "int32")
    FIELD("uint_val_1", "uint32")
    FIELD("uint_val_2", "uint32")
    FIELD("uint_val_3", "uint32")
    FIELD("float_val_1", "float")
    FIELD("float_val_2", "float")
    FIELD("float_val_3", "float")
END_RECORD()

-- ============================================================================
-- TREE DEFINITION
-- ============================================================================

start_tree("stack_test")
use_record("stack_test_state")

local body_fns = {
    function()
        se_quad(SE_QUAD_OP.MOV,
            function() uint(1) end,
            function() null_param() end,
            function() field_ref("int_val_1") end)
    end
}

se_function_interface(function()
    se_call(0, 5, 5, {}, body_fns)
end)

end_tree("stack_test")

return end_module(mod)