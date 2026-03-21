-- ============================================================================
-- se_test_module.lua
-- Simple S-Engine module for ChainTree integration test
--
-- Module: se_test_module
-- Tree:   se_test_tree
--
-- Uses only s-engine builtin functions (no user functions).
-- The tree runs a sequence: log, tick delay, log, then terminates.
-- ============================================================================

local M = require("s_expr_dsl")
local mod = start_module("se_test_module")
use_32bit()
set_debug(true)

-- ============================================================================
-- RECORD DEFINITION
-- ============================================================================

RECORD("se_test_blackboard")
    FIELD("tick_count",    "uint32")
    FIELD("test_result",   "int32")
END_RECORD()

-- ============================================================================
-- TREE DEFINITION
-- ============================================================================

start_tree("se_test_tree")
    use_record("se_test_blackboard")

    se_function_interface(function()
        --se_log("se_test_tree: INIT")
        --se_set_field("tick_count", 0)
        --se_set_field("test_result", 0)

        
        
            se_chain_flow(function()
                se_log("se_test_tree: step 1 - incrementing counter")
                se_increment_field("tick_count", 1)
                se_tick_delay(5)
                se_return_pipeline_disable()
            end)
            
          
        --se_return_halt()
    end)

end_tree()

return end_module(mod)
