--============================================================================
-- s_engine_helpers.lua
-- Core S-Expression Engine Helper Functions - Version 5.1
-- 
-- These are generic helpers for the s_expression engine, independent of
-- any specific application (ChainTree, state machines, etc.)
--============================================================================

--============================================================================
-- COMPOSABLE PREDICATE API
-- 
-- Build complex boolean expressions from any child predicates.
-- Children can be p_call() user predicates or nested se_pred_*() blocks.
--
-- Usage:
--   local p1 = se_pred_or()
--       local p2 = se_pred_and()
--           p_call("SENSOR_A_READY") end_call()
--           p_call("SENSOR_B_READY") end_call()
--       end_call(p2)
--       p_call("OVERRIDE_ENABLED") end_call()
--   end_call(p1)  -- (A AND B) OR OVERRIDE
--============================================================================


--============================================================================
-- RESULT CODE FUNCTIONS
-- 
-- These return specific result codes to control execution flow.
-- Only SE_CONTINUE and SE_DISABLE continue to next node.
-- All others terminate the current tick and propagate to caller.
--============================================================================

function se_return_continue()
    local c = m_call("SE_RETURN_CONTINUE")
    end_call(c)
end

function se_return_terminate()
    local c = m_call("SE_RETURN_TERMINATE")
    end_call(c)
end

function se_return_reset()
    local c = m_call("SE_RETURN_RESET")
    end_call(c)
end

function se_return_disable()
    local c = m_call("SE_RETURN_DISABLE")
    end_call(c)
end

function se_return_halt()
    local c = m_call("SE_RETURN_HALT")
    end_call(c)
end

function se_return_skip_continue()
    local c = m_call("SE_RETURN_SKIP_CONTINUE")
    end_call(c)
end

function se_return_function_halt()
    local c = m_call("SE_RETURN_FUNCTION_HALT")
    end_call(c)
end

function se_return_function_reset()
    local c = m_call("SE_RETURN_FUNCTION_RESET")
    end_call(c)
end

function se_return_function_terminate()
    local c = m_call("SE_RETURN_FUNCTION_TERMINATE")
    end_call(c)
end

function se_pred(name)
    local c = p_call(name)
    end_call(c)
end

function se_pred_or()
    return p_call_composite("SE_PRED_OR")
end

function se_pred_and()
    return p_call_composite("SE_PRED_AND")
end

function se_pred_nor()
    return p_call_composite("SE_PRED_NOR")
end

function se_pred_nand()
    return p_call_composite("SE_PRED_NAND")
end

function se_pred_xor()
    return p_call_composite("SE_PRED_XOR")
end

function se_pred_not()
    return p_call_composite("SE_PRED_NOT")  -- single child, inverts result
end

--============================================================================
-- PREDICATE CONSTANTS
--============================================================================

function se_true()
    local c = p_call("SE_TRUE")
    end_call(c)
end

function se_false()
    local c = p_call("SE_FALSE")
    end_call(c)
end

--============================================================================
-- MAIN FUNCTIONS
--============================================================================

function se_pipeline(actions_fn)
    local c = m_call("SE_PIPELINE")
        actions_fn()
    end_call(c)
end

function se_tick_delay(tick_count)
    local c = pt_m_call("SE_TICK_DELAY")
        int(tick_count)
    end_call(c)
end

function se_time_delay(seconds)
    local c = pt_m_call("SE_TIME_DELAY")
        flt(seconds)
    end_call(c)
end

function se_wait_event(event_id, count)
    local c = pt_m_call("SE_WAIT_EVENT")
        int(event_id)
        int(count)
    end_call(c)
end

function se_wait_event_once(event_id)
    se_wait_event(event_id, 1)
end

function se_if_then_else(pred_fn, then_fn, else_fn)
    local c = m_call("SE_IF_THEN_ELSE")
        pred_fn()
        then_fn()
        else_fn()
    end_call(c)
end

function se_if_then(pred_fn, then_fn)
    se_if_then_else(pred_fn, then_fn, function()
        se_nop()
    end)
end

function se_trigger_on_change(initial_state, pred_fn, then_fn, else_fn)
    local c = m_call("SE_TRIGGER_ON_CHANGE")
        int(initial_state)
        pred_fn()
        then_fn()
        else_fn()
    end_call(c)
end

function se_on_rising_edge(pred_fn, action_fn)
    se_trigger_on_change(0, pred_fn, action_fn, function()
        se_nop()
    end)
end

function se_on_falling_edge(pred_fn, action_fn)
    se_trigger_on_change(1, pred_fn, function()
        se_nop()
    end, action_fn)
end

function se_state_machine(state_field, state_fns)
    local c = m_call("SE_STATE_MACHINE")
        field_ref(state_field)
        for _, state_fn in ipairs(state_fns) do
            local s = m_call("SE_STATE_ACTIONS")
                state_fn()
            end_call(s)
        end
    end_call(c)
end

function se_state_actions(return_code, actions_fn)
    local c = m_call("SE_STATE_ACTIONS")
        actions_fn()
        result(return_code)
    end_call(c)
end

function se_dispatch(cases)
    local c = m_call("SE_DISPATCH")
        for _, case in ipairs(cases) do
            local case_val = case[1]
            local action_fn = case[2]
            local l = list_start("case")
                int(case_val)
                action_fn()
            list_end(l)
        end
    end_call(c)
end

function se_field_dispatch(field_name, cases)
    local c = m_call("SE_FIELD_DISPATCH")
        field_ref(field_name)
        for _, case in ipairs(cases) do
            local case_val = case[1]
            local action_fn = case[2]
            local l = list_start("case")
                int(case_val)
                local sa = m_call("SE_STATE_ACTIONS")
                    action_fn()
                end_call(sa)
            list_end(l)
        end
    end_call(c)
end

function se_event_dispatch(cases)
    local c = m_call("SE_EVENT_DISPATCH")
        for _, case in ipairs(cases) do
            local event_val = case[1]
            local action_fn = case[2]
            local l = list_start("case")
                int(event_val)
                se_pipeline(action_fn)
            list_end(l)
        end
    end_call(c)
end

function se_nop()
    local c = m_call("SE_NOP")
    end_call(c)
end

--============================================================================
-- ONESHOT FUNCTIONS
--============================================================================

function se_log(message)
    local c = o_call("SE_LOG")
        str_ptr(message)
    end_call(c)
end

function se_debug_log(message)
    if is_debug() then
        se_log(message)
    end
end

function se_debug_log_field(message, field_name)
    if is_debug() then
        local c = o_call("SE_LOG")
            str(message)
            field_ref(field_name)
        end_call(c)
    end
end

--============================================================================
-- EVENT FUNCTIONS
--============================================================================

function se_check_event(...)
    local event_ids = {...}
    local c = p_call("SE_CHECK_EVENT")
        for _, id in ipairs(event_ids) do
            int(id)
        end
    end_call(c)
end

print("S-Expression Engine helpers loaded (v5.1)")