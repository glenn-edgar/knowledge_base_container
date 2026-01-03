--============================================================================
-- CFL HELPER FUNCTIONS
-- High-level DSL wrappers for CFL system functions
-- 
-- Bit block validation handled by core DSL functions
--============================================================================

--============================================================================
-- MAIN FUNCTIONS
--============================================================================
function cfl_pipeline(actions_fn)
    local c = m_call("CFL_PIPELINE")
        actions_fn()
    end_call(c)
end


function cfl_tick_delay(tick_count)
    local c = pt_m_call("CFL_TICK_DELAY")
        int(tick_count)
    end_call(c)
end

function cfl_time_delay(seconds)
    local c = pt_m_call("CFL_TIME_DELAY")
        flt(seconds)
    end_call(c)
end

function cfl_wait_event(event_id, count)
    local c = pt_m_call("CFL_WAIT_EVENT")
        int(event_id)
        int(count)
    end_call(c)
end

function cfl_wait_child_disabled(child_node_index)
    local c = m_call("CFL_WAIT_CHILD_DISABLED")
        int(child_node_index)
    end_call(c)
end

function cfl_if_then_else(pred_fn, then_fn, else_fn)
    local c = m_call("CFL_S_IF_THEN_ELSE")
        pred_fn()
        then_fn()
        else_fn()
    end_call(c)
end

function cfl_trigger_on_change(initial_state, pred_fn, then_fn, else_fn)
    local c = m_call("CFL_TRIGGER_ON_CHANGE")
        int(initial_state)
        pred_fn()
        then_fn()
        else_fn()
    end_call(c)
end

function cfl_state_machine(state_field, state_fns)
    local c = m_call("CFL_STATE_MACHINE")
        field_ref(state_field)
        for _, state_fn in ipairs(state_fns) do
            local s = m_call("CFL_STATE_ACTIONS")
                state_fn()
            end_call(s)
        end
    end_call(c)
end

function cfl_state_actions(return_code, actions_fn)
    local c = m_call("CFL_STATE_ACTIONS")
        actions_fn()
        result(return_code)
    end_call(c)
end



function cfl_dispatch(cases)
    local c = m_call("CFL_DISPATCH")
        for _, case in ipairs(cases) do
            local event_val = case[1]
            local action_fn = case[2]
            local l = list_start("case")
                int(event_val)
                action_fn()
            list_end(l)
        end
    end_call(c)
end
--============================================================================
-- CFL_FIELD_DISPATCH: Dispatch based on blackboard field value
-- Uses: user_flags (node embedded) for branch tracking
-- Usage:
--   cfl_field_dispatch("command", {
--       { CMD_FORWARD, function() ... end },
--       { CMD_BACK, function() ... end },
--       { 0, function() ... end },  -- default case
--   })
--============================================================================
function cfl_field_dispatch(field_name, cases)
    local c = m_call("CFL_FIELD_DISPATCH")
        field_ref(field_name)
        for _, case in ipairs(cases) do
            local case_val = case[1]
            local action_fn = case[2]
            local l = list_start("case")
                int(case_val)
                local sa = m_call("CFL_STATE_ACTIONS")
                    action_fn()
                end_call(sa)
            list_end(l)
        end
    end_call(c)
end


--============================================================================
-- CFL_EVENT_DISPATCH: Dispatch based on event_id
-- Uses: user_flags (node embedded) for branch tracking
-- Usage:
--   cfl_event_dispatch({
--       { EVT_TIMER, function() ... end },
--       { EVT_BUTTON, function() ... end },
--       { 0, function() ... end },  -- default case
--   })
--============================================================================
function cfl_event_dispatch(cases)
    local c = m_call("CFL_EVENT_DISPATCH")
        for _, case in ipairs(cases) do
            local event_val = case[1]
            local action_fn = case[2]
            local l = list_start("case")
                int(event_val)
                cfl_pipeline(action_fn)  -- Auto-wrap in pipeline
            list_end(l)
        end
    end_call(c)
end

--============================================================================
-- ONESHOT FUNCTIONS
--============================================================================

function cfl_log(message)
    local c = o_call("CFL_LOG")
        str_ptr(message) -- string pointer index
    end_call(c)
end

function cfl_enable_children()
    local c = o_call("CFL_ENABLE_CHILDREN")
    end_call(c)
end

function cfl_disable_children()
    local c = o_call("CFL_DISABLE_CHILDREN")
    end_call(c)
end

function cfl_enable_child(child_index)
    local c = o_call("CFL_ENABLE_CHILD")
        int(child_index)
    end_call(c)
end

function cfl_disable_child(child_index)
    local c = o_call("CFL_DISABLE_CHILD")
        int(child_index)
    end_call(c)
end

function cfl_internal_event(event_type, event_data)
    local c = o_call("CFL_INTERNAL_EVENT")
        int(event_type)
        int(event_data)
    end_call(c)
end

function cfl_exception(message)
    local c = o_call("CFL_EXCEPTION")
        str(message)
    end_call(c)
end

--============================================================================
-- BOOLEAN/PREDICATE FUNCTIONS
--============================================================================

function cfl_true()
    local c = p_call("CFL_TRUE")
    end_call(c)
end

function cfl_false()
    local c = p_call("CFL_FALSE")
    end_call(c)
end

function cfl_read_bit(bit_index)
    local c = p_call("CFL_READ_BIT")
        int(bit_index)
    end_call(c)
end

function cfl_check_event(...)
    local event_ids = {...}
    local c = p_call("CFL_CHECK_EVENT")
        for _, id in ipairs(event_ids) do
            int(id)
        end
    end_call(c)
end

--============================================================================
-- BIT OPERATION COMPOSABLE API
--============================================================================

function cfl_s_bit_or_start()
    return p_call_bit("CFL_S_BIT_OR")
end

function cfl_s_bit_and_start()
    return p_call_bit("CFL_S_BIT_AND")
end

function cfl_s_bit_nor_start()
    return p_call_bit("CFL_S_BIT_NOR")
end

function cfl_s_bit_nand_start()
    return p_call_bit("CFL_S_BIT_NAND")
end

function cfl_s_bit_xor_start()
    return p_call_bit("CFL_S_BIT_XOR")
end

function cfl_s_bit_not_start()
    return p_call_bit("CFL_S_BIT_NOR")
end

-- This one still needs the check - uint() is allowed in bit blocks
function cfl_bit_entry(...)
    check_bit_block_only("cfl_bit_entry")
    local bit_ids = {...}
    for _, id in ipairs(bit_ids) do
        uint(id)
    end
end

--============================================================================
-- CONVENIENCE WRAPPERS
--============================================================================

function cfl_wait_event_once(event_id)
    cfl_wait_event(event_id, 1)
end

function cfl_if_then(pred_fn, then_fn)
    cfl_if_then_else(pred_fn, then_fn, function()
        local c = m_call("CFL_NOP") end_call(c)
    end)
end

function cfl_on_rising_edge(pred_fn, action_fn)
    cfl_trigger_on_change(0, pred_fn, action_fn, function()
        local c = m_call("CFL_NOP") end_call(c)
    end)
end

function cfl_on_falling_edge(pred_fn, action_fn)
    cfl_trigger_on_change(1, pred_fn, function()
        local c = m_call("CFL_NOP") end_call(c)
    end, action_fn)
end

function cfl_debug_log(message)
    if is_debug() then
        cfl_log(message)
    end
end

function cfl_debug_log_field(message, field_name)
    if is_debug() then
        local c = o_call("CFL_LOG")
            str(message)
            field_ref(field_name)
        end_call(c)
    end
end

function cfl_init_enable_children()
    local c = io_call("CFL_ENABLE_CHILDREN")
    end_call(c)
end

function cfl_init_disable_children()
    local c = io_call("CFL_DISABLE_CHILDREN")
    end_call(c)
end

function cfl_init_enable_child(child_index)
    local c = io_call("CFL_ENABLE_CHILD")
        int(child_index)
    end_call(c)
end

function cfl_init_disable_child(child_index)
    local c = io_call("CFL_DISABLE_CHILD")
        int(child_index)
    end_call(c)
end
--============================================================================
-- CFL_SET_BITS - Set one or more bits in the runtime bitmask
-- Usage: cfl_set_bits(0, 5, 12)  -- sets bits 0, 5, and 12
--============================================================================

function cfl_set_bits(...)
    local bits = {...}
    if #bits == 0 then
        dsl_error("cfl_set_bits() requires at least one bit index")
    end
    
    local c = o_call("CFL_SET_BITS")
        for _, bit_index in ipairs(bits) do
            if type(bit_index) ~= "number" then
                dsl_error("cfl_set_bits() bit index must be a number")
            end
            if bit_index < 0 or bit_index > 31 then
                dsl_error("cfl_set_bits() bit index must be 0-31")
            end
            uint(bit_index)
        end
    end_call(c)
end

--============================================================================
-- CFL_CLEAR_BITS - Clear one or more bits in the runtime bitmask
-- Usage: cfl_clear_bits(0, 5, 12)  -- clears bits 0, 5, and 12
--============================================================================

function cfl_clear_bits(...)
    local bits = {...}
    if #bits == 0 then
        dsl_error("cfl_clear_bits() requires at least one bit index")
    end
    
    local c = o_call("CFL_CLEAR_BITS")
        for _, bit_index in ipairs(bits) do
            if type(bit_index) ~= "number" then
                dsl_error("cfl_clear_bits() bit index must be a number")
            end
            if bit_index < 0 or bit_index > 31 then
                dsl_error("cfl_clear_bits() bit index must be 0-31")
            end
            uint(bit_index)
        end
    end_call(c)
end
print("CFL helper functions loaded")