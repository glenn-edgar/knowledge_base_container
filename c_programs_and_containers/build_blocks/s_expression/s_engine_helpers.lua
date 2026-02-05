--============================================================================
-- s_engine_helpers.lua
-- Core S-Expression Engine Helper Functions - Version 5.2
-- 
-- These are generic helpers for the s_expression engine, independent of
-- any specific application (ChainTree, state machines, etc.)
--
-- VERSION 5.2 CHANGES:
--   - Added dictionary-based dispatch (se_string_dispatch)
--   - Added named state machine (se_named_state_machine)
--   - Added named event dispatch (se_named_event_dispatch)
--   - Added configuration dictionary helper (se_config)
--   - Added list builder helpers (se_int_list, se_float_list, etc.)
--   - Added parameter block helper (se_params)
--============================================================================





--============================================================================
-- RESULT CODE CONSTANTS
--============================================================================

-- APPLICATION RESULT CODES (0-5)
--_G.SE_CONTINUE           = 0
--_G.SE_HALT               = 1
--_G.SE_TERMINATE          = 2
--_G.SE_RESET              = 3
--_G.SE_DISABLE            = 4
--_G.SE_SKIP_CONTINUE      = 5

-- FUNCTION RESULT CODES (6-11)
--_G.SE_FUNCTION_CONTINUE      = 6
--_G.SE_FUNCTION_HALT          = 7
--_G.SE_FUNCTION_TERMINATE     = 8
--_G.SE_FUNCTION_RESET         = 9
--_G.SE_FUNCTION_DISABLE       = 10
--_G.SE_FUNCTION_SKIP_CONTINUE = 11

-- PIPELINE RESULT CODES (12-17)
--_G.SE_PIPELINE_CONTINUE      = 12
--_G.SE_PIPELINE_HALT          = 13
--_G.SE_PIPELINE_TERMINATE     = 14
--_G.SE_PIPELINE_RESET         = 15
--_G.SE_PIPELINE_DISABLE       = 16
--_G.SE_PIPELINE_SKIP_CONTINUE = 17

--============================================================================
-- APPLICATION RESULT CODE FUNCTIONS (0-5)
--============================================================================

function se_return_continue()
    local c = m_call("SE_RETURN_CONTINUE")
    end_call(c)
end

function se_return_halt()
    local c = m_call("SE_RETURN_HALT")
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

function se_return_skip_continue()
    local c = m_call("SE_RETURN_SKIP_CONTINUE")
    end_call(c)
end

--============================================================================
-- FUNCTION RESULT CODE FUNCTIONS (6-11)
--============================================================================

function se_return_function_continue()
    local c = m_call("SE_RETURN_FUNCTION_CONTINUE")
    end_call(c)
end

function se_return_function_halt()
    local c = m_call("SE_RETURN_FUNCTION_HALT")
    end_call(c)
end

function se_return_function_terminate()
    local c = m_call("SE_RETURN_FUNCTION_TERMINATE")
    end_call(c)
end

function se_return_function_reset()
    local c = m_call("SE_RETURN_FUNCTION_RESET")
    end_call(c)
end

function se_return_function_disable()
    local c = m_call("SE_RETURN_FUNCTION_DISABLE")
    end_call(c)
end

function se_return_function_skip_continue()
    local c = m_call("SE_RETURN_FUNCTION_SKIP_CONTINUE")
    end_call(c)
end

--============================================================================
-- PIPELINE RESULT CODE FUNCTIONS (12-17)
--============================================================================

function se_return_pipeline_continue()
    local c = m_call("SE_RETURN_PIPELINE_CONTINUE")
    end_call(c)
end

function se_return_pipeline_halt()
    local c = m_call("SE_RETURN_PIPELINE_HALT")
    end_call(c)
end

function se_return_pipeline_terminate()
    local c = m_call("SE_RETURN_PIPELINE_TERMINATE")
    end_call(c)
end

function se_return_pipeline_reset()
    local c = m_call("SE_RETURN_PIPELINE_RESET")
    end_call(c)
end

function se_return_pipeline_disable()
    local c = m_call("SE_RETURN_PIPELINE_DISABLE")
    end_call(c)
end

function se_return_pipeline_skip_continue()
    local c = m_call("SE_RETURN_PIPELINE_SKIP_CONTINUE")
    end_call(c)
end
--============================================================================
-- MAIN FUNCTIONS
--============================================================================

function  se_function_interface(actions_fn)
    local c = m_call("SE_FUNCTION_INTERFACE")
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

function se_wait_event(target_event, count)
    count = count or 1
    
    if type(target_event) ~= "number" then
        error("se_wait_event: target_event must be a number")
    end
    if type(count) ~= "number" or count < 1 then
        error("se_wait_event: count must be a positive integer")
    end
    
    count = math.floor(count)
    target_event = math.floor(target_event)
    
    local c = pt_m_call("SE_WAIT_EVENT")
        uint(target_event)
        uint(count)
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

-- SE_SEQUENCE: Execute children one at a time, advance on completion
function se_sequence(...)
    local children = {...}
    local c = m_call("SE_SEQUENCE")
        for _, child_fn in ipairs(children) do
            child_fn()
        end
    end_call(c)
end



function se_fork(...)
    local children = {...}
    local f = m_call("SE_FORK")
    for _, child in ipairs(children) do
        if type(child) == "function" then
            child()
        end
    end
    end_call(f)
end

function se_fork_join(...)
    local children = {...}
    local f = m_call("SE_FORK_JOIN")
    for _, child in ipairs(children) do
        if type(child) == "function" then
            child()
        end
    end
    end_call(f)
end

function se_chain_flow(...)
    local children = {...}
    local f = m_call("SE_CHAIN_FLOW")
    for _, child in ipairs(children) do
        if type(child) == "function" then
            child()
        end
    end
    end_call(f)
end



function se_while(condition, ...)
    local children = {...}
    local w = m_call("SE_WHILE")
    condition()
    se_fork_join(unpack(children))
    end_call(w)
end


--============================================================================
-- STATE MACHINE FUNCTIONS
--============================================================================
--============================================================================
-- STATE MACHINE FUNCTIONS
--============================================================================

-- Track case values within current dispatch to detect duplicates
local dispatch_case_values = {}
local in_dispatch = false

function se_case(case_val, action_fn)
    local int_val
    
    if case_val == "default" then
        int_val = -1
    elseif type(case_val) == "number" and math.floor(case_val) == case_val then
        int_val = case_val
    else
        error("se_case: first parameter must be integer or 'default', got: " .. tostring(case_val))
    end
    
    -- Check for duplicates if inside a dispatch
    if in_dispatch then
        if dispatch_case_values[int_val] then
            local label = (int_val == -1) and "default" or tostring(int_val)
            error("se_case: duplicate case value: " .. label)
        end
        dispatch_case_values[int_val] = true
    end
    
    int(int_val)
    action_fn()
end

function se_field_dispatch(state_field, cases_fn)
    -- Reset case tracking for this dispatch
    dispatch_case_values = {}
    in_dispatch = true
    
    local success, err = pcall(function()
        local c = m_call("SE_FIELD_DISPATCH")
            field_ref(state_field)
            if type(cases_fn) == "function" then
                cases_fn()
            elseif type(cases_fn) == "table" then
                for _, case_fn in ipairs(cases_fn) do
                    case_fn()
                end
            else
                error("se_field_dispatch: cases must be function or table")
            end
        end_call(c)
    end)
    
    -- Clean up tracking state
    in_dispatch = false
    dispatch_case_values = {}
    
    if not success then
        error(err)
    end
end


function se_state_machine(state_field, cases_fn)
    -- Reset case tracking for this dispatch
    dispatch_case_values = {}
    in_dispatch = true
    
    local success, err = pcall(function()
        local c = m_call("SE_STATE_MACHINE")
            field_ref(state_field)
            if type(cases_fn) == "function" then
                cases_fn()
            elseif type(cases_fn) == "table" then
                for _, case_fn in ipairs(cases_fn) do
                    case_fn()
                end
            else
                error("se_field_dispatch: cases must be function or table")
            end
        end_call(c)
    end)
    
    -- Clean up tracking state
    in_dispatch = false
    dispatch_case_values = {}
    
    if not success then
        error(err)
    end
end




function se_queue_event(event_type, event_id, slot_name)
    if event_type > 0xFFFE then
        dsl_error("se_queue_event: event_type must be <= 0xFFFE")
    end
    if event_id > 0xFFFE then
        dsl_error("se_queue_event: event_id must be <= 0xFFFE")
    end

    local c = o_call("SE_QUEUE_EVENT")
        uint(event_type)
        uint(event_id)
        field_ref(slot_name)
    end_call(c)
end


--============================================================================
-- EVENT DISPATCH FUNCTIONS
--============================================================================
function se_event_case(event_val, action_fn)
    local int_val
    
    if event_val == "default" then
        int_val = -1
    elseif type(event_val) == "number" and math.floor(event_val) == event_val then
        int_val = event_val
    else
        error("se_event_case: event must be integer or 'default', got: " .. tostring(event_val))
    end
    
    int(int_val)
    action_fn()
end

function se_event_dispatch(cases)
    local c = m_call("SE_EVENT_DISPATCH")
        if type(cases) == "function" then
            cases()
        elseif type(cases) == "table" then
            for _, case_fn in ipairs(cases) do
                case_fn()
            end
        else
            error("se_event_dispatch: cases must be function or table")
        end
    end_call(c)
end


--============================================================================
-- SE_COND - Lisp-style conditional dispatch
--============================================================================

--============================================================================
-- SE_COND - Lisp-style conditional dispatch
--============================================================================

local cond_case_count = 0
local cond_has_default = false
local in_cond = false

function se_cond(cases)
    -- Reset tracking
    cond_case_count = 0
    cond_has_default = false
    in_cond = true
    
    local success, err = pcall(function()
        local c = m_call("SE_COND")
            if type(cases) == "function" then
                cases()
            elseif type(cases) == "table" then
                for _, case_fn in ipairs(cases) do
                    case_fn()
                end
            else
                error("se_cond: cases must be function or table")
            end
        end_call(c)
    end)
    
    -- Validate before cleanup
    local case_count = cond_case_count
    local has_default = cond_has_default
    
    -- Cleanup
    in_cond = false
    cond_case_count = 0
    cond_has_default = false
    
    if not success then
        error(err)
    end
    
    if case_count == 0 then
        error("se_cond: must have at least one case")
    end
    
    if not has_default then
        error("se_cond: must have a default case (use se_cond_default)")
    end
end

function se_cond_case(pred_fn, action_fn)
    return function()
        if not in_cond then
            error("se_cond_case: must be used inside se_cond")
        end
        if cond_has_default then
            error("se_cond_case: cannot add cases after se_cond_default (default must be last)")
        end
        cond_case_count = cond_case_count + 1
        pred_fn()
        action_fn()
    end
end

function se_cond_default(action_fn)
    return function()
        if not in_cond then
            error("se_cond_default: must be used inside se_cond")
        end
        if cond_has_default then
            error("se_cond_default: duplicate default case")
        end
        cond_has_default = true
        cond_case_count = cond_case_count + 1
        local pred = p_call("SE_TRUE")
        end_call(pred)
        action_fn()
    end
end

function se_verify_and_check_elapsed_time(timeout, reset_flag, error_function)
    if type(timeout) ~= "number" then
        error("se_verify_and_check_elapsed_time: timeout must be a number")
    end
    if type(reset_flag) ~= "boolean" then
        error("se_verify_and_check_elapsed_time: reset_flag must be a boolean")
    end
    if type(error_function) ~= "function" then
        error("se_verify_and_check_elapsed_time: error_function must be a function")
    end

    local c = pt_m_call("SE_VERIFY_AND_CHECK_ELAPSED_TIME")
        flt(timeout)
        int(reset_flag and 1 or 0)
        error_function()
    end_call(c)
end


function se_verify_and_check_elapsed_events(event_id, count, reset_flag, error_function)
    if type(event_id) ~= "number" then
        error("se_verify_and_check_elapsed_events: event_id must be a number")
    end
    if type(count) ~= "number" or count < 1 then
        error("se_verify_and_check_elapsed_events: count must be a positive number")
    end
    if type(reset_flag) ~= "boolean" then
        error("se_verify_and_check_elapsed_events: reset_flag must be a boolean")
    end
    if type(error_function) ~= "function" then
        error("se_verify_and_check_elapsed_events: error_function must be a function")
    end

    event_id = math.floor(event_id)
    count = math.floor(count)

    local c = pt_m_call("SE_VERIFY_AND_CHECK_ELAPSED_EVENTS")
        uint(event_id)
        uint(count)
        int(reset_flag and 1 or 0)
        error_function()
    end_call(c)
end

function se_verify(pred_function, reset_flag, error_function)
    if type(pred_function) ~= "function" then
        error("se_verify: pred_function must be a function")
    end
    if type(reset_flag) ~= "boolean" then
        error("se_verify: reset_flag must be a boolean")
    end
    if type(error_function) ~= "function" then
        error("se_verify: error_function must be a function")
    end

    local c = m_call("SE_VERIFY")
        pred_function()
        int(reset_flag and 1 or 0)
        error_function()
    end_call(c)
end

function se_wait(pred_function)
    if type(pred_function) ~= "function" then
        error("se_wait: pred_function must be a function")
    end

    local c = m_call("SE_WAIT")
        pred_function()
    end_call(c)
end

function se_wait_timeout(pred_function, timeout, reset_flag, error_function)
    if type(pred_function) ~= "function" then
        error("se_wait_timeout: pred_function must be a function")
    end
    if type(timeout) ~= "number" then
        error("se_wait_timeout: timeout must be a number")
    end
    if type(reset_flag) ~= "boolean" then
        error("se_wait_timeout: reset_flag must be a boolean")
    end
    if type(error_function) ~= "function" then
        error("se_wait_timeout: error_function must be a function")
    end

    local c = pt_m_call("SE_WAIT_TIMEOUT")
        pred_function()
        flt(timeout)
        int(reset_flag and 1 or 0)
        error_function()
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



function se_log_slot_integer(message, slot_name)
    if slot_name == nil or slot_name == "" then
        error("se_log_slot: slot_name cannot be nil or empty")
    end
    local c = o_call("SE_LOG_INT")
        str_ptr(message)
        field_ref(slot_name)
    end_call(c)
end

function se_log_slot_float(message, slot_name)
    if slot_name == nil or slot_name == "" then
        error("se_log_slot: slot_name cannot be nil or empty")
    end
    local c = o_call("SE_LOG_FLOAT")
        str_ptr(message)
        field_ref(slot_name)
    end_call(c)
end

local function emit_typed_value(value)
    local t = type(value)
    if t == "number" then
        if math.floor(value) == value then
            if value < 0 then
                int(value)
            else
                uint(value)
            end
        else
            flt(value)
        end
    elseif t == "string" then
        str_hash(value)  -- String becomes hash
    elseif t == "boolean" then
        uint(value and 1 or 0)
    else
        dsl_error("emit_typed_value: unsupported type: " .. t)
    end
end

function se_set_field(target_field, value)
    local c = o_call("SE_SET_FIELD")
        field_ref(target_field)
        emit_typed_value(value)
    end_call(c)
end

function se_i_set_field(target_field, value)
    local c = io_call("SE_SET_FIELD")
        field_ref(target_field)
        emit_typed_value(value)
    end_call(c)
end  

function se_increment_field(target_field, increment_value)
    local c = o_call("SE_INC_FIELD")
        field_ref(target_field)
        uint(increment_value)
    end_call(c)
end

function se_decrement_field(target_field, decrement_value)
    local c = o_call("SE_DEC_FIELD")
        field_ref(target_field)
        uint(decrement_value)
    end_call(c)
end



--============================================================================
-- Predicate Builder - Stack-based generator for composable predicates
--============================================================================

--============================================================================
-- Predicate Builder - Stack-based generator for composable predicates
--============================================================================

--============================================================================
-- Predicate Builder - Recursive tree builder
--============================================================================

local pred_builder_active = false
local pred_id_counter = 0
local pred_current_children = nil  -- current children list
local pred_parent_stack = {}       -- stack of parent children lists

function pred_begin()
    if pred_builder_active then
        error("pred_begin: already in predicate builder")
    end
    pred_builder_active = true
    pred_id_counter = 0
    pred_current_children = {}
    pred_parent_stack = {}
end

function pred_end()
    if not pred_builder_active then
        error("pred_end: not in predicate builder")
    end
    if #pred_parent_stack > 0 then
        error("pred_end: unclosed composite predicate")
    end
    if #pred_current_children == 0 then
        error("pred_end: empty predicate")
    end

    pred_builder_active = false

    local ops = {}
    for i, op in ipairs(pred_current_children) do
        ops[i] = op
    end
    pred_current_children = nil
    pred_parent_stack = {}

    return function()
        for _, op in ipairs(ops) do
            op()
        end
    end
end

local function next_pred_id()
    pred_id_counter = pred_id_counter + 1
    return pred_id_counter
end

local function pred_push_leaf(emit_fn)
    if pred_builder_active then
        table.insert(pred_current_children, emit_fn)
        return nil
    else
        return emit_fn
    end
end

local function pred_open_composite(name)
    if not pred_builder_active then
        error(name .. ": must be inside pred_begin/pred_end")
    end

    local id = next_pred_id()

    -- Save current children list, start new one
    table.insert(pred_parent_stack, { name = name, id = id, children = pred_current_children })
    pred_current_children = {}

    return id
end

function pred_close(id)
    if not pred_builder_active then
        error("pred_close: not in predicate builder")
    end
    if type(id) ~= "number" then
        error("pred_close: expected numeric id, got " .. type(id))
    end
    if #pred_parent_stack == 0 then
        error("pred_close: no open composite")
    end

    local top = pred_parent_stack[#pred_parent_stack]
    if top.id ~= id then
        error("pred_close: expected id=" .. top.id .. " (" .. top.name .. "), got id=" .. id)
    end

    table.remove(pred_parent_stack)

    local name = top.name
    local children = pred_current_children

    if #children == 0 then
        error("pred_close: composite " .. name .. " (id=" .. id .. ") has no children")
    end

    -- Restore parent children list
    pred_current_children = top.children

    -- Push composite closure onto parent
    table.insert(pred_current_children, function()
        local c = p_call_composite(name)
            for _, child_fn in ipairs(children) do
                child_fn()
            end
        end_call(c)
    end)
end
--============================================================================
-- Composite Predicates (only inside pred_begin/pred_end)
--============================================================================

function se_pred_or()
    return pred_open_composite("SE_PRED_OR")
end

function se_pred_and()
    return pred_open_composite("SE_PRED_AND")
end

function se_pred_nor()
    return pred_open_composite("SE_PRED_NOR")
end

function se_pred_nand()
    return pred_open_composite("SE_PRED_NAND")
end

function se_pred_xor()
    return pred_open_composite("SE_PRED_XOR")
end

function se_pred_not()
    return pred_open_composite("SE_PRED_NOT")
end

--============================================================================
-- Leaf Predicates (inside builder: pushed to stack, outside: return closure)
--============================================================================

function se_pred(name)
    return pred_push_leaf(function()
        local c = p_call(name)
        end_call(c)
    end)
end

function se_pred_with(name, param_fn)
    return pred_push_leaf(function()
        local c = p_call(name)
            param_fn()
        end_call(c)
    end)
end
function se_true()
    return pred_push_leaf(function()
        local c = p_call("SE_TRUE")
        end_call(c)
    end)
end

function se_false()
    return pred_push_leaf(function()
        local c = p_call("SE_FALSE")
        end_call(c)
    end)
end

function se_check_event(...)
    local event_ids = {...}
    return pred_push_leaf(function()
        local c = p_call("SE_CHECK_EVENT")
            for _, id in ipairs(event_ids) do
                int(id)
            end
        end_call(c)
    end)
end

function se_field_eq(field_name, value)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_EQ")
            field_ref(field_name)
            emit_typed_value(value)
        end_call(c)
    end)
end

function se_field_ne(field_name, value)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_NE")
            field_ref(field_name)
            emit_typed_value(value)
        end_call(c)
    end)
end

function se_field_gt(field_name, value)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_GT")
            field_ref(field_name)
            emit_typed_value(value)
        end_call(c)
    end)
end

function se_field_ge(field_name, value)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_GE")
            field_ref(field_name)
            emit_typed_value(value)
        end_call(c)
    end)
end

function se_field_lt(field_name, value)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_LT")
            field_ref(field_name)
            emit_typed_value(value)
        end_call(c)
    end)
end

function se_field_le(field_name, value)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_LE")
            field_ref(field_name)
            emit_typed_value(value)
        end_call(c)
    end)
end

function se_field_in_range(field_name, min, max)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_IN_RANGE")
            field_ref(field_name)
            emit_typed_value(min)
            emit_typed_value(max)
        end_call(c)
    end)
end

function se_field_increment_and_test(field_name, increment_value, value_to_test)
    return pred_push_leaf(function()
        local c = p_call("SE_FIELD_INCREMENT_AND_TEST")
            field_ref(field_name)
            field_ref(increment_value)
            field_ref(value_to_test)
        end_call(c)
    end)
end


 

function se_state_increment_and_test(increment_value, value_to_test)
    increment_value = math.floor(increment_value)
    value_to_test = math.floor(value_to_test)
    if increment_value <= 0 or increment_value > 0xFFFF then
        error("se_state_increment_and_test: increment_value must be 0-0xFFFF, got: " .. tostring(increment_value))
    end
    if value_to_test < 0 or value_to_test > 0xFFFF then
        error("se_state_increment_and_test: value_to_test must be 0-0xFFFF, got: " .. tostring(value_to_test))
    end
    
    return pred_push_leaf(function()
        local c = p_call("SE_STATE_INCREMENT_AND_TEST")
            uint(increment_value)
            uint(value_to_test)
        end_call(c)
    end)
end


-- ============================================================================
-- SE_LOAD_DICTIONARY with compile-time validation
-- 
-- Validates that the target field is a PTR64_FIELD before emitting code.
-- ============================================================================

-- ============================================================================
-- Field validation helpers
-- ============================================================================

local function validate_field_exists(field_name, func_name)
    if not current_tree or not current_tree.record_name then
        return nil  -- Can't validate without tree context
    end
    
    local rec = current_module.records[current_tree.record_name]
    if not rec then
        return nil
    end
    
    for _, f in ipairs(rec.fields) do
        if f.name == field_name then
            return f
        end
    end
    
    dsl_error(string.format(
        "%s: field '%s' not found in record '%s'",
        func_name, field_name, current_tree.record_name))
end

local function validate_field_is_ptr64(field_name, func_name)
    local f = validate_field_exists(field_name, func_name)
    if not f then return end
    
    if not f.is_ptr64 then
        dsl_error(string.format(
            "%s: field '%s' must be a PTR64_FIELD (got type='%s', size=%d)\n" ..
            "  Hint: Use PTR64_FIELD(\"%s\", \"void\") in record '%s'",
            func_name,
            field_name, 
            f.type or "unknown",
            f.size or 0,
            field_name,
            current_tree.record_name))
    end
    
    return f
end

local function validate_field_is_numeric(field_name, func_name)
    local f = validate_field_exists(field_name, func_name)
    if not f then return end
    
    local numeric_types = {
        int32 = true, uint32 = true,
        int64 = true, uint64 = true,
        float = true, double = true
    }
    
    if not numeric_types[f.type] then
        dsl_error(string.format(
            "%s: field '%s' must be a numeric type (got '%s')",
            func_name, field_name, f.type or "unknown"))
    end
    
    return f
end

local function validate_field_type(field_name, expected_type, func_name)
    local f = validate_field_exists(field_name, func_name)
    if not f then return end
    
    if f.type ~= expected_type then
        dsl_error(string.format(
            "%s: field '%s' must be type '%s' (got '%s')",
            func_name, field_name, expected_type, f.type or "unknown"))
    end
    
    return f
end


-- ============================================================================
-- DICTIONARY/JSON HELPERS
-- 
-- These helpers provide convenient access to JSON-style dictionary data
-- loaded into blackboard PTR64 fields.
--
-- Two storage formats:
--   - json()      : String-keyed, use dot-path strings for access
--   - json_hash() : Hash-keyed, use hash paths for access
--
-- Dictionary must first be loaded with se_load_dictionary() or 
-- se_load_dictionary_hash(), then values can be extracted to blackboard fields.
-- ============================================================================

-- ============================================================================
-- DICTIONARY LOADING
-- Stores pointer to dictionary structure in a PTR64 blackboard field
-- ============================================================================

-- Load string-keyed dictionary (use with se_dict_extract_* functions)
-- blackboard_field: PTR64_FIELD name to store dictionary pointer
-- json_expression: Lua table in JSON-like format
function se_load_dictionary(blackboard_field, json_expression)
    validate_field_is_ptr64(blackboard_field, "se_load_dictionary")
    
    local c = o_call("SE_LOAD_DICTIONARY")
        field_ref(blackboard_field)
        json(json_expression)
    end_call(c)
    return c
end

-- Load hash-keyed dictionary (use with se_dict_extract_*_h functions)
-- blackboard_field: PTR64_FIELD name to store dictionary pointer
-- json_expression: Lua table in JSON-like format
function se_load_dictionary_hash(blackboard_field, json_expression)
    validate_field_is_ptr64(blackboard_field, "se_load_dictionary_hash")
    
    local c = o_call("SE_LOAD_DICTIONARY")
        field_ref(blackboard_field)
        json_hash(json_expression)
    end_call(c)
    return c
end

-- ============================================================================
-- STRING PATH EXTRACTION (for json() dictionaries)
-- Path is dot-separated: "system.irrigation.zones.zone_1.enabled"
-- ============================================================================

-- Extract integer value from dictionary using string path
-- dict_field: PTR64_FIELD containing dictionary pointer
-- path: dot-separated path string (e.g., "system.config.timeout")
-- dest_field: destination field to store extracted value
function se_dict_extract_int(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_int")
    
    local c = o_call("SE_DICT_EXTRACT_INT")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract float value from dictionary using string path
function se_dict_extract_float(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_float")
    
    local c = o_call("SE_DICT_EXTRACT_FLOAT")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract unsigned integer value from dictionary using string path
function se_dict_extract_uint(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_uint")
    
    local c = o_call("SE_DICT_EXTRACT_UINT")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract boolean (as int 0/1) from dictionary using string path
function se_dict_extract_bool(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_bool")
    
    local c = o_call("SE_DICT_EXTRACT_BOOL")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract hash value from dictionary using string path
function se_dict_extract_hash(dict_field, path, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_hash")
    
    local c = o_call("SE_DICT_EXTRACT_HASH")
        field_ref(dict_field)
        str(path)
        field_ref(dest_field)
    end_call(c)
    return c
end

-- ============================================================================
-- HASH PATH EXTRACTION (for json_hash() dictionaries)
-- Path segments are individual hash keys, more efficient at runtime
-- ============================================================================

-- Extract integer value using hash path
-- dict_field: PTR64_FIELD containing dictionary pointer
-- path_keys: table of path segment strings {"system", "config", "timeout"}
-- dest_field: destination field to store extracted value
function se_dict_extract_int_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_int_h")
    
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("se_dict_extract_int_h: path_keys must be non-empty table")
    end
    
    local c = o_call("SE_DICT_EXTRACT_INT_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do
            str_hash(key)
        end
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract float value using hash path
function se_dict_extract_float_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_float_h")
    
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("se_dict_extract_float_h: path_keys must be non-empty table")
    end
    
    local c = o_call("SE_DICT_EXTRACT_FLOAT_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do
            str_hash(key)
        end
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract unsigned integer using hash path
function se_dict_extract_uint_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_uint_h")
    
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("se_dict_extract_uint_h: path_keys must be non-empty table")
    end
    
    local c = o_call("SE_DICT_EXTRACT_UINT_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do
            str_hash(key)
        end
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract boolean (as int 0/1) using hash path
function se_dict_extract_bool_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_bool_h")
    
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("se_dict_extract_bool_h: path_keys must be non-empty table")
    end
    
    local c = o_call("SE_DICT_EXTRACT_BOOL_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do
            str_hash(key)
        end
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Extract hash value using hash path
function se_dict_extract_hash_h(dict_field, path_keys, dest_field)
    validate_field_is_ptr64(dict_field, "se_dict_extract_hash_h")
    
    if type(path_keys) ~= "table" or #path_keys == 0 then
        dsl_error("se_dict_extract_hash_h: path_keys must be non-empty table")
    end
    
    local c = o_call("SE_DICT_EXTRACT_HASH_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do
            str_hash(key)
        end
        field_ref(dest_field)
    end_call(c)
    return c
end

-- Store pointer to sub-dictionary/array at path
function se_dict_store_ptr(dict_field, path, dest_ptr_field)
    validate_field_is_ptr64(dict_field)
    validate_field_is_ptr64(dest_ptr_field)
    
    local call = o_call("SE_DICT_STORE_PTR")
        field_ref(dict_field)
        str(path)
        field_ref(dest_ptr_field)
    end_call(call)
end

-- Hash path variant
function se_dict_store_ptr_h(dict_field, path_keys, dest_ptr_field)
    validate_field_is_ptr64(dict_field)
    validate_field_is_ptr64(dest_ptr_field)
    
    local call = o_call("SE_DICT_STORE_PTR_H")
        field_ref(dict_field)
        for _, key in ipairs(path_keys) do
            str_hash(key)
        end
        field_ref(dest_ptr_field)
    end_call(call)
end
-- ============================================================================
-- CONVENIENCE HELPERS
-- Higher-level helpers for common patterns
-- ============================================================================

-- Extract multiple fields from dictionary using string paths
-- dict_field: PTR64_FIELD containing dictionary pointer
-- extractions: table of {path = "...", dest = "field_name", type = "int"|"float"|"uint"|"bool"|"hash"}
function se_dict_extract_all(dict_field, extractions)
    validate_field_is_ptr64(dict_field, "se_dict_extract_all")
    
    for _, ext in ipairs(extractions) do
        if not ext.path or not ext.dest then
            dsl_error("se_dict_extract_all: each extraction needs 'path' and 'dest'")
        end
        
        local typ = ext.type or "int"
        
        if typ == "int" then
            se_dict_extract_int(dict_field, ext.path, ext.dest)
        elseif typ == "float" then
            se_dict_extract_float(dict_field, ext.path, ext.dest)
        elseif typ == "uint" then
            se_dict_extract_uint(dict_field, ext.path, ext.dest)
        elseif typ == "bool" then
            se_dict_extract_bool(dict_field, ext.path, ext.dest)
        elseif typ == "hash" then
            se_dict_extract_hash(dict_field, ext.path, ext.dest)
        else
            dsl_error("se_dict_extract_all: unknown type '" .. typ .. "'")
        end
    end
end

-- Extract multiple fields using hash paths
-- dict_field: PTR64_FIELD containing dictionary pointer
-- extractions: table of {path = {"key1", "key2"}, dest = "field_name", type = "int"|"float"|...}
function se_dict_extract_all_h(dict_field, extractions)
    validate_field_is_ptr64(dict_field, "se_dict_extract_all_h")
    
    for _, ext in ipairs(extractions) do
        if not ext.path or not ext.dest then
            dsl_error("se_dict_extract_all_h: each extraction needs 'path' and 'dest'")
        end
        
        local typ = ext.type or "int"
        
        if typ == "int" then
            se_dict_extract_int_h(dict_field, ext.path, ext.dest)
        elseif typ == "float" then
            se_dict_extract_float_h(dict_field, ext.path, ext.dest)
        elseif typ == "uint" then
            se_dict_extract_uint_h(dict_field, ext.path, ext.dest)
        elseif typ == "bool" then
            se_dict_extract_bool_h(dict_field, ext.path, ext.dest)
        elseif typ == "hash" then
            se_dict_extract_hash_h(dict_field, ext.path, ext.dest)
        else
            dsl_error("se_dict_extract_all_h: unknown type '" .. typ .. "'")
        end
    end
end
-- ============================================================================
-- s_engine_stack_ops_helpers.lua
-- S-Expression Engine Stack Operations DSL Helpers
-- 
-- Provides convenient Lua functions for emitting stack-based arithmetic
-- operations in the DSL. Each helper generates the appropriate o_call
-- with any required const parameters.
--
-- Stack Notation: [-n, +m] means pop n values, push m values
-- ============================================================================

-- ============================================================================
-- BASIC ARITHMETIC [-2, +1]
-- ============================================================================
--[[
-- Add top two values: a + b
function se_stack_add()
    local c = o_call("SE_STACK_ADD")
    end_call(c)
end

-- Subtract: a - b (second from top minus top)
function se_stack_sub()
    local c = o_call("SE_STACK_SUB")
    end_call(c)
end

-- Multiply: a * b
function se_stack_mul()
    local c = o_call("SE_STACK_MUL")
    end_call(c)
end

-- Divide (float): a / b
function se_stack_div()
    local c = o_call("SE_STACK_DIV")
    end_call(c)
end

-- Modulo (float fmod): a % b
function se_stack_mod()
    local c = o_call("SE_STACK_MOD")
    end_call(c)
end

-- Integer divide: a / b (truncates)
function se_stack_idiv()
    local c = o_call("SE_STACK_IDIV")
    end_call(c)
end

-- Integer modulo: a % b
function se_stack_imod()
    local c = o_call("SE_STACK_IMOD")
    end_call(c)
end

-- ============================================================================
-- UNARY ARITHMETIC [-1, +1]
-- ============================================================================

-- Negate: -a
function se_stack_neg()
    local c = o_call("SE_STACK_NEG")
    end_call(c)
end

-- Absolute value: |a|
function se_stack_abs()
    local c = o_call("SE_STACK_ABS")
    end_call(c)
end

-- Increment: a + 1
function se_stack_inc()
    local c = o_call("SE_STACK_INC")
    end_call(c)
end

-- Decrement: a - 1
function se_stack_dec()
    local c = o_call("SE_STACK_DEC")
    end_call(c)
end

-- ============================================================================
-- BITWISE OPERATIONS [-2, +1]
-- ============================================================================

-- Bitwise AND: a & b
function se_stack_band()
    local c = o_call("SE_STACK_BAND")
    end_call(c)
end

-- Bitwise OR: a | b
function se_stack_bor()
    local c = o_call("SE_STACK_BOR")
    end_call(c)
end

-- Bitwise XOR: a ^ b
function se_stack_bxor()
    local c = o_call("SE_STACK_BXOR")
    end_call(c)
end

-- Shift left: a << b
function se_stack_shl()
    local c = o_call("SE_STACK_SHL")
    end_call(c)
end

-- Logical shift right: a >> b (unsigned)
function se_stack_shr()
    local c = o_call("SE_STACK_SHR")
    end_call(c)
end

-- Arithmetic shift right: a >> b (signed, preserves sign)
function se_stack_sar()
    local c = o_call("SE_STACK_SAR")
    end_call(c)
end

-- ============================================================================
-- UNARY BITWISE [-1, +1]
-- ============================================================================

-- Bitwise NOT: ~a
function se_stack_bnot()
    local c = o_call("SE_STACK_BNOT")
    end_call(c)
end

-- ============================================================================
-- COMPARISON [-2, +1] - push 1 or 0
-- ============================================================================

-- Equal: a == b
function se_stack_eq()
    local c = o_call("SE_STACK_EQ")
    end_call(c)
end

-- Not equal: a != b
function se_stack_ne()
    local c = o_call("SE_STACK_NE")
    end_call(c)
end

-- Less than: a < b
function se_stack_lt()
    local c = o_call("SE_STACK_LT")
    end_call(c)
end

-- Less or equal: a <= b
function se_stack_le()
    local c = o_call("SE_STACK_LE")
    end_call(c)
end

-- Greater than: a > b
function se_stack_gt()
    local c = o_call("SE_STACK_GT")
    end_call(c)
end

-- Greater or equal: a >= b
function se_stack_ge()
    local c = o_call("SE_STACK_GE")
    end_call(c)
end

-- ============================================================================
-- LOGICAL OPERATIONS [-2, +1]
-- ============================================================================

-- Logical AND: a && b (push 0 or 1)
function se_stack_and()
    local c = o_call("SE_STACK_AND")
    end_call(c)
end

-- Logical OR: a || b (push 0 or 1)
function se_stack_or()
    local c = o_call("SE_STACK_OR")
    end_call(c)
end

-- ============================================================================
-- UNARY LOGICAL [-1, +1]
-- ============================================================================

-- Logical NOT: !a (push 0 or 1)
function se_stack_not()
    local c = o_call("SE_STACK_NOT")
    end_call(c)
end

-- ============================================================================
-- MATH FUNCTIONS [-1, +1]
-- ============================================================================

function se_stack_sqrt()
    local c = o_call("SE_STACK_SQRT")
    end_call(c)
end

function se_stack_exp()
    local c = o_call("SE_STACK_EXP")
    end_call(c)
end

function se_stack_log()
    local c = o_call("SE_STACK_LOG")
    end_call(c)
end

function se_stack_log10()
    local c = o_call("SE_STACK_LOG10")
    end_call(c)
end

function se_stack_sin()
    local c = o_call("SE_STACK_SIN")
    end_call(c)
end

function se_stack_cos()
    local c = o_call("SE_STACK_COS")
    end_call(c)
end

function se_stack_tan()
    local c = o_call("SE_STACK_TAN")
    end_call(c)
end

function se_stack_asin()
    local c = o_call("SE_STACK_ASIN")
    end_call(c)
end

function se_stack_acos()
    local c = o_call("SE_STACK_ACOS")
    end_call(c)
end

function se_stack_atan()
    local c = o_call("SE_STACK_ATAN")
    end_call(c)
end

function se_stack_floor()
    local c = o_call("SE_STACK_FLOOR")
    end_call(c)
end

function se_stack_ceil()
    local c = o_call("SE_STACK_CEIL")
    end_call(c)
end

function se_stack_round()
    local c = o_call("SE_STACK_ROUND")
    end_call(c)
end

function se_stack_trunc()
    local c = o_call("SE_STACK_TRUNC")
    end_call(c)
end

-- ============================================================================
-- MATH FUNCTIONS [-2, +1]
-- ============================================================================

-- Power: a^b
function se_stack_pow()
    local c = o_call("SE_STACK_POW")
    end_call(c)
end

-- atan2(y, x)
function se_stack_atan2()
    local c = o_call("SE_STACK_ATAN2")
    end_call(c)
end

-- min(a, b)
function se_stack_min()
    local c = o_call("SE_STACK_MIN")
    end_call(c)
end

-- max(a, b)
function se_stack_max()
    local c = o_call("SE_STACK_MAX")
    end_call(c)
end

-- ============================================================================
-- MATH FUNCTIONS [-3, +1]
-- ============================================================================

-- clamp(val, min_val, max_val)
function se_stack_clamp()
    local c = o_call("SE_STACK_CLAMP")
    end_call(c)
end

-- ============================================================================
-- TYPE CONVERSION [-1, +1]
-- ============================================================================

-- Convert to signed integer
function se_stack_toint()
    local c = o_call("SE_STACK_TOINT")
    end_call(c)
end

-- Convert to unsigned integer
function se_stack_touint()
    local c = o_call("SE_STACK_TOUINT")
    end_call(c)
end

-- Convert to float
function se_stack_tofloat()
    local c = o_call("SE_STACK_TOFLOAT")
    end_call(c)
end

-- ============================================================================
-- CONSTANT PUSH [+1]
-- ============================================================================

-- Push integer constant
function se_stack_push_int(value)
    local c = o_call("SE_STACK_PUSH_CONST")
        int(value)
    end_call(c)
end

-- Push unsigned constant
function se_stack_push_uint(value)
    local c = o_call("SE_STACK_PUSH_CONST")
        uint(value)
    end_call(c)
end

-- Push float constant
function se_stack_push_float(value)
    local c = o_call("SE_STACK_PUSH_CONST")
        flt(value)
    end_call(c)
end

-- Push hash constant from string
function se_stack_push_hash(str_value)
    local c = o_call("SE_STACK_PUSH_HASH")
        str_hash(str_value)
    end_call(c)
end

-- Push pre-computed hash value
function se_stack_push_hash_value(hash_value)
    local c = o_call("SE_STACK_PUSH_HASH")
        uint(hash_value)
    end_call(c)
end

-- ============================================================================
-- IMMEDIATE OPERATIONS [-1, +1]
-- Operations with inline constant second operand
-- ============================================================================

-- Add immediate: a + const
function se_stack_addi(value)
    local c = o_call("SE_STACK_ADDI")
        int(value)
    end_call(c)
end

-- Subtract immediate: a - const
function se_stack_subi(value)
    local c = o_call("SE_STACK_SUBI")
        int(value)
    end_call(c)
end

-- Multiply immediate: a * const
function se_stack_muli(value)
    local c = o_call("SE_STACK_MULI")
        int(value)
    end_call(c)
end

-- Divide immediate: a / const
function se_stack_divi(value)
    local c = o_call("SE_STACK_DIVI")
        int(value)
    end_call(c)
end

-- Modulo immediate: a % const
function se_stack_modi(value)
    local c = o_call("SE_STACK_MODI")
        int(value)
    end_call(c)
end

-- Shift left immediate: a << const
function se_stack_shli(value)
    local c = o_call("SE_STACK_SHLI")
        uint(value)
    end_call(c)
end

-- Shift right immediate (logical): a >> const
function se_stack_shri(value)
    local c = o_call("SE_STACK_SHRI")
        uint(value)
    end_call(c)
end

-- Shift right immediate (arithmetic): a >> const
function se_stack_sari(value)
    local c = o_call("SE_STACK_SARI")
        uint(value)
    end_call(c)
end

-- Bitwise AND immediate: a & const
function se_stack_bandi(value)
    local c = o_call("SE_STACK_BANDI")
        uint(value)
    end_call(c)
end

-- Bitwise OR immediate: a | const
function se_stack_bori(value)
    local c = o_call("SE_STACK_BORI")
        uint(value)
    end_call(c)
end

-- Bitwise XOR immediate: a ^ const
function se_stack_bxori(value)
    local c = o_call("SE_STACK_BXORI")
        uint(value)
    end_call(c)
end

-- ============================================================================
-- BLACKBOARD FIELD OPERATIONS
-- ============================================================================

-- Load field as signed integer [+1]
function se_stack_load_int(field_name)
    local c = o_call("SE_STACK_LOAD_INT")
        field_ref(field_name)
    end_call(c)
end

-- Load field as unsigned integer [+1]
function se_stack_load_uint(field_name)
    local c = o_call("SE_STACK_LOAD_UINT")
        field_ref(field_name)
    end_call(c)
end

-- Load field as float [+1]
function se_stack_load_float(field_name)
    local c = o_call("SE_STACK_LOAD_FLOAT")
        field_ref(field_name)
    end_call(c)
end

-- Load 8-byte field as pointer [+1]
function se_stack_load_ptr64(field_name)
    local c = o_call("SE_STACK_LOAD_PTR64")
        field_ref(field_name)
    end_call(c)
end

-- Store top as signed integer [-1]
function se_stack_store_int(field_name)
    local c = o_call("SE_STACK_STORE_INT")
        field_ref(field_name)
    end_call(c)
end

-- Store top as unsigned integer [-1]
function se_stack_store_uint(field_name)
    local c = o_call("SE_STACK_STORE_UINT")
        field_ref(field_name)
    end_call(c)
end

-- Store top as float [-1]
function se_stack_store_float(field_name)
    local c = o_call("SE_STACK_STORE_FLOAT")
        field_ref(field_name)
    end_call(c)
end

-- Store top as 8-byte pointer [-1]
function se_stack_store_ptr64(field_name)
    local c = o_call("SE_STACK_STORE_PTR64")
        field_ref(field_name)
    end_call(c)
end

-- Nested field variants
function se_stack_load_int_nested(field_path)
    local c = o_call("SE_STACK_LOAD_INT")
        nested_field_ref(field_path)
    end_call(c)
end

function se_stack_load_uint_nested(field_path)
    local c = o_call("SE_STACK_LOAD_UINT")
        nested_field_ref(field_path)
    end_call(c)
end

function se_stack_load_float_nested(field_path)
    local c = o_call("SE_STACK_LOAD_FLOAT")
        nested_field_ref(field_path)
    end_call(c)
end

function se_stack_store_int_nested(field_path)
    local c = o_call("SE_STACK_STORE_INT")
        nested_field_ref(field_path)
    end_call(c)
end

function se_stack_store_uint_nested(field_path)
    local c = o_call("SE_STACK_STORE_UINT")
        nested_field_ref(field_path)
    end_call(c)
end

function se_stack_store_float_nested(field_path)
    local c = o_call("SE_STACK_STORE_FLOAT")
        nested_field_ref(field_path)
    end_call(c)
end

-- ============================================================================
-- STACK MANIPULATION
-- ============================================================================

-- Drop top value [-1, +0]
function se_stack_drop()
    local c = o_call("SE_STACK_DROP")
    end_call(c)
end

-- Drop top two values [-2, +0]
function se_stack_drop2()
    local c = o_call("SE_STACK_DROP2")
    end_call(c)
end

-- Drop n values [-n, +0]
function se_stack_dropn(count)
    local c = o_call("SE_STACK_DROPN")
        uint(count)
    end_call(c)
end

-- Duplicate top [-1, +2] (a -- a a)
function se_stack_dup()
    local c = o_call("SE_STACK_DUP")
    end_call(c)
end

-- Duplicate top two [-2, +4] (a b -- a b a b)
function se_stack_dup2()
    local c = o_call("SE_STACK_DUP2")
    end_call(c)
end

-- Swap top two [-2, +2] (a b -- b a)
function se_stack_swap()
    local c = o_call("SE_STACK_SWAP")
    end_call(c)
end

-- Copy second to top [-2, +3] (a b -- a b a)
function se_stack_over()
    local c = o_call("SE_STACK_OVER")
    end_call(c)
end

-- Rotate three [-3, +3] (a b c -- b c a)
function se_stack_rot()
    local c = o_call("SE_STACK_ROT")
    end_call(c)
end

-- Reverse rotate [-3, +3] (a b c -- c a b)
function se_stack_nrot()
    local c = o_call("SE_STACK_NROT")
    end_call(c)
end

-- Pick nth item (0 = top) and copy to top
function se_stack_pick(index)
    local c = o_call("SE_STACK_PICK")
        uint(index)
    end_call(c)
end

-- Roll: rotate n items
function se_stack_roll(count)
    local c = o_call("SE_STACK_ROLL")
        uint(count)
    end_call(c)
end

-- ============================================================================
-- CONDITIONAL OPERATIONS
-- ============================================================================

-- Select: (cond a b -- result) if cond!=0 then a else b
function se_stack_select()
    local c = o_call("SE_STACK_SELECT")
    end_call(c)
end

-- ============================================================================
-- HASH OPERATIONS
-- ============================================================================

-- Compare two hashes on stack
function se_stack_hash_eq()
    local c = o_call("SE_STACK_HASH_EQ")
    end_call(c)
end

-- ============================================================================
-- COMPOUND OPERATIONS (convenience macros)
-- ============================================================================

-- Load field, add constant, store back
function se_stack_field_add(field_name, value)
    se_stack_load_int(field_name)
    se_stack_addi(value)
    se_stack_store_int(field_name)
end

-- Load field, increment, store back
function se_stack_field_inc(field_name)
    se_stack_load_int(field_name)
    se_stack_inc()
    se_stack_store_int(field_name)
end

-- Load field, decrement, store back
function se_stack_field_dec(field_name)
    se_stack_load_int(field_name)
    se_stack_dec()
    se_stack_store_int(field_name)
end

-- Load two fields, compare, leave result on stack
function se_stack_compare_fields(field_a, field_b, op)
    se_stack_load_int(field_a)
    se_stack_load_int(field_b)
    if op == "==" or op == "eq" then
        se_stack_eq()
    elseif op == "!=" or op == "ne" then
        se_stack_ne()
    elseif op == "<" or op == "lt" then
        se_stack_lt()
    elseif op == "<=" or op == "le" then
        se_stack_le()
    elseif op == ">" or op == "gt" then
        se_stack_gt()
    elseif op == ">=" or op == "ge" then
        se_stack_ge()
    else
        dsl_error("Unknown comparison operator: " .. tostring(op))
    end
end

-- Load field, apply binary op with constant, store back
function se_stack_field_op(field_name, op, value)
    se_stack_load_int(field_name)
    if op == "+" then
        se_stack_addi(value)
    elseif op == "-" then
        se_stack_subi(value)
    elseif op == "*" then
        se_stack_muli(value)
    elseif op == "/" then
        se_stack_divi(value)
    elseif op == "%" then
        se_stack_modi(value)
    elseif op == "&" then
        se_stack_bandi(value)
    elseif op == "|" then
        se_stack_bori(value)
    elseif op == "^" then
        se_stack_bxori(value)
    elseif op == "<<" then
        se_stack_shli(value)
    elseif op == ">>" then
        se_stack_shri(value)
    else
        dsl_error("Unknown operator: " .. tostring(op))
    end
    se_stack_store_int(field_name)
end
--]]
print("S-Expression Engine helpers loaded (v5.2)")

