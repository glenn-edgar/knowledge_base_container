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
-- Execute children sequentially, each gets one tick, then terminate.
-- Wraps SE_SEQUENCE_ONCE builtin (m_call).
function se_sequence_once(...)
    local children = {...}
    if #children == 0 then
        dsl_error("se_sequence_once: requires at least one child function")
    end
    local c = m_call("SE_SEQUENCE_ONCE")
        for _, child_fn in ipairs(children) do
            if type(child_fn) ~= "function" then
                dsl_error("se_sequence_once: all arguments must be functions")
            end
            child_fn()
        end
    end_call(c)
end

-- Low-level: emits SE_STACK_FRAME_INSTANCE builtin node
function se_stack_frame_instance(num_params, num_locals, scratch_depth, return_vars)
    if type(num_params) ~= "number" or num_params < 0 then
        dsl_error("se_stack_frame_instance: num_params must be non-negative number")
    end
    if type(num_locals) ~= "number" or num_locals < 0 then
        dsl_error("se_stack_frame_instance: num_locals must be non-negative number")
    end
    if type(scratch_depth) ~= "number" or scratch_depth < 0 then
        dsl_error("se_stack_frame_instance: scratch_depth must be non-negative number")
    end
    if type(return_vars) ~= "table" then
        dsl_error("se_stack_frame_instance: return_vars must be a table")
    end

    local max_local = num_params + num_locals
    for i, idx in ipairs(return_vars) do
        if type(idx) ~= "number" or idx < 0 or idx >= max_local then
            dsl_error("se_stack_frame_instance: return_vars[" .. i .. "] = " .. tostring(idx) ..
                      " out of range (valid: 0.." .. (max_local - 1) .. ")")
        end
    end

    num_params    = math.floor(num_params)
    num_locals    = math.floor(num_locals)
    scratch_depth = math.floor(scratch_depth)

    local c = pt_m_call("SE_STACK_FRAME_INSTANCE")
        uint(num_params)
        uint(num_locals)
        uint(scratch_depth)
        local l = list_start()
            for _, idx in ipairs(return_vars) do
                uint(math.floor(idx))
            end
        list_end(l)
    end_call(c)
end

-- Wrapper: establishes a stack frame around a list of body functions.
-- Emits SE_SEQUENCE_ONCE containing SE_STACK_FRAME_INSTANCE + body children.
-- Manages compile-time frame_stack for bounds checking stack_local/stack_tos.
--
-- num_params:     number of parameters (already on stack from caller)
-- num_locals:     number of local variable slots (zeroed on init)
-- scratch_depth:  maximum scratch stack space
-- return_vars:    list of stack_local indices to return to caller
-- body_fns:       list of functions, each emitting child nodes with frame active
function se_call(num_params, num_locals, scratch_depth, return_vars, body_fns)
    if type(body_fns) ~= "table" or #body_fns == 0 then
        dsl_error("se_call: body_fns must be a non-empty list of functions")
    end
    for i, fn in ipairs(body_fns) do
        if type(fn) ~= "function" then
            dsl_error("se_call: body_fns[" .. i .. "] must be a function")
        end
    end

    -- Validate return_vars against frame size before pushing frame
    local max_local = num_params + num_locals
    for i, idx in ipairs(return_vars) do
        if type(idx) ~= "number" or idx < 0 or idx >= max_local then
            dsl_error("se_call: return_vars[" .. i .. "] = " .. tostring(idx) ..
                      " out of range (valid: 0.." .. (max_local - 1) .. ")")
        end
    end

    -- Push compile-time frame for bounds checking stack_local/stack_tos
    table.insert(frame_stack, {
        num_params    = num_params,
        num_locals    = num_locals,
        scratch_depth = scratch_depth,
    })

    se_sequence_once(
        function()
            se_push_stack(function() uint(num_params) end)
            se_stack_frame_instance(num_params, num_locals, scratch_depth, return_vars)
            for _, fn in ipairs(body_fns) do
                fn()
            end
        end
    )
        
    

    -- Pop compile-time frame
    table.remove(frame_stack)
end

function se_frame_allocate(num_params, num_locals, scratch_depth)
    local c = m_call("SE_FRAME_ALLOCATE")
        uint(num_params)
        uint(num_locals)
        uint(scratch_depth)
    end_call(c)
end

function se_frame_free()
    local c = m_call("SE_FRAME_FREE")
    end_call(c)
end

SE_QUAD_OP = {
    -- Integer Arithmetic
    IADD         = 0x00,   -- dest = src1 + src2
    ISUB         = 0x01,   -- dest = src1 - src2
    IMUL         = 0x02,   -- dest = src1 * src2
    IDIV         = 0x03,   -- dest = src1 / src2
    IMOD         = 0x04,   -- dest = src1 % src2
    INEG         = 0x05,   -- dest = -src1 (src2 = null_param)

    -- Float Arithmetic
    FADD         = 0x08,   -- dest = src1 + src2
    FSUB         = 0x09,   -- dest = src1 - src2
    FMUL         = 0x0A,   -- dest = src1 * src2
    FDIV         = 0x0B,   -- dest = src1 / src2
    FMOD         = 0x0C,   -- dest = src1 % src2
    FNEG         = 0x0D,   -- dest = -src1 (src2 = null_param)

    -- Bitwise (integer only)
    BIT_AND      = 0x10,   -- dest = src1 & src2
    BIT_OR       = 0x11,   -- dest = src1 | src2
    BIT_XOR      = 0x12,   -- dest = src1 ^ src2
    BIT_NOT      = 0x13,   -- dest = ~src1 (src2 = null_param)
    BIT_SHL      = 0x14,   -- dest = src1 << src2
    BIT_SHR      = 0x15,   -- dest = src1 >> src2

    -- Integer Comparison (dest = 1 or 0)
    ICMP_EQ      = 0x20,   -- dest = (src1 == src2)
    ICMP_NE      = 0x21,   -- dest = (src1 != src2)
    ICMP_LT      = 0x22,   -- dest = (src1 < src2)
    ICMP_LE      = 0x23,   -- dest = (src1 <= src2)
    ICMP_GT      = 0x24,   -- dest = (src1 > src2)
    ICMP_GE      = 0x25,   -- dest = (src1 >= src2)

    -- Float Comparison (dest = 1 or 0)
    FCMP_EQ      = 0x28,   -- dest = (src1 == src2)
    FCMP_NE      = 0x29,   -- dest = (src1 != src2)
    FCMP_LT      = 0x2A,   -- dest = (src1 < src2)
    FCMP_LE      = 0x2B,   -- dest = (src1 <= src2)
    FCMP_GT      = 0x2C,   -- dest = (src1 > src2)
    FCMP_GE      = 0x2D,   -- dest = (src1 >= src2)

    -- Logical (dest = 1 or 0)
    LOG_AND      = 0x30,   -- dest = (src1 && src2)
    LOG_OR       = 0x31,   -- dest = (src1 || src2)
    LOG_NOT      = 0x32,   -- dest = !src1 (src2 = null_param)
    LOG_NAND     = 0x33,   -- dest = !(src1 && src2)
    LOG_NOR      = 0x34,   -- dest = !(src1 || src2)
    LOG_XOR      = 0x35,   -- dest = (src1 && !src2) || (!src1 && src2)

    -- Move
    MOVE         = 0x40,   -- dest = src1 (src2 = null_param)

    -- Float Math Functions (src2 = null_param unless noted)
    FSQRT        = 0x50,   -- dest = sqrt(src1)
    FPOW         = 0x51,   -- dest = src1 ^ src2
    FEXP         = 0x52,   -- dest = e^src1
    FLOG         = 0x53,   -- dest = ln(src1)
    FLOG10       = 0x54,   -- dest = log10(src1)
    FLOG2        = 0x55,   -- dest = log2(src1)
    FABS         = 0x56,   -- dest = |src1|

    -- Trigonometric (float, radians)
    FSIN         = 0x58,   -- dest = sin(src1)
    FCOS         = 0x59,   -- dest = cos(src1)
    FTAN         = 0x5A,   -- dest = tan(src1)
    FASIN        = 0x5B,   -- dest = asin(src1)
    FACOS        = 0x5C,   -- dest = acos(src1)
    FATAN        = 0x5D,   -- dest = atan(src1)
    FATAN2       = 0x5E,   -- dest = atan2(src1, src2)

    -- Hyperbolic (float)
    FSINH        = 0x60,   -- dest = sinh(src1)
    FCOSH        = 0x61,   -- dest = cosh(src1)
    FTANH        = 0x62,   -- dest = tanh(src1)

    -- Integer Math
    IABS         = 0x68,   -- dest = |src1| (src2 = null_param)
    IMIN         = 0x69,   -- dest = min(src1, src2)
    IMAX         = 0x6A,   -- dest = max(src1, src2)

    -- Float Min/Max
    FMIN         = 0x6C,   -- dest = min(src1, src2)
    FMAX         = 0x6D,   -- dest = max(src1, src2)
    MOV          = 0x6E,   -- dest = src1 (src2 = null_param)
}


-- ============================================================================
-- QUAD OPERATOR (oneshot)
-- Single three-address instruction: dest = op(src1, src2)
-- Parameters can be any DSL param type: stack_local, stack_tos, field_ref,
-- const_ref, int, flt, null_param, etc.
-- ============================================================================

-- Lookup set for compile-time validation
local SE_QUAD_OP_SET = {}
for name, val in pairs(SE_QUAD_OP) do
    SE_QUAD_OP_SET[val] = name
end

function se_quad(opcode, src1_fn, src2_fn, dest_fn)
    if type(opcode) ~= "number" then
        dsl_error("se_quad: opcode must be a number")
    end
    if not SE_QUAD_OP_SET[opcode] then
        dsl_error("se_quad: unknown opcode 0x" .. string.format("%02X", opcode) ..
                  " - not a valid SE_QUAD_OP")
    end
    if type(src1_fn) ~= "function" then
        dsl_error("se_quad: src1 must be a function emitting a parameter")
    end
    if type(src2_fn) ~= "function" then
        dsl_error("se_quad: src2 must be a function emitting a parameter")
    end
    if type(dest_fn) ~= "function" then
        dsl_error("se_quad: dest must be a function emitting a parameter")
    end

    local c = o_call("SE_QUAD")
        uint(opcode)
        src1_fn()
        src2_fn()
        dest_fn()
    end_call(c)
end


function se_push_stack(value_fn)
    if type(value_fn) ~= "function" then
        dsl_error("se_push_stack: value must be a function emitting a parameter")
    end
    
    local c = o_call("SE_PUSH_STACK")
        value_fn()
    end_call(c)
end

print("S-Expression Engine helpers loaded (v5.2)")

-- ============================================================================
-- QUAD HELPER FUNCTIONS
-- Convenience wrappers for SE_QUAD opcodes
-- ============================================================================

-- ============================================================================
-- INTEGER ARITHMETIC
-- ============================================================================

function local_ref(idx)
    return function() stack_local(idx) end
end

function tos_ref(offset)
    return function() stack_tos(offset) end
end

function int_val(v)
    return function() int(v) end
end

function uint_val(v)
    return function() uint(v) end
end

function float_val(v)
    return function() flt(v) end
end

function field_val(name)
    return function() field_ref(name) end
end

function const_val(name)
    return function() const_ref(name) end
end

function hash_val(s)
    return function() str_hash(s) end
end

function null_val()
    return function() null_param() end
end

function quad_iadd(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.IADD, src1_fn, src2_fn, dest_fn)
end

function quad_isub(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ISUB, src1_fn, src2_fn, dest_fn)
end

function quad_imul(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.IMUL, src1_fn, src2_fn, dest_fn)
end

function quad_idiv(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.IDIV, src1_fn, src2_fn, dest_fn)
end

function quad_imod(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.IMOD, src1_fn, src2_fn, dest_fn)
end

function quad_ineg(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.INEG, src_fn, null_param, dest_fn)
end

-- ============================================================================
-- FLOAT ARITHMETIC
-- ============================================================================

function quad_fadd(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FADD, src1_fn, src2_fn, dest_fn)
end

function quad_fsub(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FSUB, src1_fn, src2_fn, dest_fn)
end

function quad_fmul(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FMUL, src1_fn, src2_fn, dest_fn)
end

function quad_fdiv(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FDIV, src1_fn, src2_fn, dest_fn)
end

function quad_fmod(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FMOD, src1_fn, src2_fn, dest_fn)
end

function quad_fneg(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FNEG, src_fn, null_param, dest_fn)
end

-- ============================================================================
-- BITWISE OPERATIONS
-- ============================================================================

function quad_and(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.BIT_AND, src1_fn, src2_fn, dest_fn)
end

function quad_or(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.BIT_OR, src1_fn, src2_fn, dest_fn)
end

function quad_xor(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.BIT_XOR, src1_fn, src2_fn, dest_fn)
end

function quad_not(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.BIT_NOT, src_fn, null_param, dest_fn)
end

function quad_shl(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.BIT_SHL, src1_fn, src2_fn, dest_fn)
end

function quad_shr(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.BIT_SHR, src1_fn, src2_fn, dest_fn)
end

-- ============================================================================
-- INTEGER COMPARISON
-- ============================================================================

function quad_ieq(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ICMP_EQ, src1_fn, src2_fn, dest_fn)
end

function quad_ine(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ICMP_NE, src1_fn, src2_fn, dest_fn)
end

function quad_ilt(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ICMP_LT, src1_fn, src2_fn, dest_fn)
end

function quad_ile(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ICMP_LE, src1_fn, src2_fn, dest_fn)
end

function quad_igt(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ICMP_GT, src1_fn, src2_fn, dest_fn)
end

function quad_ige(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.ICMP_GE, src1_fn, src2_fn, dest_fn)
end

-- ============================================================================
-- FLOAT COMPARISON
-- ============================================================================

function quad_feq(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCMP_EQ, src1_fn, src2_fn, dest_fn)
end

function quad_fne(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCMP_NE, src1_fn, src2_fn, dest_fn)
end

function quad_flt(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCMP_LT, src1_fn, src2_fn, dest_fn)
end

function quad_fle(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCMP_LE, src1_fn, src2_fn, dest_fn)
end

function quad_fgt(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCMP_GT, src1_fn, src2_fn, dest_fn)
end

function quad_fge(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCMP_GE, src1_fn, src2_fn, dest_fn)
end

-- ============================================================================
-- LOGICAL OPERATIONS
-- ============================================================================

function quad_log_and(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.LOG_AND, src1_fn, src2_fn, dest_fn)
end

function quad_log_or(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.LOG_OR, src1_fn, src2_fn, dest_fn)
end

function quad_log_not(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.LOG_NOT, src_fn, null_param, dest_fn)
end

function quad_log_nand(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.LOG_NAND, src1_fn, src2_fn, dest_fn)
end

function quad_log_nor(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.LOG_NOR, src1_fn, src2_fn, dest_fn)
end

function quad_log_xor(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.LOG_XOR, src1_fn, src2_fn, dest_fn)
end

-- ============================================================================
-- MOVE
-- ============================================================================

function quad_mov(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.MOVE, src_fn, null_param, dest_fn)
end

-- ============================================================================
-- FLOAT MATH FUNCTIONS
-- ============================================================================

function quad_sqrt(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FSQRT, src_fn, null_param, dest_fn)
end

function quad_pow(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FPOW, src1_fn, src2_fn, dest_fn)
end

function quad_exp(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FEXP, src_fn, null_param, dest_fn)
end

function quad_log(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FLOG, src_fn, null_param, dest_fn)
end

function quad_log10(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FLOG10, src_fn, null_param, dest_fn)
end

function quad_log2(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FLOG2, src_fn, null_param, dest_fn)
end

function quad_fabs(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FABS, src_fn, null_param, dest_fn)
end

-- ============================================================================
-- TRIGONOMETRIC
-- ============================================================================

function quad_sin(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FSIN, src_fn, null_param, dest_fn)
end

function quad_cos(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCOS, src_fn, null_param, dest_fn)
end

function quad_tan(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FTAN, src_fn, null_param, dest_fn)
end

function quad_asin(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FASIN, src_fn, null_param, dest_fn)
end

function quad_acos(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FACOS, src_fn, null_param, dest_fn)
end

function quad_atan(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FATAN, src_fn, null_param, dest_fn)
end

function quad_atan2(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FATAN2, src1_fn, src2_fn, dest_fn)
end

-- ============================================================================
-- HYPERBOLIC
-- ============================================================================

function quad_sinh(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FSINH, src_fn, null_param, dest_fn)
end

function quad_cosh(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FCOSH, src_fn, null_param, dest_fn)
end

function quad_tanh(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.FTANH, src_fn, null_param, dest_fn)
end

-- ============================================================================
-- INTEGER MATH
-- ============================================================================

function quad_iabs(src_fn, dest_fn)
    se_quad(SE_QUAD_OP.IABS, src_fn, null_param, dest_fn)
end

function quad_imin(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.IMIN, src1_fn, src2_fn, dest_fn)
end

function quad_imax(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.IMAX, src1_fn, src2_fn, dest_fn)
end

-- ============================================================================
-- FLOAT MIN/MAX
-- ============================================================================

function quad_fmin(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FMIN, src1_fn, src2_fn, dest_fn)
end

function quad_fmax(src1_fn, src2_fn, dest_fn)
    se_quad(SE_QUAD_OP.FMAX, src1_fn, src2_fn, dest_fn)
end

--[[

-- Add two locals, store in third
quad_iadd(
    function() stack_local(0) end,
    function() stack_local(1) end,
    function() stack_local(2) end
)

-- Negate a field into a local
quad_ineg(
    function() field_ref("counter") end,
    function() stack_local(0) end
)

-- Compare TOS values
quad_ilt(
    function() stack_tos(0) end,
    function() stack_tos(1) end,
    function() stack_local(3) end
)

-- Compute sine of field
quad_sin(
    function() field_ref("angle") end,
    function() field_ref("sin_angle") end
)

-- Move constant to local
quad_mov(
    function() int(42) end,
    function() stack_local(0) end
)
--]]


-- ============================================================================
-- QUAD OPERATOR (oneshot)
-- Single three-address instruction: dest = op(src1, src2)
-- Parameters can be any DSL param type: stack_local, stack_tos, field_ref,
-- const_ref, int, flt, null_param, etc.
-- ============================================================================

SE_P_QUAD_OP = {
    -- Bitwise (integer only)
    BIT_AND      = 0x10,   -- dest = src1 & src2
    BIT_OR       = 0x11,   -- dest = src1 | src2
    BIT_XOR      = 0x12,   -- dest = src1 ^ src2
    BIT_NOT      = 0x13,   -- dest = ~src1 (src2 = null_param)
    BIT_SHL      = 0x14,   -- dest = src1 << src2
    BIT_SHR      = 0x15,   -- dest = src1 >> src2

    -- Integer Comparison (dest = 1 or 0)
    ICMP_EQ      = 0x20,   -- dest = (src1 == src2)
    ICMP_NE      = 0x21,   -- dest = (src1 != src2)
    ICMP_LT      = 0x22,   -- dest = (src1 < src2)
    ICMP_LE      = 0x23,   -- dest = (src1 <= src2)
    ICMP_GT      = 0x24,   -- dest = (src1 > src2)
    ICMP_GE      = 0x25,   -- dest = (src1 >= src2)

    -- Float Comparison (dest = 1 or 0)
    FCMP_EQ      = 0x28,   -- dest = (src1 == src2)
    FCMP_NE      = 0x29,   -- dest = (src1 != src2)
    FCMP_LT      = 0x2A,   -- dest = (src1 < src2)
    FCMP_LE      = 0x2B,   -- dest = (src1 <= src2)
    FCMP_GT      = 0x2C,   -- dest = (src1 > src2)
    FCMP_GE      = 0x2D,   -- dest = (src1 >= src2)

    -- Logical (dest = 1 or 0)
    LOG_AND      = 0x30,   -- dest = (src1 && src2)
    LOG_OR       = 0x31,   -- dest = (src1 || src2)
    LOG_NOT      = 0x32,   -- dest = !src1 (src2 = null_param)
    LOG_NAND     = 0x33,   -- dest = !(src1 && src2)
    LOG_NOR      = 0x34,   -- dest = !(src1 || src2)
    LOG_XOR      = 0x35,   -- dest = (src1 && !src2) || (!src1 && src2)
}


-- Lookup set for compile-time validation
local SE_P_QUAD_OP_SET = {}
for name, val in pairs(SE_QUAD_OP) do
    SE_QUAD_OP_SET[val] = name
end

-- Add SE_QUAD to BUILTIN_FUNCTIONS predicate section
-- "SE_QUAD",



function se_p_quad(opcode, src1_fn, src2_fn, dest_fn)
    if type(opcode) ~= "number" then
        dsl_error("se_quad: opcode must be a number")
    end
    if not SE_P_QUAD_OP_SET[opcode] then
        dsl_error("se_quad: unknown opcode 0x" .. string.format("%02X", opcode) ..
                  " - not a valid SE_QUAD_OP")
    end
    if type(src1_fn) ~= "function" then
        dsl_error("se_quad: src1 must be a function emitting a parameter")
    end
    if type(src2_fn) ~= "function" then
        dsl_error("se_quad: src2 must be a function emitting a parameter")
    end
    if type(dest_fn) ~= "function" then
        dsl_error("se_quad: dest must be a function emitting a parameter")
    end

    local c = p_call("SE_QUAD")
        uint(opcode)
        src1_fn()
        src2_fn()
        dest_fn()
    end_call(c)
end

--- ============================================================================
-- QUAD BOOLEAN HELPER FUNCTIONS
-- Convenience wrappers for se_quad predicate operations.
-- Each takes parameter-emitting functions for src1, src2 (where applicable),
-- and dest (where the 1/0 result is stored).
-- ============================================================================

-- Bitwise
function p_bit_and(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.BIT_AND, src1_fn, src2_fn, dest_fn)
end

function p_bit_or(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.BIT_OR, src1_fn, src2_fn, dest_fn)
end

function p_bit_xor(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.BIT_XOR, src1_fn, src2_fn, dest_fn)
end

function p_bit_not(src1_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.BIT_NOT, src1_fn, function() null_param() end, dest_fn)
end

function p_bit_shl(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.BIT_SHL, src1_fn, src2_fn, dest_fn)
end

function p_bit_shr(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.BIT_SHR, src1_fn, src2_fn, dest_fn)
end

-- Integer Comparison
function p_icmp_eq(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.ICMP_EQ, src1_fn, src2_fn, dest_fn)
end

function p_icmp_ne(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.ICMP_NE, src1_fn, src2_fn, dest_fn)
end

function p_icmp_lt(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.ICMP_LT, src1_fn, src2_fn, dest_fn)
end

function p_icmp_le(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.ICMP_LE, src1_fn, src2_fn, dest_fn)
end

function p_icmp_gt(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.ICMP_GT, src1_fn, src2_fn, dest_fn)
end

function p_icmp_ge(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.ICMP_GE, src1_fn, src2_fn, dest_fn)
end

-- Float Comparison
function p_fcmp_eq(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.FCMP_EQ, src1_fn, src2_fn, dest_fn)
end

function p_fcmp_ne(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.FCMP_NE, src1_fn, src2_fn, dest_fn)
end

function p_fcmp_lt(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.FCMP_LT, src1_fn, src2_fn, dest_fn)
end

function p_fcmp_le(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.FCMP_LE, src1_fn, src2_fn, dest_fn)
end

function p_fcmp_gt(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.FCMP_GT, src1_fn, src2_fn, dest_fn)
end

function p_fcmp_ge(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.FCMP_GE, src1_fn, src2_fn, dest_fn)
end

-- Logical
function p_log_and(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.LOG_AND, src1_fn, src2_fn, dest_fn)
end

function p_log_or(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.LOG_OR, src1_fn, src2_fn, dest_fn)
end

function p_log_not(src1_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.LOG_NOT, src1_fn, function() null_param() end, dest_fn)
end

function p_log_nand(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.LOG_NAND, src1_fn, src2_fn, dest_fn)
end

function se_log_nor(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.LOG_NOR, src1_fn, src2_fn, dest_fn)
end

function p_log_xor(src1_fn, src2_fn, dest_fn)
    se_p_quad(SE_P_QUAD_OP.LOG_XOR, src1_fn, src2_fn, dest_fn)
end

-- Range check: dest = (low <= src && src <= high)
-- Emits two comparisons and a logical AND using scratch locations.
-- Requires 2 scratch slots for intermediate results.
--
-- src_fn:     function emitting the value to test
-- low_fn:     function emitting the low bound (inclusive)
-- high_fn:    function emitting the high bound (inclusive)
-- dest_fn:    function emitting the destination for result (1/0)
-- scratch1_fn: function emitting scratch location for (low <= src)
-- scratch2_fn: function emitting scratch location for (src <= high)

function p_icmp_in_range(src_fn, low_fn, high_fn, dest_fn, scratch1_fn, scratch2_fn)
    -- scratch1 = (low <= src)
    p_icmp_le(low_fn, src_fn, scratch1_fn)
    -- scratch2 = (src <= high)
    p_icmp_le(src_fn, high_fn, scratch2_fn)
    -- dest = scratch1 && scratch2
    p_log_and(scratch1_fn, scratch2_fn, dest_fn)
end

function p_fcmp_in_range(src_fn, low_fn, high_fn, dest_fn, scratch1_fn, scratch2_fn)
    -- scratch1 = (low <= src)
    p_fcmp_le(low_fn, src_fn, scratch1_fn)
    -- scratch2 = (src <= high)
    p_fcmp_le(src_fn, high_fn, scratch2_fn)
    -- dest = scratch1 && scratch2
    p_log_and(scratch1_fn, scratch2_fn, dest_fn)
end

