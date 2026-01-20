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
-- RESULT CODE FUNCTIONS
-- 
-- These return specific result codes to control execution flow.
-- Only SE_CONTINUE and SE_DISABLE continue to next node.
-- All others terminate the current tick and propagate to caller.
--============================================================================
function se_set_result(result)
    if result == SE_CONTINUE then
        se_return_continue()
    elseif result == SE_DISABLE then
        se_return_disable()
    elseif result == SE_HALT then
        se_return_halt()
    elseif result == SE_RESET then
        se_return_reset()
    end
end
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

function se_nop()
    local c = m_call("SE_NOP")
    end_call(c)
end

--============================================================================
-- STATE MACHINE FUNCTIONS
--============================================================================

-- Index-based state machine (original)
-- Usage: se_state_machine("state_field", {state0_fn, state1_fn, state2_fn})
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

-- State actions helper
function se_state_actions(return_code, actions_fn)
    local c = m_call("SE_STATE_ACTIONS")
        actions_fn()
        result(return_code)
    end_call(c)
end

-- NEW: Named state machine with dictionary (string state names)
-- Usage: se_named_state_machine("state_field", {
--     {"IDLE", idle_fn},
--     {"RUNNING", running_fn},
--     {"ERROR", error_fn},
-- })
function se_named_state_machine(state_field, states)
    local c = m_call("SE_NAMED_STATE_MACHINE")
        field_ref(state_field)
        local d = dict_start("states")
            for _, state in ipairs(states) do
                local state_name = state[1]
                local state_fn = state[2]
                local k = dict_key(state_name)
                    local s = m_call("SE_STATE_ACTIONS")
                        state_fn()
                    end_call(s)
                end_dict_key(k)
            end
        dict_end(d)
    end_call(c)
end

-- NEW: Named state machine with table syntax (unordered)
-- Usage: se_named_state_machine_table("state_field", {
--     IDLE = idle_fn,
--     RUNNING = running_fn,
--     ERROR = error_fn,
-- })
function se_named_state_machine_table(state_field, states)
    local c = m_call("SE_NAMED_STATE_MACHINE")
        field_ref(state_field)
        local d = dict_start("states")
            for state_name, state_fn in pairs(states) do
                local k = dict_key(state_name)
                    local s = m_call("SE_STATE_ACTIONS")
                        state_fn()
                    end_call(s)
                end_dict_key(k)
            end
        dict_end(d)
    end_call(c)
end

--============================================================================
-- DISPATCH FUNCTIONS
--============================================================================

-- Integer dispatch (original, with improved list structure)
-- Usage: se_dispatch({
--     {0, action0_fn},
--     {1, action1_fn},
--     {2, action2_fn},
-- })
function se_dispatch(cases)
    local c = m_call("SE_DISPATCH")
        local case_list = list_start("cases")
            for _, case in ipairs(cases) do
                local case_val = case[1]
                local action_fn = case[2]
                local l = list_start("case")
                    int(case_val)
                    action_fn()
                list_end(l)
            end
        list_end(case_list)
    end_call(c)
end

-- Field-based integer dispatch
function se_field_dispatch(field_name, cases)
    local c = m_call("SE_FIELD_DISPATCH")
        field_ref(field_name)
        for _, case in ipairs(cases) do
            local case_val = case[1]
            local action_fn = case[2]
            int(case_val)
            se_pipeline(action_fn)
        end
    end_call(c)
end

-- NEW: String-based dispatch using dictionary (hash lookup)
-- Usage: se_string_dispatch("command_field", {
--     {"START", start_fn},
--     {"STOP", stop_fn},
--     {"RESET", reset_fn},
--     {"DEFAULT", default_fn},
-- })
function se_string_dispatch(field_name, cases)
    local c = m_call("SE_STRING_DISPATCH")
        field_ref(field_name)
        local d = dict_start("cases")
            for _, case in ipairs(cases) do
                local pattern = case[1]
                local action_fn = case[2]
                local k = dict_key(pattern)
                    action_fn()
                end_dict_key(k)
            end
        dict_end(d)
    end_call(c)
end

-- NEW: String dispatch with table syntax (unordered)
-- Usage: se_string_dispatch_table("command_field", {
--     START = start_fn,
--     STOP = stop_fn,
--     DEFAULT = default_fn,
-- })
function se_string_dispatch_table(field_name, cases)
    local c = m_call("SE_STRING_DISPATCH")
        field_ref(field_name)
        local d = dict_start("cases")
            for pattern, action_fn in pairs(cases) do
                local k = dict_key(pattern)
                    action_fn()
                end_dict_key(k)
            end
        dict_end(d)
    end_call(c)
end

--- Hash dispatch (dispatch on pre-computed hash value)
-- Usage: se_hash_dispatch("hash_state", {
--     {"idle",     function() int(0) str_ptr("System idle") se_log("idle") result(SE_HALT) end},
--     {"running",  function() int(1) str_ptr("System running") se_log("running") end},
--     {"error",    function() int(2) str_ptr("System error") se_log("error") end},
--     {"shutdown", function() int(3) str_ptr("System shutdown") se_log("shutdown") end},
-- }, SE_CONTINUE)
function se_hash_dispatch(field_name, cases, default_result)
    local c = m_call("SE_HASH_DISPATCH")
        field_ref(field_name)
        local d = dict_start("cases")
            for _, case in ipairs(cases) do
                local key_str    = case[1]
                local content_fn = case[2]
                
                local k = key(key_str)
                    if content_fn then
                        content_fn()
                    end
                key_end(k)
            end
        dict_end(d)
        if default_result then
            se_set_result(default_result)
        end
    end_call(c)
end
--============================================================================
-- EVENT DISPATCH FUNCTIONS
--============================================================================

-- Integer event dispatch (original)
function se_event_dispatch(cases)
    local c = m_call("SE_EVENT_DISPATCH")
        for _, case in ipairs(cases) do
            local event_val = case[1]
            local action_fn = case[2]
            int(event_val)
            se_pipeline(action_fn)
        end
    end_call(c)
end

-- NEW: Named event dispatch using dictionary (string event names)
-- Usage: se_named_event_dispatch({
--     {"BUTTON_PRESS", button_handler},
--     {"TIMEOUT", timeout_handler},
--     {"DATA_READY", data_handler},
-- })
function se_named_event_dispatch(cases)
    local c = m_call("SE_NAMED_EVENT_DISPATCH")
        local d = dict_start("events")
            for _, case in ipairs(cases) do
                local event_name = case[1]
                local action_fn = case[2]
                local k = dict_key(event_name)
                    se_pipeline(action_fn)
                end_dict_key(k)
            end
        dict_end(d)
    end_call(c)
end

-- NEW: Named event dispatch with table syntax
function se_named_event_dispatch_table(cases)
    local c = m_call("SE_NAMED_EVENT_DISPATCH")
        local d = dict_start("events")
            for event_name, action_fn in pairs(cases) do
                local k = dict_key(event_name)
                    se_pipeline(action_fn)
                end_dict_key(k)
            end
        dict_end(d)
    end_call(c)
end

--============================================================================
-- EVENT CHECK FUNCTIONS
--============================================================================

function se_check_event(...)
    local event_ids = {...}
    local c = p_call("SE_CHECK_EVENT")
        for _, id in ipairs(event_ids) do
            int(id)
        end
    end_call(c)
end

-- NEW: Check named event (string-based)
function se_check_named_event(event_name)
    local c = p_call("SE_CHECK_NAMED_EVENT")
        str(event_name)
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
    if is_debug and is_debug() then
        se_log(message)
    end
end

function se_debug_log_field(message, field_name)
    if is_debug and is_debug() then
        local c = o_call("SE_LOG")
            str(message)
            field_ref(field_name)
        end_call(c)
    end
end

-- NEW: Log with integer value
function se_log_int(message, value)
    local c = o_call("SE_LOG_INT")
        str_ptr(message)
        int(value)
    end_call(c)
end

-- NEW: Log with float value
function se_log_float(message, value)
    local c = o_call("SE_LOG_FLOAT")
        str_ptr(message)
        flt(value)
    end_call(c)
end

-- NEW: Log field value
function se_log_field(message, field_name)
    local c = o_call("SE_LOG_FIELD")
        str_ptr(message)
        field_ref(field_name)
    end_call(c)
end

--============================================================================
-- CONFIGURATION DICTIONARY HELPERS
--============================================================================

-- NEW: Create a configuration block as dictionary
-- Usage: se_config({
--     {"interval", "int", 1000},
--     {"threshold", "float", 25.5},
--     {"name", "str", "sensor1"},
--     {"enable", "bool", true},
-- })
function se_config(entries)
    local d = dict_start("config")
        for _, entry in ipairs(entries) do
            local key = entry[1]
            local vtype = entry[2]
            local value = entry[3]
            local k = dict_key(key)
                if vtype == "int" then
                    int(value)
                elseif vtype == "uint" then
                    uint(value)
                elseif vtype == "float" or vtype == "flt" then
                    flt(value)
                elseif vtype == "str" or vtype == "string" then
                    str(value)
                elseif vtype == "str_ptr" then
                    str_ptr(value)
                elseif vtype == "field" then
                    field_ref(value)
                elseif vtype == "bool" then
                    int(value and 1 or 0)
                else
                    -- Default to int
                    int(value)
                end
            end_dict_key(k)
        end
    dict_end(d)
    return d
end

-- NEW: Create named configuration with explicit name
function se_named_config(name, entries)
    local d = dict_start(name)
        for _, entry in ipairs(entries) do
            local key = entry[1]
            local vtype = entry[2]
            local value = entry[3]
            local k = dict_key(key)
                if vtype == "int" then
                    int(value)
                elseif vtype == "uint" then
                    uint(value)
                elseif vtype == "float" or vtype == "flt" then
                    flt(value)
                elseif vtype == "str" or vtype == "string" then
                    str(value)
                elseif vtype == "str_ptr" then
                    str_ptr(value)
                elseif vtype == "field" then
                    field_ref(value)
                elseif vtype == "bool" then
                    int(value and 1 or 0)
                else
                    int(value)
                end
            end_dict_key(k)
        end
    dict_end(d)
    return d
end

-- NEW: Simple key-value pair inside dict context
-- Usage inside dict_start/dict_end block:
--   se_kv("name", "str", "sensor1")
--   se_kv("count", "int", 42)
function se_kv(key, vtype, value)
    local k = dict_key(key)
        if vtype == "int" then
            int(value)
        elseif vtype == "uint" then
            uint(value)
        elseif vtype == "float" or vtype == "flt" then
            flt(value)
        elseif vtype == "str" or vtype == "string" then
            str(value)
        elseif vtype == "str_ptr" then
            str_ptr(value)
        elseif vtype == "field" then
            field_ref(value)
        elseif vtype == "bool" then
            int(value and 1 or 0)
        else
            int(value)
        end
    end_dict_key(k)
    return k
end

-- NEW: Typed key-value shortcuts
function se_kv_int(key, value)
    local k = dict_key(key)
        int(value)
    end_dict_key(k)
    return k
end

function se_kv_uint(key, value)
    local k = dict_key(key)
        uint(value)
    end_dict_key(k)
    return k
end

function se_kv_float(key, value)
    local k = dict_key(key)
        flt(value)
    end_dict_key(k)
    return k
end

function se_kv_str(key, value)
    local k = dict_key(key)
        str(value)
    end_dict_key(k)
    return k
end

function se_kv_bool(key, value)
    local k = dict_key(key)
        int(value and 1 or 0)
    end_dict_key(k)
    return k
end

function se_kv_field(key, field_name)
    local k = dict_key(key)
        field_ref(field_name)
    end_dict_key(k)
    return k
end

--============================================================================
-- LIST BUILDER HELPERS
--============================================================================

-- NEW: Integer list shorthand
-- Usage: se_int_list(1, 2, 3, 4, 5)
function se_int_list(...)
    local l = list_start("ints")
        for _, v in ipairs({...}) do
            int(v)
        end
    list_end(l)
    return l
end

-- NEW: Unsigned integer list
function se_uint_list(...)
    local l = list_start("uints")
        for _, v in ipairs({...}) do
            uint(v)
        end
    list_end(l)
    return l
end

-- NEW: Float list shorthand
function se_float_list(...)
    local l = list_start("floats")
        for _, v in ipairs({...}) do
            flt(v)
        end
    list_end(l)
    return l
end

-- NEW: String list shorthand
function se_str_list(...)
    local l = list_start("strings")
        for _, v in ipairs({...}) do
            str(v)
        end
    list_end(l)
    return l
end

-- NEW: Field reference list
function se_field_list(...)
    local l = list_start("fields")
        for _, v in ipairs({...}) do
            field_ref(v)
        end
    list_end(l)
    return l
end

-- NEW: Mixed list with type tags
-- Usage: se_list({"int", 1}, {"str", "hello"}, {"float", 3.14})
function se_list(...)
    local l = list_start("mixed")
        for _, item in ipairs({...}) do
            local vtype = item[1]
            local value = item[2]
            if vtype == "int" then
                int(value)
            elseif vtype == "uint" then
                uint(value)
            elseif vtype == "float" or vtype == "flt" then
                flt(value)
            elseif vtype == "str" or vtype == "string" then
                str(value)
            elseif vtype == "str_ptr" then
                str_ptr(value)
            elseif vtype == "field" then
                field_ref(value)
            elseif vtype == "bool" then
                int(value and 1 or 0)
            end
        end
    list_end(l)
    return l
end

-- NEW: Named list with explicit name
function se_named_list(name, ...)
    local l = list_start(name)
        for _, v in ipairs({...}) do
            local t = type(v)
            if t == "number" then
                if math.floor(v) == v then
                    int(v)
                else
                    flt(v)
                end
            elseif t == "string" then
                str(v)
            elseif t == "boolean" then
                int(v and 1 or 0)
            end
        end
    list_end(l)
    return l
end

-- NEW: Empty list
function se_empty_list(name)
    local l = list_start(name or "empty")
    list_end(l)
    return l
end

-- NEW: Range list [start, stop] or [start, stop, step]
-- Usage: se_range(1, 5) -> [1, 2, 3, 4, 5]
-- Usage: se_range(0, 10, 2) -> [0, 2, 4, 6, 8, 10]
function se_range(start_val, stop_val, step_val)
    step_val = step_val or 1
    local l = list_start("range")
        for i = start_val, stop_val, step_val do
            int(i)
        end
    list_end(l)
    return l
end

-- NEW: Repeat value N times
-- Usage: se_repeat(0, 5) -> [0, 0, 0, 0, 0]
function se_repeat_val(value, count)
    local l = list_start("repeat")
        local t = type(value)
        for i = 1, count do
            if t == "number" then
                if math.floor(value) == value then
                    int(value)
                else
                    flt(value)
                end
            elseif t == "string" then
                str(value)
            end
        end
    list_end(l)
    return l
end

--============================================================================
-- COORDINATE / VECTOR HELPERS
--============================================================================

-- NEW: 2D point (integers)
function se_point2(x, y)
    local l = list_start("point2")
        int(x)
        int(y)
    list_end(l)
    return l
end

-- NEW: 3D point (integers)
function se_point3(x, y, z)
    local l = list_start("point3")
        int(x)
        int(y)
        int(z)
    list_end(l)
    return l
end

-- NEW: 2D vector (floats)
function se_vec2(x, y)
    local l = list_start("vec2")
        flt(x)
        flt(y)
    list_end(l)
    return l
end

-- NEW: 3D vector (floats)
function se_vec3(x, y, z)
    local l = list_start("vec3")
        flt(x)
        flt(y)
        flt(z)
    list_end(l)
    return l
end

-- NEW: Rectangle (x, y, width, height)
function se_rect(x, y, w, h)
    local l = list_start("rect")
        int(x)
        int(y)
        int(w)
        int(h)
    list_end(l)
    return l
end

-- NEW: RGB color
function se_rgb(r, g, b)
    local l = list_start("rgb")
        int(r)
        int(g)
        int(b)
    list_end(l)
    return l
end

-- NEW: RGBA color
function se_rgba(r, g, b, a)
    local l = list_start("rgba")
        int(r)
        int(g)
        int(b)
        int(a)
    list_end(l)
    return l
end

--============================================================================
-- PARAMETER BLOCK HELPERS
--============================================================================

-- NEW: Wrap parameters in a labeled list
-- Usage inside m_call:
--   se_params("sensor_config",
--       {"threshold", "int", 100},
--       {"name", "str", "temp_sensor"}
--   )
function se_params(name, ...)
    local l = list_start(name)
        for _, param in ipairs({...}) do
            local pname = param[1]
            local ptype = param[2]
            local pvalue = param[3]
            -- Emit as key-value pairs (name string, then value)
            str(pname)
            if ptype == "int" then
                int(pvalue)
            elseif ptype == "uint" then
                uint(pvalue)
            elseif ptype == "float" or ptype == "flt" then
                flt(pvalue)
            elseif ptype == "str" or ptype == "string" then
                str(pvalue)
            elseif ptype == "str_ptr" then
                str_ptr(pvalue)
            elseif ptype == "field" then
                field_ref(pvalue)
            elseif ptype == "bool" then
                int(pvalue and 1 or 0)
            else
                int(pvalue)
            end
        end
    list_end(l)
    return l
end

-- NEW: Parameter block as dictionary (better for lookup)
function se_params_dict(name, ...)
    local d = dict_start(name)
        for _, param in ipairs({...}) do
            local pname = param[1]
            local ptype = param[2]
            local pvalue = param[3]
            local k = dict_key(pname)
                if ptype == "int" then
                    int(pvalue)
                elseif ptype == "uint" then
                    uint(pvalue)
                elseif ptype == "float" or ptype == "flt" then
                    flt(pvalue)
                elseif ptype == "str" or ptype == "string" then
                    str(pvalue)
                elseif ptype == "str_ptr" then
                    str_ptr(pvalue)
                elseif ptype == "field" then
                    field_ref(pvalue)
                elseif ptype == "bool" then
                    int(pvalue and 1 or 0)
                else
                    int(pvalue)
                end
            end_dict_key(k)
        end
    dict_end(d)
    return d
end

--============================================================================
-- TABLE/DICTIONARY HELPERS
--============================================================================

-- NEW: Create empty dictionary
function se_empty_dict(name)
    local d = dict_start(name or "empty")
    dict_end(d)
    return d
end

-- NEW: Create dictionary from Lua table (unordered)
-- Usage: se_dict("config", {count = 5, name = "test", enabled = true})
function se_dict(name, tbl)
    local d = dict_start(name)
        for key, value in pairs(tbl) do
            local k = dict_key(tostring(key))
                local t = type(value)
                if t == "number" then
                    if math.floor(value) == value then
                        int(value)
                    else
                        flt(value)
                    end
                elseif t == "string" then
                    str(value)
                elseif t == "boolean" then
                    int(value and 1 or 0)
                end
            end_dict_key(k)
        end
    dict_end(d)
    return d
end

-- NEW: Create dictionary from ordered array of key-value pairs
-- Usage: se_ordered_dict("config", {{"a", 1}, {"b", 2}, {"c", 3}})
function se_ordered_dict(name, pairs_array)
    local d = dict_start(name)
        for _, pair in ipairs(pairs_array) do
            local key = pair[1]
            local value = pair[2]
            local k = dict_key(tostring(key))
                local t = type(value)
                if t == "number" then
                    if math.floor(value) == value then
                        int(value)
                    else
                        flt(value)
                    end
                elseif t == "string" then
                    str(value)
                elseif t == "boolean" then
                    int(value and 1 or 0)
                end
            end_dict_key(k)
        end
    dict_end(d)
    return d
end

function se_set_hash(target_field, string_value)
    local c = o_call("SE_SET_HASH")
        field_ref(target_field)
        str_hash(string_value)  -- emits precomputed hash instead of str_ptr
    end_call(c)
end

function se_i_set_hash(target_field, string_value)
    local c = io_call("SE_SET_HASH")
        field_ref(target_field)
        str_hash(string_value)  -- emits precomputed hash instead of str_ptr
    end_call(c)
end
print("S-Expression Engine helpers loaded (v5.2)")

