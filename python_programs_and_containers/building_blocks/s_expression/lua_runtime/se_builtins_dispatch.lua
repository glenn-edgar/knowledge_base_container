-- ============================================================================
-- se_builtins_dispatch.lua
-- Mirrors s_engine_builtins_dispatch.h
--
-- Dispatch and edge-detection main functions.
-- All main functions: fn(inst, node, event_id, event_data) -> result_code
-- ============================================================================

local se_runtime = require("se_runtime")

local SE_EVENT_INIT        = se_runtime.SE_EVENT_INIT
local SE_EVENT_TERMINATE   = se_runtime.SE_EVENT_TERMINATE
local SE_EVENT_TICK        = se_runtime.SE_EVENT_TICK
local SE_PIPELINE_CONTINUE      = se_runtime.SE_PIPELINE_CONTINUE
local SE_PIPELINE_HALT          = se_runtime.SE_PIPELINE_HALT
local SE_PIPELINE_DISABLE       = se_runtime.SE_PIPELINE_DISABLE
local SE_PIPELINE_TERMINATE     = se_runtime.SE_PIPELINE_TERMINATE
local SE_PIPELINE_RESET         = se_runtime.SE_PIPELINE_RESET
local SE_PIPELINE_SKIP_CONTINUE = se_runtime.SE_PIPELINE_SKIP_CONTINUE
local SE_FUNCTION_HALT          = se_runtime.SE_FUNCTION_HALT

local child_count            = se_runtime.child_count
local child_invoke           = se_runtime.child_invoke
local child_invoke_pred      = se_runtime.child_invoke_pred
local child_invoke_oneshot   = se_runtime.child_invoke_oneshot
local child_reset            = se_runtime.child_reset
local child_reset_recursive  = se_runtime.child_reset_recursive
local child_terminate        = se_runtime.child_terminate
local children_terminate_all = se_runtime.children_terminate_all
local get_ns                 = se_runtime.get_ns
local field_get              = se_runtime.field_get

local M = {}

-- ----------------------------------------------------------------------------
-- SE_TRIGGER_ON_CHANGE
-- Edge detection on a predicate child.
-- params[1]   = initial_state (int): 0 = starts as was-clear,
--                                    1 = starts as was-set
--               This avoids a spurious edge on the very first tick.
-- children[0] = predicate (p_call or p_call_composite)
-- children[1] = rise action (m_call, typically SE_CHAIN_FLOW)
-- children[2] = fall action (m_call, typically SE_CHAIN_FLOW)
-- node_state.user_data: 1=was_set, 2=was_clear
--
-- On INIT: store initial_state from params; predicate not evaluated.
-- On TICK: evaluate predicate, compare to stored state, invoke action branch.
-- On TERMINATE: terminate both action branches.
-- ----------------------------------------------------------------------------
M.se_trigger_on_change = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)

    if event_id == SE_EVENT_INIT then
        -- params[1] = initial_state: 1 -> was_set, 0 -> was_clear
        local initial = se_runtime.param_int(node, 1)
        ns.user_data = (initial == 1) and 1 or 2
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        if child_count(node) >= 2 then child_terminate(inst, node, 1) end
        if child_count(node) >= 3 then child_terminate(inst, node, 2) end
        return SE_PIPELINE_CONTINUE
    end

    -- TICK: evaluate predicate child (children[0])
    local is_set = child_invoke_pred(inst, node, 0)
    local prev   = ns.user_data   -- 1=was_set, 2=was_clear

    if is_set and prev == 2 then
        -- Rising edge: was_clear -> now_set
        ns.user_data = 1
        if child_count(node) >= 2 then
            child_reset_recursive(inst, node, 1)
            child_invoke(inst, node, 1, event_id, event_data)
        end
    elseif not is_set and prev == 1 then
        -- Falling edge: was_set -> now_clear
        ns.user_data = 2
        if child_count(node) >= 3 then
            child_reset_recursive(inst, node, 2)
            child_invoke(inst, node, 2, event_id, event_data)
        end
    end

    return SE_PIPELINE_CONTINUE
end

-- ----------------------------------------------------------------------------
-- SE_EVENT_DISPATCH
-- Faithful translation of C se_event_dispatch.
-- Compiled layout (Lua): params[1..N] = event_id uint values (non-callables);
--                        children[0..N-1] = action handlers (callables).
-- params[i] matched positionally to children[i-1].
-- Default case: param value == -1 (or 0xFFFF as uint) used if no exact match.
-- INIT:      return PIPELINE_CONTINUE (no child setup).
-- TERMINATE: return PIPELINE_CONTINUE (no child cleanup — C does nothing).
-- TICK: find matching param; invoke corresponding child.
--   invoke_and_handle result:
--     non-pipeline (< 12)              -> propagate
--     PIPELINE_CONTINUE/HALT           -> return as-is
--     PIPELINE_DISABLE/TERMINATE/RESET -> child_terminate + child_reset_recursive, PIPELINE_CONTINUE
--     PIPELINE_SKIP_CONTINUE           -> PIPELINE_CONTINUE
-- Crashes (Erlang-style) if no match and no default.
-- ----------------------------------------------------------------------------
M.se_event_dispatch = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then
        return SE_PIPELINE_CONTINUE
    end
    if event_id == SE_EVENT_TERMINATE then
        -- C does nothing on TERMINATE for event_dispatch
        return SE_PIPELINE_CONTINUE
    end

    local params      = node.params or {}
    local default_idx = nil   -- 0-based child index for default case (-1)
    local match_idx   = nil   -- 0-based child index for exact match

    for i, p in ipairs(params) do
        local pval = p.value
        if type(pval) == "table" then pval = pval.hash end
        pval = tonumber(pval) or 0
        local child_idx = i - 1   -- 0-based

        if pval == -1 or pval == 0xFFFF then
            default_idx = child_idx
        elseif pval == event_id then
            match_idx = child_idx
            break
        end
    end

    local action_idx = match_idx or default_idx
    if action_idx == nil then
        error(string.format("se_event_dispatch: unhandled event_id 0x%04x", event_id))
    end

    local r = child_invoke(inst, node, action_idx, event_id, event_data)

    -- Non-pipeline codes: propagate
    if r < SE_PIPELINE_CONTINUE then return r end

    if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
        return r
    elseif r == SE_PIPELINE_DISABLE or r == SE_PIPELINE_TERMINATE or r == SE_PIPELINE_RESET then
        child_terminate(inst, node, action_idx)
        child_reset_recursive(inst, node, action_idx)
        return SE_PIPELINE_CONTINUE
    else  -- PIPELINE_SKIP_CONTINUE or unknown
        return SE_PIPELINE_CONTINUE
    end
end

-- ----------------------------------------------------------------------------
-- SE_STATE_MACHINE
-- Faithful translation of C se_state_machine.
-- Compiled layout: params[1]=field_ref, params[2..N]=int case values;
--                  children[0..N-2]=action handlers (one per case value).
-- Default case: param value == -1.
-- ns.user_data = active child index (0-based), 0xFFFF = none.
--
-- INIT:      ns.user_data=0xFFFF, PIPELINE_CONTINUE.
-- TERMINATE: terminate active child only (not all), PIPELINE_CONTINUE.
-- TICK: read field, match case params[2..N] to children[0..N-2].
--   Branch change: terminate+reset old, reset new.
--   SE_FUNCTION_HALT -> PIPELINE_HALT (special C handling).
--   Non-pipeline (< 12): propagate.
--   PIPELINE_CONTINUE/HALT: return as-is.
--   PIPELINE_DISABLE/TERMINATE/RESET: terminate+reset action, PIPELINE_CONTINUE.
--   PIPELINE_SKIP_CONTINUE: PIPELINE_CONTINUE.
-- ----------------------------------------------------------------------------
M.se_state_machine = function(inst, node, event_id, event_data)
    local ns     = get_ns(inst, node.node_index)
    local NO_BRANCH = 0xFFFF

    if event_id == SE_EVENT_INIT then
        ns.user_data = NO_BRANCH
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        -- Terminate active branch only
        local prev = ns.user_data
        if prev ~= NO_BRANCH then
            child_terminate(inst, node, prev)
        end
        ns.user_data = NO_BRANCH
        return SE_PIPELINE_CONTINUE
    end

    -- TICK: read field value, find matching case
    local val     = tonumber(field_get(inst, node, 1)) or 0
    local params  = node.params or {}
    local branch  = nil     -- 0-based child index
    local default_branch = nil

    -- params[2..N] are int case values; children[0..N-2] are the actions
    for i = 2, #params do
        local pval = tonumber(params[i].value)
        if pval == -1 then
            default_branch = i - 2   -- 0-based child index
        elseif pval == val then
            branch = i - 2
            break
        end
    end

    branch = branch or default_branch
    if branch == nil then
        error("se_state_machine: no matching case for value " .. tostring(val))
    end

    local prev = ns.user_data
    if branch ~= prev then
        -- Branch change: terminate+reset old recursively, reset new recursively
        if prev ~= NO_BRANCH then
            child_terminate(inst, node, prev)
            child_reset_recursive(inst, node, prev)
        end
        child_reset_recursive(inst, node, branch)
        ns.user_data = branch
    end

    local r = child_invoke(inst, node, branch, event_id, event_data)

    -- SE_FUNCTION_HALT special case (C converts this to PIPELINE_HALT)
    if r == SE_FUNCTION_HALT then return SE_PIPELINE_HALT end

    -- Non-pipeline codes: propagate
    if r < SE_PIPELINE_CONTINUE then return r end

    if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
        return r
    elseif r == SE_PIPELINE_DISABLE or r == SE_PIPELINE_TERMINATE or r == SE_PIPELINE_RESET then
        child_terminate(inst, node, branch)
        child_reset_recursive(inst, node, branch)
        return SE_PIPELINE_CONTINUE
    else  -- PIPELINE_SKIP_CONTINUE
        return SE_PIPELINE_CONTINUE
    end
end

-- ----------------------------------------------------------------------------
-- SE_FIELD_DISPATCH
-- Faithful translation of C se_field_dispatch.
-- Compiled layout: params[1]=field_ref, params[2..N]=int case values;
--                  children[0..N-2]=action handlers.
-- Default case: param value == -1.
-- ns.user_data = active child index (0-based), 0xFFFF = none.
--
-- INIT:      ns.user_data=0xFFFF, PIPELINE_CONTINUE.
-- TERMINATE: terminate active branch only (not all); return PIPELINE_CONTINUE.
--   (C returns SE_CONTINUE=0 but PIPELINE_CONTINUE is correct for pipeline.)
-- TICK: read field, match case params[2..N] to children[0..N-2].
--   Branch change: terminate+reset old, reset new.
--   Result: only PIPELINE_RESET is intercepted (terminate+reset, PIPELINE_CONTINUE).
--   All other results returned directly to caller (no special handling).
-- ----------------------------------------------------------------------------
M.se_field_dispatch = function(inst, node, event_id, event_data)
    local ns     = get_ns(inst, node.node_index)
    local NO_BRANCH = 0xFFFF

    if event_id == SE_EVENT_INIT then
        ns.user_data = NO_BRANCH
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        local prev = ns.user_data
        if prev ~= NO_BRANCH then
            child_terminate(inst, node, prev)
        end
        ns.user_data = NO_BRANCH
        return SE_PIPELINE_CONTINUE
    end

    -- TICK: read field value, find matching case
    local val    = tonumber(field_get(inst, node, 1)) or 0
    local params = node.params or {}
    local branch = nil
    local default_branch = nil

    -- params[2..N] are int case values; children[0..N-2] are the actions
    for i = 2, #params do
        local pval = tonumber(params[i].value)
        if pval == -1 then
            default_branch = i - 2
        elseif pval == val then
            branch = i - 2
            break
        end
    end

    branch = branch or default_branch
    if branch == nil then
        error("se_field_dispatch: no matching case for value " .. tostring(val))
    end

    local prev = ns.user_data
    if branch ~= prev then
        -- Branch change: terminate+reset old recursively, reset new recursively
        if prev ~= NO_BRANCH then
            child_terminate(inst, node, prev)
            child_reset_recursive(inst, node, prev)
        end
        child_reset_recursive(inst, node, branch)
        ns.user_data = branch
    end

    local r = child_invoke(inst, node, branch, event_id, event_data)

    -- Only PIPELINE_RESET is intercepted; all other results pass through
    if r == SE_PIPELINE_RESET then
        child_terminate(inst, node, branch)
        child_reset_recursive(inst, node, branch)
        return SE_PIPELINE_CONTINUE
    end

    return r
end

return M