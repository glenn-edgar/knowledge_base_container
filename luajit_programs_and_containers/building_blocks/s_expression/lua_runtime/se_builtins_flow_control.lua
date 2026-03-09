-- ============================================================================
-- se_builtins_flow_control.lua
-- Mirrors s_engine_builtins_flow_control.h
--
-- All main functions: fn(inst, node, event_id, event_data) -> result_code
-- ============================================================================

local se_runtime = require("se_runtime")

local SE_EVENT_INIT             = se_runtime.SE_EVENT_INIT
local SE_EVENT_TERMINATE        = se_runtime.SE_EVENT_TERMINATE
local SE_PIPELINE_CONTINUE      = se_runtime.SE_PIPELINE_CONTINUE
local SE_PIPELINE_DISABLE       = se_runtime.SE_PIPELINE_DISABLE
local SE_PIPELINE_TERMINATE     = se_runtime.SE_PIPELINE_TERMINATE
local SE_FUNCTION_CONTINUE      = se_runtime.SE_FUNCTION_CONTINUE
local SE_FUNCTION_DISABLE       = se_runtime.SE_FUNCTION_DISABLE
local SE_FUNCTION_HALT          = se_runtime.SE_FUNCTION_HALT

local child_count            = se_runtime.child_count
local child_invoke           = se_runtime.child_invoke
local child_reset            = se_runtime.child_reset
local children_terminate_all = se_runtime.children_terminate_all
local invoke_pred            = se_runtime.invoke_pred
local invoke_any             = se_runtime.invoke_any
local get_ns                 = se_runtime.get_ns

local M = {}

-- ----------------------------------------------------------------------------
-- SE_SEQUENCE
-- Sequential children; advances to next child when current returns
-- PIPELINE_DISABLE or PIPELINE_TERMINATE.
-- node_state.state = index of current active child (0-based)
-- ----------------------------------------------------------------------------
M.se_sequence = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)
    if event_id == SE_EVENT_INIT then
        ns.state = 0
        return
    end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    local n = child_count(node)
    if n == 0 or ns.state >= n then return SE_PIPELINE_DISABLE end

    local result = child_invoke(inst, node, ns.state, event_id, event_data)

    if result == SE_PIPELINE_DISABLE or result == SE_PIPELINE_TERMINATE then
        ns.state = ns.state + 1
        if ns.state >= n then return SE_PIPELINE_DISABLE end
        return SE_PIPELINE_CONTINUE
    end

    return result
end

-- ----------------------------------------------------------------------------
-- SE_SEQUENCE_ONCE
-- Invokes all children every tick; PIPELINE_DISABLE when all have completed.
-- ----------------------------------------------------------------------------
M.se_sequence_once = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    local n = child_count(node)
    local all_done = true
    for i = 0, n - 1 do
        local r = child_invoke(inst, node, i, event_id, event_data)
        if r ~= SE_PIPELINE_DISABLE and r ~= SE_PIPELINE_TERMINATE then
            all_done = false
        end
    end
    return all_done and SE_PIPELINE_DISABLE or SE_PIPELINE_CONTINUE
end

-- ----------------------------------------------------------------------------
-- SE_FUNCTION_INTERFACE
-- Parallel fork; returns FUNCTION_DISABLE when all MAIN children are inactive.
-- ----------------------------------------------------------------------------
M.se_function_interface = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    local children = node.children or {}
    for i = 0, #children - 1 do
        child_invoke(inst, node, i, event_id, event_data)
    end

    -- Disable when all MAIN children are no longer active
    local bit = require("bit")
    local FLAG_ACTIVE = 0x01
    for _, child in ipairs(children) do
        local ct = child.call_type
        if ct == "m_call" or ct == "pt_m_call" then
            if bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) ~= 0 then
                return SE_FUNCTION_CONTINUE
            end
        end
    end
    return SE_FUNCTION_DISABLE
end

-- ----------------------------------------------------------------------------
-- SE_FORK
-- Parallel execution; PIPELINE_DISABLE when all MAIN children complete.
-- ----------------------------------------------------------------------------
M.se_fork = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    local children = node.children or {}
    for i = 0, #children - 1 do
        child_invoke(inst, node, i, event_id, event_data)
    end

    local bit = require("bit")
    local FLAG_ACTIVE = 0x01
    for _, child in ipairs(children) do
        local ct = child.call_type
        if ct == "m_call" or ct == "pt_m_call" then
            if bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) ~= 0 then
                return SE_PIPELINE_CONTINUE
            end
        end
    end
    return SE_PIPELINE_DISABLE
end

-- ----------------------------------------------------------------------------
-- SE_FORK_JOIN
-- Parallel; FUNCTION_HALT while any MAIN child is active,
-- PIPELINE_DISABLE when all complete.
-- ----------------------------------------------------------------------------
M.se_fork_join = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    local children = node.children or {}
    for i = 0, #children - 1 do
        child_invoke(inst, node, i, event_id, event_data)
    end

    local bit = require("bit")
    local FLAG_ACTIVE = 0x01
    for _, child in ipairs(children) do
        local ct = child.call_type
        if ct == "m_call" or ct == "pt_m_call" then
            if bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) ~= 0 then
                return SE_FUNCTION_HALT
            end
        end
    end
    return SE_PIPELINE_DISABLE
end

-- ----------------------------------------------------------------------------
-- SE_CHAIN_FLOW
-- Pipeline chain; ticks all currently-active MAIN children each tick.
-- PIPELINE_DISABLE when no MAIN children remain active.
-- ----------------------------------------------------------------------------
M.se_chain_flow = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    local children = node.children or {}
    local bit = require("bit")
    local FLAG_ACTIVE = 0x01
    local any_active = false

    for i, child in ipairs(children) do
        local ct = child.call_type
        if ct == "m_call" or ct == "pt_m_call" then
            if bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) ~= 0 then
                any_active = true
                child_invoke(inst, node, i - 1, event_id, event_data)
            end
        else
            -- Oneshots and preds: invoke unconditionally
            child_invoke(inst, node, i - 1, event_id, event_data)
        end
    end

    return any_active and SE_PIPELINE_CONTINUE or SE_PIPELINE_DISABLE
end

-- ----------------------------------------------------------------------------
-- SE_WHILE
-- Loop: evaluate pred child (index 0), if true run body child (index 1).
-- Repeats until pred returns false.
-- node_state.state: 0 = check pred, 1 = running body
-- ----------------------------------------------------------------------------
M.se_while = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)
    if event_id == SE_EVENT_INIT then
        ns.state = 0
        return
    end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    if ns.state == 0 then
        -- Evaluate predicate (child 0)
        local pred_node = (node.children or {})[1]
        assert(pred_node, "se_while: missing predicate child")
        if not invoke_pred(inst, pred_node) then
            return SE_PIPELINE_DISABLE
        end
        -- Reset body (child 1 = 0-based index 1)
        child_reset(inst, node, 1)
        ns.state = 1
    end

    -- Tick body (child 1)
    local body_node = (node.children or {})[2]
    assert(body_node, "se_while: missing body child")
    local result = invoke_any(inst, body_node, event_id, event_data)

    if result == SE_PIPELINE_DISABLE or result == SE_PIPELINE_TERMINATE then
        -- Body complete; reset for next iteration, re-check pred next tick
        child_reset(inst, node, 1)
        ns.state = 0
    end

    return SE_PIPELINE_CONTINUE
end

-- ----------------------------------------------------------------------------
-- SE_IF_THEN_ELSE
-- children[1] = pred, children[2] = then-branch, children[3] = else-branch (opt)
-- node_state.state: 0=eval pred, 1=running then, 2=running else
-- ----------------------------------------------------------------------------
M.se_if_then_else = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)
    if event_id == SE_EVENT_INIT then
        ns.state = 0
        return
    end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    if ns.state == 0 then
        local pred_node = (node.children or {})[1]
        assert(pred_node, "se_if_then_else: missing predicate child")
        if invoke_pred(inst, pred_node) then
            child_reset(inst, node, 1)  -- reset then-branch (0-based 1)
            ns.state = 1
        else
            if child_count(node) >= 3 then
                child_reset(inst, node, 2)  -- reset else-branch (0-based 2)
                ns.state = 2
            else
                return SE_PIPELINE_DISABLE
            end
        end
    end

    local result = child_invoke(inst, node, ns.state, event_id, event_data)
    if result == SE_PIPELINE_DISABLE or result == SE_PIPELINE_TERMINATE then
        return SE_PIPELINE_DISABLE
    end
    return result
end

-- ----------------------------------------------------------------------------
-- SE_COND
-- Multi-branch: pairs of (pred, body) children.
-- Selects first true pred, runs its body.
-- node_state.state: 0=select, N=1-based body child index being run
-- ----------------------------------------------------------------------------
M.se_cond = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)
    if event_id == SE_EVENT_INIT then
        ns.state = 0
        return
    end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return
    end

    if ns.state == 0 then
        local children = node.children or {}
        local i = 1
        while i <= #children do
            local child = children[i]
            local ct = child.call_type
            if ct == "p_call" or ct == "p_call_composite" then
                if invoke_pred(inst, child) then
                    local body_idx_0based = i   -- body is next child (0-based = i)
                    child_reset(inst, node, body_idx_0based)
                    ns.state = body_idx_0based + 1  -- store 1-based for later
                    break
                end
                i = i + 2  -- skip body
            else
                i = i + 1
            end
        end
        if ns.state == 0 then
            return SE_PIPELINE_DISABLE  -- no branch matched
        end
    end

    local result = child_invoke(inst, node, ns.state - 1, event_id, event_data)
    if result == SE_PIPELINE_DISABLE or result == SE_PIPELINE_TERMINATE then
        return SE_PIPELINE_DISABLE
    end
    return result
end

return M