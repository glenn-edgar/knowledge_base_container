-- ============================================================================
-- se_builtins_flow_control.lua
-- Mirrors s_engine_builtins_flow_control.h
--
-- All main functions: fn(inst, node, event_id, event_data) -> result_code
-- ============================================================================

local se_runtime = require("se_runtime")

local SE_EVENT_INIT             = se_runtime.SE_EVENT_INIT
local SE_EVENT_TERMINATE        = se_runtime.SE_EVENT_TERMINATE
local SE_SKIP_CONTINUE          = 5   -- application-level
local SE_PIPELINE_CONTINUE      = se_runtime.SE_PIPELINE_CONTINUE
local SE_PIPELINE_HALT          = se_runtime.SE_PIPELINE_HALT
local SE_PIPELINE_DISABLE       = se_runtime.SE_PIPELINE_DISABLE
local SE_PIPELINE_TERMINATE     = se_runtime.SE_PIPELINE_TERMINATE
local SE_PIPELINE_RESET         = se_runtime.SE_PIPELINE_RESET
local SE_PIPELINE_SKIP_CONTINUE = se_runtime.SE_PIPELINE_SKIP_CONTINUE
local SE_FUNCTION_CONTINUE      = se_runtime.SE_FUNCTION_CONTINUE
local SE_FUNCTION_HALT          = se_runtime.SE_FUNCTION_HALT
local SE_FUNCTION_SKIP_CONTINUE = 11  -- function-level skip
local SE_FUNCTION_DISABLE       = se_runtime.SE_FUNCTION_DISABLE

local child_count            = se_runtime.child_count
local child_invoke           = se_runtime.child_invoke
local child_invoke_pred      = se_runtime.child_invoke_pred
local child_reset            = se_runtime.child_reset
local child_reset_recursive  = se_runtime.child_reset_recursive
local child_terminate        = se_runtime.child_terminate
local children_terminate_all = se_runtime.children_terminate_all
local children_reset_all     = se_runtime.children_reset_all
local invoke_pred            = se_runtime.invoke_pred
local invoke_any             = se_runtime.invoke_any
local get_ns                 = se_runtime.get_ns
local bit                    = require("bit")

-- Node state flag constants (must be declared before any function that uses them)
local FLAG_ACTIVE      = 0x01
local FLAG_INITIALIZED = 0x02

local M = {}

-- ----------------------------------------------------------------------------
-- SE_SEQUENCE
-- Faithful translation of C se_sequence.
-- Executes children one at a time in order; advances when current completes.
-- ns.state = current child index (0-based).
--
-- INIT:     state=0, return PIPELINE_CONTINUE.
-- TERMINATE: terminate only the current child if initialized, state=0.
-- TICK:     while loop — can advance multiple steps per tick for
--           oneshots/preds (fire-and-advance) and completed mains.
--           Main result dispatch:
--             app codes (0-5)              -> propagate immediately
--             function codes (6-11)        -> propagate; FUNCTION_HALT->PIPELINE_HALT
--             PIPELINE_CONTINUE/HALT       -> pause, return PIPELINE_CONTINUE
--             PIPELINE_DISABLE/TERM/RESET  -> child_terminate, advance state
--             PIPELINE_SKIP_CONTINUE       -> pause, return PIPELINE_CONTINUE
--           All children done -> PIPELINE_DISABLE.
-- ----------------------------------------------------------------------------
M.se_sequence = function(inst, node, event_id, event_data)
    local ns       = get_ns(inst, node.node_index)
    local children = node.children or {}
    local n        = #children

    if event_id == SE_EVENT_INIT then
        ns.state = 0
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        local s = ns.state
        if s < n then
            local cns = get_ns(inst, children[s + 1].node_index)
            if bit.band(cns.flags, FLAG_INITIALIZED) ~= 0 then
                child_terminate(inst, node, s)
            end
        end
        ns.state = 0
        return SE_PIPELINE_CONTINUE
    end

    -- TICK: while loop, can advance multiple children per tick
    while ns.state < n do
        local s     = ns.state
        local child = children[s + 1]
        local ct    = child.call_type

        -- Oneshot: invoke and advance immediately
        if ct == "o_call" or ct == "io_call" then
            child_invoke(inst, node, s, event_id, event_data)
            ns.state = s + 1
            goto seq_continue
        end

        -- Pred: invoke and advance immediately
        if ct == "p_call" or ct == "p_call_composite" then
            child_invoke(inst, node, s, event_id, event_data)
            ns.state = s + 1
            goto seq_continue
        end

        -- Main: invoke and dispatch result
        do
            local r = child_invoke(inst, node, s, event_id, event_data)

            -- Application codes (0-5): propagate immediately
            if r <= SE_SKIP_CONTINUE then
                return r
            end

            -- Function codes (6-11): propagate; FUNCTION_HALT -> PIPELINE_HALT
            if r >= SE_FUNCTION_CONTINUE and r <= SE_FUNCTION_SKIP_CONTINUE then
                if r == SE_FUNCTION_HALT then return SE_PIPELINE_HALT end
                return r
            end

            -- Pipeline codes (12-17)
            if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
                return SE_PIPELINE_CONTINUE   -- child still running, pause

            elseif r == SE_PIPELINE_DISABLE
                or r == SE_PIPELINE_TERMINATE
                or r == SE_PIPELINE_RESET then
                -- child complete: terminate and advance
                child_terminate(inst, node, s)
                ns.state = s + 1

            elseif r == SE_PIPELINE_SKIP_CONTINUE then
                return SE_PIPELINE_CONTINUE   -- pause this tick

            else
                return SE_PIPELINE_CONTINUE   -- unknown: pause
            end
        end

        ::seq_continue::
    end

    -- All children complete
    return SE_PIPELINE_DISABLE
end

-- ----------------------------------------------------------------------------
-- SE_SEQUENCE_ONCE
-- Faithful translation of C se_sequence_once.
-- Fires ALL children exactly once in a single tick, then terminates them all
-- and returns PIPELINE_DISABLE. Single-shot: always done after one tick.
--
-- INIT:     set state=0, return PIPELINE_CONTINUE.
-- TERMINATE: terminate only initialized children, set state=0.
-- TICK:     iterate children in order:
--             oneshot/pred: invoke unconditionally, continue
--             main: invoke; break loop if result is not PIPELINE_CONTINUE
--                   or PIPELINE_DISABLE (i.e. any non-normal code stops iteration)
--           After loop: terminate all initialized children.
--           Always return PIPELINE_DISABLE.
-- ----------------------------------------------------------------------------
M.se_sequence_once = function(inst, node, event_id, event_data)
    local ns       = get_ns(inst, node.node_index)
    local children = node.children or {}
    local n        = #children

    if event_id == SE_EVENT_INIT then
        ns.state = 0
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        for i = 1, n do
            local cns = get_ns(inst, children[i].node_index)
            if bit.band(cns.flags, FLAG_INITIALIZED) ~= 0 then
                child_terminate(inst, node, i - 1)
            end
        end
        ns.state = 0
        return SE_PIPELINE_CONTINUE
    end

    -- TICK: fire all children once, break on non-normal result
    for i = 1, n do
        local child = children[i]
        local idx   = i - 1
        local ct    = child.call_type

        -- Oneshot and pred: invoke and continue (no result check)
        if ct == "o_call" or ct == "io_call"
        or ct == "p_call" or ct == "p_call_composite" then
            child_invoke(inst, node, idx, event_id, event_data)
            goto continue_so
        end

        -- Main: invoke and break if result is not CONTINUE or DISABLE
        do
            local r = child_invoke(inst, node, idx, event_id, event_data)
            if r ~= SE_PIPELINE_CONTINUE and r ~= SE_PIPELINE_DISABLE then
                break
            end
        end

        ::continue_so::
    end

    -- Terminate all initialized children
    for i = 1, n do
        local cns = get_ns(inst, children[i].node_index)
        if bit.band(cns.flags, FLAG_INITIALIZED) ~= 0 then
            child_terminate(inst, node, i - 1)
        end
    end

    return SE_PIPELINE_DISABLE
end

-- ----------------------------------------------------------------------------
-- SE_FUNCTION_INTERFACE
-- Faithful translation of C se_function_interface.
-- Top-level parallel dispatcher; FUNCTION_DISABLE when all children complete.
-- ns.state: 0=RUNNING, 1=COMPLETE
--
-- INIT:  set RUNNING, reset all callable children, return FUNCTION_CONTINUE.
-- TERM:  terminate all, set COMPLETE, return FUNCTION_CONTINUE.
-- TICK:  if COMPLETE return FUNCTION_DISABLE immediately.
--        Invoke all active callable children with full result dispatch:
--          non-pipeline (< PIPELINE_CONTINUE) -> propagate immediately
--          PIPELINE_CONTINUE / HALT            -> active_count++
--          PIPELINE_DISABLE / TERMINATE        -> child_terminate
--          PIPELINE_RESET                      -> child_terminate + child_reset + active_count++
--          PIPELINE_SKIP_CONTINUE              -> active_count++, stop loop
--        Final: active_count==0 -> COMPLETE + FUNCTION_DISABLE
--               else              -> FUNCTION_CONTINUE
-- ----------------------------------------------------------------------------
M.se_function_interface = function(inst, node, event_id, event_data)
    local ns       = get_ns(inst, node.node_index)
    local children = node.children or {}
    local n        = #children

    if event_id == SE_EVENT_INIT then
        ns.state = FORK_RUNNING
        for i = 1, n do
            local ct = children[i].call_type
            if ct == "m_call" or ct == "pt_m_call"
            or ct == "o_call" or ct == "io_call"
            or ct == "p_call" or ct == "p_call_composite" then
                child_reset(inst, node, i - 1)
            end
        end
        return SE_FUNCTION_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        ns.state = FORK_COMPLETE
        return SE_FUNCTION_CONTINUE
    end

    -- TICK
    if ns.state ~= FORK_RUNNING then
        return SE_FUNCTION_DISABLE
    end

    local active_count = 0
    local skip         = false

    for i = 1, n do
        if skip then break end
        local child = children[i]
        local idx   = i - 1
        if not child.node_index then goto continue_fi end   -- non-callable param
        local cns   = get_ns(inst, child.node_index)

        -- Skip inactive children
        if bit.band(cns.flags, FLAG_ACTIVE) == 0 then
            goto continue_fi
        end

        do
            local r = child_invoke(inst, node, idx, event_id, event_data)

            -- Non-pipeline codes: propagate immediately
            if r < SE_PIPELINE_CONTINUE then
                return r
            end

            if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
                active_count = active_count + 1

            elseif r == SE_PIPELINE_DISABLE or r == SE_PIPELINE_TERMINATE then
                child_terminate(inst, node, idx)

            elseif r == SE_PIPELINE_RESET then
                child_terminate(inst, node, idx)
                child_reset(inst, node, idx)        -- note: child_reset, not recursive
                active_count = active_count + 1

            elseif r == SE_PIPELINE_SKIP_CONTINUE then
                active_count = active_count + 1
                skip = true

            else
                active_count = active_count + 1
            end
        end

        ::continue_fi::
    end

    if active_count == 0 then
        ns.state = FORK_COMPLETE
        return SE_FUNCTION_DISABLE
    end
    return SE_FUNCTION_CONTINUE
end

-- ----------------------------------------------------------------------------
-- SE_FORK
-- Faithful translation of C se_fork.
-- Parallel execution of all children; PIPELINE_DISABLE when all MAIN complete.
-- ns.state: 0=RUNNING, 1=COMPLETE
--
-- INIT:  mark RUNNING, reset all callable children.
-- TICK:  if COMPLETE return PIPELINE_DISABLE immediately.
--        Oneshots/preds: fire once if not yet initialized.
--        Main: invoke only if active; full result dispatch:
--          FUNCTION_HALT         -> treat as PIPELINE_HALT, propagate if < PIPELINE_CONTINUE
--          non-pipeline (< 12)   -> propagate immediately
--          PIPELINE_CONTINUE/HALT-> keep going (child still active)
--          PIPELINE_DISABLE/TERM -> child_terminate
--          PIPELINE_RESET        -> child_terminate + child_reset_recursive
--          PIPELINE_SKIP_CONTINUE-> break to completion check
--        Completion: count active MAIN; 0 -> COMPLETE + PIPELINE_DISABLE
-- TERM:  terminate all, return PIPELINE_CONTINUE.
-- ----------------------------------------------------------------------------
local FORK_RUNNING  = 0
local FORK_COMPLETE = 1

M.se_fork = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)

    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        ns.state = FORK_COMPLETE
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_INIT then
        ns.state = FORK_RUNNING
        local children = node.children or {}
        for i = 1, #children do
            local ct = children[i].call_type
            if ct == "m_call" or ct == "pt_m_call"
            or ct == "o_call" or ct == "io_call"
            or ct == "p_call" or ct == "p_call_composite" then
                child_reset(inst, node, i - 1)
            end
        end
        return SE_PIPELINE_CONTINUE
    end

    -- TICK
    if ns.state ~= FORK_RUNNING then
        return SE_PIPELINE_DISABLE
    end

    local children = node.children or {}
    local n        = #children
    local skip     = false

    for i = 1, n do
        if skip then break end
        local child = children[i]
        local idx   = i - 1
        if not child.node_index then goto continue_fork end  -- non-callable param
        local ct    = child.call_type
        local cns   = get_ns(inst, child.node_index)

        -- Oneshot: fire once if not yet initialized
        if ct == "o_call" or ct == "io_call" then
            if bit.band(cns.flags, FLAG_INITIALIZED) == 0 then
                child_invoke(inst, node, idx, event_id, event_data)
            end
            goto continue_fork
        end

        -- Pred: evaluate once if not yet initialized
        if ct == "p_call" or ct == "p_call_composite" then
            if bit.band(cns.flags, FLAG_INITIALIZED) == 0 then
                child_invoke(inst, node, idx, event_id, event_data)
            end
            goto continue_fork
        end

        -- Main: only invoke if active
        if bit.band(cns.flags, FLAG_ACTIVE) == 0 then
            goto continue_fork
        end

        do
            local r = child_invoke(inst, node, idx, event_id, event_data)

            if r == SE_FUNCTION_HALT then r = SE_PIPELINE_HALT end

            if r < SE_PIPELINE_CONTINUE then
                return r
            end

            if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
                -- child still running, keep going

            elseif r == SE_PIPELINE_DISABLE or r == SE_PIPELINE_TERMINATE then
                child_terminate(inst, node, idx)

            elseif r == SE_PIPELINE_RESET then
                child_terminate(inst, node, idx)
                child_reset_recursive(inst, node, idx)

            elseif r == SE_PIPELINE_SKIP_CONTINUE then
                skip = true   -- goto check_completion
            end
        end

        ::continue_fork::
    end

    -- check_completion: count active MAIN children
    local active_main = 0
    for i = 1, n do
        local child = children[i]
        local ct    = child.call_type
        if (ct == "m_call" or ct == "pt_m_call")
        and bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) ~= 0 then
            active_main = active_main + 1
        end
    end

    if active_main == 0 then
        ns.state = FORK_COMPLETE
        return SE_PIPELINE_DISABLE
    end
    return SE_PIPELINE_CONTINUE
end

-- ----------------------------------------------------------------------------
-- SE_FORK_JOIN
-- Faithful translation of C se_fork_join.
-- Parallel; returns FUNCTION_HALT while any MAIN child is active,
-- PIPELINE_DISABLE when all MAIN children complete.
-- Same result dispatch as se_fork; no state tracking needed.
-- ----------------------------------------------------------------------------
M.se_fork_join = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return SE_PIPELINE_CONTINUE end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return SE_PIPELINE_CONTINUE
    end

    local children = node.children or {}
    local n        = #children
    local skip     = false

    for i = 1, n do
        if skip then break end
        local child = children[i]
        local idx   = i - 1
        if not child.node_index then goto continue_fj end  -- non-callable param
        local ct    = child.call_type
        local cns   = get_ns(inst, child.node_index)

        -- Oneshot: fire once if not yet initialized
        if ct == "o_call" or ct == "io_call" then
            if bit.band(cns.flags, FLAG_INITIALIZED) == 0 then
                child_invoke(inst, node, idx, event_id, event_data)
            end
            goto continue_fj
        end

        -- Pred: evaluate once if not yet initialized
        if ct == "p_call" or ct == "p_call_composite" then
            if bit.band(cns.flags, FLAG_INITIALIZED) == 0 then
                child_invoke(inst, node, idx, event_id, event_data)
            end
            goto continue_fj
        end

        -- Main: only invoke if active
        if bit.band(cns.flags, FLAG_ACTIVE) == 0 then
            goto continue_fj
        end

        do
            local r = child_invoke(inst, node, idx, event_id, event_data)

            if r == SE_FUNCTION_HALT then r = SE_PIPELINE_HALT end

            if r < SE_PIPELINE_CONTINUE then
                return r
            end

            if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
                -- child still running

            elseif r == SE_PIPELINE_DISABLE or r == SE_PIPELINE_TERMINATE then
                child_terminate(inst, node, idx)

            elseif r == SE_PIPELINE_RESET then
                child_terminate(inst, node, idx)
                child_reset_recursive(inst, node, idx)

            elseif r == SE_PIPELINE_SKIP_CONTINUE then
                skip = true
            end
        end

        ::continue_fj::
    end

    -- check_completion: count active MAIN children
    local active_main = 0
    for i = 1, n do
        local child = children[i]
        local ct    = child.call_type
        if (ct == "m_call" or ct == "pt_m_call")
        and bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) ~= 0 then
            active_main = active_main + 1
        end
    end

    if active_main == 0 then
        return SE_PIPELINE_DISABLE
    end
    return SE_FUNCTION_HALT
end

-- ----------------------------------------------------------------------------
-- SE_CHAIN_FLOW
-- Faithful translation of the C se_chain_flow.
-- Ticks all active children each tick with full result-code dispatch:
--   Oneshot/pred children: invoke then terminate (fire-and-done).
--   Main children:
--     PIPELINE_CONTINUE     -> active_count++, continue
--     PIPELINE_HALT         -> stop loop, return PIPELINE_CONTINUE
--     PIPELINE_DISABLE      -> terminate child, continue (not counted)
--     PIPELINE_TERMINATE    -> terminate all, return PIPELINE_TERMINATE
--     PIPELINE_RESET        -> terminate all + reset all, return PIPELINE_CONTINUE
--     PIPELINE_SKIP_CONTINUE-> active_count++, stop loop (skip remaining)
--     FUNCTION_HALT         -> return PIPELINE_HALT
--     any other (< PIPELINE_CONTINUE) -> propagate immediately to caller
-- Final: PIPELINE_DISABLE if active_count==0, else PIPELINE_CONTINUE.
-- ----------------------------------------------------------------------------
M.se_chain_flow = function(inst, node, event_id, event_data)
    if event_id == SE_EVENT_INIT then return SE_PIPELINE_CONTINUE end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return SE_PIPELINE_CONTINUE
    end

    local children = node.children or {}
    local n = #children
    local bit = require("bit")
    local FLAG_ACTIVE = 0x01
    local active_count = 0
    local skip = false

    for i = 1, n do
        if skip then break end
        local child = children[i]
        local idx   = i - 1   -- 0-based index for child_invoke / child_terminate

        -- Skip inactive children (all call types)
        if bit.band(get_ns(inst, child.node_index).flags, FLAG_ACTIVE) == 0 then
            goto continue_loop
        end

        local ct = child.call_type

        -- Oneshot: fire and terminate (don't count as active)
        if ct == "o_call" or ct == "io_call" then
            child_invoke(inst, node, idx, event_id, event_data)
            child_terminate(inst, node, idx)
            goto continue_loop
        end

        -- Pred: evaluate and terminate (don't count as active)
        if ct == "p_call" or ct == "p_call_composite" then
            child_invoke(inst, node, idx, event_id, event_data)
            child_terminate(inst, node, idx)
            goto continue_loop
        end

        -- Main (m_call / pt_m_call): invoke and dispatch on result
        do
            local r = child_invoke(inst, node, idx, event_id, event_data)

            -- FUNCTION_HALT -> PIPELINE_HALT
            if r == SE_FUNCTION_HALT then
                return SE_PIPELINE_HALT
            end

            -- Non-pipeline codes (0-11, excluding FUNCTION_HALT already handled):
            -- propagate immediately to caller
            if r < SE_PIPELINE_CONTINUE then
                return r
            end

            -- Pipeline codes (12-17)
            if r == SE_PIPELINE_CONTINUE then
                active_count = active_count + 1

            elseif r == SE_PIPELINE_HALT then
                -- Stop processing remaining children; return CONTINUE to caller
                return SE_PIPELINE_CONTINUE

            elseif r == SE_PIPELINE_DISABLE then
                -- Child done; terminate it, don't count as active
                child_terminate(inst, node, idx)

            elseif r == SE_PIPELINE_TERMINATE then
                children_terminate_all(inst, node)
                return SE_PIPELINE_TERMINATE

            elseif r == SE_PIPELINE_RESET then
                children_terminate_all(inst, node)
                children_reset_all(inst, node)
                return SE_PIPELINE_CONTINUE

            elseif r == SE_PIPELINE_SKIP_CONTINUE then
                active_count = active_count + 1
                skip = true   -- mirrors goto tick_complete

            else
                -- default: count as active
                active_count = active_count + 1
            end
        end

        ::continue_loop::
    end

    if active_count == 0 then
        return SE_PIPELINE_DISABLE
    end
    return SE_PIPELINE_CONTINUE
end

-- ----------------------------------------------------------------------------
-- SE_WHILE
-- Faithful translation of C se_while.
-- children[0] = predicate
-- children[1] = body
-- ns.state: 0=EVAL_PRED, 1=RUN_BODY
--
-- INIT:     state=EVAL_PRED, return PIPELINE_CONTINUE.
-- TERMINATE: terminate body only if initialized; return PIPELINE_CONTINUE.
-- TICK state=EVAL_PRED:
--   eval pred; false -> PIPELINE_DISABLE.
--   true -> child_reset_recursive body, state=RUN_BODY, fall through to RUN_BODY.
-- TICK state=RUN_BODY:
--   invoke body; non-pipeline -> propagate.
--   CONTINUE/HALT/SKIP_CONTINUE -> body still running, return FUNCTION_HALT.
--   DISABLE/TERMINATE/RESET     -> body done: child_terminate+reset_recursive,
--                                  state=EVAL_PRED, return PIPELINE_HALT.
-- ----------------------------------------------------------------------------
local SE_WHILE_EVAL_PRED = 0
local SE_WHILE_RUN_BODY  = 1

M.se_while = function(inst, node, event_id, event_data)
    local ns = get_ns(inst, node.node_index)

    if event_id == SE_EVENT_INIT then
        ns.state = SE_WHILE_EVAL_PRED
        return SE_PIPELINE_CONTINUE
    end

    if event_id == SE_EVENT_TERMINATE then
        -- Only terminate body if it was running
        local children = node.children or {}
        if children[2] then
            local cns = get_ns(inst, children[2].node_index)
            if bit.band(cns.flags, FLAG_INITIALIZED) ~= 0 then
                child_terminate(inst, node, 1)
            end
        end
        return SE_PIPELINE_CONTINUE
    end

    -- TICK
    if ns.state == SE_WHILE_EVAL_PRED then
        if not child_invoke_pred(inst, node, 0) then
            return SE_PIPELINE_DISABLE
        end
        -- Pred true: reset body and fall through to RUN_BODY this tick
        child_reset_recursive(inst, node, 1)
        ns.state = SE_WHILE_RUN_BODY
    end

    -- RUN_BODY
    local r = child_invoke(inst, node, 1, event_id, event_data)

    -- Non-pipeline codes: propagate immediately
    if r < SE_PIPELINE_CONTINUE then
        return r
    end

    if r == SE_PIPELINE_CONTINUE
    or r == SE_PIPELINE_HALT
    or r == SE_PIPELINE_SKIP_CONTINUE then
        -- Body still running
        return SE_FUNCTION_HALT

    elseif r == SE_PIPELINE_DISABLE
        or r == SE_PIPELINE_TERMINATE
        or r == SE_PIPELINE_RESET then
        -- Body complete: terminate, reset, loop back to pred check next tick
        child_terminate(inst, node, 1)
        child_reset_recursive(inst, node, 1)
        ns.state = SE_WHILE_EVAL_PRED
        return SE_PIPELINE_HALT

    else
        return SE_PIPELINE_DISABLE
    end
end

-- ----------------------------------------------------------------------------
-- SE_IF_THEN_ELSE
-- Faithful translation of C se_if_then_else.
-- children[0] = predicate (re-evaluated every tick)
-- children[1] = then-branch
-- children[2] = else-branch (optional)
--
-- INIT/TERMINATE: passthrough (terminate all on TERMINATE).
-- TICK: evaluate pred every tick; invoke then or else branch.
--   Result dispatch:
--     non-pipeline (< 12)         -> propagate immediately
--     PIPELINE_CONTINUE/HALT      -> return as-is
--     PIPELINE_RESET              -> terminate+reset then+else, return PIPELINE_RESET
--     PIPELINE_DISABLE/TERMINATE  -> terminate+reset then+else, return PIPELINE_CONTINUE
--     PIPELINE_SKIP_CONTINUE      -> return PIPELINE_CONTINUE
-- Note: no state machine — predicate is re-evaluated on every tick.
-- ----------------------------------------------------------------------------
M.se_if_then_else = function(inst, node, event_id, event_data)
    local children = node.children or {}
    local n        = #children
    assert(n >= 2, "se_if_then_else: need at least predicate and then branch")
    local has_else = (n >= 3)

    if event_id == SE_EVENT_INIT then
        return SE_PIPELINE_CONTINUE
    end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        return SE_PIPELINE_CONTINUE
    end

    -- Evaluate predicate (child 0) every tick
    local condition = child_invoke_pred(inst, node, 0)

    local r
    if condition then
        r = child_invoke(inst, node, 1, event_id, event_data)
    elseif has_else then
        r = child_invoke(inst, node, 2, event_id, event_data)
    else
        return SE_PIPELINE_CONTINUE   -- no else, condition false
    end

    -- Non-pipeline codes: propagate immediately
    if r < SE_PIPELINE_CONTINUE then
        return r
    end

    if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
        return r

    elseif r == SE_PIPELINE_RESET then
        -- terminate and reset both branches
        child_terminate(inst, node, 1)
        child_reset(inst, node, 1)
        if has_else then
            child_terminate(inst, node, 2)
            child_reset(inst, node, 2)
        end
        return SE_PIPELINE_RESET

    elseif r == SE_PIPELINE_DISABLE or r == SE_PIPELINE_TERMINATE then
        -- branch done: terminate+reset both, return CONTINUE (not DISABLE)
        child_terminate(inst, node, 1)
        child_reset(inst, node, 1)
        if has_else then
            child_terminate(inst, node, 2)
            child_reset(inst, node, 2)
        end
        return SE_PIPELINE_CONTINUE

    else  -- PIPELINE_SKIP_CONTINUE or unknown
        return SE_PIPELINE_CONTINUE
    end
end

-- ----------------------------------------------------------------------------
-- SE_COND
-- Faithful translation of C se_cond.
-- Multi-branch conditional: pairs of (pred, action) at even/odd child indices.
-- Predicates re-evaluated every tick; active branch tracked in ns.user_data.
-- ns.user_data: 0xFFFF = no active child, else 0-based action child index.
--
-- INIT/TERMINATE: set user_data=0xFFFF, terminate all on TERMINATE.
-- TICK:
--   Walk children pairwise: even=pred, odd=action.
--   Find first pred that returns true -> matched_action index.
--   If matched_action changed: terminate+reset_recursive old, terminate+reset_recursive new.
--   Invoke matched_action.
--   Result dispatch:
--     non-pipeline (< 12)              -> propagate
--     PIPELINE_CONTINUE/HALT           -> return PIPELINE_CONTINUE
--     PIPELINE_RESET                   -> terminate+reset_recursive action, PIPELINE_CONTINUE
--     PIPELINE_DISABLE/TERMINATE/SKIP  -> return r directly
-- ----------------------------------------------------------------------------
M.se_cond = function(inst, node, event_id, event_data)
    local ns       = get_ns(inst, node.node_index)
    local children = node.children or {}
    local n        = #children
    local NO_CHILD = 0xFFFF

    if event_id == SE_EVENT_INIT then
        ns.user_data = NO_CHILD
        return SE_PIPELINE_CONTINUE
    end
    if event_id == SE_EVENT_TERMINATE then
        children_terminate_all(inst, node)
        ns.user_data = NO_CHILD
        return SE_PIPELINE_CONTINUE
    end

    -- Find first matching pred (even indices 0,2,4,...; actions at 1,3,5,...)
    local matched_action = NO_CHILD
    local i = 1  -- 1-based Lua index
    while i <= n do
        local child = children[i]
        local ct    = child.call_type
        if ct == "p_call" or ct == "p_call_composite" then
            local pred_result = child_invoke_pred(inst, node, i - 1)
            if pred_result and matched_action == NO_CHILD then
                matched_action = i   -- action is next child (1-based)
                break
            end
            i = i + 2  -- skip past action
        else
            i = i + 1
        end
    end

    if matched_action == NO_CHILD then
        -- No pred matched; exception in C — return PIPELINE_CONTINUE
        return SE_PIPELINE_CONTINUE
    end

    local action_idx = matched_action   -- 0-based index of action child
    local active     = ns.user_data

    -- Branch switch: terminate old, reset new
    if action_idx ~= active then
        if active ~= NO_CHILD then
            child_terminate(inst, node, active)
            child_reset_recursive(inst, node, active)
        end
        child_terminate(inst, node, action_idx)
        child_reset_recursive(inst, node, action_idx)
        ns.user_data = action_idx
    end

    local r = child_invoke(inst, node, action_idx, event_id, event_data)

    -- Non-pipeline codes: propagate
    if r < SE_PIPELINE_CONTINUE then
        return r
    end

    if r == SE_PIPELINE_CONTINUE or r == SE_PIPELINE_HALT then
        return SE_PIPELINE_CONTINUE

    elseif r == SE_PIPELINE_RESET then
        child_terminate(inst, node, action_idx)
        child_reset_recursive(inst, node, action_idx)
        return SE_PIPELINE_CONTINUE

    else  -- PIPELINE_DISABLE, TERMINATE, SKIP_CONTINUE
        return r
    end
end

return M