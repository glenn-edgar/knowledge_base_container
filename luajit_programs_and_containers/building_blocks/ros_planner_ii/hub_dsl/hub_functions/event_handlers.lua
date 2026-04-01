--[[
    event_handlers.lua -- Generic hub event handler functions.

    Schema-driven: configured by KB plugin data passed as node_dict.
    Each handler filters for its event_id, fires a one-shot to parse
    the event_data, and manages timeouts.

    Three handler types:
      HUB_ACK_HANDLER      — waits for ack, timeout → error_recovery
      HUB_BITMAP_HANDLER   — receives bitmap updates, parses per-KB bitmask
      HUB_KB_DONE_HANDLER  — waits for kb_done, applies pose, timeout → error_recovery

    All handlers use CFL_CONTINUE while waiting, and remain active
    after processing (bitmaps can arrive multiple times).
    KB_DONE handler terminates the column on receipt.
]]

local defs        = require("ct_definitions")
local event_ids   = require("event_ids")
local hub_control = require("hub_control")
local json_util   = require("json_util")
local engine      -- lazy loaded

local function get_engine()
    if not engine then engine = require("ct_engine") end
    return engine
end

local M = {}
M.main = {}
M.one_shot = {}
M.boolean = {}

---------------------------------------------------------------------------
-- ACK handler: waits for HUB_STREAM_ACK
---------------------------------------------------------------------------

M.main.HUB_ACK_HANDLER = function(handle, bool_fn, node, event_id, event_data)
    local bb = handle.blackboard

    if event_id == event_ids.HUB_STREAM_ACK then
        if event_data then
            bb.ack_received = true
            bb.ack_seq = event_data.seq
            bb.ack_status = event_data.status
        end
        return defs.CFL_CONTINUE
    end

    -- Timer tick: check timeout
    if event_id == defs.CFL_TIMER_EVENT then
        if not bb.ack_received and bb.ack_timeout_ticks then
            bb.ack_timeout_ticks = bb.ack_timeout_ticks - 1
            if bb.ack_timeout_ticks <= 0 then
                -- ACK timeout → trigger error recovery
                bb.fault_reason = "ack_timeout"
                bb.fault_kb = bb.active_kb
                bb.kb_done_success = false
                return defs.CFL_TERMINATE  -- terminate KB → error handler
            end
        end
    end

    return defs.CFL_CONTINUE
end

M.one_shot.HUB_ACK_INIT = function(handle, node)
    local bb = handle.blackboard
    bb.ack_received = false
    bb.ack_seq = nil
    bb.ack_status = nil
    -- Default timeout: 50 ticks (~5 seconds at 100ms tick)
    local timeout = (node.node_dict and node.node_dict.timeout_ticks) or 50
    bb.ack_timeout_ticks = timeout
end

---------------------------------------------------------------------------
-- Bitmap handler: receives HUB_STREAM_BITMAP, parses per-KB bitmask
---------------------------------------------------------------------------

M.main.HUB_BITMAP_HANDLER = function(handle, bool_fn, node, event_id, event_data)
    if event_id == event_ids.HUB_STREAM_BITMAP and event_data then
        local bb = handle.blackboard
        local kb_name = bb.active_kb
        if kb_name and kb_name ~= "" then
            -- Parse bitmask from event_data using plugin schema
            -- The bitmask_fields are stored in node_dict by the KB plugin
            local bitmask_fields = node.node_dict and node.node_dict.bitmask_fields
            if bitmask_fields then
                local mask = bb[kb_name .. ".bitmask"] or 0
                for _, field in ipairs(bitmask_fields) do
                    if event_data[field.name] then
                        mask = mask + (2 ^ field.bit)
                    end
                end
                bb[kb_name .. ".bitmask"] = mask
            end
        end
        return defs.CFL_CONTINUE
    end

    return defs.CFL_CONTINUE
end

M.one_shot.HUB_BITMAP_INIT = function(handle, node)
    local bb = handle.blackboard
    local kb_name = bb.active_kb
    if kb_name and kb_name ~= "" then
        bb[kb_name .. ".bitmask"] = 0
    end
end

---------------------------------------------------------------------------
-- KB_DONE handler: waits for HUB_STREAM_KB_DONE
-- Applies delta pose, extracts success flag, terminates column
---------------------------------------------------------------------------

M.main.HUB_KB_DONE_HANDLER = function(handle, bool_fn, node, event_id, event_data)
    local bb = handle.blackboard

    if event_id == event_ids.HUB_STREAM_KB_DONE then
        if event_data then
            -- Apply delta pose to global coordinates
            hub_control.apply_delta_pose(bb, event_data)
            -- Record success
            bb.kb_done_success = event_data.success
            bb.kb_done_received = true
        end
        return defs.CFL_TERMINATE  -- terminate column → KB completes
    end

    -- Timer tick: check timeout
    if event_id == defs.CFL_TIMER_EVENT then
        if not bb.kb_done_received and bb.kb_done_timeout_ticks then
            bb.kb_done_timeout_ticks = bb.kb_done_timeout_ticks - 1
            if bb.kb_done_timeout_ticks <= 0 then
                -- KB_DONE timeout → trigger error recovery
                bb.fault_reason = "kb_done_timeout"
                bb.fault_kb = bb.active_kb
                bb.kb_done_success = false
                return defs.CFL_TERMINATE  -- terminate KB → error handler
            end
        end
    end

    -- Other events: continue waiting
    return defs.CFL_CONTINUE
end

M.one_shot.HUB_KB_DONE_INIT = function(handle, node)
    local bb = handle.blackboard
    bb.kb_done_received = false
    bb.kb_done_success = nil
    -- Default timeout: 100 ticks (~10 seconds at 100ms tick)
    local timeout = (node.node_dict and node.node_dict.timeout_ticks) or 100
    bb.kb_done_timeout_ticks = timeout
end

---------------------------------------------------------------------------
-- Registry
---------------------------------------------------------------------------

M.registry = {
    main     = M.main,
    one_shot = M.one_shot,
    boolean  = M.boolean,
}

return M
