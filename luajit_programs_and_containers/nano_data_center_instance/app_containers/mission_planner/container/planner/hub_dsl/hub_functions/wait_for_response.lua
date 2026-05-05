--[[
    wait_for_response.lua -- Hub main function that waits for KB_DONE.

    Listens for HUB_STREAM_KB_DONE event from the remote.
    The event_data carries:
      - success: boolean (true = completed ok, false = failed)
      - delta_x, delta_y, delta_z, delta_heading, delta_arm_angle (pose deltas)

    Bitmap events (HUB_STREAM_BITMAP) arrive during execution for
    real-time monitoring but do NOT signal completion.

    KB_DONE is the definitive "I'm done" signal from the remote.
]]

local defs      = require("ct_definitions")
local event_ids = require("event_ids")

local M = {}
M.main = {}

M.main.HUB_WAIT_FOR_RESPONSE = function(handle, bool_fn, node, event_id, event_data)
    -- KB_DONE: remote signals completion
    if event_id == event_ids.HUB_STREAM_KB_DONE then
        if event_data then
            -- Store success flag on blackboard for the test harness / planner to read
            handle.blackboard.kb_done_success = event_data.success
        end
        return defs.CFL_DISABLE
    end

    -- Bitmap: informational during execution (hub tree can react)
    if event_id == event_ids.HUB_STREAM_BITMAP then
        return defs.CFL_CONTINUE
    end

    -- Ack: command acknowledged
    if event_id == event_ids.HUB_STREAM_ACK then
        return defs.CFL_CONTINUE
    end

    -- Delta pose: intermediate position update
    if event_id == event_ids.HUB_STREAM_DELTA_POSE then
        return defs.CFL_CONTINUE
    end

    -- Timer tick: keep waiting
    if event_id == defs.CFL_TIMER_EVENT then
        return defs.CFL_CONTINUE
    end

    return defs.CFL_CONTINUE
end

M.registry = {
    main = M.main,
}

return M
