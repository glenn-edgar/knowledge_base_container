--[[
    hub_control.lua -- Hub control functions.

    Tracks active KB and elapsed time on the blackboard (KeyStore).
    Handles delta pose events from remote, translates to global coordinates.

    Called by the test harness / local planner around each KB activation.
    Not a ChainTree function — operates on the blackboard directly.
]]

local json_util = require("json_util")

local M = {}

-- Global pose state (accumulated across KBs)
local global_pose = {
    x = 0, y = 0, z = 0,
    heading = 0,
    arm_angle = 0,
}

function M.get_global_pose()
    return global_pose
end

function M.set_global_pose(pose)
    for k, v in pairs(pose) do
        global_pose[k] = v
    end
end

-- Called when a KB is activated: set reference point, start timer
function M.on_kb_start(bb, kb_name, kb_plugin)
    bb.active_kb = kb_name
    bb.active_kb_started = os.clock()
    bb.active_kb_elapsed = 0

    -- Store the global reference point for this KB
    bb.kb_ref_x = global_pose.x
    bb.kb_ref_y = global_pose.y
    bb.kb_ref_z = global_pose.z
    bb.kb_ref_heading = global_pose.heading
    bb.kb_ref_arm_angle = global_pose.arm_angle

    -- Initialize per-KB bitmask to 0
    if kb_plugin and kb_plugin.bitmask then
        bb[kb_name .. ".bitmask"] = 0
    end

    -- Flush to KeyStore
    if bb.flush then bb.flush() end
end

-- Called each tick: update elapsed time
function M.on_tick(bb)
    if bb.active_kb_started then
        bb.active_kb_elapsed = math.floor((os.clock() - bb.active_kb_started) * 1000)
    end
end

-- Called when delta pose arrives from remote stream
function M.apply_delta_pose(bb, delta)
    if delta.delta_x then
        global_pose.x = (bb.kb_ref_x or 0) + delta.delta_x
    end
    if delta.delta_y then
        global_pose.y = (bb.kb_ref_y or 0) + delta.delta_y
    end
    if delta.delta_z then
        global_pose.z = (bb.kb_ref_z or 0) + delta.delta_z
    end
    if delta.delta_heading then
        global_pose.heading = (bb.kb_ref_heading or 0) + delta.delta_heading
    end
    if delta.delta_arm_angle then
        global_pose.arm_angle = (bb.kb_ref_arm_angle or 0) + delta.delta_arm_angle
    end

    -- Write current global pose to blackboard
    bb.global_x = global_pose.x
    bb.global_y = global_pose.y
    bb.global_z = global_pose.z
    bb.global_heading = global_pose.heading
    bb.global_arm_angle = global_pose.arm_angle
end

-- Called when KB completes: apply final delta pose from kb_done event
function M.on_kb_done(bb, kb_name, kb_done_data)
    -- Apply delta pose from kb_done event
    if kb_done_data then
        M.apply_delta_pose(bb, kb_done_data)
        bb.kb_done_success = kb_done_data.success
    end

    bb.active_kb = ""
    bb.active_kb_elapsed = 0
    bb.active_kb_started = nil

    -- Flush to KeyStore
    if bb.flush then bb.flush() end
end

-- Apply per-KB bitmask from stream bitmap event
function M.apply_bitmask(bb, kb_name, kb_plugin, bitmap_data)
    if not kb_plugin or not kb_plugin.bitmask then return end

    local mask = bb[kb_name .. ".bitmask"] or 0
    for _, field in ipairs(kb_plugin.bitmask) do
        if bitmap_data[field.name] then
            local bit_val = 2 ^ field.bit
            mask = mask + bit_val  -- simple set (no unset needed for streaming)
        end
    end
    bb[kb_name .. ".bitmask"] = mask
end

return M
