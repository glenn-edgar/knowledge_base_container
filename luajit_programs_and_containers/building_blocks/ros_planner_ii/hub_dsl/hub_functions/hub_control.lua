--[[
    hub_control.lua -- Hub control functions (instance-based).

    Tracks active KB and elapsed time on the blackboard (KeyStore).
    Handles delta pose events from remote, translates to global coordinates.

    Each hub_runtime creates its own hub_control instance with its own
    pose state. No shared global state — safe for coroutine concurrency.

    Usage:
        local hub_control = require("hub_control")
        local ctrl = hub_control.new({ x=0, y=0, z=0, heading=0, arm_angle=0 })
        ctrl:on_kb_start(bb, kb_name, plugin)
        ctrl:on_tick(bb)
        ctrl:on_kb_done(bb, kb_name, kb_done_data)
        local pose = ctrl:get_global_pose()
]]

local M = {}
M.__index = M

function M.new(initial_pose)
    local self = setmetatable({}, M)
    self.global_pose = {
        x         = (initial_pose and initial_pose.x) or 0,
        y         = (initial_pose and initial_pose.y) or 0,
        z         = (initial_pose and initial_pose.z) or 0,
        heading   = (initial_pose and initial_pose.heading) or 0,
        arm_angle = (initial_pose and initial_pose.arm_angle) or 0,
    }
    return self
end

function M:get_global_pose()
    return self.global_pose
end

function M:set_global_pose(pose)
    self.global_pose.x         = pose.x or 0
    self.global_pose.y         = pose.y or 0
    self.global_pose.z         = pose.z or 0
    self.global_pose.heading   = pose.heading or 0
    self.global_pose.arm_angle = pose.arm_angle or 0
end

function M:on_kb_start(bb, kb_name, kb_plugin)
    bb.active_kb = kb_name
    bb.active_kb_started = os.clock()
    bb.active_kb_elapsed = 0

    bb.kb_ref_x = self.global_pose.x
    bb.kb_ref_y = self.global_pose.y
    bb.kb_ref_z = self.global_pose.z
    bb.kb_ref_heading = self.global_pose.heading
    bb.kb_ref_arm_angle = self.global_pose.arm_angle

    if kb_plugin and kb_plugin.bitmask then
        bb[kb_name .. ".bitmask"] = 0
    end

    if bb.flush then bb.flush() end
end

function M:on_tick(bb)
    if bb.active_kb_started then
        bb.active_kb_elapsed = math.floor((os.clock() - bb.active_kb_started) * 1000)
    end
end

function M:apply_delta_pose(bb, delta)
    self.global_pose.x         = self.global_pose.x + (delta.delta_x or 0)
    self.global_pose.y         = self.global_pose.y + (delta.delta_y or 0)
    self.global_pose.z         = self.global_pose.z + (delta.delta_z or 0)
    self.global_pose.heading   = self.global_pose.heading + (delta.delta_heading or 0)
    self.global_pose.arm_angle = self.global_pose.arm_angle + (delta.delta_arm_angle or 0)

    bb.global_x = self.global_pose.x
    bb.global_y = self.global_pose.y
    bb.global_z = self.global_pose.z
    bb.global_heading = self.global_pose.heading
    bb.global_arm_angle = self.global_pose.arm_angle
end

function M:on_kb_done(bb, kb_name, kb_done_data)
    if kb_done_data then
        self:apply_delta_pose(bb, kb_done_data)
        bb.kb_done_success = kb_done_data.success
    end

    bb.active_kb = ""
    bb.active_kb_elapsed = 0
    bb.active_kb_started = nil

    if bb.flush then bb.flush() end
end

function M:apply_bitmask(bb, kb_name, kb_plugin, bitmap_data)
    if not kb_plugin or not kb_plugin.bitmask then return end

    local mask = bb[kb_name .. ".bitmask"] or 0
    for _, field in ipairs(kb_plugin.bitmask) do
        if bitmap_data[field.name] then
            local bit_val = 2 ^ field.bit
            mask = mask + bit_val
        end
    end
    bb[kb_name .. ".bitmask"] = mask
end

---------------------------------------------------------------------------
-- Backward-compatible module-level functions (for test_nats_chaintree.lua)
-- Uses a default singleton instance. New code should use hub_control.new().
---------------------------------------------------------------------------

-- Save instance method references before adding module-level wrappers
local _inst_get_global_pose = M.get_global_pose
local _inst_set_global_pose = M.set_global_pose
local _inst_on_kb_start     = M.on_kb_start
local _inst_on_tick         = M.on_tick
local _inst_on_kb_done      = M.on_kb_done
local _inst_apply_delta_pose = M.apply_delta_pose
local _inst_apply_bitmask   = M.apply_bitmask

local _default = nil

local function get_default()
    if not _default then _default = M.new() end
    return _default
end

-- Module-level wrappers (called without self, delegate to singleton)
function M.get_global_pose()
    return _inst_get_global_pose(get_default())
end

function M.set_global_pose(pose)
    if not _default then
        _default = M.new(pose)
    else
        _inst_set_global_pose(_default, pose)
    end
end

function M.on_kb_start(bb, kb_name, kb_plugin)
    _inst_on_kb_start(get_default(), bb, kb_name, kb_plugin)
end

function M.on_tick(bb)
    _inst_on_tick(get_default(), bb)
end

function M.on_kb_done(bb, kb_name, kb_done_data)
    _inst_on_kb_done(get_default(), bb, kb_name, kb_done_data)
end

function M.apply_delta_pose(bb, delta)
    _inst_apply_delta_pose(get_default(), bb, delta)
end

function M.apply_bitmask(bb, kb_name, kb_plugin, bitmap_data)
    _inst_apply_bitmask(get_default(), bb, kb_name, kb_plugin, bitmap_data)
end

return M
