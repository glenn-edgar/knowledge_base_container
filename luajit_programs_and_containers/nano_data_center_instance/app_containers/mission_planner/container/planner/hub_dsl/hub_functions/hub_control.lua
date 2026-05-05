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
--
-- These live in a separate table so they don't shadow instance methods
-- via M.__index. Instance calls (ctrl:on_kb_start) go through M.__index
-- and find the instance methods. Module calls (hub_control.on_kb_start)
-- go through the module table directly.
---------------------------------------------------------------------------

local _default = nil

local function get_default()
    if not _default then _default = M.new() end
    return _default
end

-- Module-level functions stored in M directly (non-method keys)
-- These use different names to avoid shadowing instance methods.
-- But for backward compat, we use the same names — so we need a
-- __call-style dispatch or separate the namespaces.

-- Solution: use a wrapper module table that delegates to either
-- the singleton (dot call) or the instance (colon call).
-- Lua can't distinguish, so we detect by checking if first arg is an instance.

local _inst_methods = {
    get_global_pose  = M.get_global_pose,
    set_global_pose  = M.set_global_pose,
    on_kb_start      = M.on_kb_start,
    on_tick          = M.on_tick,
    on_kb_done       = M.on_kb_done,
    apply_delta_pose = M.apply_delta_pose,
    apply_bitmask    = M.apply_bitmask,
}

-- Override __index to return instance methods for instances,
-- module-level wrappers for the module table itself.
local _orig_index = M.__index
M.__index = function(t, key)
    -- Instance method lookup: return the real instance method
    local method = _inst_methods[key]
    if method then return method end
    -- Fall through to normal table lookup
    if type(_orig_index) == "table" then
        return _orig_index[key]
    elseif type(_orig_index) == "function" then
        return _orig_index(t, key)
    end
end

-- Now overwrite M's direct entries with module-level singleton wrappers.
-- These are found when calling hub_control.on_kb_start() (no __index).
M.get_global_pose = function(maybe_self, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.get_global_pose(maybe_self, ...)
    end
    return _inst_methods.get_global_pose(get_default())
end

M.set_global_pose = function(maybe_self, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.set_global_pose(maybe_self, ...)
    end
    local pose = maybe_self  -- first arg is pose when called as module fn
    if not _default then
        _default = M.new(pose)
    else
        _inst_methods.set_global_pose(_default, pose)
    end
end

M.on_kb_start = function(maybe_self, bb_or_kb, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.on_kb_start(maybe_self, bb_or_kb, ...)
    end
    return _inst_methods.on_kb_start(get_default(), maybe_self, bb_or_kb, ...)
end

M.on_tick = function(maybe_self, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.on_tick(maybe_self, ...)
    end
    return _inst_methods.on_tick(get_default(), maybe_self, ...)
end

M.on_kb_done = function(maybe_self, bb_or_kb, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.on_kb_done(maybe_self, bb_or_kb, ...)
    end
    return _inst_methods.on_kb_done(get_default(), maybe_self, bb_or_kb, ...)
end

M.apply_delta_pose = function(maybe_self, bb_or_delta, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.apply_delta_pose(maybe_self, bb_or_delta, ...)
    end
    return _inst_methods.apply_delta_pose(get_default(), maybe_self, bb_or_delta, ...)
end

M.apply_bitmask = function(maybe_self, bb_or_kb, ...)
    if type(maybe_self) == "table" and maybe_self.global_pose then
        return _inst_methods.apply_bitmask(maybe_self, bb_or_kb, ...)
    end
    return _inst_methods.apply_bitmask(get_default(), maybe_self, bb_or_kb, ...)
end

return M
