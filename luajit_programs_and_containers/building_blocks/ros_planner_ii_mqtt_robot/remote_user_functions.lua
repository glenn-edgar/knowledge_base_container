--[[
    remote_user_functions.lua -- ChainTree worker functions for sim robot.

    New shape (continuous-motion version):
      - Path workers (line, spline, rotate) are thin: they wait for C to finish
        their seg_id. The controller has already pushed the segment to the C
        queue, so the robot keeps moving without stopping between segments.
      - Non-motion workers (deliver, paint, load, scan, idle, recharge, op,
        pass_gate) run only after the robot is at rest. robot_controller
        activates them only once phys_is_stopped() is true.
      - init_check is a self-test that just completes after a short dwell.

    bb.active_worker is the worker KB name.
    bb._seg_id is set by robot_controller when a path worker activates.
    bb._seg_start holds pose snapshot at activation (for per-packet deltas).
    bb._tool_start holds arm angle at activation (for delta_arm_angle).

    Hal handle is stashed on the blackboard (bb._hal) by robot_controller.
    All workers set bb.worker_alive = true each tick (watchdog ping).
]]

local defs        = require("ct_definitions")
local cmd_packets = require("command_packets")
local json_util   = require("json_util")

local M = {}
M.main = {}
M.one_shot = {}
M.boolean = {}

local function band(a, b)
    -- LuaJIT has bit.band; fall back to math if not loaded
    return bit.band(a, b)
end

-- =========================================================================
-- Common termination
-- =========================================================================

M.one_shot.WORKER_TERM = function(handle, node)
    local bb = handle.blackboard
    if bb.worker_done ~= true then
        bb.worker_done = true
        if bb.worker_success == nil then bb.worker_success = true end
    end
end

-- =========================================================================
-- init_check
-- =========================================================================

M.one_shot.WKR_INIT_CHECK_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    io.stderr:write(string.format("  VN[init_check] cmd: %s\n", bb.command_json))
end

M.main.WKR_INIT_CHECK_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = 3   -- very short self-test
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb["worker_init_check.bitmask"] = 0x0F  -- battery|motors|sensors|comms ok
            bb.worker_success = true
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- Path workers: C is already tracking bb._seg_id. We poll for completion.
-- =========================================================================

local function path_main(h, segment_done_mask)
    local bb = h.blackboard
    bb.worker_alive = true

    local hal = bb._hal
    local pose = hal:read_pose()
    local st   = hal:read_path_status()

    -- per-packet pose deltas (since worker activation)
    local s0 = bb._seg_start or { x = 0, y = 0, heading = 0 }
    bb.delta_x       = pose.x - s0.x
    bb.delta_y       = pose.y - s0.y
    bb.delta_heading = pose.heading - s0.heading

    -- Fault: cross-track explosion reported as PATH_F_FAULT
    if band(st.flags, hal.PATH_F.FAULT) ~= 0 then
        bb[bb.active_worker .. ".bitmask"] = 0x04  -- motor_fault
        bb.worker_success = false
        bb.fault_reason = "path_fault"
        return defs.CFL_DISABLE
    end

    -- Done when our seg_id has been declared last_done
    if bb._seg_id and st.last_done_seg_id == bb._seg_id then
        bb[bb.active_worker .. ".bitmask"] = segment_done_mask
        bb.worker_success = true
        return defs.CFL_DISABLE
    end

    -- Progress bit (progress lives in bit 0 of bitmask heartbeat - unused for now)
    return defs.CFL_CONTINUE
end

-- --- path_line / path_spline / path_wall share the same MAIN -----------

local function path_init(h, kind)
    local bb = h.blackboard
    bb.exec_start = false; bb.exec_active = true
    io.stderr:write(string.format("  VN[%s] seg_id=%s cmd: %s\n",
        kind, tostring(bb._seg_id), bb.command_json))
end

M.one_shot.WKR_PATH_LINE_INIT = function(h)   path_init(h, "path_line")   end
M.main.WKR_PATH_LINE_MAIN     = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    return path_main(h, 0x01)  -- seg_complete bit
end

M.one_shot.WKR_PATH_SPLINE_INIT = function(h) path_init(h, "path_spline") end
M.main.WKR_PATH_SPLINE_MAIN     = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    return path_main(h, 0x01)
end

M.one_shot.WKR_PATH_ROTATE_INIT = function(h) path_init(h, "path_rotate") end
M.main.WKR_PATH_ROTATE_MAIN     = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    return path_main(h, 0x01)  -- rotate_complete
end

-- path_wall: deannounced in capabilities. Keep a stub that faults.
M.one_shot.WKR_PATH_WALL_INIT = function(h)
    local bb = h.blackboard
    io.stderr:write("  VN[path_wall] UNSUPPORTED in sim (no obstacles)\n")
    bb.exec_active = true
end
M.main.WKR_PATH_WALL_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    bb.worker_success = false
    bb.fault_reason = "path_wall_unsupported"
    bb["worker_path_wall.bitmask"] = 0x04  -- motor_fault
    return defs.CFL_DISABLE
end

-- =========================================================================
-- Non-motion helpers: parse command, check stopped, run a sub-state machine.
-- robot_controller only activates non-motion workers when hal:is_stopped()
-- is already true, so workers may assume stationary at start.
-- =========================================================================

local function parse_cmd(bb)
    local ok, cmd = pcall(json_util.decode, bb.command_json)
    if not ok or not cmd then return {} end
    if cmd.params then
        for k, v in pairs(cmd.params) do
            if cmd[k] == nil then cmd[k] = v end
        end
    end
    return cmd
end

-- Arm operation: extend -> (optional grip/release) -> return home.
-- Sub-states via bb._sub.
local function arm_cycle_init(h, sub_sequence, cmd_kind)
    local bb = h.blackboard
    local cmd = parse_cmd(bb)
    bb._sub = 1
    bb._arm_target = math.rad(cmd.arm_target or 0)
    bb._arm_speed  = math.rad(cmd.arm_speed  or 60)
    bb._arm_return = math.rad(cmd.arm_return or 0)
    bb._sub_sequence = sub_sequence
    io.stderr:write(string.format("  VN[%s] seq=%s cmd: %s\n",
        cmd_kind, table.concat(sub_sequence, ","), bb.command_json))

    local hal = bb._hal
    local t = hal:read_tool_status(0)
    bb._tool_start = t.value

    -- Kick off first sub-op
    local first = sub_sequence[1]
    if first == "extend" then
        hal:begin_tool_move(0, bb._arm_target, bb._arm_speed)
    elseif first == "grip" then
        hal:begin_grip(1)
    elseif first == "release" then
        hal:begin_release(1)
    elseif first == "retract" then
        hal:begin_tool_move(0, bb._arm_return, bb._arm_speed)
    end
    bb.exec_active = true
end

local function arm_cycle_main(h)
    local bb = h.blackboard
    bb.worker_alive = true
    local hal = bb._hal

    local cur_op = bb._sub_sequence[bb._sub]
    local slot = (cur_op == "grip" or cur_op == "release") and 1 or 0
    local ts = hal:read_tool_status(slot)
    bb.delta_arm_angle = (hal:read_tool_status(0)).value - (bb._tool_start or 0)

    if band(ts.flags, hal.TOOL_F.AT_TARGET) ~= 0 then
        bb._sub = bb._sub + 1
        local nxt = bb._sub_sequence[bb._sub]
        if not nxt then
            bb[bb.active_worker .. ".bitmask"] = 0x07  -- at_target|gripped|complete
            bb.worker_success = true
            return defs.CFL_DISABLE
        end
        if nxt == "extend" then
            hal:begin_tool_move(0, bb._arm_target, bb._arm_speed)
        elseif nxt == "retract" then
            hal:begin_tool_move(0, bb._arm_return, bb._arm_speed)
        elseif nxt == "grip" then
            hal:begin_grip(1)
        elseif nxt == "release" then
            hal:begin_release(1)
        elseif nxt == "hold" then
            bb._hold_start = hal:sim_time()
            bb._hold_duration = (parse_cmd(bb).hold_time or 3.0)
        end
    elseif cur_op == "hold" then
        local now = hal:sim_time()
        if now - (bb._hold_start or now) >= (bb._hold_duration or 3.0) then
            bb._sub = bb._sub + 1
            local nxt = bb._sub_sequence[bb._sub]
            if not nxt then
                bb[bb.active_worker .. ".bitmask"] = 0x03
                bb.worker_success = true
                return defs.CFL_DISABLE
            end
            if nxt == "retract" then hal:begin_tool_move(0, bb._arm_return, bb._arm_speed) end
        end
    elseif band(ts.flags, hal.TOOL_F.FAULT) ~= 0 then
        bb[bb.active_worker .. ".bitmask"] = 0x08  -- arm_fault
        bb.worker_success = false
        bb.fault_reason = "tool_fault"
        return defs.CFL_DISABLE
    end

    return defs.CFL_CONTINUE
end

-- deliver_part: extend -> release -> retract  (drop a payload at assembly)
M.one_shot.WKR_DELIVER_PART_INIT = function(h)
    arm_cycle_init(h, { "extend", "release", "retract" }, "deliver_part")
end
M.main.WKR_DELIVER_PART_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    return arm_cycle_main(h)
end

-- paint_sample: extend -> hold -> retract
M.one_shot.WKR_PAINT_SAMPLE_INIT = function(h)
    arm_cycle_init(h, { "extend", "hold", "retract" }, "paint_sample")
end
M.main.WKR_PAINT_SAMPLE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    return arm_cycle_main(h)
end

-- load_shipping: extend -> grip -> retract  (pickup a payload at shipping)
M.one_shot.WKR_LOAD_SHIPPING_INIT = function(h)
    arm_cycle_init(h, { "extend", "grip", "retract" }, "load_shipping")
end
M.main.WKR_LOAD_SHIPPING_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    return arm_cycle_main(h)
end

-- =========================================================================
-- pass_gate: not implemented in first-pass sim (no gate infrastructure).
-- Simulate as a 1.5 sim-second dwell.
-- =========================================================================

M.one_shot.WKR_PASS_GATE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_active = true
    bb._gate_start = bb._hal:sim_time()
    io.stderr:write(string.format("  VN[pass_gate] cmd: %s\n", bb.command_json))
end
M.main.WKR_PASS_GATE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb._hal:sim_time() - (bb._gate_start or 0) >= 1.5 then
        bb["worker_pass_gate.bitmask"] = 0x0F
        bb.worker_success = true
        return defs.CFL_DISABLE
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- inspection_scan: short dwell
-- =========================================================================

M.one_shot.WKR_INSPECTION_SCAN_INIT = function(h)
    local bb = h.blackboard
    bb.exec_active = true
    bb._scan_start = bb._hal:sim_time()
    io.stderr:write(string.format("  VN[inspection_scan] cmd: %s\n", bb.command_json))
end
M.main.WKR_INSPECTION_SCAN_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb._hal:sim_time() - (bb._scan_start or 0) >= 1.0 then
        bb["worker_inspection_scan.bitmask"] = 0x01  -- reading_ready
        bb.worker_success = true
        return defs.CFL_DISABLE
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- recharge: dock + begin_charge until target_energy, then release
-- =========================================================================

M.one_shot.WKR_RECHARGE_INIT = function(h)
    local bb = h.blackboard
    local cmd = parse_cmd(bb)
    bb.exec_active = true

    local hal = bb._hal
    -- Verify we're at a charger
    local si = hal:station_at_pose("charger")
    if si < 0 then
        bb._recharge_fault = "not_at_charger"
    else
        -- target_j in joules; cmd.target_energy is the planner's request
        local target_j = cmd.target_energy or hal:read_tool_status(2).battery_capacity_j
        local rc = hal:begin_charge("charge_port", target_j)
        if rc < 0 then bb._recharge_fault = "charge_begin_failed:" .. tostring(rc) end
    end
    io.stderr:write(string.format("  VN[recharge] cmd: %s fault=%s\n",
        bb.command_json, tostring(bb._recharge_fault)))
end
M.main.WKR_RECHARGE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true

    if bb._recharge_fault then
        bb["worker_recharge.bitmask"] = 0x04  -- charger_fault
        bb.worker_success = false
        bb.fault_reason = bb._recharge_fault
        bb._recharge_fault = nil
        return defs.CFL_DISABLE
    end

    local hal = bb._hal
    local ts = hal:read_tool_status(2)
    if band(ts.flags, hal.TOOL_F.AT_TARGET) ~= 0 then
        bb["worker_recharge.bitmask"] = 0x02  -- charge_complete
        bb.worker_success = true
        return defs.CFL_DISABLE
    end
    if band(ts.flags, hal.TOOL_F.FAULT) ~= 0 then
        bb["worker_recharge.bitmask"] = 0x04
        bb.worker_success = false
        bb.fault_reason = "charger_fault"
        return defs.CFL_DISABLE
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- operation: generic, short dwell
-- =========================================================================

M.one_shot.WKR_OPERATION_INIT = function(h)
    local bb = h.blackboard
    bb.exec_active = true
    bb._op_start = bb._hal:sim_time()
    io.stderr:write(string.format("  VN[operation] cmd: %s\n", bb.command_json))
end
M.main.WKR_OPERATION_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb._hal:sim_time() - (bb._op_start or 0) >= 2.0 then
        bb["worker_operation.bitmask"] = 0x01  -- action_complete
        bb.worker_success = true
        return defs.CFL_DISABLE
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- idle: short park dwell
-- =========================================================================

M.one_shot.WKR_IDLE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_active = true
    bb._idle_start = bb._hal:sim_time()
    io.stderr:write(string.format("  VN[idle] cmd: %s\n", bb.command_json))
end
M.main.WKR_IDLE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb._hal:sim_time() - (bb._idle_start or 0) >= 0.5 then
        bb["worker_idle.bitmask"] = 0x01  -- parked
        bb.worker_success = true
        return defs.CFL_DISABLE
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
M.registry = { main = M.main, one_shot = M.one_shot, boolean = M.boolean }
return M
