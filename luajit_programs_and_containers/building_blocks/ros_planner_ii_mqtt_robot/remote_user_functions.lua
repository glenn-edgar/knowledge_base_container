--[[
    remote_user_functions.lua -- ChainTree worker functions for MQTT robot.

    Individual worker per virtual node — matches KB definitions.
    Each worker:
      1. Prints the command JSON on init (VN parameters visible)
      2. Sets bb.worker_alive = true every tick (liveness ping to controller)
      3. Simulates execution with a tick countdown
      4. Sets delta pose fields matching KB pose_fields

    Controller logic is in robot_controller.lua (not ChainTree).
]]

local defs        = require("ct_definitions")
local cmd_packets = require("command_packets")
local json_util   = require("json_util")

local M = {}

-- Simulated action durations (ticks)
local action_durations = {
    [cmd_packets.TYPE_INIT_CHECK]      = 15,
    [cmd_packets.TYPE_PATH_SPLINE]     = 25,
    [cmd_packets.TYPE_PATH_LINE]       = 25,
    [cmd_packets.TYPE_PATH_WALL]       = 25,
    [cmd_packets.TYPE_PATH_ROTATE]     = 15,
    [cmd_packets.TYPE_DELIVER_PART]    = 20,
    [cmd_packets.TYPE_PAINT_SAMPLE]    = 20,
    [cmd_packets.TYPE_LOAD_SHIPPING]   = 20,
    [cmd_packets.TYPE_PASS_GATE]       = 15,
    [cmd_packets.TYPE_INSPECTION_SCAN] = 12,
    [cmd_packets.TYPE_RECHARGE]        = 30,
    [cmd_packets.TYPE_OPERATION]       = 20,
    [cmd_packets.TYPE_IDLE]            = 5,
}

M.main = {}
M.one_shot = {}
M.boolean = {}

-- Watchdog max silence (must match robot_controller.lua)
local WATCHDOG_MAX_SILENCE = 50

-- =========================================================================
-- Common worker termination
-- =========================================================================

M.one_shot.WORKER_TERM = function(handle, node)
    local bb = handle.blackboard
    if not (bb.watchdog_silence and bb.watchdog_silence >= WATCHDOG_MAX_SILENCE) then
        bb.worker_done = true; bb.worker_success = true
    end
end

-- =========================================================================
-- Command parser helper (flattens params)
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

-- =========================================================================
-- init_check: preflight self-test
-- packet_type=1, no params
-- bitmask: battery_ok(0)|motors_ok(1)|sensors_ok(2)|comms_ok(3)
-- pose: (none)
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
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_INIT_CHECK]
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- path_spline: follow spline path between nodes
-- packet_type=2, params: from_x,from_y,to_x,to_y,speed,distance,segment_index,total_segments
-- bitmask: seg_complete(0)|obstacle(1)|motor_fault(2)
-- pose: delta_x, delta_y, delta_heading
-- =========================================================================

M.one_shot.WKR_PATH_SPLINE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_dx = 0; bb._target_dy = 0
    io.stderr:write(string.format("  VN[path_spline] cmd: %s\n", bb.command_json))
end

M.main.WKR_PATH_SPLINE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_SPLINE]
        local c = parse_cmd(bb)
        bb._target_dx = (c.to_x or 0) - (c.from_x or 0)
        bb._target_dy = (c.to_y or 0) - (c.from_y or 0)
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_PATH_SPLINE]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_x = (bb._target_dx or 0) * p
        bb.delta_y = (bb._target_dy or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_x = bb._target_dx or 0
            bb.delta_y = bb._target_dy or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- path_line: line follow between nodes
-- packet_type=3, params: from_x,from_y,to_x,to_y,speed,distance
-- bitmask: seg_complete(0)|obstacle(1)|motor_fault(2)
-- pose: delta_x, delta_y, delta_heading
-- =========================================================================

M.one_shot.WKR_PATH_LINE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_dx = 0; bb._target_dy = 0
    io.stderr:write(string.format("  VN[path_line] cmd: %s\n", bb.command_json))
end

M.main.WKR_PATH_LINE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_LINE]
        local c = parse_cmd(bb)
        bb._target_dx = (c.to_x or 0) - (c.from_x or 0)
        bb._target_dy = (c.to_y or 0) - (c.from_y or 0)
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_PATH_LINE]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_x = (bb._target_dx or 0) * p
        bb.delta_y = (bb._target_dy or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_x = bb._target_dx or 0
            bb.delta_y = bb._target_dy or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- path_wall: wall follow between nodes
-- packet_type=4, params: from_x,from_y,to_x,to_y,speed,distance,wall_standoff
-- bitmask: seg_complete(0)|obstacle(1)|motor_fault(2)|wall_lost(3)
-- pose: delta_x, delta_y, delta_heading
-- =========================================================================

M.one_shot.WKR_PATH_WALL_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_dx = 0; bb._target_dy = 0
    io.stderr:write(string.format("  VN[path_wall] cmd: %s\n", bb.command_json))
end

M.main.WKR_PATH_WALL_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_WALL]
        local c = parse_cmd(bb)
        bb._target_dx = (c.to_x or 0) - (c.from_x or 0)
        bb._target_dy = (c.to_y or 0) - (c.from_y or 0)
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_PATH_WALL]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_x = (bb._target_dx or 0) * p
        bb.delta_y = (bb._target_dy or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_x = bb._target_dx or 0
            bb.delta_y = bb._target_dy or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- path_rotate: rotate in place to heading
-- packet_type=5, params: from_heading,to_heading
-- bitmask: rotate_complete(0)|motor_fault(1)
-- pose: delta_heading
-- =========================================================================

M.one_shot.WKR_PATH_ROTATE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_dh = 0
    io.stderr:write(string.format("  VN[path_rotate] cmd: %s\n", bb.command_json))
end

M.main.WKR_PATH_ROTATE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_ROTATE]
        local c = parse_cmd(bb)
        bb._target_dh = (c.to_heading or 0) - (c.from_heading or 0)
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_PATH_ROTATE]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_heading = (bb._target_dh or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_heading = bb._target_dh or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- deliver_part: arm delivery at assembly station
-- packet_type=6, params: arm_target,arm_speed,arm_return,payload_type
-- bitmask: arm_at_target(0)|payload_gripped(1)|action_complete(2)|arm_fault(3)
-- pose: delta_arm_angle
-- =========================================================================

M.one_shot.WKR_DELIVER_PART_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_arm = 0
    io.stderr:write(string.format("  VN[deliver_part] cmd: %s\n", bb.command_json))
end

M.main.WKR_DELIVER_PART_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_DELIVER_PART]
        local c = parse_cmd(bb)
        bb._target_arm = c.arm_target or 0
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_DELIVER_PART]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_arm_angle = (bb._target_arm or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_arm_angle = bb._target_arm or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- paint_sample: paint operation at painting station
-- packet_type=7, params: arm_target,arm_speed,arm_return,hold_time
-- bitmask: arm_at_target(0)|action_complete(1)|arm_fault(2)
-- pose: delta_arm_angle
-- =========================================================================

M.one_shot.WKR_PAINT_SAMPLE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_arm = 0
    io.stderr:write(string.format("  VN[paint_sample] cmd: %s\n", bb.command_json))
end

M.main.WKR_PAINT_SAMPLE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_PAINT_SAMPLE]
        local c = parse_cmd(bb)
        bb._target_arm = c.arm_target or 0
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_PAINT_SAMPLE]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_arm_angle = (bb._target_arm or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_arm_angle = bb._target_arm or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- load_shipping: load container at shipping station
-- packet_type=8, params: arm_target,arm_speed,arm_return,payload_type
-- bitmask: arm_at_target(0)|payload_gripped(1)|action_complete(2)|arm_fault(3)
-- pose: delta_arm_angle
-- =========================================================================

M.one_shot.WKR_LOAD_SHIPPING_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    bb._target_arm = 0
    io.stderr:write(string.format("  VN[load_shipping] cmd: %s\n", bb.command_json))
end

M.main.WKR_LOAD_SHIPPING_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_LOAD_SHIPPING]
        local c = parse_cmd(bb)
        bb._target_arm = c.arm_target or 0
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        local total = action_durations[cmd_packets.TYPE_LOAD_SHIPPING]
        local p = 1.0 - (bb.ticks_remaining / total)
        bb.delta_arm_angle = (bb._target_arm or 0) * p
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb.delta_arm_angle = bb._target_arm or 0
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- pass_gate: open gate, drive through, close gate
-- packet_type=9, params: rpc_open_hash,rpc_close_hash,drive_through
-- bitmask: gate_opened(0)|drive_complete(1)|gate_closed(2)|action_complete(3)
-- pose: delta_x, delta_y, delta_heading
-- =========================================================================

M.one_shot.WKR_PASS_GATE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    io.stderr:write(string.format("  VN[pass_gate] cmd: %s\n", bb.command_json))
end

M.main.WKR_PASS_GATE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_PASS_GATE]
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- inspection_scan: sensor read at inspection point
-- packet_type=10, params: sensor_port,sensor_type
-- bitmask: reading_ready(0)|sensor_fault(1)
-- pose: (none)
-- =========================================================================

M.one_shot.WKR_INSPECTION_SCAN_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    io.stderr:write(string.format("  VN[inspection_scan] cmd: %s\n", bb.command_json))
end

M.main.WKR_INSPECTION_SCAN_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_INSPECTION_SCAN]
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- recharge: recharge energy at charging station
-- packet_type=12, params: target_energy
-- bitmask: charging(0)|charge_complete(1)|charger_fault(2)
-- pose: (none)
-- =========================================================================

M.one_shot.WKR_RECHARGE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    io.stderr:write(string.format("  VN[recharge] cmd: %s\n", bb.command_json))
end

M.main.WKR_RECHARGE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_RECHARGE]
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- operation: generic operation at a stop
-- packet_type=20, params: operation_type, data
-- bitmask: action_complete(0)|action_fault(1)
-- pose: (none)
-- =========================================================================

M.one_shot.WKR_OPERATION_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    local cmd = parse_cmd(bb)
    io.stderr:write(string.format("  VN[operation] type=%s cmd: %s\n",
        tostring(cmd.operation_type), bb.command_json))
end

M.main.WKR_OPERATION_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_OPERATION]
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
-- idle: park robot
-- packet_type=11, no params
-- bitmask: parked(0)
-- pose: (none)
-- =========================================================================

M.one_shot.WKR_IDLE_INIT = function(h)
    local bb = h.blackboard
    bb.exec_start = true; bb.exec_active = false
    io.stderr:write(string.format("  VN[idle] cmd: %s\n", bb.command_json))
end

M.main.WKR_IDLE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = action_durations[cmd_packets.TYPE_IDLE]
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            return defs.CFL_DISABLE
        end
    end
    return defs.CFL_CONTINUE
end

-- =========================================================================
M.registry = { main = M.main, one_shot = M.one_shot, boolean = M.boolean }
return M
