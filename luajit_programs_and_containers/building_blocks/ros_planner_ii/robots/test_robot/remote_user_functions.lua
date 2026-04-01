--[[
    remote_user_functions.lua -- ChainTree user functions for test_robot.

    Controller functions (CTRL_*) — always running, supervisory
    Worker functions (WKR_*) — one per virtual node, dormant until activated
]]

local defs        = require("ct_definitions")
local event_ids   = require("event_ids")
local cmd_packets = require("command_packets")
local json_util   = require("json_util")
local engine

local function get_engine()
    if not engine then engine = require("ct_engine") end
    return engine
end

local M = {}

local transport = nil
local kb_rt = nil
local global_pos = { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 }

function M.set_transport(tx) transport = tx end
function M.set_kb_runtime(rt) kb_rt = rt end

-- Worker KB name by packet_type (one per virtual node)
local worker_by_packet_type = {
    [cmd_packets.TYPE_INIT_CHECK]      = "worker_init_check",
    [cmd_packets.TYPE_PATH_SPLINE]     = "worker_path_spline",
    [cmd_packets.TYPE_PATH_LINE]       = "worker_path_line",
    [cmd_packets.TYPE_PATH_WALL]       = "worker_path_wall",
    [cmd_packets.TYPE_PATH_ROTATE]     = "worker_path_rotate",
    [cmd_packets.TYPE_DELIVER_PART]    = "worker_deliver_part",
    [cmd_packets.TYPE_PAINT_SAMPLE]    = "worker_paint_sample",
    [cmd_packets.TYPE_LOAD_SHIPPING]   = "worker_load_shipping",
    [cmd_packets.TYPE_PASS_GATE]       = "worker_pass_gate",
    [cmd_packets.TYPE_INSPECTION_SCAN] = "worker_inspection_scan",
    [cmd_packets.TYPE_IDLE]            = "worker_idle",
}

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
    [cmd_packets.TYPE_IDLE]            = 5,
}

local default_max_time = {
    [cmd_packets.TYPE_INIT_CHECK]      = 50,
    [cmd_packets.TYPE_PATH_SPLINE]     = 200,
    [cmd_packets.TYPE_PATH_LINE]       = 200,
    [cmd_packets.TYPE_PATH_WALL]       = 200,
    [cmd_packets.TYPE_PATH_ROTATE]     = 100,
    [cmd_packets.TYPE_DELIVER_PART]    = 150,
    [cmd_packets.TYPE_PAINT_SAMPLE]    = 150,
    [cmd_packets.TYPE_LOAD_SHIPPING]   = 150,
    [cmd_packets.TYPE_PASS_GATE]       = 100,
    [cmd_packets.TYPE_INSPECTION_SCAN] = 50,
    [cmd_packets.TYPE_IDLE]            = 20,
}

M.main = {}
M.one_shot = {}
M.boolean = {}

-- =========================================================================
-- Heartbeat helper
-- =========================================================================

local function send_heartbeat(bb, phase)
    local gx, gy, gz, gh, ga
    if phase == "final" then
        gx, gy, gz = global_pos.x, global_pos.y, global_pos.z
        gh, ga = global_pos.heading, global_pos.arm_angle
    else
        gx = global_pos.x + (bb.delta_x or 0)
        gy = global_pos.y + (bb.delta_y or 0)
        gz = global_pos.z + (bb.delta_z or 0)
        gh = global_pos.heading + (bb.delta_heading or 0)
        ga = global_pos.arm_angle + (bb.delta_arm_angle or 0)
    end
    local heartbeat = {
        type = "heartbeat", phase = phase, test_id = bb.current_test_id,
        delta_x = bb.delta_x, delta_y = bb.delta_y, delta_z = bb.delta_z,
        delta_heading = bb.delta_heading, delta_arm_angle = bb.delta_arm_angle,
        global_x = gx, global_y = gy, global_z = gz,
        global_heading = gh, global_arm_angle = ga,
        watchdog_ticks = bb.watchdog_ticks, worker = bb.active_worker,
    }
    if transport then transport:send_stream(json_util.encode(heartbeat)) end
    if kb_rt then pcall(kb_rt.write_heartbeat, kb_rt, heartbeat) end
end

-- =========================================================================
-- Worker activation helper
-- =========================================================================

local function activate_worker(handle, cmd)
    local bb = handle.blackboard
    local pkt_type = cmd.packet_type
    local worker_name = worker_by_packet_type[pkt_type]

    bb.current_packet_type = pkt_type
    bb.current_test_id = cmd.test_id or 0
    bb.current_seq = cmd.seq or 0
    bb.current_max_time = cmd.max_time or default_max_time[pkt_type] or 100
    bb.command_json = json_util.encode(cmd)
    bb.active_worker = worker_name
    bb.worker_done = false; bb.worker_success = false
    bb.delta_x = 0; bb.delta_y = 0; bb.delta_z = 0
    bb.delta_heading = 0; bb.delta_arm_angle = 0
    bb.watchdog_expired = false; bb.watchdog_ticks = 0
    bb.heartbeat_counter = 0; bb.fault_reason = ""

    local eng = get_engine()
    local kb = handle.kb_table[worker_name]
    if kb then
        for _, nid in ipairs(kb.node_ids) do
            local n = handle.nodes[nid]
            if n then n.ct_control.enabled = false; n.ct_control.initialized = false end
            handle.node_state[nid] = nil
        end
        eng.init_test(handle, worker_name)
        handle.active_tests[worker_name] = true
        handle.active_test_count = (handle.active_test_count or 0) + 1
    end

    if kb_rt then pcall(kb_rt.merge_status, kb_rt, { active_kb = worker_name, active_worker = worker_name }) end
    send_heartbeat(bb, "initial")
end

-- =========================================================================
-- CONTROLLER: Dispatch
-- =========================================================================

M.main.CTRL_DISPATCH_MAIN = function(handle, bool_fn, node, event_id, event_data)
    if event_id == event_ids.ROBOT_RPC_COMMAND and event_data then
        local bb = handle.blackboard
        local cmd = event_data
        local pkt_type = cmd.packet_type

        local worker_name = worker_by_packet_type[pkt_type]
        if not worker_name then
            if transport then
                transport:send_stream(json_util.encode({
                    type = "kb_done", test_id = cmd.test_id or 0,
                    success = false, fault_reason = "unknown_packet_type",
                }))
            end
            return defs.CFL_CONTINUE
        end

        if transport then
            transport:send_stream(json_util.encode({
                type = "ack", seq = cmd.seq or 0,
                test_id = cmd.test_id or 0, status = "ok",
            }))
        end

        if bb.active_worker ~= "" and not bb.worker_done then
            bb.lookahead_pending = true
            bb.lookahead_json = json_util.encode(cmd)
            return defs.CFL_CONTINUE
        end

        activate_worker(handle, cmd)
        return defs.CFL_CONTINUE
    end

    if event_id == event_ids.ROBOT_RPC_SHUTDOWN then
        if transport and event_data then
            transport:send_stream(json_util.encode({
                type = "ack", seq = event_data.seq or 0, status = "ok",
            }))
        end
        handle.blackboard.shutdown_requested = true
        return defs.CFL_CONTINUE
    end
    return defs.CFL_CONTINUE
end

M.one_shot.CTRL_DISPATCH_INIT = function(handle, node)
    handle.blackboard.controller_active = true
    if kb_rt then pcall(kb_rt.merge_status, kb_rt, { connected = true }) end
end

-- =========================================================================
-- CONTROLLER: Watchdog
-- =========================================================================

M.main.CTRL_WATCHDOG_MAIN = function(handle, bool_fn, node, event_id)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = handle.blackboard
    if bb.active_worker ~= "" and not bb.worker_done then
        bb.watchdog_ticks = bb.watchdog_ticks + 1
        if bb.watchdog_ticks >= bb.current_max_time then
            bb.watchdog_expired = true; bb.worker_done = true
            bb.worker_success = false; bb.fault_reason = "watchdog_timeout"
            local wn = bb.active_worker
            if handle.active_tests[wn] then
                handle.active_tests[wn] = nil
                handle.active_test_count = handle.active_test_count - 1
            end
        end
    end
    return defs.CFL_CONTINUE
end

M.one_shot.CTRL_WATCHDOG_INIT = function(handle, node) handle.blackboard.watchdog_ticks = 0 end

-- =========================================================================
-- CONTROLLER: Heartbeat
-- =========================================================================

M.main.CTRL_HEARTBEAT_MAIN = function(handle, bool_fn, node, event_id)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = handle.blackboard
    if bb.active_worker ~= "" and not bb.worker_done then
        bb.heartbeat_counter = bb.heartbeat_counter + 1
        if bb.heartbeat_counter >= bb.heartbeat_interval then
            bb.heartbeat_counter = 0
            send_heartbeat(bb, "periodic")
        end
    end
    return defs.CFL_CONTINUE
end

M.one_shot.CTRL_HEARTBEAT_INIT = function(handle, node) handle.blackboard.heartbeat_counter = 0 end

-- =========================================================================
-- CONTROLLER: Completion
-- =========================================================================

M.main.CTRL_COMPLETION_MAIN = function(handle, bool_fn, node, event_id)
    if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = handle.blackboard

    if bb.worker_done and bb.active_worker ~= "" then
        global_pos.x = global_pos.x + (bb.delta_x or 0)
        global_pos.y = global_pos.y + (bb.delta_y or 0)
        global_pos.z = global_pos.z + (bb.delta_z or 0)
        global_pos.heading = global_pos.heading + (bb.delta_heading or 0)
        global_pos.arm_angle = global_pos.arm_angle + (bb.delta_arm_angle or 0)

        send_heartbeat(bb, "final")

        if transport then
            transport:send_stream(json_util.encode({
                type = "kb_done", test_id = bb.current_test_id,
                success = bb.worker_success == true,
                delta_x = bb.delta_x, delta_y = bb.delta_y, delta_z = bb.delta_z,
                delta_heading = bb.delta_heading, delta_arm_angle = bb.delta_arm_angle,
                fault_reason = bb.fault_reason ~= "" and bb.fault_reason or nil,
            }))
        end

        if kb_rt then
            pcall(kb_rt.merge_status, kb_rt, {
                active_kb = "", active_worker = "",
                global_x = global_pos.x, global_y = global_pos.y, global_z = global_pos.z,
                global_heading = global_pos.heading, global_arm_angle = global_pos.arm_angle,
                last_success = bb.worker_success == true, last_test_id = bb.current_test_id,
                last_fault = bb.fault_reason ~= "" and bb.fault_reason or nil,
            })
        end

        local wn = bb.active_worker
        if handle.active_tests[wn] then
            handle.active_tests[wn] = nil
            handle.active_test_count = handle.active_test_count - 1
        end

        if bb.lookahead_pending == true then
            local ok, next_cmd = pcall(json_util.decode, bb.lookahead_json)
            bb.lookahead_pending = false; bb.lookahead_json = ""
            if ok and next_cmd then activate_worker(handle, next_cmd)
            else bb.active_worker = "" end
        else
            bb.active_worker = ""; bb.watchdog_ticks = 0; bb.heartbeat_counter = 0
        end
    end
    return defs.CFL_CONTINUE
end

M.one_shot.CTRL_COMPLETION_INIT = function(handle, node) end

-- =========================================================================
-- Common worker termination
-- =========================================================================

M.one_shot.WORKER_TERM = function(handle, node)
    local bb = handle.blackboard
    if not bb.watchdog_expired then
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
-- WORKERS: per virtual node
-- =========================================================================

-- === init_check ===
M.one_shot.WKR_INIT_CHECK_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false end
M.main.WKR_INIT_CHECK_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_INIT_CHECK] end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; if bb.ticks_remaining <= 0 then bb.exec_active = false; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === path_spline ===
M.one_shot.WKR_PATH_SPLINE_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_dx = 0; h.blackboard._target_dy = 0 end
M.main.WKR_PATH_SPLINE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_SPLINE]; local c = parse_cmd(bb); bb._target_dx = (c.to_x or 0) - (c.from_x or 0); bb._target_dy = (c.to_y or 0) - (c.from_y or 0) end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_PATH_SPLINE]); bb.delta_x = (bb._target_dx or 0) * p; bb.delta_y = (bb._target_dy or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_x = bb._target_dx or 0; bb.delta_y = bb._target_dy or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === path_line ===
M.one_shot.WKR_PATH_LINE_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_dx = 0; h.blackboard._target_dy = 0 end
M.main.WKR_PATH_LINE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_LINE]; local c = parse_cmd(bb); bb._target_dx = (c.to_x or 0) - (c.from_x or 0); bb._target_dy = (c.to_y or 0) - (c.from_y or 0) end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_PATH_LINE]); bb.delta_x = (bb._target_dx or 0) * p; bb.delta_y = (bb._target_dy or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_x = bb._target_dx or 0; bb.delta_y = bb._target_dy or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === path_wall ===
M.one_shot.WKR_PATH_WALL_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_dx = 0; h.blackboard._target_dy = 0 end
M.main.WKR_PATH_WALL_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_WALL]; local c = parse_cmd(bb); bb._target_dx = (c.to_x or 0) - (c.from_x or 0); bb._target_dy = (c.to_y or 0) - (c.from_y or 0) end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_PATH_WALL]); bb.delta_x = (bb._target_dx or 0) * p; bb.delta_y = (bb._target_dy or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_x = bb._target_dx or 0; bb.delta_y = bb._target_dy or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === path_rotate ===
M.one_shot.WKR_PATH_ROTATE_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_dh = 0 end
M.main.WKR_PATH_ROTATE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_PATH_ROTATE]; local c = parse_cmd(bb); bb._target_dh = (c.to_heading or 0) - (c.from_heading or 0) end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_PATH_ROTATE]); bb.delta_heading = (bb._target_dh or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_heading = bb._target_dh or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === deliver_part ===
M.one_shot.WKR_DELIVER_PART_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_arm = 0 end
M.main.WKR_DELIVER_PART_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_DELIVER_PART]; local c = parse_cmd(bb); bb._target_arm = c.arm_target or 0 end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_DELIVER_PART]); bb.delta_arm_angle = (bb._target_arm or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_arm_angle = bb._target_arm or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === paint_sample ===
M.one_shot.WKR_PAINT_SAMPLE_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_arm = 0 end
M.main.WKR_PAINT_SAMPLE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_PAINT_SAMPLE]; local c = parse_cmd(bb); bb._target_arm = c.arm_target or 0 end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_PAINT_SAMPLE]); bb.delta_arm_angle = (bb._target_arm or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_arm_angle = bb._target_arm or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === load_shipping ===
M.one_shot.WKR_LOAD_SHIPPING_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false; h.blackboard._target_arm = 0 end
M.main.WKR_LOAD_SHIPPING_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_LOAD_SHIPPING]; local c = parse_cmd(bb); bb._target_arm = c.arm_target or 0 end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; local p = 1.0 - (bb.ticks_remaining / action_durations[cmd_packets.TYPE_LOAD_SHIPPING]); bb.delta_arm_angle = (bb._target_arm or 0) * p; if bb.ticks_remaining <= 0 then bb.exec_active = false; bb.delta_arm_angle = bb._target_arm or 0; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === pass_gate ===
M.one_shot.WKR_PASS_GATE_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false end
M.main.WKR_PASS_GATE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_PASS_GATE] end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; if bb.ticks_remaining <= 0 then bb.exec_active = false; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === inspection_scan ===
M.one_shot.WKR_INSPECTION_SCAN_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false end
M.main.WKR_INSPECTION_SCAN_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_INSPECTION_SCAN] end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; if bb.ticks_remaining <= 0 then bb.exec_active = false; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- === idle ===
M.one_shot.WKR_IDLE_INIT = function(h) h.blackboard.exec_start = true; h.blackboard.exec_active = false end
M.main.WKR_IDLE_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    if bb.exec_start then bb.exec_start = false; bb.exec_active = true; bb.ticks_remaining = action_durations[cmd_packets.TYPE_IDLE] end
    if bb.exec_active then bb.ticks_remaining = bb.ticks_remaining - 1; if bb.ticks_remaining <= 0 then bb.exec_active = false; return defs.CFL_DISABLE end end
    return defs.CFL_CONTINUE
end

-- =========================================================================
M.registry = { main = M.main, one_shot = M.one_shot, boolean = M.boolean }
return M
