--[[
    robot_controller.lua -- Robot-independent controller.

    Handles dispatch, watchdog (liveness), heartbeat, and completion.
    Called directly from the tick loop — no ChainTree dependency.

    Workers must set bb.worker_alive = true each tick as a liveness ping.

    Usage:
        local robot_controller = require("robot_controller")
        local ctrl = robot_controller.new({
            handle     = remote_handle,
            transport  = tx,
            kb_rt      = kb_rt,          -- optional
            energy_max = 10000,
            energy_infinite = false,
        })

        -- In tick loop:
        ctrl:tick()
]]

local json_util   = require("json_util")
local event_ids   = require("event_ids")
local cmd_packets = require("command_packets")
local defs        = require("ct_definitions")
local engine

local function get_engine()
    if not engine then engine = require("ct_engine") end
    return engine
end

local M = {}
M.__index = M

-- Worker KB name by packet_type
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
    [cmd_packets.TYPE_RECHARGE]        = "worker_recharge",
}

-- Simulated energy cost per action type (real robot would measure actual consumption)
local energy_costs = {
    [cmd_packets.TYPE_INIT_CHECK]      = 50,
    [cmd_packets.TYPE_PATH_SPLINE]     = 200,
    [cmd_packets.TYPE_PATH_LINE]       = 200,
    [cmd_packets.TYPE_PATH_WALL]       = 200,
    [cmd_packets.TYPE_PATH_ROTATE]     = 50,
    [cmd_packets.TYPE_DELIVER_PART]    = 100,
    [cmd_packets.TYPE_PAINT_SAMPLE]    = 100,
    [cmd_packets.TYPE_LOAD_SHIPPING]   = 100,
    [cmd_packets.TYPE_PASS_GATE]       = 80,
    [cmd_packets.TYPE_INSPECTION_SCAN] = 30,
    [cmd_packets.TYPE_RECHARGE]        = 0,
    [cmd_packets.TYPE_IDLE]            = 10,
}

-- Watchdog and heartbeat constants
local WATCHDOG_MAX_SILENCE = 50
local HEARTBEAT_INTERVAL   = 10

function M.new(opts)
    assert(opts.handle, "robot_controller: handle required")
    assert(opts.transport, "robot_controller: transport required")

    local ctrl = setmetatable({
        handle     = opts.handle,
        transport  = opts.transport,
        kb_rt      = opts.kb_rt,
        global_pos = { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 },
        energy     = {
            max       = opts.energy_max or 10000,
            remaining = opts.energy_max or 10000,
            infinite  = opts.energy_infinite or false,
        },
    }, M)

    -- Init controller state on blackboard
    local bb = opts.handle.blackboard
    bb.controller_active = true
    bb.watchdog_silence = 0
    bb.worker_alive = false
    if ctrl.kb_rt then pcall(ctrl.kb_rt.merge_status, ctrl.kb_rt, { connected = true }) end

    return ctrl
end

function M:get_energy() return self.energy end
function M:set_energy_remaining(val) self.energy.remaining = val end

-- =========================================================================
-- Heartbeat helper
-- =========================================================================

function M:send_heartbeat(phase)
    local bb = self.handle.blackboard
    local gp = self.global_pos
    local gx, gy, gz, gh, ga
    if phase == "final" then
        gx, gy, gz = gp.x, gp.y, gp.z
        gh, ga = gp.heading, gp.arm_angle
    else
        gx = gp.x + (bb.delta_x or 0)
        gy = gp.y + (bb.delta_y or 0)
        gz = gp.z + (bb.delta_z or 0)
        gh = gp.heading + (bb.delta_heading or 0)
        ga = gp.arm_angle + (bb.delta_arm_angle or 0)
    end
    local heartbeat = {
        type = "heartbeat", phase = phase, test_id = bb.current_test_id,
        delta_x = bb.delta_x, delta_y = bb.delta_y, delta_z = bb.delta_z,
        delta_heading = bb.delta_heading, delta_arm_angle = bb.delta_arm_angle,
        global_x = gx, global_y = gy, global_z = gz,
        global_heading = gh, global_arm_angle = ga,
        watchdog_ticks = bb.watchdog_ticks, worker = bb.active_worker,
    }
    self.transport:send_stream(json_util.encode(heartbeat))
    if self.kb_rt then pcall(self.kb_rt.write_heartbeat, self.kb_rt, heartbeat) end
end

-- =========================================================================
-- Worker activation
-- =========================================================================

function M:activate_worker(cmd)
    local handle = self.handle
    local bb = handle.blackboard
    local pkt_type = cmd.packet_type
    local worker_name = worker_by_packet_type[pkt_type]

    bb.current_packet_type = pkt_type
    bb.current_test_id = cmd.test_id or 0
    bb.current_seq = cmd.seq or 0
    bb.command_json = json_util.encode(cmd)
    bb.active_worker = worker_name
    bb.worker_done = false; bb.worker_success = false
    bb.delta_x = 0; bb.delta_y = 0; bb.delta_z = 0
    bb.delta_heading = 0; bb.delta_arm_angle = 0
    bb.watchdog_ticks = 0; bb.watchdog_silence = 0
    bb.worker_alive = false
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

    if self.kb_rt then pcall(self.kb_rt.merge_status, self.kb_rt, { active_kb = worker_name, active_worker = worker_name }) end
    self:send_heartbeat("initial")
end

-- =========================================================================
-- Dispatch: drain RPC commands from transport
-- =========================================================================

function M:drain_commands()
    local bb = self.handle.blackboard
    local max_per_tick = 10

    for _ = 1, max_per_tick do
        local payload_str = self.transport:recv_rpc()
        if not payload_str then break end

        local ok, cmd = pcall(json_util.decode, payload_str)
        if not ok or not cmd then break end

        local pkt_type = cmd.packet_type
        if not pkt_type then break end

        -- Shutdown command
        if pkt_type == 255 then
            self.transport:send_stream(json_util.encode({
                type = "ack", seq = cmd.seq or 0, status = "ok",
            }))
            bb.shutdown_requested = true
            return
        end

        -- Normal command
        local worker_name = worker_by_packet_type[pkt_type]
        if not worker_name then
            self.transport:send_stream(json_util.encode({
                type = "kb_done", test_id = cmd.test_id or 0,
                success = false, fault_reason = "unknown_packet_type",
            }))
        else
            self.transport:send_stream(json_util.encode({
                type = "ack", seq = cmd.seq or 0,
                test_id = cmd.test_id or 0, status = "ok",
            }))

            if bb.active_worker ~= "" and not bb.worker_done then
                bb.lookahead_pending = true
                bb.lookahead_json = json_util.encode(cmd)
            else
                self:activate_worker(cmd)
            end
        end
    end
end

-- =========================================================================
-- Watchdog + Heartbeat + Completion (timer-driven)
-- =========================================================================

function M:timer_tick()
    local handle = self.handle
    local bb = handle.blackboard

    if bb.active_worker ~= "" and not bb.worker_done then
        -- Watchdog: liveness check
        bb.watchdog_ticks = bb.watchdog_ticks + 1
        if bb.worker_alive then
            bb.worker_alive = false
            bb.watchdog_silence = 0
        else
            bb.watchdog_silence = bb.watchdog_silence + 1
            if bb.watchdog_silence >= WATCHDOG_MAX_SILENCE then
                bb.worker_done = true
                bb.worker_success = false
                bb.fault_reason = "watchdog_timeout"
                local wn = bb.active_worker
                if handle.active_tests[wn] then
                    handle.active_tests[wn] = nil
                    handle.active_test_count = handle.active_test_count - 1
                end
            end
        end

        -- Heartbeat: periodic to planner
        bb.heartbeat_counter = bb.heartbeat_counter + 1
        if bb.heartbeat_counter >= HEARTBEAT_INTERVAL then
            bb.heartbeat_counter = 0
            self:send_heartbeat("periodic")
        end
    end

    -- Completion: detect worker done, apply pose, send KB_DONE
    if bb.worker_done and bb.active_worker ~= "" then
        local gp = self.global_pos
        gp.x = gp.x + (bb.delta_x or 0)
        gp.y = gp.y + (bb.delta_y or 0)
        gp.z = gp.z + (bb.delta_z or 0)
        gp.heading = gp.heading + (bb.delta_heading or 0)
        gp.arm_angle = gp.arm_angle + (bb.delta_arm_angle or 0)

        -- Deduct energy
        local pkt_type = bb.current_packet_type
        if not self.energy.infinite then
            local cost = energy_costs[pkt_type] or 0
            self.energy.remaining = math.max(0, self.energy.remaining - cost)
            if pkt_type == cmd_packets.TYPE_RECHARGE then
                self.energy.remaining = self.energy.max
            end
        end

        self:send_heartbeat("final")

        self.transport:send_stream(json_util.encode({
            type = "kb_done", test_id = bb.current_test_id,
            success = bb.worker_success == true,
            delta_x = bb.delta_x, delta_y = bb.delta_y, delta_z = bb.delta_z,
            delta_heading = bb.delta_heading, delta_arm_angle = bb.delta_arm_angle,
            fault_reason = bb.fault_reason ~= "" and bb.fault_reason or nil,
            energy_remaining = self.energy.remaining,
            energy_max       = self.energy.max,
        }))

        if self.kb_rt then
            pcall(self.kb_rt.merge_status, self.kb_rt, {
                active_kb = "", active_worker = "",
                global_x = gp.x, global_y = gp.y, global_z = gp.z,
                global_heading = gp.heading, global_arm_angle = gp.arm_angle,
                last_success = bb.worker_success == true, last_test_id = bb.current_test_id,
                last_fault = bb.fault_reason ~= "" and bb.fault_reason or nil,
                energy_remaining = self.energy.remaining, energy_max = self.energy.max,
            })
        end

        local wn = bb.active_worker
        if handle.active_tests[wn] then
            handle.active_tests[wn] = nil
            handle.active_test_count = handle.active_test_count - 1
        end

        if bb.lookahead_pending == true then
            local ok2, next_cmd = pcall(json_util.decode, bb.lookahead_json)
            bb.lookahead_pending = false; bb.lookahead_json = ""
            if ok2 and next_cmd then self:activate_worker(next_cmd)
            else bb.active_worker = "" end
        else
            bb.active_worker = ""; bb.watchdog_ticks = 0; bb.heartbeat_counter = 0
        end
    end
end

-- =========================================================================
-- Main tick: drain commands + timer logic
-- =========================================================================

function M:tick()
    self:drain_commands()
    self:timer_tick()
end

return M
