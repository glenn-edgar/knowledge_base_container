--[[
    robot_controller.lua -- Continuous-motion dispatcher.

    Contract:
      - Path packets (line / spline / rotate) are pushed to the C segment
        queue as soon as they arrive, up to the next non-motion boundary.
        This keeps the robot moving smoothly across waypoints.
      - Non-motion packets sit in a Lua worker_queue. When the head of the
        worker_queue is a non-motion packet, we wait for the C queue to
        drain AND the robot to be fully stopped before activating the
        worker. After it finishes, deferred path packets get pushed.
      - One active worker at a time, tracked in blackboard.active_worker.
      - Watchdog, heartbeat, completion (kb_done + energy) unchanged from
        the original design.
]]

local json_util   = require("json_util")
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
    [cmd_packets.TYPE_OPERATION]       = "worker_operation",
}

local path_packet_set = {
    [cmd_packets.TYPE_PATH_LINE]   = true,
    [cmd_packets.TYPE_PATH_SPLINE] = true,
    [cmd_packets.TYPE_PATH_ROTATE] = true,
}

-- Watchdog + heartbeat (sim-clock ticks at 10 Hz == 0.1 s, both wall and sim)
local WATCHDOG_MAX_SILENCE = 50
local HEARTBEAT_INTERVAL   = 10

function M.new(opts)
    assert(opts.handle,    "robot_controller: handle required")
    assert(opts.transport, "robot_controller: transport required")
    assert(opts.hal,       "robot_controller: hal required")

    local ctrl = setmetatable({
        handle       = opts.handle,
        transport    = opts.transport,
        kb_rt        = opts.kb_rt,
        hal          = opts.hal,
        worker_queue = {},
        seg_id_to_idx = {},   -- for cross-ref if needed
        global_pos   = { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 },
        last_heading_for_spline = 0,
        energy       = {
            max       = opts.energy_max or 10000,
            remaining = opts.energy_max or 10000,
            infinite  = opts.energy_infinite or false,
        },
        _blocked_by_non_motion = false,
        _energy_at_worker_start = 0,
    }, M)

    local bb = opts.handle.blackboard
    bb.controller_active = true
    bb.watchdog_silence = 0
    bb.worker_alive = false
    bb._hal = opts.hal   -- workers reach hal via blackboard

    if ctrl.kb_rt then pcall(ctrl.kb_rt.merge_status, ctrl.kb_rt, { connected = true }) end
    return ctrl
end

function M:get_energy() return self.energy end
function M:set_energy_remaining(v) self.energy.remaining = v end

-- =========================================================================
-- Helpers: push a single path packet to the C queue.
-- =========================================================================

function M:_push_segment_to_c(cmd)
    local hal = self.hal
    local pkt = cmd.packet_type
    local p = cmd.params or cmd

    if pkt == cmd_packets.TYPE_PATH_LINE then
        local from_h = p.from_heading or self.last_heading_for_spline or 0
        local to_h   = p.to_heading   or math.atan2((p.to_y or 0) - (p.from_y or 0),
                                                    (p.to_x or 0) - (p.from_x or 0))
        self.last_heading_for_spline = to_h
        return hal:push_line(p.from_x or 0, p.from_y or 0,
                             p.to_x or 0,   p.to_y   or 0,
                             from_h, to_h, p.speed or 0.5)
    elseif pkt == cmd_packets.TYPE_PATH_SPLINE then
        local from_h = p.from_heading or self.last_heading_for_spline or 0
        local to_h   = p.to_heading   or math.atan2((p.to_y or 0) - (p.from_y or 0),
                                                    (p.to_x or 0) - (p.from_x or 0))
        self.last_heading_for_spline = to_h
        return hal:push_spline(p.from_x or 0, p.from_y or 0,
                               p.to_x or 0,   p.to_y   or 0,
                               from_h, to_h, p.speed or 0.5)
    elseif pkt == cmd_packets.TYPE_PATH_ROTATE then
        local from_h = p.from_heading or 0
        local to_h   = p.to_heading   or 0
        self.last_heading_for_spline = to_h
        return hal:push_rotate(from_h, to_h, p.rate or 1.0)
    end
    return 0
end

-- =========================================================================
-- Heartbeat + completion (mostly unchanged from the pre-sim design)
-- =========================================================================

function M:_send_heartbeat(phase)
    local bb = self.handle.blackboard
    local pose = self.hal:read_pose()
    local hb = {
        type = "heartbeat", phase = phase, test_id = bb.current_test_id,
        delta_x = bb.delta_x, delta_y = bb.delta_y, delta_z = bb.delta_z,
        delta_heading   = bb.delta_heading,
        delta_arm_angle = bb.delta_arm_angle,
        global_x = pose.x, global_y = pose.y, global_z = 0,
        global_heading   = pose.heading,
        global_arm_angle = self.global_pos.arm_angle + (bb.delta_arm_angle or 0),
        watchdog_ticks   = bb.watchdog_ticks,
        worker           = bb.active_worker,
        sim_t            = pose.sim_t,
    }
    self.transport:send_stream(json_util.encode(hb))
    if self.kb_rt then pcall(self.kb_rt.write_heartbeat, self.kb_rt, hb) end
end

function M:_activate_worker(cmd, seg_id)
    local handle = self.handle
    local bb = handle.blackboard
    local pkt_type = cmd.packet_type
    local worker_name = worker_by_packet_type[pkt_type]
    if not worker_name then return false end

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

    bb._seg_id = seg_id
    bb._seg_start = self.hal:read_pose_truth()   -- use truth for delta accounting
    self._energy_at_worker_start = self.hal:read_path_status().energy_used_total

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

    if self.kb_rt then
        pcall(self.kb_rt.merge_status, self.kb_rt,
            { active_kb = worker_name, active_worker = worker_name })
    end
    self:_send_heartbeat("initial")
    return true
end

-- =========================================================================
-- drain_commands: read RPC packets, route to C queue / worker_queue.
-- =========================================================================

function M:drain_commands()
    local bb = self.handle.blackboard
    local max_per_tick = 10

    for _ = 1, max_per_tick do
        local payload_str = self.transport:recv_rpc()
        if not payload_str then break end

        local ok, cmd = pcall(json_util.decode, payload_str)
        if not (ok and cmd) then
            -- ignore malformed
        else
            local pkt = cmd.packet_type
            if not pkt then
                -- ignore
            elseif pkt == 255 then
                -- shutdown
                self.transport:send_stream(json_util.encode({
                    type = "ack", seq = cmd.seq or 0, status = "ok" }))
                bb.shutdown_requested = true
                return
            elseif worker_by_packet_type[pkt] then
                self.transport:send_stream(json_util.encode({
                    type = "ack", seq = cmd.seq or 0,
                    test_id = cmd.test_id or 0, status = "ok" }))

                local handle = { cmd = cmd, seg_id = nil, pushed = false }
                if path_packet_set[pkt] then
                    if not self._blocked_by_non_motion then
                        handle.seg_id = self:_push_segment_to_c(cmd)
                        handle.pushed = true
                    end
                    handle.kind = "path"
                else
                    handle.kind = "non_motion"
                    self._blocked_by_non_motion = true
                end
                table.insert(self.worker_queue, handle)
            else
                self.transport:send_stream(json_util.encode({
                    type = "kb_done", test_id = cmd.test_id or 0,
                    success = false, fault_reason = "unknown_packet_type" }))
            end
        end
    end
end

-- =========================================================================
-- Activate the next worker when conditions are met.
-- =========================================================================

function M:_advance_worker_queue()
    local bb = self.handle.blackboard
    if bb.active_worker and bb.active_worker ~= "" then return end  -- busy
    local head = self.worker_queue[1]
    if not head then return end

    if head.kind == "non_motion" then
        if self.hal:queue_depth() == 0 and self.hal:is_stopped() then
            table.remove(self.worker_queue, 1)
            self:_activate_worker(head.cmd, nil)
        end
    else
        -- Path worker. If not yet in C queue, push now (unblock case).
        if not head.pushed then
            head.seg_id = self:_push_segment_to_c(head.cmd)
            head.pushed = true
        end
        table.remove(self.worker_queue, 1)
        self:_activate_worker(head.cmd, head.seg_id)
    end
end

-- When a non-motion worker completes, push any deferred path packets to C
-- up to the next non-motion boundary.
function M:_after_non_motion_done()
    self._blocked_by_non_motion = false
    for _, h in ipairs(self.worker_queue) do
        if h.kind == "non_motion" then
            self._blocked_by_non_motion = true
            break
        end
        if not h.pushed then
            h.seg_id = self:_push_segment_to_c(h.cmd)
            h.pushed = true
        end
    end
    self.hal:release_stop()
end

-- =========================================================================
-- timer_tick: watchdog, heartbeat, completion, then queue advance
-- =========================================================================

function M:timer_tick()
    local handle = self.handle
    local bb = handle.blackboard

    if bb.active_worker ~= nil and bb.active_worker ~= "" and not bb.worker_done then
        -- Watchdog
        bb.watchdog_ticks = (bb.watchdog_ticks or 0) + 1
        if bb.worker_alive then
            bb.worker_alive = false
            bb.watchdog_silence = 0
        else
            bb.watchdog_silence = (bb.watchdog_silence or 0) + 1
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

        -- Heartbeat
        bb.heartbeat_counter = (bb.heartbeat_counter or 0) + 1
        if bb.heartbeat_counter >= HEARTBEAT_INTERVAL then
            bb.heartbeat_counter = 0
            self:_send_heartbeat("periodic")
        end
    end

    -- Completion
    if bb.worker_done and bb.active_worker and bb.active_worker ~= "" then
        local was_non_motion = bb.current_packet_type and
            not path_packet_set[bb.current_packet_type]

        -- Freeze pose at completion
        local pose = self.hal:read_pose_truth()
        local gp = self.global_pos
        gp.x = pose.x; gp.y = pose.y; gp.heading = pose.heading
        gp.arm_angle = gp.arm_angle + (bb.delta_arm_angle or 0)

        -- Energy deduction: measured from C since worker activation
        local st = self.hal:read_path_status()
        local measured = st.energy_used_total - (self._energy_at_worker_start or 0)
        if not self.energy.infinite then
            self.energy.remaining = math.max(0, self.energy.remaining - measured)
            if bb.current_packet_type == cmd_packets.TYPE_RECHARGE then
                self.energy.remaining = math.floor(self.hal:read_tool_status(2).battery_j)
            end
        end

        self:_send_heartbeat("final")

        self.transport:send_stream(json_util.encode({
            type = "kb_done", test_id = bb.current_test_id,
            success = bb.worker_success == true,
            delta_x = bb.delta_x, delta_y = bb.delta_y, delta_z = bb.delta_z,
            delta_heading   = bb.delta_heading,
            delta_arm_angle = bb.delta_arm_angle,
            fault_reason    = bb.fault_reason ~= "" and bb.fault_reason or nil,
            energy_remaining = self.energy.remaining,
            energy_max       = self.energy.max,
            energy_measured  = measured,
            sim_t            = pose.sim_t,
        }))

        if self.kb_rt then
            pcall(self.kb_rt.merge_status, self.kb_rt, {
                active_kb = "", active_worker = "",
                global_x = gp.x, global_y = gp.y, global_z = 0,
                global_heading = gp.heading, global_arm_angle = gp.arm_angle,
                last_success = bb.worker_success == true,
                last_test_id = bb.current_test_id,
                last_fault   = bb.fault_reason ~= "" and bb.fault_reason or nil,
                energy_remaining = self.energy.remaining,
                energy_max       = self.energy.max,
            })
        end

        local wn = bb.active_worker
        if handle.active_tests[wn] then
            handle.active_tests[wn] = nil
            handle.active_test_count = handle.active_test_count - 1
        end
        bb.active_worker = ""
        bb.watchdog_ticks = 0; bb.heartbeat_counter = 0

        if was_non_motion then
            self:_after_non_motion_done()
        end
    end

    -- Activate next worker if free
    self:_advance_worker_queue()
end

-- =========================================================================
-- tick: called from main loop at 10 Hz (post-physics-step)
-- =========================================================================

function M:tick()
    self:drain_commands()
    self:timer_tick()
end

-- =========================================================================
-- Manual abort/reset (for test harness)
-- =========================================================================

function M:abort_all()
    self.worker_queue = {}
    self._blocked_by_non_motion = false
    self.hal:abort_path()
end

return M
