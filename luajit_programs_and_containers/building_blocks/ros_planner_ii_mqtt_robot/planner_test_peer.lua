-- planner_test_peer.lua -- Reusable mission-side peer for rover e2e tests.
--
-- Subsumes the boilerplate that lived in test_mock_planner.lua (handshake
-- + heartbeat keep-alive) and test_random_paths.lua (command dispatch +
-- kb_done collection). Each new mixed_* / paths_only / fault scenario
-- is now ~30 lines on top of this peer.
--
-- The peer can serve BOTH roles for self-contained tests: bring the
-- rover live + drive commands + collect dones, all from one process.
-- mock_planner-style "just keep heartbeats flowing for a while" is
-- :pump_until(deadline_fn) without any send_command calls.
--
-- Usage:
--   local peer = require("planner_test_peer").new{
--       robot = "rover_1", site = "moonbase.alpha.surface_ops",
--       host = "localhost", port = 1883,
--   }
--   peer:bring_robot_live(15)               -- wait for announce + ack + confirm
--   peer:send_command{ packet_type=T.PATH_LINE, params={...} }
--   peer:send_command{ packet_type=T.LOAD_SHIPPING, params={...} }
--   local stats = peer:wait_for_dones(2, 60)
--   peer:close()

local json_util = require("json_util")
local pubsub    = require("lib.mqtt_pubsub")

local M = {}
local peer_mt = {}
peer_mt.__index = peer_mt

local function topic_paths(site, robot)
    local site_path = site:gsub("%.", "/")
    return {
        rpc          = site_path .. "/robots/" .. robot .. "/rpc",
        stream       = site_path .. "/robots/" .. robot .. "/stream_bus",
        link_in      = site_path .. "/robots/" .. robot .. "/link",
        planner_pref = site_path .. "/robots/" .. robot .. "/planner/",
    }
end

local function iso_now() return os.date("!%Y-%m-%dT%H:%M:%SZ") end

function M.new(opts)
    opts = opts or {}
    local robot = opts.robot or "rover_1"
    local site  = opts.site  or "moonbase.alpha.surface_ops"
    local host  = opts.host  or "localhost"
    local port  = opts.port  or 1883
    local client_id = opts.client_id or
        ("planner_test_peer_" .. robot .. "_" .. tostring(math.random(1, 1e6)))

    local topics = topic_paths(site, robot)
    local ps = pubsub.PubSub.new(host, port, client_id)
    ps:connect(5000)
    ps:subscribe(topics.link_in, 1)
    ps:subscribe(topics.stream,  1)

    return setmetatable({
        robot          = robot,
        site           = site,
        topics         = topics,
        ps             = ps,
        verbose        = opts.verbose or false,
        log            = opts.log or function(s) io.stderr:write(s) end,

        -- handshake state
        ack_seq        = 0,
        hb_seq         = 0,
        live           = false,
        capabilities_seen = false,

        -- heartbeat pacing (1 Hz, wall clock)
        last_hb_wall   = 0,

        -- collected events
        dones          = {},  -- list of { test_id, success, dx, dy, dh,
                              --           energy_remaining, fault_reason }
        ack_count      = 0,
        hb_count       = 0,   -- robot->planner heartbeats (telemetry)

        -- callbacks (optional)
        on_done        = opts.on_done,         -- fn(done_event) -> nil
        on_telemetry   = opts.on_telemetry,    -- fn(hb_event) -> nil
    }, peer_mt)
end

-- ----- private -----

function peer_mt:_send_link_ack()
    self.ack_seq = self.ack_seq + 1
    local p = json_util.encode({
        type      = "link_bridge_ack",
        robot_id  = self.robot,
        bridge_id = "planner",
        ack_seq   = self.ack_seq,
        seq       = self.hb_seq,
        ts        = iso_now(),
    })
    self.ps:publish(self.topics.planner_pref .. "ack", p, 1, false)
    if self.verbose then
        self.log(string.format("  SENT link_bridge_ack seq=%d\n", self.ack_seq))
    end
end

function peer_mt:_send_link_hb()
    self.hb_seq = self.hb_seq + 1
    local p = json_util.encode({
        type      = "link_bridge_heartbeat",
        robot_id  = self.robot,
        bridge_id = "planner",
        seq       = self.hb_seq,
        ts        = iso_now(),
    })
    self.ps:publish(self.topics.planner_pref .. "hb", p, 1, false)
end

function peer_mt:_handle_link(ev)
    if ev.type == "link_announce" then
        if self.verbose then
            self.log(string.format("  RECV link_announce seq=%s energy=%s\n",
                tostring(ev.seq), tostring(ev.energy_remaining)))
        end
        self:_send_link_ack()
        self.live = true
    elseif ev.type == "link_confirm" then
        self.capabilities_seen = true
        if self.verbose then
            self.log(string.format("  RECV link_confirm (%d capabilities)\n",
                ev.capabilities and #ev.capabilities or 0))
        end
    end
end

function peer_mt:_handle_stream(ev)
    if ev.type == "kb_done" then
        local done = {
            test_id          = ev.test_id,
            success          = ev.success,
            delta_x          = ev.delta_x,
            delta_y          = ev.delta_y,
            delta_heading    = ev.delta_heading,
            energy_remaining = ev.energy_remaining,
            fault_reason     = ev.fault_reason,
        }
        self.dones[#self.dones + 1] = done
        if self.on_done then self.on_done(done) end
        if self.verbose or not self.on_done then
            self.log(string.format(
                "  DONE test=%s success=%s dx=%s dy=%s dh=%s energy=%s%s\n",
                tostring(ev.test_id), tostring(ev.success),
                tostring(ev.delta_x), tostring(ev.delta_y),
                tostring(ev.delta_heading),
                tostring(ev.energy_remaining),
                ev.fault_reason and (" fault=" .. ev.fault_reason) or ""))
        end
    elseif ev.type == "ack" then
        self.ack_count = self.ack_count + 1
    elseif ev.type == "heartbeat" then
        self.hb_count = self.hb_count + 1
        if self.on_telemetry then self.on_telemetry(ev) end
    end
end

-- Drain any pending mqtt messages once. Returns the number of messages
-- handled. Call repeatedly inside :pump_until / :wait_for_dones loops.
function peer_mt:tick(poll_ms)
    local msgs = self.ps:poll(poll_ms or 100)
    for _, m in ipairs(msgs) do
        local ok, ev = pcall(json_util.decode, m.payload)
        if ok and ev then
            if m.topic == self.topics.link_in then
                self:_handle_link(ev)
            elseif m.topic == self.topics.stream then
                self:_handle_stream(ev)
            end
        end
    end
    -- Maintain link-bridge heartbeat (1 Hz, wall clock).
    if self.live then
        local now = os.time()
        if now - self.last_hb_wall >= 1 then
            self:_send_link_hb()
            self.last_hb_wall = now
        end
    end
    return #msgs
end

-- ----- public -----

-- Wait for the robot's link_announce + send ack + see link_confirm.
-- Returns true on success; nil, errstring on timeout.
function peer_mt:bring_robot_live(timeout_s)
    local deadline = os.time() + (timeout_s or 30)
    while os.time() < deadline do
        self:tick(100)
        if self.live and self.capabilities_seen then return true end
    end
    return nil, string.format(
        "bring_robot_live: timeout (live=%s confirm=%s)",
        tostring(self.live), tostring(self.capabilities_seen))
end

-- Pump events + heartbeats until predicate(self) returns truthy or
-- deadline. Returns whatever the predicate returned (nil on timeout).
function peer_mt:pump_until(predicate, timeout_s)
    local deadline = os.time() + (timeout_s or 60)
    while os.time() < deadline do
        self:tick(200)
        local r = predicate(self)
        if r then return r end
    end
    return nil
end

-- Pump heartbeats for `duration` seconds with no command traffic.
-- Subsumes the "long-running keep-alive" role of test_mock_planner.lua.
function peer_mt:pump_for(duration_s)
    local deadline = os.time() + (duration_s or 30)
    while os.time() < deadline do self:tick(200) end
end

-- Publish a command packet to the rover's rpc topic. The packet is a
-- plain table that encodes to JSON; matches the shape test_random_paths
-- uses (packet_type, seq, test_id, params, energy).
function peer_mt:send_command(cmd)
    self.ps:publish(self.topics.rpc, json_util.encode(cmd), 1, false)
    if self.verbose then
        self.log("  -> " .. json_util.encode(cmd) .. "\n")
    end
end

-- Convenience: assign sequential seq + test_id, send each. Returns the
-- list of test_ids in order so callers can pass them to wait_for_dones.
function peer_mt:send_batch(cmds, opts)
    opts = opts or {}
    local first_test_id = opts.first_test_id or 2000
    local seq           = opts.first_seq     or 0
    local test_ids = {}
    for i, c in ipairs(cmds) do
        c.seq     = seq
        c.test_id = first_test_id + (i - 1)
        seq = seq + 1
        self:send_command(c)
        test_ids[#test_ids + 1] = c.test_id
    end
    return test_ids
end

-- Pump until expected_count kb_dones have arrived (or timeout). Returns
-- a stats table { sent, ack=ack_count, hb=hb_count, done=#dones,
-- ok=count_ok, fail=count_fail }.
function peer_mt:wait_for_dones(expected_count, timeout_s)
    self:pump_until(function(self_)
        return #self_.dones >= expected_count
    end, timeout_s or 60)
    local ok, fail = 0, 0
    for _, d in ipairs(self.dones) do
        if d.success then ok = ok + 1 else fail = fail + 1 end
    end
    return {
        ack    = self.ack_count,
        hb     = self.hb_count,
        done   = #self.dones,
        ok     = ok,
        fail   = fail,
    }
end

function peer_mt:close()
    pcall(function() self.ps:disconnect() end)
    pcall(function() self.ps:destroy() end)
end

return M
