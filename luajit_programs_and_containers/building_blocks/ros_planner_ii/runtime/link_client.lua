--[[
    link_client.lua -- Robot-side link protocol (URLP v1).

    Manages the three-way handshake with the planner and monitors
    planner liveness via heartbeat. Symmetric counterpart to
    link_manager.lua on the planner side.

    States: offline → announcing → wait_ack → live → planner_lost → offline

    On planner loss (missed heartbeats or seq reset):
      - Aborts active mission via callback
      - Returns to announcing state

    Usage:
        local link_client = require("link_client")
        local lc = link_client.new({
            robot_id     = "rover_1",
            class_name   = "lunar_rover",
            transport    = mqtt_transport,   -- needs :send_link() and :recv_link()
            wire_format  = "json",
            capabilities = {"init_check", "path_spline", ...},
            energy_max   = 10000,
            on_planner_lost = function(reason) ... end,  -- abort mission callback
        })

        -- In tick loop:
        lc:tick()
        if lc:is_live() then
            -- process missions
        end

        -- Shutdown:
        lc:shutdown()
]]

local json_util = require("json_util")

local M = {}
M.__index = M

-- Protocol constants
local ANNOUNCE_INTERVAL        = 2      -- seconds between announce sends
local ACK_TIMEOUT              = 10     -- seconds to wait for bridge_ack
local PLANNER_HB_MISS_LIMIT   = 3      -- consecutive misses → planner_lost
local PLANNER_HB_INTERVAL     = 3      -- expected interval (matches link_manager)
local HEARTBEAT_INTERVAL       = 2      -- seconds between robot keepalive sends

function M.new(opts)
    assert(opts.robot_id, "link_client: robot_id required")
    assert(opts.class_name, "link_client: class_name required")
    assert(opts.transport, "link_client: transport required")

    return setmetatable({
        robot_id        = opts.robot_id,
        class_name      = opts.class_name,
        transport       = opts.transport,
        wire_format     = opts.wire_format or "json",
        capabilities    = opts.capabilities or {},
        energy_max      = opts.energy_max or 10000,
        energy_remaining = opts.energy_remaining or opts.energy_max or 10000,
        on_planner_lost = opts.on_planner_lost,

        -- State
        link_state      = "offline",
        announce_seq    = 0,

        -- Timers (wall-clock seconds)
        last_announce   = 0,
        ack_deadline    = nil,
        last_planner_hb = 0,
        planner_hb_seq  = nil,
        last_keepalive  = 0,
    }, M)
end

---------------------------------------------------------------------------
-- State queries
---------------------------------------------------------------------------

function M:is_live()
    return self.link_state == "live"
end

function M:get_state()
    return self.link_state
end

---------------------------------------------------------------------------
-- Tick: called every iteration of the robot main loop
---------------------------------------------------------------------------

function M:tick()
    local now = os.time()

    -- Process incoming link messages from planner
    self:_poll_link_messages()

    if self.link_state == "offline" or self.link_state == "announcing" then
        -- Send periodic announce
        if now - self.last_announce >= ANNOUNCE_INTERVAL then
            self:_send_announce(now)
        end

    elseif self.link_state == "wait_ack" then
        -- Check ack timeout
        if self.ack_deadline and now >= self.ack_deadline then
            io.stderr:write(string.format(
                "LINK_CLIENT [%s]: ack timeout → announcing\n", self.robot_id))
            self.link_state = "announcing"
            self.ack_deadline = nil
        end

    elseif self.link_state == "live" then
        -- Monitor planner heartbeats
        local age = now - self.last_planner_hb
        if age > PLANNER_HB_MISS_LIMIT * PLANNER_HB_INTERVAL then
            self:_planner_lost("heartbeat_timeout")
        end

        -- Send keepalive heartbeats to planner
        if now - self.last_keepalive >= HEARTBEAT_INTERVAL then
            self:_send_heartbeat(now)
        end
    end
end

---------------------------------------------------------------------------
-- Internal: send messages
---------------------------------------------------------------------------

function M:_send_announce(now)
    self.announce_seq = self.announce_seq + 1
    self.last_announce = now

    self.transport:send_link(json_util.encode({
        type       = "link_announce",
        robot_id   = self.robot_id,
        class_name = self.class_name,
        seq        = self.announce_seq,
        energy_remaining = self.energy_remaining,
        ts         = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    if self.link_state == "offline" then
        self.link_state = "announcing"
        io.stderr:write(string.format(
            "LINK_CLIENT [%s]: announcing (seq=%d)\n",
            self.robot_id, self.announce_seq))
    end
end

function M:_send_confirm()
    self.transport:send_link(json_util.encode({
        type         = "link_confirm",
        robot_id     = self.robot_id,
        class_name   = self.class_name,
        wire_format  = self.wire_format,
        capabilities = self.capabilities,
        energy_max   = self.energy_max,
        energy_remaining = self.energy_remaining,
        ts           = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))
end

function M:_send_heartbeat(now)
    self.last_keepalive = now

    self.transport:send_link(json_util.encode({
        type             = "link_heartbeat",
        robot_id         = self.robot_id,
        energy_remaining = self.energy_remaining,
        ts               = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))
end

function M:_send_disconnect(reason)
    self.transport:send_link(json_util.encode({
        type             = "link_disconnect",
        robot_id         = self.robot_id,
        reason           = reason,
        energy_remaining = self.energy_remaining,
        ts               = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))
end

---------------------------------------------------------------------------
-- Internal: poll and dispatch incoming link messages
---------------------------------------------------------------------------

function M:_poll_link_messages()
    local msgs = self.transport:recv_link()
    if not msgs then return end

    for _, raw in ipairs(msgs) do
        local ok, data = pcall(json_util.decode, raw)
        if ok and data then
            self:_handle_message(data)
        end
    end
end

function M:_handle_message(data)
    local msg_type = data.type

    if msg_type == "link_bridge_ack" then
        self:_on_bridge_ack(data)

    elseif msg_type == "link_bridge_heartbeat" then
        self:_on_bridge_heartbeat(data)

    elseif msg_type == "link_bridge_disconnect" then
        self:_on_bridge_disconnect(data)
    end
end

---------------------------------------------------------------------------
-- Message handlers
---------------------------------------------------------------------------

function M:_on_bridge_ack(data)
    if self.link_state ~= "announcing" and self.link_state ~= "wait_ack" then
        return
    end

    -- Planner acknowledged our announce — send confirm
    self.link_state = "wait_ack"
    self.ack_deadline = os.time() + ACK_TIMEOUT

    io.stderr:write(string.format(
        "LINK_CLIENT [%s]: received bridge_ack (ack_seq=%s) → sending confirm\n",
        self.robot_id, tostring(data.ack_seq)))

    self:_send_confirm()

    -- Transition to live (planner will also transition on receiving confirm)
    self.link_state = "live"
    self.last_planner_hb = os.time()
    self.last_keepalive = os.time()
    self.planner_hb_seq = data.seq or 0
    self.ack_deadline = nil

    io.stderr:write(string.format(
        "LINK_CLIENT [%s]: → live\n", self.robot_id))
end

function M:_on_bridge_heartbeat(data)
    if self.link_state ~= "live" then return end

    local now = os.time()
    local new_seq = data.seq or 0

    -- Detect planner restart: seq reset to lower value
    if self.planner_hb_seq and new_seq < self.planner_hb_seq then
        self:_planner_lost("planner_restart")
        return
    end

    self.last_planner_hb = now
    self.planner_hb_seq = new_seq
end

function M:_on_bridge_disconnect(data)
    local reason = data.reason or "planner_disconnect"
    io.stderr:write(string.format(
        "LINK_CLIENT [%s]: planner disconnect (%s)\n",
        self.robot_id, reason))
    self:_planner_lost(reason)
end

---------------------------------------------------------------------------
-- Planner lost: abort mission, return to announcing
---------------------------------------------------------------------------

function M:_planner_lost(reason)
    io.stderr:write(string.format(
        "LINK_CLIENT [%s]: planner lost (%s) → offline → announcing\n",
        self.robot_id, reason))

    self.link_state = "offline"
    self.last_planner_hb = 0
    self.planner_hb_seq = nil

    -- Notify robot main loop to abort mission
    if self.on_planner_lost then
        self.on_planner_lost(reason)
    end

    -- Immediately start re-announcing
    self:_send_announce(os.time())
end

---------------------------------------------------------------------------
-- Update energy (called by robot when energy changes)
---------------------------------------------------------------------------

function M:set_energy(remaining)
    self.energy_remaining = remaining
end

---------------------------------------------------------------------------
-- Shutdown: notify planner, go offline
---------------------------------------------------------------------------

function M:shutdown()
    if self.link_state == "live" or self.link_state == "announcing"
       or self.link_state == "wait_ack" then
        self:_send_disconnect("robot_shutdown")
    end
    self.link_state = "offline"
    io.stderr:write(string.format(
        "LINK_CLIENT [%s]: shutdown\n", self.robot_id))
end

return M
