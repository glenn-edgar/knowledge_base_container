--[[
    link_manager.lua -- Per-robot link state machine (URLP v1).

    Tracks heartbeat freshness, manages three-way handshake, detects
    stale/offline robots. Runs inside the planner process.

    Protocol messages:
      Robot → Planner:
        link_announce   — robot boot/reboot, starts handshake
        link_heartbeat  — keepalive only (no state transitions)
        link_confirm    — completes handshake (wire_format, capabilities)
        link_disconnect — clean shutdown

      Planner → Robot:
        link_bridge_ack       — acknowledges announce
        link_bridge_heartbeat — planner keepalive (seq for restart detection)
        link_bridge_disconnect — planner shutdown

    States: offline → registering → live → stale → offline

    Re-announce from a live robot is a link exception:
      - Active mission cancelled via on_link_exception callback
      - Robot deregistered, then fresh handshake starts

    Usage:
        local link_manager = require("link_manager")
        local lm = link_manager.new(mqtt_hub, kv_writer, site, {
            on_link_exception = function(robot_id, reason) ... end,
        })

        -- In tick loop:
        lm:on_announce(robot_id, payload)     -- called when link_announce arrives
        lm:on_heartbeat(robot_id, payload)    -- called when link_heartbeat arrives
        lm:on_confirm(robot_id, payload)      -- called when link_confirm arrives
        lm:on_disconnect(robot_id, payload)   -- called when link_disconnect arrives
        lm:tick()                             -- check freshness, send planner heartbeats
        lm:is_live(robot_id) → bool

        -- Shutdown:
        lm:shutdown()
]]

local json_util = require("json_util")

local M = {}
M.__index = M

-- Protocol constants
local HEARTBEAT_MISS_LIMIT     = 3      -- consecutive misses → stale
local HEARTBEAT_INTERVAL       = 2      -- seconds (robot sends this often)
local STALE_TO_OFFLINE_TIMEOUT = 15     -- seconds
local REGISTRATION_TIMEOUT     = 10     -- seconds
local PLANNER_HB_INTERVAL     = 3      -- seconds (planner sends to each robot)

function M.new(mqtt_hub, kv_writer, site, opts)
    opts = opts or {}
    return setmetatable({
        mqtt_hub  = mqtt_hub,
        kv_writer = kv_writer,
        site      = site,
        -- Per-robot state
        robots = {},
        -- Planner heartbeat timer and sequence
        last_planner_hb = os.time(),
        planner_hb_seq  = 0,
        -- Ack sequence counter
        ack_seq = 0,
        -- Callback for link exceptions (re-announce from live robot)
        on_link_exception = opts.on_link_exception,
        -- Callback for any link state change (live, offline, stale)
        on_link_change = opts.on_link_change,
    }, M)
end

---------------------------------------------------------------------------
-- Robot state access
---------------------------------------------------------------------------

local function get_robot(self, robot_id)
    if not self.robots[robot_id] then
        self.robots[robot_id] = {
            link_state      = "offline",
            last_heartbeat  = 0,
            heartbeat_seq   = 0,
            class_name      = nil,
            wire_format     = "json",
            capabilities    = {},
            energy_max      = 10000,
            energy_remaining = 0,
            registered_at   = nil,
            stale_since     = nil,
        }
    end
    return self.robots[robot_id]
end

function M:is_live(robot_id)
    local r = self.robots[robot_id]
    return r and r.link_state == "live"
end

function M:get_state(robot_id)
    local r = self.robots[robot_id]
    if not r then return "offline" end
    return r.link_state
end

function M:list_live()
    local result = {}
    for rid, r in pairs(self.robots) do
        if r.link_state == "live" then
            result[#result + 1] = rid
        end
    end
    return result
end

function M:get_capabilities(robot_id)
    local r = self.robots[robot_id]
    if not r or r.link_state ~= "live" then return {} end
    return r.capabilities or {}
end

function M:get_wire_format(robot_id)
    local r = self.robots[robot_id]
    if not r then return "json" end
    return r.wire_format or "json"
end

function M:get_energy(robot_id)
    local r = self.robots[robot_id]
    if not r then return nil end
    return r.energy_remaining, r.energy_max
end

function M:get_class(robot_id)
    local r = self.robots[robot_id]
    if not r then return nil end
    return r.class_name
end

---------------------------------------------------------------------------
-- Event: link_announce (robot boot/reboot)
---------------------------------------------------------------------------

function M:on_announce(robot_id, payload)
    local r = get_robot(self, robot_id)

    local ok, data = pcall(json_util.decode, payload)
    if not ok or not data then return end
    if data.type ~= "link_announce" then return end

    -- Re-announce from a live or registering robot → link exception
    if r.link_state == "live" or r.link_state == "registering" then
        io.stderr:write(string.format(
            "LINK: %s re-announce while %s → link exception\n",
            robot_id, r.link_state))

        -- Notify action server to cancel active mission
        if self.on_link_exception then
            self.on_link_exception(robot_id, "robot_reboot")
        end

        -- Deregister (writes offline to KV)
        self:_deregister(robot_id, "robot_reboot")
    end

    -- Start fresh handshake
    r.link_state = "registering"
    r.registered_at = os.time()
    r.last_heartbeat = os.time()
    r.class_name = data.class_name
    r.energy_remaining = data.energy_remaining or 0
    self.ack_seq = self.ack_seq + 1

    -- Send planner ack
    self.mqtt_hub:send_planner_ack(robot_id, json_util.encode({
        type       = "link_bridge_ack",
        robot_id   = robot_id,
        bridge_id  = "planner",
        ack_seq    = self.ack_seq,
        seq        = self.planner_hb_seq,
        ts         = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    io.stderr:write(string.format("LINK: %s → registering (ack_seq=%d)\n",
        robot_id, self.ack_seq))
end

---------------------------------------------------------------------------
-- Event: link_heartbeat (keepalive only — no state transitions)
---------------------------------------------------------------------------

function M:on_heartbeat(robot_id, payload)
    local r = get_robot(self, robot_id)

    local ok, data = pcall(json_util.decode, payload)
    if not ok or not data then return end
    if data.type ~= "link_heartbeat" then return end

    -- Heartbeat only accepted from live robots
    if r.link_state ~= "live" then return end

    r.last_heartbeat = os.time()
    r.energy_remaining = data.energy_remaining or r.energy_remaining

    -- Update KV
    self:_write_link_kv(robot_id, r)
end

---------------------------------------------------------------------------
-- Event: link_confirm (completes handshake)
---------------------------------------------------------------------------

function M:on_confirm(robot_id, payload)
    local r = get_robot(self, robot_id)

    local ok, data = pcall(json_util.decode, payload)
    if not ok or not data then return end
    if data.type ~= "link_confirm" then return end

    if r.link_state ~= "registering" then return end

    -- Complete handshake
    r.link_state = "live"
    r.class_name = data.class_name or r.class_name
    r.wire_format = data.wire_format or "json"
    r.capabilities = data.capabilities or {}
    r.energy_max = data.energy_max or 10000
    r.energy_remaining = data.energy_remaining or r.energy_remaining
    r.stale_since = nil

    -- Tell MQTT hub about wire format
    self.mqtt_hub:set_wire_format(robot_id, r.wire_format)

    -- Write to KV for external consumers
    self:_write_link_kv(robot_id, r)

    io.stderr:write(string.format("LINK: %s → live (wire=%s, caps=%d)\n",
        robot_id, r.wire_format, #r.capabilities))

    if self.on_link_change then
        self.on_link_change(robot_id, "live")
    end
end

---------------------------------------------------------------------------
-- Event: link_disconnect (clean shutdown)
---------------------------------------------------------------------------

function M:on_disconnect(robot_id, payload)
    local r = self.robots[robot_id]
    if not r then return end

    local ok, data = pcall(json_util.decode, payload)
    if ok and data and data.energy_remaining then
        r.energy_remaining = data.energy_remaining
    end

    -- Cancel mission if active
    if r.link_state == "live" and self.on_link_exception then
        self.on_link_exception(robot_id, "robot_disconnect")
    end

    self:_deregister(robot_id, "clean_disconnect")
end

---------------------------------------------------------------------------
-- Tick: check freshness, send planner heartbeats
---------------------------------------------------------------------------

function M:tick()
    local now = os.time()

    for robot_id, r in pairs(self.robots) do
        if r.link_state == "registering" then
            -- Check registration timeout
            if now - r.registered_at > REGISTRATION_TIMEOUT then
                r.link_state = "offline"
                io.stderr:write(string.format("LINK: %s → offline (registration timeout)\n",
                    robot_id))
            end

        elseif r.link_state == "live" then
            -- Check heartbeat freshness
            local age = now - r.last_heartbeat
            if age > HEARTBEAT_MISS_LIMIT * HEARTBEAT_INTERVAL then
                r.link_state = "stale"
                r.stale_since = now
                self:_write_link_kv(robot_id, r)
                io.stderr:write(string.format("LINK: %s → stale (no heartbeat for %ds)\n",
                    robot_id, age))
            end

        elseif r.link_state == "stale" then
            -- Check stale→offline timeout
            if r.stale_since and now - r.stale_since > STALE_TO_OFFLINE_TIMEOUT then
                -- Cancel mission if any
                if self.on_link_exception then
                    self.on_link_exception(robot_id, "stale_timeout")
                end
                self:_deregister(robot_id, "stale_timeout")
            end
        end
    end

    -- Send planner heartbeats to all live/registering robots
    if now - self.last_planner_hb >= PLANNER_HB_INTERVAL then
        self.last_planner_hb = now
        self.planner_hb_seq = self.planner_hb_seq + 1
        local hb_json = json_util.encode({
            type      = "link_bridge_heartbeat",
            bridge_id = "planner",
            seq       = self.planner_hb_seq,
            ts        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        })
        for robot_id, r in pairs(self.robots) do
            if r.link_state == "live" or r.link_state == "registering" then
                self.mqtt_hub:send_planner_heartbeat(robot_id, hb_json)
            end
        end
    end
end

---------------------------------------------------------------------------
-- Internal: write link status to KV via queue (non-blocking)
---------------------------------------------------------------------------

function M:_write_link_kv(robot_id, r)
    local key = self.site .. ".robots." .. robot_id .. ".status.link"
    self.kv_writer:push(key, json_util.encode({
        link_state        = r.link_state,
        robot_id          = robot_id,
        transport         = "mqtt",
        wire_format       = r.wire_format,
        heartbeat_seq     = r.heartbeat_seq,
        heartbeat_at      = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        registered_at     = r.registered_at and
            os.date("!%Y-%m-%dT%H:%M:%SZ", r.registered_at) or "",
        energy_remaining  = r.energy_remaining,
        missed_heartbeats = 0,
    }))
end

--- Write robot position to KV (board node name).
-- Called on initial registration and after each navigation leg.
function M:write_position(robot_id, node_name)
    local r = self.robots[robot_id]
    if r then r.current_node = node_name end
    local key = self.site .. ".robots." .. robot_id .. ".status.position"
    self.kv_writer:push(key, json_util.encode({
        robot_id     = robot_id,
        current_node = node_name,
        timestamp    = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))
end

function M:get_position(robot_id)
    local r = self.robots[robot_id]
    return r and r.current_node
end

---------------------------------------------------------------------------
-- Internal: deregister a robot
---------------------------------------------------------------------------

function M:_deregister(robot_id, reason)
    local r = self.robots[robot_id]
    if not r then return end

    r.link_state = "offline"
    r.stale_since = nil

    -- Write offline to KV
    local key = self.site .. ".robots." .. robot_id .. ".status.link"
    self.kv_writer:push(key, json_util.encode({
        link_state       = "offline",
        robot_id         = robot_id,
        transport        = "mqtt",
        energy_remaining = r.energy_remaining,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    -- Clear retained MQTT status
    self.mqtt_hub:clear_retained(robot_id)

    io.stderr:write(string.format("LINK: %s → offline (%s)\n", robot_id, reason))

    if self.on_link_change then
        self.on_link_change(robot_id, "offline")
    end
end

---------------------------------------------------------------------------
-- Shutdown: notify all robots, write offline to KV
---------------------------------------------------------------------------

function M:shutdown()
    local disc_json = json_util.encode({
        type      = "link_bridge_disconnect",
        bridge_id = "planner",
        reason    = "shutdown",
        ts        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })

    for robot_id, r in pairs(self.robots) do
        if r.link_state == "live" or r.link_state == "registering" then
            self.mqtt_hub:send_planner_disconnect(robot_id, disc_json)
            self:_deregister(robot_id, "planner_shutdown")
        end
    end
end

return M
