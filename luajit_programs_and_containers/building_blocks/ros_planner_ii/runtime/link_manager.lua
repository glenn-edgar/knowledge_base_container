--[[
    link_manager.lua -- Per-robot link state machine (URLP v1).

    Tracks heartbeat freshness, manages three-way handshake, detects
    stale/offline robots. Runs inside the planner process.

    States: offline → announcing → registering → live → stale → offline

    Usage:
        local link_manager = require("link_manager")
        local lm = link_manager.new(mqtt_hub, kv_writer)

        -- In tick loop:
        lm:on_heartbeat(robot_id, payload)    -- called when heartbeat arrives
        lm:on_confirm(robot_id, payload)      -- called when link_confirm arrives
        lm:on_disconnect(robot_id, payload)   -- called when link_disconnect arrives
        lm:tick()                              -- check freshness, send planner heartbeats
        lm:is_live(robot_id) → bool           -- planner gates on this

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

function M.new(mqtt_hub, kv_writer, site)
    return setmetatable({
        mqtt_hub  = mqtt_hub,
        kv_writer = kv_writer,
        site      = site,
        -- Per-robot state: robot_id → { link_state, last_heartbeat, heartbeat_seq,
        --   wire_format, capabilities, energy_max, registered_at, stale_since }
        robots = {},
        -- Planner heartbeat timer
        last_planner_hb = os.time(),
        -- Ack sequence counter
        ack_seq = 0,
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

---------------------------------------------------------------------------
-- Event handlers (called by main loop when MQTT messages arrive)
---------------------------------------------------------------------------

function M:on_heartbeat(robot_id, payload)
    local r = get_robot(self, robot_id)
    local now = os.time()

    -- Parse heartbeat
    local ok, data = pcall(json_util.decode, payload)
    if not ok or not data then return end
    if data.type ~= "link_heartbeat" then return end

    r.last_heartbeat = now
    r.heartbeat_seq = data.seq or 0
    r.energy_remaining = data.energy_remaining or 0

    if r.link_state == "offline" or data.link_state == "announcing" then
        -- Robot is announcing — start handshake
        r.link_state = "registering"
        r.registered_at = now
        self.ack_seq = self.ack_seq + 1

        -- Send planner ack
        self.mqtt_hub:send_planner_ack(robot_id, json_util.encode({
            type       = "link_bridge_ack",
            robot_id   = robot_id,
            bridge_id  = "planner",
            ack_seq    = self.ack_seq,
            ts         = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        }))

        io.stderr:write(string.format("LINK: %s → registering (ack_seq=%d)\n",
            robot_id, self.ack_seq))

    elseif r.link_state == "stale" then
        -- Robot came back from stale
        r.link_state = "live"
        r.stale_since = nil
        self:_write_link_kv(robot_id, r)
        io.stderr:write(string.format("LINK: %s → live (recovered from stale)\n", robot_id))

    elseif r.link_state == "live" then
        -- Normal heartbeat — update KV
        self:_write_link_kv(robot_id, r)
    end
end

function M:on_confirm(robot_id, payload)
    local r = get_robot(self, robot_id)

    local ok, data = pcall(json_util.decode, payload)
    if not ok or not data then return end
    if data.type ~= "link_confirm" then return end

    if r.link_state ~= "registering" then return end

    -- Complete handshake
    r.link_state = "live"
    r.wire_format = data.wire_format or "json"
    r.capabilities = data.capabilities or {}
    r.energy_max = data.energy_max or 10000
    r.stale_since = nil

    -- Tell MQTT hub about wire format
    self.mqtt_hub:set_wire_format(robot_id, r.wire_format)

    -- Write to KV for external consumers
    self:_write_link_kv(robot_id, r)

    io.stderr:write(string.format("LINK: %s → live (wire=%s, caps=%d)\n",
        robot_id, r.wire_format, #r.capabilities))
end

function M:on_disconnect(robot_id, payload)
    local r = self.robots[robot_id]
    if not r then return end

    local ok, data = pcall(json_util.decode, payload)
    if ok and data and data.energy_remaining then
        r.energy_remaining = data.energy_remaining
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
                self:_deregister(robot_id, "stale_timeout")
            end
        end
    end

    -- Send planner heartbeats to all live robots
    if now - self.last_planner_hb >= PLANNER_HB_INTERVAL then
        self.last_planner_hb = now
        local hb_json = json_util.encode({
            type      = "link_bridge_heartbeat",
            bridge_id = "planner",
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
