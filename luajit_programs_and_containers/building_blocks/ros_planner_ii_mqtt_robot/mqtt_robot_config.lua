--[[
    mqtt_robot_config.lua -- MQTT robot configuration loader and status publisher.

    Like robot_config.lua but for MQTT robots (ESP32, Pico 2 W, or LuaJIT).
    No NATS KV validation — class config comes from the JSON config file.
    Status published as MQTT retained messages.

    Config JSON format:
        {
            "robot_id": "rover_1",
            "site": "moonbase.alpha.surface_ops",
            "mqtt_host": "localhost",
            "mqtt_port": 1883,
            "robot_class": "lunar_rover",
            "remote_json": "remote.json",
            "energy_max": 10000,
            "energy_infinite": false,
            "capabilities": ["init_check", "path_spline", ...]
        }

    Status topics (MQTT retained):
        {site_path}/robots/{id}/status/state    — connected, started_at
        {site_path}/robots/{id}/status/energy   — energy_max, energy_remaining
        {site_path}/robots/{id}/status/bitmask  — raw, fields, heartbeat
]]

local json_util = require("json_util")

local M = {}

---------------------------------------------------------------------------
-- Load and parse config file
---------------------------------------------------------------------------

local function load_config_file(path)
    local f, err = io.open(path, "r")
    if not f then return nil, "cannot open config: " .. (err or path) end
    local content = f:read("*a")
    f:close()

    local ok, config = pcall(json_util.decode, content)
    if not ok then return nil, "invalid JSON in config: " .. tostring(config) end

    if not config.robot_id then return nil, "config missing robot_id" end
    if not config.site then return nil, "config missing site" end
    if not config.remote_json then return nil, "config missing remote_json" end

    -- Defaults
    config.mqtt_host = config.mqtt_host or "localhost"
    config.mqtt_port = config.mqtt_port or 1883
    config.robot_class = config.robot_class or "unknown"
    config.energy_max = config.energy_max or 10000
    config.energy_infinite = config.energy_infinite or false
    config.capabilities = config.capabilities or {}
    config.wire_format = config.wire_format or "json"

    return config
end

---------------------------------------------------------------------------
-- MQTT status publisher
---------------------------------------------------------------------------

local function make_status_publisher(mqtt_host, mqtt_port, site, robot_id)
    local pubsub_mod = require("lib.mqtt_pubsub")
    local site_path = site:gsub("%.", "/")
    local prefix = site_path .. "/robots/" .. robot_id .. "/status/"

    local ps = pubsub_mod.PubSub.new(mqtt_host, mqtt_port,
        "robot_status_" .. robot_id)
    ps:connect(5000)

    local pub = {}

    function pub:put(key, value)
        -- key is like "state", "energy", "bitmask"
        ps:publish(prefix .. key, value, 1, true)  -- retained
    end

    function pub:delete(key)
        -- MQTT: publish empty retained message to delete
        ps:publish(prefix .. key, "", 1, true)
    end

    function pub:disconnect()
        pcall(function() ps:disconnect() end)
    end

    function pub:destroy()
        pcall(function() ps:destroy() end)
    end

    pub._ps = ps
    pub._prefix = prefix
    return pub
end

---------------------------------------------------------------------------
-- Slot claim / release
---------------------------------------------------------------------------

local function claim_slot(status_pub, robot_id, energy_max, wire_format)
    -- Publish connected state (includes wire_format so bridge can detect it)
    status_pub:put("state", json_util.encode({
        connected     = true,
        robot_id      = robot_id,
        wire_format   = wire_format,
        active_kb     = "",
        active_worker = "",
        started_at    = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    -- Publish initial energy
    status_pub:put("energy", json_util.encode({
        energy_max       = energy_max,
        energy_remaining = energy_max,
        robot_id         = robot_id,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    return energy_max  -- start at full (no recovery from MQTT retained)
end

local function release_slot(status_pub, robot_id, energy_remaining, energy_max)
    status_pub:put("state", json_util.encode({
        connected     = false,
        robot_id      = robot_id,
        active_kb     = "",
        active_worker = "",
        stopped_at    = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    status_pub:put("energy", json_util.encode({
        energy_max       = energy_max,
        energy_remaining = energy_remaining,
        robot_id         = robot_id,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))
end

---------------------------------------------------------------------------
-- Public API: load config, connect, claim slot
---------------------------------------------------------------------------

function M.load(config_path)
    local config, err = load_config_file(config_path)
    if not config then return nil, err end

    local site      = config.site
    local robot_id  = config.robot_id
    local mqtt_host = config.mqtt_host
    local mqtt_port = config.mqtt_port

    -- Connect MQTT transport for rpc/stream (with wire_format)
    local mqtt_transport = require("mqtt_transport")
    local tx = mqtt_transport.remote_side(robot_id, mqtt_host, mqtt_port, site, {
        wire_format = config.wire_format,
    })
    tx:flush()

    -- Connect status publisher (separate connection for retained status)
    local status_pub = make_status_publisher(mqtt_host, mqtt_port, site, robot_id)

    -- Claim slot
    local energy_remaining = claim_slot(status_pub, robot_id, config.energy_max, config.wire_format)

    local result = {
        robot_id         = robot_id,
        site             = site,
        mqtt_host        = mqtt_host,
        mqtt_port        = mqtt_port,
        robot_class      = config.robot_class,
        remote_json      = config.remote_json,
        energy_max       = config.energy_max,
        energy_infinite  = config.energy_infinite,
        energy_remaining = energy_remaining,
        wire_format      = config.wire_format,
        capabilities     = config.capabilities,
        transport        = tx,
        status_pub       = status_pub,
    }

    function result.cleanup(final_energy)
        release_slot(status_pub, robot_id,
            final_energy or energy_remaining, config.energy_max)
        tx:close()
        status_pub:disconnect()
        status_pub:destroy()
    end

    return result
end

---------------------------------------------------------------------------
-- Energy persistence: save to MQTT retained
---------------------------------------------------------------------------

function M.save_energy(status_pub, site, robot_id, energy_max, energy_remaining)
    pcall(function()
        status_pub:put("energy", json_util.encode({
            energy_max       = energy_max,
            energy_remaining = energy_remaining,
            robot_id         = robot_id,
            timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        }))
    end)
end

---------------------------------------------------------------------------
-- Bitmask + heartbeat publishing
---------------------------------------------------------------------------

function M.publish_bitmask(status_pub, site, robot_id, kb_name, raw, fields)
    -- Set heartbeat bit (bit 0)
    if raw % 2 == 0 then raw = raw + 1 end
    fields.heartbeat = true

    pcall(function()
        status_pub:put("bitmask", json_util.encode({
            kb_name   = kb_name,
            raw       = raw,
            fields    = fields,
            robot_id  = robot_id,
            timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        }))
    end)
end

return M
