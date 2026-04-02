--[[
    robot_config.lua -- Robot configuration loader, inventory validator, and slot manager.

    Reads a JSON config file, connects to NATS KV, validates the robot
    against the KB inventory, claims the slot, and manages energy persistence.

    Startup sequence:
      1. Load config JSON
      2. Connect to NATS
      3. Read slot from NATS KV ({site}.robots.{id}.connection)
      4. Validate: slot exists, class matches, comm_type matches
      5. Check slot not already claimed (status.state.connected)
      6. Claim slot (publish connected=true)
      7. Read last saved energy (or default to energy_max from class)
      8. Return validated config for robot_main to use

    Usage:
        local robot_config = require("robot_config")
        local cfg, err = robot_config.load("robot.json")
        if not cfg then print(err); os.exit(1) end

        -- cfg contains:
        --   robot_id, site, nats_server, robot_class, remote_json
        --   energy_max, energy_remaining
        --   transport (nats_transport remote_side, already connected)
        --   status_ks (KeyStore for status publishing)
        --   cleanup() function to release slot on shutdown
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

    -- Validate required fields
    if not config.robot_id then return nil, "config missing robot_id" end
    if not config.site then return nil, "config missing site" end
    if not config.nats_server then return nil, "config missing nats_server" end
    if not config.robot_class then return nil, "config missing robot_class" end
    if not config.remote_json then return nil, "config missing remote_json" end

    return config
end

---------------------------------------------------------------------------
-- Connect to NATS KV and validate against inventory
---------------------------------------------------------------------------

local function validate_inventory(config)
    local ks_lib = require("lib.nats_key_store")
    local site = config.site
    local robot_id = config.robot_id
    local site_bucket = site:gsub("%.", "_")

    -- Connect to KB export bucket (published by KB exporter at startup)
    local kb_ks = ks_lib.KeyStore.new({
        server  = config.nats_server,
        bucket  = "kb_export",
        history = 1,
        client_name = "robot_validator_" .. robot_id,
    })
    local ok, err = pcall(function() kb_ks:connect() end)
    if not ok then
        return nil, "cannot connect to NATS KV: " .. tostring(err)
    end

    -- Check slot exists (robot registered in inventory)
    local conn_key = site .. ".robots." .. robot_id .. ".connection"
    local conn_json = kb_ks:get(conn_key)
    if not conn_json then
        kb_ks:disconnect(); kb_ks:destroy()
        return nil, "robot '" .. robot_id .. "' not found in inventory (no connection entry)"
    end

    local conn_ok, conn = pcall(json_util.decode, conn_json)
    if not conn_ok then
        kb_ks:disconnect(); kb_ks:destroy()
        return nil, "corrupt connection data for " .. robot_id
    end

    -- Validate comm_type matches
    if conn.comm_type and conn.comm_type ~= "nats" then
        kb_ks:disconnect(); kb_ks:destroy()
        return nil, string.format("robot '%s' comm_type is '%s', expected 'nats'",
            robot_id, conn.comm_type)
    end

    -- Read class config to get energy_max
    local class_key = site .. ".robot_class." .. config.robot_class .. ".infra"
    local class_json = kb_ks:get(class_key)
    local energy_max = 10000  -- fallback default
    local class_capabilities = {}

    if class_json then
        local cls_ok, cls = pcall(json_util.decode, class_json)
        if cls_ok and cls then
            energy_max = cls.energy_max or energy_max
            class_capabilities = cls.virtual_nodes or {}
        end
    end

    -- Read robot capabilities
    local caps_key = site .. ".robots." .. robot_id .. ".capabilities"
    local caps_json = kb_ks:get(caps_key)
    if caps_json then
        local caps_ok, caps = pcall(json_util.decode, caps_json)
        if caps_ok and caps then
            class_capabilities = caps
        end
    end

    kb_ks:disconnect()
    kb_ks:destroy()

    return {
        connection   = conn,
        energy_max   = energy_max,
        capabilities = class_capabilities,
    }
end

---------------------------------------------------------------------------
-- Slot claim: check not already connected, publish connected=true
---------------------------------------------------------------------------

local function claim_slot(status_ks, site, robot_id, energy_max)
    -- Check if slot is already claimed
    local state_key = site .. ".robots." .. robot_id .. ".status.state"
    local state_json = status_ks:get(state_key)
    if state_json then
        local ok, state = pcall(json_util.decode, state_json)
        if ok and state and state.connected == true then
            return nil, "slot '" .. robot_id .. "' already claimed (connected=true)"
        end
    end

    -- Read last saved energy (recovery after crash)
    local energy_key = site .. ".robots." .. robot_id .. ".status.energy"
    local energy_remaining = energy_max
    local energy_json = status_ks:get(energy_key)
    if energy_json then
        local ok, energy = pcall(json_util.decode, energy_json)
        if ok and energy and energy.energy_remaining then
            energy_remaining = energy.energy_remaining
        end
    end

    -- Claim the slot
    status_ks:put(state_key, json_util.encode({
        connected      = true,
        robot_id       = robot_id,
        active_kb      = "",
        active_worker  = "",
        started_at     = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    -- Publish initial energy
    status_ks:put(energy_key, json_util.encode({
        energy_max       = energy_max,
        energy_remaining = energy_remaining,
        robot_id         = robot_id,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }))

    return energy_remaining
end

---------------------------------------------------------------------------
-- Release slot on shutdown
---------------------------------------------------------------------------

local function release_slot(status_ks, site, robot_id, energy_remaining, energy_max)
    local state_key = site .. ".robots." .. robot_id .. ".status.state"
    pcall(function()
        status_ks:put(state_key, json_util.encode({
            connected    = false,
            robot_id     = robot_id,
            active_kb    = "",
            active_worker = "",
            stopped_at   = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        }))
    end)

    -- Save final energy
    local energy_key = site .. ".robots." .. robot_id .. ".status.energy"
    pcall(function()
        status_ks:put(energy_key, json_util.encode({
            energy_max       = energy_max,
            energy_remaining = energy_remaining,
            robot_id         = robot_id,
            timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        }))
    end)
end

---------------------------------------------------------------------------
-- Public API: load config, validate, claim slot
---------------------------------------------------------------------------

function M.load(config_path)
    -- Step 1: Load config file
    local config, err = load_config_file(config_path)
    if not config then return nil, err end

    local site = config.site
    local robot_id = config.robot_id

    -- Step 2: Validate against NATS KV inventory
    local inventory, inv_err = validate_inventory(config)
    if not inventory then return nil, inv_err end

    -- Step 3: Connect NATS transport
    local nats_transport = require("nats_transport")
    local tx = nats_transport.remote_side(robot_id, config.nats_server, site)
    tx:flush()

    -- Step 4: Connect status KeyStore for publishing
    local ks_lib = require("lib.nats_key_store")
    local site_bucket = site:gsub("%.", "_")
    local status_ks = ks_lib.KeyStore.new({
        server        = config.nats_server,
        bucket        = site_bucket .. "_robot_status",
        description   = "Robot status: " .. robot_id,
        create_bucket = true,
        history       = 1,
        client_name   = "robot_status_" .. robot_id,
    })
    status_ks:connect()

    -- Step 5: Claim slot
    local energy_remaining, claim_err = claim_slot(
        status_ks, site, robot_id, inventory.energy_max)
    if not energy_remaining then
        tx:close()
        status_ks:disconnect(); status_ks:destroy()
        return nil, claim_err
    end

    -- Build result
    local result = {
        robot_id         = robot_id,
        site             = site,
        nats_server      = config.nats_server,
        robot_class      = config.robot_class,
        remote_json      = config.remote_json,
        energy_max       = inventory.energy_max,
        energy_remaining = energy_remaining,
        capabilities     = inventory.capabilities,
        connection       = inventory.connection,
        transport        = tx,
        status_ks        = status_ks,
    }

    -- Cleanup function for shutdown
    function result.cleanup(final_energy)
        release_slot(status_ks, site, robot_id,
            final_energy or energy_remaining, inventory.energy_max)
        tx:close()
        status_ks:disconnect()
        status_ks:destroy()
    end

    return result
end

---------------------------------------------------------------------------
-- Energy persistence: save to NATS KV
---------------------------------------------------------------------------

function M.save_energy(status_ks, site, robot_id, energy_max, energy_remaining)
    local energy_key = site .. ".robots." .. robot_id .. ".status.energy"
    pcall(function()
        status_ks:put(energy_key, json_util.encode({
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

function M.publish_bitmask(status_ks, site, robot_id, kb_name, raw, fields)
    local key = site .. ".robots." .. robot_id .. ".status.bitmask"
    -- Set heartbeat bit (bit 0)
    if raw % 2 == 0 then raw = raw + 1 end
    fields.heartbeat = true

    pcall(function()
        status_ks:put(key, json_util.encode({
            kb_name   = kb_name,
            raw       = raw,
            fields    = fields,
            robot_id  = robot_id,
            timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        }))
    end)
end

return M
