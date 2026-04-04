--[[
    mqtt_transport.lua -- MQTT-based hub↔robot transport using PubSub.

    Drop-in replacement for nats_transport.lua. Same API surface:
      hub_side:    send_rpc / recv_stream / flush / close
      remote_side: recv_rpc / send_stream / flush / close
      loopback:    in-memory queues (no broker needed)

    Backed by mqtt_pubsub.PubSub (subscribe once, poll for messages).
    Topics use '/' separator:
      {site_path}/robots/{id}/rpc          -- hub → robot commands
      {site_path}/robots/{id}/stream_bus   -- robot → hub events

    QoS 1 for reliable delivery. No re-subscribe per read.
]]

local M = {}

local MQTT_HOST = "localhost"
local MQTT_PORT = 1883

---------------------------------------------------------------------------
-- Topic helpers
---------------------------------------------------------------------------

local function make_topics(site, robot_id)
    local site_path = site:gsub("%.", "/")
    return {
        rpc    = site_path .. "/robots/" .. robot_id .. "/rpc",
        stream = site_path .. "/robots/" .. robot_id .. "/stream_bus",
    }
end

---------------------------------------------------------------------------
-- Hub side
---------------------------------------------------------------------------

function M.hub_side(robot_id, host, port, site)
    host = host or MQTT_HOST
    port = port or MQTT_PORT
    site = site or "moonbase.alpha.surface_ops"

    local pubsub = require("lib.mqtt_pubsub")
    local topics = make_topics(site, robot_id)

    local ps = pubsub.PubSub.new(host, port, "hub_" .. robot_id)
    ps:connect(5000)
    ps:subscribe(topics.stream, 1)

    local buffer = {}

    local tx = {}

    function tx:flush()
        -- Drain any stale messages
        repeat
            local msgs = ps:poll(10)
            if #msgs == 0 then break end
        until false
        buffer = {}
    end

    function tx:send_rpc(json_str)
        ps:publish(topics.rpc, json_str, 1, false)
        return "ok"
    end

    function tx:recv_stream()
        if #buffer > 0 then
            return table.remove(buffer, 1)
        end
        local msgs = ps:poll(1)  -- 1ms poll — fast, non-blocking
        if #msgs == 0 then return nil end
        for i = 2, #msgs do
            buffer[#buffer + 1] = msgs[i].payload
        end
        return msgs[1].payload
    end

    function tx:close()
        pcall(function() ps:disconnect() end)
        pcall(function() ps:destroy() end)
    end

    return tx
end

---------------------------------------------------------------------------
-- Remote side
---------------------------------------------------------------------------

function M.remote_side(robot_id, host, port, site, opts)
    host = host or MQTT_HOST
    port = port or MQTT_PORT
    site = site or "moonbase.alpha.surface_ops"
    opts = opts or {}

    local wire_format = opts.wire_format or "json"
    local cbor_mod = nil
    if wire_format == "cbor" then
        cbor_mod = require("lib.lua_cbor")
    end

    local pubsub = require("lib.mqtt_pubsub")
    local topics = make_topics(site, robot_id)

    local ps = pubsub.PubSub.new(host, port, "robot_" .. robot_id)
    ps:connect(5000)
    ps:subscribe(topics.rpc, 1)

    local buffer = {}

    local tx = {}

    function tx:flush()
        repeat
            local msgs = ps:poll(10)
            if #msgs == 0 then break end
        until false
        buffer = {}
    end

    function tx:recv_rpc()
        if #buffer > 0 then
            return table.remove(buffer, 1)
        end
        local msgs = ps:poll(1)
        if #msgs == 0 then return nil end
        -- Buffer remaining, decode first
        for i = 2, #msgs do
            local payload = msgs[i].payload
            if cbor_mod then payload = cbor_mod.decode(payload) end
            buffer[#buffer + 1] = payload
        end
        local first = msgs[1].payload
        if cbor_mod then first = cbor_mod.decode(first) end
        return first
    end

    function tx:send_stream(json_str)
        local payload = json_str
        if cbor_mod then payload = cbor_mod.encode(json_str) end
        ps:publish(topics.stream, payload, 1, false)
        return "ok"
    end

    function tx:close()
        pcall(function() ps:disconnect() end)
        pcall(function() ps:destroy() end)
    end

    return tx
end

---------------------------------------------------------------------------
-- Loopback (in-process, no broker)
---------------------------------------------------------------------------

function M.loopback()
    local rpc_queue = {}
    local stream_queue = {}

    local hub = {}
    function hub:send_rpc(json)
        rpc_queue[#rpc_queue + 1] = json
        return "ok"
    end
    function hub:recv_stream()
        if #stream_queue == 0 then return nil end
        return table.remove(stream_queue, 1)
    end
    function hub:flush() end
    function hub:close() end

    local remote = {}
    function remote:recv_rpc()
        if #rpc_queue == 0 then return nil end
        return table.remove(rpc_queue, 1)
    end
    function remote:send_stream(json)
        stream_queue[#stream_queue + 1] = json
        return "ok"
    end
    function remote:flush() end
    function remote:close() end

    return hub, remote
end

return M
