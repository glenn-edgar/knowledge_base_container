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
        link_out = site_path .. "/robots/" .. robot_id .. "/link",
        link_in  = site_path .. "/robots/" .. robot_id .. "/planner/#",
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
    ps:subscribe(topics.link_in, 1)

    local rpc_buffer = {}
    local link_buffer = {}
    local planner_prefix = site:gsub("%.", "/") .. "/robots/" .. robot_id .. "/planner/"

    local tx = {}

    -- Drain poll into rpc vs link buffers by topic
    local function drain_poll(timeout_ms)
        local msgs = ps:poll(timeout_ms or 1)
        for _, m in ipairs(msgs) do
            if m.topic:sub(1, #planner_prefix) == planner_prefix then
                link_buffer[#link_buffer + 1] = m.payload
            else
                local payload = m.payload
                if cbor_mod then payload = cbor_mod.decode(payload) end
                rpc_buffer[#rpc_buffer + 1] = payload
            end
        end
    end

    function tx:flush()
        repeat
            local msgs = ps:poll(10)
            if #msgs == 0 then break end
        until false
        rpc_buffer = {}
        link_buffer = {}
    end

    function tx:recv_rpc()
        if #rpc_buffer > 0 then
            return table.remove(rpc_buffer, 1)
        end
        drain_poll(1)
        if #rpc_buffer > 0 then
            return table.remove(rpc_buffer, 1)
        end
        return nil
    end

    function tx:send_stream(json_str)
        local payload = json_str
        if cbor_mod then payload = cbor_mod.encode(json_str) end
        ps:publish(topics.stream, payload, 1, false)
        return "ok"
    end

    -- Link protocol: robot → planner
    function tx:send_link(json_str)
        ps:publish(topics.link_out, json_str, 1, false)
    end

    -- Link protocol: planner → robot (returns array of payloads or nil)
    function tx:recv_link()
        drain_poll(0)
        if #link_buffer == 0 then return nil end
        local result = link_buffer
        link_buffer = {}
        return result
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
    local link_to_hub = {}     -- robot → planner
    local link_to_robot = {}   -- planner → robot

    local hub = {}
    function hub:send_rpc(json)
        rpc_queue[#rpc_queue + 1] = json
        return "ok"
    end
    function hub:recv_stream()
        if #stream_queue == 0 then return nil end
        return table.remove(stream_queue, 1)
    end
    -- Link: planner → robot
    function hub:send_link(json)
        link_to_robot[#link_to_robot + 1] = json
    end
    -- Link: robot → planner
    function hub:recv_link()
        if #link_to_hub == 0 then return nil end
        local result = link_to_hub
        link_to_hub = {}
        return result
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
    -- Link: robot → planner
    function remote:send_link(json)
        link_to_hub[#link_to_hub + 1] = json
    end
    -- Link: planner → robot
    function remote:recv_link()
        if #link_to_robot == 0 then return nil end
        local result = link_to_robot
        link_to_robot = {}
        return result
    end
    function remote:flush() end
    function remote:close() end

    return hub, remote
end

return M
