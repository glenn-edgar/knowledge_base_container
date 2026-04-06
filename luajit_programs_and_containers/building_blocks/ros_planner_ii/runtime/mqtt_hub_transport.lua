--[[
    mqtt_hub_transport.lua -- Shared MQTT client for the planner.

    Single PubSub connection handles ALL robots. Wildcard subscriptions
    route messages by robot_id parsed from the topic. Replaces both
    nats_transport.hub_side and the separate mqtt_bridge process.

    Usage:
        local hub_tx = require("mqtt_hub_transport")
        local hub = hub_tx.new(mqtt_host, mqtt_port, site)
        hub:connect()

        -- In tick loop:
        local msgs = hub:poll()    -- {robot_id, msg_type, payload, topic}
        hub:send_rpc("rover_1", json_str)

        -- Shutdown:
        hub:close()
]]

local json_util = require("json_util")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function M.new(host, port, site, opts)
    opts = opts or {}
    local site_path = site:gsub("%.", "/")

    return setmetatable({
        host      = host or "localhost",
        port      = port or 1883,
        site      = site,
        site_path = site_path,
        ps        = nil,  -- PubSub handle, set on connect()
        -- Topic prefixes
        robots_prefix     = site_path .. "/robots/",
        robots_prefix_len = #(site_path .. "/robots/"),
        -- Per-robot stream buffers: robot_id → {payload, ...}
        stream_buffers = {},
        -- Per-robot CBOR codec: robot_id → cbor_mod or nil
        wire_formats   = {},
    }, M)
end

---------------------------------------------------------------------------
-- Connect and subscribe to wildcard topics
---------------------------------------------------------------------------

function M:connect()
    local pubsub = require("lib.mqtt_pubsub")
    self.ps = pubsub.PubSub.new(self.host, self.port, "planner_hub")
    self.ps:connect(5000)

    -- Subscribe to all robot streams, status, and link protocol
    self.ps:subscribe(self.site_path .. "/robots/+/stream_bus", 1)
    self.ps:subscribe(self.site_path .. "/robots/+/status/#", 1)
    self.ps:subscribe(self.site_path .. "/robots/+/link", 1)

    -- Drain stale retained messages
    repeat
        local stale = self.ps:poll(50)
    until #stale == 0
end

---------------------------------------------------------------------------
-- Topic parsing
---------------------------------------------------------------------------

function M:parse_robot_id(topic)
    if topic:sub(1, self.robots_prefix_len) ~= self.robots_prefix then
        return nil, nil
    end
    local rest = topic:sub(self.robots_prefix_len + 1)
    local slash = rest:find("/")
    if not slash then return nil, nil end

    local robot_id = rest:sub(1, slash - 1)
    local suffix = rest:sub(slash + 1)  -- "stream_bus", "status/heartbeat", etc.
    return robot_id, suffix
end

---------------------------------------------------------------------------
-- Poll: drain all MQTT messages, return parsed list
---------------------------------------------------------------------------

function M:poll(timeout_ms)
    timeout_ms = timeout_ms or 1
    local msgs = self.ps:poll(timeout_ms)
    local result = {}

    for _, msg in ipairs(msgs) do
        if #msg.payload == 0 then goto continue end  -- skip empty retained clears

        local robot_id, suffix = self:parse_robot_id(msg.topic)
        if not robot_id then goto continue end

        local msg_type
        if suffix == "stream_bus" then
            msg_type = "stream"
        elseif suffix == "link" then
            msg_type = "link"
        elseif suffix == "status/heartbeat" then
            msg_type = "heartbeat"
        elseif suffix == "status/state" then
            msg_type = "status_state"
        elseif suffix == "status/energy" then
            msg_type = "status_energy"
        elseif suffix == "status/bitmask" then
            msg_type = "status_bitmask"
        else
            msg_type = "unknown"
        end

        -- Decode CBOR → JSON for robots with wire_format=cbor
        local payload = msg.payload
        if self.wire_formats[robot_id] == "cbor" then
            local cbor = require("lib.lua_cbor")
            local ok, decoded = pcall(cbor.decode, payload)
            if ok then
                payload = decoded
            end
        end

        result[#result + 1] = {
            robot_id = robot_id,
            msg_type = msg_type,
            payload  = payload,
            topic    = msg.topic,
            retain   = msg.retain,
        }

        ::continue::
    end

    return result
end

---------------------------------------------------------------------------
-- Per-robot stream buffer (for queue_monitor compatibility)
---------------------------------------------------------------------------

function M:buffer_stream(robot_id, payload)
    if not self.stream_buffers[robot_id] then
        self.stream_buffers[robot_id] = {}
    end
    local buf = self.stream_buffers[robot_id]
    buf[#buf + 1] = payload
end

function M:recv_stream(robot_id)
    local buf = self.stream_buffers[robot_id]
    if not buf or #buf == 0 then return nil end
    return table.remove(buf, 1)
end

---------------------------------------------------------------------------
-- Send RPC command to a specific robot
---------------------------------------------------------------------------

function M:send_rpc(robot_id, json_str)
    local wire = json_str
    local fmt = self.wire_formats[robot_id]
    if fmt == "cbor" then
        local cbor = require("lib.lua_cbor")
        wire = cbor.encode(json_str)
    end
    local topic = self.site_path .. "/robots/" .. robot_id .. "/rpc"
    self.ps:publish(topic, wire, 1, false)
    return "ok"
end

---------------------------------------------------------------------------
-- Publish to planner/ control topics (ack, heartbeat, disconnect)
---------------------------------------------------------------------------

function M:send_planner_ack(robot_id, json_str)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/planner/ack"
    self.ps:publish(topic, json_str, 1, false)
end

function M:send_planner_heartbeat(robot_id, json_str)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/planner/heartbeat"
    self.ps:publish(topic, json_str, 1, false)
end

function M:send_planner_disconnect(robot_id, json_str)
    local topic = self.site_path .. "/robots/" .. robot_id .. "/planner/disconnect"
    self.ps:publish(topic, json_str, 1, false)
end

---------------------------------------------------------------------------
-- Wire format management
---------------------------------------------------------------------------

function M:set_wire_format(robot_id, format)
    self.wire_formats[robot_id] = format
end

---------------------------------------------------------------------------
-- Per-robot transport adapter (queue_monitor compatible)
--
-- Returns a lightweight handle with send_rpc/recv_stream for one robot.
-- Backed by the shared connection.
---------------------------------------------------------------------------

function M:robot_transport(robot_id)
    local hub = self
    local tx = {}

    function tx:send_rpc(json_str)
        return hub:send_rpc(robot_id, json_str)
    end

    function tx:recv_stream()
        return hub:recv_stream(robot_id)
    end

    function tx:flush()
        hub.stream_buffers[robot_id] = {}
    end

    function tx:close()
        hub.stream_buffers[robot_id] = nil
    end

    return tx
end

---------------------------------------------------------------------------
-- Clear retained status topics for a robot
---------------------------------------------------------------------------

function M:clear_retained(robot_id)
    local prefix = self.site_path .. "/robots/" .. robot_id .. "/status/"
    for _, suffix in ipairs({"state", "energy", "bitmask"}) do
        self.ps:publish(prefix .. suffix, "", 1, true)
    end
end

---------------------------------------------------------------------------
-- Publish to kv-bridge container (fire-and-forget, never blocks)
--
-- The kv-bridge subscribes to MQTT_TOPIC (default: kv_bridge/write) and
-- writes to NATS KV asynchronously. This keeps all KV writes off the
-- hub tick path.
---------------------------------------------------------------------------

function M:publish_kv(bucket, key, value)
    local payload = '{"bucket":"' .. bucket .. '","key":"' .. key .. '","value":' .. value .. '}'
    self.ps:publish("kv_bridge/write", payload, 0, false)
end

function M:delete_kv(bucket, key)
    local payload = '{"bucket":"' .. bucket .. '","key":"' .. key .. '","op":"delete"}'
    self.ps:publish("kv_bridge/write", payload, 0, false)
end

---------------------------------------------------------------------------
-- Poll and route: poll MQTT, auto-buffer stream messages, return the rest
--
-- Convenience for tick loops. Stream messages go into per-robot buffers
-- (readable via robot_transport:recv_stream()). Non-stream messages
-- (heartbeats, status) are returned for link_manager / caller handling.
---------------------------------------------------------------------------

function M:poll_and_route(timeout_ms)
    local msgs = self:poll(timeout_ms)
    local non_stream = {}
    for _, msg in ipairs(msgs) do
        if msg.msg_type == "stream" then
            self:buffer_stream(msg.robot_id, msg.payload)
        else
            non_stream[#non_stream + 1] = msg
        end
    end
    return non_stream
end

---------------------------------------------------------------------------
-- Shutdown
---------------------------------------------------------------------------

function M:close()
    if self.ps then
        pcall(function() self.ps:disconnect() end)
        pcall(function() self.ps:destroy() end)
        self.ps = nil
    end
end

return M
